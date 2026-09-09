"""Synthetic exact-certificate recovery; no private data or provider I/O."""
import copy
from fractions import Fraction as F
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE / "src"))
import context_lp_r3 as context
import lp_model
import run_stage1c as inherited
import stage1c_intervals as intervals
import stage1d_experiments as batch
from collector import Scope
from scipy.optimize import linprog as real_linprog


def document(with_target=False):
    flow = {"event_id": "seed", "from_account": None, "to_account": "A|ETH",
            "amount_raw": "80", "role": "SEED", "flow_kind": "top",
            "order_basis": "OBSERVED_TOP_OR_TRACE_EXECUTION_ORDER", "evidence_ids": ["synthetic:seed"]}
    doc = {"schema_version": context.SCHEMA, "query_id": "synthetic:empty-recovery",
            "accounts": [{"account_id": "A|ETH", "initial_actual_balance_raw": "0",
                "initial_position": {"block_number": 0, "tx_index": -1, "phase": "BLOCK_END"},
                "initial_source_raw": "0", "initial_source_basis": "BEFORE_FIRST_CAUSALLY_POSSIBLE_SEED_ARRIVAL",
                "evidence_ids": ["synthetic:balance"]}],
            "transactions": [{"tx_id": "seed-tx", "block_number": 1, "tx_index": 0, "flows": [flow], "fees": []}],
            "objective_groups": {"T|ETH": ["enter"]} if with_target else {},
            "all_service_entries": ["enter"] if with_target else [], "anchors": [], "gaps": []}
    if with_target:
        candidate = dict(flow, event_id="enter", from_account="A|ETH", to_account="T|ETH", role="CANDIDATE")
        doc["transactions"].append({"tx_id": "out-tx", "block_number": 2, "tx_index": 0, "flows": [candidate], "fees": []})
    return doc


class CertificateFixture:
    def __init__(self, reject_all_upper=False):
        self.upper_count = 0
        self.reject_all_upper = reject_all_upper
        self.actual_results = []

    def certify(self, model, result, cost):
        certificate, vector, value = lp_model._certify(model, result, cost)
        if any(c < 0 for c in cost):
            self.upper_count += 1
            if self.upper_count == 1 or self.reject_all_upper:
                certificate = copy.deepcopy(certificate)
                certificate.update(certified=False, exact_dual_feasible=True,
                    exact_dual_objective=lp_model.fmt(value - F(1, 10**17)),
                    synthetic_failure_marker="FORCED_UNCERTIFIED_FIRST_UPPER_CANDIDATE")
        return certificate, vector, value

    def linprog(self, *args, **kwargs):
        result = real_linprog(*args, **kwargs)
        self.actual_results.append((copy.deepcopy(kwargs), result))
        return result


class Stage1DEmptyTargetRecoveryTests(unittest.TestCase):
    def solve(self, fixture=None, enabled=True):
        doc = document()
        model = context.build_context_model(doc, remove_balance_information=True)
        original = copy.deepcopy((model.variables, model.eq, model.rhs))
        fixture = fixture or CertificateFixture()
        with patch.object(context, "_certify", side_effect=fixture.certify), patch("scipy.optimize.linprog", side_effect=fixture.linprog):
            result = context.solve_context_interval(model, doc, ["seed"], stage1d_empty_target_recovery=enabled)
        self.assertEqual(original, (model.variables, model.eq, model.rhs))
        return doc, model, fixture, result

    def test_exact_retry_with_identical_model_and_tolerances(self):
        _, _, fixture, result = self.solve()
        self.assertEqual(result["status"], "OPTIMAL_EXACT_CERTIFIED")
        self.assertEqual((result["lower_raw"], result["upper_raw"]), ("80", "80"))
        self.assertEqual(len(fixture.actual_results), 3)
        first, retry = fixture.actual_results[1][0], fixture.actual_results[2][0]
        self.assertEqual(retry["method"], "highs-ds")
        self.assertIs(retry["options"]["presolve"], False)
        for key in ("primal_feasibility_tolerance", "dual_feasibility_tolerance"):
            self.assertEqual(first["options"][key], retry["options"][key])
        self.assertEqual(first["bounds"], retry["bounds"])
        self.assertEqual((first["A_eq"] != retry["A_eq"]).nnz, 0)
        self.assertEqual(first["b_eq"].tolist(), retry["b_eq"].tolist())
        self.assertLessEqual(retry["options"]["time_limit"], first["options"]["time_limit"])

    def test_original_failure_and_success_audits_preserved(self):
        _, _, _, result = self.solve()
        endpoint = result["endpoints"]["upper"]
        evidence = endpoint["certificate_recovery"]
        self.assertEqual(evidence["selected_attempt"], 2)
        self.assertFalse(evidence["attempts"][0]["certificate"]["certified"])
        self.assertEqual(evidence["attempts"][0]["certificate"]["synthetic_failure_marker"], "FORCED_UNCERTIFIED_FIRST_UPPER_CANDIDATE")
        self.assertTrue(evidence["attempts"][1]["certificate"]["certified"])
        self.assertTrue(evidence["attempts"][1]["independent_audit"]["exact_feasible"])
        self.assertTrue(endpoint["independent_audit"]["exact_feasible"])

    def test_selected_dual_bound_provenance_is_from_retry(self):
        _, model, fixture, result = self.solve()
        selected = fixture.actual_results[-1][1]
        expected = {}
        for i, variable in enumerate(model.variables):
            for side, dual in (("lower", selected.lower.marginals[i]), ("upper", selected.upper.marginals[i])):
                rational = lp_model._rational_candidate([dual])[0]
                if rational:
                    expected[(variable.name, side)] = lp_model.fmt(rational)
        actual = {(r["variable"], r["bound_side"]): r["exact_dual_multiplier"]
                  for r in result["endpoints"]["upper"]["nonzero_dual_bound_provenance"]}
        self.assertEqual(actual, expected)

    def test_second_uncertified_candidate_remains_unresolved(self):
        _, _, fixture, result = self.solve(CertificateFixture(reject_all_upper=True))
        self.assertEqual(len(fixture.actual_results), 3)
        self.assertEqual(result["status"], "UNRESOLVED")
        upper = result["endpoints"]["upper"]
        self.assertIsNone(upper["raw"])
        self.assertIsNone(upper["witness_event_source_raw"])
        self.assertIsNone(upper["certificate_recovery"]["selected_attempt"])

    def test_default_disabled_retains_failure_with_no_retry(self):
        _, _, fixture, result = self.solve(enabled=False)
        self.assertEqual(len(fixture.actual_results), 2)
        self.assertEqual(result["status"], "UNRESOLVED")
        self.assertNotIn("certificate_recovery", result["endpoints"]["upper"])

    def test_certified_first_result_never_retries(self):
        doc = document()
        with patch("scipy.optimize.linprog", wraps=real_linprog) as solver:
            result = context.solve_context_interval(context.build_context_model(doc), doc, ["seed"], stage1d_empty_target_recovery=True)
        self.assertEqual(solver.call_count, 2)
        self.assertEqual(result["status"], "OPTIMAL_EXACT_CERTIFIED")
        self.assertTrue(all("certificate_recovery" not in e for e in result["endpoints"].values()))

    def test_nonempty_target_rejected_by_recovery_guard(self):
        doc = document(with_target=True)
        with patch("scipy.optimize.linprog") as solver, self.assertRaisesRegex(ValueError, "empty fixed target"):
            context.solve_context_interval(context.build_context_model(doc), doc, ["seed"], stage1d_empty_target_recovery=True)
        solver.assert_not_called()

    def test_unfixed_feasibility_objective_rejected(self):
        doc = document()
        model = context.build_context_model(doc)
        model.variables[model.event_variables["seed"]].lower = F(0)
        with patch("scipy.optimize.linprog") as solver, self.assertRaisesRegex(ValueError, "exactly fixed"):
            context.solve_context_interval(model, doc, ["seed"], stage1d_empty_target_recovery=True)
        solver.assert_not_called()

    def test_independent_witness_failure_cannot_promote_retry(self):
        fixture = CertificateFixture()
        actual_auditor = context.audit_context_witness
        def reject_retry(doc, witness, **kwargs):
            result = actual_auditor(doc, witness, **kwargs)
            if fixture.upper_count > 1:
                result = dict(result, exact_feasible=False, synthetic_rejection=True)
            return result
        with patch.object(context, "audit_context_witness", side_effect=reject_retry):
            _, _, _, result = self.solve(fixture)
        self.assertEqual(result["status"], "UNRESOLVED")
        self.assertIsNone(result["endpoints"]["upper"]["raw"])

    def test_exhausted_endpoint_budget_does_not_start_retry(self):
        fixture = CertificateFixture()
        doc = document()
        with patch.object(context, "_certify", side_effect=fixture.certify), patch("scipy.optimize.linprog", side_effect=fixture.linprog), patch("time.perf_counter", side_effect=[0, 0, 61]):
            result = context.solve_context_interval(context.build_context_model(doc), doc, ["seed"], stage1d_empty_target_recovery=True)
        self.assertEqual(len(fixture.actual_results), 2)
        self.assertEqual(result["status"], "UNRESOLVED")
        evidence = result["endpoints"]["upper"]["certificate_recovery"]
        self.assertEqual(evidence["retry_skipped_reason"], "ORIGINAL_ENDPOINT_TIME_BUDGET_EXHAUSTED")

    def test_retry_exception_retains_original_failure(self):
        fixture = CertificateFixture()
        doc = document()
        def solver(*args, **kwargs):
            if kwargs.get("method") == "highs-ds":
                raise RuntimeError("synthetic solver failure")
            return fixture.linprog(*args, **kwargs)
        with patch.object(context, "_certify", side_effect=fixture.certify), patch("scipy.optimize.linprog", side_effect=solver):
            result = context.solve_context_interval(context.build_context_model(doc), doc, ["seed"], stage1d_empty_target_recovery=True)
        self.assertEqual(result["status"], "UNRESOLVED")
        attempts = result["endpoints"]["upper"]["certificate_recovery"]["attempts"]
        self.assertEqual(len(attempts), 2)
        self.assertFalse(attempts[0]["certificate"]["certified"])
        self.assertEqual(attempts[1]["error"], "RuntimeError: synthetic solver failure")

    def test_interval_error_and_extra_solver_count_propagate(self):
        for reject, expected in ((False, "COMPLETED"), (True, "ERROR")):
            with self.subTest(reject=reject):
                fixture = CertificateFixture(reject_all_upper=reject)
                with patch.object(context, "_certify", side_effect=fixture.certify):
                    result = intervals.run_interval(document(), "BALANCE_INFORMATION_REMOVED", stage1d_empty_target_recovery=True)
                self.assertEqual(result["status"], expected)
                self.assertEqual(result["task_counts"]["endpoint_optimizations"], 3)
                self.assertEqual(result["joint_by_asset"]["ETH"]["upper_raw"], "0" if not reject else None)

    def test_nonempty_interval_keeps_original_solver_path(self):
        fixture = CertificateFixture()
        with patch.object(context, "_certify", side_effect=fixture.certify), patch("scipy.optimize.linprog", side_effect=fixture.linprog):
            result = intervals.run_interval(document(with_target=True), stage1d_empty_target_recovery=True)
        self.assertEqual(result["status"], "ERROR")
        self.assertEqual(len(fixture.actual_results), 2)

    def test_dispatch_default_and_explicit_stage1d_option(self):
        with patch.object(inherited, "run_interval", return_value={}) as runner:
            inherited.dispatch(document(), "FULL_INTERVAL")
            self.assertEqual(runner.call_args.kwargs, {})
            inherited.dispatch(document(), "FULL_INTERVAL", stage1d_empty_target_recovery=True)
            self.assertEqual(runner.call_args.kwargs, {"stage1d_empty_target_recovery": True})

    def test_measure_six_calls_preserve_explicit_flag_and_determinism(self):
        result = intervals.run_interval(document())
        with patch.object(inherited, "dispatch", return_value=result) as runner:
            measured, profile = inherited.measure(document(), "FULL_INTERVAL", stage1d_empty_target_recovery=True)
        self.assertEqual(runner.call_count, 6)
        self.assertTrue(all(c.kwargs == {"stage1d_empty_target_recovery": True} for c in runner.call_args_list))
        self.assertEqual(measured["status"], "COMPLETED")

    def test_freeze_and_saved_gate_bind_solver_policy(self):
        temporary_root = BASE / ".test_tmp"
        temporary_root.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=temporary_root) as temporary:
            tree = Path(temporary)
            path = tree / "batch"
            queries = [{"query_id": "synthetic:policy:" + str(i), "name": "source" + str(i),
                        "seed_event_id": "seed", "seed_amount_raw": "80"} for i in range(4)]
            batch.initialize_batch(tree, path, queries)
            frozen = batch.freeze_batch(tree, path)
            self.assertEqual(frozen["solver_execution_policy"], batch.SOLVER_EXECUTION_POLICY)
            batch.verify_freeze(tree, path)
            changed = copy.deepcopy(frozen)
            changed["solver_execution_policy"]["retry_presolve"] = True
            batch.write(path / "EXECUTION_FREEZE.json", changed)
            with self.assertRaisesRegex(ValueError, "solver execution policy changed"):
                batch.verify_freeze(tree, path)
            self.assertFalse(batch.validate_saved_batch(tree, path, tree / "missing_results")["passed"])

    def test_fresh_offline_package_uses_frozen_explicit_recovery(self):
        from stage1d_validation import validate_package
        temporary_root = BASE / ".test_tmp"
        temporary_root.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=temporary_root) as temporary:
            root = Path(temporary)
            tree = root / "tree"
            tree.mkdir()
            shutil.copytree(BASE / "src", tree / "src", ignore=shutil.ignore_patterns("__pycache__"))
            queries = [{"query_id": "synthetic:package-recovery:" + str(i), "name": "source" + str(i),
                        "seed_event_id": "seed", "seed_amount_raw": "100000000000000000"} for i in range(4)]
            path = tree / "batch"
            batch.initialize_batch(tree, path, queries)
            doc = document()
            doc["query_id"] = queries[0]["query_id"]
            doc["transactions"][0]["flows"][0]["amount_raw"] = "100000000000000000"
            outflows = [dict(doc["transactions"][0]["flows"][0], event_id="out" + str(i),
                from_account="A|ETH", to_account=None, amount_raw="1", role="BOUNDARY_OUTFLOW") for i in range(2)]
            doc["transactions"].append({"tx_id": "out-tx", "block_number": 2, "tx_index": 0, "flows": outflows, "fees": []})
            scope = Scope(doc["query_id"], queries[0]["name"], 1, 2, 1, 2, 0, None, "REFERENCE_FULL").freeze_dict()
            batch.register_document(tree, path, doc["query_id"], doc, scope, {}, {"source": "SYNTHETIC_ONLY"},
                                    "COMPLETE_FULL_SCOPE", "NO_OBSERVED_TARGET")
            frozen = batch.freeze_batch(tree, path)
            result = batch.run_batch(tree, path, tree / "results")
            self.assertTrue(result["passed"], result)
            self.assertTrue(batch.validate_saved_batch(tree, path, tree / "results")["passed"])
            receipt = validate_package(tree, "batch", "results", root / "validation", "public")
            self.assertTrue(receipt["passed"], receipt)
            self.assertTrue(receipt["network_disabled"])
            self.assertTrue(receipt["credentials_removed_from_child_environment"])
            self.assertTrue(frozen["solver_execution_policy"]["stage1d_empty_target_recovery"])
            folder = tree / "results" / result["method_results_index"][0]["path"]
            native = batch.read(folder / "RAW_METHOD_RETURNS.json")
            recovered = native["BALANCE_INFORMATION_REMOVED"]["first_return"]["modifications"]["empty_target_feasibility"]
            self.assertEqual(recovered["status"], "OPTIMAL_EXACT_CERTIFIED")
            upper = recovered["endpoints"]["upper"]
            self.assertTrue(upper["certificate"]["certified"])
            if "certificate_recovery" in upper:
                self.assertFalse(upper["certificate_recovery"]["attempts"][0]["certificate"]["certified"])
                self.assertEqual(upper["certificate_recovery"]["selected_attempt"], 2)


if __name__ == "__main__":
    unittest.main()
