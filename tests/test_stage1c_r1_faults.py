"""Output-boundary faults; immutable facts and scientific methods are unchanged.

The same mutation functions are used by direct acceptance tests and subprocess
runner-main probes. ``--fault-child`` invokes actual runner.main, preserving its
real freeze verifier and using its explicit frozen-query selection interface.
"""
from __future__ import annotations
import argparse
from contextlib import redirect_stdout, redirect_stderr
import copy
import io
import json
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE / "src"))

FOUR = ("full_missing_output", "haircut_missing_witness_bad_point", "haircut_bad_point_with_valid_witness", "balance_ablation_non_nested")
ADDITIONAL = ("partial_missing_target", "extra_target", "wrong_asset_target", "missing_whole_method", "wrong_input_identity",
              "wrong_sample_identity", "wrong_method_identity", "wrong_scope_identity", "wrong_label_identity",
              "missing_nontarget_allocation", "event_point_inconsistent", "invalid_nan", "invalid_bool", "invalid_null",
              "invalid_fraction", "invalid_status", "invalid_na_on_supported_input", "reversed_interval", "duplicate_group_member",
              "positive_set_inconsistent", "wrong_event_asset", "product_union_wrong_sum", "protocol_absent_changed", "balance_address_non_nested", "real_full_missing_output", "real_haircut_bad_point_with_valid_witness",
              "real_wrong_input_identity", "mixed_bad_then_good")
POSITIVE = ("normal_positive_control", "explicit_zero_control", "zero_hop_control", "six_haircut_na_control",
            "poison_and_product_scope_control", "real_no_protocol_control")
ALL_CASES = POSITIVE + FOUR + ADDITIONAL


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def selected_samples(case):
    if case.startswith("real_"):
        return ["atomic_simple_transfer"]
    if case == "mixed_bad_then_good":
        return ["controlled-v1/normal_mixing/00", "controlled-v1/normal_mixing/01"]
    if case == "six_haircut_na_control":
        return [f"controlled-v1/missing_information_and_boundary_controls/{i:02d}" for i in range(4, 10)]
    if case == "zero_hop_control":
        return ["controlled-v1/missing_information_and_boundary_controls/00"]
    if case == "explicit_zero_control":
        return ["controlled-v1/missing_information_and_boundary_controls/02"]
    if case in {"partial_missing_target", "extra_target", "wrong_asset_target", "poison_and_product_scope_control"}:
        return ["controlled-v1/split_merge_shared_targets/00"]
    return ["controlled-v1/normal_mixing/00"]


def mutate_return(case, doc, method, returned):
    value = copy.deepcopy(returned)
    if case in POSITIVE:
        return value
    if case.startswith("real_"):
        case = case.removeprefix("real_")
    if case == "mixed_bad_then_good":
        if doc.get("scenario_id") != "controlled-v1/normal_mixing/00":
            return value
        case = "full_missing_output"
    if case == "full_missing_output" and method == "FULL_INTERVAL":
        value.update(addresses={}, events={}, joint_by_asset={}, positive_addresses=[])
    elif case in {"haircut_missing_witness_bad_point", "haircut_bad_point_with_valid_witness"} and method == "HAIRCUT":
        if case == "haircut_missing_witness_bad_point":
            value["allocation_raw"] = None
        for section in ("addresses", "joint_by_asset"):
            for row in value[section].values():
                row["point_raw"] = row["source_amount_raw"] = "999"
    elif case == "balance_ablation_non_nested" and method == "BALANCE_INFORMATION_REMOVED":
        for row in value["joint_by_asset"].values():
            row["lower_raw"] = row["upper_raw"] = "2"
    elif case == "partial_missing_target" and method == "FULL_INTERVAL":
        value["addresses"].pop(sorted(value["addresses"])[0])
    elif case == "extra_target" and method == "FULL_INTERVAL":
        value["addresses"]["UNOBSERVED|ETH"] = copy.deepcopy(next(iter(value["addresses"].values())))
    elif case == "wrong_asset_target" and method == "FULL_INTERVAL":
        name = sorted(value["addresses"])[0]
        row = value["addresses"].pop(name); row["asset"] = "TOK"
        value["addresses"][name.rsplit("|", 1)[0] + "|TOK"] = row
    elif case == "wrong_input_identity" and method == "HAIRCUT":
        value["input_fact_hash"] = "0" * 64
    elif case == "wrong_sample_identity" and method == "HAIRCUT":
        value["query_or_sample_id"] = "WRONG_SAMPLE"
        value["sample_id"] = "WRONG_SAMPLE"
    elif case == "wrong_method_identity" and method == "HAIRCUT":
        value["method_id"] = "FULL_INTERVAL"
    elif case == "wrong_scope_identity" and method == "FULL_INTERVAL":
        value["scope_hash"] = "0" * 64
    elif case == "wrong_label_identity" and method == "FULL_INTERVAL":
        value["label_version"] = "WRONG_LABEL_VERSION"
    elif case == "missing_nontarget_allocation" and method == "HAIRCUT":
        service = {eid for row in value["addresses"].values() for eid in row["events"]}
        name = next(eid for eid in value["allocation_raw"] if eid not in service)
        value["allocation_raw"].pop(name)
    elif case == "event_point_inconsistent" and method == "HAIRCUT":
        row = next(iter(value["events"].values()))
        row["point_raw"] = row["source_amount_raw"] = "999"
    elif case in {"invalid_nan", "invalid_bool", "invalid_null", "invalid_fraction"} and method == "FULL_INTERVAL":
        next(iter(value["addresses"].values()))["upper_raw"] = {"invalid_nan": "NaN", "invalid_bool": True, "invalid_null": None, "invalid_fraction": "1/0"}[case]
    elif case == "invalid_status" and method == "FULL_INTERVAL":
        value["status"] = "LOOKS_SUCCESSFUL"
    elif case == "invalid_na_on_supported_input" and method == "HAIRCUT":
        value.update(status="NOT_APPLICABLE", applicability="NOT_APPLICABLE", failure_reason="UNSUPPORTED_BECAUSE_RESULT_UNAVAILABLE",
                     addresses=None, events=None, joint_by_asset=None, allocation_raw=None, output_address_ids=None,
                     output_event_ids=None, positive_addresses=None, boundary_completion=None, construction_audit=None)
    elif case == "reversed_interval" and method == "FULL_INTERVAL":
        next(iter(value["addresses"].values())).update(lower_raw="3", upper_raw="2")
    elif case == "duplicate_group_member" and method == "HAIRCUT":
        row = next(iter(value["addresses"].values())); row["events"].append(row["events"][0])
    elif case == "positive_set_inconsistent" and method == "FULL_INTERVAL":
        value["positive_addresses"] = []
    elif case == "wrong_event_asset" and method == "HAIRCUT":
        next(iter(value["events"].values()))["asset"] = "TOK"
    elif case == "product_union_wrong_sum" and method == "NO_CROSS_TARGET_COUPLING":
        next(iter(value["joint_by_asset"].values())).update(lower_raw="0", upper_raw="0")
    elif case == "protocol_absent_changed" and method == "NO_PROTOCOL_CONTINUATION":
        next(iter(value["joint_by_asset"].values())).update(lower_raw="0", upper_raw="0")
    elif case == "balance_address_non_nested" and method == "BALANCE_INFORMATION_REMOVED":
        next(iter(value["addresses"].values())).update(lower_raw="2", upper_raw="2")
    return value


def child_main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--fault-child", action="store_true")
    parser.add_argument("--tree", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", choices=ALL_CASES, required=True)
    parser.add_argument("--kind", choices=("public", "min"), default="min")
    parser.add_argument("--batch-selection", choices=("controlled",), help="Full extracted-package fault run; mutate only the case's first selected query, retain the rest")
    args = parser.parse_args(argv)
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(args.tree / "src"))
    import run_stage1c as runner
    desired = selected_samples(args.case)
    if args.output.exists():
        raise ValueError("Fault output must be fresh")
    original_dispatch, original_verify = runner.dispatch, runner.verify_freeze
    original_methods = runner.METHODS
    verified = []

    def deny(*a, **kw):
        raise RuntimeError("STAGE1C_R1_FAULT_PROBE_NETWORK_DISABLED")

    def checked_selection(tree, kind):
        frozen, rows = original_verify(tree, kind)
        chosen = [row for sid in desired for row in rows if row["sample_id"] == sid]
        if len(chosen) != len(desired):
            raise ValueError("Frozen requested fault-test selection is incomplete")
        verified.append({"checked_full_input_count": len(rows), "selected": desired, "frozen_version": frozen["version"]})
        return frozen, rows

    def faulty_dispatch(doc, method):
        if args.batch_selection and doc.get("scenario_id", doc.get("name")) != desired[0]:
            return original_dispatch(doc, method)
        return mutate_return(args.case, doc, method, original_dispatch(doc, method))

    if args.case == "missing_whole_method":
        runner.METHODS = tuple(m for m in original_methods if m != "POISON")
    log, err = io.StringIO(), io.StringIO()
    selection = args.batch_selection or (desired[0] if len(desired) == 1 else "ids:" + ",".join(desired))
    command = ["run_stage1c.py", "--tree", str(args.tree), "--kind", args.kind, "--selection", selection, "--output", str(args.output)]
    with patch.object(runner, "dispatch", side_effect=faulty_dispatch), patch.object(runner, "verify_freeze", side_effect=checked_selection), \
         patch.object(sys, "argv", command), patch.object(socket, "create_connection", side_effect=deny), \
         patch.object(socket.socket, "connect", side_effect=deny), patch.object(socket, "getaddrinfo", side_effect=deny), \
         redirect_stdout(log), redirect_stderr(err):
        status = runner.main()
    runner.METHODS = original_methods
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "probe_stdout.log").write_text(log.getvalue(), encoding="utf-8")
    (args.output / "probe_stderr.log").write_text(err.getvalue(), encoding="utf-8")
    idx = read(args.output / "RESULTS_INDEX.json") if (args.output / "RESULTS_INDEX.json").exists() else {}
    queries = []
    for row in idx.get("method_results_index", []):
        folder = args.output / row["path"]
        evaluation = read(folder / "EVALUATION.json") if (folder / "EVALUATION.json").exists() else {}
        receipt_files = [p.name for p in folder.iterdir() if p.is_file() and ("ACCEPT" in p.name or "CONTRACT" in p.name or "VALID" in p.name)]
        queries.append({"sample_id": row["sample_id"], "evaluation_passed": evaluation.get("passed"),
                        "evaluation_errors": evaluation.get("errors"), "raw_method_output_saved": (folder / "METHOD_RESULTS.json").exists(),
                        "acceptance_receipt_files": sorted(receipt_files), "method_statuses": row.get("method_statuses")})
    receipt = {"case": args.case, "expected_cli_exit_code": 0 if args.case in POSITIVE else 1,
               "cli_exit_code": status, "batch_passed": idx.get("passed"), "batch_status": idx.get("status"),
               "sample_count": idx.get("sample_count"), "queries": queries, "freeze_verifier_calls": verified,
               "source_or_inputs_modified_by_probe": False, "fault_injection_boundary": "METHOD_DISPATCH_RETURN_OR_MANDATORY_METHOD_DISPATCH_SET",
               "selection_note": "Real original verifier executes all frozen source/input checks and returns the full row list unchanged; production CLI selects explicit frozen query IDs."}
    write(args.output / "FAULT_CHILD_RECEIPT.json", receipt)
    print(json.dumps(receipt, ensure_ascii=False))
    return status


class Stage1CR1FaultContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import run_stage1c as runner
        from stage1c_output_contract import accept_method_results
        cls.runner = runner
        cls.accept = staticmethod(accept_method_results)
        cls.cache = {}

    def prepared(self, sid):
        if sid in self.cache:
            return copy.deepcopy(self.cache[sid])
        if sid.startswith("controlled-v1/"):
            doc = read(BASE / "controlled_v1" / sid.removeprefix("controlled-v1/") / "observed.json")
            result = {method: self.runner.normalized(self.runner.dispatch(copy.deepcopy(doc), method)) for method in self.runner.METHODS}
        else:
            path = BASE / "derived/context_pipeline" / sid / "model_input.json"
            if not path.exists():
                # Public package still exercises the real-context schema with
                # a declared synthetic account/flow fixture. Full MIN fault
                # subprocesses separately use the actual immutable real query.
                from test_context_lp_r3 import example
                doc = example(); doc["query_id"] = "SYNTHETIC_R1_CONTEXT_FORMAT_CONTROL"
                result = {method: self.runner.normalized(self.runner.dispatch(copy.deepcopy(doc), method)) for method in self.runner.METHODS}
            else:
                doc = read(path)
                # Saved real values are accepted only after full contract checking;
                # no expensive new method batch is hidden inside unit discovery.
                result = read(BASE / "results/real/samples" / sid / "METHOD_RESULTS.json")
        identity = {"sample_id": sid, "query_id": doc.get("query_id", sid), "input_fact_hash": self.runner.digest(self.runner.canonical(doc)),
                    "scope_hash": "UNIT_FROZEN_SCOPE", "label_version": "UNIT_FROZEN_LABELS",
                    "method_versions": {m: "UNIT_METHOD_VERSION" for m in self.runner.METHODS}}
        for method, value in result.items():
            value.update(method_id=method, method_version="UNIT_METHOD_VERSION", sample_id=sid, query_id=identity["query_id"],
                         input_fact_hash=identity["input_fact_hash"], scope_hash=identity["scope_hash"], label_version=identity["label_version"])
        self.cache[sid] = (doc, result, identity)
        return copy.deepcopy(self.cache[sid])

    def check_fault(self, case):
        sid = selected_samples(case)[0]
        doc, good, identity = self.prepared(sid)
        positive = self.accept(doc, copy.deepcopy(good), expected_identity=identity)
        self.assertTrue(positive["passed"], positive.get("errors"))
        bad = {m: mutate_return(case, doc, m, v) for m, v in good.items()}
        if case == "missing_whole_method":
            bad.pop("POISON")
        receipt = self.accept(doc, bad, expected_identity=identity)
        self.assertFalse(receipt["passed"], case)
        self.assertTrue(receipt["errors"], case)
        self.assertTrue(all(isinstance(e, dict) and e.get("code") for e in receipt["errors"]))

    def test_normal_nonzero_control(self):
        doc, results, identity = self.prepared(selected_samples("normal_positive_control")[0])
        self.assertTrue(self.accept(doc, results, expected_identity=identity)["passed"])

    def test_legal_empty_target_and_explicit_zero_rules(self):
        doc, _, identity = self.prepared(selected_samples("normal_positive_control")[0])
        doc["target_accounts"] = []; doc["objective_groups"] = {}
        results = {m: self.runner.normalized(self.runner.dispatch(copy.deepcopy(doc), m)) for m in self.runner.METHODS}
        identity["input_fact_hash"] = self.runner.digest(self.runner.canonical(doc))
        for method, value in results.items():
            value.update(method_id=method, method_version=identity["method_versions"][method],
                         **{k: v for k, v in identity.items() if k != "method_versions"})
        receipt = self.accept(doc, results, expected_identity=identity)
        self.assertTrue(receipt["passed"], receipt.get("errors"))
        self.assertEqual(results["FULL_INTERVAL"]["joint_by_asset"]["ETH"]["lower_raw"], "0")
        self.assertEqual(results["BOUNDED_REACHABILITY"]["joint_by_asset"], {})

    def test_six_prespecified_haircut_not_applicable_remain_valid(self):
        for sid in selected_samples("six_haircut_na_control"):
            with self.subTest(sample=sid):
                doc, results, identity = self.prepared(sid)
                receipt = self.accept(doc, results, expected_identity=identity)
                self.assertTrue(receipt["passed"], receipt.get("errors"))
                self.assertEqual(results["HAIRCUT"]["status"], "NOT_APPLICABLE")

    def test_scope_controls_poison_and_target_product_not_source_capped(self):
        from fractions import Fraction
        doc, results, identity = self.prepared(selected_samples("poison_and_product_scope_control")[0])
        self.assertTrue(self.accept(doc, results, expected_identity=identity)["passed"])
        full = Fraction(results["FULL_INTERVAL"]["joint_by_asset"]["ETH"]["upper_raw"])
        self.assertGreater(Fraction(results["POISON"]["joint_by_asset"]["ETH"]["nominal_raw"]), full)
        self.assertGreater(Fraction(results["NO_CROSS_TARGET_COUPLING"]["joint_by_asset"]["ETH"]["upper_raw"]), full)

    def test_zero_hop_and_explicit_zero_normal_controls(self):
        for case in ("zero_hop_control", "explicit_zero_control"):
            doc, results, identity = self.prepared(selected_samples(case)[0])
            self.assertTrue(self.accept(doc, results, expected_identity=identity)["passed"])

    def test_feasible_haircut_can_differ_from_hidden_assignment(self):
        from fractions import Fraction
        sid = selected_samples("normal_positive_control")[0]
        doc, results, identity = self.prepared(sid)
        hidden = read(BASE / "controlled_v1" / sid.removeprefix("controlled-v1/") / "hidden.json")
        self.assertTrue(self.accept(doc, results, expected_identity=identity)["passed"])
        group, amount = next(iter(results["HAIRCUT"]["addresses"].items()))
        actual_hidden = sum(Fraction(hidden["event_source_amounts_raw"][e]) for e in doc["objective_groups"][group])
        self.assertNotEqual(Fraction(amount["point_raw"]), actual_hidden)


def _fault_test(case):
    def test(self):
        self.check_fault(case)
    return test


for _case in FOUR + tuple(c for c in ADDITIONAL if c != "mixed_bad_then_good"):
    setattr(Stage1CR1FaultContractTests, "test_reject_" + _case, _fault_test(_case))


if __name__ == "__main__":
    if "--fault-child" in sys.argv:
        raise SystemExit(child_main())
    unittest.main()
