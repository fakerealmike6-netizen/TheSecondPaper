"""Local runner integration and metric contracts; never execute the 62-row batch."""
import copy
from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from fractions import Fraction as F
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE / "src"))
import run_stage1c as runner
import stage1c_intervals as intervals


def observed():
    return {"schema_version": "stage1c-controlled-observed-1.0", "scenario_id": "runner-unit",
            "query_id": "runner-unit", "index": 9, "family": "normal_mixing",
            "label_version": "UNIT_TARGETS_V1", "source_layer": "SYNTHETIC",
            "initial_balances": {"A|ETH": "2", "T|ETH": "0"},
            "target_accounts": ["T"], "objective_groups": {"T|ETH": ["enter"]},
            "events": [{"id": "seed", "kind": "seed", "order": 1, "to": "A", "asset": "ETH", "amount_raw": "2"},
                       {"id": "enter", "kind": "transfer", "order": 2, "from": "A", "to": "T", "asset": "ETH", "amount_raw": "2"}]}


def hidden(enter="0"):
    return {"event_source_amounts_raw": {"seed": "2", "enter": enter}}


def all_methods(doc):
    return {method: runner.normalized(runner.dispatch(doc, method)) for method in runner.METHODS}


class Stage1CRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = observed()
        cls.results = all_methods(cls.document)

    def test_dispatch_accepts_only_observations_not_hidden_or_oracle(self):
        with patch("builtins.open", side_effect=AssertionError("Method dispatch must not open hidden/reference files")), \
             patch("stage1c_oracle.oracle_intervals", side_effect=AssertionError("Oracle cannot generate a method answer")):
            a = runner.semantic_result(runner.dispatch(observed(), "FULL_INTERVAL"))
            b = runner.semantic_result(runner.dispatch(observed(), "HAIRCUT"))
        self.assertEqual(a["addresses"]["T|ETH"]["upper_raw"], "2")
        self.assertEqual(b["addresses"]["T|ETH"]["point_raw"], "1")

    def test_mutating_hidden_changes_metrics_not_method_outputs(self):
        original = copy.deepcopy(self.results)
        low = runner.evaluate_controlled(copy.deepcopy(self.document), hidden("0"), self.results)
        high = runner.evaluate_controlled(copy.deepcopy(self.document), hidden("2"), self.results)
        self.assertTrue(low["passed"])
        self.assertTrue(high["passed"])
        self.assertEqual(low["address_metrics"], high["address_metrics"])
        self.assertNotEqual(low["comparisons"], high["comparisons"])
        self.assertEqual(self.results, original)
        for method in runner.METHODS:
            rerun = runner.normalized(runner.dispatch(copy.deepcopy(self.document), method))
            self.assertEqual(runner.semantic_result(rerun), runner.semantic_result(original[method]))

    def test_controlled_positive_denominator_is_oracle_upper_not_hidden(self):
        evaluation = runner.evaluate_controlled(self.document, hidden("0"), self.results)
        self.assertEqual(evaluation["oracle"]["addresses"]["T|ETH"]["lower_raw"], "0")
        for method in ("FULL_INTERVAL", "BOUNDED_REACHABILITY", "POISON", "HAIRCUT"):
            metric = evaluation["address_metrics"][method]
            self.assertEqual(metric["oracle_positive_count"], 1)
            self.assertEqual(metric["true_positive_count"], 1)
            self.assertEqual(metric["false_positive_count"], 0)
            self.assertEqual(metric["recall"], "1")

    def test_na_is_null_through_normalization_and_metrics(self):
        doc = observed(); doc["initial_balances"]["A|ETH"] = None
        results = all_methods(doc)
        value = results["HAIRCUT"]
        self.assertEqual(value["status"], "NOT_APPLICABLE")
        for field in ("allocation_raw", "addresses", "events", "joint_by_asset", "positive_addresses"):
            self.assertIsNone(value[field])
        evaluation = runner.evaluate_controlled(doc, hidden("0"), results)
        self.assertTrue(evaluation["passed"])
        self.assertIsNone(evaluation["address_metrics"]["HAIRCUT"]["recall"])
        self.assertIsNone(evaluation["haircut_full_assignment_audit"])

    def test_invalid_hidden_allocation_fails_evaluation_without_changing_methods(self):
        bad = hidden("3")
        original = copy.deepcopy(self.results)
        evaluation = runner.evaluate_controlled(self.document, bad, self.results)
        self.assertFalse(evaluation["passed"])
        self.assertFalse(evaluation["hidden_exact_feasibility"]["exact_feasible"])
        self.assertEqual(self.results, original)

    def test_empty_service_domain_has_feasible_zero_not_missing_oracle(self):
        doc = observed(); doc["target_accounts"] = []; doc["objective_groups"] = {}
        results = all_methods(doc)
        evaluation = runner.evaluate_controlled(doc, hidden("0"), results)
        self.assertTrue(evaluation["passed"], evaluation["errors"])
        self.assertEqual(evaluation["oracle"]["joint_by_asset"]["ETH"]["upper_raw"], "0")
        self.assertEqual(evaluation["address_metrics"]["FULL_INTERVAL"]["oracle_positive_count"], 0)
        self.assertIsNone(evaluation["address_metrics"]["FULL_INTERVAL"]["recall"])

    def test_measure_runs_one_warmup_and_five_timed_calls(self):
        with patch.object(runner, "dispatch", return_value=copy.deepcopy(self.results["HAIRCUT"])) as dispatch:
            value, profile = runner.measure(self.document, "HAIRCUT")
        self.assertEqual(dispatch.call_count, 6)
        self.assertEqual(sum(row["warmup"] for row in profile["repetitions"]), 1)
        self.assertEqual([row["repetition"] for row in profile["repetitions"]], list(range(6)))
        self.assertTrue(profile["deterministic"])
        self.assertTrue(profile["different_output_workloads_not_speed_equivalent"])
        self.assertEqual(value["addresses"]["T|ETH"]["point_raw"], "1")

    def test_each_measured_method_call_gets_a_fresh_observation_copy(self):
        source = observed(); original = copy.deepcopy(source); inputs = []

        def mutate_received_copy(doc, method):
            self.assertEqual(doc, original)
            inputs.append(doc)
            doc["events"][0]["amount_raw"] = "999"
            return copy.deepcopy(self.results["HAIRCUT"])

        with patch.object(runner, "dispatch", side_effect=mutate_received_copy):
            result, profile = runner.measure(source, "HAIRCUT")
        self.assertEqual(source, original)
        self.assertEqual(len({id(doc) for doc in inputs}), 6)
        self.assertTrue(profile["deterministic"])
        self.assertEqual(result["status"], "COMPLETED")

    def test_measure_exception_is_error_never_a_zero_result(self):
        with patch.object(runner, "dispatch", side_effect=ValueError("unit-invalid-ledger")):
            result, profile = runner.measure(self.document, "HAIRCUT")
        self.assertEqual(result["status"], "ERROR")
        self.assertIn("unit-invalid-ledger", result["failure_reason"])
        self.assertIsNone(result["joint_by_asset"])
        self.assertEqual(len(profile["repetitions"]), 6)

    def test_nondeterministic_method_is_failed_with_raw_repetitions(self):
        first = copy.deepcopy(self.results["HAIRCUT"])
        changed = copy.deepcopy(first); changed["addresses"]["T|ETH"]["point_raw"] = "2"
        with patch.object(runner, "dispatch", side_effect=[first, changed, first, first, first, first]):
            result, profile = runner.measure(self.document, "HAIRCUT")
        self.assertEqual(result["status"], "ERROR")
        self.assertFalse(profile["deterministic"])
        self.assertTrue(result["execution_errors"])

    def test_semantic_hash_ignores_timing_but_retains_unknown_status(self):
        result = copy.deepcopy(self.results["FULL_INTERVAL"])
        changed = copy.deepcopy(result); changed["timing_parts"] = {"arbitrary_seconds": 99}
        self.assertEqual(runner.semantic_result(result), runner.semantic_result(changed))
        changed["status"] = "ERROR"
        self.assertNotEqual(runner.semantic_result(result), runner.semantic_result(changed))

    def test_ablation_error_does_not_crash_comparison(self):
        values = copy.deepcopy(self.results)
        values["NO_PROTOCOL_CONTINUATION"] = {"status": "ERROR", "joint_by_asset": None, "failure_reason": "injected unit failure"}
        rows = runner.ablation_comparison(values)
        self.assertIsInstance(rows, list)
        self.assertTrue(rows, "The unresolved asset comparison must be retained")
        self.assertTrue(any(row.get("status") in {"ERROR", "UNRESOLVED", "NOT_COMPARABLE"} for row in rows), rows)

    def test_unknown_method_and_unknown_selection_are_explicit_errors(self):
        with self.assertRaisesRegex(ValueError, "Unknown baseline"):
            runner.dispatch(self.document, "UNKNOWN_METHOD")
        with patch.object(runner, "verify_freeze", return_value=({}, [{"kind": "controlled", "sample_id": "known"}])):
            with self.assertRaisesRegex(ValueError, "Empty or unknown selection"):
                runner.run(BASE, BASE / "not-created-unit-selection", "public", "missing-sample")

    def test_cli_main_returns_nonzero_for_runtime_failure(self):
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["run_stage1c.py", "--output", str(BASE / "not-created-unit-output")]), \
             patch.object(runner, "run", side_effect=RuntimeError("deliberate-unit-failure")), redirect_stderr(stderr):
            status = runner.main()
        self.assertEqual(status, 1)
        self.assertIn("deliberate-unit-failure", stderr.getvalue())

    def test_cli_main_returns_nonzero_for_partial_results(self):
        with patch.object(sys, "argv", ["run_stage1c.py", "--output", str(BASE / "not-created-unit-output")]), \
             patch.object(runner, "run", return_value={"passed": False}):
            self.assertEqual(runner.main(), 1)

    def test_actual_cli_process_without_required_output_exits_nonzero(self):
        result = subprocess.run([sys.executable, str(BASE / "src/run_stage1c.py"), "--kind", "public"],
                                cwd=BASE, capture_output=True, text=True, timeout=20)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--output required", result.stderr)

    def test_na_normalization_has_no_zero_task_or_amount_fabrication(self):
        raw = {"status": "NOT_APPLICABLE", "addresses": None, "events": None, "joint_by_asset": None,
               "output_address_ids": None, "failure_reason": "unknown balance"}
        normalized = runner.normalized(raw)
        self.assertIsNone(normalized["positive_addresses"])
        self.assertNotIn("task_counts", normalized)
        self.assertEqual(raw["status"], "NOT_APPLICABLE")

    def test_protocol_absence_is_an_explicit_identical_interval_control(self):
        full = self.results["FULL_INTERVAL"]
        ablated = self.results["NO_PROTOCOL_CONTINUATION"]
        self.assertEqual(ablated["modifications"]["feature_status"], "FEATURE_ABSENT_SAME_INPUT")
        self.assertEqual(full["joint_by_asset"], ablated["joint_by_asset"])

    def test_target_group_validation_rejects_duplicate_physical_entries(self):
        doc = observed(); doc["objective_groups"]["U|ETH"] = ["enter"]
        with self.assertRaisesRegex(ValueError, "exactly one"):
            intervals.groups(doc)

    def test_frozen_controlled_manifest_counts_and_limits(self):
        # Checks the frozen sample catalog only, without executing any samples.
        manifest = runner.read(BASE / "controlled_v1/MANIFEST.json")
        samples = manifest["samples"]
        self.assertEqual(manifest["sample_count"], 60)
        self.assertEqual(len(samples), 60)
        self.assertEqual(len({row["sample_id"] for row in samples}), 60)
        self.assertEqual(sorted(Counter(row["family"] for row in samples).values()), [10] * 6)
        self.assertEqual(sum(row["tiny_enumeration_required"] for row in samples), 12)
        self.assertGreaterEqual(sum(row["zero_hop"] for row in samples), 2)
        self.assertTrue(all(row["operations"] <= 40 and row["accounts"] <= 16 and row["target_count"] <= 3 for row in samples))

    def test_single_sample_runner_persists_methods_before_hidden_and_counts_seven_methods(self):
        # One tiny graph, real method APIs and evaluator. No 60/62 sample batch.
        scratch = BASE / "test_scratch"
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="runner_unit_", dir=scratch) as temporary:
            tree = Path(temporary); out = tree / "results"
            runner.write(tree / "observed.json", self.document)
            runner.write(tree / "hidden.json", hidden())
            runner.write(tree / "EXPERIMENT_FREEZE.json", {"unit": "local integration"})
            frozen = {"method_versions": {method: "unit-v1" for method in runner.METHODS}}
            row = {"sample_id": "runner-unit", "kind": "controlled", "family": "normal_mixing", "query_id": "runner-unit",
                   "incident_id": "SYNTHETIC", "source_layer": "SYNTHETIC", "zero_hop": False,
                   "observed_path": "observed.json", "hidden_path": "hidden.json", "label_version": "unit-labels",
                   "input_fact_hash": runner.file_hash(tree / "observed.json"), "scope_hash": "unit-scope"}
            real_read = runner.read
            hidden_read_count = []

            def guarded_read(path):
                if Path(path).name == "hidden.json":
                    self.assertTrue((out / "samples/runner-unit/METHOD_RESULTS.json").is_file())
                    hidden_read_count.append(1)
                return real_read(path)

            with patch.object(runner, "verify_freeze", return_value=(frozen, [row])), \
                 patch.object(runner, "read", side_effect=guarded_read), redirect_stdout(io.StringIO()):
                summary = runner.run(tree, out, "public", "all")
            self.assertTrue(summary["passed"])
            self.assertEqual(summary["sample_count"], 1)
            self.assertEqual(summary["controlled_count"], 1)
            self.assertEqual(summary["real_count"], 0)
            self.assertEqual(len(hidden_read_count), 1)
            self.assertEqual(summary["research_platform_requests"], 0)
            self.assertEqual(len(summary["method_results_index"][0]["method_statuses"]), 7)
            import csv
            with (out / "METHOD_SUPPORT_MATRIX.csv").open(encoding="utf-8", newline="") as handle:
                support = list(csv.DictReader(handle))
            self.assertEqual(len(support), 7)
            self.assertEqual({row["method"] for row in support}, set(runner.METHODS))
            before = (tree / "observed.json").read_bytes()
            self.assertEqual(runner.digest(before), row["input_fact_hash"])


if __name__ == "__main__":
    unittest.main()
