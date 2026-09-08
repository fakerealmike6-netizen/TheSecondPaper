"""Input-defined result acceptance; small local controls, no full batch run."""
import copy
from fractions import Fraction as F
import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE / "src"))
from stage1c_baselines import run_baseline
from stage1c_intervals import run_interval
from stage1c_output_contract import METHODS, INTERVAL_METHODS, accept_method_results, expected_domains, exact


def load(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def toy():
    return {"scenario_id": "contract-unit", "query_id": "contract-unit", "index": 9,
            "initial_balances": {"A|ETH": "2", "T|ETH": "0"}, "target_accounts": ["T"],
            "objective_groups": {"T|ETH": ["enter"]},
            "events": [{"id": "seed", "kind": "seed", "order": 1, "to": "A", "asset": "ETH", "amount_raw": "2"},
                       {"id": "enter", "kind": "transfer", "order": 2, "from": "A", "to": "T", "asset": "ETH", "amount_raw": "3"}]}


def identity(doc):
    return {"sample_id": doc.get("scenario_id", doc.get("name")), "query_id": doc["query_id"],
            "input_fact_hash": hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest(),
            "scope_hash": "f" * 64, "label_version": "CONTRACT_UNIT_LABELS", "method_versions": {m: "frozen-unit-v1" for m in METHODS}}


def execute(doc):
    expected = identity(doc)
    results = {}
    for method in METHODS:
        value = run_interval(doc, method) if method in INTERVAL_METHODS else run_baseline(doc, method)
        if value.get("status") == "OK":
            value["status"] = "COMPLETED"
        value.setdefault("positive_addresses", value.get("output_address_ids"))
        value.update({k: expected[k] for k in ("sample_id", "query_id", "input_fact_hash", "scope_hash", "label_version")})
        value.update(method_id=method, method_version=expected["method_versions"][method])
        results[method] = value
    return results


def context_toy():
    flows = [{"event_id": "seed", "role": "SEED", "from_account": "outside|ETH", "to_account": "A|ETH", "amount_raw": "2"},
             {"event_id": "normal", "role": "BACKGROUND_NORMAL", "from_account": "normal|ETH", "to_account": "A|ETH", "amount_raw": "2", "source_zero_basis": "synthetic normal origin"},
             {"event_id": "enter", "role": "CANDIDATE", "from_account": "A|ETH", "to_account": "T|ETH", "amount_raw": "3"}]
    return {"schema_version": "stage1b-r3-context-model-v1", "query_id": "contract-context-unit", "name": "contract-context-unit",
            "accounts": [{"account_id": "A|ETH", "initial_actual_balance_raw": "0", "initial_source_raw": "0",
                          "initial_source_basis": "synthetic causal scope", "initial_position": {"block_number": 0, "phase": "BLOCK_END"}}],
            "transactions": [{"tx_id": "tx" + str(i), "block_number": i + 1, "tx_index": 0, "flows": [flow], "fees": []} for i, flow in enumerate(flows)],
            "objective_groups": {"T|ETH": ["enter"]}, "all_service_entries": ["enter"]}


class Stage1COutputContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.doc = toy()
        cls.results = execute(cls.doc)
        cls.context = context_toy()
        cls.context_results = execute(cls.context)

    def receipt(self, results=..., doc=None):
        doc = self.doc if doc is None else doc
        return accept_method_results(doc, self.results if results is ... else results, expected_identity=identity(doc))

    def rejects(self, results, code, *, doc=None):
        receipt = self.receipt(results, doc)
        self.assertFalse(receipt["passed"])
        self.assertIn(code, {e["code"] for e in receipt["errors"]}, receipt["errors"])
        self.assertTrue(all({"code", "method", "path", "detail"} <= set(error) for error in receipt["errors"]))
        return receipt

    def test_genuine_result_passes_and_receipt_is_deterministic(self):
        a = self.receipt()
        self.assertTrue(a["passed"], a["errors"])
        self.assertEqual(a, self.receipt())
        self.assertEqual(a["counts"]["required_methods"], 7)
        self.assertEqual(a["counts"]["required_interval_events"], 1)
        self.assertEqual(a["counts"]["required_baseline_ports"], 2)

    def test_domains_differ_by_method_and_never_follow_returned_keys(self):
        domain = expected_domains(self.doc)
        self.assertEqual(domain["interval_events"], ["enter"])
        self.assertEqual(domain["baseline_events"], ["enter", "seed"])
        self.assertEqual(domain["allocation_ports"], ["enter", "seed"])
        values = copy.deepcopy(self.results)
        for category in ("addresses", "events", "joint_by_asset"):
            values["FULL_INTERVAL"][category] = {}
        values["FULL_INTERVAL"]["positive_addresses"] = []
        receipt = self.rejects(values, "OUTPUT_DOMAIN")
        self.assertEqual(receipt["expected_domains"], domain)

    def test_missing_haircut_allocation_with_bad_points_fails(self):
        values = copy.deepcopy(self.results)
        values["HAIRCUT"]["allocation_raw"] = None
        values["HAIRCUT"]["addresses"]["T|ETH"]["point_raw"] = "999"
        values["HAIRCUT"]["joint_by_asset"]["ETH"]["point_raw"] = "999"
        self.rejects(values, "POINT_AGGREGATE_MISMATCH")

    def test_legal_allocation_beside_bad_reported_points_fails(self):
        values = copy.deepcopy(self.results)
        values["HAIRCUT"]["addresses"]["T|ETH"]["point_raw"] = "999"
        values["HAIRCUT"]["joint_by_asset"]["ETH"]["point_raw"] = "999"
        receipt = self.rejects(values, "POINT_AGGREGATE_MISMATCH")
        self.assertTrue(receipt["method_checks"]["HAIRCUT"]["full_allocation_audit"]["exact_feasible"])

    def test_haircut_event_point_and_alias_must_equal_allocation(self):
        for field in ("point_raw", "source_amount_raw"):
            with self.subTest(field=field):
                values = copy.deepcopy(self.results)
                values["HAIRCUT"]["events"]["enter"][field] = "1"
                self.rejects(values, "POINT_ALLOCATION_MISMATCH" if field == "point_raw" else "POINT_ALIAS_MISMATCH")

    def test_allocation_must_include_nontarget_seed(self):
        values = copy.deepcopy(self.results)
        del values["HAIRCUT"]["allocation_raw"]["seed"]
        self.rejects(values, "OUTPUT_DOMAIN")

    def test_point_consistency_does_not_replace_global_feasibility(self):
        values = copy.deepcopy(self.results)
        h = values["HAIRCUT"]
        h["allocation_raw"]["seed"] = "1"
        h["events"]["seed"]["point_raw"] = h["events"]["seed"]["source_amount_raw"] = "1"
        self.rejects(values, "ALLOCATION_INFEASIBLE")

    def test_missing_whole_method_and_extra_method_fail(self):
        for missing in (True, False):
            with self.subTest(missing=missing):
                values = copy.deepcopy(self.results)
                if missing:
                    del values["POISON"]
                else:
                    values["UNKNOWN_METHOD"] = {}
                self.rejects(values, "OUTPUT_DOMAIN")

    def test_missing_target_extra_target_and_wrong_asset_fail(self):
        for mutation in ("missing", "extra", "asset"):
            with self.subTest(mutation=mutation):
                values = copy.deepcopy(self.results)
                if mutation == "missing":
                    del values["FULL_INTERVAL"]["events"]["enter"]
                elif mutation == "extra":
                    values["FULL_INTERVAL"]["addresses"]["U|ETH"] = copy.deepcopy(values["FULL_INTERVAL"]["addresses"]["T|ETH"])
                else:
                    values["FULL_INTERVAL"]["addresses"]["T|ETH"]["asset"] = "WETH"
                self.rejects(values, "ASSET_IDENTITY" if mutation == "asset" else "OUTPUT_DOMAIN")

    def test_each_envelope_identity_is_checked_against_manifest(self):
        for field in ("method_id", "sample_id", "query_id", "input_fact_hash", "scope_hash", "label_version", "method_version"):
            with self.subTest(field=field):
                values = copy.deepcopy(self.results)
                values["POISON"][field] = "incorrect"
                self.rejects(values, "METHOD_IDENTITY" if field == "method_id" else "METHOD_VERSION" if field == "method_version" else "RESULT_IDENTITY")

    def test_trusted_identity_is_mandatory(self):
        receipt = accept_method_results(self.doc, self.results)
        self.assertFalse(receipt["passed"])
        self.assertIn("EXPECTED_IDENTITY_MISSING", {e["code"] for e in receipt["errors"]})

    def test_invalid_exact_numbers_fail_without_throwing(self):
        for value in (None, True, False, float("nan"), float("inf"), 1.0, "NaN", "Infinity", "1/0", "-1", [], {}):
            with self.subTest(value=value):
                values = copy.deepcopy(self.results)
                values["FULL_INTERVAL"]["addresses"]["T|ETH"]["lower_raw"] = value
                self.rejects(values, "EXACT_NUMBER")

    def test_legal_equivalent_exact_representations_are_accepted(self):
        values = copy.deepcopy(self.results)
        h = values["HAIRCUT"]
        h["allocation_raw"]["seed"] = 2
        h["events"]["enter"]["point_raw"] = "1.50"
        h["addresses"]["T|ETH"]["point_raw"] = "3/2"
        h["joint_by_asset"]["ETH"]["point_raw"] = "1.5e0"
        h["boundary_completion"]["B0_out_raw"] = 0
        h["boundary_completion"]["S0_out_raw"] = "0.0"
        self.assertTrue(self.receipt(values)["passed"], self.receipt(values)["errors"])
        self.assertEqual(exact("3/2"), F(3, 2))

    def test_invalid_business_status_and_type_do_not_masquerade_as_success(self):
        for field, value, code in (("status", "OK", "METHOD_STATUS"), ("status", "UNRESOLVED", "METHOD_STATUS"),
                                  ("output_kind", "feasible_interval", "OUTPUT_KIND")):
            values = copy.deepcopy(self.results); values["POISON"][field] = value
            self.rejects(values, code)

    def test_all_shapes_are_diagnostic_not_exceptions(self):
        for bad in (None, [], "COMPLETED", 0):
            with self.subTest(bad=bad):
                self.rejects(bad, "FIELD_TYPE")
        values = copy.deepcopy(self.results); values["FULL_INTERVAL"] = ["invalid"]
        self.rejects(values, "METHOD_MISSING_OR_TYPE")

    def test_duplicate_and_wrong_positive_output_members_fail(self):
        for bad in (["T|ETH", "T|ETH"], [], ["U|ETH"], None):
            values = copy.deepcopy(self.results); values["BOUNDED_REACHABILITY"]["positive_addresses"] = bad
            self.rejects(values, "FIELD_TYPE" if bad is None else "OUTPUT_MEMBERS")

    def test_wrong_membership_and_baseline_port_asset_fail(self):
        values = copy.deepcopy(self.results)
        values["HAIRCUT"]["addresses"]["T|ETH"]["events"] = ["seed"]
        self.rejects(values, "OUTPUT_MEMBERS")
        values = copy.deepcopy(self.results); values["POISON"]["events"]["seed"]["asset"] = "WETH"
        self.rejects(values, "PORT_IDENTITY")

    def test_false_na_in_supported_graph_fails(self):
        values = copy.deepcopy(self.results); values["HAIRCUT"]["status"] = "NOT_APPLICABLE"
        self.rejects(values, "UNJUSTIFIED_NOT_APPLICABLE")

    def test_six_frozen_haircut_unknown_balance_cases_are_valid_na(self):
        # The validator's unit mirror supplies controlled_v1, not saved results
        # or root experiment manifests. Generate genuine method returns from
        # the six frozen observations and use the same trusted fixture envelope
        # as the other contract tests; no hidden or stored answer is consumed.
        manifest = load(BASE / "controlled_v1/MANIFEST.json")
        rows = [r for r in manifest["samples"] if r["family"] == "missing_information_and_boundary_controls" and r["index"] >= 4]
        self.assertEqual(len(rows), 6)
        self.assertEqual({r["index"] for r in rows}, set(range(4, 10)))
        for row in rows:
            with self.subTest(sample=row["sample_id"]):
                observed = BASE / row["observed_path"]
                self.assertEqual(hashlib.sha256(observed.read_bytes()).hexdigest(), row["observed_sha256"])
                doc = load(observed)
                self.assertEqual(doc["scenario_id"], row["sample_id"])
                results = execute(doc)
                receipt = accept_method_results(doc, results, expected_identity=identity(doc))
                self.assertTrue(receipt["passed"], receipt["errors"])
                self.assertEqual(results["HAIRCUT"]["status"], "NOT_APPLICABLE")
                self.assertIsNotNone(receipt["expected_domains"]["haircut_not_applicable_basis"])
                for field in ("addresses", "events", "joint_by_asset", "allocation_raw", "positive_addresses", "output_address_ids", "output_event_ids"):
                    self.assertIsNone(results["HAIRCUT"][field])

    def test_unrelated_unknown_balance_does_not_justify_abstention(self):
        doc = toy(); doc["initial_balances"]["UNUSED|ETH"] = None
        self.assertIsNone(expected_domains(doc)["haircut_not_applicable_basis"])

    def test_balance_nesting_is_hard_failure_at_joint_and_address(self):
        for category, key in (("addresses", "T|ETH"), ("joint_by_asset", "ETH")):
            values = copy.deepcopy(self.results)
            values["BALANCE_INFORMATION_REMOVED"][category][key]["lower_raw"] = "2"
            receipt = self.rejects(values, "BALANCE_ENDPOINT_NESTING")
            self.assertTrue(any(not r["passed"] and r["code"] == "BALANCE_ENDPOINT_NESTING" for r in receipt["hard_invariants"]))

    def test_structural_nesting_flag_is_not_optional(self):
        values = copy.deepcopy(self.results)
        values["BALANCE_INFORMATION_REMOVED"]["modifications"]["nesting"]["nested"] = False
        self.rejects(values, "BALANCE_STRUCTURAL_NESTING")

    def test_copy_joint_exact_sum_is_required_without_one_seed_cap(self):
        doc = toy(); doc["events"][1]["amount_raw"] = "2"
        doc["events"].append({"id": "enter_u", "kind": "transfer", "order": 3, "from": "A", "to": "U", "asset": "ETH", "amount_raw": "2"})
        doc["initial_balances"]["U|ETH"] = "0"; doc["target_accounts"].append("U"); doc["objective_groups"]["U|ETH"] = ["enter_u"]
        values = execute(doc)
        self.assertEqual(values["NO_CROSS_TARGET_COUPLING"]["joint_by_asset"]["ETH"]["upper_raw"], "4")
        self.assertTrue(self.receipt(values, doc)["passed"], self.receipt(values, doc)["errors"])
        values["NO_CROSS_TARGET_COUPLING"]["joint_by_asset"]["ETH"]["upper_raw"] = "3"
        self.rejects(values, "TARGET_COPY_EXACT_DECOMPOSITION", doc=doc)

    def test_copy_internal_full_constraints_and_namespaces_required(self):
        values = copy.deepcopy(self.results)
        values["NO_CROSS_TARGET_COUPLING"]["modifications"]["product_copies"][0]["cross_copy_identity_equalities_present"] = True
        self.rejects(values, "TARGET_COPY_STRUCTURE")

    def test_protocol_absence_means_all_output_intervals_are_equal(self):
        values = copy.deepcopy(self.results)
        values["NO_PROTOCOL_CONTINUATION"]["events"]["enter"]["lower_raw"] = "0"
        self.rejects(values, "NO_PROTOCOL_FEATURE_EQUALITY")

    def test_protocol_boundary_metadata_is_checked_against_input(self):
        values = copy.deepcopy(self.results)
        values["NO_PROTOCOL_CONTINUATION"]["modifications"]["connected_protocol_count"] = 1
        self.rejects(values, "PROTOCOL_BOUNDARY_STRUCTURE")

    def test_poison_above_full_upper_and_haircut_not_equal_hidden_are_legal(self):
        self.assertGreater(F(self.results["POISON"]["joint_by_asset"]["ETH"]["nominal_raw"]), F(self.results["FULL_INTERVAL"]["joint_by_asset"]["ETH"]["upper_raw"]))
        self.assertEqual(self.results["HAIRCUT"]["joint_by_asset"]["ETH"]["point_raw"], "3/2")
        self.assertTrue(self.receipt()["passed"])

    def test_reachability_cannot_report_amount_and_poison_cannot_report_point(self):
        for method, field in (("BOUNDED_REACHABILITY", "source_amount_raw"), ("POISON", "point_raw")):
            values = copy.deepcopy(self.results); values[method]["events"]["enter"][field] = "3"
            self.rejects(values, "AMOUNT_KIND")

    def test_marking_flags_and_witness_cannot_invent_an_output_set(self):
        values = copy.deepcopy(self.results); values["POISON"]["events"]["enter"]["supported"] = False
        self.rejects(values, "OUTPUT_SUPPORT")
        values = copy.deepcopy(self.results); values["BOUNDED_REACHABILITY"]["events"]["enter"]["witness"] = ["enter", "enter"]
        self.rejects(values, "MARKING_WITNESS")

    def test_legal_empty_target_preserves_method_specific_union_rules(self):
        doc = toy(); doc["target_accounts"] = []; doc["objective_groups"] = {}
        values = execute(doc)
        self.assertEqual(values["HAIRCUT"]["joint_by_asset"], {})
        self.assertEqual(values["FULL_INTERVAL"]["joint_by_asset"]["ETH"]["upper_raw"], "0")
        self.assertTrue(self.receipt(values, doc)["passed"], self.receipt(values, doc)["errors"])
        values["FULL_INTERVAL"]["joint_by_asset"] = {}
        self.rejects(values, "OUTPUT_DOMAIN", doc=doc)

    def test_zero_hop_and_explicit_zero_upper_are_accepted(self):
        for mode in ("zero_hop", "zero_upper"):
            doc = toy()
            if mode == "zero_hop":
                doc["initial_balances"] = {"T|ETH": "0"}
                doc["events"] = [dict(doc["events"][0], to="T")]
                doc["objective_groups"] = {"T|ETH": ["seed"]}
            else:
                doc["events"][1]["amount_raw"] = "0"
            values = execute(doc)
            self.assertTrue(self.receipt(values, doc)["passed"], self.receipt(values, doc)["errors"])

    def test_context_format_shares_contract_without_any_amount_truth(self):
        receipt = self.receipt(self.context_results, self.context)
        self.assertTrue(receipt["passed"], receipt["errors"])
        values = copy.deepcopy(self.context_results)
        values["HAIRCUT"]["joint_by_asset"]["ETH"]["point_raw"] = "999"
        self.rejects(values, "POINT_AGGREGATE_MISMATCH", doc=self.context)
        values = copy.deepcopy(self.context_results); values["FULL_INTERVAL"]["addresses"] = {}
        self.rejects(values, "OUTPUT_DOMAIN", doc=self.context)

    def test_canonical_weth_ports_and_boundary_ablation_acceptance(self):
        doc = load(BASE / "controlled_v1/canonical_weth_1to1/01/observed.json")
        values = execute(doc)
        domain = expected_domains(doc)
        self.assertTrue(any(name.endswith(":refund") for name in domain["allocation_ports"]))
        self.assertTrue(any(name.endswith(":output0") for name in domain["allocation_ports"]))
        self.assertTrue(self.receipt(values, doc)["passed"], self.receipt(values, doc)["errors"])
        port = next(name for name in domain["allocation_ports"] if name.endswith(":refund"))
        del values["HAIRCUT"]["allocation_raw"][port]
        self.rejects(values, "OUTPUT_DOMAIN", doc=doc)

    def test_no_solver_or_oracle_is_used_for_acceptance(self):
        with patch("stage1c_intervals.solve_interval", side_effect=AssertionError("Cannot solve a new answer during acceptance")), \
             patch("stage1c_intervals.solve_context_interval", side_effect=AssertionError("Cannot solve a new context answer")), \
             patch("stage1c_oracle.oracle_intervals", side_effect=AssertionError("No Oracle in common contract")):
            self.assertTrue(self.receipt()["passed"])

    def test_malformed_input_domain_is_structured_failure(self):
        doc = toy(); doc["objective_groups"] = {}
        receipt = self.receipt(self.results, doc)
        self.assertFalse(receipt["passed"])
        self.assertIn("INPUT_DOMAIN_INVALID", {e["code"] for e in receipt["errors"]})

    def test_receipt_does_not_mutate_observations_or_returned_results(self):
        before_doc, before_results = copy.deepcopy(self.doc), copy.deepcopy(self.results)
        self.receipt()
        self.assertEqual(before_doc, self.doc)
        self.assertEqual(before_results, self.results)


if __name__ == "__main__":
    unittest.main()
