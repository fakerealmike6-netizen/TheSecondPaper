"""Meaningful local method semantics checks; no providers or real credentials."""
import copy
from fractions import Fraction as F
import inspect
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE / "src"))
import stage1c_baselines as baselines
from stage1c_baselines import run_baseline, observed_targets


def graph(initial=None, events=None, targets=None):
    return {"scenario_id": "baseline_unit", "initial_balances": initial or {"A|ETH": "10", "T|ETH": "0"},
            "events": events or [{"id": "seed", "kind": "seed", "order": 1, "to": "A", "asset": "ETH", "amount_raw": "10"},
                                 {"id": "enter", "kind": "transfer", "order": 2, "from": "A", "to": "T", "asset": "ETH", "amount_raw": "10"}],
            "target_accounts": ["T"] if targets is None else targets}


def transfer(eid, order, sender, receiver, amount, asset="ETH"):
    return {"id": eid, "kind": "transfer", "order": order, "from": sender, "to": receiver, "asset": asset, "amount_raw": str(amount)}


def context():
    flows = [
        {"event_id": "seed", "role": "SEED", "from_account": "X|ETH", "to_account": "A|ETH", "amount_raw": "10"},
        {"event_id": "early_return", "role": "UNKNOWN_EXTERNAL_INCOMING", "from_account": "X|ETH", "to_account": "A|ETH", "amount_raw": "4"},
        {"event_id": "exit", "role": "BOUNDARY_OUTFLOW", "from_account": "A|ETH", "to_account": "X|ETH", "amount_raw": "6"},
        {"event_id": "return", "role": "UNKNOWN_EXTERNAL_INCOMING", "from_account": "Y|ETH", "to_account": "A|ETH", "amount_raw": "3"},
        {"event_id": "enter", "role": "CANDIDATE", "from_account": "A|ETH", "to_account": "T|ETH", "amount_raw": "11"}]
    return {"schema_version": baselines.CONTEXT_SCHEMA, "query_id": "ctx_unit",
            "accounts": [{"account_id": "A|ETH", "initial_actual_balance_raw": "0", "initial_source_raw": "0",
                          "initial_source_basis": "synthetic causal initialization", "initial_position": {"block_number": 0, "phase": "BLOCK_END"}}],
            "transactions": [{"tx_id": "tx" + str(i), "block_number": i + 1, "tx_index": 0, "flows": [flow], "fees": []} for i, flow in enumerate(flows)],
            "objective_groups": {"T|ETH": ["enter"]}}


class Stage1CBaselineTests(unittest.TestCase):
    def test_temporal_marking_normal_income_is_not_a_seed(self):
        data = graph()
        data["events"].insert(0, transfer("early", 0, "A", "T", 5))
        data["events"].insert(0, {"id": "normal", "kind": "normal_incoming", "order": -1, "to": "A", "asset": "ETH", "amount_raw": "5"})
        result = run_baseline(data, "POISON")
        self.assertFalse(result["events"]["early"]["supported"])
        self.assertEqual(result["events"]["normal"]["nominal_raw"], "0")
        self.assertTrue(result["events"]["enter"]["supported"])

    def test_reachability_has_no_amount_fields(self):
        result = run_baseline(graph(), "BOUNDED_REACHABILITY")
        for value in result["events"].values():
            self.assertIsNone(value["source_amount_raw"])
            self.assertIsNone(value["point_raw"])
            self.assertIsNone(value["nominal_raw"])
        self.assertIsNone(result["joint_by_asset"]["ETH"]["point_raw"])
        self.assertEqual(result["events"]["enter"]["witness"], ["seed", "enter"])

    def test_poison_and_reachability_can_have_identical_sets(self):
        a = run_baseline(graph(), "POISON")
        b = run_baseline(graph(), "BOUNDED_REACHABILITY")
        self.assertEqual(a["output_address_ids"], b["output_address_ids"])
        self.assertEqual(a["output_event_ids"], b["output_event_ids"])
        self.assertEqual(a["joint_by_asset"]["ETH"]["nominal_raw"], "10")

    def test_haircut_is_exact_proportion_not_full_amount(self):
        result = run_baseline(graph(), "HAIRCUT")
        self.assertEqual(result["allocation_raw"], {"seed": "10", "enter": "5"})
        self.assertEqual(result["joint_by_asset"]["ETH"]["point_raw"], "5")
        self.assertTrue(result["construction_audit"]["per_asset"]["ETH"]["exact_conserved"])

    def test_sub_raw_unit_fraction_never_floored(self):
        data = graph({"A|ETH": "2", "T|ETH": "0"})
        for event in data["events"]:
            event["amount_raw"] = "1"
        self.assertEqual(run_baseline(data, "HAIRCUT")["allocation_raw"]["enter"], "1/3")

    def test_multiple_outputs_share_updated_source(self):
        data = graph(targets=["T", "U"])
        data["initial_balances"]["U|ETH"] = "0"
        data["events"].append(transfer("enter_u", 3, "A", "U", 10))
        result = run_baseline(data, "HAIRCUT")
        self.assertEqual(result["allocation_raw"]["enter"], "5")
        self.assertEqual(result["allocation_raw"]["enter_u"], "5")
        self.assertEqual(result["joint_by_asset"]["ETH"]["point_raw"], "10")

    def test_same_address_repeated_entries_are_jointly_summed_once(self):
        data = graph()
        data["events"].append(transfer("enter_again", 3, "A", "T", 10))
        result = run_baseline(data, "HAIRCUT")
        self.assertEqual(result["addresses"]["T|ETH"]["events"], ["enter", "enter_again"])
        self.assertEqual(result["addresses"]["T|ETH"]["point_raw"], "10")

    def test_poison_persists_after_actual_balance_clear(self):
        data = graph({"A|ETH": "0", "B|ETH": "0", "T|ETH": "0"})
        data["events"][1] = transfer("drain", 2, "A", "B", 10)
        data["events"].extend([{"id": "normal", "kind": "normal_incoming", "order": 3, "to": "A", "asset": "ETH", "amount_raw": "7"},
                               transfer("later", 4, "A", "T", 7)])
        self.assertEqual(run_baseline(data, "POISON")["events"]["later"]["nominal_raw"], "7")
        self.assertEqual(run_baseline(data, "HAIRCUT")["events"]["later"]["point_raw"], "0")

    def test_zero_hop_service_seed_is_legal(self):
        data = graph({"T|ETH": "0"}, [{"id": "seed", "kind": "seed", "order": 1, "to": "T", "asset": "ETH", "amount_raw": "3"}])
        for method in sorted(baselines.METHODS):
            result = run_baseline(data, method)
            self.assertEqual(result["output_event_ids"], ["seed"])
            self.assertEqual(result["output_address_ids"], ["T|ETH"])

    def test_no_service_is_valid_empty_result(self):
        data = graph(targets=[])
        for method in sorted(baselines.METHODS):
            result = run_baseline(data, method)
            self.assertEqual(result["status"], "OK")
            self.assertEqual(result["addresses"], {})

    def test_unknown_modeled_haircut_balance_is_null_not_zero(self):
        data = graph({"A|ETH": None, "T|ETH": "0"})
        result = run_baseline(data, "HAIRCUT")
        self.assertEqual(result["status"], "NOT_APPLICABLE")
        self.assertIn("UNKNOWN_MODELED_ACTUAL_BALANCE", result["failure_reason"])
        self.assertIsNone(result["allocation_raw"])
        self.assertIsNone(result["joint_by_asset"])
        self.assertEqual(run_baseline(data, "POISON")["status"], "OK")

    def test_aligned_anchor_resolves_unknown_before_spend(self):
        data = graph({"A|ETH": None, "T|ETH": "0"})
        data["events"][0]["balance_anchors_after"] = {"A|ETH": "20"}
        self.assertEqual(run_baseline(data, "HAIRCUT")["allocation_raw"]["enter"], "5")

    def test_conflicting_anchor_is_error_not_na(self):
        data = graph()
        data["events"][0]["balance_anchors_after"] = {"A|ETH": "19"}
        with self.assertRaisesRegex(ValueError, "Anchor conflicts"):
            run_baseline(data, "HAIRCUT")

    def test_actual_overdraft_is_error_not_divide_by_zero(self):
        data = graph({"A|ETH": "0", "T|ETH": "0"})
        data["events"].append(transfer("overdraft", 3, "A", "T", 1))
        with self.assertRaisesRegex(ValueError, "overdraft"):
            run_baseline(data, "HAIRCUT")

    def test_asset_states_and_native_gas_are_isolated(self):
        data = graph({"A|TOKEN": "0", "A|ETH": "2", "T|TOKEN": "0"})
        data["events"][0]["asset"] = "TOKEN"
        data["events"][1]["asset"] = "TOKEN"
        data["events"].insert(1, {"id": "gas", "kind": "gas", "order": 0, "from": "A", "asset": "ETH", "amount_raw": "1"})
        for method in sorted(baselines.METHODS):
            result = run_baseline(data, method)
            self.assertFalse(result["events"]["gas"]["supported"])
        self.assertEqual(run_baseline(data, "HAIRCUT")["allocation_raw"]["enter"], "10")

    def test_invalid_non_native_gas_rejected(self):
        data = graph()
        data["events"][1].update({"kind": "gas", "asset": "TOKEN"})
        with self.assertRaisesRegex(ValueError, "gas"):
            run_baseline(data, "HAIRCUT")

    def test_weth_gross_refund_and_net_conserve_exact_source(self):
        data = graph({"A|ETH": "10", "A|WETH": "0", "T|WETH": "0"})
        data["events"][1] = {"id": "wrap", "kind": "conversion", "order": 2, "from": "A", "to": "A", "asset": "ETH",
                             "output_asset": "WETH", "gross_raw": "12", "refund_raw": "2", "output_raw": "10", "certified_semantics": "SYNTHETIC_CONTROLLED"}
        data["events"].append(transfer("enter", 3, "A", "T", 10, "WETH"))
        result = run_baseline(data, "HAIRCUT")
        self.assertEqual(result["allocation_raw"], {"seed": "10", "wrap:input": "6", "wrap:refund": "1", "wrap:net": "5", "wrap:output": "5", "enter": "5"})
        self.assertTrue(all(v["exact_conserved"] for v in result["construction_audit"]["per_asset"].values()))

    def test_unknown_protocol_is_na_not_fabricated_support(self):
        data = graph()
        data["events"][1] = {"id": "swap", "kind": "conversion", "order": 2, "from": "A", "to": "T", "asset": "ETH",
                             "output_asset": "TOKEN", "gross_raw": "10", "output_raw": "20", "certified_semantics": "SYNTHETIC_CONTROLLED"}
        for method in sorted(baselines.METHODS):
            self.assertEqual(run_baseline(data, method)["status"], "NOT_APPLICABLE")

    def test_multioutput_haircut_uses_one_funded_gross(self):
        data = graph(targets=["T", "U"])
        data["initial_balances"]["U|ETH"] = "0"
        data["events"][1] = {"id": "split", "kind": "multioutput", "order": 2, "from": "A", "asset": "ETH", "gross_raw": "10",
                             "outputs": [{"to": "T", "amount_raw": "6"}, {"to": "U", "amount_raw": "4"}], "certified_semantics": "SYNTHETIC_CONTROLLED"}
        result = run_baseline(data, "HAIRCUT")
        self.assertEqual(result["allocation_raw"], {"seed": "10", "split:input": "5", "split:output0": "3", "split:output1": "2"})

    def test_methods_do_not_mutate_or_share_attribution_state(self):
        data = graph()
        original = copy.deepcopy(data)
        a = run_baseline(data, "HAIRCUT")
        run_baseline(data, "POISON")
        b = run_baseline(data, "HAIRCUT")
        self.assertEqual(a, b)
        self.assertEqual(data, original)

    def test_no_primary_solver_or_hidden_file_can_be_called(self):
        source = inspect.getsource(baselines)
        self.assertNotIn("import lp_model", source)
        self.assertNotIn("import context_lp", source)
        self.assertNotIn("import scipy", source)
        with patch("builtins.open", side_effect=AssertionError("Baseline may not open answer files")):
            for method in sorted(baselines.METHODS):
                self.assertEqual(run_baseline(graph(), method)["status"], "OK")

    def test_context_bmin_positive_is_explicit_not_observed(self):
        result = run_baseline(context(), "HAIRCUT")
        convention = result["boundary_completion"]
        self.assertTrue(convention["adopted"])
        self.assertFalse(convention["is_observed_fact"])
        self.assertEqual(convention["B0_out_raw"], "4")
        self.assertEqual(result["allocation_raw"]["early_return"], "0")
        self.assertEqual(result["allocation_raw"]["return"], "15/7")
        self.assertEqual(result["allocation_raw"]["enter"], "55/7")
        self.assertTrue(result["construction_audit"]["per_asset"]["ETH"]["exact_conserved"])

    def test_outside_pool_temporal_marking_is_not_new_seed(self):
        result = run_baseline(context(), "POISON")
        self.assertFalse(result["events"]["early_return"]["supported"])
        self.assertTrue(result["events"]["return"]["supported"])
        self.assertEqual(result["events"]["return"]["witness"], ["seed", "exit", "return"])

    def test_closed_context_does_not_adopt_boundary_completion(self):
        data = context()
        data["transactions"] = [data["transactions"][0], data["transactions"][-1]]
        data["transactions"][-1]["flows"][0]["amount_raw"] = "10"
        result = run_baseline(data, "HAIRCUT")
        self.assertFalse(result["boundary_completion"]["adopted"])
        self.assertEqual(result["boundary_completion"]["B0_out_raw"], "0")

    def test_context_normal_incoming_needs_causal_zero_basis(self):
        data = context()
        data["transactions"][1]["flows"][0]["role"] = "BACKGROUND_NORMAL"
        with self.assertRaisesRegex(ValueError, "normal-origin"):
            run_baseline(data, "HAIRCUT")

    def test_transaction_gas_charged_before_value_once(self):
        data = context()
        data["transactions"] = [data["transactions"][0], data["transactions"][-1]]
        data["transactions"][-1]["flows"][0]["amount_raw"] = "9"
        data["transactions"][-1]["fees"] = [{"fee_id": "gas", "payer_account": "A|ETH", "amount_raw": "1", "timing": "TX_BEGIN_NET_FEE"}]
        result = run_baseline(data, "HAIRCUT")
        self.assertEqual(result["allocation_raw"]["gas"], "1")
        self.assertEqual(result["allocation_raw"]["enter"], "9")
        data["transactions"][-1]["fees"].append(copy.deepcopy(data["transactions"][-1]["fees"][0]))
        with self.assertRaisesRegex(ValueError, "once"):
            run_baseline(data, "HAIRCUT")

    def test_same_physical_self_transfer_is_debited_before_credit(self):
        data = graph({"A|ETH": "0", "T|ETH": "0"})
        data["events"].insert(1, transfer("self", 0, "A", "A", 1))
        with self.assertRaisesRegex(ValueError, "overdraft"):
            run_baseline(data, "HAIRCUT")

    def test_objective_diagnostics_are_not_service_labels(self):
        data = graph()
        data["objective_groups"] = {"arbitrary_diagnostic": ["seed"], "ALL_SERVICE": ["enter"]}
        self.assertEqual(observed_targets(data), {"T|ETH": ["enter"]})

    def test_lp_verifier_checks_haircut_only_after_construction(self):
        from lp_model import build_model
        data = graph()
        result = run_baseline(data, "HAIRCUT")
        model = build_model(data)
        vector = model.lift_hidden_allocation_for_audit(result["allocation_raw"])
        self.assertTrue(model.audit_vector(vector)["exact_feasible"])

    def test_context_verifier_accepts_explicit_bmin_point(self):
        from context_lp_r3 import build_context_model
        data = context()
        result = run_baseline(data, "HAIRCUT")
        model = build_context_model(data)
        self.assertTrue(model.audit_vector(model.lift_hidden_allocation_for_audit(result["allocation_raw"]))["exact_feasible"])


if __name__ == "__main__":
    unittest.main()
