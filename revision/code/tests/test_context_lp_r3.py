"""R3 source/balance/fee controls with analytical expectations (no oracle input)."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from context_lp_r3 import (SCHEMA, BOUNDARY, build_context_model, solve_context_interval,
    run_context_document, structural_nesting, audit_context_witness, validate_document, target_completeness)


def account(name, balance="20", block=0):
    return {"account_id": name + "|ETH", "initial_actual_balance_raw": balance,
            "initial_position": {"block_number": block, "tx_index": -1, "phase": "BLOCK_END"},
            "initial_source_raw": "0", "initial_source_basis": "BEFORE_FIRST_CAUSALLY_POSSIBLE_SEED_ARRIVAL",
            "evidence_ids": ["balance-response:" + name]}


def flow(name, sender, receiver, amount, role="CANDIDATE", **extra):
    return {"event_id": name, "from_account": None if sender is None else sender + "|ETH",
            "to_account": None if receiver is None else receiver + "|ETH",
            "amount_raw": str(amount), "role": role, "flow_kind": "top",
            "order_basis": "OBSERVED_TOP_OR_TRACE_EXECUTION_ORDER", "evidence_ids": ["response:" + name], **extra}


def tx(name, block, flows, fees=None):
    return {"tx_id": name, "block_number": block, "tx_index": 0, "flows": flows, "fees": fees or []}


def fee(name, payer, amount, **extra):
    return {"fee_id": name, "payer_account": payer + "|ETH", "amount_raw": str(amount),
            "timing": "TX_BEGIN_NET_FEE", "evidence_ids": ["receipt:" + name], **extra}


def example(balance="20"):
    return {"schema_version": SCHEMA, "query_id": "SYNTHETIC_R3", "accounts": [account("A", balance)],
        "transactions": [tx("seed-tx", 1, [flow("seed", None, "A", 80, "SEED")]),
                         tx("out-tx", 2, [flow("enter", "A", "T", 90)])],
        "objective_groups": {"T|ETH": ["enter"]}, "all_service_entries": ["enter"], "anchors": [], "gaps": []}


def interval(document, events=("enter",), relaxed=False):
    model = build_context_model(document, remove_balance_information=relaxed)
    result = solve_context_interval(model, document, list(events))
    if result["status"] != "OPTIMAL_EXACT_CERTIFIED":
        raise AssertionError(json.dumps(result, indent=2))
    return result


class ContextLPR3Tests(unittest.TestCase):
    def test_known_twenty_seed_eighty_out_ninety(self):
        result = interval(example())
        self.assertEqual((result["lower_raw"], result["upper_raw"]), ("70", "80"))
        self.assertTrue(result["endpoints"]["lower"]["independent_audit"]["exact_feasible"])

    def test_unknown_same_graph_retains_source_cap(self):
        result = interval(example(None))
        self.assertEqual((result["lower_raw"], result["upper_raw"]), ("0", "80"))

    def test_known_other_money_sufficient_zero_lower(self):
        result = interval(example("100"))
        self.assertEqual((result["lower_raw"], result["upper_raw"]), ("0", "80"))

    def test_real_normal_income_is_counted(self):
        doc = example()
        doc["transactions"].insert(1, tx("normal-tx", 2, [flow("normal", None, "A", 100, "BACKGROUND_NORMAL", source_zero_basis="EXTERNAL_ACCOUNT_COULD_NOT_RECEIVE_SOURCE_BEFORE_THIS_BLOCK")]))
        doc["transactions"][-1]["block_number"] = 3
        self.assertEqual(interval(doc)["lower_raw"], "0")
        self.assertEqual(interval(example())["lower_raw"], "70")

    def test_context_label_alone_not_normal_proof(self):
        doc = example()
        doc["transactions"][0]["flows"].insert(0, flow("normal", None, "A", 1, "BACKGROUND_NORMAL"))
        with self.assertRaisesRegex(ValueError, "proof"):
            build_context_model(doc)

    def test_modeled_sender_cannot_be_reclassified_normal(self):
        doc = example()
        doc["accounts"].append(account("B", "100"))
        doc["transactions"][0]["flows"].insert(0, flow("normal", "B", "A", 1, "BACKGROUND_NORMAL", source_zero_basis="not enough"))
        with self.assertRaisesRegex(ValueError, "shared source"):
            build_context_model(doc)

    def test_double_seed_anchor_rejected(self):
        doc = example()
        doc["accounts"][0]["anchor_includes_seed"] = True
        with self.assertRaisesRegex(ValueError, "double injection"):
            build_context_model(doc)

    def test_seed_double_event_rejected(self):
        doc = example()
        doc["transactions"][1]["flows"].append(flow("seed-again", None, "A", 80, "SEED"))
        with self.assertRaisesRegex(ValueError, "Exactly one"):
            build_context_model(doc)

    def test_block_end_is_not_same_block_pre_event(self):
        doc = example()
        doc["accounts"][0]["initial_position"]["block_number"] = 1
        with self.assertRaisesRegex(ValueError, "Block-end"):
            build_context_model(doc)

    def test_fee_only_transaction_cannot_use_later_block_end_anchor(self):
        doc = example()
        doc["accounts"].append(account("B", "100", block=1))
        doc["transactions"].insert(0, tx("before-late-anchor", 0, [], [fee("early-fee", "B", 1)]))
        with self.assertRaisesRegex(ValueError, "Block-end"):
            build_context_model(doc)

    def test_late_account_anchor_retains_own_time(self):
        doc = example()
        doc["accounts"].append(account("B", "0", 1))
        doc["transactions"][1] = tx("move", 2, [flow("move", "A", "B", 90)])
        doc["transactions"].append(tx("out", 3, [flow("enter", "B", "T", 90)]))
        result = interval(doc)
        self.assertEqual(result["lower_raw"], "70")
        model = build_context_model(doc)
        self.assertEqual(model.metadata["initialization_positions"]["B|ETH"]["block_number"], 1)

    def test_missing_one_anchor_keeps_other_actual_bounds(self):
        doc = example()
        doc["accounts"].append(account("B", None))
        doc["gaps"] = [{"account_id": "B|ETH", "reason": "HISTORICAL_BALANCE_RESPONSE_MISSING"}]
        model = build_context_model(doc)
        self.assertEqual(model.metadata["known_initial_balances"], 1)
        self.assertEqual(model.metadata["unknown_initial_balances"], ["B|ETH"])
        self.assertEqual(interval(doc)["lower_raw"], "70")
        self.assertEqual(target_completeness(doc, ["enter"])["status"], "FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE")

    def test_aligned_late_anchor_constrains_without_injection(self):
        doc = example(None)
        doc["anchors"] = [{"anchor_id": "after-seed", "account_id": "A|ETH", "tx_id": "seed-tx",
                           "when": "post", "actual_balance_raw": "100", "evidence_ids": ["poststate"]}]
        result = interval(doc)
        self.assertEqual((result["lower_raw"], result["upper_raw"]), ("70", "80"))
        self.assertEqual(build_context_model(doc).metadata["enabled_anchor_caps"], 1)

    def test_anchor_contradiction_is_not_relaxed_away(self):
        doc = example()
        doc["anchors"] = [{"anchor_id": "wrong", "account_id": "A|ETH", "tx_id": "seed-tx",
                           "when": "post", "actual_balance_raw": "99", "evidence_ids": ["wrong"]}]
        for relaxed in (False, True):
            with self.assertRaisesRegex(ValueError, "CONFLICT"):
                build_context_model(doc, remove_balance_information=relaxed)

    def test_joint_lower_positive_while_individual_zero(self):
        doc = example("100")
        doc["transactions"].append(tx("second", 3, [flow("enter-u", "A", "U", 90)]))
        doc["objective_groups"]["U|ETH"] = ["enter-u"]
        doc["all_service_entries"].append("enter-u")
        self.assertEqual(interval(doc)["lower_raw"], "0")
        self.assertEqual(interval(doc, ["enter-u"])["lower_raw"], "0")
        joint = interval(doc, ["enter", "enter-u"])
        self.assertEqual((joint["lower_raw"], joint["upper_raw"]), ("80", "80"))

    def test_fee_and_transfer_share_beginning_funds(self):
        doc = example()
        doc["transactions"][1]["fees"] = [fee("fee", "A", 10)]
        result = interval(doc)
        self.assertEqual((result["lower_raw"], result["upper_raw"]), ("70", "80"))
        lower = result["endpoints"]["lower"]["witness_event_source_raw"]
        self.assertEqual(lower["fee"], "10")
        self.assertTrue(audit_context_witness(doc, lower)["exact_feasible"])

    def test_fee_cannot_use_same_transaction_later_income(self):
        doc = example("0")
        doc["transactions"][0]["fees"] = [fee("bad-payer", "A", 1)]
        with self.assertRaisesRegex(ValueError, "negative pre-credit"):
            build_context_model(doc)

    def test_failed_transaction_without_value_still_spends_gas(self):
        doc = example()
        doc["transactions"].insert(1, tx("failed", 2, [], [fee("failed-fee", "A", 10)]))
        doc["transactions"][-1]["block_number"] = 3
        model = build_context_model(doc)
        self.assertIn("failed-fee", model.event_variables)
        self.assertEqual(model.metadata["final_actual_balances_raw"]["A|ETH"], "0")
        self.assertEqual(interval(doc, ["failed-fee", "enter"])["lower_raw"], "80")

    def test_seed_payer_is_not_recipient(self):
        doc = example()
        doc["transactions"][0]["fees"] = [fee("seed-gas", "EXTERNAL", 9,
            source_zero_basis="PAID_BEFORE_SOURCE_DEFINITION_AT_SEED_RECIPIENT")]
        self.assertEqual(interval(doc)["lower_raw"], "70")
        self.assertEqual(interval(doc, ["seed-gas"])["upper_raw"], "0")

    def test_fee_duplicate_trace_window_rejected(self):
        doc = example()
        doc["transactions"][1]["fees"] = [fee("same-fee", "A", 1), fee("same-fee", "A", 1)]
        with self.assertRaisesRegex(ValueError, "one gas charge"):
            build_context_model(doc)

    def test_source_fee_not_prefunded_by_internal_return(self):
        doc = example(None)
        doc["accounts"].append(account("B", None))
        doc["transactions"][0]["flows"][0]["to_account"] = "B|ETH"
        doc["transactions"][1]["flows"] = [flow("to-a", "B", "A", 80, "MODELED_INTERNAL", flow_kind="internal"), flow("enter", "A", "T", 80)]
        doc["transactions"][1]["fees"] = [fee("before-internal", "A", 2)]
        self.assertEqual(interval(doc, ["before-internal"])["upper_raw"], "0")

    def test_internal_incoming_can_fund_later_internal_outgoing(self):
        doc = example()
        doc["accounts"].append(account("B", "0"))
        doc["transactions"][1]["flows"] = [flow("to-b", "A", "B", 90, "MODELED_INTERNAL", flow_kind="internal"), flow("enter", "B", "T", 90, flow_kind="internal")]
        self.assertEqual(interval(doc)["lower_raw"], "70")

    def test_boundary_return_cannot_create_source(self):
        doc = example("20")
        doc["transactions"].insert(1, tx("unknown", 2, [flow("return", "OUTSIDE", "A", 100, "UNKNOWN_EXTERNAL_INCOMING")]))
        doc["transactions"][-1]["block_number"] = 3
        self.assertEqual(interval(doc, ["return"])["upper_raw"], "0")

    def test_boundary_multiple_returns_share_prior_capacity(self):
        doc = example("0")
        doc["transactions"] = [doc["transactions"][0],
            tx("exit", 2, [flow("exit", "A", "OUTSIDE", 80, "BOUNDARY_OUTFLOW")]),
            tx("return-a", 3, [flow("return-a", "OUTSIDE", "A", 80, "UNKNOWN_EXTERNAL_INCOMING")]),
            tx("return-b", 4, [flow("return-b", "OUTSIDE", "A", 80, "UNKNOWN_EXTERNAL_INCOMING")]),
            tx("enter-tx", 5, [flow("enter", "A", "T", 160)])]
        result = interval(doc, ["return-a", "return-b"])
        # The unobserved external account may retain the seed and return its
        # own normal funds.  It may never return the same 80 source twice.
        self.assertEqual((result["lower_raw"], result["upper_raw"]), ("0", "80"))
        self.assertEqual(interval(doc)["upper_raw"], "80")

    def test_boundary_return_before_exit_cannot_borrow_future_source(self):
        doc = example("100")
        doc["transactions"].insert(1, tx("return-tx", 2, [flow("return", "OUTSIDE", "A", 80, "UNKNOWN_EXTERNAL_INCOMING")]))
        doc["transactions"].insert(2, tx("exit-tx", 3, [flow("exit", "A", "OUTSIDE", 80, "BOUNDARY_OUTFLOW")]))
        doc["transactions"][-1]["block_number"] = 4
        self.assertEqual(interval(doc, ["return"])["upper_raw"], "0")

    def test_preseed_external_source_is_zero_by_single_seed_conservation(self):
        doc = example("0")
        doc["transactions"][0]["block_number"] = 2
        doc["transactions"][1]["block_number"] = 3
        doc["transactions"].insert(0, tx("prior", 1, [flow("prior", "OUTSIDE", "A", 20, "UNKNOWN_EXTERNAL_INCOMING")]))
        self.assertEqual(interval(doc, ["prior"])["upper_raw"], "0")
        self.assertEqual(interval(doc)["lower_raw"], "70")

    def test_first_service_absorbing_no_reservoir_return(self):
        doc = example("100")
        doc["transactions"].append(tx("service-return", 3, [flow("bad", "T", "A", 10, "UNKNOWN_EXTERNAL_INCOMING")]))
        with self.assertRaisesRegex(ValueError, "absorbing"):
            build_context_model(doc)

    def test_same_graph_relaxation_structurally_nested(self):
        doc = example()
        full, wide = build_context_model(doc), build_context_model(doc, remove_balance_information=True)
        self.assertTrue(structural_nesting(full, wide)["nested"])
        result = run_context_document(doc)
        self.assertTrue(result["all_same_graph_comparisons_passed"])
        self.assertEqual(result["variants"]["MATCHED_INFORMATION_RELAXED"]["all_service_joint"]["lower_raw"], "0")

    def test_all_downstream_zero_real_constraints_exclude(self):
        result = run_context_document(example())
        self.assertFalse(result["variants"]["BEST_AVAILABLE_CONTEXT"]["all_downstream_zero"]["feasible"])
        self.assertTrue(result["variants"]["MATCHED_INFORMATION_RELAXED"]["all_downstream_zero"]["feasible"])

    def test_hidden_allocations_ignored_by_solver(self):
        doc = example()
        doc["hidden_allocation"] = {"enter": "3"}
        doc["reference_answer"] = {"lower_raw": "3", "upper_raw": "3"}
        self.assertEqual(interval(doc)["lower_raw"], "70")

    def test_witness_tamper_detected_independently(self):
        doc = example()
        witness = interval(doc)["endpoints"]["lower"]["witness_event_source_raw"]
        witness["enter"] = "0"
        audit = audit_context_witness(doc, witness)
        self.assertFalse(audit["exact_feasible"])
        self.assertTrue(any("exceeds observed" in error for error in audit["errors"]))

    def test_event_duplicate_and_conflict_rejected(self):
        doc = example()
        doc["transactions"][1]["flows"].append(copy.deepcopy(doc["transactions"][1]["flows"][0]))
        with self.assertRaisesRegex(ValueError, "Duplicate physical value"):
            build_context_model(doc)
        doc = example()
        doc["fact_conflicts"] = [{"event": "enter", "amounts": [90, 91]}]
        with self.assertRaisesRegex(ValueError, "CONFLICT"):
            build_context_model(doc)

    def test_reordered_or_missing_real_position_rejected(self):
        doc = example()
        doc["transactions"][1]["tx_index"] = None
        with self.assertRaisesRegex(ValueError, "observed integer"):
            build_context_model(doc)

    def test_delegated_execution_is_not_new_value_flow(self):
        doc = example()
        doc["transactions"][1]["flows"][0]["flow_kind"] = "delegatecall"
        with self.assertRaisesRegex(ValueError, "independent ETH value"):
            build_context_model(doc)

    def test_provenance_contains_facts_and_real_constraints(self):
        doc = example()
        doc["transactions"][1]["fees"] = [fee("gas", "A", 1)]
        model = build_context_model(doc)
        rows = model.metadata["constraint_provenance"]
        self.assertTrue(any(row.get("evidence_ids") == ["balance-response:A"] and row.get("actual_capacity_applied") for row in rows))
        self.assertTrue(any(row.get("operation_id") == "gas" and row.get("role") == "FEE" for row in rows))
        self.assertEqual(model.metadata["known_initial_balances"], 1)

    def test_serialized_actual_input_path_reproducible(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "context.json"
            path.write_text(json.dumps(example()), encoding="utf-8")
            document = json.loads(path.read_text(encoding="utf-8"))
            result = run_context_document(document)
            self.assertEqual(result["variants"]["BEST_AVAILABLE_CONTEXT"]["all_service_joint"]["lower_raw"], "70")

    def test_large_context_vertex_preserves_irregular_integer_wei(self):
        # An analytical single chain exceeds the old 512-variable recovery
        # limit.  Irregular wei must survive a floating optimizer proposal.
        seed_amount = 80000000000000000007
        payment = 90000000000000000003
        doc = example("20000000000000000000")
        doc["transactions"][0]["flows"][0]["amount_raw"] = str(seed_amount)
        doc["transactions"] = doc["transactions"][:1]
        previous = "A"
        for i in range(160):
            recipient = "N" + str(i)
            doc["accounts"].append(account(recipient, "0"))
            doc["transactions"].append(tx("chain-tx-" + str(i), i + 2,
                [flow("chain-" + str(i), previous, recipient, payment)]))
            previous = recipient
        doc["transactions"].append(tx("final", 162, [flow("enter", previous, "T", payment)]))
        model = build_context_model(doc)
        self.assertGreater(len(model.variables), 512)
        result = interval(doc)
        self.assertEqual((result["lower_raw"], result["upper_raw"]),
                         ("70000000000000000003", str(seed_amount)))
        self.assertTrue(result["endpoints"]["lower"]["independent_audit"]["exact_feasible"])
        self.assertEqual(result["endpoints"]["lower"]["certificate"]["primal_recovery"]["method"],
                         "R3_SPARSE_EXACT_ACTIVE_BOUND_SOLVE")


if __name__ == "__main__":
    unittest.main()
