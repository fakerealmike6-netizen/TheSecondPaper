"""R3 tests begin with saved provider response shapes, then use the real adapter."""
import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from context_ledger_r3 import (EvidenceConflict, assemble_model, coverage_complete,
                               decode_saved_response, normalize_anchor, normalize_rows,
                               replay_manifest)


A = "0x" + "1" * 40
B = "0x" + "2" * 40
S = "0x" + "3" * 40
T = "0x" + "4" * 40
REQUIRED = ["ALL_TOP_LEVEL_TRANSACTIONS_INCLUDING_ZERO_VALUE_AND_FAILED", "ALL_EFFECTIVE_NATIVE_INTERNAL_TRANSFERS_INCLUDING_SELFDESTRUCT", "ALL_TRANSACTION_FEES_PAID_BY_ACCOUNT", "APPLICABLE_PROTOCOL_NATIVE_CHANGES"]


def tx(identity, block=10, index=0, sender=S, recipient=A, value=80, success=True, fee=0):
    return {"record_type": "transaction", "block_number": block, "block_hash": "0x" + str(block).zfill(64), "tx_hash": "0x" + identity * 64, "tx_index": index, "from_address": sender, "to_address": recipient, "value_raw": str(value), "success": success, "gas_used": 21000, "fee_raw": str(fee)}


def trace(identity, path, sender, recipient, value, success=True, call_type="call"):
    row = tx(identity, sender=sender, recipient=recipient, value=value, success=success)
    row.update(record_type="trace", trace_address=json.dumps(path), trace_type="call", call_type=call_type)
    return row


def anchor(address, block, value):
    return normalize_anchor({"id": 7, "method": "eth_getBalance", "params": [address, hex(block)]}, {"jsonrpc": "2.0", "id": 7, "result": hex(value)}, {"jsonrpc": "2.0", "id": 8, "result": {"number": hex(block), "hash": "0x" + str(block).zfill(64)}}, ["synthetic_balance_response_sha256"])


def fixture(rows=None, before=20, after=10, coverage=True):
    rows = rows or [tx("a", value=80), tx("b", block=11, sender=A, recipient=T, value=90)]
    saved = {"execution_id": "SYNTHETIC_EXECUTION", "result": {"rows": rows, "metadata": {"total_row_count": len(rows)}}}
    normalized = normalize_rows(decode_saved_response(saved, "synthetic_saved_response_sha256"))
    seed = "eip155:1:tx:0x" + "a" * 64 + ":top"
    out = "eip155:1:tx:0x" + "b" * 64 + ":top"
    seed_row = next(r for r in rows if r.get("tx_hash") == "0x" + "a" * 64 and r["record_type"] == "transaction")
    graph = {"scenario_id": "synthetic_context", "target_accounts": [T], "events": [{"id": seed, "kind": "seed"}, {"id": out, "kind": "transfer"}], "physical_fact_manifest": [{"event_id": seed, "block": 10, "tx_index": seed_row["tx_index"], "tx_hash": "0x" + "a" * 64}], "objective_groups": {T + "|ETH": [out]}}
    target = {"name": "synthetic_context", "rows": [{"account_id": A + "|ETH", "address": A, "before_anchor_block": 9, "after_anchor_block": 11, "ledger_start_block": 10, "ledger_end_block": 11, "required_coverage": REQUIRED}]}
    anchors = ([anchor(A, 9, before)] if before is not None else []) + ([anchor(A, 11, after)] if after is not None else [])
    coverage_rows = [{"address": A, "data_type": kind, "start_block": 10, "end_block": 11, "pagination_complete": True, "status": "COMPLETE", "evidence_ids": ["synthetic_complete_page"]} for kind in REQUIRED] if coverage else []
    return assemble_model(graph, target, normalized, anchors, coverage_rows)


class ContextLedgerR3Tests(unittest.TestCase):
    def test_saved_response_to_known_balance_model_is_connected(self):
        result = fixture()
        self.assertEqual(result["model_input"]["accounts"][0]["initial_actual_balance_raw"], "20")
        self.assertEqual(result["ledger_reconciliation"][0]["difference_raw"], "0")
        self.assertEqual(result["account_ledgers"][-1]["balance_after_raw"], "10")
        self.assertTrue(any(p["fact_type"] == "BALANCE_ANCHOR" and p["used"] for p in result["constraint_provenance"]))

    def test_saved_response_pipeline_reaches_solver_when_available(self):
        from context_lp_r3 import run_context_document
        result = run_context_document(fixture()["model_input"])
        informed = result["variants"]["BEST_AVAILABLE_CONTEXT"]["all_service_joint"]
        relaxed = result["variants"]["MATCHED_INFORMATION_RELAXED"]["all_service_joint"]
        self.assertEqual((informed["lower_raw"], informed["upper_raw"]), ("70", "80"))
        self.assertEqual((relaxed["lower_raw"], relaxed["upper_raw"]), ("0", "80"))
        self.assertTrue(result["all_same_graph_comparisons_passed"])

    def test_missing_balance_remains_local_not_zero(self):
        result = fixture(before=None, after=None)
        self.assertIsNone(result["model_input"]["accounts"][0]["initial_actual_balance_raw"])
        self.assertEqual(result["completion_status"], "PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS")

    def test_rpc_latest_rejected(self):
        with self.assertRaises(ValueError):
            normalize_anchor({"method": "eth_getBalance", "params": [A, "latest"]}, {"result": "0x10"})

    def test_rpc_missing_result_not_zero(self):
        with self.assertRaises(ValueError):
            normalize_anchor({"method": "eth_getBalance", "params": [A, "0xa"]}, {"result": None})

    def test_block_identity_conflict_rejected(self):
        with self.assertRaises(EvidenceConflict):
            normalize_anchor({"method": "eth_getBalance", "params": [A, "0xa"]}, {"result": "0x10"}, {"result": {"number": "0xb", "hash": "0x123"}})

    def test_anchor_semantics_are_block_end_not_event_pre(self):
        item = anchor(A, 10, 100)
        self.assertEqual(item["position"], "BLOCK_END")
        self.assertNotIn("event_before", item)

    def test_fee_dedup_root_trace_and_windows(self):
        row = tx("a", sender=A, recipient=B, value=30, fee=2)
        result = normalize_rows([row, copy.deepcopy(row), trace("a", [], A, B, 30)])
        self.assertEqual(len(result["transactions"]), 1)
        self.assertEqual(len(result["flows"]), 1)
        self.assertEqual(result["transactions"][0]["fee_raw"], "2")
        self.assertFalse(result["conflicts"])

    def test_failed_value_rolled_back_fee_kept(self):
        result = normalize_rows([tx("a", sender=A, recipient=B, value=30, fee=2, success=False), trace("a", [], A, B, 30), trace("a", [0], B, T, 20)])
        self.assertEqual(result["flows"], [])
        self.assertEqual(result["transactions"][0]["fee_raw"], "2")

    def test_failed_ancestor_rolls_back_success_child(self):
        result = normalize_rows([tx("a", sender=A, recipient=B, value=0), trace("a", [], A, B, 0), trace("a", [0], B, S, 10, success=False), trace("a", [0, 0], S, T, 5)])
        self.assertEqual(result["flows"], [])

    def test_delegatecall_value_is_not_capacity(self):
        result = normalize_rows([tx("a", sender=A, recipient=B, value=20), trace("a", [], A, B, 20), trace("a", [0], B, S, 20, call_type="delegatecall")])
        self.assertEqual(len(result["flows"]), 1)

    def test_contract_create_uses_created_recipient_and_root_is_one_value(self):
        row = tx("a", sender=A, recipient=None, value=20)
        root = trace("a", [], A, None, 20)
        root.update(trace_type="create", created_address=B)
        result = normalize_rows([row, root])
        self.assertEqual(len(result["flows"]), 1)
        self.assertEqual(result["flows"][0]["recipient"], B)
        self.assertFalse(result["conflicts"])

    def test_selfdestruct_value_uses_refund_address(self):
        root = trace("a", [], A, B, 0)
        child = trace("a", [0], None, None, 15)
        child.update(trace_type="suicide", created_address=B, refund_address=T)
        result = normalize_rows([tx("a", sender=A, recipient=B, value=0), root, child])
        self.assertEqual([(f["sender"], f["recipient"], f["amount_raw"]) for f in result["flows"]], [(B, T, "15")])

    def test_withdrawal_is_a_block_end_actual_credit(self):
        result = normalize_rows([{"record_type": "withdrawal", "block_number": 10, "withdrawal_index": 77, "to_address": A, "value_raw": "15000"}])
        flow = result["flows"][0]
        self.assertEqual(flow["amount_raw"], "15000")
        self.assertEqual(flow["tx_index"], 2147483647)
        self.assertEqual(flow["protocol_role"], "BLOCK_END_WITHDRAWAL")

    def test_positive_fee_recipient_hit_cannot_be_ignored_as_full(self):
        result = fixture(rows=[tx("a", value=80), tx("b", block=11, sender=A, recipient=T, value=90), {"record_type": "fee_recipient", "block_number": 10, "fee_recipient": A}])
        self.assertEqual(result["completion_status"], "PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS")
        self.assertFalse(result["fact_conflicts"])

    def test_cross_source_conflict_is_retained(self):
        first = tx("a", value=80)
        second = copy.deepcopy(first)
        second["value_raw"] = "81"
        result = normalize_rows([first, second])
        self.assertEqual(len(result["conflicts"]), 1)

    def test_gas_price_arithmetic_conflict_not_smoothed(self):
        row = tx("a", fee=2)
        row["gas_used"] = 1
        row["effective_gas_price"] = "2"
        row["gas_raw"] = "3"
        self.assertTrue(normalize_rows([row])["conflicts"])

    def test_zero_residual_does_not_prove_coverage(self):
        result = fixture(coverage=False)
        self.assertEqual(result["ledger_reconciliation"][0]["difference_raw"], "0")
        self.assertEqual(result["completion_status"], "PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS")

    def test_nonadjacent_windows_have_real_hole(self):
        cov = [{"address": A, "data_type": "top", "start_block": a, "end_block": b, "pagination_complete": True, "status": "COMPLETE", "evidence_ids": ["receipt"]} for a, b in [(10, 11), (13, 14)]]
        self.assertFalse(coverage_complete(cov, A, 10, 14, ["top"])[0])

    def test_missing_page_not_complete(self):
        cov = [{"address": A, "data_type": "top", "start_block": 10, "end_block": 14, "pagination_complete": False, "status": "COMPLETE", "evidence_ids": ["receipt"]}]
        self.assertFalse(coverage_complete(cov, A, 10, 14, ["top"])[0])

    def test_nonzero_reconciliation_is_hard_conflict(self):
        result = fixture(after=11)
        self.assertEqual(result["completion_status"], "EVIDENCE_CONFLICT_MODEL_BLOCKED")
        self.assertTrue(any(c["reason"] == "ANCHOR_RECONCILIATION_MISMATCH" for c in result["fact_conflicts"]))

    def test_seed_payer_not_recipient(self):
        result = fixture(rows=[tx("a", value=80, fee=2), tx("b", block=11, sender=A, recipient=T, value=90)])
        first_step = result["model_input"]["transactions"][0]
        self.assertFalse(first_step["fees"])
        self.assertTrue(any(p.get("used") is False and "seed payer" in p.get("reason", "") for p in result["constraint_provenance"]))

    def test_internal_inflow_does_not_finance_prior_top_fee(self):
        rows = [tx("a", value=80), tx("b", block=11, sender=A, recipient=T, value=90, fee=11)]
        result = fixture(rows=rows, before=20, after=0)
        self.assertTrue(any(c["reason"] == "NEGATIVE_PRE_TRANSACTION_ACTUAL_CAPACITY" for c in result["fact_conflicts"]))

    def test_saved_response_replay_is_idempotent(self):
        self.assertEqual(fixture(), fixture())

    def test_observed_other_inflow_precedes_seed_and_is_retained(self):
        # Same complete boundary block, strictly before the seed transaction.
        rows = [tx("c", index=0, sender=B, recipient=A, value=20), tx("a", index=1, value=80), tx("b", block=11, sender=A, recipient=T, value=90)]
        result = fixture(rows=rows, before=0, after=10)
        self.assertEqual(result["model_input"]["transactions"][0]["flows"][0]["role"], "BACKGROUND_NORMAL")
        self.assertEqual(sum(int(r["incoming_raw"]) for r in result["account_ledgers"]), 100)
        from context_lp_r3 import run_context_document
        amount = run_context_document(result["model_input"])["variants"]["BEST_AVAILABLE_CONTEXT"]["all_service_joint"]
        self.assertEqual((amount["lower_raw"], amount["upper_raw"]), ("70", "80"))

    def test_hash_bound_rpc_envelopes_to_ledger_to_lp_and_tamper_refusal(self):
        from context_lp_r3 import run_context_document
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            root = Path(temporary)

            def save(path, value):
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(json.dumps(value), encoding="utf-8")
                return {"path": path, "sha256": hashlib.sha256(target.read_bytes()).hexdigest()}

            request_rows, response_rows = [], []
            for index, (method, params, result) in enumerate([
                ("eth_getBalance", [A, "0x9"], "0x14"),
                ("eth_getBalance", [A, "0xb"], "0xa"),
                ("eth_getBlockByNumber", ["0x9", False], {"number": "0x9", "hash": "0x" + "9".zfill(64)}),
                ("eth_getBlockByNumber", ["0xb", False], {"number": "0xb", "hash": "0x" + "11".zfill(64)}),
            ]):
                request_rows.append({"jsonrpc": "2.0", "id": index, "method": method, "params": params})
                response_rows.append({"jsonrpc": "2.0", "id": index, "result": result})
            wire = save("raw/batch/response_body.bin", response_rows)
            members = []
            for index, (request, response) in enumerate(zip(request_rows, response_rows)):
                envelope = save(f"raw/batch/envelope_{index}.json", {"request": request, "response": response, "http_status": 200, "status": "SUCCESS_VALIDATED", "raw_body_sha256": wire["sha256"]})
                members.append({"artifact_path": envelope["path"], "artifact_sha256": envelope["sha256"]})
            receipt = save("raw/batch/receipt.json", {"members": members, "raw_path": wire["path"], "raw_sha256": wire["sha256"], "raw_bytes": (root / wire["path"]).stat().st_size, "http_status": 200})
            intent = save("raw/batch/dispatch_intent.json", {"requests": request_rows})
            seed_id = "eip155:1:tx:0x" + "a" * 64 + ":top"
            out_id = "eip155:1:tx:0x" + "b" * 64 + ":top"
            graph = save("baseline/fixed_graph.json", {"scenario_id": "synthetic_context", "target_accounts": [T], "events": [{"id": seed_id, "kind": "seed"}, {"id": out_id, "kind": "transfer"}], "physical_fact_manifest": [{"event_id": seed_id, "tx_hash": "0x" + "a" * 64, "block": 10, "tx_index": 0}], "objective_groups": {T + "|ETH": [out_id]}})
            collection = save("baseline/collection.json", {"candidate_events": [dict(tx("a"), asset="native:eip155:1"), dict(tx("b", block=11, sender=A, recipient=T, value=90), asset="native:eip155:1")], "context_events": []})
            target = save("targets.json", {"queries": [{"name": "synthetic_context", "rows": [{"address": A, "account_id": A + "|ETH", "before_anchor_block": 9, "after_anchor_block": 11, "ledger_start_block": 10, "ledger_end_block": 11, "required_coverage": REQUIRED}]}]})
            manifest = {"schema_version": "stage1b-r3-context-replay-v1", "targets": target, "rpc_batches": [{"receipt": receipt, "intent": intent}], "queries": [{"name": "synthetic_context", "fixed_graph": graph, "collection": collection}]}
            save("manifest.json", manifest)
            rebuilt = replay_manifest(root / "manifest.json", root, "synthetic_context", root / "output")
            model = rebuilt["model_input"]
            self.assertEqual(model["accounts"][0]["initial_actual_balance_raw"], "20")
            self.assertEqual(rebuilt["ledger_reconciliation"][0]["difference_raw"], "0")
            solved = run_context_document(model)["variants"]["BEST_AVAILABLE_CONTEXT"]["all_service_joint"]
            self.assertEqual((solved["lower_raw"], solved["upper_raw"]), ("70", "80"))
            (root / wire["path"]).write_text("[]", encoding="utf-8")
            with self.assertRaises(EvidenceConflict):
                replay_manifest(root / "manifest.json", root, "synthetic_context", root / "tamper_output")


if __name__ == "__main__":
    unittest.main()
