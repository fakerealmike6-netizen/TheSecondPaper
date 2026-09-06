"""Offline synthetic WETH proof-obligation tests; no historical code claim."""
import copy
import hashlib
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from weth_component import WETH, DEPOSIT_TOPIC, verify_component

class WethTests(unittest.TestCase):
    def inputs(self):
        tx_hash, sender, caller = "0x" + "a" * 64, "0x" + "1" * 40, "0x" + "2" * 40
        policy = {"tx_hash": tx_hash, "contract": WETH, "credited_address": caller, "amount_raw": "10", "block_number": 123, "deposit_log_index": 7, "input_trace_address": "1"}
        tx = {"hash": tx_hash, "from": sender, "blockNumber": "0x7b", "chainId": "0x1"}
        log = {"logIndex": "0x7", "address": WETH, "transactionHash": tx_hash, "topics": [DEPOSIT_TOPIC, "0x" + "0" * 24 + caller[2:]], "data": "0x" + format(10, "064x"), "removed": False}
        receipt = {"transactionHash": tx_hash, "blockNumber": "0x7b", "status": "0x1", "logs": [log], "gasUsed": "0x5", "effectiveGasPrice": "0x2"}
        internal = {"result": [{"from": caller, "to": WETH, "value": "10", "isError": "0", "blockNumber": "123"}]}
        call = {"type": "CALL", "from": caller, "to": WETH, "value": "0xa", "input": "0xd0e30db0", "logs": [copy.deepcopy(log)]}
        trace = {"from": sender, "to": caller, "type": "CALL", "value": "0xa", "calls": [call]}
        # Synthetic bytecode only. This proves verifier behavior, never mainnet.
        code = "0x6001600055"
        attestation = {"reference_runtime_code": code, "source_sha256": hashlib.sha256(b"synthetic source").hexdigest(), "source_url": "https://example.invalid/synthetic-weth", "chain_id": 1, "contract": WETH, "deposit_semantics": "balanceOf[msg.sender] += msg.value; Deposit(msg.sender,msg.value)"}
        return policy, tx, receipt, internal, {"call_trace": trace, "historical_code": code, "source_attestation": attestation}

    def test_complete_synthetic_proof_and_separate_gas(self):
        policy, tx, receipt, internal, extra = self.inputs()
        result = verify_component(policy, tx, receipt, internal, **extra)
        self.assertEqual("CERTIFIED_LOCAL_WETH_DEPOSIT", result["status"])
        self.assertEqual([0], result["native_locator"]["actual_call_tree_path"])
        unit = result["semantic_unit"]
        self.assertEqual("10", unit["input_port"]["amount_raw"])
        self.assertEqual("10", unit["output_port"]["amount_raw"])
        self.assertEqual("10", unit["transaction_gas_context"]["total_fee_raw"])
        self.assertTrue(unit["transaction_gas_context"]["outside_component_exchange_ratio"])
        self.assertFalse(unit["surrounding_transaction_certified"])
        self.assertFalse(unit["bridge_or_cross_chain_certified"])
        self.assertEqual("Deposit balance credit, not ERC20 Transfer", unit["output_port"]["evidence_kind"])

    def test_cached_pattern_is_not_certified_without_runtime_or_context(self):
        policy, tx, receipt, internal, _ = self.inputs()
        result = verify_component(policy, tx, receipt, internal)
        self.assertEqual("OBSERVED_LOCAL_DEPOSIT_PATTERN_WITH_GAPS", result["status"])
        self.assertIsNone(result["semantic_unit"]["refund_port"]["amount_raw"])
        self.assertTrue(result["native_locator"]["legacy_trace_locator_is_row_ordinal"])
        self.assertFalse(result["semantic_unit"]["next_state"]["enabled_for_real_collection"])

    def test_wrong_credited_recipient_rejects_evidence(self):
        policy, tx, receipt, internal, extra = self.inputs()
        receipt["logs"][0]["topics"][1] = "0x" + "0" * 24 + "3" * 40
        result = verify_component(policy, tx, receipt, internal, **extra)
        self.assertEqual("COMPONENT_EVIDENCE_INCONSISTENT", result["status"])

    def test_reverted_ancestor_never_certifies_child(self):
        policy, tx, receipt, internal, extra = self.inputs()
        extra["call_trace"]["calls"][0] = {"error": "execution reverted", "calls": [extra["call_trace"]["calls"][0]]}
        result = verify_component(policy, tx, receipt, internal, **extra)
        self.assertFalse(result["checks"]["unique_successful_call_frame"])
        self.assertFalse(result["semantic_unit"]["certified"])

    def test_runtime_mismatch_not_canonical_by_address_alone(self):
        policy, tx, receipt, internal, extra = self.inputs()
        extra["historical_code"] = "0x6002600055"
        result = verify_component(policy, tx, receipt, internal, **extra)
        self.assertTrue(result["checks"]["historical_runtime_nonempty"])
        self.assertFalse(result["checks"]["runtime_matches_attested_source"])
        self.assertFalse(result["semantic_unit"]["certified"])

    def test_unbound_log_and_refund_cannot_pass(self):
        policy, tx, receipt, internal, extra = self.inputs()
        frame = extra["call_trace"]["calls"][0]
        frame["logs"] = []
        frame["calls"] = [{"type": "CALL", "from": WETH, "to": policy["credited_address"], "value": "0x1"}]
        result = verify_component(policy, tx, receipt, internal, **extra)
        self.assertFalse(result["checks"]["deposit_bound_to_call_frame"])
        self.assertFalse(result["checks"]["no_weth_child_value_or_refund"])

if __name__ == "__main__":
    unittest.main()
