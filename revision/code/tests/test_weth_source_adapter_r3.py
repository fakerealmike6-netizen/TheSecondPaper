"""Synthetic cross-provider adapter controls, never real acquisition claims."""
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from weth_source_adapter_r3 import (WETH, SOURCE_TYPE, inspect_sourcify_source,
    source_request_matches, validate_sourcify_records, indexed_tree)
from weth_evidence import is_verified_context


def source_fixture(metadata=False):
    text = "pragma solidity ^0.4.18; contract WETH9 { function() public payable { deposit(); } function deposit() public payable { balanceOf[msg.sender] += msg.value; Deposit(msg.sender, msg.value); } }"
    prefix = bytes.fromhex("600160005500")
    first = bytes.fromhex("a165627a7a72305820" + "11" * 32 + "0029")
    second = bytes.fromhex("a165627a7a72305820" + "22" * 32 + "0029")
    recompiled = prefix + first if metadata else prefix
    onchain = prefix + second if metadata else prefix
    runtime = {"onchainBytecode": "0x" + onchain.hex(), "recompiledBytecode": "0x" + recompiled.hex(),
               "linkReferences": {}, "immutableReferences": None, "transformations": []}
    if metadata:
        runtime.update({"cborAuxdata": {"1": {"value": "0x" + first.hex(), "offset": len(prefix)}},
            "transformations": [{"id": "1", "type": "replace", "reason": "cborAuxdata", "offset": len(prefix)}],
            "transformationValues": {"cborAuxdata": {"1": "0x" + second.hex()}}})
    payload = {"chainId": "1", "address": WETH, "runtimeMatch": "match", "matchId": "SYNTHETIC_ADAPTER_CONTROL",
        "verifiedAt": "2024-01-01T00:00:00Z", "proxyResolution": {"isProxy": False},
        "compilation": {"language": "Solidity", "compiler": "solc", "compilerVersion": "0.4.19+controlled", "name": "WETH9"},
        "sources": {"WETH9.sol": {"content": text}}, "stdJsonInput": {"sources": {"WETH9.sol": {"content": text}}},
        "stdJsonOutput": {"contracts": {"WETH9.sol": {"WETH9": {"evm": {"deployedBytecode": {"object": recompiled.hex()}}}}}},
        "runtimeBytecode": runtime}
    return payload, "0x" + onchain.hex()


def indexed_fixture():
    policy = {"tx_hash": "0x" + "a" * 64, "block_number": 123}
    common = {"tx_hash": policy["tx_hash"], "block_number": 123, "block_hash": "0x" + "b" * 64,
        "success": True, "tx_success": True, "error": None, "input_data": "0xd0e30db0", "output_data": None,
        "from_address": "0x" + "1" * 40, "to_address": WETH, "call_type": "call", "value_raw": "10"}
    rows = [{**common, "trace_address": [], "sub_traces": 1},
            {**common, "trace_address": [0], "sub_traces": 0}]
    return policy, rows


class WethSourceAdapterR3Tests(unittest.TestCase):
    def test_exact_bytecode_source_shape_positive(self):
        source, history = source_fixture()
        result = inspect_sourcify_source(source, history)
        self.assertTrue(result["source_validated"])
        self.assertFalse(result["local_compiler_execution_claimed"])
        self.assertFalse(is_verified_context(result))

    def test_only_metadata_change_reconstructs_exact_bytes(self):
        source, history = source_fixture(True)
        result = inspect_sourcify_source(source, history)
        self.assertEqual(result["permitted_metadata_transformations"],
            [{"offset": 6, "bytes": 43, "reason": "SOLIDITY_0_4_BZZR0_METADATA_ONLY"}])

    def test_historical_runtime_conflict_rejected(self):
        source, history = source_fixture()
        with self.assertRaisesRegex(ValueError, "historical RPC"):
            inspect_sourcify_source(source, history + "00")

    def test_executable_substitution_cannot_hide_in_transform(self):
        source, history = source_fixture(True)
        source["runtimeBytecode"]["transformations"][0]["reason"] = "library"
        with self.assertRaisesRegex(ValueError, "Executable"):
            inspect_sourcify_source(source, history)

    def test_metadata_must_be_terminal_and_exact(self):
        source, history = source_fixture(True)
        source["runtimeBytecode"]["transformations"][0]["offset"] = 0
        with self.assertRaisesRegex(ValueError, "exact bytecode tail"):
            inspect_sourcify_source(source, history)

    def test_arbitrary_bytes_are_not_cbor_metadata(self):
        source, history = source_fixture(True)
        source["runtimeBytecode"]["transformationValues"]["cborAuxdata"]["1"] = "0x" + "00" * 43
        with self.assertRaisesRegex(ValueError, "CBOR"):
            inspect_sourcify_source(source, history)

    def test_source_payload_and_compiler_input_must_agree(self):
        source, history = source_fixture()
        source["sources"]["WETH9.sol"]["content"] += " changed"
        with self.assertRaisesRegex(ValueError, "compilation input"):
            inspect_sourcify_source(source, history)

    def test_compiler_output_must_match_runtime_record(self):
        source, history = source_fixture()
        source["stdJsonOutput"]["contracts"]["WETH9.sol"]["WETH9"]["evm"]["deployedBytecode"]["object"] = "6002"
        with self.assertRaisesRegex(ValueError, "Compiler output"):
            inspect_sourcify_source(source, history)

    def test_deposit_fee_mutation_rejected(self):
        source, history = source_fixture()
        changed = source["sources"]["WETH9.sol"]["content"].replace("+= msg.value", "+= msg.value - 1")
        source["sources"]["WETH9.sol"]["content"] = changed
        source["stdJsonInput"]["sources"]["WETH9.sol"]["content"] = changed
        with self.assertRaisesRegex(ValueError, "without fee/refund"):
            inspect_sourcify_source(source, history)

    def test_quoted_canonical_body_cannot_impersonate_deposit(self):
        source, history = source_fixture()
        original = source["sources"]["WETH9.sol"]["content"]
        changed = original.replace("+= msg.value", "+= msg.value - 1")
        changed += ' string bait = "function deposit() public payable { balanceOf[msg.sender] += msg.value; Deposit(msg.sender,msg.value); }";'
        source["sources"]["WETH9.sol"]["content"] = changed
        source["stdJsonInput"]["sources"]["WETH9.sol"]["content"] = changed
        with self.assertRaisesRegex(ValueError, "without fee/refund"):
            inspect_sourcify_source(source, history)

    def test_fallback_refund_mutation_rejected(self):
        source, history = source_fixture()
        changed = source["sources"]["WETH9.sol"]["content"].replace("deposit();", "msg.sender.transfer(msg.value);")
        source["sources"]["WETH9.sol"]["content"] = changed
        source["stdJsonInput"]["sources"]["WETH9.sol"]["content"] = changed
        with self.assertRaisesRegex(ValueError, "fallback"):
            inspect_sourcify_source(source, history)

    def test_proxy_and_wrong_chain_rejected(self):
        source, history = source_fixture()
        source["proxyResolution"]["isProxy"] = True
        with self.assertRaisesRegex(ValueError, "Proxy"):
            inspect_sourcify_source(source, history)
        source, history = source_fixture()
        source["chainId"] = "10"
        with self.assertRaisesRegex(ValueError, "chain/contract"):
            inspect_sourcify_source(source, history)

    def test_plain_url_or_spoofed_origin_not_source_proof(self):
        request = {"method": "GET", "url": "https://sourcify.dev/server/v2/contract/1/" + WETH + "?fields=all"}
        self.assertTrue(source_request_matches(request, WETH))
        for bad in (request["url"].replace("sourcify.dev", "sourcify.dev.invalid"),
                    request["url"].replace("https://", "https://" + "synthetic_user" + ":" + "synthetic_password" + "@"),
                    request["url"].replace("/1/", "/10/")):
            self.assertFalse(source_request_matches({"method": "GET", "url": bad}, WETH))

    def test_separate_pinned_review_must_match_source_bytes(self):
        source, history = source_fixture()
        review = inspect_sourcify_source(source, history)
        review.update({"review_id": "SYNTHETIC_CONTROL", "source_record_id": "source1"})
        records = {"source": {"source_type": SOURCE_TYPE, "origin_url": "https://sourcify.dev", "chain_id": 1,
            "request": {"method": "GET", "url": "https://sourcify.dev/server/v2/contract/1/" + WETH + "?fields=all"}, "payload": source},
            "historical_code": {"payload": history}}
        self.assertTrue(validate_sourcify_records(records, review, "source1")["source_validated"])
        review["source_text_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "Pinned"):
            validate_sourcify_records(records, review, "source1")

    def test_indexed_tree_never_fabricates_provider_logs_or_empty_output(self):
        policy, rows = indexed_fixture()
        tree = indexed_tree(rows, policy)
        self.assertEqual(tree["logs"], [])
        self.assertNotIn("output", tree)
        self.assertEqual(tree["evidence_provider"], "DUNE_INDEXED_CALL_TREE")
        self.assertEqual(tree["calls"][0]["calls"], [])

    def test_missing_indexed_child_closure_rejected(self):
        policy, rows = indexed_fixture()
        with self.assertRaisesRegex(ValueError, "coverage"):
            indexed_tree(rows[:1], policy)

    def test_failed_indexed_ancestor_rejected(self):
        policy, rows = indexed_fixture()
        rows[0]["success"] = False
        with self.assertRaisesRegex(ValueError, "Failed/unknown"):
            indexed_tree(rows, policy)

    def test_indexed_trace_identity_conflict_rejected(self):
        policy, rows = indexed_fixture()
        rows[1]["tx_hash"] = "0x" + "c" * 64
        with self.assertRaisesRegex(ValueError, "transaction/block"):
            indexed_tree(rows, policy)

    def test_source_inspection_dict_cannot_self_authorize_real_component(self):
        import test_weth
        from weth_component import verify_component
        policy, tx, receipt, internal, extra = test_weth.WethTests().inputs()
        source, history = source_fixture()
        extra["evidence_context"] = inspect_sourcify_source(source, history)
        result = verify_component(policy, tx, receipt, internal, **extra)
        self.assertFalse(result["real_component_certified"])
        self.assertFalse(result["checks"]["runtime_matches_attested_source"])
        self.assertEqual(len(result["checks"]), 12)


if __name__ == "__main__":
    unittest.main()
