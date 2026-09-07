"""Evidence checks for one local WETH deposit, never the surrounding bridge.

No network. Historical bytes and optional source/runtime attestation must be
provided as inputs. A legacy explorer row ordinal is not a call-trace address.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
from weth_evidence import audit_bindings, is_verified_context, load_evidence_context

DEPOSIT_TOPIC = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

def number(value):
    return int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)

def unwrap(value):
    return value.get("result", value) if isinstance(value, dict) else value

def code_hash(code):
    if not isinstance(code, str) or not code.startswith("0x") or len(code) <= 2:
        return None
    return hashlib.sha256(bytes.fromhex(code[2:])).hexdigest()

def frames(tree, path=()):
    yield path, tree
    for i, child in enumerate(tree.get("calls", [])):
        yield from frames(child, path + (i,))

def verify_component(policy, tx, receipt, internal_response, *, call_trace=None, historical_code=None, source_attestation=None, evidence_context=None):
    binding = audit_bindings(policy, tx, receipt, internal_response, call_trace,
                             historical_code, source_attestation, evidence_context)
    tx, receipt = unwrap(tx), unwrap(receipt)
    internal_response = internal_response.get("response", internal_response)
    tx_hash, contract = policy["tx_hash"].lower(), policy["contract"].lower()
    credited, amount, block = policy["credited_address"].lower(), int(policy["amount_raw"]), policy["block_number"]
    checks, gaps = {}, []
    checks["transaction_receipt_identity"] = tx.get("hash", "").lower() == receipt.get("transactionHash", "").lower() == tx_hash and number(tx["blockNumber"]) == number(receipt["blockNumber"]) == block
    checks["transaction_success"] = number(receipt.get("status", "0x0")) == 1
    checks["ethereum_mainnet"] = number(tx.get("chainId", "0x0")) == 1
    candidates = [r for r in internal_response.get("result", []) if r.get("from", "").lower() == credited and r.get("to", "").lower() == contract and number(r.get("value", "0")) == amount and r.get("isError") == "0" and number(r["blockNumber"]) == block]
    checks["unique_successful_indexed_native_input"] = len(candidates) == 1
    target_logs = [log for log in receipt.get("logs", []) if number(log["logIndex"]) == policy["deposit_log_index"]]
    deposit = target_logs[0] if len(target_logs) == 1 else None
    checks["exact_deposit_log"] = bool(deposit and deposit.get("address", "").lower() == contract and not deposit.get("removed") and deposit.get("transactionHash", "").lower() == tx_hash and len(deposit.get("topics", [])) == 2 and deposit["topics"][0].lower() == DEPOSIT_TOPIC and ("0x" + deposit["topics"][1][-40:]).lower() == credited and number(deposit["data"]) == amount)
    indexed_position = internal_response.get("result", []).index(candidates[0]) if len(candidates) == 1 else None
    # A row-only archived Etherscan response is not an execution-path assertion.
    native_locator = {"legacy_event_id": f"eip155:1:tx:{tx_hash}:trace:{policy['input_trace_address']}", "indexed_response_row_zero_based": indexed_position, "legacy_trace_locator_is_row_ordinal": bool(candidates and "traceId" not in candidates[0] and "trace_address" not in candidates[0]), "actual_call_tree_path": None}
    matched_frame, frame_path = None, None
    if call_trace:
        tree = unwrap(call_trace)
        all_frames = list(frames(tree))
        failed_paths = [path for path, frame in all_frames if frame.get("error")]
        matches = [(path, frame) for path, frame in all_frames if frame.get("from", "").lower() == credited and frame.get("to", "").lower() == contract and number(frame.get("value", "0x0")) == amount and not any(path[:len(failed)] == failed for failed in failed_paths) and frame.get("type", "").upper() == "CALL"]
        checks["unique_successful_call_frame"] = len(matches) == 1 and not tree.get("error")
        if checks["unique_successful_call_frame"]:
            frame_path, matched_frame = matches[0]
            native_locator["actual_call_tree_path"] = list(frame_path)
            checks["supported_deposit_entrypoint"] = matched_frame.get("input", "").lower() in ("0x", "0xd0e30db0")
            frame_logs = [log for log in matched_frame.get("logs", []) if log.get("address", "").lower() == contract and log.get("topics") == deposit.get("topics") and log.get("data", "").lower() == deposit.get("data", "").lower()] if deposit else []
            unique_locator = len(frame_logs) == 1
            if unique_locator and not any(frame_logs[0].get(k) is not None for k in ('logIndex', 'log_index')):
                # Some callTracer contracts omit global log indices. A unique
                # request-bound log pattern is sufficient; repeated identical
                # receipt logs cannot be assigned to an exact event by guessing.
                same_receipt_logs = [log for log in receipt.get('logs', [])
                                     if log.get('address', '').lower() == contract
                                     and log.get('topics') == deposit.get('topics')
                                     and log.get('data', '').lower() == deposit.get('data', '').lower()]
                unique_locator = len(same_receipt_logs) == 1
            checks["deposit_bound_to_call_frame"] = unique_locator and binding['trace_bound']
            equivalent = binding.get('equivalent_deposit_binding')
            if equivalent:
                # This sealed proof validates the complete execution-context
                # tree and receipt. It never inserts synthetic frame.logs.
                checks["deposit_bound_to_call_frame"] = bool(
                    list(frame_path) == equivalent['actual_call_tree_path']
                    and checks['exact_deposit_log'] and binding['trace_bound'])
            checks["no_weth_child_value_or_refund"] = not any(number(frame.get("value", "0x0")) > 0 for path, frame in frames(matched_frame) if path)
        else:
            checks["supported_deposit_entrypoint"] = False
            checks["deposit_bound_to_call_frame"] = False
            checks["no_weth_child_value_or_refund"] = False
    else:
        checks["unique_successful_call_frame"] = False
        checks["supported_deposit_entrypoint"] = False
        checks["deposit_bound_to_call_frame"] = False
        checks["no_weth_child_value_or_refund"] = False
        gaps.append("CALL_CONTEXT_NOT_AVAILABLE")
    runtime = unwrap(historical_code) if historical_code else None
    runtime_sha = code_hash(runtime) if isinstance(runtime, str) else None
    checks["historical_runtime_nonempty"] = runtime_sha is not None
    attestation = source_attestation or {}
    reference_runtime_sha = code_hash(attestation.get("reference_runtime_code"))
    checks["runtime_matches_attested_source"] = bool(runtime_sha and reference_runtime_sha == runtime_sha and attestation.get("source_sha256") and attestation.get("source_url") and attestation.get("chain_id") == 1 and attestation.get("contract", "").lower() == contract and attestation.get("deposit_semantics") == "balanceOf[msg.sender] += msg.value; Deposit(msg.sender,msg.value)")
    # The legacy attestation remains useful for synthetic shape checks only.
    # Real source matching requires pinned acquisition and verified source data.
    if binding['evidence_kind'] != 'SYNTHETIC':
        checks['runtime_matches_attested_source'] = binding['source_verified']
    checks["canonical_address"] = contract == WETH
    for key, passed in checks.items():
        if not passed:
            gaps.append(key.upper() + "_UNVERIFIED")
    base_valid = all(checks[k] for k in ("transaction_receipt_identity", "transaction_success", "ethereum_mainnet", "unique_successful_indexed_native_input", "exact_deposit_log", "canonical_address"))
    semantic_checks_passed = base_valid and all(checks.values()) and not binding['conflicts']
    synthetic_verified = semantic_checks_passed and binding['evidence_kind'] == 'SYNTHETIC'
    certified = semantic_checks_passed and binding['real_provenance_ready']
    status = ("COMPONENT_EVIDENCE_INCONSISTENT" if binding['conflicts'] or not base_valid else
              "CERTIFIED_LOCAL_WETH_DEPOSIT" if certified else
              "SYNTHETIC_COMPONENT_VERIFIED_NOT_REAL" if synthetic_verified else
              "OBSERVED_LOCAL_DEPOSIT_PATTERN_WITH_GAPS")
    gaps.extend(binding['gaps'])
    gaps.extend('IDENTITY_CONFLICT:' + item for item in binding['conflicts'])
    gas = number(receipt["gasUsed"]) * number(receipt["effectiveGasPrice"])
    unit = {"unit_id": f"weth_deposit:{tx_hash}:{policy['deposit_log_index']}", "scope": "isolated deposit suboperation", "certified": certified, "input_port": {"address": credited, "asset": "native:eip155:1", "amount_raw": str(amount), "event_locator": native_locator}, "output_port": {"address": credited, "asset": "erc20:eip155:1:" + contract, "amount_raw": str(amount), "evidence_kind": "Deposit balance credit, not ERC20 Transfer", "event_id": f"eip155:1:tx:{tx_hash}:log:{policy['deposit_log_index']}"}, "refund_port": {"asset": "native:eip155:1", "amount_raw": "0" if checks["no_weth_child_value_or_refund"] else None, "status": "verified no child value refund" if checks["no_weth_child_value_or_refund"] else "not certified"}, "component_fee_port": {"amount_raw": "0" if certified else None, "basis": "verified source has no deposit fee" if certified else "pending source/runtime match"}, "transaction_gas_context": {"payer": tx["from"].lower(), "asset": "native:eip155:1", "gas_used": str(number(receipt["gasUsed"])), "effective_gas_price_raw": str(number(receipt["effectiveGasPrice"])), "total_fee_raw": str(gas), "outside_component_exchange_ratio": True, "source_share": "not determined by this component"}, "lp_constraints": [f"0 <= x_native_input <= {amount}", "x_weth_credit = x_native_input", "x_refund = 0 only after no-refund closure"], "next_state": {"address": credited, "asset": "erc20:eip155:1:" + contract, "arrival": f"deposit-log:{policy['deposit_log_index']}", "enabled_for_real_collection": certified}, "surrounding_transaction_certified": False, "bridge_or_cross_chain_certified": False, "inserted_into_reference_graph": False}
    return {"status": status, "checks": checks, "gaps": sorted(set(gaps)), "runtime_code_sha256": runtime_sha, "source_sha256": evidence_context.source_review.get('source_text_sha256') if is_verified_context(evidence_context) else attestation.get("source_sha256"), "native_locator": native_locator, "semantic_unit": unit, "evidence_bindings": binding, "synthetic_component_verified": synthetic_verified, "real_component_certified": certified, "boundary_comparison": {"model_rule": "1:1 source amount conversion on isolated deposit", "input_source_range_raw": ["0", str(amount)], "output_source_range_raw": ["0", str(amount)], "joint_relation": "x_native_input = x_weth_credit; these ports do not represent two independent source injections", "proof_scope": "single input / single output only; not arbitrary multioutput DSU equivalence", "real_component_use_permitted": certified}}

def verify_archived_sources(project_root, evidence_dir):
    root, evidence_dir = Path(project_root).resolve(), Path(evidence_dir)
    results, payloads = [], {}
    for kind, filename in (("transaction", "WETH_COMPONENT_TRANSACTION_EXCERPT.json"), ("receipt", "WETH_COMPONENT_RECEIPT_EXCERPT.json"), ("internal", "WETH_COMPONENT_INTERNAL_RESPONSE.json")):
        path = evidence_dir / filename
        excerpt = json.loads(path.read_text(encoding="utf-8"))
        evidence = excerpt["evidence"]
        original = (root / evidence["path"]).resolve()
        if not original.is_relative_to(root):
            raise ValueError("evidence path outside project")
        digest = hashlib.sha256(original.read_bytes()).hexdigest()
        if digest != evidence["sha256"]:
            raise ValueError("archived source hash mismatch")
        raw = json.loads(original.read_text(encoding="utf-8"))
        payload = excerpt.get("result", excerpt.get("response"))
        if isinstance(raw, list):
            matched = [r.get("result") for r in raw if r.get("id") == evidence["rpc_id"]]
            if matched != [payload]:
                raise ValueError("excerpt differs from original RPC batch payload")
        elif raw != payload:
            raise ValueError("internal excerpt differs from original response")
        results.append({"kind": kind, "original_path": evidence["path"], "original_sha256": digest, "excerpt_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "payload_identical": True})
        payloads[kind] = payload
    return payloads, results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--call-trace")
    parser.add_argument("--historical-code")
    parser.add_argument("--source-attestation")
    parser.add_argument("--evidence-bundle")
    parser.add_argument("--acquisition-catalogue")
    args = parser.parse_args()
    def read(path):
        return json.loads(Path(path).read_text(encoding="utf-8")) if path else None
    payloads, sources = verify_archived_sources(args.project_root, args.evidence_dir)
    if bool(args.evidence_bundle) != bool(args.acquisition_catalogue):
        parser.error('evidence-bundle and acquisition-catalogue must be supplied together')
    context = load_evidence_context(args.evidence_bundle, args.acquisition_catalogue) if args.evidence_bundle else None
    if context:
        payloads.update({kind: context.records[kind]['payload'] for kind in ('transaction', 'receipt', 'internal') if kind in context.records})
    result = verify_component(read(args.policy)["weth_component"], payloads["transaction"], payloads["receipt"], payloads["internal"], call_trace=read(args.call_trace) if args.call_trace else context.records.get('trace', {}).get('payload') if context else None, historical_code=read(args.historical_code) if args.historical_code else context.records.get('historical_code', {}).get('payload') if context else None, source_attestation=read(args.source_attestation), evidence_context=context)
    result["source_manifest"] = sources
    Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"status": result["status"], "checks_passed": sum(result["checks"].values()), "checks_total": len(result["checks"]), "gaps": result["gaps"]}))

if __name__ == "__main__":
    main()
