"""Finite Sourcify v2 / indexed-call evidence adapter for canonical WETH9.

No network and no attestation by arbitrary URL/hash. Source service bytes must
match the separately acquired historical runtime. Only the observed Solidity
0.4 bzzr0 CBOR metadata substitution is accepted; executable bytes cannot differ.
The old twelve component predicates and real log-to-frame gate remain intact.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit, parse_qs

WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
SOURCE_TYPE = "SOURCIFY_V2_VERIFIED_SOURCE"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def source_request_matches(request, contract):
    parsed = urlsplit(request.get("url", ""))
    return (request.get("method") == "GET" and parsed.scheme == "https"
            and parsed.hostname == "sourcify.dev" and not parsed.username and not parsed.password
            and parsed.path.lower() == "/server/v2/contract/1/" + contract.lower()
            and parse_qs(parsed.query) == {"fields": ["all"]} and not parsed.fragment)


def _hex(value):
    if not isinstance(value, str) or not value.startswith("0x") or len(value) <= 2:
        raise ValueError("Nonempty exact bytecode required")
    try:
        return bytes.fromhex(value[2:])
    except ValueError as exc:
        raise ValueError("Malformed bytecode") from exc


def _bzzr0_metadata(value):
    # The fixed verified WETH9 compiler's complete CBOR map plus length trailer.
    return len(value) == 43 and value[:9] == bytes.fromhex("a165627a7a72305820") and value[-2:] == bytes.fromhex("0029")


def inspect_sourcify_source(response, historical_code, *, contract=WETH):
    if str(response.get("chainId")) != "1" or response.get("address", "").lower() != contract.lower() or contract.lower() != WETH:
        raise ValueError("Source chain/contract differs from fixed canonical WETH")
    if response.get("runtimeMatch") not in {"match", "exact_match"} or not response.get("matchId") or not response.get("verifiedAt"):
        raise ValueError("Successful verified runtime match identity required")
    if response.get("proxyResolution", {}).get("isProxy") is not False:
        raise ValueError("Proxy source cannot certify canonical direct deposit")
    compilation = response.get("compilation", {})
    if compilation.get("language") != "Solidity" or compilation.get("compiler") != "solc" or not compilation.get("compilerVersion") or compilation.get("name") != "WETH9":
        raise ValueError("Verified canonical WETH9 compilation metadata required")
    sources = response.get("sources", {})
    if set(sources) != {"WETH9.sol"}:
        raise ValueError("Only this fixed single-file canonical WETH9 source is supported")
    text = sources["WETH9.sol"].get("content")
    if not isinstance(text, str) or not text:
        raise ValueError("Verified source content missing")
    std_source = response.get("stdJsonInput", {}).get("sources", {}).get("WETH9.sol", {}).get("content")
    if std_source != text:
        raise ValueError("Source service compilation input differs from served source")
    # Comments and quoted literals cannot impersonate executable declarations.
    stripped = re.sub(
        r"/\*.*?\*/|//[^\n]*|\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'",
        lambda match: "" if match.group(0).startswith(("/*", "//")) else "__STRING_LITERAL__",
        text, flags=re.S)
    compact = re.sub(r"\s+", "", stripped)
    if compact.count("contractWETH9{") != 1 or len(re.findall(r"\bcontract\b", stripped)) != 1:
        raise ValueError("Canonical contract cannot inherit or delegate deposit semantics")
    if compact.count("functiondeposit()publicpayable{balanceOf[msg.sender]+=msg.value;Deposit(msg.sender,msg.value);}") != 1:
        raise ValueError("Deposit body must exactly credit msg.value and emit Deposit without fee/refund")
    if compact.count("function()publicpayable{deposit();}") != 1:
        raise ValueError("Canonical payable fallback must directly invoke deposit")
    runtime = response.get("runtimeBytecode", {})
    original = _hex(runtime.get("recompiledBytecode"))
    compiled_object = response.get("stdJsonOutput", {}).get("contracts", {}).get("WETH9.sol", {}).get("WETH9", {}).get("evm", {}).get("deployedBytecode", {}).get("object")
    if not isinstance(compiled_object, str) or _hex("0x" + compiled_object.removeprefix("0x")) != original:
        raise ValueError("Compiler output does not match the recompiled runtime record")
    onchain = _hex(runtime.get("onchainBytecode"))
    history = _hex(historical_code)
    if history != onchain:
        raise ValueError("Verified-service onchain bytecode differs from actual historical RPC bytes")
    if runtime.get("linkReferences") not in ({}, None) or runtime.get("immutableReferences") not in ({}, None):
        raise ValueError("No library or immutable runtime substitution is authorized")
    transformed = bytearray(original)
    transformations = runtime.get("transformations") or []
    if len(transformations) > 1:
        raise ValueError("Only one terminal CBOR metadata transformation is supported")
    metadata_records = runtime.get("cborAuxdata") or {}
    used = []
    for item in transformations:
        if item.get("type") != "replace" or item.get("reason") != "cborAuxdata":
            raise ValueError("Executable bytecode transformation forbidden")
        identity = str(item.get("id"))
        metadata = metadata_records.get(identity, {})
        before = _hex(metadata.get("value"))
        after = _hex(runtime.get("transformationValues", {}).get("cborAuxdata", {}).get(identity))
        offset = item.get("offset")
        if not isinstance(offset, int) or metadata.get("offset") != offset or offset + len(before) != len(original):
            raise ValueError("Metadata transformation must be at the exact bytecode tail")
        if not _bzzr0_metadata(before) or not _bzzr0_metadata(after) or original[offset:] != before:
            raise ValueError("Unsupported, noncanonical or mismatched CBOR metadata")
        transformed[offset:] = after
        used.append({"offset": offset, "bytes": len(before), "reason": "SOLIDITY_0_4_BZZR0_METADATA_ONLY"})
    if bytes(transformed) != onchain:
        raise ValueError("Recompiled executable bytecode does not reproduce observed runtime")
    executable_size = used[0]["offset"] if used else len(original)
    return {"source_validated": True, "chain_id": 1, "contract": contract.lower(),
        "source_text_sha256": digest(text.encode()), "runtime_code_sha256": digest(history),
        "source_type": SOURCE_TYPE, "source_validation_method": "SOURCIFY_V2_VERIFIED_COMPILATION_PLUS_EXACT_LOCAL_BYTECODE_COMPARISON",
        "semantics": "CANONICAL_WETH9_DEPOSIT_NO_FEE_NO_REFUND",
        "compiler_version": compilation["compilerVersion"], "match_id": response["matchId"],
        "verified_at": response["verifiedAt"], "runtime_match_label": response["runtimeMatch"],
        "runtime_bytes": len(history), "executable_prefix_bytes": executable_size,
        "permitted_metadata_transformations": used, "local_compiler_execution_claimed": False,
        "source_service_origin": "https://sourcify.dev", "external_acceptance_status": "PENDING_REVIEW"}


def validate_sourcify_records(records, review, selected_source_id):
    source, history = records["source"], records["historical_code"]
    if source.get("source_type") != SOURCE_TYPE or source.get("origin_url") != "https://sourcify.dev" or source.get("chain_id") != 1:
        raise ValueError("Sourcify source must retain its genuine provider/type identity")
    if not source_request_matches(source.get("request", {}), WETH):
        raise ValueError("Saved source request does not address fixed mainnet WETH")
    inspection = inspect_sourcify_source(source["payload"], history["payload"])
    if review.get("source_record_id") != selected_source_id or not review.get("review_id"):
        raise ValueError("Separate pinned source review record is required")
    for key in ("source_text_sha256", "runtime_code_sha256", "semantics", "chain_id", "contract"):
        if review.get(key) != inspection[key]:
            raise ValueError("Pinned source semantic review differs from inspected source/runtime")
    return {**review, **inspection,
            "deployment_runtime_evidence_basis": "Verified service supplies onchain and recompiled bytes; separate historical RPC bytes match exactly. No invented eth_getCode deployment request."}


def indexed_tree(rows, policy):
    """Preserve actual index provenance; deliberately supply no fabricated logs."""
    if not rows:
        raise ValueError("Indexed call rows unavailable")
    paths = {}
    for row in rows:
        path = tuple(row["trace_address"])
        if path in paths or any(not isinstance(i, int) or i < 0 for i in path):
            raise ValueError("Duplicate/malformed indexed call path")
        if row.get("tx_hash", "").lower() != policy["tx_hash"].lower() or row.get("block_number") != policy["block_number"]:
            raise ValueError("Indexed trace transaction/block conflict")
        paths[path] = row
    if () not in paths:
        raise ValueError("Complete indexed tree root missing")
    frames = {}
    for path in sorted(paths, key=lambda p: (len(p), p)):
        row = paths[path]
        children = sorted(child[-1] for child in paths if len(child) == len(path) + 1 and child[:-1] == path)
        if row.get("sub_traces") != len(children) or children != list(range(len(children))):
            raise ValueError("Indexed child coverage is incomplete")
        if path and path[:-1] not in paths:
            raise ValueError("Indexed ancestor is missing")
        if row.get("success") is not True or row.get("tx_success") is not True or row.get("error"):
            raise ValueError("Failed/unknown indexed execution cannot certify successful subtree")
        frame = {"from": row["from_address"], "to": row["to_address"], "type": row["call_type"].upper(),
            "value": hex(int(row["value_raw"])), "input": row["input_data"], "calls": [],
            "blockHash": row["block_hash"], "blockNumber": row["block_number"],
            "transactionHash": row["tx_hash"], "evidence_provider": "DUNE_INDEXED_CALL_TREE", "logs": []}
        # output_data None is intentionally left absent, not changed into 0x.
        if row.get("output_data") is not None:
            frame["output"] = row["output_data"]
        frames[path] = frame
        if path:
            frames[path[:-1]]["calls"].append(frame)
    return frames[()]


def replay_weth_manifest(input_root, output):
    from weth_evidence import payload_hash, load_evidence_context
    from weth_component import verify_component
    input_root, output = Path(input_root).resolve(), Path(output).resolve()
    manifest = read(input_root / "INPUT_MANIFEST.json")
    if manifest.get("schema_version") != "stage1b-r3-weth-replay-input-v1":
        raise ValueError("Unsupported WETH replay manifest")
    payloads, checked = {}, []
    for role, item in manifest["files"].items():
        file = (input_root / item["path"]).resolve()
        if not file.is_relative_to(input_root):
            raise ValueError("WETH replay path escaped input root")
        data = file.read_bytes()
        if digest(data) != item["sha256"] or len(data) != item["bytes"]:
            raise ValueError("WETH input bytes differ from acquisition manifest: " + role)
        payloads[role] = json.loads(data)
        checked.append({"role": role, **item})
    output.mkdir(parents=True, exist_ok=True)
    policy = payloads["policy"]["weth_component"]
    block_envelope = payloads["block"]
    block_request, block_response = block_envelope.get("request", {}), block_envelope.get("response", {})
    block = block_response.get("result", {})
    if (block_envelope.get("status") != "SUCCESS_VALIDATED" or block_response.get("id") != block_request.get("id")
            or block_request.get("method") != "eth_getBlockByNumber"
            or block_request.get("params") != [hex(policy["block_number"]), False]
            or int(block.get("number", "0x0"), 16) != policy["block_number"]):
        raise ValueError("Fixed block identity request/response mismatch")
    for role in ("transaction", "receipt"):
        if payloads[role]["response"]["result"].get("blockHash") != block.get("hash"):
            raise ValueError("New transaction/receipt and block identity disagree")
    records, selected = {}, {}
    for role in ("transaction", "receipt", "historical_code"):
        envelope = payloads[role]
        if envelope.get("status") != "SUCCESS_VALIDATED" or envelope.get("evidence_kind") != "REAL_CHAIN":
            raise ValueError("Successful real RPC envelope required")
        copy_path = output / "acquisition" / (role + ".json")
        write(copy_path, envelope)
        record_id = envelope["request"]["id"]
        selected[role] = record_id
        records[record_id] = {"role": role, "evidence_kind": "REAL_CHAIN", "status": "SUCCESS_VALIDATED",
            "chain_id": 1, "origin_url": "https://eth-mainnet.g.alchemy.com",
            "artifact_path": "acquisition/" + role + ".json", "artifact_sha256": digest(copy_path.read_bytes()),
            "request": envelope["request"], "payload_sha256": payload_hash(envelope["response"]["result"])}
    source_response = payloads["source_response"]
    source_receipt, source_request = payloads["source_receipt"], payloads["source_request"]
    source_file = manifest["files"]["source_response"]
    if source_receipt.get("http_status") != 200 or source_receipt.get("complete") is not True or source_receipt.get("sha256") != source_file["sha256"] or not source_request_matches(source_request, policy["contract"]):
        raise ValueError("Actual source-service acquisition receipt/request mismatch")
    source_path = output / "acquisition" / "source.json"
    write(source_path, {"request": source_request, "response": source_response, "http_status": 200,
                        "original_source_response_sha256": source_file["sha256"]})
    source_id = "sourcify-v2-match:" + str(source_response["matchId"])
    selected["source"] = source_id
    records[source_id] = {"role": "source", "evidence_kind": "REAL_CHAIN", "status": "SUCCESS_VALIDATED",
        "chain_id": 1, "origin_url": "https://sourcify.dev", "source_type": SOURCE_TYPE,
        "artifact_path": "acquisition/source.json", "artifact_sha256": digest(source_path.read_bytes()),
        "request": source_request, "payload_sha256": payload_hash(source_response)}
    review = inspect_sourcify_source(source_response, payloads["historical_code"]["response"]["result"])
    review.update({"review_id": "R3_LOCAL_CANONICAL_SOURCE_BODY_AND_RUNTIME_REVIEW",
                   "source_record_id": source_id, "review_is_external_acceptance": False})
    catalogue = {"schema_version": "weth-acquisition-catalogue-1", "evidence_kind": "REAL_CHAIN",
        "acquisition_run_id": manifest["acquisition_run_id"],
        "reviewer_record_id": "R3_LOCAL_PINNED_EVIDENCE_INSPECTION_PENDING_EXTERNAL_REVIEW",
        "approved_provider_hosts": ["eth-mainnet.g.alchemy.com", "sourcify.dev"],
        "records": records, "source_review": review}
    write(output / "ACQUISITION_CATALOGUE.json", catalogue)
    write(output / "EVIDENCE_BUNDLE.json", {"records": selected})
    context = load_evidence_context(output / "EVIDENCE_BUNDLE.json", output / "ACQUISITION_CATALOGUE.json")
    internal_excerpt, internal_original = payloads["internal_excerpt"], payloads["internal_original"]
    if internal_excerpt.get("response") != internal_original or internal_excerpt["evidence"]["sha256"] != manifest["files"]["internal_original"]["sha256"]:
        raise ValueError("Legacy internal excerpt must match the unchanged original bytes")
    analysis = payloads["r2_trace_analysis"]
    for source in analysis["sources"]:
        if not any(item["sha256"] == source["sha256"] for item in manifest["files"].values()):
            raise ValueError("R2 indexed context raw-source closure missing")
    page = payloads["r2_trace_raw_results"]
    rows = page.get("result", {}).get("rows")
    metadata = page.get("result", {}).get("metadata", {})
    if (rows != payloads["r2_trace_rows"] or not analysis["page_contract"]["complete"]
            or page.get("execution_id") != analysis["execution_id"]
            or page.get("state") != "QUERY_STATE_COMPLETED" or page.get("next_offset") is not None
            or metadata.get("total_row_count") != len(rows)):
        raise ValueError("Indexed call rows differ from complete archived result page")
    tree = indexed_tree(rows, policy)
    legacy_policy = copy.deepcopy(policy)
    legacy_policy["input_trace_address"] = analysis["legacy_indexed_row_ordinal"]
    result = verify_component(legacy_policy, context.records["transaction"]["payload"],
        context.records["receipt"]["payload"], internal_original, call_trace=tree,
        historical_code=context.records["historical_code"]["payload"], evidence_context=context)
    expected_path = policy["input_trace_address"]
    if result["native_locator"]["actual_call_tree_path"] != expected_path:
        raise ValueError("Observed call-tree path differs from frozen R3 component")
    blocked = payloads["trace_attempt"]
    if blocked.get("response", {}).get("error") is None:
        raise ValueError("This finite replay expects the saved unavailable tracer receipt, not an invented successful trace")
    result.update({"schema_version": "stage1b-r3-weth-result-v1", "checks_passed": sum(result["checks"].values()),
        "checks_total": len(result["checks"]), "source_review": context.source_review,
        "index_adapter_provenance": "DUNE_INDEXED_CALL_TREE_WITHOUT_FRAME_LOGS; not a successful debug_traceTransaction response",
        "source_request_bound_internal_available": False,
        "legacy_internal_binding": "Exact excerpt-to-original-byte binding retained; absent original request envelope is not manufactured",
        "trace_rpc_attempt": {"status": blocked.get("status"), "error": blocked["response"]["error"], "retried": False},
        "fixed_block_identity": {"number": policy["block_number"], "hash": block["hash"], "request_bound": True},
        "real_conversion_enabled": result["real_component_certified"],
        "external_acceptance_status": "PENDING_REVIEW", "input_files": checked})
    write(output / "WETH_COMPONENT_RESULT.json", result)
    write(output / "SOURCE_RUNTIME_REVIEW.json", context.source_review)
    write(output / "INDEXED_CALL_TREE_WITHOUT_LOGS.json", tree)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = replay_weth_manifest(args.input_root, args.output)
    print(json.dumps({key: result[key] for key in ("status", "checks_passed", "checks_total", "real_component_certified", "gaps")}, indent=2))


if __name__ == "__main__":
    main()
