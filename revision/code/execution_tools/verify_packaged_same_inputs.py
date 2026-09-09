"""Verify compact parent scientific projections against a self-contained R1 MIN.

Requires the unchanged compare_same_inputs.py alongside this script. Reads only
local package files; does not import or execute experimental algorithms.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import compare_same_inputs as comparison


SCHEMA = "stage1c-r1-packaged-same-input-verification-1.0"


def verify(tree):
    bundle_path = tree / "comparison_inputs/BASELINE_SCIENTIFIC_PROJECTIONS.json"
    bundle = comparison.read(bundle_path)
    issues, pairs, contracts, input_checks = [], [], [], []

    def check(condition, code, path, detail):
        if not condition:
            issues.append({"code": code, "path": path, "detail": str(detail)})
        return bool(condition)

    check(bundle.get("schema_version") == "stage1c-r1-parent-scientific-projections-1.0", "SCHEMA", str(bundle_path), "Expected frozen projection schema")
    bound_tool = bundle.get("projection_contract", {})
    check(bound_tool.get("version") == comparison.VERSION, "PROJECTION_VERSION", "projection_contract", "Projection version differs")
    check(bound_tool.get("source_sha256") == comparison.sha(Path(comparison.__file__)), "PROJECTION_SOURCE", "projection_contract", "Projection source bytes differ from the parent projection generator")
    check(comparison.self_test()["passed"], "COMPARATOR_SELF_TEST", "comparison", "Comparator unit controls must pass")
    provenance = bundle.get("provenance", {})
    parent_min = provenance.get("accepted_parent_min", {})
    check(parent_min.get("sha256") == "ecae2712435e2bf548fdf94c290fce8d834037900e4255adb252f8096269800e" and parent_min.get("bytes") == 5064825,
          "PARENT_MIN_IDENTITY", "provenance.accepted_parent_min", "Expected previously verified accepted Stage1C MIN identity")
    check(provenance.get("parent_commit") == "d88b839361b8fa93308430641baf7af423d76ae8", "PARENT_COMMIT_IDENTITY", "provenance.parent_commit", "Expected accepted Stage1C public source commit")
    frozen = comparison.read(tree / "EXPERIMENT_FREEZE.json")
    manifest = comparison.read(tree / "EXPERIMENT_INPUTS.json")
    check(comparison.sha(tree / "EXPERIMENT_FREEZE.json") == provenance.get("parent_experiment_freeze_sha256"), "PARENT_FREEZE", "EXPERIMENT_FREEZE.json", "Original experiment freeze must remain byte-identical")
    check(comparison.sha(tree / "EXPERIMENT_INPUTS.json") == frozen.get("inputs_manifest_sha256"), "INPUT_MANIFEST", "EXPERIMENT_INPUTS.json", "Frozen input manifest mismatch")
    private_path = comparison.within(tree, manifest["real_private_manifest"])
    check(comparison.sha(private_path) == frozen.get("private_inputs_manifest_sha256"), "REAL_MANIFEST", manifest["real_private_manifest"], "Frozen real manifest mismatch")
    rows = manifest["controlled"] + comparison.read(private_path)
    expected = {r["sample_id"]: r for r in rows}
    check(len(rows) == len(expected) == 62 and sum(r["kind"] == "controlled" for r in rows) == 60 and sum(r["kind"] == "real" for r in rows) == 2,
          "FROZEN_SAMPLE_DOMAIN", "EXPERIMENT_INPUTS.json", "Exactly the frozen 60 controlled + 2 real samples required")
    samples = bundle.get("samples", [])
    sample_ids = [r["sample_id"] for r in samples]
    check(len(sample_ids) == 62 and set(sample_ids) == set(expected), "PROJECTION_SAMPLE_DOMAIN", "samples", "Parent projection sample universe differs")
    for row in rows:
        for path_key, hash_key in (("observed_path", "observed_sha256"), ("hidden_path", "hidden_sha256")):
            if path_key in row:
                actual = comparison.sha(comparison.within(tree, row[path_key]))
                same = actual == row[hash_key]
                input_checks.append({"sample_id": row["sample_id"], "path": row[path_key], "sha256": actual, "passed": same})
                check(same, "FROZEN_INPUT_IDENTITY", row[path_key], "Packaged observed/hidden input identity changed")
    method_records = bundle.get("method_projections", [])
    auxiliary_records = bundle.get("auxiliary_projections", [])
    expected_method_keys = {(sid, method) for sid in expected for method in comparison.METHODS}
    received_method_keys = [(r["sample_id"], r["method"]) for r in method_records]
    check(len(received_method_keys) == 434 and set(received_method_keys) == expected_method_keys, "PROJECTION_METHOD_DOMAIN", "method_projections", "All 434 unique parent method projections required")
    expected_auxiliary_keys = {(sid, artifact) for sid in expected for artifact in ("EVALUATION", "ABLATIONS")}
    received_auxiliary_keys = [(r["sample_id"], r["artifact"]) for r in auxiliary_records]
    check(len(received_auxiliary_keys) == 124 and set(received_auxiliary_keys) == expected_auxiliary_keys, "PROJECTION_AUXILIARY_DOMAIN", "auxiliary_projections", "All 124 unique parent scientific auxiliary projections required")
    new_results = {}
    for sid, row in sorted(expected.items()):
        slug = sid.replace(":", "_").replace("/", "_")
        folder = tree / "results" / row["kind"] / "samples" / slug
        methods_path = folder / "METHOD_RESULTS.json"
        result = comparison.read(methods_path)
        new_results[sid] = (folder, result)
        check(set(result) == set(comparison.METHODS), "CURRENT_METHOD_DOMAIN", sid, "All seven methods required")
        contract = comparison.read(folder / "OUTPUT_CONTRACT.json")
        acceptance = comparison.read(folder / "QUERY_ACCEPTANCE.json")
        binding = acceptance.get("binding", {}).get("files", {})
        record = {"sample_id": sid,
                  "contract_passed": contract.get("passed") is True and contract.get("status") == "PASS",
                  "contract_result_binding": contract.get("bindings", {}).get("method_results_sha256") == comparison.object_hash(result),
                  "contract_source_binding": contract.get("bindings", {}).get("contract_module_sha256") == comparison.sha(tree / "src/stage1c_output_contract.py"),
                  "query_accepted": acceptance.get("passed") is True and acceptance.get("contract_passed") is True,
                  "query_file_bindings": all(binding.get(n) == comparison.sha(folder / n) for n in ("METHOD_RESULTS.json", "EVALUATION.json", "ABLATIONS.json", "OUTPUT_CONTRACT.json"))}
        record["passed"] = all(v for k, v in record.items() if k != "sample_id")
        contracts.append(record)
        check(record["passed"], "CURRENT_CONTRACT_BINDING", sid, record)
    for entry in [*method_records, *auxiliary_records]:
        sid = entry["sample_id"]
        row = expected.get(sid)
        if row is None:
            continue
        artifact = entry.get("artifact", "METHOD_RESULTS")
        method = entry.get("method", "*")
        folder, results = new_results[sid]
        raw = results.get(method, comparison.MISSING) if artifact == "METHOD_RESULTS" else comparison.read(folder / (artifact + ".json"))
        current = comparison.projection(raw, artifact=artifact)
        parent = entry["projection"]
        parent_projection_valid = comparison.object_hash(parent) == entry.get("projection_sha256")
        check(parent_projection_valid, "PARENT_PROJECTION_INTEGRITY", sid + "/" + method + "/" + artifact, "Stored parent projection hash mismatch")
        counters = {}
        delta = comparison.differences(parent, current, counters=counters)
        record = {"sample_id": sid, "kind": row["kind"], "method": method, "artifact": artifact,
                  "passed": parent_projection_valid and not delta,
                  "parent_projection_sha256": entry.get("projection_sha256"), "current_projection_sha256": comparison.object_hash(current),
                  "original_parent_raw_sha256": entry.get("original_file_sha256"), "original_parent_result_path": entry.get("baseline_result_path"),
                  "differences_count": len(delta), "field_differences": delta, **counters}
        pairs.append(record)
        check(not delta, "SCIENTIFIC_PROJECTION_DIFFERENCE", sid + "/" + method + "/" + artifact, delta)
    passed = not issues
    return {"schema_version": SCHEMA, "status": "PASS" if passed else "FAIL", "passed": passed,
            "sample_count": len(expected), "method_pairs_checked": sum(r["artifact"] == "METHOD_RESULTS" for r in pairs),
            "auxiliary_pairs_checked": sum(r["artifact"] != "METHOD_RESULTS" for r in pairs),
            "pairs_equal": sum(r["passed"] for r in pairs), "field_differences_count": sum(r["differences_count"] for r in pairs),
            "current_contracts_passed_and_bound": sum(r["passed"] for r in contracts),
            "frozen_input_files_checked": len(input_checks), "projection_bundle_sha256": comparison.sha(bundle_path),
            "comparison_source_sha256": comparison.sha(Path(comparison.__file__)), "verification_source_sha256": comparison.sha(Path(__file__)),
            "parent_provenance": provenance, "issues": issues, "pairs": pairs, "input_checks": input_checks, "contract_receipts": contracts,
            "authenticity_limit": "Parent raw file hashes and commit are provenance references from the already verified Stage1C MIN. The old MIN and raw parent results are intentionally not embedded; this offline check does not independently reacquire parent Git data or re-verify the original ZIP. The current package manifest protects the supplied compact projection bundle.",
            "witness_limit": "Nonunique LP witness/certificate recovery details are excluded from equality. Current saved contract receipts bind complete returned witnesses and were separately generated after full constraint/objective checks; this lightweight tool verifies the stored receipts and file bindings, without executing the contract or any solver.",
            "network_requests": 0, "experimental_algorithms_executed": False, "external_review_status": "PENDING_REVIEW"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tree", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists():
        print(json.dumps({"status": "ERROR", "error": "Output must be a new file; previous evidence is retained"}), file=sys.stderr)
        return 1
    try:
        result = verify(args.tree.resolve())
    except Exception as exc:
        result = {"schema_version": SCHEMA, "status": "ERROR", "passed": False,
                  "error": type(exc).__name__ + ": " + str(exc), "experimental_algorithms_executed": False}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k not in ("pairs", "input_checks", "contract_receipts", "issues")}, ensure_ascii=False, indent=2))
    return 0 if result.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
