"""Offline comparison of frozen Stage1C scientific returns, without executing code.

The comparison schema is fixed here before the R1 canonical batch completes.
Amounts, domains and deterministic baseline allocations are retained. Only new
execution identities, timings and non-unique LP witness/certificate details are
excluded. New witnesses are independently checked by each OUTPUT_CONTRACT.
"""
from __future__ import annotations

import argparse
import csv
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import sys


VERSION = "stage1c-r1-same-input-comparison-1.0"
METHODS = (
    "FULL_INTERVAL", "BOUNDED_REACHABILITY", "POISON", "HAIRCUT",
    "NO_CROSS_TARGET_COUPLING", "NO_PROTOCOL_CONTINUATION", "BALANCE_INFORMATION_REMOVED",
)
NEW_METHOD_ENVELOPE = {"query_id", "native_identity", "identity_errors"}
METHOD_TIMING = {"timing_parts", "elapsed_seconds", "profile"}
LP_DETAILS = {"certificate", "witness_event_source_raw", "approximate_raw"}
SET_FIELDS = {
    "positive_addresses", "output_address_ids", "output_event_ids", "objective_events",
    "events", "addresses", "removed_bound_variables", "unbound_identical_physical_variables",
    "unlinked_source_ratio_equalities", "boundary_source_variables",
    "physical_outputs_retained_as_source_zero", "bad_bounds", "bad_equalities",
}
SCIENTIFIC_SOURCES = (
    "src/lp_model.py", "src/context_lp_r3.py", "src/physical_facts.py",
    "src/stage1c_baselines.py", "src/stage1c_intervals.py", "src/stage1c_controlled.py",
    "src/stage1c_oracle.py", "src/stage1c_real_replay.py", "src/stage1c_catalog.py",
)
MISSING = {"$missing": True}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def object_hash(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def within(root, relative):
    if not isinstance(relative, str) or not relative or "\\" in relative:
        raise ValueError("Expected a nonempty POSIX relative path")
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()) or path == root.resolve():
        raise ValueError("Path escapes declared input tree: " + relative)
    return path


def pointer(path):
    return "/" + "/".join(str(p).replace("~", "~0").replace("/", "~1") for p in path)


def projection(value, *, artifact, path=(), raw_domain=False, excluded=None):
    """Normalize exact representations, preserving all non-excluded field domains."""
    excluded = [] if excluded is None else excluded
    if isinstance(value, dict):
        result = {}
        for key in sorted(value):
            exclude = (artifact == "METHOD_RESULTS" and not path and key in NEW_METHOD_ENVELOPE | METHOD_TIMING)
            exclude = exclude or (artifact == "METHOD_RESULTS" and key in LP_DETAILS and "endpoints" in path)
            exclude = exclude or (artifact == "EVALUATION" and not path and key == "contract_passed")
            # R1 adds an empty errors diagnostic to the real scientific report.
            exclude = exclude or (artifact == "EVALUATION" and not path and key == "errors" and value[key] == [])
            if exclude:
                excluded.append(pointer(path + (key,)))
                continue
            numeric_domain = raw_domain or key.endswith("_raw") or key == "raw"
            result[key] = projection(value[key], artifact=artifact, path=path + (key,),
                                     raw_domain=numeric_domain, excluded=excluded)
        return result
    if isinstance(value, list):
        values = [projection(v, artifact=artifact, path=path + (i,), raw_domain=raw_domain,
                             excluded=excluded) for i, v in enumerate(value)]
        if path and path[-1] in SET_FIELDS and all(isinstance(v, str) for v in values):
            return sorted(values)  # preserves multiplicity, so duplicates still differ
        return values
    if raw_domain and value is not None:
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise ValueError("Invalid exact amount at " + pointer(path))
        return str(Fraction(value))
    if isinstance(value, float):
        # Method timings are excluded above; an unexpected float is not silently rounded.
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError("Nonfinite value at " + pointer(path))
    return value


def differences(left, right, path=(), counters=None):
    counters = counters if counters is not None else {}
    counters["nodes_compared"] = counters.get("nodes_compared", 0) + 1
    if type(left) is not type(right):
        return [{"path": pointer(path), "kind": "TYPE", "baseline": left, "revision": right}]
    if isinstance(left, dict):
        out = []
        counters["dictionary_domains_compared"] = counters.get("dictionary_domains_compared", 0) + 1
        for key in sorted(set(left) | set(right)):
            if key not in left or key not in right:
                out.append({"path": pointer(path + (key,)), "kind": "MISSING_OR_EXTRA_FIELD",
                            "baseline": left.get(key, MISSING), "revision": right.get(key, MISSING)})
            else:
                out.extend(differences(left[key], right[key], path + (key,), counters))
        return out
    if isinstance(left, list):
        out = []
        counters["lists_compared"] = counters.get("lists_compared", 0) + 1
        for index in range(max(len(left), len(right))):
            if index >= len(left) or index >= len(right):
                out.append({"path": pointer(path + (index,)), "kind": "MISSING_OR_EXTRA_MEMBER",
                            "baseline": left[index] if index < len(left) else MISSING,
                            "revision": right[index] if index < len(right) else MISSING})
            else:
                out.extend(differences(left[index], right[index], path + (index,), counters))
        return out
    counters["scalar_fields_compared"] = counters.get("scalar_fields_compared", 0) + 1
    return [] if left == right else [{"path": pointer(path), "kind": "VALUE", "baseline": left, "revision": right}]


def self_test():
    def compare(a, b):
        return differences(projection(a, artifact="METHOD_RESULTS"), projection(b, artifact="METHOD_RESULTS"))
    checks = {
        "amount_mutation_detected": bool(compare({"point_raw": "1/2"}, {"point_raw": "3/2"})),
        "equivalent_exact_amount_accepted": not compare({"point_raw": "1/2"}, {"point_raw": "0.5"}),
        "missing_output_domain_detected": bool(compare({"addresses": {"T|ETH": {"point_raw": "1"}}}, {"addresses": {}})),
        "set_order_ignored": not compare({"output_event_ids": ["b", "a"]}, {"output_event_ids": ["a", "b"]}),
        "duplicate_member_detected": bool(compare({"output_event_ids": ["a"]}, {"output_event_ids": ["a", "a"]})),
        "allocation_not_excluded": bool(compare({"allocation_raw": {"seed": "2"}}, {"allocation_raw": {"seed": "1"}})),
        "baseline_marking_witness_retained": bool(compare({"events": {"e": {"witness": ["seed", "e"]}}}, {"events": {"e": {"witness": ["e"]}}})),
        "lp_witness_details_ignored": not compare({"endpoints": {"lower": {"raw": "0", "witness_event_source_raw": {"a": "0"}}}}, {"endpoints": {"lower": {"raw": "0", "witness_event_source_raw": {"a": "1"}}}}),
        "lp_endpoint_amount_retained": bool(compare({"endpoints": {"lower": {"raw": "0"}}}, {"endpoints": {"lower": {"raw": "1"}}})),
        "new_envelope_only_ignored": not compare({"status": "COMPLETED"}, {"status": "COMPLETED", "query_id": "x", "native_identity": {}, "identity_errors": []}),
        "unknown_new_scientific_field_detected": bool(compare({"status": "COMPLETED"}, {"status": "COMPLETED", "new_amount": "1"})),
    }
    return {"passed": all(checks.values()), "checks": checks, "count": len(checks)}


def identity_evidence(base, code, manifest, frozen):
    records = []

    def add(path, category, expected=None, sample_id=None):
        a, b = within(base, path), within(code, path)
        left, right = sha(a) if a.is_file() else None, sha(b) if b.is_file() else None
        records.append({"category": category, "path": path, "sample_id": sample_id,
                        "frozen_sha256": expected, "baseline_sha256": left, "revision_sha256": right,
                        "passed": left is not None and left == right and (expected is None or left == expected)})

    add("EXPERIMENT_FREEZE.json", "parent_freeze")
    for path, key in (("EXPERIMENT_INPUTS.json", "inputs_manifest_sha256"),
                      ("controlled_v1/MANIFEST.json", "controlled_manifest_sha256"),
                      ("METHOD_SPEC_EFFECTIVE.md", "method_spec_sha256"),
                      ("configs/STAGE1C_EFFECTIVE_POLICY.json", "effective_policy_sha256"),
                      (manifest["real_private_manifest"], "private_inputs_manifest_sha256")):
        add(path, "frozen_manifest_or_rule", frozen[key])
    rows = manifest["controlled"] + read(within(base, manifest["real_private_manifest"]))
    for row in rows:
        add(row["observed_path"], "observed_query_input", row["observed_sha256"], row["sample_id"])
        if row.get("hidden_path"):
            add(row["hidden_path"], "controlled_hidden", row["hidden_sha256"], row["sample_id"])
    inventory = {r["path"]: r["sha256"] for r in frozen["source_inventory"]}
    for path in SCIENTIFIC_SOURCES:
        add(path, "unchanged_scientific_module", inventory[path])
    # Byte identity only for retained data/evidence, not a new historical audit.
    already = {r["path"] for r in records}
    for prefix in ("controlled_v1", "private", "raw", "derived"):
        for path in sorted((base / prefix).rglob("*")):
            if path.is_file():
                relative = path.relative_to(base).as_posix()
                if relative not in already:
                    add(relative, "retained_input_evidence_byte_identity")
    return records, rows


def run(rev, batch, output):
    base, code = rev / "baseline/min", rev / "code"
    test = self_test()
    if not test["passed"]:
        raise ValueError("Comparator self-test failed")
    frozen, manifest = read(base / "EXPERIMENT_FREEZE.json"), read(base / "EXPERIMENT_INPUTS.json")
    identities, rows = identity_evidence(base, code, manifest, frozen)
    if len(rows) != 62 or sum(r["kind"] == "controlled" for r in rows) != 60 or sum(r["kind"] == "real" for r in rows) != 2:
        raise ValueError("Frozen 60 + 2 sample universe changed")
    if len({r["sample_id"] for r in rows}) != 62:
        raise ValueError("Frozen sample IDs duplicated")
    index = read(batch / "RESULTS_INDEX.json")
    expected_ids = {r["sample_id"] for r in rows}
    actual_rows = index.get("method_results_index", [])
    actual_ids = [r["sample_id"] for r in actual_rows]
    prerequisites = {
        "canonical_batch_completed": index.get("status") == "COMPLETED" and index.get("passed") is True,
        "complete_frozen_sample_domain": len(actual_ids) == 62 and set(actual_ids) == expected_ids,
        "all_index_queries_accepted": all(r.get("passed") is True and r.get("contract_passed") is True for r in actual_rows),
        "parent_freeze_identity": index.get("freeze_sha256") == sha(base / "EXPERIMENT_FREEZE.json"),
        "input_freeze_sources_unchanged": all(r["passed"] for r in identities),
        "comparator_self_test": test["passed"],
    }
    by_sample = {r["sample_id"]: r for r in actual_rows}
    method_checks, artifact_checks, diffs, query_checks = [], [], [], []
    totals = {}
    for row in sorted(rows, key=lambda r: r["sample_id"]):
        sid = row["sample_id"]
        slug = sid.replace(":", "_").replace("/", "_")
        old = base / "results" / row["kind"] / "samples" / slug
        new = batch / "samples" / slug
        old_results, new_results = read(old / "METHOD_RESULTS.json"), read(new / "METHOD_RESULTS.json")
        method_domain = set(old_results) == set(METHODS) == set(new_results)
        if not method_domain:
            diffs.append({"sample_id": sid, "kind": row["kind"], "method": "*", "artifact": "METHOD_RESULTS",
                          "path": "/", "kind_of_difference": "METHOD_DOMAIN", "baseline": sorted(old_results), "revision": sorted(new_results)})
        contract, acceptance = read(new / "OUTPUT_CONTRACT.json"), read(new / "QUERY_ACCEPTANCE.json")
        bindings = acceptance.get("binding", {}).get("files", {})
        qc = {
            "sample_id": sid, "kind": row["kind"], "method_domain_exact": method_domain,
            "new_contract_passed": contract.get("passed") is True and contract.get("status") == "PASS",
            "query_accepted": acceptance.get("passed") is True and acceptance.get("contract_passed") is True,
            "new_contract_results_binding": contract.get("bindings", {}).get("method_results_sha256") == object_hash(new_results),
            "new_contract_source_binding": contract.get("bindings", {}).get("contract_module_sha256") == sha(code / "src/stage1c_output_contract.py"),
            "new_query_file_bindings": all(bindings.get(n) == sha(new / n) for n in ("METHOD_RESULTS.json", "OUTPUT_CONTRACT.json", "EVALUATION.json", "ABLATIONS.json")),
            "index_acceptance_binding": by_sample.get(sid, {}).get("query_acceptance_sha256") == sha(new / "QUERY_ACCEPTANCE.json"),
            "new_contract_allocation_audits": contract.get("counts", {}).get("post_generation_unique_allocation_audits", 0),
        }
        qc["passed"] = all(v for k, v in qc.items() if k not in ("sample_id", "kind", "new_contract_allocation_audits"))
        query_checks.append(qc)
        for method in METHODS:
            excluded_left, excluded_right = [], []
            a = projection(old_results.get(method, MISSING), artifact="METHOD_RESULTS", excluded=excluded_left)
            b = projection(new_results.get(method, MISSING), artifact="METHOD_RESULTS", excluded=excluded_right)
            counters = {}
            current = differences(a, b, counters=counters)
            record = {"sample_id": sid, "kind": row["kind"], "method": method,
                      "passed": not current, "baseline_projection_sha256": object_hash(a), "revision_projection_sha256": object_hash(b),
                      "differences_count": len(current), **counters,
                      "excluded_baseline_fields": excluded_left, "excluded_revision_fields": excluded_right,
                      "address_domain_count": len((old_results.get(method) or {}).get("addresses") or {}),
                      "event_domain_count": len((old_results.get(method) or {}).get("events") or {}),
                      "joint_asset_domain_count": len((old_results.get(method) or {}).get("joint_by_asset") or {}),
                      "haircut_allocation_ports": len((old_results.get(method) or {}).get("allocation_raw") or {}) if method == "HAIRCUT" else None}
            method_checks.append(record)
            for key, count in counters.items():
                totals[key] = totals.get(key, 0) + count
            diffs.extend({"sample_id": sid, "kind": row["kind"], "method": method, "artifact": "METHOD_RESULTS",
                          "kind_of_difference": d.pop("kind"), **d} for d in current)
        for name in ("EVALUATION", "ABLATIONS"):
            a = projection(read(old / (name + ".json")), artifact=name)
            b = projection(read(new / (name + ".json")), artifact=name)
            current = differences(a, b)
            artifact_checks.append({"sample_id": sid, "kind": row["kind"], "artifact": name, "passed": not current,
                                    "baseline_projection_sha256": object_hash(a), "revision_projection_sha256": object_hash(b), "differences_count": len(current)})
            diffs.extend({"sample_id": sid, "kind": row["kind"], "method": "*", "artifact": name,
                          "kind_of_difference": d.pop("kind"), **d} for d in current)
    passed = all(prerequisites.values()) and all(r["passed"] for r in query_checks) and not diffs
    summary = {
        "schema_version": VERSION, "status": "PASS" if passed else "FAIL", "passed": passed,
        "prerequisites": prerequisites, "controlled_count": 60, "real_count": 2,
        "method_result_pairs": len(method_checks), "method_pairs_equal": sum(r["passed"] for r in method_checks),
        "auxiliary_artifact_pairs": len(artifact_checks), "auxiliary_pairs_equal": sum(r["passed"] for r in artifact_checks),
        "field_differences_count": len(diffs), "method_field_comparison_counts": totals,
        "identity_items": len(identities), "identity_items_unchanged": sum(r["passed"] for r in identities),
        "identity_category_counts": {c: sum(r["category"] == c for r in identities) for c in sorted({r["category"] for r in identities})},
        "new_query_contracts_passed_and_bound": sum(r["passed"] for r in query_checks),
        "new_unique_allocation_audits": sum(r["new_contract_allocation_audits"] for r in query_checks),
        "comparison_tool_sha256": sha(Path(__file__)), "baseline_root": str(base), "revision_code_root": str(code), "new_batch_root": str(batch),
        "baseline_freeze_sha256": sha(base / "EXPERIMENT_FREEZE.json"), "revision_freeze_sha256": sha(code / "REVISION_FREEZE.json"),
        "new_results_index_sha256": sha(batch / "RESULTS_INDEX.json"), "self_test": test,
        "comparison_scope": "All existing typed method scientific fields, statuses, domain/member/support sets, physical capacities, structural metadata, deterministic Haircut allocation and H_BMIN boundary completion; common scientific evaluation and ablation fields.",
        "exclusions": {"new_method_envelope": sorted(NEW_METHOD_ENVELOPE), "method_timing": sorted(METHOD_TIMING), "lp_endpoint_details_only": sorted(LP_DETAILS), "evaluation_metadata": ["contract_passed", "empty errors diagnostic"], "set_order_normalized": sorted(SET_FIELDS)},
        "witness_statement": "LP witnesses/certification recovery details may be nonunique and are excluded from equality comparison. R1 OUTPUT_CONTRACT independently re-audits each supplied complete witness against the relevant unchanged constraints and exact objective value; this tool verifies stored PASS receipts and their result/source/file bindings, and does not rerun the contract or any algorithm.",
        "limits": ["Same-input regression of developed 60 controlled samples and 2 real queries; not new holdout or external blind validation.", "No real source-amount truth or real amount accuracy inferred from equality.", "Retained evidence byte hashes certify identity only, not a repeated historical evidence/content audit.", "Timing and online collection savings are not compared; no algorithm or research-platform request is executed by this tool."],
        "method_checks": method_checks, "auxiliary_checks": artifact_checks, "query_contract_checks": query_checks,
        "identity_evidence": identities, "field_differences": diffs, "external_review_status": "PENDING_REVIEW",
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "SAME_INPUT_COMPARISON.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    csv_fields = ["record_type", "sample_id", "kind", "method", "artifact", "passed", "path", "differences_count", "kind_of_difference", "baseline", "revision"]
    with (output / "SAME_INPUT_COMPARISON.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        for record in method_checks:
            writer.writerow({"record_type": "METHOD_PAIR", "artifact": "METHOD_RESULTS", **record})
        for record in artifact_checks:
            writer.writerow({"record_type": "AUXILIARY_PAIR", "method": "*", **record})
        for record in diffs:
            writer.writerow({"record_type": "FIELD_DIFFERENCE", "passed": False, **record,
                             "baseline": canonical(record["baseline"]), "revision": canonical(record["revision"])})
    report = f"""# Stage1C-R1 同输入结果比较

结果：**{summary['status']}**。冻结 60 个受控样本与 2 个真实查询，共 {len(method_checks)} 对方法返回；{summary['method_pairs_equal']} 对科学结果完全相等，精确字段级差异 {len(diffs)} 项。EVALUATION/ABLATIONS 的 {len(artifact_checks)} 对共同科学内容中，{summary['auxiliary_pairs_equal']} 对相等。

比较包含全部既有金额类型、上下界/点/名义值、方法状态、完整地址/事件/联合域、集合成员、支持标志、端口物理容量、结构 metadata、Haircut 完整 allocation 和 H_BMIN_BOUNDARY_V1。精确数值按有理数比较；集合只忽略顺序，保留重复成员差异。逐方法投影身份、计数及全部字段差异见 JSON，逐方法及差异表见 CSV。

新增 envelope/contract metadata、计时和 LP 非唯一见证/证书恢复细节不参与相等性判断。新契约已在结果生成后独立重审见证的完整性、对应约束可行性和目标值一致性；本比较工具核对了 {summary['new_query_contracts_passed_and_bound']} 份新 query 契约、源与结果文件绑定，共记录 {summary['new_unique_allocation_audits']} 个去重后的完整分配审计。它没有重跑算法，也不以旧新见证字节相等作为正确性条件。

逐项身份核对 {len(identities)} 个文件，{summary['identity_items_unchanged']} 项相同，包括 62 个 observed、60 个 hidden、原 freeze/manifest/方法规则、{len(SCIENTIFIC_SOURCES)} 个科学源码模块及保留输入证据。保留证据仅核字节身份，不宣称重新内容验收。原 EXPERIMENT_FREEZE 与方法版本不变，新执行代码由 REVISION_FREEZE 单独绑定。

本结果是既开发样本的同输入回归，不是新 holdout，不构成真实金额准确率或外部盲审真值。计时另行报告；新增研究请求为 0。外部验收状态 PENDING_REVIEW。
"""
    (output / "SAME_INPUT_COMPARISON.md").write_text(report, encoding="utf-8")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--batch", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        receipt = self_test()
        print(json.dumps(receipt, ensure_ascii=False, indent=2))
        return 0 if receipt["passed"] else 1
    rev = args.revision.resolve()
    batch = (args.batch or rev / "batch_r1").resolve()
    output = (args.output or rev).resolve()
    if not batch.is_relative_to(rev) or not output.is_relative_to(rev):
        raise ValueError("Batch and output must remain within this revision workspace")
    try:
        summary = run(rev, batch, output)
        print(json.dumps({k: v for k, v in summary.items() if k not in ("method_checks", "auxiliary_checks", "query_contract_checks", "identity_evidence", "field_differences")}, ensure_ascii=False, indent=2))
        return 0 if summary["passed"] else 1
    except Exception as exc:
        print(json.dumps({"status": "ERROR", "error": type(exc).__name__ + ": " + str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
