"""Audit stored fault-chain reasons, independently of subprocess exit codes.

Confirms failed reports and saved-result gates actually recomputed and rejected
the method contract, and did not fail at input hash, receipt binding or generic
exceptions. Source/inputs are not opened or altered by this diagnostic reader.
"""
import argparse
import hashlib
import json
from pathlib import Path


def audit(root):
    root = Path(root)
    source = root / "FAULT_PROPAGATION_RESULTS.json"
    suite = json.loads(source.read_text(encoding="utf-8"))
    cases = []
    for case in suite["cases"]:
        folder = root / case["case"]
        positive = case["expected_cli_exit_code"] == 0
        row = {"case": case["case"], "positive_control": positive, "structured_receipts": {}}
        for name, path in (("report", "reports/REPORT_ACCEPTANCE.json"), ("saved_gate", "validator/VALIDATOR_FAULT_RECEIPT.json")):
            receipt = json.loads((folder / path).read_text(encoding="utf-8"))
            querycodes = {e["code"] for q in receipt.get("queries", []) for e in q.get("errors", [])}
            topcodes = {e["code"] for e in receipt.get("errors", [])}
            targeted = receipt.get("passed") is True if positive else receipt.get("passed") is False and "RECOMPUTED_OUTPUT_CONTRACT_FAILED" in querycodes
            row["structured_receipts"][name] = {"passed": receipt.get("passed"), "recomputed": receipt.get("recomputed_common_contract_and_scientific_checks"),
                "query_error_codes": sorted(querycodes), "batch_error_codes": sorted(topcodes), "targeted_expected_behavior": targeted,
                "not_hash_binding_or_exception_shortcut": not any("BINDING" in c or "EXCEPTION" in c or "MISMATCH" in c for c in querycodes | topcodes)}
        index = json.loads((folder / "RESULTS_INDEX.json").read_text(encoding="utf-8"))
        row["raw_returns_saved_for_every_query"] = all((folder / r["path"] / "RAW_METHOD_RETURNS.json").is_file() for r in index["method_results_index"])
        row["passed"] = row["raw_returns_saved_for_every_query"] and all(r["targeted_expected_behavior"] and r["not_hash_binding_or_exception_shortcut"] and r["recomputed"] for r in row["structured_receipts"].values())
        cases.append(row)
    return {"passed": bool(cases) and suite.get("passed") is True and all(r["passed"] for r in cases), "case_count": len(cases),
            "negative_count": sum(not r["positive_control"] for r in cases), "positive_count": sum(r["positive_control"] for r in cases),
            "all_source_and_scientific_inputs_unchanged": suite["source_and_scientific_inputs_unchanged"],
            "source_suite_receipt_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "audit_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "cases": cases}


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args(); result = audit(args.root)
    (args.root / "CHAIN_DIAGNOSTIC_AUDIT.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("passed", "case_count", "negative_count", "positive_count", "all_source_and_scientific_inputs_unchanged")}))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
