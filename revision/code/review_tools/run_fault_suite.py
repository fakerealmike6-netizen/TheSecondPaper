"""Bounded, offline Stage1C-R1 output acceptance fault subprocess suite.

Never edits source, frozen facts, or manifests. Each actual runner.main probe
is a fresh process and output directory. Expected failures are suite successes
only when nonzero CLI, persisted bad return, query failure and batch failure
are all observed. Report/validator checks can be requested after integration.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def inventory(tree):
    paths = [*sorted((tree / "src").glob("*.py")), *sorted((tree / "tests").glob("*.py")), tree / "EXPERIMENT_FREEZE.json", tree / "EXPERIMENT_INPUTS.json"]
    inputs = read(tree / "EXPERIMENT_INPUTS.json")
    for row in inputs["controlled"]:
        paths.extend(tree / row[k] for k in ("observed_path", "hidden_path"))
    private = tree / inputs.get("real_private_manifest", "private/REAL_EXPERIMENT_INPUTS.json")
    if private.exists():
        paths.append(private)
        paths.extend(tree / row["observed_path"] for row in read(private))
    paths.extend(p for p in (tree / "REVISION_FREEZE.json", tree / "METHOD_SPEC_EFFECTIVE.md", tree / "configs/STAGE1C_EFFECTIVE_POLICY.json",
                              tree / "configs/STAGE1C_R1_EXECUTION_POLICY.json", tree / "configs/STAGE1C_R1_POLICY.json", tree / "controlled_v1/MANIFEST.json") if p.exists())
    return {p.relative_to(tree).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tree", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--kind", choices=("min", "public"), default="min")
    parser.add_argument("--cases", nargs="+", default=["all"])
    parser.add_argument("--with-reports", action="store_true")
    parser.add_argument("--validator-hook", type=Path, help="Optional actual-validator subprocess hook supplied by root integration; receives --tree --results --output")
    args = parser.parse_args()
    tree, output = args.tree.resolve(), args.output.resolve()
    if output.exists():
        raise ValueError("Suite output must be fresh; failures are never overwritten")
    output.mkdir(parents=True)
    test = tree / "tests/test_stage1c_r1_faults.py"
    spec = importlib.util.spec_from_file_location("stage1c_r1_fault_definition", test)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    cases = list(module.ALL_CASES) if args.cases == ["all"] else args.cases
    if not set(cases) <= set(module.ALL_CASES):
        raise ValueError("Unknown requested fault case")
    if args.kind == "public":
        cases = [case for case in cases if not case.startswith("real_")]
    before = inventory(tree)
    env = {k: v for k, v in os.environ.items() if not any(word in k.upper() for word in ("TOKEN", "SECRET", "API_KEY", "PASSWORD", "CREDENTIAL", "DUNE", "ALCHEMY", "ETHERSCAN", "BIGQUERY", "GOOGLE_APPLICATION"))}
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    rows = []
    for case in cases:
        dest = output / case
        command = [sys.executable, "-B", str(test), "--fault-child", "--tree", str(tree), "--kind", args.kind, "--case", case, "--output", str(dest)]
        start = time.perf_counter()
        process = subprocess.run(command, cwd=tree, env=env, text=True, capture_output=True, timeout=180)
        elapsed = time.perf_counter() - start
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "process_stdout.log").write_text(process.stdout, encoding="utf-8")
        (dest / "process_stderr.log").write_text(process.stderr, encoding="utf-8")
        receipt = read(dest / "FAULT_CHILD_RECEIPT.json") if (dest / "FAULT_CHILD_RECEIPT.json").exists() else {}
        positive = case in module.POSITIVE
        expected = 0 if positive else 1
        propagation = receipt.get("cli_exit_code") == expected and process.returncode == expected and bool(receipt.get("freeze_verifier_calls"))
        if positive:
            propagation = propagation and receipt.get("batch_passed") is True and all(q.get("evaluation_passed") is True for q in receipt.get("queries", []))
        else:
            propagation = propagation and receipt.get("batch_passed") is False and bool(receipt.get("queries"))
            propagation = propagation and receipt["queries"][0].get("evaluation_passed") is False and receipt["queries"][0].get("raw_method_output_saved") is True
        if case == "mixed_bad_then_good":
            propagation = propagation and len(receipt.get("queries", [])) == 2 and receipt["queries"][1].get("evaluation_passed") is True
        row = {"case": case, "expected_cli_exit_code": expected, "process_exit_code": process.returncode, "elapsed_seconds": elapsed,
               "fault_chain_verified": bool(propagation), "receipt": receipt}
        if args.with_reports:
            report_command = [sys.executable, "-B", str(tree / "src/stage1c_reports.py"), "--tree", str(tree), "--results", str(dest), "--output", str(dest / "reports")]
            report = subprocess.run(report_command, cwd=tree, env=env, text=True, capture_output=True, timeout=180)
            (dest / "report_stdout.log").write_text(report.stdout, encoding="utf-8")
            (dest / "report_stderr.log").write_text(report.stderr, encoding="utf-8")
            row["report_process_exit_code"] = report.returncode
            row["report_rejected_bad_or_accepted_good"] = (report.returncode == 0) if positive else (report.returncode != 0)
            row["fault_chain_verified"] &= row["report_rejected_bad_or_accepted_good"]
        if args.validator_hook:
            hook = subprocess.run([sys.executable, "-B", str(args.validator_hook.resolve()), "--tree", str(tree), "--results", str(dest), "--output", str(dest / "validator")], cwd=tree, env=env, text=True, capture_output=True, timeout=180)
            (dest / "validator_stdout.log").write_text(hook.stdout, encoding="utf-8")
            (dest / "validator_stderr.log").write_text(hook.stderr, encoding="utf-8")
            row["validator_process_exit_code"] = hook.returncode
            row["validator_rejected_bad_or_accepted_good"] = (hook.returncode == 0) if positive else (hook.returncode != 0)
            row["fault_chain_verified"] &= row["validator_rejected_bad_or_accepted_good"]
        rows.append(row)
        print(json.dumps({"case": case, "process_exit_code": process.returncode, "verified": row["fault_chain_verified"]}), flush=True)
    after = inventory(tree)
    result = {"schema_version": "stage1c-r1-output-fault-suite-v1", "passed": all(r["fault_chain_verified"] for r in rows) and before == after,
              "cases_executed": len(rows), "fault_cases": sum(c not in module.POSITIVE for c in cases), "positive_cases": sum(c in module.POSITIVE for c in cases),
              "source_and_scientific_inputs_unchanged": before == after, "inventory_count": len(before),
              "changed_files": [p for p, h in before.items() if after.get(p) != h],
              "reports_checked": args.with_reports, "validator_checked": bool(args.validator_hook),
              "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "fault_definitions_sha256": hashlib.sha256(test.read_bytes()).hexdigest(),
              "network_calls_authorized": 0, "cases": rows, "external_acceptance": "PENDING_REVIEW"}
    write(output / "FAULT_PROPAGATION_RESULTS.json", result)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
