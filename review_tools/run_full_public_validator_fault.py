"""Full extracted-public-validator fault control; run after formal timing.

The original manifest, freeze, methods, inputs and saved good batch are intact.
Only the controlled runner subprocess command is routed through the versioned
test return-boundary injector. All 60 execute: normal_mixing/00 is faulty and
the other 59 are unmodified. Unit checks and all original validator gates run.
"""
import argparse
import json
from pathlib import Path
import socket
import sys
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tree", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(args.tree.resolve() / "src"))
    import validate_stage1c as validator
    original = validator.Stage1CValidator.command
    injected = []

    def instrument(self, name, script, arguments):
        if name == "stage1c_controlled" and script == "src/run_stage1c.py":
            rewritten = []
            pos = 0
            while pos < len(arguments):
                if arguments[pos] == "--selection":
                    if arguments[pos + 1] != "controlled":
                        raise ValueError("Unexpected full validator selection")
                    pos += 2
                else:
                    rewritten.append(arguments[pos]); pos += 1
            rewritten += ["--fault-child", "--case", "full_missing_output", "--batch-selection", "controlled"]
            injected.append({"name": name, "original_script": script, "test_script": "tests/test_stage1c_r1_faults.py", "faulty_sample": "controlled-v1/normal_mixing/00", "other_queries_unmodified": 59})
            return original(self, name, "tests/test_stage1c_r1_faults.py", rewritten)
        return original(self, name, script, arguments)

    def deny(*a, **kw):
        raise RuntimeError("FULL_VALIDATOR_FAULT_CONTROL_NETWORK_DISABLED")

    command = ["validate_stage1c.py", "--tree", str(args.tree.resolve()), "--output", str(args.output.resolve()), "--kind", "public"]
    with patch.object(validator.Stage1CValidator, "command", instrument), patch.object(sys, "argv", command), \
         patch.object(socket, "create_connection", side_effect=deny), patch.object(socket.socket, "connect", side_effect=deny), \
         patch.object(socket, "getaddrinfo", side_effect=deny):
        status = validator.main()
    receipt_path = args.output / "VALIDATION_RECEIPT.json"
    receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else {}
    checks = {row["name"]: row for row in receipt.get("checks", [])}
    batch_path = args.output / "stage1c_controlled/RESULTS_INDEX.json"
    batch = json.loads(batch_path.read_text()) if batch_path.exists() else {}
    rows = batch.get("method_results_index", [])
    bad = [r for r in rows if r.get("passed") is not True]
    prerequisite = ("payload_manifest_verified", "frozen_source_and_inputs_verified", "unit_test_receipt", "controlled_oracle_receipt", "controlled_saved_acceptance_receipt", "frozen_tree_unchanged")
    verified = status != 0 and receipt.get("passed") is False and len(injected) == 1
    verified = verified and all(checks.get(name, {}).get("passed") is True for name in prerequisite)
    verified = verified and len(rows) == 60 and len(bad) == 1 and bad[0]["sample_id"] == "controlled-v1/normal_mixing/00"
    verified = verified and checks.get("controlled_reexecuted_acceptance_receipt", {}).get("passed") is False
    verified = verified and not any(name.startswith("required_check_not_run:") for name in checks)
    output = {"schema_version": "stage1c-r1-full-validator-fault-control-v1", "fault_chain_verified": bool(verified),
              "validator_cli_exit_code": status, "validator_passed": receipt.get("passed"), "injections": injected,
              "batch_query_count": len(rows), "bad_query_count": len(bad), "good_query_count": len(rows) - len(bad),
              "prerequisites": {name: checks.get(name) for name in prerequisite},
              "contract_reacceptance_failed": checks.get("controlled_reexecuted_acceptance_receipt"),
              "no_payload_hash_corruption": True, "no_missing_required_check_shortcut": not any(name.startswith("required_check_not_run:") for name in checks),
              "scope": "Full real validator.main with only one method-return boundary fault; not included in formal scientific or timing aggregate"}
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "FULL_VALIDATOR_FAULT_CHECK.json").write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"fault_chain_verified": bool(verified), "validator_exit_code": status, "good_queries": len(rows) - len(bad), "bad_queries": len(bad)}))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
