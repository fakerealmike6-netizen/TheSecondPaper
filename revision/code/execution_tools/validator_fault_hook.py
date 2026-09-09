"""Fault-test entry into the same saved-result gate used by the bundle validator.

No payload hash is corrupted or bypassed: this bounded integration hook calls
the production result reacceptance layer directly on runner-produced output.
The final extracted bundle additionally exercises its full validator chain.
"""
import argparse
import json
from pathlib import Path
import socket
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tree", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(args.tree / "src"))

    def deny(*a, **kw):
        raise RuntimeError("STAGE1C_R1_VALIDATOR_FAULT_HOOK_NETWORK_DISABLED")

    socket.create_connection = deny
    socket.socket.connect = deny
    socket.getaddrinfo = deny
    try:
        from stage1c_result_gate import validate_saved_batch
        result = validate_saved_batch(args.tree, args.results)
        if not isinstance(result, dict):
            raise TypeError("Production result gate returned a non-dictionary receipt")
    except Exception as exc:
        result = {"passed": False, "status": "ERROR", "error": type(exc).__name__ + ": " + str(exc)}
    result["fault_hook_scope"] = "PRODUCTION_SAVED_RESULT_GATE_NOT_PAYLOAD_HASH_FAILURE"
    (args.output / "VALIDATOR_FAULT_RECEIPT.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"passed": result.get("passed"), "status": result.get("status"), "scope": result["fault_hook_scope"]}))
    return 0 if result.get("passed") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
