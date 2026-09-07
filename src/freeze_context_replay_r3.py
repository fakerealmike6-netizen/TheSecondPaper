"""Freeze the already acquired R3 context inputs; performs no network requests."""
import argparse
import copy
import json
from pathlib import Path

from context_ledger_r3 import file_identity, read_json, write_json
from context_queries_r3 import load_verified_block_headers, verify_frozen_scope


def freeze(work):
    work = Path(work).resolve()
    block_headers = load_verified_block_headers(work)

    def entry(path):
        path = Path(path).resolve()
        if not path.is_relative_to(work):
            raise ValueError("Input outside current revision")
        return {"path": path.relative_to(work).as_posix(), "sha256": file_identity(path)["sha256"]}

    original = read_json(work / "derived/CONTEXT_TARGETS.json")
    enhanced = copy.deepcopy(original)
    enhanced["schema"] = "stage1b-r3-context-targets-enhanced-v1"
    enhanced["original_targets_identity"] = entry(work / "derived/CONTEXT_TARGETS.json")
    enhanced["context_window_adjustments"] = []
    jobs_by_query = {q["query_id"]: [] for q in original["queries"]}
    for job_path in sorted((work / "private/dune_r2_jobs").glob("*/job.json")):
        job = read_json(job_path)
        if job.get("kind") != "context" or job.get("state") != "QUERY_STATE_COMPLETED":
            continue
        frozen_path = Path(job["scope_freeze_path"])
        if not frozen_path.is_absolute():
            frozen_path = work / frozen_path
        frozen = read_json(frozen_path)
        if frozen.get("query_id") not in jobs_by_query or not frozen.get("r3_context_scope"):
            continue
        # A complete exported SQL page is reusable only when both its partition
        # date predicate and numeric account window cover the actual ledger.
        verify_frozen_scope(frozen_path, work, block_headers=block_headers)
        jobs_by_query[frozen["query_id"]].append({"freeze": entry(frozen_path), "job": entry(job_path)})
        target = next(q for q in enhanced["queries"] if q["query_id"] == frozen["query_id"])
        for window in frozen["account_windows"]:
            account = next(r for r in target["rows"] if r["address"] == window["address"])
            if window["ledger_start_block"] < account["ledger_start_block"]:
                enhanced["context_window_adjustments"].append({"query": target["name"], "address": account["address"], "old_start_block": account["ledger_start_block"], "new_start_block": window["ledger_start_block"], "new_initial_anchor_block": window["ledger_start_block"] - 1, "reason": "A real observed context transfer links two already modeled accounts before this account's original first candidate; preserve the common source variable with finite missing-window evidence", "frozen_evidence": entry(frozen_path)})
                account["ledger_start_block"] = window["ledger_start_block"]
                account["before_anchor_block"] = window["ledger_start_block"] - 1
            if window["ledger_end_block"] > account["ledger_end_block"]:
                raise ValueError("End-window expansion needs explicit contextual review")
    enhanced_path = work / "derived/CONTEXT_TARGETS_ENHANCED.json"
    write_json(enhanced_path, enhanced)
    addresses = {r["address"] for q in enhanced["queries"] for r in q["rows"]}
    anchor_blocks = {r[k] for doc in (original, enhanced) for q in doc["queries"] for r in q["rows"] for k in ("before_anchor_block", "after_anchor_block")}
    seeds = {q["seed_event_id"].split(":tx:")[1].split(":")[0] for q in original["queries"]}
    spec = {"schema_version": "stage1b-r3-context-replay-v1", "targets": entry(enhanced_path), "rpc_batches": [], "queries": []}
    for receipt_path in sorted((work / "raw/rpc_r3").glob("*/receipt.json")):
        intent_path = receipt_path.parent / "dispatch_intent.json"
        if not intent_path.is_file():
            continue
        intent = read_json(intent_path)
        relevant = False
        for request in intent["requests"]:
            method, params = request["method"], request["params"]
            relevant |= method in ("eth_getBalance", "eth_getCode") and params[0].lower() in addresses
            relevant |= method == "eth_getBlockByNumber" and int(params[0], 16) in anchor_blocks
            relevant |= method == "eth_getTransactionReceipt" and params[0].lower() in seeds
        if relevant:
            spec["rpc_batches"].append({"receipt": entry(receipt_path), "intent": entry(intent_path)})
    for query in enhanced["queries"]:
        folder = work / "baseline/r2/derived/new_observed_graph" / query["name"]
        spec["queries"].append({"name": query["name"], "fixed_graph": entry(folder / "fixed_graph.json"), "collection": entry(folder / "collection.json"), "context_jobs": jobs_by_query[query["query_id"]]})
    manifest_path = work / "configs/CONTEXT_REPLAY_R3.json"
    write_json(manifest_path, spec)
    return {"manifest": entry(manifest_path), "rpc_batches": len(spec["rpc_batches"]), "context_jobs": sum(len(v) for v in jobs_by_query.values()), "window_adjustments": enhanced["context_window_adjustments"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", required=True)
    args = parser.parse_args()
    print(json.dumps(freeze(args.work), indent=2))
