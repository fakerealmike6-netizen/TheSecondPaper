"""Exact ERC20 receipt enrichment for indexed records; injected I/O only.

receipt_loader(tx_hash) -> (RPC receipt dict, operation counters dict). The caller
owns receipt cache, entitlement, shared budget, provider identity, and provenance.
"""
from collections import Counter, defaultdict
from provider_etherscan import integer

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

class ReceiptEnricher:
    def __init__(self, receipt_loader):
        self.receipt_loader = receipt_loader
        self.loaded = {}

    def __call__(self, action, rows):
        counters = {"real_requests": 0, "cache_hits": 0, "new_raw_bytes": 0}
        if action not in ("tokentx", "txlistinternal"):
            return rows, [], counters
        grouped = defaultdict(list)
        for row in rows:
            grouped[row["hash"].lower()].append(row)
        output, gaps = [], []
        for tx, txrows in grouped.items():
            need = (action == "tokentx" and any(r.get("logIndex") in (None, "") for r in txrows)) or (action == "txlistinternal" and any(r.get("transactionIndex") in (None, "") for r in txrows))
            if not need:
                output.extend(txrows)
                continue
            if tx in self.loaded:
                receipt = self.loaded[tx]
            else:
                try:
                    receipt, counts = self.receipt_loader(tx)
                    for name in counters:
                        counters[name] += counts.get(name, 0)
                    if not isinstance(receipt, dict) or str(receipt.get("transactionHash", "")).lower() != tx:
                        raise ValueError("receipt transaction mismatch")
                    self.loaded[tx] = receipt
                except Exception as exc:
                    gaps.append({"reason": "RECEIPT_DETAIL_UNAVAILABLE", "tx_hash": tx, "exception_type": type(exc).__name__})
                    output.extend(txrows)
                    continue
            if action == "txlistinternal":
                # Receipt success alone cannot prove a subcall was not reverted.
                output.extend({**row, "transactionIndex": str(integer(receipt["transactionIndex"]))} for row in txrows)
                continue
            if integer(receipt.get("status")) != 1:
                gaps.append({"reason": "TOKEN_RECEIPT_NOT_SUCCESSFUL", "tx_hash": tx})
                continue
            expected = Counter((str(r["contractAddress"]).lower(), str(r["from"]).lower(), str(r["to"]).lower(), integer(r["value"])) for r in txrows)
            templates = {(str(r["contractAddress"]).lower(), str(r["from"]).lower(), str(r["to"]).lower(), integer(r["value"])): r for r in txrows}
            observed = Counter()
            seen_log_ids = set()
            for log in receipt.get("logs", []):
                topics = log.get("topics", [])
                if len(topics) != 3 or topics[0].lower() != TRANSFER_TOPIC or log.get("removed"):
                    continue
                try:
                    if len(topics[1]) != 66 or len(topics[2]) != 66 or len(log.get("data", "")) != 66:
                        continue
                    key = (log["address"].lower(), "0x" + topics[1][-40:].lower(), "0x" + topics[2][-40:].lower(), integer(log["data"]))
                    if key not in expected:
                        continue
                    log_index = integer(log["logIndex"])
                    if log_index in seen_log_ids:
                        gaps.append({"reason": "DUPLICATE_RECEIPT_LOG_INDEX", "tx_hash": tx, "log_index": log_index})
                        continue
                    seen_log_ids.add(log_index)
                    observed[key] += 1
                    output.append({**templates[key], "logIndex": str(log_index), "transactionIndex": str(integer(receipt["transactionIndex"])), "blockNumber": str(integer(receipt["blockNumber"])), "txreceipt_status": "1", "receipt_verified_transfer": True})
                except (ValueError, KeyError, TypeError):
                    gaps.append({"reason": "RECEIPT_LOG_PARSE_FAILED", "tx_hash": tx})
            for key in expected:
                if observed[key] != expected[key]:
                    gaps.append({"reason": "INDEX_RECEIPT_TRANSFER_MULTIPLICITY_MISMATCH", "tx_hash": tx, "index_count": expected[key], "receipt_count": observed[key]})
        return output, gaps, counters
