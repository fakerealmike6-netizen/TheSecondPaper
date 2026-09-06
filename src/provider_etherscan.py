"""Etherscan V2 indexed-account adapter with immutable response/page caching.

No network or credentials are accessed here. The caller supplies an authorized,
serial budgeted transport(params)->JSON dict. Explorer enumeration is not a
claim of independent chain completeness. Missing event positions remain gaps.
"""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
from collector import Event, FetchResult, NATIVE

def integer(value):
    if value in (None, ""):
        return None
    return int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)

def normalize_rows(action, rows, tx_positions=None):
    events, gaps = [], []
    tx_positions = tx_positions or {}
    for row in rows:
        tx = str(row.get("hash", row.get("transactionHash", ""))).lower()
        try:
            if not tx.startswith("0x") or len(tx) != 66:
                raise ValueError("bad transaction hash")
            position = integer(row.get("transactionIndex"))
            if position is None:
                position = tx_positions.get(tx)
            log_index, trace_id = None, None
            kind, asset = "top", NATIVE
            failed = str(row.get("isError", "0")) != "0" or str(row.get("txreceipt_status", "1")) == "0"
            gas = None
            if action == "txlist":
                suffix = "top"
                if row.get("gasUsed") not in (None, "") and row.get("gasPrice") not in (None, ""):
                    gas = integer(row["gasUsed"]) * integer(row["gasPrice"])
            elif action == "txlistinternal":
                kind = "internal"
                trace_id = row.get("traceId", row.get("trace_address"))
                if trace_id is None:
                    gaps.append({"reason": "EVENT_IDENTITY_UNRESOLVED", "action": action, "tx_hash": tx})
                    continue
                suffix = "trace:" + str(trace_id)
                if "isError" not in row:
                    gaps.append({"reason": "INTERNAL_STATUS_UNRESOLVED", "tx_hash": tx, "trace_id": str(trace_id)})
                    continue
            elif action == "tokentx":
                kind = "erc20"
                log_index = integer(row.get("logIndex"))
                if log_index is None:
                    gaps.append({"reason": "EVENT_IDENTITY_UNRESOLVED", "action": action, "tx_hash": tx, "needs": "receipt Transfer logIndex; index-row ordinal is not event identity"})
                    continue
                contract = str(row.get("contractAddress", "")).lower()
                if len(contract) != 42:
                    raise ValueError("bad contract address")
                asset = "erc20:eip155:1:" + contract
                suffix = "log:" + str(log_index)
            else:
                raise ValueError("unsupported action")
            event = Event("eip155:1:tx:" + tx + ":" + suffix, tx, row.get("from", ""), row.get("to") or row.get("contractAddress", ""), asset, integer(row.get("value", "0")), integer(row["blockNumber"]), position, integer(row["timeStamp"]), kind, log_index, str(trace_id) if trace_id is not None else None, integer(row.get("executionIndex")), not failed, "ETHERSCAN_V2_ACCOUNT_" + action, gas)
            if len(event.sender) != 42 or len(event.recipient) != 42:
                raise ValueError("bad value-event address")
            events.append(event)
        except (ValueError, TypeError, KeyError) as exc:
            gaps.append({"reason": "NORMALIZATION_FAILED", "action": action, "tx_hash": tx, "exception_type": type(exc).__name__})
    return events, gaps

class EtherscanProvider:
    def __init__(self, transport, cache_dir, page_size=1000, max_pages=50, *, replay_only=False, raw_limit_bytes=536870912, block_at_time=None, detail_enricher=None):
        if not 1 <= page_size <= 1000:
            raise ValueError("bounded page_size required")
        self.transport, self.cache_dir = transport, Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.page_size, self.max_pages = page_size, max_pages
        self.replay_only = replay_only
        self.raw_limit_bytes = raw_limit_bytes
        self.block_at_time, self.detail_enricher = block_at_time, detail_enricher
        self.request_log = []
        self.new_raw_bytes = 0

    def _page(self, params):
        key = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()
        path = self.cache_dir / (key + ".json")
        meta = {"request_sha256": key, "params": params, "cache_hit": path.exists(), "raw_path": path.name}
        self.request_log.append(meta)
        if path.exists():
            raw = path.read_bytes()
            return json.loads(raw), meta | {"response_sha256": hashlib.sha256(raw).hexdigest(), "raw_bytes": len(raw)}, 0, 0, 1
        if self.replay_only:
            raise RuntimeError("CACHE_MISS_REPLAY_ONLY")
        # Conservative response-size reservation before calling; caller enforces
        # shared stage ceiling as well. Retain oversized response and stop later.
        if self.new_raw_bytes + self.page_size * 4096 > self.raw_limit_bytes:
            raise RuntimeError("RAW_BYTES_RESOURCE_LIMIT")
        result = self.transport(dict(params))
        raw = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
        self.new_raw_bytes += len(raw)
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(raw)
        tmp.replace(path)
        return result, meta | {"response_sha256": hashlib.sha256(raw).hexdigest(), "raw_bytes": len(raw)}, len(raw), 1, 0

    def fetch_interval(self, address, asset, start_block, end_block, *, start_time, end_time, global_end_time):
        address = address.lower()
        if len(address) != 42 or not address.startswith("0x"):
            raise ValueError("complete address required")
        if end_time < global_end_time:
            if self.block_at_time is None:
                return FetchResult(gaps=[{"reason": "LOCAL_WINDOW_BLOCK_BOUND_UNRESOLVED", "end_time": end_time}])
            end_block = min(end_block, self.block_at_time(end_time))
        collected, all_gaps, coverage = [], [], []
        counts = {"new_raw_bytes": 0, "real_requests": 0, "cache_hits": 0}
        complete = True
        tx_positions = {}
        # Native may interact with contracts via zero-value calls. All token rows
        # are contextual unless they match the current asset; no guessed swap.
        for action in ("txlist", "txlistinternal", "tokentx"):
            rows, action_complete = [], False
            previous_page_fingerprint = None
            for page in range(1, self.max_pages + 1):
                params = {"chainid": "1", "module": "account", "action": action, "address": address, "startblock": start_block, "endblock": end_block, "page": page, "offset": self.page_size, "sort": "asc"}
                if asset.startswith("erc20:") and action == "tokentx":
                    params["contractaddress"] = asset.rsplit(":", 1)[1]
                try:
                    response, metadata, nbytes, nrequests, nhits = self._page(params)
                    counts["new_raw_bytes"] += nbytes
                    counts["real_requests"] += nrequests
                    counts["cache_hits"] += nhits
                except Exception as exc:
                    reason = getattr(exc, "reason", None)
                    if reason is None:
                        reason = "BUDGET_BLOCKED" if "budget" in type(exc).__name__.lower() else "PROVIDER_ACCESS_OR_TRANSPORT_BLOCKED"
                    all_gaps.append({"reason": reason, "exception_type": type(exc).__name__, "action": action, "page": page})
                    coverage.append({"action": action, "address": address, "start_block": start_block, "end_block": end_block, "start_time": start_time, "end_time": end_time, "page": page, "returned_rows": None, "complete": False, "next_cursor": page})
                    break
                values = response.get("result")
                is_empty = str(response.get("status")) == "0" and isinstance(values, list) and not values and "no transactions" in str(response.get("message", "")).lower()
                valid = str(response.get("status")) == "1" and isinstance(values, list)
                if not valid and not is_empty:
                    # Do not echo unknown provider strings (potential URL/key).
                    all_gaps.append({"reason": "PROVIDER_REJECTED_OR_UNCERTAIN", "action": action, "page": page, "response_sha256": metadata["response_sha256"]})
                    coverage.append(metadata | {"action": action, "address": address, "start_block": start_block, "end_block": end_block, "start_time": start_time, "end_time": end_time, "page": page, "returned_rows": None, "complete": False, "next_cursor": page})
                    break
                page_fingerprint = hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()
                if values and page_fingerprint == previous_page_fingerprint:
                    all_gaps.append({"reason": "PAGINATION_REPEATED_PAGE", "action": action, "page": page})
                    break
                previous_page_fingerprint = page_fingerprint
                rows.extend(values)
                action_complete = len(values) < self.page_size
                coverage.append(metadata | {"action": action, "address": address, "start_block": start_block, "end_block": end_block, "start_time": start_time, "end_time": end_time, "page": page, "returned_rows": len(values), "complete": action_complete, "next_cursor": None if action_complete else page + 1, "completeness_basis": "provider enumeration exhausted" if action_complete else "nonterminal page"})
                if action_complete:
                    break
                if self.new_raw_bytes >= self.raw_limit_bytes:
                    all_gaps.append({"reason": "RAW_BYTES_RESOURCE_LIMIT", "action": action, "page": page})
                    break
            if not action_complete:
                complete = False
                if len(coverage) and coverage[-1].get("next_cursor") and coverage[-1]["page"] == self.max_pages:
                    all_gaps.append({"reason": "PAGINATION_PAGE_RESOURCE_LIMIT", "action": action})
            if action == "txlist":
                tx_positions.update({r["hash"].lower(): integer(r["transactionIndex"]) for r in rows if r.get("hash") and r.get("transactionIndex") not in (None, "")})
            if self.detail_enricher is not None and rows:
                # Hook must use the same external budget ledger. It returns rows,
                # explicit gaps and counters; no hidden request is allowed.
                rows, enrichment_gaps, enrichment_counts = self.detail_enricher(action, rows)
                all_gaps.extend(enrichment_gaps)
                for name in counts:
                    counts[name] += enrichment_counts.get(name, 0)
            events, gaps = normalize_rows(action, rows, tx_positions)
            collected.extend(events)
            all_gaps.extend(gaps)
        unique = {}
        for event in collected:
            if event.event_id in unique and event != unique[event.event_id]:
                all_gaps.append({"reason": "EVENT_IDENTITY_CONFLICT", "event_id": event.event_id})
                complete = False
            else:
                unique[event.event_id] = event
        if all_gaps:
            complete = False
        return FetchResult(list(unique.values()), coverage, complete, all_gaps, **counts)
