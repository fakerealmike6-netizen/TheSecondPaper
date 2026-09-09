"""Etherscan V2 indexed-account adapter with immutable response/page caching.

No network or credentials are accessed here. The caller supplies an authorized,
serial budgeted transport(params)->JSON dict. Explorer enumeration is not a
claim of independent chain completeness. Missing event positions remain gaps.
"""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import time
import random
import re
from collector import Event, FetchResult, NATIVE
from exact_fields_r4 import exact_uint, field_uint, field_text, field_status, ExactFieldError
from read_retry_r4 import ReadRetryStore, classify_failure

def integer(value):
    if value in (None, ""):
        return None
    return exact_uint(value)

def _optional_uint(row, names):
    # Etherscan legitimately uses empty strings for unavailable locators.
    normalized = {k:v for k,v in row.items() if not (k in names and v == "")}
    return field_uint(normalized,names,required=False,strict_null=True)

def _provider_status(row, names):
    value = field_uint(row,names,required=False,strict_null=True)
    if value is not None and value not in (0,1): raise ExactFieldError("INVALID","|".join(names))
    return None if value is None else bool(value)

def _success(row, action):
    inverse = _provider_status(row,("isError",))
    direct = _provider_status(row,("txreceipt_status","receipt_status"))
    if action == "txlistinternal":
        # Parent transaction success cannot establish successful subcall value.
        if inverse is None: raise ExactFieldError("MISSING","isError")
        if direct is False and inverse is False: raise ExactFieldError("CONFLICT","isError|txreceipt_status")
        return not inverse
    known = ([not inverse] if inverse is not None else []) + ([direct] if direct is not None else [])
    if not known: raise ExactFieldError("MISSING","success_status")
    if any(value != known[0] for value in known): raise ExactFieldError("CONFLICT","isError|txreceipt_status")
    return known[0]

def classify_response(response):
    if not isinstance(response,dict): return {"outcome":"PERMANENT_FAILURE","error_class":"INVALID_JSON_SCHEMA","retryable":False}
    values = response.get("result")
    status = response.get("status")
    if type(status) in (str,int) and str(status) == "1" and isinstance(values,list):
        return {"outcome":"SUCCESS","retryable":False,"empty":not values}
    if type(status) in (str,int) and str(status) == "0" and values == [] and "no transactions" in str(response.get("message","")).lower():
        return {"outcome":"SUCCESS","retryable":False,"empty":True}
    return classify_failure(provider_error={"message":str(response.get("message",""))+" "+str(values)})

class ProviderReadFailure(RuntimeError):
    def __init__(self,reason,counters=None,metadata=None):
        super().__init__(reason); self.reason=reason; self.counters=counters or {}; self.metadata=metadata or {}

def normalize_rows(action, rows, tx_positions=None):
    events, gaps = [], []
    tx_positions = tx_positions or {}
    for row in rows:
        tx = ""
        try:
            tx = field_text(row,("hash","transactionHash"),required=True,lower=True)
            if not re.fullmatch(r"0x[0-9a-f]{64}",tx):
                raise ValueError("bad transaction hash")
            position = _optional_uint(row,("transactionIndex","tx_index"))
            if position is None:
                position = exact_uint(tx_positions[tx],"tx_positions") if tx in tx_positions else None
            log_index, trace_id = None, None
            kind, asset = "top", NATIVE
            succeeded = _success(row,action)
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
                log_index = _optional_uint(row,("logIndex","log_index"))
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
            amount = field_uint(row,("value",),required=True,strict_null=True)
            chain = _optional_uint(row,("chainId","chain_id"))
            if chain not in (None,1): raise ExactFieldError("CONFLICT","chainId")
            event = Event("eip155:1:tx:" + tx + ":" + suffix, tx, row.get("from", ""), row.get("to") or row.get("contractAddress", ""), asset, amount, field_uint(row,("blockNumber","block_number"),strict_null=True), position, field_uint(row,("timeStamp","timestamp"),strict_null=True), kind, log_index, str(trace_id) if trace_id is not None else None, _optional_uint(row,("executionIndex",)), succeeded, "ETHERSCAN_V2_ACCOUNT_" + action, gas, block_hash=field_text(row,("blockHash","block_hash"),lower=True), gas_used=_optional_uint(row,("gasUsed",)) if action == "txlist" else None, gas_price=_optional_uint(row,("gasPrice",)) if action == "txlist" else None)
            if position is None:
                gaps.append({"reason":"TRANSACTION_POSITION_UNRESOLVED", "action":action, "tx_hash":tx})
            if len(event.sender) != 42 or len(event.recipient) != 42:
                raise ValueError("bad value-event address")
            events.append(event)
        except (ValueError, TypeError, KeyError) as exc:
            gaps.append({"reason": "NORMALIZATION_FAILED", "action": action, "tx_hash": tx, "exception_type": type(exc).__name__,"field_reason":getattr(exc,"reason_code","INVALID"),"field":getattr(exc,"field",None)})
    return events, gaps

class EtherscanProvider:
    def __init__(self, transport, cache_dir, page_size=1000, max_pages=50, *, replay_only=False, raw_limit_bytes=536870912, block_at_time=None, detail_enricher=None, retry_store=None, clock=time.time, sleep=time.sleep, rng=random.random, deadline=None, attempt_hook=None, legacy_failure_history=None):
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
        self.clock,self.sleep,self.deadline,self.attempt_hook=clock,sleep,deadline,attempt_hook
        self.retry_store=retry_store or ReadRetryStore(self.cache_dir/'read_retry_r4.sqlite',clock=clock,rng=rng)
        self.legacy_failure_history = legacy_failure_history or {}

    def _page(self, params):
        key = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()
        path = self.cache_dir / (key + ".json")
        identity={"provider":"ETHERSCAN_V2_ACCOUNT","method":"GET","params":params}
        meta = {"request_sha256": key, "params": params, "cache_hit": False, "raw_path": None}
        self.request_log.append(meta)
        if path.exists():
            raw = path.read_bytes()
            try:
                legacy=json.loads(raw); classification=classify_response(legacy)
            except (ValueError,TypeError):
                legacy=None; classification={"outcome":"PERMANENT_FAILURE"}
            self.retry_store.migrate_cache(identity,path,classification['outcome'],payload=legacy if classification['outcome']=='SUCCESS' else None,history=self.legacy_failure_history.get(key))
        counts={"new_raw_bytes":0,"real_requests":0,"cache_hits":0}
        while True:
            claim=self.retry_store.claim(identity,deadline=self.deadline)
            state=claim['state']
            if state=='CACHE_HIT':
                payload=claim['payload']; encoded=json.dumps(payload,sort_keys=True,separators=(',',':')).encode()
                receipt=claim.get('receipt',{})
                return payload,meta|receipt|{"cache_hit":True,"response_sha256":hashlib.sha256(encoded).hexdigest(),"raw_bytes":len(encoded),"logical_key":claim['logical_key']},counts['new_raw_bytes'],counts['real_requests'],1 if not counts['real_requests'] else 0
            if state=='DEFERRED':
                wait=max(0,claim['next_eligible_at']-self.clock())
                if self.replay_only or wait>60 or self.deadline is not None and self.clock()+wait>=self.deadline:
                    raise ProviderReadFailure('DEFERRED',counts,claim)
                self.sleep(wait); continue
            if state!='CLAIMED': raise ProviderReadFailure(state,counts,claim)
            attempt_id=claim['attempt_id']
            if self.replay_only:
                self.retry_store.abandon_before_dispatch(attempt_id,'CACHE_MISS_REPLAY_ONLY')
                raise ProviderReadFailure('CACHE_MISS_REPLAY_ONLY',counts,claim)
            if self.new_raw_bytes + self.page_size * 4096 > self.raw_limit_bytes:
                self.retry_store.abandon_before_dispatch(attempt_id,'RAW_BYTES_RESOURCE_LIMIT')
                raise ProviderReadFailure('RAW_BYTES_RESOURCE_LIMIT',counts,claim)
            try:
                accounting=self.attempt_hook(claim,dict(params)) if self.attempt_hook else {}
            except Exception:
                self.retry_store.abandon_before_dispatch(attempt_id,'BUDGET_BLOCKED')
                raise ProviderReadFailure('BUDGET_BLOCKED',counts,claim)
            self.retry_store.mark_dispatched(attempt_id,accounting=accounting)
            counts['real_requests']+=1
            response=None; exception_type=None
            try:
                response=self.transport(dict(params))
                classification=classify_response(response)
            except Exception as exc:
                exception_type=type(exc).__name__
                classification=classify_failure(exc,http_status=getattr(exc,'code',None),headers=getattr(exc,'headers',None))
            raw=json.dumps(response,sort_keys=True,separators=(',',':')).encode() if response is not None else b''
            self.new_raw_bytes+=len(raw); counts['new_raw_bytes']+=len(raw)
            attempt_path=self.cache_dir/'attempts'/f'{attempt_id}.json'; attempt_path.parent.mkdir(exist_ok=True)
            attempt_path.write_text(json.dumps({"request":params,"response":response,"classification":classification,"exception_type":exception_type,"attempt":claim},sort_keys=True),encoding='utf-8')
            receipt={"raw_path":attempt_path.relative_to(self.cache_dir).as_posix(),"response_sha256":hashlib.sha256(raw).hexdigest(),"raw_bytes":len(raw),"attempt_id":attempt_id,"retry_of":claim['retry_of']}
            self.retry_store.finish(attempt_id,classification['outcome'],payload=response if classification['outcome']=='SUCCESS' else None,error_class=classification.get('error_class'),retry_after=classification.get('retry_after'),receipt=receipt)
            if classification['outcome']=='SUCCESS':
                success_path=self.cache_dir/'success'/f"{claim['logical_key']}.json"; success_path.parent.mkdir(exist_ok=True)
                success_path.write_bytes(raw)
                return response,meta|receipt|{"logical_key":claim['logical_key']},counts['new_raw_bytes'],counts['real_requests'],0
            if classification['outcome']=='PERMANENT_FAILURE': raise ProviderReadFailure(classification.get('error_class','PERMANENT_FAILURE'),counts,receipt)

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
                    for name in counts:
                        counts[name] += getattr(exc, "counters", {}).get(name, 0)
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
            if self.detail_enricher is not None and rows:
                # Hook must use the same external budget ledger. It returns rows,
                # explicit gaps and counters; no hidden request is allowed.
                try:
                    rows, enrichment_gaps, enrichment_counts = self.detail_enricher(action, rows)
                    all_gaps.extend(enrichment_gaps)
                    for name in counts:
                        counts[name] += enrichment_counts.get(name, 0)
                except Exception as exc:
                    for name in counts:
                        counts[name] += getattr(exc, "counters", {}).get(name, 0)
                    all_gaps.append({"reason":"RECEIPT_ENRICHMENT_FAILED", "action":action, "exception_type":type(exc).__name__})
                    rows = []
            events, gaps = normalize_rows(action, rows, tx_positions)
            if action == "txlist":
                for event in events:
                    if event.tx_index is not None:
                        if event.tx_hash in tx_positions and tx_positions[event.tx_hash] != event.tx_index:
                            gaps.append({"reason":"TRANSACTION_POSITION_CONFLICT", "tx_hash":event.tx_hash})
                        else:
                            tx_positions[event.tx_hash] = event.tx_index
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
