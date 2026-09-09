"""Bounded native Transfers index adapter for the current Stage1D recovery.

No import-time I/O, no independent HTTP/ledger implementation and no graph run.
Use the existing budgeted RpcAccess with TransfersRuntime. Its permission hook
must apply permission_with_transfer_rate to the unchanged permission snapshot.
Transfers does not supply the complete normal/failed/zero-value/Gas ledger.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import re

from collector import Event, NATIVE
from stage1d_runtime import Runtime


METHOD = "alchemy_getAssetTransfers"
VERSION = "stage1d-alchemy-native-transfers-v1"
AUTHORIZATION_ID = "STAGE1D_RECOVERY_ROUTING_REUSE_V1"
TRANSFER_CU_UPPER = 120
PAGE_KEY_TTL_SECONDS = 600
CANARY_KINDS = {"ordinary_top", "internal", "empty", "pagination_or_boundary_duplicate"}
RATE_SOURCE = "https://www.alchemy.com/docs/data/transfers-api/transfers-endpoints/alchemy-get-asset-transfers"
PAGINATION_SOURCE = "https://www.alchemy.com/docs/reference/transfers-api-quickstart"


def _sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False).encode()).hexdigest()


def _hex(value, digits):
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{" + str(digits) + "}", value):
        raise ValueError("Exact hexadecimal locator required")
    return value.lower()


def _quantity(value):
    if not isinstance(value, str) or not re.fullmatch(r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)", value):
        raise ValueError("Exact hexadecimal integer required")
    return int(value, 16)


def _raw_integer(value):
    # Transfers rawContract.value is hexadecimal data and can be zero padded.
    # Ordinary Ethereum QUANTITY fields retain the stricter canonical grammar.
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]+", value):
        raise ValueError("Exact raw hexadecimal integer required")
    return int(value, 16)


def _uint(value):
    if type(value) is not int or value < 0:
        raise ValueError("Nonnegative exact integer required")
    return value


def _timestamp(value):
    if type(value) is int:
        return value
    if not isinstance(value, str):
        raise ValueError("Explicit UTC time required")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError("Explicit UTC time required")
    if parsed.microsecond:
        raise ValueError("Ethereum timestamp must have whole seconds")
    return int(parsed.timestamp())


def validate_need(need):
    required = {"query_id", "query_name", "scope_id", "scope_hash", "address", "asset",
                "direction", "start_block", "end_block", "start_time", "end_time",
                "fact_type", "gap_reason"}
    if not isinstance(need, dict) or not required <= set(need):
        raise ValueError("Current independently produced NEEDED_RANGE is required")
    if need["asset"] != NATIVE or need["direction"] != "OUTGOING" or need["fact_type"] != "POSITIVE_NATIVE_CANDIDATE_INDEX":
        raise ValueError("Transfers adapter is outgoing positive native candidate index only")
    if any(not isinstance(need[k], str) or not need[k] for k in
           ["query_id", "query_name", "scope_id", "scope_hash", "gap_reason"]):
        raise ValueError("Frozen query/scope and gap identities required")
    clean = deepcopy(need)
    clean["address"] = _hex(need["address"], 40)
    for key in ["start_block", "end_block"]:
        clean[key] = _uint(need[key])
    for key in ["start_time", "end_time"]:
        clean[key] = _timestamp(need[key])
    if clean["start_block"] > clean["end_block"] or clean["start_time"] > clean["end_time"]:
        raise ValueError("Inverted range")
    return clean


def bind_need_to_scope(need, scope):
    """Root preparation binds a demand to the existing frozen collector Scope."""
    clean = validate_need(need)
    if (clean["query_id"] != scope.query_id or clean["query_name"] != scope.name
            or clean["scope_id"] != scope.scope_id or clean["scope_hash"] != scope.scope_hash
            or not scope.start_block <= clean["start_block"] <= clean["end_block"] <= scope.end_block
            or not scope.start_time <= clean["start_time"] <= clean["end_time"] <= scope.local_end(clean["start_time"])):
        raise ValueError("Transfers demand differs from current frozen Scope")
    return clean


def request_plan(need, *, page_size=1000, page_key=None, timestamp_bracket=None, page_start_block=None):
    need = validate_need(need)
    if type(page_size) is not int or not 1 <= page_size <= 1000:
        raise ValueError("Transfers page size must be 1..1000")
    first, last = need['start_block'], need['end_block']
    if timestamp_bracket is not None:
        if (timestamp_bracket.get('logical_bounds') != {k: need[k] for k in ('start_block','end_block','start_time','end_time')}
                or timestamp_bracket.get('empty') is not False):
            raise ValueError('Nonempty exact physical timestamp bracket required for Transfers request')
        first, last = timestamp_bracket['first_block'], timestamp_bracket['last_block']
        if type(first) is not int or type(last) is not int or not need['start_block'] <= first <= last <= need['end_block']:
            raise ValueError('Physical request range exceeds logical need')
    if page_start_block is not None:
        if timestamp_bracket is None or type(page_start_block) is not int or not first <= page_start_block <= last:
            raise ValueError('Verified legacy-prefix continuation must stay in physical range')
        first = page_start_block
    params = {"fromBlock": hex(first), "toBlock": hex(last),
              "fromAddress": need["address"], "category": ["external", "internal"],
              "order": "asc", "maxCount": hex(page_size),
              "withMetadata": True, "excludeZeroValue": True}
    if page_key is not None:
        if not isinstance(page_key, str) or not page_key or len(page_key) > 1024:
            raise ValueError("Invalid page key")
        params["pageKey"] = page_key
    return {"method": METHOD, "params": [params]}


def validate_transfers_rpc(plan):
    if not isinstance(plan, dict) or set(plan) != {"method", "params"} or plan["method"] != METHOD:
        raise ValueError("Exact Transfers method/params required")
    if not isinstance(plan["params"], list) or len(plan["params"]) != 1 or not isinstance(plan["params"][0], dict):
        raise ValueError("One fixed Transfers parameter object required")
    p = plan["params"][0]
    keys = {"fromBlock", "toBlock", "fromAddress", "category", "order", "maxCount", "withMetadata", "excludeZeroValue"}
    if not keys <= set(p) or set(p) - keys - {"pageKey"}:
        raise ValueError("No moving range, incoming filter, token filter or unapproved parameter")
    if (_quantity(p["fromBlock"]) > _quantity(p["toBlock"])
            or not 1 <= _quantity(p["maxCount"]) <= 1000):
        raise ValueError("Invalid fixed range or page size")
    _hex(p["fromAddress"], 40)
    if (p["category"] != ["external", "internal"] or p["order"] != "asc"
            or p["withMetadata"] is not True or p["excludeZeroValue"] is not True):
        raise ValueError("Frozen positive native index semantics required")
    if "pageKey" in p and (not isinstance(p["pageKey"], str) or not p["pageKey"] or len(p["pageKey"]) > 1024):
        raise ValueError("Invalid page key")
    return deepcopy(plan)


def _row_core(row):
    """Display float, including rounded ETH value, is never a physical fact."""
    return {k: deepcopy(row.get(k)) for k in
            ("blockNum", "uniqueId", "hash", "from", "to", "category", "asset", "rawContract", "metadata")}


def transfers_result_status(request, response):
    try:
        plan = validate_transfers_rpc({k: request[k] for k in ("method", "params")})
        if (not isinstance(response, dict) or response.get("jsonrpc") != "2.0"
                or response.get("id") != request["id"] or type(response.get("id")) is not type(request["id"])
                or ("result" in response) == ("error" in response)):
            return "INVALID_RPC_BINDING"
        if "error" in response:
            return "RPC_ERROR"
        result, p = response["result"], plan["params"][0]
        if not isinstance(result, dict) or not isinstance(result.get("transfers"), list):
            return "INVALID_TRANSFERS_RESULT"
        key = result.get("pageKey")
        if key not in (None, "") and (not isinstance(key, str) or len(key) > 1024):
            return "INVALID_TRANSFERS_PAGE_KEY"
        if len(result["transfers"]) > _quantity(p["maxCount"]):
            return "TRANSFERS_PAGE_SIZE_EXCEEDED"
        previous = -1
        for row in result["transfers"]:
            if not isinstance(row, dict):
                return "INVALID_TRANSFERS_ROW"
            block = _quantity(row["blockNum"])
            if not _quantity(p["fromBlock"]) <= block <= _quantity(p["toBlock"]) or block < previous:
                return "TRANSFERS_RANGE_OR_ORDER_MISMATCH"
            previous = block
            _hex(row["hash"], 64)
            if _hex(row["from"], 40) != p["fromAddress"].lower():
                return "TRANSFERS_ADDRESS_MISMATCH"
            _hex(row["to"], 40)
            if row.get("category") not in ("external", "internal") or row.get("asset") != "ETH":
                return "TRANSFERS_NATIVE_CATEGORY_MISMATCH"
            if not isinstance(row.get("uniqueId"), str) or not row["uniqueId"]:
                return "TRANSFERS_INDEX_ID_MISSING"
            _timestamp(row["metadata"]["blockTimestamp"])
            raw = row.get("rawContract")
            if raw is not None and not isinstance(raw, dict):
                return "INVALID_TRANSFERS_RAW_CONTRACT"
            if isinstance(raw, dict):
                if raw.get("address") is not None:
                    return "TRANSFERS_NATIVE_CATEGORY_MISMATCH"
                if raw.get("decimal") is not None and _quantity(raw["decimal"]) != 18:
                    return "TRANSFERS_NATIVE_DECIMALS_MISMATCH"
                if raw.get("value") is not None:
                    # Full raw integer is optional. Never substitute displayed value.
                    _raw_integer(raw["value"])
        return "SUCCESS_VALIDATED"
    except (ValueError, KeyError, TypeError, OverflowError):
        return "INVALID_TRANSFERS_RESULT"


class TransfersRuntime(Runtime):
    """Only the new method is extended; all existing identities/validation delegate."""
    def rpc_permission(self, permission):
        # RpcAccess has already verified the original permission snapshot.
        return permission_with_transfer_rate(permission)

    def validate_rpc(self, plan):
        if isinstance(plan, dict) and plan.get("method") == METHOD:
            return validate_transfers_rpc(plan)
        return super().validate_rpc(plan)

    def rpc_identity(self, provider, plan):
        if isinstance(plan, dict) and plan.get("method") == METHOD:
            return {"provider": provider, "chain": 1, **self.validate_rpc(plan)}
        return super().rpc_identity(provider, plan)

    def rpc_result_status(self, request, response):
        if isinstance(request, dict) and request.get("method") == METHOD:
            return transfers_result_status(request, response)
        return super().rpc_result_status(request, response)


def permission_with_transfer_rate(permission):
    """After inherited permission() validation, extend only a returned copy.

    The root recovery accessor records the new authority/rate proof separately;
    this helper cannot refill allowance or rewrite the old permission hash.
    """
    result = deepcopy(permission)
    rates = result.get("method_cu_upper_bounds")
    if not isinstance(rates, dict):
        raise ValueError("Existing validated permission rates required")
    if METHOD in rates and rates[METHOD] != TRANSFER_CU_UPPER:
        raise ValueError("Conflicting Transfers CU evidence")
    rates[METHOD] = TRANSFER_CU_UPPER
    return result


class TransferPageChain:
    """In-memory page chain; caller persists each successful RPC receipt immediately."""
    def __init__(self, need, *, page_size=1000, timestamp_bracket=None, bracket_headers=None):
        self.need = validate_need(need)
        self.page_size = page_size
        self.timestamp_bracket = deepcopy(timestamp_bracket)
        if timestamp_bracket is not None:
            from stage1d_timestamp_bracket import verify_timestamp_bracket
            verify_timestamp_bracket(self.need, timestamp_bracket, bracket_headers or {})
        self.empty_time_intersection = bool(timestamp_bracket and timestamp_bracket['empty'])
        self.base_plan = None if self.empty_time_intersection else request_plan(self.need, page_size=page_size, timestamp_bracket=timestamp_bracket)
        self.legacy_superset_summary = None
        self.legacy_prefix_summary = None
        self.page_start_block = None
        self._required_legacy_boundary_rows = {}
        self.pages = []
        self.rows = []
        self._seen_rows = {}
        self._seen_keys = set()
        self.duplicates = 0
        self.closed = self.empty_time_intersection
        self.next_key = None

    def next_plan(self, now_seconds):
        if self.closed:
            raise ValueError("Closed page chain has no next request")
        if self.next_key:
            origin = self.pages[-1]["cursor_origin_seconds"]
            if origin is None:
                raise ValueError("PAGE_KEY_AGE_UNKNOWN_REQUIRES_EXPLICIT_OVERLAP_RECOVERY")
            if now_seconds - origin >= PAGE_KEY_TTL_SECONDS:
                raise ValueError("PAGE_KEY_EXPIRED_REQUIRES_EXPLICIT_OVERLAP_RECOVERY")
        return request_plan(self.need, page_size=self.page_size, page_key=self.next_key,
                            timestamp_bracket=self.timestamp_bracket, page_start_block=self.page_start_block)

    def append(self, plan, member, *, received_at_seconds, requested_at_seconds=None):
        if self.closed:
            raise ValueError("Cannot append after natural exhaustion")
        if plan != request_plan(self.need, page_size=self.page_size, page_key=self.next_key,
                                timestamp_bracket=self.timestamp_bracket, page_start_block=self.page_start_block):
            raise ValueError("Page does not continue the exact fixed request")
        if self.next_key and requested_at_seconds is None:
            raise ValueError("Subsequent page requires its actual request time")
        if requested_at_seconds is not None and self.pages:
            origin = self.pages[-1]["cursor_origin_seconds"]
            if origin is None or requested_at_seconds - origin >= PAGE_KEY_TTL_SECONDS:
                raise ValueError("Expired page key request is not accepted as successful continuation")
        if member.get("status") != "SUCCESS_VALIDATED" or not isinstance(member.get("result"), dict):
            raise ValueError("Failed/missing page cannot close coverage")
        for key in ("artifact_path", "artifact_sha256"):
            if not isinstance(member.get(key), str) or not member[key]:
                raise ValueError("Durable successful RPC member evidence required")
        if not re.fullmatch(r"[a-fA-F0-9]{64}", member["artifact_sha256"]):
            raise ValueError("RPC artifact SHA required")
        synthetic_request = {"jsonrpc":"2.0", "id":"page-validation", **plan}
        status = transfers_result_status(synthetic_request,
            {"jsonrpc":"2.0", "id":"page-validation", "result":member["result"]})
        if status != "SUCCESS_VALIDATED":
            raise ValueError(status)
        result = member["result"]
        next_key = result.get("pageKey") or None
        if next_key is not None and next_key in self._seen_keys:
            raise ValueError("Page-key cycle cannot prove exhaustion")
        if self.rows and result["transfers"] and _quantity(result["transfers"][0]["blockNum"]) < _quantity(self.rows[-1]["blockNum"]):
            raise ValueError("Cross-page block ordering mismatch")
        # Validate before mutation so a conflicting page cannot partly advance state.
        seen = dict(self._seen_rows)
        fresh = []
        duplicates = 0
        for row in result["transfers"]:
            index_id = row["uniqueId"]
            core = _row_core(row)
            if index_id in self._required_legacy_boundary_rows and core != self._required_legacy_boundary_rows[index_id]:
                raise ValueError('Legacy boundary index fact conflicts with new exact range')
            if index_id in seen:
                if seen[index_id] != core:
                    raise ValueError("Contradictory index identity across pages")
                duplicates += 1
            else:
                seen[index_id] = core
                fresh.append(deepcopy(row))
        if next_key is None and not set(self._required_legacy_boundary_rows).issubset(seen):
            raise ValueError('Legacy boundary fact missing from newly exhausted exact range')
        self._seen_rows = seen
        self.rows.extend(fresh)
        self.duplicates += duplicates
        if next_key is not None:
            self._seen_keys.add(next_key)
        self.pages.append({"plan":deepcopy(plan), "result_sha256":_sha(result),
            "artifact_path":member["artifact_path"], "artifact_sha256":member["artifact_sha256"],
            "received_at_seconds":received_at_seconds, "requested_at_seconds":requested_at_seconds,
            "cursor_origin_seconds":member.get("origin_received_at_seconds") if member.get("cache_hit") is True else received_at_seconds,
            "row_count":len(result["transfers"]), "next_page_key":next_key,
            "cache_hit":member.get("cache_hit") is True})
        self.next_key = next_key
        self.closed = next_key is None

    def recovery_range(self):
        """Proposal only. Keep the last touched block because its page may be partial."""
        if self.closed:
            return None
        recovered = deepcopy(self.need)
        if self.rows:
            recovered["start_block"] = max(self.need["start_block"], _quantity(self.rows[-1]["blockNum"]))
        return {"needed_range": recovered, "retained_prefix_through_block":recovered["start_block"] - 1,
            "last_touched_block_requeried":True, "requires_persisted_recovery_decision":True,
            "original_need_sha256":_sha(self.need), "does_not_claim_original_range_closed":True}

    def summary(self):
        result = {"version":VERSION, "needed_range":deepcopy(self.need), "pages":deepcopy(self.pages),
            "page_chain_naturally_exhausted":self.closed, "unique_index_rows":len(self.rows),
            "duplicate_index_rows":self.duplicates, "next_page_key":self.next_key,
            "context_complete":False, "fees_zero_and_failed_transaction_ledger_complete":False,
            "strict_internal_trace_coverage_complete":False}
        if self.timestamp_bracket is not None:
            result['timestamp_bracket'] = deepcopy(self.timestamp_bracket)
            result['page_chain_naturally_exhausted'] = self.closed and not self.empty_time_intersection and self.legacy_superset_summary is None
            result['logical_index_complete'] = self.closed
            result['closure_basis'] = ('VERIFIED_EMPTY_TIMESTAMP_INTERSECTION' if self.empty_time_intersection else
                'VERIFIED_LEGACY_SUPERSET_OR_EXHAUSTED_PREFIX' if self.legacy_superset_summary is not None else 'EXACT_PHYSICAL_REQUEST_PAGE_CHAIN')
            if self.legacy_superset_summary is not None:
                result['legacy_superset_summary'] = deepcopy(self.legacy_superset_summary)
            if self.legacy_prefix_summary is not None:
                result['legacy_prefix_summary'] = deepcopy(self.legacy_prefix_summary)
                result['physical_page_start_block'] = self.page_start_block
        return result


def _bound_top(row, transactions, receipts, headers):
    tx_hash = _hex(row["hash"], 64)
    tx, receipt = transactions[tx_hash], receipts[tx_hash]
    block = _quantity(row["blockNum"])
    header = headers.get(block, headers.get(str(block)))
    if not isinstance(header, dict):
        raise ValueError("Block header missing")
    block_hash = _hex(header["hash"], 64)
    tx_index = _quantity(tx["transactionIndex"])
    if (_hex(tx["hash"],64) != tx_hash or _hex(receipt["transactionHash"],64) != tx_hash
            or _quantity(tx["blockNumber"]) != block or _quantity(receipt["blockNumber"]) != block
            or _quantity(header["number"]) != block
            or _hex(tx["blockHash"],64) != block_hash or _hex(receipt["blockHash"],64) != block_hash
            or _quantity(receipt["transactionIndex"]) != tx_index):
        raise ValueError("Top tx/receipt/header binding differs")
    hashes = header.get("transactions")
    if not isinstance(hashes,list) or tx_index >= len(hashes) or _hex(hashes[tx_index],64) != tx_hash:
        raise ValueError("Full header transaction order binding missing")
    if _hex(tx["from"],40) != _hex(row["from"],40) or _hex(tx["to"],40) != _hex(row["to"],40):
        raise ValueError("Top endpoint binding differs")
    timestamp = _quantity(header["timestamp"])
    if _timestamp(row["metadata"]["blockTimestamp"]) != timestamp:
        raise ValueError("Index/header time conflict")
    amount = _quantity(tx["value"])
    raw = row.get("rawContract") or {}
    if raw.get("value") is not None and _raw_integer(raw["value"]) != amount:
        raise ValueError("Raw index value conflicts with transaction integer")
    if _quantity(receipt["status"]) != 1 or amount <= 0:
        raise ValueError("Index positive successful top transaction not established")
    used, price = _quantity(receipt["gasUsed"]), _quantity(receipt["effectiveGasPrice"])
    return Event("eip155:1:tx:"+tx_hash+":top",tx_hash,_hex(tx["from"],40),_hex(tx["to"],40),
        NATIVE,amount,block,tx_index,timestamp,"top",success=True,provenance=VERSION,
        gas_raw=used*price,block_hash=block_hash,gas_used=used,gas_price=price)


def normalize_chain(chain, *, transactions, receipts, headers, strict_internal_events=(),
                    complete_trace_transactions=()):
    """Bind index rows to current hash-verified RPC/strict trace facts supplied by caller.

    complete_trace_transactions names tx whose full relevant trace/ancestor set
    is independently proved. It cannot be inferred from Transfers uniqueIds.
    """
    if not isinstance(chain, TransferPageChain):
        raise ValueError("Validated page chain required")
    verified_traces = [e if isinstance(e,Event) else Event(**e) for e in strict_internal_events]
    trace_variants = {}
    for event in verified_traces:
        # Repeated source annotations are harmless; contradictory physical facts
        # must not be silently selected by a dictionary's last-wins behavior.
        value = asdict(event)
        value.pop("provenance",None)
        if event.event_id in trace_variants and trace_variants[event.event_id] != value:
            raise ValueError("Conflicting supplied strict trace facts require current registry reconciliation")
        trace_variants[event.event_id] = value
    complete_txs = {_hex(tx,64) for tx in complete_trace_transactions}
    events, gaps, index_binding, event_bindings = {}, [], {}, {}
    for n,row in enumerate(chain.rows):
        try:
            if row["category"] == "external":
                event = _bound_top(row,transactions,receipts,headers)
            else:
                tx_hash, block = _hex(row["hash"],64), _quantity(row["blockNum"])
                if tx_hash not in complete_txs:
                    raise ValueError("Complete strict trace ancestry not available")
                raw_value = (row.get("rawContract") or {}).get("value")
                amount = _raw_integer(raw_value) if raw_value is not None else None
                candidates = [e for e in verified_traces if e.kind == "internal" and e.tx_hash == tx_hash
                    and e.asset == NATIVE and e.block == block and e.success is True and e.amount_raw > 0
                    and e.sender == _hex(row["from"],40) and e.recipient == _hex(row["to"],40)
                    and e.timestamp == _timestamp(row["metadata"]["blockTimestamp"])
                    and e.trace_address is not None and (amount is None or e.amount_raw == amount)]
                by_id = {e.event_id:e for e in candidates}
                if len(by_id) != 1:
                    raise ValueError("No unique strict trace binding; never guess position")
                event = next(iter(by_id.values()))
            if not chain.need["start_time"] <= event.timestamp <= chain.need["end_time"]:
                # Block-range index can include boundary-time rows. Keep raw but
                # preserve exact current time bounds when returning candidates.
                continue
            index_id = row["uniqueId"]
            prior_index = event_bindings.get(event.event_id)
            if prior_index is not None and prior_index != index_id:
                events.pop(event.event_id,None)
                index_binding.pop(prior_index,None)
                raise ValueError("Multiple index identities cannot be bound to one physical event")
            event_bindings[event.event_id] = index_id
            if event.event_id in events and asdict(events[event.event_id]) != asdict(event):
                raise ValueError("Conflicting physical binding")
            events[event.event_id] = event
            index_binding[index_id] = event.event_id
        except (ValueError,KeyError,TypeError,IndexError) as exc:
            gaps.append({"reason":"TOP_RPC_BINDING_UNRESOLVED" if row.get("category")=="external" else "TRACE_POSITION_OR_ANCESTRY_UNRESOLVED",
                "index_row":n,"index_id":row.get("uniqueId"),"tx_hash":row.get("hash"),
                "detail":str(exc),"fallback":"CLASSIC_BIGQUERY_OR_VALIDATED_DUNE_NATIVE"})
    if not chain.closed:
        gaps.append({"reason":"TRANSFERS_PAGE_CHAIN_NOT_EXHAUSTED","next_page_key":chain.next_key})
    return {"events":[asdict(e) for e in sorted(events.values(),key=lambda e:e.stable_key())],
        "gaps":gaps,"index_to_physical_bindings":index_binding,"index_chain":chain.summary(),
        "positive_native_index_complete":chain.closed and not gaps,"context_complete":False,
        "normal_failed_zero_value_gas_ledger_required":True}


def compare_canary(kind, need, normalized, reference_events, reference_proof):
    """Compare with an independently verified current complete range, never experts.

    Caller must first hash-validate reference_proof.evidence_refs and its complete
    source export. This function validates the declared exact comparison scope.
    """
    if kind not in CANARY_KINDS:
        raise ValueError("Unknown required canary class")
    need = validate_need(need)
    scope_keys = ("query_id","scope_id","scope_hash","address","asset","direction",
                  "start_block","end_block","start_time","end_time")
    if (reference_proof.get("complete") is not True
            or reference_proof.get("source") != "CURRENT_VERIFIED_DUNE_RPC"
            or reference_proof.get("source_hashes_verified") is not True
            or not reference_proof.get("evidence_refs")
            or any(reference_proof.get("needed_range",{}).get(k) != need[k] for k in scope_keys)):
        raise ValueError("Exact existing complete Dune/RPC comparison evidence required")
    for ref in reference_proof["evidence_refs"]:
        if not isinstance(ref.get("path"),str) or not re.fullmatch(r"[a-fA-F0-9]{64}",ref.get("sha256","")):
            raise ValueError("Comparison provenance reference required")
    if any(normalized.get("index_chain",{}).get("needed_range",{}).get(k) != need[k] for k in scope_keys):
        raise ValueError("Canary index does not cover declared request")
    reference = [e if isinstance(e,Event) else Event(**e) for e in reference_events]
    reference = [e for e in reference if e.asset==NATIVE and e.success and e.amount_raw>0
        and e.sender==need["address"] and need["start_block"]<=e.block<=need["end_block"]
        and need["start_time"]<=e.timestamp<=need["end_time"] and e.kind in ("top","internal")]
    observed = [Event(**e) for e in normalized["events"]]
    if kind=="ordinary_top" and not any(e.kind=="top" for e in reference):
        raise ValueError("Top canary must include a positive known top event")
    if kind=="internal" and not any(e.kind=="internal" for e in reference):
        raise ValueError("Internal canary must include a positive known internal event")
    if kind=="empty" and reference:
        raise ValueError("Empty canary reference is not empty")
    if kind=="pagination_or_boundary_duplicate" and not (
        len(normalized["index_chain"]["pages"])>1 or normalized["index_chain"]["duplicate_index_rows"]>0):
        raise ValueError("Pagination/boundary canary must actually exercise pagination")
    def signature(e):
        return (e.event_id,e.tx_hash,e.sender,e.recipient,e.asset,e.amount_raw,e.block,e.tx_index,
                e.timestamp,e.kind,e.trace_address,e.success)
    expected, actual = {signature(e) for e in reference}, {signature(e) for e in observed}
    closed = normalized["index_chain"]["page_chain_naturally_exhausted"] is True
    good = closed and not normalized["gaps"] and expected==actual
    return {"canary_kind":kind,"status":"PASS" if good else "MISMATCH_FALLBACK",
        "needed_range":need,"reference_proof":deepcopy(reference_proof),"expected_events":len(expected),
        "observed_events":len(actual),"missing_event_ids":sorted(x[0] for x in expected-actual),
        "unexpected_event_ids":sorted(x[0] for x in actual-expected),"gaps":deepcopy(normalized["gaps"]),
        "page_chain_naturally_exhausted":closed,"context_complete":False,
        "result_sha256":_sha(normalized),"fallback":"CLASSIC_BIGQUERY_OR_VALIDATED_DUNE_NATIVE"}


def route_gate(canary_results):
    by_kind = {r["canary_kind"]:r for r in canary_results}
    if set(by_kind)!=CANARY_KINDS or len(canary_results)!=4:
        raise ValueError("All four distinct actual canaries required")
    ordinary = all(by_kind[k]["status"]=="PASS" for k in CANARY_KINDS-{"internal"})
    internal = ordinary and by_kind["internal"]["status"]=="PASS"
    return {"authorization_id":AUTHORIZATION_ID,"canary_results_sha256":_sha(canary_results),
        "external_route":"ALCHEMY_TRANSFERS" if ordinary else "VALIDATED_DUNE_NATIVE",
        "internal_route":"ALCHEMY_INDEX_WITH_STRICT_TRACE_BINDING" if internal else "CLASSIC_BIGQUERY_OR_VALIDATED_DUNE_NATIVE",
        "all_four_pass":ordinary and internal,"context_complete":False,
        "cost_per_individual_transfer_request_cu_upper":TRANSFER_CU_UPPER,"rate_source":RATE_SOURCE}


def fetch_next_page(access, chain, *, clock, label, deadline=None):
    """Optional root-owned live hook: one persisted/R4-budgeted member only.

    It is never invoked at import, by normalization, by comparison or by tests.
    The root caller enforces adapter canary gate before ordinary acquisition.
    """
    plan = chain.next_plan(clock())
    dispatched_at = clock()
    result = access.call_batch([plan],chain.need["query_name"],label,
                               capability=False,deadline=deadline)
    if len(result.get("members",[])) != 1:
        raise ValueError("One Transfers member result required")
    member = result["members"][0]
    if member.get("status")=="SUCCESS_VALIDATED":
        chain.append(plan,member,received_at_seconds=clock(),requested_at_seconds=dispatched_at)
    return result
