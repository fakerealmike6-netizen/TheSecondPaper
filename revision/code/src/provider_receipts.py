"""Exact receipt enrichment. Known identities constrain, never get overwritten.

Only injected read I/O is used. Parent success cannot prove nested call success.
Conflicting receipt transactions produce gaps and no enriched transaction rows.
"""
from collections import Counter, defaultdict
import re
from exact_fields_r4 import exact_uint, field_uint, field_text, field_status, ExactFieldError

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

def _number(row, keys, required=False):
    values = {k:v for k,v in row.items() if not (k in keys and v == "")}
    return field_uint(values, keys, required=required, strict_null=True)

def _hash(row, keys, required=False):
    value = field_text(row, keys, required=required, lower=True)
    if value is not None and not re.fullmatch(r"0x[0-9a-f]{64}", value):
        raise ExactFieldError("INVALID", "|".join(keys))
    return value

def _equal(known, expected, field):
    if known is not None and known != expected:
        raise ExactFieldError("CONFLICT", field)

def _identity(row, tx, block, index, block_hash):
    _equal(_hash(row, ("hash", "transactionHash", "transaction_hash")), tx, "transactionHash")
    _equal(_number(row, ("blockNumber", "block_number")), block, "blockNumber")
    _equal(_number(row, ("transactionIndex", "tx_index", "transaction_index")), index, "transactionIndex")
    known_hash = _hash(row, ("blockHash", "block_hash"))
    if known_hash is not None:
        if block_hash is None: raise ExactFieldError("MISSING", "receipt.blockHash")
        _equal(known_hash, block_hash, "blockHash")
    _equal(_number(row, ("chainId", "chain_id")), 1, "chainId")

def _fill(row, key, aliases, value):
    result = dict(row)
    if _number(row, aliases) is None:
        result[key] = str(value)
        for alias in aliases:
            if alias != key and result.get(alias) in (None, ""):
                result.pop(alias, None)
    return result

def _transfer_key(row):
    fields = [field_text(row, (name,), required=True, lower=True) for name in ("contractAddress", "from", "to")]
    if any(not re.fullmatch(r"0x[0-9a-f]{40}", value) for value in fields):
        raise ExactFieldError("INVALID", "Transfer.address")
    return (*fields, field_uint(row, ("value",), strict_null=True))

def _status(row, keys, required=False):
    value = field_uint(row,keys,required=required,strict_null=True)
    if value is not None and value not in (0,1): raise ExactFieldError("INVALID","|".join(keys))
    return None if value is None else bool(value)

class ReceiptEnricher:
    def __init__(self, receipt_loader):
        self.receipt_loader, self.loaded = receipt_loader, {}

    def __call__(self, action, rows):
        counters = {"real_requests": 0, "cache_hits": 0, "new_raw_bytes": 0}
        if action not in ("tokentx", "txlistinternal"):
            return rows, [], counters
        grouped, output, gaps = defaultdict(list), [], []
        for row in rows:
            try:
                grouped[_hash(row, ("hash", "transactionHash"), True)].append(row)
            except (ValueError, TypeError, KeyError) as exc:
                gaps.append({"reason":"RECEIPT_INDEX_IDENTITY_INVALID", "field":getattr(exc,"field",None), "field_reason":getattr(exc,"reason_code","INVALID")})
        for tx, txrows in grouped.items():
            try:
                need = any(_number(r,("transactionIndex","tx_index")) is None or _number(r,("blockNumber","block_number")) is None or action == "tokentx" and _number(r,("logIndex","log_index")) is None for r in txrows)
                if not need:
                    output.extend(txrows)
                    continue
                if tx in self.loaded:
                    receipt = self.loaded[tx]
                    counters["cache_hits"] += 1
                else:
                    try:
                        receipt, counts = self.receipt_loader(tx)
                        for name in counters:
                            counters[name] += counts.get(name, 0)
                    except Exception as exc:
                        for name in counters:
                            counters[name] += getattr(exc, "counters", {}).get(name, 0)
                        gaps.append({"reason":"RECEIPT_DETAIL_UNAVAILABLE", "tx_hash":tx, "exception_type":type(exc).__name__})
                        continue
                if not isinstance(receipt, dict): raise ExactFieldError("INVALID", "receipt")
                _equal(_hash(receipt, ("transactionHash",), True), tx, "receipt.transactionHash")
                block = _number(receipt, ("blockNumber",), True)
                index = _number(receipt, ("transactionIndex",), True)
                block_hash = _hash(receipt, ("blockHash",))
                _equal(_number(receipt, ("chainId", "chain_id")), 1, "receipt.chainId")
                status = _status(receipt, ("status",), required=True)
                for row in txrows:
                    _identity(row, tx, block, index, block_hash)
                    known_status = _status(row, ("txreceipt_status", "receipt_status"))
                    _equal(known_status, status, "txreceipt_status")
                    if action == "tokentx":
                        error = _status(row, ("isError",))
                        _equal(None if error is None else not error, status, "isError")
                if action == "txlistinternal":
                    result = []
                    for row in txrows:
                        enriched = _fill(_fill(row, "transactionIndex", ("transactionIndex","tx_index"), index), "blockNumber", ("blockNumber","block_number"), block)
                        enriched["txreceipt_status"] = "1" if status else "0"
                        result.append(enriched)
                    output.extend(result)
                    self.loaded[tx] = receipt
                    continue
                if not status:
                    gaps.append({"reason":"TOKEN_RECEIPT_NOT_SUCCESSFUL", "tx_hash":tx})
                    continue
                expected = Counter(_transfer_key(row) for row in txrows)
                required_contracts = {key[0] for key in expected}
                by_key, observed, seen_indices = defaultdict(list), Counter(), set()
                logs = receipt.get("logs")
                if not isinstance(logs, list): raise ExactFieldError("INVALID", "receipt.logs")
                for log in logs:
                    if not isinstance(log, dict): raise ExactFieldError("INVALID", "receipt.log")
                    _identity(log, tx, block, index, block_hash)
                    log_index = _number(log, ("logIndex","log_index"), True)
                    if log_index in seen_indices: raise ExactFieldError("CONFLICT", "DUPLICATE_RECEIPT_LOG_INDEX")
                    seen_indices.add(log_index)
                    # Identity, emitter, global position and removal apply to
                    # every log, including logs outside this ERC20 request.
                    contract = field_text(log,("address",),required=True,lower=True)
                    if not re.fullmatch(r"0x[0-9a-f]{40}",contract): raise ExactFieldError("INVALID", "receipt.Transfer.address")
                    topics = log.get("topics", [])
                    if not isinstance(topics,list): raise ExactFieldError("INVALID", "receipt.log.topics")
                    if "removed" in log:
                        removed = log["removed"]
                        if type(removed) is not bool and not (isinstance(removed,str) and removed.lower() in ("true","false")):
                            raise ExactFieldError("INVALID", "receipt.log.removed")
                        if removed is True or isinstance(removed,str) and removed.lower() == "true":
                            raise ExactFieldError("CONFLICT", "receipt.log.removed")
                    # Contract relevance precedes ERC20 shape. Another emitter
                    # may use the Transfer signature with a different standard;
                    # it neither supplies nor invalidates this contract's rows.
                    if contract not in required_contracts:
                        continue
                    if not topics or not isinstance(topics[0],str) or topics[0].lower() != TRANSFER_TOPIC:
                        continue
                    if len(topics) != 3 or any(not isinstance(t,str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}",t) for t in topics) or not isinstance(log.get("data"),str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}",log["data"]):
                        raise ExactFieldError("INVALID", "receipt.Transfer.encoding")
                    if any(int(t[2:26],16) for t in topics[1:]): raise ExactFieldError("INVALID", "receipt.Transfer.addressPadding")
                    key = (contract, "0x"+topics[1][-40:].lower(), "0x"+topics[2][-40:].lower(), exact_uint(log["data"]))
                    if key in expected:
                        observed[key] += 1
                        by_key[key].append(log_index)
                if observed != expected:
                    for key in expected:
                        if observed[key] != expected[key]:
                            gaps.append({"reason":"INDEX_RECEIPT_TRANSFER_MULTIPLICITY_MISMATCH", "tx_hash":tx, "index_count":expected[key], "receipt_count":observed[key]})
                    continue
                assigned, used = [], set()
                for row in sorted(txrows, key=lambda r: _number(r,("logIndex","log_index")) is None):
                    key = _transfer_key(row)
                    known = _number(row,("logIndex","log_index"))
                    candidates = sorted(i for i in by_key[key] if i not in used)
                    if known is not None and known not in candidates: raise ExactFieldError("CONFLICT", "logIndex")
                    chosen = known if known is not None else candidates[0]
                    used.add(chosen)
                    enriched = _fill(_fill(_fill(row,"logIndex",("logIndex","log_index"),chosen),"transactionIndex",("transactionIndex","tx_index"),index),"blockNumber",("blockNumber","block_number"),block)
                    enriched["txreceipt_status"] = "1"
                    enriched["receipt_verified_transfer"] = True
                    assigned.append(enriched)
                output.extend(assigned)
                self.loaded[tx] = receipt
            except (ValueError, TypeError, KeyError, IndexError) as exc:
                gaps.append({"reason":"RECEIPT_IDENTITY_OR_SCHEMA_CONFLICT", "tx_hash":tx, "field":getattr(exc,"field",None), "field_reason":getattr(exc,"reason_code","INVALID"), "exception_type":type(exc).__name__})
        return output, gaps, counters
