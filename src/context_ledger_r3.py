"""Evidence-preserving finite ETH context ledger for Stage1B-R3.

This module performs no network requests. Amounts are integer wei; an observed
block-end balance is never relabelled as an event-pre balance.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path


class EvidenceConflict(ValueError):
    """Two physical facts disagree; this is not missing information."""


def integer(value, *, allow_none=False):
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError("Amounts and positions must be exact integers")
    return int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)


def first(row, *keys, default=None):
    return next((row[k] for k in keys if row.get(k) is not None), default)


def truth(value):
    if value in (True, 1, "1", "0x1", "true", "True"):
        return True
    if value in (False, 0, "0", "0x0", "false", "False"):
        return False
    if value is None:
        return None
    raise ValueError("Unrecognized success value")


def trace_path(value):
    if value is None:
        return None
    if isinstance(value, list):
        return tuple(integer(x) for x in value)
    text = str(value).strip()
    if text in ("", "[]", "{}"):
        return ()
    if text.startswith("["):
        return tuple(integer(x) for x in json.loads(text))
    return tuple(integer(x.strip()) for x in text.strip("{}").split(","))


def account(address):
    return str(address).lower() + "|ETH" if address else None


def _evidence_ids(row, fallback):
    value = row.get("evidence_ids") or row.get("provenance") or [fallback]
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            value = [value]
    return sorted(set(value))


def decode_saved_response(document, identity):
    """Decode saved Dune results and JSON-RPC responses without fetching data.

    The caller supplies the actual saved response identity, not a fabricated
    network identity. RPC transaction responses may be joined to receipts.
    """
    if isinstance(document, list):
        return [dict(row, evidence_ids=sorted(set(row.get("evidence_ids", []) + [identity]))) for row in document]
    rows = document.get("result", {}).get("rows") if isinstance(document.get("result"), dict) else None
    if rows is None:
        rows = document.get("rows")
    if rows is not None:
        return [dict(row, evidence_ids=sorted(set(row.get("evidence_ids", []) + [identity]))) for row in rows]
    raise ValueError("Saved response has no recognized row table")


def normalize_anchor(request, response, block_response=None, evidence_ids=None):
    """Normalize eth_getBalance at an explicitly fixed block or hash.

    Missing RPC result is not zero. Block identity must come from a separately
    saved corresponding block response; a mismatch is a hard fact conflict.
    """
    if request.get("method") != "eth_getBalance":
        raise ValueError("Unsupported balance method")
    params = request["params"]
    selector = params[1]
    if selector in ("latest", "pending", "safe", "finalized", "earliest"):
        raise ValueError("Balance anchor needs a fixed historical block")
    if response.get("error") or response.get("result") is None:
        raise ValueError("No successful historical balance response")
    result = block_response.get("result", block_response) if block_response else None
    if isinstance(selector, dict):
        block_hash = selector["blockHash"].lower()
        if result is None or result["hash"].lower() != block_hash:
            raise EvidenceConflict("Hash-anchored balance block identity mismatch")
        block_number = integer(result["number"])
    else:
        block_number = integer(selector)
        block_hash = result["hash"].lower() if result else None
        if result and integer(result["number"]) != block_number:
            raise EvidenceConflict("Historical balance block number mismatch")
    amount = integer(response["result"])
    if amount < 0:
        raise EvidenceConflict("Negative balance anchor")
    address = params[0].lower()
    return {"anchor_id": f"balance:eip155:1:{address}:{block_number}:BLOCK_END", "account_id": account(address), "address": address, "asset": "ETH", "block_number": block_number, "block_hash": block_hash, "position": "BLOCK_END", "actual_balance_raw": str(amount), "raw_value": response["result"], "request_id": request.get("id"), "verification_status": "BLOCK_IDENTITY_BOUND" if block_hash else "BLOCK_NUMBER_BOUND_HASH_MISSING", "evidence_ids": list(evidence_ids or [])}


def normalize_rows(rows):
    """Normalize all transaction/trace facts, then deduplicate by physical ID.

    Transaction root traces are the same value movement as top-level value.
    A failed ancestor rolls back successful-looking descendants. Delegatecalls
    carry a call-context value but do not move that amount again.
    """
    transactions, trace_rows, other, conflicts, excluded = {}, [], [], [], []

    def merge(table, key, item, physical_fields):
        if key in table:
            prior = table[key]
            disagreement = [field for field in physical_fields if prior.get(field) is not None and item.get(field) is not None and prior[field] != item[field]]
            if disagreement:
                conflicts.append({"physical_id": key, "fields": disagreement, "left": prior, "right": item})
                return
            for field, value in item.items():
                if prior.get(field) is None:
                    prior[field] = value
            prior["evidence_ids"] = sorted(set(prior["evidence_ids"] + item["evidence_ids"]))
        else:
            table[key] = item

    for index, raw in enumerate(rows):
        kind = str(first(raw, "record_type", "kind", default="transaction")).lower()
        if kind in ("block", "withdrawal", "protocol_credit", "fee_recipient", "coverage"):
            other.append(raw)
            continue
        tx_hash = str(first(raw, "tx_hash", "hash", "transactionHash", default="")).lower()
        if not tx_hash:
            raise ValueError("Transaction fact missing tx hash")
        block = integer(first(raw, "block_number", "block", "blockNumber"))
        tx_index = integer(first(raw, "tx_index", "index", "transactionIndex"), allow_none=True)
        sender = str(first(raw, "from_address", "sender", "from", default="")).lower()
        recipient = first(raw, "to_address", "recipient", "to")
        recipient = str(recipient).lower() if recipient else None
        amount = integer(first(raw, "value_raw", "amount_raw", "value", default=0))
        status = truth(first(raw, "success", "tx_success", "status"))
        evidence = _evidence_ids(raw, f"saved_row:{index}")
        common = {"tx_hash": tx_hash, "block_number": block, "block_hash": first(raw, "block_hash", "blockHash"), "tx_index": tx_index, "sender": sender, "recipient": recipient, "amount_raw": str(amount), "success": status, "evidence_ids": evidence}
        path = trace_path(first(raw, "trace_address", "traceAddress"))
        if kind in ("transaction", "top", "tx", "receipt"):
            gas_used = integer(first(raw, "gas_used", "gasUsed"), allow_none=True)
            gas_price = integer(first(raw, "effective_gas_price", "effectiveGasPrice", "gas_price", "gasPrice"), allow_none=True)
            fee = integer(first(raw, "gas_raw", "fee_raw"), allow_none=True)
            calculated = gas_used * gas_price if gas_used is not None and gas_price is not None else None
            if fee is not None and calculated is not None and fee != calculated:
                conflicts.append({"physical_id": "fee:" + tx_hash, "reason": "gas arithmetic mismatch", "row": raw})
            common.update({"gas_used": gas_used, "effective_gas_price": gas_price, "fee_raw": str(calculated if calculated is not None else fee) if calculated is not None or fee is not None else None, "input_data": first(raw, "input_data", "input")})
            merge(transactions, tx_hash, common, ("block_number", "block_hash", "tx_index", "sender", "recipient", "amount_raw", "success", "fee_raw"))
        elif kind in ("trace", "internal", "call", "create", "suicide", "selfdestruct"):
            common.update({"trace_address": path, "trace_type": str(first(raw, "trace_type", "type", default=kind)).lower(), "call_type": str(first(raw, "call_type", "callType", default="call")).lower(), "error": raw.get("error"), "subtraces": integer(first(raw, "subtraces", "sub_traces"), allow_none=True), "ancestor_success_verified": raw.get("ancestor_success_verified", False)})
            if common["trace_type"] in ("create", "create2"):
                common["recipient"] = first(raw, "created_address", "address", default=recipient)
            elif common["trace_type"] in ("suicide", "selfdestruct"):
                common["sender"] = first(raw, "created_address", "address", default=sender)
                common["recipient"] = first(raw, "refund_address", "refundAddress", default=recipient)
            trace_rows.append(common)
        else:
            excluded.append({"reason": "NON_NATIVE_OR_UNSUPPORTED_RECORD", "row": raw})
    traces = {}
    for row in trace_rows:
        if row["trace_address"] is None:
            excluded.append({"reason": "TRACE_PATH_MISSING_NO_HASH_ORDER", "row": row})
            continue
        key = row["tx_hash"] + ":" + ",".join(map(str, row["trace_address"]))
        merge(traces, key, row, ("block_number", "block_hash", "tx_index", "sender", "recipient", "amount_raw", "success", "call_type"))
    for key, row in traces.items():
        if row["subtraces"] is not None:
            children = [child for child in traces.values() if child["tx_hash"] == row["tx_hash"] and len(child["trace_address"]) == len(row["trace_address"]) + 1 and child["trace_address"][:-1] == row["trace_address"]]
            if len(children) != row["subtraces"]:
                conflicts.append({"physical_id": key, "reason": "TRACE_TREE_CHILD_COUNT_CONFLICT", "expected_children": row["subtraces"], "observed_children": len(children), "evidence_ids": row["evidence_ids"]})
    for tx_hash, tx in transactions.items():
        root = traces.get(tx_hash + ":")
        if tx["recipient"] is None and root and root["trace_type"] in ("create", "create2"):
            tx["recipient"] = root["recipient"]
    flows = {}
    for tx_hash, tx in transactions.items():
        if tx["success"] is True and integer(tx["amount_raw"]) > 0:
            event = dict(tx, event_id=f"eip155:1:tx:{tx_hash}:top", flow_kind="top", trace_address=[])
            flows[event["event_id"]] = event
        elif tx["success"] is not True:
            excluded.append({"reason": "FAILED_OR_UNKNOWN_TOP_VALUE", "tx_hash": tx_hash, "success": tx["success"], "fee_retained": tx["fee_raw"] is not None})
    for row in traces.values():
        tx_hash, path = row["tx_hash"], row["trace_address"]
        if row["call_type"] in ("delegatecall", "callcode", "staticcall"):
            excluded.append({"reason": "CALL_CONTEXT_VALUE_NOT_PHYSICAL_TRANSFER", "row": row})
            continue
        tx = transactions.get(tx_hash)
        if tx and tx["success"] is not True:
            excluded.append({"reason": "FAILED_TRANSACTION_ROLLBACK", "row": row})
            continue
        ancestors = [traces.get(tx_hash + ":" + ",".join(map(str, path[:length]))) for length in range(len(path))]
        failed = any(parent and (parent["success"] is False or parent["error"]) for parent in ancestors)
        if row["success"] is not True or row["error"] or failed:
            excluded.append({"reason": "FAILED_FRAME_OR_ANCESTOR_ROLLBACK", "row": row})
            continue
        if not path:
            if tx:
                if (row["sender"], row["recipient"], row["amount_raw"]) != (tx["sender"], tx["recipient"], tx["amount_raw"]):
                    conflicts.append({"physical_id": tx_hash + ":top", "reason": "ROOT_TRACE_TOP_CONFLICT", "left": tx, "right": row})
                elif tx_hash and f"eip155:1:tx:{tx_hash}:top" in flows:
                    key = f"eip155:1:tx:{tx_hash}:top"
                    flows[key]["evidence_ids"] = sorted(set(flows[key]["evidence_ids"] + row["evidence_ids"]))
                excluded.append({"reason": "ROOT_TRACE_TOP_PHYSICAL_DUPLICATE", "row": row})
                continue
        if path and not row["ancestor_success_verified"] and any(parent is None for parent in ancestors):
            excluded.append({"reason": "ANCESTOR_SUCCESS_EVIDENCE_MISSING", "row": row})
            continue
        if integer(row["amount_raw"]) <= 0:
            excluded.append({"reason": "ZERO_VALUE_SEMANTIC_EVIDENCE", "row": row})
            continue
        event_id = f"eip155:1:tx:{tx_hash}:" + ("top" if not path else "trace:" + ",".join(map(str, path)))
        flows[event_id] = dict(row, event_id=event_id, flow_kind="top" if not path else "internal", trace_address=list(path))
    for row in other:
        kind = str(first(row, "record_type", "kind", default="")).lower()
        if kind == "withdrawal":
            block = integer(first(row, "block_number", "block"))
            withdrawal_index = integer(row["withdrawal_index"])
            event_id = f"eip155:1:withdrawal:{withdrawal_index}"
            item = {"event_id": event_id, "tx_hash": f"protocol:eip155:1:block:{block}:withdrawals", "block_number": block, "block_hash": row.get("block_hash"), "tx_index": 2147483647, "sender": None, "recipient": first(row, "to_address", "address").lower(), "amount_raw": str(integer(first(row, "value_raw", "amount_raw"))), "success": True, "evidence_ids": _evidence_ids(row, event_id), "flow_kind": "internal", "trace_address": [withdrawal_index], "protocol_role": "BLOCK_END_WITHDRAWAL"}
            merge(flows, event_id, item, ("block_number", "recipient", "amount_raw"))
    return {"transactions": list(transactions.values()), "flows": list(flows.values()), "other_rows": other, "excluded": excluded, "conflicts": conflicts}


def coverage_complete(coverage, address, first_block, last_block, required_types):
    """Check each required type's contiguous union; net residual proves no coverage."""
    detail = {}
    for data_type in required_types:
        intervals = sorted((integer(r["start_block"]), integer(r["end_block"])) for r in coverage if r.get("address", "").lower() == address.lower() and r.get("data_type") == data_type and r.get("status") == "COMPLETE" and r.get("pagination_complete") is True and r.get("evidence_ids"))
        cursor = first_block
        for start, end in intervals:
            if start > cursor:
                break
            if end >= cursor:
                cursor = end + 1
        detail[data_type] = cursor > last_block
    return all(detail.values()), detail


def assemble_model(graph, target_query, normalized, anchors, coverage, *, name=None):
    """Join saved facts to time-aligned ledgers and the actual LP input schema.

    A known initialization anchor stays known when another account is missing.
    Completeness requires every native evidence type plus end reconciliation;
    reconciliation failure is retained as a conflict, never a balancing entry.
    """
    name = name or target_query["name"]
    windows = {r["account_id"]: r for r in target_query["rows"]}
    modeled = set(windows)
    services = set(graph["target_accounts"])
    candidates = {e["id"]: e for e in graph["events"]}
    seeds = [e for e in graph["events"] if e["kind"] == "seed"]
    if len(seeds) != 1:
        raise ValueError("R3 is a single frozen source model")
    seed_id = seeds[0]["id"]
    seed_fact = next(f for f in graph["physical_fact_manifest"] if f["event_id"] == seed_id)
    seed_position = (seed_fact["block"], seed_fact["tx_index"])
    normalized_anchors = []
    anchor_lookup = {}
    conflicts = list(normalized["conflicts"])
    gaps, ledgers, reconciliation, provenance, accounts, attribution_uncertainties = [], [], [], [], [], []
    for row in normalized["other_rows"]:
        kind = str(first(row, "record_type", "kind", default="")).lower()
        if kind in ("fee_recipient", "protocol_credit"):
            key = account(first(row, "fee_recipient", "to_address", "refund_address", "created_address"))
            gaps.append({"type": "NATIVE_PROTOCOL_CHANGE_REQUIRES_ADDITIONAL_ACCOUNTING", "record_type": kind, "block_number": first(row, "block_number", "block"), "account_id": key, "evidence_ids": _evidence_ids(row, "native_protocol_row"), "effect": "A native-change hit requires exact fee/native-credit accounting before certifying this account; missing amount/timing is not itself conflicting chain evidence"})
            provenance.append({"fact_id": f"native_protocol:{kind}:{first(row, 'block_number', 'block')}:{key}", "fact_type": "NATIVE_PROTOCOL_CHANGE", "used": False, "reason": "Additional exact native-credit accounting required; affected account remains partial", "evidence_ids": _evidence_ids(row, "native_protocol_row")})
    for exclusion in normalized["excluded"]:
        reason = exclusion["reason"]
        if reason in ("TRACE_PATH_MISSING_NO_HASH_ORDER", "ANCESTOR_SUCCESS_EVIDENCE_MISSING") or (reason == "FAILED_OR_UNKNOWN_TOP_VALUE" and exclusion.get("success") is None):
            row = exclusion.get("row", {})
            relevant_accounts = {account(row.get(k)) for k in ("sender", "recipient")} & modeled
            for key in relevant_accounts or {None}:
                gaps.append({"type": "NORMALIZATION_EVIDENCE_GAP", "reason": reason, "account_id": key, "tx_id": row.get("tx_hash", exclusion.get("tx_hash")), "effect": "Incomplete order/success evidence cannot be turned into a complete native ledger"})
    for original in anchors:
        anchor = dict(original)
        key = anchor.get("account_id") or account(anchor.get("address"))
        anchor["account_id"] = key
        anchor["actual_balance_raw"] = str(integer(first(anchor, "actual_balance_raw", "balance_raw")))
        if anchor.get("position") != "BLOCK_END":
            raise ValueError("Event-pre and block-end anchor semantics must not be mixed")
        block = integer(anchor["block_number"])
        anchor["block_number"] = block
        identity = (key, block)
        if identity in anchor_lookup and anchor_lookup[identity]["actual_balance_raw"] != anchor["actual_balance_raw"]:
            conflicts.append({"physical_id": str(identity), "reason": "BALANCE_ANCHOR_CONFLICT", "left": anchor_lookup[identity], "right": anchor})
        else:
            anchor_lookup[identity] = anchor
        normalized_anchors.append(anchor)
        if key in modeled and not anchor.get("block_hash"):
            gaps.append({"type": "BALANCE_ANCHOR_BLOCK_IDENTITY_MISSING", "account_id": key, "anchor_id": anchor.get("anchor_id"), "block_number": block})
    for key, window in windows.items():
        anchor = anchor_lookup.get((key, window["before_anchor_block"]))
        if anchor is None:
            gaps.append({"account_id": key, "type": "INITIAL_BALANCE_ANCHOR_MISSING", "required_block": window["before_anchor_block"], "effect": "This initial actual balance remains unknown; all other known anchors are retained"})
        accounts.append({"account_id": key, "address": window["address"], "asset": "ETH", "initial_position": {"block_number": window["before_anchor_block"], "tx_index": -1, "phase": "BLOCK_END"}, "initial_actual_balance_raw": anchor["actual_balance_raw"] if anchor else None, "initial_source_raw": "0", "initial_source_basis": "Frozen single-source candidate-scope causal initialization before the first observed arrival; no extra source injection. Outside-scope prior source arrival remains an explicit model boundary assumption.", "evidence_ids": anchor.get("evidence_ids", []) if anchor else [], "initial_anchor_id": anchor.get("anchor_id") if anchor else None})
        if anchor:
            provenance.append({"fact_id": anchor["anchor_id"], "fact_type": "BALANCE_ANCHOR", "account_id": key, "used": True, "constraint": f"initial_actual_balance[{key}] = {anchor['actual_balance_raw']}", "evidence_ids": anchor.get("evidence_ids", [])})
        ok, detail = coverage_complete(coverage, window["address"], window["ledger_start_block"], window["ledger_end_block"], window["required_coverage"])
        if not ok:
            gaps.append({"account_id": key, "type": "INCOMPLETE_NATIVE_EVIDENCE_COVERAGE", "missing_types": [k for k, v in detail.items() if not v], "effect": "Observed ledger is conditional on these exact missing evidence types; zero residual alone does not close the gap"})

    def active(key, block):
        window = windows.get(key)
        return bool(window and window["ledger_start_block"] <= block <= window["ledger_end_block"])

    selected_transactions = {}
    flow_ids = set()
    for flow in normalized["flows"]:
        sender, recipient, block = account(flow["sender"]), account(flow["recipient"]), flow["block_number"]
        if not active(sender, block) and not active(recipient, block):
            continue
        for key in (sender, recipient):
            if key in modeled and not active(key, block):
                gaps.append({"account_id": key, "type": "RELATED_FLOW_OUTSIDE_ACCOUNT_WINDOW", "event_id": flow["event_id"], "block_number": block, "effect": "Do not silently reclassify a modeled related account as normal external money; context window needs extension or explicit boundary treatment"})
        event_id = flow["event_id"]
        if event_id in flow_ids:
            raise EvidenceConflict("Duplicate normalized flow variable")
        flow_ids.add(event_id)
        from_account = sender if active(sender, block) else None
        to_account = recipient if active(recipient, block) else None
        terminal = flow["recipient"] in services and event_id in candidates
        if terminal:
            to_account = recipient
        role, zero_basis = None, None
        if event_id == seed_id:
            role = "SEED"
            from_account = None
        elif event_id in candidates:
            role = "CANDIDATE"
        elif from_account is not None and to_account is not None:
            role = "MODELED_INTERNAL"
        elif from_account is not None:
            role = "BOUNDARY_OUTFLOW"
        elif (block, flow["tx_index"] if flow["tx_index"] is not None else -1) < seed_position:
            role = "BACKGROUND_NORMAL"
            zero_basis = "Observed transaction precedes the sole frozen seed injection in chain order; this source does not yet exist in the model."
        else:
            role = "UNKNOWN_EXTERNAL_INCOMING"
            attribution_uncertainties.append({"type": "OBSERVED_EXTERNAL_INCOMING_SOURCE_SHARE_IS_A_MODEL_VARIABLE", "event_id": event_id, "account_id": recipient, "effect": "Actual incoming amount is known and retained; source share uses conserved external-boundary capacity rather than a new source or a forced zero", "is_missing_actual_evidence": False, "scope_assumption": "Only the frozen single source and observed exits from this finite first-service-stop model may fund the shared external source reservoir"})
        tx_hash = flow["tx_hash"]
        tx = selected_transactions.setdefault(tx_hash, {"tx_id": tx_hash, "block_number": block, "tx_index": flow["tx_index"], "flows": [], "fees": [], "pre_actual_balances": {}, "post_actual_balances": {}})
        item = {"event_id": event_id, "from_account": from_account, "to_account": to_account, "amount_raw": flow["amount_raw"], "role": role, "flow_kind": flow["flow_kind"], "trace_address": flow.get("trace_address", []), "order_basis": "block_number, transaction_index, verified trace-tree preorder" if not flow.get("protocol_role") else "Consensus block-end withdrawal order after all execution transactions", "evidence_ids": flow["evidence_ids"]}
        if zero_basis:
            item["source_zero_basis"] = zero_basis
        if terminal:
            item["terminal_target"] = recipient
        if flow.get("protocol_role"):
            item["protocol_role"] = flow["protocol_role"]
        tx["flows"].append(item)
        provenance.append({"fact_id": event_id, "fact_type": "NATIVE_VALUE_FLOW", "role": role, "used": True, "constraint": f"flow_source[{event_id}] in [0,{flow['amount_raw']}]; linked sender/receiver conservation", "evidence_ids": flow["evidence_ids"]})
    for physical_tx in normalized["transactions"]:
        payer = account(physical_tx["sender"])
        if not active(payer, physical_tx["block_number"]):
            if physical_tx["fee_raw"] is not None and physical_tx["tx_hash"] == seed_fact["tx_hash"]:
                provenance.append({"fact_id": "fee:" + physical_tx["tx_hash"], "fact_type": "TRANSACTION_FEE", "used": False, "reason": "Frozen seed payer is outside modeled recipient accounts; do not charge the seed recipient or create another funding source", "evidence_ids": physical_tx["evidence_ids"]})
            elif physical_tx["fee_raw"] is not None:
                provenance.append({"fact_id": "fee:" + physical_tx["tx_hash"], "fact_type": "TRANSACTION_FEE", "used": False, "reason": "Actual fee payer is outside this nonterminal account window; a context recipient is not charged the sender's fee", "evidence_ids": physical_tx["evidence_ids"]})
            continue
        tx_hash = physical_tx["tx_hash"]
        tx = selected_transactions.setdefault(tx_hash, {"tx_id": tx_hash, "block_number": physical_tx["block_number"], "tx_index": physical_tx["tx_index"], "flows": [], "fees": [], "pre_actual_balances": {}, "post_actual_balances": {}})
        fee = physical_tx["fee_raw"]
        if fee is None:
            gaps.append({"type": "ACTUAL_TRANSACTION_FEE_MISSING", "tx_id": tx_hash, "account_id": payer})
            continue
        fee_id = "fee:eip155:1:" + tx_hash
        tx["fees"].append({"fee_id": fee_id, "payer_account": payer, "amount_raw": fee, "timing": "TX_BEGIN_NET_FEE", "evidence_ids": physical_tx["evidence_ids"]})
        provenance.append({"fact_id": fee_id, "fact_type": "TRANSACTION_FEE", "used": True, "constraint": f"fee_source[{fee_id}] in [0,{fee}]; paid from payer state before transaction value movements", "evidence_ids": physical_tx["evidence_ids"]})
        if physical_tx.get("gas_used") not in (None, 21000) or physical_tx.get("input_data") not in (None, "", "0x"):
            gaps.append({"type": "CONTRACT_FEE_PREPAY_REFUND_TIMING_CONDITIONAL", "tx_id": tx_hash, "account_id": payer, "effect": "Net actual fee is charged at transaction start as a documented necessary-capacity model; full gas-limit prepay/refund timing is not certified"})
    transactions = list(selected_transactions.values())
    if any(tx["tx_index"] is None for tx in transactions):
        raise ValueError("UNRESOLVED_CHAIN_ORDER: missing transaction index is not ordered by hash")
    transactions.sort(key=lambda tx: (tx["block_number"], tx["tx_index"]))
    for tx in transactions:
        tx["flows"].sort(key=lambda flow: (0 if flow["flow_kind"] == "top" else 1, tuple(flow["trace_address"])))
    if sum(flow["role"] == "SEED" for tx in transactions for flow in tx["flows"]) != 1:
        raise ValueError("Frozen seed must appear exactly once in the reconstructed ledger")
    model_anchors = []
    for key, window in windows.items():
        relevant = [tx for tx in transactions if any(f["from_account"] == key or f["to_account"] == key for f in tx["flows"]) or any(f["payer_account"] == key for f in tx["fees"])]
        initial_anchor = anchor_lookup.get((key, window["before_anchor_block"]))
        closing_anchor = anchor_lookup.get((key, window["after_anchor_block"]))
        current = integer(initial_anchor["actual_balance_raw"]) if initial_anchor else None
        start = current
        for tx in relevant:
            before = current
            income = sum(integer(f["amount_raw"]) for f in tx["flows"] if f["to_account"] == key)
            outgoing = sum(integer(f["amount_raw"]) for f in tx["flows"] if f["from_account"] == key)
            fees = sum(integer(f["amount_raw"]) for f in tx["fees"] if f["payer_account"] == key)
            if current is not None:
                tx["pre_actual_balances"][key] = str(current)
                tx.setdefault("state_evidence_ids", {})[key] = sorted(set((initial_anchor or {}).get("evidence_ids", []) + [eid for prior in relevant if (prior["block_number"], prior["tx_index"]) <= (tx["block_number"], tx["tx_index"]) for item in prior["flows"] + prior["fees"] for eid in item.get("evidence_ids", [])]))
                # Fee and top-level payment precede any internal transaction inflow.
                first_spend = fees + sum(integer(f["amount_raw"]) for f in tx["flows"] if f["from_account"] == key and f["flow_kind"] == "top")
                if first_spend > current:
                    conflicts.append({"reason": "NEGATIVE_PRE_TRANSACTION_ACTUAL_CAPACITY", "account_id": key, "tx_id": tx["tx_id"], "before_raw": str(current), "top_value_plus_fee_raw": str(first_spend)})
                current += income - outgoing - fees
                tx["post_actual_balances"][key] = str(current)
                if current < 0:
                    conflicts.append({"reason": "NEGATIVE_REPLAYED_ACTUAL_BALANCE", "account_id": key, "tx_id": tx["tx_id"], "balance_raw": str(current)})
            ledgers.append({"account_id": key, "tx_id": tx["tx_id"], "block_number": tx["block_number"], "tx_index": tx["tx_index"], "balance_before_raw": str(before) if before is not None else None, "incoming_raw": str(income), "outgoing_raw": str(outgoing), "fee_raw": str(fees), "balance_after_raw": str(current) if current is not None else None, "flow_ids": [f["event_id"] for f in tx["flows"] if key in (f["from_account"], f["to_account"])], "fee_ids": [f["fee_id"] for f in tx["fees"] if f["payer_account"] == key]})
        residual = current - integer(closing_anchor["actual_balance_raw"]) if current is not None and closing_anchor else None
        coverage_ok, coverage_detail = coverage_complete(coverage, window["address"], window["ledger_start_block"], window["ledger_end_block"], window["required_coverage"])
        if residual not in (None, 0):
            conflicts.append({"reason": "ANCHOR_RECONCILIATION_MISMATCH", "account_id": key, "expected_end_raw": str(current), "observed_end_raw": closing_anchor["actual_balance_raw"], "difference_raw": str(residual), "coverage_complete": coverage_ok})
        if closing_anchor and relevant:
            if closing_anchor["block_number"] != window["ledger_end_block"]:
                raise ValueError("Closing anchor block does not match ledger window")
            model_anchors.append({"anchor_id": closing_anchor["anchor_id"], "account_id": key, "tx_id": relevant[-1]["tx_id"], "when": "post", "actual_balance_raw": closing_anchor["actual_balance_raw"], "evidence_ids": closing_anchor.get("evidence_ids", []), "alignment_basis": "Replay every declared native type through the complete end block; conditional if coverage has explicit gaps"})
            provenance.append({"fact_id": closing_anchor["anchor_id"], "fact_type": "BALANCE_ANCHOR", "account_id": key, "used": True, "constraint": f"source_state[{key},after:{relevant[-1]['tx_id']}] <= {closing_anchor['actual_balance_raw']}", "evidence_ids": closing_anchor.get("evidence_ids", [])})
        elif not closing_anchor:
            gaps.append({"type": "CLOSING_BALANCE_ANCHOR_MISSING", "account_id": key, "required_block": window["after_anchor_block"]})
        intermediate_checks = []
        for (anchor_account, anchor_block), item in sorted(anchor_lookup.items()):
            if anchor_account != key or not window["before_anchor_block"] < anchor_block < window["after_anchor_block"]:
                continue
            prior = [tx for tx in relevant if tx["block_number"] <= anchor_block]
            actual = integer(item["actual_balance_raw"])
            expected = start
            if expected is not None:
                for tx in prior:
                    expected += sum(integer(f["amount_raw"]) for f in tx["flows"] if f["to_account"] == key)
                    expected -= sum(integer(f["amount_raw"]) for f in tx["flows"] if f["from_account"] == key)
                    expected -= sum(integer(f["amount_raw"]) for f in tx["fees"] if f["payer_account"] == key)
                if expected != actual:
                    conflicts.append({"reason": "INTERMEDIATE_ANCHOR_RECONCILIATION_MISMATCH", "account_id": key, "anchor_id": item["anchor_id"], "expected_raw": str(expected), "observed_raw": str(actual), "difference_raw": str(expected - actual)})
            if relevant:
                aligned_tx, when = (prior[-1], "post") if prior else (relevant[0], "pre")
                model_anchors.append({"anchor_id": item["anchor_id"], "account_id": key, "tx_id": aligned_tx["tx_id"], "when": when, "actual_balance_raw": item["actual_balance_raw"], "evidence_ids": item.get("evidence_ids", []), "alignment_basis": "Complete native account changes replayed through this intermediate block end; no account movement between anchor and mapped transaction state under recorded coverage"})
                provenance.append({"fact_id": item["anchor_id"], "fact_type": "BALANCE_ANCHOR", "account_id": key, "used": True, "constraint": f"source_state[{key},{when}:{aligned_tx['tx_id']}] <= {item['actual_balance_raw']}", "evidence_ids": item.get("evidence_ids", [])})
            else:
                provenance.append({"fact_id": item["anchor_id"], "fact_type": "BALANCE_ANCHOR", "account_id": key, "used": False, "reason": "No modeled account operation to align this intermediate anchor", "evidence_ids": item.get("evidence_ids", [])})
            intermediate_checks.append({"anchor_id": item["anchor_id"], "block_number": anchor_block, "expected_raw": str(expected) if expected is not None else None, "observed_raw": str(actual), "difference_raw": str(expected - actual) if expected is not None else None})
        relevant_gaps = [gap for gap in gaps if gap.get("account_id") == key]
        status = "FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE" if start is not None and residual == 0 and coverage_ok and not relevant_gaps else "PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS"
        if any(c.get("account_id") == key for c in conflicts):
            status = "EVIDENCE_CONFLICT_MODEL_BLOCKED"
        reconciliation.append({"account_id": key, "start_block": window["ledger_start_block"], "end_block": window["ledger_end_block"], "initial_actual_balance_raw": str(start) if start is not None else None, "expected_final_balance_raw": str(current) if current is not None else None, "observed_final_balance_raw": closing_anchor["actual_balance_raw"] if closing_anchor else None, "difference_raw": str(residual) if residual is not None else None, "intermediate_anchor_checks": intermediate_checks, "coverage_by_type": coverage_detail, "coverage_complete": coverage_ok, "reconciliation_status": "MATCH" if residual == 0 else "NOT_AVAILABLE" if residual is None else "CONFLICT", "completion_status": status})
    used_evidence = {eid for row in provenance for eid in row.get("evidence_ids", [])}
    result = {"schema_version": "stage1b-r3-context-model-v1", "query_id": graph["scenario_id"], "name": name, "seed_event_id": seed_id, "accounts": accounts, "transactions": transactions, "anchors": model_anchors, "objective_groups": graph["objective_groups"], "all_service_entries": sorted({event for group in graph["objective_groups"].values() for event in group}), "gaps": gaps, "attribution_uncertainties": attribution_uncertainties, "fact_conflicts": conflicts, "assumptions": ["One frozen source, first service entry stops; finite declared R2 candidate scope retained", "Source-free initial account state is a candidate-scope causal assumption, not proof of absence of prior source on Ethereum", "Unknown observed post-seed external inflows retain actual value and share conserved external-boundary source capacity", "Gas source share is optimized by the model independently of the observed actual fee"], "evidence_ids": sorted(used_evidence)}
    outputs = {"model_input": result, "balance_anchors": normalized_anchors, "account_ledgers": ledgers, "ledger_reconciliation": reconciliation, "constraint_provenance": provenance, "evidence_gaps": gaps, "attribution_uncertainties": attribution_uncertainties, "fact_conflicts": conflicts, "normalization_exclusions": normalized["excluded"], "completion_status": "EVIDENCE_CONFLICT_MODEL_BLOCKED" if conflicts else "FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE" if not gaps and all(r["completion_status"] == "FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE" for r in reconciliation) else "PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS"}
    return outputs


def replay_manifest(manifest_path, bundle_root, name, output):
    """Rebuild the entire evidence-to-ledger input in an offline review bundle."""
    root = Path(bundle_root).resolve()
    spec = read_json(manifest_path)
    if spec.get("schema_version") != "stage1b-r3-context-replay-v1":
        raise ValueError("Unknown context replay manifest schema")
    selected = next(q for q in spec["queries"] if q["name"] == name)
    identities = []

    def verified(entry, binary=False):
        if isinstance(entry, str):
            raise ValueError("Every replay input requires its source SHA-256")
        path = (root / entry["path"]).resolve()
        if not path.is_relative_to(root):
            raise ValueError("Replay input outside bundle")
        identity = file_identity(path)
        if identity["sha256"] != entry["sha256"]:
            raise EvidenceConflict("Saved replay input hash mismatch: " + entry["path"])
        identities.append({"path": entry["path"], **identity})
        return (path.read_bytes() if binary else read_json(path)), "sha256:" + identity["sha256"]

    graph, graph_evidence = verified(selected["fixed_graph"])
    collection, collection_evidence = verified(selected["collection"])
    targets_doc, _ = verified(spec["targets"])
    target_query = next(q for q in targets_doc["queries"] if q["name"] == name)
    rows = []
    for row in collection["candidate_events"] + collection["context_events"]:
        if row.get("asset") == "native:eip155:1":
            rows.append(dict(row, evidence_ids=sorted(set(_evidence_ids(row, collection_evidence) + [collection_evidence]))))
    for source in selected.get("saved_response_pages", []):
        document, identity = verified(source)
        rows.extend(decode_saved_response(document, identity))
    anchors = []
    rpc_envelopes = []
    for batch in spec.get("rpc_batches", []):
        receipt, receipt_identity = verified(batch["receipt"])
        intent, intent_identity = verified(batch["intent"])
        wire, wire_identity = verified({"path": receipt["raw_path"], "sha256": receipt["raw_sha256"]}, binary=True)
        if len(wire) != receipt["raw_bytes"]:
            raise EvidenceConflict("RPC raw body size mismatch")
        if receipt.get("error_class") or receipt.get("http_status") != 200:
            # Retain failed/uncertain request evidence without treating an empty
            # or non-JSON transport response as a successful fact.
            continue
        responses = json.loads(wire)
        responses = responses if isinstance(responses, list) else [responses]
        request_map = {r["id"]: r for r in intent["requests"]}
        response_map = {r["id"]: r for r in responses}
        if len(request_map) != len(intent["requests"]) or len(response_map) != len(responses):
            raise EvidenceConflict("Duplicate RPC request or response id")
        for member in receipt["members"]:
            envelope, envelope_identity = verified({"path": member["artifact_path"], "sha256": member["artifact_sha256"]})
            request, response = envelope["request"], envelope["response"]
            if request_map.get(request["id"]) != request or response_map.get(request["id"]) != response or request["id"] != response.get("id"):
                raise EvidenceConflict("RPC envelope differs from dispatch intent or actual wire result")
            if envelope.get("raw_body_sha256") != receipt["raw_sha256"] or envelope.get("status") != "SUCCESS_VALIDATED" or envelope.get("http_status") != 200:
                continue
            rpc_envelopes.append((envelope, [receipt_identity, intent_identity, wire_identity, envelope_identity]))
    block_envelopes = {}
    for envelope, evidence in rpc_envelopes:
        request = envelope["request"]
        if request["method"] == "eth_getBlockByNumber":
            block = envelope["response"]["result"]
            number = integer(request["params"][0])
            if block is None or integer(block["number"]) != number:
                raise EvidenceConflict("RPC fixed-block response does not match requested number")
            if number in block_envelopes and block_envelopes[number][0]["response"]["result"]["hash"] != block["hash"]:
                raise EvidenceConflict("RPC block hash changes across saved observations")
            block_envelopes[number] = (envelope, evidence)
    for envelope, evidence in rpc_envelopes:
        request = envelope["request"]
        if request["method"] == "eth_getBalance":
            address, selector = request["params"]
            if address.lower() not in {row["address"] for row in target_query["rows"]}:
                continue
            number = integer(selector)
            block, block_evidence = block_envelopes.get(number, (None, []))
            anchors.append(normalize_anchor(request, envelope["response"], block["response"] if block else None, sorted(set(evidence + block_evidence))))
    for evidence in selected.get("rpc_anchor_evidence", []):
        response, response_identity = verified(evidence["response"])
        request = evidence["request"]
        if isinstance(response, list):
            matches = [r for r in response if r.get("id") == request["id"]]
            if len(matches) != 1:
                raise EvidenceConflict("RPC response id must bind exactly once")
            response = matches[0]
        if response.get("id") != request["id"]:
            raise EvidenceConflict("RPC anchor request/response identity mismatch")
        block, block_identity = (verified(evidence["block_response"]) if evidence.get("block_response") else (None, None))
        if isinstance(block, list):
            matches = [r for r in block if r.get("id") == evidence.get("block_request_id")]
            if len(matches) != 1:
                raise EvidenceConflict("Block response id must bind exactly once")
            block = matches[0]
        anchors.append(normalize_anchor(request, response, block, [x for x in (response_identity, block_identity) if x]))
    if selected.get("normalized_anchors"):
        # Already-normalized facts remain identifiable but cannot substitute for
        # raw response replay when raw RPC evidence has also been supplied.
        normalized_anchors, identity = verified(selected["normalized_anchors"])
        if anchors:
            compare = normalized_anchors.get("anchors", normalized_anchors) if isinstance(normalized_anchors, dict) else normalized_anchors
            actual = {(a["account_id"], a["block_number"]): a["actual_balance_raw"] for a in anchors}
            for item in compare:
                key = (item.get("account_id") or account(item["address"]), integer(item["block_number"]))
                if key not in actual or actual[key] != str(integer(first(item, "actual_balance_raw", "balance_raw"))):
                    raise EvidenceConflict("Normalized anchor does not match replayed raw response")
        else:
            raise ValueError("Normalized anchors alone are not sufficient for evidence-to-ledger replay")
    coverage_doc, _ = verified(selected["coverage"]) if selected.get("coverage") else ([], None)
    coverage = coverage_doc.get("rows", coverage_doc) if isinstance(coverage_doc, dict) else coverage_doc
    for context_job in selected.get("context_jobs", []):
        from dune_batch_r1 import verified_raw
        from page_contract import initial_progress, validate_page
        frozen, frozen_identity = verified(context_job["freeze"])
        job, job_identity = verified(context_job["job"])
        job_folder = (root / context_job["job"]["path"]).parent
        sql_path = job_folder / "query.sql"
        sql_identity = file_identity(sql_path)
        if not job_folder.is_relative_to(root) or job.get("kind") != "context" or job.get("sql_sha256") != frozen["sql_sha256"] or sql_identity["sha256"] != frozen["sql_sha256"] or job.get("scope_freeze_sha256") != context_job["freeze"]["sha256"]:
            raise EvidenceConflict("Context job not bound to frozen SQL and scope")
        submit, status = verified_raw(job["submit_receipt"], root), verified_raw(job["status_receipt"], root)
        execution = job["execution_id"]
        if submit != job["submit_response"] or status != job["status_response"] or submit.get("execution_id") != execution or status.get("execution_id") != execution or status.get("state") != "QUERY_STATE_COMPLETED":
            raise EvidenceConflict("Context execution identity or completion mismatch")
        offsets = job.get("export_offsets", [])
        if len(offsets) != job.get("export_requests") or len(offsets) != len(set(offsets)):
            raise EvidenceConflict("Context export page history mismatch")
        progress = initial_progress(status["result_metadata"]["total_row_count"])
        evidence = [frozen_identity, job_identity]
        for offset in offsets:
            page_path, receipt_path = job_folder / f"page_{offset}.json", job_folder / f"page_{offset}_receipt.json"
            page, receipt = read_json(page_path), read_json(receipt_path)
            params = receipt["parameters"]
            if verified_raw(receipt, root) != page:
                raise EvidenceConflict("Context saved page differs from wire response")
            progress = validate_page(page, execution_id=execution, offset=offset, limit=params["limit"], progress=progress, status_metadata=status["result_metadata"], receipt=receipt, parameters=params)
            identity = "sha256:" + receipt["sha256"]
            rows.extend(decode_saved_response(page, identity))
            evidence.append(identity)
            for path in (page_path, receipt_path, root / receipt["raw_path"]):
                identities.append({"path": path.relative_to(root).as_posix(), **file_identity(path)})
        if not progress["complete"]:
            raise EvidenceConflict("Full necessary context results not exported")
        for window in frozen["account_windows"]:
            expected = next((r for r in target_query["rows"] if r["address"] == window["address"]), None)
            if expected is None or window["ledger_start_block"] < expected["ledger_start_block"] or window["ledger_end_block"] > expected["ledger_end_block"]:
                raise EvidenceConflict("Frozen context coverage is not the declared account window")
            coverage.extend({"address": window["address"], "data_type": kind, "start_block": window["ledger_start_block"], "end_block": window["ledger_end_block"], "pagination_complete": True, "status": "COMPLETE", "evidence_ids": evidence, "execution_id": execution} for kind in window["required_coverage"])
    normalized = normalize_rows(rows)
    for fact in normalized["transactions"] + normalized["flows"]:
        header = block_envelopes.get(fact["block_number"])
        if header and fact.get("block_hash") and fact["block_hash"].lower() != header[0]["response"]["result"]["hash"].lower():
            normalized["conflicts"].append({"reason": "RPC_DUNE_BLOCK_IDENTITY_CONFLICT", "block_number": fact["block_number"], "tx_id": fact["tx_hash"], "dune_block_hash": fact["block_hash"], "rpc_block_hash": header[0]["response"]["result"]["hash"], "evidence_ids": sorted(set(fact["evidence_ids"] + header[1]))})
    receipt_checks, code_evidence = [], []
    tx_lookup = {tx["tx_hash"]: tx for tx in normalized["transactions"]}
    for envelope, evidence in rpc_envelopes:
        request, response = envelope["request"], envelope["response"]
        if request["method"] == "eth_getTransactionReceipt" and response.get("result"):
            receipt = response["result"]
            tx_hash = request["params"][0].lower()
            if receipt.get("transactionHash", "").lower() != tx_hash:
                raise EvidenceConflict("RPC receipt belongs to a different requested transaction")
            tx = tx_lookup.get(tx_hash)
            if tx:
                rpc_fee = integer(receipt["gasUsed"]) * integer(receipt["effectiveGasPrice"])
                mismatch = []
                for label, left, right in [("block_number", integer(receipt["blockNumber"]), tx["block_number"]), ("tx_index", integer(receipt["transactionIndex"]), tx["tx_index"]), ("fee_raw", str(rpc_fee), tx["fee_raw"]), ("sender", receipt["from"].lower(), tx["sender"]), ("success", truth(receipt["status"]), tx["success"])]:
                    if left != right:
                        mismatch.append(label)
                if tx.get("block_hash") and receipt["blockHash"].lower() != tx["block_hash"].lower():
                    mismatch.append("block_hash")
                if mismatch:
                    normalized["conflicts"].append({"reason": "RPC_DUNE_RECEIPT_FACT_CONFLICT", "tx_id": tx_hash, "fields": mismatch, "evidence_ids": sorted(set(tx["evidence_ids"] + evidence))})
                else:
                    tx["evidence_ids"] = sorted(set(tx["evidence_ids"] + evidence))
                receipt_checks.append({"tx_id": tx_hash, "status": "CONFLICT" if mismatch else "MATCH", "fields": ["block_number", "block_hash", "tx_index", "sender", "gas_used_times_effective_gas_price", "success"], "evidence_ids": evidence})
        if request["method"] == "eth_getCode" and request["params"][0].lower() in {row["address"] for row in target_query["rows"]}:
            code_evidence.append({"account_id": account(request["params"][0]), "block_number": integer(request["params"][1]), "code_empty": response.get("result") == "0x", "code_observed": response.get("result") is not None, "evidence_ids": evidence, "purpose": "Bounded historical account-type evidence; no live contract interaction or code execution"})
    result = assemble_model(graph, target_query, normalized, anchors, coverage, name=name)
    result["receipt_cross_checks"] = receipt_checks
    result["account_code_evidence"] = code_evidence
    result["source_manifest"] = {"schema_version": "stage1b-r3-context-used-sources-v1", "query": name, "files": sorted(identities, key=lambda x: x["path"]), "network_requests": 0, "r2_graph_identity": graph_evidence}
    for key, value in result.items():
        write_json(Path(output) / (key + ".json"), value)
    return result


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def file_identity(path):
    data = Path(path).read_bytes()
    return {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def targets(graph_root, policy, mechanical_baseline=None):
    """Recompute the account queue from physical facts, not the supplied queue."""
    graph_root = Path(graph_root)
    result = {"schema": "stage1b-r3-context-targets-v1", "basis": "R2 frozen physical fact manifests, independently regrouped by nonterminal account", "queries": []}
    reference = {q["name"]: q for q in (mechanical_baseline or {}).get("queries", [])}
    for pilot in policy["query_pilots"]:
        name = pilot["name"]
        graph_path = graph_root / name / "fixed_graph.json"
        collection_path = graph_root / name / "collection.json"
        graph, collection = read_json(graph_path), read_json(collection_path)
        services = set(graph["target_accounts"])
        accounts = {key.split("|")[0] for key in graph["initial_balances"]} - services
        facts = {f["event_id"]: f for f in graph["physical_fact_manifest"]}
        event_ids = {e["id"] for e in graph["events"]}
        if set(facts) != event_ids:
            raise ValueError("R2 physical facts differ from graph events")
        rows = []
        for address in sorted(accounts):
            needed = sorted([f for f in facts.values() if address in (f["sender"], f["recipient"])], key=lambda f: (f["block"], f["tx_index"] if f["tx_index"] is not None else -1))
            first, last = needed[0], needed[-1]
            cached = [f for f in collection["candidate_events"] + collection["context_events"] if address in (f["sender"], f["recipient"]) and first["block"] <= f["block"] <= last["block"]]
            cached_unique = {f["event_id"]: f for f in cached}
            rows.append({"address": address, "asset": "ETH", "account_id": address + "|ETH", "role": "NON_TERMINAL_MODEL_ACCOUNT", "candidate_fact_count": len(needed), "first_candidate_block": first["block"], "last_candidate_block": last["block"], "first_candidate_timestamp": first["timestamp"], "last_candidate_timestamp": last["timestamp"], "first_needed_position": {"block_number": first["block"], "tx_index": first["tx_index"], "event_id": first["event_id"]}, "last_needed_position": {"block_number": last["block"], "tx_index": last["tx_index"], "event_id": last["event_id"]}, "ledger_start_block": first["block"], "ledger_end_block": last["block"], "before_anchor_block": first["block"] - 1, "after_anchor_block": last["block"], "anchor_semantics": "BLOCK_END", "cached_unique_value_events_in_window": len(cached_unique), "cached_physical_transaction_gas_facts": len({f["tx_hash"] for f in cached_unique.values() if f.get("gas_raw") is not None and f.get("kind") == "top" and f["sender"] == address}), "required_coverage": ["ALL_TOP_LEVEL_TRANSACTIONS_INCLUDING_ZERO_VALUE_AND_FAILED", "ALL_EFFECTIVE_NATIVE_INTERNAL_TRANSFERS_INCLUDING_SELFDESTRUCT", "ALL_TRANSACTION_FEES_PAID_BY_ACCOUNT", "APPLICABLE_PROTOCOL_NATIVE_CHANGES"], "cached_coverage_is_sufficient_for_full_context": False, "context_boundary_rule": "Full first/last blocks and contiguous intermediate window only; no recursive expansion"})
        baseline = reference.get(name)
        comparison = None
        if baseline:
            fields = ("address", "asset", "candidate_fact_count", "first_candidate_block", "last_candidate_block", "first_candidate_timestamp", "last_candidate_timestamp")
            old = sorted(tuple(r[k] for k in fields) for r in baseline["rows"])
            new = sorted(tuple(r[k] for k in fields) for r in rows)
            comparison = {"status": "MATCH" if new == old else "MISMATCH", "graph_hash_matches": file_identity(graph_path)["sha256"] == baseline["fixed_graph_sha256"], "baseline_is_chain_evidence": False}
            if new != old or not comparison["graph_hash_matches"]:
                raise ValueError("Baseline identity/account queue mismatch")
        result["queries"].append({"name": name, "query_id": graph["scenario_id"], "seed_event_id": pilot["seed_event_id"], "graph_identity": file_identity(graph_path), "collection_identity": file_identity(collection_path), "candidate_event_count": len(event_ids), "service_terminal_count": len(services), "nonterminal_account_count": len(rows), "mechanical_baseline_comparison": comparison, "rows": rows})
    result["totals"] = {"accounts": sum(q["nonterminal_account_count"] for q in result["queries"]), "requested_balance_anchors": sum(q["nonterminal_account_count"] * 2 for q in result["queries"]), "distinct_anchor_blocks": len({r[k] for q in result["queries"] for r in q["rows"] for k in ("before_anchor_block", "after_anchor_block")})}
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["targets", "replay"])
    parser.add_argument("--graph-root")
    parser.add_argument("--policy")
    parser.add_argument("--baseline")
    parser.add_argument("--manifest")
    parser.add_argument("--bundle-root", default=".")
    parser.add_argument("--name")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.command == "targets":
        output = targets(args.graph_root, read_json(args.policy), read_json(args.baseline) if args.baseline else None)
        write_json(args.output, output)
        print(json.dumps(output["totals"]))
    else:
        result = replay_manifest(args.manifest, args.bundle_root, args.name, args.output)
        print(json.dumps({"status": result["completion_status"], "accounts": len(result["model_input"]["accounts"]), "ledger_rows": len(result["account_ledgers"]), "conflicts": len(result["fact_conflicts"])}))


if __name__ == "__main__":
    main()
