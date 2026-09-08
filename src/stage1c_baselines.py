"""Independent Stage1C baselines on the frozen common observation document.

This module deliberately has no primary-model, solver, Oracle, file, or network
imports.  It reads physical ports only, and constructs its own fresh state on
every call. Haircut's output is an event allocation for *subsequent* verification,
never a point selected or repaired with an LP.
"""
from __future__ import annotations

from fractions import Fraction as F
import hashlib
import json

VERSION = "stage1c-baselines-v1.0.0"
CONTEXT_SCHEMA = "stage1b-r3-context-model-v1"
OUTSIDE = "@outside_reservoir|ETH"
METHODS = {"BOUNDED_REACHABILITY", "POISON", "HAIRCUT"}


class NotApplicable(ValueError):
    """An explicit input/semantic limitation, distinct from an invalid ledger."""


def _raw(value):
    if isinstance(value, (float, bool)):
        raise ValueError("Physical amounts must be exact integer raw units")
    value = F(value)
    if value < 0 or value.denominator != 1:
        raise ValueError("Physical amounts must be nonnegative integer raw units")
    return value


def _fmt(value):
    if value is None:
        return None
    return str(value.numerator) if value.denominator == 1 else str(value)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode("utf-8")).hexdigest()


def _port(account, asset):
    if account is None:
        return None
    return str(account) + "|" + asset


def observed_targets(document):
    """Address--asset groups derived only from legal observed recipient ports.

    The generic graph's objective_groups also contains non-service diagnostics;
    those are not promoted into service targets. Empty registered accounts have
    no entering events and therefore no amount objective.
    """
    if document.get("schema_version") == CONTEXT_SCHEMA:
        return {name: list(events) for name, events in sorted(document.get("objective_groups", {}).items())}
    targets = set(document.get("target_accounts", []))
    result = {}

    def add(account, asset, name):
        if account in targets:
            result.setdefault(_port(account, asset), []).append(name)

    for e in sorted(document["events"], key=lambda e: e["order"]):
        kind, eid, asset = e["kind"], e["id"], e["asset"]
        if kind in {"seed", "transfer", "normal_incoming"}:
            add(e["to"], asset, eid)
        elif kind == "conversion":
            add(e["to"], e["output_asset"], eid + ":output")
            if _raw(e.get("refund_raw", "0")):
                add(e.get("refund_to", e["from"]), asset, eid + ":refund")
        elif kind == "multioutput":
            for index, output in enumerate(e["outputs"]):
                add(output["to"], asset, eid + f":output{index}")
    return dict(sorted(result.items()))


def _flow(eid, sender, receiver, asset, amount, role, fixed=None):
    return {"kind": "flow", "id": eid, "sender": sender, "receiver": receiver,
            "asset": asset, "amount": _raw(amount), "role": role, "fixed": fixed}


def _compile_context(document):
    if document.get("fact_conflicts"):
        raise ValueError("EVIDENCE_CONFLICT_MODEL_BLOCKED")
    accounts = {a["account_id"]: a for a in document["accounts"]}
    if len(accounts) != len(document["accounts"]) or OUTSIDE in accounts:
        raise ValueError("Duplicate or reserved account identity")
    targets = observed_targets(document)
    terminals = set(targets)
    if terminals & accounts.keys():
        raise ValueError("Service target must be absorbing")
    initial = {}
    for key, account in accounts.items():
        if not key.endswith("|ETH"):
            raise ValueError("Context baseline supports explicitly identified ETH accounts")
        if _raw(account.get("initial_source_raw", "0")) != 0 or not account.get("initial_source_basis"):
            raise ValueError("Initial source zero requires a recorded causal basis")
        initial[key] = None if account.get("initial_actual_balance_raw") is None else _raw(account["initial_actual_balance_raw"])
    # These are absorbing accounting accumulators, not claimed chain balances.
    initial.update({key: F(0) for key in terminals})
    initial[OUTSIDE] = F(0)
    anchors = {}
    tx_ids = {tx["tx_id"] for tx in document["transactions"]}
    for anchor in document.get("anchors", []):
        if anchor["account_id"] not in accounts or anchor.get("tx_id") not in tx_ids or anchor.get("when") not in {"pre", "post"}:
            raise ValueError("Unknown or unaligned anchor")
        anchors.setdefault((anchor["tx_id"], anchor["when"]), []).append(anchor)
    operations, previous = [], None
    for tx in document["transactions"]:
        position = (tx["block_number"], tx["tx_index"])
        if any(not isinstance(v, int) for v in position) or position[1] < 0 or (previous is not None and position <= previous):
            raise ValueError("Transactions need distinct observed increasing positions")
        previous = position

        def emit_anchors(when):
            for anchor in anchors.get((tx["tx_id"], when), []):
                operations.append({"kind": "anchor", "id": anchor["anchor_id"],
                                   "balances": {anchor["account_id"]: _raw(anchor["actual_balance_raw"])}})
            declared = {key: _raw(v) for key, v in tx.get(when + "_actual_balances", {}).items() if v is not None}
            if declared:
                if not declared.keys() <= initial.keys():
                    raise ValueError("Transaction state names unknown account")
                operations.append({"kind": "anchor", "id": tx["tx_id"] + ":" + when, "balances": declared})

        emit_anchors("pre")
        if len(tx.get("fees", [])) > 1:
            raise ValueError("A transaction may charge actual gas only once")
        for fee in tx.get("fees", []):
            payer = fee["payer_account"]
            if fee.get("timing") != "TX_BEGIN_NET_FEE" or payer in terminals:
                raise ValueError("Unsupported fee timing or terminal payer")
            if payer not in accounts and not fee.get("source_zero_basis"):
                raise ValueError("Outside payer fee needs source-zero evidence")
            operations.append(_flow(fee["fee_id"], payer if payer in accounts else None, None,
                                    "ETH", fee["amount_raw"], "FEE", F(0) if payer not in accounts else None))
        for flow in tx.get("flows", []):
            role, sender, receiver = flow["role"], flow.get("from_account"), flow.get("to_account")
            if sender in terminals or flow.get("reverted") or flow.get("flow_kind") in {"delegatecall", "callcode"}:
                raise ValueError("Unsupported physical flow or outgoing terminal ledger")
            if len(tx.get("flows", [])) > 1 and not flow.get("order_basis"):
                raise ValueError("Multiple flows need observed execution order")
            for key in (sender, receiver):
                if key in accounts:
                    init = accounts[key]["initial_position"]
                    phase = init.get("phase")
                    if phase == "BLOCK_END":
                        if position[0] <= init["block_number"]:
                            raise ValueError("Initial anchor is not before its account event")
                    elif phase == "TRANSACTION_PRE":
                        if position < (init["block_number"], init["tx_index"]):
                            raise ValueError("Initial anchor is after its account event")
                    else:
                        raise ValueError("Unsupported initial anchor phase")
            fixed = None
            if role == "SEED":
                if sender in accounts or receiver not in accounts:
                    raise ValueError("Unique seed needs a modeled recipient and external origin")
                sender, fixed = None, _raw(flow["amount_raw"])
            elif role == "BACKGROUND_NORMAL":
                if sender in accounts or receiver not in accounts or not flow.get("source_zero_basis"):
                    raise ValueError("Background flow needs explicit normal-origin evidence")
                sender, fixed = None, F(0)
            elif role == "UNKNOWN_EXTERNAL_INCOMING":
                if sender in accounts or receiver not in accounts:
                    raise ValueError("Unknown outside incoming port is inconsistent")
                sender = OUTSIDE
            elif role in {"CANDIDATE", "MODELED_INTERNAL", "BOUNDARY_OUTFLOW"}:
                if sender not in accounts:
                    raise ValueError("Candidate or context exit sender must be modeled")
                if role == "BOUNDARY_OUTFLOW" and (receiver in accounts or receiver in terminals):
                    raise ValueError("Explicit boundary outflow cannot discard modeled recipient")
                if receiver not in accounts and receiver not in terminals:
                    receiver = OUTSIDE
            else:
                raise ValueError("Unknown context role: " + str(role))
            operations.append(_flow(flow["event_id"], sender, receiver, "ETH", flow["amount_raw"], role, fixed))
        emit_anchors("post")
    return initial, operations, targets, True


def _compile_graph(document):
    if document.get("scope") == "ORDER_UNRESOLVED_MODEL_NOT_SOLVABLE" or document.get("fact_conflicts"):
        raise ValueError("Unresolved order or conflicting facts")
    initial = {key: None if value is None else _raw(value) for key, value in document.get("initial_balances", {}).items()}
    targets = observed_targets(document)
    terminals = set(document.get("target_accounts", []))
    operations = []
    events = sorted(document["events"], key=lambda e: e["order"])
    if len({e["order"] for e in events}) != len(events) or any(not isinstance(e["order"], int) for e in events):
        raise ValueError("Distinct observed integer event order required")
    for e in events:
        kind, eid, asset = e["kind"], e["id"], e["asset"]
        if e.get("from") in terminals or (e.get("reverted") and kind != "gas"):
            raise ValueError("Outgoing terminal or reverted physical flow")
        if e.get("context_only") and kind not in {"normal_incoming", "gas", "boundary_outflow"}:
            raise ValueError("Context-only record lacks explicit safe role")
        if kind in {"seed", "normal_incoming", "transfer", "gas", "boundary_outflow"}:
            if kind == "gas" and asset != "ETH":
                raise ValueError("Native gas cannot consume non-ETH source")
            sender = _port(e["from"], asset) if kind in {"transfer", "gas", "boundary_outflow"} else None
            receiver = _port(e["to"], asset) if kind not in {"gas", "boundary_outflow"} else None
            fixed = _raw(e["amount_raw"]) if kind == "seed" else F(0) if kind == "normal_incoming" else None
            operations.append(_flow(eid, sender, receiver, asset, e["amount_raw"], kind.upper(), fixed))
        elif kind == "conversion":
            gross, refund, output = _raw(e["gross_raw"]), _raw(e.get("refund_raw", "0")), _raw(e["output_raw"])
            if e.get("certified_semantics") not in {"SYNTHETIC_CONTROLLED", "CERTIFIED_LOCAL_COMPONENT"}:
                raise NotApplicable("Uncertified protocol conversion")
            if gross <= refund or output != gross - refund or {asset, e["output_asset"]} != {"ETH", "WETH"}:
                raise NotApplicable("Stage1C Haircut/marking only supports certified ETH-WETH 1:1 conversion")
            operations.append({"kind": "conversion", "id": eid, "asset": asset, "output_asset": e["output_asset"],
                               "sender": _port(e["from"], asset), "receiver": _port(e["to"], e["output_asset"]),
                               "refund_to": _port(e.get("refund_to", e["from"]), asset),
                               "gross": gross, "refund": refund, "output": output})
        elif kind == "multioutput":
            if e.get("certified_semantics") != "SYNTHETIC_CONTROLLED":
                raise NotApplicable("Only controlled multioutput semantics supported")
            outputs = [{"receiver": _port(output["to"], asset), "amount": _raw(output["amount_raw"])} for output in e["outputs"]]
            gross = _raw(e["gross_raw"])
            if sum((output["amount"] for output in outputs), F(0)) != gross:
                raise ValueError("Multioutput physical ports do not close")
            operations.append({"kind": "multioutput", "id": eid, "asset": asset, "sender": _port(e["from"], asset),
                               "gross": gross, "outputs": outputs})
        else:
            raise NotApplicable("Unsupported physical operation: " + kind)
        if e.get("balance_anchors_after"):
            operations.append({"kind": "anchor", "id": "after:" + eid,
                               "balances": {k: _raw(v) for k, v in e["balance_anchors_after"].items()}})
    # Unknown absorbing service balances do not affect proportional outgoing
    # allocation: their credited totals are retained only as sink accounting.
    for key in targets:
        initial[key] = initial.get(key) if initial.get(key) is not None else F(0)
    return initial, operations, targets, False


def _operation_ports(op):
    """Physical event port capacities for typed event output, no attribution."""
    if op["kind"] == "flow":
        return [(op["id"], op["asset"], op["amount"], op["receiver"], op["role"])]
    if op["kind"] == "conversion":
        eid, asset = op["id"], op["asset"]
        return [(eid + ":input", asset, op["gross"], None, "PROTOCOL_INPUT"),
                (eid + ":refund", asset, op["refund"], op["refund_to"], "PROTOCOL_REFUND"),
                (eid + ":net", asset, op["gross"] - op["refund"], None, "PROTOCOL_NET"),
                (eid + ":output", op["output_asset"], op["output"], op["receiver"], "PROTOCOL_OUTPUT")]
    if op["kind"] == "multioutput":
        return [(op["id"] + ":input", op["asset"], op["gross"], None, "MULTIOUTPUT_INPUT")] + [
            (op["id"] + f":output{i}", op["asset"], o["amount"], o["receiver"], "MULTIOUTPUT_OUTPUT") for i, o in enumerate(op["outputs"])]
    return []


def _compile(document):
    initial, operations, targets, context = _compile_context(document) if document.get("schema_version") == CONTEXT_SCHEMA else _compile_graph(document)
    names = [port[0] for op in operations for port in _operation_ports(op)]
    seeds = [op for op in operations if op["kind"] == "flow" and op["role"] == "SEED"]
    if len(names) != len(set(names)) or len(seeds) != 1 or seeds[0]["amount"] <= 0:
        raise ValueError("Distinct event identities and exactly one positive seed required")
    named = {port[0]: port for op in operations for port in _operation_ports(op)}
    for target, entries in targets.items():
        if not entries or len(entries) != len(set(entries)):
            raise ValueError("Target needs distinct observed entering events")
        for eid in entries:
            if eid not in named or named[eid][3] != target:
                raise ValueError("Target entry recipient does not match observation")
    return initial, operations, targets, context


def _marking(operations):
    """Temporal asset states; persistent marking deliberately ignores balances."""
    state, marked, witness = {}, {}, {}
    for op in operations:
        kind, eid = op["kind"], op["id"]
        if kind == "anchor":
            continue
        sender = op.get("sender")
        if kind == "flow":
            active = op["amount"] > 0 and (op["role"] == "SEED" or (op["fixed"] is None and sender in state))
            path = ([] if op["role"] == "SEED" else state.get(sender, [])) + [eid] if active else None
            marked[eid], witness[eid] = active, path
            if active and op["receiver"] is not None:
                state.setdefault(op["receiver"], path)
        else:
            active = op["gross"] > 0 and sender in state
            for port_id, _, amount, receiver, _ in _operation_ports(op):
                marked[port_id] = active and amount > 0
                path = state.get(sender, []) + [port_id] if marked[port_id] else None
                witness[port_id] = path
                if path and receiver is not None:
                    state.setdefault(receiver, path)
    return marked, witness


def _boundary_completion(operations, context):
    incoming, outgoing, bmin = F(0), F(0), F(0)
    sequence = []
    if context:
        for op in operations:
            if op["kind"] != "flow":
                continue
            if op["sender"] == OUTSIDE:
                incoming += op["amount"]
            elif op["receiver"] == OUTSIDE:
                outgoing += op["amount"]
            else:
                continue
            bmin = max(bmin, incoming - outgoing)
            sequence.append({"event_id": op["id"], "direction": "RETURN" if op["sender"] == OUTSIDE else "EXIT",
                             "actual_raw": _fmt(op["amount"]), "cumulative_incoming_raw": _fmt(incoming),
                             "cumulative_outgoing_raw": _fmt(outgoing), "prefix_deficit_raw": _fmt(incoming - outgoing)})
    return {"convention": "H_BMIN_BOUNDARY_V1", "adopted": bool(context and incoming),
            "is_observed_fact": False, "B0_out_raw": _fmt(bmin), "S0_out_raw": "0", "asset": "ETH",
            "selection_basis": "max(0, max_prefix(cumulative_unknown_returns - cumulative_nonservice_exits))",
            "sequence": sequence}, bmin


def _haircut(initial, operations, context):
    actual, source, allocation, trace = dict(initial), {k: F(0) for k in initial}, {}, []
    boundary, bmin = _boundary_completion(operations, context)
    if context:
        actual[OUTSIDE] = bmin
    injected, losses, converted = {}, {}, {}

    def credit(key, amount, attributed):
        if key is None:
            return
        actual[key] = None if actual.get(key) is None else actual[key] + amount
        source[key] = source.get(key, F(0)) + attributed
        if actual[key] is not None and not F(0) <= source[key] <= actual[key]:
            raise ValueError("Haircut source outside observed actual balance at credit " + key)

    def debit(key, amount, eid):
        if key is None:
            return F(0)
        available, related = actual.get(key), source.get(key, F(0))
        if available is None:
            if related and amount:
                raise NotApplicable("UNKNOWN_MODELED_ACTUAL_BALANCE at " + key + " before " + eid)
            value = F(0)
        else:
            if amount > available or not F(0) <= related <= available:
                raise ValueError("Haircut physical overdraft or invalid source state before " + eid)
            value = F(0) if amount == 0 else amount * related / available
            actual[key] -= amount
        source[key] = related - value
        trace.append({"event_id": eid, "account_asset": key, "actual_before_raw": _fmt(available),
                      "source_before_raw": _fmt(related), "actual_debit_raw": _fmt(amount), "source_debit_raw": _fmt(value),
                      "ratio": _fmt(related / available) if available else "0" if related == 0 else None})
        return value

    for op in operations:
        kind, eid = op["kind"], op["id"]
        if kind == "anchor":
            for key, observed in op["balances"].items():
                if actual.get(key) is not None and actual[key] != observed:
                    raise ValueError("Anchor conflicts with physical ledger: " + eid + ":" + key)
                if source.get(key, F(0)) > observed:
                    raise ValueError("Haircut point violates reliable source capacity: " + eid)
                actual[key] = observed
            continue
        asset = op["asset"]
        if kind == "flow":
            value = op["fixed"] if op["fixed"] is not None else debit(op["sender"], op["amount"], eid)
            allocation[eid] = value
            credit(op["receiver"], op["amount"], value)
            if op["role"] == "SEED":
                injected[asset] = injected.get(asset, F(0)) + value
            if op["receiver"] is None:
                losses[asset] = losses.get(asset, F(0)) + value
        elif kind == "conversion":
            gross_source = debit(op["sender"], op["gross"], eid + ":input")
            refund_source = gross_source * op["refund"] / op["gross"]
            net_source = gross_source - refund_source
            # Restricted compiler guarantees matching decimals/raw 1:1 ports.
            output_source = net_source
            allocation.update({eid + ":input": gross_source, eid + ":refund": refund_source,
                               eid + ":net": net_source, eid + ":output": output_source})
            credit(op["refund_to"], op["refund"], refund_source)
            credit(op["receiver"], op["output"], output_source)
            converted[asset] = converted.get(asset, F(0)) - net_source
            converted[op["output_asset"]] = converted.get(op["output_asset"], F(0)) + output_source
        elif kind == "multioutput":
            value = debit(op["sender"], op["gross"], eid + ":input")
            allocation[eid + ":input"] = value
            for i, output in enumerate(op["outputs"]):
                attributed = value * output["amount"] / op["gross"] if op["gross"] else F(0)
                allocation[eid + f":output{i}"] = attributed
                credit(output["receiver"], output["amount"], attributed)
    remaining = {}
    for key, value in source.items():
        asset = key.rsplit("|", 1)[1]
        remaining[asset] = remaining.get(asset, F(0)) + value
    conservation = {}
    for asset in sorted(set(injected) | set(remaining) | set(losses) | set(converted)):
        lhs = injected.get(asset, F(0)) + converted.get(asset, F(0))
        rhs = remaining.get(asset, F(0)) + losses.get(asset, F(0))
        conservation[asset] = {"seed_raw": _fmt(injected.get(asset, F(0))), "net_conversion_raw": _fmt(converted.get(asset, F(0))),
                               "retained_in_accounts_terminals_and_outside_raw": _fmt(remaining.get(asset, F(0))),
                               "fees_and_boundary_sinks_raw": _fmt(losses.get(asset, F(0))), "exact_conserved": lhs == rhs}
    if not all(row["exact_conserved"] for row in conservation.values()):
        raise ValueError("Internal Haircut conservation failure")
    return allocation, boundary, {"allocation_construction": "EXACT_FRACTION_PROPORTIONAL_REPLAY", "per_asset": conservation,
                                 "final_actual_balance_raw": {k: _fmt(v) for k, v in sorted(actual.items())},
                                 "final_source_balance_raw": {k: _fmt(v) for k, v in sorted(source.items())}, "debit_trace": trace}


def run_baseline(document, method_id):
    """Run one independent baseline, returning typed results on all targets.

    A NOT_APPLICABLE result has null amount/allocation fields, and is retained
    in denominators by the experiment runner. Malformed facts raise ValueError;
    the CLI is responsible for retaining that error and returning nonzero.
    """
    if method_id not in METHODS:
        raise ValueError("Unknown baseline: " + str(method_id))
    kind = {"BOUNDED_REACHABILITY": "ADDRESS_EVENT_SET_NO_AMOUNT", "POISON": "POISON_NOMINAL_RAW", "HAIRCUT": "PROPORTIONAL_FEASIBLE_POINT"}[method_id]
    base = {"method_id": method_id, "method_version": VERSION, "query_or_sample_id": document.get("query_id", document.get("scenario_id")),
            "input_fact_hash": _hash(document), "output_kind": kind, "amount_unit": "RAW_PER_ASSET_EXACT_RATIONAL",
            "input_usage": {"physical_events_order_assets_targets": True, "source_zero_roles": True,
                            "balances_and_anchors": method_id == "HAIRCUT", "actual_fees": True,
                            "main_endpoints": False, "hidden_or_oracle": False}}
    try:
        initial, operations, targets, context = _compile(document)
        marks, witnesses = _marking(operations)
        allocation, boundary, audit = (None, None, None)
        if method_id == "HAIRCUT":
            allocation, boundary, audit = _haircut(initial, operations, context)
        events = {}
        for op in operations:
            for eid, asset, amount, recipient, role in _operation_ports(op):
                value = allocation[eid] if allocation is not None else amount if marks[eid] else F(0)
                events[eid] = {"asset": asset, "actual_raw": _fmt(amount), "recipient": recipient, "role": role,
                               "supported": bool(value > 0) if allocation is not None else marks[eid],
                               "point_raw": _fmt(value) if method_id == "HAIRCUT" else None,
                               "nominal_raw": _fmt(value) if method_id == "POISON" else None,
                               "source_amount_raw": _fmt(value) if method_id == "HAIRCUT" else None,
                               "witness": witnesses[eid] if method_id != "HAIRCUT" else None}
        addresses, joint = {}, {}
        for account, entries in targets.items():
            asset = account.rsplit("|", 1)[1]
            supported = any(events[eid]["supported"] for eid in entries)
            point = sum((F(events[eid]["point_raw"]) for eid in entries), F(0)) if method_id == "HAIRCUT" else None
            nominal = sum((F(events[eid]["nominal_raw"]) for eid in entries), F(0)) if method_id == "POISON" else None
            addresses[account] = {"asset": asset, "events": list(entries), "supported": supported,
                                  "point_raw": _fmt(point), "nominal_raw": _fmt(nominal), "source_amount_raw": _fmt(point)}
            group = joint.setdefault(asset, {"events": [], "addresses": [], "point_raw": "0" if method_id == "HAIRCUT" else None,
                                             "nominal_raw": "0" if method_id == "POISON" else None, "source_amount_raw": None})
            group["events"].extend(entries); group["addresses"].append(account)
            if point is not None:
                group["point_raw"] = _fmt(F(group["point_raw"]) + point)
                group["source_amount_raw"] = group["point_raw"]
            if nominal is not None:
                group["nominal_raw"] = _fmt(F(group["nominal_raw"]) + nominal)
        return {**base, "status": "OK", "applicability": "APPLICABLE_WITH_EXPLICIT_BOUNDARY_ASSUMPTION" if boundary and boundary["adopted"] else "APPLICABLE",
                "events": events, "addresses": addresses, "joint_by_asset": joint,
                "output_address_ids": [a for a, row in addresses.items() if row["supported"]],
                "output_event_ids": [eid for entries in targets.values() for eid in entries if events[eid]["supported"]],
                "allocation_raw": {eid: _fmt(v) for eid, v in allocation.items()} if allocation is not None else None,
                "boundary_completion": boundary, "construction_audit": audit, "failure_reason": None}
    except NotApplicable as exc:
        return {**base, "status": "NOT_APPLICABLE", "applicability": "NOT_APPLICABLE", "failure_reason": str(exc),
                "events": None, "addresses": None, "joint_by_asset": None, "output_address_ids": None,
                "output_event_ids": None, "allocation_raw": None, "boundary_completion": None, "construction_audit": None}
