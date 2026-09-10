"""R3 exact source conservation from aligned, evidence-bound ETH account ledgers.

This is a separate context interface: the accepted R2 graph builder is unchanged.
Balances are physical capacities, never new source.  A single observed seed is
the only injection; an explicit outside reservoir can only return prior exits.
The supplied ledger must establish order and actual-state alignment.  This
module independently rejects contradictions and audits every reported witness.
"""
from __future__ import annotations

import argparse
import copy
from fractions import Fraction as F
import hashlib
import json
import math
from pathlib import Path

from lp_model import Model, raw, q, fmt, _certify, _rational_candidate
from stage1d_gap_sequence import (GapSequence, GapAccumulator, gap_count,
    serialize_gaps, filter_gaps_by_account)

from stage1d_multiasset_context import (SCHEMA as MULTIASSET_SCHEMA, ASSETS, ETH, WETH,
    key_asset, boundary, ordered_operations, conversion_ports, validate_extension)

SCHEMA = "stage1b-r3-context-model-v1"
BOUNDARY = "@outside_reservoir|ETH"
ROLES = {"SEED", "CANDIDATE", "MODELED_INTERNAL", "BACKGROUND_NORMAL",
         "BOUNDARY_OUTFLOW", "UNKNOWN_EXTERNAL_INCOMING"}


def _gap_json(value):
    if isinstance(value, (GapSequence, GapAccumulator)):
        return serialize_gaps(value)
    raise TypeError('Not JSON serializable: ' + type(value).__name__)


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False, default=_gap_json).encode()).hexdigest()


def _key(value):
    if value is None:
        return None
    key_asset(value)
    return value


def _position(tx):
    block, index = tx.get("block_number"), tx.get("tx_index")
    if not isinstance(block, int) or not isinstance(index, int) or index < 0:
        raise ValueError("A transaction needs observed integer block and tx_index")
    return block, index


def validate_document(document):
    if document.get("schema_version") not in {SCHEMA, MULTIASSET_SCHEMA}:
        raise ValueError("Unsupported R3 context model schema")
    multiasset = document.get("schema_version") == MULTIASSET_SCHEMA
    if multiasset:
        validate_extension(document)
    elif any(not a.get("account_id", "").endswith("|ETH") for a in document.get("accounts", [])):
        raise ValueError("Native context schema requires ETH accounts")
    if document.get("fact_conflicts"):
        raise ValueError("EVIDENCE_CONFLICT_MODEL_BLOCKED: contradictory physical facts")
    accounts = {}
    for account in document.get("accounts", []):
        key = _key(account["account_id"])
        if key in accounts or key.startswith("@outside_reservoir|"):
            raise ValueError("Duplicate or reserved account identity")
        value = account.get("initial_actual_balance_raw")
        if value is not None:
            raw(value)
        if raw(account.get("initial_source_raw", "0")) != 0:
            raise ValueError("Single seed model forbids additional initial source")
        if not account.get("initial_source_basis"):
            raise ValueError("Initial source zero needs a recorded causal basis")
        position = account.get("initial_position", {})
        if position.get("phase") not in {"BLOCK_END", "TRANSACTION_PRE", "EVENT_PRE"}:
            raise ValueError("Explicit initial anchor position required")
        if not isinstance(position.get("block_number"), int):
            raise ValueError("Initial anchor block missing")
        if position.get("phase") != "BLOCK_END":
            if not isinstance(position.get("tx_index"), int):
                raise ValueError("Transaction/event pre anchor needs transaction index")
            if position.get("phase") == "EVENT_PRE":
                raise ValueError("Event-pre initialization requires a dedicated aligned state adapter")
        if account.get("anchor_includes_seed"):
            raise ValueError("Anchor already includes seed; double injection rejected")
        accounts[key] = account
    def check_account_position(key, position):
        if key not in accounts:
            return
        init = accounts[key]["initial_position"]
        if init["phase"] == "BLOCK_END" and position[0] <= init["block_number"]:
            raise ValueError("Block-end balance cannot be used before an event in that block")
        if init["phase"] == "TRANSACTION_PRE" and position < (init["block_number"], init["tx_index"]):
            raise ValueError("Later account initialization applied to an earlier event")

    tx_ids, positions, event_ids, fee_ids, seeds = set(), set(), set(), set(), []
    previous = None
    for tx in document.get("transactions", []):
        position = _position(tx)
        if previous is not None and position <= previous:
            raise ValueError("Transactions must be strictly ordered by real block/tx index")
        previous = position
        if tx["tx_id"] in tx_ids or position in positions:
            raise ValueError("Duplicate physical transaction identity or position")
        tx_ids.add(tx["tx_id"]); positions.add(position)
        if len(tx.get("fees", [])) > 1:
            raise ValueError("A physical Ethereum transaction has exactly one gas charge")
        for fee in tx.get("fees", []):
            if fee["fee_id"] in fee_ids or fee["fee_id"] in event_ids:
                raise ValueError("Duplicate physical fee")
            fee_ids.add(fee["fee_id"])
            _key(fee["payer_account"]); raw(fee["amount_raw"])
            if key_asset(fee["payer_account"]) != ETH:
                raise ValueError("Gas payer must be an ETH account")
            check_account_position(fee["payer_account"], position)
            if fee.get("timing") != "TX_BEGIN_NET_FEE":
                raise ValueError("Unsupported or unknown fee timing")
        flows = ordered_operations(tx)
        for index, flow in enumerate(flows):
            if flow.get("kind") == "conversion":
                if not multiasset:
                    raise ValueError("Native schema cannot contain conversions")
                for event, a, amount in conversion_ports(flow):
                    if event in event_ids or event in fee_ids:
                        raise ValueError("Duplicate semantic source port")
                    event_ids.add(event); raw(amount)
                for key in (flow["from_account"], flow["to_account"]):
                    if key not in accounts:
                        raise ValueError("Conversion holder must have a modeled ledger in both assets")
                    check_account_position(key, position)
                continue
            event = flow["event_id"]
            if event in event_ids or event in fee_ids:
                raise ValueError("Duplicate physical value event")
            event_ids.add(event)
            if flow.get("role") not in ROLES:
                raise ValueError("Every value fact needs an explicit supported role")
            raw(flow["amount_raw"])
            sender, receiver = _key(flow.get("from_account")), _key(flow.get("to_account"))
            flow_asset = flow.get("asset", "ETH")
            if flow_asset not in (ASSETS if multiasset else {ETH}):
                raise ValueError("Unregistered flow asset")
            if any(key is not None and key_asset(key) != flow_asset for key in (sender, receiver)):
                raise ValueError("Flow account/asset mismatch")
            if flow.get("reverted"):
                raise ValueError("Reverted value is evidence, not successful ledger flow")
            if flow.get("flow_kind") in {"delegatecall", "callcode"}:
                raise ValueError("Delegate/callcode execution is not independent ETH value")
            if len(flows) > 1 and not flow.get("order_basis"):
                raise ValueError("Multiple transaction flows need observed execution order")
            if flow["role"] == "SEED":
                seeds.append((tx, flow))
                if receiver not in accounts:
                    raise ValueError("Seed recipient must be a modeled account")
                if sender in accounts:
                    raise ValueError("Seed sender cannot be debited as already injected source")
            if flow["role"] == "BACKGROUND_NORMAL":
                if not flow.get("source_zero_basis"):
                    raise ValueError("Context membership is not proof of source-zero income")
                if sender in accounts:
                    raise ValueError("Modeled account transfer must retain shared source variable")
            for key in (sender, receiver):
                check_account_position(key, position)
    if len(seeds) != 1 or raw(seeds[0][1]["amount_raw"]) <= 0:
        raise ValueError("Exactly one positive seed event required")
    for tx in document.get("transactions", []):
        for fee in tx.get("fees", []):
            if fee["payer_account"] not in accounts and not fee.get("source_zero_basis"):
                raise ValueError("Outside payer fee needs explicit source-zero basis")
    targets = document.get("objective_groups", {})
    all_flows = {flow["event_id"]: flow for tx in document.get("transactions", []) for flow in tx.get("flows", [])}
    objective_events = set()
    for group, names in targets.items():
        _key(group)
        if not names or len(set(names)) != len(names) or not set(names) <= event_ids:
            raise ValueError("Objectives must name distinct observed events")
        if any(all_flows[name]["role"] != "CANDIDATE" or all_flows[name].get("to_account") != group for name in names):
            raise ValueError("Service objective must match its observed candidate recipient")
        if objective_events & set(names):
            raise ValueError("One physical service entry cannot belong to different recipients")
        objective_events.update(names)
    if set(document.get("all_service_entries", objective_events)) != objective_events:
        raise ValueError("All-service objective must equal union of service entry events")
    for anchor in document.get("anchors", []):
        if anchor["account_id"] not in accounts:
            raise ValueError("Anchor names unknown modeled account")
        if anchor.get("tx_id") not in tx_ids or anchor.get("when") not in {"pre", "post"}:
            raise ValueError("Anchor must be aligned to a specific transaction pre/post state")
        raw(anchor["actual_balance_raw"])
    return accounts, seeds[0], objective_events


def _ports(flow, accounts, terminals):
    role = flow["role"]
    pool = boundary(flow.get("asset", "ETH"))
    sender, receiver = flow.get("from_account"), flow.get("to_account")
    if sender in terminals:
        raise ValueError("First service is absorbing; service ledger and returns forbidden")
    if role == "SEED":
        return None, receiver, "SEED_FIXED"
    if role == "BACKGROUND_NORMAL":
        if receiver not in accounts:
            raise ValueError("Background income must affect a modeled account")
        return None, receiver, "NORMAL_FIXED_ZERO"
    if role == "UNKNOWN_EXTERNAL_INCOMING":
        if sender in accounts or receiver not in accounts:
            raise ValueError("Unknown external income has inconsistent endpoints")
        return pool, receiver, "PRIOR_BOUNDARY_SOURCE_ONLY"
    if sender not in accounts:
        raise ValueError("Candidate/internal/boundary sender must be modeled")
    if role == "BOUNDARY_OUTFLOW":
        if receiver in accounts or receiver in terminals:
            raise ValueError("Boundary outflow cannot discard a modeled or service recipient")
        return sender, pool, "BOUNDARY_RESERVOIR"
    if receiver in accounts or receiver in terminals:
        return sender, receiver, "SHARED_TRANSFER"
    return sender, pool, "BOUNDARY_RESERVOIR"


def build_context_model(document, *, remove_balance_information=False):
    accounts, (_, seed), objective_events = validate_document(document)
    potential = raw(seed["amount_raw"])
    terminals = set(document.get("objective_groups", {}))
    if terminals & set(accounts):
        raise ValueError("Service terminal cannot have collected complete account history")
    assets = ASSETS if document.get("schema_version") == MULTIASSET_SCHEMA else {ETH}
    source_assets = {seed.get('asset', ETH)} | {c['output_asset'] for t in document.get('transactions', []) for c in t.get('conversions', [])}
    model = Model(metadata={"model_type": "CANONICAL_MULTI_ASSET_CONTEXT_TIME_EXPANDED_LP" if len(assets) > 1 else "R3_ETH_CONTEXT_TIME_EXPANDED_LP",
        "schema_version": document["schema_version"], "query_id": document.get("query_id"),
        "document_sha256": canonical_hash(document),
        "variant": "MATCHED_INFORMATION_RELAXED" if remove_balance_information else "BEST_AVAILABLE_CONTEXT",
        "source_potential_raw": {a: fmt(potential) for a in sorted(source_assets)},
        "scales_raw_per_solver_unit": {a: fmt(potential) for a in sorted(assets)},
        "acquisition_depth_constraints_in_lp": False, "fund_age_constraints_in_lp": False,
        "boundary_assumption": "Only previously observed nonservice boundary exits may return; pooled external locations; no unobserved external source transport certified",
        "fee_timing": "Net actual fee funded before transaction flows; stronger upfront/refund requirements require separate evidence",
        "gaps": serialize_gaps(document.get("gaps", [])),
        "declared_model_assumptions": copy.deepcopy(document.get("assumptions", [])),
        "constraint_provenance": []})
    if document.get('schema_version') == MULTIASSET_SCHEMA:
        model.metadata.update(semantic_scope_id=document['semantic_scope_id'],
            asset_registry=copy.deepcopy(document['asset_registry']),
            cross_asset_source_conservation='CERTIFIED_1TO1_RAW_EQUIVALENCE_INTERNAL_ONLY',
            external_amount_reporting='SEPARATE_ASSETS_NO_INDEPENDENT_UPPER_SUM')
    initial = {k: None if a.get("initial_actual_balance_raw") is None else raw(a["initial_actual_balance_raw"])
               for k, a in accounts.items()}
    actual = {**initial, **{k: None for k in terminals}, **{boundary(a): None for a in assets}}
    last = {}
    provenance = model.metadata["constraint_provenance"]

    def cap(value):
        return potential if value is None or remove_balance_information else min(potential, value)

    def state(key, name, previous, delta, actual_balance, evidence, operation, kind="balance"):
        idx = model.var(name, key_asset(key), potential, F(0), cap(actual_balance), kind)
        row = {idx: F(1)}
        if previous is not None:
            row[previous] = F(-1)
        for event_index, sign in delta.items():
            row[event_index] = row.get(event_index, F(0)) - sign
        model.equality("conservation:" + name, row)
        model.balance_lifts.append((idx, previous, delta))
        provenance.append({"operation_id": operation, "account_id": key,
            "variable": name, "equality": "conservation:" + name,
            "physical_actual_balance_raw": fmt(actual_balance),
            "actual_capacity_applied": actual_balance is not None and not remove_balance_information,
            "source_capacity_raw": fmt(cap(actual_balance)), "evidence_ids": evidence})
        return idx

    for key in sorted(actual):
        last[key] = state(key, "initial:" + key, None, {}, actual[key],
                          accounts.get(key, {}).get("evidence_ids", []), "INITIALIZATION")
        provenance[-1].update({"initial_anchor_id": accounts.get(key, {}).get("initial_anchor_id"),
            "initial_position": accounts.get(key, {}).get("initial_position"),
            "initial_source_basis": accounts.get(key, {}).get("initial_source_basis", "ABSORBING_TERMINAL_OR_CONSERVED_OUTSIDE_RESERVOIR_STARTS_WITH_ZERO_SOURCE")})

    anchors = {}
    for anchor in document.get("anchors", []):
        anchors.setdefault((anchor["tx_id"], anchor["when"]), []).append(anchor)

    def apply_anchors(tx, when):
        declared = tx.get(when + "_actual_balances", {})
        facts = list(anchors.get((tx["tx_id"], when), []))
        for key, value in declared.items():
            if value is not None:
                facts.append({"anchor_id": tx["tx_id"] + ":" + when + ":" + key,
                    "account_id": key, "actual_balance_raw": value,
                    "evidence_ids": tx.get("state_evidence_ids", [])})
        for anchor in facts:
            key, observed = anchor["account_id"], raw(anchor["actual_balance_raw"])
            if key not in actual:
                raise ValueError("Actual state names an unmodeled account")
            if actual[key] is not None and actual[key] != observed:
                raise ValueError("EVIDENCE_CONFLICT_MODEL_BLOCKED: aligned anchor disagrees with integer replay at " + anchor["anchor_id"])
            actual[key] = observed
            # Keep variables/equalities identical in the information-relaxed model.
            last[key] = state(key, "anchor:" + anchor["anchor_id"], last[key], {}, observed,
                              anchor.get("evidence_ids", []), anchor["anchor_id"], "anchored_balance")

    def operation(eid, sender, receiver, amount, fixed, evidence, role, asset="ETH"):
        idx = model.var(eid, asset, potential, fixed if fixed is not None else F(0),
                        fixed if fixed is not None else min(amount, potential),
                        "gas_source" if role == "FEE" else "flow_source")
        model.event_variables[eid] = idx
        provenance.append({"operation_id": eid, "role": role, "variable": eid,
            "physical_amount_raw": fmt(amount), "source_lower_raw": fmt(fixed or F(0)),
            "source_upper_raw": fmt(fixed if fixed is not None else min(amount, potential)),
            "sender": sender, "receiver": receiver, "evidence_ids": evidence})
        # Debit before credit is necessary even for self-transfer/refund flows.
        if sender is not None:
            value = actual[sender]
            residue = None if value is None else value - amount
            if residue is not None and residue < 0:
                raise ValueError("EVIDENCE_CONFLICT_MODEL_BLOCKED: negative pre-credit actual residue at " + eid)
            actual[sender] = residue
            last[sender] = state(sender, "debit:" + eid, last[sender], {idx: F(-1)},
                                 residue, evidence, eid, "pre_credit_residue")
        if receiver is not None:
            value = actual[receiver]
            total = None if value is None else value + amount
            actual[receiver] = total
            last[receiver] = state(receiver, "credit:" + eid, last[receiver], {idx: F(1)},
                                   total, evidence, eid)

    for tx in document.get("transactions", []):
        apply_anchors(tx, "pre")
        for fee in tx.get("fees", []):
            payer = fee["payer_account"]
            if payer in terminals:
                raise ValueError("Service terminal fee history outside model scope")
            operation(fee["fee_id"], payer if payer in accounts else None, None,
                      raw(fee["amount_raw"]), None if payer in accounts else F(0),
                      fee.get("evidence_ids", []), "FEE")
        for flow in ordered_operations(tx):
            if flow.get("kind") == "conversion":
                eid, a, out_asset = flow["id"], flow["asset"], flow["output_asset"]
                gross, output = raw(flow["gross_raw"]), raw(flow["output_raw"])
                evidence = flow.get("evidence_ids", [])
                operation(eid + ":input", flow["from_account"], None, gross, None, evidence, "PROTOCOL_INPUT", a)
                operation(eid + ":refund", None, flow["from_account"], F(0), F(0), evidence, "PROTOCOL_REFUND", a)
                net = model.var(eid + ":net", a, potential, F(0), min(gross, potential), "protocol_net_source")
                model.event_variables[eid + ":net"] = net
                model.equality("protocol:" + eid + ":gross_refund_net", {
                    model.event_variables[eid + ":input"]: F(1), model.event_variables[eid + ":refund"]: F(-1), net: F(-1)})
                operation(eid + ":output", None, flow["to_account"], output, None, evidence, "PROTOCOL_OUTPUT", out_asset)
                model.equality("protocol:" + eid + ":fixed_ratio", {net: F(-1), model.event_variables[eid + ":output"]: F(1)})
                provenance.append({"operation_id": eid, "unit_id": flow["unit_id"], "variable": eid + ":net",
                    "role": "CERTIFIED_1TO1_CONVERSION", "raw_consumed_event_ids": flow["raw_consumed_event_ids"],
                    "evidence_ids": evidence, "source_link_equality": "protocol:" + eid + ":fixed_ratio"})
                continue
            sender, receiver, semantics = _ports(flow, accounts, terminals)
            amount = raw(flow["amount_raw"])
            fixed = amount if flow["role"] == "SEED" else F(0) if flow["role"] == "BACKGROUND_NORMAL" else None
            operation(flow["event_id"], sender, receiver, amount, fixed,
                      flow.get("evidence_ids", []), flow["role"], flow.get("asset", "ETH"))
        apply_anchors(tx, "post")
    model.metadata.update({"known_initial_balances": sum(v is not None for v in initial.values()),
        "unknown_initial_balances": [k for k, v in initial.items() if v is None],
        "initialization_positions": {k: a["initial_position"] for k, a in accounts.items()},
        "enabled_initial_actual_caps": 0 if remove_balance_information else sum(v is not None for v in initial.values()),
        "enabled_anchor_caps": 0 if remove_balance_information else sum(p["actual_capacity_applied"] for p in provenance if p.get("variable", "").startswith("anchor:")),
        "enabled_explicit_aligned_anchor_caps": 0 if remove_balance_information else len(document.get("anchors", [])),
        "enabled_derived_transaction_state_caps": 0 if remove_balance_information else sum(value is not None for tx in document["transactions"] for side in ("pre", "post") for value in tx.get(side + "_actual_balances", {}).values()),
        "anchor_count_semantics": "Initial actual caps and explicit aligned anchors correspond to anchor facts; derived transaction state caps are replayed states, not additional RPC balance acquisitions.",
        "fee_count": sum(len(tx.get("fees", [])) for tx in document.get("transactions", [])),
        "context_role_counts": {role: sum(flow["role"] == role for tx in document.get("transactions", []) for flow in tx.get("flows", [])) for role in sorted(ROLES)},
        "terminal_balances": {k: model.variables[i].name for k, i in last.items()},
        "final_actual_balances_raw": {k: fmt(v) for k, v in actual.items()},
        "state_variable_count": sum(v.kind not in {"flow_source", "gas_source"} for v in model.variables),
        "balance_status": "INFORMATION_RELAXED" if remove_balance_information else "PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS" if gap_count(document.get("gaps", [])) or any(v is None for v in initial.values()) else "FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE"})
    return model


def structural_nesting(informed, relaxed):
    same = (len(informed.variables) == len(relaxed.variables) and informed.eq == relaxed.eq
            and informed.rhs == relaxed.rhs and informed.event_variables == relaxed.event_variables)
    same &= all(a.name == b.name and a.scale == b.scale and a.lower >= b.lower and a.upper <= b.upper
                for a, b in zip(informed.variables, relaxed.variables))
    return {"nested": bool(same), "proof": "Identical variables, event ports, source injection, ordering and equalities; only physical actual-balance upper bounds are removed.",
            "removed_information": "INITIAL_AND_ALIGNED_AND_REPLAYED_ACTUAL_BALANCE_CAPS"}


def target_completeness(document, events):
    """Conservative dependency components, keeping reliable local bounds intact.

    Shared upstream source creates coupling with competing downstream branches,
    so a directed ancestor-only walk would understate dependencies.  Terminals
    are absorbing and therefore never connect their other entry branches.
    """
    accounts, (_, seed), _ = validate_document(document)
    terminals = set(document.get("objective_groups", {}))
    pools = {boundary(a) for a in ASSETS} if document.get('schema_version') == MULTIASSET_SCHEMA else {BOUNDARY}
    adjacent = {key: set() for key in set(accounts) | pools}
    event_senders = {}
    for tx in document["transactions"]:
        for flow in ordered_operations(tx):
            if flow.get('kind') == 'conversion':
                sender, receiver = flow['from_account'], flow['to_account']
                for name, _, _ in conversion_ports(flow): event_senders[name] = sender
            else:
                sender, receiver, _ = _ports(flow, accounts, terminals)
                event_senders[flow["event_id"]] = sender
            if sender in adjacent and receiver in adjacent:
                adjacent[sender].add(receiver); adjacent[receiver].add(sender)
    pending = [event_senders[name] for name in events if event_senders[name] in adjacent]
    dependencies = set()
    while pending:
        key = pending.pop()
        if key not in dependencies:
            dependencies.add(key); pending.extend(adjacent[key] - dependencies)
    relevant_gaps = serialize_gaps(filter_gaps_by_account(
        document.get("gaps", []), dependencies, include_none=True))
    unknown = [key for key in sorted(dependencies & set(accounts))
               if accounts[key].get("initial_actual_balance_raw") is None]
    return {"status": "PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS" if gap_count(relevant_gaps) or unknown else "FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE",
        "dependency_accounts": sorted(dependencies & set(accounts)),
        "unknown_initial_balance_accounts": unknown, "gaps": relevant_gaps,
        "dependency_rule": "Connected shared-source flow component excluding absorbing service terminals; conservative about competing-branch constraints.",
        "known_constraints_retained_despite_other_gaps": True,
        "scope_assumptions": document.get("assumptions", [])}


def audit_context_witness(document, event_source_raw, *, remove_balance_information=False, no_protocol_continuation=False):
    """Independent integer/rational ledger replay; never reads LP rows/bounds."""
    accounts, (_, seed), objectives = validate_document(document)
    terminals = set(document.get("objective_groups", {}))
    assets = ASSETS if document.get("schema_version") == MULTIASSET_SCHEMA else {ETH}
    pools = {boundary(a) for a in assets}
    source = {k: F(0) for k in sorted(set(accounts) | terminals | pools)}
    actual = {k: None if a.get("initial_actual_balance_raw") is None else raw(a["initial_actual_balance_raw"])
              for k, a in accounts.items()}
    actual.update({k: None for k in terminals | pools})
    expected = {f["event_id"] for t in document["transactions"] for f in t.get("flows", [])}
    expected |= {f["fee_id"] for t in document["transactions"] for f in t.get("fees", [])}
    expected |= {name for t in document["transactions"] for c in t.get("conversions", []) for name, _, _ in conversion_ports(c)}
    if set(event_source_raw) != expected:
        return {"exact_feasible": False, "errors": ["Witness does not name every physical flow/fee exactly once"]}
    errors, snapshots = [], []
    total_fee, seed_total, protocol_loss = F(0), F(0), F(0)
    anchors = {}
    for anchor in document.get("anchors", []):
        anchors.setdefault((anchor["tx_id"], anchor["when"]), []).append(anchor)

    def check(key, operation):
        if source[key] < 0:
            errors.append(operation + ": source spends before available at " + key)
        if actual[key] is not None and actual[key] < 0:
            errors.append(operation + ": actual balance negative at " + key)
        if not remove_balance_information and actual[key] is not None and source[key] > actual[key]:
            errors.append(operation + ": source exceeds observed/replayed balance at " + key)

    def apply_states(tx, when):
        facts = list(anchors.get((tx["tx_id"], when), []))
        facts += [{"anchor_id": tx["tx_id"] + ":" + when, "account_id": key, "actual_balance_raw": value}
                  for key, value in tx.get(when + "_actual_balances", {}).items() if value is not None]
        for anchor in facts:
            key, amount = anchor["account_id"], raw(anchor["actual_balance_raw"])
            if actual[key] is not None and actual[key] != amount:
                errors.append(anchor["anchor_id"] + ": actual anchor mismatch")
            actual[key] = amount
            check(key, anchor["anchor_id"])

    def transfer(name, amount, sender, receiver, fixed=None):
        value = q(event_source_raw[name])
        if value < 0 or value > amount or (fixed is not None and value != fixed):
            errors.append(name + ": invalid physical source share")
        if sender:
            source[sender] -= value
            if actual[sender] is not None:
                actual[sender] -= amount
            check(sender, name + ":debit")
        if receiver:
            source[receiver] += value
            if actual[receiver] is not None:
                actual[receiver] += amount
            check(receiver, name + ":credit")
        snapshots.append({"operation_id": name, "source_raw": fmt(value),
                          "sender_source_after_raw": fmt(source[sender]) if sender else None,
                          "receiver_source_after_raw": fmt(source[receiver]) if receiver else None})

    for tx in document["transactions"]:
        apply_states(tx, "pre")
        for fee in tx.get("fees", []):
            payer, amount = fee["payer_account"], raw(fee["amount_raw"])
            transfer(fee["fee_id"], amount, payer if payer in accounts else None, None,
                     None if payer in accounts else F(0))
            total_fee += q(event_source_raw[fee["fee_id"]])
        for flow in ordered_operations(tx):
            if flow.get("kind") == "conversion":
                eid, gross = flow["id"], raw(flow["gross_raw"])
                values = {suffix: q(event_source_raw[eid + ":" + suffix]) for suffix in ("input", "refund", "net", "output")}
                if values["input"] - values["refund"] != values["net"] or values["refund"] != 0:
                    errors.append(eid + ": conversion gross/refund/net source mismatch")
                if (values["output"] != (F(0) if no_protocol_continuation else values["net"])):
                    errors.append(eid + ": independently certified source ratio or ablation mismatch")
                if not F(0) <= values["net"] <= gross:
                    errors.append(eid + ": invalid net source capacity")
                transfer(eid + ":input", gross, flow["from_account"], None)
                transfer(eid + ":refund", F(0), None, flow["from_account"], F(0))
                transfer(eid + ":output", raw(flow["output_raw"]), None, flow["to_account"], F(0) if no_protocol_continuation else None)
                protocol_loss += values["net"] - values["output"]
                continue
            # Role interpretation is repeated here rather than using builder ports.
            role, sender, receiver = flow["role"], flow.get("from_account"), flow.get("to_account")
            amount, fixed = raw(flow["amount_raw"]), None
            if role == "SEED":
                sender, fixed = None, amount
                seed_total += amount
            elif role == "BACKGROUND_NORMAL":
                sender, fixed = None, F(0)
            elif role == "UNKNOWN_EXTERNAL_INCOMING":
                sender = boundary(flow.get("asset", "ETH"))
            elif role == "BOUNDARY_OUTFLOW" or receiver not in accounts and receiver not in terminals:
                receiver = boundary(flow.get("asset", "ETH"))
            if sender in terminals:
                errors.append(flow["event_id"] + ": terminal outflow forbidden")
            transfer(flow["event_id"], amount, sender, receiver, fixed)
        apply_states(tx, "post")
    remaining = sum(source.values(), F(0))
    if remaining + total_fee + protocol_loss != seed_total:
        errors.append("Global single-seed source conservation failed")
    return {"exact_feasible": not errors, "errors": errors,
        "source_seed_raw": fmt(seed_total), "source_fees_raw": fmt(total_fee),
        "source_protocol_boundary_raw": fmt(protocol_loss),
        "conservation_unit": "CERTIFIED_1TO1_SOURCE_EQUIVALENT_RAW_INTERNAL_ONLY" if len(assets) > 1 else "ETH_RAW",
        "source_remaining_in_accounts_terminals_and_boundary_raw": fmt(remaining),
        "final_source_balances_raw": {k: fmt(v) for k, v in source.items()},
        "operation_checks": snapshots,
        "method": "INDEPENDENT_EXACT_RATIONAL_LEDGER_AND_SOURCE_REPLAY_WITHOUT_LP_ROWS"}


def _propose_large_network_vertex(model, result, priority_bounds=()):
    """Sparse exact active-bound recovery; no coefficient or evidence relaxation.

    R3 retains the original small-model routine but permits larger context
    ledgers.  The numeric optimizer proposes a basis only; final exact primal
    and dual equality remain mandatory.
    """
    if len(model.variables) > 20000:
        return None
    basis = {}

    def add(row, value):
        row, value = {i: F(v) for i, v in row.items() if v}, F(value)
        while row:
            pivot = min(row)
            if pivot not in basis:
                divisor = row[pivot]
                basis[pivot] = {i: v / divisor for i, v in row.items()}, value / divisor
                return True
            old, rhs = basis[pivot]
            factor = row[pivot]
            for i, coefficient in old.items():
                updated = row.get(i, F(0)) - factor * coefficient
                if updated:
                    row[i] = updated
                else:
                    row.pop(i, None)
            value -= factor * rhs
        return value == 0

    for row, rhs in zip(model.eq, model.rhs):
        if not add(row, rhs):
            return None
    for i, variable in enumerate(model.variables):
        if variable.lower == variable.upper and not add({i: F(1)}, variable.lower):
            return None
    # These are proposals to select a vertex, never new model constraints.
    # A prior rejected candidate can nominate the exact bound it violated.
    for i, bound in priority_bounds:
        if bound not in (model.variables[i].lower, model.variables[i].upper):
            raise ValueError('Vertex recovery may only nominate original exact bounds')
        if not add({i: F(1)}, bound):
            return None
    candidates = []
    for i, variable in enumerate(model.variables):
        if variable.lower == variable.upper:
            continue
        for bound, multiplier in ((variable.lower, result.lower.marginals[i]),
                                  (variable.upper, result.upper.marginals[i])):
            distance = abs(float(result.x[i]) - float(bound))
            if distance <= 1e-7:
                candidates.append((abs(float(multiplier)) <= 1e-8, distance, i, bound))
    for _, _, i, bound in sorted(candidates):
        add({i: F(1)}, bound)
        if len(basis) == len(model.variables):
            break
    if len(basis) < len(model.variables):
        for i, value in enumerate(_rational_candidate(result.x)):
            add({i: F(1)}, value)
            if len(basis) == len(model.variables):
                break
    if len(basis) != len(model.variables):
        return None
    vector = [F(0)] * len(model.variables)
    for pivot in sorted(basis, reverse=True):
        row, rhs = basis[pivot]
        vector[pivot] = rhs - sum((coefficient * vector[i] for i, coefficient in row.items() if i != pivot), F(0))
    return vector


def _recover_large_network_vertex(model, result):
    vector = _propose_large_network_vertex(model, result)
    return vector if vector is not None and model.audit_vector(vector)['exact_feasible'] else None


def solve_context_interval(model, document, events, *, time_limit_seconds=60, stage1d_empty_target_recovery=False,
                           stage1d_coordinate_recovery=False):
    import numpy as np
    from scipy.optimize import linprog
    from scipy.sparse import coo_matrix
    from time import perf_counter
    if not isinstance(stage1d_coordinate_recovery, bool):
        raise ValueError('stage1d_coordinate_recovery must be boolean')
    if not events or len(events) != len(set(events)):
        raise ValueError("Distinct, nonempty objective ports required")
    indices = [model.event_variables[e] for e in events]
    scale = model.variables[indices[0]].scale
    objective_asset = model.variables[indices[0]].asset
    if any(model.variables[i].asset != objective_asset for i in indices):
        raise ValueError("Mixed asset source objectives are forbidden")
    if stage1d_empty_target_recovery:
        if document.get("objective_groups") or document.get("all_service_entries"):
            raise ValueError("Stage1D certificate recovery requires an empty fixed target union")
        if any(model.variables[i].lower != model.variables[i].upper for i in indices):
            raise ValueError("Stage1D certificate recovery requires an exactly fixed feasibility objective")
        from time import perf_counter
    if model._solver_cache is None:
        rows, cols, values = [], [], []
        for number, row in enumerate(model.eq):
            for i, coefficient in row.items():
                rows.append(number); cols.append(i); values.append(float(coefficient))
        matrix = coo_matrix((values, (rows, cols)), shape=(len(model.eq), len(model.variables))).tocsr()
        bounds = [(float(v.lower), float(v.upper)) for v in model.variables]
        model._solver_cache = matrix, bounds, values
    matrix, bounds, values = model._solver_cache
    endpoints = {}
    for label, sign in (("lower", 1), ("upper", -1)):
        cost = [F(0)] * len(model.variables)
        for i in indices:
            cost[i] = F(sign)
        endpoint_started = perf_counter()
        recovery_evidence = None
        result = linprog(np.array([float(v) for v in cost]), A_eq=matrix,
                         b_eq=np.array([float(v) for v in model.rhs]), bounds=bounds,
                         method="highs", options={"time_limit": time_limit_seconds,
                         "primal_feasibility_tolerance": 1e-9, "dual_feasibility_tolerance": 1e-9})
        if result.status != 0:
            endpoints[label] = {"status": "INFEASIBLE_REPORTED_BY_SOLVER" if result.status == 2 else "SOLVER_UNRESOLVED",
                                "raw": None, "solver_status": int(result.status), "message": result.message}
            continue
        certificate, vector, value = _certify(model, result, cost)
        if not certificate["certified"] and certificate["exact_dual_feasible"]:
            recovered = _recover_large_network_vertex(model, result)
            if recovered is not None:
                primal = sum((c * x for c, x in zip(cost, recovered)), F(0))
                if fmt(primal) == certificate["exact_dual_objective"]:
                    vector, value = recovered, primal
                    certificate.update({"certified": True, "exact_primal_feasible": True,
                        "exact_primal_objective": fmt(primal), "failure_detail": None,
                        "primal_recovery": {"method": "R3_SPARSE_EXACT_ACTIVE_BOUND_SOLVE", "status": "EXACT_CANDIDATE_RECOVERED", "original_model_changed": False}})
        if stage1d_empty_target_recovery and not certificate["certified"]:
            # Same LP and exact audits. Only this explicitly enabled empty-target
            # feasibility check gets one bounded numerical certificate retry.
            recovery_evidence = {
                "policy": "STAGE1D_EMPTY_FIXED_TARGET_CERTIFICATE_RETRY_V1",
                "original_model_changed": False, "original_objective_changed": False,
                "original_tolerances_changed": False, "max_additional_attempts": 1,
                "time_budget_rule": "ORIGINAL_ENDPOINT_BUDGET_SHARED_WITH_RETRY",
                "configured_total_time_limit_seconds": time_limit_seconds,
                "attempts": [{"attempt": 1, "method": "highs", "presolve": True,
                    "solver_status": int(result.status), "message": result.message,
                    "certificate": copy.deepcopy(certificate)}]}
            remaining = time_limit_seconds - (perf_counter() - endpoint_started)
            if remaining > 0:
                attempt = {"attempt": 2, "method": "highs-ds", "presolve": False,
                    "primal_feasibility_tolerance": "1e-9", "dual_feasibility_tolerance": "1e-9"}
                try:
                    retried = linprog(np.array([float(v) for v in cost]), A_eq=matrix,
                        b_eq=np.array([float(v) for v in model.rhs]), bounds=bounds,
                        method="highs-ds", options={"time_limit": remaining, "presolve": False,
                        "primal_feasibility_tolerance": 1e-9, "dual_feasibility_tolerance": 1e-9})
                    attempt.update(solver_status=int(retried.status), message=retried.message)
                except Exception as exc:
                    retried = None
                    attempt.update(solver_status=None, error=type(exc).__name__ + ": " + str(exc))
                if retried is not None and retried.status == 0:
                    retry_certificate, retry_vector, retry_value = _certify(model, retried, cost)
                    attempt["certificate"] = retry_certificate
                    if retry_certificate["certified"]:
                        retry_witness = {name: fmt(retry_vector[i] * model.variables[i].scale)
                            for name, i in model.event_variables.items()}
                        retry_audit = audit_context_witness(document, retry_witness,
                            remove_balance_information=model.metadata["variant"] == "MATCHED_INFORMATION_RELAXED",
                            no_protocol_continuation=model.metadata.get("no_protocol_continuation", False))
                        attempt["independent_audit"] = retry_audit
                        if retry_audit["exact_feasible"]:
                            result, certificate, vector, value = retried, retry_certificate, retry_vector, retry_value
                            recovery_evidence["selected_attempt"] = 2
                recovery_evidence["attempts"].append(attempt)
            else:
                recovery_evidence["retry_skipped_reason"] = "ORIGINAL_ENDPOINT_TIME_BUDGET_EXHAUSTED"
            recovery_evidence.setdefault("selected_attempt", None)
        if stage1d_coordinate_recovery and not stage1d_empty_target_recovery and not certificate['certified']:
            from stage1d_context_certificate_recovery import recover, POLICY
            original_certificate = certificate
            recovered, attempt = recover(model, cost, matrix,
                time_limit_seconds - (perf_counter() - endpoint_started),
                certify=_certify, recover_vertex=_recover_large_network_vertex,
                propose_vertex=_propose_large_network_vertex)
            recovery_evidence = {'policy': POLICY, 'max_additional_attempts': 1,
                'time_budget_rule': 'ORIGINAL_ENDPOINT_BUDGET_SHARED_WITH_RETRY',
                'configured_total_time_limit_seconds': time_limit_seconds,
                'original_model_changed': False, 'original_objective_changed': False,
                'attempts': [{'attempt': 1, 'method': 'highs', 'presolve': True,
                    'certificate_sha256': canonical_hash(original_certificate), 'certified': False}],
                'selected_attempt': None}
            if 'skip_reason' not in attempt:
                recovery_evidence['attempts'].append(attempt)
            else:
                recovery_evidence['retry_skipped_reason'] = attempt['skip_reason']
            if recovered is not None:
                retry_result, retry_certificate, retry_vector, retry_value = recovered
                retry_witness = {name: fmt(retry_vector[i] * model.variables[i].scale)
                    for name, i in model.event_variables.items()}
                retry_audit = audit_context_witness(document, retry_witness,
                    remove_balance_information=model.metadata['variant'] == 'MATCHED_INFORMATION_RELAXED',
                    no_protocol_continuation=model.metadata.get('no_protocol_continuation', False))
                attempt['independent_audit'] = retry_audit
                if retry_audit['exact_feasible']:
                    result, certificate, vector, value = recovered
                    recovery_evidence['selected_attempt'] = 2
        witness = {name: fmt(vector[i] * model.variables[i].scale) for name, i in model.event_variables.items()} if certificate["certified"] else None
        audit = audit_context_witness(document, witness, remove_balance_information=model.metadata["variant"] == "MATCHED_INFORMATION_RELAXED",
                            no_protocol_continuation=model.metadata.get("no_protocol_continuation", False)) if witness is not None else None
        passed = bool(certificate["certified"] and audit["exact_feasible"])
        evidence = {row["variable"]: row for row in model.metadata["constraint_provenance"]}
        supporting_bounds = []
        if passed:
            lower_dual = _rational_candidate(result.lower.marginals)
            upper_dual = _rational_candidate(result.upper.marginals)
            for i, variable in enumerate(model.variables):
                for side, bound, dual in (("lower", variable.lower, lower_dual[i]),
                                          ("upper", variable.upper, upper_dual[i])):
                    if dual:
                        fact = evidence.get(variable.name, {})
                        supporting_bounds.append({"variable": variable.name, "bound_side": side,
                            "bound_raw": fmt(bound * scale), "exact_dual_multiplier": fmt(dual),
                            "exact_objective_contribution_raw": fmt(bound * dual * scale * sign),
                            "operation_id": fact.get("operation_id"), "role": fact.get("role"),
                            "actual_capacity_applied": fact.get("actual_capacity_applied", False),
                            "evidence_ids": fact.get("evidence_ids", []),
                            "initial_anchor_id": fact.get("initial_anchor_id")})
        endpoints[label] = {"status": "OPTIMAL_EXACT_CERTIFIED" if passed else "NUMERICAL_OR_AUDIT_UNKNOWN",
            "raw": fmt(value * sign * scale) if passed else None, "certificate": certificate,
            "witness_event_source_raw": witness, "independent_audit": audit,
            "nonzero_dual_bound_provenance": supporting_bounds}
        if recovery_evidence is not None:
            endpoints[label]["certificate_recovery"] = recovery_evidence
    passed = all(e["status"] == "OPTIMAL_EXACT_CERTIFIED" for e in endpoints.values())
    return {"status": "OPTIMAL_EXACT_CERTIFIED" if passed else "UNRESOLVED", "asset": objective_asset,
            "objective_events": events, "lower_raw": endpoints["lower"]["raw"],
            "upper_raw": endpoints["upper"]["raw"], "endpoints": endpoints}


def _zero_check(model, document, candidate_events):
    if not candidate_events:
        return {"status": "NO_DOWNSTREAM_CANDIDATE_EVENTS", "feasible": True}
    solved = solve_context_interval(model, document, candidate_events)
    value = solved["lower_raw"]
    if value is None:
        return {"status": "UNRESOLVED", "feasible": None, "diagnostic": solved}
    feasible = F(value) == 0
    return {"status": "ALL_DOWNSTREAM_ZERO_FEASIBLE" if feasible else "ALL_DOWNSTREAM_ZERO_EXCLUDED",
        "feasible": feasible, "minimum_sum_of_downstream_source_raw": value,
        "definition": "All nonseed CANDIDATE source variables simultaneously zero; context/fees/source residue remain endogenous.",
        "witness": solved["endpoints"]["lower"] if feasible else None,
        "exclusion_certificate": None if feasible else solved["endpoints"]["lower"],
        "explanation": "A nonnegative sum is zero iff every downstream candidate source share is zero. Exact optimum and independent ledger audit establish this conclusion."}


def run_context_document(document):
    informed = build_context_model(document)
    relaxed = build_context_model(document, remove_balance_information=True)
    nesting = structural_nesting(informed, relaxed)
    if not nesting["nested"]:
        raise ValueError("Same-graph information relaxation changed source/equalities")
    groups = document.get("objective_groups", {})
    all_entries = sorted(set(document.get("all_service_entries", [e for names in groups.values() for e in names])))
    results, comparisons = {}, []
    candidates = [flow["event_id"] for tx in document["transactions"] for flow in tx.get("flows", []) if flow["role"] == "CANDIDATE"]
    for model in (informed, relaxed):
        cache = {}

        def solve(names):
            key = tuple(sorted(names))
            if key not in cache:
                cache[key] = solve_context_interval(model, document, list(key))
                cache[key]["context_completeness"] = target_completeness(document, list(key))
                if model.metadata["variant"] == "MATCHED_INFORMATION_RELAXED":
                    cache[key]["information_removed_intentionally"] = "ALL_PHYSICAL_BALANCE_UPPER_BOUNDS"
            return cache[key]

        entries = {eid: solve([eid]) for eid in all_entries}
        group_results = {group: solve(names) for group, names in groups.items()}
        union = solve(all_entries) if all_entries else {"status": "NO_OBSERVED_TARGET", "lower_raw": None, "upper_raw": None}
        results[model.metadata["variant"]] = {"model": model.statistics(), "entry_intervals": entries,
            "address_asset_intervals": group_results, "all_service_joint": union,
            "all_downstream_zero": _zero_check(model, document, candidates)}
    for category in ("entry_intervals", "address_asset_intervals"):
        for name, full in results["BEST_AVAILABLE_CONTEXT"][category].items():
            wide = results["MATCHED_INFORMATION_RELAXED"][category][name]
            certified = full["status"] == wide["status"] == "OPTIMAL_EXACT_CERTIFIED"
            passed = certified and F(wide["lower_raw"]) <= F(full["lower_raw"]) <= F(full["upper_raw"]) <= F(wide["upper_raw"])
            comparisons.append({"category": category, "objective": name, "passed": passed,
                                "certified": certified})
    if all_entries:
        full = results["BEST_AVAILABLE_CONTEXT"]["all_service_joint"]
        wide = results["MATCHED_INFORMATION_RELAXED"]["all_service_joint"]
        certified = full["status"] == wide["status"] == "OPTIMAL_EXACT_CERTIFIED"
        comparisons.append({"category": "all_service_joint", "objective": "ALL_FIRST_SERVICE_ENTRIES|ETH",
            "passed": certified and F(wide["lower_raw"]) <= F(full["lower_raw"]) <= F(full["upper_raw"]) <= F(wide["upper_raw"]), "certified": certified})
    return {"schema_version": "stage1b-r3-context-amount-results-v1", "input_sha256": canonical_hash(document),
        "query_id": document.get("query_id"), "external_acceptance": "PENDING_REVIEW",
        "structural_nesting": nesting, "same_graph_comparisons": comparisons,
        "all_same_graph_comparisons_passed": all(c["passed"] for c in comparisons),
        "variants": results, "r2_comparison_rule": "R2_BASELINE is a different graph; no endpoint monotonicity is asserted against it."}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    document = json.loads(args.input.read_text(encoding="utf-8"))
    args.output.mkdir(parents=True, exist_ok=True)
    result = run_context_document(document)
    path = args.output / "CONTEXT_AMOUNT_RESULTS.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for variant, data in result["variants"].items():
        (args.output / (variant + "_CONSTRAINT_PROVENANCE.json")).write_text(
            json.dumps(data["model"]["constraint_provenance"], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(path), "comparisons_passed": result["all_same_graph_comparisons_passed"],
                      "variants": {k: {"joint_lower_raw": v["all_service_joint"]["lower_raw"],
                                         "joint_upper_raw": v["all_service_joint"]["upper_raw"],
                                         "all_downstream_zero_feasible": v["all_downstream_zero"]["feasible"]}
                                   for k, v in result["variants"].items()}}, indent=2))
    return 0 if result["all_same_graph_comparisons_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
