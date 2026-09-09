"""Stage1C-R1 acceptance of already generated typed method results.

Expected keys come from observations, never returned dictionaries, hidden data,
Oracle answers or optimal endpoints. Primary constraint code is used only after
generation to audit supplied allocations/witnesses and structural relaxations;
this module never calls a solver or repairs an answer.
"""
from __future__ import annotations

from fractions import Fraction as F
import hashlib
import json
from pathlib import Path

from stage1c_baselines import _compile, _operation_ports, _boundary_completion, OUTSIDE
from stage1c_zero_derivation import RULE as ZERO_DERIVATION_RULE, validate_derived_zero

CONTRACT_VERSION = "stage1c-r1-output-acceptance-v1"
INTERVAL_METHODS = ("FULL_INTERVAL", "NO_CROSS_TARGET_COUPLING", "NO_PROTOCOL_CONTINUATION", "BALANCE_INFORMATION_REMOVED")
METHODS = ("FULL_INTERVAL", "BOUNDED_REACHABILITY", "POISON", "HAIRCUT",
           "NO_CROSS_TARGET_COUPLING", "NO_PROTOCOL_CONTINUATION", "BALANCE_INFORMATION_REMOVED")
KINDS = {**{m: "feasible_interval" for m in INTERVAL_METHODS},
         "NO_CROSS_TARGET_COUPLING": "product_of_target_specific_copies",
         "BOUNDED_REACHABILITY": "ADDRESS_EVENT_SET_NO_AMOUNT", "POISON": "POISON_NOMINAL_RAW",
         "HAIRCUT": "PROPORTIONAL_FEASIBLE_POINT"}


def _hash(value):
    try:
        data = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=True).encode("utf-8")
        return hashlib.sha256(data).hexdigest()
    except (TypeError, ValueError, RecursionError):
        return None


def exact(value):
    """Accept exact integer/rational/decimal strings; reject float/bool/null."""
    if isinstance(value, bool) or not isinstance(value, (int, str, F)):
        raise ValueError("Expected a finite exact integer or rational representation")
    return F(value)


def _input_analysis(doc):
    initial, operations, compiled_targets, context = _compile(doc)
    ports = {}
    for ordinal, op in enumerate(operations):
        for eid, asset, amount, receiver, role in _operation_ports(op):
            ports[eid] = {"asset": asset, "amount_raw": str(amount), "recipient": receiver,
                          "sender": op.get("sender"), "role": role, "operation_ordinal": ordinal}
    declared = doc.get("objective_groups")
    if not isinstance(declared, dict):
        raise ValueError("Frozen objective_groups dictionary is required")
    independent = {}
    if context:
        flows = {f["event_id"]: f for tx in doc["transactions"] for f in tx.get("flows", [])}
        for group, entries in declared.items():
            if not isinstance(group, str) or not isinstance(entries, list) or not entries:
                raise ValueError("Declared context target group is malformed")
            for eid in entries:
                f = flows[eid]
                if f["role"] != "CANDIDATE" or f.get("to_account") != group:
                    raise ValueError("Context service entry must be a candidate to its declared terminal")
            independent[group] = list(entries)
        if set(doc.get("all_service_entries", [e for es in independent.values() for e in es])) != {e for es in independent.values() for e in es}:
            raise ValueError("Declared all-service union differs from candidate target groups")
    else:
        targets = doc.get("target_accounts", [])
        if not isinstance(targets, list) or len(targets) != len(set(targets)):
            raise ValueError("Target account identities must be a distinct list")
        for eid, port in ports.items():
            receiver = port["recipient"]
            if receiver is not None and receiver.rsplit("|", 1)[0] in targets:
                independent.setdefault(receiver, []).append(eid)
    if declared != independent or declared != compiled_targets:
        raise ValueError("Frozen declared target domain differs from independently parsed service ports")
    flat = [e for es in independent.values() for e in es]
    if len(flat) != len(set(flat)) or any(not isinstance(g, str) or "|" not in g for g in independent):
        raise ValueError("Each physical service entry must belong to one address-asset group")
    assets = sorted({g.rsplit("|", 1)[1] for g in independent})
    potential_assets = sorted({op["asset"] for op in operations if op["kind"] == "flow" and op["role"] == "SEED"}
                              | {op["output_asset"] for op in operations if op["kind"] == "conversion"})
    boundary, bmin = _boundary_completion(operations, context)
    return initial, operations, context, boundary, bmin, {
        "addresses": dict(sorted(independent.items())), "interval_events": sorted(flat),
        "baseline_events": sorted(ports), "allocation_ports": sorted(ports), "port_facts": ports,
        "interval_joint_assets": assets if independent else potential_assets,
        "baseline_joint_assets": assets, "source_potential_assets": potential_assets,
        "service_entries_by_asset": {a: sorted(e for g, es in independent.items() if g.rsplit("|", 1)[1] == a for e in es) for a in (assets or potential_assets)},
        "context_format": context, "connected_protocol_ids": [op["id"] for op in operations if op["kind"] == "conversion"],
        "empty_target_domain": not independent,
    }


def _applicability_and_marks(initial, operations, context, bmin):
    """Input-only sign/actual-state guard, not an amount allocation generator.

    A known full debit clears proportional source presence. The first positive
    withdrawal from an unknown actual balance is exactly the frozen Haircut
    unsupported condition. Persistent reachability separately ignores clears.
    """
    actual, related, reached, marks = dict(initial), set(), set(), {}
    if context:
        actual.update(bmin if isinstance(bmin, dict) else {OUTSIDE: bmin})
    obstruction = None

    def credit(key, amount, positive):
        if key is not None:
            actual[key] = None if actual.get(key) is None else actual[key] + amount
            if positive:
                related.add(key)

    def debit(key, amount, eid):
        nonlocal obstruction
        if key is None or amount == 0:
            return False
        value = actual.get(key)
        positive = key in related
        if value is None:
            if positive and obstruction is None:
                obstruction = {"code": "UNKNOWN_MODELED_ACTUAL_BALANCE", "account_asset": key, "event_id": eid}
        else:
            if value < amount:
                raise ValueError("Observed physical ledger overdraft before " + eid)
            actual[key] = value - amount
            if amount == value:
                related.discard(key)
        return positive

    for op in operations:
        kind = op["kind"]
        if kind == "anchor":
            for key, observed in op["balances"].items():
                if actual.get(key) is not None and actual[key] != observed:
                    raise ValueError("Observed aligned anchor conflicts with replay")
                actual[key] = observed
            continue
        if kind == "flow":
            mark = op["amount"] > 0 and (op["role"] == "SEED" or (op["fixed"] is None and op["sender"] in reached))
            marks[op["id"]] = mark
            if mark and op["receiver"] is not None:
                reached.add(op["receiver"])
            positive = op["fixed"] > 0 if op["fixed"] is not None else debit(op["sender"], op["amount"], op["id"])
            credit(op["receiver"], op["amount"], positive)
        else:
            mark = op["gross"] > 0 and op["sender"] in reached
            for eid, _, amount, receiver, _ in _operation_ports(op):
                marks[eid] = bool(mark and amount > 0)
                if marks[eid] and receiver is not None:
                    reached.add(receiver)
            positive = debit(op["sender"], op["gross"], op["id"] + ":input")
            if kind == "conversion":
                credit(op["refund_to"], op["refund"], positive and op["refund"] > 0)
                credit(op["receiver"], op["output"], positive and op["output"] > 0)
            else:
                for output in op["outputs"]:
                    credit(output["receiver"], output["amount"], positive and output["amount"] > 0)
    return obstruction, marks


def expected_domains(doc):
    try:
        initial, operations, context, boundary, bmin, domains = _input_analysis(doc)
        obstruction, marks = _applicability_and_marks(initial, operations, context, bmin)
        return {"passed": True, "errors": [], **domains, "haircut_not_applicable_basis": obstruction,
                "boundary_completion_expected": boundary, "temporal_marking_expected": marks,
                "derivation": "Frozen observations and unchanged method-specific physical port semantics only"}
    except Exception as exc:
        return {"passed": False, "errors": [{"code": "INPUT_DOMAIN_INVALID", "method": None,
                 "path": "$input", "detail": type(exc).__name__ + ": " + str(exc)}]}


def accept_method_results(doc, results, *, expected_identity=None):
    """Return a fail-closed receipt without discarding malformed-return detail."""
    errors, method_checks, hard = [], {}, []

    def fail(code, method, path, detail):
        errors.append({"code": code, "method": method, "path": path, "detail": str(detail)})

    def check(condition, code, method, path, detail):
        if not condition:
            fail(code, method, path, detail)
        return bool(condition)

    def mapping(value, keys, method, path):
        if not check(isinstance(value, dict), "FIELD_TYPE", method, path, "Expected dictionary"):
            return {}
        check(set(value) == set(keys), "OUTPUT_DOMAIN", method, path,
              "missing=" + repr(sorted(set(keys) - set(value), key=str)) + "; extra=" + repr(sorted(set(value) - set(keys), key=str)))
        return value

    def members(value, expected, method, path):
        if not check(isinstance(value, list) and all(isinstance(v, str) for v in value), "FIELD_TYPE", method, path, "Expected list of string identities"):
            return False
        return check(len(value) == len(set(value)) and set(value) == set(expected), "OUTPUT_MEMBERS", method, path, "Expected distinct members " + repr(sorted(expected)))

    def number(value, method, path, *, nonnegative=True):
        try:
            parsed = exact(value)
            if nonnegative and parsed < 0:
                raise ValueError("Negative source amount")
            return parsed
        except Exception as exc:
            fail("EXACT_NUMBER", method, path, type(exc).__name__ + ": " + str(exc))
            return None

    def no_amount(row, fields, method, path):
        for field in fields:
            check(row.get(field) is None, "AMOUNT_KIND", method, path + "." + field, "This method does not supply this amount kind")

    domains = expected_domains(doc)
    errors.extend(domains["errors"])
    identity = expected_identity if isinstance(expected_identity, dict) else {}
    for key in ("sample_id", "query_id", "input_fact_hash", "scope_hash", "label_version"):
        check(isinstance(identity.get(key), str) and bool(identity[key]), "EXPECTED_IDENTITY_MISSING", None, "$identity." + key, "Trusted manifest identity is required")
    if isinstance(doc, dict):
        expected_query = doc.get("query_id", doc.get("scenario_id"))
        if expected_query is not None:
            check(identity.get("query_id") == expected_query, "EXPECTED_IDENTITY_INPUT_MISMATCH", None, "$identity.query_id", "Trusted identity must match observed query")
        expected_sample = doc.get("scenario_id", doc.get("name", doc.get("sample_id")))
        if expected_sample is not None:
            check(identity.get("sample_id") == expected_sample, "EXPECTED_IDENTITY_INPUT_MISMATCH", None, "$identity.sample_id", "Trusted identity must match observed sample")
    mapping(results, METHODS, None, "$results")
    supplied = results if isinstance(results, dict) else {}
    models, audit_cache, parsed_rows = {}, {}, {}
    zero_derivation_cache = {}

    def model_for(method):
        key = "FULL_INTERVAL" if method in ("HAIRCUT", "NO_CROSS_TARGET_COUPLING") else method
        if key not in models:
            from stage1c_intervals import build_variant
            models[key] = build_variant(doc, key)
        return models[key]

    def audit(values, method, path):
        try:
            model, _ = model_for(method)
            signature = ("FULL_INTERVAL" if method in ("HAIRCUT", "NO_CROSS_TARGET_COUPLING") else method, tuple(sorted((k, str(exact(v))) for k, v in values.items())))
            if signature not in audit_cache:
                audit_cache[signature] = model.audit_vector(model.lift_hidden_allocation_for_audit(values))
            receipt = audit_cache[signature]
            check(receipt.get("exact_feasible") is True, "ALLOCATION_INFEASIBLE", method, path, receipt)
            return receipt
        except Exception as exc:
            fail("ALLOCATION_AUDIT_ERROR", method, path, type(exc).__name__ + ": " + str(exc))
            return {"exact_feasible": False}

    def interval_row(row, method, path, asset, entries, *, product_joint=False):
        if not check(isinstance(row, dict), "FIELD_TYPE", method, path, "Expected interval object"):
            return None
        check(row.get("status") == "OPTIMAL_EXACT_CERTIFIED", "INTERVAL_STATUS", method, path + ".status", "Successful interval must be exactly certified")
        check(row.get("asset") == asset, "ASSET_IDENTITY", method, path + ".asset", "Expected " + asset)
        lower = number(row.get("lower_raw"), method, path + ".lower_raw")
        upper = number(row.get("upper_raw"), method, path + ".upper_raw")
        if lower is not None and upper is not None:
            check(lower <= upper, "INTERVAL_ORDER", method, path, "Require lower <= upper")
            capacity = sum((F(domains["port_facts"][e]["amount_raw"]) for e in entries), F(0))
            check(upper <= capacity, "PHYSICAL_CAPACITY", method, path, "Interval exceeds frozen objective physical capacity")
            if "positive_support" in row:
                check(row["positive_support"] == ("CERTIFIED_POSITIVE" if upper > 0 else "CERTIFIED_ZERO"),
                      "OUTPUT_SUPPORT", method, path + ".positive_support", "Support must agree with its certified upper endpoint")
        no_amount(row, ("point_raw", "nominal_raw"), method, path)
        endpoint_rows = row.get('endpoints')
        derived = ('zero_derivation' in row or row.get('solution_origin') == ZERO_DERIVATION_RULE
            or (isinstance(endpoint_rows, dict) and any(isinstance(e, dict)
                and e.get('status') == ZERO_DERIVATION_RULE for e in endpoint_rows.values())))
        if derived and (product_joint or not entries):
            fail('ZERO_DERIVATION', method, path,
                 'Product unions and empty targets require their own existing proofs, not a subset shortcut')
        if product_joint and entries:
            check(isinstance(row.get("copy_count"), int) and not isinstance(row.get("copy_count"), bool)
                  and row["copy_count"] == sum(g.rsplit("|", 1)[1] == asset for g in domains["addresses"]),
                  "COPY_COUNT", method, path + ".copy_count", "Address-asset copy count mismatch")
        else:
            members(row.get("objective_events"), entries, method, path + ".objective_events")
            if not entries:
                check(lower == 0 and upper == 0 and row.get("proof") == "EMPTY_FIXED_TARGET_UNION_AFTER_FEASIBILITY_CHECK",
                      "EMPTY_TARGET_INTERVAL", method, path, "Legal empty union requires explicit feasible [0,0]")
            else:
                if derived:
                    try:
                        model, _ = model_for(method)
                        validate_derived_zero(model, doc, method, row, supplied[method],
                            domains['addresses'], entries, cache=zero_derivation_cache)
                    except Exception as exc:
                        fail('ZERO_DERIVATION', method, path, type(exc).__name__ + ': ' + str(exc))
                    return (lower, upper) if lower is not None and upper is not None else None
                endpoints = mapping(row.get("endpoints"), ("lower", "upper"), method, path + ".endpoints")
                for label, value in (("lower", lower), ("upper", upper)):
                    endpoint = endpoints.get(label)
                    if not check(isinstance(endpoint, dict), "FIELD_TYPE", method, path + ".endpoints." + label, "Expected endpoint object"):
                        continue
                    ep_path = path + ".endpoints." + label
                    check(endpoint.get("status") == "OPTIMAL_EXACT_CERTIFIED", "INTERVAL_STATUS", method, ep_path, "Uncertified endpoint")
                    parsed = number(endpoint.get("raw"), method, ep_path + ".raw")
                    check(value is not None and parsed == value, "ENDPOINT_VALUE_MISMATCH", method, ep_path, "Endpoint must equal reported interval")
                    certificate = endpoint.get("certificate")
                    check(isinstance(certificate, dict) and all(certificate.get(k) is True for k in ("certified", "exact_primal_feasible", "exact_dual_feasible")),
                          "ENDPOINT_CERTIFICATE", method, ep_path + ".certificate", "Exact certification flags required")
                    values = mapping(endpoint.get("witness_event_source_raw"), domains["allocation_ports"], method, ep_path + ".witness_event_source_raw")
                    vv = {e: number(v, method, ep_path + ".witness_event_source_raw." + str(e)) for e, v in values.items()}
                    if set(vv) == set(domains["allocation_ports"]) and all(v is not None for v in vv.values()):
                        check(sum((vv[e] for e in entries), F(0)) == value, "WITNESS_OBJECTIVE_MISMATCH", method, ep_path, "Witness objective differs from endpoint")
                        audit(values, method, ep_path)
        return (lower, upper) if lower is not None and upper is not None else None

    if domains.get("passed"):
        for method in METHODS:
            beginning = len(errors)
            result = supplied.get(method)
            record = {"passed": False, "business_status": result.get("status") if isinstance(result, dict) else None}
            method_checks[method] = record
            try:
                if not check(isinstance(result, dict), "METHOD_MISSING_OR_TYPE", method, "$results." + method, "Required method result must be a dictionary"):
                    continue
                for key in ("sample_id", "query_id", "input_fact_hash", "scope_hash", "label_version"):
                    check(key in result and result[key] == identity.get(key), "RESULT_IDENTITY", method, key, "Expected trusted " + key)
                check(result.get("method_id") == method, "METHOD_IDENTITY", method, "method_id", "Dictionary method key and method_id disagree")
                check(isinstance(result.get("method_version"), str) and bool(result["method_version"]), "METHOD_VERSION", method, "method_version", "Version must be explicit")
                if isinstance(identity.get("method_versions"), dict):
                    check(result.get("method_version") == identity["method_versions"].get(method), "METHOD_VERSION", method, "method_version", "Frozen method version mismatch")
                if "query_or_sample_id" in result:
                    check(result["query_or_sample_id"] == identity.get("query_id"), "RESULT_IDENTITY", method, "query_or_sample_id", "Native query identity mismatch")
                check(result.get("output_kind") == KINDS[method], "OUTPUT_KIND", method, "output_kind", "Expected " + KINDS[method])
                status = result.get("status")
                if status == "NOT_APPLICABLE":
                    basis = domains["haircut_not_applicable_basis"]
                    valid = method == "HAIRCUT" and basis is not None
                    check(valid, "UNJUSTIFIED_NOT_APPLICABLE", method, "status", "Only the input-proven unknown actual Haircut withdrawal is supported as NA")
                    check(result.get("applicability") == "NOT_APPLICABLE", "APPLICABILITY", method, "applicability", "NA applicability must be explicit")
                    reason = result.get("failure_reason")
                    check(isinstance(reason, str) and bool(reason) and (not valid or all(str(basis[k]) in reason for k in ("code", "account_asset", "event_id"))),
                          "NOT_APPLICABLE_REASON", method, "failure_reason", "NA reason must identify the actual input obstruction")
                    for key in ("addresses", "events", "joint_by_asset", "allocation_raw", "positive_addresses", "output_address_ids", "output_event_ids"):
                        check(key in result and result[key] is None, "NOT_APPLICABLE_NULL", method, key, "NA is null, never an empty/zero successful result")
                    record["not_applicable_basis"] = basis
                    continue
                if not check(status == "COMPLETED", "METHOD_STATUS", method, "status", "A failed, unresolved or unrun method is not accepted"):
                    continue
                expected_app = "SUPPORTED" if method in INTERVAL_METHODS else "APPLICABLE_WITH_EXPLICIT_BOUNDARY_ASSUMPTION" if method == "HAIRCUT" and domains["boundary_completion_expected"]["adopted"] else "APPLICABLE"
                check(result.get("applicability") == expected_app, "APPLICABILITY", method, "applicability", "Expected " + expected_app)
                if method == "HAIRCUT":
                    check(domains["haircut_not_applicable_basis"] is None, "UNSUPPORTED_SUCCESS", method, "status", "Cannot produce a supported proportional point at unknown required actual balance")
                addr = mapping(result.get("addresses"), domains["addresses"], method, "addresses")
                event_keys = domains["interval_events"] if method in INTERVAL_METHODS else domains["baseline_events"]
                events = mapping(result.get("events"), event_keys, method, "events")
                assets = domains["interval_joint_assets"] if method in INTERVAL_METHODS else domains["baseline_joint_assets"]
                joint = mapping(result.get("joint_by_asset"), assets, method, "joint_by_asset")
                parsed_rows[method] = {"addresses": {}, "events": {}, "joint_by_asset": {}}
                if method in INTERVAL_METHODS:
                    for category, rows, expected_keys in (("addresses", addr, domains["addresses"]), ("events", events, event_keys), ("joint_by_asset", joint, assets)):
                        for key in expected_keys:
                            if key not in rows:
                                continue
                            entries = domains["addresses"][key] if category == "addresses" else [key] if category == "events" else domains["service_entries_by_asset"][key]
                            asset = key.rsplit("|", 1)[1] if category == "addresses" else domains["port_facts"][key]["asset"] if category == "events" else key
                            parsed_rows[method][category][key] = interval_row(rows[key], method, category + "." + key, asset, entries,
                                                                             product_joint=method == "NO_CROSS_TARGET_COUPLING" and category == "joint_by_asset")
                    positives = [g for g, bounds in parsed_rows[method]["addresses"].items() if bounds is not None and bounds[1] > 0]
                    members(result.get("positive_addresses"), positives, method, "positive_addresses")
                    for alias in ("output_address_ids",):
                        if alias in result:
                            members(result[alias], positives, method, alias)
                    if "output_event_ids" in result:
                        members(result["output_event_ids"], [e for e, b in parsed_rows[method]["events"].items() if b is not None and b[1] > 0], method, "output_event_ids")
                    if domains["empty_target_domain"]:
                        proof = result.get("modifications", {}).get("empty_target_feasibility")
                        seed = next(e for e, f in domains["port_facts"].items() if f["role"] == "SEED")
                        interval_row(proof, method, "modifications.empty_target_feasibility", domains["port_facts"][seed]["asset"], [seed])
                else:
                    allocations = {}
                    if method == "HAIRCUT":
                        raw_alloc = mapping(result.get("allocation_raw"), domains["allocation_ports"], method, "allocation_raw")
                        allocations = {e: number(v, method, "allocation_raw." + str(e)) for e, v in raw_alloc.items()}
                        if set(allocations) == set(domains["allocation_ports"]) and all(v is not None for v in allocations.values()):
                            record["full_allocation_audit"] = audit(raw_alloc, method, "allocation_raw")
                        expected_boundary = domains["boundary_completion_expected"]
                        boundary = result.get("boundary_completion")
                        if check(isinstance(boundary, dict), "BOUNDARY_COMPLETION", method, "boundary_completion", "Explicit input-derived B_min convention required"):
                            if 'by_asset' in expected_boundary:
                                check(boundary.get('by_asset') == expected_boundary['by_asset']
                                      and boundary.get('amount_scope') == expected_boundary['amount_scope'],
                                      'BOUNDARY_COMPLETION', method, 'boundary_completion.by_asset',
                                      'All independent asset pools must match the observed prefix ledger; no cross-asset sum')
                            for field in ("convention", "adopted", "is_observed_fact", "asset"):
                                check(type(boundary.get(field)) is type(expected_boundary[field]) and boundary.get(field) == expected_boundary[field],
                                      "BOUNDARY_COMPLETION", method, "boundary_completion." + field, "Boundary identity/assumption mismatch")
                            for field in ("B0_out_raw", "S0_out_raw"):
                                check(number(boundary.get(field), method, "boundary_completion." + field) == F(expected_boundary[field]),
                                      "BOUNDARY_COMPLETION", method, "boundary_completion." + field, "Exact boundary completion amount mismatch")
                            sequence = boundary.get("sequence")
                            if check(isinstance(sequence, list) and len(sequence) == len(expected_boundary["sequence"]), "BOUNDARY_COMPLETION", method, "boundary_completion.sequence", "Complete observed boundary sequence required"):
                                for index, (given, expected) in enumerate(zip(sequence, expected_boundary["sequence"])):
                                    path = "boundary_completion.sequence." + str(index)
                                    if not check(isinstance(given, dict), "BOUNDARY_COMPLETION", method, path, "Expected sequence item"):
                                        continue
                                    for field in ("event_id", "direction"):
                                        check(given.get(field) == expected[field], "BOUNDARY_COMPLETION", method, path + "." + field, "Boundary event order/identity mismatch")
                                    for field in ("actual_raw", "cumulative_incoming_raw", "cumulative_outgoing_raw", "prefix_deficit_raw"):
                                        check(number(given.get(field), method, path + "." + field, nonnegative=field != "prefix_deficit_raw") == F(expected[field]),
                                              "BOUNDARY_COMPLETION", method, path + "." + field, "Exact observed boundary sequence mismatch")
                    else:
                        check(result.get("allocation_raw") is None, "AMOUNT_KIND", method, "allocation_raw", "Set/nominal baselines do not claim a feasible allocation")
                    event_values, event_support = {}, {}
                    for eid in event_keys:
                        row = events.get(eid); path = "events." + eid; fact = domains["port_facts"][eid]
                        if not check(isinstance(row, dict), "FIELD_TYPE", method, path, "Expected visited port object"):
                            continue
                        check(row.get("asset") == fact["asset"] and row.get("recipient") == fact["recipient"] and row.get("role") == fact["role"], "PORT_IDENTITY", method, path, "Port asset/recipient/role differs from observations")
                        check(number(row.get("actual_raw"), method, path + ".actual_raw") == F(fact["amount_raw"]), "PHYSICAL_CAPACITY", method, path + ".actual_raw", "Observed port capacity mismatch")
                        check(isinstance(row.get("supported"), bool), "FIELD_TYPE", method, path + ".supported", "Support flag must be boolean")
                        if method == "HAIRCUT":
                            value = number(row.get("point_raw"), method, path + ".point_raw")
                            check(eid in allocations and allocations.get(eid) is not None and value == allocations[eid], "POINT_ALLOCATION_MISMATCH", method, path, "Event point differs from complete allocation")
                            support = value is not None and value > 0
                            no_amount(row, ("nominal_raw", "lower_raw", "upper_raw"), method, path)
                        else:
                            support = domains["temporal_marking_expected"][eid]
                            value = F(fact["amount_raw"]) if support else F(0)
                            no_amount(row, ("point_raw", "lower_raw", "upper_raw", "source_amount_raw"), method, path)
                            if method == "POISON":
                                check(number(row.get("nominal_raw"), method, path + ".nominal_raw") == value, "NOMINAL_CAPACITY_MISMATCH", method, path, "Poison nominal is the marked physical capacity")
                            else:
                                no_amount(row, ("nominal_raw",), method, path)
                            witness = row.get("witness")
                            valid_witness = isinstance(witness, list) and bool(witness) and all(isinstance(e, str) and e in domains["port_facts"] for e in witness) and witness[-1] == eid
                            if valid_witness:
                                facts = [domains["port_facts"][e] for e in witness]
                                valid_witness = facts[0]["role"] == "SEED" and all(a["recipient"] == b["sender"] and a["operation_ordinal"] < b["operation_ordinal"] for a, b in zip(facts, facts[1:]))
                            check(valid_witness if support else witness is None,
                                  "MARKING_WITNESS", method, path + ".witness", "Marked event needs a physical witness ending at this port; unmarked is null")
                        if method == "HAIRCUT" and "source_amount_raw" in row:
                            check(number(row["source_amount_raw"], method, path + ".source_amount_raw") == value, "POINT_ALIAS_MISMATCH", method, path, "Point alias mismatch")
                        check(row.get("supported") is support, "OUTPUT_SUPPORT", method, path + ".supported", "Flag disagrees with this method's own verified output")
                        event_values[eid], event_support[eid] = value, support
                    address_support = {}
                    for category, rows, keys in (("addresses", addr, domains["addresses"]), ("joint_by_asset", joint, assets)):
                        for key in keys:
                            row = rows.get(key); path = category + "." + key
                            if not check(isinstance(row, dict), "FIELD_TYPE", method, path, "Expected aggregate object"):
                                continue
                            entries = domains["addresses"][key] if category == "addresses" else domains["service_entries_by_asset"][key]
                            asset = key.rsplit("|", 1)[1] if category == "addresses" else key
                            members(row.get("events"), entries, method, path + ".events")
                            if category == "addresses":
                                check(row.get("asset") == asset, "ASSET_IDENTITY", method, path + ".asset", "Wrong address asset")
                            else:
                                members(row.get("addresses"), [g for g in domains["addresses"] if g.rsplit("|", 1)[1] == asset], method, path + ".addresses")
                                if "asset" in row:
                                    check(row["asset"] == asset, "ASSET_IDENTITY", method, path + ".asset", "Wrong joint asset")
                            valid = all(e in event_values and event_values[e] is not None for e in entries)
                            total = sum((event_values[e] for e in entries), F(0)) if valid else None
                            if method == "HAIRCUT":
                                check(number(row.get("point_raw"), method, path + ".point_raw") == total and total is not None, "POINT_AGGREGATE_MISMATCH", method, path, "Aggregate differs from frozen service-entry allocation sum")
                                if "source_amount_raw" in row:
                                    check(number(row["source_amount_raw"], method, path + ".source_amount_raw") == total and total is not None, "POINT_ALIAS_MISMATCH", method, path, "Point alias mismatch")
                                no_amount(row, ("nominal_raw", "lower_raw", "upper_raw"), method, path)
                            elif method == "POISON":
                                check(number(row.get("nominal_raw"), method, path + ".nominal_raw") == total and total is not None, "NOMINAL_AGGREGATE_MISMATCH", method, path, "Nominal aggregate differs from its marked service entries")
                                no_amount(row, ("point_raw", "source_amount_raw", "lower_raw", "upper_raw"), method, path)
                            else:
                                no_amount(row, ("point_raw", "nominal_raw", "source_amount_raw", "lower_raw", "upper_raw"), method, path)
                            if category == "addresses":
                                support = any(event_support.get(e) is True for e in entries)
                                check(row.get("supported") is support, "OUTPUT_SUPPORT", method, path + ".supported", "Aggregate support mismatch")
                                address_support[key] = support
                    positive = [g for g, supported in address_support.items() if supported]
                    members(result.get("positive_addresses"), positive, method, "positive_addresses")
                    members(result.get("output_address_ids"), positive, method, "output_address_ids")
                    members(result.get("output_event_ids"), [e for e in domains["interval_events"] if event_support.get(e)], method, "output_event_ids")
            except Exception as exc:
                fail("METHOD_CONTRACT_EXCEPTION", method, "$results." + method, type(exc).__name__ + ": " + str(exc))
            finally:
                record["passed"] = len(errors) == beginning
                record["errors"] = errors[beginning:]

        def invariant(code, method, path, condition, detail):
            hard.append({"code": code, "method": method, "path": path, "passed": bool(condition), "detail": detail})
            check(condition, code, method, path, detail)

        def bounds(method, category, key):
            return parsed_rows.get(method, {}).get(category, {}).get(key)

        try:
            for method in INTERVAL_METHODS:
                if not isinstance(supplied.get(method), dict) or supplied[method].get("status") != "COMPLETED":
                    continue
                actual_mod = supplied[method].get("modifications")
                model, expected_mod = model_for(method)
                if not isinstance(actual_mod, dict):
                    fail("STRUCTURAL_MODIFICATIONS", method, "modifications", "Structural provenance required")
                    continue
                invariant("RAW_GRAPH_PRESERVED", method, "modifications.raw_graph_unchanged", actual_mod.get("raw_graph_unchanged") is True, "Observed graph unchanged")
                if method == "BALANCE_INFORMATION_REMOVED":
                    invariant("BALANCE_STRUCTURAL_NESTING", method, "modifications.nesting", expected_mod.get("nesting", {}).get("nested") is True and actual_mod.get("nesting", {}).get("nested") is True,
                              "Independent unchanged-model reconstruction confirms same variables/equalities and relaxed balance caps")
                    members(actual_mod.get("removed_bound_variables"), expected_mod["removed_bound_variables"], method, "modifications.removed_bound_variables")
                if method == "NO_PROTOCOL_CONTINUATION":
                    for field in ("connected_protocol_count", "feature_status", "gross_and_refund_conservation_preserved"):
                        invariant("PROTOCOL_BOUNDARY_STRUCTURE", method, "modifications." + field, type(actual_mod.get(field)) is type(expected_mod[field]) and actual_mod.get(field) == expected_mod[field], "Input-derived continuation boundary field must agree")
                    for field in ("unlinked_source_ratio_equalities", "boundary_source_variables", "physical_outputs_retained_as_source_zero"):
                        members(actual_mod.get(field), expected_mod[field], method, "modifications." + field)
                if method == "NO_CROSS_TARGET_COUPLING":
                    copies = actual_mod.get("product_copies")
                    valid = isinstance(copies, list) and len(copies) == len(domains["addresses"]) and all(isinstance(c, dict) for c in copies)
                    if valid:
                        valid = {c.get("address_asset") for c in copies} == set(domains["addresses"]) and len({c.get("copy_id") for c in copies}) == len(copies) and len({c.get("physical_identity_namespace") for c in copies}) == len(copies)
                        valid = valid and all(c.get("variables") == len(model.variables) and c.get("equalities") == len(model.eq) and c.get("cross_copy_identity_equalities_present") is False for c in copies)
                    invariant("TARGET_COPY_STRUCTURE", method, "modifications.product_copies", valid and actual_mod.get("global_common_source_budget") is False and actual_mod.get("within_each_copy_common_source_budget") is True and actual_mod.get("unbound_all_state_variables_count") == len(model.variables),
                              "One full-constraint independent namespace per address-asset; internal budget retained; cross-copy shared identity removed")
                    members(actual_mod.get("unbound_identical_physical_variables"), domains["allocation_ports"], method, "modifications.unbound_identical_physical_variables")
            for category, keys in (("addresses", domains["addresses"]), ("events", domains["interval_events"]), ("joint_by_asset", domains["interval_joint_assets"])):
                for key in keys:
                    full = bounds("FULL_INTERVAL", category, key)
                    relaxed = bounds("BALANCE_INFORMATION_REMOVED", category, key)
                    independent = bounds("NO_CROSS_TARGET_COUPLING", category, key)
                    protocol = bounds("NO_PROTOCOL_CONTINUATION", category, key)
                    if full is not None and relaxed is not None:
                        invariant("BALANCE_ENDPOINT_NESTING", "BALANCE_INFORMATION_REMOVED", category + "." + key, relaxed[0] <= full[0] <= full[1] <= relaxed[1], "Same-input relaxed interval must contain FULL")
                    if full is not None and independent is not None:
                        invariant("TARGET_COPY_SINGLETON_EQUALITY" if category != "joint_by_asset" else "TARGET_COPY_JOINT_CONTAINS_FULL", "NO_CROSS_TARGET_COUPLING", category + "." + key,
                                  independent == full if category != "joint_by_asset" else independent[0] <= full[0] <= full[1] <= independent[1], "Independent copies preserve singleton intervals and contain original joint range")
                    if not domains["connected_protocol_ids"] and full is not None and protocol is not None:
                        invariant("NO_PROTOCOL_FEATURE_EQUALITY", "NO_PROTOCOL_CONTINUATION", category + "." + key, protocol == full, "No connected conversion implies no endpoint change")
            for asset in domains["interval_joint_assets"]:
                joint = bounds("NO_CROSS_TARGET_COUPLING", "joint_by_asset", asset)
                values = [bounds("NO_CROSS_TARGET_COUPLING", "addresses", g) for g in domains["addresses"] if g.rsplit("|", 1)[1] == asset]
                if joint is not None and all(v is not None for v in values):
                    summed = tuple(sum((v[i] for v in values), F(0)) for i in (0, 1))
                    invariant("TARGET_COPY_EXACT_DECOMPOSITION", "NO_CROSS_TARGET_COUPLING", "joint_by_asset." + asset, joint == summed, "Independent joint is the exact sum of target-copy extrema, not a one-seed feasible total")
        except Exception as exc:
            fail("HARD_INVARIANT_EXCEPTION", None, "$invariants", type(exc).__name__ + ": " + str(exc))
    # Hard failures belong to method check status as well as the query receipt.
    for method, record in method_checks.items():
        record["errors"] = [e for e in errors if e["method"] == method]
        record["passed"] = not record["errors"]
    return {"schema_version": CONTRACT_VERSION, "contract_version": CONTRACT_VERSION,
            "passed": not errors, "status": "PASS" if not errors else "FAIL", "errors": errors,
            "method_checks": method_checks, "hard_invariants": hard, "expected_domains": domains,
            "counts": {"required_methods": len(METHODS), "received_methods": len(supplied),
                       "required_addresses": len(domains.get("addresses", {})),
                       "required_interval_events": len(domains.get("interval_events", [])),
                       "required_baseline_ports": len(domains.get("baseline_events", [])),
                       "post_generation_unique_allocation_audits": len(audit_cache)},
            "bindings": {"input_document_sha256": _hash(doc), "method_results_sha256": _hash(results),
                         "expected_identity_sha256": _hash(expected_identity),
                         "contract_module_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
            "no_solver_or_oracle_called": True, "hidden_or_reference_read": False}
