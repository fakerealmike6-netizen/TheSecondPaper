"""Exact nonnegative-subset certificates, with no optimizer or Oracle calls.

A zero parent is trusted only after reconstructing its rational dual bound and
its full feasible source allocation against this exact model. The receiver
repeats those checks from the supplied parent, never from derived status flags.
"""
from __future__ import annotations

import copy
import hashlib
import json
from fractions import Fraction as F

RULE = "DERIVED_ZERO_FROM_CERTIFIED_NONNEGATIVE_SUPERSET"
DUAL_SCHEMA = "exact-zero-upper-dual-v1"


def exact(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, F)):
        raise ValueError("Expected an exact rational value")
    return F(value)


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def model_identity(model, document, method, target_copy=None):
    constraints = {
        "variables": [[v.name, v.asset, str(v.scale), str(v.lower), str(v.upper), v.kind]
                      for v in model.variables],
        "equalities": [[name, [[i, str(c)] for i, c in sorted(row.items())], str(rhs)]
                       for name, row, rhs in zip(model.eq_names, model.eq, model.rhs)],
        "event_variables": sorted(model.event_variables.items()),
        "balance_lifts": [[i, previous, [[j, str(c)] for j, c in sorted(changes.items())]]
                          for i, previous, changes in model.balance_lifts],
    }
    return {"schema": "stage1c-exact-model-identity-v1", "model_sha256": digest(constraints),
        "input_sha256": digest(document),
        "query_id": document.get("query_id", document.get("scenario_id", document.get("name"))),
        "method": method, "target_copy": copy.deepcopy(target_copy)}


def copy_identity(targets, group):
    return {"copy_id": "F^" + str(sorted(targets).index(group)), "address_asset": group}


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def certify_zero_parent(model, parent, identity):
    """Audit a directly solved zero maximum once per model/parent, exactly."""
    _require(isinstance(parent, dict), "Missing parent interval")
    _require(parent.get("status") == "OPTIMAL_EXACT_CERTIFIED", "Parent is not certified")
    _require(parent.get("solution_origin") != RULE and "zero_derivation" not in parent,
             "Parent must carry a direct upper certificate, not another derivation")
    _require(parent.get("proof_model_identity") == identity, "Parent model/input/variant/copy differs")
    _require(exact(parent.get("upper_raw")) == 0 and exact(parent.get("lower_raw")) == 0,
             "Parent must have an exact zero upper, not a zero lower or approximate zero")
    names = parent.get("objective_events")
    _require(isinstance(names, list) and names and all(isinstance(e, str) for e in names)
             and len(names) == len(set(names)), "Parent objective has missing/duplicate events")
    indices = [model.event_variables[e] for e in names]
    _require(len(indices) == len(set(indices)), "Parent objective aliases a physical variable")
    assets = {model.variables[i].asset for i in indices}
    _require(len(assets) == 1 and parent.get("asset") in assets, "Parent asset differs")
    _require(all(model.variables[i].lower >= 0 and model.variables[i].scale > 0 for i in indices),
             "Parent source variables are not proven nonnegative")
    upper = parent.get("endpoints", {}).get("upper")
    _require(isinstance(upper, dict) and upper.get("status") == "OPTIMAL_EXACT_CERTIFIED"
             and exact(upper.get("raw")) == 0, "Missing exact parent upper endpoint")
    cert = upper.get("certificate")
    _require(isinstance(cert, dict) and all(cert.get(k) is True for k in
        ("certified", "exact_primal_feasible", "exact_dual_feasible")), "Parent flags are not certified")
    dual = cert.get("exact_zero_upper_dual")
    _require(isinstance(dual, dict) and dual.get("schema") == DUAL_SCHEMA,
             "Missing reconstructible parent dual certificate")
    yy = [exact(v) for v in dual["equality_multipliers"]]
    ll = [exact(v) for v in dual["lower_multipliers"]]
    uu = [exact(v) for v in dual["upper_multipliers"]]
    _require(len(yy) == len(model.eq) and len(ll) == len(uu) == len(model.variables),
             "Parent dual dimensions differ")
    _require(all(v >= 0 for v in ll) and all(v <= 0 for v in uu), "Parent dual signs fail")
    scale = model.variables[indices[0]].scale
    cost = [F(0)] * len(model.variables)
    for i in indices:
        cost[i] = -model.variables[i].scale / scale
    stationarity = list(cost)
    for row, y in zip(model.eq, yy):
        for i, coefficient in row.items():
            stationarity[i] -= coefficient * y
    for i in range(len(cost)):
        stationarity[i] -= ll[i] + uu[i]
    bound = sum((rhs * y for rhs, y in zip(model.rhs, yy)), F(0))
    bound += sum((v.lower * low + v.upper * up for v, low, up in zip(model.variables, ll, uu)), F(0))
    _require(not any(stationarity) and bound == 0, "Parent exact dual bound is not zero")
    _require(exact(cert.get("exact_primal_objective")) == 0
             and exact(cert.get("exact_dual_objective")) == bound, "Parent objective certificate differs")
    witness = upper.get("witness_event_source_raw")
    _require(isinstance(witness, dict) and set(witness) == set(model.event_variables),
             "Missing full parent feasibility witness")
    values = {e: exact(v) for e, v in witness.items()}
    vector = model.lift_hidden_allocation_for_audit(values)
    _require(model.audit_vector(vector).get("exact_feasible") is True, "Parent witness is infeasible")
    _require(sum((values[e] for e in names), F(0)) == 0, "Parent witness objective is not zero")
    return {"identity": identity, "events": names, "asset": parent["asset"],
        "parent_sha256": digest(parent), "certificate_sha256": digest(cert),
        "witness_sha256": digest(witness), "witness": witness,
        "nonnegative_variables": [model.variables[i].name for i in indices]}


def derive_zero(parent_evidence, parent_ref, names):
    names = sorted(names)
    _require(names and len(names) == len(set(names)) and set(names) <= set(parent_evidence["events"]),
             "Derived objective must be a nonempty distinct subset")
    proof = {"schema": "stage1c-certified-zero-subset-v1", "rule": RULE,
        "parent_ref": copy.deepcopy(parent_ref), "parent_sha256": parent_evidence["parent_sha256"],
        "parent_certificate_sha256": parent_evidence["certificate_sha256"],
        "model_identity": copy.deepcopy(parent_evidence["identity"]),
        "parent_objective_events": list(parent_evidence["events"]), "subset_events": names,
        "asset": parent_evidence["asset"], "feasible_witness_sha256": parent_evidence["witness_sha256"],
        "nonnegative_parent_variables": list(parent_evidence["nonnegative_variables"]),
        "argument": "For every feasible x: 0 <= sum(S) <= sum(J) <= 0; a feasible x exists."}
    endpoints = {label: {"status": RULE, "raw": "0", "solver_called": False,
        "certificate": {"kind": RULE, "parent_certificate_sha256": proof["parent_certificate_sha256"]},
        "witness_event_source_raw": copy.deepcopy(parent_evidence["witness"])} for label in ("lower", "upper")}
    return {"status": "OPTIMAL_EXACT_CERTIFIED", "asset": proof["asset"],
        "objective_events": names, "lower_raw": "0", "upper_raw": "0", "positive_support": "CERTIFIED_ZERO",
        "solution_origin": RULE, "solver_called": False, "zero_derivation": proof,
        "proof_model_identity": copy.deepcopy(parent_evidence["identity"]), "endpoints": endpoints}


def validate_derived_zero(model, document, method, row, result, targets, entries, *, cache=None):
    """Receiver-only verification from observed model and the actual parent row.

    No solver call, no generated answer, no chained proof, no global memo.
    ``cache`` belongs only to this one acceptance call.
    """
    cache = {} if cache is None else cache
    _require(row.get("solution_origin") == RULE and row.get("solver_called") is False,
             "Derived output must explicitly deny a child solver call")
    proof = row.get("zero_derivation")
    _require(isinstance(proof, dict) and proof.get("schema") == "stage1c-certified-zero-subset-v1"
             and proof.get("rule") == RULE, "Missing derived-zero proof")
    names = sorted(entries)
    _require(names and len(names) == len(set(names)), "Invalid expected subset")
    target_copy = None
    if method == "NO_CROSS_TARGET_COUPLING":
        containing = [g for g, es in targets.items() if set(names) <= set(es)]
        _require(len(containing) == 1, "Target-copy subset is not unique")
        target_copy = copy_identity(targets, containing[0])
    identity_key = ("identity", method, None if target_copy is None else target_copy["address_asset"])
    if identity_key not in cache:
        cache[identity_key] = model_identity(model, document, method, target_copy)
    identity = cache[identity_key]
    _require(proof.get("model_identity") == row.get("proof_model_identity") == identity,
             "Derived model/input/query/variant/copy identity differs")
    ref = proof.get("parent_ref")
    _require(isinstance(ref, dict) and set(ref) == {"category", "key"}, "Invalid parent reference")
    category, key = ref["category"], ref["key"]
    _require(isinstance(key, str), "Invalid parent key")
    asset = row.get("asset")
    if category == "joint_by_asset":
        _require(target_copy is None and key == asset, "Cross-copy or wrong-asset joint parent")
        expected_parent_events = sorted(e for g, es in targets.items() if g.rsplit("|", 1)[1] == asset for e in es)
    else:
        _require(category == "addresses" and key in targets, "Parent must be a current address or asset union")
        _require(target_copy is None or key == target_copy["address_asset"], "Cross-copy address parent")
        expected_parent_events = sorted(targets[key])
    parent = result.get(category, {}).get(key)
    _require(isinstance(parent, dict) and parent.get("objective_events") == expected_parent_events,
             "Parent objective differs from the frozen output domain")
    parent_hash = digest(parent)
    _require(proof.get("parent_sha256") == parent_hash, "Parent result reference hash differs")
    parent_key = ("parent", digest(identity), parent_hash)
    if parent_key not in cache:
        cache[parent_key] = certify_zero_parent(model, parent, identity)
    verified = cache[parent_key]
    _require(proof.get("parent_objective_events") == verified["events"]
             and proof.get("subset_events") == names and set(names) <= set(verified["events"]),
             "Subset relation is false")
    _require(proof.get("asset") == asset == verified["asset"]
             and all(model.variables[model.event_variables[e]].asset == asset for e in names), "Subset asset differs")
    _require(proof.get("nonnegative_parent_variables") == verified["nonnegative_variables"],
             "Nonnegative variable proof differs")
    _require(proof.get("parent_certificate_sha256") == verified["certificate_sha256"]
             and proof.get("feasible_witness_sha256") == verified["witness_sha256"], "Parent proof reference differs")
    _require(exact(row.get("lower_raw")) == exact(row.get("upper_raw")) == 0, "Derived interval is not zero")
    _require(row.get("objective_events") == names and row.get("status") == "OPTIMAL_EXACT_CERTIFIED"
             and row.get("positive_support") == "CERTIFIED_ZERO", "Derived interval fields differ")
    endpoints = row.get("endpoints")
    _require(isinstance(endpoints, dict) and set(endpoints) == {"lower", "upper"}, "Missing derived endpoints")
    for endpoint in endpoints.values():
        _require(isinstance(endpoint, dict) and endpoint.get("status") == RULE
                 and endpoint.get("solver_called") is False and exact(endpoint.get("raw")) == 0,
                 "Derived endpoint impersonates an optimizer or has a wrong value")
        _require(endpoint.get("certificate") == {"kind": RULE,
            "parent_certificate_sha256": verified["certificate_sha256"]}, "Derived endpoint certificate differs")
        _require(endpoint.get("witness_event_source_raw") == verified["witness"], "Derived feasible witness differs")
    return {"passed": True, "rule": RULE, "parent_ref": ref, "model_identity": identity}
