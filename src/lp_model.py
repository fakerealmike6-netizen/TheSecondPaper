"""Small continuous time-expanded source-amount LP, authored for Stage1B.

Inputs contain observed physical amounts and explicit semantic operations only.
No label/reference files, oracle answers or hidden allocations are read here.
All model coefficients are retained as Fraction; HiGHS supplies candidates and
exact rational primal/dual checks certify reported optimal endpoints.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from fractions import Fraction as F
import math
from typing import Any


def q(value: Any) -> F:
    if isinstance(value, float) or isinstance(value, bool):
        raise ValueError("Amounts/ratios must be exact integer or rational strings")
    return F(value)


def raw(value: Any) -> F:
    result = q(value)
    if result.denominator != 1 or result < 0:
        raise ValueError("Observed raw amounts must be nonnegative integers")
    return result


def fmt(value: F | None) -> str | None:
    if value is None:
        return None
    return str(value.numerator) if value.denominator == 1 else str(value)


@dataclass
class Variable:
    name: str
    asset: str
    scale: F
    lower: F
    upper: F
    kind: str


@dataclass
class Model:
    variables: list[Variable] = field(default_factory=list)
    eq: list[dict[int, F]] = field(default_factory=list)
    rhs: list[F] = field(default_factory=list)
    eq_names: list[str] = field(default_factory=list)
    event_variables: dict[str, int] = field(default_factory=dict)
    balance_lifts: list[tuple[int, int | None, dict[int, F]]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    variable_names: set[str] = field(default_factory=set)
    _solver_cache: Any = field(default=None, repr=False)

    def var(self, name, asset, scale, lower, upper, kind):
        if name in self.variable_names:
            raise ValueError(f"Duplicate physical variable: {name}")
        idx = len(self.variables)
        self.variables.append(Variable(name, asset, scale, lower / scale, upper / scale, kind))
        self.variable_names.add(name)
        self._solver_cache = None
        return idx

    def equality(self, name, coefficients, rhs=F(0)):
        self._solver_cache = None
        self.eq_names.append(name)
        self.eq.append({i: v for i, v in coefficients.items() if v})
        self.rhs.append(rhs)

    def audit_vector(self, vector: list[F]) -> dict:
        if len(vector) != len(self.variables):
            return {"exact_feasible": False, "reason": "dimension"}
        bad_bounds = [v.name for v, x in zip(self.variables, vector) if not v.lower <= x <= v.upper]
        bad_eq = [name for name, row, b in zip(self.eq_names, self.eq, self.rhs)
                  if sum((a * vector[i] for i, a in row.items()), F(0)) != b]
        return {"exact_feasible": not bad_bounds and not bad_eq,
                "bad_bounds": bad_bounds, "bad_equalities": bad_eq}

    def lift_hidden_allocation_for_audit(self, event_values: dict[str, str]) -> list[F]:
        """Verification-only: never called by solve_interval or LP construction."""
        if set(event_values) != set(self.event_variables):
            raise ValueError("Hidden allocation must name every physical source variable exactly once")
        vector = [F(0)] * len(self.variables)
        for name, idx in self.event_variables.items():
            vector[idx] = q(event_values[name]) / self.variables[idx].scale
        for idx, previous, changes in self.balance_lifts:
            total = vector[previous] * self.variables[previous].scale if previous is not None else F(0)
            total += sum((coefficient * vector[i] * self.variables[i].scale for i, coefficient in changes.items()), F(0))
            vector[idx] = total / self.variables[idx].scale
        return vector

    def statistics(self):
        return {"variables": len(self.variables), "equalities": len(self.eq),
                "nonzero_coefficients": sum(len(row) for row in self.eq),
                "physical_source_variables": len(self.event_variables),
                **self.metadata}


def build_model(graph: dict) -> Model:
    """Build one sparse LP from a fixed, ordered, observed graph.

    A known initial actual balance is replayed, never used as initial source.
    A missing balance remains unknown; it receives a source-cap upper bound,
    not an invented actual zero. All source starts at exact seed events.
    Validated semantic operations are atomic; callers must establish ordering
    and certification before supplying real conversion operations.
    """
    if graph.get('scope') == 'ORDER_UNRESOLVED_MODEL_NOT_SOLVABLE':
        raise ValueError('Unresolved physical execution order cannot be solved by invented ordinal')
    events = sorted(graph["events"], key=lambda e: e["order"])
    if sum(event["kind"] == "seed" for event in events) != 1:
        raise ValueError("Stage1B single-event seed model requires exactly one seed injection")
    if len({e["id"] for e in events}) != len(events):
        raise ValueError("Duplicate physical event identity")
    if len({e["order"] for e in events}) != len(events):
        raise ValueError("Ambiguous execution order; supply certified distinct order or semantic unit")
    if any(not isinstance(e["order"], int) for e in events):
        raise ValueError("Order must be a certified integer ordinal, not guessed trace/log interleaving")
    if any(e.get("reverted", False) for e in events if e["kind"] != "gas"):
        raise ValueError("Reverted value flows cannot enter the LP")
    targets = set(graph.get("target_accounts", []))
    initial = {k: (None if value is None else raw(value)) for k, value in graph.get("initial_balances", {}).items()}
    actual = dict(initial)
    potential: dict[str, F] = {}
    for event in events:
        kind = event["kind"]
        if kind == "seed":
            a = event["asset"]
            potential[a] = potential.get(a, F(0)) + raw(event["amount_raw"])
        elif kind == "conversion":
            if event.get("certified_semantics") not in ("SYNTHETIC_CONTROLLED", "CERTIFIED_LOCAL_COMPONENT"):
                raise ValueError("Conversion requires explicit component certification")
            gross, refund = raw(event["gross_raw"]), raw(event.get("refund_raw", "0"))
            out = raw(event["output_raw"])
            net = gross - refund
            if net <= 0:
                raise ValueError("Conversion net input must be positive")
            asset, output_asset = event["asset"], event["output_asset"]
            cap = min(out, potential.get(asset, F(0)) * out / net)
            potential[output_asset] = potential.get(output_asset, F(0)) + cap
        elif kind not in {"transfer", "normal_incoming", "gas", "multioutput", "boundary_outflow"}:
            raise ValueError(f"Unsupported semantic operation: {kind}")
    # A conservative finite overestimate: conversions add potential without
    # subtracting the input. It cannot create source because equality rows
    # still enforce every actual input, refund, output and time balance.
    scales = {a: (value if value > 0 else F(1)) for a, value in potential.items()}
    model = Model(metadata={"scenario_id": graph.get("scenario_id"),
                            "model_type": "CONTINUOUS_TIME_EXPANDED_LP",
                            "balance_status": "CONDITIONAL_MISSING_BALANCE" if any(v is None for v in initial.values()) else "KNOWN_DECLARED_LEDGER",
                            "source_potential_raw": {a: fmt(v) for a, v in potential.items()},
                            "scales_raw_per_solver_unit": {a: fmt(v) for a, v in scales.items()},
                            "unknown_initial_balances": [k for k,v in initial.items() if v is None],
                            "acquisition_depth_constraints_in_lp": False,
                            "fund_age_constraints_in_lp": False})
    last: dict[str, int] = {}

    def variable(name, asset, cap, fixed=None, kind="event"):
        cap = min(cap, potential.get(asset, F(0)))
        lo = fixed if fixed is not None else F(0)
        hi = fixed if fixed is not None else cap
        idx = model.var(name, asset, scales.get(asset, F(1)), lo, hi, kind)
        model.event_variables[name] = idx
        return idx

    for event in events:
        eid, kind = event["id"], event["kind"]
        changes: dict[str, dict[int, F]] = {}
        actual_changes: dict[str, F] = {}
        outgoing: dict[str, dict[int, F]] = {}
        actual_outgoing: dict[str, F] = {}

        def port(account, asset, amount, idx, sign):
            key = account + "|" + asset
            if sign < 0 and account in targets:
                raise ValueError("Service account is a terminal; outgoing platform ledger is forbidden")
            actual_changes[key] = actual_changes.get(key, F(0)) + sign * amount
            row = changes.setdefault(key, {})
            row[idx] = row.get(idx, F(0)) + sign
            if sign < 0:
                debit = outgoing.setdefault(key, {})
                debit[idx] = debit.get(idx, F(0)) - sign
                actual_outgoing[key] = actual_outgoing.get(key, F(0)) - sign * amount

        if event.get("context_only") and kind not in {"normal_incoming", "gas", "boundary_outflow"}:
            raise ValueError("Context-only records require an explicit non-propagating balance or boundary role")
        if kind in {"seed", "normal_incoming", "transfer", "gas", "boundary_outflow"}:
            a, amount = event["asset"], raw(event["amount_raw"])
            if kind == "gas" and a != "ETH":
                raise ValueError("Native gas cannot consume ERC20 source")
            fixed = amount if kind == "seed" else (F(0) if kind == "normal_incoming" else None)
            idx = variable(eid, a, amount, fixed, "gas" if kind == "gas" else "event")
            if kind in {"transfer", "gas", "boundary_outflow"}:
                port(event["from"], a, amount, idx, -1)
            if kind not in {"gas", "boundary_outflow"}:
                port(event["to"], a, amount, idx, 1)
        elif kind == "conversion":
            a, outa = event["asset"], event["output_asset"]
            gross, refund, output = raw(event["gross_raw"]), raw(event.get("refund_raw", "0")), raw(event["output_raw"])
            net = gross - refund
            gi = variable(eid + ":input", a, gross)
            ri = variable(eid + ":refund", a, refund)
            oi = variable(eid + ":output", outa, output)
            ni = variable(eid + ":net", a, net)
            sa, so = scales.get(a, F(1)), scales.get(outa, F(1))
            model.equality("protocol:" + eid + ":gross_refund", {gi: F(1), ri: F(-1), ni: F(-1)})
            model.equality("protocol:" + eid + ":fixed_ratio", {oi: F(1), ni: -sa * output / net / so})
            port(event["from"], a, gross, gi, -1)
            port(event.get("refund_to", event["from"]), a, refund, ri, 1)
            port(event["to"], outa, output, oi, 1)
        elif kind == "multioutput":
            if event.get("certified_semantics") != "SYNTHETIC_CONTROLLED":
                raise ValueError("Only the controlled multioutput operation is supported")
            a, gross = event["asset"], raw(event["gross_raw"])
            gi = variable(eid + ":input", a, gross)
            coupling = {gi: F(1)}
            total = F(0)
            port(event["from"], a, gross, gi, -1)
            for number, output in enumerate(event["outputs"]):
                amount = raw(output["amount_raw"])
                total += amount
                oi = variable(eid + f":output{number}", a, amount)
                coupling[oi] = F(-1)
                port(output["to"], a, amount, oi, 1)
            if total != gross:
                raise ValueError("Controlled multioutput physical ports do not close")
            model.equality("protocol:" + eid + ":shared_capacity", coupling)

        # Even a closed atomic protocol must fund its gross input before its
        # refund/output can return. The residual after physical debit is a
        # separate state; netting a refund first would admit circular funding.
        for key, debits in sorted(outgoing.items()):
            asset = key.split("|", 1)[1]
            sourcecap = potential.get(asset, F(0))
            pre_actual = actual.get(key)
            after_debit = None if pre_actual is None else pre_actual - actual_outgoing[key]
            if after_debit is not None and after_debit < 0:
                raise ValueError("Known gross input exceeds pre-operation actual balance: " + key + " at " + eid)
            cap = sourcecap if after_debit is None else min(after_debit, sourcecap)
            hi = model.var("h:" + key + ":" + eid, asset, scales.get(asset, F(1)), F(0), cap, "pre_refund_residue")
            previous = last.get(key)
            row = {hi: F(1)}
            if previous is not None:
                row[previous] = F(-1)
            for idx, coefficient in debits.items():
                row[idx] = row.get(idx, F(0)) + coefficient * model.variables[idx].scale / model.variables[hi].scale
            model.equality("pre_refund_availability:" + key + ":" + eid, row)
            model.balance_lifts.append((hi, previous, {idx: -coefficient for idx, coefficient in debits.items()}))

        anchors = {k: raw(v) for k, v in event.get("balance_anchors_after", {}).items()}
        for key in sorted(set(changes) | set(anchors)):
            asset = key.split("|", 1)[1]
            if key not in actual:
                actual[key] = None
                model.metadata["unknown_initial_balances"].append(key)
                model.metadata["balance_status"] = "CONDITIONAL_MISSING_BALANCE"
            if actual[key] is not None:
                actual[key] += actual_changes.get(key, F(0))
                if actual[key] < 0:
                    raise ValueError("Known ledger becomes negative: " + key + " at " + eid)
            if key in anchors:
                if actual[key] is not None and actual[key] != anchors[key]:
                    raise ValueError("Balance anchor disagrees with complete declared replay")
                actual[key] = anchors[key]
            sourcecap = potential.get(asset, F(0))
            cap = sourcecap if actual[key] is None else min(actual[key], sourcecap)
            zi = model.var("z:" + key + ":" + eid, asset, scales.get(asset, F(1)), F(0), cap, "balance")
            row = {zi: F(1)}
            previous = last.get(key)
            if previous is not None:
                row[previous] = F(-1)
            for idx, sign in changes.get(key, {}).items():
                row[idx] = row.get(idx, F(0)) - sign * model.variables[idx].scale / model.variables[zi].scale
            model.equality("balance:" + key + ":" + eid, row)
            model.balance_lifts.append((zi, previous, changes.get(key, {})))
            last[key] = zi
    model.metadata["terminal_balances"] = {key: model.variables[idx].name for key, idx in last.items()}
    return model


def _rational_candidate(values):
    return [F(str(float(value))).limit_denominator(10**9) for value in values]


def _recover_exact_primal_from_active_bounds(model: Model, result):
    """Recover a small rational vertex, never relax the original constraints.

    The double solution only proposes which *exact* physical bounds are
    active. Sparse rational elimination then solves the original equalities
    and those exact bounds. A proposal is accepted only after exact primal
    feasibility and the separate exact dual certificate both pass.
    """
    n = len(model.variables)
    if n > 512 or len(model.eq) > 2048:
        return None, {"method": "ACTIVE_BOUND_EXACT_LINEAR_SOLVE", "status": "RECOVERY_SIZE_LIMIT", "max_variables": 512}
    basis: dict[int, tuple[dict[int, F], F]] = {}

    def add(coefficients, rhs):
        row = {i: F(a) for i, a in coefficients.items() if a}
        rhs = F(rhs)
        while row:
            pivot = min(row)
            if pivot not in basis:
                divisor = row[pivot]
                row = {i: a / divisor for i, a in row.items()}
                basis[pivot] = row, rhs / divisor
                return True
            old, old_rhs = basis[pivot]
            factor = row[pivot]
            for i, a in old.items():
                value = row.get(i, F(0)) - factor * a
                if value:
                    row[i] = value
                else:
                    row.pop(i, None)
            rhs -= factor * old_rhs
        return rhs == 0

    for row, rhs in zip(model.eq, model.rhs):
        if not add(row, rhs):
            return None, {"method": "ACTIVE_BOUND_EXACT_LINEAR_SOLVE", "status": "INCONSISTENT_EXACT_EQUALITIES"}
    for i, var in enumerate(model.variables):
        if var.lower == var.upper and not add({i: F(1)}, var.lower):
            return None, {"method": "ACTIVE_BOUND_EXACT_LINEAR_SOLVE", "status": "INCONSISTENT_EXACT_FIXED_BOUND"}
    candidates = []
    for i, var in enumerate(model.variables):
        if var.lower == var.upper:
            continue
        value = float(result.x[i])
        for side, bound, multiplier in (("lower", var.lower, result.lower.marginals[i]), ("upper", var.upper, result.upper.marginals[i])):
            distance = abs(value - float(bound))
            if distance <= 1e-7:
                nonzero_dual = abs(float(multiplier)) > 1e-8
                candidates.append((0 if nonzero_dual else 1, distance, i, side, bound))
    chosen = 0
    for _, _, i, _, bound in sorted(candidates):
        before = len(basis)
        if add({i: F(1)}, bound) and len(basis) > before:
            chosen += 1
        if len(basis) == n:
            break
    # Free zero-cost coordinates need not lie at a bound. A rational proposal
    # can complete the system, but receives no special trust: all exact
    # original bounds and KKT equations below must still hold.
    free_proposals = 0
    if len(basis) < n:
        for i, value in enumerate(_rational_candidate(result.x)):
            before = len(basis)
            if add({i: F(1)}, value) and len(basis) > before:
                free_proposals += 1
            if len(basis) == n:
                break
    if len(basis) != n:
        return None, {"method": "ACTIVE_BOUND_EXACT_LINEAR_SOLVE", "status": "RANK_DEFICIENT", "rank": len(basis)}
    vector = [F(0)] * n
    for pivot in sorted(basis, reverse=True):
        row, rhs = basis[pivot]
        vector[pivot] = rhs - sum((a * vector[i] for i, a in row.items() if i != pivot), F(0))
    return vector, {"method": "ACTIVE_BOUND_EXACT_LINEAR_SOLVE", "status": "EXACT_CANDIDATE_RECOVERED",
                    "active_bound_rows_added": chosen, "free_coordinate_proposals": free_proposals,
                    "bound_selection_tolerance_solver_units": "1e-7",
                    "original_model_changed": False}


def _certify(model: Model, result, cost: list[F]):
    """Independent exact KKT arithmetic for a HiGHS candidate, not an oracle."""
    vector = _rational_candidate(result.x)
    feasibility = model.audit_vector(vector)
    recovery = {"method": "DIRECT_RATIONAL_RECONSTRUCTION", "status": "EXACT_CANDIDATE_RECOVERED" if feasibility["exact_feasible"] else "DIRECT_RECONSTRUCTION_FAILED"}
    if not feasibility["exact_feasible"]:
        recovered, recovery = _recover_exact_primal_from_active_bounds(model, result)
        if recovered is not None:
            vector = recovered
            feasibility = model.audit_vector(vector)
    eqdual = _rational_candidate(result.eqlin.marginals)
    lowerdual = _rational_candidate(result.lower.marginals)
    upperdual = _rational_candidate(result.upper.marginals)
    stationarity = list(cost)
    for row, y in zip(model.eq, eqdual):
        for i, a in row.items():
            stationarity[i] -= a * y
    for i in range(len(cost)):
        stationarity[i] -= lowerdual[i] + upperdual[i]
    primal = sum((c * x for c, x in zip(cost, vector)), F(0))
    dual = sum((b * y for b, y in zip(model.rhs, eqdual)), F(0))
    dual += sum((v.lower * l + v.upper * u for v, l, u in zip(model.variables, lowerdual, upperdual)), F(0))
    passed = feasibility["exact_feasible"] and all(l >= 0 for l in lowerdual) and all(u <= 0 for u in upperdual) and not any(stationarity) and primal == dual
    return {"certified": passed, "exact_primal_feasible": feasibility["exact_feasible"],
            "exact_dual_feasible": all(l >= 0 for l in lowerdual) and all(u <= 0 for u in upperdual) and not any(stationarity),
            "exact_primal_objective": fmt(primal), "exact_dual_objective": fmt(dual),
            "rational_reconstruction_denominator_limit": 10**9,
            "primal_recovery": recovery,
            "failure_detail": None if passed else feasibility}, vector, primal


def solve_interval(model: Model, event_names: list[str], time_limit_seconds=30) -> dict:
    import numpy as np
    from scipy.optimize import linprog
    from scipy.sparse import coo_matrix

    if not event_names or len(event_names) != len(set(event_names)):
        raise ValueError("Objective must contain distinct observed source ports")
    indices = [model.event_variables[name] for name in event_names]
    assets = {model.variables[i].asset for i in indices}
    if len(assets) != 1:
        raise ValueError("Do not sum raw quantities across assets")
    asset = next(iter(assets))
    scale = model.variables[indices[0]].scale
    if model._solver_cache is None:
        rr, cc, vv = [], [], []
        for r, row in enumerate(model.eq):
            for c, value in row.items():
                rr.append(r); cc.append(c); vv.append(float(value))
        matrix = coo_matrix((vv, (rr, cc)), shape=(len(model.eq), len(model.variables))).tocsr()
        bounds = [(float(v.lower), float(v.upper)) for v in model.variables]
        model._solver_cache = matrix, bounds, vv
    matrix, bounds, vv = model._solver_cache
    if not all(math.isfinite(v) for pair in bounds for v in pair) or not all(math.isfinite(v) for v in vv):
        return {"status": "NUMERICAL_UNKNOWN", "lower_raw": None, "upper_raw": None, "reason": "nonfinite_double_representation"}
    endpoints = {}
    for label, sign in (("upper", -1), ("lower", 1)):
        cost = [F(0)] * len(model.variables)
        for i in indices:
            cost[i] = F(sign) * model.variables[i].scale / scale
        result = linprog(np.array([float(c) for c in cost]), A_eq=matrix, b_eq=np.array([float(b) for b in model.rhs]),
                         bounds=bounds, method="highs", options={"time_limit": time_limit_seconds,
                         "primal_feasibility_tolerance": 1e-9, "dual_feasibility_tolerance": 1e-9})
        if result.status != 0:
            status = {1: "SOLVER_LIMIT", 2: "INFEASIBLE_REPORTED_BY_SOLVER", 3: "UNBOUNDED_REPORTED_BY_SOLVER"}.get(result.status, "NUMERICAL_UNKNOWN")
            endpoints[label] = {"status": status, "raw": None, "solver_status": int(result.status), "message": result.message}
            continue
        certificate, vector, value = _certify(model, result, cost)
        exact = value * sign * scale if certificate["certified"] else None
        endpoints[label] = {"status": "OPTIMAL_EXACT_CERTIFIED" if certificate["certified"] else "NUMERICAL_UNKNOWN",
                            "raw": fmt(exact), "approximate_raw": str(float(result.fun) * sign * float(scale)),
                            "certificate": certificate,
                            "witness_event_source_raw": {name: fmt(vector[i] * model.variables[i].scale) for name, i in model.event_variables.items()} if certificate["certified"] else None}
    good = all(e["status"] == "OPTIMAL_EXACT_CERTIFIED" for e in endpoints.values())
    upper = q(endpoints["upper"]["raw"]) if endpoints["upper"]["raw"] is not None else None
    return {"status": "OPTIMAL_EXACT_CERTIFIED" if good else "UNRESOLVED",
            "asset": asset, "objective_events": event_names,
            "lower_raw": endpoints["lower"]["raw"], "upper_raw": endpoints["upper"]["raw"],
            "positive_support": "CERTIFIED_POSITIVE" if upper is not None and upper > 0 else "CERTIFIED_ZERO" if upper == 0 else "NUMERICAL_OR_SOLVER_UNKNOWN",
            "endpoints": endpoints}


def solve_target_groups(model: Model, groups: dict[str, list[str]]) -> dict:
    """Optimize each joint group first. Certified zero skips its entry solves."""
    output = {}
    for group, entries in groups.items():
        joint = solve_interval(model, entries)
        individual = {}
        if len(entries) > 1:
            if joint["positive_support"] == "CERTIFIED_ZERO":
                individual = {e: {"status": "SKIPPED_CERTIFIED_ZERO_JOINT", "lower_raw": "0", "upper_raw": "0"} for e in entries}
            else:
                individual = {e: solve_interval(model, [e]) for e in entries}
        output[group] = {"joint": joint, "entry_intervals": individual}
    return output
