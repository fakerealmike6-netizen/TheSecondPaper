"""Independent CONTROLLED_V1 oracle: integral minimum-cost time-state flow.

Only the observed event dictionary is accepted. No primary model, solver,
constraint matrix, hidden allocation, or result is imported or read. For the
supported integer-capacity, 1:1 operations the time-state flow polytope is a
network polytope; total unimodularity makes these integral optima exact extrema
of the CONTINUOUS feasible set, rather than an integer approximation to it.
"""
from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from itertools import product

ORACLE_VERSION = "CONTROLLED_TIME_STATE_MINCOST_V1"


def _ports(graph):
    keys = set(graph.get("initial_balances", {}))
    for e in graph["events"]:
        a = e["asset"]
        if "from" in e:
            keys.add(e["from"] + "|" + a)
        if "to" in e:
            keys.add(e["to"] + "|" + e.get("output_asset", a))
        if e["kind"] == "conversion":
            keys.add(e.get("refund_to", e["from"]) + "|" + a)
        for out in e.get("outputs", []):
            keys.add(out["to"] + "|" + a)
    return sorted(keys)


def _physical_step(event, before):
    """Independent actual-ledger interpreter, including gross-before-refund."""
    after = dict(before)
    debit, credit = defaultdict(int), defaultdict(int)
    kind, asset = event["kind"], event["asset"]
    if kind in ("transfer", "gas", "boundary_outflow"):
        debit[event["from"] + "|" + asset] += int(event["amount_raw"])
    if kind in ("seed", "normal_incoming", "transfer"):
        credit[event["to"] + "|" + asset] += int(event["amount_raw"])
    if kind == "conversion":
        gross, refund, output = map(int, (event["gross_raw"], event.get("refund_raw", 0), event["output_raw"]))
        if gross - refund != output or output <= 0:
            raise ValueError("Oracle supports positive net 1:1 conversions only")
        debit[event["from"] + "|" + asset] += gross
        credit[event.get("refund_to", event["from"]) + "|" + asset] += refund
        credit[event["to"] + "|" + event["output_asset"]] += output
    if kind == "multioutput":
        debit[event["from"] + "|" + asset] += int(event["gross_raw"])
        if sum(int(o["amount_raw"]) for o in event["outputs"]) != int(event["gross_raw"]):
            raise ValueError("Unclosed multioutput")
        for output in event["outputs"]:
            credit[output["to"] + "|" + asset] += int(output["amount_raw"])
    residue = {}
    for key in set(debit) | set(credit):
        b = before.get(key)
        residue[key] = None if b is None else b - debit[key]
        if residue[key] is not None and residue[key] < 0:
            raise ValueError("Negative pre-refund actual residue")
        after[key] = None if b is None else b - debit[key] + credit[key]
    for key, anchor in event.get("balance_anchors_after", {}).items():
        anchor = int(anchor)
        if after.get(key) is not None and after[key] != anchor:
            raise ValueError("Inconsistent observed anchor")
        after[key] = anchor
    return after, residue


def _network(graph):
    events = sorted(graph["events"], key=lambda e: e["order"])
    if len({e["order"] for e in events}) != len(events):
        raise ValueError("Ambiguous event order")
    seeds = [e for e in events if e["kind"] == "seed"]
    if len(seeds) != 1:
        raise ValueError("Exactly one seed required")
    supply = int(seeds[0]["amount_raw"])
    keys = _ports(graph)
    actual = {k: None if graph.get("initial_balances", {}).get(k) is None else int(graph["initial_balances"][k]) for k in keys}
    arcs, aliases = [], {}
    node_count = 2

    def node():
        nonlocal node_count
        n = node_count
        node_count += 1
        return n

    def arc(u, v, cap, names=()):
        idx = len(arcs)
        arcs.append((u, v, min(supply, int(cap))))
        for name in names:
            aliases[name] = idx
        return idx

    cap = lambda b: supply if b is None else min(supply, b)
    state = {k: node() for k in keys}
    constants = {}
    for e in events:
        kind, eid, asset = e["kind"], e["id"], e["asset"]
        after, residue = _physical_step(e, actual)
        incoming = {k: node() for k in keys}
        next_state = {k: node() for k in keys}
        for key in keys:
            carry = residue.get(key, actual[key])
            arc(state[key], incoming[key], cap(carry))
            arc(incoming[key], next_state[key], cap(after[key]))
        if kind == "seed":
            arc(0, incoming[e["to"] + "|" + asset], supply, [eid])
        elif kind == "normal_incoming":
            constants[eid] = 0
        elif kind in ("transfer", "gas", "boundary_outflow"):
            dest = incoming[e["to"] + "|" + asset] if kind == "transfer" else 1
            arc(state[e["from"] + "|" + asset], dest, e["amount_raw"], [eid])
        elif kind == "conversion":
            junction = node()
            arc(state[e["from"] + "|" + asset], junction, e["gross_raw"], [eid + ":input"])
            arc(junction, incoming[e.get("refund_to", e["from"]) + "|" + asset], e.get("refund_raw", 0), [eid + ":refund"])
            arc(junction, incoming[e["to"] + "|" + e["output_asset"]], e["output_raw"], [eid + ":net", eid + ":output"])
        elif kind == "multioutput":
            junction = node()
            arc(state[e["from"] + "|" + asset], junction, e["gross_raw"], [eid + ":input"])
            for i, out in enumerate(e["outputs"]):
                arc(junction, incoming[out["to"] + "|" + asset], out["amount_raw"], [eid + f":output{i}"])
        else:
            raise ValueError("Unsupported observed operation: " + kind)
        state, actual = next_state, after
    for key in keys:
        arc(state[key], 1, supply)
    return node_count, arcs, aliases, constants, supply


def _min_cost(network, weights):
    n, edges, aliases, constants, required = network
    costs = defaultdict(int)
    constant = sum(constants.get(name, 0) * weight for name, weight in weights.items())
    for name, weight in weights.items():
        if name in aliases:
            costs[aliases[name]] += weight
        elif name not in constants:
            raise ValueError("Unknown objective event: " + name)
    adjacency = [[] for _ in range(n)]
    # [destination, residual capacity, cost, reverse index]
    forward = []
    for i, (u, v, capacity) in enumerate(edges):
        forward.append((u, len(adjacency[u]), capacity))
        adjacency[u].append([v, capacity, costs[i], len(adjacency[v])])
        adjacency[v].append([u, 0, -costs[i], len(adjacency[u]) - 1])
    sent, total = 0, constant
    while sent < required:
        dist, prev = [None] * n, [None] * n
        dist[0] = 0
        for _ in range(n - 1):
            changed = False
            for u in range(n):
                if dist[u] is None:
                    continue
                for i, (v, capacity, cost, _) in enumerate(adjacency[u]):
                    proposed = dist[u] + cost
                    if capacity and (dist[v] is None or proposed < dist[v]):
                        dist[v], prev[v], changed = proposed, (u, i), True
            if not changed:
                break
        if dist[1] is None:
            raise ValueError("Observed source-flow network infeasible")
        amount, v = required - sent, 1
        while v != 0:
            u, i = prev[v]
            amount = min(amount, adjacency[u][i][1])
            v = u
        v = 1
        while v != 0:
            u, i = prev[v]
            edge = adjacency[u][i]
            edge[1] -= amount
            adjacency[v][edge[3]][1] += amount
            v = u
        sent += amount
        total += amount * dist[1]
    values = {name: str(forward[idx][2] - adjacency[forward[idx][0]][forward[idx][1]][1]) for name, idx in aliases.items()}
    values.update({name: str(value) for name, value in constants.items()})
    return total, values


def oracle_weighted_interval(graph, weights):
    network = _network(graph)
    lower, lw = _min_cost(network, weights)
    negupper, uw = _min_cost(network, {k: -v for k, v in weights.items()})
    return {"lower_raw": str(lower), "upper_raw": str(-negupper), "lower_witness": lw, "upper_witness": uw, "status": "OPTIMAL_EXACT_INTEGER_NETWORK"}


def oracle_intervals(graph):
    network = _network(graph)

    def interval(names):
        weights = {name: 1 for name in names}
        low, _ = _min_cost(network, weights)
        high, _ = _min_cost(network, {name: -1 for name in names})
        return {"lower_raw": str(low), "upper_raw": str(-high), "status": "OPTIMAL_EXACT_INTEGER_NETWORK"}

    groups = graph.get("objective_groups", {})
    by_asset = defaultdict(list)
    for group, names in groups.items():
        by_asset[group.rsplit("|", 1)[1]].extend(names)
    if not groups:
        for event in graph["events"]:
            if event["kind"] == "seed":
                by_asset[event["asset"]] = []
            elif event["kind"] == "conversion":
                by_asset[event["output_asset"]] = []
    # Includes all source ports, so conservation and fees are externally checkable.
    return {"oracle_version": ORACLE_VERSION, "status": "OPTIMAL_EXACT_INTEGER_NETWORK",
            "addresses": {group: interval(names) for group, names in sorted(groups.items())},
            "events": {name: interval([name]) for name in sorted(set(network[2]) | set(network[3]))},
            "joint_by_asset": {a: interval(sorted(set(names))) for a, names in sorted(by_asset.items())},
            "continuous_extrema_exact": True,
            "justification": "Integer capacities and 1:1 semantic junctions yield a totally unimodular network; integral extrema equal continuous extrema."}


def enumerate_integer_allocations(graph, limit=200000):
    """Third check: direct chronological source allocation, no flow network.

    Intended for two tiny instances in each family. The network-integrality
    theorem relates the complete integer enumeration to continuous endpoints.
    """
    keys = _ports(graph)
    actual = {k: None if graph.get("initial_balances", {}).get(k) is None else int(graph["initial_balances"][k]) for k in keys}
    states = [({k: 0 for k in keys}, {})]
    for e in sorted(graph["events"], key=lambda e: e["order"]):
        after, residue = _physical_step(e, actual)
        kind, eid, asset = e["kind"], e["id"], e["asset"]
        next_states = []
        for source, allocation in states:
            if kind in ("seed", "normal_incoming"):
                candidates = [{eid: int(e["amount_raw"]) if kind == "seed" else 0}]
            else:
                sender = e["from"] + "|" + asset
                gross = int(e.get("gross_raw", e.get("amount_raw", 0)))
                lo = max(0, source[sender] - residue[sender]) if residue[sender] is not None else 0
                hi = min(source[sender], gross)
                candidates = []
                for x in range(lo, hi + 1):
                    if kind == "conversion":
                        refund, output = int(e.get("refund_raw", 0)), int(e["output_raw"])
                        for r in range(max(0, x - output), min(x, refund) + 1):
                            candidates.append({eid + ":input": x, eid + ":refund": r, eid + ":net": x - r, eid + ":output": x - r})
                    elif kind == "multioutput":
                        caps = [int(o["amount_raw"]) for o in e["outputs"]]
                        for shares in product(*(range(min(c, x) + 1) for c in caps)):
                            if sum(shares) == x:
                                candidates.append({eid + ":input": x, **{eid + f":output{i}": v for i, v in enumerate(shares)}})
                    else:
                        candidates.append({eid: x})
            for values in candidates:
                s = dict(source)
                if kind in ("seed", "normal_incoming", "transfer"):
                    s[e["to"] + "|" + asset] += values[eid]
                if kind in ("transfer", "gas", "boundary_outflow"):
                    s[e["from"] + "|" + asset] -= values[eid]
                if kind == "conversion":
                    s[e["from"] + "|" + asset] -= values[eid + ":input"]
                    s[e.get("refund_to", e["from"]) + "|" + asset] += values[eid + ":refund"]
                    s[e["to"] + "|" + e["output_asset"]] += values[eid + ":output"]
                if kind == "multioutput":
                    s[e["from"] + "|" + asset] -= values[eid + ":input"]
                    for i, out in enumerate(e["outputs"]):
                        s[out["to"] + "|" + asset] += values[eid + f":output{i}"]
                if all(v >= 0 and (after[k] is None or v <= after[k]) for k, v in s.items()):
                    next_states.append((s, allocation | values))
                    if len(next_states) > limit:
                        raise ValueError("Tiny exhaustive enumeration state limit exceeded")
        states, actual = next_states, after
    if not states:
        raise ValueError("Enumeration found no feasible allocation")
    return [allocation for _, allocation in states]


def tiny_enumeration_check(graph):
    allocations = enumerate_integer_allocations(graph)
    oracle = oracle_intervals(graph)
    checked = 0
    failures = []
    objectives = {"event:" + name: [name] for name in oracle["events"]}
    objectives.update({"address:" + name: entries for name, entries in graph["objective_groups"].items()})
    for asset in oracle["joint_by_asset"]:
        objectives["joint:" + asset] = sorted({e for g, es in graph["objective_groups"].items() if g.endswith("|" + asset) for e in es})
    for objective, names in objectives.items():
        values = [sum(a[name] for name in names) for a in allocations]
        kind, name = objective.split(":", 1)
        expected = oracle[{"event": "events", "address": "addresses", "joint": "joint_by_asset"}[kind]][name]
        if (str(min(values)), str(max(values))) != (expected["lower_raw"], expected["upper_raw"]):
            failures.append(objective)
        checked += 1
    return {"sample_id": graph["scenario_id"], "status": "PASS" if not failures else "FAIL", "allocation_count": len(allocations), "objectives_checked": checked,
            "failures": failures, "independent_check": "DIRECT_OPERATION_INTEGER_ENUMERATION", "continuous_endpoint_link": "1:1 network integrality; not a fractional-grid approximation"}


def semantic_equivalence_report(graph):
    """Audit the full tiny boundary relation, plus whole-query directions.

    Raw component: gross enters an adapter; adapter splits refund/deposit;
    WETH issues exactly deposit; wallet splits WETH into observed outputs.
    Folded component: output-sum = gross-source - refund-source. The map
    (g,r,d,m,y) -> (g,r,y), inverse d=m=sum(y), is a bijection for all real
    nonnegative source amounts obeying the displayed capacities. Enumeration
    verifies all integer boundary tuples on tiny samples, with input alpha
    ranging from zero through gross (not presumed entirely source money).
    """
    conversions = [e for e in graph["events"] if e["kind"] == "conversion"]
    if not conversions:
        return {"sample_id": graph["scenario_id"], "status": "NOT_APPLICABLE", "reason": "NO_CONNECTED_CONVERSION"}
    if len(conversions) != 1:
        raise ValueError("This semantic component check requires exactly one conversion")
    conversion = conversions[0]
    gross, refund, net = map(int, (conversion["gross_raw"], conversion.get("refund_raw", 0), conversion["output_raw"]))
    if gross - refund != net:
        raise ValueError("Only net 1:1 semantics are certified by this report")
    entries = sorted({entry for group, names in graph["objective_groups"].items() if group.endswith("|WETH") for entry in names})
    capacity = {}
    for event in graph["events"]:
        if event["kind"] == "transfer":
            capacity[event["id"]] = int(event["amount_raw"])
        if event["kind"] == "multioutput":
            capacity.update({event["id"] + f":output{i}": int(output["amount_raw"]) for i, output in enumerate(event["outputs"])})
    if sum(capacity[e] for e in entries) != net:
        raise ValueError("Semantic check requires a fully closed WETH component")
    directions = []
    vectors = [[1] * len(entries)]
    vectors.extend([[2 if i == j else 1 for i in range(len(entries))] for j in range(len(entries))])
    if len(entries) == 2:
        vectors.extend([[1, 2], [2, -1]])
    allocations = enumerate_integer_allocations(graph)
    for weights in vectors:
        mapping = dict(zip(entries, weights))
        result = oracle_weighted_interval(graph, mapping)
        brute = [sum(a[e] * w for e, w in mapping.items()) for a in allocations]
        directions.append({"weights": mapping, "flow_lower_raw": result["lower_raw"], "flow_upper_raw": result["upper_raw"],
                           "enumeration_lower_raw": str(min(brute)), "enumeration_upper_raw": str(max(brute)),
                           "status": "PASS" if (int(result["lower_raw"]), int(result["upper_raw"])) == (min(brute), max(brute)) else "FAIL"})
    full_boundary = None
    if graph.get("index", 99) < 2:
        raw_relation, folded_relation = set(), set()
        # Raw primitive junction constraints, with separate deposit and mint.
        for alpha, r, deposit, minted in product(range(gross + 1), range(refund + 1), range(net + 1), range(net + 1)):
            if alpha != r + deposit or deposit != minted:
                continue
            for shares in product(*(range(capacity[e] + 1) for e in entries)):
                if sum(shares) == minted:
                    raw_relation.add((alpha, r, *shares))
        # Collapsed independent formula; no internal deposit/mint variables.
        for alpha, r in product(range(gross + 1), range(refund + 1)):
            for shares in product(*(range(capacity[e] + 1) for e in entries)):
                if 0 <= alpha - r <= net and sum(shares) == alpha - r:
                    folded_relation.add((alpha, r, *shares))
        full_boundary = {"columns": ["synthetic_input_source_alpha", "refund_source", *entries],
                         "alpha_domain": [0, gross], "raw_boundary_count": len(raw_relation), "folded_boundary_count": len(folded_relation),
                         "raw_boundary_tuples": sorted(raw_relation), "folded_boundary_tuples": sorted(folded_relation),
                         "status": "PASS" if raw_relation == folded_relation else "FAIL"}
    status = "PASS" if all(d["status"] == "PASS" for d in directions) and (full_boundary is None or full_boundary["status"] == "PASS") else "FAIL"
    return {"sample_id": graph["scenario_id"], "status": status, "scope": "SYNTHETIC_CLOSED_1TO1_COMPONENT",
            "raw_equations": ["gross_source = refund_source + deposit_source", "minted_source = deposit_source", "sum(output_source) = minted_source"],
            "folded_equation": "sum(output_source) = gross_source - refund_source",
            "continuous_bijection": {"forward": "(g,r,d,m,y) -> (g,r,y)", "inverse": "d=m=sum(y)=g-r", "validity": "All nonnegative real amounts with the same gross/refund/net/output capacities; no discretization needed for the algebraic bijection."},
            "full_tiny_boundary_relation": full_boundary, "whole_query_weighted_directions": directions,
            "refund_note": "Any refund is a synthetic gross/refund envelope around canonical net WETH deposit, not a claim that WETH9.deposit refunds.",
            "not_real_case_source_truth": True}
