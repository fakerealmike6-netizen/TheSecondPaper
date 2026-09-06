"""Independent audit of the four frozen Stage1B native fixed-graph results.

Standard library only. Does NOT import the delivered LP builder, its Oracle,
SciPy, NumPy, or any optimizer. No network and no frozen-artifact mutations.

The proof applies only to seed/transfer ETH graphs with unknown actual initial
balances and terminal targets. Such models are time-expanded capacity networks:
continuous max-flow equals an integral optimum when every capacity is integer.
A residual reachable cut certifies the maximum; the minimum is zero because all
source may remain at the seed recipient under the declared unknown-balance
bounds. Neither fact establishes completeness of real blockchain data.
"""

import argparse
from collections import defaultdict, deque
from datetime import datetime, timezone
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import platform
import sys


CASES = (
    ("cache_probe", "atomic_simple_transfer"),
    ("cache_probe", "harmony_high_branch"),
    ("dune_live_replay", "atomic_simple_transfer"),
    ("dune_live_replay", "harmony_high_branch"),
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def checked_events(graph):
    require(graph.get("scope") not in {"ORDER_UNRESOLVED_MODEL_NOT_SOLVABLE", "FACT_CONFLICT_MODEL_NOT_SOLVABLE"},
            "Unresolved order is outside this audit's supported graph scope")
    events = graph["events"]
    require(all(type(e["order"]) is int for e in events), "Integer ordinals required")
    require(len({e["order"] for e in events}) == len(events), "Duplicate ordinal")
    require(len({e["id"] for e in events}) == len(events), "Duplicate physical identity")
    require(sum(e["kind"] == "seed" for e in events) == 1, "Exactly one seed required")
    require(all(e["kind"] in {"seed", "transfer"} and e["asset"] == "ETH"
                for e in events), "Only native seed/transfer models supported")
    require(graph["initial_balances"] and
            all(v is None for v in graph["initial_balances"].values()),
            "Proof only covers unknown initial actual balance bounds")
    require(all(not e.get("balance_anchors_after") and not e.get("context_only")
                and not e.get("reverted") for e in events),
            "Anchors/context/reverted events outside audit scope")
    targets = set(graph["target_accounts"])
    require(all(e["kind"] == "seed" or e["from"] not in targets for e in events),
            "Service targets must remain terminal")
    for event in events:
        value = Fraction(event["amount_raw"])
        require(value >= 0 and value.denominator == 1, "Raw physical amount must be integer")
    return sorted(events, key=lambda e: e["order"])


def exact_maximum(events, selected):
    """Edmonds-Karp with integer capacities, plus an exact residual cut check."""
    seed = next(e for e in events if e["kind"] == "seed")
    supply = int(seed["amount_raw"])
    require(supply > 0, "Positive seed required")
    residual = defaultdict(dict)
    original = defaultdict(dict)

    def edge(a, b, capacity):
        residual[a][b] = residual[a].get(b, 0) + capacity
        residual[b].setdefault(a, 0)
        original[a][b] = original[a].get(b, 0) + capacity

    accounts = {e["to"] for e in events} | {
        e["from"] for e in events if e["kind"] == "transfer"
    }
    for time, event in enumerate(events):
        for account in sorted(accounts):
            edge(("account", account, time), ("account", account, time + 1), supply)
        if event["kind"] == "seed":
            edge("SOURCE", ("account", event["to"], time + 1), supply)
        else:
            amount = int(event["amount_raw"])
            node = ("event", event["id"])
            # One shared physical capacity, even when this event is an objective.
            edge(("account", event["from"], time), node, amount)
            edge(node, ("account", event["to"], time + 1), amount)
            if event["id"] in selected:
                edge(node, "SINK", amount)

    value = 0
    while True:
        parent = {"SOURCE": None}
        queue = deque(["SOURCE"])
        while queue and "SINK" not in parent:
            node = queue.popleft()
            for other, capacity in residual[node].items():
                if capacity > 0 and other not in parent:
                    parent[other] = node
                    queue.append(other)
        if "SINK" not in parent:
            break
        node, amount = "SINK", supply
        while parent[node] is not None:
            previous = parent[node]
            amount = min(amount, residual[previous][node])
            node = previous
        node = "SINK"
        while parent[node] is not None:
            previous = parent[node]
            residual[previous][node] -= amount
            residual[node][previous] += amount
            node = previous
        value += amount

    # BFS exhausted all positive residual arcs on the final iteration.
    reachable = set(parent)
    cut = sum(capacity for node, outgoing in original.items() if node in reachable
              for other, capacity in outgoing.items() if other not in reachable)
    require(cut == value, "Independent integer maximum and cut do not agree")
    return {"maximum_raw": str(value), "minimum_cut_capacity_raw": str(cut),
            "max_flow_equals_cut": True, "reachable_cut_nodes": len(reachable)}


def check_primal_witness(events, endpoint, selected):
    """Replay physical source amounts without any LP construction helpers."""
    source = endpoint["witness_event_source_raw"]
    require(set(source) == {e["id"] for e in events}, "Witness physical identities differ")
    balances = defaultdict(Fraction)
    supply = Fraction(next(e["amount_raw"] for e in events if e["kind"] == "seed"))
    injected = Fraction(0)
    for event in events:
        amount = Fraction(source[event["id"]])
        require(0 <= amount <= Fraction(event["amount_raw"]), "Physical capacity violated")
        if event["kind"] == "seed":
            require(amount == supply, "Seed injection not exact")
            injected += amount
        else:
            require(amount <= balances[event["from"]], "Future source funds prior debit")
            balances[event["from"]] -= amount
        balances[event["to"]] += amount
        require(all(0 <= balance <= supply for balance in balances.values()),
                "Negative or excess source state")
        require(sum(balances.values()) == injected, "Source not conserved")
    objective = sum((Fraction(source[event]) for event in selected), Fraction(0))
    require(objective == Fraction(endpoint["raw"]), "Stored witness objective differs")
    return {"physical_capacity": True, "causal_debit_availability": True,
            "source_conservation": True, "objective_raw": str(objective)}


def audit_case(root, scope, pilot, derived_subdir="derived/bugfix_only_same_input"):
    folder = (root / derived_subdir / scope / pilot).resolve()
    require(folder.is_relative_to(root.resolve()), "Graph input path escapes root")
    graph_path, result_path = folder / "fixed_graph.json", folder / "lp/lp_fixed_graph_result.json"
    for path in (graph_path, result_path):
        require(path.resolve().is_relative_to(root.resolve()) and not path.is_symlink(), "Graph or result file escapes root")
    graph, saved = read_json(graph_path), read_json(result_path)
    events = checked_events(graph)
    by_id = {e["id"]: e for e in events}
    require(saved["graph_file_sha256"] == sha256(graph_path), "Result names different graph bytes")
    require(set(saved["results"]) == set(graph["objective_groups"]), "Target groups differ")
    comparisons = []
    for group, stored in saved["results"].items():
        for objective, interval in [("joint", stored["joint"])] + list(stored["entry_intervals"].items()):
            selected_list = interval.get("objective_events", [objective])
            require(len(selected_list) == len(set(selected_list)), "Duplicate objective capacity")
            selected = set(selected_list)
            if objective == "joint":
                require(selected == set(graph["objective_groups"][group]), "Wrong joint objective")
            require(selected and all(e in by_id and by_id[e]["kind"] == "transfer"
                                     and by_id[e]["to"] in graph["target_accounts"] for e in selected),
                    "Objective must select observed terminal transfer entries")
            maximum = exact_maximum(events, selected)
            require(Fraction(interval["upper_raw"]) == Fraction(maximum["maximum_raw"]),
                    "Stored upper bound disagrees with independent maximum flow")
            require(Fraction(interval["lower_raw"]) == 0, "Stored lower bound disagrees with zero witness")
            witnesses = {side: check_primal_witness(events, interval["endpoints"][side], selected)
                         for side in ("lower", "upper")}
            comparisons.append({"group": group, "objective": objective,
                                "objective_events": sorted(selected),
                                "stored_lower_raw": interval["lower_raw"],
                                "stored_upper_raw": interval["upper_raw"],
                                "independent_maximum": maximum, "primal_witness_checks": witnesses,
                                "passed": True})
    return {"scope": scope, "pilot": pilot, "events": len(events),
            "target_groups": len(saved["results"]),
            "status": "PASSED" if comparisons else "NO_OBSERVED_TARGET_NO_INTERVAL_CLAIM",
            "graph_path": graph_path.relative_to(root.resolve()).as_posix(), "graph_sha256": sha256(graph_path),
            "result_path": result_path.relative_to(root.resolve()).as_posix(), "result_sha256": sha256(result_path),
            "intervals_compared": len(comparisons), "primal_witnesses_checked": 2 * len(comparisons),
            "comparisons": comparisons}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="Frozen review extraction root containing derived/")
    parser.add_argument("--output", type=Path, required=True, help="New audit result path; frozen inputs are read-only")
    parser.add_argument("--derived-subdir", default="derived/bugfix_only_same_input", help="Relative in-root parent of cache_probe and dune_live_replay")
    args = parser.parse_args()
    result = {"audit": "INDEPENDENT_NATIVE_FIXED_GRAPH_MAXFLOW_AND_PRIMAL_REPLAY",
              "created_at_utc": datetime.now(timezone.utc).isoformat(),
              "python": platform.python_version(), "script_sha256": sha256(Path(__file__)),
              "lp_oracle_scipy_numpy_imported": False, "network_requests": 0,
              "scope": "Declared fixed-graph mathematics only; not real-chain completeness or missing-flow conservatism",
              "minimum_zero_proof": "With all actual initial balance bounds unknown, keep the exact seed source at its recipient and assign zero source to every physical transfer. This remains within the declared model's source bounds.",
              "maximum_proof": "Integer time-expanded max-flow equals the final residual cut capacity. Integer capacities give an integral optimum of this continuous network-flow model; no integer-grid approximation is used.",
              "cases": [], "failures": []}
    for scope, pilot in CASES:
        try:
            result["cases"].append(audit_case(args.root, scope, pilot, args.derived_subdir))
        except Exception as error:
            result["failures"].append({"scope": scope, "pilot": pilot,
                                       "error_type": type(error).__name__, "message": str(error)})
    result["intervals_compared"] = sum(case["intervals_compared"] for case in result["cases"])
    result["primal_witnesses_checked"] = sum(case["primal_witnesses_checked"] for case in result["cases"])
    result["passed"] = not result["failures"] and len(result["cases"]) == len(CASES)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: result[key] for key in ("passed", "intervals_compared", "primal_witnesses_checked", "failures")}, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
