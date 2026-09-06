"""Generate the twelve small, entirely synthetic Stage1B controls.

graph.json, oracle_spec.json and hidden_allocation.json are separate files.
The LP input contains neither oracle inequalities nor hidden source allocation.
"""
import json
from pathlib import Path


def seed(n, to="A", asset="ETH", order=1, eid="seed"):
    return dict(id=eid, kind="seed", order=order, to=to, asset=asset, amount_raw=str(n))


def transfer(eid, n, sender, recipient, order, asset="ETH"):
    return dict(id=eid, kind="transfer", order=order, **{"from": sender}, to=recipient, asset=asset, amount_raw=str(n))


def oracle(variables, inequalities, objectives, derivation):
    return {"variables": variables,
            "inequalities": [{"a": list(map(str, a)), "b": str(b)} for a, b in inequalities],
            "objectives": {name: {"a": list(map(str, a)), "constant": str(c)} for name, (a, c) in objectives.items()},
            "derivation": derivation,
            "semantics": "Bounded continuous rational polytope; extrema occur at vertices. No integer discretization."}


def assemble(sid, title, events, groups, spec, hidden, initial=None, targets=None):
    balances = {}
    for e in events:
        if "from" in e:
            balances[e["from"] + "|" + e["asset"]] = "0"
        if "to" in e:
            asset = e.get("output_asset", e["asset"]) if e["kind"] == "conversion" else e["asset"]
            balances[e["to"] + "|" + asset] = "0"
        if e["kind"] == "multioutput":
            for output in e["outputs"]:
                balances[output["to"] + "|" + e["asset"]] = "0"
    balances.update(initial or {})
    return {"id": sid, "graph": {"schema_version": "stage1b-controlled-1.0", "scenario_id": sid, "title": title,
                                   "provenance": "ORIGINAL_SYNTHETIC_FIXTURE", "initial_balances": balances,
                                   "events": events, "target_accounts": targets or ["T", "U"], "objective_groups": groups},
            "oracle": spec, "hidden": {"scenario_id": sid, "scope": "HELD_OUT_FROM_MAIN_SOLVER",
                                         "event_source_amounts_raw": {k: str(v) for k, v in hidden.items()}}}


def fixtures():
    out = []
    out.append(assemble("01_single_path", "Certain path", [seed(10), transfer("enter", 10, "A", "T", 2)],
        {"T": ["enter"]}, oracle([], [], {"T": ([], 10)}, "No normal balance and zero source residue imply target=seed=10."),
        {"seed": 10, "enter": 10}))
    out.append(assemble("02_normal_mixture", "Normal 100 plus seed 50 pays 120", [seed(50), transfer("enter", 120, "A", "T", 2)],
        {"T": ["enter"]}, oracle(["t"], [([-1], -20), ([1], 50)], {"T": ([1], 0)},
        "Final actual balance is 30. Source conservation: residue=50-t in [0,30], hence t in [20,50]."),
        {"seed": 50, "enter": 35}, {"A|ETH": "100"}))
    events = [seed(10), transfer("a_b", 6, "A", "B", 2), transfer("a_c", 4, "A", "C", 3),
              transfer("b_d", 4, "B", "D", 4), transfer("c_d", 4, "C", "D", 5),
              transfer("d_t", 8, "D", "T", 6), transfer("b_u", 2, "B", "U", 7)]
    out.append(assemble("03_split_merge", "Split and merge preserve a single source capacity", events,
        {"T": ["d_t"], "U": ["b_u"], "T_plus_U": ["d_t", "b_u"]},
        oracle([], [], {"T": ([], 8), "U": ([], 2), "T_plus_U": ([], 10)},
               "Every account initially has zero normal funds. Conservation fixes all flows at their actual amounts: 8+2=10."),
        {"seed": 10, "a_b": 6, "a_c": 4, "b_d": 4, "c_d": 4, "d_t": 8, "b_u": 2}))
    out.append(assemble("04_shared_targets", "Independent upper bounds are not simultaneous", [seed(100), transfer("t", 100, "A", "T", 2), transfer("u", 100, "A", "U", 3)],
        {"T": ["t"], "U": ["u"], "T_plus_U": ["t", "u"]},
        oracle(["t"], [([-1], 0), ([1], 100)], {"T": ([1], 0), "U": ([-1], 100), "T_plus_U": ([0], 100)},
               "Normal100+seed100 is fully paid to two targets: t+u=100 with each in [0,100]. The sum of separate maxima is 200; joint maximum is 100."),
        {"seed": 100, "t": 40, "u": 60}, {"A|ETH": "100"}))
    out.append(assemble("05_repeated_target", "Multiple entries at one address require a joint objective", [seed(100), transfer("first", 100, "A", "T", 2), transfer("second", 100, "A", "T", 3)],
        {"first": ["first"], "second": ["second"], "T_all_entries": ["first", "second"]},
        oracle(["first"], [([-1], 0), ([1], 100)], {"first": ([1], 0), "second": ([-1], 100), "T_all_entries": ([0], 100)},
               "The same receiving address has two payments. Final source residue is zero, so first+second=100, not their independent maxima sum."),
        {"seed": 100, "first": 40, "second": 60}, {"A|ETH": "100"}))
    future = dict(id="future_normal", kind="normal_incoming", order=4, to="A", asset="ETH", amount_raw="100")
    out.append(assemble("06_strict_time", "A later seed or normal inflow cannot explain earlier output",
        [transfer("earlier", 5, "A", "T", 1), seed(10, order=2), transfer("later", 10, "A", "T", 3), future, transfer("future_out", 100, "A", "U", 5)],
        {"earlier": ["earlier"], "later": ["later"], "future_out": ["future_out"], "T": ["earlier", "later"]},
        oracle([], [], {"earlier": ([], 0), "later": ([], 10), "future_out": ([], 0), "T": ([], 10)},
               "Before seed source=0. Before future normal inflow the sender drains the seeded10 completely, leaving no source for the later normal100 payment."),
        {"earlier": 0, "seed": 10, "later": 10, "future_normal": 0, "future_out": 0}, {"A|ETH": "5"}))
    gas1 = dict(id="gas_success", kind="gas", order=2, **{"from": "A"}, asset="ETH", amount_raw="2", transaction_status="SUCCESS")
    gas2 = dict(id="gas_failed", kind="gas", order=3, **{"from": "A"}, asset="ETH", amount_raw="3", transaction_status="FAILED")
    out.append(assemble("07_native_gas", "Successful and failed transaction gas have uncertain source shares", [seed(10), gas1, gas2, transfer("enter", 15, "A", "T", 4)],
        {"T": ["enter"], "success_gas": ["gas_success"], "failed_gas": ["gas_failed"], "all_gas": ["gas_success", "gas_failed"]},
        oracle(["g1", "g2"], [([-1,0],0), ([1,0],2), ([0,-1],0), ([0,1],3)],
               {"T": ([-1,-1],10), "success_gas": ([1,0],0), "failed_gas": ([0,1],0), "all_gas": ([1,1],0)},
               "0<=g1<=2, 0<=g2<=3. The sender drains its20 actual funds as gas5 and target15, so target source=10-g1-g2."),
        {"seed": 10, "gas_success": 1, "gas_failed": 2, "enter": 7}, {"A|ETH": "10"}))
    gas = dict(id="eth_gas", kind="gas", order=2, **{"from": "A"}, asset="ETH", amount_raw="10", transaction_status="FAILED")
    out.append(assemble("08_erc20_gas_separation", "Token source is not charged native ETH gas", [seed(50, asset="TOK"), gas, transfer("enter",120,"A","T",3,"TOK")],
        {"T_TOK": ["enter"], "ETH_gas": ["eth_gas"]},
        oracle(["t"], [([-1],-20),([1],50)], {"T_TOK": ([1],0), "ETH_gas": ([0],0)},
               "The token account has normal100+seed50 and pays120: t in [20,50]. The independent ETH account contains no seeded ETH, so source gas is0."),
        {"seed": 50, "eth_gas": 0, "enter": 35}, {"A|TOK": "100", "A|ETH": "10"}))
    weth = dict(id="wrap",kind="conversion",order=2,**{"from":"A"},to="A",asset="ETH",output_asset="WETH",
                gross_raw="120",refund_raw="0",output_raw="120",certified_semantics="SYNTHETIC_CONTROLLED",protocol="Canonical_WETH9_deposit")
    out.append(assemble("09_weth_one_to_one", "Closed WETH deposit ports preserve1:1 source amount", [seed(50),weth,transfer("enter",120,"A","T",3,"WETH")],
        {"ETH_input": ["wrap:input"], "WETH_output": ["wrap:output"], "T_WETH": ["enter"]},
        oracle(["x"], [([-1],-20),([1],50)], {"ETH_input": ([1],0), "WETH_output": ([1],0), "T_WETH": ([1],0)},
               "ETH actual residue30 forces input source x>=20; x<=seed50. The closed wrap has zero refund and one output at1:1, then the WETH account drains to target."),
        {"seed":50,"wrap:input":35,"wrap:refund":0,"wrap:net":35,"wrap:output":35,"enter":35}, {"A|ETH":"100"}))
    swap = dict(id="swap",kind="conversion",order=2,**{"from":"A"},to="A",asset="ETH",output_asset="TOK",
                gross_raw="4",refund_raw="1",output_raw="2",certified_semantics="SYNTHETIC_CONTROLLED",protocol="CONTROLLED_FIXED_RATIO_ONLY")
    out.append(assemble("10_fractional_swap_refund", "Fixed2/3 ratio with an explicit refundable input port", [seed(3),swap,transfer("enter",2,"A","T",3,"TOK")],
        {"gross_source": ["swap:input"], "net_source": ["swap:net"], "refund_source": ["swap:refund"], "T_TOK": ["enter"]},
        oracle(["v","r"], [([-1,0],-2),([1,0],3),([0,-1],0),([0,1],1),([1,1],3),([-1,-1],-3)],
               {"gross_source":([1,1],0),"net_source":([1,0],0),"refund_source":([0,1],0),"T_TOK":(["2/3",0],0)},
               "Seed3+normal1 pays its entire actual balance4 before the refund, so gross source is exactly3. Net source v leaves source3-v in actual ETH residue1, hence2<=v<=3. Refund r in[0,1] and v+r=3; token source=(2/3)v. Exact target interval[4/3,2], gross[3,3]. No integer grid. The pre-refund residue equality corrects an initially omitted physical ordering constraint; preserved in lp_oracle_correction.json."),
        {"seed":3,"swap:input":3,"swap:refund":"1/2","swap:net":"5/2","swap:output":"5/3","enter":"5/3"}, {"A|ETH":"1"}))
    out.append(assemble("11_missing_balance_relaxation", "Missing initial balance removes bounds without new source", [seed(50),transfer("enter",120,"A","T",2)],
        {"T": ["enter"]}, oracle(["t"],[([-1],-20),([1],50)],{"T":([1],0)},
        "Complete case: residue=50-t<=30, so20<=t<=50. Relaxed case deletes the supported actual residue bound only, leaving0<=t<=50. Every complete vector remains feasible and seed supply remains50."),
        {"seed":50,"enter":30}, {"A|ETH":"100"}))
    split = dict(id="split",kind="multioutput",order=2,**{"from":"A"},asset="ETH",gross_raw="200",
                 outputs=[{"to":"T","amount_raw":"100"},{"to":"U","amount_raw":"100"}],certified_semantics="SYNTHETIC_CONTROLLED")
    out.append(assemble("12_multioutput_coupling_negative", "Removing shared output coupling must change the joint feasible set", [seed(100),split],
        {"T":["split:output0"],"U":["split:output1"],"T_plus_U":["split:output0","split:output1"]},
        oracle(["t"],[([-1],0),([1],100)],{"T":([1],0),"U":([-1],100),"T_plus_U":([0],100)},
               "The input drains seed100 plus normal100. Source input is100 and shared coupling t+u=100. Dropping this row leaves the wrong square[0,100]^2: individual extrema match but joint upper doubles to200."),
        {"seed":100,"split:input":100,"split:output0":40,"split:output1":60},{"A|ETH":"100"}))
    return out


def generate(destination: Path):
    destination.mkdir(parents=True, exist_ok=True)
    index = []
    for item in fixtures():
        folder = destination / item["id"]
        folder.mkdir(exist_ok=True)
        for name, value in (("graph.json",item["graph"]),("oracle_spec.json",item["oracle"]),("hidden_allocation.json",item["hidden"])):
            (folder / name).write_text(json.dumps(value,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
        index.append({"scenario_id":item["id"],"title":item["graph"]["title"],"scope":"SYNTHETIC_CONTROLLED"})
    (destination / "index.json").write_text(json.dumps(index,indent=2)+"\n",encoding="utf-8")


if __name__ == "__main__":
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument("destination",type=Path)
    generate(parser.parse_args().destination)
