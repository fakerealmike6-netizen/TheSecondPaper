"""Portable controlled verification and fixed-graph LP CLI (no network)."""
import argparse
import copy
import csv
from datetime import datetime, timezone
from fractions import Fraction as F
import hashlib
import json
from pathlib import Path
import platform

from lp_model import build_model, solve_interval, solve_target_groups
from lp_oracle import enumerate_oracle


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, data):
    path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")


def write_csv(path, rows):
    with path.open("w",encoding="utf-8",newline="") as handle:
        writer=csv.DictWriter(handle,fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)


def remove_shared_constraint_negative_control(model):
    """Deliberately wrong model, isolated in verification runner only."""
    bad=copy.deepcopy(model)
    bad._solver_cache=None
    selected=[i for i,name in enumerate(bad.eq_names) if name.endswith(":shared_capacity")]
    if len(selected)!=1:
        raise ValueError("Negative control must remove exactly one named shared coupling")
    i=selected[0]
    for values in (bad.eq,bad.rhs,bad.eq_names):
        del values[i]
    bad.metadata["negative_control_only"]="DELIBERATELY_REMOVED_MULTIOUTPUT_SHARED_CAPACITY"
    return bad


def controlled_verification(fixtures_dir, output_dir):
    import scipy
    import numpy
    output_dir.mkdir(parents=True,exist_ok=True)
    verification={"schema_version":"stage1b-lp-verification-1.0","created_at_utc":datetime.now(timezone.utc).isoformat(),
        "execution_scope":"OFFLINE_SYNTHETIC_CONTROLS_ONLY","model_status":"COMPLETED",
        "runtime":{"python":platform.python_version(),"scipy":scipy.__version__,"numpy":numpy.__version__,"solver":"HiGHS via scipy.optimize.linprog"},
        "oracle_independent_of_lp_builder_and_solver":True,"hidden_allocations_passed_to_solver":False,
        "scenario_count":0,"objective_comparisons":0,"all_passed":True,"scenarios":[],"variants":{},
        "max_exact_endpoint_error_raw":"0","real_provider_validation_claimed":False}
    oracle_rows=[]; interval_rows=[]; models=[]; hashes=[]
    for entry in read(fixtures_dir/"index.json"):
        sid=entry["scenario_id"]; folder=fixtures_dir/sid
        for name in ("graph.json","oracle_spec.json","hidden_allocation.json"):
            file=folder/name
            hashes.append({"path":file.relative_to(fixtures_dir).as_posix(),"sha256":hashlib.sha256(file.read_bytes()).hexdigest(),"bytes":file.stat().st_size})
        graph=read(folder/"graph.json")
        model=build_model(graph)
        # Only graph reaches build/solve; oracle and hidden data are opened by
        # this verifier after model construction, and are never solver inputs.
        result=solve_target_groups(model,graph["objective_groups"])
        reference=enumerate_oracle(read(folder/"oracle_spec.json"))
        hidden=read(folder/"hidden_allocation.json")["event_source_amounts_raw"]
        hidden_vector=model.lift_hidden_allocation_for_audit(hidden)
        audit=model.audit_vector(hidden_vector)
        comparisons=[]
        for name,expected in reference["intervals"].items():
            got=result[name]["joint"]
            match=got["status"]=="OPTIMAL_EXACT_CERTIFIED" and all(F(got[k])==F(expected[k]) for k in ("lower_raw","upper_raw"))
            if got["lower_raw"] is not None and got["upper_raw"] is not None:
                error=max(abs(F(got[k])-F(expected[k])) for k in ("lower_raw","upper_raw"))
                verification["max_exact_endpoint_error_raw"]=str(max(F(verification["max_exact_endpoint_error_raw"]),error))
            hidden_value=sum((F(hidden[event]) for event in graph["objective_groups"][name]),F(0))
            hidden_covered=got["lower_raw"] is not None and got["upper_raw"] is not None and F(got["lower_raw"])<=hidden_value<=F(got["upper_raw"])
            comparisons.append({"objective":name,"oracle_match":match,"hidden_allocation_covered":hidden_covered,"hidden_objective_raw":str(hidden_value)})
            oracle_rows.append({"scenario_id":sid,"objective":name,"asset":got["asset"],"lower_raw":expected["lower_raw"],"upper_raw":expected["upper_raw"],"oracle_method":reference["method"],"vertex_count":reference["vertex_count"]})
            interval_rows.append({"scenario_id":sid,"objective":name,"asset":got["asset"],"lower_raw":got["lower_raw"],"upper_raw":got["upper_raw"],"status":got["status"],"scope":"SYNTHETIC_CONTROLLED","balance_status":model.metadata["balance_status"],"positive_support":got["positive_support"],"oracle_match":match})
        passed=audit["exact_feasible"] and all(c["oracle_match"] and c["hidden_allocation_covered"] for c in comparisons)
        verification["scenarios"].append({"scenario_id":sid,"passed":passed,"comparisons":comparisons,"hidden_exact_feasibility":audit,"oracle":reference,"results":result})
        verification["scenario_count"]+=1;verification["objective_comparisons"]+=len(comparisons)
        verification["all_passed"] &= passed
        models.append(model.statistics())
    # Variant of scenario11: same equalities, only supported actual balance
    # caps removed. This is a structural inclusion proof, not sampling points.
    g=read(fixtures_dir/"11_missing_balance_relaxation"/"graph.json")
    complete=build_model(g);g["initial_balances"]["A|ETH"]=None
    relaxed=build_model(g)
    structural=complete.eq==relaxed.eq and complete.rhs==relaxed.rhs and len(complete.variables)==len(relaxed.variables) and all(a.name==b.name and a.scale==b.scale and a.lower>=b.lower and a.upper<=b.upper for a,b in zip(complete.variables,relaxed.variables))
    full=solve_interval(complete,["enter"]);wide=solve_interval(relaxed,["enter"])
    inclusion_pass=structural and wide["lower_raw"]=="0" and wide["upper_raw"]=="50" and full["lower_raw"]=="20"
    verification["variants"]["missing_balance"]={"passed":inclusion_pass,"full_interval":full,"relaxed_interval":wide,
        "structural_feasible_set_inclusion_proved":structural,"proof":"Identical variables/scales/equalities. Every relaxed lower bound is no higher and every relaxed upper bound is no lower. No new source variable is introduced.",
        "source_capacity_preserved_raw":"50"}
    write_json(output_dir/"lp_missing_balance_relaxed_graph.json",g)
    # Exact one raw unit, even alongside a10^18 normal bankroll. No small
    # amount threshold exists in construction, solving or support decisions.
    one=read(fixtures_dir/"01_single_path"/"graph.json")
    one["events"][0]["amount_raw"]="1";one["events"][1]["amount_raw"]="1"
    one["initial_balances"]["A|ETH"]="1000000000000000000"
    one_result=solve_interval(build_model(one),["enter"])
    one_pass=one_result["lower_raw"]=="0" and one_result["upper_raw"]=="1" and one_result["positive_support"]=="CERTIFIED_POSITIVE"
    verification["variants"]["one_raw_unit"]={"passed":one_pass,"analytic_oracle_interval":["0","1"],"result":one_result}
    write_json(output_dir/"lp_one_raw_unit_graph.json",one)
    # Keep correct and deliberately decoupled models side by side.
    mg=read(fixtures_dir/"12_multioutput_coupling_negative"/"graph.json")
    correct=build_model(mg);wrong=remove_shared_constraint_negative_control(correct)
    right=solve_target_groups(correct,mg["objective_groups"]);bad=solve_target_groups(wrong,mg["objective_groups"])
    same_individual=all(right[name]["joint"][key]==bad[name]["joint"][key] for name in ("T","U") for key in ("lower_raw","upper_raw"))
    detected=same_individual and right["T_plus_U"]["joint"]["upper_raw"]=="100" and bad["T_plus_U"]["joint"]["upper_raw"]=="200"
    verification["variants"]["wrong_multioutput_negative_control"]={"passed":detected,"individual_endpoints_unchanged":same_individual,
        "correct_joint":right["T_plus_U"]["joint"],"deliberately_wrong_joint":bad["T_plus_U"]["joint"],
        "analytic_oracle_wrong_joint":["0","200"],"interpretation":"The wrong square has the same one-dimensional output projections but a different joint feasible set; this validates the necessity of coupling, not arbitrary DSU equivalence."}
    verification["all_passed"] &= inclusion_pass and one_pass and detected
    verification["model_status"]="COMPLETED" if verification["all_passed"] else "PARTIAL"
    write_json(output_dir/"lp_verification_results.json",verification)
    write_json(output_dir/"lp_model_statistics.json",models)
    write_json(output_dir/"lp_fixture_hashes.json",hashes)
    write_csv(output_dir/"oracle_expected.csv",oracle_rows)
    write_csv(output_dir/"amount_intervals_controlled.csv",interval_rows)
    print(json.dumps({k:verification[k] for k in ("model_status","scenario_count","objective_comparisons","all_passed","max_exact_endpoint_error_raw")},indent=2))
    return verification


def fixed_graph_run(graph_path, output_dir):
    output_dir.mkdir(parents=True,exist_ok=True)
    graph=read(graph_path);model=build_model(graph)
    result={"graph_file_sha256":hashlib.sha256(graph_path.read_bytes()).hexdigest(),"model":model.statistics(),
            "scope":graph.get("scope","ASSUMPTION_CONDITIONAL"),"assumptions":graph.get("assumptions",[]),
            "results":solve_target_groups(model,graph.get("objective_groups",{}))}
    write_json(output_dir/"lp_fixed_graph_result.json",result)
    print(json.dumps({"output":"lp_fixed_graph_result.json","model":model.statistics(),"target_groups":len(result["results"])},indent=2))


if __name__=="__main__":
    p=argparse.ArgumentParser()
    mode=p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--fixtures",type=Path);mode.add_argument("--graph",type=Path)
    p.add_argument("--output",type=Path,required=True)
    args=p.parse_args()
    if args.fixtures:
        result=controlled_verification(args.fixtures,args.output)
        raise SystemExit(0 if result["all_passed"] else 1)
    fixed_graph_run(args.graph,args.output)
