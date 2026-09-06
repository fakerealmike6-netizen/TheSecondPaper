"""Offline synthetic LP tests; no provider credentials or network required."""
import copy
from fractions import Fraction as F
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE/"src"))
from lp_model import build_model, solve_interval, solve_target_groups, raw, Model, _certify
from lp_oracle import enumerate_oracle
from lp_run import remove_shared_constraint_negative_control


def fixture(sid):
    folder=BASE/"fixtures"/"controlled"/sid
    return json.loads((folder/"graph.json").read_text(encoding="utf-8"))


class LPTests(unittest.TestCase):
    def test_external_unresolved_order_marker_rejects_invented_ordinals(self):
        graph=fixture('01_single_path')
        graph['scope']='ORDER_UNRESOLVED_MODEL_NOT_SOLVABLE'
        with self.assertRaises(ValueError):build_model(graph)
    def test_all_twelve_scenarios_match_independent_exact_oracle(self):
        folders=sorted(p for p in (BASE/"fixtures"/"controlled").iterdir() if p.is_dir())
        self.assertEqual(len(folders),12)
        for folder in folders:
            with self.subTest(scenario=folder.name):
                graph=json.loads((folder/"graph.json").read_text())
                model=build_model(graph)
                results=solve_target_groups(model,graph["objective_groups"])
                oracle=enumerate_oracle(json.loads((folder/"oracle_spec.json").read_text()))
                for name,expected in oracle["intervals"].items():
                    got=results[name]["joint"]
                    self.assertEqual(got["status"],"OPTIMAL_EXACT_CERTIFIED")
                    for endpoint in ("lower_raw","upper_raw"):
                        self.assertEqual(F(got[endpoint]),F(expected[endpoint]))

    def test_hidden_allocations_feasible_and_not_solver_inputs(self):
        for folder in sorted(p for p in (BASE/"fixtures"/"controlled").iterdir() if p.is_dir()):
            graph=json.loads((folder/"graph.json").read_text())
            hidden=json.loads((folder/"hidden_allocation.json").read_text())["event_source_amounts_raw"]
            with patch("builtins.open",side_effect=AssertionError("Core LP must not read hidden/oracle files")):
                model=build_model(graph)
                result=solve_interval(model,next(iter(graph["objective_groups"].values())))
            self.assertEqual(result["status"],"OPTIMAL_EXACT_CERTIFIED")
            self.assertTrue(model.audit_vector(model.lift_hidden_allocation_for_audit(hidden))["exact_feasible"])

    def test_one_raw_unit_is_retained_next_to_huge_normal_balance(self):
        graph=fixture("01_single_path")
        for event in graph["events"]:
            event["amount_raw"]="1"
        graph["initial_balances"]["A|ETH"]="1000000000000000000"
        result=solve_interval(build_model(graph),["enter"])
        self.assertEqual((result["lower_raw"],result["upper_raw"]),("0","1"))
        self.assertEqual(result["positive_support"],"CERTIFIED_POSITIVE")

    def test_unresolved_numerics_never_report_zero(self):
        graph=fixture("01_single_path")
        graph["events"][0]["amount_raw"]="1000000000000000000"
        graph["events"][1]["amount_raw"]="1"
        result=solve_interval(build_model(graph),["enter"])
        # A float solver can miss this tiny scaled unit. Exact certificates
        # either recover it, or preserve an unknown state; never certify0.
        if result["upper_raw"] is not None:
            self.assertEqual(result["upper_raw"],"1")
        else:
            self.assertEqual(result["positive_support"],"NUMERICAL_OR_SOLVER_UNKNOWN")

    def test_large_raw_integer_active_bound_exact_recovery(self):
        graph={"scenario_id":"synthetic_large_integer_recovery","initial_balances":{"A|ETH":None,"B|ETH":None,"T|ETH":None},"target_accounts":["T"],
            "events":[{"id":"seed","kind":"seed","order":1,"to":"A","asset":"ETH","amount_raw":"123456789012345678901"},
                      {"id":"a_b","kind":"transfer","order":2,"from":"A","to":"B","asset":"ETH","amount_raw":"40000000000000000011"},
                      {"id":"b_t","kind":"transfer","order":3,"from":"B","to":"T","asset":"ETH","amount_raw":"30000000000000000009"},
                      {"id":"a_t","kind":"transfer","order":4,"from":"A","to":"T","asset":"ETH","amount_raw":"20000000000000000003"}]}
        result=solve_interval(build_model(graph),["b_t","a_t"])
        self.assertEqual((result["lower_raw"],result["upper_raw"]),("0","50000000000000000012"))
        self.assertEqual(result["endpoints"]["upper"]["certificate"]["primal_recovery"]["method"],"ACTIVE_BOUND_EXACT_LINEAR_SOLVE")
        self.assertTrue(result["endpoints"]["upper"]["certificate"]["certified"])

    def test_exact_primal_recovery_cannot_override_invalid_dual(self):
        from types import SimpleNamespace
        model=build_model(fixture("01_single_path"))
        vector=model.lift_hidden_allocation_for_audit({"seed":"10","enter":"10"})
        fake=SimpleNamespace(x=[float(x) for x in vector],
            eqlin=SimpleNamespace(marginals=[0]*len(model.eq)),
            lower=SimpleNamespace(marginals=[0]*len(model.variables)),
            upper=SimpleNamespace(marginals=[0]*len(model.variables)))
        cost=[F(0)]*len(model.variables);cost[model.event_variables["enter"]]=F(-1)
        certificate,_,_=_certify(model,fake,cost)
        self.assertTrue(certificate["exact_primal_feasible"])
        self.assertFalse(certificate["exact_dual_feasible"])
        self.assertFalse(certificate["certified"])

    def test_independent_target_upper_sum_is_not_joint_upper(self):
        graph=fixture("04_shared_targets");model=build_model(graph)
        results=solve_target_groups(model,graph["objective_groups"])
        self.assertEqual(int(results["T"]["joint"]["upper_raw"])+int(results["U"]["joint"]["upper_raw"]),200)
        self.assertEqual(results["T_plus_U"]["joint"]["upper_raw"],"100")

    def test_repeated_target_enters_share_source(self):
        graph=fixture("05_repeated_target");result=solve_interval(build_model(graph),["first","second"])
        self.assertEqual((result["lower_raw"],result["upper_raw"]),("100","100"))

    def test_shared_multioutput_negative_control_detects_joint_error(self):
        graph=fixture("12_multioutput_coupling_negative")
        correct=build_model(graph);wrong=remove_shared_constraint_negative_control(correct)
        for event in ("split:output0","split:output1"):
            a=solve_interval(correct,[event]);b=solve_interval(wrong,[event])
            self.assertEqual((a["lower_raw"],a["upper_raw"]),(b["lower_raw"],b["upper_raw"]))
        self.assertEqual(solve_interval(correct,["split:output0","split:output1"])["upper_raw"],"100")
        self.assertEqual(solve_interval(wrong,["split:output0","split:output1"])["upper_raw"],"200")

    def test_missing_balance_is_structural_conservative_relaxation(self):
        graph=fixture("11_missing_balance_relaxation");full=build_model(graph)
        graph["initial_balances"]["A|ETH"]=None;relaxed=build_model(graph)
        self.assertEqual(full.eq,relaxed.eq);self.assertEqual(full.rhs,relaxed.rhs)
        for a,b in zip(full.variables,relaxed.variables):
            self.assertEqual(a.name,b.name);self.assertEqual(a.scale,b.scale)
            self.assertLessEqual(b.lower,a.lower);self.assertGreaterEqual(b.upper,a.upper)
        result=solve_interval(relaxed,["enter"])
        self.assertEqual((result["lower_raw"],result["upper_raw"]),("0","50"))

    def test_source_before_seed_and_after_exhaustion_is_zero(self):
        model=build_model(fixture("06_strict_time"))
        for name in ("earlier","future_out"):
            self.assertEqual(solve_interval(model,[name])["positive_support"],"CERTIFIED_ZERO")

    def test_native_gas_including_failed_gas(self):
        model=build_model(fixture("07_native_gas"))
        self.assertEqual(solve_interval(model,["gas_failed"])["upper_raw"],"3")
        self.assertEqual(solve_interval(model,["enter"])["lower_raw"],"5")

    def test_erc20_source_does_not_pay_eth_gas(self):
        model=build_model(fixture("08_erc20_gas_separation"))
        self.assertEqual(solve_interval(model,["eth_gas"])["upper_raw"],"0")
        self.assertEqual(solve_interval(model,["enter"])["lower_raw"],"20")

    def test_weth_boundary_input_output_equal(self):
        model=build_model(fixture("09_weth_one_to_one"))
        inside=solve_interval(model,["wrap:input"]);outside=solve_interval(model,["wrap:output"])
        for key in ("lower_raw","upper_raw"):
            self.assertEqual(inside[key],outside[key])

    def test_fractional_ratio_and_refund_are_exact(self):
        model=build_model(fixture("10_fractional_swap_refund"))
        self.assertEqual(solve_interval(model,["enter"])["lower_raw"],"4/3")
        self.assertEqual(solve_interval(model,["swap:refund"])["upper_raw"],"1")

    def test_gross_source_precedes_refund(self):
        model=build_model(fixture("10_fractional_swap_refund"))
        gross=solve_interval(model,["swap:input"])
        self.assertEqual((gross["lower_raw"],gross["upper_raw"]),("3","3"))

    def test_refund_cannot_borrow_exhausted_source(self):
        graph=fixture("01_single_path")
        graph["events"][1]["amount_raw"]="8"
        graph["events"].append({"id":"normal","kind":"normal_incoming","order":3,"to":"A","asset":"ETH","amount_raw":"100"})
        graph["events"].append({"id":"wrap","kind":"conversion","order":4,"from":"A","to":"A","asset":"ETH","output_asset":"WETH",
                                "gross_raw":"100","refund_raw":"99","output_raw":"1","certified_semantics":"SYNTHETIC_CONTROLLED"})
        graph["initial_balances"]["A|WETH"]="0"
        # Source8 already entered T before normal100 arrives. Only source2
        # remains; source charged to the gross100 input cannot exceed2.
        result=solve_interval(build_model(graph),["wrap:input"])
        self.assertEqual((result["lower_raw"],result["upper_raw"]),("0","2"))

    def test_infeasible_solver_state_is_not_zero(self):
        model=Model()
        idx=model.var("x","ETH",F(1),F(0),F(0),"event")
        model.event_variables["x"]=idx
        model.equality("contradiction",{idx:F(1)},F(1))
        result=solve_interval(model,["x"])
        self.assertIsNone(result["upper_raw"]);self.assertIsNone(result["lower_raw"])
        self.assertEqual(result["endpoints"]["upper"]["status"],"INFEASIBLE_REPORTED_BY_SOLVER")

    def test_duplicate_physical_event_rejected(self):
        graph=fixture("01_single_path");other=copy.deepcopy(graph["events"][1]);other["order"]=3
        graph["events"].append(other)
        with self.assertRaisesRegex(ValueError,"Duplicate physical"):
            build_model(graph)

    def test_ambiguous_execution_order_rejected(self):
        graph=fixture("01_single_path");graph["events"][1]["order"]=1
        with self.assertRaisesRegex(ValueError,"Ambiguous execution"):
            build_model(graph)

    def test_service_terminal_cannot_send_again(self):
        graph=fixture("01_single_path")
        graph["events"].append({"id":"withdrawal","kind":"transfer","order":3,"from":"T","to":"U","asset":"ETH","amount_raw":"1"})
        with self.assertRaisesRegex(ValueError,"Service account is a terminal"):
            build_model(graph)

    def test_context_only_transfer_not_silently_promoted(self):
        graph=fixture("01_single_path");graph["events"][1]["context_only"]=True
        with self.assertRaisesRegex(ValueError,"Context-only"):
            build_model(graph)

    def test_unknown_boundary_retains_source_sink(self):
        graph=fixture("01_single_path")
        graph["events"][1]["kind"]="boundary_outflow";graph["events"][1]["context_only"]=True
        result=solve_interval(build_model(graph),["enter"])
        self.assertEqual((result["lower_raw"],result["upper_raw"]),("10","10"))

    def test_zero_transfer_is_present_but_carries_no_source(self):
        graph=fixture("01_single_path");graph["events"][1]["amount_raw"]="0"
        model=build_model(graph);self.assertIn("enter",model.event_variables)
        self.assertEqual(solve_interval(model,["enter"])["positive_support"],"CERTIFIED_ZERO")

    def test_joint_certified_zero_skips_redundant_entry_optimizations(self):
        graph=fixture("06_strict_time");model=build_model(graph)
        result=solve_target_groups(model,{"zero":["earlier","future_out"]})
        self.assertTrue(all(x["status"]=="SKIPPED_CERTIFIED_ZERO_JOINT" for x in result["zero"]["entry_intervals"].values()))

    def test_seed_direct_service_zero_hop_is_valid(self):
        graph=fixture("01_single_path");graph["events"]=[graph["events"][0]];graph["events"][0]["to"]="T"
        self.assertEqual(solve_interval(build_model(graph),["seed"])["lower_raw"],"10")

    def test_multiseed_injection_outside_stage1b_rejected(self):
        graph=fixture("01_single_path");second=copy.deepcopy(graph["events"][0]);second.update(id="seed2",order=3);graph["events"].append(second)
        with self.assertRaisesRegex(ValueError,"exactly one seed"):
            build_model(graph)

    def test_float_raw_amount_and_reverted_value_rejected(self):
        with self.assertRaises(ValueError):
            raw(0.1)
        graph=fixture("01_single_path");graph["events"][1]["reverted"]=True
        with self.assertRaisesRegex(ValueError,"Reverted"):
            build_model(graph)

    def test_unverified_conversion_is_rejected(self):
        graph=fixture("09_weth_one_to_one");graph["events"][1]["certified_semantics"]="UNVERIFIED"
        with self.assertRaisesRegex(ValueError,"certification"):
            build_model(graph)

    def test_cross_asset_raw_sum_is_rejected(self):
        model=build_model(fixture("09_weth_one_to_one"))
        with self.assertRaisesRegex(ValueError,"across assets"):
            solve_interval(model,["wrap:input","wrap:output"])

    def test_no_acquisition_depth_or_fund_age_constraints(self):
        graph=fixture("01_single_path");graph["acquisition_depth"]=0;graph["fund_age_days"]=9000
        model=build_model(graph)
        self.assertFalse(model.metadata["acquisition_depth_constraints_in_lp"])
        self.assertFalse(model.metadata["fund_age_constraints_in_lp"])
        self.assertEqual(solve_interval(model,["enter"])["upper_raw"],"10")


if __name__=="__main__":
    unittest.main()
