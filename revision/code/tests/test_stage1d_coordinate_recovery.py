"""Exact numerical-coordinate recovery contracts with synthetic full ledgers."""
import copy
from fractions import Fraction as F
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
import context_lp_r3 as ctx
import lp_model
import run_stage1c as runner
import stage1c_intervals as intervals
import stage1d_experiments as experiments
import stage1d_context_certificate_recovery as recovery
from test_context_lp_r3 import example
from scipy.optimize import linprog as actual_solver


class CoordinateRecoveryTests(unittest.TestCase):
    def invoke(self, reject_all=False, **kwargs):
        doc=example();model=ctx.build_context_model(doc)
        original=copy.deepcopy((doc,model.variables,model.eq,model.rhs));calls=[];attempts={}
        def certify(m,result,cost):
            c,v,p=lp_model._certify(m,result,cost)
            side=tuple(cost);attempts[side]=attempts.get(side,0)+1
            if reject_all or attempts[side]==1:
                c=copy.deepcopy(c);c.update(certified=False,exact_dual_feasible=False)
            return c,v,p
        def solve(*a,**k):calls.append(copy.deepcopy(k));return actual_solver(*a,**k)
        with patch.object(ctx,'_certify',side_effect=certify),patch('scipy.optimize.linprog',side_effect=solve):
            result=ctx.solve_context_interval(model,doc,['enter'],stage1d_coordinate_recovery=True,**kwargs)
        self.assertEqual(original,(doc,model.variables,model.eq,model.rhs))
        return result,calls

    def test_real_solver_retry_preserves_model_and_exact_endpoint(self):
        result,calls=self.invoke()
        self.assertEqual(result['status'],'OPTIMAL_EXACT_CERTIFIED')
        self.assertEqual((result['lower_raw'],result['upper_raw']),('70','80'))
        self.assertEqual(len(calls),4)
        for primary,secondary in zip(calls[::2],calls[1::2]):
            self.assertEqual((primary['A_eq']!=secondary['A_eq']).nnz,0)
            for a,b in zip(primary['bounds'],secondary['bounds']):
                self.assertEqual(tuple(v*recovery.FACTOR for v in a),b)
            self.assertEqual((primary['b_eq']*recovery.FACTOR).tolist(),secondary['b_eq'].tolist())
            self.assertLessEqual(secondary['options']['time_limit'],primary['options']['time_limit'])
        for endpoint in result['endpoints'].values():
            self.assertTrue(endpoint['certificate']['certified'])
            self.assertTrue(endpoint['independent_audit']['exact_feasible'])
            self.assertEqual(endpoint['certificate_recovery']['selected_attempt'],2)

    def test_uncertified_retry_never_becomes_an_amount(self):
        result,calls=self.invoke(reject_all=True)
        self.assertEqual(len(calls),4)
        self.assertEqual(result['status'],'UNRESOLVED')
        self.assertIsNone(result['upper_raw'])
        self.assertIsNone(result['endpoints']['lower']['witness_event_source_raw'])

    def test_independent_ledger_audit_still_required(self):
        with patch.object(ctx,'audit_context_witness',return_value={'exact_feasible':False,'errors':['synthetic bad ledger']}):
            result,_=self.invoke()
        self.assertEqual(result['status'],'UNRESOLVED')
        self.assertIsNone(result['upper_raw'])

    def test_good_primary_does_not_retry(self):
        doc=example()
        with patch('scipy.optimize.linprog',wraps=actual_solver) as solver:
            result=ctx.solve_context_interval(ctx.build_context_model(doc),doc,['enter'],stage1d_coordinate_recovery=True)
        self.assertEqual(solver.call_count,2)
        self.assertTrue(all('certificate_recovery' not in x for x in result['endpoints'].values()))

    def test_expired_budget_never_dispatches_retry(self):
        with patch.object(recovery,'recover',wraps=recovery.recover) as retry,patch('time.perf_counter',side_effect=[0,10,20,30]):
            result,calls=self.invoke(time_limit_seconds=1)
        self.assertEqual(len(calls),2)
        self.assertEqual(result['status'],'UNRESOLVED')
        self.assertTrue(all(c.args[3]<0 for c in retry.call_args_list))

    def test_scaled_solver_failure_keeps_original_failure(self):
        def solve(*a,**k):
            if k['method']=='highs-ds':raise RuntimeError('synthetic secondary solver failure')
            return actual_solver(*a,**k)
        original=lp_model._certify
        def uncertified(*a):
            c,v,p=original(*a);c.update(certified=False,exact_dual_feasible=False);return c,v,p
        doc=example()
        with patch('scipy.optimize.linprog',side_effect=solve),patch.object(ctx,'_certify',side_effect=uncertified):
            result=ctx.solve_context_interval(ctx.build_context_model(doc),doc,['enter'],stage1d_coordinate_recovery=True)
        self.assertEqual(result['status'],'UNRESOLVED')
        self.assertIn('synthetic secondary solver failure',result['endpoints']['upper']['certificate_recovery']['attempts'][1]['error'])

    def test_dispatch_and_six_measurements_forward_explicit_option(self):
        with patch.object(runner,'run_interval',return_value={}) as method:
            runner.dispatch(example(),'FULL_INTERVAL',stage1d_coordinate_recovery=True)
            self.assertTrue(method.call_args.kwargs['stage1d_coordinate_recovery'])
        result=intervals.run_interval(example())
        with patch.object(runner,'dispatch',return_value=result) as method:
            _,profile=runner.measure(example(),'FULL_INTERVAL',stage1d_coordinate_recovery=True)
        self.assertEqual(method.call_count,6)
        self.assertTrue(all(c.kwargs['stage1d_coordinate_recovery'] for c in method.call_args_list))
        self.assertEqual(len(profile['repetitions']),6)

    def test_real_interval_receiver_counts_retry_calls(self):
        attempts={}
        def certify(m,result,cost):
            c,v,p=lp_model._certify(m,result,cost);key=tuple(cost)
            attempts[key]=attempts.get(key,0)+1
            if attempts[key]==1:c.update(certified=False,exact_dual_feasible=False)
            return c,v,p
        with patch.object(ctx,'_certify',side_effect=certify):
            result=intervals.run_interval(example(),stage1d_coordinate_recovery=True)
        self.assertEqual(result['status'],'COMPLETED')
        self.assertEqual(result['task_counts']['endpoint_optimizations'],4)

    def test_freeze_policy_explicitly_binds_representation(self):
        self.assertEqual(experiments.SOLVER_EXECUTION_POLICY['schema_version'],'stage1d-solver-execution-policy-v2')
        self.assertIs(experiments.SOLVER_EXECUTION_POLICY['stage1d_coordinate_recovery'],True)
        self.assertEqual(experiments.SOLVER_EXECUTION_POLICY['uniform_coordinate_factor'],recovery.FACTOR)
        self.assertEqual(experiments.SOLVER_EXECUTION_POLICY['coordinate_recovery_bound_reselections_max'],recovery.MAX_BOUND_RESELECTIONS)

    def test_nonboolean_enable_is_rejected(self):
        doc=example()
        with self.assertRaises(ValueError):
            ctx.solve_context_interval(ctx.build_context_model(doc),doc,['enter'],stage1d_coordinate_recovery='yes')

    def test_exact_original_bound_reselection_repairs_rejected_vertex(self):
        import numpy as np
        from scipy.optimize import OptimizeResult
        from scipy.sparse import csr_matrix
        model=lp_model.Model();delta=F(1,10**12)
        model.var('retained','ETH',F(1),F(0),F(1),'balance')
        model.var('residue','ETH',F(1),F(0),delta,'balance')
        model.equality('conservation',{0:F(1),1:F(1)},1+delta)
        numeric=OptimizeResult(status=0,message='synthetic near-bound vertex',fun=0.0,
            x=np.array([float((1+delta)*recovery.FACTOR),0.0]),
            eqlin=OptimizeResult(marginals=np.array([0.0])),
            lower=OptimizeResult(marginals=np.array([0.0,0.0])),
            upper=OptimizeResult(marginals=np.array([0.0,0.0])))
        original=copy.deepcopy((model.variables,model.eq,model.rhs))
        with patch('scipy.optimize.linprog',return_value=numeric):
            selected,evidence=recovery.recover(model,[F(0),F(0)],csr_matrix([[1.,1.]]),1,
                certify=lp_model._certify,recover_vertex=ctx._recover_large_network_vertex,
                propose_vertex=ctx._propose_large_network_vertex)
        self.assertIsNotNone(selected)
        self.assertEqual(selected[2],[F(1),delta])
        self.assertTrue(selected[1]['certified'])
        self.assertTrue(model.audit_vector(selected[2])['exact_feasible'])
        self.assertEqual(evidence['exact_bound_reselection']['nominated_variable_count'],1)
        self.assertEqual(original,(model.variables,model.eq,model.rhs))

    def test_reselection_cannot_introduce_a_relaxed_bound(self):
        model=lp_model.Model();model.var('x','ETH',F(1),F(0),F(1),'balance')
        with self.assertRaisesRegex(ValueError,'original exact bounds'):
            ctx._propose_large_network_vertex(model,None,[(0,F(2))])

if __name__=='__main__':unittest.main()
