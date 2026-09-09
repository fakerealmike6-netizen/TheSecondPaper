"""Portable exact-zero controls; no private inputs, online calls, or answers."""
import copy
from fractions import Fraction as F
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from lp_model import build_model, solve_interval
from run_stage1c import dispatch, measure, normalized
from stage1c_intervals import METHODS as INTERVALS, run_interval, build_variant
from stage1c_output_contract import METHODS, accept_method_results
from stage1c_zero_derivation import (RULE, digest, model_identity, certify_zero_parent,
    derive_zero, validate_derived_zero)


def zero_graph():
    return {'scenario_id':'zero-subset-control', 'query_id':'zero-subset-control',
        'initial_balances':{'A|ETH':'0','B|ETH':'5','T|ETH':'0','U|ETH':'0'},
        'target_accounts':['T','U'], 'objective_groups':{'T|ETH':['t1','t2'],'U|ETH':['u']},
        'events':[{'id':'seed','kind':'seed','to':'A','asset':'ETH','amount_raw':'4','order':0},
            {'id':'t1','kind':'transfer','from':'B','to':'T','asset':'ETH','amount_raw':'1','order':1},
            {'id':'t2','kind':'transfer','from':'B','to':'T','asset':'ETH','amount_raw':'1','order':2},
            {'id':'u','kind':'transfer','from':'B','to':'U','asset':'ETH','amount_raw':'1','order':3}]}


def trusted(doc):
    return {'sample_id':doc['scenario_id'],'query_id':doc['query_id'], 'input_fact_hash':digest(doc),
        'scope_hash':'f'*64,'label_version':'SYNTHETIC_ZERO_V1',
        'method_versions':{m:'synthetic-joint-zero-v1' for m in METHODS}}


def seven(doc):
    identity = trusted(doc)
    results = {}
    for method in METHODS:
        value = normalized(dispatch(copy.deepcopy(doc), method))
        value.update({k:identity[k] for k in ('sample_id','query_id','input_fact_hash','scope_hash','label_version')})
        value.update(method_id=method, method_version=identity['method_versions'][method])
        results[method] = value
    return results


def amounts(value):
    return {category:{key:(row['asset'],row['lower_raw'],row['upper_raw']) for key,row in value[category].items()}
            for category in ('addresses','events','joint_by_asset')}


class JointZeroTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.doc=zero_graph()
        cls.results=seven(cls.doc)

    def reject(self, values, code='ZERO_DERIVATION', doc=None):
        doc=self.doc if doc is None else doc
        receipt=accept_method_results(doc,values,expected_identity=trusted(doc))
        self.assertFalse(receipt['passed'])
        self.assertIn(code,{e['code'] for e in receipt['errors']},receipt['errors'])
        return receipt

    def test_actual_seven_method_receiver_accepts_derived_outputs_without_solver(self):
        before=copy.deepcopy(self.results)
        with patch('scipy.optimize.linprog',side_effect=AssertionError('Receiver called optimizer')):
            a=accept_method_results(self.doc,self.results,expected_identity=trusted(self.doc))
            b=accept_method_results(self.doc,self.results,expected_identity=trusted(self.doc))
        self.assertTrue(a['passed'],a['errors'])
        self.assertEqual(a,b)
        self.assertEqual(before,self.results)
        self.assertEqual(set(a['method_checks']),set(METHODS))

    def test_fixed_input_switch_preserves_full_output_domain_and_endpoints(self):
        from scipy.optimize import linprog
        for method in INTERVALS:
            with self.subTest(method=method):
                with patch('scipy.optimize.linprog', wraps=linprog) as actual:
                    old=run_interval(self.doc,method,joint_zero_optimization=False)
                    old_calls=actual.call_count
                with patch('scipy.optimize.linprog', wraps=linprog) as actual:
                    new=run_interval(self.doc,method,joint_zero_optimization=True)
                    new_calls=actual.call_count
                self.assertEqual(amounts(old),amounts(new))
                self.assertEqual(old_calls,old['task_counts']['endpoint_optimizations'])
                self.assertEqual(new_calls,new['task_counts']['endpoint_optimizations'])
                self.assertLess(new_calls,old_calls)
                self.assertEqual(old_calls-new_calls,new['task_counts']['derived_endpoint_optimizations_avoided'])
                self.assertEqual(self.doc,zero_graph())

    def test_joint_is_first_and_child_does_not_claim_optimizer(self):
        import stage1c_intervals as intervals
        actual=intervals.solve_interval
        calls=[]
        def seen(model,names):
            calls.append(names)
            return actual(model,names)
        with patch.object(intervals,'solve_interval',side_effect=seen):
            result=run_interval(self.doc)
        self.assertEqual(calls,[['t1','t2','u']])
        row=result['events']['t1']
        self.assertEqual(row['solution_origin'],RULE)
        self.assertFalse(row['solver_called'])
        self.assertEqual(row['endpoints']['upper']['status'],RULE)
        self.assertNotIn('exact_dual_feasible',row['endpoints']['upper']['certificate'])
        self.assertEqual(row['zero_derivation']['parent_ref'],{'category':'joint_by_asset','key':'ETH'})

    def test_each_of_six_actual_timing_runs_builds_fresh_model_memo(self):
        from scipy.optimize import linprog
        import stage1c_intervals as intervals
        with patch.object(intervals,'build_model',wraps=intervals.build_model) as builder:
            with patch('scipy.optimize.linprog',wraps=linprog) as solver:
                result,profile=measure(self.doc,'FULL_INTERVAL')
        self.assertEqual(builder.call_count,6)
        self.assertEqual(solver.call_count,12)
        self.assertEqual([r['task_counts']['endpoint_optimizations'] for r in profile['repetitions']],[2]*6)
        self.assertTrue(profile['deterministic'])
        self.assertEqual(result['status'],'COMPLETED')

    def test_target_copies_get_separate_parents_and_matrices(self):
        row=self.results['NO_CROSS_TARGET_COUPLING']
        t=row['events']['t1']
        self.assertEqual(t['zero_derivation']['parent_ref'],{'category':'addresses','key':'T|ETH'})
        self.assertEqual(t['proof_model_identity']['target_copy'],{'copy_id':'F^0','address_asset':'T|ETH'})
        self.assertEqual(row['addresses']['U|ETH']['proof_model_identity']['target_copy']['copy_id'],'F^1')
        self.assertEqual(row['task_counts']['solver_matrices_built'],2)
        self.assertEqual(row['task_counts']['endpoint_optimizations'],4)

    def test_per_asset_joint_never_combines_eth_and_weth(self):
        doc=zero_graph()
        doc['initial_balances'].update({'H|WETH':'0','V|WETH':'0'})
        doc['target_accounts'].append('V');doc['objective_groups']['V|WETH']=['w1','w2']
        doc['events'] += [
            {'id':'deposit','kind':'conversion','from':'A','to':'H','asset':'ETH','output_asset':'WETH',
             'gross_raw':'4','refund_raw':'0','output_raw':'4','order':4,'certified_semantics':'SYNTHETIC_CONTROLLED'},
            {'id':'w1','kind':'transfer','from':'H','to':'V','asset':'WETH','amount_raw':'1','order':5},
            {'id':'w2','kind':'transfer','from':'H','to':'V','asset':'WETH','amount_raw':'1','order':6}]
        for method in INTERVALS:
            old=run_interval(doc,method,joint_zero_optimization=False)
            new=run_interval(doc,method)
            self.assertEqual(amounts(old),amounts(new))
            self.assertEqual(set(new['joint_by_asset']),{'ETH','WETH'})
        results=seven(doc)
        check=accept_method_results(doc,results,expected_identity=trusted(doc))
        self.assertTrue(check['passed'],check['errors'])
        self.assertEqual(results['FULL_INTERVAL']['joint_by_asset']['WETH']['upper_raw'],'2')

    def test_zero_lower_is_not_zero_upper(self):
        doc=zero_graph();doc['events'][0]['to']='B';doc['initial_balances']['B|ETH']='5'
        result=run_interval(doc)
        self.assertEqual(result['joint_by_asset']['ETH']['lower_raw'],'0')
        self.assertEqual(result['joint_by_asset']['ETH']['upper_raw'],'3')
        self.assertEqual(result['task_counts']['derived_zero_unique_objectives'],0)

    def test_zero_address_can_derive_entries_after_nonzero_joint(self):
        doc=zero_graph();doc['initial_balances']['V|ETH']='0'
        doc['target_accounts'].append('V');doc['objective_groups']['V|ETH']=['v']
        doc['events'].append({'id':'v','kind':'transfer','from':'A','to':'V','asset':'ETH','amount_raw':'4','order':4})
        result=run_interval(doc)
        self.assertEqual(result['joint_by_asset']['ETH']['upper_raw'],'4')
        self.assertEqual(result['events']['t1']['zero_derivation']['parent_ref'],{'category':'addresses','key':'T|ETH'})
        values=seven(doc)
        receipt=accept_method_results(doc,values,expected_identity=trusted(doc))
        self.assertTrue(receipt['passed'],receipt['errors'])

    def test_unknown_joint_does_not_skip_children(self):
        import stage1c_intervals as intervals
        actual=intervals.solve_interval;calls=[]
        def failed_joint(model,names):
            calls.append(names)
            if len(names)==3:
                return {'status':'UNRESOLVED','asset':'ETH','objective_events':names,
                    'lower_raw':'0','upper_raw':None,'endpoints':{}}
            return actual(model,names)
        with patch.object(intervals,'solve_interval',side_effect=failed_joint):
            result=run_interval(self.doc)
        self.assertEqual(result['status'],'ERROR')
        self.assertIn(['t1','t2'],calls)
        self.assertIn(['u'],calls)

    def test_empty_target_still_calls_feasibility(self):
        doc=zero_graph();doc['objective_groups']={};doc['target_accounts']=[]
        result=run_interval(doc)
        self.assertEqual(result['task_counts']['endpoint_optimizations'],2)
        self.assertEqual(result['task_counts']['derived_zero_unique_objectives'],0)
        self.assertEqual(result['modifications']['empty_target_feasibility']['status'],'OPTIMAL_EXACT_CERTIFIED')

    def test_missing_parent_certificate_fails(self):
        values=copy.deepcopy(self.results)
        del values['FULL_INTERVAL']['joint_by_asset']['ETH']['endpoints']['upper']['certificate']['exact_zero_upper_dual']
        self.reject(values)

    def test_actual_query_gate_propagates_bad_derivation_before_science(self):
        from run_stage1c import evaluate_query
        values=copy.deepcopy(self.results)
        del values['FULL_INTERVAL']['events']['t1']['zero_derivation']['parent_ref']
        identity=trusted(self.doc)
        row={**identity,'kind':'controlled','hidden_path':'MUST_NOT_BE_OPENED'}
        with patch('run_stage1c.read',side_effect=AssertionError('No reference may repair failed proof')):
            contract,evaluation,ablations,_=evaluate_query(Path('.'),row,
                {'method_versions':identity['method_versions']},self.doc,values)
        self.assertFalse(contract['passed'])
        self.assertEqual(evaluation['status'],'NOT_CERTIFIED_CONTRACT_FAILED')
        self.assertFalse(evaluation['passed'])
        self.assertEqual(ablations,[])

    def test_forged_boolean_certificate_without_valid_dual_fails(self):
        values=copy.deepcopy(self.results);result=values['FULL_INTERVAL'];parent=result['joint_by_asset']['ETH']
        dual=parent['endpoints']['upper']['certificate']['exact_zero_upper_dual']
        dual['lower_multipliers']=['0']*len(dual['lower_multipliers'])
        dual['upper_multipliers']=['0']*len(dual['upper_multipliers'])
        dual['equality_multipliers']=['0']*len(dual['equality_multipliers'])
        for category in ('addresses','events'):
            for row in result[category].values():
                if row.get('zero_derivation'):
                    row['zero_derivation']['parent_sha256']=digest(parent)
        self.reject(values)

    def test_wrong_model_query_variant_and_copy_fail(self):
        for field,value in [('model_sha256','0'*64),('query_id','other-query'),('method','NO_PROTOCOL_CONTINUATION'),
                            ('target_copy',{'copy_id':'F^9','address_asset':'elsewhere|ETH'})]:
            with self.subTest(field=field):
                values=copy.deepcopy(self.results)
                row=values['FULL_INTERVAL']['events']['t1']
                row['zero_derivation']['model_identity'][field]=value
                self.reject(values)

    def test_wrong_asset_and_non_subset_fail(self):
        for field,value in [('asset','WETH'),('subset_events',['seed']),('parent_objective_events',['seed'])]:
            values=copy.deepcopy(self.results)
            values['FULL_INTERVAL']['events']['t1']['zero_derivation'][field]=value
            self.reject(values)

    def test_copy_cannot_reuse_shared_joint_or_another_copy(self):
        for ref in ({'category':'joint_by_asset','key':'ETH'},{'category':'addresses','key':'U|ETH'}):
            values=copy.deepcopy(self.results)
            values['NO_CROSS_TARGET_COUPLING']['events']['t1']['zero_derivation']['parent_ref']=ref
            self.reject(values)

    def test_product_union_cannot_masquerade_as_subset_derivation(self):
        values=copy.deepcopy(self.results)
        values['NO_CROSS_TARGET_COUPLING']['joint_by_asset']['ETH']['solution_origin']=RULE
        self.reject(values)

    def test_changed_witness_and_optimizer_impersonation_fail(self):
        values=copy.deepcopy(self.results)
        values['FULL_INTERVAL']['events']['t1']['endpoints']['upper']['witness_event_source_raw']['t1']='1'
        self.reject(values)
        values=copy.deepcopy(self.results)
        values['FULL_INTERVAL']['events']['t1']['endpoints']['upper']['status']='OPTIMAL_EXACT_CERTIFIED'
        self.reject(values)

    def test_negative_objective_variable_rejected_even_with_matching_identity(self):
        model=build_model(self.doc)
        parent=solve_interval(model,['t1','t2','u'])
        model.variables[model.event_variables['t1']].lower=F(-1)
        identity=model_identity(model,self.doc,'FULL_INTERVAL')
        parent['proof_model_identity']=identity
        with self.assertRaisesRegex(ValueError,'nonnegative'):
            certify_zero_parent(model,parent,identity)

    def test_receiver_independently_rejects_negative_parent_variable(self):
        import stage1c_intervals as intervals
        original=intervals.build_variant
        model,_=original(self.doc,'FULL_INTERVAL')
        model.variables[model.event_variables['t1']].lower=F(-1)
        identity=model_identity(model,self.doc,'FULL_INTERVAL')
        values=copy.deepcopy(self.results);result=values['FULL_INTERVAL']
        parent=result['joint_by_asset']['ETH'];parent['proof_model_identity']=identity
        for category in ('addresses','events'):
            for row in result[category].values():
                row['proof_model_identity']=copy.deepcopy(identity)
                if 'zero_derivation' in row:
                    row['zero_derivation']['model_identity']=copy.deepcopy(identity)
                    row['zero_derivation']['parent_sha256']=digest(parent)
        def changed(doc,method):
            m,mods=original(doc,method)
            if method=='FULL_INTERVAL':m.variables[m.event_variables['t1']].lower=F(-1)
            return m,mods
        with patch.object(intervals,'build_variant',side_effect=changed):
            receipt=self.reject(values)
        self.assertTrue(any(e['code']=='ZERO_DERIVATION' and 'nonnegative' in e['detail'] for e in receipt['errors']))

    def test_model_and_input_identity_change_is_rejected_by_receiver(self):
        doc=copy.deepcopy(self.doc);doc['semantic_scope_id']='new-semantic-scope'
        values=copy.deepcopy(self.results)
        for row in values.values():
            row['input_fact_hash']=trusted(doc)['input_fact_hash']
        self.reject(values,doc=doc)

    def test_context_native_actual_entry_uses_same_zero_proof(self):
        doc={'schema_version':'stage1b-r3-context-model-v1','query_id':'native-context-zero','scenario_id':'native-context-zero',
            'accounts':[{'account_id':a+'|ETH','initial_actual_balance_raw':b,'initial_source_raw':'0',
                'initial_source_basis':'synthetic','initial_position':{'block_number':0,'phase':'BLOCK_END'}} for a,b in [('A','0'),('B','3')]],
            'transactions':[{'tx_id':'s','block_number':1,'tx_index':0,'flows':[{'event_id':'seed','role':'SEED',
                'from_account':'outside|ETH','to_account':'A|ETH','amount_raw':'2'}],'fees':[]},
                *[{'tx_id':e,'block_number':i+2,'tx_index':0,'flows':[{'event_id':e,'role':'CANDIDATE',
                 'from_account':'B|ETH','to_account':'T|ETH','amount_raw':'1'}],'fees':[]} for i,e in enumerate(('t1','t2'))]],
            'objective_groups':{'T|ETH':['t1','t2']},'all_service_entries':['t1','t2']}
        results=seven(doc)
        self.assertEqual(results['FULL_INTERVAL']['task_counts']['endpoint_optimizations'],2)
        receipt=accept_method_results(doc,results,expected_identity=trusted(doc))
        self.assertTrue(receipt['passed'],receipt['errors'])


if __name__=='__main__':
    unittest.main()
