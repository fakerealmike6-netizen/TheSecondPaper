import copy
import unittest
from fractions import Fraction as F
from stage1c_intervals import run_interval, verify_product, build_variant, audit_allocation

def shared():
    return {'scenario_id':'stage1c_unit_shared','initial_balances':{'A|ETH':'5','T|ETH':'0','U|ETH':'0'},
      'target_accounts':['T','U'],'objective_groups':{'T|ETH':['t'],'U|ETH':['u']},
      'events':[{'id':'s','kind':'seed','to':'A','asset':'ETH','amount_raw':'5','order':0},
        {'id':'t','kind':'transfer','from':'A','to':'T','asset':'ETH','amount_raw':'5','order':1},
        {'id':'u','kind':'transfer','from':'A','to':'U','asset':'ETH','amount_raw':'5','order':2}]}

class IntervalSemantics(unittest.TestCase):
    def test_actual_target_copies_conflict_with_single_source(self):
        data=verify_product(shared())
        self.assertEqual(data['status'],'PASS')
        self.assertEqual(data['copy_count'],2)
        self.assertEqual(data['explicit_variables'],2*data['base_variables'])
        self.assertEqual(data['full_joint_interval']['upper_raw'],'5')
        self.assertEqual(data['explicit_interval']['upper_raw'],'10')
        self.assertFalse(data['independent_upper_combination_feasible_in_full'])

    def test_balance_relaxation_retains_physical_operations(self):
        g=shared();full=run_interval(g);relax=run_interval(g,'BALANCE_INFORMATION_REMOVED')
        self.assertEqual(full['joint_by_asset']['ETH']['lower_raw'],'5')
        self.assertEqual(relax['joint_by_asset']['ETH']['lower_raw'],'0')
        self.assertTrue(relax['modifications']['nesting']['nested'])
        self.assertEqual(set(full['events']),set(relax['events']))

    def test_seed_zero_hop(self):
        g={'initial_balances':{'T|ETH':'0'},'target_accounts':['T'],
           'objective_groups':{'T|ETH':['s']},'events':[{'id':'s','kind':'seed','to':'T','asset':'ETH','amount_raw':'3','order':0}]}
        self.assertEqual(run_interval(g)['addresses']['T|ETH']['lower_raw'],'3')

    def test_no_protocol_boundary_preserves_input_source(self):
        g={'initial_balances':{'A|ETH':'0','B|WETH':'0','T|WETH':'0'},'target_accounts':['T'],
          'objective_groups':{'T|WETH':['t']},'events':[
           {'id':'s','kind':'seed','to':'A','asset':'ETH','amount_raw':'4','order':0},
           {'id':'w','kind':'conversion','from':'A','to':'B','asset':'ETH','output_asset':'WETH','gross_raw':'4','refund_raw':'0','output_raw':'4','order':1,'certified_semantics':'SYNTHETIC_CONTROLLED'},
           {'id':'t','kind':'transfer','from':'B','to':'T','asset':'WETH','amount_raw':'4','order':2}]}
        full=run_interval(g);stop=run_interval(g,'NO_PROTOCOL_CONTINUATION')
        self.assertEqual(full['addresses']['T|WETH']['lower_raw'],'4')
        self.assertEqual(stop['addresses']['T|WETH']['upper_raw'],'0')
        witness=stop['addresses']['T|WETH']['endpoints']['upper']['witness_event_source_raw']
        self.assertEqual(witness['w:net'],'4')
        self.assertEqual(witness['w:output'],'0')
        self.assertEqual(stop['modifications']['boundary_source_variables'],['w:net'])

    def test_unsupported_and_infeasible_not_zero(self):
        g=shared();g['initial_balances']['A|ETH']='0'
        with self.assertRaises(ValueError):run_interval(g)
        with self.assertRaises(ValueError):run_interval(shared(),'UNKNOWN')

    def test_input_is_immutable_and_independent(self):
        g=shared();before=copy.deepcopy(g)
        a=run_interval(g);b=run_interval(g,'NO_CROSS_TARGET_COUPLING');c=run_interval(g)
        self.assertEqual(g,before)
        self.assertEqual(a['joint_by_asset'],c['joint_by_asset'])
        self.assertNotEqual(a['joint_by_asset']['ETH']['upper_raw'],b['joint_by_asset']['ETH']['upper_raw'])

    def test_point_checked_without_solver_construction_of_point(self):
        g=shared()
        self.assertTrue(audit_allocation(g,{'s':'5','t':'5/2','u':'5/2'})['exact_feasible'])
        self.assertFalse(audit_allocation(g,{'s':'5','t':'5','u':'5'})['exact_feasible'])

    def test_empty_targets_dont_hide_invalid_input(self):
        g=shared();g['objective_groups']={};g['target_accounts']=[]
        self.assertEqual(run_interval(g)['joint_by_asset']['ETH']['upper_raw'],'0')
        g['initial_balances']['A|ETH']='0'
        with self.assertRaises(ValueError):run_interval(g)
