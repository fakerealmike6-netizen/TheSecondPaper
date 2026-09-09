from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

STAGE = Path(__file__).resolve().parents[1]
CODE = STAGE
sys.path[:0] = [str(STAGE), str(CODE / 'src')]
import stage1d_transfers_acquisition as a


def request(number, method='eth_getTransactionByHash'):
    return {'method':method,'params':[hex(number),False] if method=='eth_getBlockByNumber' else ['0x'+format(number,'064x')]}


class BindingBatchTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(dir=STAGE)
        self.w=Path(self.temp.name)
        self.path=self.w/'state.json'
        self.state={'bindings':[],'failures':[],'status':'READY'}
        self.counters={'members':0,'dispatches':0}
        self.calls=[];self.saved={};self.order=[]
        test=self
        class Access:
            def call_batch(self,plans,*args,**kw):
                test.calls.append(deepcopy(plans));test.order.append(('dispatch',len(plans)))
                return {'members':[{'status':'SUCCESS_VALIDATED','selector':p} for p in plans]}
        self.access=Access()

    def tearDown(self):self.temp.cleanup()

    def verify(self,work,plan,member):
        if member['selector']!=plan:raise ValueError('exact selector mismatch')
        self.order.append(('save',a.digest(plan)))

    def run_bindings(self,plans,maximum=2000):
        with patch.object(a,'cached_member',side_effect=lambda w,p:self.saved.get(a.digest(p))),patch.object(a,'verified_member',side_effect=self.verify):
            a._acquire_bindings(self.w,self.state,self.path,plans,self.access,'txphish_src001',self.counters,maximum)

    def test_five_ordinary_one_receipt_preserves_order(self):
        plans=[request(i) for i in range(1,8)]+[request(i,'eth_getTransactionReceipt') for i in range(1,4)]+[request(i,'eth_getBlockByNumber') for i in range(1,7)]
        self.run_bindings(plans)
        self.assertEqual([len(b) for b in self.calls],[5,2,1,1,1,5,1])
        self.assertEqual([p for b in self.calls for p in b],plans)
        self.assertEqual(self.counters,{'members':16,'dispatches':7})
        self.assertEqual(len(self.state['bindings']),16)

    def test_cache_first_costs_no_dispatch_and_is_saved_first(self):
        plans=[request(i) for i in range(1,7)]
        for p in plans[::2]:self.saved[a.digest(p)]={'status':'SUCCESS_VALIDATED','selector':p,'cache_hit':True}
        self.run_bindings(plans)
        self.assertEqual(self.calls,[plans[1::2]])
        self.assertEqual(self.counters,{'members':3,'dispatches':1})
        self.assertTrue(all(x[0]=='save' for x in self.order[:3]))

    def test_receipt_is_never_combined_even_between_ordinary(self):
        plans=[request(1),request(1,'eth_getTransactionReceipt'),request(2,'eth_getBlockByNumber')]
        self.run_bindings(plans)
        self.assertEqual([len(b) for b in self.calls],[1,1,1])

    def test_partial_failure_saves_success_after_failed_member(self):
        plans=[request(i) for i in range(1,7)]
        def call(batch,*args,**kw):
            self.calls.append(batch)
            return {'members':[{'status':'DEFERRED','identity':'unchanged-key'} if i==0 else
                               {'status':'SUCCESS_VALIDATED','selector':p} for i,p in enumerate(batch)]}
        self.access.call_batch=call
        self.run_bindings(plans)
        self.assertEqual(len(self.calls),1)
        self.assertEqual([b['plan'] for b in self.state['bindings']],plans[1:5])
        saved=json.loads(self.path.read_text(encoding='utf8'))
        self.assertEqual(len(saved['bindings']),4)
        self.assertEqual(saved['failures'][0]['plan'],plans[0])
        self.assertEqual(saved['status'],'RPC_MEMBER_BLOCKED')

    def test_exception_recovers_same_key_success_without_retry(self):
        plans=[request(i) for i in range(1,7)]
        def call(batch,*args,**kw):
            self.calls.append(batch)
            self.saved[a.digest(batch[1])]={'status':'SUCCESS_VALIDATED','selector':batch[1],'cache_hit':True}
            raise RuntimeError('Cumulative raw risk cap after a saved success')
        self.access.call_batch=call
        with self.assertRaises(RuntimeError):self.run_bindings(plans)
        self.assertEqual(len(self.calls),1)
        self.assertEqual([b['plan'] for b in self.state['bindings']],[plans[1]])
        self.assertEqual(self.state['status'],'RAW_OR_RESOURCE_BLOCKED_SAME_REQUEST_PRESERVED')

    def test_member_count_mismatch_saves_durable_cache_then_stops(self):
        plans=[request(i) for i in range(1,6)]
        def call(batch,*args,**kw):
            self.calls.append(batch)
            self.saved[a.digest(batch[-1])]={'status':'SUCCESS_VALIDATED','selector':batch[-1],'cache_hit':True}
            return {'members':[]}
        self.access.call_batch=call
        with self.assertRaisesRegex(ValueError,'member count'):self.run_bindings(plans)
        self.assertEqual([b['plan'] for b in self.state['bindings']],[plans[-1]])
        self.assertEqual(len(self.calls),1)

    def test_selector_order_mismatch_never_adopts_wrong_fact(self):
        plans=[request(i) for i in range(1,4)]
        self.access.call_batch=lambda batch,*args,**kw:{'members':[{'status':'SUCCESS_VALIDATED','selector':p} for p in reversed(batch)]}
        with self.assertRaisesRegex(ValueError,'selector mismatch'):self.run_bindings(plans)
        self.assertEqual(self.state['bindings'],[])

    def test_schedule_bound_counts_selectors_not_batch_envelopes(self):
        plans=[request(i) for i in range(1,12)]
        self.run_bindings(plans,7)
        self.assertEqual([len(b) for b in self.calls],[5,2])
        self.assertEqual(self.counters,{'members':7,'dispatches':2})
        self.assertEqual(self.state['status'],'SCHEDULING_BINDING_BOUND_PAUSED')

    def test_prior_completed_selectors_are_not_resubmitted(self):
        plans=[request(i) for i in range(1,8)]
        self.state['bindings']=[{'plan':plans[0],'member':{'status':'SUCCESS_VALIDATED','selector':plans[0]}}]
        self.run_bindings(plans)
        self.assertEqual(self.calls,[plans[1:6],plans[6:]])
        self.assertEqual(len(self.state['bindings']),7)

    def test_each_success_written_before_next_member(self):
        plans=[request(i) for i in range(1,6)]
        counts=[]
        real=a.atomic_json
        def write(path,value):
            real(path,value)
            if Path(path)==self.path:counts.append(len(json.loads(self.path.read_text(encoding='utf8'))['bindings']))
        with patch.object(a,'atomic_json',side_effect=write):self.run_bindings(plans)
        self.assertEqual(counts,[1,2,3,4,5])


import test_transfers_acquisition as worker_fixtures


class CacheAdmitterTests(unittest.TestCase):
    setUp=worker_fixtures.Tests.setUp
    tearDown=worker_fixtures.Tests.tearDown
    member=worker_fixtures.Tests.member
    row=worker_fixtures.Tests.row
    state=worker_fixtures.Tests.state
    bind_top=worker_fixtures.Tests.bind_top

    def worker(self):
        from stage1d_alchemy_transfers import TransfersRuntime
        state=self.state([self.row()]);self.bind_top(state)
        members={a.digest(x['plan']):x['member'] for x in state['bindings']}
        state['bindings']=[]
        folder=self.w/'private/stage1d_transfers_acquisition/needs'/a.digest(self.need)
        a.atomic_json(folder/'state.json',state)
        frontier={'reason':'INTERVAL_INCOMPLETE','state':{'address':self.addr,'asset':a.NATIVE,'depth':1,
            'protocol_context':'ordinary','arrival':{'block':self.scope.start_block,'timestamp':self.scope.start_time,
                                                  'tx_index':0,'event_id':'fixture-arrival'}}}
        boundary={'queries':[{'query_name':self.q['name'],'needed_ranges':[self.need],'frontier':[frontier]}]}
        a.atomic_json(self.w/'boundary.json',boundary)
        plan={'version':a.VERSION,'query_name':self.q['name'],'needs':[self.need],'source_needs':[self.need],
            'maximum_needs':20,'boundary_snapshot':a.ref(self.w,'boundary.json'),'route_dependencies':self.gate_refs}
        a.atomic_json(self.w/'plan.json',plan)
        test=self
        class Access:
            runtime=TransfersRuntime()
            w=test.w
            def __init__(self):self.calls=[]
            def call_batch(self,plans,*args,**kw):
                self.calls.append(deepcopy(plans))
                return {'members':[members[a.digest(p)] for p in plans]}
        return Access(),folder/'state.json',members

    def test_callback_precedes_point_cache_and_network(self):
        access,path,members=self.worker();order=[]
        def callback(query,need,summary):
            self.assertEqual(query['query_id'],self.q['query_id']);self.assertEqual(need,self.need)
            self.assertTrue(summary['page_chain_naturally_exhausted']);order.append('callback')
            return {'path':'synthetic_admission.json','sha256':'a'*64}
        def cached(work,plan):
            order.append('point-cache');self.assertEqual(order[0],'callback')
            # Simulate the root callback admitting an exact previous receipt.
            return dict(members[a.digest(plan)],cache_hit=True) if plan['method']=='eth_getTransactionReceipt' else None
        with patch.object(a,'cached_member',side_effect=cached):
            result=a.acquire(self.w,'plan.json',access,clock=lambda:1000,point_cache_admitter=callback)
        self.assertEqual(result['results'][0]['status'],'NATIVE_CANDIDATE_INDEX_COMPLETE')
        self.assertEqual(len(access.calls),1)
        self.assertEqual([p['method'] for p in access.calls[0]],['eth_getTransactionByHash','eth_getBlockByNumber'])
        self.assertEqual(a.read(path)['point_cache_admission']['audit_ref']['path'],'synthetic_admission.json')

    def test_callback_exception_saves_same_need_with_zero_point_dispatch(self):
        access,path,_=self.worker()
        def callback(*args):raise RuntimeError('exact legacy index unavailable')
        with patch.object(a,'cached_member',side_effect=AssertionError('Point cache must not be touched after callback failure')):
            result=a.acquire(self.w,'plan.json',access,clock=lambda:1000,point_cache_admitter=callback)
        state=a.read(path)
        self.assertEqual(access.calls,[]);self.assertEqual(state['need'],self.need)
        self.assertEqual(result['binding_calls_this_invocation'],0)
        self.assertTrue(any(f.get('point_network_dispatched') is False for f in state['failures']))
        self.assertFalse((path.parent/'binding_call.json').exists())

    def test_callback_nonserializable_result_pauses_without_point_dispatch(self):
        access,path,_=self.worker()
        a.acquire(self.w,'plan.json',access,clock=lambda:1000,point_cache_admitter=lambda *args:{'invalid':{1,2}})
        self.assertEqual(access.calls,[]);self.assertNotIn('point_cache_admission',a.read(path))

    def test_same_closed_chain_callback_runs_once_across_resume(self):
        access,path,_=self.worker();callbacks=[]
        def callback(*args):callbacks.append(args);return {'audit_id':'synthetic-exact-admission'}
        first=a.acquire(self.w,'plan.json',access,clock=lambda:1000,maximum_binding_calls=1,point_cache_admitter=callback)
        self.assertEqual(first['results'][0]['status'],'SCHEDULING_BINDING_BOUND_PAUSED')
        second=a.acquire(self.w,'plan.json',access,clock=lambda:1000,point_cache_admitter=callback)
        self.assertEqual(second['results'][0]['status'],'NATIVE_CANDIDATE_INDEX_COMPLETE')
        self.assertEqual(len(callbacks),1)


if __name__=='__main__':unittest.main()
