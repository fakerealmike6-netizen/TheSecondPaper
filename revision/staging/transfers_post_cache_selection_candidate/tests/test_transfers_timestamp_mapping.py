"""Original raw fixtures exercise physical requests, legacy reuse and empty W."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

CODE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(CODE/'src'), str(CODE/'tests')]
import test_transfers_acquisition as fixtures
import stage1d_transfers_acquisition as a
from stage1d_alchemy_transfers import METHOD, request_plan


class MappingTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.Tests(); self.f.setUp()
        self.addCleanup(self.f.doCleanups); self.addCleanup(self.f.tearDown)
        self.w = self.f.w; self.q = self.f.q
        self.need = dict(self.f.need, start_time=self.f.scope.start_time+60, end_time=self.f.scope.start_time+120)
        self.calls = []
        owner = self
        class Access:
            runtime = a.TransfersRuntime()
            w = owner.w
            def call_batch(self, plans, *args, **kw):
                owner.calls.extend(deepcopy(plans))
                return {'status':'COMPLETE','members':[owner.member(p) for p in plans]}
        self.access = Access()

    def member(self, plan):
        if plan['method'] == METHOD:
            return self.f.member(plan, {'transfers': []})
        block = int(plan['params'][0], 16)
        return self.f.member(plan, {'number':hex(block), 'timestamp':hex(self.f.scope.start_time+(block-100)*12),
            'hash':'0x'+format(block, '064x'), 'transactions':[]})

    def state(self, need=None):
        return {'version':a.VERSION,'need':deepcopy(need or self.need),'pages':[],'bindings':[],
            'failures':[],'route_dependencies':self.f.gate_refs,'plan_sources':[],'status':'READY'}

    def test_actual_transfer_bounds_and_every_page_keep_window(self):
        state = self.state(); path = self.w/'state.json'
        with patch.object(a, 'cached_member', return_value=None):
            chain = a._ensure_timestamp_mapping(self.w, self.q, state, path, self.access)
        plan = chain.next_plan(0)
        self.assertEqual((plan['params'][0]['fromBlock'],plan['params'][0]['toBlock']), ('0x69','0x6e'))
        self.assertEqual(state['need']['end_block'], 200)
        member = self.f.member(plan, {'transfers':[], 'pageKey':'next'})
        chain.append(plan, member, received_at_seconds=0, requested_at_seconds=0)
        next_plan = chain.next_plan(1)
        self.assertEqual(next_plan['params'][0]['fromBlock'], '0x69')
        self.assertEqual(next_plan['params'][0]['toBlock'], '0x6e')
        self.assertEqual(next_plan['params'][0]['pageKey'], 'next')
        with self.assertRaises(ValueError):
            chain.append(request_plan(self.need, page_key='next'), self.member(next_plan), received_at_seconds=1, requested_at_seconds=1)

    def test_cache_first_no_header_network_and_receiver_detects_changed_raw(self):
        state = self.state()
        with patch.object(a, 'cached_member', side_effect=lambda work, plan: None if plan['method']==METHOD else self.member(plan)):
            chain = a._ensure_timestamp_mapping(self.w,self.q,state,self.w/'state.json',self.access)
        self.assertEqual(self.calls, [])
        plan=chain.next_plan(0);chain.append(plan,self.member(plan),received_at_seconds=0,requested_at_seconds=0)
        state['pages']=chain.summary()['pages']
        result=a.emit_interval(self.w,state)
        record=a.read(self.w/result['coverage_record']['path'])
        self.assertTrue(record['complete']);self.assertEqual(record['end_block'],200)
        self.assertEqual(record['end_time'],self.need['end_time'])
        a.verify_interval_record(self.w,record)
        member=next(iter(state['timestamp_headers'].values()))
        (self.w/member['artifact_path']).parent.joinpath('response_body.bin').write_bytes(b'[]')
        with self.assertRaises(ValueError):a.verify_interval_record(self.w,record)

    def test_empty_gap_seconds_prove_coverage_without_transfers_call(self):
        need=dict(self.need,start_time=self.f.scope.start_time+1,end_time=self.f.scope.start_time+5)
        state=self.state(need)
        with patch.object(a,'cached_member',side_effect=lambda work,plan:None if plan['method']==METHOD else self.member(plan)):
            chain=a._ensure_timestamp_mapping(self.w,self.q,state,self.w/'state.json',self.access)
        self.assertTrue(chain.closed);self.assertTrue(chain.empty_time_intersection)
        self.assertFalse(chain.summary()['page_chain_naturally_exhausted'])
        self.assertEqual(chain.summary()['closure_basis'],'VERIFIED_EMPTY_TIMESTAMP_INTERSECTION')
        with self.assertRaises(ValueError):chain.next_plan(0)
        self.assertEqual(self.calls,[])
        result=a.emit_interval(self.w,state)
        self.assertTrue(result['complete']);self.assertEqual(result['events'],0)

    def test_complete_old_global_page_is_reused_without_repost(self):
        state=self.state();plan=request_plan(state['need']);member=self.f.member(plan,{'transfers':[]})
        old=a.TransferPageChain(state['need']);old.append(plan,member,received_at_seconds=0,requested_at_seconds=0)
        state['pages']=old.summary()['pages'];original=a.sha(self.w/member['artifact_path'])
        with patch.object(a,'cached_member',side_effect=AssertionError('Closed legacy index precedes header/cache discovery')):
            chain=a._ensure_timestamp_mapping(self.w,self.q,state,self.w/'state.json',self.access)
        self.assertTrue(chain.closed);self.assertEqual(self.calls,[])
        self.assertEqual(a.sha(self.w/member['artifact_path']),original)
        self.assertEqual(state['pages'][0]['plan'],plan)
        self.assertNotIn('timestamp_bracket',state)
        self.assertNotIn('prior_timestamp_mapping_state',state)
        self.assertTrue(a.emit_interval(self.w,state)['complete'])

    def test_old_prefix_reuses_finished_blocks_and_requeries_last_block(self):
        state=self.state(dict(self.need,start_time=self.f.scope.start_time))
        rows=[]
        for block in (100,104,106):
            row=self.f.row();row.update(blockNum=hex(block),uniqueId='legacy-'+str(block),hash='0x'+format(block,'064x'))
            row['metadata']={'blockTimestamp':datetime.fromtimestamp(self.f.scope.start_time+(block-100)*12,timezone.utc).isoformat()}
            rows.append(row)
        old=a.TransferPageChain(state['need']);plan=old.next_plan(0)
        old.append(plan,self.f.member(plan,{'transfers':rows,'pageKey':'old-cursor'}),received_at_seconds=0,requested_at_seconds=0)
        state['pages']=old.summary()['pages']
        with patch.object(a,'cached_member',side_effect=lambda work,p:self.member(p)):
            chain=a._ensure_timestamp_mapping(self.w,self.q,state,self.w/'state.json',self.access)
        next_plan=chain.next_plan(5000) # Does not reuse the expired old cursor.
        self.assertEqual(next_plan['params'][0]['fromBlock'],hex(106))
        self.assertEqual(next_plan['params'][0]['toBlock'],hex(110))
        self.assertNotIn('pageKey',next_plan['params'][0])
        self.assertEqual([int(r['blockNum'],16) for r in chain.rows],[100,104])
        with self.assertRaisesRegex(ValueError,'Legacy boundary fact missing'):
            chain.append(next_plan,self.f.member(next_plan,{'transfers':[]}),received_at_seconds=5000,requested_at_seconds=5000)
        chain.append(next_plan,self.f.member(next_plan,{'transfers':[rows[-1]]}),received_at_seconds=5000,requested_at_seconds=5000)
        self.assertTrue(chain.closed)
        state['pages']=chain.summary()['pages']
        rebuilt=a.restore_state_chain(self.w,state)
        self.assertEqual(rebuilt.summary(),chain.summary())
        self.assertEqual([int(r['blockNum'],16) for r in rebuilt.rows],[100,104,106])

    def test_failure_keeps_same_logical_need_and_no_transfer_dispatch(self):
        state=self.state();path=self.w/'state.json'
        with patch.object(a,'cached_member',return_value=None), \
             patch.object(self.access,'call_batch',return_value={'members':[{'status':'TEMPORARY_FAILURE','reason':'SYNTHETIC_TLS'}]}) as access:
            with self.assertRaisesRegex(ValueError,'TIMESTAMP_HEADER_BLOCKED_SAME_REQUEST_PRESERVED'):
                a._ensure_timestamp_mapping(self.w,self.q,state,path,self.access)
        saved=a.read(path)
        self.assertEqual(saved['need'],self.need)
        self.assertNotIn('timestamp_bracket',saved)
        self.assertEqual(saved['timestamp_header_selectors_submitted'],1)
        self.assertEqual(access.call_args.args[0],[{'method':'eth_getBlockByNumber','params':[hex(self.need['end_block']),False]}])
        self.assertEqual(saved['failures'][0]['phase'],'TIMESTAMP_BRACKET')

    def test_existing_exact_page_cache_precedes_all_new_header_requests(self):
        state=self.state();plan=request_plan(self.need)
        member=dict(self.f.member(plan,{'transfers':[]}),cache_hit=True)
        def cached(work,request):
            self.assertEqual(request,plan)
            return member
        with patch.object(a,'cached_member',side_effect=cached):
            chain=a._ensure_timestamp_mapping(self.w,self.q,state,self.w/'state.json',self.access)
        self.assertTrue(chain.closed);self.assertEqual(self.calls,[])
        self.assertNotIn('timestamp_bracket',state)
        self.assertEqual(state['pages'][0]['plan'],plan)

    def test_serial_worker_dispatches_actual_short_range_then_offline_resume(self):
        frontier={'reason':'INTERVAL_INCOMPLETE','state':{'address':self.need['address'],'asset':a.NATIVE,'depth':1,
            'protocol_context':'ordinary','arrival':{'block':self.need['start_block'],'timestamp':self.need['start_time'],
                'tx_index':0,'event_id':'synthetic-arrival'}}}
        boundary={'queries':[{'query_name':self.q['name'],'needed_ranges':[self.need],'frontier':[frontier]}]}
        a.atomic_json(self.w/'boundary.json',boundary)
        plan={'version':a.VERSION,'query_name':self.q['name'],'needs':[self.need],'source_needs':[self.need],
            'maximum_needs':20,'boundary_snapshot':a.ref(self.w,'boundary.json'),'route_dependencies':self.f.gate_refs}
        a.atomic_json(self.w/'plan.json',plan)
        with patch.object(a,'cached_member',return_value=None):
            result=a.acquire(self.w,'plan.json',self.access,clock=lambda:1000)
        self.assertEqual(result['results'][0]['status'],'NATIVE_CANDIDATE_INDEX_COMPLETE')
        transfers=[p for p in self.calls if p['method']==METHOD]
        self.assertEqual(len(transfers),1)
        self.assertEqual((transfers[0]['params'][0]['fromBlock'],transfers[0]['params'][0]['toBlock']),('0x69','0x6e'))
        self.assertEqual(result['results'][0]['need']['end_block'],200)
        count=len(self.calls)
        again=a.acquire(self.w,'plan.json',self.access,clock=lambda:1000)
        self.assertEqual(again['results'][0]['status'],'CURRENT_OR_ADMITTED_INTERVAL_CACHE_COMPLETE')
        self.assertEqual(len(self.calls),count)


if __name__=='__main__':unittest.main()
