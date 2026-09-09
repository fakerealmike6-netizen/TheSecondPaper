"""Synthetic current-boundary/cache differences; no provider, DB or real graph."""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import contextlib
import io
import json
import tempfile
import unittest
from unittest.mock import patch
from stage1d_window import missing_rectangles

import stage1d_transfers_acquisition as a
import test_transfers_acquisition as fixtures


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.f=fixtures.Tests();self.f.setUp()
        self.addCleanup(self.f.doCleanups);self.addCleanup(self.f.tearDown)
        self.w,self.q,self.scope=self.f.w,self.f.q,self.f.scope
        self.counter=0

    def boundary(self,count):
        needs=[];frontier=[]
        for n in range(count):
            need=dict(self.f.need,address='0x'+format(n+1,'040x'))
            needs.append(need)
            frontier.append({'reason':'INTERVAL_INCOMPLETE','state':{'depth':1,'address':need['address'],
                'asset':a.NATIVE,'protocol_context':'ordinary','arrival':{'block':self.scope.start_block,
                'timestamp':self.scope.start_time,'tx_index':n,'event_id':'synthetic-'+str(n)}}})
        a.atomic_json(self.w/'collection.json',{'synthetic':True})
        a.atomic_json(self.w/'labels.json',{'synthetic':True})
        entry={'query_name':self.q['name'],'query_id':self.q['query_id'],'scope_hash':self.q['scope_hash'],
            'collection_path':'collection.json','collection_sha256':a.sha(self.w/'collection.json'),
            'needed_ranges':needs,'frontier':frontier}
        boundary={'batch_freeze_sha256':a.sha(a.active_batch_path(self.w)),'queries':[entry],
                  'labels':[a.ref(self.w,'labels.json')]}
        a.atomic_json(self.w/'boundary.json',boundary)
        return entry

    def prepare(self,entry,*,maximum=20,missing=None,conflicts=()):
        self.counter+=1;calls=[]
        class Cache:
            def __init__(self,work):pass
            def bind_scope(self,scope):pass
            def fetch_interval(self,address,asset,start_block,end_block,**kw):
                calls.append(address)
                rect={'address':address,'asset':asset,'start_block':start_block,'end_block':end_block,'start_time':kw['start_time'],'end_time':kw['end_time']}
                rows=deepcopy(missing.get(address,[rect])) if missing is not None else [rect]
                rows=[dict(row,address=address,asset=asset) for row in rows]
                complement=missing_rectangles(rect,[dict(row,complete=True) for row in rows])
                coverage=dict(rect,uncovered_intervals=rows,verified_content_intervals=[dict(row,complete=True) for row in complement])
                return SimpleNamespace(fact_conflicts=list(conflicts),coverage=[coverage],complete=not rows)
        with patch('stage1d_acquisition.CachedIntervals',Cache),patch.object(a,'gate_dependencies',return_value=self.f.gate_refs):
            plan=a.prepare(self.w,self.w/'boundary.json',self.q['name'],'plans/'+str(self.counter),maximum=maximum)
        a._validate_selection(self.q,entry,plan)
        return plan,calls

    def test_real_verified_empty_first_twenty_reaches_twenty_first(self):
        entry=self.boundary(21);original=self.f.need
        for need in entry['needed_ranges'][:20]:
            self.f.need=need
            emitted=a.emit_interval(self.w,self.f.state([]))
            self.assertTrue(a.read(self.w/emitted['coverage_record']['path'])['complete'])
        self.f.need=original
        with patch.object(a,'gate_dependencies',return_value=self.f.gate_refs):
            plan=a.prepare(self.w,self.w/'boundary.json',self.q['name'],'actual-verified',maximum=20)
        self.assertEqual(plan['needs'],entry['needed_ranges'][20:])
        self.assertEqual(len(plan['source_needs']),21)
        self.assertEqual(plan['ordered_source_count'],21)
        self.assertEqual(plan['ordered_source_sha256'],a.digest(a._ordered_needs(self.q,entry)))
        a._validate_selection(self.q,entry,plan)

    def test_twenty_limit_and_next_cache_state_progress_same_graph(self):
        entry=self.boundary(45);first,calls=self.prepare(entry)
        self.assertEqual(first['needs'],entry['needed_ranges'][:20]);self.assertEqual(len(calls),20)
        success={n['address']:[] for n in first['needs']}
        second,calls=self.prepare(entry,missing=success)
        self.assertEqual(second['needs'],entry['needed_ranges'][20:40]);self.assertEqual(len(calls),40)
        self.assertEqual(first['ordered_source_sha256'],second['ordered_source_sha256'])
        self.assertEqual(first['original_boundary_sha256'],second['original_boundary_sha256'])
        # New plan identity cannot alter the exact physical request identity.
        self.assertEqual(a.digest(second['needs'][0]),a.digest(entry['needed_ranges'][20]))

    def test_partial_and_failed_uncovered_ranges_remain_in_original_order(self):
        entry=self.boundary(3);need=entry['needed_ranges'][0]
        left={k:need[k] for k in a.FIELDS};left['end_block']=120
        right=dict(left,start_block=150,end_block=need['end_block'])
        missing={need['address']:[left,right],entry['needed_ranges'][1]['address']:[]}
        plan,calls=self.prepare(entry,missing=missing)
        self.assertEqual(plan['needs'],[dict(need,**left),dict(need,**right),entry['needed_ranges'][2]])
        self.assertEqual(len(calls),3)  # Failed/no-success third need is still missing.

    def test_one_limit_preserves_first_difference_and_stops_scan(self):
        entry=self.boundary(3);need=entry['needed_ranges'][1]
        a1={k:need[k] for k in a.FIELDS};a1['end_block']=120
        a2=dict(a1,start_block=150,end_block=200)
        plan,calls=self.prepare(entry,maximum=1,missing={entry['needed_ranges'][0]['address']:[],need['address']:[a1,a2]})
        self.assertEqual(plan['needs'],[dict(need,**a1)]);self.assertEqual(len(calls),2)

    def test_all_success_is_empty_only_after_complete_scan(self):
        entry=self.boundary(25);plan,calls=self.prepare(entry,missing={n['address']:[] for n in entry['needed_ranges']})
        self.assertEqual(plan['needs'],[]);self.assertEqual(len(calls),25)

    def test_conflict_refuses_plan_without_skipping_source(self):
        entry=self.boundary(2)
        with self.assertRaisesRegex(ValueError,'physical fact conflict'):
            self.prepare(entry,conflicts=[{'reason':'SYNTHETIC_CONFLICT'}])
        self.assertFalse((self.w/'plans/1/plan.json').exists())

    def test_bound_validation_rejects_bad_limits(self):
        entry=self.boundary(1)
        for bound in (0,21,True):
            with self.subTest(bound=bound),self.assertRaises(ValueError):self.prepare(entry,maximum=bound)

    def test_new_policy_full_hash_prefix_and_difference_tampering_rejected(self):
        entry=self.boundary(24);missing={n['address']:[] for n in entry['needed_ranges'][:21]}
        original,_=self.prepare(entry,maximum=1,missing=missing)
        def assert_bad(edit):
            plan=deepcopy(original);edit(plan)
            with self.assertRaises(ValueError):a._validate_selection(self.q,entry,plan)
        edits=[lambda p:p.update(selection_policy='invented'),lambda p:p.pop('selection_policy'),
          lambda p:p.update(ordered_source_sha256='0'*64),lambda p:p.update(ordered_source_count=23),
          lambda p:p['source_needs'].reverse(),lambda p:p['source_needs'].pop(0),
          lambda p:p['cache_checks'].pop(0),lambda p:p['cache_checks'][0].update(need=p['source_needs'][1]),
          lambda p:p.update(needs=[]),lambda p:p['needs'][0].update(end_block=201),
          lambda p:p['cache_checks'][-1]['missing'][0].update(end_block=201)]
        for edit in edits:assert_bad(edit)
        # Even coherently edited saved coverage cannot make a source-escaping gap valid.
        def escape(p):
            p['cache_checks'][-1]['missing'][0]['end_block']=201
            p['cache_checks'][-1]['coverage'][0]['uncovered_intervals']=deepcopy(p['cache_checks'][-1]['missing'])
        assert_bad(escape)

    def test_difference_reordering_and_incomplete_or_excess_scan_rejected(self):
        entry=self.boundary(3);plan,_=self.prepare(entry)
        bad=deepcopy(plan);bad['needs'].reverse()
        with self.assertRaises(ValueError):a._validate_selection(self.q,entry,bad)
        bad=deepcopy(plan);bad['source_needs'].pop();bad['cache_checks'].pop();bad['needs'].pop()
        with self.assertRaisesRegex(ValueError,'stopped before'):a._validate_selection(self.q,entry,bad)
        short,_=self.prepare(entry,maximum=1)
        short['source_needs'].append(plan['source_needs'][1]);short['cache_checks'].append(plan['cache_checks'][1])
        with self.assertRaisesRegex(ValueError,'continued beyond'):a._validate_selection(self.q,entry,short)

    def test_coherently_reordered_missing_coverage_and_needs_rejected(self):
        entry=self.boundary(1);need=entry['needed_ranges'][0]
        first={k:need[k] for k in a.FIELDS};first['end_block']=120
        last=dict(first,start_block=150,end_block=200)
        plan,_=self.prepare(entry,missing={need['address']:[first,last]})
        plan['cache_checks'][0]['missing'].reverse()
        plan['cache_checks'][0]['coverage'][0]['uncovered_intervals']=deepcopy(plan['cache_checks'][0]['missing'])
        plan['needs'].reverse()
        with self.assertRaisesRegex(ValueError,'canonical exact'):a._validate_selection(self.q,entry,plan)

    def test_forged_empty_or_partial_cache_checks_rejected_in_actual_entry_before_dispatch(self):
        entry=self.boundary(2)
        for count in (1,2):
            plan,_=self.prepare(entry,missing={n['address']:[] for n in entry['needed_ranges'][:count]})
            # This plan is internally coherent, but the actual verified cache
            # has NO successful coverage for the allegedly skipped portions.
            path=self.w/('forged-'+str(count)+'.json');a.atomic_json(path,plan)
            class Access:
                w=self.w;runtime=a.TransfersRuntime()
                def call_batch(self,*args,**kw):raise AssertionError('Must reject before network')
            with self.assertRaisesRegex(ValueError,'currently unproved source gap'):
                a.acquire(self.w,path,Access())
            self.assertFalse((self.w/'private/stage1d_transfers_acquisition/needs').exists())

    def test_new_cache_success_can_reduce_saved_difference_without_changing_plan(self):
        entry=self.boundary(1);plan,_=self.prepare(entry)
        path=self.w/'growing-cache.json';a.atomic_json(path,plan);before=path.read_bytes()
        self.f.need=entry['needed_ranges'][0];a.emit_interval(self.w,self.f.state([]))
        class Access:
            w=self.w;runtime=a.TransfersRuntime()
            def call_batch(self,*args,**kw):raise AssertionError('New success must prevent dispatch')
        result=a.acquire(self.w,path,Access())
        self.assertEqual(result['results'][0]['status'],'CURRENT_OR_ADMITTED_INTERVAL_CACHE_COMPLETE')
        self.assertEqual(path.read_bytes(),before)

    def test_skipped_source_current_conflict_is_not_hidden(self):
        entry=self.boundary(1);plan,_=self.prepare(entry,missing={entry['needed_ranges'][0]['address']:[]})
        path=self.w/'conflict.json';a.atomic_json(path,plan)
        class Conflict:
            def __init__(self,work):pass
            def bind_scope(self,scope):pass
            def fetch_interval(self,*args,**kw):return SimpleNamespace(fact_conflicts=[{'reason':'SYNTHETIC_CONFLICT'}])
        with patch('stage1d_acquisition.CachedIntervals',Conflict),self.assertRaisesRegex(ValueError,'physical fact conflict'):
            a.acquire(self.w,path,SimpleNamespace(w=self.w,runtime=a.TransfersRuntime()))

    def test_missing_scope_or_expandable_role_source_still_refused(self):
        entry=self.boundary(1)
        for field,value in [('reason','USER_REQUESTED_BRANCH_HOLD'),('reason','PROTOCOL_BOUNDARY')]:
            bad=deepcopy(entry);bad['frontier'][0][field]=value
            with self.assertRaises(ValueError):a._ordered_needs(self.q,bad)
        bad=deepcopy(entry);bad['needed_ranges'][0]['scope_hash']='other-query'
        with self.assertRaises(ValueError):a._ordered_needs(self.q,bad)

    def test_legacy_source_prefix_unchanged_and_old_plan_resumes_in_real_entry(self):
        entry=self.boundary(25);legacy=a.select_needs(self.q,entry,20)
        self.assertEqual(legacy,entry['needed_ranges'][:20])
        a.atomic_json(self.w/'oldplan.json',{'version':a.VERSION,'query_name':self.q['name'],
            'needs':legacy[:1],'source_needs':legacy,'maximum_needs':20,
            'boundary_snapshot':a.ref(self.w,'boundary.json'),'route_dependencies':self.f.gate_refs})
        before=(self.w/'oldplan.json').read_bytes()
        class Complete:
            def __init__(self,work):pass
            def bind_scope(self,scope):pass
            def fetch_interval(self,*args,**kw):return SimpleNamespace(complete=True)
        access=SimpleNamespace(w=self.w,runtime=a.TransfersRuntime())
        with patch('stage1d_acquisition.CachedIntervals',Complete):result=a.acquire(self.w,'oldplan.json',access)
        self.assertEqual(result['results'][0]['status'],'CURRENT_OR_ADMITTED_INTERVAL_CACHE_COMPLETE')
        self.assertEqual((self.w/'oldplan.json').read_bytes(),before)


if __name__=='__main__':unittest.main()
