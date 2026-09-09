"""Only synthetic envelopes and an injected no-I/O importer; no real index/DB."""
from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import Mock,patch
import contextlib,io,json,unittest
import execute_current_transfers as driver
import stage1d_transfers_acquisition as a
from stage1d_alchemy_transfers import METHOD
from test_transfers_timestamp_mapping import MappingTests


class Tests(unittest.TestCase):
    def setUp(self):
        self.f=MappingTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.w=self.f.w;self.q=self.f.q
        self.importer=Mock()
        self.importer.apply_many.side_effect=lambda q,needs:{'results':[{'status':'CURRENT_CACHE_SUCCESS'} for n in needs],
            'imported_storage_added_bytes':0}
        self.callback=driver.CurrentIndexPointCacheAdmission(self.w,self.importer,
            {'path':'controlled/index.sqlite','sha256':'a'*64,'bytes':1},a)

    def save(self,state):
        path=self.w/'private/stage1d_transfers_acquisition/needs'/a.digest(state['need'])/'state.json'
        a.atomic_json(path,state);return path

    def mapped(self,empty=False,prefix=False,closed_superset=False):
        need=dict(self.f.need)
        if empty:need.update(start_time=self.f.f.scope.start_time+1,end_time=self.f.f.scope.start_time+5)
        if prefix:need['start_time']=self.f.f.scope.start_time
        state=self.f.state(need);path=self.save(state)
        rows=[]
        if prefix:
            for block in ((100,104,112) if closed_superset else (100,104,106)):
                row=self.f.f.row();row.update(blockNum=hex(block),uniqueId='legacy-'+str(block),hash='0x'+format(block,'064x'),
                    metadata={'blockTimestamp':datetime.fromtimestamp(self.f.f.scope.start_time+(block-100)*12,timezone.utc).isoformat()})
                rows.append(row)
            chain=a.TransferPageChain(need);plan=chain.next_plan(0)
            chain.append(plan,self.f.f.member(plan,{'transfers':rows,'pageKey':'prior-cursor'}),received_at_seconds=0,requested_at_seconds=0)
            state['pages']=chain.summary()['pages'];self.save(state)
        with patch.object(a,'cached_member',side_effect=lambda work,p:None if p['method']==METHOD else self.f.member(p)):
            chain=a._ensure_timestamp_mapping(self.w,self.q,state,path,self.f.access)
        if not empty and not chain.closed:
            plan=chain.next_plan(100)
            if not prefix:
                row=self.f.f.row();row.update(blockNum=hex(105),metadata={'blockTimestamp':datetime.fromtimestamp(need['start_time'],timezone.utc).isoformat()})
                selected=[row]
            else:selected=[rows[-1]]
            chain.append(plan,self.f.f.member(plan,{'transfers':selected}),received_at_seconds=100,requested_at_seconds=100)
            state['pages']=chain.summary()['pages']
        self.save(state)
        return state,a.restore_state_chain(self.w,state)

    def test_closed_empty_timestamp_is_accepted_without_natural_exhaustion(self):
        state,chain=self.mapped(empty=True)
        self.assertTrue(chain.closed);self.assertFalse(chain.summary()['page_chain_naturally_exhausted'])
        ref=self.callback(self.q,state['need'],chain.summary())
        result=a.read(a.checked(self.w,ref));self.assertEqual(result['statuses'],{})
        self.assertFalse(result['context_complete']);self.importer.apply_many.assert_not_called()
        self.assertEqual(self.f.calls,[])

    def test_closed_prefix_exact_tx_block_demands_and_source_hash(self):
        state,chain=self.mapped(prefix=True)
        self.assertTrue(chain.closed);self.assertIn('legacy_prefix_summary',chain.summary())
        ref=self.callback(self.q,state['need'],chain.summary())
        result=a.read(a.checked(self.w,ref));demands=a.read(a.checked(self.w,result['needed_points']))
        self.assertEqual(len(demands['needs']),9)
        self.assertEqual({n['expected_block'] for n in demands['needs']},{100,104,106})
        for n in demands['needs']:
            self.assertEqual(n['query_id'],self.q['query_id']);self.assertEqual(n['scope_hash'],self.q['scope_hash'])
            origin=a.read(a.checked(self.w,n['evidence_refs'][0]))
            source=a.checked(self.w,origin['state_snapshot'])
            self.assertEqual(source.read_bytes(),self.save(state).read_bytes())
            self.assertEqual(origin['index'],chain.summary())
        self.assertEqual(self.f.calls,[])

    def test_closed_legacy_superset_with_rows_does_not_require_natural_exhaustion(self):
        state,chain=self.mapped(prefix=True,closed_superset=True)
        self.assertTrue(chain.closed);self.assertFalse(chain.summary()['page_chain_naturally_exhausted'])
        ref=self.callback(self.q,state['need'],chain.summary())
        result=a.read(a.checked(self.w,ref));demands=a.read(a.checked(self.w,result['needed_points']))
        self.assertEqual(len(demands['needs']),6)
        self.assertEqual({n['expected_block'] for n in demands['needs']},{100,104})
        self.assertEqual(self.f.calls,[])

    def test_same_closed_chain_admission_resumes_without_reimport(self):
        state,chain=self.mapped();first=self.callback(self.q,state['need'],chain.summary())
        self.assertEqual(self.importer.apply_many.call_count,1)
        state['point_cache_admission']={'audit_ref':first,'chain_sha256':a.digest(chain.summary())};self.save(state)
        again=self.callback(self.q,state['need'],chain.summary())
        self.assertTrue(again['cache_reused']);self.assertEqual(first['sha256'],again['sha256'])
        self.assertEqual(self.importer.apply_many.call_count,1)

    def test_open_chain_changed_summary_and_wrong_scope_reject_before_import(self):
        state,chain=self.mapped();summary=chain.summary()
        bad=deepcopy(summary);bad['needed_range']['end_block']-=1
        with self.assertRaises(ValueError):self.callback(self.q,state['need'],bad)
        q=dict(self.q,scope_hash='invented')
        with self.assertRaises(ValueError):self.callback(q,state['need'],summary)
        state['pages']=[];self.save(state)
        with self.assertRaisesRegex(ValueError,'closed'):self.callback(self.q,state['need'],summary)
        self.importer.apply_many.assert_not_called()

    def test_changed_original_raw_cannot_be_admitted(self):
        state,chain=self.mapped();summary=chain.summary()
        raw=self.w/state['pages'][0]['artifact_path'];raw.write_bytes(raw.read_bytes()+b' ')
        with self.assertRaises(ValueError):self.callback(self.q,state['need'],summary)
        self.importer.apply_many.assert_not_called()

    def test_default_is_review_only_and_binding_bound_cannot_exceed_200(self):
        with patch.object(driver,'run_root',side_effect=AssertionError('No real execution')),contextlib.redirect_stdout(io.StringIO()) as output:
            driver.main([])
        result=json.loads(output.getvalue());self.assertEqual(result['query'],'txphish_src002')
        self.assertEqual(result['status'],'REVIEW_ONLY_NO_IO_OR_NETWORK')
        self.assertEqual(result['sealed_index']['sha256'],driver.INDEX_SHA256)
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):
            driver.main(['--maximum-binding-selectors','201'])


if __name__=='__main__':unittest.main()
