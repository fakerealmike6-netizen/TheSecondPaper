"""Synthetic actual-cache/collector controls; no production data or requests."""
from collections import Counter
from dataclasses import asdict
import copy,hashlib,json,pathlib,tempfile,time,tracemalloc,unittest
from unittest.mock import patch

import stage1d_acquisition as acquisition
import stage1d_batch_binding_route as route
import stage1d_bq_context_prepare as h
from collector import Collector,Event,Scope,NATIVE
from stage1d_gap_sequence import as_gap_sequence,gap_count,iter_gaps,serialize_gaps
import test_stage1d_batch_binding_route as fixtures

A,B,X=['0x'+str(i)*40 for i in (1,2,3)]
MATRIX_MEASUREMENTS={}

def ev(i,sender=A,recipient=B):
    return Event('synthetic:'+str(i),'0x'+format(i,'064x'),sender,recipient,NATIVE,
                 1,100 if i==0 else 101,i,1000+i)

def scoped():return Scope('synthetic:cache','synthetic',100,1000,1000,2000,2,None,'REFERENCE_FULL')

class CompactGapCacheTests(unittest.TestCase):
    def scratch(self):
        folder=tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).resolve().parent)
        work=pathlib.Path(folder.name)
        assert work.resolve().is_relative_to(pathlib.Path(__file__).resolve().parent)
        self.addCleanup(folder.cleanup)
        return work

    def partial_import(self,work,count=39,gap_count_value=128):
        # Only normalization is a controlled seam. Import serialization,
        # SHA/proof/record verification and CachedIntervals are real entry code.
        needs=[dict(address=B,start_block=100,end_block=1000,start_time=1000,end_time=1500+i,
                    query_id='synthetic:cache',scope_id='synthetic:scope') for i in range(count)]
        h.save(work/'PREPARATION.json',{'need_rectangles':needs})
        h.save(work/'job.json',{'synthetic':'explicit normalization seam'})
        gaps=[{'reason':'ROOT_EQUIVALENCE_UNRESOLVED_GAP','tx_hash':'synthetic:'+str(i),
               'root_equivalence_requests':[{'method':'synthetic-only','params':[i]}]} for i in range(gap_count_value)]
        result={'events':[],'rows':[],'gaps':gaps,'root_bindings':[],
                'validated':{'fee_gaps':[]},'native_complete':False,'full_context_claimed':False}
        with patch.object(route,'_result',return_value=result):
            receipt=route.import_completed(work,'PREPARATION.json',{'synthetic-spec':'job.json'})
        return result,receipt

    def test_verified_plain_base_and_frozen_sequence_intern_once(self):
        work=self.scratch();result,receipt=self.partial_import(work,count=115)
        with patch.object(route,'_result',return_value=result) as validation, \
             patch.object(acquisition,'as_gap_sequence',wraps=acquisition.as_gap_sequence) as freezes:
            provider=acquisition.CachedIntervals(work)
        self.assertEqual(1,validation.call_count)
        self.assertEqual(1,freezes.call_count)
        self.assertEqual(1,len({id(r['normalization_gaps']) for r in provider.records}))
        self.assertEqual(1,len({id(r['_normalization_gap_sequence']) for r in provider.records}))
        self.assertEqual(result['gaps'],provider.records[0]['normalization_gaps'])
        self.assertTrue(all(r['complete'] is False for r in provider.records))
        # A changed later record is not rescued by the first verified base.
        path=work/receipt['coverage_records'][-1]['path'];record=h.read(path)
        record['normalization_gaps'][0]['tx_hash']='synthetic:tampered'
        path.write_text(json.dumps(record),encoding='utf8')  # Deliberate synthetic corruption.
        with patch.object(route,'_result',return_value=result),self.assertRaises(ValueError):
            acquisition.CachedIntervals(work)

    def test_actual_bq_pages_proof_and_cached_init_without_normalizer_mock(self):
        f=fixtures.BatchBindingTests();f.setUp();self.addCleanup(f.doCleanups)
        assert f.w.resolve().is_relative_to(pathlib.Path(__file__).resolve().parents[1])
        document=h.read(f.w/'NEEDS.json')
        document['need_rectangles']=[dict(f.need,end_time=f.need['end_time']+i) for i in range(3)]
        (f.w/'NEEDS.json').write_text(json.dumps(document),encoding='utf8');manifest=f.prepare()
        rows=f.family();rows[1]['trace_address']=None
        _,states=f.saved_export(manifest,rows)
        receipt=route.import_completed(f.w,'batch/PREPARATION.json',states)
        self.assertEqual('BATCH_BINDING_PARTIAL_WITH_EXPLICIT_GAPS',receipt['status'])
        provider=acquisition.CachedIntervals(f.w)
        self.assertEqual(3,len(provider.records))
        self.assertEqual(1,len({id(r['normalization_gaps']) for r in provider.records}))
        out=provider.fetch_interval(f.need['address'],NATIVE,100,1000,
            start_time=f.need['start_time'],end_time=f.need['end_time'],global_end_time=f.scope.end_time)
        expected=[{'reason':'UNCOLLECTED_ADDRESS_INTERVAL',**r} for r in provider.pending]
        expected += [g for r in provider.records for g in r['normalization_gaps'] if g not in expected]
        self.assertEqual(expected,list(iter_gaps(out.gaps)))
        self.assertFalse(out.complete)

    def test_million_logical_global_gap_matrix_stays_compact(self):
        work=self.scratch();result,_=self.partial_import(work)
        folder=work/'derived/stage1d/intervals';events=[asdict(ev(i)) for i in range(1,301)]
        ep=folder/'entry.events.json';h.save(ep,events)
        h.save(folder/'entry.coverage.json',dict(evidence_id='synthetic:entry',addresses=[A],asset=NATIVE,
            start_block=100,end_block=1000,start_time=1000,end_time=2000,complete=True,normalization_gaps=[],
            events_path=ep.relative_to(work).as_posix(),events_sha256=h.sha(ep)))
        with patch.object(route,'_result',return_value=result):provider=acquisition.CachedIntervals(work)
        one=provider.fetch_interval(B,NATIVE,101,1000,start_time=1001,end_time=2000,global_end_time=2000)
        self.assertEqual(1+39*128,gap_count(one.gaps))
        self.assertLess(len(json.dumps(serialize_gaps(one.gaps))),100000)
        started=time.perf_counter();tracemalloc.start()
        try:
            out=Collector(provider,lambda a:{'kind':'UNKNOWN','status':'LOCAL_FROZEN'}).run(scoped(),ev(0,X,A))
            wire=out.to_dict()
            serialized=json.dumps(wire['gaps'])
            _,peak=tracemalloc.get_traced_memory()
        finally:tracemalloc.stop()
        MATRIX_MEASUREMENTS.update(logical_gaps=gap_count(out.gaps),gap_wire_bytes=len(serialized.encode()),
            peak_traced_bytes=peak,wall_seconds=time.perf_counter()-started,arrivals=300,
            duplicate_matching_records=39,global_base_gaps=128)
        # Even empty per-occurrence dictionaries for 1.49M rows would exceed
        # this bound; physical templates below independently prove no flat copy.
        self.assertLess(peak,64*1024*1024)
        self.assertEqual(301,len(out.candidate_events))
        self.assertEqual(301,len(out.states))
        self.assertEqual(300*(1+39*128),gap_count(out.gaps))
        # This would contain almost 1.5 million state-bound dictionaries flat.
        self.assertLess(len(serialized),4*1024*1024)
        self.assertLessEqual(sum(len(t['items']) for t in wire['gaps']['templates'].values()),128+300)
        self.assertLessEqual(len(wire['gaps']['segments']),300*40)
        self.assertEqual(gap_count(out.gaps),gap_count(wire['gaps']))
        first=next(iter_gaps(wire['gaps']))
        self.assertEqual(B,first['address']);self.assertEqual('synthetic:1',first['arrival_event_id'])
        self.assertFalse(any(r['complete'] for r in out.coverage if r['address']==B))

    def test_suffix_freezes_values_and_keeps_original_membership_semantics(self):
        base=[{'reason':'R','value':[True]}]
        kept={'reason':'R','value':[2]}
        rows=[{'normalization_gaps':[{'reason':'R','value':[1]},kept,kept]}]
        seq=acquisition._normalization_gap_suffix(base,rows)
        self.assertEqual([kept,kept],list(iter_gaps(seq)))
        kept['value'][0]=9
        self.assertEqual([{'reason':'R','value':[2]}]*2,list(iter_gaps(seq)))
        item=next(iter_gaps(seq));item['value'][0]=99
        self.assertEqual([{'reason':'R','value':[2]}]*2,list(iter_gaps(seq)))

if __name__=='__main__':unittest.main()
