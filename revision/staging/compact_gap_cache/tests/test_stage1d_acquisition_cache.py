"""Actual Stage1D CachedIntervals path, using only synthetic saved responses."""
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from collector import Collector,Event,NATIVE,Scope
from stage1d_acquisition import CachedIntervals

DAY=86400
BASE=1700000000
A,B,S,X=['0x'+str(i)*40 for i in range(1,5)]

def event(i,sender,recipient,day):
    tx='0x'+format(i,'064x')
    return Event('eip155:1:tx:'+tx+':top',tx,sender,recipient,NATIVE,100,
                 100+int(day*DAY),0,BASE+int(day*DAY))

def scope(mode='REFERENCE_FULL',query='synthetic:q',start_day=0):
    return Scope(query,'synthetic',100+start_day*DAY,100+108*DAY,
                 BASE+start_day*DAY,BASE+108*DAY,6,
                 None if mode=='REFERENCE_FULL' else 90*DAY,mode)


class AcquisitionCacheTests(unittest.TestCase):
    def save(self,root,name,start_day,end_day,events=(),*,complete=True,start_block=None,end_block=None):
        folder=Path(root)/'derived/stage1d/intervals';folder.mkdir(parents=True,exist_ok=True)
        ep=folder/(name+'.events.json');ep.write_text(json.dumps([asdict(e) for e in events]))
        record={'evidence_id':name,'addresses':[A], 'start_block':100 if start_block is None else start_block,
                'end_block':100+108*DAY if end_block is None else end_block,
                'start_time':BASE+start_day*DAY,'end_time':BASE+end_day*DAY,
                'complete':complete,'normalization_gaps':[],
                'events_path':ep.relative_to(root).as_posix(),
                'events_sha256':hashlib.sha256(ep.read_bytes()).hexdigest(),
                'query_id_at_acquisition':'synthetic:old','scope_id_at_acquisition':'synthetic:old-scope'}
        (folder/(name+'.coverage.json')).write_text(json.dumps(record))

    def fetch(self,p,*,start_day=0,end_day=108,start_block=100):
        return p.fetch_interval(A,NATIVE,start_block,100+108*DAY,start_time=BASE+start_day*DAY,
                                end_time=BASE+end_day*DAY,global_end_time=BASE+108*DAY)

    def test_day95_full_and90_actual_collector(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            outgoing=event(2,A,S,95);self.save(tmp,'full',0,108,[outgoing])
            labels=lambda a:{'kind':'SERVICE' if a==S else 'UNKNOWN'}
            full=Collector(CachedIntervals(tmp),labels).run(scope(),event(1,X,A,0))
            reduced=Collector(CachedIntervals(tmp),labels).run(scope('ARRIVAL_90D'),event(1,X,A,0))
            self.assertEqual(2,len(full.candidate_events));self.assertEqual(1,len(reduced.candidate_events))

    def test_old90_success_only_enqueues_missing_tail(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            self.save(tmp,'old90',0,90)
            provider=CachedIntervals(tmp);result=self.fetch(provider)
            self.assertFalse(result.complete);self.assertEqual(1,len(provider.pending))
            self.assertEqual(BASE+90*DAY+1,provider.pending[0]['start_time'])
            self.assertEqual(BASE+108*DAY,provider.pending[0]['end_time'])

    def test_crossquery_earlier_start_only_missing_rectangles(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            old_start=100+20*DAY
            self.save(tmp,'later-source',20,108,start_block=old_start)
            provider=CachedIntervals(tmp);provider.bind_scope(scope(query='synthetic:earlier'))
            result=self.fetch(provider)
            self.assertFalse(result.complete);self.assertEqual(2,len(provider.pending))
            for gap in provider.pending:
                self.assertFalse(gap['end_block']>=old_start and gap['end_time']>=BASE+20*DAY
                                 and gap['start_block']<=100+108*DAY and gap['start_time']<=BASE+108*DAY)
            self.assertEqual('synthetic:earlier',result.coverage[0]['query_id'])
            provider2=CachedIntervals(tmp);provider2.bind_scope(scope(query='synthetic:later',start_day=20))
            self.assertTrue(self.fetch(provider2,start_day=20,start_block=old_start).complete)
            self.assertEqual([],provider2.pending)

    def test_full_union_and_duplicate_capacity(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            duplicate=event(2,A,S,80)
            self.save(tmp,'first',0,90,[duplicate]);self.save(tmp,'second',70,108,[duplicate])
            provider=CachedIntervals(tmp);result=self.fetch(provider)
            self.assertTrue(result.complete);self.assertEqual([],provider.pending)
            self.assertEqual(1,len(result.events))

    def test_partial_cache_is_not_empty_success(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            self.save(tmp,'partial',0,108,complete=False)
            provider=CachedIntervals(tmp);result=self.fetch(provider)
            self.assertFalse(result.complete);self.assertEqual(BASE,provider.pending[0]['start_time'])

    def test_gap_second_and_block_are_not_inferred_covered(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            self.save(tmp,'first',0,108,end_block=999)
            self.save(tmp,'second',0,108,start_block=1001)
            provider=CachedIntervals(tmp);self.fetch(provider)
            self.assertEqual(1000,provider.pending[0]['start_block'])
            self.assertEqual(1000,provider.pending[0]['end_block'])

    def test_crossquery_conflict_invalidates_capacity(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            outgoing=event(2,A,S,80)
            self.save(tmp,'one',0,108,[outgoing]);self.save(tmp,'two',0,108,[replace(outgoing,amount_raw=101)])
            result=self.fetch(CachedIntervals(tmp))
            self.assertFalse(result.complete);self.assertTrue(result.fact_conflicts);self.assertEqual([],result.events)

    def test_global_end_is_applied_to_coverage_and_content(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            self.save(tmp,'whole',0,108,[event(2,A,S,95)])
            result=CachedIntervals(tmp).fetch_interval(A,NATIVE,100,100+108*DAY,
                start_time=BASE,end_time=BASE+108*DAY,global_end_time=BASE+90*DAY)
            self.assertTrue(result.complete);self.assertEqual([],result.events)
            self.assertEqual(BASE+90*DAY,result.coverage[0]['end_time'])

    def test_changed_content_refused(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            self.save(tmp,'whole',0,108)
            (Path(tmp)/'derived/stage1d/intervals/whole.events.json').write_text('[{}]')
            with self.assertRaises(ValueError):CachedIntervals(tmp)


if __name__=='__main__':unittest.main()
