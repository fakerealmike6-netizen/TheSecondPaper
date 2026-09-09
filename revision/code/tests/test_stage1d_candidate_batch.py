import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from collector import NATIVE,Scope
from stage1d_acquisition import CachedIntervals
from stage1d_candidate_batch import build_sql,rectangles,acquire

A='0x'+'a'*40;B='0x'+'b'*40;Z='0x'+'c'*40
T=1700000000


def rect(address=A,lo=10,hi=12,t0=T,t1=T+20):
    return dict(address=address,asset=NATIVE,start_block=lo,end_block=hi,start_time=t0,end_time=t1)


class ExactRectangleBatch(unittest.TestCase):
    def test_unequal_windows_preserve_all_three_event_kind_guards(self):
        sql,chosen=build_sql([rect(),rect(B,20,22,T+86400,T+86420)])
        self.assertEqual(len(chosen),2)
        self.assertEqual(sql.count('SELECT 1 FROM request_windows w'),3)
        self.assertIn('t.block_number BETWEEN w.first_block AND w.last_block',sql)
        self.assertIn('e.evt_block_time BETWEEN w.first_time AND w.last_time',sql)
        self.assertIn('NOT EXISTS (',sql);self.assertIn('a.success=false',sql)
        self.assertIn("SELECT 'erc20'",sql);self.assertIn('t.gas_used AS VARCHAR',sql)
        self.assertNotIn(' LIMIT ',sql)

    def test_same_address_disjoint_holes_are_not_envelope(self):
        sql,chosen=build_sql([rect(),rect(A,20,22,T+30,T+40)])
        self.assertEqual(len(chosen),2)
        self.assertIn('request_windows',sql)
        self.assertEqual(len(rectangles([rect(),rect()])),1)

    def test_bad_limits_asset_and_ranges_rejected(self):
        for cap in (0,33,True):
            with self.assertRaises(ValueError):rectangles([rect()],cap)
        for r in (dict(rect(),asset='erc20:eip155:1:'+A),rect(lo=20,hi=10),rect(t0=T+1,t1=T)):
            with self.assertRaises(ValueError):build_sql([r])

    def test_partition_predicates_cover_cross_day_endpoints(self):
        sql,_=build_sql([rect(t1=T+86400)])
        self.assertIn("block_date BETWEEN DATE '2023-11-14' AND DATE '2023-11-15'",sql)
        self.assertIn("TIMESTAMP '2023-11-15 22:13:20'",sql)

    def test_exact_claims_share_one_content_without_filling_hole(self):
        with tempfile.TemporaryDirectory() as tmp:
            work=Path(tmp).resolve();cp=work/'collection.json';cp.write_text('{}')
            job=work/'private/jobs/synthetic';job.mkdir(parents=True)
            query=dict(name='synthetic',query_id='synthetic-query',scope_id='synthetic-scope',scope_hash='synthetic-hash',
                start_block=10,end_block=30,scope={'start_time':T,'end_time':T+100})
            def event(n,block,time):
                return dict(event_kind='top',tx_hash='0x'+format(n,'064x'),sender=A,recipient=Z,amount_raw='7',
                    block_number=block,tx_index=0,block_time=time,success=True,gas_used='21000',gas_price='1')
            raw=[event(1,11,T+1),event(2,21,T+31)]
            result={'status':'COMPLETED_EXPORTED','job_folder':str(job)}
            with patch('stage1d_candidate_batch.execute_sql',return_value=result),patch('stage1d_candidate_batch.result_rows',return_value=raw):
                acquire(work,query,[rect(),rect(A,20,22,T+30,T+40)],cp)
            records=list((work/'derived/stage1d/intervals').glob('*.coverage.json'))
            self.assertEqual(len(records),2)
            self.assertEqual(len(list((work/'derived/stage1d/intervals').glob('*.events.json'))),1)
            cache=CachedIntervals(work)
            complete=cache.fetch_interval(A,NATIVE,10,12,start_time=T,end_time=T+20,global_end_time=T+100)
            self.assertTrue(complete.complete);self.assertEqual(len(complete.events),1)
            incomplete=cache.fetch_interval(A,NATIVE,10,22,start_time=T,end_time=T+40,global_end_time=T+100)
            self.assertFalse(incomplete.complete);self.assertEqual(len(incomplete.events),2)
            self.assertTrue(cache.pending)
            other=cache.fetch_interval(B,NATIVE,10,12,start_time=T,end_time=T+20,global_end_time=T+100)
            self.assertFalse(other.complete)

    def test_failed_export_creates_no_complete_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            work=Path(tmp).resolve();cp=work/'collection.json';cp.write_text('{}')
            query=dict(name='synthetic',query_id='synthetic-query',scope_id='synthetic-scope',scope_hash='synthetic-hash',
                start_block=10,end_block=30,scope={'start_time':T,'end_time':T+100})
            with patch('stage1d_candidate_batch.execute_sql',return_value={'status':'EXPORT_INCOMPLETE'}):
                self.assertEqual(acquire(work,query,[rect()],cp)['status'],'EXPORT_INCOMPLETE')
            self.assertFalse((work/'derived/stage1d/intervals').exists())

    def test_request_scope_guard_precedes_external_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            work=Path(tmp).resolve();cp=work/'collection.json';cp.write_text('{}')
            query=dict(name='synthetic',query_id='synthetic-query',scope_id='synthetic-scope',scope_hash='synthetic-hash',
                start_block=10,end_block=30,scope={'start_time':T,'end_time':T+100})
            with patch('stage1d_candidate_batch.execute_sql') as external:
                with self.assertRaises(ValueError):acquire(work,query,[rect(lo=9)],cp)
                external.assert_not_called()


if __name__=='__main__':unittest.main()
