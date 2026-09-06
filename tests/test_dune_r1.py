import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import test_budget_r1
from dune_r1 import RevisionLive
from page_attempts import AttemptStore

class DuneR1Tests(unittest.TestCase):
    def setUp(self):
        self.b=test_budget_r1.BudgetR1Tests();self.b.setUp();self.p=self.b.p
        (self.p/'private').mkdir();self.live=object.__new__(RevisionLive);self.live.w=self.p;self.live.db=self.b.db
        self.live.account_context_ref='synthetic';self.live.attempts=AttemptStore(self.p/'attempts.sqlite')
        (self.p/'private/current_dune_usage.json').write_text(json.dumps({'billing_period':{'start_date':'2000-01-01','end_date':'2099-01-01'}}))
        self.sql=self.p/'query.sql';self.sql.write_text('-- synthetic read only\nSELECT 2 AS x')
        self.freeze=self.p/'freeze.json';self.freeze.write_text('{}')
        self.live.require_gate=lambda:None
    def tearDown(self):self.b.tearDown()
    def rate(self):
        (self.p/'private/dune_rate_evidence.json').write_text(json.dumps({'account_context_ref':'synthetic','status':'USER_CONFIRMED','plan':'Free'}))
    def test_both_schemes_ceiling_and_pages(self):
        self.rate();s={'status_response':{'result_metadata':{'total_row_count':9,'total_result_set_bytes':20000,'column_names':['address','payload']}}}
        upper,basis=self.live.export_envelope(s,50);self.assertEqual(str(upper),'1');self.assertFalse(basis['is_actual'])
        s['status_response']['result_metadata']['total_row_count']=51
        self.assertEqual(str(self.live.export_envelope(s,50)[0]),'2')
    def test_unknown_rate_blocks_export_before_callback(self):
        folder=self.p/'job';folder.mkdir();(folder/'job.json').write_text('{}')
        self.live.call=lambda *a,**k:self.fail('Export dispatched without rate applicability')
        with self.assertRaises(RuntimeError):self.live.export(folder,50,0)
    def test_submit_no_automatic_retry_after_timeout(self):
        calls=[]
        def call(*a,**k):calls.append(a);raise TimeoutError('synthetic uncertain submit')
        self.live.call=call
        with self.assertRaises(TimeoutError):self.live.submit(self.sql,'synthetic',freeze_manifest=self.freeze)
        with self.assertRaises(RuntimeError):self.live.submit(self.sql,'synthetic',freeze_manifest=self.freeze)
        self.assertEqual(len(calls),1);self.assertEqual(self.live.db.snapshot()['dune_credits']['buckets']['R1_NEW']['risk'],'2')
    def test_no_export_final_cost_closes_only_execution(self):
        self.b.db.reserve_dune_job('n','synthetic','1','1');self.b.db.observe_execution('n','.3',{'synthetic':True})
        folder=self.p/'job';folder.mkdir();(folder/'job.json').write_text(json.dumps({'logical_job_id':'n','state':'QUERY_STATE_FAILED','execution_cost_credits':'.3','export_requests':0,'status_receipt':{'sha256':'a'*64}}))
        result=self.live.settle(folder);self.assertEqual(result['actual'],'0.3');self.assertEqual(result['export_actual'],'0')
    def test_gate_failure_before_reservation_or_call(self):
        self.live.require_gate=lambda:(_ for _ in ()).throw(RuntimeError('gate failed'))
        with self.assertRaises(RuntimeError):self.live.submit(self.sql,'synthetic',freeze_manifest=self.freeze)
        self.assertEqual(self.b.db.snapshot()['dune_credits']['buckets']['R1_NEW']['risk'],'0')

if __name__=='__main__':unittest.main()
