import hashlib,json,unittest
from pathlib import Path
from dune_r2 import RevisionLive
from page_attempts import AttemptStore,atomic_json
import test_budget_r2

class DuneR2Tests(unittest.TestCase):
    def setUp(self):
        self.b=test_budget_r2.BudgetR2Tests();self.b.setUp();self.p=self.b.p;(self.p/'private').mkdir()
        self.live=object.__new__(RevisionLive);self.live.w=self.p;self.live.db=self.b.db;self.live.account_context_ref='synthetic';self.live.attempts=AttemptStore(self.p/'attempts.sqlite');self.live.require_gate=lambda:None
        atomic_json(self.p/'private/current_dune_usage.json',{'billing_period':{'start_date':'2000-01-01','end_date':'2099-01-01'}})
        atomic_json(self.p/'private/dune_rate_evidence.json',{'status':'USER_CONFIRMED','account_context_ref':'synthetic','plan':'Plus trial','credit_economics_tier':'Free','export_credits_per_decimal_MB_for_budget':'20'})
        self.sql=self.p/'query.sql';self.sql.write_text('-- synthetic scope\nSELECT 1 AS x',encoding='utf-8');self.freeze=self.p/'freeze.json';atomic_json(self.freeze,{'export_plan':{'columns':['x'],'all_pages_required':True}})
        self.execution='A'*26;self.calls=[]
    def tearDown(self):self.b.tearDown()
    def receipt(self,op,params=None):return {'request_id':'synthetic-'+op,'sha256':'a'*64,'http_status':200,'error_class':None,'utc':'2026-01-01','execution_id':self.execution if op!='execute' else None,'parameters':params,'raw_bytes':123}
    def transport(self,op,execution=None,payload=None,params=None):
        self.calls.append(op)
        if op=='execute':return {'execution_id':self.execution,'state':'QUERY_STATE_PENDING'},self.receipt(op)
        md={'total_row_count':2,'total_result_set_bytes':120000,'column_names':['x']}
        if op=='status':return {'execution_id':self.execution,'state':'QUERY_STATE_COMPLETED','execution_cost_credits':'0.5','result_metadata':md},self.receipt(op)
        if op=='results':return {'execution_id':self.execution,'state':'QUERY_STATE_COMPLETED','result':{'rows':[{'x':1},{'x':2}],'metadata':md|{'row_count':2,'result_set_bytes':120000}}},self.receipt(op,params)
        self.fail('unexpected operation')
    def test_cap20_does_not_restore1_and_dynamic_export_over1_completes(self):
        self.live.call=self.transport;folder=Path(self.live.submit(self.sql,'synthetic',kind='context',freeze_manifest=self.freeze)['job_folder']);self.live.poll(folder)
        self.assertEqual(self.b.db.snapshot()['dune_credits']['r2_risk'],'0.5')
        result=self.live.export(folder,1000,0);self.assertTrue(result['progress']['complete']);self.assertEqual(result['upper_not_actual'],'3')
        self.assertEqual(self.live.settle(folder)['settlement_status'],'BOUNDED_ACCOUNTING_NOT_FINAL')
        self.assertEqual(self.calls,['execute','status','results']);self.assertFalse((self.p/'private/dune_cap5_control.json').exists())
    def test_timeout_keeps20_and_never_resubmits_changed_sql(self):
        def fail(*a,**kw):self.calls.append('execute');raise TimeoutError('synthetic')
        self.live.call=fail
        with self.assertRaises(TimeoutError):self.live.submit(self.sql,'synthetic',kind='context',freeze_manifest=self.freeze)
        self.sql.write_text('-- other comment\nSELECT 2 AS x')
        with self.assertRaises(RuntimeError):self.live.submit(self.sql,'synthetic',kind='context',freeze_manifest=self.freeze)
        self.assertEqual(self.calls,['execute']);self.assertEqual(self.b.db.snapshot()['dune_credits']['r2_risk'],'20')
    def test_unknown_page_cannot_retry_with_new_limit_or_job(self):
        self.live.call=self.transport;folder=Path(self.live.submit(self.sql,'synthetic',kind='context',freeze_manifest=self.freeze)['job_folder']);self.live.poll(folder)
        def fail(*a,**kw):self.calls.append('results');raise TimeoutError('synthetic')
        self.live.call=fail
        with self.assertRaises(TimeoutError):self.live.export(folder,1000,0)
        with self.assertRaises((RuntimeError,FileNotFoundError)):self.live.export(folder,500,0)
        self.assertEqual(self.calls,['execute','status','results']);self.assertEqual(self.b.db.snapshot()['dune_credits']['r2_risk'],'3.5')
    def test_gate_failure_does_not_reserve(self):
        self.live.require_gate=lambda:(_ for _ in ()).throw(RuntimeError('gate failure'))
        with self.assertRaises(RuntimeError):self.live.submit(self.sql,'synthetic',kind='context',freeze_manifest=self.freeze)
        self.assertEqual(self.b.db.snapshot()['dune_credits']['r2_risk'],'0')
    def test_export_budget_refusal_keeps_execution_and_metadata(self):
        self.live.call=self.transport;folder=Path(self.live.submit(self.sql,'synthetic',kind='context',freeze_manifest=self.freeze)['job_folder']);self.live.poll(folder)
        state=json.loads((folder/'job.json').read_text());state['status_response']['result_metadata']['total_result_set_bytes']=10000000;atomic_json(folder/'job.json',state)
        with self.assertRaises(RuntimeError):self.live.export(folder,1000,0)
        self.assertEqual(self.calls,['execute','status']);self.assertEqual(self.b.db.snapshot()['dune_credits']['r2_risk'],'0.5')
    def test_missing_export_plan_prevents_submit(self):
        atomic_json(self.freeze,{})
        with self.assertRaises(RuntimeError):self.live.submit(self.sql,'synthetic',kind='context',freeze_manifest=self.freeze)
    def test_large_rejected(self):
        with self.assertRaises(ValueError):self.live.submit(self.sql,'synthetic',kind='context',freeze_manifest=self.freeze,performance='large')
    def test_multi_page_size_metadata_binds_all_pages(self):
        state={'status_response':{'result_metadata':{'total_row_count':1500,'total_result_set_bytes':1000,'column_names':['x']}},'export_requests':0}
        upper,basis=self.live.export_envelope(state,1000);self.assertEqual(upper,4);self.assertEqual(basis['maximum_pages'],2)
    def test_candidate_freeze_requires_full_scope_binding(self):
        self.live.call=lambda *a,**k:self.fail('Unverified candidate submitted')
        with self.assertRaises(ValueError):self.live.submit(self.sql,'synthetic',kind='candidate',freeze_manifest=self.freeze)
        self.assertEqual(self.b.db.snapshot()['dune_credits']['r2_risk'],'0')
    def test_status_missing_execution_identity_never_releases20(self):
        self.live.call=self.transport;folder=Path(self.live.submit(self.sql,'synthetic',kind='context',freeze_manifest=self.freeze)['job_folder'])
        self.live.call=lambda *a,**k:({'state':'QUERY_STATE_COMPLETED','execution_cost_credits':'.01'},self.receipt('status'))
        self.live.poll(folder)
        self.assertEqual(self.b.db.snapshot()['dune_credits']['r2_risk'],'20')
        self.assertIsNone(json.loads((folder/'job.json').read_text()).get('execution_cost_credits'))
    def test_settle_reports_terminal_actual_and_preserves_higher_running_peak(self):
        self.live.call=self.transport;folder=Path(self.live.submit(self.sql,'synthetic',kind='context',freeze_manifest=self.freeze)['job_folder'])
        self.live.db.observe_execution(json.loads((folder/'job.json').read_text())['logical_job_id'],'1.5',self.receipt('running'),terminal=False)
        state=json.loads((folder/'job.json').read_text());state['execution_cost_credits']='1.5';atomic_json(folder/'job.json',state)
        self.live.poll(folder);self.live.export(folder,1000,0)
        result=self.live.settle(folder);self.assertEqual(result['known_execution_actual'],'0.5');self.assertEqual(result['observed_execution_peak_retained'],'1.5');self.assertEqual(result['execution_peak_discrepancy_risk'],'1.0')
        self.assertEqual(result['cumulative']['r2_risk'],'4.5');before=self.live.db.rows();self.assertEqual(self.live.settle(folder)['known_execution_actual'],'0.5');self.assertEqual(self.live.db.rows(),before)
if __name__=='__main__':unittest.main()
