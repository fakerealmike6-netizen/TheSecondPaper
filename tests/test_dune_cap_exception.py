"""Synthetic one-use cap5 exception. No real account, journal or transport."""
import concurrent.futures,contextlib,copy,hashlib,io,json,tempfile,unittest,sys
from pathlib import Path
from decimal import Decimal
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from budget import Ledger
from budget_r1 import RevisionLedger,consistent_backup
from dune_r1 import RevisionLive
from dune_cap_exception import CAP5_AUTH,ordinary_sql_allowed
from dune_live import dump,read
from page_attempts import AttemptStore

class Cap5Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.w=Path(self.tmp.name);(self.w/'private').mkdir()
        old=Ledger(self.w/'old.sqlite');old.confirm('dune_credits','2500','SYNTHETIC')
        for i in range(5):old.reserve_dune_job('old'+str(i),'synthetic','1','1');old.settle('old'+str(i),{'dune_credits':None})
        consistent_backup(self.w/'old.sqlite',self.w/'old_snapshot.sqlite')
        self.db=RevisionLedger(self.w/'new.sqlite');self.db.initialize(self.w/'old_snapshot.sqlite',{'old'+str(i):{'execution_cost_credits':'.1'} for i in range(5)},{'synthetic':True})
        self.prior=['failed_1','failed_2'];self.executions=['B'*26,'C'*26]
        for job,execution in zip(self.prior,self.executions):
            self.db.reserve_dune_job(job,'synthetic prior','1','1');self.db.observe_execution(job,'1',{'synthetic':True});self.db.reconcile(job,total_actual='1',export_actual='0',evidence=self.evidence())
            dump(self.w/'private/dune_r1_jobs'/job/'job.json',{'logical_job_id':job,'execution_id':execution,'state':'QUERY_STATE_FAILED','execution_cost_credits':'1','sql_sha256':hashlib.sha256(b'-- original optimized label query\nSELECT 9 AS n').hexdigest()})
        self.sql=self.w/'retry.sql';self.sql.write_text('-- original optimized label query\nSELECT 9 AS n')
        self.queue=self.w/'queue.json';dump(self.queue,{'synthetic_original_queue':[str(i) for i in range(9)]})
        self.control={'authorization_id':CAP5_AUTH,'status':'PENDING_USER_CONFIRMATION','account_context_ref':'synthetic','queue_count':9,
            'queue_sha256':hashlib.sha256(self.queue.read_bytes()).hexdigest(),'sql_sha256':hashlib.sha256(self.sql.read_text().encode()).hexdigest(),
            'prior_failed_job_ids':self.prior,'original_failed_execution_ids':self.executions,'exception_execution_cap_credits':'5','exception_export_cap_credits':'1','exception_total_cap_credits':'6',
            'new_stage_budget_unchanged':'10','parent_risk_cap_unchanged':'20','account_execution_cap_confirmed_credits':'1','sql_submissions_paused':True,
            'confirmation_source':None,'payment_method_added':False,'extra_credits_enabled':False}
        self.save_control()
        self.live=object.__new__(RevisionLive);self.live.w=self.w;self.live.db=self.db;self.live.account_context_ref='synthetic';self.live.attempts=AttemptStore(self.w/'private/attempts.sqlite');self.live.require_gate=lambda:None
        dump(self.w/'private/current_dune_usage.json',{'billing_period':{'start_date':'2000-01-01','end_date':'2099-01-01'}})
        dump(self.w/'private/dune_rate_evidence.json',{'account_context_ref':'synthetic','status':'USER_CONFIRMED','plan':'Free'})
        self.calls=[]
    def tearDown(self):self.tmp.cleanup()
    def save_control(self):dump(self.w/'private/dune_cap5_control.json',self.control)
    def confirmed(self):
        self.control.update(status='USER_CONFIRMED_CAP5',confirmation_source='USER_CONFIRMED',confirmation_received_utc='2026-09-06T00:00:00Z',account_execution_cap_confirmed_credits='5')
        self.save_control();return self.control
    def evidence(self):return {'request_set_closed':True,'source_receipt_sha256':'a'*64,'account_context_ref':'synthetic','applicable_rate_evidence':'synthetic','metered_units_evidence':'synthetic','all_attempts_included':True,'rounding_and_minimum_evidence':'synthetic','unknown_attempt_risk_included':True}
    def reserve_exception(self):self.db.register_cap5_exception(self.confirmed());return self.db.reserve_cap5_exception(self.control,'synthetic cap5')
    def quiet(self,fn,*args,**kwargs):
        with contextlib.redirect_stdout(io.StringIO()):return fn(*args,**kwargs)
    def fake_submit(self,op,execution=None,payload=None,params=None):
        self.calls.append(op)
        return {'execution_id':'A'*26,'state':'QUERY_STATE_EXECUTING'},{'http_status':200,'request_id':'synthetic','error_class':None,'sha256':'a'*64}
    def test_pending_settings_prevents_normal_and_exception_callbacks(self):
        self.live.call=self.fake_submit
        for fn in [lambda:self.live.submit(self.sql,'normal',freeze_manifest=self.queue),lambda:self.live.submit_cap5(self.sql,'exception',self.queue)]:
            with self.assertRaises(RuntimeError):fn()
        self.assertEqual(self.calls,[]);self.assertEqual(self.db.snapshot()['dune_credits']['buckets']['R1_NEW']['risk'],'2')
    def test_repeated_authorization_adds_no_grant_or_slot(self):
        self.confirmed();self.assertTrue(self.db.register_cap5_exception(self.control));self.assertFalse(self.db.register_cap5_exception(self.control))
        self.db.reserve_cap5_exception(self.control,'exception')
        self.db.observe_execution(CAP5_AUTH,'.1',{'synthetic':True});self.db.reconcile(CAP5_AUTH,total_actual='.1',export_actual='0',evidence=self.evidence())
        other=RevisionLedger(self.db.path);self.assertFalse(other.register_cap5_exception(self.control))
        with self.assertRaises(RuntimeError):other.reserve_cap5_exception(self.control,'again')
        s=other.snapshot()['dune_credits'];self.assertEqual(s['cap'],'20');self.assertEqual(s['buckets']['R1_NEW']['cap'],'10');self.assertEqual(s['buckets']['LEGACY_STAGE1B']['risk'],'10')
    def test_concurrent_exception_claim_consumes_once(self):
        self.confirmed();self.db.register_cap5_exception(self.control)
        def submit(_):
            try:RevisionLedger(self.db.path).reserve_cap5_exception(self.control,'synthetic');return True
            except RuntimeError:return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:r=list(pool.map(submit,range(4)))
        self.assertEqual(sum(r),1);self.assertEqual(self.db.snapshot()['dune_credits']['reserved'],'16')
        self.assertEqual(self.db.snapshot()['dune_credits']['buckets']['R1_NEW']['risk'],'8')
    def test_budget_shortfall_does_not_consume_slot(self):
        self.confirmed();self.db.register_cap5_exception(self.control)
        self.db.reserve_dune_job('ordinary_a','synthetic','1','1');self.db.reserve_dune_job('ordinary_b','synthetic','1','1')
        with self.assertRaises(RuntimeError):self.db.reserve_cap5_exception(self.control,'exception')
        with self.db.connection() as db:self.assertIsNone(db.execute('SELECT used_job FROM dune_cap_exceptions').fetchone()[0])
    def test_generic_job_cannot_request_five_or_mutate_binding(self):
        with self.assertRaises(ValueError):self.db.reserve_dune_job('ordinary','synthetic','5','1')
        self.confirmed();self.db.register_cap5_exception(self.control);changed=copy.deepcopy(self.control);changed['sql_sha256']='f'*64
        with self.assertRaises(RuntimeError):self.db.register_cap5_exception(changed)
    def test_exact_sql_queue_account_and_previous_failures_bind_retry(self):
        self.confirmed();self.live.call=self.fake_submit
        self.sql.write_text('-- changed SQL\nSELECT 10 AS n')
        with self.assertRaises(RuntimeError):self.live.submit_cap5(self.sql,'exception',self.queue)
        self.assertEqual(self.calls,[])
    def test_confirmation_restoration_required_even_after_terminal(self):
        self.confirmed();self.live.call=self.fake_submit
        result=self.live.submit_cap5(self.sql,'exception',self.queue);state=read(Path(result['job_folder'])/'job.json')
        self.assertEqual(state['logical_job_id'],CAP5_AUTH);self.assertEqual(state['combined_reserved'],'6');self.assertEqual(self.calls,['execute'])
        self.live.observe_execution_charge(state,{'state':'QUERY_STATE_FAILED','execution_cost_credits':'4.5'},{'request_id':'synthetic','sha256':'a'*64})
        control=read(self.w/'private/dune_cap5_control.json');self.assertEqual(control['status'],'PENDING_RESTORE_1');self.assertEqual(control['last_observed_exception_state'],'QUERY_STATE_FAILED')
        with self.assertRaises(RuntimeError):ordinary_sql_allowed(control)
        control.update(status='RESTORED_1_CONFIRMED',sql_submissions_paused=False,account_execution_cap_confirmed_credits='1',restore_confirmation_source='USER_CONFIRMED',restore_received_utc='2026-09-06T01:00:00Z')
        ordinary_sql_allowed(control)
    def test_exception_execution_4point5_does_not_trigger_default_cap1(self):
        self.reserve_exception();state={'logical_job_id':CAP5_AUTH,'execution_id':'A'*26,'state':'QUERY_STATE_COMPLETED'}
        self.live.observe_execution_charge(state,{'execution_cost_credits':'4.5','state':'QUERY_STATE_COMPLETED'},{'request_id':'synthetic','sha256':'a'*64})
        self.assertFalse(self.db.snapshot()['dune_credits']['overrun']);self.assertFalse((self.w/'private/dune_live_halt.json').exists())
        self.db.reconcile(CAP5_AUTH,total_upper='5.5',evidence=self.evidence())
        self.assertEqual(self.db.snapshot()['dune_credits']['buckets']['R1_NEW']['risk'],'7.5')
    def test_execution_over5_retained_in_full_and_halts(self):
        self.reserve_exception();state={'logical_job_id':CAP5_AUTH,'execution_id':'A'*26,'state':'QUERY_STATE_FAILED'}
        cost='5.123456789123456789'
        self.live.observe_execution_charge(state,{'execution_cost_credits':cost,'state':'QUERY_STATE_FAILED'},{'request_id':'synthetic','sha256':'a'*64})
        self.assertEqual(next(r for r in self.db.detailed_jobs() if r['job']==CAP5_AUTH)['execution_known'],cost)
        self.assertTrue(self.db.snapshot()['dune_credits']['overrun']);self.assertTrue((self.w/'private/dune_live_halt.json').exists())
        with self.assertRaises(RuntimeError):self.db.reserve_dune_job('after','synthetic','1','1')
    def export_state(self,rows=9):
        self.reserve_exception();self.db.observe_execution(CAP5_AUTH,'4.5',{'synthetic':True})
        folder=self.w/'exception_job'
        state={'logical_job_id':CAP5_AUTH,'execution_id':'A'*26,'state':'QUERY_STATE_COMPLETED','execution_cost_credits':'4.5','export_requests':0,'export_offsets':[],
            'status_response':{'state':'QUERY_STATE_COMPLETED','execution_cost_credits':'4.5','result_metadata':{'total_row_count':rows,'total_result_set_bytes':1000,'column_names':['v']}}}
        dump(folder/'job.json',state);return folder
    def test_export_one_credit_bound_permitted_and_closed(self):
        folder=self.export_state()
        def response(op,execution=None,payload=None,params=None):
            self.calls.append(op);return {'execution_id':'A'*26,'state':'QUERY_STATE_COMPLETED','result':{'rows':[{'v':i} for i in range(9)],'metadata':{'row_count':9,'total_row_count':9}}},{'http_status':200,'error_class':None,'request_id':'synthetic','execution_id':'A'*26,'parameters':params,'raw_bytes':1000}
        self.live.call=response;self.quiet(self.live.export,folder,50,0)
        self.assertEqual(self.calls,['results']);self.assertEqual(read(folder/'job.json')['export_status'],'COMPLETED_DECLARED_RESULT_ROWS')
    def test_export_more_than_one_credit_stops_before_callback(self):
        folder=self.export_state(rows=51);self.live.call=self.fake_submit
        with self.assertRaises(RuntimeError):self.live.export(folder,50,0)
        self.assertEqual(self.calls,[])
    def test_timeout_consumes_slot_and_holds_full_six(self):
        self.confirmed()
        def fail(*args,**kwargs):self.calls.append('execute');raise TimeoutError('synthetic unknown')
        self.live.call=fail
        with self.assertRaises(TimeoutError):self.live.submit_cap5(self.sql,'exception',self.queue)
        self.assertEqual(self.db.snapshot()['dune_credits']['buckets']['R1_NEW']['risk'],'8')
        self.assertEqual(read(self.w/'private/dune_cap5_control.json')['status'],'PENDING_RESTORE_1')
        with self.assertRaises(RuntimeError):self.live.submit_cap5(self.sql,'again',self.queue)
        self.assertEqual(self.calls,['execute'])
    def test_void_only_demonstrably_unsubmitted_ordinary_proposal(self):
        self.db.reserve_dune_job('never_sent','synthetic planned','1','1')
        with self.assertRaises(ValueError):self.db.void_unsubmitted('never_sent',{'job_state':'PLANNED'})
        self.db.void_unsubmitted('never_sent',{'job_state':'PLANNED','dispatch_attempt_count':0,'request_journal_verified':True,'unsubmitted_state_sha256':'a'*64})
        row=next(r for r in self.db.rows() if r['request_id']=='never_sent');self.assertEqual(row['reserved'],'0');self.assertIsNone(row['actual'])
        with self.assertRaises(ValueError):self.db.void_unsubmitted('failed_1',{'job_state':'PLANNED','dispatch_attempt_count':0,'request_journal_verified':True,'unsubmitted_state_sha256':'a'*64})

if __name__=='__main__':unittest.main()
