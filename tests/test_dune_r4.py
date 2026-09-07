"""Real access/ledger/read-journal paths using explicitly synthetic transport."""
from contextlib import closing
from decimal import Decimal
import hashlib
from http.client import IncompleteRead
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from budget_r1 import consistent_backup
from context_queries_r3 import build, date_contract
from dune_r4 import ContextDuneR4
from page_attempts import AttemptStore, atomic_json, RequestBlocked
from read_retry_r4 import ReadRetryStore
import test_budget_r2
from test_context_repairs_r4 import query, header


class FakeClock:
    def __init__(self): self.value=2000000000.;self.waits=[]
    def __call__(self):return self.value
    def sleep(self,seconds):self.waits.append(seconds);self.value+=seconds


class DuneR4Tests(unittest.TestCase):
    def setUp(self):
        self.b=test_budget_r2.BudgetR2Tests();self.b.setUp();self.w=self.b.p
        (self.w/'private').mkdir()
        consistent_backup(self.b.db.path,self.w/'private/shared_budget_r4.sqlite')
        atomic_json(self.w/'private/dune_user_confirmation.json',test_budget_r2.confirmation())
        atomic_json(self.w/'private/current_dune_usage.json',{'billing_period':{'start_date':'2000-01-01','end_date':'2099-01-01'}})
        atomic_json(self.w/'private/dune_rate_evidence.json',{'status':'USER_CONFIRMED','account_context_ref':'synthetic','plan':'Plus trial','credit_economics_tier':'Free','export_credits_per_decimal_MB_for_budget':'20'})
        atomic_json(self.w/'private/INHERITED_RESOURCE_R4.json',{'inherited_raw_risk_bytes':0,'context_seconds_at_start':{'atomic_simple_transfer':'0','harmony_high_branch':'0'}})
        self.clock=FakeClock();self.calls=[];self.responses={};self.execution='A'*26
        self.live=ContextDuneR4(self.w,self.transport,clock=self.clock,sleeper=self.clock.sleep,rng=lambda:0)
        self.folder=self.w/'private/dune_r2_jobs'/'job'
        self.folder.mkdir(parents=True)
        self.job='synthetic-job';self.live.db.reserve_dune_job(self.job,'synthetic')
        self.live.db.observe_execution(self.job,'.5',{'sha256':'a'*64},terminal=True)
        self.md={'total_row_count':2,'total_result_set_bytes':120000,'column_names':['x']}
        self.state={'logical_job_id':self.job,'execution_id':self.execution,'sql_sha256':'a'*64,'state':'QUERY_STATE_COMPLETED',
                    'execution_cost_credits':'.5','export_requests':0,'export_offsets':[],
                    'status_response':{'execution_id':self.execution,'state':'QUERY_STATE_COMPLETED','result_metadata':self.md},
                    'status_receipt':{'sha256':'b'*64,'http_status':200,'execution_id':self.execution},'account_context_ref':'synthetic'}
        self.save()

    def tearDown(self):self.b.tearDown()
    def save(self):atomic_json(self.folder/'job.json',self.state)
    def state_now(self):return json.loads((self.folder/'job.json').read_text())
    def transport(self,op,execution,payload,params,bound):
        self.calls.append((op,execution,params))
        if self.responses.get(op):
            value=self.responses[op].pop(0)
            if isinstance(value,BaseException):raise value
            return value
        body={'execution_id':self.execution,'state':'QUERY_STATE_COMPLETED','result':{'rows':[{'x':1},{'x':2}],'metadata':self.md|{'row_count':2,'result_set_bytes':120000}}}
        if op=='status':body={'execution_id':self.execution,'state':'QUERY_STATE_COMPLETED','execution_cost_credits':'.5','result_metadata':self.md}
        if op=='execute':body={'execution_id':'B'*26,'state':'QUERY_STATE_PENDING'}
        return 200,json.dumps(body).encode(),{}
    def response(self,code,body=None,headers=None):return code,json.dumps(body or {}).encode(),headers or {}
    def attempts(self):return self.live.reads.attempts(self.live.read_identity('results',self.state,{'limit':1000,'offset':0}))

    def test_timeout_page_recovers_and_adds_cost_without_releasing_old(self):
        self.responses['results']=[TimeoutError('synthetic')]
        result=self.live.export(self.folder)
        self.assertTrue(result['progress']['complete']);self.assertEqual(result['upper_not_actual'],'6')
        self.assertEqual(self.live.db.snapshot()['dune_credits']['r2_risk'],'6.5')
        rows=self.attempts();self.assertEqual([r['outcome'] for r in rows],['RETRYABLE_FAILURE','SUCCESS']);self.assertEqual(rows[1]['retry_of'],rows[0]['attempt_id'])
        self.assertEqual(self.live.settle(self.folder)['settlement_status'],'EXPORTED_WITH_RETAINED_ATTEMPT_RISK')
        self.assertTrue(self.live.export(self.folder)['cache_reused']);self.assertEqual(len(self.calls),2)

    def test_retry_after_is_honored(self):
        self.responses['results']=[self.response(429,headers={'Retry-After':'7'})]
        self.assertTrue(self.live.export(self.folder)['progress']['complete']);self.assertEqual(self.clock.waits,[7])

    def test_retry_after_beyond_deadline_is_deferred_and_survives_restart(self):
        self.live.deadline=self.clock()+35
        self.responses['results']=[self.response(503,headers={'Retry-After':'40'})]
        self.assertEqual(self.live.export(self.folder)['export_status'],'DEFERRED')
        live=ContextDuneR4(self.w,self.transport,clock=self.clock,sleeper=self.clock.sleep,rng=lambda:0,deadline=self.clock()+35)
        self.assertEqual(live.export(self.folder)['export_status'],'DEFERRED');self.assertEqual(len(self.calls),1)
        self.clock.value+=40;live.deadline=self.clock()+100
        self.assertTrue(live.export(self.folder)['progress']['complete']);self.assertEqual(len(self.calls),2)

    def test_three_failures_exhaust_after_restart_and_preserve_nine_export_risk(self):
        self.responses['results']=[TimeoutError(),IncompleteRead(b'prefix'),self.response(503)]
        self.assertEqual(self.live.export(self.folder)['export_status'],'RETRIES_EXHAUSTED')
        other=ContextDuneR4(self.w,self.transport,clock=self.clock,sleeper=self.clock.sleep,rng=lambda:0)
        self.assertEqual(other.export(self.folder)['export_status'],'RETRIES_EXHAUSTED');self.assertEqual(len(self.calls),3)
        self.assertEqual(other.db.snapshot()['dune_credits']['r2_risk'],'9.5')

    def test_invalid_json_identity_and_permission_do_not_retry(self):
        for value in [(200,b'bad-json',{}),self.response(200,{'execution_id':'Z'*26}),self.response(403)]:
            with self.subTest(value=value):
                # Each subtest has one independent immutable execution identity.
                self.state['execution_id']=chr(66+len(self.calls))*26;self.save();self.responses['results']=[value]
                before=len(self.calls);result=self.live.export(self.folder)
                self.assertEqual(result['export_status'],'PERMANENT_FAILURE');self.assertEqual(len(self.calls),before+1)

    def test_entitlement_error_never_retries_even_with_503_wrapper(self):
        self.responses['results']=[self.response(503,{'error':{'message':'not authorized; upgrade required'}})]
        self.assertEqual(self.live.export(self.folder)['export_status'],'PERMANENT_FAILURE');self.assertEqual(len(self.calls),1)

    def test_two_workers_share_single_dispatch_claim(self):
        from concurrent.futures import ThreadPoolExecutor
        import threading
        entered,release=threading.Event(),threading.Event()
        def blocking(*args):
            entered.set()
            if not release.wait(5):raise RuntimeError('Synthetic worker synchronization failed')
            return self.transport(*args)
        a=ContextDuneR4(self.w,blocking,clock=self.clock,sleeper=self.clock.sleep,rng=lambda:0)
        b=ContextDuneR4(self.w,self.transport,clock=self.clock,sleeper=self.clock.sleep,rng=lambda:0)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending=pool.submit(a.export,self.folder)
            self.assertTrue(entered.wait(5))
            try:self.assertEqual(b.export(self.folder)['export_status'],'IN_FLIGHT')
            finally:release.set()
            self.assertTrue(pending.result(5)['progress']['complete'])
        self.assertEqual(len(self.calls),1)

    def test_insufficient_clock_defers_without_attempt_or_fee_reservation(self):
        self.live.deadline=self.clock()+10
        self.assertEqual(self.live.export(self.folder)['export_status'],'DEFERRED')
        self.assertFalse(self.calls);self.assertFalse(self.attempts())
        self.assertEqual(self.live.db.snapshot()['dune_credits']['r2_risk'],'0.5')

    def test_r4_policy_routes_existing_cli_snapshot_to_r4_budget(self):
        atomic_json(self.w/'configs/STAGE1B_R4_POLICY.json',{'authorization_id':'STAGE1B_R4_ALL_FIXES_AND_READ_RETRY_V1'})
        source=Path(__file__).resolve().parents[1]/'src/context_access_r3.py'
        result=subprocess.run([sys.executable,str(source),'snapshot','--work',str(self.w)],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads(result.stdout)['dune_credits']['r2_risk'],'0.5')

    def test_recovery_budget_failure_does_not_dispatch_and_does_not_release_first_risk(self):
        self.responses['results']=[TimeoutError()]
        original=self.live.db.reserve_export;counter=[0]
        def reserve(*args,**kwargs):
            counter[0]+=1
            if counter[0]>1:raise RuntimeError('Necessary export does not fit')
            return original(*args,**kwargs)
        with patch.object(self.live.db,'reserve_export',side_effect=reserve):
            with self.assertRaisesRegex(RuntimeError,'does not fit'):self.live.export(self.folder)
        self.assertEqual(len(self.calls),1);self.assertEqual(self.live.db.snapshot()['dune_credits']['r2_risk'],'3.5')
        self.assertEqual(self.attempts()[-1]['outcome'],'ABANDONED_BEFORE_DISPATCH')
        self.assertTrue(self.live.export(self.folder)['progress']['complete'])
        self.assertEqual(self.live.db.snapshot()['dune_credits']['r2_risk'],'6.5')

    def test_real_cumulative100_denies_recovery_before_second_request(self):
        self.md['total_result_set_bytes']=3000000;self.save();self.responses['results']=[TimeoutError()]
        with self.assertRaisesRegex(RuntimeError,'cumulative100'):self.live.export(self.folder)
        self.assertEqual(len(self.calls),1)
        self.assertEqual(self.live.db.snapshot()['dune_credits']['r2_risk'],'60.5')
        self.assertEqual(self.attempts()[-1]['outcome'],'ABANDONED_BEFORE_DISPATCH')

    def test_larger_server_metadata_raises_old_interrupted_request_bound_too(self):
        larger=self.md|{'total_result_set_bytes':240000,'result_set_bytes':240000,'row_count':2}
        page={'execution_id':self.execution,'state':'QUERY_STATE_COMPLETED','result':{'rows':[{'x':1},{'x':2}],'metadata':larger}}
        self.responses['results']=[TimeoutError(),self.response(200,page)]
        self.assertEqual(self.live.export(self.folder)['upper_not_actual'],'10')
        self.assertEqual(self.live.db.snapshot()['dune_credits']['r2_risk'],'10.5')

    def test_r4_policy_routes_rpc_and_dune_cli_to_new_access(self):
        atomic_json(self.w/'configs/STAGE1B_R4_POLICY.json',{'authorization_id':'STAGE1B_R4_ALL_FIXES_AND_READ_RETRY_V1'})
        plans=self.w/'plans.json';atomic_json(plans,[{'method':'eth_chainId','params':[]}])
        source=Path(__file__).resolve().parents[1]/'src/context_access_r3.py'
        code="""import sys,runpy,json
from pathlib import Path
source,work,plans,action=sys.argv[1:]
sys.path.insert(0,str(Path(source).parent))
import context_access_r4,dune_r4
class FakeRPC:
 def __init__(self,work):pass
 def call_batch(self,plans,probe,label,capability=False):return {'route':'R4_RPC','plans':plans,'status':'COMPLETE'}
context_access_r4.RpcAccess=FakeRPC
dune_r4.execute_dune=lambda *a,**k: {'route':'R4_DUNE','status':'COMPLETED_EXPORTED'}
sys.argv=[source,action,'--work',work,'--plans',plans]
runpy.run_path(source,run_name='__main__')
"""
        for action in ('rpc','dune'):
            result=subprocess.run([sys.executable,'-c',code,str(source),str(self.w),str(plans),action],capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertEqual(json.loads(result.stdout)['route'],'R4_'+action.upper())

    def test_limit_and_columns_cannot_change_after_failed_read(self):
        self.responses['results']=[self.response(429,headers={'Retry-After':'100'})]
        self.live.export(self.folder)
        with self.assertRaises(RequestBlocked):self.live.export(self.folder,limit=500)
        state=self.state_now();state['status_response']['result_metadata']['column_names']=['y'];atomic_json(self.folder/'job.json',state)
        with self.assertRaises(RequestBlocked):self.live.export(self.folder)
        self.assertEqual(len(self.calls),1)

    def legacy(self,success=False):
        params={'limit':1000,'offset':0}
        ident=AttemptStore.identity('synthetic',self.job,self.execution,'results',params)
        aid=self.live.attempts.dispatch(ident)
        receipt={'request_id':'legacy','http_status':200 if success else None,'error_class':None if success else 'IncompleteRead',
                 'parameters':params,'execution_id':self.execution,'sha256':'c'*64,'raw_bytes':0}
        body=json.loads(self.transport('results',self.execution,None,params,1)[1]) if success else None
        self.calls.clear()
        self.live.attempts.save_response(aid,body,receipt,self.folder/'page_0.json',self.folder/'page_0_receipt.json')
        if success:self.live.attempts.validated(aid,{'complete':True,'next_offset':None})
        else:self.live.attempts.mark(aid,'UNKNOWN_TRANSPORT','IncompleteRead')
        self.state.update(export_requests=1,export_offsets=[0]);self.save()
        self.live.db.reserve_export(self.job,3,{'rate_evidence':{'synthetic':True},'result_metadata':self.md})
        return aid

    def test_old_failed_page_import_counts_attempt_and_retains_original_bytes(self):
        aid=self.legacy();before={p.name:p.read_bytes() for p in self.folder.glob('page*.json')};rows=self.live.attempts.rows()
        self.assertTrue(self.live.export(self.folder)['progress']['complete'])
        self.assertEqual(len(self.calls),1);self.assertEqual(len(self.attempts()),2)
        self.assertEqual(self.live.attempts.rows(),rows);self.assertEqual(before,{p.name:p.read_bytes() for p in self.folder.glob('page*.json')})
        self.assertEqual(self.live.db.snapshot()['dune_credits']['r2_risk'],'6.5')

    def test_old_success_page_reused_and_migration_is_idempotent(self):
        self.legacy(True);a=self.live.migrate_legacy_reads();b=self.live.migrate_legacy_reads()
        self.assertEqual(a['attempts'][0]['state'],'IMPORTED');self.assertEqual(b['attempts'][0]['state'],'ALREADY_IMPORTED')
        self.assertTrue(self.live.export(self.folder)['cache_reused']);self.assertFalse(self.calls)

    def test_success_cache_artifact_tamper_is_rejected(self):
        self.live.export(self.folder)
        state=self.state_now();p=self.w/state['r4_verified_pages']['0']['page_path'];p.write_text('{}')
        with self.assertRaises(RequestBlocked):self.live.export(self.folder)
        self.assertEqual(len(self.calls),1)

    def test_status_pending_polls_are_separate_successful_observations(self):
        self.state['state']='QUERY_STATE_PENDING';self.save()
        pending={'execution_id':self.execution,'state':'QUERY_STATE_EXECUTING'}
        self.responses['status']=[self.response(200,pending)]*5
        for i in range(5):self.assertEqual(self.live.poll(self.folder)['observation_sequence'],i)
        self.assertEqual(self.live.poll(self.folder)['state'],'QUERY_STATE_COMPLETED');self.assertEqual(len(self.calls),6)
        self.assertEqual(self.live.poll(self.folder)['status'],'TERMINAL_CACHE_REUSED');self.assertEqual(len(self.calls),6)

    def test_status_failure_chain_is_bounded_and_normal_poll_can_then_continue(self):
        self.state['state']='QUERY_STATE_PENDING';self.save()
        self.responses['status']=[TimeoutError(),self.response(503),self.response(200,{'execution_id':self.execution,'state':'QUERY_STATE_EXECUTING'})]
        self.assertEqual(self.live.poll(self.folder)['state'],'QUERY_STATE_EXECUTING')
        self.assertEqual(self.live.poll(self.folder)['state'],'QUERY_STATE_COMPLETED');self.assertEqual(len(self.calls),4)
        first=self.live.reads.attempts(self.live.read_identity('status',self.state,sequence=0));self.assertEqual(len(first),3)
        self.assertEqual(self.live.db.snapshot()['dune_credits']['r2_risk'],'0.5')

    def make_freeze(self):
        q=query();headers={99:header(99,'2023-09-01T23:59:48Z'),102:header(102,'2023-09-02T00:00:24Z')}
        sql=build(q,block_headers=headers);folder=self.w/'private/context_queries/frozen';folder.mkdir(parents=True)
        (folder/'query.sql').write_text(sql,encoding='utf-8',newline='\n')
        atomic_json(folder/'freeze_manifest.json',{'schema_version':'stage1b-r4-context-query-v1','account_windows':q['rows'],
            'coverage_contract':date_contract(q,headers),'sql_sha256':hashlib.sha256(sql.encode()).hexdigest(),'export_plan':{'page_limit':1000}})
        return folder,headers

    def test_submit_uses_real_scope_verifier_before_any_reservation(self):
        folder,headers=self.make_freeze()
        with patch('context_queries_r3.load_verified_block_headers',return_value=headers):
            self.assertEqual(self.live.submit(folder/'query.sql','synthetic',freeze_manifest=folder/'freeze_manifest.json')['execution_id'],'B'*26)
        self.assertEqual(self.calls[0][0],'execute')
        bad=(folder/'query.sql').read_text().replace("DATE '2023-09-01'","DATE '2023-09-02'")
        (folder/'query.sql').write_text(bad,encoding='utf-8',newline='\n')
        freeze=json.loads((folder/'freeze_manifest.json').read_text());freeze['sql_sha256']=hashlib.sha256(bad.encode()).hexdigest();atomic_json(folder/'freeze_manifest.json',freeze)
        before=self.live.db.snapshot()
        with patch('context_queries_r3.load_verified_block_headers',return_value=headers),self.assertRaises(ValueError):
            self.live.submit(folder/'query.sql','tampered',freeze_manifest=folder/'freeze_manifest.json')
        self.assertEqual(self.live.db.snapshot(),before);self.assertEqual(len(self.calls),1)

    def test_unknown_post_is_never_resubmitted_and_keeps20(self):
        folder,headers=self.make_freeze();self.responses['execute']=[TimeoutError()]
        with patch('context_queries_r3.load_verified_block_headers',return_value=headers):
            result=self.live.submit(folder/'query.sql','unknown',freeze_manifest=folder/'freeze_manifest.json');self.assertIsNone(result['execution_id'])
            other=ContextDuneR4(self.w,self.transport,clock=self.clock,sleeper=self.clock.sleep,rng=lambda:0)
            with self.assertRaises(RequestBlocked):other.submit(folder/'query.sql','again',freeze_manifest=folder/'freeze_manifest.json')
        self.assertEqual(len(self.calls),1);self.assertEqual(self.live.db.snapshot()['dune_credits']['r2_risk'],'20.5')

    def test_terminal_failed_sql_single_reasoned_retry_and_cap_failure_refusal(self):
        self.state.update(state='QUERY_STATE_FAILED',status_response={'error':{'message':'temporarily unavailable'}});self.save()
        folder,headers=self.make_freeze()
        with patch('context_queries_r3.load_verified_block_headers',return_value=headers):
            result=self.live.retry_failed(self.folder,folder/'query.sql','transient',folder/'freeze_manifest.json','Provider terminal temporary outage','CONFIRMED_TRANSIENT')
            self.assertEqual(result['execution_id'],'B'*26)
            with self.assertRaises(RequestBlocked):self.live.retry_failed(self.folder,folder/'query.sql','again',folder/'freeze_manifest.json','same outage','CONFIRMED_TRANSIENT')
        self.assertEqual(len(self.calls),1)
        self.state['status_response']['error']['message']='max credit cost reached';self.save()
        with self.assertRaises(RequestBlocked):self.live.retry_failed(self.folder,folder/'query.sql','cap',folder/'freeze_manifest.json','raise cap','CONFIRMED_TRANSIENT')


class HistoricalAuditCliExitTests(unittest.TestCase):
    def test_audit_cli_failure_decision_propagates_nonzero(self):
        root=Path(__file__).resolve().parents[1]
        code="import audit_context_contracts_r4 as m; m.audit=lambda *a: {'all_block_and_date_domains_verified':False,'top_root_fact_conflicts':1,'decision':'DEPENDENT_EVIDENCE_REPAIR_REQUIRED'}; raise SystemExit(m.main(['--tree','.','--output','unused']))"
        result=subprocess.run([sys.executable,'-c',code],cwd=root/'src',capture_output=True,text=True)
        self.assertEqual(result.returncode,1,result.stderr)

    def test_dune_cli_deferred_output_is_nonzero(self):
        root=Path(__file__).resolve().parents[1]
        code="import dune_r4 as m; m.execute_dune=lambda *a,**k: {'status':'DEFERRED_CONTEXT_CLOCK'}; raise SystemExit(m.main(['dune','--work','.','--freeze','unused']))"
        result=subprocess.run([sys.executable,'-c',code],cwd=root/'src',capture_output=True,text=True)
        self.assertEqual(result.returncode,1,result.stderr)


if __name__=='__main__':unittest.main()
