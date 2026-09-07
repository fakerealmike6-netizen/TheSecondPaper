"""Independent bounded E01 integration review; synthetic temporary ledgers only."""
import json
from pathlib import Path
import sqlite3
import unittest
from contextlib import closing
import subprocess
import time
from unittest.mock import patch
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
import test_dune_r4 as dune_support
import test_context_access_r4 as rpc_support
import test_context_access_r3 as old_rpc_support
from dune_r4 import ContextDuneR4
from context_access_r4 import _import_rpc, RpcAccess, rpc_identity, migrate, R4_AUTH, db_path
from context_access_r3 import RpcAccess as OldRpc
from budget_r2 import AUTH
from read_retry_r4 import ReadRetryStore
from page_attempts import atomic_json, RequestBlocked
from context_access_r3 import sha

class RetryIntegrationReviewR4Tests(unittest.TestCase):
    def dune(self):
        case=dune_support.DuneR4Tests();case.setUp();self.addCleanup(case.tearDown);return case
    def rpc(self):
        case=rpc_support.ContextAccessR4Tests();case.setUp();self.addCleanup(case.tearDown);return case

    def test_prior_larger_unknown_export_risk_is_additive_with_new_retry(self):
        case=self.dune();case.legacy()
        case.live.db.reserve_export(case.job,10,{'rate_evidence':{'synthetic':True},'result_metadata':case.md,'basis':'Original failed request unresolved upper; no reduction evidence'})
        self.assertEqual('10.5',case.live.db.snapshot()['dune_credits']['r2_risk'])
        result=case.live.export(case.folder)
        self.assertTrue(result['progress']['complete']);self.assertEqual(1,len(case.calls))
        self.assertEqual('13.5',case.live.db.snapshot()['dune_credits']['r2_risk'],
                         'Old unresolved 10 plus newly dispatched page upper 3 must both remain occupied')

    def test_old_rpc_row_selector_must_match_imported_envelope_request(self):
        case=self.rpc()
        # Mutate only this synthetic historical ledger row, retaining a valid
        # receipt for a different selector. It must not seed the new cache.
        path=case.old/'private/shared_budget_r3.sqlite'
        with closing(sqlite3.connect(path)) as db:
            identity,plan=db.execute("SELECT identity,plan FROM r3_rpc_requests WHERE plan LIKE '%eth_getBalance%' LIMIT 1").fetchone()
            changed=json.loads(plan);changed['params'][0]='0x'+'f'*40
            db.execute('UPDATE r3_rpc_requests SET plan=? WHERE identity=?',(json.dumps(changed),identity))
            db.commit()
        new=case.w/'private/review_import.sqlite'
        with self.assertRaises((ValueError,RuntimeError),msg='A valid receipt for another address cannot populate this logical key'):
            _import_rpc(case.w,case.old,ReadRetryStore(new))

    def test_valid_zero_row_dune_page_reused_without_new_fee_after_restart(self):
        case=self.dune();case.md.update(total_row_count=0,total_result_set_bytes=0);case.save()
        body={'execution_id':case.execution,'state':'QUERY_STATE_COMPLETED','result':{'rows':[],'metadata':case.md|{'row_count':0,'result_set_bytes':0}}}
        case.responses['results']=[case.response(200,body)]
        result=case.live.export(case.folder);self.assertTrue(result['progress']['complete'])
        before=case.live.db.snapshot();other=ContextDuneR4(case.w,case.transport,clock=case.clock,sleeper=case.clock.sleep)
        self.assertTrue(other.export(case.folder)['cache_reused']);self.assertEqual(before,other.db.snapshot());self.assertEqual(1,len(case.calls))

    def test_rpc_successful_sibling_zero_survives_retry_exhaustion_and_restart(self):
        case=self.rpc()
        def transport(requests,bound):
            case.calls.append(requests)
            rows=[]
            for request in requests:
                value={'result':'0x0'} if request['params'][0]==case.plans()[0]['params'][0] else {'error':{'code':-32005,'message':'Rate limit'}}
                rows.append({'jsonrpc':'2.0','id':request['id'],**value})
            return 200,json.dumps(rows).encode()
        result=case.access(transport).call_batch(case.plans(),'SHARED','review_partial')
        self.assertEqual('PARTIAL',result['status']);self.assertEqual([2,1,1],[len(r) for r in case.calls])
        before=result['snapshot'];restarted=case.access(transport).call_batch(case.plans(),'SHARED','review_restart')
        self.assertEqual(before,restarted['snapshot']);self.assertEqual(0,restarted['actual_operations_this_call'])
        self.assertEqual('0x0',restarted['members'][0]['result']);self.assertEqual('RETRIES_EXHAUSTED',restarted['members'][1]['status'])

    def test_unknown_sql_post_remains_blocked_after_noncreation_get_success(self):
        case=self.dune();folder,headers=case.make_freeze();case.responses['execute']=[TimeoutError()]
        with patch('context_queries_r3.load_verified_block_headers',return_value=headers):
            result=case.live.submit(folder/'query.sql','review_unknown',freeze_manifest=folder/'freeze_manifest.json')
            self.assertIsNone(result['execution_id'])
            # Independent known execution GET success cannot resolve an unknown
            # creation POST or clear its retained twenty-credit reservation.
            case.state['state']='QUERY_STATE_PENDING';case.save();case.live.poll(case.folder)
            with self.assertRaises(RequestBlocked):
                ContextDuneR4(case.w,case.transport,clock=case.clock,sleeper=case.clock.sleep).submit(folder/'query.sql','changed-label',freeze_manifest=folder/'freeze_manifest.json')
        self.assertEqual(1,sum(op=='execute' for op,_,_ in case.calls));self.assertEqual('20.5',case.live.db.snapshot()['dune_credits']['r2_risk'])

    def test_dune_cli_deferred_or_exhausted_work_cannot_exit_as_success(self):
        source=Path(__file__).resolve().parents[1]/'src'
        for result in ({'status':'DEFERRED_CONTEXT_CLOCK'},{'export_status':'RETRIES_EXHAUSTED','progress':{'complete':False}}):
            code="import dune_r4 as m,sys; m.execute_dune=lambda *a,**k:"+repr(result)+"; sys.argv=['dune_r4','dune','--work','.','--freeze','synthetic.json']; raise SystemExit(m.main())"
            process=subprocess.run([sys.executable,'-B','-c',code],cwd=source,capture_output=True,text=True)
            self.assertNotEqual(0,process.returncode,'Deferred/exhausted output must not count as a successful CLI run: '+process.stdout)

    def test_two_actual_old_rpc_failures_leave_only_one_r4_attempt(self):
        old=old_rpc_support.ContextAccessR3Tests();old.setUp();self.addCleanup(old.tearDown);old.capabilities()
        plan={'method':'eth_getBalance','params':['0x'+'d'*40,'0x456']}
        def fail(*_):raise TimeoutError('synthetic old/new timeout')
        first=OldRpc(old.w,fail).call_batch([plan],'SHARED','review_old_first')
        OldRpc(old.w,fail).call_batch([plan],'SHARED','review_old_retry',retry_of=[first['members'][0]['identity']],retry_reason='Documented synthetic timeout')
        old.put(old.w/'RUN_STATE.json',{'phase':'CHECKPOINT_1B_R3_REACHED','new_network_submissions_allowed':False})
        work=old.w.parent/'r4'
        old.put(work/'configs/STAGE1B_R4_POLICY.json',{'authorization_id':R4_AUTH,'dune':{'authorization_id':AUTH}})
        old_hash=sha(old.w/'private/shared_budget_r3.sqlite');migrate(work,old.w)
        # migrate() records historical import deadlines on the actual clock;
        # keep the injected continuation clock in the same epoch.
        clock=rpc_support.FakeClock();clock.value=time.time()+1;calls=[]
        def failed(requests,bound):calls.append(requests);raise TimeoutError('synthetic third timeout')
        access=RpcAccess(work,failed,clock=clock,monotonic=clock,sleep=clock.sleep,rng=lambda:0)
        result=access.call_batch([plan],'SHARED','review_new_third')
        self.assertEqual(1,len(calls));self.assertEqual('RETRIES_EXHAUSTED',result['members'][0]['status'])
        self.assertEqual('5',result['snapshot']['rpc_operations']['actual']);self.assertEqual('50',result['snapshot']['alchemy_cu']['reserved'])
        self.assertEqual(old_hash,sha(old.w/'private/shared_budget_r3.sqlite'))
        self.assertEqual(3,len(access.store.attempts(rpc_identity('SYNTHETIC_TRANSPORT',plan))))
        restarted=RpcAccess(work,failed,clock=clock,monotonic=clock,sleep=clock.sleep,rng=lambda:0)
        self.assertEqual(result['snapshot'],restarted.call_batch([plan],'SHARED','review_final_restart')['snapshot'])
        self.assertEqual(1,len(calls))

if __name__=='__main__':unittest.main()
