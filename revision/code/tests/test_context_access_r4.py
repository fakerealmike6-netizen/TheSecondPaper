import json
from pathlib import Path
import sqlite3
import unittest

import test_context_access_r3 as old_test_support
from context_access_r3 import RpcAccess as OldRpc, session as old_session
from context_access_r4 import (RpcAccess, R4_AUTH, migrate, db_path, retry_path,
    clock_usage, raw_risk, rpc_identity, readonly_snapshot, read, sha)
from budget_r2 import AUTH
from read_retry_r4 import ReadRetryStore

class FakeClock:
    def __init__(self): self.value=1000.0; self.sleeps=[]
    def __call__(self): return self.value
    def sleep(self, seconds): self.sleeps.append(seconds); self.value += seconds

class ContextAccessR4Tests(unittest.TestCase):
    def setUp(self):
        self.oldcase=old_test_support.ContextAccessR3Tests(); self.oldcase.setUp()
        self.old=self.oldcase.w; self.w=self.old.parent/'r4'
        self.oldcase.capabilities()
        self.oldcase.put(self.old/'RUN_STATE.json', {'phase':'CHECKPOINT_1B_R3_REACHED','new_network_submissions_allowed':False})
        self.oldcase.put(self.w/'configs/STAGE1B_R4_POLICY.json', {'authorization_id':R4_AUTH,'dune':{'authorization_id':AUTH}})
        with old_session(self.old,'atomic_simple_transfer','earlier_context'): pass
        self.before=readonly_snapshot(self.old/'private/shared_budget_r3.sqlite')
        self.oldhash=sha(self.old/'private/shared_budget_r3.sqlite')
        self.receipt=migrate(self.w,self.old)
        self.clock=FakeClock(); self.calls=[]

    def tearDown(self): self.oldcase.tearDown()
    def plans(self, n=2):
        return [{'method':'eth_getBalance','params':['0x'+str(i+1)*40,'0x123']} for i in range(n)]
    def success(self, requests, bound):
        self.calls.append(requests)
        return 200,json.dumps([{'jsonrpc':'2.0','id':r['id'],'result':'0x0'} for r in requests]).encode()
    def access(self, transport=None):
        return RpcAccess(self.w, transport or self.success, clock=self.clock, monotonic=self.clock, sleep=self.clock.sleep, rng=lambda:0.5)

    def test_migration_preserves_all_old_rows_costs_and_clocks_idempotently(self):
        self.assertEqual(self.before,readonly_snapshot(db_path(self.w)))
        self.assertEqual(self.oldhash,sha(self.old/'private/shared_budget_r3.sqlite'))
        self.assertTrue(all(x['identical'] for x in self.receipt['historical_tables'].values()))
        self.assertFalse(self.receipt['new_allowance_granted'])
        self.assertEqual(self.receipt,migrate(self.w,self.old))
        self.assertGreater(clock_usage(self.w,'atomic_simple_transfer'),0)
        self.assertEqual(raw_risk(self.w),self.receipt['inherited_resource']['inherited_raw_risk_bytes'])

    def test_old_success_cache_does_not_send_or_charge_again(self):
        result=self.access().call_batch(self.oldcase.plans(),'SHARED','renamed_run_label')
        self.assertEqual(result['status'],'COMPLETE'); self.assertEqual(self.calls,[])
        self.assertEqual(result['actual_operations_this_call'],0)
        self.assertEqual(self.before,result['snapshot'])

    def test_only_failed_or_missing_members_retried_by_exact_response_id(self):
        def transport(requests,bound):
            self.calls.append(requests)
            rows=[{'jsonrpc':'2.0','id':r['id'],'result':'0x0'} for r in requests]
            if len(self.calls)==1: rows=rows[:1]
            return 200,json.dumps(list(reversed(rows))).encode()
        result=self.access(transport).call_batch(self.plans(),'SHARED','partial')
        self.assertEqual(result['status'],'COMPLETE')
        self.assertEqual([len(x) for x in self.calls],[2,1])
        self.assertEqual(self.calls[1][0]['params'],self.plans()[1]['params'])
        self.assertEqual(result['actual_operations_this_call'],3)
        self.assertEqual(result['snapshot']['rpc_operations']['actual'],'5')
        self.assertEqual(result['snapshot']['alchemy_cu']['reserved'],'50')

    def test_timeout_three_attempts_persist_across_restart_batch_split_and_renaming(self):
        def failed(requests,bound): self.calls.append(requests); raise TimeoutError('synthetic')
        result=self.access(failed).call_batch(self.plans(1),'SHARED','first')
        self.assertEqual(len(self.calls),3); self.assertEqual(result['status'],'PARTIAL')
        self.assertEqual(result['members'][0]['status'],'RETRIES_EXHAUSTED')
        again=self.access(failed).call_batch(self.plans(1),'SHARED','new_label')
        self.assertEqual(len(self.calls),3); self.assertEqual(again['actual_operations_this_call'],0)
        self.assertEqual(result['snapshot'],again['snapshot'])
        self.assertEqual(len(ReadRetryStore(retry_path(self.w)).attempts(rpc_identity('SYNTHETIC_TRANSPORT',self.plans(1)[0]))),3)

    def test_http_429_retry_after_respected_then_success_without_reset(self):
        def transport(requests,bound):
            self.calls.append(requests)
            if len(self.calls)==1:return 429,b'limited',{'Retry-After':'7'}
            return 200,json.dumps([{'jsonrpc':'2.0','id':r['id'],'result':'0x1'} for r in requests]).encode()
        result=self.access(transport).call_batch(self.plans(1),'SHARED','limited')
        self.assertEqual(result['status'],'COMPLETE'); self.assertEqual(self.clock.sleeps,[7])
        self.assertEqual(result['actual_operations_this_call'],2)

    def test_retry_after_beyond_deadline_defers_without_shortening_or_empty_success(self):
        def transport(requests,bound):self.calls.append(requests);return 503,b'busy',{'Retry-After':'60'}
        result=self.access(transport).call_batch(self.plans(1),'SHARED','defer',deadline=self.clock()+40)
        self.assertEqual(len(self.calls),1); self.assertEqual(self.clock.sleeps,[])
        self.assertEqual(result['members'][0]['status'],'DEFERRED')
        self.clock.value+=60
        result=self.access().call_batch(self.plans(1),'SHARED','resume')
        self.assertEqual(result['status'],'COMPLETE')
        self.assertEqual(result['members'][0]['attempt_no'],2)

    def test_permanent_entitlement_error_does_not_retry_or_poison_other_member(self):
        def transport(requests,bound):
            self.calls.append(requests)
            return 200,json.dumps([{'jsonrpc':'2.0','id':r['id'], **({'error':{'code':-32602,'message':'invalid params'}} if i==0 else {'result':'0x0'})} for i,r in enumerate(requests)]).encode()
        result=self.access(transport).call_batch(self.plans(),'SHARED','permanent')
        self.assertEqual(len(self.calls),1);self.assertEqual(result['status'],'PARTIAL')
        self.assertEqual([m['status'] for m in result['members']],['PERMANENT_FAILURE','SUCCESS_VALIDATED'])

    def test_duplicate_response_id_not_bound_to_first_and_success_sibling_reused(self):
        def transport(requests,bound):
            self.calls.append(requests)
            rows=[{'jsonrpc':'2.0','id':r['id'],'result':'0x0'} for r in requests]
            return 200,json.dumps(rows+[rows[0]]).encode()
        result=self.access(transport).call_batch(self.plans(),'SHARED','duplicate')
        self.assertEqual(result['members'][0]['error_class'],'DUPLICATE_RPC_RESPONSE_ID')
        self.assertEqual(result['members'][1]['status'],'SUCCESS_VALIDATED')
        self.assertEqual(len(self.calls),1)

    def test_unrelated_unknown_read_does_not_globally_block_other_read(self):
        store=ReadRetryStore(retry_path(self.w),clock=self.clock)
        claim=store.claim(rpc_identity('SYNTHETIC_TRANSPORT',self.plans()[0]))
        store.mark_dispatched(claim['attempt_id'],accounting={'synthetic_old_risk':'retained'})
        result=self.access().call_batch([self.plans()[1]],'SHARED','independent')
        self.assertEqual(result['status'],'COMPLETE'); self.assertEqual(len(self.calls),1)
        self.assertEqual(store.attempts(rpc_identity('SYNTHETIC_TRANSPORT',self.plans()[0]))[0]['outcome'],'IN_FLIGHT')

    def test_budget_insufficient_no_dispatch_no_phantom_attempt_cost(self):
        access=self.access()
        with access.ledger.connection() as db: db.execute("UPDATE limits SET cap='2' WHERE unit='rpc_operations'")
        with self.assertRaisesRegex(RuntimeError,'insufficient'):access.call_batch(self.plans(),'SHARED','budget')
        self.assertEqual(self.calls,[])
        attempts=access.store.attempts(rpc_identity('SYNTHETIC_TRANSPORT',self.plans()[0]))
        self.assertEqual(attempts[0]['outcome'],'ABANDONED_BEFORE_DISPATCH')

    def test_success_cache_requires_unchanged_evidence(self):
        result=self.access().call_batch(self.plans(1),'SHARED','first')
        (self.w/result['members'][0]['artifact_path']).write_text('{}')
        with self.assertRaisesRegex(RuntimeError,'cache provenance'): self.access().call_batch(self.plans(1),'SHARED','reuse')

    def test_capability_scope_counts_selectors_not_three_retry_attempts(self):
        def transport(requests,bound):
            self.calls.append(requests)
            if len(self.calls)<3:raise TimeoutError('synthetic')
            return 200,json.dumps([{'jsonrpc':'2.0','id':r['id'],'result':'0x0'} for r in requests]).encode()
        result=self.access(transport).call_batch(self.plans(3),'SHARED','cap_retry',capability=True)
        self.assertEqual(result['status'],'COMPLETE')
        self.assertEqual(result['actual_operations_this_call'],9)
        self.assertEqual(len(self.calls),3)

if __name__=='__main__': unittest.main()
