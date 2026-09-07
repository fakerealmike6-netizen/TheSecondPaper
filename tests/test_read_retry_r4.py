"""R4 read attempts: synthetic transports, persistent state, no real networking."""
import concurrent.futures
import hashlib
import http.client
import json
import socket
import tempfile
import unittest
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from read_retry_r4 import ReadRetryStore, logical_key, classify_failure, retry_after_seconds

IDENTITY = {'provider':'SYNTHETIC_RPC','method':'eth_getBalance','params':['0x'+'1'*40,'0x2']}
class Clock:
    def __init__(self): self.now = 1000.0
    def __call__(self): return self.now
    def sleep(self, seconds): self.now += seconds

class ReadRetryR4Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'attempts.sqlite'; self.clock = Clock()
        self.store = ReadRetryStore(self.path, clock=self.clock, rng=lambda:0.5)
    def dispatched(self, store=None):
        store = store or self.store
        claim = store.claim(IDENTITY); self.assertEqual('CLAIMED',claim['state'])
        store.mark_dispatched(claim['attempt_id'],accounting={'risk':'20','ledger_id':claim['attempt_id']})
        return claim
    def test_identity_stable_including_historical_selectors(self):
        self.assertEqual(logical_key(IDENTITY),logical_key(dict(reversed(list(IDENTITY.items())))))
        self.assertNotEqual(logical_key(IDENTITY),logical_key(IDENTITY|{'params':[IDENTITY['params'][0],'latest']}))
        for key in ('run_id','api_key','authorization'):
            with self.subTest(key=key), self.assertRaises(ValueError): logical_key(IDENTITY|{key:'synthetic'})
    def test_success_is_reused_after_restart(self):
        claim = self.dispatched()
        self.store.finish(claim['attempt_id'],'SUCCESS',payload={'result':'0x0'},receipt={'sha':'a'})
        restarted = ReadRetryStore(self.path,clock=self.clock)
        self.assertEqual({'result':'0x0'},restarted.claim(IDENTITY)['payload'])
        self.assertEqual(1,len(restarted.attempts(IDENTITY)))
    def test_three_total_attempts_survive_restart_and_retain_risk(self):
        ids=[]
        for number in range(1,4):
            store=ReadRetryStore(self.path,clock=self.clock,rng=lambda:0)
            claim=self.dispatched(store); ids.append(claim['attempt_id'])
            self.assertEqual(number,claim['attempt_no'])
            self.assertEqual(ids[-2] if number>1 else None,claim['retry_of'])
            store.finish(claim['attempt_id'],'RETRYABLE_FAILURE',error_class='TimeoutError')
        self.assertEqual('RETRIES_EXHAUSTED',self.store.claim(IDENTITY)['state'])
        self.assertEqual(['20']*3,[json.loads(x['accounting_json'])['risk'] for x in self.store.attempts(IDENTITY)])
    def test_retry_after_seconds_persisted_and_deadline_deferred(self):
        claim=self.dispatched()
        self.store.finish(claim['attempt_id'],'RETRYABLE_FAILURE',retry_after='120')
        result=ReadRetryStore(self.path,clock=self.clock).claim(IDENTITY,deadline=1010)
        self.assertEqual('DEFERRED',result['state']); self.assertEqual(1120,result['next_eligible_at'])
        self.clock.sleep(120); self.assertEqual('CLAIMED',self.store.claim(IDENTITY)['state'])
    def test_http_date_and_jitter_lower_bound(self):
        stamp=format_datetime(datetime.fromtimestamp(1040,timezone.utc),usegmt=True)
        self.assertEqual(40,retry_after_seconds(stamp,self.clock()))
        claim=self.dispatched(); result=self.store.finish(claim['attempt_id'],'RETRYABLE_FAILURE',retry_after=stamp)
        self.assertEqual(1040,result['next_eligible_at'])
    def test_permanent_failures_do_not_retry(self):
        for code in (401,403,400): self.assertFalse(classify_failure(http_status=code)['retryable'])
        for error in ({'code':-32602,'message':'invalid params'},{'code':-32600,'message':'Free tier upgrade'},{'message':'Invalid API Key'}):
            self.assertFalse(classify_failure(provider_error=error)['retryable'])
        claim=self.dispatched(); self.store.finish(claim['attempt_id'],'PERMANENT_FAILURE')
        self.assertEqual('PERMANENT_FAILURE',self.store.claim(IDENTITY)['state'])
    def test_transient_failures_only(self):
        for error in (TimeoutError(),http.client.IncompleteRead(b'a',2),ConnectionResetError(),socket.gaierror(socket.EAI_AGAIN,'temporary')):
            self.assertTrue(classify_failure(error)['retryable'])
        self.assertFalse(classify_failure(socket.gaierror(socket.EAI_NONAME,'not known'))['retryable'])
        self.assertFalse(classify_failure(ValueError('schema'))['retryable'])
        for code in (408,429,500,502,503,504): self.assertTrue(classify_failure(http_status=code)['retryable'])
        self.assertTrue(classify_failure(provider_error={'code':-32005,'message':'rate limit'})['retryable'])
    def test_concurrent_claims_allow_one_transport(self):
        def claim(_): return ReadRetryStore(self.path,clock=self.clock).claim(IDENTITY)['state']
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            states=list(pool.map(claim,range(8)))
        self.assertEqual(1,states.count('CLAIMED')); self.assertEqual(7,states.count('IN_FLIGHT'))
    def test_recovery_requires_dead_owner_and_preserves_prior_attempt(self):
        first=self.dispatched(); self.clock.sleep(61)
        self.assertEqual('IN_FLIGHT',self.store.claim(IDENTITY)['state'])
        restarted=ReadRetryStore(self.path,clock=self.clock,rng=lambda:0,owner_alive=lambda _:False)
        second=restarted.claim(IDENTITY)
        self.assertEqual(2,second['attempt_no']); self.assertEqual(first['attempt_id'],second['retry_of'])
        old=restarted.attempts(IDENTITY)[0]
        self.assertEqual('UNRESOLVED',old['outcome']); self.assertEqual('20',json.loads(old['accounting_json'])['risk'])
    def test_budget_blocked_before_dispatch_does_not_consume_attempt(self):
        first=self.store.claim(IDENTITY)
        self.store.abandon_before_dispatch(first['attempt_id'],'BUDGET_BLOCKED')
        next_claim=self.store.claim(IDENTITY)
        self.assertEqual(1,next_claim['attempt_no']); self.assertIsNone(next_claim['retry_of'])
    def test_dispatch_and_accounting_are_immutable(self):
        claim=self.dispatched()
        with self.assertRaises(ValueError): self.store.mark_dispatched(claim['attempt_id'])
        with self.assertRaises(ValueError): self.store.abandon_before_dispatch(claim['attempt_id'])
        with self.assertRaises(ValueError): self.store.finish(claim['attempt_id'],'SUCCESS',payload={},accounting={'risk':'0'})
        self.store.finish(claim['attempt_id'],'SUCCESS',payload={})
        with self.assertRaises(ValueError): self.store.finish(claim['attempt_id'],'PERMANENT_FAILURE')
        with self.assertRaises(ValueError): self.store.finish(claim['attempt_id'],'SUCCESS',payload={'changed':True})
    def test_json_rpc_envelope_and_explicit_category_mapping(self):
        self.assertTrue(classify_failure(category='RPC_ERROR',provider_error={'error':{'code':-32005,'message':'Rate limit'}})['retryable'])
        self.assertTrue(classify_failure(category='HTTP_503')['retryable'])
        self.assertFalse(classify_failure(category='FACT_CONFLICT',http_status=503)['retryable'])
    def test_legacy_attempts_idempotent_and_success_reused(self):
        result=self.store.import_attempt(IDENTITY,'historic:first',outcome='RETRYABLE_FAILURE',accounting={'risk':'7'})
        self.store.import_attempt(IDENTITY,'historic:first',outcome='RETRYABLE_FAILURE',accounting={'risk':'7'})
        claim=self.dispatched(); self.assertEqual(2,claim['attempt_no']); self.assertEqual(result['attempt_id'],claim['retry_of'])
        self.store.finish(claim['attempt_id'],'SUCCESS',payload={'value':'80'})
        self.store.import_attempt(IDENTITY,'historic:late-failure',outcome='UNRESOLVED',accounting={'risk':'9'})
        self.assertEqual('CACHE_HIT',self.store.claim(IDENTITY)['state'])
        self.assertEqual(['7','20','9'],[json.loads(a['accounting_json'])['risk'] for a in self.store.attempts(IDENTITY)])
    def test_legacy_successful_empty_cache_is_not_new_request(self):
        path=Path(self.tmp.name)/'legacy.json'; path.write_text('{"result": []}')
        before=hashlib.sha256(path.read_bytes()).hexdigest()
        self.store.migrate_cache(IDENTITY,path,'SUCCESS',payload={'result':[]})
        self.assertEqual('CACHE_HIT',self.store.claim(IDENTITY)['state'])
        self.assertEqual(before,hashlib.sha256(path.read_bytes()).hexdigest())
    def test_imported_failure_can_retry_without_mutating_original_bytes(self):
        path=Path(self.tmp.name)/'error.json'; path.write_text('{"status":"0","result":"Max rate limit reached"}')
        original=path.read_bytes()
        history=[{'legacy_id':'known:original','outcome':'RETRYABLE_FAILURE','accounting':{'risk':'old-unknown-kept'}}]
        self.store.migrate_cache(IDENTITY,path,'RETRYABLE_FAILURE',history=history)
        self.store.migrate_cache(IDENTITY,path,'RETRYABLE_FAILURE',history=history)
        self.assertEqual(2,self.store.claim(IDENTITY)['attempt_no']); self.assertEqual(original,path.read_bytes())
    def test_unknown_history_never_resets_error_cache_to_attempt_one(self):
        path=Path(self.tmp.name)/'error.json'; path.write_text('{"status":"0"}')
        self.store.migrate_cache(IDENTITY,path,'RETRYABLE_FAILURE')
        self.assertEqual('HISTORICAL_ATTEMPT_COUNT_UNRESOLVED',self.store.claim(IDENTITY)['state'])
        self.assertEqual([],self.store.attempts(IDENTITY))
    def test_complete_three_attempt_history_exhausts_migrated_cache(self):
        path=Path(self.tmp.name)/'error.json'; path.write_text('{"status":"0"}')
        history=[{'legacy_id':str(i),'outcome':'RETRYABLE_FAILURE','accounting':{'risk':'kept'}} for i in range(3)]
        self.store.migrate_cache(IDENTITY,path,'RETRYABLE_FAILURE',history=history)
        self.assertEqual('RETRIES_EXHAUSTED',self.store.claim(IDENTITY)['state'])
    def test_migration_proof_persists_across_restart_without_resupplying_history(self):
        path=Path(self.tmp.name)/'error.json'; path.write_text('{"status":"0"}')
        history=[{'legacy_id':'one-proved','outcome':'RETRYABLE_FAILURE'}]
        self.store.migrate_cache(IDENTITY,path,'RETRYABLE_FAILURE',history=history)
        restarted=ReadRetryStore(self.path,clock=self.clock)
        restarted.migrate_cache(IDENTITY,path,'RETRYABLE_FAILURE')
        self.assertEqual(2,restarted.claim(IDENTITY)['attempt_no'])
    def test_legacy_import_rejects_conflicting_provenance_outcome(self):
        self.store.import_attempt(IDENTITY,'once',outcome='SUCCESS',payload={'value':'1'})
        with self.assertRaises(ValueError): self.store.import_attempt(IDENTITY,'once',outcome='SUCCESS',payload={'value':'2'})
    def test_equal_payload_different_identity_imports_are_separate(self):
        path=Path(self.tmp.name)/'empty.json'; path.write_text('{"result": []}')
        other=IDENTITY|{'params':['other','0x2']}
        for identity in (IDENTITY,other): self.store.migrate_cache(identity,path,'SUCCESS',payload={'result':[]})
        self.assertEqual('CACHE_HIT',self.store.claim(other)['state'])

if __name__=='__main__': unittest.main()
