from pathlib import Path
import copy
import importlib
import json
import sqlite3
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
C = HERE.parent if HERE.name == 'tests' else HERE.parents[2] / 'code'
R = C.parent
sys.path[:0] = [str(C / 'src'), str(C / 'tests')]
if HERE.name != 'tests': tempfile.tempdir = str(R / 'audits/context/testtmp')
import stage1d_bq_auth_recovery as fix
from read_retry_r4 import ReadRetryStore
from page_attempts import atomic_json
staged = importlib.import_module('stage1d_bigquery_probe_staged'
    if (HERE / 'stage1d_bigquery_probe_staged.py').exists() else 'stage1d_bigquery_probe')
import test_stage1d_bigquery_probe as fixture
TABLE = fixture.TABLE

TransportError = type('TransportError', (Exception,), {'__module__':'google.auth.exceptions'})
RefreshError = type('RefreshError', (Exception,), {'__module__':'google.auth.exceptions'})
NO_RESPONSE = [{'status':None, 'body':b'', 'complete':False, 'retry_after':None}]


class ClassificationTests(unittest.TestCase):
    def test_exact_google_transport_is_retryable_without_message(self):
        value = fix.classify_bigquery_failure(TransportError('DO_NOT_PERSIST_SECRET'), transport_evidence=NO_RESPONSE)
        self.assertEqual(('RETRYABLE_FAILURE', fix.CORRECTED), (value['outcome'], value['error_class']))
        self.assertEqual(fix.TYPE, value['exception_type'])
        self.assertNotIn('DO_NOT_PERSIST_SECRET', json.dumps(value))

    def test_same_named_unrelated_exception_is_not_unlocked(self):
        Other = type('TransportError', (Exception,), {'__module__':'unrelated'})
        self.assertEqual('PERMANENT_FAILURE', fix.classify_bigquery_failure(Other(), transport_evidence=NO_RESPONSE)['outcome'])

    def test_actual_http_auth_errors_and_refresh_failure_remain_permanent(self):
        for code in (401,403,404,400):
            with self.subTest(code=code):
                self.assertEqual('PERMANENT_FAILURE', fix.classify_bigquery_failure(TransportError(), http_status=code, transport_evidence=NO_RESPONSE)['outcome'])
        self.assertEqual('PERMANENT_FAILURE', fix.classify_bigquery_failure(RefreshError(), transport_evidence=NO_RESPONSE)['outcome'])

    def test_no_body_is_not_assumed_when_transport_shape_different(self):
        for evidence in ([], NO_RESPONSE * 2, [dict(NO_RESPONSE[0], body=b'partial')],
                         [dict(NO_RESPONSE[0], status=403)], [dict(NO_RESPONSE[0], complete=True)]):
            with self.subTest(evidence=evidence):
                self.assertEqual('PERMANENT_FAILURE', fix.classify_bigquery_failure(TransportError(), transport_evidence=evidence)['outcome'])

    def test_existing_timeout_and_rate_limit_rules_preserved(self):
        self.assertEqual('RETRYABLE_FAILURE', fix.classify_bigquery_failure(TimeoutError(), transport_evidence=NO_RESPONSE)['outcome'])
        self.assertEqual('HTTP_429', fix.classify_bigquery_failure(TransportError(), http_status=429)['error_class'])


class AdoptionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name); self.time = 1000.0
        self.store = ReadRetryStore(self.work/'private/retry.sqlite', clock=lambda:self.time, rng=lambda:0)
        self.identity = {'provider':'GOOGLE_BIGQUERY_EXISTING_ADC', 'method':'tables.get',
            'project':'synthetic-project','location':'US','table':TABLE}
        self.claim = self.store.claim(self.identity)
        self.job = 'bq_read_' + self.claim['attempt_id']
        self.store.mark_dispatched(self.claim['attempt_id'], accounting={'job':self.job,'rpc_operations':1,'bigquery_bytes':0})
        raw = self.work/'raw/response.bin'; raw.parent.mkdir(); raw.write_bytes(b'')
        self.receipt = self.work/'raw/receipt.json'
        self.envelope = {'status':'TransportError','identity':self.identity,'attempt_id':self.claim['attempt_id'],
            'attempt_no':1,'job':self.job,'result':None,'actual_query_executed':False,
            'raw_sources':[{'path':'raw/response.bin','sha256':fix.sha(raw),'bytes':0,'http_status':None,'complete':False}]}
        atomic_json(self.receipt,self.envelope)
        self.store.finish(self.claim['attempt_id'],'PERMANENT_FAILURE',error_class='TransportError',
            receipt={'artifact_path':'raw/receipt.json','artifact_sha256':fix.sha(self.receipt),'job':self.job})
        self.diagnosis = self.work/'private/diagnosis.json'
        atomic_json(self.diagnosis, {'schema_version':'stage1d-sdk-exception-type-diagnosis-v1',
            'status':'EVIDENCE_VERIFIED','exception_type':fix.TYPE,'attempt_id':self.claim['attempt_id'],
            'original_receipt_sha256':fix.sha(self.receipt),'reviewer':'SYNTHETIC_REVIEWER',
            'type_evidence_basis':'SYNTHETIC_EXPLICIT_TYPED_TRANSPORT_EXCEPTION'})
        self.environment = self.work/'private/environment.json'
        atomic_json(self.environment, {'status':'SYNTHETIC_TCP_CHECK_AVAILABLE','endpoint':'127.0.0.1:7890'})
        self.resource = self.work/'private/resource_clock_raw.json'
        atomic_json(self.resource, {'rpc_operations':1,'bigquery_scan_bytes':0,'dune_risk':'325.8',
            'raw_uncertainty':8388608,'elapsed_online_seconds':169})
        self.resource_bytes = self.resource.read_bytes()
        self.before_attempts = self.store.attempts(self.identity)
        self.review_path = self.work/'private/review.json'

    def prepare(self):
        result = fix.prepare_review(self.work,self.store.path,self.claim['attempt_id'],
            diagnosis=self.diagnosis,environment_evidence=[self.environment])
        atomic_json(self.review_path,result); return result

    def adopt(self): return fix.adopt_review(self.work,self.store.path,self.review_path,now=self.time)

    def test_original_attempt_immutable_next_is_two_and_idempotent(self):
        self.prepare(); result = self.adopt()
        self.assertEqual('APPLIED', result['status'])
        self.assertEqual(self.before_attempts, self.store.attempts(self.identity))
        self.assertEqual(self.resource_bytes, self.resource.read_bytes())
        self.assertEqual('DEFERRED', self.store.claim(self.identity)['state'])
        self.time += 2
        second = self.store.claim(self.identity)
        self.assertEqual((2,self.claim['attempt_id']), (second['attempt_no'],second['retry_of']))
        self.assertEqual('ALREADY_APPLIED', self.adopt()['status'])
        self.assertEqual(self.before_attempts[0], self.store.attempts(self.identity)[0])

    def test_original_three_total_attempts_not_reset_after_correction(self):
        self.prepare(); self.adopt(); self.time += 2
        second = self.store.claim(self.identity); self.store.mark_dispatched(second['attempt_id'])
        self.store.finish(second['attempt_id'],'RETRYABLE_FAILURE',error_class=fix.CORRECTED)
        third = self.store.claim(self.identity); self.store.mark_dispatched(third['attempt_id'])
        self.store.finish(third['attempt_id'],'RETRYABLE_FAILURE',error_class=fix.CORRECTED)
        self.assertEqual('RETRIES_EXHAUSTED', self.store.claim(self.identity)['state'])
        self.assertEqual('ALREADY_APPLIED', self.adopt()['status'])
        self.assertEqual('RETRIES_EXHAUSTED', self.store.claim(self.identity)['state'])
        self.assertEqual([1,2,3],[a['attempt_no'] for a in self.store.attempts(self.identity)])
        self.assertEqual(self.resource_bytes, self.resource.read_bytes())

    def test_unverified_or_wrong_type_diagnosis_is_rejected(self):
        original = json.loads(self.diagnosis.read_text())
        for patch in ({'status':'PENDING_REVIEW'}, {'exception_type':{'module':'unrelated','qualname':'TransportError'}},
                      {'original_receipt_sha256':'f'*64}, {'type_evidence_basis':''}):
            atomic_json(self.diagnosis, original|patch)
            with self.assertRaises(ValueError): self.prepare()
        self.assertEqual(self.before_attempts, self.store.attempts(self.identity))

    def test_changed_old_attempt_or_receipt_cannot_be_reinterpreted(self):
        self.prepare(); self.receipt.write_text('{}')
        with self.assertRaises(ValueError): self.adopt()
        self.assertEqual(self.before_attempts,self.store.attempts(self.identity))

    def test_active_other_read_blocks_without_changing_original(self):
        self.prepare(); self.store.claim(self.identity|{'table':TABLE+'_different'})
        with self.assertRaises(RuntimeError): self.adopt()
        self.assertEqual('PERMANENT_FAILURE',self.store.claim(self.identity)['state'])
        self.assertEqual(self.before_attempts,self.store.attempts(self.identity))

    def test_active_writer_or_online_session_blocks(self):
        self.prepare(); lock=self.work/'private/network_worker.lock'; lock.write_text('SYNTHETIC_ACTIVE')
        with self.assertRaises(RuntimeError): self.adopt()
        lock.unlink(); atomic_json(self.work/'private/stage1d_sessions/active.json',{'closed':False})
        with self.assertRaises(RuntimeError): self.adopt()
        self.assertEqual(self.before_attempts,self.store.attempts(self.identity))

    def test_changed_request_success_or_increased_attempt_allowance_rejected(self):
        review = self.prepare(); review['remaining_attempts']=3; atomic_json(self.review_path,review)
        with self.assertRaises(ValueError): self.adopt()
        self.assertEqual('PERMANENT_FAILURE',self.store.claim(self.identity)['state'])

    def test_genuine_http_failure_and_missing_failure_never_unlock(self):
        self.envelope['raw_sources'][0]['http_status']=403
        atomic_json(self.receipt,self.envelope)
        with self.assertRaises(ValueError): self.prepare()
        with self.assertRaises(ValueError): fix.prepare_review(self.work,self.store.path,'ABSENT',
            diagnosis=self.diagnosis,environment_evidence=[self.environment])

    def test_static_inference_keeps_missing_module_and_verifies_bound_source(self):
        value = json.loads(self.diagnosis.read_text())
        source = self.work/'private/sdk_static.py'; source.write_text('SYNTHETIC_SOURCE')
        value.update(type_evidence_kind='BOUND_SDK_STATIC_INFERENCE', historical_module_directly_recorded=False,
                     type_source_evidence=[fix.binding(self.work, source)])
        atomic_json(self.diagnosis,value); self.prepare()
        source.write_text('DIFFERENT_SOURCE')
        with self.assertRaises(ValueError): self.adopt()
        self.assertEqual(self.before_attempts,self.store.attempts(self.identity))

    def test_static_inference_cannot_claim_historical_module_was_observed(self):
        value=json.loads(self.diagnosis.read_text())
        value.update(type_evidence_kind='BOUND_SDK_STATIC_INFERENCE', historical_module_directly_recorded=True,
                     type_source_evidence=[fix.binding(self.work,self.environment)])
        atomic_json(self.diagnosis,value)
        with self.assertRaises(ValueError): self.prepare()


class ProbeIntegrationTests(unittest.TestCase):
    def test_staged_probe_exhausts_three_transport_attempts_and_preserves_each_cost(self):
        h = fixture.ProbeTests(); h.setUp(); self.addCleanup(h.doCleanups)
        class Sdk(h.Sdk):
            def schema(self, table):
                h.calls.append(table); self.transport_evidence.extend(copy.deepcopy(NO_RESPONSE))
                raise TransportError('DO_NOT_PERSIST_SECRET')
        def invoke(): return staged.probe(h.work,'synthetic_query','private/config.json','schema',table=TABLE,
            runtime=h.runtime,sdk_factory=Sdk,clock=lambda:h.runtime.time,sleep=h.runtime.sleep)
        self.assertEqual('RETRIES_EXHAUSTED',invoke()['status'])
        self.assertEqual('RETRIES_EXHAUSTED',invoke()['status'])
        self.assertEqual(3,len(h.calls))
        self.assertEqual(3,sum(j['rpc_operations'] for j in h.ledger.jobs.values()))
        self.assertEqual(3*staged.RESPONSE_BOUND,h.runtime.raw_risk(h.work))
        for path in (h.work/'raw').rglob('receipt.json'):
            envelope=json.loads(path.read_text())
            self.assertEqual(fix.TYPE,envelope['failure_classification']['exception_type'])
            self.assertNotIn('DO_NOT_PERSIST_SECRET',path.read_text())

    def test_staged_probe_genuine_credentials_error_stops_at_one(self):
        h = fixture.ProbeTests(); h.setUp(); self.addCleanup(h.doCleanups)
        class Sdk(h.Sdk):
            def schema(self,table):
                h.calls.append(table); self.transport_evidence.extend(copy.deepcopy(NO_RESPONSE))
                raise RefreshError('invalid grant')
        result=staged.probe(h.work,'synthetic_query','private/config.json','schema',table=TABLE,
            runtime=h.runtime,sdk_factory=Sdk,clock=lambda:h.runtime.time,sleep=h.runtime.sleep)
        self.assertEqual('PERMANENT_FAILURE',result['status']);self.assertEqual(1,len(h.calls))


if __name__=='__main__': unittest.main()
