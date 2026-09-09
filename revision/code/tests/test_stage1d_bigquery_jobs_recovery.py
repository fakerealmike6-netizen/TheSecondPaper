"""Offline SQL submission, metadata, wire-type and restart contracts."""
from contextlib import contextmanager, closing
from datetime import datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import stage1d_bigquery_jobs as jobs
import stage1d_bigquery_probe as probe
from read_retry_r4 import ReadRetryStore

def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding='utf8')


class Ledger:
    def __init__(self, root):
        self.path = root / 'fake_ledger.sqlite'
        with self.connection() as db:
            db.execute('CREATE TABLE amounts(job TEXT, unit TEXT, reserved TEXT, actual TEXT)')
    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path)
        try:
            yield db; db.commit()
        finally: db.close()
    def snapshot(self): return {}
    def reserve(self, job, provider, purpose, amounts):
        with self.connection() as db:
            for unit, amount in amounts.items():
                db.execute('INSERT INTO amounts VALUES(?,?,?,NULL)', (job, unit, str(amount)))
    def settle(self, job, amounts):
        with self.connection() as db:
            for unit, amount in amounts.items():
                db.execute('UPDATE amounts SET actual=? WHERE job=? AND unit=?', (str(amount), job, unit))


class FaultTests(unittest.TestCase):
    def fixture(self, root):
        query = {'name': 'synthetic', 'query_id': 'synthetic:q', 'scope_hash': 'scopehash'}
        write(root / 'private/BATCH_QUERY_FREEZE.json', {'queries': [query]})
        write(root / 'spec.json', {'sql_sha256': 'sqlhash', 'query_id': 'synthetic:q', 'scope_hash': 'scopehash'})
        write(root / 'dry.json', {'status': 'SUCCESS_VALIDATED', 'result': {
            'kind': 'DRY_RUN', 'estimated_processed_bytes': 100}, 'identity': {'sql_sha256': 'sqlhash'}})
        write(root / 'plan.json', {'dry_spec_path': 'spec.json', 'dry_spec_sha256': jobs.sha(root / 'spec.json'),
                                 'dry_receipt_path': 'dry.json', 'dry_receipt_sha256': jobs.sha(root / 'dry.json')})
        return {'project': 'ethereum-paper-analysis'}, Ledger(root)

    @contextmanager
    def environment(self, root, fake_call):
        config, ledger = self.fixture(root)
        class Runtime:
            ledger_factory = staticmethod(lambda path: ledger)
        policy = types.ModuleType('stage1d_recovery_policy')
        policy.bq_reservation = lambda estimate, snapshot: {'maximum_bytes_billed': 110}
        with patch.object(jobs, 'Runtime', Runtime), patch.object(jobs, 'call', fake_call), \
             patch.object(probe, 'dry_spec', return_value='SELECT 1'), \
             patch.dict(sys.modules, {'stage1d_recovery_policy': policy}):
            yield config

    def test_failed_poll_retains_same_persisted_attempt_identity(self):
        selectors = []
        def call(work, query, config, operation, values, **kwargs):
            if operation == 'jobs.insert': return {'status': 'SUCCESS_VALIDATED', 'result': {}}
            selectors.append(dict(values))
            return {'status': 'RETRIES_EXHAUSTED'}
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temporary:
            root = Path(temporary)
            with self.environment(root, call) as config:
                jobs.execute_plan(root, 'synthetic', config, 'plan.json')
                jobs.execute_plan(root, 'synthetic', config, 'plan.json')
        self.assertEqual(selectors[0], selectors[1], 'A failed identical poll must not receive a fresh retry key')

    def test_explicitly_undispatched_submit_can_retry_same_job(self):
        insert_ids = []
        def call(work, query, config, operation, values, **kwargs):
            if operation == 'jobs.insert':
                insert_ids.append(kwargs['body']['jobReference']['jobId'])
                return {'status': 'SDK_ADC_SETUP_FAILED', 'dispatched': False}
            return {'status': 'HTTP_404'}
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temporary:
            root = Path(temporary)
            with self.environment(root, call) as config:
                jobs.execute_plan(root, 'synthetic', config, 'plan.json')
                jobs.execute_plan(root, 'synthetic', config, 'plan.json')
        self.assertEqual(2, len(insert_ids), 'A proven local no-dispatch failure must not permanently strand an unsubmitted job')
        self.assertEqual(insert_ids[0], insert_ids[1])

    def test_script_parent_without_confirmed_children_cannot_be_complete_zero(self):
        parent = {'jobReference': {'projectId': 'p', 'jobId': 'script'},
                  'configuration': {'query': {}}, 'status': {'state': 'DONE'},
                  'statistics': {'creationTime': '20', 'query': {'totalBytesBilled': '100', 'statementType': 'SCRIPT'}}}
        result = jobs.monthly_usage([parent], 10, 30)
        self.assertTrue(result['known_billed_bytes'] >= 100 or not result['complete_settlement'],
                        'A billed SCRIPT parent cannot disappear without verified child coverage')

    def test_mutating_sql_is_rejected_at_http_submission_boundary(self):
        body = {'jobReference': {'projectId': 'ethereum-paper-analysis', 'location': 'US', 'jobId': 'fixed'},
                'configuration': {'query': {'query': 'DELETE FROM `allowed.table.name` WHERE TRUE',
                    'useLegacySql': False, 'maximumBytesBilled': '100'}}}
        with self.assertRaises(ValueError):
            jobs.request_spec('ethereum-paper-analysis', 'jobs.insert', {}, body)

    def test_boolean_wire_value_must_not_accept_integer_one(self):
        with self.assertRaises(ValueError):
            jobs.decode_rows({'schema': {'fields': [{'name': 'success', 'type': 'BOOLEAN'}]},
                              'rows': [{'f': [{'v': 1}]}]})

    def test_partial_export_resumes_same_job_same_page_and_settles_once(self):
        calls = []
        second_page_reads = 0
        def call(work, query, config, operation, values, **kwargs):
            nonlocal second_page_reads
            calls.append((operation, dict(values)))
            if operation == 'jobs.insert': return {'status': 'SUCCESS_VALIDATED', 'result': {}}
            job_id = values['job_id']
            if operation == 'jobs.get':
                return {'status': 'SUCCESS_VALIDATED', 'result': {'jobReference': {'jobId': job_id, 'projectId': 'ethereum-paper-analysis', 'location': 'US'},
                    'status': {'state': 'DONE'}, 'statistics': {'query': {'totalBytesBilled': '100'}}}}
            if values.get('pageToken'):
                second_page_reads += 1
                if second_page_reads == 1: return {'status': 'RETRIES_EXHAUSTED'}
                page = {'jobComplete': True, 'jobReference': {'jobId': job_id, 'projectId': 'ethereum-paper-analysis', 'location': 'US'}, 'totalRows': '2',
                        'rows': [{'f': [{'v': '3879688898937200589031'}]}]}
            else:
                page = {'jobComplete': True, 'jobReference': {'jobId': job_id, 'projectId': 'ethereum-paper-analysis', 'location': 'US'}, 'totalRows': '2',
                        'schema': {'fields': [{'name': 'amount_raw', 'type': 'INTEGER'}]},
                        'rows': [{'f': [{'v': '3879688898937200589030'}]}], 'pageToken': 'next'}
            return {'status': 'SUCCESS_VALIDATED', 'result': page}
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temporary:
            root = Path(temporary)
            with self.environment(root, call) as config:
                first = jobs.execute_plan(root, 'synthetic', config, 'plan.json')
                self.assertEqual('DONE', first['state']); self.assertEqual(1, len(first['pages']))
                second = jobs.execute_plan(root, 'synthetic', config, 'plan.json')
                self.assertEqual('COMPLETE_EXPORTED', second['state'])
                self.assertEqual(100, second['actual_billed_bytes'])
                self.assertEqual(2, sum(page['row_count'] for page in second['pages']))
                rows = [json.loads(line) for line in (root / second['rows_path']).read_text().splitlines()]
                self.assertEqual(['3879688898937200589030', '3879688898937200589031'], [r['amount_raw'] for r in rows])
                third = jobs.execute_plan(root, 'synthetic', config, 'plan.json')
                self.assertEqual(second, third)
        self.assertEqual(1, sum(operation == 'jobs.insert' for operation, _ in calls))
        self.assertEqual(1, sum(operation == 'jobs.get' for operation, _ in calls))
        page_calls = [values for operation, values in calls if operation == 'jobs.getQueryResults']
        self.assertEqual(page_calls[1], page_calls[2])

    def test_natural_page_end_with_wrong_total_never_becomes_complete(self):
        def call(work, query, config, operation, values, **kwargs):
            if operation == 'jobs.insert': return {'status': 'SUCCESS_VALIDATED', 'result': {}}
            job_id = values['job_id']
            if operation == 'jobs.get':
                return {'status': 'SUCCESS_VALIDATED', 'result': {'jobReference': {'jobId': job_id, 'projectId': 'ethereum-paper-analysis', 'location': 'US'},
                    'status': {'state': 'DONE'}, 'statistics': {'query': {'totalBytesBilled': '100'}}}}
            return {'status': 'SUCCESS_VALIDATED', 'result': {
                'jobComplete': True, 'jobReference': {'jobId': job_id, 'projectId': 'ethereum-paper-analysis', 'location': 'US'}, 'totalRows': '2',
                'schema': {'fields': [{'name': 'amount_raw', 'type': 'INTEGER'}]},
                'rows': [{'f': [{'v': '7'}]}]}}
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temporary:
            root = Path(temporary)
            with self.environment(root, call) as config:
                with self.assertRaisesRegex(ValueError, 'exact totalRows'):
                    jobs.execute_plan(root, 'synthetic', config, 'plan.json')
                state_file = next((root / 'private/stage1d_bigquery_jobs').glob('*/job.json'))
                self.assertNotEqual('COMPLETE_EXPORTED', json.loads(state_file.read_text())['state'])


class ExtendedFaultTests(unittest.TestCase):
    fixture = FaultTests.fixture
    environment = FaultTests.environment
    def test_submission_intent_without_dispatch_record_resumes_same_job(self):
        inserts = []; gets = []
        def call(work, query, config, operation, values, **kwargs):
            if operation == 'jobs.insert':
                inserts.append(kwargs['body']['jobReference']['jobId'])
                if len(inserts) == 1: raise RuntimeError('synthetic crash before claim')
                return {'status': 'SDK_ADC_SETUP_FAILED', 'dispatched': False}
            gets.append(values); return {'status': 'RETRIES_EXHAUSTED'}
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temporary:
            root = Path(temporary)
            with self.environment(root, call) as config:
                with self.assertRaisesRegex(RuntimeError, 'synthetic crash'):
                    jobs.execute_plan(root, 'synthetic', config, 'plan.json')
                result = jobs.execute_plan(root, 'synthetic', config, 'plan.json')
                self.assertIn('undispatched_recovery', result)
        self.assertEqual(2, len(inserts)); self.assertEqual(inserts[0], inserts[1]); self.assertFalse(gets)

    def test_dispatched_submission_intent_only_reads_original_job(self):
        inserts = []; gets = []
        def call(work, query, config, operation, values, **kwargs):
            if operation == 'jobs.insert':
                body = kwargs['body']; inserts.append(body['jobReference']['jobId'])
                identity = {'provider': 'GOOGLE_BIGQUERY_EXISTING_ADC', 'method': 'jobs.insert',
                    'project': config['project'], 'location': 'US', 'selectors': {},
                    'body_sha256': hashlib.sha256(jobs.canonical(body)).hexdigest()}
                store = ReadRetryStore(jobs.retry_path(work)); claim = store.claim(identity)
                store.mark_dispatched(claim['attempt_id'])
                raise RuntimeError('synthetic crash after dispatched intent')
            gets.append(values); return {'status': 'RETRIES_EXHAUSTED'}
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temporary:
            root = Path(temporary)
            with self.environment(root, call) as config:
                with self.assertRaisesRegex(RuntimeError, 'synthetic crash'):
                    jobs.execute_plan(root, 'synthetic', config, 'plan.json')
                jobs.execute_plan(root, 'synthetic', config, 'plan.json')
        self.assertEqual(1, len(inserts)); self.assertEqual(inserts[0], gets[0]['job_id'])

    def test_unknown_month_job_requires_details_and_failed_poll_keeps_version(self):
        polls = []; observation = 0
        created = str(int(time.time() * 1000) - 1000)
        reference = {'projectId': 'ethereum-paper-analysis', 'jobId': 'known', 'location': 'US'}
        def call(work, query, config, operation, values, **kwargs):
            nonlocal observation
            if operation == 'jobs.list':
                return {'status': 'SUCCESS_VALIDATED', 'result': {'jobs': [{
                    'jobReference': reference, 'statistics': {'creationTime': created}}]}}
            polls.append(dict(values)); observation += 1
            if observation == 1: return {'status': 'RETRIES_EXHAUSTED'}
            job = {'jobReference': reference, 'configuration': {'query': {}},
                   'statistics': {'creationTime': created},
                   'status': {'state': 'RUNNING' if observation == 2 else 'DONE'}}
            if observation == 3: job['statistics']['query'] = {'totalBytesBilled': '100'}
            return {'status': 'SUCCESS_VALIDATED', 'result': job}
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temporary:
            root = Path(temporary)
            with patch.object(jobs, 'call', call):
                states = [jobs.month_inventory(root, 'synthetic', {'project': 'ethereum-paper-analysis'}) for _ in range(3)]
        self.assertEqual(['MONTH_METADATA_PARTIAL', 'MONTH_METADATA_PARTIAL', 'MONTH_METADATA_COMPLETE'],
                         [state['status'] for state in states])
        self.assertEqual([0, 0, 1], [p['poll_sequence'] for p in polls])
        self.assertEqual(100, states[-1]['usage']['known_billed_bytes'])

    def test_failed_month_listing_keeps_original_time_range(self):
        selectors = []
        stamps = iter([datetime(2026, 9, 2, tzinfo=timezone.utc), datetime(2026, 9, 3, tzinfo=timezone.utc)])
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None): return next(stamps)
        def call(work, query, config, operation, values, **kwargs):
            selectors.append(dict(values))
            return {'status': 'RETRIES_EXHAUSTED'} if len(selectors) == 1 else {'status': 'SUCCESS_VALIDATED', 'result': {'jobs': []}}
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temporary:
            with patch.object(jobs, 'call', call), patch.object(jobs, 'datetime', Clock):
                for _ in range(2): jobs.month_inventory(Path(temporary), 'synthetic', {'project': 'ethereum-paper-analysis'})
        self.assertEqual(selectors[0], selectors[1])


class TransportBoundaryTests(unittest.TestCase):
    def runtime(self, root, *, disk_failure=False, raw_limit=10**9):
        ledger = Ledger(root)
        class Runtime:
            ledger_factory = staticmethod(lambda path: ledger)
            retry_store = staticmethod(ReadRetryStore)
            @contextmanager
            def session(self, *args, **kwargs): yield {'deadline': time.time() + 1000}
            def raw_limit(self, work): return raw_limit
            def raw_risk(self, work): return 0
            def reserve_raw(self, *args):
                if disk_failure: raise RuntimeError('synthetic disk cap')
            def close_raw(self, *args): pass
        return Runtime()

    def body(self):
        return {'jobReference': {'projectId': 'ethereum-paper-analysis', 'location': 'US', 'jobId': 'fixed'},
                'configuration': {'query': {'query': 'SELECT 1', 'useLegacySql': False, 'maximumBytesBilled': '100'}}}

    def test_resource_failure_is_explicitly_undispatched(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temporary:
            root = Path(temporary); runtime = self.runtime(root, raw_limit=1)
            with patch.object(jobs, 'config_check'), patch.object(jobs, 'GoogleSdk') as factory:
                result = jobs.call(root, 'synthetic', {'project': 'ethereum-paper-analysis'}, 'jobs.insert', {},
                                   body=self.body(), runtime=runtime, sdk_factory=factory)
            self.assertFalse(result['dispatched']); self.assertEqual('BIGQUERY_RESOURCE_DEFERRED', result['status'])
            factory.assert_not_called()

    def test_disk_failure_abandons_before_dispatch_and_closes_sdk(self):
        sdk = types.SimpleNamespace(close=lambda: None)
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temporary:
            root = Path(temporary); runtime = self.runtime(root, disk_failure=True)
            with patch.object(jobs, 'config_check'), patch.object(sdk, 'close') as close:
                result = jobs.call(root, 'synthetic', {'project': 'ethereum-paper-analysis'}, 'jobs.insert', {},
                                   body=self.body(), runtime=runtime, sdk_factory=lambda config: sdk)
            self.assertFalse(result['dispatched']); self.assertEqual('BIGQUERY_DISK_RESERVATION_DEFERRED', result['status'])
            close.assert_called_once()

    def test_auth_retry_after_is_preserved_in_persisted_failure(self):
        TransportError = type('TransportError', (Exception,), {'__module__': 'google.auth.exceptions'})
        class SDK:
            def __init__(self, config):
                self.auth_retry_after = 17
                self.transport_evidence = [{'status': None, 'body': b'', 'complete': False, 'retry_after': None}]
                self.network_evidence = [{'kind': 'AUTH'}]
                self.client = types.SimpleNamespace(_connection=self)
            def api_request(self, **kwargs): raise TransportError('synthetic redacted')
            def close(self): pass
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temporary:
            root = Path(temporary); runtime = self.runtime(root)
            with patch.object(jobs, 'config_check'):
                result = jobs.call(root, 'synthetic', {'project': 'ethereum-paper-analysis'}, 'jobs.insert', {},
                                   body=self.body(), runtime=runtime, sdk_factory=SDK)
            self.assertEqual(17, result['failure']['retry_after'])
            with closing(sqlite3.connect(jobs.retry_path(root))) as db:
                eligible = db.execute('SELECT next_eligible_at FROM read_requests').fetchone()[0]
            self.assertGreater(eligible, time.time() + 15)

    def test_sql_keywords_inside_literal_and_comment_remain_valid(self):
        body = self.body(); body['configuration']['query']['query'] = "/* DELETE FROM x */ SELECT 'UPDATE' AS word"
        self.assertEqual('POST', jobs.request_spec('ethereum-paper-analysis', 'jobs.insert', {}, body)[0])


if __name__ == '__main__': unittest.main()
