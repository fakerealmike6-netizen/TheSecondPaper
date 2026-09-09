"""Synthetic direct native scopes, same-job restart and bounded batch tests."""
import copy
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

from context_access_r3 import sha
from page_attempts import atomic_json
from stage1d_native_candidate import prepare as recovery_prepare, native_sql, CAPABILITY
from stage1d_native_candidate_batch import prepare, acquire, run
import test_stage1d_native_candidate as recovery_tests
from test_stage1d_native_candidate import rectangle, raw_event, A, NATIVE, T


class NativeBatchTests(unittest.TestCase):
    def setUp(self):
        self.fixture = recovery_tests.NativeCandidateTests(); self.fixture.setUp()
        self.work = self.fixture.work; self.query = self.fixture.query; self.pending = self.fixture.selected
        self.folder = self.work / 'derived/stage1d/queries' / self.query['name']; self.folder.mkdir(parents=True)
        self.cp = self.folder / 'collection.json'
        self.collection = {'query_id': self.query['query_id'], 'status': 'INCOMPLETE_PROVIDER_OR_DATA_GAP',
            'candidate_events': [dict(asset=NATIVE)], 'fact_conflicts': [],
            'metrics': {'scope_id': self.query['scope_id'], 'scope_hash': self.query['scope_hash']}}
        atomic_json(self.cp, self.collection)

    def tearDown(self): self.fixture.tearDown()

    def test_direct_prepare_bound_native16_without_old_execution(self):
        before = (self.fixture.folder / 'job.json').read_bytes()
        result = prepare(self.work, self.query, self.pending, self.cp)
        p = result['proof']; self.assertIsNone(p['source_execution_id'])
        self.assertIsNone(p['native_total_rows_before_new_execution'])
        self.assertTrue(p['source_sql_generated_for_equivalence_only_not_submitted'])
        self.assertEqual(p['coverage_capability'], CAPABILITY)
        self.assertEqual(p['native_sql_sha256'], hashlib.sha256(native_sql(self.pending)[0].encode()).hexdigest())
        f = json.loads(Path(result['freeze_path']).read_text())
        self.assertEqual(f['scope_hash'], self.query['scope_hash'])
        for d in f['dependencies']: self.assertEqual(sha(self.work / d['path']), d['sha256'])
        self.assertEqual(before, (self.fixture.folder / 'job.json').read_bytes())
        self.assertFalse(list(self.work.glob('**/*.sqlite')))

    def test_reuses_preceding_source_job_recovery_and_same_native_identity(self):
        recovered = recovery_prepare(self.work, self.fixture.folder)
        before = Path(recovered['decision_path']).read_bytes()
        direct = prepare(self.work, self.query, self.pending, self.cp)
        self.assertEqual(direct['freeze_path'], recovered['freeze_path'])
        self.assertEqual(direct['proof'], recovered['proof'])
        self.assertEqual(before, Path(recovered['decision_path']).read_bytes())

    def test_unregistered_changed_scope_and_token_conversion_rejected(self):
        with self.assertRaises(ValueError): prepare(self.work, dict(self.query, max_acquisition_depth=14), self.pending, self.cp)
        for key, value in [('seed_asset', 'erc20:eip155:1:' + A), ('certified_conversions', [{'id': 'synthetic'}])]:
            q = dict(self.query); q[key] = value
            atomic_json(self.work / 'private/BATCH_QUERY_FREEZE.json', {'queries': [q]})
            with self.assertRaises(ValueError): prepare(self.work, q, self.pending, self.cp)

    def test_actual_frontier_scope_and_exact_interval_escape_rejected(self):
        for mutation in (dict(rectangle(), asset='erc20:eip155:1:' + A), rectangle(lo=0), rectangle(end=T + 109 * 86400)):
            with self.assertRaises(ValueError): prepare(self.work, self.query, [mutation], self.cp)
        c = copy.deepcopy(self.collection); c['metrics']['scope_hash'] = 'wrong'; atomic_json(self.cp, c)
        with self.assertRaises(ValueError): prepare(self.work, self.query, self.pending, self.cp)

    def test_direct_result_import_and_existing_cache_share_exact_native_claims(self):
        from stage1d_acquisition import CachedIntervals
        from stage1d_native_candidate import import_result
        prepared = prepare(self.work, self.query, self.pending, self.cp)
        job = self.work / 'private/dune_r2_jobs' / prepared['proof']['native_sql_sha256']
        atomic_json(job / 'job.json', {'sql_sha256': prepared['proof']['native_sql_sha256'], 'execution_id': 'D' * 26})
        with patch('stage1d_native_candidate.result_rows', return_value=[raw_event(), raw_event(2, block=21, time=T + 86401)]):
            result = import_result(self.work, prepared, {'status': 'COMPLETED_EXPORTED', 'job_folder': str(job)})
        self.assertTrue(result['native_scope_complete'])
        r = CachedIntervals(self.work).fetch_interval(A, NATIVE, 10, 12, start_time=T, end_time=T + 20, global_end_time=T + 200000)
        self.assertTrue(r.complete); self.assertEqual(r.coverage[0]['coverage_capability'], CAPABILITY)
        self.assertFalse(r.coverage[0]['all_asset_export_complete'])

    def test_unknown_submitted_identity_never_reposts_existing_job(self):
        prepared = prepare(self.work, self.query, self.pending, self.cp)
        job = self.work / 'private/dune_r2_jobs' / prepared['proof']['native_sql_sha256']
        unknown = {'state': 'SUBMISSION_UNKNOWN', 'execution_id': None, 'sql_sha256': prepared['proof']['native_sql_sha256']}
        atomic_json(job / 'job.json', unknown)
        before = (job / 'job.json').read_bytes(); live = MagicMock()
        class RuntimeFake:
            def verify_sql_freeze(self, freeze, work, sql_path=None): return json.loads(Path(freeze).read_text())
            @contextmanager
            def session(self, *args): yield {'deadline': 100000000000, 'record': {}}
        with patch('stage1d_runtime.Runtime', RuntimeFake), patch('stage1d_export_reconcile.Stage1DPageDune', return_value=live):
            result = acquire(self.work, self.query, self.pending, self.cp)
        self.assertEqual(result['status'], 'ACQUISITION_PARTIAL')
        self.assertIn('Unknown SQL submission', result['reason'])
        live.submit.assert_not_called(); self.assertEqual(before, (job / 'job.json').read_bytes())

    def test_large_native_index_rows_keep_resource_stop_not_candidate_claim(self):
        prepared = prepare(self.work, self.query, self.pending, self.cp)
        job = self.work / 'private/dune_r2_jobs' / prepared['proof']['native_sql_sha256']
        atomic_json(job / 'job.json', {'state': 'QUERY_STATE_COMPLETED', 'execution_id': 'E' * 26,
            'status_response': {'result_metadata': {'total_row_count': 30000, 'total_result_set_bytes': 7000000}}})
        with patch('stage1d_native_candidate_batch.execute_sql', side_effect=ValueError('Declared query/context row resource cap blocks this result')):
            result = acquire(self.work, self.query, self.pending, self.cp)
        self.assertEqual(result['status'], 'ACQUISITION_PARTIAL')
        self.assertEqual(result['native_index_metadata']['total_row_count'], 30000)
        self.assertIsNone(result['propagated_candidate_cap_exceeded'])
        self.assertTrue(result['raw_index_rows_are_not_propagated_candidate_count'])
        self.assertFalse((self.work / 'derived/stage1d/intervals').exists())

    def drive(self, collections, results, max_batches=3):
        sequence = iter(collections)
        def replay(*args):
            c, p = next(sequence); atomic_json(self.cp, c); atomic_json(self.folder / 'pending_intervals.json', p)
            return c, p
        with patch('stage1d_native_candidate_batch.replay', side_effect=replay), \
             patch('stage1d_native_candidate_batch.acquire_labels', return_value={'new_addresses': 0}), \
             patch('stage1d_native_candidate_batch.Runtime.raw_risk', return_value=0), \
             patch('stage1d_native_candidate_batch.acquire', side_effect=results) as acquisition:
            output = run(self.work, self.query, max_batches)
        return output, acquisition

    def test_multibatch_progress_stops_on_resource_failure_with_frontier(self):
        p1 = self.pending[:1]; p2 = self.pending[1:]
        success = {'status': 'COMPLETED_EXPORTED', 'native_scope_complete': True,
                   'sql_sha256': hashlib.sha256(native_sql(p1)[0].encode()).hexdigest()}
        failure = {'status': 'ACQUISITION_PARTIAL', 'reason': 'budget shortage'}
        out, calls = self.drive([(self.collection, p1), (self.collection, p2), (self.collection, p2)], [success, failure])
        self.assertEqual(calls.call_count, 2); self.assertEqual(out['completed_batches_this_invocation'], 1)
        self.assertEqual(out['pending_intervals'], 1); self.assertEqual(out['stop_reason'], 'ACQUISITION_PARTIAL')
        self.assertEqual(out['candidate_events'], 1); self.assertEqual(len(out['attempts']), 2)
        self.assertTrue(list((self.work / 'private/stage1d_native_batch_history').glob('*.json')))

    def test_same_successful_rectangle_not_repeated_within_invocation(self):
        sid = hashlib.sha256(native_sql(self.pending)[0].encode()).hexdigest()
        out, calls = self.drive([(self.collection, self.pending)] * 3,
            [{'status': 'COMPLETED_EXPORTED', 'native_scope_complete': True, 'sql_sha256': sid}])
        self.assertEqual(calls.call_count, 1); self.assertEqual(out['stop_reason'], 'SAME_COMPLETE_SQL_NO_FRONTIER_PROGRESS')

    def test_collector_resource_status_stops_before_new_provider_or_label_calls(self):
        c = dict(self.collection, status='INCOMPLETE_RESOURCE_LIMIT')
        with patch('stage1d_native_candidate_batch.replay', return_value=(c, self.pending)), \
             patch('stage1d_native_candidate_batch.acquire_labels') as labels, \
             patch('stage1d_native_candidate_batch.acquire') as acquisition:
            out = run(self.work, self.query, 3)
        labels.assert_not_called(); acquisition.assert_not_called()
        self.assertEqual(out['stop_reason'], 'INCOMPLETE_RESOURCE_LIMIT')
        self.assertEqual(out['pending_intervals'], 2)

    def test_empty_frontier_and_invalid_loop_limits(self):
        out, calls = self.drive([(self.collection, [])] * 2, [])
        calls.assert_not_called(); self.assertEqual(out['stop_reason'], 'NO_UNCOVERED_NATIVE_RECTANGLES')
        for batches, maximum in [(0, 32), (101, 32), (True, 32), (1, 33)]:
            with self.assertRaises(ValueError): run(self.work, self.query, batches, maximum)


if __name__ == '__main__': unittest.main(verbosity=2)
