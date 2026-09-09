"""Synthetic exact native SQL and capability-aware real cache import tests."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from collector import NATIVE, Scope
from context_access_r3 import sha
from page_attempts import atomic_json
from stage1d_acquisition import CachedIntervals
from stage1d_candidate_batch import build_sql
from stage1d_export import FULL_COLUMNS
from stage1d_native_candidate import CAPABILITY, ENDING, TOKEN_MARKER, native_sql, source_plan, prepare, import_result, run

A = '0x' + '1' * 40
B = '0x' + '2' * 40
Z = '0x' + '3' * 40
T = 1700000000


def rectangle(address=A, lo=10, hi=12, start=T, end=T + 20):
    return dict(address=address, asset=NATIVE, start_block=lo, end_block=hi, start_time=start, end_time=end)


def raw_event(n=1, address=A, block=11, time=T + 1, amount='7', success=True):
    row = dict.fromkeys(FULL_COLUMNS)
    row.update(event_kind='top', tx_hash='0x' + format(n, '064x'), sender=address, recipient=Z,
               amount_raw=amount, block_number=block, tx_index=0, block_time=time,
               success=success, gas_used='21000', gas_price='2')
    return row


class NativeCandidateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.work = Path(self.tmp.name).resolve()
        self.query = {'query_id': 'synthetic-query', 'name': 'synthetic', 'start_block': 1, 'end_block': 300,
            'start_time_utc': T, 'end_time_utc': T + 108 * 86400, 'max_acquisition_depth': 13,
            'window_mode': 'REFERENCE_FULL', 'local_window_seconds': None, 'seed_asset': NATIVE,
            'seed_event': {'asset': NATIVE}}
        scope = Scope.from_policy(self.query); self.query.update(scope_id=scope.scope_id, scope_hash=scope.scope_hash)
        atomic_json(self.work / 'private/BATCH_QUERY_FREEZE.json', {'queries': [self.query]})
        self.selected = [rectangle(), rectangle(A, 20, 22, T + 86400, T + 86420)]
        sql, _ = build_sql(self.selected)
        digest = hashlib.sha256(sql.encode()).hexdigest()
        self.folder = self.work / 'private/dune_r2_jobs' / digest
        self.folder.mkdir(parents=True); (self.folder / 'query.sql').write_text(sql, encoding='utf8', newline='\n')
        self.freeze = self.work / 'private/source_freeze.json'
        atomic_json(self.freeze, {'schema_version': 'stage1d-sql-freeze-v1', 'authorization_id': 'STAGE1D_BATCH01_REFERENCE_FULL_V1',
            'kind': 'candidate', 'query_ids': [self.query['query_id']], 'scope_id': scope.scope_id, 'scope_hash': scope.scope_hash,
            'sql_sha256': digest, 'addresses': [A], 'dependencies': [], 'export_plan': {'all_pages_required': True},
            'intervals': [dict(r, query_id=self.query['query_id'], kind='candidate') for r in self.selected]})
        self.metadata = {'column_names': FULL_COLUMNS, 'column_types': ['synthetic'] * 16,
                         'total_row_count': 135303, 'total_result_set_bytes': 31268161}
        body = {'execution_id': 'A' * 26, 'state': 'QUERY_STATE_COMPLETED', 'execution_cost_credits': '6.044852942', 'result_metadata': self.metadata}
        raw = self.work / 'raw/dune/source_status.json'; atomic_json(raw, body)
        receipt = {'request_id': 'source_status', 'operation': 'status', 'http_status': 200, 'error_class': None,
                   'raw_path': raw.relative_to(self.work).as_posix(), 'raw_bytes': raw.stat().st_size, 'sha256': sha(raw)}
        atomic_json(self.work / 'logs/source_status.json', receipt)
        self.state = {'state': 'QUERY_STATE_COMPLETED', 'execution_id': 'A' * 26, 'logical_job_id': 'synthetic-old',
            'sql_sha256': digest, 'scope_freeze_path': self.freeze.relative_to(self.work).as_posix(),
            'scope_freeze_sha256': sha(self.freeze), 'status_response': body, 'status_receipt': receipt,
            'export_requests': 0, 'export_offsets': []}
        atomic_json(self.folder / 'job.json', self.state)

    def tearDown(self):
        self.tmp.cleanup()

    def imported(self, rows=None):
        prepared = prepare(self.work, self.folder)
        new = self.work / 'private/dune_r2_jobs' / prepared['proof']['native_sql_sha256']
        atomic_json(new / 'job.json', {'sql_sha256': prepared['proof']['native_sql_sha256'], 'execution_id': 'B' * 26})
        result = {'status': 'COMPLETED_EXPORTED', 'job_folder': str(new)}
        if rows is None: rows = [raw_event(), raw_event(2, block=21, time=T + 86401)]
        with patch('stage1d_native_candidate.result_rows', return_value=rows) as actual_reader:
            output = import_result(self.work, prepared, result)
        actual_reader.assert_called_once_with(self.work, new)
        return prepared, output

    def test_exact_native_prefix_cross_day_and_ancestor_sql_unchanged(self):
        native, full, selected = native_sql(self.selected)
        self.assertEqual(native, full.split(TOKEN_MARKER)[0] + '\n' + ENDING)
        self.assertEqual(selected, self.selected)
        self.assertEqual(native.count('SELECT 1 FROM request_windows w'), 2)
        for text in ('NOT EXISTS (', 'a.success=false', 't.tx_success', 't.gas_used AS VARCHAR', 't.refund_address', 't.call_type', 'block_date BETWEEN DATE'):
            self.assertIn(text, native)
        self.assertNotIn('erc20_ethereum', native)
        self.assertNotIn(' LIMIT ', native)

    def test_all_asset_135303_not_relabelled_as_native_candidates(self):
        before = (self.folder / 'job.json').read_bytes()
        _, _, proof, _ = source_plan(self.work, self.folder)
        self.assertEqual(proof['source_all_asset_metadata']['total_row_count'], 135303)
        self.assertIsNone(proof['native_total_rows_before_new_execution'])
        self.assertEqual(proof['source_execution_cost_credits'], '6.044852942')
        self.assertTrue(proof['original_financial_facts_retained'])
        self.assertEqual(before, (self.folder / 'job.json').read_bytes())
        self.assertFalse(list(self.work.glob('**/*.sqlite')))

    def test_scope_and_conversions_fail_closed(self):
        for key, value in [('seed_asset', 'erc20:eip155:1:' + A), ('certified_conversions', [{'old_weth': True}]), ('scope_hash', '0' * 64)]:
            with self.subTest(key=key):
                q = dict(self.query); q[key] = value
                atomic_json(self.work / 'private/BATCH_QUERY_FREEZE.json', {'queries': [q]})
                with self.assertRaises(ValueError): source_plan(self.work, self.folder)

    def test_changed_original_sql_and_raw_status_are_rejected(self):
        sql = self.folder / 'query.sql'; saved = sql.read_bytes(); sql.write_bytes(saved.replace(b'a.success=false', b'a.success=true'))
        with self.assertRaises(ValueError): source_plan(self.work, self.folder)
        sql.write_bytes(saved)
        raw = self.work / self.state['status_receipt']['raw_path']; raw.write_text('{}')
        with self.assertRaises(ValueError): source_plan(self.work, self.folder)

    def test_prepare_idempotent_dependencies_bind_scope_raw_and_source_fee(self):
        first = prepare(self.work, self.folder); second = prepare(self.work, self.folder)
        self.assertEqual(first, second)
        f = json.loads(Path(first['freeze_path']).read_text())
        self.assertEqual(f['scope_hash'], self.query['scope_hash'])
        self.assertEqual(f['intervals'], json.loads(self.freeze.read_text())['intervals'])
        for dep in f['dependencies']: self.assertEqual(sha(self.work / dep['path']), dep['sha256'])
        self.assertNotEqual(first['proof']['native_sql_sha256'], self.state['sql_sha256'])

    def test_complete_import_real_cache_exact_holes_and_scope_metadata(self):
        prepared, output = self.imported()
        self.assertEqual(output['coverage_capability'], CAPABILITY)
        self.assertFalse(output['all_asset_export_complete'])
        self.assertEqual(len(list((self.work / 'derived/stage1d/intervals').glob('*.events.json'))), 1)
        self.assertEqual(len(list((self.work / 'derived/stage1d/intervals').glob('*.coverage.json'))), 2)
        cache = CachedIntervals(self.work); cache.bind_scope(Scope.from_policy(self.query))
        one = cache.fetch_interval(A, NATIVE, 10, 12, start_time=T, end_time=T + 20, global_end_time=T + 200000)
        self.assertTrue(one.complete); self.assertEqual(len(one.events), 1)
        self.assertEqual(one.coverage[0]['coverage_capability'], CAPABILITY)
        self.assertFalse(one.coverage[0]['verified_content_intervals'][0]['all_asset_export_complete'])
        both = cache.fetch_interval(A, NATIVE, 10, 22, start_time=T, end_time=T + 86420, global_end_time=T + 200000)
        self.assertFalse(both.complete); self.assertEqual(len(both.events), 2)
        token = cache.fetch_interval(A, 'erc20:eip155:1:' + A, 10, 12, start_time=T, end_time=T + 20, global_end_time=T + 200000)
        self.assertFalse(token.complete); self.assertFalse(token.events)

    def test_zero_failed_native_fees_and_duplicate_physical_rows_retained(self):
        zero = raw_event(amount='0'); failed = raw_event(2, success=False)
        _, out = self.imported([zero, failed, copy.deepcopy(zero)])
        self.assertEqual(out['raw_rows'], 3); self.assertEqual(out['unique_events'], 2)
        cache = CachedIntervals(self.work)
        r = cache.fetch_interval(A, NATIVE, 10, 12, start_time=T, end_time=T + 20, global_end_time=T + 200000)
        self.assertTrue(r.complete)
        self.assertEqual({(e.amount_raw, e.success, e.gas_raw) for e in r.events}, {(0, True, 42000), (7, False, 42000)})

    def test_day95_full_window_and_crossquery_share_native_content(self):
        self.selected = [rectangle(A, 100, 200, T + 95 * 86400, T + 107 * 86400)]
        sql, _ = build_sql(self.selected); sid = hashlib.sha256(sql.encode()).hexdigest()
        frozen = json.loads(self.freeze.read_text()); frozen.update(sql_sha256=sid,
            intervals=[dict(r, query_id=self.query['query_id'], kind='candidate') for r in self.selected])
        atomic_json(self.freeze, frozen)
        self.folder = self.work / 'private/dune_r2_jobs' / sid
        self.folder.mkdir(); (self.folder / 'query.sql').write_text(sql, encoding='utf8', newline='\n')
        self.state.update(sql_sha256=sid, scope_freeze_sha256=sha(self.freeze))
        atomic_json(self.folder / 'job.json', self.state)
        self.imported([raw_event(block=150, time=T + 100 * 86400)])
        cache = CachedIntervals(self.work)
        other = dict(self.query, query_id='synthetic-other-query', scope_id=None)
        other.pop('scope_id'); other.pop('scope_hash')
        scope = Scope.from_policy(other); cache.bind_scope(scope)
        r = cache.fetch_interval(A, NATIVE, 100, 200, start_time=T + 95 * 86400,
            end_time=T + 107 * 86400, global_end_time=T + 108 * 86400)
        self.assertTrue(r.complete); self.assertEqual(len(r.events), 1)
        self.assertEqual(r.coverage[0]['query_id'], 'synthetic-other-query')
        self.assertEqual(r.coverage[0]['window_mode'], 'REFERENCE_FULL')
        self.assertEqual(r.coverage[0]['end_time'], T + 107 * 86400)
        self.assertEqual(r.coverage[0]['coverage_capability'], CAPABILITY)

    def test_incomplete_export_imports_no_claim(self):
        p = prepare(self.work, self.folder)
        with patch('stage1d_native_candidate.result_rows') as reader:
            out = import_result(self.work, p, {'status': 'PARTIAL_EXPORT'})
        reader.assert_not_called(); self.assertEqual(out['status'], 'PARTIAL_EXPORT')
        self.assertFalse((self.work / 'derived').exists())

    def test_token_or_nonconstant_native_result_rejected(self):
        for key, value in [('event_kind', 'erc20'), ('contract_address', A), ('log_index', 1)]:
            with self.subTest(key=key):
                row = raw_event(); row[key] = value
                with self.assertRaises(ValueError): self.imported([row])
        self.assertFalse((self.work / 'derived').exists())

    def test_physical_conflict_does_not_certify_coverage(self):
        first = raw_event(); other = dict(first, amount_raw='8')
        _, out = self.imported([first, other])
        self.assertFalse(out['native_scope_complete']); self.assertTrue(out['normalization_gaps'])
        r = CachedIntervals(self.work).fetch_interval(A, NATIVE, 10, 12, start_time=T, end_time=T + 20, global_end_time=T + 200000)
        self.assertFalse(r.complete); self.assertTrue(r.fact_conflicts)

    def test_cache_capability_or_decision_tamper_rejected(self):
        prepared, _ = self.imported()
        record_path = next((self.work / 'derived/stage1d/intervals').glob('*.coverage.json'))
        record = json.loads(record_path.read_text()); record['all_asset_export_complete'] = True; atomic_json(record_path, record)
        with self.assertRaises(ValueError): CachedIntervals(self.work)
        record['all_asset_export_complete'] = False; atomic_json(record_path, record)
        Path(prepared['decision_path']).write_text('{}')
        with self.assertRaises(ValueError): CachedIntervals(self.work)

    def test_run_uses_existing_execute_sql_and_preserves_partial(self):
        before = (self.folder / 'job.json').read_bytes()
        with patch('stage1d_native_candidate.execute_sql', return_value={'status': 'DEFERRED_CLOCK'}) as executor:
            result = run(self.work, self.folder)
        self.assertEqual(result['status'], 'DEFERRED_CLOCK')
        self.assertEqual(executor.call_args.args[0], self.work)
        self.assertEqual(executor.call_args.args[2], 'synthetic')
        self.assertEqual(before, (self.folder / 'job.json').read_bytes())
        self.assertFalse((self.work / 'derived').exists())


if __name__ == '__main__': unittest.main(verbosity=2)
