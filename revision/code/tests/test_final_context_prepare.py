"""Synthetic offline tests; temporary artifacts stay in this assigned staging dir."""
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import importlib.util
import json
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parent / 'src')]
import stage1d_final_context_prepare as f
import stage1d_bq_context_prepare as bq
import stage1d_bigquery_jobs as jobs
import stage1d_bigquery_probe as probe
from stage1d_recovery_policy import AUTH, BQ_TOTAL
import stage1d_bq_root_binding as roots

A = '0x' + '1' * 40
B = '0x' + '2' * 40
C = '0x' + '3' * 40
TX = '0x' + 'a' * 64


class ContextTests(unittest.TestCase):
    def coverage(self, kind, lo=10, hi=20, **extra):
        return dict(address=A, data_type=kind, start_block=lo, end_block=hi,
                    status='COMPLETE', pagination_complete=True, evidence_ids=['SYNTHETIC'], **extra)

    def test_protocol_gap_does_not_repeat_completed_three_kind_ledger(self):
        row = dict(address=A, ledger_start_block=10, ledger_end_block=20)
        coverage = [self.coverage(k) for k in f.KINDS]
        self.assertEqual(f.gaps_for_kinds(row, coverage), [])
        self.assertEqual(f.gaps_for_kinds(row, coverage, [bq.PROTOCOL]), [[10, 20]])

    def test_incomplete_unproven_coverage_and_disjoint_islands(self):
        row = dict(address=A, ledger_start_block=10, ledger_end_block=20)
        coverage = [self.coverage(k, 12, 14) for k in f.KINDS]
        coverage += [self.coverage(k, 17, 18, provider_frozen_scope=True,
                                  date_domain_verified=True, block_domain_verified=False) for k in f.KINDS]
        self.assertEqual(f.gaps_for_kinds(row, coverage), [[10, 11], [15, 20]])
        coverage[0]['evidence_ids'] = []
        self.assertEqual(f.gaps_for_kinds(row, coverage), [[10, 20]])

    def test_shared_union_no_hole_filling_or_new_address(self):
        ranges = [dict(address=A, start_block=10, end_block=11), dict(address=A, start_block=12, end_block=13),
                  dict(address=A, start_block=16, end_block=18), dict(address=B, start_block=11, end_block=12)]
        out = f.merge_address_ranges(ranges)
        self.assertEqual([(r['address'], r['start_block'], r['end_block']) for r in out],
                         [(A, 10, 13), (A, 16, 18), (B, 11, 12)])

    def test_at_most_64_distinct_addresses_without_losing_disjoint_ranges(self):
        ranges = [dict(address='0x' + str(i).zfill(40), start_block=10, end_block=10) for i in range(130)]
        ranges += [dict(ranges[0], start_block=12, end_block=12)]
        batches = f.address_batches(ranges)
        self.assertEqual([len({r['address'] for r in batch}) for batch in batches], [64, 64, 2])
        self.assertEqual(sum(map(len, batches)), 131)

    def test_whole_transaction_tree_projection_preserves_zero_failed_and_siblings(self):
        rows = [dict(record_type='transaction', tx_hash=TX, block_number=15, from_address=A,
                     to_address=B, value_raw='0', success=False),
                dict(record_type='trace', tx_hash=TX, block_number=15, from_address=C,
                     to_address=B, trace_address='[0,0]', value_raw='0', success=False),
                dict(record_type='transaction', tx_hash='0x' + 'b' * 64, block_number=30,
                     from_address=A, to_address=B, value_raw='10', success=True)]
        out = f.project_rows(rows, [dict(address=A, start_block=10, end_block=20)])
        self.assertEqual(out, rows[:2])
        self.assertEqual(rows[0]['success'], False)

    def test_bound_root_replaces_only_same_fact_null_copy_and_keeps_provenance(self):
        old = dict(record_type='trace', tx_hash=TX, trace_address=None, from_address=A,
                   to_address=B, value_raw='5', evidence_ids=['OLD'])
        new = dict(old, trace_address='[]', evidence_ids=['NEW'], root_position_binding={'status': 'ROOT_EQUIVALENT'})
        out = f.merge_bound_rows([old], [new])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]['trace_address'], '[]')
        self.assertEqual(out[0]['evidence_ids'], ['NEW', 'OLD'])
        self.assertIsNone(old['trace_address'])
        self.assertEqual(new['evidence_ids'], ['NEW'])

    def test_bound_root_cannot_replace_disagreeing_old_amount(self):
        old = dict(record_type='trace', tx_hash=TX, trace_address=None, value_raw='5')
        new = dict(old, trace_address='[]', value_raw='6', root_position_binding={'status': 'ROOT_EQUIVALENT'})
        with self.assertRaises(f.EvidenceConflict):
            f.merge_bound_rows([old], [new])

    def test_two_real_job_binding_proofs_are_provenance_not_amount_conflict(self):
        row = dict(record_type='trace', tx_hash=TX, trace_address='[]', value_raw='5')
        first = dict(row, root_position_binding={'status': 'ROOT_EQUIVALENT', 'original_row_sha256': 'a' * 64})
        second = dict(row, root_position_binding={'status': 'ROOT_EQUIVALENT', 'original_row_sha256': 'b' * 64})
        out = f.merge_bound_rows([first], [second])
        self.assertEqual(len(out), 1)
        self.assertEqual(len(out[0]['root_position_binding_proofs']), 2)
        self.assertEqual(out[0]['root_position_binding']['original_row_sha256'], 'a' * 64)

    def test_consolidating_old_null_and_proven_root_does_not_require_new_job(self):
        old = dict(record_type='trace', tx_hash=TX, trace_address=None, value_raw='5', evidence_ids=['OLD_NULL'])
        root = dict(old, trace_address='[]', evidence_ids=['BOUND'], root_position_binding={'status': 'ROOT_EQUIVALENT'})
        out = f.merge_bound_rows([old, root], [])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]['evidence_ids'], ['BOUND', 'OLD_NULL'])

    def test_current_success_rpc_is_used_without_network(self):
        request = {'method': 'eth_getBalance', 'params': [A, '0xa']}
        balances = {}
        with patch.object(f, '_cached_rpc', return_value=[(request, '0x123', ['CURRENT_SHA'])]):
            self.assertEqual(f.cached_points(HERE, [request], {}, balances, {}), [])
        self.assertEqual(balances[A + ':10']['response']['result'], '0x123')

    def test_missing_point_stays_explicit(self):
        request = {'method': 'eth_getBalance', 'params': [A, '0xa']}
        with patch.object(f, '_cached_rpc', return_value=[]):
            self.assertEqual(f.cached_points(HERE, [request], {}, {}, {}), [request])


class ClockTests(unittest.TestCase):
    def test_call_shared_clock_uses_existing_session_without_changing_sql_owner(self):
        seen = []
        @contextmanager
        def session(work, owner, label, **kwargs):
            seen.append(owner)
            raise RuntimeError('STOP_BEFORE_ANY_CLAIM')
            yield
        runtime = SimpleNamespace(retry_store=lambda _: None, ledger_factory=lambda _: None, session=session)
        with patch.object(jobs, 'config_check'), self.assertRaisesRegex(RuntimeError, 'STOP_BEFORE'):
            jobs.call(HERE, 'txphish_src002', {'project': 'example-project'}, 'jobs.get', {'job_id': 'saved'},
                      runtime=runtime, sdk_factory=object, clock_query='SHARED')
        self.assertEqual(seen, ['SHARED'])

    def test_wrong_single_query_clock_is_rejected_before_read_or_write(self):
        with self.assertRaisesRegex(ValueError, 'Clock owner'):
            jobs.execute_plan(HERE, 'txphish_src002', {}, 'missing.json', clock_query='txphish_src001')

    def test_probe_validates_sql_owner_but_times_shared(self):
        seen = []
        @contextmanager
        def session(work, owner, label, **kwargs):
            seen.append(owner)
            raise RuntimeError('STOP_BEFORE_ANY_CLAIM')
            yield
        query = {'name': 'txphish_src002', 'query_id': 'q2', 'scope_hash': 's2'}
        def read(path):
            if str(path).endswith('BATCH_QUERY_FREEZE.json'):
                return {'queries': [query]}
            if str(path).endswith('spec.json'):
                return {'sql_sha256': 'SYNTHETIC'}
            return {'project': 'example-project', 'location': 'US'}
        runtime = SimpleNamespace(retry_store=lambda *a, **k: None, ledger_factory=lambda _: None, session=session)
        with patch.object(probe, 'config_check'), patch.object(probe, 'read', side_effect=read), \
                patch.object(probe, 'active_batch', return_value={'queries':[query]}), \
                patch.object(probe, 'sha', return_value='SYNTHETIC'), patch.object(probe, 'dry_spec', return_value='SELECT 1') as validate, \
                self.assertRaisesRegex(RuntimeError, 'STOP_BEFORE'):
            probe.probe(HERE, 'txphish_src002', 'config.json', 'dry-run', spec_path='spec.json',
                        runtime=runtime, sdk_factory=object, clock_query='SHARED')
        self.assertEqual(seen, ['SHARED'])
        self.assertEqual(validate.call_args.args[3], query)


class BudgetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=HERE, prefix='synthetic_')
        self.addCleanup(self.tmp.cleanup)
        self.work = Path(self.tmp.name)
        self.config = {'project': 'example-project'}
        self.query = {'name': 'txphish_src002', 'query_id': 'q2', 'scope_hash': 's2'}
        bq.save(self.work/'private/BATCH_QUERY_FREEZE.json',{'queries':[self.query]})
        self.snapshot = {'bigquery_bytes': {'authorization_id': AUTH, 'cap': str(BQ_TOTAL),
                                           'remaining': str(30 * 1073741824)}}

    def plan(self, key, estimate=10 * 1073741824):
        spec = {'sql_sha256': key * 64, 'query_id': 'q2', 'scope_hash': 's2'}
        spec_path = self.work / (key + '_spec.json')
        bq.save(spec_path, spec)
        dry_path = self.work / (key + '_dry.json')
        bq.save(dry_path, {'status': 'SUCCESS_VALIDATED', 'identity': {'project': self.config['project'],
                    'sql_sha256': spec['sql_sha256'], 'spec_sha256': bq.sha(spec_path), 'scope_hash': 's2'},
                    'result': {'kind': 'DRY_RUN', 'actual_query_executed': False, 'estimated_processed_bytes': estimate}})
        path = self.work / (key + '_plan.json')
        bq.save(path, {'dry_spec_path': spec_path.name, 'dry_spec_sha256': bq.sha(spec_path),
                       'dry_receipt_path': dry_path.name, 'dry_receipt_sha256': bq.sha(dry_path)})
        return bq.dep(self.work, path), spec

    def audit(self, plans, amounts=None):
        specs = []
        for dependency in plans:
            plan = bq.read(bq.checked(self.work, dependency))
            specs.append({'path': plan['dry_spec_path'], 'sha256': plan['dry_spec_sha256'],
                          'template': 'transaction_and_trace'})
        bq.save(self.work / 'family.json', {'plans': specs})
        bq.save(self.work / 'bundle.json', {'freeze_sha256': bq.sha(self.work/'private/BATCH_QUERY_FREEZE.json'),
                                          'families': [bq.dep(self.work, self.work / 'family.json')]})
        with patch.object(f, 'checked_freeze', return_value={'txphish_src002': self.query}), \
                patch.object(f, 'budget_snapshot_ro', return_value=(self.snapshot, amounts or {})), \
                patch.object(probe, 'dry_spec', return_value='SYNTHETIC_ONLY'):
            return f.audit_actual_dryruns(self.work, 'bundle.json', plans, self.config)

    def test_total_bounds_deduplicate_identical_saved_sql_jobs(self):
        first, _ = self.plan('a')
        second, _ = self.plan('b')
        result = self.audit([first, first, second])
        self.assertEqual(result['new_upper_bound_total_bytes'], 22 * 1073741824)
        self.assertEqual(result['status'], 'ACTUAL_DRYRUN_AGGREGATE_FITS')
        self.assertFalse(result['actual_query_executed'])

    def test_individually_fitting_jobs_cannot_bypass_aggregate_cap(self):
        plans = [self.plan(k)[0] for k in 'abc']
        self.assertEqual(self.audit(plans)['status'], 'HARD_RESOURCE_GAP')

    def test_existing_risk_not_counted_twice(self):
        plan, spec = self.plan('a')
        digest = bq.digest({'sql_sha256': spec['sql_sha256'], 'project': self.config['project'], 'location': 'US'})
        bq.save(self.work / 'private/stage1d_bigquery_jobs' / digest / 'job.json', {
            'sql_sha256': spec['sql_sha256'], 'job_id': 'stage1d_recovery_' + digest[:48],
            'plan_sha256': plan['sha256'], 'maximum_bytes_billed': 11 * 1073741824, 'state': 'SUBMITTED'})
        result = self.audit([plan], {'bq_scan_' + digest: {'reserved': 11 * 1073741824, 'actual': None}})
        self.assertEqual(result['new_upper_bound_total_bytes'], 0)

    def test_mutated_actual_dryrun_dependency_is_rejected(self):
        plan, _ = self.plan('a')
        (self.work / 'a_dry.json').write_text('{}', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'SHA-bound'):
            self.audit([plan])

    def test_subset_of_remaining_preparation_cannot_claim_aggregate_ready(self):
        first, _ = self.plan('a')
        second, _ = self.plan('b')
        self.audit([first, second])
        with patch.object(f, 'checked_freeze', return_value={'txphish_src002': self.query}), \
                self.assertRaisesRegex(ValueError, 'ALL remaining'):
            f.audit_actual_dryruns(self.work, 'bundle.json', [first], self.config)


class BatchRootTests(unittest.TestCase):
    def test_cache_read_is_once_per_export_and_missing_requests_are_exact(self):
        exported = [dict(record_type='trace', tx_hash=TX, block_number='100', trace_address=None),
                    dict(record_type='trace', tx_hash='0x' + 'b' * 64, block_number='100', trace_address=None)]
        spec = {'template': 'transaction_and_trace', 'schema_evidence': [], 'needed_ranges': [],
                'date_start_inclusive': '2024-01-01', 'date_end_exclusive': '2024-01-08',
                'sql_path': 'sql', 'sql_sha256': 'synthetic'}
        with tempfile.TemporaryDirectory(dir=HERE, prefix='synthetic_') as temporary:
            sql = Path(temporary) / 'sql'
            sql.write_text('SYNTHETIC_SQL', encoding='utf-8')
            with patch.object(bq, 'checked', return_value=sql), patch.object(bq, 'read', return_value=spec), \
                    patch.object(bq, 'schemas', return_value={}), patch.object(bq, 'build_sql', return_value='SYNTHETIC_SQL'), \
                    patch('stage1d_context_online._cached_rpc', return_value=[]) as cached:
                result = roots._bind_verified_export_roots(Path(temporary), {}, exported, 'SYNTHETIC_VERIFIED_EXPORT')
        self.assertEqual(cached.call_count, 1)
        self.assertEqual(len(cached.call_args.args[1]), 5)  # two tx, two receipts, one exact shared header
        self.assertEqual(result['rows'], exported)
        self.assertEqual(len(result['gaps']), 2)
        self.assertEqual({r['method'] for r in result['gaps'][0]['needed_rpc']},
                         {'eth_getTransactionByHash', 'eth_getTransactionReceipt', 'eth_getBlockByNumber'})


if __name__ == '__main__':
    # Run the existing fee/root strictness regression against these staged helper
    # modules as well; import cache already binds them to staged_final_context.
    spec = importlib.util.spec_from_file_location('fee_root_regression', HERE / 'test_bq_fee_tree_review.py')
    regression = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(regression)
    suite = unittest.TestSuite([unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]),
                                unittest.defaultTestLoader.loadTestsFromModule(regression)])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
