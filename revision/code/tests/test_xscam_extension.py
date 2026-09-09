"""Synthetic only; temp writes remain in this assigned extension directory."""
from pathlib import Path
from datetime import datetime, timezone
import importlib.util
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
CODE = HERE.parent
sys.path[:0] = [str(HERE), str(CODE / 'src')]
import stage1d_final_context_prepare as f
import stage1d_bq_context_prepare as bq
import stage1d_bigquery_probe as probe
from stage1d_recovery_policy import AUTH, BQ_TOTAL

GIB = 1073741824
A = '0x' + '1' * 40
B = '0x' + '2' * 40
TX = '0x' + 'a' * 64
BH = '0x' + 'b' * 64
X = {'xscam_src001'}


class GroupTests(unittest.TestCase):
    def test_default_two_tx_clock_unchanged_and_x_only_clock(self):
        self.assertEqual(f.query_group()[1:3], ('txphish_src002', 'SHARED'))
        self.assertEqual(f.query_group(['xscam_src001'])[1:3], ('xscam_src001', 'xscam_src001'))

    def test_no_lifi_subset_mixed_group_duplicates_or_string(self):
        for names in ({'lifi_src001'}, {'txphish_src001'}, {'xscam_src001', 'txphish_src002'},
                      ['xscam_src001', 'xscam_src001'], 'xscam_src001', set()):
            with self.subTest(names=names), self.assertRaises(ValueError):
                f.query_group(names)

    def test_x_cannot_inherit_shared_clock_or_another_owner(self):
        for owner, clock in [('txphish_src002', 'xscam_src001'), ('xscam_src001', 'SHARED')]:
            with self.assertRaises(ValueError):
                f.check_bundle_group({'consumers': {'xscam_src001': {}}, 'sql_owner': owner, 'clock_query': clock}, X)


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=HERE, prefix='synthetic_')
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        start = int(datetime(2022, 7, 20, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2022, 11, 4, 12, tzinfo=timezone.utc).timestamp())
        self.query = {'name': 'xscam_src001', 'query_id': 'qX', 'scope_hash': 'scopeX',
                      'start_block': 100, 'end_block': 1000, 'start_block_hash': BH, 'end_block_hash': BH,
                      'scope': {'start_block': 100, 'end_block': 1000, 'start_time': start, 'end_time': end},
                      'seed_event_id': 'synthetic_seed', 'seed_tx_hash': TX, 'seed_amount_raw': '7',
                      'seed_from': B, 'seed_to': A}
        bq.save(self.work / 'private/BATCH_QUERY_FREEZE.json', {'queries': [self.query]})
        self.freeze_sha = bq.sha(self.work / 'private/BATCH_QUERY_FREEZE.json')
        collection = {'query_id': 'qX', 'candidate_events': [{'event_id': 'synthetic_seed', 'sender': B,
            'recipient': A, 'block': 100, 'tx_index': 0, 'tx_hash': TX, 'amount_raw': '7', 'success': True,
            'kind': 'top', 'asset': 'native:eip155:1'}], 'context_events': [], 'stops': [], 'complete': False}
        bq.save(self.work / 'collection.json', collection)
        bq.save(self.work / 'labels.json', {})
        bq.save(self.work / 'frozen_inputs.json', {'freeze_sha256': self.freeze_sha,
            'candidates_frozen': True, 'labels_frozen': True, 'queries': {'xscam_src001': {
                'collection': bq.dep(self.work, self.work / 'collection.json'),
                'labels': bq.dep(self.work, self.work / 'labels.json')}}})
        (self.work / 'derived/stage1d/queries/xscam_src001/context').mkdir(parents=True)

    def prepare(self, **kwargs):
        fields = {bq.TX: {name: 'INTEGER' for name in ('receipt_status', 'receipt_gas_used',
                    'receipt_effective_gas_price', 'transaction_type', 'receipt_blob_gas_used', 'receipt_blob_gas_price')}, bq.TR: {}}
        with patch.object(bq, 'FREEZE_SHA', self.freeze_sha), patch.object(bq, 'schemas', return_value=fields), \
                patch.object(f, '_load_context', return_value=({}, {}, {}, [], [], [], [])), \
                patch.object(f, '_cached_rpc', return_value=[]):
            return f.prepare_current(self.work, 'frozen_inputs.json', 'prepared', {'tables': [bq.TX, bq.TR]}, [],
                                     query_names=X, **kwargs)

    def test_x_full_reference_dates_seven_day_family_and_exact_owner(self):
        result = self.prepare()
        self.assertEqual((result['sql_owner'], result['clock_query']), ('xscam_src001', 'xscam_src001'))
        self.assertEqual(result['query_names'], ['xscam_src001'])
        self.assertEqual(len(result['families']), 1)
        family = bq.read(bq.checked(self.work, result['families'][0]))
        partitions = [(r['date_start_inclusive'], r['date_end_exclusive']) for r in family['plans']]
        self.assertEqual(partitions, bq.date_chunks(self.query, 7))
        self.assertEqual(partitions[0][0], '2022-07-20')
        self.assertEqual(partitions[-1][1], '2022-11-05')
        self.assertEqual(len(partitions), 16)
        self.assertTrue(result['consumers']['xscam_src001']['missing_rpc'])
        self.assertFalse(bq.read(self.work / 'collection.json')['complete'])
        self.assertEqual(family['needed_ranges'], [{'address': A, 'start_block': 100, 'end_block': 100}])

    def test_no_hourly_or_unguarded_daily_replan(self):
        for value in (0, 0.5, 2, True, 1):
            with self.subTest(days=value), self.assertRaises(ValueError):
                self.prepare(chunk_days=value)

    def test_daily_replan_retains_entire_date_domain_and_exact_demand(self):
        prior = {'shared_ranges': [{'address': A, 'start_block': 100, 'end_block': 100}]}
        with patch.object(f, 'check_one_day_replan', return_value=prior):
            result = self.prepare(chunk_days=1, repartition_receipt=bq.dep(self.work, self.work / 'frozen_inputs.json'))
        family = bq.read(bq.checked(self.work, result['families'][0]))
        self.assertEqual([(r['date_start_inclusive'], r['date_end_exclusive']) for r in family['plans']],
                         bq.date_chunks(self.query, 1))
        self.assertEqual(len(family['plans']), 108)
        self.assertEqual(family['needed_ranges'], prior['shared_ranges'])

    def test_changed_current_demand_cannot_use_old_split_authority(self):
        with patch.object(f, 'check_one_day_replan', return_value={'shared_ranges': []}), \
                self.assertRaisesRegex(ValueError, 'same current ordinary-account demand'):
            self.prepare(chunk_days=1, repartition_receipt=bq.dep(self.work, self.work / 'frozen_inputs.json'))

    def test_existing_materialize_interface_accepts_only_x_consumer_and_preserves_gaps(self):
        bundle = self.prepare()
        normalized = {'rows': [], 'normalized': {'conflicts': []}, 'coverage': [],
                      'gaps': [{'type': 'SYNTHETIC_FEE_GAP', 'data_type': bq.FEES}], 'full_context_claimed': False}
        with patch.object(bq, 'FREEZE_SHA', self.freeze_sha), \
                patch.object(bq, 'normalize_complete_family', return_value=normalized) as normalize, \
                patch.object(f, '_cached_rpc', return_value=[]):
            result = f.materialize_family(self.work, 'prepared/SHARED_CONTEXT_PREPARATION.json',
                bundle['families'][0], {}, 'materialized', query_names=X)
        self.assertEqual(set(result), X)
        self.assertEqual(result['xscam_src001']['provider_gaps'], normalized['gaps'])
        self.assertFalse(result['xscam_src001']['formal_model_built'])
        self.assertTrue(normalize.call_args.kwargs['bind_roots'])
        self.assertEqual(bq.read(self.work / 'materialized/xscam_src001/coverage.json'), [])


class AuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=HERE, prefix='synthetic_')
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.query = {'name': 'xscam_src001', 'query_id': 'qX', 'scope_hash': 'scopeX'}
        self.config = {'project': 'example-project'}
        self.snapshot = {'bigquery_bytes': {'authorization_id': AUTH, 'cap': str(BQ_TOTAL), 'remaining': str(800 * GIB)}}

    def fixtures(self, estimates, days=7):
        plans, specs = [], []
        for index, estimate in enumerate(estimates):
            key = str(index)
            spec = {'sql_sha256': key.zfill(64), 'query_id': 'qX', 'scope_hash': 'scopeX'}
            spec_path, dry_path, plan_path = [self.work / (key + '_' + suffix + '.json') for suffix in ('spec', 'dry', 'plan')]
            bq.save(spec_path, spec)
            bq.save(dry_path, {'status': 'SUCCESS_VALIDATED', 'identity': {'project': 'example-project',
                'sql_sha256': spec['sql_sha256'], 'spec_sha256': bq.sha(spec_path), 'scope_hash': 'scopeX'},
                'result': {'kind': 'DRY_RUN', 'actual_query_executed': False, 'estimated_processed_bytes': estimate}})
            bq.save(plan_path, {'dry_spec_path': spec_path.name, 'dry_spec_sha256': bq.sha(spec_path),
                               'dry_receipt_path': dry_path.name, 'dry_receipt_sha256': bq.sha(dry_path)})
            plans.append(bq.dep(self.work, plan_path))
            specs.append(dict(bq.dep(self.work, spec_path), template='transaction_and_trace'))
        bq.save(self.work / 'family.json', {'plans': specs})
        bq.save(self.work / 'frozen.json', {'SYNTHETIC': True})
        bq.save(self.work/'private/BATCH_QUERY_FREEZE.json',{'queries':[self.query]})
        bq.save(self.work / 'bundle.json', {'freeze_sha256': bq.sha(self.work/'private/BATCH_QUERY_FREEZE.json'), 'chunk_days': days,
            'consumers': {'xscam_src001': {}}, 'query_names': ['xscam_src001'],
            'sql_owner': 'xscam_src001', 'clock_query': 'xscam_src001',
            'frozen_inputs': bq.dep(self.work, self.work / 'frozen.json'),
            'families': [bq.dep(self.work, self.work / 'family.json')]})
        return plans

    def audit(self, plans):
        with patch.object(f, 'checked_freeze', return_value={'xscam_src001': self.query}), \
                patch.object(f, 'budget_snapshot_ro', return_value=(self.snapshot, {})), \
                patch.object(probe, 'dry_spec', return_value='SYNTHETIC_ONLY'):
            return f.audit_actual_dryruns(self.work, 'bundle.json', plans, self.config, query_names=X)

    def test_all_full_plan_actual_bounds_are_summed_with_x_clock(self):
        result = self.audit(self.fixtures([10 * GIB] * 16))
        self.assertEqual(result['new_upper_bound_total_bytes'], 176 * GIB)
        self.assertEqual(result['status'], 'ACTUAL_DRYRUN_AGGREGATE_FITS')
        self.assertEqual(result['clock_query'], 'xscam_src001')

    def test_only_single_job_overcap_allows_explicit_daily_full_replan(self):
        result = self.audit(self.fixtures([101 * GIB, 10 * GIB]))
        self.assertEqual(result['status'], 'SINGLE_JOB_REQUIRES_ONE_DAY_REPLAN')
        self.assertEqual(result['route'], 'BQ_FULL_SCOPE_ONE_DAY_REPLAN')
        self.assertEqual(result['new_upper_bound_total_bytes'], 123 * GIB)
        self.assertFalse(result['actual_query_executed'])
        self.assertIsNone(result['jobs'][0]['maximum_bytes_billed'])

    def test_total_hard_limit_routes_compact_dune_without_starting_any_query(self):
        self.snapshot['bigquery_bytes']['remaining'] = str(100 * GIB)
        result = self.audit(self.fixtures([101 * GIB, 10 * GIB]))
        self.assertEqual(result['status'], 'HARD_RESOURCE_GAP')
        self.assertEqual(result['route'], 'COMPACT_DUNE_CURRENT_DEMAND')
        self.assertTrue(result['original_context_gaps_preserved'])
        self.assertFalse(result['automatic_replan_or_dune_query_executed'])

    def test_one_day_overcap_cannot_recurse_to_hours(self):
        result = self.audit(self.fixtures([101 * GIB], days=1))
        self.assertEqual(result['status'], 'HARD_PLATFORM_CAPABILITY_GAP')
        self.assertEqual(result['route'], 'COMPACT_DUNE_CURRENT_DEMAND')

    def test_unknown_month_room_stays_explicit(self):
        self.snapshot['bigquery_bytes']['remaining'] = None
        result = self.audit(self.fixtures([10 * GIB]))
        self.assertEqual(result['status'], 'HARD_RESOURCE_GAP')
        self.assertEqual(result['route'], 'COMPACT_DUNE_CURRENT_DEMAND')

    def test_subset_cannot_pass_full_long_window_audit(self):
        plans = self.fixtures([10 * GIB] * 16)
        with self.assertRaisesRegex(ValueError, 'ALL remaining'):
            self.audit(plans[:1])

    def test_one_day_authority_rechecks_original_all_plan_and_current_room(self):
        plans = self.fixtures([101 * GIB])
        receipt = self.audit(plans)
        bq.save(self.work / 'audit.json', receipt)
        dependency = bq.dep(self.work, self.work / 'audit.json')
        with patch.object(f, 'checked_freeze', return_value={'xscam_src001': self.query}), \
                patch.object(f, 'budget_snapshot_ro', return_value=(self.snapshot, {})), \
                patch.object(probe, 'dry_spec', return_value='SYNTHETIC_ONLY'):
            previous = f.check_one_day_replan(self.work, dependency, X, self.work / 'frozen.json', self.config)
            self.assertEqual(previous['chunk_days'], 7)
            self.snapshot['bigquery_bytes']['remaining'] = str(10 * GIB)
            with self.assertRaisesRegex(ValueError, 'no longer permits'):
                f.check_one_day_replan(self.work, dependency, X, self.work / 'frozen.json', self.config)
            retained = dict(receipt, jobs=[dict(receipt['jobs'][0], existing_state='PREPARED')])
            with patch.object(f, 'audit_actual_dryruns', return_value=retained), \
                    self.assertRaisesRegex(ValueError, 'cannot be replaced'):
                f.check_one_day_replan(self.work, dependency, X, self.work / 'frozen.json', self.config)


def installed_suite(filename, module_name, writable_temp=False):
    spec = importlib.util.spec_from_file_location(module_name, CODE / 'tests' / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if writable_temp:
        module.HERE = HERE  # Existing BudgetTests tempfile fixture remains inside assignment.
    return unittest.defaultTestLoader.loadTestsFromModule(module)


if __name__ == '__main__':
    suite = unittest.TestSuite([unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]),
        installed_suite('test_final_context_prepare.py', 'installed_context_regression', writable_temp=True),
        installed_suite('test_bq_fee_tree_review.py', 'installed_fee_root_regression')])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
