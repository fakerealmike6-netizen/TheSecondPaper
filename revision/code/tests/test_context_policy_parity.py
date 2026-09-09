"""Tx/X policy parity, root CLI, installed context and fee/root tests, offline."""
from pathlib import Path
from unittest.mock import patch
import importlib.util
import json
import sys
import unittest

HERE = Path(__file__).resolve().parent
CODE = HERE.parent
sys.path[:0] = [str(HERE), str(CODE / 'src')]
import stage1d_final_context_prepare as context
import stage1d_bq_context_prepare as bq
import stage1d_bigquery_probe as probe


def load(name, path, temp_here=False):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if temp_here:
        module.HERE = HERE
    return module


x_tests = load('x_extension_regression', CODE / 'tests/test_xscam_extension.py', temp_here=True)
existing_context = load('existing_context_regression', CODE / 'tests/test_final_context_prepare.py', temp_here=True)
fee_tests = load('fee_tree_regression', CODE / 'tests/test_bq_fee_tree_review.py')


class TxAuditParityTests(unittest.TestCase):
    def fixture(self, estimates, days=7):
        fixture = x_tests.AuditTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.query['name'] = 'txphish_src002'
        plans = fixture.fixtures(estimates, days=days)
        path = fixture.work / 'bundle.json'
        value = bq.read(path)
        value['freeze_sha256']=bq.sha(fixture.work/'private/BATCH_QUERY_FREEZE.json')
        value.update(consumers={'txphish_src001': {}, 'txphish_src002': {}}, query_names=sorted(context.QUERIES),
                     sql_owner='txphish_src002', clock_query='SHARED')
        path.write_text(json.dumps(value), encoding='utf-8')
        return fixture, plans

    def audit(self, fixture, plans):
        with patch.object(context, 'checked_freeze', return_value={'txphish_src002': fixture.query}), \
                patch.object(context, 'budget_snapshot_ro', return_value=(fixture.snapshot, {})), \
                patch.object(probe, 'dry_spec', return_value='SYNTHETIC_ONLY'):
            return context.audit_actual_dryruns(fixture.work, 'bundle.json', plans, fixture.config)

    def test_tx_single_job_overcap_is_not_misreported_as_total_exhaustion(self):
        fixture, plans = self.fixture([101 * x_tests.GIB, 10 * x_tests.GIB])
        result = self.audit(fixture, plans)
        self.assertEqual(result['status'], 'SINGLE_JOB_REQUIRES_ONE_DAY_REPLAN')
        self.assertEqual(result['clock_query'], 'SHARED')
        self.assertEqual(result['new_upper_bound_total_bytes'], 123 * x_tests.GIB)
        self.assertIsNone(result['jobs'][0]['maximum_bytes_billed'])

    def test_tx_complete_actual_audit_permits_same_demand_daily_plan(self):
        fixture, plans = self.fixture([101 * x_tests.GIB])
        result = self.audit(fixture, plans)
        path = fixture.work / 'audit.json'; bq.save(path, result)
        with patch.object(context, 'checked_freeze', return_value={'txphish_src002': fixture.query}), \
                patch.object(context, 'budget_snapshot_ro', return_value=(fixture.snapshot, {})), \
                patch.object(probe, 'dry_spec', return_value='SYNTHETIC_ONLY'):
            previous = context.check_one_day_replan(fixture.work, bq.dep(fixture.work, path), context.QUERIES,
                                                    fixture.work / 'frozen.json', fixture.config)
        self.assertEqual(previous['clock_query'], 'SHARED')
        self.assertEqual(previous['sql_owner'], 'txphish_src002')

    def test_tx_whole_total_overrun_stays_hard_resource_gap(self):
        fixture, plans = self.fixture([101 * x_tests.GIB, 10 * x_tests.GIB])
        fixture.snapshot['bigquery_bytes']['remaining'] = str(100 * x_tests.GIB)
        result = self.audit(fixture, plans)
        self.assertEqual(result['status'], 'HARD_RESOURCE_GAP')
        self.assertEqual(result['route'], 'COMPACT_DUNE_CURRENT_DEMAND')

    def test_tx_one_day_oversize_cannot_turn_into_hourly_scan(self):
        fixture, plans = self.fixture([101 * x_tests.GIB], days=1)
        result = self.audit(fixture, plans)
        self.assertEqual(result['status'], 'HARD_PLATFORM_CAPABILITY_GAP')
        self.assertEqual(result['route'], 'COMPACT_DUNE_CURRENT_DEMAND')


if __name__ == '__main__':
    suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromModule(module) for module in
        (sys.modules[__name__], x_tests, existing_context, fee_tests))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
