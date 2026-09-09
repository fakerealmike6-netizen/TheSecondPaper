"""Synthetic resource adoption and effective-controller tests; no providers."""
import copy
import hashlib
import json
from decimal import Decimal
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import test_stage1d_budget as prior
from budget_r2 import RevisionLedger
from page_attempts import atomic_json
from stage1d_budget import Stage1DLedger
import stage1d_resource_alignment as aligned


class ResourceAlignmentTests(unittest.TestCase):
    def setUp(self):
        self.base = prior.Stage1DBudgetTests(); self.base.setUp()
        self.addCleanup(self.base.tearDown)
        self.w, self.path = self.base.w, self.base.path
        self.old = self.base.amended()
        self.old.confirm('rpc_operations', '500', 'Synthetic old project request quota')
        self.old.confirm('alchemy_cu', '50000', 'Synthetic old project CU quota')
        self.old.confirm('bigquery_bytes', '5368709120', 'Synthetic same BQ allowance')
        self.old.reserve_dune_job('old_done', 'synthetic old20')
        self.old.observe_execution('old_done', '2', {'sha256': 'a' * 64}, True)
        self.old.reserve_export('old_done', '12', self.ev())
        self.old.reserve('old_rpc_unknown', 'alchemy', 'synthetic old unknown reads',
                         {'rpc_operations': 500, 'alchemy_cu': 50000})
        self.source = self.w / 'private/resource_alignment_synthetic.txt'
        data = b'SYNTHETIC explicit cumulative2000 new50 warn1600 RPC10000 CU1000000.'
        self.source.write_bytes(data)
        digest = hashlib.sha256(data).hexdigest()
        atomic_json(self.w / aligned.EXPECTED_FILE, {'schema_version': 'stage1d-resource-alignment-expected-v1',
            'authorization_id': aligned.AUTH, 'source_sha256': digest, 'source_bytes': len(data)})
        self.proof = {'authorization_id': aligned.AUTH, 'source_path': self.source.relative_to(self.w).as_posix(),
            'source_sha256': digest, 'source_bytes': len(data), 'status': 'USER_CONFIRMED',
            'cumulative_cap_credits': '2000', 'warning_credits': '1600', 'execution_cap_credits': '50',
            'rpc_operations_cap': '10000', 'alchemy_cu_cap': '1000000',
            'payment_method_added': False, 'extra_credits_enabled': False,
            'account_usage_snapshot': {'used': '163.876', 'included': '2500', 'project_settlement': False}}

    def ev(self):
        return {'rate_evidence': {'synthetic': True}, 'result_metadata': {'total_row_count': 100}}

    def adopt(self):
        aligned.adopt(self.w, self.proof)
        return aligned.Stage1DResourceLedger(self.path)

    def test_adoption_is_append_only_idempotent_and_retains_old_controllers(self):
        with self.old.connection() as db: tables = aligned._old_tables(db)
        old500 = self.old.snapshot(); old100 = RevisionLedger(self.path).snapshot()
        receipt = aligned.adopt(self.w, self.proof)
        self.assertEqual(receipt, aligned.adopt(self.w, self.proof))
        with self.old.connection() as db:
            self.assertEqual(tables, aligned._old_tables(db))
            self.assertEqual(1, db.execute('SELECT COUNT(*) FROM stage1d_resource_authorizations').fetchone()[0])
            self.assertEqual(1, db.execute('SELECT COUNT(*) FROM stage1d_resource_receipts').fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM stage1d_journal WHERE kind='RESOURCE_TOTALS_ALIGNED_WITH_NEW_AUTHORITY'").fetchone()[0])
        self.assertEqual(old500, Stage1DLedger(self.path).snapshot())
        self.assertEqual(old100, RevisionLedger(self.path).snapshot())
        now = aligned.Stage1DResourceLedger(self.path).snapshot()
        self.assertEqual('2000', now['dune_credits']['cap'])
        self.assertEqual('30.433088236', now['dune_credits']['cumulative_risk'])
        self.assertEqual('1969.566911764', now['dune_credits']['remaining'])
        self.assertEqual('9500', now['rpc_operations']['remaining'])
        self.assertEqual('950000', now['alchemy_cu']['remaining'])

    def test_read_only_probe_false_then_true_and_corruption_never_falls_back(self):
        before = self.path.read_bytes(); self.assertFalse(aligned.is_aligned(self.path))
        self.assertEqual(before, self.path.read_bytes())
        self.adopt(); before = self.path.read_bytes()
        self.assertTrue(aligned.is_aligned(self.path)); self.assertEqual(before, self.path.read_bytes())
        with self.old.connection() as db:
            db.execute('UPDATE stage1d_resource_authorizations SET source_sha256=?', ('0' * 64,))
        with self.assertRaises(ValueError): aligned.is_aligned(self.path)

    def test_no_implicit_adoption_or_new_pool(self):
        with self.assertRaises(RuntimeError): aligned.Stage1DResourceLedger(self.path)
        missing = self.w / 'private/new_pool.sqlite'
        self.assertFalse(aligned.is_aligned(missing))
        with self.assertRaises(ValueError): aligned.Stage1DResourceLedger(missing)
        self.assertFalse(missing.exists())

    def test_new_job_reserves50_and_old_job_retains20_submission_proof(self):
        live = self.adopt()
        self.assertEqual(Decimal('20'), live.execution_cap('old_done'))
        self.assertEqual(Decimal('50'), live.execution_cap())
        live.reserve_dune_job('new_job', 'synthetic new50')
        with live.connection() as db:
            self.assertEqual('50', db.execute("SELECT execution_risk FROM r2_components WHERE job='new_job'").fetchone()[0])
            self.assertEqual('2', db.execute("SELECT execution_risk FROM r2_components WHERE job='old_done'").fetchone()[0])
        policy = live.submission_policy('new_job')
        self.assertEqual(aligned.AUTH, policy['authorization_id'])
        self.assertEqual('50', policy['execution_cap_credits'])
        self.assertIn('authorization_proof_sha256', policy)
        before = live.snapshot()['dune_credits']['cumulative_risk']
        with self.assertRaises(ValueError): live.reserve_dune_job('old_done', 'do not reprice old job')
        self.assertEqual(before, live.snapshot()['dune_credits']['cumulative_risk'])
        with self.assertRaises(ValueError): live.reserve_dune_job('another', 'wrong old default', execution_estimate='20')

    def test_old_unknown_execution_resumes_without50_reservation(self):
        self.old.reserve_dune_job('old_unknown', 'synthetic before adoption')
        before = self.old.snapshot()['dune_credits']['cumulative_risk']
        live = self.adopt()
        self.assertEqual(before, live.snapshot()['dune_credits']['cumulative_risk'])
        self.assertEqual(Decimal(20), live.execution_cap('old_unknown'))
        live.observe_execution('old_unknown', '3', {'sha256': 'b' * 64}, False)
        with live.connection() as db:
            self.assertEqual('20', db.execute("SELECT execution_risk FROM r2_components WHERE job='old_unknown'").fetchone()[0])
        with self.assertRaises(RuntimeError): live.reserve_dune_job('new', 'must wait for old execution')

    def test_new_observed40_allowed_but_old21_is_still_cap_violation(self):
        live = self.adopt(); live.reserve_dune_job('new', 'synthetic')
        live.observe_execution('new', '40', {'sha256': 'b' * 64}, True)
        self.assertFalse(live.snapshot()['dune_credits']['overrun'])
        live.observe_execution('old_done', '21', {'sha256': 'c' * 64}, True)
        self.assertIn('EXECUTION_CAP20_EXCEEDED', live.snapshot()['dune_credits']['halt_reason'])

    def test_new51_observation_retained_and_halts(self):
        live = self.adopt(); live.reserve_dune_job('new', 'synthetic')
        live.observe_execution('new', '51', {'sha256': 'b' * 64}, True)
        self.assertIn('EXECUTION_CAP50_EXCEEDED', live.snapshot()['dune_credits']['halt_reason'])
        self.assertEqual('51', live.final_execution_cost('new').to_eng_string())

    def test_exports_above500_warning1600_then_total2000_blocks(self):
        live = self.adopt()
        live.reserve_export('old_done', '1600', self.ev())
        snapshot = live.snapshot()['dune_credits']
        self.assertTrue(snapshot['warning_at_1600']); self.assertFalse(snapshot['overrun'])
        self.assertNotIn('warning_at_400', snapshot)
        live.reserve_dune_job('new', 'synthetic allowed above500')
        live.observe_execution('new', '1', {'sha256': 'c' * 64}, True)
        with self.assertRaises(RuntimeError): live.reserve_export('old_done', '2000', self.ev())
        live.reserve_export('old_done', '2000', self.ev(), observed=True)
        self.assertTrue(live.snapshot()['dune_credits']['overrun'])

    def test_rpc_and_cu_reserve_use_new_total_after_inherited_consumption(self):
        live = self.adopt()
        live.reserve('new_rpc', 'alchemy', 'synthetic newly authorized operations',
                     {'rpc_operations': 9500, 'alchemy_cu': 950000})
        self.assertEqual('0', live.snapshot()['rpc_operations']['remaining'])
        self.assertEqual('0', live.snapshot()['alchemy_cu']['remaining'])
        with self.assertRaises(RuntimeError):
            live.reserve('extra_rpc', 'alchemy', 'outside total', {'rpc_operations': 1, 'alchemy_cu': 1})
        with live.connection() as db:
            self.assertEqual(('500', '500'), db.execute("SELECT cap,available FROM limits WHERE unit='rpc_operations'").fetchone())
            self.assertEqual(('50000', '50000'), db.execute("SELECT cap,available FROM limits WHERE unit='alchemy_cu'").fetchone())

    def test_bigquery_meta_and_provider_allowance_remain_unchanged(self):
        live = self.adopt()
        with self.assertRaises(ValueError): live.reserve('bq', 'bigquery', 'over single1GiB', {'bigquery_bytes': 1073741825})
        with self.assertRaises(RuntimeError): live.reserve('meta', 'metasleuth', 'unauthorized', {'meta_requests': 1})
        self.assertEqual('5368709120', live.snapshot()['bigquery_bytes']['cap'])
        self.assertEqual('2500', live.snapshot()['dune_credits']['confirmed_allowance'])
        self.assertEqual('30.433088236', live.snapshot()['dune_credits']['cumulative_risk'])

    def test_smaller_confirmed_dune_allowance_still_limits_new_execution(self):
        live = self.adopt(); live.confirm('dune_credits', '60', 'Synthetic verified smaller provider allowance')
        self.assertEqual('29.566911764', live.snapshot()['dune_credits']['remaining'])
        with self.assertRaises(RuntimeError): live.reserve_dune_job('new', 'does not fit provider allowance')

    def test_changed_proof_source_and_active_writer_rejected(self):
        lock = self.w / 'private/network_worker.lock'; lock.write_text('synthetic')
        with self.assertRaises(RuntimeError): aligned.adopt(self.w, self.proof)
        lock.unlink(); self.adopt()
        with self.assertRaises(ValueError): aligned.adopt(self.w, dict(self.proof, cumulative_cap_credits='4000'))
        self.source.write_bytes(b'changed')
        with self.assertRaises(ValueError): aligned.is_aligned(self.path)
        with self.assertRaises(ValueError): aligned.Stage1DResourceLedger(self.path).snapshot()


if __name__ == '__main__': unittest.main(verbosity=2)
