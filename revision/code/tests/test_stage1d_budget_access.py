"""Real R4 accessor paths with isolated synthetic Stage1D budget amendments."""
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import context_access_r4 as rpc
from context_queries_r3 import verify_frozen_scope
from dune_r4 import ContextDuneR4
from page_attempts import atomic_json
import stage1d_budget as budget
import test_dune_r4 as dune_support
import test_context_access_r4 as rpc_support


class Runtime:
    ledger_factory = staticmethod(budget.Stage1DLedger)
    raw_risk = staticmethod(rpc.raw_risk)
    session = staticmethod(rpc.session)
    verify_sql_freeze = staticmethod(verify_frozen_scope)
    allowed_kinds = ('context',)
    warning_threshold = Decimal('400')

    def __init__(self): self.warnings = []
    def warn_budget(self, work, snapshot): self.warnings.append(snapshot['dune_credits']['cumulative_risk'])


def amend_synthetic(case, work):
    source = b'SYNTHETIC isolated accessor amendment, never live account authorization'
    source_sha256 = hashlib.sha256(source).hexdigest()
    atomic_json(work / 'private/STAGE1D_BUDGET_AUTHORITY_EXPECTED.json', {
        'schema_version': 'stage1d-budget-authority-expected-v1', 'authorization_id': budget.AUTH,
        'source_sha256': source_sha256, 'source_bytes': len(source)})
    path = work / 'private/synthetic_authority.txt'; path.write_bytes(source)
    atomic_json(work / 'private/BATCH_QUERY_FREEZE.json', {'authorization_id': budget.SCOPE_AUTH,
        'queries': [{'query_id': 'synthetic:' + str(i),'scope_id':'synthetic:scope:'+str(i),
                     'scope_hash':hashlib.sha256(('synthetic-scope-'+str(i)).encode()).hexdigest()} for i in range(4)]})
    proof = {'authorization_id': budget.AUTH, 'source_path': path.relative_to(work).as_posix(),
        'source_sha256': source_sha256, 'source_bytes': len(source), 'status': 'USER_CONFIRMED',
        'cumulative_cap_credits': '500', 'warning_credits': '400', 'execution_cap_credits': '20',
        'payment_method_added': False, 'extra_credits_enabled': False}
    return budget.amend(work, proof)


class Stage1DBudgetDuneAccessTests(unittest.TestCase):
    def setUp(self):
        self.s = dune_support.DuneR4Tests(); self.s.setUp(); self.addCleanup(self.s.tearDown)
        amend_synthetic(self, self.s.w)
        self.runtime = Runtime()
        self.live = ContextDuneR4(self.s.w, self.s.transport, clock=self.s.clock,
            sleeper=self.s.clock.sleep, rng=lambda: 0, runtime=self.runtime)

    def test_submit_above100_uses500_ledger_and_cap20(self):
        self.live.db.reserve_export(self.s.job, 100, {'rate_evidence': {'synthetic': True}, 'result_metadata': self.s.md})
        folder, headers = self.s.make_freeze()
        with patch('context_queries_r3.load_verified_block_headers', return_value=headers):
            result = self.live.submit(folder / 'query.sql', 'synthetic', freeze_manifest=folder / 'freeze_manifest.json')
        self.assertEqual('B' * 26, result['execution_id'])
        state = json.loads((Path(result['job_folder']) / 'job.json').read_text())
        self.assertEqual('20', state['reserved_execution']); self.assertEqual('500', state['cumulative_cap_credits'])
        self.assertEqual(budget.AUTH, state['cumulative_budget_authorization_id'])
        self.assertEqual(Decimal(500), self.live.job_caps(state)['logical'])
        self.assertFalse(self.live.db.snapshot()['dune_credits']['overrun'])

    def test_export_recovery_above100_retains_risk_with500(self):
        self.s.md['total_result_set_bytes'] = 3000000; self.s.save()
        self.s.responses['results'] = [TimeoutError('synthetic')]
        result = self.live.export(self.s.folder)
        self.assertTrue(result['progress']['complete']); self.assertEqual('120', result['upper_not_actual'])
        self.assertEqual(2, len(self.s.calls)); self.assertEqual('500', result['cumulative']['cap'])
        self.assertFalse(result['cumulative']['overrun']); self.assertIsNone(result['cumulative']['halt_reason'])
        again = ContextDuneR4(self.s.w, self.s.transport, clock=self.s.clock,
            sleeper=self.s.clock.sleep, runtime=self.runtime)
        self.assertTrue(again.export(self.s.folder)['cache_reused']); self.assertEqual(2, len(self.s.calls))

    def test_observed_larger_metadata_crossing100_does_not_use_old_halt(self):
        md = self.s.md | {'total_result_set_bytes': 6000000, 'result_set_bytes': 6000000, 'row_count': 2}
        body = {'execution_id': self.s.execution, 'state': 'QUERY_STATE_COMPLETED',
                'result': {'rows': [{'x': 1}, {'x': 2}], 'metadata': md}}
        self.s.responses['results'] = [self.s.response(200, body)]
        result = self.live.export(self.s.folder)
        self.assertEqual('120', result['upper_not_actual']); self.assertFalse(result['cumulative']['overrun'])
        self.assertIsNone(result['cumulative']['halt_reason'])
        self.s.state['state'] = 'QUERY_STATE_PENDING'; self.s.save()
        result = self.live.poll(self.s.folder)
        self.assertFalse(result['cumulative']['overrun']); self.assertIsNone(result['cumulative']['halt_reason'])

    def test_observed_over500_retained_and_next_submission_blocked(self):
        md = self.s.md | {'total_result_set_bytes': 25000000, 'result_set_bytes': 25000000, 'row_count': 2}
        body = {'execution_id': self.s.execution, 'state': 'QUERY_STATE_COMPLETED',
                'result': {'rows': [{'x': 1}, {'x': 2}], 'metadata': md}}
        self.s.responses['results'] = [self.s.response(200, body)]
        result = self.live.export(self.s.folder)
        self.assertEqual('500', result['upper_not_actual']); self.assertTrue(result['cumulative']['overrun'])
        with self.assertRaises(RuntimeError): self.live.ensure_not_halted()
        self.assertEqual(1, len(self.s.calls))

    def test_warning400_is_delivered_without_pausing_submission(self):
        self.live.db.reserve_export(self.s.job, 385, {'rate_evidence': {'synthetic': True}, 'result_metadata': self.s.md})
        folder, headers = self.s.make_freeze()
        with patch('context_queries_r3.load_verified_block_headers', return_value=headers):
            self.live.submit(folder / 'query.sql', 'warning', freeze_manifest=folder / 'freeze_manifest.json')
        self.assertTrue(self.runtime.warnings); self.assertEqual(1, len(self.s.calls))
        self.assertTrue(all(Decimal(x) >= 400 for x in self.runtime.warnings))

    def test_warning_callback_failure_does_not_pause_reserved_operation(self):
        def failing(*args): raise ValueError('synthetic reminder failure')
        self.runtime.warn_budget = failing
        self.live.db.reserve_export(self.s.job, 385, {'rate_evidence': {'synthetic': True}, 'result_metadata': self.s.md})
        self.live.ensure_not_halted()
        self.assertEqual({'warning_delivery_failed': 'ValueError', 'warning_requires_pause': False}, self.live.last_budget_warning)

    def test_default100_remains_even_on_amended_ledger(self):
        self.s.md['total_result_set_bytes'] = 3000000; self.s.save()
        self.s.responses['results'] = [TimeoutError('synthetic')]
        default = ContextDuneR4(self.s.w, self.s.transport, clock=self.s.clock, sleeper=self.s.clock.sleep, rng=lambda: 0)
        with self.assertRaisesRegex(RuntimeError, 'cumulative100'): default.export(self.s.folder)
        self.assertEqual(1, len(self.s.calls)); self.assertEqual('100', default.db.snapshot()['dune_credits']['cap'])

    def test_runtime_cap_assertion_cannot_grant_or_conflict(self):
        self.runtime.cumulative_cap = Decimal(600)
        with self.assertRaisesRegex(RuntimeError, 'does not match'):
            ContextDuneR4(self.s.w, self.s.transport, runtime=self.runtime)

    def test_usage_observation_preserves500_and_provider_allowance(self):
        body = {'billing_periods': [{'start_date': '2000-01-01', 'end_date': '2099-01-01',
                'credits_included': '1000', 'credits_used': '100'}]}
        self.s.responses['usage'] = [self.s.response(200, body)]
        before = self.live.db.snapshot()['dune_credits']['cumulative_risk']
        result = self.live.usage()
        self.assertEqual('500', result['cumulative']['cap']); self.assertEqual(before, result['cumulative']['cumulative_risk'])
        self.assertEqual('900', result['included_remaining_credits'])


class Stage1DBudgetRPCAccessTests(unittest.TestCase):
    def test_rpc_constructor_and_real_reservation_use_injected_ledger(self):
        s = rpc_support.ContextAccessR4Tests(); s.setUp(); self.addCleanup(s.tearDown)
        amend_synthetic(self, s.w)
        runtime = Runtime()
        access = rpc.RpcAccess(s.w, s.success, clock=s.clock, monotonic=s.clock,
            sleep=s.clock.sleep, rng=lambda: 0, runtime=runtime)
        self.assertIsInstance(access.ledger, budget.Stage1DLedger)
        before = access.ledger.snapshot()
        result = access.call_batch(s.plans(1), 'SHARED', 'synthetic')
        self.assertEqual('COMPLETE', result['status']); self.assertEqual('500', result['snapshot']['dune_credits']['cap'])
        self.assertEqual(before['dune_credits']['cumulative_risk'], result['snapshot']['dune_credits']['cumulative_risk'])
        self.assertEqual(1, result['actual_operations_this_call'])


if __name__ == '__main__': unittest.main()
