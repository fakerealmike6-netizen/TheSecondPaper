"""Real Runtime/R4/cost/RPC hooks with isolated synthetic provider transports."""
import hashlib
import json
from decimal import Decimal
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import context_access_r4 as rpc
from context_queries_r3 import verify_frozen_scope
from dune_r4 import ContextDuneR4
from context_access_r3 import sha
from page_attempts import atomic_json
from stage1d_runtime import Runtime
from stage1d_costs import Stage1DDune
from stage1d_budget import Stage1DLedger
import stage1d_resource_alignment as aligned
from test_stage1d_budget_access import amend_synthetic
import test_dune_r4 as dune_fixture
import test_context_access_r4 as rpc_fixture
import test_stage1d_costs as costs_fixture


def adopt_synthetic(work):
    data = b'SYNTHETIC resource hooks50/2000/RPC10000/CU1000000; no real authorization.'
    source = work / 'private/synthetic_resource_hooks.txt'; source.write_bytes(data)
    digest = hashlib.sha256(data).hexdigest()
    atomic_json(work / aligned.EXPECTED_FILE, {'schema_version': 'stage1d-resource-alignment-expected-v1',
        'authorization_id': aligned.AUTH, 'source_sha256': digest, 'source_bytes': len(data)})
    return aligned.adopt(work, {'authorization_id': aligned.AUTH, 'source_path': source.relative_to(work).as_posix(),
        'source_sha256': digest, 'source_bytes': len(data), 'status': 'USER_CONFIRMED',
        'cumulative_cap_credits': '2000', 'warning_credits': '1600', 'execution_cap_credits': '50',
        'rpc_operations_cap': '10000', 'alchemy_cu_cap': '1000000',
        'payment_method_added': False, 'extra_credits_enabled': False})


class AccessRuntime(Runtime):
    # Synthetic R4 fixtures carry the accepted R3 query/clock fixtures. The
    # actual Runtime ledger selection, warning and RPC validation remain active.
    raw_risk = staticmethod(rpc.raw_risk)
    session = staticmethod(rpc.session)
    verify_sql_freeze = staticmethod(verify_frozen_scope)


class ResourceDuneHooksTests(unittest.TestCase):
    def setUp(self):
        self.s = dune_fixture.DuneR4Tests(); self.s.setUp(); self.addCleanup(self.s.tearDown)
        amend_synthetic(self, self.s.w)
        self.runtime = AccessRuntime()

    def aligned_live(self):
        adopt_synthetic(self.s.w)
        return ContextDuneR4(self.s.w, self.s.transport, clock=self.s.clock,
            sleeper=self.s.clock.sleep, rng=lambda: 0, runtime=self.runtime)

    def test_actual_runtime_selects_old500_then_aligned2000_and_bad_state_fails(self):
        path = self.s.w / 'private/shared_budget_r4.sqlite'
        self.assertIs(type(self.runtime.ledger_factory(path)), Stage1DLedger)
        adopt_synthetic(self.s.w)
        self.assertIsInstance(self.runtime.ledger_factory(path), aligned.Stage1DResourceLedger)
        with self.runtime.ledger_factory(path).connection() as db:
            db.execute('UPDATE stage1d_resource_authorizations SET source_sha256=?', ('0' * 64,))
        with self.assertRaises(ValueError): self.runtime.ledger_factory(path)

    def test_real_submit_reserves50_binds_policy_and_keeps_old_job20(self):
        before = (self.s.w / 'private/dune_user_confirmation.json').read_bytes()
        live = self.aligned_live(); folder, headers = self.s.make_freeze()
        self.assertEqual(Decimal(20), live.job_caps(self.s.state)['execution'])
        with patch('context_queries_r3.load_verified_block_headers', return_value=headers):
            result = live.submit(folder / 'query.sql', 'synthetic50', freeze_manifest=folder / 'freeze_manifest.json')
        state = json.loads((Path(result['job_folder']) / 'job.json').read_text())
        self.assertEqual('50', state['reserved_execution'])
        self.assertEqual('50', state['submission_execution_cap_credits'])
        self.assertEqual('2000', state['cumulative_cap_credits'])
        self.assertEqual(aligned.AUTH, state['submission_policy']['authorization_id'])
        self.assertEqual(Decimal(50), live.job_caps(state)['execution'])
        self.assertEqual(before, (self.s.w / 'private/dune_user_confirmation.json').read_bytes())
        self.assertEqual(1, len(self.s.calls))

    def test_warning1600_is_real_runtime_warning_and_does_not_pause_submit(self):
        live = self.aligned_live()
        live.db.reserve_export(self.s.job, '900', {'rate_evidence': {'synthetic': True}, 'result_metadata': self.s.md})
        live.ensure_not_halted()
        warning_dir = self.s.w / 'private/stage1d_authority'
        self.assertFalse((warning_dir / 'WARNING_400.json').exists())
        self.assertFalse((warning_dir / 'WARNING_1600.json').exists())
        live.db.reserve_export(self.s.job, '1600', {'rate_evidence': {'synthetic': True}, 'result_metadata': self.s.md})
        folder, headers = self.s.make_freeze()
        with patch('context_queries_r3.load_verified_block_headers', return_value=headers):
            live.submit(folder / 'query.sql', 'warning1600', freeze_manifest=folder / 'freeze_manifest.json')
        self.assertTrue((warning_dir / 'WARNING_1600.json').is_file())
        self.assertFalse(live.db.snapshot()['dune_credits']['overrun'])
        self.assertEqual(1, len(self.s.calls))


class ResourceRPCHooksTests(unittest.TestCase):
    def test_rpc_dispatch_resumes_after_old500_limit_and_success_cache_is_reused(self):
        s = rpc_fixture.ContextAccessR4Tests(); s.setUp(); self.addCleanup(s.tearDown)
        amend_synthetic(self, s.w)
        old = Stage1DLedger(s.w / 'private/shared_budget_r4.sqlite')
        snapshot = old.snapshot()
        old.reserve('synthetic_old_rpc_capacity', 'alchemy', 'exhaust previous project allocation',
            {'rpc_operations': snapshot['rpc_operations']['remaining'], 'alchemy_cu': snapshot['alchemy_cu']['remaining']})
        self.assertEqual('0', old.snapshot()['rpc_operations']['remaining'])
        adopt_synthetic(s.w)
        access = rpc.RpcAccess(s.w, s.success, clock=s.clock, monotonic=s.clock,
            sleep=s.clock.sleep, rng=lambda: 0, runtime=AccessRuntime())
        self.assertIsInstance(access.ledger, aligned.Stage1DResourceLedger)
        before = access.ledger.snapshot()
        result = access.call_batch(s.plans(1), 'SHARED', 'synthetic aligned RPC')
        self.assertEqual('COMPLETE', result['status'])
        self.assertEqual(1, result['actual_operations_this_call'])
        self.assertEqual('2000', result['snapshot']['dune_credits']['cap'])
        self.assertEqual(Decimal(before['rpc_operations']['remaining']) - 1,
                         Decimal(result['snapshot']['rpc_operations']['remaining']))
        again = access.call_batch(s.plans(1), 'SHARED', 'synthetic same RPC')
        self.assertEqual(0, again['actual_operations_this_call'])


class ResourceTerminalCostHooksTests(unittest.TestCase):
    def setUp(self):
        self.s = costs_fixture.Stage1DCostTests(); self.s.setUp(); self.addCleanup(self.s.tearDown)
        amend_synthetic(self, self.s.w)
        # Bind the unchanged synthetic SQL to a query in the synthetic four.
        freeze = json.loads(self.s.freeze.read_text()); freeze['query_ids'] = ['synthetic:0']
        query=json.loads((self.s.w/'private/BATCH_QUERY_FREEZE.json').read_text())['queries'][0]
        freeze.update(scope_id=query['scope_id'],scope_hash=query['scope_hash'])
        atomic_json(self.s.freeze, freeze); self.s.state['scope_freeze_sha256'] = sha(self.s.freeze)
        atomic_json(self.s.folder / 'job.json', self.s.state)
        adopt_synthetic(self.s.w)
        self.live = Stage1DDune(self.s.w, transport=lambda *a: self.fail('No data calls permitted'), runtime=Runtime())

    def test_real_cost_verifier_rejects30_for_old20_without_repricing(self):
        body = dict(self.s.body, execution_cost_credits='30'); receipt = self.s.make_receipt(body)
        before = self.live.db.snapshot()['dune_credits']['cumulative_risk']
        with self.assertRaisesRegex(ValueError, 'cap20'):
            self.live.observe_execution_charge(self.s.state, body, receipt)
        self.assertEqual(before, self.live.db.snapshot()['dune_credits']['cumulative_risk'])
        self.assertEqual(Decimal(20), self.live.db.execution_cap(self.s.job))

    def test_real_cost_verifier_accepts30_for_new50_and_checks_submission_proof(self):
        sql = '-- Synthetic new aligned execution\nSELECT 2\n'
        digest = hashlib.sha256(sql.encode()).hexdigest(); job = 'dune_r4:' + digest
        folder = self.s.w / 'private/dune_r2_jobs' / digest; folder.mkdir()
        (folder / 'query.sql').write_text(sql, encoding='utf8', newline='\n')
        freeze = self.s.w / 'private/synthetic_aligned_scope.json'
        value = json.loads(self.s.freeze.read_text()); value['sql_sha256'] = digest; atomic_json(freeze, value)
        self.live.db.reserve_dune_job(job, 'synthetic new50')
        state = dict(self.s.state, logical_job_id=job, sql_sha256=digest, execution_cost_credits='0',
            reserved_execution='50', scope_freeze_path=freeze.relative_to(self.s.w).as_posix(),
            scope_freeze_sha256=sha(freeze), request_set_closed=False,
            submission_execution_cap_credits='50', submission_policy=self.live.db.submission_policy(job))
        body = dict(self.s.body, execution_cost_credits='30'); receipt = self.s.make_receipt(body)
        state.update(status_response=body, status_receipt=receipt); atomic_json(folder / 'job.json', state)
        result = self.live.observe_execution_charge(state, body, receipt)
        self.assertEqual('30', self.live.db.final_execution_cost(job).to_eng_string())
        self.assertFalse(self.live.db.snapshot()['dune_credits']['overrun'])
        changed = dict(state, submission_execution_cap_credits='20')
        with self.assertRaisesRegex(ValueError, 'submission cap'):
            self.live.observe_execution_charge(changed, body, receipt)


if __name__ == '__main__': unittest.main(verbosity=2)
