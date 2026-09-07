import hashlib
from http.client import IncompleteRead
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from budget_r2 import RevisionLedger, AUTH
from page_attempts import AttemptStore
from context_access_r3 import (R3_AUTH, RpcAccess, clock_usage, db_path, ledger_snapshot,
                               migrate, read, session, sha, validate_rpc)


class ContextAccessR3Tests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[1] / 'checks/r3_access_test_tmp'
        root.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=root)
        self.root = Path(self.temp.name)
        revisions = self.root / '03_workspaces/stage1B/revisions'
        self.old, self.w = revisions / 'r2', revisions / 'r3'
        for path in (self.old / 'private', self.w / 'private', self.w / 'configs', self.w / 'src'):
            path.mkdir(parents=True, exist_ok=True)
        self.put(self.old / 'RUN_STATE.json', {'checkpoint': 'CHECKPOINT_1B_R2_REACHED',
                 'new_network_submissions_allowed': False, 'delivery_directory': '04_deliverables/r2'})
        self.put(self.root / '04_deliverables/r2/02_FINAL_STATUS.json',
                 {'online_seconds_cumulative_upper': {'atomic_simple_transfer': '123', 'harmony_high_branch': '456'},
                  'raw_resource_summary': {'cumulative_conservative_raw_occupancy_bytes': 4321}})
        self.put(self.w / 'configs/STAGE1B_R3_POLICY.json', {'authorization_id': R3_AUTH, 'dune': {'authorization_id': AUTH}})
        confirmation = {'status': 'USER_CONFIRMED', 'execution_cap_credits': '20', 'authorization_id': AUTH,
                        'payment_method_added': False, 'extra_credits_enabled': False, 'account_context_ref': 'SYNTHETIC'}
        db = RevisionLedger(self.old / 'private/shared_budget_r2.sqlite')
        db.confirm('dune_credits', '2500', 'synthetic')
        db.confirm('rpc_operations', '500', 'synthetic')
        db.initialize('a' * 64, confirmation)
        db.reserve_dune_job('old_pending', 'synthetic')
        db.observe_execution('old_pending', '1.25', {'sha256': 'b' * 64}, terminal=True)
        db.reserve_export('old_pending', '2', {'rate_evidence': {'synthetic': True}, 'result_metadata': {'total_row_count': 1}})
        AttemptStore(self.old / 'private/dune_request_attempts.sqlite')
        self.put(self.old / 'private/dune_user_confirmation.json', confirmation)
        self.put(self.old / 'private/dune_rate_evidence.json', {'synthetic': True})
        self.put(self.old / 'private/current_dune_usage.json', {'synthetic': True})
        migrate(self.w, self.old)
        source = Path(__file__).resolve().parents[1] / 'src/context_access_r3.py'
        (self.w / 'src/context_access_r3.py').write_bytes(source.read_bytes())
        self.put(self.w / 'CONTINUATION_GATE_R3.json', {'status': 'PASS', 'run_id': self.w.name,
                 'source_sha256': {'context_access_r3.py': sha(self.w / 'src/context_access_r3.py')}})
        self.put(self.w / 'private/alchemy_permission_r3.json',
                 {'status': 'USER_CONFIRMED', 'existing_account_authorized': True, 'paid_overage_authorized': False,
                  'paid_overage_setting_verified': False, 'included_compute_units_remaining': 30000000,
                  'evidence': 'SYNTHETIC confirmation', 'method_cu_upper_bounds': {'eth_chainId': 10, 'eth_getBalance': 10},
                  'rate_evidence': {'synthetic': True}})

    def tearDown(self):
        self.temp.cleanup()

    def put(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding='utf-8')

    def plans(self):
        return [{'method': 'eth_chainId', 'params': []},
                {'method': 'eth_getBalance', 'params': ['0x' + 'a' * 40, '0x123']}]

    def transport(self, requests, bound):
        return 200, json.dumps([{'jsonrpc': '2.0', 'id': row['id'],
                                'result': '0x1' if row['method'] == 'eth_chainId' else '0x0'} for row in requests]).encode()

    def capabilities(self):
        return RpcAccess(self.w, self.transport).call_batch(self.plans(), 'SHARED', 'capability', True)

    def test_migration_is_identical_idempotent_and_no_new_grant(self):
        before = sha(self.old / 'private/shared_budget_r2.sqlite')
        receipt = migrate(self.w, self.old)
        self.assertFalse(receipt['new_allowance_granted'])
        self.assertTrue(all(row['identical'] for row in receipt['historical_tables'].values()))
        self.assertEqual(ledger_snapshot(self.w)['dune_credits']['cumulative_risk'], '3.25')
        self.assertEqual(before, sha(self.old / 'private/shared_budget_r2.sqlite'))

    def test_original_candidate_clock_is_retained_separately(self):
        inherited = read(self.w / 'private/INHERITED_RESOURCE_R3.json')
        self.assertEqual(inherited['inherited_candidate_online_seconds']['atomic_simple_transfer'], '123')
        self.assertEqual(clock_usage(self.w, 'atomic_simple_transfer'), 0)
        with session(self.w, 'atomic_simple_transfer', 'some_context'):
            pass
        self.assertGreater(clock_usage(self.w, 'atomic_simple_transfer'), 0)
        self.assertEqual(clock_usage(self.w, 'harmony_high_branch'), 0)

    def test_unclosed_clock_blocks_restart(self):
        self.put(self.w / 'private/context_sessions/interrupted.json',
                 {'probes': ['atomic_simple_transfer'], 'closed': False})
        with self.assertRaisesRegex(RuntimeError, 'Unresolved'):
            clock_usage(self.w, 'atomic_simple_transfer')

    def test_shared_context_is_charged_to_both_queries(self):
        self.capabilities()
        self.assertEqual(clock_usage(self.w, 'atomic_simple_transfer'), clock_usage(self.w, 'harmony_high_branch'))

    def test_exact_historical_requests_reject_latest_and_send(self):
        for plan in ({'method': 'eth_getBalance', 'params': ['0x' + 'a' * 40, 'latest']},
                     {'method': 'eth_getBlockByNumber', 'params': ['0x10', True]},
                     {'method': 'eth_sendRawTransaction', 'params': ['0x00']}):
            with self.assertRaises(ValueError):
                validate_rpc(plan)

    def test_successful_zero_balance_is_evidence_without_claiming_real_test(self):
        receipt = self.capabilities()
        self.assertEqual([m['status'] for m in receipt['members']], ['SUCCESS_VALIDATED'] * 2)
        envelope = read(self.w / receipt['members'][1]['artifact_path'])
        self.assertEqual(envelope['response']['result'], '0x0')
        self.assertEqual(envelope['evidence_kind'], 'SYNTHETIC_TRANSPORT')

    def test_operations_are_actual_and_cu_estimate_stays_reserved(self):
        self.capabilities()
        snapshot = ledger_snapshot(self.w)
        self.assertEqual(snapshot['rpc_operations']['actual'], '2')
        self.assertEqual(snapshot['alchemy_cu']['actual'], '0')
        self.assertEqual(snapshot['alchemy_cu']['reserved'], '20')

    def test_documented_zero_cu_chainid_still_counts_one_operation(self):
        path = self.w / 'private/alchemy_permission_r3.json'
        permission = read(path)
        permission['method_cu_upper_bounds']['eth_chainId'] = 0
        self.put(path, permission)
        receipt = RpcAccess(self.w, self.transport).call_batch([self.plans()[0]], 'SHARED', 'free_chainid', True)
        self.assertEqual(receipt['cu_upper_bound_not_actual'], 0)
        self.assertEqual(ledger_snapshot(self.w)['rpc_operations']['actual'], '1')

    def test_duplicate_request_cannot_charge_or_dispatch_twice(self):
        self.capabilities()
        before = ledger_snapshot(self.w)
        with self.assertRaisesRegex(RuntimeError, 'already attempted'):
            RpcAccess(self.w, self.transport).call_batch(self.plans(), 'SHARED', 'duplicate', True)
        self.assertEqual(before, ledger_snapshot(self.w))

    def test_provider_capability_maximum_is_five_members(self):
        self.capabilities()
        requests = [{'method': 'eth_getBalance', 'params': ['0x' + str(i) * 40, '0x123']} for i in range(1, 5)]
        with self.assertRaisesRegex(RuntimeError, 'five'):
            RpcAccess(self.w, self.transport).call_batch(requests, 'SHARED', 'too_many', True)

    def test_real_collection_requires_chain_and_historical_capability(self):
        with self.assertRaisesRegex(RuntimeError, 'capability'):
            RpcAccess(self.w, self.transport).call_batch([self.plans()[1]], 'SHARED', 'not_proven', False)

    def test_http_failure_consumes_attempt_units_and_is_not_retried(self):
        def denied(requests, bound): return 403, b'Forbidden'
        receipt = RpcAccess(self.w, denied).call_batch(self.plans(), 'SHARED', 'denied', True)
        self.assertTrue(all(m['status'] == 'HTTP_ERROR' for m in receipt['members']))
        self.assertEqual(ledger_snapshot(self.w)['rpc_operations']['actual'], '2')
        with self.assertRaises(RuntimeError):
            RpcAccess(self.w, denied).call_batch(self.plans(), 'SHARED', 'retry_denied', True)

    def test_secret_echo_is_withheld_without_disabling_guard(self):
        sentinel = 'synthetic-credential-do-not-export'
        def echo(requests, bound): return 200, sentinel.encode()
        with patch.dict('os.environ', {'ALCHEMY_API_KEY': sentinel}):
            receipt = RpcAccess(self.w, echo).call_batch(self.plans(), 'SHARED', 'echo', True)
        self.assertEqual(receipt['error_class'], 'CREDENTIAL_ECHO_WITHHELD')
        self.assertEqual((self.w / receipt['raw_path']).read_bytes(), b'')
        self.assertNotIn(sentinel, json.dumps(receipt))

    def test_permission_confirmed_once_does_not_refresh_allowance(self):
        access = RpcAccess(self.w, self.transport)
        access.permission()
        old = ledger_snapshot(self.w)['alchemy_cu']['confirmed_allowance']
        self.capabilities()
        access.permission()
        self.assertEqual(ledger_snapshot(self.w)['alchemy_cu']['confirmed_allowance'], old)

    def test_pending_rpc_intent_blocks_new_plan_after_restart(self):
        with closing(sqlite3.connect(db_path(self.w))) as db:
            db.execute('INSERT INTO r3_rpc_requests VALUES(?,?,?,?,?,?,?)', ('unknown', 'j', 'ALCHEMY', '{}', 0, 'DISPATCH_INTENT', None))
            db.commit()
        with self.assertRaisesRegex(RuntimeError, 'Unresolved'):
            RpcAccess(self.w, self.transport).call_batch(self.plans(), 'SHARED', 'after_interruption', True)

    def timeout_request(self):
        self.capabilities()
        plan = {'method': 'eth_getBalance', 'params': ['0x' + 'b' * 40, '0x124']}
        def timeout(requests, bound): raise TimeoutError('synthetic timeout')
        receipt = RpcAccess(self.w, timeout).call_batch([plan], 'SHARED', 'timeout')
        return plan, receipt['members'][0]['identity']

    def test_explicit_timeout_retry_keeps_old_cost_and_has_one_lineage(self):
        plan, original = self.timeout_request()
        receipt = RpcAccess(self.w, self.transport).call_batch([plan], 'SHARED', 'split_retry', retry_of=[original], retry_reason='One explicit smaller batch after zero-byte timeout')
        self.assertEqual(receipt['retry_of'], [original])
        self.assertFalse(receipt['old_attempt_risk_released'])
        self.assertEqual(ledger_snapshot(self.w)['rpc_operations']['actual'], '4')
        self.assertEqual(ledger_snapshot(self.w)['alchemy_cu']['reserved'], '40')
        with self.assertRaisesRegex(RuntimeError, 'one documented'):
            RpcAccess(self.w, self.transport).call_batch([plan], 'SHARED', 'second_retry', retry_of=[original], retry_reason='Forbidden second retry')

    def test_timeout_retry_cannot_change_request_parameters(self):
        plan, original = self.timeout_request()
        plan['params'][1] = '0x125'
        with self.assertRaisesRegex(RuntimeError, 'exact original'):
            RpcAccess(self.w, self.transport).call_batch([plan], 'SHARED', 'changed_retry', retry_of=[original], retry_reason='Changed request')

    def test_successful_rpc_cannot_use_timeout_retry_exception(self):
        receipt = self.capabilities()
        with self.assertRaisesRegex(RuntimeError, 'exact original'):
            RpcAccess(self.w, self.transport).call_batch([self.plans()[1]], 'SHARED', 'success_retry', retry_of=[receipt['members'][1]['identity']], retry_reason='Forbidden success replay')

    def test_timeout_retry_requires_explicit_reason_and_small_batch(self):
        plan, original = self.timeout_request()
        with self.assertRaises(ValueError):
            RpcAccess(self.w, self.transport).call_batch([plan], 'SHARED', 'no_reason', retry_of=[original])

    def test_zero_persisted_incomplete_read_can_retry_once_with_old_risk_retained(self):
        self.capabilities()
        plan = {'method': 'eth_getBalance', 'params': ['0x' + 'c' * 40, '0x126']}
        def interrupted(requests, bound): raise IncompleteRead(b'unpersisted prefix', 100)
        receipt = RpcAccess(self.w, interrupted).call_batch([plan], 'SHARED', 'incomplete_read')
        self.assertEqual(receipt['error_class'], 'IncompleteRead')
        self.assertEqual(receipt['raw_bytes'], 0)
        original = receipt['members'][0]['identity']
        retried = RpcAccess(self.w, self.transport).call_batch([plan], 'SHARED', 'incomplete_retry', retry_of=[original], retry_reason='Explicitly split after zero-persisted-body IncompleteRead')
        self.assertFalse(retried['old_attempt_risk_released'])
        self.assertEqual(ledger_snapshot(self.w)['rpc_operations']['actual'], '4')


if __name__ == '__main__':
    unittest.main()
