"""Synthetic-only hydrated blocks and inherited persistent paid-read machinery."""
import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import context_access_r4 as old
import context_access_r3 as legacy
import test_context_access_r4 as support
from stage1d_rpc import Stage1DRpcValidation


def header(number=42, count=151):
    return {'number': hex(number), 'hash': '0x' + format(number, '064x'), 'timestamp': '0x1234',
            'transactions': ['0x' + format(index + 1000, '064x') for index in range(count)]}


def hydrated(h):
    return {**h, 'transactions': [{'hash': tx_hash, 'blockHash': h['hash'], 'blockNumber': h['number'],
        'transactionIndex': hex(i), 'from': '0x' + '1' * 40, 'to': '0x' + '2' * 40,
        'value': '0x1', 'gas': '0x5208', 'input': '0x'} for i, tx_hash in enumerate(h['transactions'])]}


class Stage1DRpcTests(unittest.TestCase):
    def setUp(self):
        self.h = header()
        self.hooks = Stage1DRpcValidation({'42': self.h})
        self.plan = {'method': 'eth_getBlockByNumber', 'params': ['0x2a', True]}
        self.request = {'jsonrpc': '2.0', 'id': 'synthetic', **self.plan}
        self.response = {'jsonrpc': '2.0', 'id': 'synthetic', 'result': hydrated(self.h)}

    def test_complete_151_hydrated_objects_validate(self):
        self.assertEqual('SUCCESS_VALIDATED', self.hooks.rpc_result_status(self.request, self.response))

    def test_empty_block_can_be_complete(self):
        h = header(count=0)
        hooks = Stage1DRpcValidation({42: h})
        self.assertEqual('SUCCESS_VALIDATED', hooks.rpc_result_status(self.request, self.response | {'result': hydrated(h)}))

    def test_legacy_entry_rejects_true_and_false_identity_unchanged(self):
        with self.assertRaises(ValueError): legacy.validate_rpc(self.plan)
        with self.assertRaises(ValueError): old.rpc_identity('synthetic', self.plan)
        plan = self.plan | {'params': ['0x2a', False]}
        self.assertEqual(legacy.validate_rpc(plan), self.hooks.validate_rpc(plan))
        self.assertEqual(old.rpc_identity('synthetic', plan), self.hooks.rpc_identity('synthetic', plan))
        request = self.request | plan
        response = self.response | {'result': self.h}
        self.assertEqual(legacy.rpc_result_status(request, response), self.hooks.rpc_result_status(request, response))

    def test_true_identity_binds_header_and_order_not_run_name(self):
        first = self.hooks.rpc_identity('synthetic', self.plan)
        self.assertEqual(first, Stage1DRpcValidation({42: self.h}).rpc_identity('synthetic', self.plan))
        altered = {**self.h, 'transactions': list(reversed(self.h['transactions']))}
        self.assertNotEqual(first, Stage1DRpcValidation({42: altered}).rpc_identity('synthetic', self.plan))
        false = self.plan | {'params': ['0x2a', False]}
        self.assertNotEqual(first, self.hooks.rpc_identity('synthetic', false))

    def test_unbound_and_moving_blocks_rejected(self):
        for selector in ['latest', 'pending', '0x2b', 42, '0x02a']:
            with self.subTest(selector=selector), self.assertRaises(ValueError):
                self.hooks.validate_rpc(self.plan | {'params': [selector, True]})
        with self.assertRaises(ValueError): self.hooks.validate_rpc(self.plan | {'extra': 1})

    def test_no_debug_trace_extension(self):
        with self.assertRaises(ValueError):
            self.hooks.validate_rpc({'method': 'debug_traceBlockByNumber', 'params': ['0x2a', {}]})
        with self.assertRaises(ValueError):
            self.hooks.validate_rpc({'method': 'trace_block', 'params': ['0x2a']})
        plan = {'method': 'debug_traceTransaction', 'params': ['0x' + '1' * 64,
            {'tracer': 'callTracer', 'tracerConfig': {'withLog': True}}]}
        self.assertEqual(plan, legacy.validate_rpc(plan))
        with self.assertRaises(ValueError): self.hooks.validate_rpc(plan)

    def test_prior_header_identity_and_hash_list_must_validate(self):
        for value in [self.h | {'transactions': [self.h['transactions'][0]] * 2},
                      self.h | {'transactions': hydrated(self.h)['transactions']},
                      self.h | {'timestamp': None}, self.h | {'number': 'latest'}]:
            with self.subTest(value=list(value)), self.assertRaises(ValueError): Stage1DRpcValidation({42: value})
        with self.assertRaises(ValueError): Stage1DRpcValidation({41: self.h})

    def test_short_mixed_or_hash_only_response_not_complete(self):
        for ts in [self.h['transactions'], self.response['result']['transactions'][:-1],
                   [self.h['transactions'][0]] + self.response['result']['transactions'][1:]]:
            response = copy.deepcopy(self.response); response['result']['transactions'] = ts
            with self.subTest(count=len(ts)):
                self.assertNotEqual('SUCCESS_VALIDATED', self.hooks.rpc_result_status(self.request, response))

    def test_every_transaction_block_hash_index_and_identity_bind(self):
        for field, value in [('hash', '0x' + 'e' * 64), ('blockHash', '0x' + 'f' * 64),
                             ('blockNumber', '0x2b'), ('transactionIndex', '0x0'),
                             ('transactionIndex', 150), ('value', None), ('gas', '0x00'),
                             ('from', 'invalid'), ('input', '0x1')]:
            response = copy.deepcopy(self.response)
            response['result']['transactions'][-1][field] = value
            with self.subTest(field=field, value=value):
                self.assertNotEqual('SUCCESS_VALIDATED', self.hooks.rpc_result_status(self.request, response))

    def test_duplicate_and_reordered_transactions_rejected(self):
        for ts in [list(reversed(self.response['result']['transactions'])),
                   [self.response['result']['transactions'][0]] * 151]:
            response = copy.deepcopy(self.response); response['result']['transactions'] = ts
            self.assertNotEqual('SUCCESS_VALIDATED', self.hooks.rpc_result_status(self.request, response))

    def test_wrong_response_id_block_or_timestamp_rejected(self):
        for response in [self.response | {'id': 'other'}, self.response | {'id': 1},
                         self.response | {'result': self.response['result'] | {'hash': '0x' + 'f' * 64}},
                         self.response | {'result': self.response['result'] | {'number': '0x2b'}},
                         self.response | {'result': self.response['result'] | {'timestamp': '0x1235'}}]:
            self.assertNotEqual('SUCCESS_VALIDATED', self.hooks.rpc_result_status(self.request, response))

    def test_null_error_and_legacy_other_reads_propagate(self):
        self.assertEqual('NULL_RESULT_NOT_EVIDENCE', self.hooks.rpc_result_status(self.request, self.response | {'result': None}))
        error = {'jsonrpc': '2.0', 'id': 'synthetic', 'error': {'code': -32600, 'message': 'not available'}}
        self.assertEqual('RPC_ERROR', self.hooks.rpc_result_status(self.request, error))
        plan = {'method': 'eth_getBalance', 'params': ['0x' + '1' * 40, '0x2a']}
        self.assertEqual(old.rpc_identity('synthetic', plan), self.hooks.rpc_identity('synthetic', plan))


class SyntheticRuntime(Stage1DRpcValidation):
    session = staticmethod(old.session)
    raw_risk = staticmethod(old.raw_risk)


class Stage1DRpcIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.support = support.ContextAccessR4Tests(); self.support.setUp()
        self.addCleanup(self.support.tearDown)
        self.h = header(count=3)
        self.runtime = SyntheticRuntime({42: self.h})
        self.plan = {'method': 'eth_getBlockByNumber', 'params': ['0x2a', True]}
        self.calls = []

    def access(self, transport):
        s = self.support
        result = old.RpcAccess(s.w, transport, clock=s.clock, monotonic=s.clock,
            sleep=s.clock.sleep, rng=lambda: 0.5, runtime=self.runtime)
        # This isolated synthetic fixture extends only its in-memory rate view.
        # The production permission file and inherited financial ledger are unchanged.
        original = result.permission
        result.permission = lambda: original() | {'method_cu_upper_bounds': {'eth_chainId': 10,
            'eth_getBalance': 10, 'eth_getBlockByNumber': 20}}
        return result

    def success(self, requests, bound):
        self.calls.append(requests)
        return 200, json.dumps([{'jsonrpc': '2.0', 'id': row['id'],
            'result': hydrated(self.h) if row['method'] == 'eth_getBlockByNumber' else '0x0'} for row in requests]).encode()

    def test_actual_r4_path_uses_hooks_costs20_and_caches_after_restart(self):
        before = old.readonly_snapshot(old.db_path(self.support.w))
        first = self.access(self.success).call_batch([self.plan], 'SHARED', 'full')
        second = self.access(self.success).call_batch([self.plan], 'SHARED', 'resume')
        self.assertEqual('COMPLETE', first['status']); self.assertEqual(1, len(self.calls))
        self.assertEqual(0, second['actual_operations_this_call']); self.assertTrue(second['members'][0]['cache_hit'])
        from decimal import Decimal
        self.assertEqual(Decimal('20'), Decimal(first['snapshot']['alchemy_cu']['reserved']) - Decimal(before['alchemy_cu']['reserved']))
        self.assertEqual(first['snapshot'], second['snapshot'])

    def test_partial_batch_retries_missing_member_only_and_reuses_block(self):
        balance = {'method': 'eth_getBalance', 'params': ['0x' + '3' * 40, '0x2a']}
        def transport(requests, bound):
            status, body = self.success(requests, bound)
            if len(self.calls) == 1: body = json.dumps(json.loads(body)[:1]).encode()
            return status, body
        result = self.access(transport).call_batch([self.plan, balance], 'SHARED', 'mixed')
        self.assertEqual('COMPLETE', result['status'])
        self.assertEqual([2, 1], [len(rows) for rows in self.calls])
        self.assertEqual('eth_getBalance', self.calls[1][0]['method'])

    def test_timeout_total_three_attempts_and_new_label_does_not_reset(self):
        def transport(requests, bound): self.calls.append(requests); raise TimeoutError('synthetic only')
        first = self.access(transport).call_batch([self.plan], 'SHARED', 'one')
        again = self.access(transport).call_batch([self.plan], 'SHARED', 'two')
        self.assertEqual(3, len(self.calls)); self.assertEqual('RETRIES_EXHAUSTED', first['members'][0]['status'])
        self.assertEqual(0, again['actual_operations_this_call']); self.assertEqual(first['snapshot'], again['snapshot'])

    def test_wrong_hydrated_binding_never_cached_as_success(self):
        def transport(requests, bound):
            status, body = self.success(requests, bound); rows = json.loads(body)
            rows[0]['result']['transactions'][-1]['transactionIndex'] = '0x0'
            return status, json.dumps(rows).encode()
        result = self.access(transport).call_batch([self.plan], 'SHARED', 'bad')
        self.assertEqual('PARTIAL', result['status'])
        self.assertNotEqual('SUCCESS_VALIDATED', result['members'][0]['status'])

    def test_default_r4_path_still_rejects_true_before_transport(self):
        access = self.support.access(self.success)
        with self.assertRaises(ValueError): access.call_batch([self.plan], 'SHARED', 'legacy')
        self.assertEqual([], self.calls)


if __name__ == '__main__': unittest.main()
