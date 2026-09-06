"""Offline RPC controls. All identities/provider payloads below are synthetic."""
import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE / 'src'))
from budget import Ledger
from budget_r1 import RevisionLedger
from rpc_context_r1 import RpcContextClient, RAW_CAP, MAX_RESPONSE_BYTES, NoRedirect, encoded, validate_plan
from weth_evidence import load_evidence_context

ADDRESS = '0x' + 'a' * 40
TX = '0x' + 'b' * 64


class RpcContextR1Tests(unittest.TestCase):
    def setUp(self):
        parent = BASE / '.testtmp'; parent.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=parent)
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        (self.work / 'private').mkdir()
        old = Ledger(self.work / 'legacy.sqlite')
        old.confirm('rpc_operations', 500, 'SYNTHETIC cumulative allowance')
        old.reserve('old', 'publicnode', 'SYNTHETIC inherited usage', {'rpc_operations': 4})
        old.settle('old', {'rpc_operations': 4})
        self.ledger = RevisionLedger(self.work / 'private/shared_budget_r1.sqlite')
        self.ledger.initialize(self.work / 'legacy.sqlite', {}, {'kind': 'SYNTHETIC_TEST'})

    def review(self, method='eth_getCode', params=None, cumulative=100):
        return {'schema_version': 'rpc-resource-review-1', 'review_id': 'SYNTHETIC-offline-resource-review',
                'approved': True, 'offline_repair_gate_passed': True,
                'cumulative_raw_bytes': cumulative, 'cumulative_raw_cap_bytes': RAW_CAP,
                'method': method, 'params': params or [ADDRESS, '0x100']}

    def good(self, request, limit):
        self.assertEqual(self.ledger.snapshot()['rpc_operations']['reserved'], '1')
        with self.ledger.connection() as db:
            self.assertEqual(db.execute('SELECT status FROM rpc_context_intents').fetchone()[0], 'DISPATCH_INTENT')
        return 200, encoded({'jsonrpc': '2.0', 'id': request['id'], 'result': '0x6000'})

    def test_success_binds_request_response_and_preserves_four_prior_operations(self):
        client = RpcContextClient(self.work, transport=self.good)
        result = client.call('eth_getCode', [ADDRESS, '0x100'], self.review())
        self.assertEqual(result['status'], 'SUCCESS_VALIDATED')
        self.assertEqual(self.ledger.snapshot()['rpc_operations']['actual'], '5')
        self.assertEqual(self.ledger.snapshot()['rpc_operations']['remaining'], '495')
        self.assertEqual(result['evidence_kind'], 'SYNTHETIC_TRANSPORT')
        self.assertIsNone(result['monetary_cost'])
        envelope = json.loads((self.work / result['members'][0]['artifact_path']).read_bytes())
        self.assertEqual(envelope['response']['id'], envelope['request']['id'])
        self.assertEqual(envelope['request']['params'], [ADDRESS, '0x100'])
        with self.assertRaisesRegex(RuntimeError, 'already'):
            client.call('eth_getCode', [ADDRESS, '0x100'], self.review(cumulative=1000))

    def test_injected_transport_evidence_cannot_enter_real_catalogue(self):
        result = RpcContextClient(self.work, transport=self.good).call('eth_getCode', [ADDRESS, '0x100'], self.review())
        catalogue = {'schema_version': 'weth-acquisition-catalogue-1', 'evidence_kind': 'REAL_CHAIN',
                     'acquisition_run_id': 'test', 'reviewer_record_id': 'test',
                     'approved_provider_hosts': ['ethereum-rpc.publicnode.com'],
                     'records': {'code': result['members'][0]}}
        (self.work / 'catalogue.json').write_bytes(encoded(catalogue))
        (self.work / 'bundle.json').write_bytes(encoded({'records': {'historical_code': 'code'}}))
        with self.assertRaisesRegex(ValueError, 'synthetic or unsuccessful'):
            load_evidence_context(self.work / 'bundle.json', self.work / 'catalogue.json')

    def test_timeout_counts_attempt_and_never_retries(self):
        calls = []
        def fail(request, limit):
            calls.append(request)
            raise TimeoutError('SYNTHETIC private exception text must not be saved')
        client = RpcContextClient(self.work, transport=fail)
        result = client.call('eth_getCode', [ADDRESS, '0x100'], self.review())
        self.assertEqual(result['status'], 'TRANSPORT_ERROR_NO_RETRY')
        self.assertEqual(result['error_class'], 'TimeoutError')
        self.assertEqual(self.ledger.snapshot()['rpc_operations']['actual'], '5')
        self.assertNotIn('private exception', json.dumps(result))
        with self.assertRaises(RuntimeError):
            client.call('eth_getCode', [ADDRESS, '0x100'], self.review())
        self.assertEqual(len(calls), 1)
        self.assertEqual(result['raw_bytes_accounted_as_resource_risk'], MAX_RESPONSE_BYTES + 1)
        self.assertEqual(result['raw_accounting_status'], 'UPPER_BOUND_RESERVED_NOT_ACTUAL')

    def test_http_rejection_saved_and_not_successful_evidence(self):
        result = RpcContextClient(self.work, transport=lambda request, limit: (403, b'Forbidden')).call('eth_getCode', [ADDRESS, '0x100'], self.review())
        self.assertEqual(result['status'], 'HTTP_ERROR')
        self.assertEqual(result['http_status'], 403)
        self.assertIsNone(result['members'][0]['payload_sha256'])
        self.assertEqual((self.work / result['raw_path']).read_bytes(), b'Forbidden')
        self.assertIsNone(NoRedirect().redirect_request(None, None, None, None, None, None))

    def test_oversize_retains_truncated_prefix_and_never_claims_complete(self):
        result = RpcContextClient(self.work, transport=lambda request, limit: (200, b'x' * (limit + 1)),
                                  max_response_bytes=128).call('eth_getCode', [ADDRESS, '0x100'], self.review())
        self.assertEqual(result['status'], 'RESPONSE_TOO_LARGE')
        self.assertTrue(result['truncated'])
        self.assertFalse(result['response_complete'])
        self.assertEqual(result['raw_bytes_saved'], 128)
        self.assertEqual(result['observed_body_bytes_lower_bound'], 129)
        self.assertIsNone(result['members'][0]['payload_sha256'])

    def test_invalid_id_error_or_null_not_success(self):
        for response in ({'jsonrpc': '2.0', 'id': 'wrong', 'result': '0x6000'},
                         {'jsonrpc': '2.0', 'error': {'code': -1, 'message': 'synthetic denied'}},
                         {'jsonrpc': '2.0', 'result': None}):
            params = [ADDRESS, hex(0x200 + len(list((self.work / 'raw').rglob('receipt.json'))) if (self.work / 'raw').exists() else 0x200)]
            def transport(request, limit):
                value = copy.deepcopy(response)
                value.setdefault('id', request['id'])
                return 200, encoded(value)
            result = RpcContextClient(self.work, transport=transport).call('eth_getCode', params, self.review(params=params, cumulative=10000 + int(params[1], 16) * 1000))
            self.assertNotEqual(result['status'], 'SUCCESS_VALIDATED')

    def test_batch_each_member_counts_and_reordering_cannot_retry(self):
        plans = [{'method': 'eth_getCode', 'params': [ADDRESS, '0x100']},
                 {'method': 'eth_getBalance', 'params': [ADDRESS, '0x100']}]
        review = self.review(); review['requests'] = plans
        def transport(requests, limit):
            self.assertEqual(self.ledger.snapshot()['rpc_operations']['reserved'], '2')
            return 200, encoded([{'jsonrpc': '2.0', 'id': r['id'], 'result': '0x6000' if r['method'] == 'eth_getCode' else '0x0'} for r in reversed(requests)])
        client = RpcContextClient(self.work, transport=transport)
        result = client.call_batch(plans, review)
        self.assertEqual(result['status'], 'SUCCESS_VALIDATED')
        self.assertEqual(result['rpc_operations_attempted'], 2)
        self.assertEqual(self.ledger.snapshot()['rpc_operations']['actual'], '6')
        with self.assertRaises(RuntimeError):
            client.call('eth_getCode', [ADDRESS, '0x100'], self.review(cumulative=10000))

    def test_unknown_allowance_blocks_before_transport(self):
        with self.ledger.connection() as db:
            db.execute("UPDATE limits SET available=NULL WHERE unit='rpc_operations'")
        called = []
        client = RpcContextClient(self.work, transport=lambda *args: called.append(True))
        with self.assertRaisesRegex(RuntimeError, 'Allowance unknown'):
            client.call('eth_getCode', [ADDRESS, '0x100'], self.review())
        self.assertEqual(called, [])

    def test_restart_cannot_erase_prior_raw_or_recase_a_request(self):
        client = RpcContextClient(self.work, transport=self.good)
        result = client.call('eth_getCode', [ADDRESS, '0x100'], self.review())
        restarted = RpcContextClient(self.work, transport=lambda *args: self.fail('must not dispatch'))
        with self.assertRaisesRegex(RuntimeError, 'already'):
            restarted.call('eth_getCode', [ADDRESS.upper().replace('0X', '0x'), '0x100'], self.review(cumulative=10000))
        with self.assertRaisesRegex(RuntimeError, 'omits prior'):
            restarted.call('eth_getCode', [ADDRESS, '0x101'], self.review(params=[ADDRESS, '0x101'], cumulative=100))

    def test_resource_review_exact_plan_and_inherited_raw_cap_required(self):
        client = RpcContextClient(self.work, transport=lambda *args: self.fail('must not dispatch'))
        for changes in ({'approved': False}, {'offline_repair_gate_passed': False},
                        {'cumulative_raw_bytes': RAW_CAP - MAX_RESPONSE_BYTES + 1},
                        {'params': [ADDRESS, '0x101']}, {'cumulative_raw_cap_bytes': RAW_CAP * 2}):
            with self.assertRaises(RuntimeError):
                client.call('eth_getCode', [ADDRESS, '0x100'], self.review() | changes)

    def test_fixed_historical_and_call_tracer_scope(self):
        trace_params = [TX, {'tracer': 'callTracer', 'tracerConfig': {'withLog': True}}]
        self.assertEqual(validate_plan('debug_traceTransaction', trace_params)['params'], trace_params)
        for method, params in [('eth_getCode', [ADDRESS, 'latest']),
                               ('debug_traceTransaction', [TX, {}]),
                               ('debug_traceTransaction', [TX, {'tracer': 'prestateTracer'}]),
                               ('eth_sendRawTransaction', ['0x1234'])]:
            with self.assertRaises(ValueError):
                validate_plan(method, params)

    def test_network_read_bound_and_timeout_without_real_network(self):
        from rpc_context_r1 import http_transport
        class Response(io.BytesIO):
            status = 200
        class Opener:
            def open(self, request, timeout):
                self_request['url'] = request.full_url
                self_request['timeout'] = timeout
                return Response(b'a' * 100)
        self_request = {}
        with patch('rpc_context_r1.urllib.request.build_opener', return_value=Opener()):
            status, raw = http_transport({'jsonrpc': '2.0'}, 16)
        self.assertEqual(len(raw), 17)
        self.assertEqual(self_request['timeout'], 30)
        self.assertEqual(self_request['url'], 'https://ethereum-rpc.publicnode.com')


if __name__ == '__main__':
    unittest.main()
