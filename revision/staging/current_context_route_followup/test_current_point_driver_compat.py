"""Bounded synthetic compatibility of the unchanged prepared-only point driver."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import prepare_context_points as driver
from test_stage1d_closure_context import fixture
from stage1d_gap_sequence import serialize_gaps


class DriverCompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        self.addCleanup(self.temporary.cleanup)
        self.work = Path(self.temporary.name)
        (self.work/'private').mkdir()
        (self.work/'src').mkdir()

    def put(self, name, value):
        raw = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
        (self.work/name).write_bytes(raw)
        return {'path': name, 'sha256': hashlib.sha256(raw).hexdigest()}

    def data(self, gaps):
        query, collection, labels, material = fixture()
        query['name'] = 'txphish_src002'
        collection['gaps'] = gaps
        spec = {'schema_version': driver.SCHEMA, 'final_candidate_freeze': False,
            'source_sha256': {},
            'active_batch': self.put('private/BATCH_QUERY_FREEZE.json', {'queries': [query]}),
            'queries': {'txphish_src002': {
                'collection': self.put('collection.json', collection),
                'labels': self.put('labels.json', labels),
                'material': self.put('material.json', material)}}}
        return spec, collection, labels

    def test_compact_wire_keeps_identical_exact_point_requests(self):
        gaps = [{'reason': 'ROOT_EQUIVALENCE_UNRESOLVED_GAP', 'address': '0x'+'1'*40}]*180
        spec, _, _ = self.data(gaps)
        flat = driver.derive(self.work, spec, check_cache=False)
        spec, _, _ = self.data(serialize_gaps(gaps, force_compact=True))
        compact = driver.derive(self.work, spec, check_cache=False)
        name = 'txphish_src002'
        self.assertEqual([r['request'] for r in flat['queries'][name]['point_requests']],
                         [r['request'] for r in compact['queries'][name]['point_requests']])
        self.assertEqual(flat['queries'][name]['context_plan'], compact['queries'][name]['context_plan'])
        self.assertTrue(any(r['request']['method'] == 'eth_call' for r in compact['queries'][name]['point_requests']))
        self.assertGreater(len(compact['queries'][name]['weth_ledger_requirements']), 0)
        self.assertEqual(compact['status'], 'PREPARED_ONLY')
        self.assertIs(compact['formal_model_built'], False)
        self.assertFalse((self.work/'private/read_retry_r4.sqlite').exists())
        json.dumps(compact)

    def test_new_label_without_actual_replay_is_rejected(self):
        spec, collection, labels = self.data(serialize_gaps([], force_compact=True))
        address = collection['states'][0]['state']['address']
        labels[address] = {'kind': 'UNSUPPORTED_PROTOCOL', 'branch_action': 'UNSUPPORTED_PROTOCOL_STOP',
                           'task_boundary_ids': ['SYNTHETIC_NEW_BOUNDARY_NOT_YET_REPLAYED']}
        spec['queries']['txphish_src002']['labels'] = self.put('labels.json', labels)
        with self.assertRaisesRegex(ValueError, 'adopted role differs'):
            driver.derive(self.work, spec, check_cache=False)

    def test_changed_source_requires_new_explicit_spec(self):
        spec, _, _ = self.data([])
        (self.work/'src/new_codec.py').write_text('# synthetic changed source\n')
        with self.assertRaisesRegex(ValueError, 'source inventory changed'):
            driver.derive(self.work, spec, check_cache=False)

    def test_partial_tree_keeps_exact_top_and_failed_zero_facts(self):
        from stage1d_batch_binding_route import _events
        transactions, rows = [], []
        for n, success, amount in ((1, True, '7'), (2, False, '0')):
            tx = '0x'+format(n, '064x')
            transactions.append({'tx_hash': tx, 'sender': '0x'+'1'*40, 'recipient': '0x'+'2'*40,
                'amount_raw': amount, 'block_number': 123, 'tx_index': n, 'success': success,
                'evidence_ids': ['SYNTHETIC_NORMALIZED_TOP'], 'block_hash': '0x'+'3'*64, 'fee_raw': '2'})
            rows.append({'tx_hash': tx, 'block_time': '2024-08-23T00:00:00+00:00'})
        unproved_internal = dict(transactions[0], event_id='unproved-internal', flow_kind='internal', trace_address=[0])
        validated = {'normalized': {'transactions': transactions, 'flows': [unproved_internal]},
            'full_tree_proofs': {}, 'gaps': [{'reason': 'ROOT_EQUIVALENCE_UNRESOLVED_GAP'}]}
        events, gaps = _events(rows, validated)
        self.assertEqual(len(events), 2)
        self.assertTrue(all(e['kind'] == 'top' for e in events))
        self.assertEqual([(e['success'], e['amount_raw'], e['gas_raw']) for e in events], [(True, 7, 2), (False, 0, 2)])
        self.assertEqual(gaps, [])
        self.assertEqual(validated['full_tree_proofs'], {})
        self.assertEqual(len(validated['gaps']), 1)


if __name__ == '__main__':
    unittest.main()
