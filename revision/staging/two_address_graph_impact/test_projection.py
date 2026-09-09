"""Only synthetic files and in-memory report projection; no active graph read."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import hashlib, json, tempfile, unittest
import project_latest_graph as p


class ProjectionTests(unittest.TestCase):
    def scratch(self):
        t = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent)
        self.addCleanup(t.cleanup); return Path(t.name)
    def event(self, eid='e1', sender=p.F621, recipient=p.WETH):
        return {'event_id': eid, 'chain_id': 'eip155:1', 'tx_hash': '0x'+'1'*64,
                'kind': 'top', 'sender': sender, 'recipient': recipient,
                'asset': 'native:eip155:1', 'amount_raw': 2**128, 'block': 1, 'timestamp': 2}
    def graph(self):
        return {'query_id': 'synthetic:q', 'status': 'COMPLETED_WITHIN_DECLARED_SCOPE',
                'candidate_events': [self.event()], 'states': [], 'stops': [],
                'unresolved_frontier': [], 'metrics': {'scope_hash': 'scope'}}
    def test_projection_skips_gap_and_shared_blob_fields(self):
        graph = self.graph(); graph.update(gaps={'templates': {'irrelevant': [None]*1000}},
                                           semantic_evidence_context={'blob': 'x'*5000})
        path = self.scratch()/'collection.json'; path.write_text(json.dumps(graph, indent=2, sort_keys=True))
        got = p.project_collection(path)
        self.assertNotIn('gaps', got); self.assertNotIn('semantic_evidence_context', got)
        self.assertEqual(got['candidate_events'][0]['amount_raw'], 2**128)
        self.assertEqual(got['candidate_events'], graph['candidate_events'])
    def test_duplicate_top_level_fields_refused(self):
        path = self.scratch()/'collection.json'
        path.write_text('{\n  "status": "A",\n  "status": "B"\n}\n')
        with self.assertRaises(ValueError): p.project_collection(path)
    def test_compact_current_writer_projection_skips_nested_punctuation(self):
        graph = self.graph(); graph['gaps'] = {'templates': [{'value': 'escaped" [ } ,', 'nested': [None, {}]}]}
        path = self.scratch()/'collection.json'; path.write_text(json.dumps(graph, separators=(',', ':'), sort_keys=True))
        self.assertEqual(p.project_collection(path), {k: v for k, v in graph.items() if k in p.FIELDS})
    def test_lock_prevents_any_current_read(self):
        work = self.scratch(); (work/'private').mkdir(); (work/'private/network_worker.lock').write_text('owned')
        with patch.object(p, 'C', work), patch.object(p, 'Reader', side_effect=AssertionError('must not read')):
            with self.assertRaises(RuntimeError): p.run(SimpleNamespace(hold_receipt=None, expect_971_action=None))
    def test_physical_dedup_ignores_alias_and_provenance(self):
        a = self.event(); b = dict(a, event_id='alias', provenance='new provider')
        self.assertEqual(p.delta([a], [a, b])['added_physical_count'], 0)
        self.assertEqual(p.delta([a], [a, b])['after_physical_count'], 1)
    def test_incoming_and_actual_membership_are_separate(self):
        graph = self.graph(); graph['candidate_events'].append(self.event('e2', '0x'+'2'*40, p.F621))
        graph['semantic_units'] = [{'unit_id': 'u', 'holder': p.F621, 'input': {'physical_event_id': 'e1'}}]
        graph['semantic_membership'] = [{'unit_id': 'u', 'output_arrival_event_id': 'virtual:out'}]
        result = p.per_address(graph, p.F621)
        self.assertEqual(result['incoming_physical_events'], 1)
        self.assertEqual(result['weth']['to_weth_event_ids_matched_to_actual_units'], ['e1'])
        self.assertEqual(result['weth']['actual_membership_count'], 1)
    def test_role_receipt_and_unknown_hold_must_both_bind(self):
        graph = self.graph(); labels = {p.F621: dict(p.ROLE), p.A971: {
            'kind': 'UNKNOWN', 'branch_action': 'USER_REQUESTED_BRANCH_HOLD', 'task_boundary_ids': [p.HOLD_ID]}}
        for addr in p.TARGETS:
            state = {'address': addr, 'arrival': {'event_id': 'arrival:'+addr}, 'asset': 'native:eip155:1', 'depth': 1}
            graph['states'].append({'state': state, 'identity': labels[addr]})
            if addr == p.F621: graph['stops'].append({'state': state, 'reason': 'PROTOCOL_BOUNDARY'})
            else:
                graph['stops'].append({'state': state, 'reason': 'USER_REQUESTED_BRANCH_HOLD'})
                graph['unresolved_frontier'].append({'state': state, 'reason': 'USER_REQUESTED_BRANCH_HOLD'})
        bindings = [{'address': s['state']['address'], 'arrival_id': s['state']['arrival']['event_id'],
                     'depth': 1, 'asset': 'native:eip155:1', 'identity': s['identity']} for s in graph['states']]
        rolehash = hashlib.sha256(p.canonical(bindings)).hexdigest()
        graph['metrics'].update(actual_state_roles_sha256=rolehash,
            label_snapshot_hash=hashlib.sha256(p.canonical(labels)).hexdigest())
        adoption = {'query_id': 'synthetic:q', 'scope_hash': 'scope', 'collection_sha256': 'graph',
                    'label_snapshot_sha256': 'labels', 'actual_state_roles_sha256': rolehash}
        args = (graph, labels, adoption, {'sha256': 'graph'}, {'sha256': 'labels'}, 'USER_REQUESTED_BRANCH_HOLD', {'sha256': 'hold'})
        self.assertTrue(p.role_assertions(*args)['passed'])
        adoption['collection_sha256'] = 'old graph'
        self.assertFalse(p.role_assertions(*args)['passed'])
    def test_reader_detects_post_read_change(self):
        root = self.scratch(); path = root/'x.json'; path.write_text('{}')
        reader = p.Reader(root); reader.read(path); path.write_text('{"changed":true}')
        self.assertFalse(reader.stable())
    def test_new_report_and_operations_output_paths_are_allowed_without_writes(self):
        root = self.scratch()
        for rel in ('reports/new_snapshot/GRAPH.json', 'operations/user_971_hold_sync/GRAPH_SYNC.json'):
            target = p.report_output(root, rel)
            self.assertEqual(target, root/rel)
            self.assertFalse(target.exists()); self.assertFalse(target.parent.exists())
    def test_output_rejects_existing_path_and_preserves_bytes(self):
        root = self.scratch(); (root/'reports').mkdir()
        prior = root/'reports/SEALED.json'; prior.write_bytes(b'SEALED')
        with self.assertRaises(ValueError): p.report_output(root, 'reports/SEALED.json')
        self.assertEqual(prior.read_bytes(), b'SEALED')
    def test_output_rejects_escapes_active_inputs_and_device_paths(self):
        root = self.scratch()
        for rel in ('code/new.json', '01_inputs/new.json', 'reports/../code/new.json',
                    '/reports/new.json', 'D:/outside.json', 'operations\\new.json',
                    'reports//new.json', 'reports/NUL.json', 'reports/new.json:stream'):
            with self.subTest(path=rel), self.assertRaises(ValueError): p.report_output(root, rel)


if __name__ == '__main__': unittest.main()
