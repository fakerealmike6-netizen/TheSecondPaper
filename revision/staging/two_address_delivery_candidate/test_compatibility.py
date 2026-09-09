"""Only the three post-delivery source compatibility boundaries; staging data."""
import json
import unittest
from test_delivery import DeliveryChecks, save, reference
from build_delivery import TARGETS, build, sha, snapshot_projection, validate_impact


class CompatibilityChecks(unittest.TestCase):
    setUp = DeliveryChecks.setUp
    tearDown = DeliveryChecks.tearDown
    fixture = DeliveryChecks.fixture

    def private_receipt(self):
        return self.c / 'private/stage1d_roles/adoptions/f621_official_scope_stop_v1/ADOPTION_RECEIPT.json'

    def test_saved_semantic_units_accept_integer_zero_and_list_counts(self):
        snap, _ = self.fixture()
        snap['queries'][0]['semantic_units'] = 0
        snap['queries'][1]['semantic_units'] = [{'unit_id': 'one'}, {'unit_id': 'two'}]
        projected = snapshot_projection(snap)
        self.assertEqual([q['semantic_unit_count_in_saved_graph'] for q in projected['queries']], [0, 2])
        snap['queries'][0]['semantic_units'] = 2
        self.assertEqual(snapshot_projection(snap)['queries'][0]['semantic_unit_count_in_saved_graph'], 2)

    def test_invalid_integer_semantic_count_stays_rejected(self):
        snap, _ = self.fixture()
        for value in (-1, True):
            snap['queries'][0]['semantic_units'] = value
            with self.assertRaises((ValueError, TypeError)):
                snapshot_projection(snap)

    def test_equal_receipt_objects_with_different_bytes_bind_private_original(self):
        self.fixture()
        operations = self.r / 'operations/F621_OFFICIAL_ROLE_ADOPTION.json'
        obj = json.loads(operations.read_text(encoding='utf-8'))
        original = self.private_receipt()
        original.write_text(json.dumps(obj, separators=(',', ':'), sort_keys=False), encoding='utf-8')
        self.assertNotEqual(sha(original), sha(operations))
        impact_path = self.t / 'inputs/LATEST_GRAPH_IMPACT.json'
        impact = json.loads(impact_path.read_text(encoding='utf-8'))
        impact['adoption_receipt_sha256'] = sha(original)
        save(impact_path, impact)
        output = self.t / 'delivery/files'
        build(self.r, self.t, output)
        self.assertEqual(sha(output / 'evidence/F621_OFFICIAL_ROLE_ADOPTION.json'), sha(original))
        manifest = json.loads((output / 'SOURCE_MANIFEST.json').read_text(encoding='utf-8'))
        copied = [r for r in manifest['copied_originals'] if r['bundle_path'] == 'evidence/F621_OFFICIAL_ROLE_ADOPTION.json']
        self.assertEqual(len(copied), 1)
        self.assertEqual((self.r / copied[0]['original_path']).resolve(), original.resolve())

    def test_private_operations_object_conflict_is_not_format_compatibility(self):
        self.fixture()
        operations = self.r / 'operations/F621_OFFICIAL_ROLE_ADOPTION.json'
        obj = json.loads(operations.read_text(encoding='utf-8'))
        obj['address'] = TARGETS[0]
        save(self.private_receipt(), obj)
        with self.assertRaisesRegex(ValueError, 'receipts disagree'):
            build(self.r, self.t, self.t / 'delivery/files')

    def test_before_hold_snapshot_allows_later_adoption_without_sync_claim(self):
        _, scopes = self.fixture()
        impact_path = self.t / 'inputs/LATEST_GRAPH_IMPACT.json'
        impact = json.loads(impact_path.read_text(encoding='utf-8'))
        impact['status'] = 'ACTUAL_REPLAY_COMPLETE_BEFORE_971_HOLD'
        impact['user_971_hold_adoption_sha256'] = None
        impact['user_requested_971_hold']['synchronization_status'] = 'REPLAY_PENDING'
        impact['semantic_replay_status'] = 'NOT_APPLICABLE_AFTER_ROLE_STOP'
        for query in scopes['queries']:
            graph = save(self.c / (query['name'] + '.json'), {'synthetic_before_hold_graph': True})
            impact['query_results'].append({'query_name': query['name'], 'query_id': query['query_id'],
                'scope_hash': query['scope_hash'], 'after_collection': reference(graph, self.r), 'addresses': {}})
        save(impact_path, impact)
        hold = self.r / 'operations/USER_971_BRANCH_HOLD_ADOPTION.json'
        validated = validate_impact(impact, self.r, impact['prior_snapshot_sha256'],
                                    impact['adoption_receipt_sha256'], scopes, sha(hold))
        self.assertEqual(validated['status'], 'ACTUAL_REPLAY_COMPLETE_BEFORE_971_HOLD')
        output = self.t / 'delivery/files'; build(self.r, self.t, output)
        result = json.loads((output / 'TWO_ADDRESS_IDENTITY_RESULTS.json').read_text(encoding='utf-8'))
        self.assertEqual(result['user_requested_971_hold']['adoption_sha256'], sha(hold))
        self.assertEqual(result['user_requested_971_hold']['graph_synchronization_status'], 'REPLAY_PENDING')
        self.assertEqual(result['graph_role_semantic_sync_status'], 'F621_REPLAY_COMPLETE_971_PRODUCTION_SYNC_PENDING')
        self.assertFalse(result['main_stage_completion_claimed'])
        impact['status'] = 'ACTUAL_REPLAY_COMPLETE'
        with self.assertRaisesRegex(ValueError, 'actual 971 user-hold adoption'):
            validate_impact(impact, self.r, impact['prior_snapshot_sha256'],
                            impact['adoption_receipt_sha256'], scopes, sha(hold))


if __name__ == '__main__': unittest.main()
