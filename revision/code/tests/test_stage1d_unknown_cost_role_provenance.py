"""Synthetic role-only checks; no activity, production graph, DB or network."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from stage1d_unknown_cost_registry import Registry
from test_stage1d_role_adoption_evidence import fixture as role_fixture


def write(root, path, value):
    target = root/path
    target.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(value, sort_keys=True).encode()
    target.write_bytes(raw)
    return {'path': path, 'sha256': hashlib.sha256(raw).hexdigest()}


def role_only(work):
    registry = Registry.__new__(Registry)
    registry.work, registry.root = Path(work).resolve(), Path(work).resolve().parent
    registry._bytes, registry._json, registry._role_refs = {}, {}, []
    validation = registry._load_role_binding()
    return registry, validation


class RoleProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent)
        self.root = Path(self.temp.name)
        self.work = self.root/'code'

    def tearDown(self): self.temp.cleanup()

    def fixture(self):
        certificate = role_fixture(self.work, public_text=True)
        ref = write(self.work, 'private/stage1d_roles/certificate.json', certificate)
        write(self.work, 'private/stage1d_roles/CURRENT.json', {'certificates': [ref]})
        request = write(self.root, 'operations/hold_request.json', {'user_message': 'Hold this branch'})
        history = {'path': 'derived/stage1d/queries/txphish_src001/collection.json', 'sha256': '0'*64}
        write(self.work, history['path'], {'new_replayed_graph': True})
        write(self.work, 'private/stage1d_roles/USER_TASK_BOUNDARIES.json',
              {'boundaries': [{'pre_adoption_collections': [history], 'authority_request_ref': request}]})
        return request, history

    def test_historical_alias_not_read_or_in_identity_but_parent_and_authority_remain_bound(self):
        request, history = self.fixture()
        first, validation = role_only(self.work)
        self.assertEqual(validation['certificate_ids'], ['controlled-cert'])
        paths = {ref['path'] for ref in first._role_refs}
        self.assertNotIn(history['path'], paths)
        self.assertIn('../operations/hold_request.json', paths)
        self.assertIn('private/stage1d_roles/USER_TASK_BOUNDARIES.json', paths)
        self.assertIn('private/stage1d_roles/historical_code.json', paths)
        self.assertIn('private/stage1d_roles/journal.json', paths)
        write(self.work, history['path'], {'new_replayed_graph': 'next version'})
        second, _ = role_only(self.work)
        self.assertEqual(first._role_refs, second._role_refs)
        (self.root/request['path']).write_text('{}', encoding='utf-8')
        with self.assertRaises(ValueError): role_only(self.work)

    def test_real_raw_mismatch_and_unrelated_history_named_field_still_reject(self):
        self.fixture()
        path = self.work/'private/stage1d_roles/historical_code.json'
        original = path.read_bytes()
        path.write_text('{}', encoding='utf-8')
        with self.assertRaises(ValueError): role_only(self.work)
        path.write_bytes(original)
        user = self.work/'private/stage1d_roles/USER_TASK_BOUNDARIES.json'
        doc = json.loads(user.read_text())
        doc['boundaries'][0]['different_history_field'] = {
            'path': 'derived/stage1d/queries/txphish_src001/collection.json', 'sha256': '0'*64}
        user.write_text(json.dumps(doc), encoding='utf-8')
        with self.assertRaises(ValueError): role_only(self.work)


if __name__ == '__main__': unittest.main()
