"""Staged current-scope/authority routing; synthetic local files only."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from collector import Scope, QUERY_WINDOW_MODE
import stage1d_legacy_rpc_import as importer_module
import test_stage1d_legacy_rpc_import as fixtures
canonical = fixtures.canonical


class ClosureScopeTests(unittest.TestCase):
    setUp = fixtures.ImporterTest.setUp
    tearDown = fixtures.ImporterTest.tearDown
    need = fixtures.ImporterTest.need
    importer = fixtures.ImporterTest.importer

    def closure(self):
        q = deepcopy(self.query)
        q.update(window_mode=QUERY_WINDOW_MODE, local_window_seconds=17)
        q.pop('scope_id');q.pop('scope_hash');q.pop('scope')
        scope = Scope.from_policy(q)
        q.update(scope_id=scope.scope_id, scope_hash=scope.scope_hash, scope=scope.freeze_dict())
        path = self.work/'private/STAGE1D_CLOSURE_BATCH.json'
        path.write_bytes(canonical({'queries':[q]}))
        return path, q

    def test_staged_module_identity(self):
        self.assertEqual(Path(importer_module.__file__).resolve(), Path(__file__).resolve().parents[1]/'src/stage1d_legacy_rpc_import.py')

    def test_active_closure_query_passes_current_need_binding_without_old_raw(self):
        path, q = self.closure()
        old = self.freeze.read_bytes()
        with patch.object(importer_module, 'active_batch_path', return_value=path):
            reader = self.importer()
            need = {**self.need(), **{k:q[k] for k in ('query_id','scope_id','scope_hash')}}
            result = reader.prepare_one(q, need)
        self.assertEqual(result['status'], 'NO_EXACT_LEGACY_REQUEST_MATCH')
        self.assertEqual(result['freeze_path'], 'private/STAGE1D_CLOSURE_BATCH.json')
        self.assertEqual(result['freeze_sha256'], hashlib.sha256(path.read_bytes()).hexdigest())
        self.assertEqual(self.freeze.read_bytes(), old)
        self.assertFalse((self.work/'raw').exists())
        self.assertFalse((self.work/'private/read_retry_r4.sqlite').exists())

    def test_historical_query_is_not_current_after_closure_adoption(self):
        path, q = self.closure()
        with patch.object(importer_module, 'active_batch_path', return_value=path):
            result = self.importer().prepare_one(self.query, self.need())
        self.assertEqual(result['status'], 'POINT_ADMISSION_QUARANTINED')
        self.assertIn('exact current frozen query', result['reason'])

    def test_current_id_with_wrong_scope_is_rejected(self):
        path, q = self.closure()
        with patch.object(importer_module, 'active_batch_path', return_value=path):
            result = self.importer().prepare_one(q, self.need())
        self.assertEqual(result['status'], 'POINT_ADMISSION_QUARANTINED')
        self.assertIn('scope identity', result['reason'])

    def test_active_path_change_requires_reopening(self):
        reader = self.importer()
        path, q = self.closure()
        with patch.object(importer_module, 'active_batch_path', return_value=path):
            result = reader.prepare_one(self.query, self.need())
        self.assertEqual(result['status'], 'POINT_ADMISSION_QUARANTINED')
        self.assertIn('Active batch freeze changed', result['reason'])

    def production_fixture(self):
        # Explicitly synthetic authority dependency; no real legacy root exists
        # in this test. Production calls use the unchanged compiled policy SHA.
        value = {'authorization_id':importer_module.AUTH, 'legacy_reuse':{'authorized':True,
                 'roots':{'raw':str(self.raw)}, 'priority_groups':['approved-test-group']}}
        path = self.work/importer_module.POLICY_PATH
        path.parent.mkdir(parents=True, exist_ok=True);path.write_bytes(canonical(value))
        return value, path

    def test_local_frozen_policy_preserves_exact_whitelist_and_rejects_mutation(self):
        policy, path = self.production_fixture()
        policy_sha = hashlib.sha256(path.read_bytes()).hexdigest()
        with patch.object(importer_module, 'POLICY_SHA', policy_sha), \
             patch.object(importer_module, 'recovery_policy', return_value=policy) as getter:
            reader = importer_module.LegacyPointImporter(self.work,self.index)
            result = reader.prepare_one(self.query,self.need())
            getter.assert_called_once_with(self.work)
            self.assertEqual(reader.raw_root,self.raw)
            self.assertEqual(reader.allowed_groups,{'approved-test-group'})
            self.assertEqual(result['status'],'NO_EXACT_LEGACY_REQUEST_MATCH')
            self.assertEqual(result['legacy_reuse_policy_sha256'],policy_sha)
            path.write_bytes(path.read_bytes()+b' ')
            changed = reader.prepare_one(self.query,self.need())
            self.assertIn('reuse policy changed', changed['reason'])

    def test_wrong_policy_sha_rejected_before_raw_lookup(self):
        policy, path = self.production_fixture()
        with patch.object(importer_module, 'recovery_policy') as getter:
            with self.assertRaisesRegex(ValueError,'SHA mismatch'):
                importer_module.LegacyPointImporter(self.work,self.index)
            getter.assert_not_called()

    def test_index_still_must_be_inside_revision_root(self):
        with self.assertRaisesRegex(ValueError,'escapes declared root'):
            importer_module.LegacyPointImporter(self.work,self.work.parent.parent/'outside-index.sqlite')


if __name__ == '__main__':
    unittest.main()
