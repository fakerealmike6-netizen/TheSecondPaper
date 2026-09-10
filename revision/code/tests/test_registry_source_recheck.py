"""End-of-operation byte integrity without duplicate global reconstruction."""
from pathlib import Path
import tempfile,unittest
from unittest.mock import patch
from test_cost_request_guard import Fixture,write
from stage1d_unknown_cost_registry import Registry
import stage1d_cost_request_guard as guard

class SourceRecheckTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.work=Path(self.temp.name)/'code';self.work.mkdir();self.f=Fixture(self.work)
    def tearDown(self):self.temp.cleanup()
    def test_actual_guard_builds_one_registry_and_rehashes_all_inputs(self):
        entry=self.f.save()
        with patch('stage1d_unknown_cost_registry.Registry',wraps=Registry) as factory:
            result=guard.validate_candidate_entry(self.work,self.f.q,entry)
        self.assertEqual(factory.call_count,1)
        self.assertGreaterEqual(result['source_recheck']['files_rehashed'],3)
        self.assertFalse(result['source_recheck']['global_observation_view_rebuilt'])
    def test_original_change_during_actual_guard_is_rejected(self):
        entry=self.f.save();original=guard._allowed
        def changed(*args):
            value=original(*args);p=self.work/self.f.initial_ref['path'];p.write_bytes(p.read_bytes()+b' ');return value
        with patch.object(guard,'_allowed',side_effect=changed),self.assertRaisesRegex(ValueError,'original bytes changed'):
            guard.validate_candidate_entry(self.work,self.f.q,entry)
    def test_new_label_file_during_actual_guard_is_rejected(self):
        entry=self.f.save();original=guard._allowed
        def changed(*args):
            value=original(*args);write(self.work,'derived/stage1d/labels/new.json',{});return value
        with patch.object(guard,'_allowed',side_effect=changed),self.assertRaisesRegex(ValueError,'inventory changed'):
            guard.validate_candidate_entry(self.work,self.f.q,entry)
    def test_optional_role_file_added_is_rejected(self):
        registry=Registry(self.work);write(self.work,'private/stage1d_roles/AUTHORITIES.json',{})
        with self.assertRaisesRegex(ValueError,'inventory changed'):registry.assert_source_snapshot_unchanged()
    def test_missing_original_and_unregistered_change_are_distinguished(self):
        registry=Registry(self.work);write(self.work,'unrelated_report.json',{})
        self.assertTrue(registry.assert_source_snapshot_unchanged())
        (self.work/self.f.initial_ref['path']).unlink()
        with self.assertRaises((ValueError,FileNotFoundError)):registry.assert_source_snapshot_unchanged()

if __name__=='__main__':unittest.main()
