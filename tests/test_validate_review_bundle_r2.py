"""Synthetic R2 orchestration tests; stub LP receipts are not scientific proof."""
import contextlib
import copy
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from package_review_r2 import freeze_payloads, write_manifest
from review_replay_r2 import checked_config, semantic_collection_r2, semantic_sources
from validate_review_bundle_r2 import ValidatorR2, verify_single_tree_payload


class PortableValidationR2Tests(unittest.TestCase):
    def minimal_tree(self, root):
        public = root / 'public'
        (public / 'src').mkdir(parents=True)
        (public / 'fixtures/controlled').mkdir(parents=True)
        (public / 'src/run_tests.py').write_text(
            "import argparse,json,pathlib\np=argparse.ArgumentParser();p.add_argument('--output');a=p.parse_args();pathlib.Path(a.output).write_text(json.dumps({'success':True,'tests_run':3,'passed':3,'failed':0,'errors':0,'skipped':0,'network_attempts':[]}))\n", encoding='utf-8')
        (public / 'src/lp_run.py').write_text(
            "import argparse,json,pathlib\np=argparse.ArgumentParser();p.add_argument('--fixtures');p.add_argument('--output');a=p.parse_args();o=pathlib.Path(a.output);o.mkdir(parents=True);(o/'lp_verification_results.json').write_text(json.dumps({'scenario_count':12,'objective_comparisons':32,'all_passed':True}))\n", encoding='utf-8')
        private = root / 'private'
        shutil.copytree(public, private)
        freeze_payloads(private, public)
        write_manifest(public)
        write_manifest(private)
        return private, public

    def test_public_runs_shared_guard_synthetic_checks_without_private_reference_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, public = self.minimal_tree(root)
            before = {path.relative_to(public).as_posix(): path.read_bytes() for path in public.rglob('*') if path.is_file()}
            validator = ValidatorR2(public, root / 'out', 'public')
            with contextlib.redirect_stdout(io.StringIO()):
                code = validator.run()
            receipt = json.loads((root / 'out/validation_receipt.json').read_text(encoding='utf-8'))
            self.assertEqual(code, 0, receipt['commands'])
            self.assertFalse(receipt['private_real_data_replay_requested'])
            self.assertFalse(receipt['unchanged_full_reference_tables_required'])
            self.assertTrue(receipt['network_disabled'])
            self.assertTrue(receipt['input_tree_unchanged'])
            self.assertTrue(receipt['legitimate_python_children_use_inherited_guard'])
            self.assertEqual(len([row for row in receipt['commands'] if row['status'] == 'SKIP']), 1)
            after = {path.relative_to(public).as_posix(): path.read_bytes() for path in public.rglob('*') if path.is_file()}
            self.assertEqual(before, after)

    def test_min_missing_config_fails_instead_of_skipping_real_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            private, _ = self.minimal_tree(root)
            with self.assertRaises(FileNotFoundError):
                ValidatorR2(private, root / 'out', 'min')
            self.assertFalse((root / 'out').exists())

    def test_public_config_cannot_inject_private_paths_or_commands(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, public = self.minimal_tree(root)
            (public / 'configs').mkdir()
            path = public / 'configs/PUBLIC_VALIDATION_R2.json'
            path.write_text(json.dumps({'schema_version': 'stage1b-r2-public-validation-v1', 'commands': ['forbidden']}), encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'cannot request'):
                ValidatorR2(public, root / 'out', 'public', 'configs/PUBLIC_VALIDATION_R2.json')

    def test_private_manifest_rejects_unsupported_or_missing_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            for config in ({}, {'commands': []}, {'schema_version': 'stage1b-r2-private-validation-v1'}):
                with self.assertRaises(ValueError):
                    checked_config(Path(tmp), config)

    def test_individual_tree_mapping_verification_catches_tampered_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            private, public = self.minimal_tree(Path(tmp))
            self.assertTrue(verify_single_tree_payload(private, 'min')['passed'])
            self.assertTrue(verify_single_tree_payload(public, 'public')['passed'])
            (public / 'src/run_tests.py').write_text('changed', encoding='utf-8')
            with self.assertRaises(ValueError):
                verify_single_tree_payload(public, 'public')

    def test_semantic_comparison_keeps_context_frontier_and_coverage_source_changes(self):
        original = {'query_id': 'synthetic', 'status': 'PARTIAL',
                    'candidate_events': [{'event_id': 'seed', 'amount_raw': 10, 'provenance': 'old'}],
                    'context_events': [{'event_id': 'gas', 'amount_raw': 3}],
                    'states': [{'depth': 1}], 'stops': [], 'unresolved_frontier': [{'reason': 'MISSING'}],
                    'coverage': [{'complete': False, 'response_sha256': 'old', 'raw_path': 'old/path'}], 'gaps': []}
        relocated = copy.deepcopy(original)
        relocated['candidate_events'][0]['provenance'] = 'new portable location'
        relocated['coverage'][0]['raw_path'] = 'new/path'
        self.assertEqual(semantic_collection_r2(original), semantic_collection_r2(relocated))
        for key, field, replacement in [('context_events', 'amount_raw', 4), ('states', 'depth', 2),
                                        ('unresolved_frontier', 'reason', 'REMOVED'), ('coverage', 'complete', True),
                                        ('coverage', 'response_sha256', 'other')]:
            changed = copy.deepcopy(original)
            changed[key][0][field] = replacement
            self.assertNotEqual(semantic_collection_r2(original), semantic_collection_r2(changed))

    def test_source_comparison_keeps_seed_identity_execution_and_page_hashes(self):
        source = [{'query': 'synthetic', 'seed': {'seed_event_id': 'seed', 'seed_raw_evidence_sha256': 'bound'},
                   'used_jobs': [{'execution_id': 'one', 'job_path': 'old', 'pages': [{'response_sha256': 'bound'}]}]}]
        relocated = copy.deepcopy(source)
        relocated[0]['used_jobs'][0]['job_path'] = 'new'
        self.assertEqual(semantic_sources(source), semantic_sources(relocated))
        changed = copy.deepcopy(source)
        changed[0]['used_jobs'][0]['pages'][0]['response_sha256'] = 'wrong'
        self.assertNotEqual(semantic_sources(source), semantic_sources(changed))
        changed = copy.deepcopy(source)
        changed[0]['seed']['seed_raw_evidence_sha256'] = 'wrong'
        self.assertNotEqual(semantic_sources(source), semantic_sources(changed))


if __name__ == '__main__':
    unittest.main()
