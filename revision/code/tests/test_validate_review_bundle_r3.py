"""R3 orchestration and exact-witness checks; synthetic stubs are not real data."""
import contextlib
import copy
import hashlib
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from package_review_r3 import freeze_payloads, write_manifest
from validate_review_bundle_r3 import (ValidatorR3, checked_config_r3,
                                       validate_public_tree_r3, verify_payload_r3)
from review_replay_r3 import semantic_ledger, semantic_amounts, audit_result_witnesses, check_weth_input_paths
from context_lp_r3 import run_context_document
from test_context_lp_r3 import example


class PortableValidationR3Tests(unittest.TestCase):
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

    def test_public_uses_existing_guard_without_private_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, public = self.minimal_tree(root)
            validator = ValidatorR3(public, root / 'output', 'public')
            with contextlib.redirect_stdout(io.StringIO()):
                result = validator.run()
            receipt = json.loads((root / 'output/validation_receipt.json').read_text(encoding='utf-8'))
            self.assertEqual(result, 0, receipt['commands'])
            self.assertTrue(receipt['input_tree_unchanged'])
            self.assertTrue(receipt['network_disabled'])
            self.assertTrue(receipt['credentials_removed_from_child_environment'])
            self.assertTrue(receipt['legitimate_python_children_use_inherited_guard'])
            self.assertFalse(receipt['private_real_data_replay_requested'])
            self.assertFalse(receipt['real_provider_access_verified'])

    def test_min_missing_context_config_does_not_silently_skip(self):
        with tempfile.TemporaryDirectory() as tmp:
            private, _ = self.minimal_tree(Path(tmp))
            with self.assertRaises(FileNotFoundError):
                ValidatorR3(private, Path(tmp) / 'output', 'min')
            self.assertFalse((Path(tmp) / 'output').exists())

    def test_public_config_cannot_inject_commands(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, public = self.minimal_tree(Path(tmp))
            (public / 'configs').mkdir()
            (public / 'configs/PUBLIC_VALIDATION_R3.json').write_text(json.dumps({'schema_version': 'stage1b-r3-public-validation-v1', 'commands': ['forbidden']}))
            with self.assertRaisesRegex(ValueError, 'arbitrary commands'):
                ValidatorR3(public, Path(tmp) / 'output', 'public', 'configs/PUBLIC_VALIDATION_R3.json')

    def test_private_config_rejects_missing_or_unsupported_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            for config in ({}, {'schema_version': 'stage1b-r3-private-validation-v1'}, {'commands': []}):
                with self.assertRaises(ValueError):
                    checked_config_r3(Path(tmp), config)

    def test_public_tree_rejects_private_rpc_and_account_material(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, public = self.minimal_tree(root)
            (public / 'private').mkdir()
            (public / 'private/account.json').write_text('{}')
            with self.assertRaisesRegex(ValueError, 'allowlisted'):
                validate_public_tree_r3(public)

    def test_payload_tamper_cannot_pass_by_running_green_tests(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, public = self.minimal_tree(Path(tmp))
            self.assertTrue(verify_payload_r3(public, 'public')['passed'])
            (public / 'src/run_tests.py').write_text('print("different")')
            with self.assertRaises(ValueError):
                verify_payload_r3(public, 'public')

    def test_ledger_comparison_only_excludes_source_path_relocation(self):
        original = {'model_input': {'accounts': [{'initial_actual_balance_raw': '20'}]},
                    'balance_anchors': [{'block_number': 9, 'balance_raw': '20'}],
                    'source_manifest': {'files': [{'path': 'old', 'sha256': 'bound', 'bytes': 10}]}}
        relocated = copy.deepcopy(original)
        relocated['source_manifest']['files'][0]['path'] = 'new'
        self.assertEqual(semantic_ledger(original), semantic_ledger(relocated))
        changed = copy.deepcopy(relocated)
        changed['balance_anchors'][0]['balance_raw'] = '21'
        self.assertNotEqual(semantic_ledger(original), semantic_ledger(changed))
        changed = copy.deepcopy(relocated)
        changed['source_manifest']['files'][0]['sha256'] = 'other'
        self.assertNotEqual(semantic_ledger(original), semantic_ledger(changed))

    def test_ledger_comparison_matches_json_tuple_representation_without_losing_facts(self):
        rebuilt = {'normalization_exclusions': [{'event_id': 'synthetic', 'trace_address': (0, 1),
                                                'amount_raw': '3', 'nested': {'paths': [(2, 0)]}}]}
        artifact = json.loads(json.dumps(rebuilt))
        self.assertEqual(semantic_ledger(rebuilt), semantic_ledger(artifact))
        changed = copy.deepcopy(artifact)
        changed['normalization_exclusions'][0]['amount_raw'] = '4'
        self.assertNotEqual(semantic_ledger(rebuilt), semantic_ledger(changed))
        changed = copy.deepcopy(artifact)
        changed['normalization_exclusions'][0]['trace_address'] = [1, 0]
        self.assertNotEqual(semantic_ledger(rebuilt), semantic_ledger(changed))

    def test_every_claimed_exact_witness_is_independently_checked(self):
        document = example()
        result = run_context_document(document)
        audit = audit_result_witnesses(document, result)
        self.assertTrue(audit['passed'], audit)
        self.assertGreater(audit['claimed_exact_witnesses_checked'], 0)
        tampered = copy.deepcopy(result)
        tampered['variants']['BEST_AVAILABLE_CONTEXT']['all_service_joint']['endpoints']['lower']['witness_event_source_raw']['enter'] = '1'
        self.assertFalse(audit_result_witnesses(document, tampered)['passed'])

    def test_semantic_interval_comparison_keeps_joint_and_information_changes(self):
        document = example()
        result = run_context_document(document)
        changed = copy.deepcopy(result)
        changed['variants']['BEST_AVAILABLE_CONTEXT']['all_service_joint']['lower_raw'] = '1'
        self.assertNotEqual(semantic_amounts(result), semantic_amounts(changed))
        changed = copy.deepcopy(result)
        changed['variants']['BEST_AVAILABLE_CONTEXT']['model']['variables'] = -1
        self.assertNotEqual(semantic_amounts(result), semantic_amounts(changed))

    def test_missing_endpoint_cannot_keep_an_exact_interval_claim(self):
        document = example()
        result = run_context_document(document)
        del result['variants']['BEST_AVAILABLE_CONTEXT']['all_service_joint']['endpoints']
        self.assertFalse(audit_result_witnesses(document, result)['passed'])

    def test_optional_weth_config_has_fixed_relative_inputs_no_commands(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'inputs').mkdir()
            for name in ('context.json', 'graph.json', 'lp.json', 'ledger.json', 'amounts.json', 'weth.json', 'inputs/INPUT_MANIFEST.json'):
                (root / name).write_text('{}', encoding='utf-8')
            config = {'schema_version': 'stage1b-r3-private-validation-v1', 'context_manifest': 'context.json',
                'r2_baseline': [{'name': name, 'graph': 'graph.json', 'expected_lp': 'lp.json'} for name in ('atomic_simple_transfer', 'harmony_high_branch')],
                'expected_context': [{'name': name, 'ledger': 'ledger.json', 'amounts': 'amounts.json'} for name in ('atomic_simple_transfer', 'harmony_high_branch')],
                'weth': {'manifest': 'inputs/INPUT_MANIFEST.json', 'expected_result': 'weth.json'}}
            self.assertEqual(checked_config_r3(root, config), config)
            changed = copy.deepcopy(config)
            changed['weth']['command'] = 'forbidden'
            with self.assertRaises(ValueError):
                checked_config_r3(root, changed)
            changed = copy.deepcopy(config)
            changed['weth']['manifest'] = '../INPUT_MANIFEST.json'
            with self.assertRaises(ValueError):
                checked_config_r3(root, changed)

    def test_weth_replay_manifest_tamper_is_rejected_before_adapter(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'inputs').mkdir()
            payload = root / 'inputs/payload.json'
            payload.write_bytes(b'{}')
            manifest = root / 'inputs/INPUT_MANIFEST.json'
            manifest.write_text(json.dumps({'schema_version': 'stage1b-r3-weth-replay-input-v1',
                'files': {'synthetic': {'path': 'payload.json', 'bytes': 2, 'sha256': hashlib.sha256(b'{}').hexdigest()}}}), encoding='utf-8')
            self.assertTrue(check_weth_input_paths(root, manifest))
            payload.write_bytes(b'[]')
            with self.assertRaisesRegex(ValueError, 'hash or length'):
                check_weth_input_paths(root, manifest)


if __name__ == '__main__':
    unittest.main()
