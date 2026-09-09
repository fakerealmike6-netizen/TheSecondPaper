"""R3 extracted-bundle validation through the unchanged strict R1 guard.

Public bundles run all synthetic tests and controlled LP checks. Private MIN
bundles additionally replay the evidence-to-ledger-to-LP pipeline and the exact
two R2 baseline graphs. No full external label/reference library is required.
"""
import argparse
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import platform
import json

from package_review_r3 import (EQUIVALENCE, MANIFEST, payload_files,
                               json_bytes, validate_public_tree, verify_manifest)
from validate_review_bundle_r1 import Validator, input_path, read, write, tree_hashes

PILOTS = {'atomic_simple_transfer', 'harmony_high_branch'}


def validate_public_tree_r3(tree):
    return validate_public_tree(tree)


def verify_payload_r3(tree, kind):
    tree = Path(tree).resolve()
    manifest = verify_manifest(tree)
    mapping = read(input_path(tree, EQUIVALENCE))
    if mapping.get('schema') != 'stage1b-r3-payload-equivalence-dag-v1':
        raise ValueError('A non-circular frozen payload equivalence mapping is required')
    payload = payload_files(tree)
    rows = mapping['payload_rows']
    if kind == 'public':
        validate_public_tree_r3(tree)
        if len(rows) != len({row['public_path'] for row in rows}) or set(payload) != {row['public_path'] for row in rows}:
            raise ValueError('Public payload mapping membership mismatch')
        for row in rows:
            path = input_path(tree, row['public_path'])
            if path.stat().st_size != row['public_bytes'] or hashlib.sha256(path.read_bytes()).hexdigest() != row['public_sha256']:
                raise ValueError('Public payload mapping hash mismatch')
    else:
        inventory = [{'path': name, 'bytes': path.stat().st_size, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
                     for name, path in sorted(payload.items())]
        if len(inventory) != mapping['local_payload_count'] or hashlib.sha256(json_bytes(inventory)).hexdigest() != mapping['local_payload_inventory_sha256']:
            raise ValueError('Private payload mapping inventory commitment differs')
        for row in rows:
            if row['local_path'] is None:
                continue
            path = input_path(tree, row['local_path'])
            if path.stat().st_size != row['local_bytes'] or hashlib.sha256(path.read_bytes()).hexdigest() != row['local_sha256']:
                raise ValueError('Private payload mapping hash mismatch')
    return {'passed': True, 'manifest': manifest, 'payload_files': len(payload),
            'counterpart_tree_independently_verified': False}


def checked_config_r3(tree, config):
    required = {'schema_version', 'context_manifest', 'r2_baseline', 'expected_context'}
    if not isinstance(config, dict) or set(config) - required - {'weth'} or required - set(config) or config['schema_version'] != 'stage1b-r3-private-validation-v1':
        raise ValueError('R3 private config must contain only fixed documented pipeline inputs')
    input_path(tree, config['context_manifest'])
    for group, keys in (('r2_baseline', {'name', 'graph', 'expected_lp'}),
                        ('expected_context', {'name', 'ledger', 'amounts'})):
        rows = config[group]
        if not isinstance(rows, list) or len(rows) != 2 or {row.get('name') for row in rows if isinstance(row, dict)} != PILOTS:
            raise ValueError('Exactly the two frozen R3 pilots are required')
        for row in rows:
            if set(row) != keys:
                raise ValueError('Unknown replay fields or arbitrary commands are forbidden')
            for key in keys - {'name'}:
                input_path(tree, row[key])
    if 'weth' in config:
        weth = config['weth']
        if not isinstance(weth, dict) or set(weth) != {'manifest', 'expected_result'}:
            raise ValueError('WETH can only select the fixed relative manifest and expected result')
        manifest = input_path(tree, weth['manifest'])
        input_path(tree, weth['expected_result'])
        if manifest.name != 'INPUT_MANIFEST.json':
            raise ValueError('Fixed WETH adapter requires INPUT_MANIFEST.json')
    return config


class ValidatorR3(Validator):
    def __init__(self, tree, output, kind, manifest_path=None):
        if kind not in ('min', 'public'):
            raise ValueError('Explicit R3 tree kind required')
        tree = Path(tree).resolve()
        self.manifest_path = manifest_path or ('configs/PRIVATE_VALIDATION_R3.json' if kind == 'min' else None)
        self.r3_config = None
        if kind == 'min':
            self.r3_config = checked_config_r3(tree, read(input_path(tree, self.manifest_path)))
        elif manifest_path:
            value = read(input_path(tree, manifest_path))
            if value != {'schema_version': 'stage1b-r3-public-validation-v1'}:
                raise ValueError('Public config cannot request private replay or arbitrary commands')
        # Do not replace the repaired credential, subprocess, posix_spawn,
        # network or filesystem guard with a simpler R3 implementation.
        super().__init__(tree, output, kind)

    def private_checks(self):
        if self.command('r3_evidence_to_ledger_and_lp', 'src/review_replay_r3.py',
                        ['--tree', self.tree, '--config', self.manifest_path, '--output', self.out / 'r3_context_replay']):
            receipt = read(self.out / 'r3_context_replay/R3_REPLAY_COMPARISON.json')
            self.check('r3_real_context_reproduction_receipt', receipt.get('passed') is True
                       and len(receipt.get('context_comparisons', [])) == 2
                       and len(receipt.get('r2_baseline_comparisons', [])) == 2
                       and receipt.get('network_requests') == 0,
                       context_comparisons=receipt.get('context_comparisons'),
                       r2_baseline_comparisons=receipt.get('r2_baseline_comparisons'),
                       proof_audits=receipt.get('proof_audits'),
                       weth_comparison=receipt.get('weth_comparison'),
                       real_provider_access_verified=False)
            if self.r3_config.get('weth'):
                weth = receipt.get('weth_comparison') or {}
                self.check('r3_weth_actual_input_replay_receipt', weth.get('passed') is True,
                           **{key: value for key, value in weth.items() if key != 'passed'})

    def run(self):
        self.bounded('r3_package_integrity', lambda: self.check('r3_manifest_and_payload_binding', True,
                     **{key: value for key, value in verify_payload_r3(self.tree, self.kind).items() if key != 'passed'}))
        self.bounded('public_checks', self.public_checks)
        if self.kind == 'min':
            self.bounded('r3_private_checks', self.private_checks)
        else:
            self.skipped('r3_private_evidence_replay', 'PUBLIC_EXCLUDES_PRIVATE_CHAIN_EVIDENCE; no real private replay is claimed')
        after = tree_hashes(self.tree)
        self.check('frozen_tree_unchanged', self.before == after, files=len(self.before),
                   changed=sorted(path for path in set(self.before) | set(after) if self.before.get(path) != after.get(path)))
        required = ['r3_manifest_and_payload_binding', 'unit_tests', 'unit_test_receipt', 'controlled_lp', 'controlled_oracle_receipt', 'frozen_tree_unchanged']
        if self.kind == 'min':
            required += ['r3_evidence_to_ledger_and_lp', 'r3_real_context_reproduction_receipt']
            if self.r3_config.get('weth'):
                required += ['r3_weth_actual_input_replay_receipt']
        failures = self.validation_failures(required)
        receipt = {'schema_version': 'stage1b-r3-portable-validation-v1',
                   'created_at_utc': datetime.now(timezone.utc).isoformat(),
                   'platform': platform.platform(), 'python': platform.python_version(),
                   'status': 'PASS' if not failures else 'FAIL', 'tree_kind': self.kind,
                   'network_disabled': True, 'credentials_removed_from_child_environment': True,
                   'legitimate_python_children_use_inherited_guard': True,
                   'writes_restricted_to_fresh_output': True, 'input_tree_unchanged': self.before == after,
                   'private_real_data_replay_requested': self.kind == 'min',
                   'unchanged_full_reference_tables_required': False,
                   'real_provider_access_verified': False, 'external_acceptance_status': 'PENDING_REVIEW',
                   'commands': self.commands, 'input_files': self.before,
                   'guard_source_sha256': hashlib.sha256((self.bootstrap / 'sitecustomize.py').read_bytes()).hexdigest(),
                   'validation_output': str(self.out)}
        write(self.out / 'validation_receipt.json', receipt)
        print(json.dumps({key: receipt[key] for key in ('status', 'tree_kind', 'input_tree_unchanged', 'private_real_data_replay_requested')}))
        return 0 if not failures else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tree', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--kind', choices=('min', 'public'), required=True)
    parser.add_argument('--manifest', help='Relative R3 validation configuration')
    args = parser.parse_args()
    return ValidatorR3(args.tree, args.output, args.kind, args.manifest).run()


if __name__ == '__main__':
    raise SystemExit(main())
