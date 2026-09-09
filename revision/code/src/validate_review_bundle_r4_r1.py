"""R4-R1 extracted-bundle validation through the unchanged strict R1 guard.

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

from package_review_r4_r1 import (EQUIVALENCE, MANIFEST, payload_files,
                               json_bytes, validate_public_tree, verify_manifest)
from validate_review_bundle_r1 import Validator, input_path, read, write, tree_hashes

PILOTS = {'atomic_simple_transfer', 'harmony_high_branch'}


def validate_public_tree_r4(tree):
    return validate_public_tree(tree)


def verify_payload_r4(tree, kind):
    tree = Path(tree).resolve()
    manifest = verify_manifest(tree)
    mapping = read(input_path(tree, EQUIVALENCE))
    if mapping.get('schema') != 'stage1b-r4-r1-payload-equivalence-dag-v1':
        raise ValueError('A non-circular frozen payload equivalence mapping is required')
    payload = payload_files(tree)
    rows = mapping['payload_rows']
    if kind == 'public':
        validate_public_tree_r4(tree)
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


def checked_config_r4(tree, config):
    required = {'schema_version', 'context_manifest', 'r2_baseline', 'r3_baseline', 'expected_context'}
    if not isinstance(config, dict) or set(config) - required - {'weth'} or required - set(config) or config['schema_version'] != 'stage1b-r4-private-validation-v1':
        raise ValueError('R4 private config must contain only fixed documented pipeline inputs')
    input_path(tree, config['context_manifest'])
    for group, keys in (('r2_baseline', {'name', 'graph', 'expected_lp'}),
                        ('r3_baseline', {'name', 'ledger', 'amounts'}),
                        ('expected_context', {'name', 'ledger', 'amounts'})):
        rows = config[group]
        if not isinstance(rows, list) or len(rows) != 2 or {row.get('name') for row in rows if isinstance(row, dict)} != PILOTS:
            raise ValueError('Exactly the two frozen R4 pilots are required')
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
        if manifest.name != 'R4_INPUT_MANIFEST.json':
            raise ValueError('Fixed WETH adapter requires INPUT_MANIFEST.json')
    return config


class ValidatorR4R1(Validator):
    def __init__(self, tree, output, kind, manifest_path=None):
        if kind not in ('min', 'public'):
            raise ValueError('Explicit R4 tree kind required')
        tree = Path(tree).resolve()
        self.manifest_path = manifest_path or ('configs/PRIVATE_VALIDATION_R4.json' if kind == 'min' else None)
        self.r4_config = None
        if kind == 'min':
            self.r4_config = checked_config_r4(tree, read(input_path(tree, self.manifest_path)))
        elif manifest_path:
            value = read(input_path(tree, manifest_path))
            if value != {'schema_version': 'stage1b-r4-r1-public-validation-v1'}:
                raise ValueError('Public config cannot request private replay or arbitrary commands')
        # Do not replace the repaired credential, subprocess, posix_spawn,
        # network or filesystem guard with a simpler R4 implementation.
        super().__init__(tree, output, kind)

    def private_checks(self):
        if self.command('r4_evidence_to_ledger_and_lp', 'src/review_replay_r4.py',
                        ['--tree', self.tree, '--config', self.manifest_path, '--output', self.out / 'r4_context_replay']):
            receipt = read(self.out / 'r4_context_replay/R4_REPLAY_COMPARISON.json')
            self.check('r4_real_context_reproduction_receipt', receipt.get('passed') is True
                       and len(receipt.get('context_comparisons', [])) == 2
                       and len(receipt.get('r2_baseline_comparisons', [])) == 2
                       and len(receipt.get('r3_baseline_comparisons', [])) == 2
                       and receipt.get('network_requests') == 0,
                       context_comparisons=receipt.get('context_comparisons'),
                       r2_baseline_comparisons=receipt.get('r2_baseline_comparisons'),
                       r3_baseline_comparisons=receipt.get('r3_baseline_comparisons'),
                       proof_audits=receipt.get('proof_audits'),
                       weth_comparison=receipt.get('weth_comparison'),
                       real_provider_access_verified=False)
            if self.r4_config.get('weth'):
                weth = receipt.get('weth_comparison') or {}
                self.check('r4_weth_actual_input_replay_receipt', weth.get('passed') is True,
                           **{key: value for key, value in weth.items() if key != 'passed'})
        for label, script in (('independent_evidence', 'independent_evidence_audit_r4.py'),
                              ('independent_amounts', 'independent_amount_audit_r4.py'),
                              ('historical_context_contracts', 'audit_context_contracts_r4.py')):
            path = self.out / (label + '.json')
            if self.command(label, 'src/' + script, ['--tree', self.tree, '--output', path]):
                data = read(path)
                if label == 'independent_evidence':
                    passed = data.get('status') == 'PASS' and data.get('network_calls') == 0 and len(data.get('queries', [])) == 2
                elif label == 'independent_amounts':
                    passed = data.get('status') == 'PASS' and data.get('network_requests') == 0 and data.get('intervals_verified') == 48 and data.get('endpoint_witnesses_verified') == 100
                else:
                    passed = data.get('all_block_and_date_domains_verified') is True and data.get('top_root_fact_conflicts') == 0 and data.get('network_requests') == 0
                self.check(label + '_receipt', passed, status=data.get('status', data.get('decision')),
                           checks=data.get('checks') if type(data.get('checks')) is int else None,
                           intervals=data.get('intervals_verified'), witnesses=data.get('endpoint_witnesses_verified'))

        path = self.out / 'weth_shared_wire_verification.json'
        if self.command('weth_shared_wire', 'src/verify_weth_shared_wire_r4_r1.py',
                        ['--tree', self.tree, '--output', path]):
            data = read(path)
            self.check('weth_shared_wire_receipt', data.get('status') == 'PASS'
                       and data.get('network_requests') == 0,
                       material_result=data)

    def closure_checks(self):
        matrix = read(input_path(self.tree, 'REPAIR_CLOSURE.json'))
        self.check('receipt_repair_closure', matrix.get('id') == 'R4-F05-R1'
                   and matrix.get('status') == 'FIXED_AND_TESTED'
                   and matrix.get('same_fixture_old_failure_reproduced') is True
                   and matrix.get('required_erc20_and_global_checks_preserved') is True)
        old_tests = matrix.get('unchanged_original_test_sha256', {})
        self.check('original_660_tests_preserved', bool(old_tests)
                   and matrix.get('original_test_count') == 660
                   and all(hashlib.sha256(input_path(self.tree, p).read_bytes()).hexdigest() == v for p,v in old_tests.items()))
        tests = read(self.out / 'tests.json')
        self.check('complete_revision_test_count', tests.get('tests_run') == matrix.get('total_test_count')
                   and tests.get('tests_run', 0) >= 660 and tests.get('success') is True)
        source_rows = matrix.get('source_sha256', {})
        self.check('repair_source_binding', bool(source_rows) and all(hashlib.sha256(input_path(self.tree, p).read_bytes()).hexdigest() == v for p,v in source_rows.items()))
        self.check('external_acceptance_remains_pending', matrix.get('external_acceptance_status') == 'PENDING_REVIEW')

    def run(self):
        self.bounded('r4_package_integrity', lambda: self.check('r4_manifest_and_payload_binding', True,
                     **{key: value for key, value in verify_payload_r4(self.tree, self.kind).items() if key != 'passed'}))
        self.bounded('public_checks', self.public_checks)
        self.bounded('closure_checks', self.closure_checks)
        if self.kind == 'min':
            self.bounded('r4_private_checks', self.private_checks)
        else:
            self.skipped('r4_private_evidence_replay', 'PUBLIC_EXCLUDES_PRIVATE_CHAIN_EVIDENCE; no real private replay is claimed')
        after = tree_hashes(self.tree)
        self.check('frozen_tree_unchanged', self.before == after, files=len(self.before),
                   changed=sorted(path for path in set(self.before) | set(after) if self.before.get(path) != after.get(path)))
        required = ['r4_manifest_and_payload_binding', 'unit_tests', 'unit_test_receipt', 'controlled_lp', 'controlled_oracle_receipt', 'frozen_tree_unchanged']
        required += ['receipt_repair_closure', 'original_660_tests_preserved', 'complete_revision_test_count',
                     'repair_source_binding', 'external_acceptance_remains_pending']
        if self.kind == 'min':
            required += ['r4_evidence_to_ledger_and_lp', 'r4_real_context_reproduction_receipt']
            for label in ('independent_evidence', 'independent_amounts', 'historical_context_contracts'):
                required += [label, label + '_receipt']
            if self.r4_config.get('weth'):
                required += ['r4_weth_actual_input_replay_receipt']
            required += ['weth_shared_wire', 'weth_shared_wire_receipt']
        failures = self.validation_failures(required)
        receipt = {'schema_version': 'stage1b-r4-r1-portable-validation-v1',
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
    parser.add_argument('--manifest', help='Relative R4 validation configuration')
    args = parser.parse_args()
    return ValidatorR4R1(args.tree, args.output, args.kind, args.manifest).run()


if __name__ == '__main__':
    raise SystemExit(main())
