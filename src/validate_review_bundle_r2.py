"""R2 extracted-bundle validation with the unchanged strict R1 isolation engine.

Both kinds run the bundled synthetic suite and 12/32 controlled LP checks.
MIN additionally rebuilds the four same-input R1 graphs and replays the latest
explicit saved Dune batches. Full unchanged reference tables are not required.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform

from package_review_r2 import (EQUIVALENCE, payload_files, json_bytes,
                               validate_public_tree, verify_manifest)
from review_replay_r2 import checked_config
from validate_review_bundle_r1 import Validator, input_path, read, write, tree_hashes


def verify_single_tree_payload(tree, kind):
    tree = Path(tree).resolve()
    manifest = verify_manifest(tree)
    mapping = read(input_path(tree, EQUIVALENCE))
    if mapping.get('schema') != 'stage1b-r2-payload-equivalence-dag-v1':
        raise ValueError('Expected the R2 non-circular payload equivalence schema')
    payload = payload_files(tree)
    rows = mapping['payload_rows']
    if kind == 'public':
        validate_public_tree(tree)
        if len(rows) != len({row['public_path'] for row in rows}) or set(payload) != {row['public_path'] for row in rows}:
            raise ValueError('Public payload mapping membership mismatch')
        for row in rows:
            path = input_path(tree, row['public_path'])
            if path.stat().st_size != row['public_bytes'] or hashlib.sha256(path.read_bytes()).hexdigest() != row['public_sha256']:
                raise ValueError('Public payload mapping hash mismatch')
    else:
        inventory = [{'path': name, 'bytes': path.stat().st_size,
                      'sha256': hashlib.sha256(path.read_bytes()).hexdigest()} for name, path in sorted(payload.items())]
        if len(inventory) != mapping['local_payload_count'] or hashlib.sha256(json_bytes(inventory)).hexdigest() != mapping['local_payload_inventory_sha256']:
            raise ValueError('Private payload aggregate mapping commitment differs')
        for row in rows:
            if row['local_path'] is None:
                continue
            path = input_path(tree, row['local_path'])
            if path.stat().st_size != row['local_bytes'] or hashlib.sha256(path.read_bytes()).hexdigest() != row['local_sha256']:
                raise ValueError('Private payload mapping hash mismatch')
    return {'passed': True, 'kind': kind, 'manifest': manifest,
            'payload_files': len(payload), 'counterpart_tree_independently_verified': False}


class ValidatorR2(Validator):
    def __init__(self, tree, output, kind, manifest_path=None):
        if kind not in ('min', 'public'):
            raise ValueError('Explicit R2 tree kind required')
        tree = Path(tree).resolve()
        self.r2_manifest_path = manifest_path or ('configs/PRIVATE_VALIDATION_R2.json' if kind == 'min' else None)
        self.r2_config = None
        if kind == 'min':
            self.r2_config = read(input_path(tree, self.r2_manifest_path))
            checked_config(tree, self.r2_config)
        elif manifest_path:
            config = read(input_path(tree, manifest_path))
            if config != {'schema_version': 'stage1b-r2-public-validation-v1'}:
                raise ValueError('Public validation manifest cannot request private replay or custom commands')
        # Reuse the repaired R1 bootstrap, allowlist, write boundary, environment
        # sanitization, command dispatch and fresh execution mirror unchanged.
        super().__init__(tree, output, kind)

    def private_checks(self):
        baseline = input_path(self.tree, self.r2_config['baseline_root'])
        same = self.out / 'same_input'
        if self.command('r2_same_input_graphs', 'src/replay_order_same_input_r2.py',
                        ['--baseline', baseline, '--output', same]):
            receipt = read(same / 'same_input_replay.json')
            self.check('r2_same_input_graph_receipt', receipt.get('passed') and receipt.get('graph_count') == 4,
                       graphs=receipt.get('graph_count'), event_entries=receipt.get('event_entries_in_four_graphs'),
                       network_requests=receipt.get('network_requests'))
            if self.command('r2_same_input_independent_proof', 'src/verify_fixed_graph_independent.py',
                            ['--root', same, '--derived-subdir', 'graphs', '--output', self.out / 'independent_fixed_graphs.json']):
                proof = read(self.out / 'independent_fixed_graphs.json')
                self.check('r2_independent_graph_receipt', proof.get('passed') and not proof.get('failures'),
                           intervals_compared=proof.get('intervals_compared'), primal_witnesses_checked=proof.get('primal_witnesses_checked'))
        if self.command('r2_latest_saved_replay', 'src/review_replay_r2.py',
                        ['--tree', self.tree, '--config', self.r2_manifest_path, '--output', self.out / 'new_observed']):
            receipt = read(self.out / 'new_observed/r2_replay_comparison.json')
            self.check('r2_latest_collection_and_lp_receipt', receipt.get('passed') and len(receipt.get('comparisons', [])) == 2,
                       comparisons=receipt.get('comparisons'), network_requests=receipt.get('network_requests'),
                       real_provider_validation_claimed=False)
        if self.r2_config.get('reference_delta'):
            delta = input_path(self.tree, self.r2_config['reference_delta'])
            if self.command('r2_finite_reference_delta', 'src/reference_delta_r2.py', ['verify', '--output', delta]):
                receipt = read(self.out / self.commands[-1]['stdout'])
                self.check('r2_finite_reference_delta_receipt', receipt.get('passed') and receipt.get('network_calls') == 0
                           and receipt.get('requires_full_baseline_library') is False,
                           **{key: value for key, value in receipt.items() if key != 'passed'})
                write(self.out / 'finite_reference_delta_verification.json', receipt)
        else:
            self.skipped('r2_finite_reference_delta', 'OPTIONAL_FINITE_DELTA_NOT_DECLARED; no reference replay is claimed')

    def run(self):
        self.bounded('r2_package_integrity', lambda: self.check('r2_manifest_and_payload_binding', True,
                     **{key: value for key, value in verify_single_tree_payload(self.tree, self.kind).items() if key != 'passed'}))
        self.bounded('public_checks', self.public_checks)
        if self.kind == 'min':
            self.bounded('r2_private_checks', self.private_checks)
        else:
            self.skipped('r2_private_saved_data_replays', 'PUBLIC_BUNDLE_INTENTIONALLY_EXCLUDES_PRIVATE_DATA; no real replay is claimed')
        after = tree_hashes(self.tree)
        self.check('frozen_tree_unchanged', self.before == after, files=len(self.before),
                   changed=sorted(path for path in set(self.before) | set(after) if self.before.get(path) != after.get(path)))
        required = ['r2_manifest_and_payload_binding', 'unit_tests', 'unit_test_receipt', 'controlled_lp', 'controlled_oracle_receipt', 'frozen_tree_unchanged']
        if self.kind == 'min':
            required += ['r2_same_input_graphs', 'r2_same_input_graph_receipt', 'r2_same_input_independent_proof', 'r2_independent_graph_receipt', 'r2_latest_saved_replay', 'r2_latest_collection_and_lp_receipt']
        failures = self.validation_failures(required)
        receipt = {'schema_version': 'stage1b-r2-portable-validation-v1',
                   'created_at_utc': datetime.now(timezone.utc).isoformat(),
                   'platform': platform.platform(), 'python': platform.python_version(),
                   'status': 'PASS' if not failures else 'FAIL', 'tree_kind': self.kind,
                   'network_disabled': True, 'credentials_removed_from_child_environment': True,
                   'legitimate_python_children_use_inherited_guard': True,
                   'writes_restricted_to_fresh_output': True,
                   'input_tree_unchanged': self.before == after,
                   'private_real_data_replay_requested': self.kind == 'min',
                   'unchanged_full_reference_tables_required': False,
                   'real_provider_access_verified': False,
                   'external_acceptance_status': 'PENDING_REVIEW',
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
    parser.add_argument('--manifest', help='Relative R2 config inside the input tree')
    args = parser.parse_args()
    return ValidatorR2(args.tree, args.output, args.kind, args.manifest).run()


if __name__ == '__main__':
    raise SystemExit(main())
