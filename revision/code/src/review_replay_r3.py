"""Guarded private R3 reproduction from source responses through exact LP.

No saved normalized anchors or hand-authored model alone satisfy this replay.
The context evidence loader verifies wire bodies, frozen requests, identities,
pagination and coverage before assembling every accounting constraint again.
"""
import argparse
from fractions import Fraction
import hashlib
import json
import os
from pathlib import Path
import sys

from context_ledger_r3 import replay_manifest
from context_lp_r3 import run_context_document, audit_context_witness, canonical_hash
from lp_run import fixed_graph_run
from validate_review_bundle_r1 import input_path, read, write, semantic_intervals
from validate_review_bundle_r3 import checked_config_r3, PILOTS


def semantic_ledger(value):
    # The provider adapter may keep trace paths as Python tuples, while the
    # delivered ledger is JSON and therefore contains arrays. Compare the
    # exact JSON artifact representation, preserving all fields and list order.
    # This does not discard physical facts or relax numeric/evidence checks.
    result = json.loads(json.dumps(value, ensure_ascii=False))
    if 'source_manifest' in result:
        source = dict(result['source_manifest'])
        source['files'] = sorted([{'sha256': row['sha256'], 'bytes': row['bytes']}
                                  for row in source['files']], key=lambda row: (row['sha256'], row['bytes']))
        result['source_manifest'] = source
    return result


def semantic_amounts(value):
    def interval(data):
        return {key: data.get(key) for key in ('status', 'asset', 'objective_events', 'lower_raw', 'upper_raw', 'context_completeness')}
    result = {key: value.get(key) for key in ('schema_version', 'input_sha256', 'query_id', 'structural_nesting',
                                             'same_graph_comparisons', 'all_same_graph_comparisons_passed')}
    result['variants'] = {}
    for variant, data in value['variants'].items():
        zero = {key: item for key, item in data['all_downstream_zero'].items()
                if key not in ('witness', 'exclusion_certificate', 'diagnostic')}
        result['variants'][variant] = {'model': data['model'],
            'entry_intervals': {key: interval(item) for key, item in data['entry_intervals'].items()},
            'address_asset_intervals': {key: interval(item) for key, item in data['address_asset_intervals'].items()},
            'all_service_joint': interval(data['all_service_joint']), 'all_downstream_zero': zero}
    return result


def audit_result_witnesses(document, result):
    """Audit every claimed exact primal and objective against the actual ledger.

    Solver-specific optimal witnesses need not be byte-identical. Both the
    bundled witness and the freshly solved witness must independently satisfy
    the event/fee capacities, anchored actual balances and source conservation.
    """
    errors, checked, unresolved = [], 0, 0
    if result.get('input_sha256') != canonical_hash(document):
        errors.append('Amount result is not bound to the rebuilt context input')
    variants = result.get('variants', {})
    if set(variants) != {'BEST_AVAILABLE_CONTEXT', 'MATCHED_INFORMATION_RELAXED'}:
        errors.append('Exactly informed and same-graph relaxed models are required')
    for variant, data in variants.items():
        relaxed = variant == 'MATCHED_INFORMATION_RELAXED'

        def audit_endpoint(endpoint, objective_events, expected, label):
            nonlocal checked, unresolved
            if endpoint.get('status') != 'OPTIMAL_EXACT_CERTIFIED':
                unresolved += 1
                return
            witness = endpoint.get('witness_event_source_raw')
            if not isinstance(witness, dict):
                errors.append(label + ': certified endpoint missing witness')
                return
            audit = audit_context_witness(document, witness, remove_balance_information=relaxed)
            checked += 1
            if not audit['exact_feasible']:
                errors.append(label + ': independent ledger witness is infeasible')
            if not endpoint.get('certificate', {}).get('certified'):
                errors.append(label + ': exact certificate flag absent')
            try:
                objective = sum((Fraction(witness[event]) for event in set(objective_events)), Fraction(0))
                if objective != Fraction(endpoint['raw']) or objective != Fraction(expected):
                    errors.append(label + ': witness objective does not equal reported interval endpoint')
            except (KeyError, TypeError, ValueError, ZeroDivisionError):
                errors.append(label + ': malformed objective witness')

        groups = [(category + ':' + key, interval) for category in ('entry_intervals', 'address_asset_intervals')
                  for key, interval in data.get(category, {}).items()]
        groups.append(('all_service_joint', data['all_service_joint']))
        for label, interval in groups:
            if interval.get('status') == 'NO_OBSERVED_TARGET':
                if document.get('all_service_entries'):
                    errors.append(label + ': existing targets suppressed')
                continue
            for side in ('lower', 'upper'):
                endpoint = interval.get('endpoints', {}).get(side, {})
                if interval.get('status') == 'OPTIMAL_EXACT_CERTIFIED' and endpoint.get('status') != 'OPTIMAL_EXACT_CERTIFIED':
                    errors.append(variant + ':' + label + ':' + side + ': exact interval claim lacks an exact endpoint')
                audit_endpoint(endpoint, interval.get('objective_events', []), interval.get(side + '_raw'), variant + ':' + label + ':' + side)
        zero = data.get('all_downstream_zero', {})
        if zero.get('status') in ('ALL_DOWNSTREAM_ZERO_FEASIBLE', 'ALL_DOWNSTREAM_ZERO_EXCLUDED'):
            events = [flow['event_id'] for tx in document['transactions'] for flow in tx.get('flows', []) if flow['role'] == 'CANDIDATE']
            endpoint = zero.get('witness') if zero.get('feasible') else zero.get('exclusion_certificate')
            if not isinstance(endpoint, dict) or endpoint.get('status') != 'OPTIMAL_EXACT_CERTIFIED':
                errors.append(variant + ': all-zero conclusion lacks its exact witness/certificate')
            audit_endpoint(endpoint or {}, events, zero.get('minimum_sum_of_downstream_source_raw'), variant + ':all_downstream_zero')
            try:
                if (Fraction(zero['minimum_sum_of_downstream_source_raw']) == 0) != zero['feasible']:
                    errors.append(variant + ': all-zero feasibility disagrees with exact minimum')
            except (KeyError, TypeError, ValueError, ZeroDivisionError):
                errors.append(variant + ': malformed all-zero result')
    return {'passed': not errors, 'claimed_exact_witnesses_checked': checked,
            'unresolved_endpoints_not_promoted_to_proof': unresolved, 'errors': errors,
            'independent_check': 'Integer/rational ledger replay, not LP matrix rows'}


def check_context_input_paths(tree, manifest_path):
    """Reject escaped manifest paths before any private replay code runs."""
    spec = read(manifest_path)
    if spec.get('schema_version') != 'stage1b-r3-context-replay-v1':
        raise ValueError('Expected actual context response replay manifest')
    if {row.get('name') for row in spec.get('queries', [])} != PILOTS or len(spec['queries']) != 2:
        raise ValueError('Context manifest must name the two frozen queries exactly once')

    def walk(value):
        if isinstance(value, dict):
            if 'path' in value:
                path = input_path(tree, value['path'])
                if not value.get('sha256') or hashlib.sha256(path.read_bytes()).hexdigest() != value['sha256']:
                    raise ValueError('Explicit evidence manifest SHA mismatch')
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
    walk(spec)
    return spec


def check_weth_input_paths(tree, manifest_path):
    manifest = read(manifest_path)
    if manifest.get('schema_version') != 'stage1b-r3-weth-replay-input-v1' or not isinstance(manifest.get('files'), dict) or not manifest['files']:
        raise ValueError('Finite WETH source replay manifest required')
    input_root = manifest_path.parent
    for entry in manifest['files'].values():
        path = input_path(input_root, entry['path'])
        if not path.is_relative_to(tree) or path.stat().st_size != entry['bytes'] or hashlib.sha256(path.read_bytes()).hexdigest() != entry['sha256']:
            raise ValueError('WETH input manifest hash or length mismatch')
    return manifest


def replay_review_r3(tree, config, output):
    tree, output = Path(tree).resolve(), Path(output).resolve()
    checked_config_r3(tree, config)
    context_manifest = input_path(tree, config['context_manifest'])
    check_context_input_paths(tree, context_manifest)
    output.mkdir(parents=True, exist_ok=True)
    baseline_comparisons, context_comparisons, proofs = [], [], []
    for case in config['r2_baseline']:
        graph_path = input_path(tree, case['graph'])
        expected = read(input_path(tree, case['expected_lp']))
        destination = output / 'R2_BASELINE' / case['name']
        fixed_graph_run(graph_path, destination)
        actual = read(destination / 'lp_fixed_graph_result.json')
        bound = expected.get('graph_file_sha256') == hashlib.sha256(graph_path.read_bytes()).hexdigest()
        equal = semantic_intervals(expected) == semantic_intervals(actual)
        baseline_comparisons.append({'name': case['name'], 'expected_lp_bound_to_graph': bound,
                                     'event_and_joint_intervals_equal': equal, 'passed': bound and equal})
    for case in config['expected_context']:
        destination = output / 'CONTEXT' / case['name']
        expected_ledger = read(input_path(tree, case['ledger']))
        rebuilt = replay_manifest(context_manifest, tree, case['name'], destination / 'ledger')
        # Source paths can be relocated in the manifest, but every source byte,
        # all model facts and row-level provenance identities must still match.
        ledger_equal = semantic_ledger(expected_ledger) == semantic_ledger(rebuilt)
        document = rebuilt['model_input']
        expected_amounts = read(input_path(tree, case['amounts']))
        actual_amounts = run_context_document(document)
        write(destination / 'amounts/CONTEXT_AMOUNT_RESULTS.json', actual_amounts)
        amount_equal = semantic_amounts(expected_amounts) == semantic_amounts(actual_amounts)
        original_audit = audit_result_witnesses(document, expected_amounts)
        replay_audit = audit_result_witnesses(document, actual_amounts)
        proofs.append({'name': case['name'], 'bundled': original_audit, 'regenerated': replay_audit})
        context_comparisons.append({'name': case['name'], 'evidence_to_ledger_rows_and_constraints_equal': ledger_equal,
            'same_input_amount_endpoints_and_status_equal': amount_equal,
            'source_files_checked': len(rebuilt['source_manifest']['files']),
            'initial_balances_known': sum(row.get('initial_actual_balance_raw') is not None for row in document['accounts']),
            'initial_balances_unknown': sum(row.get('initial_actual_balance_raw') is None for row in document['accounts']),
            'real_anchor_rows': len(rebuilt['balance_anchors']), 'reconstructed_ledger_rows': len(rebuilt['account_ledgers']),
            'completion_status': rebuilt['completion_status'],
            'passed': ledger_equal and amount_equal and original_audit['passed'] and replay_audit['passed']})
    weth_comparison = None
    if config.get('weth'):
        from weth_source_adapter_r3 import replay_weth_manifest
        weth_manifest_path = input_path(tree, config['weth']['manifest'])
        weth_manifest = check_weth_input_paths(tree, weth_manifest_path)
        expected_weth = read(input_path(tree, config['weth']['expected_result']))
        actual_weth = replay_weth_manifest(weth_manifest_path.parent, output / 'WETH_COMPONENT')
        weth_comparison = {'passed': expected_weth == actual_weth,
            'all_component_facts_checks_and_status_equal': expected_weth == actual_weth,
            'input_files_verified': len(weth_manifest['files']),
            'status': actual_weth.get('status'), 'checks_passed': actual_weth.get('checks_passed'),
            'checks_total': actual_weth.get('checks_total'),
            'real_component_certified': actual_weth.get('real_component_certified'),
            'real_conversion_enabled': actual_weth.get('real_conversion_enabled'),
            'fixed_block_identity': actual_weth.get('fixed_block_identity'),
            'runtime_code_sha256': actual_weth.get('runtime_code_sha256'),
            'network_requests': 0, 'new_provider_access_verified': False}
    result = {'schema_version': 'stage1b-r3-private-replay-comparison-v1',
              'passed': all(row['passed'] for row in baseline_comparisons + context_comparisons) and (weth_comparison is None or weth_comparison['passed']),
              'r2_baseline_comparisons': baseline_comparisons, 'context_comparisons': context_comparisons,
              'proof_audits': proofs, 'network_requests': 0, 'new_provider_data': 0,
              'weth_comparison': weth_comparison,
              'real_provider_access_verified': False, 'external_acceptance_status': 'PENDING_REVIEW',
              'comparison_exclusions': ['Source path relocation in source manifest only', 'Solver-specific optimal witness choice; both witnesses independently audited'],
              'full_unchanged_reference_or_label_library_required': False}
    write(output / 'R3_REPLAY_COMPARISON.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tree', type=Path, required=True)
    parser.add_argument('--config', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if 'sitecustomize' not in sys.modules or 'REVIEW_VALIDATION_OUTPUT' not in os.environ:
        raise RuntimeError('Private worker requires the inherited offline validation guard')
    if not args.output.resolve().is_relative_to(Path(os.environ['REVIEW_VALIDATION_OUTPUT']).resolve()):
        raise ValueError('Replay output must stay inside the fresh validation output')
    tree = args.tree.resolve()
    result = replay_review_r3(tree, read(input_path(tree, args.config)), args.output)
    print(json.dumps({'passed': result['passed'], 'context_pilots': len(result['context_comparisons']), 'network_requests': 0}))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
