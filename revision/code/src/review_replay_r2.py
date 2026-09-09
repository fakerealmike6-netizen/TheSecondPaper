"""Portable R2 saved-result replay; no provider execution or network transport.

Only explicit in-tree mappings relocate original bytes. Required completed
candidate batches are enumerated and cannot silently disappear. Synthetic
public review never calls this private worker.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

from lp_run import fixed_graph_run
from verify_fixed_graph_independent import checked_events, audit_case
from validate_review_bundle_r1 import (input_path, batch_inputs, batch_worker,
                                     batch_status_identity, read, write, semantic_intervals)

SCHEMA = 'stage1b-r2-private-validation-v1'
REQUIRED = {'schema_version', 'baseline_root', 'policy', 'seeds', 'members', 'registry',
            'label_manifest', 'old_jobs', 'new_jobs', 'expected_graph_root', 'batch_specs', 'work'}
OPTIONAL = {'label_slice_mapping', 'reference_delta'}


def checked_config(tree, config):
    tree = Path(tree).resolve()
    if not isinstance(config, dict) or set(config) - (REQUIRED | OPTIONAL) or REQUIRED - set(config):
        raise ValueError('R2 private manifest must have only the documented required/optional keys')
    if config['schema_version'] != SCHEMA:
        raise ValueError('Unsupported R2 private validation schema')
    paths = {name: input_path(tree, config[name]) for name in REQUIRED - {'schema_version', 'batch_specs'}}
    if 'reference_delta' in config:
        paths['reference_delta'] = input_path(tree, config['reference_delta'])
        if not paths['reference_delta'].is_dir():
            raise ValueError('Optional finite reference_delta must name an in-tree directory')
    spec = {key: config[key] for key in ('policy', 'members', 'registry', 'label_manifest', 'work', 'batch_specs')}
    spec.update(events=config['seeds'], jobs=config['old_jobs'])
    if 'label_slice_mapping' in config:
        spec['label_slice_mapping'] = config['label_slice_mapping']
    batch_inputs(tree, spec)
    if paths['old_jobs'] == paths['new_jobs']:
        raise ValueError('Old and new saved-job roots must be distinct')
    if not paths['new_jobs'].is_dir() or not paths['baseline_root'].is_dir() or not paths['expected_graph_root'].is_dir():
        raise ValueError('R2 replay roots must be directories')
    selected = {input_path(tree, item['folder']) for item in config['batch_specs']}
    if any(folder.parent != paths['new_jobs'] for folder in selected):
        raise ValueError('Each required new batch must be directly inside declared new_jobs')
    completed, excluded = set(), []
    for folder in sorted(paths['new_jobs'].iterdir()):
        if not folder.is_dir() or not (folder / 'job.json').is_file():
            raise ValueError('new_jobs contains an undeclared non-job entry')
        input_path(tree, (folder / 'job.json').relative_to(tree).as_posix())
        job = read(folder / 'job.json')
        if job.get('kind') == 'candidate' and job.get('state') == 'QUERY_STATE_COMPLETED':
            completed.add(folder)
        else:
            excluded.append({'folder': folder.relative_to(tree).as_posix(),
                             'kind': job.get('kind'), 'state': job.get('state'),
                             'job_sha256': hashlib.sha256((folder / 'job.json').read_bytes()).hexdigest(),
                             'reason': 'NONCANDIDATE_OR_NOT_COMPLETED; retained evidence is not a completed interval'})
    if completed != selected:
        raise ValueError('batch_specs must include every completed new candidate job exactly once')
    return paths, spec, excluded


def _stable(value):
    if isinstance(value, dict):
        return {key: _stable(item) for key, item in value.items() if key not in ('provenance', 'raw_path', 'job_path')}
    if isinstance(value, list):
        return [_stable(item) for item in value]
    return value


def _records(rows):
    return sorted((_stable(row) for row in rows), key=lambda row: json.dumps(row, sort_keys=True))


def semantic_collection_r2(value):
    """Compare full candidate/context/state/coverage facts, excluding location/provenance text."""
    return {'query_id': value['query_id'], 'status': value['status'],
            **{key: _records(value.get(key, [])) for key in
               ('candidate_events', 'context_events', 'states', 'stops', 'unresolved_frontier',
                'coverage', 'gaps', 'fact_conflicts', 'quarantined_facts')},
            'invalidated_evidence': _stable(value.get('invalidated_evidence', {}))}


def semantic_sources(value):
    return sorted([{'query': row['query'], 'used_jobs': _records(row['used_jobs']),
                    'seed': {key: row['seed'].get(key) for key in
                             ('query_id', 'seed_event_id', 'seed_raw_evidence_sha256', 'input_filter')}}
                   for row in value], key=lambda row: row['query'])


STATUS_KEYS = ('name', 'query_id', 'status', 'collector_status', 'live_query_interval_count',
               'live_logical_job_count', 'live_exported_rows', 'live_interval_rows',
               'live_normalized_distinct_events', 'live_queried_address_count',
               'unresolved_frontier_count', 'unqueried_label_addresses', 'failed_label_addresses',
               'conflicted_label_addresses', 'candidate_event_count', 'candidate_transaction_count',
               'expanded_address_count', 'candidate_address_count', 'state_count',
               'service_entry_state_count', 'service_address_count', 'fact_validation_status',
               'candidate_stop_coverage_sha256')


def replay_review(tree, config, output):
    tree, output = Path(tree).resolve(), Path(output).resolve()
    paths, spec, excluded = checked_config(tree, config)
    mapping = batch_worker(tree, spec, output)
    expected = paths['expected_graph_root']
    comparisons, independent = [], []
    for pilot in read(paths['policy'])['query_pilots']:
        name = pilot['name']
        prior, current = input_path(expected, name), output / name
        old_collection, new_collection = read(prior / 'collection.json'), read(current / 'collection.json')
        old_graph, new_graph = read(prior / 'fixed_graph.json'), read(current / 'fixed_graph.json')
        old_status, new_status = read(prior / 'status.json'), read(current / 'status.json')
        same_collection = semantic_collection_r2(old_collection) == semantic_collection_r2(new_collection)
        same_graph = old_graph == new_graph
        same_status = {key: old_status.get(key) for key in STATUS_KEYS} == {key: new_status.get(key) for key in STATUS_KEYS}
        identity = batch_status_identity(old_status, new_status, mapping.get('label_slice_identity'))
        fixed_graph_run(current / 'fixed_graph.json', current / 'lp')
        old_lp, new_lp = read(prior / 'lp/lp_fixed_graph_result.json'), read(current / 'lp/lp_fixed_graph_result.json')
        old_lp_bound = old_lp.get('graph_file_sha256') == hashlib.sha256((prior / 'fixed_graph.json').read_bytes()).hexdigest()
        same_lp = semantic_intervals(old_lp) == semantic_intervals(new_lp)
        try:
            checked_events(new_graph)
        except ValueError as exc:
            independent_case = {'pilot': name, 'status': 'NOT_APPLICABLE_OUTSIDE_DECLARED_NATIVE_PROOF_ASSUMPTIONS',
                                'reason': str(exc), 'intervals_compared': 0, 'primal_witnesses_checked': 0}
            independent_ok = True
        else:
            try:
                independent_case = audit_case(output.parent, output.name, name, derived_subdir='.')
                independent_ok = True
            except ValueError as exc:
                independent_case = {'pilot': name, 'status': 'FAIL', 'reason': str(exc),
                                    'intervals_compared': 0, 'primal_witnesses_checked': 0}
                independent_ok = False
        independent.append(independent_case)
        comparisons.append({'pilot': name, 'candidate_context_states_stops_frontiers_coverage_equal': same_collection,
                            'fixed_graph_equal': same_graph, 'status_equal': same_status,
                            'label_source_or_verified_slice_identity': identity,
                            'expected_lp_bound_to_graph_bytes': old_lp_bound,
                            'event_and_joint_lp_intervals_equal': same_lp,
                            'graph_events': len(new_graph['events']),
                            'target_status': 'OBSERVED_TARGET' if new_graph['objective_groups'] else 'NO_OBSERVED_TARGET',
                            'independent_native_proof_status': independent_case['status'],
                            'passed': all((same_collection, same_graph, same_status, identity, old_lp_bound, same_lp, independent_ok))})
    source_equal = semantic_sources(read(expected / 'source_manifest.json')) == semantic_sources(read(output / 'source_manifest.json'))
    frontier_equal = _records(read(expected / 'next_actual_frontier.json')) == _records(read(output / 'next_actual_frontier.json'))
    independent_receipt = {'scope': 'NEW_OBSERVED_GRAPH_ONLY; separate from four baseline graphs',
                           'cases': independent,
                           'intervals_compared': sum(row['intervals_compared'] for row in independent),
                           'primal_witnesses_checked': sum(row['primal_witnesses_checked'] for row in independent),
                           'applicability_checked_before_proof': True,
                           'passed_for_applicable_cases': all(row['status'] != 'FAIL' for row in independent)}
    write(output / 'new_graph_independent.json', independent_receipt)
    receipt = {'schema_version': 'stage1b-r2-private-saved-replay-v1',
               'passed': all(row['passed'] for row in comparisons) and source_equal and frontier_equal,
               'comparisons': comparisons, 'used_job_scope_execution_page_hash_sources_equal': source_equal,
               'next_actual_frontier_equal': frontier_equal, 'excluded_new_job_evidence': excluded,
               'portable_mapping_receipt': 'portable_mapping_receipt.json',
               'new_graph_independent_proof': {key: value for key, value in independent_receipt.items() if key != 'cases'},
               'network_requests': 0, 'new_provider_data': 0, 'real_provider_validation_claimed': False,
               'comparison_excludes_only': ['provenance text', 'raw_path', 'job_path', 'measured execution timings'],
               'minimal_seed_slice_file_identities': {key: {'path': config[key], 'sha256': hashlib.sha256(paths[key].read_bytes()).hexdigest()}
                                                     for key in ('seeds', 'members')}}
    write(output / 'r2_replay_comparison.json', receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tree', type=Path, required=True)
    parser.add_argument('--config', required=True, help='Relative manifest path inside the frozen tree')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if 'sitecustomize' not in sys.modules or 'REVIEW_VALIDATION_OUTPUT' not in os.environ:
        raise RuntimeError('Private replay worker requires the inherited offline validation guard')
    if not args.output.resolve().is_relative_to(Path(os.environ['REVIEW_VALIDATION_OUTPUT']).resolve()):
        raise ValueError('Worker output must stay in the inherited validation output')
    tree = args.tree.resolve()
    result = replay_review(tree, read(input_path(tree, args.config)), args.output)
    print(json.dumps({'passed': result['passed'], 'pilots': len(result['comparisons']), 'network_requests': 0}))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
