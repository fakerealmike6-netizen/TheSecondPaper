"""Prepare fresh R3 MIN/public payload inputs; do not freeze or publish them.

The root adds final reports, then freezes the payload DAG and tests final ZIPs.
Only source-manifest-selected chain evidence enters MIN. No account database,
permission document, usage response or full third-party library enters public.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + '\n').encode('utf-8')


def prepare(work, destination, *, context_manifest='configs/CONTEXT_REPLAY_R3.json',
            pipeline_root='derived/context_pipeline', amount_root='derived/context_amounts',
            weth_roots=(), includes=(), dry_run=False):
    work, destination = Path(work).resolve(), Path(destination).resolve()
    if not destination.is_relative_to(work) or destination == work or destination.exists():
        raise ValueError('Select a fresh staging directory strictly inside this revision')
    sys.path.insert(0, str(work / 'src'))
    from public_export_r1 import selected_sources, scan_selected
    from package_review_r3 import public_path_allowed, tree_files
    from validate_review_bundle_r1 import input_path
    selected, _ = selected_sources(work)
    # Preserve the reusable payload selection implementation in both review
    # bundles, even when this caller script lives at the workspace root.
    selected['src/assemble_review_inputs_r3.py'] = Path(__file__).read_bytes()
    policy = read(input_path(work, 'configs/STAGE1B_R3_POLICY.json'))
    public_policy = {'schema_version': 'stage1b-r3-public-policy-v1', 'stage': policy['stage'],
        'checkpoint': policy['checkpoint'], 'primary_asset': 'ETH',
        'query_pilots': [{key: row[key] for key in ('name', 'start_block', 'end_block', 'max_acquisition_depth')}
                         for row in policy['query_pilots']],
        'dune': {key: policy['dune'][key] for key in ('account_execution_cap_credits', 'cumulative_risk_cap_credits',
                 'cumulative_risk_warning_credits', 'warning_requires_pause_or_new_confirmation',
                 'unknown_execution_initial_reservation_credits', 'payment_changes_authorized')},
        'other_budgets': {key: policy['other_budgets'][key] for key in ('rpc_rest_max_individual_operations',
                          'alchemy_max_compute_units', 'bigquery_max_bytes_billed', 'bigquery_job_max_bytes_billed',
                          'metasleuth_new_requests')},
        'external_acceptance_status': 'PENDING_REVIEW', 'account_material_included': False}
    selected['configs/STAGE1B_R3_PUBLIC_POLICY.json'] = encoded(public_policy)
    selected['configs/PUBLIC_VALIDATION_R3.json'] = encoded({'schema_version': 'stage1b-r3-public-validation-v1'})
    for name in ('VALIDATION_R2.md', 'VALIDATION_R3.md'):
        path = work / 'configs' / name
        if path.exists():
            selected['configs/' + name] = path.read_bytes()
    requirements = work / 'baseline/r2/requirements.txt'
    if requirements.is_file():
        selected['requirements.txt'] = requirements.read_bytes()
    rejected = [name for name in selected if not public_path_allowed(name)]
    findings = scan_selected(selected)
    if rejected or findings:
        raise ValueError(json.dumps({'public_path_rejected': rejected, 'public_scan_findings': findings}))
    manifest_path = input_path(work, context_manifest)
    manifest = read(manifest_path)
    if manifest.get('schema_version') != 'stage1b-r3-context-replay-v1':
        raise ValueError('R3 source replay manifest required')
    expected_names = {'atomic_simple_transfer', 'harmony_high_branch'}
    if {row['name'] for row in manifest['queries']} != expected_names or len(manifest['queries']) != 2:
        raise ValueError('Exactly the frozen two queries required')
    private_files, source_rows = {}, []

    def add(name, expected=None):
        path = input_path(work, name)
        if not path.is_file():
            raise ValueError('Finite source file required: ' + name)
        value = path.read_bytes()
        actual = hashlib.sha256(value).hexdigest()
        if expected and expected != actual:
            raise ValueError('Source manifest identity changed: ' + name)
        if name in private_files:
            if private_files[name] != value:
                raise ValueError('Same path has conflicting selected bytes')
            return
        private_files[name] = value
        source_rows.append({'path': name, 'bytes': len(value), 'sha256': actual, 'status': 'COPIED_IDENTICAL'})

    def add_directory(name, suffixes=('.json', '.csv', '.sql', '.bin', '.md', '.txt', '.sha256')):
        path = input_path(work, name)
        for child in sorted(path.rglob('*')):
            if child.is_file() and child.suffix in suffixes and '__pycache__' not in child.parts:
                add(child.relative_to(work).as_posix())

    def explicit_references(value):
        if isinstance(value, dict):
            if 'path' in value and 'sha256' in value:
                add(value['path'], value['sha256'])
            for child in value.values():
                explicit_references(child)
        elif isinstance(value, list):
            for child in value:
                explicit_references(child)

    def raw_references(value):
        if isinstance(value, dict):
            if value.get('raw_path'):
                add(value['raw_path'], value.get('sha256') or value.get('raw_sha256'))
            for child in value.values():
                raw_references(child)
        elif isinstance(value, list):
            for child in value:
                raw_references(child)

    add(context_manifest)
    explicit_references(manifest)
    for batch in manifest.get('rpc_batches', []):
        receipt_name = batch['receipt']['path']
        receipt = read(input_path(work, receipt_name))
        add_directory(Path(receipt_name).parent.as_posix())
        add(receipt['raw_path'], receipt['raw_sha256'])
        for member in receipt['members']:
            add(member['artifact_path'], member['artifact_sha256'])
    config = {'schema_version': 'stage1b-r3-private-validation-v1', 'context_manifest': context_manifest,
              'r2_baseline': [], 'expected_context': []}
    generated = {}
    for query in manifest['queries']:
        name = query['name']
        graph_name = query['fixed_graph']['path']
        expected_lp = (Path(graph_name).parent / 'lp/lp_fixed_graph_result.json').as_posix()
        add(expected_lp)
        config['r2_baseline'].append({'name': name, 'graph': graph_name, 'expected_lp': expected_lp})
        for job_spec in query.get('context_jobs', []):
            job_name = job_spec['job']['path']
            job = read(input_path(work, job_name))
            add_directory(Path(job_name).parent.as_posix())
            raw_references(job)
            add_directory(Path(job_spec['freeze']['path']).parent.as_posix())
            for receipt_path in input_path(work, Path(job_name).parent.as_posix()).glob('*_receipt.json'):
                raw_references(read(receipt_path))
        pipeline_dir = input_path(work, (Path(pipeline_root) / name).as_posix())
        aggregate = {path.stem: read(path) for path in sorted(pipeline_dir.glob('*.json'))
                     if path.name != 'ASSEMBLED_CONTEXT.json'}
        required = {'model_input', 'balance_anchors', 'account_ledgers', 'ledger_reconciliation',
                    'constraint_provenance', 'evidence_gaps', 'fact_conflicts', 'normalization_exclusions',
                    'completion_status', 'source_manifest'}
        if not required.issubset(aggregate):
            raise ValueError('Final actual context pipeline is incomplete: ' + name)
        add_directory(pipeline_dir.relative_to(work).as_posix())
        # The loader-selected identity list can include source bytes beyond the
        # top manifest, such as protocol-receipt and account-code evidence.
        for row in aggregate['source_manifest']['files']:
            add(row['path'], row['sha256'])
        aggregate_name = (Path(pipeline_root) / name / 'ASSEMBLED_CONTEXT.json').as_posix()
        generated[aggregate_name] = encoded(aggregate)
        amount_name = (Path(amount_root) / name / 'CONTEXT_AMOUNT_RESULTS.json').as_posix()
        add_directory((Path(amount_root) / name).as_posix())
        config['expected_context'].append({'name': name, 'ledger': aggregate_name, 'amounts': amount_name})
    for name in ('configs/STAGE1B_R3_POLICY.json', 'configs/RESEARCH_CHARTER_v1.0.md',
                 'CONTINUATION_GATE_R3.json', 'private/BUDGET_MIGRATION_R3.json', 'private/INHERITED_RESOURCE_R3.json'):
        if (work / name).exists():
            add(name)
    for name in ('derived/amount_comparison_r3', 'derived/usage_r3'):
        if (work / name).is_dir():
            add_directory(name)
    for name in weth_roots:
        add_directory(name)
        weth_manifest_name = (Path(name) / 'inputs/INPUT_MANIFEST.json').as_posix()
        expected_weth_name = (Path(name) / 'replay/WETH_COMPONENT_RESULT.json').as_posix()
        if 'weth' in config:
            raise ValueError('Only the fixed single WETH component may be selected')
        weth_manifest_path = input_path(work, weth_manifest_name)
        weth_manifest = read(weth_manifest_path)
        if weth_manifest.get('schema_version') != 'stage1b-r3-weth-replay-input-v1':
            raise ValueError('Fixed WETH input manifest schema required')
        for entry in weth_manifest['files'].values():
            relative = (Path(weth_manifest_name).parent / entry['path']).as_posix()
            add(relative, entry['sha256'])
            if input_path(work, relative).stat().st_size != entry['bytes']:
                raise ValueError('WETH fixed input size mismatch')
        add(expected_weth_name)
        config['weth'] = {'manifest': weth_manifest_name, 'expected_result': expected_weth_name}
    for name in includes:
        path = input_path(work, name)
        if path.is_dir():
            add_directory(name)
        else:
            add(name)
    generated['configs/PRIVATE_VALIDATION_R3.json'] = encoded(config)
    generated['manifests/R3_INPUT_SELECTION.json'] = encoded({'schema_version': 'stage1b-r3-min-input-selection-v1',
        'context_manifest_sha256': digest(manifest_path), 'source_rows': sorted(source_rows, key=lambda row: row['path']),
        'generated_aggregate_files': sorted(generated), 'full_label_registry_required': False,
        'account_database_or_permission_in_public': False, 'payload_freeze_performed': False})
    selected['manifests/PUBLIC_SOURCE_MAPPING.json'] = encoded({'schema_version': 'stage1b-r3-public-source-selection-v1',
        'files': [{'path': name, 'sha256': hashlib.sha256(value).hexdigest(), 'bytes': len(value)}
                  for name, value in sorted(selected.items())], 'all_selected_source_bytes_identical_between_min_public': True})
    if dry_run:
        return {'status': 'INPUT_SELECTION_VALIDATED_NO_TREES_CREATED', 'private_input_files': len(private_files),
                'public_files': len(selected), 'generated_files': len(generated),
                'private_input_bytes': sum(map(len, private_files.values())), 'destination_created': False}
    destination.mkdir(parents=True)
    private_tree, public_tree = destination / 'min_tree', destination / 'public_tree'
    for tree, values in ((public_tree, selected), (private_tree, {**selected, **private_files, **generated})):
        tree.mkdir()
        for name, value in sorted(values.items()):
            path = tree / name
            if not path.resolve().is_relative_to(tree):
                raise ValueError('Output path escape')
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('xb') as handle:
                handle.write(value)
        tree_files(tree)
    return {'min_tree': str(private_tree), 'public_tree': str(public_tree),
            'private_input_files': len(private_files), 'public_files': len(selected),
            'generated_files': len(generated), 'reports_added': False,
            'payload_frozen': False, 'github_actions': 0,
            'source_manifest': str(private_tree / 'manifests/R3_INPUT_SELECTION.json')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    parser.add_argument('--context-manifest', default='configs/CONTEXT_REPLAY_R3.json')
    parser.add_argument('--pipeline-root', default='derived/context_pipeline')
    parser.add_argument('--amount-root', default='derived/context_amounts')
    parser.add_argument('--weth-root', action='append', default=[])
    parser.add_argument('--include', action='append', default=[])
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    result = prepare(args.work, args.destination, context_manifest=args.context_manifest,
                     pipeline_root=args.pipeline_root, amount_root=args.amount_root,
                     weth_roots=args.weth_root, includes=args.include, dry_run=args.dry_run)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
