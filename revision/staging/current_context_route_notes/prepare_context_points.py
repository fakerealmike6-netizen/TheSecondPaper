"""Root-invoked, read-only ETH/WETH requirement and exact-cache inventory.

Only the requested staging output is written. No provider, importer apply,
context assembly, experiment registration, ledger constructor, or solver call.
The SHA-bound spec is a diagnostic snapshot, never a final candidate freeze.
"""
from __future__ import annotations
import argparse
from collections import Counter
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

SCHEMA = 'stage1d-current-context-point-preparation-v1'
PROVIDER = 'ALCHEMY_ETH_MAINNET_EXISTING'
NAMES = frozenset(('txphish_src001', 'txphish_src002'))

def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()

def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def inside(root, name):
    root = Path(root).resolve()
    original = root / name
    path = original.resolve()
    if not path.is_relative_to(root):
        raise ValueError('Path escaped declared work/staging root')
    for parent in (original, *original.parents):
        if parent.is_symlink() or getattr(parent, 'is_junction', lambda: False)():
            raise ValueError('Linked inputs/outputs are not accepted')
        if parent == root:
            break
    return path

def checked(work, reference):
    path = inside(work, reference['path'])
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != reference['sha256']:
        raise ValueError('Exact input SHA changed: ' + reference['path'])
    return json.loads(data)

def source_inventory(work):
    # Direct src/*.py only: no raw tree or repository audit.
    return {p.name: sha(p) for p in sorted((work / 'src').glob('*.py'))}

def load_material(work, entry):
    from stage1d_context_online import _merge, _merge_rows
    from stage1d_context_coverage import merge_coverage
    material = checked(work, entry['material']) if entry.get('material') else {}
    refs = [entry['material']] if entry.get('material') else []
    for part in entry.get('material_parts', []):
        for field, ref in part.items():
            if field not in {'events', 'headers', 'balances', 'receipts', 'transactions', 'coverage', 'weth_logs'}:
                raise ValueError('Unsupported material field')
            value = checked(work, ref)
            refs.append(ref)
            if field == 'events':
                material[field] = _merge_rows(material.get(field, []), value)
            elif field == 'coverage':
                material[field] = merge_coverage(material.get(field, []), value)
            elif field == 'weth_logs':
                material[field] = list({digest(v): v for v in material.get(field, []) + value}.values())
            else:
                target = material.setdefault(field, {})
                for key, item in value.items():
                    target[key] = _merge(target[key], item) if key in target else item
    return material, refs

def cached_points(work, plans, runtime):
    """One exact-key read transaction; validate only hit envelopes, not global raw.

    This uses the accepted current _cached_rpc point contract with the finite
    WETH validator. It does not certify full HTTP/log/ledger coverage.
    """
    from read_retry_r4 import logical_key
    path = work / 'private/read_retry_r4.sqlite'
    if not path.is_file():
        return {digest(p): {'status': 'NO_CURRENT_CACHE_DATABASE'} for p in plans}
    if (work / 'private/network_worker.lock').exists():
        raise ValueError('Root must wait for the existing writer safe point')
    output = {}
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        for plan in plans:
            identity = runtime.rpc_identity(PROVIDER, plan)
            key = logical_key(identity)
            row = db.execute('SELECT identity_json,state,success_payload,success_receipt FROM read_requests WHERE logical_key=?', (key,)).fetchone()
            result = {'logical_key': key, 'status': 'NO_CURRENT_EXACT_REQUEST'}
            if row:
                if json.loads(row[0]) != identity:
                    raise ValueError('Exact cache logical identity conflict')
                result['persistent_request_state'] = row[1]
                if row[1] == 'IN_FLIGHT':
                    raise ValueError('Exact current point remains in flight; no recovery or retry here')
                result['status'] = 'CURRENT_REQUEST_NOT_SUCCESS'
                if row[1] == 'SUCCESS':
                    receipt = json.loads(row[3])
                    ref = {'path': receipt['artifact_path'], 'sha256': receipt['artifact_sha256']}
                    env = checked(work, ref)
                    request, response = env['request'], env['response']
                    if ({k: request.get(k) for k in ('method', 'params')} != plan
                            or runtime.rpc_result_status(request, response) != 'SUCCESS_VALIDATED'
                            or response.get('result') != json.loads(row[2])):
                        raise ValueError('Exact cache artifact no longer satisfies current runtime')
                    result.update(status='SUCCESS_ENVELOPE_SHA_AND_RUNTIME_VALIDATED', artifact=ref,
                                  new_requests=0, coverage_claimed=False)
            output[digest(plan)] = result
    return output

def derive(work, spec, *, check_cache=False):
    from stage1d_closure_scope import active_batch_path
    from stage1d_closure_context import requirements, _points, _missing_points
    from stage1d_finite_state_rpc import FiniteStateRuntime
    from stage1d_context import _event
    from context_ledger_r3 import integer
    if spec.get('schema_version') != SCHEMA or not set(spec['queries']) <= NAMES or not spec['queries']:
        raise ValueError('Explicit TxPhish current diagnostic input spec required')
    if spec.get('final_candidate_freeze') is not False:
        raise ValueError('Preparation must explicitly disclaim final candidate freeze')
    sources = source_inventory(work)
    if sources != spec['source_sha256']:
        raise ValueError('Root source inventory changed; regenerate the reviewed diagnostic spec')
    batch = checked(work, spec['active_batch'])
    if inside(work, spec['active_batch']['path']) != active_batch_path(work).resolve():
        raise ValueError('Spec is not the adopted current scope')
    queries = {q['name']: q for q in batch['queries']}
    out, unique = {}, {}
    for name, entry in spec['queries'].items():
        collection = checked(work, entry['collection'])
        labels = checked(work, entry['labels'])
        material, refs = load_material(work, entry)
        needed = requirements(queries[name], collection, labels, material)
        missing = {digest(p) for p in _missing_points(needed['point_requests'], *_points(material))}
        blocks = {}
        for original in collection.get('candidate_events', []) + collection.get('context_events', []) + needed['selected_events']:
            event = _event(original)
            if event.get('tx_hash') and event.get('block') is not None:
                if event['tx_hash'] in blocks and blocks[event['tx_hash']] != event['block']:
                    raise ValueError('Current physical transaction block conflict')
                blocks[event['tx_hash']] = event['block']
        requests = []
        for plan in needed['point_requests']:
            key = digest(plan)
            unique[key] = plan
            method, params = plan['method'], plan['params']
            block = (integer(params[0]) if method == 'eth_getBlockByNumber' else
                     integer(params[1]) if method in ('eth_getBalance', 'eth_call') else blocks.get(params[0]))
            row = {'request_sha256': key, 'request': plan, 'material_presence': 'MISSING' if key in missing else 'PRESENT_REQUIRES_FINAL_ASSEMBLY_VALIDATION'}
            if method != 'eth_call' and block is not None:
                row['legacy_need_for_root_only'] = dict(plan, expected_block=block,
                    query_id=queries[name]['query_id'], scope_id=queries[name]['scope_id'], scope_hash=queries[name]['scope_hash'],
                    reason='CURRENT_SHA_BOUND_CLOSURE_CONTEXT_POINT_REQUIREMENT', evidence_refs=[entry['collection'], spec['active_batch'], *refs])
            elif method == 'eth_call':
                row['legacy_route'] = 'UNSUPPORTED_BY_CURRENT_LEGACY_POINT_IMPORTER'
            else:
                row['legacy_route'] = 'CURRENT_EXPECTED_BLOCK_EVIDENCE_MISSING'
            requests.append(row)
        out[name] = {'query_id': queries[name]['query_id'], 'scope_hash': queries[name]['scope_hash'],
            'inputs': entry, 'material_sources': refs, 'material_omitted': not bool(refs),
            'binding': needed['binding'], 'requirements_sha256': digest(needed),
            'context_plan': needed['context_plan'], 'point_requests': requests,
            'weth_ledger_requirements': needed['weth_ledger_requirements'],
            'native_receipt_point_requests_satisfied_by_ledger': needed['native_receipt_point_requests_satisfied_by_ledger'],
            'selected_physical_rows': len(needed['selected_events']),
            'selected_physical_rows_sha256': digest(needed['selected_events']),
            'background_asset_projection': needed['background_asset_projection']}
    runtime = FiniteStateRuntime([p for p in unique.values() if p['method'] == 'eth_call'])
    for plan in unique.values():
        runtime.validate_rpc(plan)
    cache = cached_points(work, list(unique.values()), runtime) if check_cache else {}
    for name, value in out.items():
        for row in value['point_requests']:
            row['current_cache'] = cache.get(row['request_sha256'], {'status': 'NOT_INSPECTED'})
        value['summary'] = {'account_asset_windows': len(value['context_plan']['rows']),
            'weth_ledger_windows': len(value['weth_ledger_requirements']),
            'points': len(value['point_requests']),
            'material_missing_points': sum(r['material_presence'] == 'MISSING' for r in value['point_requests']),
            'current_cache_states': dict(Counter(r['current_cache']['status'] for r in value['point_requests']))}
    # Fail rather than seal results if replay/source/material changed while reading.
    if source_inventory(work) != sources:
        raise ValueError('Source changed during preparation')
    checked(work, spec['active_batch'])
    for entry in spec['queries'].values():
        for field in ('collection', 'labels', 'material'):
            if entry.get(field):
                checked(work, entry[field])
        for part in entry.get('material_parts', []):
            for ref in part.values():
                checked(work, ref)
    return {'schema_version': SCHEMA, 'status': 'PREPARED_ONLY', 'source_sha256': sources,
        'active_batch': spec['active_batch'], 'queries': out, 'unique_points_across_pair': len(unique),
        'clock_on_later_shared_dispatch': 'SHARED_ALL_FOUR_EXISTING_QUERY_CLOCKS',
        'new_external_requests': 0, 'legacy_imports': 0, 'production_writes': 0,
        'formal_model_built': False, 'candidate_freeze_created': False,
        'full_context_claimed': False, 'execution_supported': False,
        'native_bq_route': 'OPEN_UNTIL_CURRENT_NATIVE_PLAN_HAS_EXPLICIT_FULL_LEDGER_COVERAGE_AND_FEE_TREE_VALIDATION',
        'weth_ledger_route': 'OPEN_UNTIL_EXACT_REQUIRED_LOG_INTERVALS_FULL_RECEIPTS_SEMANTIC_UNITS_AND_ANCHORS_ARE_VALIDATED'}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    parser.add_argument('--spec', required=True, help='Path relative to work with explicit current input refs')
    parser.add_argument('--spec-sha256', required=True)
    parser.add_argument('--output', required=True, help='Relative to this exclusive staging directory')
    parser.add_argument('--check-current-cache', action='store_true')
    args = parser.parse_args()
    sys.dont_write_bytecode = True
    work = Path(args.work).resolve()
    sys.path.insert(0, str(work / 'src'))
    spec_ref = {'path': args.spec, 'sha256': args.spec_sha256}
    spec = checked(work, spec_ref)
    result = derive(work, spec, check_cache=args.check_current_cache)
    checked(work, spec_ref)
    result['input_spec'] = spec_ref
    result['driver_sha256'] = sha(Path(__file__))
    output = inside(Path(__file__).resolve().parent, args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('xb') as handle:
        handle.write(encoded(result) + b'\n')
    print(json.dumps({'status': result['status'], 'output': str(output), 'sha256': sha(output),
        'queries': {k: v['summary'] for k, v in result['queries'].items()}}, ensure_ascii=False))

if __name__ == '__main__':
    main()
