"""Exact native candidate SQL optimization from a retained all-asset execution.

The old execution and financial facts remain untouched. This is a new SQL
identity under the same native Scope, not a shorter window or converted asset.
All native top/internal index rows survive. Whole-block native ledger, receipt,
balance and protocol context remain independent required evidence.
"""
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha

from collector import NATIVE, Scope
from context_access_r3 import read, sha
from page_attempts import atomic_json
from provider_dune import normalize_rows
from stage1d_acquisition import save_sql
from stage1d_candidate_batch import build_sql, FIELDS
from stage1d_export import FULL_COLUMNS, bound_receipt
from stage1d_runtime import Runtime, execute_sql, result_rows

CAPABILITY = 'NATIVE_INDEX_ONLY'
SCHEMA = 'stage1d-native-candidate-sql-v1'
TOKEN_MARKER = "  UNION ALL\n  SELECT 'erc20'"
ENDING = ')\nSELECT * FROM indexed_events\nORDER BY block_number,tx_index,tx_hash,event_kind,log_index,trace_address\n'


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def inside(work, path):
    result = (Path(work) / path).resolve()
    if not result.is_relative_to(Path(work).resolve()):
        raise ValueError('Native candidate dependency escapes workspace')
    cursor = Path(work).resolve()
    for part in result.relative_to(cursor).parts:
        cursor = cursor / part
        if cursor.is_symlink() or getattr(cursor, 'is_junction', lambda: False)():
            raise ValueError('Linked native candidate dependency is not accepted')
    return result


def native_scope(query):
    scope = Scope.from_policy(query)
    if query.get('seed_asset') != NATIVE or query.get('seed_event', {}).get('asset') != NATIVE:
        raise ValueError('Only a frozen native seed is supported')
    if scope.scope_id != query.get('scope_id') or scope.scope_hash != query.get('scope_hash'):
        raise ValueError('Frozen native Scope identity differs')
    if any(query.get(k) for k in ('certified_conversions', 'protocol_conversions', 'conversion_edges', 'weth_certification')):
        raise ValueError('Certified cross-asset conversion cannot use native-only SQL')
    return scope


def native_sql(pending, maximum=32):
    """Remove exactly the known token branch; preserve every native SQL byte."""
    full, selected = build_sql(pending, maximum)
    if full.count(TOKEN_MARKER) != 1 or not full.endswith(ENDING):
        raise ValueError('Inherited candidate SQL grammar changed')
    prefix, token = full.split(TOKEN_MARKER)
    if 'FROM erc20_ethereum.evt_Transfer e' not in token or 'UNION ALL' in token:
        raise ValueError('Exactly the original ERC20 branch must be removed')
    native = prefix + '\n' + ENDING
    if "SELECT 'erc20'" in native or 'erc20_ethereum.' in native:
        raise ValueError('Unexpected remaining token branch')
    return native, full, selected


def source_plan(work, source_job):
    """Pure local proof; the completed old source execution is never replayed."""
    work = Path(work).resolve(); folder = inside(work, source_job)
    state = read(folder / 'job.json')
    if state.get('state') != 'QUERY_STATE_COMPLETED' or not state.get('execution_id'):
        raise ValueError('Retained completed all-asset source execution is required')
    source_freeze = inside(work, state['scope_freeze_path'])
    if sha(source_freeze) != state.get('scope_freeze_sha256'):
        raise ValueError('Original SQL freeze changed')
    frozen = Runtime().verify_sql_freeze(source_freeze, work, sql_path=folder / 'query.sql')
    if frozen.get('kind') != 'candidate' or len(frozen.get('query_ids', [])) != 1:
        raise ValueError('Only one frozen candidate query is supported')
    query = next(q for q in batch_for_scope(work, frozen['scope_id'], frozen['scope_hash'])['queries'] if q['query_id'] == frozen['query_ids'][0])
    scope = native_scope(query)
    if frozen['scope_hash'] != scope.scope_hash or frozen['scope_id'] != scope.scope_id:
        raise ValueError('Original and optimized Scope differ')
    intervals = frozen['intervals']
    if not 1 <= len(intervals) <= 32 or any(i.get('query_id') != query['query_id'] or i.get('kind') != 'candidate' or i.get('asset') != NATIVE for i in intervals):
        raise ValueError('One to32 exact native candidate rectangles are required')
    sql, original, selected = native_sql(intervals, len(intervals))
    if len(selected) != len(intervals) or any(any(s[k] != i[k] for k in ('address', 'asset', *FIELDS)) for s, i in zip(selected, intervals)):
        raise ValueError('Original ordered rectangle list differs')
    if sorted(set(frozen['addresses'])) != sorted({i['address'] for i in intervals}):
        raise ValueError('Original frozen addresses differ')
    for i in intervals:
        if not (scope.start_block <= i['start_block'] <= i['end_block'] <= scope.end_block and
                scope.start_time <= i['start_time'] <= i['end_time'] <= scope.end_time):
            raise ValueError('Optimized interval exceeds original Scope')
    original_sha = hashlib.sha256(original.encode()).hexdigest()
    if original_sha != state['sql_sha256'] or sha(folder / 'query.sql') != original_sha:
        raise ValueError('Original exact batch SQL does not regenerate')
    body, receipt = state['status_response'], state['status_receipt']
    bound_receipt(work, body, receipt)
    if receipt.get('operation') != 'status' or receipt.get('http_status') != 200 or receipt.get('error_class') or body.get('execution_id') != state['execution_id'] or body.get('state') != 'QUERY_STATE_COMPLETED':
        raise ValueError('Bound original completed status required')
    metadata = body['result_metadata']
    if metadata['column_names'] != FULL_COLUMNS or len(metadata['column_types']) != 16:
        raise ValueError('Original all-asset schema differs')
    proof = {'schema_version': SCHEMA, 'query_id': query['query_id'], 'query_name': query['name'],
        'scope_id': scope.scope_id, 'scope_hash': scope.scope_hash, 'window_mode': scope.window_mode,
        'source_job_folder': folder.relative_to(work).as_posix(), 'source_execution_id': state['execution_id'],
        'source_sql_sha256': original_sha, 'source_sql_freeze_sha256': sha(source_freeze),
        'source_status_raw_sha256': receipt['sha256'], 'source_all_asset_metadata': metadata,
        'source_execution_cost_credits': body.get('execution_cost_credits'),
        'native_sql_sha256': hashlib.sha256(sql.encode()).hexdigest(), 'intervals': intervals,
        'coverage_capability': CAPABILITY, 'all_asset_export_complete': False,
        'same_scope_and_native_prefix': True, 'original_financial_facts_retained': True,
        'raw_all_asset_rows_are_not_native_candidates': True, 'native_total_rows_before_new_execution': None,
        'removed_branch': 'ERC20 index rows only; no certified conversion exists in this frozen instance',
        'retained_context_requirement': 'Independent full native whole-block ledger, receipts, balances and necessary protocol/trace evidence remain required',
        'no_native_amount_success_address_time_filter_added': True,
        'no_sampling_or_result_limit': True, 'new_execution_requires_existing_shared_budget': True}
    return query, sql, proof, state


def prepare(work, source_job):
    """Write immutable local evidence/new SQL freeze; no research or ledger call."""
    work = Path(work).resolve()
    query, sql, proof, state = source_plan(work, source_job)
    root = work / 'private/stage1d_native_candidate_plans' / proof['native_sql_sha256']
    source = inside(work, source_job)
    # Snapshot source state: subsequent legitimate fee observations cannot erase
    # the original completed execution evidence behind this optimization.
    snapshots = {'source_job.json': state, 'NATIVE_SCOPE_DECISION.json': proof}
    for name, value in snapshots.items():
        p = root / name
        if p.exists() and read(p) != value:
            raise ValueError('Immutable native optimization evidence changed')
        if not p.exists(): atomic_json(p, value)
    dependencies = []
    for path in (root / 'source_job.json', root / 'NATIVE_SCOPE_DECISION.json', source / 'query.sql',
                 inside(work, state['scope_freeze_path']), inside(work, state['status_receipt']['raw_path']),
                 work / 'logs' / (state['status_receipt']['request_id'] + '.json')):
        dependencies.append({'path': path.relative_to(work).as_posix(), 'sha256': sha(path)})
    original_freeze = read(inside(work, state['scope_freeze_path']))
    dependencies += [d for d in original_freeze['dependencies'] if d not in dependencies]
    freeze = save_sql(work, sql, 'candidate', query, proof['intervals'], dependencies,
                      sorted({i['address'] for i in proof['intervals']}))
    saved_freeze = read(freeze)
    if (saved_freeze.get('query_ids') != [query['query_id']] or saved_freeze.get('scope_hash') != query['scope_hash']
            or saved_freeze.get('scope_id') != query['scope_id'] or saved_freeze.get('intervals') != proof['intervals']
            or any(d not in saved_freeze.get('dependencies', []) for d in dependencies)):
        raise ValueError('Existing native SQL freeze does not retain this exact optimization proof')
    return {'query': query, 'freeze_path': str(freeze), 'decision_path': str(root / 'NATIVE_SCOPE_DECISION.json'),
            'status': 'PREPARED_NATIVE_SQL_NO_NETWORK', 'proof': proof}


def verify_capability_record(work, record):
    if record.get('coverage_capability') != CAPABILITY:
        return
    if (record.get('asset') != NATIVE or record.get('all_asset_export_complete') is not False
            or type(record.get('complete')) is not bool or record.get('native_scope_complete') is not record.get('complete')
            or len(record.get('addresses', [])) != 1):
        raise ValueError('Native-only cache cannot claim other-asset completeness')
    path = inside(work, record['native_decision_path'])
    if sha(path) != record.get('native_decision_sha256'):
        raise ValueError('Native-only decision changed')
    proof = read(path)
    if proof.get('schema_version') != SCHEMA or proof.get('coverage_capability') != CAPABILITY or proof.get('native_sql_sha256') != record.get('evidence_id'):
        raise ValueError('Native-only capability proof differs')
    rectangle = {k: record[k] for k in FIELDS}
    if not any(i['asset'] == NATIVE and i['address'] in record['addresses'] and all(i[k] == rectangle[k] for k in FIELDS) for i in proof['intervals']):
        raise ValueError('Native-only cache claims an unfrozen rectangle')


def import_result(work, prepared, result):
    """Import only the complete16-column native execution, one file per batch."""
    work = Path(work).resolve(); proof = prepared['proof']; query = prepared['query']
    if result.get('status') != 'COMPLETED_EXPORTED':
        return result
    decision = inside(work, prepared['decision_path'])
    if read(decision) != proof or sha(inside(work, prepared['freeze_path']).parent / 'query.sql') != proof['native_sql_sha256']:
        raise ValueError('Native candidate preparation changed')
    job = inside(work, result['job_folder']); state = read(job / 'job.json')
    if state['sql_sha256'] != proof['native_sql_sha256'] or state['execution_id'] == proof['source_execution_id']:
        raise ValueError('New native execution identity differs')
    raw = result_rows(work, job)
    if any(set(r) != set(FULL_COLUMNS) or r.get('event_kind') not in ('top', 'internal') or r.get('log_index') is not None or r.get('contract_address') is not None for r in raw):
        raise ValueError('Native SQL result contains non-native rows or nonconstant columns')
    events, gaps = normalize_rows(raw)
    folder = work / 'derived/stage1d/intervals'; eid = proof['native_sql_sha256']
    ep = folder / (eid + '.events.json'); payload = [asdict(e) for e in events]
    if ep.exists() and read(ep) != payload: raise ValueError('Immutable native physical events changed')
    if not ep.exists(): atomic_json(ep, payload)
    for interval in proof['intervals']:
        rectangle = {k: interval[k] for k in ('address', 'asset', *FIELDS)}
        record = {'evidence_id': eid, 'addresses': [interval['address']], 'asset': NATIVE,
            **{k: interval[k] for k in FIELDS}, 'complete': not gaps, 'normalization_gaps': gaps,
            'native_scope_complete': not gaps, 'all_asset_export_complete': False, 'coverage_capability': CAPABILITY,
            'native_decision_path': decision.relative_to(work).as_posix(), 'native_decision_sha256': sha(decision),
            'raw_rows': len(raw), 'events_path': ep.relative_to(work).as_posix(), 'events_sha256': sha(ep),
            'query_id_at_acquisition': query['query_id'], 'scope_id_at_acquisition': query['scope_id'],
            'job_folder': job.relative_to(work).as_posix(), 'coverage_basis': 'ONE_EXACT_REQUEST_RECTANGLE_NOT_BATCH_ENVELOPE',
            'shared_physical_content_identity': eid, 'batch_rectangle_count': len(proof['intervals'])}
        verify_capability_record(work, record)
        target = folder / (digest({'sql_sha256': eid, 'rectangle': rectangle}) + '.coverage.json')
        if target.exists() and read(target) != record: raise ValueError('Immutable native rectangle coverage changed')
        if not target.exists(): atomic_json(target, record)
    return dict(result, coverage_capability=CAPABILITY, native_scope_complete=not gaps,
                all_asset_export_complete=False, raw_rows=len(raw), unique_events=len(events), normalization_gaps=gaps)


def run(work, source_job):
    prepared = prepare(work, source_job)
    result = execute_sql(work, prepared['freeze_path'], prepared['query']['name'],
                         'candidate_native_exact_' + prepared['query']['name'])
    return import_result(work, prepared, result)


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('--work', type=Path, required=True)
    p.add_argument('--source-job', required=True); p.add_argument('--execute', action='store_true')
    a = p.parse_args()
    out = run(a.work, a.source_job) if a.execute else prepare(a.work, a.source_job)
    print(json.dumps({k: v for k, v in out.items() if k not in ('query', 'proof')}, sort_keys=True))
