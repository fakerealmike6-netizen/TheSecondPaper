"""Evidence-bound computational splits, never a window or asset fallback.

No request or budget mutation occurs here. The scheduler intersects persistent
child rectangles with the current uncached frontier. Its geometric subtraction
is a work plan only; it never writes a successful coverage claim.
"""
import argparse
import hashlib
import json
from pathlib import Path
from stage1d_closure_scope import active_batch

from collector import NATIVE
from context_access_r3 import read, sha
from page_attempts import atomic_json
from stage1d_export import FULL_COLUMNS, bound_receipt
from stage1d_native_candidate import inside, native_scope, native_sql
from stage1d_runtime import Runtime

SCHEMA = 'stage1d-native-computation-split-v1'
FIELDS = ('address', 'asset', 'start_block', 'end_block', 'start_time', 'end_time')
INDEX_ROW_LIMIT = 25000
RAW_CAP = 536870912


def rectangle(row): return {k: row[k] for k in FIELDS}


def sql_id(group): return hashlib.sha256(native_sql(group, len(group))[0].encode()).hexdigest()


def divide(group):
    """Exact finite set division independent of values, labels or hits."""
    group = [rectangle(r) for r in group]
    if not group: raise ValueError('No native rectangle to split')
    if len(group) > 1:
        mid = len(group) // 2
        return 'ORIGINAL_RECTANGLE_GROUP_BISECTION', [group[:mid], group[mid:]], None
    parent = group[0]
    if parent['start_block'] == parent['end_block']:
        return 'UNSPLITTABLE_SINGLE_BLOCK', [], None
    mid = (parent['start_block'] + parent['end_block']) // 2
    left = dict(parent, end_block=mid); right = dict(parent, start_block=mid + 1)
    return 'INTEGER_BLOCK_BISECTION', [[left], [right]], mid


def proof_of_union(parent, mode, children, midpoint):
    expected = divide(parent)
    if (mode, children, midpoint) != expected:
        raise ValueError('Computation split is not the deterministic exact union')
    return {'exact_parent_union': mode != 'UNSPLITTABLE_SINGLE_BLOCK',
            'single_block_gap_retained': mode == 'UNSPLITTABLE_SINGLE_BLOCK',
            'time_bounds_unchanged': True, 'scope_depth_seed_asset_unchanged': True,
            'selection_uses_amount_label_or_recall': False,
            'cut_rule': 'Preserve original rectangle order; split group at count//2, otherwise integer block midpoint; no estimated block timestamps.'}


def trigger_proof(work, query, job_folder):
    work = Path(work).resolve(); scope = native_scope(query)
    batch = active_batch(work)
    if [q for q in batch['queries'] if q.get('query_id') == query['query_id']] != [query]:
        raise ValueError('Registered query identity differs')
    job = inside(work, job_folder); state = read(job / 'job.json')
    frozen_path = inside(work, state['scope_freeze_path'])
    if sha(frozen_path) != state.get('scope_freeze_sha256'): raise ValueError('Source freeze changed')
    frozen = Runtime().verify_sql_freeze(frozen_path, work, sql_path=job / 'query.sql')
    if (frozen.get('kind') != 'candidate' or frozen.get('query_ids') != [query['query_id']]
            or frozen.get('scope_hash') != scope.scope_hash or frozen.get('scope_id') != scope.scope_id):
        raise ValueError('Only the same frozen native candidate query may be split')
    group = [rectangle(r) for r in frozen['intervals']]
    if not 1 <= len(group) <= 32 or any(r['asset'] != NATIVE for r in group):
        raise ValueError('Exact native rectangles required')
    if sql_id(group) != state['sql_sha256'] or sha(job / 'query.sql') != state['sql_sha256']:
        raise ValueError('Only exact regenerated native candidate SQL may be split')
    body, receipt = state['status_response'], state['status_receipt']
    bound_receipt(work, body, receipt)
    if (receipt.get('operation') != 'status' or receipt.get('http_status') != 200 or receipt.get('error_class')
            or body.get('execution_id') != state.get('execution_id') or body.get('state') != state.get('state')):
        raise ValueError('A verified original terminal status is required')
    md = body.get('result_metadata', {})
    if state['state'] == 'QUERY_STATE_COMPLETED':
        if md.get('column_names') != FULL_COLUMNS or len(md.get('column_types', [])) != 16:
            raise ValueError('Native16 completed metadata required')
        total = md.get('total_row_count'); size = md.get('total_result_set_bytes')
        if type(total) is not int or type(size) is not int or total < 0 or size < 0:
            raise ValueError('Exact terminal metadata counts required')
        if total <= INDEX_ROW_LIMIT and size <= RAW_CAP:
            raise ValueError('No evidenced complete-result size trigger; do not split for a timeout or outcome')
        reason = 'COMPLETED_NATIVE_INDEX_ROW_LIMIT' if total > INDEX_ROW_LIMIT else 'COMPLETED_RAW_BYTES_LIMIT'
    elif state['state'] == 'QUERY_STATE_FAILED' and body.get('error', {}).get('type') == 'FAILED_TYPE_RESOURCES_CAP_REACHED':
        reason = 'VERIFIED_SINGLE_EXECUTION_RESOURCE_CAP'
    else:
        raise ValueError('Timeout, unknown submission and ordinary failures do not authorize computation splitting')
    mode, children, midpoint = divide(group)
    dependencies = [
        {'path': frozen_path.relative_to(work).as_posix(), 'sha256': sha(frozen_path)},
        {'path': (job / 'query.sql').relative_to(work).as_posix(), 'sha256': sha(job / 'query.sql')},
        {'path': receipt['raw_path'], 'sha256': receipt['sha256']},
        {'path': 'logs/' + receipt['request_id'] + '.json', 'sha256': sha(work / 'logs' / (receipt['request_id'] + '.json'))},
    ]
    return {'schema_version': SCHEMA, 'query_id': query['query_id'], 'scope_id': scope.scope_id,
        'scope_hash': scope.scope_hash, 'window_mode': scope.window_mode,
        'source_job_folder': job.relative_to(work).as_posix(), 'source_sql_sha256': state['sql_sha256'],
        'source_execution_id': state['execution_id'], 'trigger': reason,
        'original_terminal_body': body, 'original_status_receipt': receipt,
        'source_execution_cost_credits': body.get('execution_cost_credits'), 'dependencies': dependencies,
        'parent_rectangles': group, 'mode': mode, 'children_groups': children, 'midpoint_block': midpoint,
        'child_sql_sha256': [sql_id(g) for g in children],
        'union_proof': proof_of_union(group, mode, children, midpoint),
        'raw_index_row_count_is_not_propagated_candidate_count': True,
        'original_risk_and_clock_retained': True, 'coverage_claim_written': False, 'network_requests': 0}


def register(work, query, job_folder):
    current = read(inside(work, job_folder) / 'job.json')
    path = Path(work) / 'private/stage1d_native_computation_splits' / query['scope_hash'] / (current['sql_sha256'] + '.json')
    if path.exists():
        old = verify(work, query, path)
        return {'path': path.relative_to(Path(work)).as_posix(), 'sha256': sha(path),
                'mode': old['mode'], 'source_sql_sha256': old['source_sql_sha256']}
    proof = trigger_proof(work, query, job_folder)
    if path.exists() and read(path) != proof:
        raise ValueError('Immutable computation split proof changed')
    if not path.exists(): atomic_json(path, proof)
    return {'path': path.relative_to(Path(work)).as_posix(), 'sha256': sha(path),
            'mode': proof['mode'], 'source_sql_sha256': proof['source_sql_sha256']}


def verify(work, query, path):
    path = inside(work, path); proof = read(path)
    if (proof.get('schema_version') != SCHEMA or proof.get('query_id') != query['query_id']
            or proof.get('scope_id') != query['scope_id'] or proof.get('scope_hash') != query['scope_hash']):
        raise ValueError('Persistent computation split Scope differs')
    # Source job may gain later fee observations. Recheck immutable SQL/status
    # evidence rather than rewriting this original terminal snapshot.
    for dep in proof['dependencies']:
        if sha(inside(work, dep['path'])) != dep['sha256']: raise ValueError('Computation split dependency changed')
    bound_receipt(Path(work), proof['original_terminal_body'], proof['original_status_receipt'])
    frozen = Runtime().verify_sql_freeze(inside(work, proof['dependencies'][0]['path']), work,
                                         sql_path=inside(work, proof['dependencies'][1]['path']))
    if (frozen.get('kind') != 'candidate' or frozen.get('query_ids') != [query['query_id']]
            or frozen.get('scope_hash') != query['scope_hash'] or frozen.get('scope_id') != query['scope_id']
            or frozen.get('sql_sha256') != proof['source_sql_sha256']
            or [rectangle(r) for r in frozen['intervals']] != proof['parent_rectangles']):
        raise ValueError('Computation parents differ from the original frozen SQL')
    if proof['original_terminal_body'].get('execution_id') != proof['source_execution_id']:
        raise ValueError('Computation execution identity differs')
    if proof['source_sql_sha256'] != sql_id(proof['parent_rectangles']): raise ValueError('Source native SQL identity differs')
    if proof['child_sql_sha256'] != [sql_id(g) for g in proof['children_groups']]: raise ValueError('Child SQL identities differ')
    if proof['union_proof'] != proof_of_union(proof['parent_rectangles'], proof['mode'], proof['children_groups'], proof['midpoint_block']):
        raise ValueError('Computation union proof changed')
    body = proof['original_terminal_body']; md = body.get('result_metadata', {})
    valid = ((proof['trigger'] == 'COMPLETED_NATIVE_INDEX_ROW_LIMIT' and body.get('state') == 'QUERY_STATE_COMPLETED' and md.get('total_row_count', 0) > INDEX_ROW_LIMIT)
             or (proof['trigger'] == 'COMPLETED_RAW_BYTES_LIMIT' and body.get('state') == 'QUERY_STATE_COMPLETED' and md.get('total_result_set_bytes', 0) > RAW_CAP)
             or (proof['trigger'] == 'VERIFIED_SINGLE_EXECUTION_RESOURCE_CAP' and body.get('state') == 'QUERY_STATE_FAILED' and body.get('error', {}).get('type') == 'FAILED_TYPE_RESOURCES_CAP_REACHED'))
    if not valid: raise ValueError('Persistent split no longer has its exact resource evidence')
    return proof


def intersection(left, right):
    if left['address'] != right['address'] or left['asset'] != right['asset']: return None
    lo, hi = max(left['start_block'], right['start_block']), min(left['end_block'], right['end_block'])
    start, end = max(left['start_time'], right['start_time']), min(left['end_time'], right['end_time'])
    if lo > hi or start > end: return None
    return dict(address=left['address'], asset=left['asset'], start_block=lo, end_block=hi, start_time=start, end_time=end)


def schedule(work, query, pending, maximum=32):
    """Return one eligible child group; never recombine a known failed parent."""
    from stage1d_window import missing_rectangles
    if type(maximum) is not int or not 1 <= maximum <= 32: raise ValueError('Finite rectangle limit required')
    scope = native_scope(query); plans = {}; refs = {}
    root = Path(work) / 'private/stage1d_native_computation_splits' / scope.scope_hash
    for path in root.glob('*.json'):
        proof = verify(work, query, path); key = proof['source_sql_sha256']; plans[key] = proof
        refs[key] = {'path': path.relative_to(Path(work)).as_posix(), 'sha256': sha(path)}
    blocked = []
    single_blocks = []
    for key, plan in plans.items():
        if plan['mode'] != 'UNSPLITTABLE_SINGLE_BLOCK': continue
        for parent in plan['parent_rectangles']:
            single_blocks.append(dict(parent, complete=True))
            for row in pending:
                part = intersection(parent, row)
                if part:
                    blocked.append(dict(part, reason='UNSPLITTABLE_NATIVE_SINGLE_BLOCK_RESOURCE_GAP', split_proof=refs[key]))
    eligible_pending = [part for row in pending for part in missing_rectangles(row, single_blocks)]
    def subset(group):
        out = []
        for parent in group:
            for row in eligible_pending:
                part = intersection(parent, row)
                if part and part not in out: out.append(part)
        return out
    def walk(key, chain):
        if key in chain: raise ValueError('Computation split cycle')
        plan = plans[key]; chain = chain + [key]
        if plan['mode'] == 'UNSPLITTABLE_SINGLE_BLOCK':
            return None
        for group in plan['children_groups']:
            child_key = sql_id(group)
            if child_key in plans:
                result = walk(child_key, chain)
                if result: return result
                continue
            selected = subset(group)[:maximum]
            if not selected: continue
            selected_key = sql_id(selected)
            if selected_key in plans:
                result = walk(selected_key, chain)
                if result: return result
                continue
            return {'selected': selected, 'dependencies': [refs[k] for k in chain], 'resource_gaps': list(blocked)}
        return None
    children = {k for p in plans.values() for k in p['child_sql_sha256']}
    roots = [k for k in plans if k not in children]
    # Most-specific proven failed subsets take precedence over an overlapping
    # earlier parent. Geometry is deterministic; result values/labels never
    # choose a cut or a processing priority.
    roots.sort(key=lambda k: (sum((r['end_block']-r['start_block']+1)*(r['end_time']-r['start_time']+1)
                                  for r in plans[k]['parent_rectangles']),
                               min((r['start_block'], r['start_time'], r['address']) for r in plans[k]['parent_rectangles'])))
    for key in roots:
        result = walk(key, [])
        if result: return result
    # The parent areas below are *scheduled*, not data-complete. Their pending
    # children were handled above. Subtract only to find unrelated work; no
    # coverage record is emitted by this function.
    parents = [dict(r, complete=True) for k in roots for r in plans[k]['parent_rectangles']]
    unrelated = []
    for row in pending:
        for part in missing_rectangles(row, parents):
            if part not in unrelated: unrelated.append(part)
    return {'selected': unrelated[:maximum], 'dependencies': [], 'resource_gaps': blocked}


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('--work', type=Path, required=True)
    p.add_argument('--query', required=True); p.add_argument('--source-job', required=True); a = p.parse_args()
    q = next(q for q in active_batch(a.work)['queries'] if q['name'] == a.query)
    print(json.dumps(register(a.work, q, a.source_job), sort_keys=True))
