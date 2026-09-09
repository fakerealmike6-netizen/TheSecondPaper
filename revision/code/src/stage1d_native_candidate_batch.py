"""Stage1D native candidate batches on registered exact ETH scopes only.

The historical all-asset batch path is unchanged. Native16 is an explicit input
capability, never a token ledger or a replacement for whole-block context.
"""
import argparse
import hashlib
import json
from pathlib import Path
from stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha

from context_access_r3 import read, sha
from page_attempts import atomic_json
from stage1d_acquisition import save_sql, replay, acquire_labels
from stage1d_runtime import Runtime, execute_sql
from stage1d_native_candidate import (CAPABILITY, SCHEMA, inside, native_scope,
                                      native_sql, import_result)


def registered_query(work, query):
    batch = active_batch(work)
    registered = [q for q in batch['queries'] if q['query_id'] == query.get('query_id')]
    if len(registered) != 1 or registered[0] != query:
        raise ValueError('Exact registered Stage1D query is required')
    return native_scope(query)


def archive_query_files(work, query):
    """Retain prior successful replay evidence before updating current aliases."""
    work = Path(work).resolve(); source = work / 'derived/stage1d/queries' / query['name']
    archived = []
    for name in ('collection.json', 'pending_intervals.json', 'label_snapshot.json',
                 'ACQUISITION_STATUS.json', 'NATIVE_ACQUISITION_STATUS.json', 'acquisition_attempts.json'):
        path = source / name
        if not path.exists(): continue
        data = path.read_bytes(); digest = hashlib.sha256(data).hexdigest()
        target = work / 'private/stage1d_native_batch_history' / (digest + '.json')
        if target.exists() and target.read_bytes() != data:
            raise ValueError('Content-addressed query history conflict')
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(data)
        archived.append({'original_path': path.relative_to(work).as_posix(),
                         'snapshot_path': target.relative_to(work).as_posix(), 'sha256': digest})
    return archived


def prepare(work, query, pending, collection_path, maximum=32, split_dependencies=()):
    """Local preparation only; reuse a prior recovery proof when SQL is equal."""
    work = Path(work).resolve(); scope = registered_query(work, query)
    from stage1d_native_candidate_split import verify as verify_split
    for dep in split_dependencies:
        if sha(inside(work, dep['path'])) != dep['sha256']: raise ValueError('Scheduled split proof changed')
        verify_split(work, query, dep['path'])
    collection_path = inside(work, collection_path); collection = read(collection_path)
    if collection.get('query_id') != query['query_id']:
        raise ValueError('Actual frontier query identity differs')
    metrics = collection.get('metrics', {})
    if metrics.get('scope_hash') != scope.scope_hash or metrics.get('scope_id') != scope.scope_id:
        raise ValueError('Actual frontier frozen Scope differs')
    sql, original_sql, selected = native_sql(pending, maximum)
    for row in selected:
        if not (scope.start_block <= row['start_block'] <= row['end_block'] <= scope.end_block
                and scope.start_time <= row['start_time'] <= row['end_time'] <= scope.end_time):
            raise ValueError('Exact native rectangle escapes the registered Scope')
    intervals = [dict(row, query_id=query['query_id'], kind='candidate') for row in selected]
    sid = hashlib.sha256(sql.encode()).hexdigest()
    root = work / 'private/stage1d_native_candidate_plans' / sid
    decision = root / 'NATIVE_SCOPE_DECISION.json'
    freeze = work / 'private/stage1d_sql' / sid / 'freeze_manifest.json'
    if decision.exists() and (read(decision).get('scope_id')!=scope.scope_id or read(decision).get('scope_hash')!=scope.scope_hash):
        root=root/'scope_claims'/scope.scope_hash
        decision=root/'NATIVE_SCOPE_DECISION.json'
        freeze=work/'private/stage1d_sql'/sid/'scope_claims'/scope.scope_hash/'freeze_manifest.json'
    if decision.exists():
        proof = read(decision)
        if (proof.get('schema_version') != SCHEMA or proof.get('coverage_capability') != CAPABILITY
                or proof.get('native_sql_sha256') != sid or proof.get('query_id') != query['query_id']
                or proof.get('scope_id') != scope.scope_id or proof.get('scope_hash') != scope.scope_hash
                or proof.get('intervals') != intervals or proof.get('all_asset_export_complete') is not False):
            raise ValueError('Existing immutable native proof differs')
        if not freeze.exists():
            raise ValueError('Incomplete prior preparation requires local recovery; never submit a new alias')
        frozen = Runtime().verify_sql_freeze(freeze, work, sql_path=freeze.parent / 'query.sql')
        decision_dep = {'path': decision.relative_to(work).as_posix(), 'sha256': sha(decision)}
        if (frozen.get('query_ids') != [query['query_id']] or frozen.get('scope_hash') != scope.scope_hash
                or frozen.get('intervals') != intervals or decision_dep not in frozen['dependencies']):
            raise ValueError('Prior native freeze does not bind this proof')
        return {'query': query, 'freeze_path': str(freeze), 'decision_path': str(decision),
                'status': 'REUSED_PREPARED_NATIVE_SQL_NO_NETWORK', 'proof': proof,
                'scheduler_dependencies': list(split_dependencies)}
    frontier_sha = sha(collection_path)
    frontier = work / 'private/stage1d_frontier_evidence' / (frontier_sha + '.json')
    if frontier.exists() and sha(frontier) != frontier_sha: raise ValueError('Immutable frontier changed')
    if not frontier.exists():
        frontier.parent.mkdir(parents=True, exist_ok=True); frontier.write_bytes(collection_path.read_bytes())
    if sha(frontier) != frontier_sha: raise ValueError('Frontier changed during snapshot')
    proof = {'schema_version': SCHEMA, 'origin': 'DIRECT_FROZEN_NATIVE_FRONTIER',
        'query_id': query['query_id'], 'query_name': query['name'], 'scope_id': scope.scope_id,
        'scope_hash': scope.scope_hash, 'window_mode': scope.window_mode,
        'source_execution_id': None, 'source_sql_sha256': hashlib.sha256(original_sql.encode()).hexdigest(),
        'source_sql_generated_for_equivalence_only_not_submitted': True,
        'frontier_evidence_sha256': frontier_sha, 'native_sql_sha256': sid, 'intervals': intervals,
        'coverage_capability': CAPABILITY, 'all_asset_export_complete': False,
        'same_scope_and_native_prefix': True, 'original_financial_facts_retained': True,
        'native_total_rows_before_new_execution': None, 'raw_index_rows_are_not_propagated_candidate_count': True,
        'removed_branch': 'ERC20 index rows only; registered native seed has no certified conversion',
        'retained_context_requirement': 'Independent whole-block native ledger, receipt, balances and necessary protocol evidence remain required',
        'computation_split_dependencies': list(split_dependencies),
        'no_native_amount_success_address_time_filter_added': True,
        'no_sampling_or_result_limit': True, 'new_execution_requires_existing_shared_budget': True}
    atomic_json(decision, proof)
    dependencies = [{'path': frontier.relative_to(work).as_posix(), 'sha256': frontier_sha},
                    {'path': decision.relative_to(work).as_posix(), 'sha256': sha(decision)},
                    {'path': active_batch_path(work).relative_to(work).as_posix(), 'sha256': sha(active_batch_path(work))}]
    dependencies += [d for d in split_dependencies if d not in dependencies]
    freeze = save_sql(work, sql, 'candidate', query, intervals, dependencies, sorted({r['address'] for r in selected}))
    frozen = read(freeze)
    if (frozen.get('query_ids') != [query['query_id']] or frozen.get('scope_hash') != scope.scope_hash
            or frozen.get('intervals') != intervals or any(d not in frozen.get('dependencies', []) for d in dependencies)):
        raise ValueError('Existing SQL freeze differs from direct native proof')
    return {'query': query, 'freeze_path': str(freeze), 'decision_path': str(decision),
            'status': 'PREPARED_NATIVE_SQL_NO_NETWORK', 'proof': proof}


def acquire(work, query, pending, collection_path, maximum=32, split_dependencies=()):
    prepared = prepare(work, query, pending, collection_path, maximum, split_dependencies)
    job = Path(work) / 'private/dune_r2_jobs' / prepared['proof']['native_sql_sha256']
    try:
        result = execute_sql(work, prepared['freeze_path'], query['name'], 'candidate_native_exact_' + query['name'])
        result = import_result(work, prepared, result)
    except Exception as exc:
        result = {'status': 'ACQUISITION_PARTIAL', 'error_class': type(exc).__name__, 'reason': str(exc),
                  'failure_stage': 'NATIVE_CANDIDATE_QUERY_OR_COMPLETE_EXPORT',
                  'complete_native_index': False, 'propagated_candidate_cap_exceeded': None,
                  'raw_index_rows_are_not_propagated_candidate_count': True}
        if (job / 'job.json').exists():
            state = read(job / 'job.json'); md = state.get('status_response', {}).get('result_metadata', {})
            result.update(execution_id=state.get('execution_id'), native_index_metadata=md,
                          retained_job_state=state.get('state'))
    if result.get('status') != 'COMPLETED_EXPORTED' and (job / 'job.json').exists():
        from stage1d_native_candidate_split import register as register_split
        try:
            result['computation_split_plan'] = register_split(work, query, job)
        except (ValueError, KeyError) as exc:
            result['computation_split_not_registered'] = {'reason': str(exc), 'error_class': type(exc).__name__}
    return dict(result, phase='candidate_native_exact', job_folder=str(job),
        sql_sha256=prepared['proof']['native_sql_sha256'], exact_rectangles=len(prepared['proof']['intervals']),
        decision_path=prepared['decision_path'], coverage_capability=CAPABILITY,
        scheduler_dependencies=list(split_dependencies),
        all_asset_export_complete=False)


def run(work, query, max_batches=1, maximum=32):
    work = Path(work).resolve(); registered_query(work, query)
    if isinstance(max_batches, bool) or not isinstance(max_batches, int) or not 1 <= max_batches <= 100:
        raise ValueError('One to100 finite batches per invocation are allowed')
    if isinstance(maximum, bool) or not isinstance(maximum, int) or not 1 <= maximum <= 32:
        raise ValueError('One to32 exact rectangles per batch are allowed')
    folder = work / 'derived/stage1d/queries' / query['name']; folder.mkdir(parents=True, exist_ok=True)
    attempts_path = folder / 'acquisition_attempts.json'
    attempts = read(attempts_path) if attempts_path.exists() else []
    completed = 0; stop_reason = 'INVOCATION_BATCH_BOUNDARY'; previous_sid = None; split_gaps = []
    for _ in range(max_batches):
        archive_query_files(work, query)
        collection, pending = replay(work, query)
        if collection['status'] == 'INCOMPLETE_RESOURCE_LIMIT' or collection.get('fact_conflicts'):
            stop_reason = collection['status']; break
        try:
            label = acquire_labels(work, query, collection)
            if label.get('new_addresses') or label.get('labels_updated'):
                attempts.append(dict(label, phase='labels')); atomic_json(attempts_path, attempts)
                archive_query_files(work, query); collection, pending = replay(work, query)
        except Exception as exc:
            attempts.append({'phase': 'labels', 'status': 'LABEL_LOOKUP_FAILED_OR_RESOURCE_BLOCKED',
                             'error_class': type(exc).__name__, 'reason': str(exc)})
            atomic_json(attempts_path, attempts)
        if collection['status'] == 'INCOMPLETE_RESOURCE_LIMIT' or collection.get('fact_conflicts'):
            stop_reason = collection['status']; break
        if not pending: stop_reason = 'NO_UNCOVERED_NATIVE_RECTANGLES'; break
        if Runtime().raw_risk(work) >= Runtime().raw_limit(work): stop_reason = 'RAW_RESOURCE_LIMIT'; break
        from stage1d_native_candidate_split import schedule
        scheduled = schedule(work, query, pending, maximum); selected = scheduled['selected']
        split_gaps = scheduled['resource_gaps']
        if not selected:
            stop_reason = 'UNSPLITTABLE_NATIVE_INDEX_RESOURCE_GAP' if split_gaps else 'NO_ELIGIBLE_COMPUTATION_RECTANGLE'
            break
        current_sid = hashlib.sha256(native_sql(selected, len(selected))[0].encode()).hexdigest()
        if current_sid == previous_sid:
            stop_reason = 'SAME_COMPLETE_SQL_NO_FRONTIER_PROGRESS'; break
        try:
            result = acquire(work, query, selected, folder / 'collection.json', len(selected),
                             split_dependencies=scheduled['dependencies'])
        except Exception as exc:
            result = {'phase': 'candidate_native_exact', 'status': 'ACQUISITION_PARTIAL',
                      'error_class': type(exc).__name__, 'reason': str(exc)}
        attempts.append(result); atomic_json(attempts_path, attempts)
        if result.get('status') != 'COMPLETED_EXPORTED' or result.get('native_scope_complete') is not True:
            stop_reason = result['status']; break
        completed += 1
        previous_sid = result.get('sql_sha256')
    archive_query_files(work, query)
    collection, pending = replay(work, query)
    status = {'name': query['name'], 'query_id': query['query_id'], 'scope_id': query['scope_id'],
        'window_mode': query['window_mode'], 'max_depth': query['max_acquisition_depth'],
        'channel': 'NATIVE_ONLY_EXACT_CANDIDATE_BATCHES', 'collection_status': collection['status'],
        'stop_reason': stop_reason, 'completed_batches_this_invocation': completed,
        'pending_intervals': len(pending), 'candidate_events': len(collection['candidate_events']),
        'propagated_candidate_event_count': collection.get('metrics', {}).get('candidate_event_count'),
        'candidate_count_basis': 'candidate_events is the inherited Collector fact-list length; propagated_candidate_event_count is its actual propagation metric. Neither is SQL raw index rows.',
        'coverage_capability': CAPABILITY, 'all_asset_export_complete': False, 'attempts': attempts}
    status['computational_resource_gaps'] = split_gaps
    atomic_json(folder / 'NATIVE_ACQUISITION_STATUS.json', status)
    atomic_json(folder / 'ACQUISITION_STATUS.json', status)
    return status


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('--work', type=Path, required=True)
    p.add_argument('--query', required=True); p.add_argument('--max-batches', type=int, default=1)
    p.add_argument('--max-rectangles', type=int, default=32); a = p.parse_args()
    q = next(q for q in active_batch(a.work)['queries'] if q['name'] == a.query)
    out = run(a.work, q, a.max_batches, a.max_rectangles)
    print(json.dumps({k: v for k, v in out.items() if k != 'attempts'}, sort_keys=True))
