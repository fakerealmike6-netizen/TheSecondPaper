"""Resume finite native context by header-proven block partitions.

This is SQL computational sharding, not a different tracing window. No RPC,
label lookup, balance invention, candidate expansion, or new budget is added.
Successful full pages and their exact block coverage are reused on restart.
"""
import argparse
import copy
import hashlib
import json
import re
from bisect import bisect_left, bisect_right
from datetime import datetime, timezone
from pathlib import Path
from stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha

from context_access_r3 import read, sha
from context_ledger_r3 import EvidenceConflict
from context_queries_r3 import _build_sql
from page_attempts import atomic_json
from stage1d_context_coverage import merge_coverage
from stage1d_acquisition import Labels, save_sql
from stage1d_context import REQUIRED, build_document, required_context_windows, necessary_context_windows
from stage1d_context_online import _load_context, _merge_rows, _number
from stage1d_runtime import execute_sql, result_rows


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _union(intervals):
    merged = []
    for lo, hi in sorted(intervals):
        if lo > hi:
            raise ValueError('Inverted context interval')
        if merged and lo <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([lo, hi])
    return merged


def missing_intervals(row, coverage):
    """Union of gaps in the four required coverage types, including endpoints."""
    start, end = row['ledger_start_block'], row['ledger_end_block']
    gaps = []
    for kind in REQUIRED:
        cursor = start
        intervals = _union((max(start, r['start_block']), min(end, r['end_block']))
            for r in coverage if r.get('address') == row['address']
            and r.get('data_type') == kind and r.get('status') == 'COMPLETE'
            and r.get('pagination_complete') is True
            and r.get('evidence_ids') and (not r.get('provider_frozen_scope') or
                r.get('date_domain_verified') is True and r.get('block_domain_verified') is True)
            and r['start_block'] <= end and r['end_block'] >= start)
        for lo, hi in intervals:
            if cursor < lo:
                gaps.append([cursor, lo - 1])
            cursor = max(cursor, hi + 1)
        if cursor <= end:
            gaps.append([cursor, end])
    return _union(gaps)


def verified_headers(headers):
    """Validate saved RPC identities and numeric timestamp ordering, not hashes."""
    result = {}
    for key, value in headers.items():
        block = _number(key)
        if _number(value['number']) != block or not re.fullmatch('0x[0-9a-fA-F]{64}', value.get('hash', '')):
            raise EvidenceConflict('Saved partition header has wrong physical block identity')
        result[block] = {'block_number': block, 'block_hash': value['hash'].lower(),
                         'timestamp': _number(value['timestamp'])}
    numbers = sorted(result)
    if any(result[a]['timestamp'] >= result[b]['timestamp'] for a, b in zip(numbers, numbers[1:])):
        raise EvidenceConflict('Saved partition header timestamps are not strictly increasing')
    return result


def add_frozen_boundaries(query, headers):
    """Use exact previously verified freeze endpoints, never estimated block times.

    The caller must first match the entire query to BATCH_QUERY_FREEZE. Endpoint
    identities and times are part of that private freeze dependency.
    """
    result = copy.deepcopy(headers)
    scope = query.get('scope', query)
    for side in ('start', 'end'):
        digest = query.get(side + '_block_hash')
        if digest is None:
            continue
        if not re.fullmatch('0x[0-9a-fA-F]{64}', digest):
            raise EvidenceConflict('Frozen context endpoint lacks exact block hash')
        number = _number(query[side + '_block'])
        if number != _number(scope[side + '_block']):
            raise EvidenceConflict('Frozen endpoint block differs from Scope')
        timestamp = _number(scope[side + '_time'])
        endpoint = {'number': hex(number), 'hash': digest.lower(), 'timestamp': hex(timestamp)}
        if number in result:
            actual = result[number]
            if actual['hash'].lower() != endpoint['hash'] or _number(actual['timestamp']) != timestamp:
                raise EvidenceConflict('Frozen endpoint conflicts with saved RPC header')
        else:
            result[number] = endpoint
    return result


def make_plan(rows, headers, coverage=(), *, chunk_days=14):
    """Intersect exact uncovered account ranges with saved-header block cuts."""
    if isinstance(chunk_days, bool) or not isinstance(chunk_days, int) or not 1 <= chunk_days <= 14:
        raise ValueError('Computational context chunks require 1..14 days')
    if len({r['address'] for r in rows}) != len(rows):
        raise ValueError('One original context interval per account required')
    facts = verified_headers(headers)
    numbers = sorted(facts)
    remaining, gaps = [], []
    for row in rows:
        if row['ledger_start_block'] > row['ledger_end_block']:
            raise ValueError('Inverted original context range')
        for lo, hi in missing_intervals(row, coverage):
            left, right = bisect_right(numbers, lo) - 1, bisect_left(numbers, hi)
            if left < 0 or right >= len(numbers):
                gaps.append({'type': 'CONTEXT_DATE_DOMAIN_UNPROVED', 'address': row['address'],
                             'start_block': lo, 'end_block': hi})
            else:
                remaining.append(dict(row, ledger_start_block=lo, ledger_end_block=hi))
    batches = []
    if remaining:
        start = min(r['ledger_start_block'] for r in remaining)
        final = max(r['ledger_end_block'] for r in remaining)
        while start <= final:
            lower = numbers[bisect_right(numbers, start) - 1]
            target = facts[lower]['timestamp'] + chunk_days * 86400
            valid = [n for n in numbers if start <= n <= final and facts[n]['timestamp'] <= target]
            end = max(valid) if valid else min(final, numbers[bisect_left(numbers, start)])
            upper = numbers[bisect_left(numbers, end)]
            active = [dict(r, ledger_start_block=max(start, r['ledger_start_block']),
                           ledger_end_block=min(end, r['ledger_end_block'])) for r in remaining
                      if max(start, r['ledger_start_block']) <= min(end, r['ledger_end_block'])]
            # Existing successful islands may leave two disjoint ranges for the
            # same account. Keep distinct-address SQL inputs without filling gaps.
            while active:
                chosen, rest, seen = [], [], set()
                for row in sorted(active, key=lambda r: (r['ledger_start_block'], r['address'], r['ledger_end_block'])):
                    if row['address'] in seen:
                        rest.append(row)
                    else:
                        chosen.append(row); seen.add(row['address'])
                active = rest
                lo = min(r['ledger_start_block'] for r in chosen)
                hi = max(r['ledger_end_block'] for r in chosen)
                a = facts[numbers[bisect_right(numbers, lo) - 1]]
                b = facts[numbers[bisect_left(numbers, hi)]]
                date = lambda t: datetime.fromtimestamp(t, timezone.utc).date().isoformat()
                domain = {'basis': 'SAVED_PHYSICAL_HEADERS_BRACKET_EXACT_BLOCK_INTERSECTIONS',
                          'start_date_utc': date(a['timestamp']), 'end_date_utc': date(b['timestamp']),
                          'lower_header': a, 'upper_header': b,
                          'elapsed_days': (b['timestamp'] - a['timestamp']) / 86400,
                          'requested_chunk_days': chunk_days,
                          'sparse_headers_exceed_requested_chunk': b['timestamp'] - a['timestamp'] > chunk_days * 86400,
                          'start_block': lo, 'end_block': hi}
                if domain['sparse_headers_exceed_requested_chunk']:
                    gaps.extend({'type': 'CONTEXT_PARTITION_HEADER_DENSITY_GAP', 'address': r['address'],
                                 'start_block': r['ledger_start_block'], 'end_block': r['ledger_end_block'],
                                 'date_domain': domain, 'new_rpc_requests': 0} for r in chosen)
                else:
                    batches.append({'rows': sorted(chosen, key=lambda r: r['address']), 'date_domain': domain})
            start = end + 1
    # Chronology is the only batching priority; no amounts or model outputs.
    batches.sort(key=lambda b: (b['date_domain']['start_block'], b['date_domain']['end_block'],
                               [r['address'] for r in b['rows']]))
    return {'schema_version': 'stage1d-context-recovery-plan-v1', 'chunk_days': chunk_days,
            'scope_unchanged': True, 'candidate_expansion': False, 'new_rpc_requests': 0,
            'original_rows': rows, 'batches': batches, 'resource_gaps': gaps,
            'batch_order': 'ASCENDING_EXACT_BLOCK_INTERVAL_THEN_ADDRESS'}


def _new_round(root, prefix):
    if not re.fullmatch('round_[A-Za-z0-9_-]{1,42}', prefix):
        raise ValueError('Safe round_ output prefix required')
    used = [int(p.name[len(prefix) + 1:]) for p in root.iterdir()
            if p.is_dir() and re.fullmatch(re.escape(prefix) + r'_[0-9]{6}', p.name)]
    folder = root / (prefix + '_' + format(max(used, default=0) + 1, '06d'))
    folder.mkdir(exist_ok=False)
    return folder


def _full_plan(query, collection, ledger, prior, label_snapshot=None):
    plan = necessary_context_windows(query, collection, ledger, label_snapshot, prior_rows=prior)
    plan.update(account_selection='all', selection_reason='All existing finite ordinary model accounts; verified service/protocol boundaries retain entry facts without platform history.')
    return plan


def _cap(work):
    from stage1d_recovery_policy import effective
    recovery=effective(work)
    if recovery:return recovery['resources']['context_events_per_query_hard']
    policy = work / 'private/STAGE1D_POLICY.json'
    cap = int(read(policy).get('new_batch_resource_limits', {}).get('context_events_per_query', 50000)) if policy.exists() else 50000
    if not 1 <= cap <= 50000:
        raise ValueError('Invalid existing per-query context cap')
    return cap


def _persist_round(folder, query, collection, plan, headers, balances, receipts, ledger, coverage,
                   sources, acquisition, resource_gaps, cap, labels):
    coverage = merge_coverage(coverage)
    for name, value in [('headers', headers), ('balances', balances), ('receipts', receipts),
                        ('ledger_rows', ledger), ('coverage', coverage), ('context_plan', plan),
                        ('acquisition', acquisition), ('label_snapshot', labels),
                        ('input_evidence_manifest', {'sources': sources})]:
        atomic_json(folder / (name + '.json'), value)
    gaps = list(resource_gaps)
    if len(ledger) > cap:
        gaps.append({'type': 'CONTEXT_PHYSICAL_EVENT_CAP_EXCEEDED', 'observed_unique_physical_records': len(ledger),
                     'cap': cap, 'raw_retained': True, 'not_truncated': True})
        result = {'completion_status': 'CONTEXT_RESOURCE_LIMIT_MODEL_BLOCKED', 'evidence_gaps': gaps}
    else:
        try:
            result = build_document(query, collection, ledger, balances, headers, receipts, labels, coverage=coverage, context_plan=plan)
        except Exception as exc:
            result = {'completion_status': 'EVIDENCE_CONFLICT_MODEL_BLOCKED' if isinstance(exc, EvidenceConflict) else 'CONTEXT_ADAPTER_ERROR',
                      'error_class': type(exc).__name__, 'reason': str(exc)}
        if gaps:
            result.setdefault('evidence_gaps', []).extend(gaps)
            if result.get('model_input'):
                result['model_input'].setdefault('gaps', []).extend(gaps)
            if result.get('completion_status') == 'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE':
                result['completion_status'] = 'PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS'
    result['context_acquisition_metadata'] = {'output_variant': folder.name, 'account_selection': 'all',
        'computational_sharding_only': True, 'scope_unchanged': True, 'new_rpc_requests': 0,
        'physical_context_records': len(ledger), 'context_event_cap': cap, 'raw_never_truncated': True,
        'resource_gaps': gaps, 'inherited_evidence_files': len(sources)}
    atomic_json(folder / 'context_result.json', result)
    if result.get('model_input'):
        atomic_json(folder / 'model_input.json', result['model_input'])
    return result


def run_recovery(work, query, *, max_batches=1, chunk_days=14, output_prefix='round_recovery'):
    if isinstance(max_batches, bool) or not isinstance(max_batches, int) or max_batches < 0:
        raise ValueError('Nonnegative max_batches required; zero prepares a local-only plan')
    work = Path(work).resolve()
    frozen = active_batch_path(work)
    match = next((q for q in read(frozen)['queries'] if q['query_id'] == query['query_id']), None)
    if match != query:
        raise ValueError('Recovery requires the exact current frozen query document')
    qdir = work / 'derived/stage1d/queries' / query['name']
    if not qdir.resolve().is_relative_to(work):
        raise ValueError('Query path escapes work')
    root = qdir / 'context'; root.mkdir(exist_ok=True)
    folder = _new_round(root, output_prefix)
    collection = read(qdir / 'collection.json')
    headers, balances, receipts, ledger, coverage, prior, sources = _load_context(work, root, folder)
    headers = add_frozen_boundaries(query, headers)
    labels_reader = Labels(work)
    addresses = {s['state']['address'] for s in collection['states']}
    addresses.update(s['state']['address'] for s in collection.get('stops', []))
    labels = {address: labels_reader(address) for address in sorted(addresses)}
    plan = _full_plan(query, collection, ledger, prior, labels)
    recovery = make_plan(plan['rows'], headers, coverage, chunk_days=chunk_days)
    recovery.update(query_id=query['query_id'], scope_id=query['scope_id'], scope_hash=query['scope_hash'],
                    batch_freeze_sha256=sha(frozen), collection_sha256=sha(qdir / 'collection.json'))
    identity = _digest(recovery)
    stable = work / 'private/stage1d_context_recovery' / query['name'] / identity
    stable.mkdir(parents=True, exist_ok=True)
    for name, value in [('recovery_plan', recovery), ('context_plan', plan), ('headers', headers), ('label_snapshot', labels)]:
        path = stable / (name + '.json')
        if path.exists() and read(path) != json.loads(json.dumps(value)):
            raise EvidenceConflict('Immutable recovery plan dependency differs')
        if not path.exists():
            atomic_json(path, value)
    dependencies = [{'path': p.relative_to(work).as_posix(), 'sha256': sha(p)}
                    for p in (stable / 'recovery_plan.json', stable / 'context_plan.json', stable / 'headers.json', stable / 'label_snapshot.json')]
    cap = _cap(work)
    outcomes, attempted = [], 0
    checkpoint = {'schema_version': 'stage1d-context-recovery-checkpoint-v1', 'plan_sha256': sha(stable / 'recovery_plan.json'),
                  'scope_id': query['scope_id'], 'scope_hash': query['scope_hash'], 'attempts': [],
                  'budget_and_online_clock': 'CONTINUED_EXISTING_RUNTIME_ONLY', 'new_rpc_requests': 0}
    checkpoint_path = stable / 'checkpoint.json'
    if checkpoint_path.exists():
        checkpoint = read(checkpoint_path)
    status = {'status': 'LOCAL_PLAN_ONLY' if max_batches == 0 else 'COMPLETE_CONTEXT_CACHE_REUSED'}
    result = _persist_round(folder, query, collection, plan, headers, balances, receipts, ledger, coverage,
                            sources, status, recovery['resource_gaps'], cap, labels)
    for index, batch in enumerate(recovery['batches']):
        if attempted >= max_batches or len(ledger) > cap:
            break
        domain = batch['date_domain']
        sql = _build_sql({'rows': [dict(r, role='NON_TERMINAL_MODEL_ACCOUNT') for r in batch['rows']]},
                         domain['start_date_utc'], domain['end_date_utc'])
        intervals = [{'query_id': query['query_id'], 'kind': 'context', 'address': r['address'],
                      'start_block': r['ledger_start_block'], 'end_block': r['ledger_end_block']} for r in batch['rows']]
        freeze = save_sql(work, sql, 'context', query, intervals, dependencies)
        # Never submit a second execution for an already terminal failed SQL.
        job = work / 'private/dune_r2_jobs' / sha(freeze.parent / 'query.sql') / 'job.json'
        if job.exists() and read(job).get('state') in {'QUERY_STATE_FAILED', 'QUERY_STATE_CANCELLED', 'QUERY_STATE_EXPIRED'}:
            outcomes.append({'batch_index': index, 'status': 'EXISTING_TERMINAL_FAILURE_RETAINED',
                             'job_folder': job.parent.relative_to(work).as_posix()})
            continue
        folder = _new_round(root, output_prefix)
        atomic_json(folder / 'date_domain.json', dict(domain, rows=batch['rows']))
        attempt = {'batch_index': index, 'freeze_path': freeze.relative_to(work).as_posix(),
                   'freeze_sha256': sha(freeze), 'round_path': folder.relative_to(work).as_posix(), 'status': 'DISPATCH_OR_RESUME_PENDING'}
        checkpoint['attempts'].append(attempt); atomic_json(checkpoint_path, checkpoint)
        attempted += 1
        try:
            outcome = execute_sql(work, freeze, query['name'], 'context_recovery_' + query['name'])
        except Exception as exc:
            outcome = {'status': 'CONTEXT_ACQUISITION_PARTIAL', 'error_class': type(exc).__name__, 'reason': str(exc)}
        if outcome.get('status') == 'COMPLETED_EXPORTED':
            # result_rows checks the persisted full pagination and every page SHA.
            try:
                new_rows = result_rows(work, outcome['job_folder'])
                ledger = _merge_rows(ledger, [dict(r, evidence_ids=sorted(set(r.get('evidence_ids', []) + ['sha256:' + sha(freeze)]))) for r in new_rows])
                for row in batch['rows']:
                    coverage.extend({'address': row['address'], 'data_type': kind, 'start_block': row['ledger_start_block'],
                        'end_block': row['ledger_end_block'], 'status': 'COMPLETE', 'pagination_complete': True,
                        'provider_frozen_scope': True, 'date_domain_verified': True, 'block_domain_verified': True,
                        'evidence_ids': ['sha256:' + sha(freeze)]} for kind in REQUIRED)
            except Exception as exc:
                outcome = dict(outcome, status='CONTEXT_EXPORT_OR_PHYSICAL_VALIDATION_FAILED',
                               error_class=type(exc).__name__, reason=str(exc), raw_retained=True)
        attempt.update(status=outcome.get('status'), outcome=outcome)
        result = _persist_round(folder, query, collection, plan, headers, balances, receipts, ledger, coverage,
                                sources, outcome, recovery['resource_gaps'], cap, labels)
        atomic_json(checkpoint_path, checkpoint)
        outcomes.append(dict(outcome, batch_index=index, context_folder=folder.relative_to(work).as_posix()))
        if outcome.get('status') not in {'COMPLETED_EXPORTED', 'QUERY_STATE_FAILED', 'QUERY_STATE_CANCELLED', 'QUERY_STATE_EXPIRED'}:
            break
    final_plan = _full_plan(query, collection, ledger, plan['rows'], labels)
    atomic_json(folder / 'post_export_context_requirements.json', final_plan)
    pending = [{'address': r['address'], 'intervals': missing_intervals(r, coverage)} for r in final_plan['rows']
               if missing_intervals(r, coverage)]
    summary = {'query': query['name'], 'scope_id': query['scope_id'], 'scope_hash': query['scope_hash'],
               'context_status': result['completion_status'], 'context_folder': folder.relative_to(work).as_posix(),
               'recovery_plan': (stable / 'recovery_plan.json').relative_to(work).as_posix(),
               'planned_batches': len(recovery['batches']), 'attempted_batches': attempted,
               'ledger_rows': len(ledger), 'context_event_cap': cap, 'new_rpc_requests': 0,
               'remaining_account_intervals': pending, 'resource_gaps': recovery['resource_gaps'], 'outcomes': outcomes}
    atomic_json(folder / 'recovery_summary.json', summary)
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--query', required=True)
    parser.add_argument('--max-batches', type=int, default=1)
    parser.add_argument('--chunk-days', type=int, default=14)
    parser.add_argument('--output-prefix', default='round_recovery')
    args = parser.parse_args()
    query = next(q for q in active_batch(args.work)['queries'] if q['name'] == args.query)
    print(json.dumps(run_recovery(args.work, query, max_batches=args.max_batches, chunk_days=args.chunk_days,
                                 output_prefix=args.output_prefix), indent=2))
