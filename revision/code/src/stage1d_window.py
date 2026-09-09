"""Explicit window coverage, immutable content reuse, and recorded fallback.

No transport, budget reservation or method implementation lives here. The live
caller supplies the already authorized serial provider and resource snapshots.
"""
from dataclasses import asdict
from hashlib import sha256
import json
from pathlib import Path

from collector import Event, FetchResult
from physical_facts import PhysicalFactRegistry
from stage1d_gap_sequence import GapAccumulator


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def _write_once(path, value):
    path = Path(path)
    data = canonical(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != data:
            raise ValueError('immutable record already exists with different content')
        return
    with path.open('xb') as handle:
        handle.write(data)


def missing_rectangles(request, completed):
    """Exact integer-second/block rectangle union, with no gap interpolation.

    A date or block alone never proves timestamp coverage. Completeness must
    originate in a successful full provider export; partial records are ignored.
    """
    b0, b1 = request['start_block'], request['end_block']
    t0, t1 = request['start_time'], request['end_time']
    if b0 > b1 or t0 > t1:
        raise ValueError('inverted request')
    relevant = [r for r in completed if r.get('complete') is True
                and r.get('address') == request['address'] and r.get('asset') == request['asset']
                and r['end_block'] >= b0 and r['start_block'] <= b1
                and r['end_time'] >= t0 and r['start_time'] <= t1]
    cuts = sorted({t0, t1 + 1} | {x for r in relevant for x in
                  (max(t0, r['start_time']), min(t1 + 1, r['end_time'] + 1))})
    missing = []
    for lo, next_time in zip(cuts, cuts[1:]):
        hi = next_time - 1
        cursor = b0
        for left, right in sorted((max(b0, r['start_block']), min(b1, r['end_block']))
                                  for r in relevant if r['start_time'] <= lo and r['end_time'] >= hi):
            if cursor < left:
                missing.append(request | dict(start_block=cursor, end_block=left - 1, start_time=lo, end_time=hi))
            cursor = max(cursor, right + 1)
        if cursor <= b1:
            missing.append(request | dict(start_block=cursor, end_block=b1, start_time=lo, end_time=hi))
    return missing


class IntervalContentCache:
    """Immutable successful content cache shared across queries and scope modes.

    Successful empty exports are reusable. Missing or unsuccessful intervals
    remain missing; physical duplicates do not create extra transfer capacity.
    Resource and request counters are owned by the injected provider, never
    reset when bind_scope changes the separate collection identity.
    """
    def __init__(self, provider, directory, *, replay_only=False):
        self.provider = provider
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.replay_only = replay_only or bool(getattr(provider, 'replay_only', False))
        self.scope = None
        self.requests = []

    def bind_scope(self, scope):
        self.scope = scope
        if self.provider is not None and hasattr(self.provider, 'bind_scope'):
            self.provider.bind_scope(scope)

    def store(self, request, result):
        payload = {'schema': 'stage1d-interval-content-v1', 'request': request,
                   'result': result.to_dict()}
        digest = sha256(canonical(payload)).hexdigest()
        _write_once(self.directory / (digest + '.json'), payload)
        return digest

    def _records(self):
        values = []
        for path in sorted(self.directory.glob('*.json')):
            raw = path.read_bytes()
            if sha256(raw).hexdigest() != path.stem:
                raise ValueError('cached interval content hash mismatch')
            value = json.loads(raw)
            if value.get('schema') != 'stage1d-interval-content-v1':
                raise ValueError('unrecognized interval cache schema')
            values.append(value | {'content_sha256': path.stem})
        return values

    def fetch_interval(self, address, asset, start_block, end_block, *, start_time, end_time, global_end_time):
        request = dict(address=address, asset=asset, start_block=start_block, end_block=end_block,
                       start_time=start_time, end_time=min(end_time, global_end_time))
        records = [r for r in self._records() if r['request']['address'] == address and r['request']['asset'] == asset]
        completed = [r['request'] | {'complete': r['result']['complete']} for r in records]
        missing = missing_rectangles(request, completed)
        self.requests.append(request | {'missing_intervals': missing,
                             'scope_id': self.scope.scope_id if self.scope else None})
        fetched = []
        for part in missing:
            if self.replay_only or self.provider is None:
                continue
            result = self.provider.fetch_interval(**part, global_end_time=global_end_time)
            digest = self.store(part, result)
            fetched.append({'request': part, 'result': result.to_dict(), 'content_sha256': digest})
        selected = [r for r in records + fetched if
                    r['request']['end_block'] >= start_block and r['request']['start_block'] <= end_block
                    and r['request']['end_time'] >= start_time and r['request']['start_time'] <= request['end_time']]
        completed = [r['request'] | {'complete': r['result']['complete']} for r in selected]
        remaining = missing_rectangles(request, completed)
        registry = PhysicalFactRegistry()
        gaps = []
        for record in selected:
            for data in record['result']['events']:
                registry.add(Event(**data), source='INTERVAL_CONTENT:' + record['content_sha256'])
            # Retain explicit physical contradictions even from partial pages.
            for conflict in record['result'].get('fact_conflicts', []):
                gaps.append(conflict)
        snapshot = registry.snapshot()
        conflicts = snapshot['conflicts'] + gaps
        events = [Event(**e) for e in snapshot['events'] if
                  start_block <= e['block'] <= end_block and start_time <= e['timestamp'] <= request['end_time']]
        coverage = [r['request'] | {'complete': r['result']['complete'],
                    'content_sha256': r['content_sha256'], 'provider_coverage': r['result']['coverage'],
                    'basis': 'IMMUTABLE_FULL_EXPORT_CONTENT' if r['result']['complete'] else 'PARTIAL_CONTENT_ONLY'}
                    for r in selected]
        if self.scope is not None:
            coverage = [r | {'scope_id': self.scope.scope_id, 'scope_hash': self.scope.scope_hash,
                            'window_mode': self.scope.window_mode} for r in coverage]
        gaps = GapAccumulator([{'reason': 'UNCOVERED_INTERVAL', **r} for r in remaining] + conflicts)
        for r in selected:
            if r not in fetched and (r['result']['complete'] or not remaining):
                continue
            gaps.extend(r['result']['gaps'])
        return FetchResult([] if conflicts else events, coverage, not remaining and not conflicts, gaps,
            sum(r['result']['new_raw_bytes'] for r in fetched),
            sum(r['result']['real_requests'] for r in fetched),
            len(selected) - len(fetched) + sum(r['result']['cache_hits'] for r in fetched),
            conflicts, snapshot['quarantined_versions'])


def record_window_fallback(path, full_scope, reduced_scope, *, authorized_query_id,
                           completed_intervals, pending_frontier, resource_evidence,
                           attempted_recovery, shortened_intervals, resources_before,
                           resources_after, retained_context_ids):
    """Validate a pre-switch record; never infer authorization from outcomes."""
    if full_scope.query_id != authorized_query_id or reduced_scope.query_id != full_scope.query_id:
        raise ValueError('fallback query is not authorized')
    if full_scope.window_mode != 'REFERENCE_FULL' or reduced_scope.window_mode != 'ARRIVAL_90D':
        raise ValueError('fallback requires full and reduced scope identities')
    for key in ('start_block', 'end_block', 'start_time', 'end_time', 'max_depth'):
        if getattr(full_scope, key) != getattr(reduced_scope, key):
            raise ValueError('fallback may change only arrival window mode')
    if full_scope.scope_id == reduced_scope.scope_id:
        raise ValueError('distinct fallback scope_id required')
    if resources_before != resources_after:
        raise ValueError('switch must preserve budget, attempts, clock and raw usage')
    required = {'budget_ledger_sha256', 'attempt_state_sha256', 'online_seconds', 'raw_bytes'}
    if not required <= set(resources_before):
        raise ValueError('shared persisted resource snapshot is incomplete')
    permitted = {'frontier_count', 'candidate_events', 'pages', 'online_seconds',
                 'confirmed_cost', 'remaining_risk', 'estimated_remaining_seconds',
                 'estimated_remaining_cost', 'evidence_refs', 'blocking_resource', 'actual_full_attempts'}
    if not resource_evidence or set(resource_evidence) - permitted:
        raise ValueError('fallback trigger must contain only resource evidence')
    if not resource_evidence.get('evidence_refs') or not resource_evidence.get('blocking_resource'):
        raise ValueError('measured resource evidence and blocking resource required')
    if not pending_frontier or not attempted_recovery or not resource_evidence.get('actual_full_attempts'):
        raise ValueError('actual full attempt, pending frontier and recovery evidence required')
    if resource_evidence.get('actual_full_attempts', 0) < 2 and len(attempted_recovery) < 2:
        raise ValueError('single failure does not justify fallback')
    if not shortened_intervals or any(r['reduced_end'] >= r['full_end'] or
            r['full_end'] != full_scope.local_end(r['arrival_time']) or
            r['reduced_end'] != reduced_scope.local_end(r['arrival_time']) or
            r.get('uncollected_seconds_saved', 0) <= 0 for r in shortened_intervals):
        raise ValueError('fallback must demonstrably reduce uncollected intervals')
    record = {'schema': 'stage1d-window-fallback-v1', 'decision': 'RESOURCE_FALLBACK_RECORDED_BEFORE_SWITCH',
              'full_scope': full_scope.freeze_dict(), 'full_scope_hash': full_scope.scope_hash,
              'reduced_scope': reduced_scope.freeze_dict(), 'reduced_scope_hash': reduced_scope.scope_hash,
              'full_status': 'PARTIAL_FULL_SCOPE', 'reduced_status': 'NOT_STARTED',
              'completed_intervals': completed_intervals, 'pending_frontier': pending_frontier,
              'resource_evidence': resource_evidence, 'attempted_recovery': attempted_recovery,
              'shortened_intervals': shortened_intervals, 'shared_resources': resources_before,
              'retained_context_ids': retained_context_ids,
              'known_context_may_not_be_deleted': True, 'reset_resource_counters': False}
    _write_once(path, record)
    return record
