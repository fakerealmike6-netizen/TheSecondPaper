"""Offline frozen unions of actual Dune frontier intervals; no network imports.

Execution/export remain exclusively in the root's separately budgeted runner.
Zero-row subintervals are certified only after the ENTIRE batch page chain passes.
"""
import argparse
import csv
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path

from collector import Collector, Event, Scope
from collector_inputs import load_exact_seed
from dune_observed_replay import SavedDuneProvider, live_fixed_graph, read_json, sha, write_json
from page_contract import initial_progress, validate_page
from physical_facts import PhysicalFactRegistry
from provider_dune import build_interval_sql, normalize_rows, timestamp, exact_hex

SCHEMA = 'dune-frontier-batch-r1-1'
STOP_CLASSES = {'SERVICE', 'BRIDGE_BOUNDARY', 'MIXER_BOUNDARY', 'DEX_OR_PROTOCOL'}
SCOPE_KEYS = ('address', 'asset', 'start_block', 'end_block', 'start_time', 'end_time')


def hash_text(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def read_registry(path):
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'rt', encoding='utf-8-sig', newline='') as handle:
        rows = list(csv.DictReader(handle))
    registry = {}
    for row in rows:
        address = exact_hex(row['address'], 20)
        if address in registry and registry[address] != row:
            raise ValueError('Duplicate contradictory registry rows')
        registry[address] = row
    return registry


def registry_from_manifest(path, project_root):
    path, root = Path(path).resolve(), Path(project_root).resolve()
    manifest = read_json(path)
    if manifest.get('label_snapshot_path'):
        registry = root / manifest['label_snapshot_path'] / 'address_registry.csv.gz'
        expected = manifest['registry_sha256']
    elif manifest.get('registry_path'):
        registry = path.parent / manifest['registry_path']
        expected = manifest['registry_sha256']
    else:
        raise ValueError('An applied, hash-pinned label snapshot manifest is required')
    registry = registry.resolve()
    if not registry.is_relative_to(root) or not registry.is_file() or sha(registry) != expected:
        raise ValueError('Frozen label registry path/hash mismatch')
    return registry, manifest


def label_identity(registry, address):
    row = registry.get(address)
    if row is None:
        return {'kind': 'UNKNOWN', 'status': 'UNQUERIED', 'acquisition_scope': 'ACTUAL_FRONTIER_LOOKUP_PENDING'}
    kind = {'SERVICE': 'SERVICE', 'BRIDGE_BOUNDARY': 'BRIDGE', 'MIXER_BOUNDARY': 'MIXER',
            'DEX_OR_PROTOCOL': 'UNSUPPORTED_PROTOCOL'}.get(row['identity_class'], 'UNKNOWN')
    lookup = row.get('lookup_status', '')
    status = next((status for status in ('UNQUERIED', 'LOOKUP_FAILED', 'BUDGET_BLOCKED')
                   if lookup == status or lookup.startswith(status + '_')), 'FROZEN_LOCAL_OBSERVATION')
    conflict = row['identity_class'] == 'CONFLICTED_IDENTITY'
    if conflict and status == 'FROZEN_LOCAL_OBSERVATION':
        status = 'LABEL_CONFLICT'
    return {'kind': kind, 'actor': row.get('actor'), 'identity_class': row['identity_class'],
            'status': status, 'lookup_status': lookup, 'label_conflict': conflict,
            'acquisition_scope': 'R1_FROZEN_LABEL_REGISTRY'}


def collect_with_labels(provider, registry, scope, seed):
    """Ownership conflict is a label gap, never a physical/protocol prohibition.

    The frozen Collector recognizes LOOKUP_FAILED/UNQUERIED/BUDGET_BLOCKED. Add
    the distinct ownership conflict record here without editing that gate source.
    """
    result = Collector(provider, lambda address: label_identity(registry, address)).run(scope, seed)
    conflicts = sorted({s['state']['address'] for s in result.states if s['identity'].get('label_conflict')})
    result.gaps.extend({'reason': 'LABEL_CONFLICT', 'address': address, 'continues_as_unknown': True,
                        'physical_fact_conflict': False} for address in conflicts)
    return result


def select_intervals(frontier, policy, name, registry):
    pilots = [p for p in policy['query_pilots'] if p['name'] == name]
    if len(pilots) != 1:
        raise ValueError('One fixed authorized probe required')
    pilot = pilots[0]; scope = Scope.from_policy(pilot)
    selected, excluded, seen = [], [], set()
    for original in frontier:
        if original.get('name') != name:
            continue
        r = dict(original)
        if r.get('query_id') != pilot['query_id']:
            raise ValueError('Frontier query identity differs from fixed probe')
        address = exact_hex(r['address'], 20)
        if r['address'] != address:
            raise ValueError('Frontier address must already be canonical')
        if any(type(r.get(k)) is not int for k in ('depth', 'start_block', 'end_block', 'start_time', 'end_time')):
            raise ValueError('Exact frontier integer fields required')
        if (not scope.start_block <= r['start_block'] <= r['end_block'] == scope.end_block
                or not scope.start_time <= r['start_time'] <= r['end_time'] <= scope.end_time
                or r['end_time'] != min(scope.end_time, r['start_time'] + 90 * 86400)
                or r['depth'] < 0 or not r.get('arrival_event_id')):
            raise ValueError('Frontier interval changed or widened beyond per-arrival fixed policy')
        label = registry.get(address, {'address': address, 'identity_class': 'UNKNOWN', 'lookup_status': 'UNQUERIED'})
        reason = ('FROZEN_LABEL_BOUNDARY' if label.get('identity_class') in STOP_CLASSES else
                  'ACQUISITION_DEPTH_BOUNDARY' if r['depth'] >= scope.max_depth else
                  'NOT_AN_UNFINISHED_PROVIDER_INTERVAL' if r.get('reason') != 'INTERVAL_INCOMPLETE' else None)
        if reason:
            excluded.append({'original_scope': original, 'reason': reason, 'label_identity_class': label.get('identity_class')})
            continue
        identity = hash_text(canonical(original))
        if identity in seen:
            continue
        seen.add(identity)
        selected.append({'interval_id': identity, 'original_scope': original,
                         'scope': {k: r[k] for k in SCOPE_KEYS}, 'frozen_label': label})
    selected.sort(key=lambda r: (r['original_scope']['depth'], r['scope']['start_block'],
                  r['scope']['start_time'], r['original_scope']['arrival_event_id'], r['scope']['address'], r['interval_id']))
    if len(selected) > 1000:
        raise ValueError('Probe expanded-address resource limit cannot be bypassed by batching')
    return selected, excluded


def fragment_sql(scope):
    sql = build_interval_sql(**scope)
    day = lambda t: datetime.fromtimestamp(timestamp(t), timezone.utc).strftime('%Y-%m-%d')
    predicate = "block_date BETWEEN DATE '%s' AND DATE '%s' AND " % (day(scope['start_time']), day(scope['end_time']))
    # Exactly the accepted original optimization: raw transactions and traces.
    for table in ('ethereum.transactions', 'ethereum.traces'):
        old = 'FROM ' + table + ' WHERE '
        if sql.count(old) != 1:
            raise ValueError('Reviewed interval template changed')
        sql = sql.replace(old, old + predicate)
    return sql


def union_sql(intervals):
    if not intervals:
        raise ValueError('No unfinished nonterminal actual frontier intervals')
    parts = []
    for index, interval in enumerate(intervals):
        parts.append("SELECT '%s' AS interval_id, f%d.* FROM (\n%s) f%d" %
                     (interval['interval_id'], index, fragment_sql(interval['scope']), index))
    return ('-- ' + SCHEMA + '; frozen actual frontier union; no reference neighbors\n'
            + '\nUNION ALL\n'.join(parts)
            + '\nORDER BY interval_id,block_number,tx_index,tx_hash,event_kind,log_index,trace_address\n')


def prepare(frontier_path, name, label_manifest_path, policy_path, project_root, output):
    output = Path(output)
    if output.exists():
        raise FileExistsError('A frozen batch directory cannot be overwritten')
    registry_path, _ = registry_from_manifest(label_manifest_path, project_root)
    frontier, policy, registry = read_json(frontier_path), read_json(policy_path), read_registry(registry_path)
    intervals, excluded = select_intervals(frontier, policy, name, registry)
    sql = union_sql(intervals)
    output.mkdir(parents=True)
    # Small selected label slice; the full third-party registry stays private in
    # its existing immutable snapshot, pinned by the label manifest hash.
    labels = {r['address']: registry.get(r['address'], {'address': r['address'], 'identity_class': 'UNKNOWN', 'lookup_status': 'UNQUERIED'})
              for r in frontier if r.get('name') == name}
    for filename, value in [('frontier.json', frontier), ('policy.json', policy), ('labels_used.json', labels)]:
        write_json(output / filename, value)
    (output / 'label_manifest.json').write_bytes(Path(label_manifest_path).read_bytes())
    (output / 'query.sql').write_text(sql, encoding='utf-8', newline='\n')
    for index, interval in enumerate(intervals):
        fragment = fragment_sql(interval['scope'])
        relative = 'fragments/%04d.sql' % index
        path = output / relative; path.parent.mkdir(exist_ok=True)
        path.write_text(fragment, encoding='utf-8', newline='\n')
        interval.update(fragment_path=relative, fragment_sql_sha256=hash_text(fragment), fragment_file_sha256=sha(path))
    manifest = {'schema_version': SCHEMA, 'name': name, 'prepared_at_utc': datetime.now(timezone.utc).isoformat(),
                'query_id': next(p['query_id'] for p in policy['query_pilots'] if p['name'] == name),
                'frontier_source_sha256': sha(frontier_path), 'policy_source_sha256': sha(policy_path),
                'label_manifest_sha256': sha(label_manifest_path), 'registry_sha256': sha(registry_path),
                'input_files': {p: sha(output / p) for p in ('frontier.json', 'policy.json', 'labels_used.json', 'label_manifest.json')},
                'intervals': intervals, 'excluded': excluded, 'ordered_interval_ids': [r['interval_id'] for r in intervals],
                'full_sql_sha256': hash_text(sql), 'query_file_sha256': sha(output / 'query.sql'),
                'no_reference_neighbors': True, 'no_sql_limit': True, 'network_requests': 0,
                'scope_end_is_per_arrival_90_days_intersect_fixed_global_end': True,
                'zero_row_interval_complete_requires_full_batch_page_chain': True}
    write_json(output / 'freeze_manifest.json', manifest)
    return manifest


def verify_frozen(path, expected_sha256=None):
    path = Path(path).resolve(); folder = path.parent
    if expected_sha256 is not None and sha(path) != expected_sha256:
        raise ValueError('Job-bound frozen manifest hash mismatch')
    manifest = read_json(path)
    if manifest.get('schema_version') != SCHEMA:
        raise ValueError('Unsupported batch freeze version')
    for name, expected in manifest['input_files'].items():
        target = (folder / name).resolve()
        if not target.is_relative_to(folder) or sha(target) != expected:
            raise ValueError('Frozen input bytes mismatch')
    if sha(folder / 'label_manifest.json') != manifest['label_manifest_sha256']:
        raise ValueError('Frozen label manifest changed')
    expected, excluded = select_intervals(read_json(folder / 'frontier.json'), read_json(folder / 'policy.json'),
                                         manifest['name'], read_json(folder / 'labels_used.json'))
    if manifest['ordered_interval_ids'] != [r['interval_id'] for r in expected] or manifest['excluded'] != excluded:
        raise ValueError('Frozen selected/omitted scopes changed')
    if len(expected) != len(manifest['intervals']):
        raise ValueError('Frozen interval count changed')
    for actual, original in zip(manifest['intervals'], expected):
        if any(actual.get(k) != v for k, v in original.items()):
            raise ValueError('Frozen scope or label altered')
        fragment = fragment_sql(original['scope'])
        target = (folder / actual['fragment_path']).resolve()
        if (not target.is_relative_to(folder) or sha(target) != actual['fragment_file_sha256']
                or target.read_text(encoding='utf-8') != fragment or hash_text(fragment) != actual['fragment_sql_sha256']):
            raise ValueError('Frozen full fragment SQL changed')
    sql = union_sql(expected)
    if (sha(folder / 'query.sql') != manifest['query_file_sha256'] or hash_text(sql) != manifest['full_sql_sha256']
            or (folder / 'query.sql').read_text(encoding='utf-8') != sql):
        raise ValueError('Frozen full union SQL changed')
    return manifest


def verified_raw(receipt, work):
    root = Path(work).resolve(); path = (root / receipt['raw_path']).resolve()
    if not path.is_relative_to(root) or path.is_symlink() or not path.is_file():
        raise ValueError('Raw receipt path escapes work')
    if receipt.get('http_status') != 200 or receipt.get('error_class') or sha(path) != receipt.get('sha256'):
        raise ValueError('Raw HTTP receipt does not verify')
    if type(receipt.get('raw_bytes')) is not int or path.stat().st_size != receipt['raw_bytes']:
        raise ValueError('Raw HTTP receipt byte count differs')
    # RevisionLive parses wire decimals exactly, then its persisted JSON encodes
    # Decimal as strings. Reproduce that representation, never a binary-float
    # approximation; otherwise valid real execution_cost_credits cannot bind.
    wire = json.loads(path.read_text(encoding='utf-8'), parse_float=Decimal)
    return json.loads(json.dumps(wire, default=str))


def load_completed_batch(folder, work, freeze_path=None):
    folder, work = Path(folder), Path(work).resolve()
    job = read_json(folder / 'job.json')
    if freeze_path is None:
        freeze_path = Path(job['scope_freeze_path'])
        if not freeze_path.is_absolute():
            freeze_path = work / freeze_path
    freeze_path = Path(freeze_path).resolve()
    if not freeze_path.is_relative_to(work):
        raise ValueError('Batch freeze must be inside current work; supply relocated freeze explicitly')
    manifest = verify_frozen(freeze_path, job.get('scope_freeze_sha256'))
    if not job.get('scope_freeze_sha256'):
        raise ValueError('Job did not pin the pre-submission scope manifest')
    sql = (folder / 'query.sql').read_text(encoding='utf-8')
    if hash_text(sql) != job.get('sql_sha256') or hash_text(sql) != manifest['full_sql_sha256']:
        raise ValueError('Submitted batch SQL differs from frozen SQL')
    execution = job.get('execution_id')
    if job.get('state') != 'QUERY_STATE_COMPLETED' or not execution:
        raise ValueError('Batch job not completed')
    # The root runner persists raw submit/status receipts. Bind accepted execution
    # and terminal metadata to their actual responses, not only mutable job flags.
    submit = verified_raw(job['submit_receipt'], work)
    status = verified_raw(job['status_receipt'], work)
    if (job['submit_receipt'].get('operation') != 'execute'
            or job['status_receipt'].get('operation') != 'status'
            or job['status_receipt'].get('execution_id') != execution):
        raise ValueError('Raw submission/status request method or execution differs')
    if (submit != job.get('submit_response') or submit.get('execution_id') != execution
            or status != job.get('status_response') or status.get('execution_id') != execution
            or status.get('state') != 'QUERY_STATE_COMPLETED'):
        raise ValueError('Raw submit/status execution binding differs')
    progress, offset, rows, pages = initial_progress(), 0, [], []
    for _ in range(1000):
        receipt_path = folder / ('page_%d_receipt.json' % offset)
        page_path = folder / ('page_%d.json' % offset)
        receipt, page = read_json(receipt_path), read_json(page_path)
        if verified_raw(receipt, work) != page:
            raise ValueError('Saved page differs from actual raw response')
        params = receipt.get('parameters')
        if (receipt.get('operation') != 'results' or receipt.get('execution_id') != execution or not isinstance(params, dict)
                or set(params) != {'offset', 'limit'} or params['offset'] != offset):
            raise ValueError('Unfiltered execution/page request binding required')
        progress = validate_page(page, execution_id=execution, offset=offset, limit=params['limit'], progress=progress,
                                 status_metadata=status.get('result_metadata'), receipt=receipt, parameters=params)
        rows.extend(page['result']['rows'])
        pages.append({'offset': offset, 'rows': len(page['result']['rows']), 'response_sha256': receipt['sha256'],
                      'saved_page_sha256': sha(page_path), 'receipt_sha256': sha(receipt_path), 'raw_bytes': receipt['raw_bytes']})
        if progress['complete']:
            break
        offset = progress['next_offset']
    else:
        raise ValueError('Batch page resource boundary reached before completeness')
    groups = {r['interval_id']: [] for r in manifest['intervals']}
    for row in rows:
        interval_id = row.get('interval_id')
        if interval_id not in groups:
            raise ValueError('Result row belongs to an unknown frozen interval')
        groups[interval_id].append({k: v for k, v in row.items() if k != 'interval_id'})
    jobs = []
    for interval in manifest['intervals']:
        identity = interval['interval_id']; values = groups[identity]
        events, gaps = normalize_rows(values)
        scope = interval['scope']
        for event in events:
            if (event.chain_id != 'eip155:1' or not scope['start_block'] <= event.block <= scope['end_block']
                    or not scope['start_time'] <= event.timestamp <= scope['end_time']
                    or scope['address'] not in (event.sender, event.recipient)
                    or (scope['asset'] != 'native:eip155:1' and event.asset not in ('native:eip155:1', scope['asset']))):
                raise ValueError('Batch row violates its frozen interval scope')
        events = [replace(e, provenance='DUNE_EXECUTION:' + execution + ':SQL:' + job['sql_sha256'] + ':INTERVAL:' + identity) for e in events]
        jobs.append({'scope': scope, 'events': events, 'normalization_gaps': gaps,
                     'logical_job_id': job['logical_job_id'] + '#interval:' + identity,
                     'batch_parent_logical_job_id': job['logical_job_id'], 'interval_id': identity,
                     'execution_id': execution, 'sql_sha256': job['sql_sha256'],
                     'sql_file_sha256': sha(folder / 'query.sql'), 'job_file_sha256': sha(folder / 'job.json'),
                     'scope_freeze_sha256': sha(freeze_path), 'label_manifest_sha256': manifest['label_manifest_sha256'],
                     'job_path': folder.as_posix(), 'exported_rows': len(values), 'batch_exported_rows': len(rows),
                     'pages': pages, 'export_complete': True,
                     'zero_row_completeness_basis': 'ENTIRE_FROZEN_BATCH_RESULT_PAGINATION_COMPLETE' if not values else None,
                     'execution_cost_credits': job.get('execution_cost_credits')})
    return jobs


class BatchSavedDuneProvider(SavedDuneProvider):
    def __init__(self, jobs_root, work, batch_jobs_root=None, *, batch_specs=()):
        super().__init__(jobs_root, work)
        specs = list(batch_specs)
        if batch_jobs_root and Path(batch_jobs_root).exists():
            specs += [(folder, None) for folder in sorted(Path(batch_jobs_root).iterdir())
                      if folder.is_dir() and (folder / 'job.json').is_file()
                      and read_json(folder / 'job.json').get('kind') == 'candidate']
        for folder, freeze in specs:
            try:
                self.jobs.extend(load_completed_batch(folder, work, freeze))
            except (ValueError, KeyError, TypeError, OSError) as exc:
                self.rejected_jobs.append({'job_path': str(folder), 'reason': str(exc), 'exception_type': type(exc).__name__})
        self.fact_registry = PhysicalFactRegistry()
        for job in self.jobs:
            for event in job['events']:
                self.fact_registry.add(event, source=job['logical_job_id'])
            for gap in job['normalization_gaps']:
                for version in gap.get('versions', []):
                    self.fact_registry.add(version['facts'] | {'event_id': version['event_id'],
                        'provenance': canonical(version.get('provenance', []))}, source=job['logical_job_id'], raw=version.get('raw'))
        self.fact_snapshot = self.fact_registry.snapshot()


def replay(policy_path, events_path, members_path, label_manifest_path, project_root, jobs_root, batch_jobs_root, work, output):
    output = Path(output)
    if output.exists():
        raise FileExistsError('Replay output must be a new revision directory')
    policy = read_json(policy_path)
    registry_path, _ = registry_from_manifest(label_manifest_path, project_root)
    registry = read_registry(registry_path)
    summaries, sources, next_frontier = [], [], []
    for pilot in policy['query_pilots']:
        seed, seed_manifest = load_exact_seed(pilot, members_path, events_path)
        provider = BatchSavedDuneProvider(jobs_root, work, batch_jobs_root)
        result = collect_with_labels(provider, registry, Scope.from_policy(pilot), seed)
        folder = output / pilot['name']; result.write(folder / 'collection.json')
        graph, model_scope = live_fixed_graph(result, seed)
        write_json(folder / 'fixed_graph.json', graph); write_json(folder / 'model_scope.json', model_scope)
        write_json(folder / 'request_plan.json', provider.requests)
        used = [j for j in provider.jobs if j['logical_job_id'] in provider.used_jobs]
        observed = {e.event_id for j in used for e in j['events']}
        actual_jobs = {j.get('batch_parent_logical_job_id', j['logical_job_id']): j for j in used}
        summary = {'name': pilot['name'], 'query_id': pilot['query_id'],
                   'status': 'PARTIAL' if result.unresolved_frontier or result.gaps else 'COMPLETED_WITH_RECORDED_GAPS',
                   'external_acceptance_status': 'PENDING_REVIEW', 'collector_status': result.status,
                   'live_query_interval_count': len(used), 'live_logical_job_count': len(actual_jobs),
                   'live_exported_rows': sum(j.get('batch_exported_rows', j['exported_rows']) for j in actual_jobs.values()),
                   'live_interval_rows': sum(j['exported_rows'] for j in used),
                   'live_normalized_distinct_events': len(observed), 'live_queried_address_count': len({j['scope']['address'] for j in used}),
                   'unresolved_frontier_count': len(result.unresolved_frontier),
                   'unqueried_label_addresses': sorted({g['address'] for g in result.gaps if g['reason'] == 'LABEL_UNQUERIED'}),
                   'failed_label_addresses': sorted({g['address'] for g in result.gaps if g['reason'] in ('LABEL_LOOKUP_FAILED', 'LABEL_BUDGET_BLOCKED')}),
                   'conflicted_label_addresses': sorted({g['address'] for g in result.gaps if g['reason'] == 'LABEL_CONFLICT'}),
                   'replay_network_requests': 0, 'source_event_pool_usage': 'EXACT_SEED_ONLY',
                   'registry_sha256': sha(registry_path), 'label_manifest_sha256': sha(label_manifest_path),
                   **{k: v for k, v in result.metrics.items() if k != 'candidate_membership'}}
        summaries.append(summary); write_json(folder / 'status.json', summary)
        sources.append({'query': pilot['name'], 'seed': seed_manifest, 'registry_sha256': sha(registry_path),
                        'label_manifest_sha256': sha(label_manifest_path),
                        'used_jobs': [{k: v for k, v in j.items() if k != 'events'} for j in used],
                        'rejected_or_inflight_jobs': provider.rejected_jobs})
        for pending in result.unresolved_frontier:
            state = pending['state']
            request = {'query_id': pilot['query_id'], 'name': pilot['name'], 'address': state['address'], 'asset': state['asset'],
                       'arrival_event_id': state['arrival']['event_id'], 'depth': state['depth'],
                       'start_block': state['arrival']['block'], 'end_block': pilot['end_block'],
                       'start_time': state['arrival']['timestamp'], 'end_time': state['local_end'], 'reason': pending['reason']}
            if request not in next_frontier:
                next_frontier.append(request)
    write_json(output / 'summary.json', summaries); write_json(output / 'source_manifest.json', sources)
    write_json(output / 'next_actual_frontier.json', next_frontier)
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    prep = sub.add_parser('prepare')
    for name in ('frontier', 'label-manifest', 'policy', 'project-root', 'output'):
        prep.add_argument('--' + name, type=Path, required=True)
    prep.add_argument('--name', required=True)
    verify = sub.add_parser('verify'); verify.add_argument('--manifest', type=Path, required=True)
    load = sub.add_parser('load')
    for name in ('folder', 'work', 'manifest', 'output'):
        load.add_argument('--' + name, type=Path, required=True)
    run = sub.add_parser('replay')
    for name in ('policy', 'events', 'members', 'label-manifest', 'project-root', 'jobs', 'batch-jobs', 'work', 'output'):
        run.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    if args.action == 'prepare':
        result = prepare(args.frontier, args.name, args.label_manifest, args.policy, args.project_root, args.output)
        print(canonical({'status': 'FROZEN', 'intervals': len(result['intervals']), 'sql_sha256': result['full_sql_sha256']}))
    elif args.action == 'verify':
        result = verify_frozen(args.manifest); print(canonical({'status': 'VERIFIED', 'intervals': len(result['intervals'])}))
    elif args.action == 'load':
        jobs = load_completed_batch(args.folder, args.work, args.manifest)
        write_json(args.output, [{k: [e.__dict__ for e in v] if k == 'events' else v for k, v in j.items()} for j in jobs])
        print(canonical({'status': 'VERIFIED', 'intervals': len(jobs), 'rows': sum(j['exported_rows'] for j in jobs)}))
    else:
        print(canonical(replay(args.policy, args.events, args.members, args.label_manifest, args.project_root,
                               args.jobs, args.batch_jobs, args.work, args.output)))


if __name__ == '__main__':
    main()
