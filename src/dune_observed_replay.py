"""Replay hash-verified completed Dune intervals without any network or billing.

Only the exact seed is read from inherited chain data. Missing Dune intervals
remain explicit actual-frontier requests; inherited reference caches never fill
them. Raw SQL is accepted only when it matches the reviewed adapter template,
optionally with equivalent DATE partition predicates.
"""
from __future__ import annotations
import argparse, csv, gzip, hashlib, json, re
from dataclasses import asdict, replace
from pathlib import Path
from collector import Collector, FetchResult, NATIVE, Scope, strictly_after
from collector_inputs import load_exact_seed
from provider_dune import build_interval_sql, normalize_rows, timestamp, integer
from cache_probe import fixed_graph
from physical_facts import PhysicalFactRegistry, CONFLICT_STATUS


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def parse_interval_sql(sql):
    """Recover scope only from a recognized complete, unfiltered query template."""
    address = re.search(r'FROM tx_window t WHERE t\."from" = (0x[0-9a-fA-F]{40}) OR', sql)
    blocks = re.findall(r'(?<!evt_)block_number BETWEEN (\d+) AND (\d+)', sql)
    times = re.findall(r"(?<!evt_)block_time BETWEEN TIMESTAMP '([^']+)' AND TIMESTAMP '([^']+)'", sql)
    if not address or len(blocks) != 2 or len(set(blocks)) != 1 or len(times) != 2 or len(set(times)) != 1:
        raise ValueError('Unrecognized or inconsistent saved query interval')
    token = re.search(r'e\.contract_address = (0x[0-9a-fA-F]{40})', sql)
    scope = dict(address=address[1].lower(), asset=('erc20:eip155:1:' + token[1].lower()) if token else NATIVE,
                 start_block=int(blocks[0][0]), end_block=int(blocks[0][1]),
                 start_time=timestamp(times[0][0]), end_time=timestamp(times[0][1]))
    # The live runner adds date partition pruning. It may not narrow the actual
    # authorized timestamp interval; all remaining SQL must equal our template.
    dates = re.findall(r"block_date BETWEEN DATE '([^']+)' AND DATE '([^']+)' AND ", sql)
    if dates and (len(dates) != 2 or any(d != (times[0][0][:10], times[0][1][:10]) for d in dates)):
        raise ValueError('Partition dates do not match interval dates')
    stripped = re.sub(r"block_date BETWEEN DATE '[^']+' AND DATE '[^']+' AND ", '', sql)
    expected = build_interval_sql(**scope)
    if re.sub(r'\s+', ' ', stripped).strip() != re.sub(r'\s+', ' ', expected).strip():
        raise ValueError('Saved SQL differs from reviewed full interval template')
    return scope


def load_completed_job(folder, work):
    folder, work = Path(folder), Path(work).resolve()
    job = read_json(folder / 'job.json')
    sqlpath = folder / 'query.sql'
    sql = sqlpath.read_text(encoding='utf-8')
    # Stored Windows file bytes can have CRLF; logical SQL hash records the exact
    # submitted Python text (universal-newline decoding), not the file-byte hash.
    logical_sha = hashlib.sha256(sql.encode('utf-8')).hexdigest()
    if logical_sha != job.get('sql_sha256'):
        raise ValueError('Submitted SQL hash mismatch')
    scope = parse_interval_sql(sql)
    if job.get('state') != 'QUERY_STATE_COMPLETED' or not job.get('execution_id'):
        raise ValueError('Job not completed')
    execution = job['execution_id']
    offset, rows, page_evidence, seen = 0, [], [], set()
    total = None
    for _ in range(1000):
        if offset in seen:
            raise ValueError('Repeated pagination cursor')
        seen.add(offset)
        pagepath = folder / ('page_%s.json' % offset)
        receipt = read_json(folder / ('page_%s_receipt.json' % offset))
        page = read_json(pagepath)
        rawpath = (work / receipt['raw_path']).resolve()
        if not rawpath.is_relative_to(work) or rawpath.is_symlink():
            raise ValueError('Raw response path escape')
        if receipt.get('http_status') != 200 or sha(rawpath) != receipt.get('sha256'):
            raise ValueError('Raw response receipt does not verify')
        if rawpath.stat().st_size != receipt['raw_bytes'] or read_json(rawpath) != page:
            raise ValueError('Saved page differs from original response')
        if receipt.get('execution_id') != execution or receipt.get('parameters', {}).get('offset') != offset:
            raise ValueError('Receipt execution/cursor mismatch')
        if page.get('execution_id') != execution or page.get('state') != 'QUERY_STATE_COMPLETED':
            raise ValueError('Result execution not completed or mismatched')
        data = page.get('result', {})
        values = data.get('rows')
        if not isinstance(values, list):
            raise ValueError('Rows missing')
        md = data.get('metadata', {})
        count = integer(md.get('total_row_count'))
        if total is not None and total != count:
            raise ValueError('Result total changed')
        total = count
        if integer(md.get('row_count')) != len(values):
            raise ValueError('Page row count mismatch')
        rows.extend(values)
        next_offset = page.get('next_offset')
        page_evidence.append(dict(offset=offset, rows=len(values), response_sha256=sha(rawpath),
                                  saved_page_sha256=sha(pagepath), raw_bytes=rawpath.stat().st_size))
        if next_offset is None:
            if len(rows) != total:
                raise ValueError('Terminal pagination is incomplete')
            break
        next_offset = integer(next_offset)
        if not values or next_offset != offset + len(values):
            raise ValueError('Pagination cursor not contiguous')
        offset = next_offset
    else:
        raise ValueError('Too many pages')
    status_total = job.get('status_response', {}).get('result_metadata', {}).get('total_row_count')
    if status_total is not None and integer(status_total) != total:
        raise ValueError('Status/export result totals differ')
    events, gaps = normalize_rows(rows)
    registry=PhysicalFactRegistry()
    for event in events:
        event = replace(event, provenance='DUNE_EXECUTION:' + execution + ':SQL:' + logical_sha)
        registry.add(event)
    snapshot=registry.snapshot();gaps.extend(snapshot['conflicts'])
    from collector import Event
    return dict(scope=scope, events=[Event(**e) for e in snapshot['events']], normalization_gaps=gaps,
                logical_job_id=job['logical_job_id'], execution_id=execution,
                sql_sha256=logical_sha, sql_file_sha256=sha(sqlpath), job_file_sha256=sha(folder / 'job.json'),
                job_path=folder.as_posix(), exported_rows=total, pages=page_evidence,
                execution_cost_credits=job.get('execution_cost_credits'), export_complete=True)


class SavedDuneProvider:
    replay_only = True

    def __init__(self, jobs_root, work):
        self.jobs, self.rejected_jobs, self.requests, self.used_jobs = [], [], [], set()
        self.fact_registry=PhysicalFactRegistry()
        for folder in sorted(Path(jobs_root).iterdir()):
            if not folder.is_dir():
                continue
            try:
                self.jobs.append(load_completed_job(folder, work))
            except (ValueError, KeyError, TypeError, OSError) as exc:
                self.rejected_jobs.append(dict(job_path=folder.as_posix(), reason=str(exc), exception_type=type(exc).__name__))
        # Reconcile all saved jobs before selecting any interval. A different
        # job's contradictory version cannot be hidden by the match ranking.
        for job in self.jobs:
            for event in job['events']:self.fact_registry.add(event)
            for gap in job['normalization_gaps']:
                for version in gap.get('versions',[]):
                    self.fact_registry.add(version['facts']|{'event_id':version['event_id'],'provenance':json.dumps(version.get('provenance',[]))},source=job['logical_job_id'],raw=version.get('raw'))
        self.fact_snapshot=self.fact_registry.snapshot()

    def fetch_interval(self, address, asset, start_block, end_block, *, start_time, end_time, global_end_time):
        request = dict(address=address, asset=asset, start_block=start_block, end_block=end_block,
                       start_time=start_time, end_time=min(end_time, global_end_time))
        matches = [j for j in self.jobs if j['scope']['address'] == address and j['scope']['asset'] == asset
                   and j['scope']['start_block'] <= start_block and j['scope']['end_block'] >= end_block
                   and j['scope']['start_time'] <= start_time and j['scope']['end_time'] >= request['end_time']]
        if not matches:
            self.requests.append(request | dict(available_saved_interval=False))
            return FetchResult([], [request | dict(provider='Dune', complete=False, basis='NO_COMPLETED_SAVED_DUNE_INTERVAL')],
                               False, [dict(reason='DUNE_SAVED_INTERVAL_MISSING', **request)], cache_hits=1)
        matches=sorted(matches,key=lambda j:j['logical_job_id'])
        ids={e.event_id for job in matches for e in job['events']}
        for job in matches:
            self.used_jobs.add(job['logical_job_id'])
            for gap in job['normalization_gaps']:
                ids.update(gap.get('event_ids',[]))
        conflicts=[c for c in self.fact_snapshot['conflicts'] if ids.intersection(c['event_ids'])]
        gaps=[gap for job in matches for gap in job['normalization_gaps']]
        gaps.extend(c for c in conflicts if c not in gaps)
        from collector import Event
        canonical={self.fact_registry.get(eid)['event_id']:self.fact_registry.get(eid) for eid in ids if self.fact_registry.get(eid) is not None}
        selected=[] if conflicts else [Event(**d) for d in sorted(canonical.values(),key=lambda d:d['event_id']) if start_block<=d['block']<=end_block and start_time<=d['timestamp']<=request['end_time']]
        complete=not gaps
        evidence=[request|dict(provider='Dune',complete=complete,export_complete=True,
                  basis=CONFLICT_STATUS if conflicts else 'COMPLETE_EXPORTED_RECOGNIZED_DUNE_QUERY_INTERVAL',
                  execution_id=job['execution_id'],logical_job_id=job['logical_job_id'],sql_sha256=job['sql_sha256'],
                  exported_rows=job['exported_rows'],returned_events=len(selected),pages=job['pages']) for job in matches]
        self.requests.append(request|dict(available_saved_interval=True,logical_job_ids=[j['logical_job_id'] for j in matches]))
        return FetchResult(selected,evidence,complete,gaps,cache_hits=len(matches),fact_conflicts=conflicts,
                           quarantined_facts=[v for c in conflicts for v in c['versions']])


def live_fixed_graph(result, seed):
    graph, scope = fixed_graph(result, seed)
    if graph['scope']==CONFLICT_STATUS:return graph,scope
    by_id = {e['event_id']: e for e in result.candidate_events}
    from collector import Event
    ordered = [Event(**{k: v for k, v in by_id[e['id']].items() if k != 'context_only'}) for e in graph['events']]
    unresolved = []
    for index, event in enumerate(ordered):
        for previous in ordered[:index]:
            if event.block == previous.block and strictly_after(event, previous) is not True:
                unresolved.append([previous.event_id, event.event_id])
    if unresolved:
        scope['status'] = 'ORDER_UNRESOLVED_MODEL_NOT_SOLVABLE'
        scope['unresolved_order_pairs'] = unresolved
    graph['scope'] = scope['status']
    graph['assumptions'] = [
        'Candidate transfers derive solely from completed and hash-verified live Dune query results plus the exact inherited seed.',
        'Missing actual-frontier intervals are never filled with inherited reference-targeted cache data.',
        'Initial actual balances remain unknown. Transaction gas facts are retained as context but are not completely modeled here.',
        'Fixed-graph intervals are conditional; missing frontier intervals, labels, unobserved routes and unsupported conversions prohibit a global real-chain guarantee.',
        'No acquisition depth or 90-day source-age constraint is added to this fixed-graph LP.',
    ]
    scope['acquisition_mode'] = 'REAL_DUNE_SAVED_RESULT_REPLAY'
    scope['online_complete'] = not result.unresolved_frontier
    scope['provider_interval_completion_does_not_certify_initial_balances_or_labels'] = True
    return graph, scope


def replay(policy_path, events_path, members_path, registry_path, jobs_root, work, output):
    output = Path(output)
    policy = read_json(policy_path)
    with gzip.open(registry_path, 'rt', encoding='utf-8-sig', newline='') as handle:
        registry = {r['address']: r for r in csv.DictReader(handle)}

    def identity(address):
        r = registry.get(address)
        if not r:
            return dict(kind='UNKNOWN', status='UNQUERIED', acquisition_scope='NEW_FRONTIER_SAME_POLICY_LOOKUP_PENDING')
        kind = {'SERVICE': 'SERVICE', 'BRIDGE_BOUNDARY': 'BRIDGE', 'MIXER_BOUNDARY': 'MIXER',
                'DEX_OR_PROTOCOL': 'UNSUPPORTED_PROTOCOL', 'CONFLICTED_IDENTITY': 'UNSUPPORTED_PROTOCOL'}.get(r['identity_class'], 'UNKNOWN')
        return dict(kind=kind, actor=r.get('actor'), identity_class=r['identity_class'],
                    status='FROZEN_LOCAL_OBSERVATION', acquisition_scope='FROZEN_STAGE1B_REGISTRY')

    summaries, sources, next_requests = [], [], []
    for pilot in policy['query_pilots']:
        seed, seed_manifest = load_exact_seed(pilot, members_path, events_path)
        provider = SavedDuneProvider(jobs_root, work)
        result = Collector(provider, identity).run(Scope.from_policy(pilot), seed)
        folder = output / pilot['name']
        result.write(folder / 'collection.json')
        write_json(folder / 'request_plan.json', provider.requests)
        graph, scope = live_fixed_graph(result, seed)
        write_json(folder / 'fixed_graph.json', graph)
        write_json(folder / 'model_scope.json', scope)
        used = [j for j in provider.jobs if j['logical_job_id'] in provider.used_jobs]
        observed_ids = {e.event_id for j in used for e in j['events']}
        summary = dict(query_id=pilot['query_id'], name=pilot['name'],
                       status='PARTIAL' if result.unresolved_frontier or result.gaps else 'COMPLETED_WITH_RECORDED_GAPS',
                       collector_status=result.status, acquisition_mode='REAL_DUNE_SAVED_RESULT_REPLAY',
                       live_query_interval_count=len(used), live_exported_rows=sum(j['exported_rows'] for j in used),
                       live_queried_address_count=len({j['scope']['address'] for j in used}),
                       live_normalized_distinct_events=len(observed_ids), candidate_events_observed_by_live_dune=len(set(e['event_id'] for e in result.candidate_events) & observed_ids),
                       live_candidate_events_excluding_seed=len((set(e['event_id'] for e in result.candidate_events) - {seed.event_id}) & observed_ids),
                       unresolved_frontier_count=len(result.unresolved_frontier),
                       complete_requested_interval_count=sum(c['complete'] for c in result.coverage),
                       unqueried_label_addresses=sorted({g['address'] for g in result.gaps if g['reason'] == 'LABEL_UNQUERIED'}),
                       replay_network_requests=0, source_event_pool_usage='EXACT_SEED_ONLY',
                       **{k: v for k, v in result.metrics.items() if k != 'candidate_membership'})
        write_json(folder / 'status.json', summary)
        summaries.append(summary)
        sources.append(dict(query=pilot['name'], seed=seed_manifest, registry_sha256=sha(registry_path),
                            used_jobs=[{k: v for k, v in j.items() if k != 'events'} for j in used],
                            rejected_or_inflight_jobs=provider.rejected_jobs))
        for state in result.unresolved_frontier:
            s = state['state']
            request = dict(query_id=pilot['query_id'], name=pilot['name'], address=s['address'], asset=s['asset'],
                           arrival_event_id=s['arrival']['event_id'], depth=s['depth'], start_block=s['arrival']['block'],
                           end_block=pilot['end_block'], start_time=s['arrival']['timestamp'], end_time=s['local_end'],
                           reason=state['reason'])
            if request not in next_requests:
                next_requests.append(request)
    write_json(output / 'summary.json', summaries)
    write_json(output / 'source_manifest.json', sources)
    write_json(output / 'next_actual_frontier.json', next_requests)
    return summaries


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for name in ['policy', 'events', 'members', 'registry', 'jobs', 'work', 'output']:
        parser.add_argument('--' + name, required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(replay(args.policy, args.events, args.members, args.registry, args.jobs, args.work, args.output), indent=2))
