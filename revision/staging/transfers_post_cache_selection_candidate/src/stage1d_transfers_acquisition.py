"""Finite root-executed Transfers discovery, with independently replayable evidence.

No HTTP, database write or collection occurs on import. The existing RpcAccess
owns transport, retries, shared clock, operation/CU limits and raw reservations.
"""
from contextlib import closing
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
from stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha
import sqlite3
import time

from collector import Scope, NATIVE, Event
from context_access_r3 import read, sha
from context_access_r4 import retry_path
from page_attempts import atomic_json
from read_retry_r4 import logical_key
from stage1d_alchemy_transfers import (TransfersRuntime, TransferPageChain, bind_need_to_scope,
                                      normalize_chain, route_gate, PAGE_KEY_TTL_SECONDS, METHOD)

VERSION = 'stage1d-transfers-acquisition-v1'
FREEZE_SHA = 'ba3f1368ab4f6daaac19e62607892e0a29ad19afd849facd3e7eedd7dc8f9776'
PROVIDER = 'ALCHEMY_ETH_MAINNET_EXISTING'
FIELDS = ('start_block', 'end_block', 'start_time', 'end_time')
POST_CACHE_SELECTION = 'FIRST_20_UNCOVERED_AFTER_VERIFIED_CACHE_V1'


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def inside(work, path):
    work = Path(work).resolve()
    path = (work / path).resolve()
    if not path.is_relative_to(work):
        raise ValueError('Transfers evidence escapes the current workspace')
    cursor = work
    for part in path.relative_to(work).parts:
        cursor /= part
        if cursor.is_symlink() or getattr(cursor, 'is_junction', lambda: False)():
            raise ValueError('Linked Transfers evidence is not accepted')
    return path


def ref(work, path):
    path = inside(work, path)
    return {'path': path.relative_to(Path(work).resolve()).as_posix(), 'sha256': sha(path)}


def checked(work, dependency):
    path = inside(work, dependency['path'])
    if not path.is_file() or sha(path) != dependency['sha256']:
        raise ValueError('SHA-bound Transfers dependency missing or changed')
    return path


def immutable(path, value):
    if Path(path).exists():
        if read(path) != value:
            raise ValueError('Immutable acquisition artifact differs')
    else:
        atomic_json(path, value)


def frozen_query(work, name, *, scope_id=None, scope_hash=None):
    path = active_batch_path(work)
    if path.name == 'BATCH_QUERY_FREEZE.json' and sha(path) != FREEZE_SHA:
        raise ValueError('Original four-query freeze changed')
    batch = batch_for_scope(work, scope_id, scope_hash) if scope_id is not None else read(path)
    query = next(q for q in batch['queries'] if q['name'] == name)
    if name == 'lifi_src001':
        raise ValueError('LI.FI cannot be expanded')
    return query, Scope.from_policy(query)


def gate_dependencies(work):
    folder = Path(work) / 'private/stage1d_transfers_canaries'
    paths = [folder / (kind + '.result.json') for kind in
             ('ordinary_top', 'internal', 'empty', 'pagination_or_boundary_duplicate')]
    results = [read(p) for p in paths]
    gate_path = folder / 'ROUTE_GATE.json'
    gate = read(gate_path)
    if route_gate(results) != gate or gate['external_route'] != 'ALCHEMY_TRANSFERS':
        raise ValueError('Four actual canary decisions do not establish the external route')
    for result in results:
        for dependency in result.get('reference_proof', {}).get('evidence_refs', []):
            checked(work, dependency)
    return [ref(work, p) for p in [gate_path] + paths]


def state_order(state, need):
    arrival = state['arrival']
    return (state['depth'], arrival['block'], -1 if arrival.get('tx_index') is None else arrival['tx_index'],
            arrival['timestamp'], arrival['event_id'], state['address'], *(need[k] for k in FIELDS))


def verify_gate_dependencies(work, dependencies):
    if len(dependencies) != 5:
        raise ValueError('The saved route decision and four actual canary decisions are required')
    gate, *results = [read(checked(work, dependency)) for dependency in dependencies]
    if route_gate(results) != gate or gate['external_route'] != 'ALCHEMY_TRANSFERS':
        raise ValueError('Saved route decision does not regenerate')


def _merge_overlapping_needs(ranked):
    """Merge only exact rectangle unions within an otherwise identical need.

    A crossing/L-shaped union is deliberately retained as separate requests.
    The earliest original frontier rank survives; caller-owned rows are copied.
    """
    groups = {}
    for order, need in ranked:
        identity = json.dumps({k: v for k, v in need.items() if k not in FIELDS},
                              sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        rows = groups.setdefault(identity, [])
        current, current_order = deepcopy(need), order
        index = 0
        while index < len(rows):
            prior_order, prior = rows[index]
            contains = lambda a, b: (a['start_block'] <= b['start_block'] <= b['end_block'] <= a['end_block']
                                    and a['start_time'] <= b['start_time'] <= b['end_time'] <= a['end_time'])
            block_same = all(current[k] == prior[k] for k in ('start_block', 'end_block'))
            time_same = all(current[k] == prior[k] for k in ('start_time', 'end_time'))
            block_overlap = max(current['start_block'], prior['start_block']) <= min(current['end_block'], prior['end_block'])
            time_overlap = max(current['start_time'], prior['start_time']) <= min(current['end_time'], prior['end_time'])
            exact_union = (contains(current, prior) or contains(prior, current)
                           or block_same and time_overlap or time_same and block_overlap)
            if not exact_union:
                index += 1
                continue
            current.update(start_block=min(current['start_block'], prior['start_block']),
                           end_block=max(current['end_block'], prior['end_block']),
                           start_time=min(current['start_time'], prior['start_time']),
                           end_time=max(current['end_time'], prior['end_time']))
            current_order = min(current_order, prior_order)
            rows.pop(index)
            # A new exact union can contain an earlier unmergeable rectangle.
            index = 0
        rows.append((current_order, current))
    return sorted((row for rows in groups.values() for row in rows), key=lambda item: item[0])


def _ordered_needs(query, entry):
    """The original complete frontier order and exact rectangle merge, unsliced."""
    scope = Scope.from_policy(query)
    ranked = []
    for need in entry['needed_ranges']:
        need = bind_need_to_scope(need, scope)
        states = [front['state'] for front in entry['frontier']
                  if front.get('reason') == 'INTERVAL_INCOMPLETE'
                  and front['state']['address'] == need['address']
                  and front['state']['asset'] == NATIVE
                  and front['state'].get('protocol_context') == 'ordinary'
                  and front['state']['depth'] < scope.max_depth
                  and front['state']['arrival']['block'] <= need['start_block']
                  and front['state']['arrival']['timestamp'] <= need['start_time']]
        if not states:
            raise ValueError('Current need has no expandable ordinary frontier state')
        state = min(states, key=lambda s: state_order(s, need))
        ranked.append((state_order(state, need), need))
    ranked.sort(key=lambda item: item[0])
    return [need for _, need in _merge_overlapping_needs(ranked)]


def select_needs(query, entry, maximum=20):
    """Preserve the original source-prefix contract for existing saved plans."""
    if type(maximum) is not int or not 1 <= maximum <= 20:
        raise ValueError('One query can select at most twenty current needs')
    return _ordered_needs(query, entry)[:maximum]


def _validate_selection(query, entry, plan):
    """Bind a new plan to its ordered, scanned source prefix and saved difference.

    Historical cache checks describe preparation. acquire still rechecks current
    verified coverage before dispatch; a newer successful cache only skips work.
    """
    if 'selection_policy' not in plan:
        if any(k in plan for k in ('ordered_source_count', 'ordered_source_sha256')):
            raise ValueError('Post-cache selection metadata requires its explicit policy')
        if select_needs(query, entry, plan['maximum_needs']) != plan['source_needs']:
            raise ValueError('Scheduled needs do not regenerate from current boundary states')
        return
    if plan['selection_policy'] != POST_CACHE_SELECTION:
        raise ValueError('Unknown Transfers selection policy')
    maximum = plan['maximum_needs']
    if type(maximum) is not int or not 1 <= maximum <= 20:
        raise ValueError('One query can select at most twenty actual missing needs')
    ordered = _ordered_needs(query, entry)
    if (type(plan.get('ordered_source_count')) is not int
            or plan['ordered_source_count'] != len(ordered)
            or plan.get('ordered_source_sha256') != digest(ordered)):
        raise ValueError('Complete ordered source identity differs')
    source = plan['source_needs']
    checks = plan.get('cache_checks')
    if (not isinstance(source, list) or len(source) > len(ordered)
            or source != ordered[:len(source)] or not isinstance(checks, list)
            or len(checks) != len(source)):
        raise ValueError('Cache scan is not the continuous ordered source prefix')
    from stage1d_window import missing_rectangles
    expected = []
    for need, check in zip(source, checks):
        if len(expected) >= maximum:
            raise ValueError('Cache scan continued beyond its first actual need bound')
        if (check.get('need') != need or not isinstance(check.get('coverage'), list)
                or not check['coverage'] or not isinstance(check.get('missing'), list)
                or check['coverage'][0].get('uncovered_intervals') != check['missing']):
            raise ValueError('Saved cache difference does not bind to its source and coverage')
        coverage = check['coverage'][0]
        requested = {k: need[k] for k in ('address', 'asset') + FIELDS}
        intervals = coverage.get('verified_content_intervals')
        if (any(coverage.get(k) != v for k, v in requested.items())
                or not isinstance(intervals, list)):
            raise ValueError('Saved cache coverage has no exact source/interval binding')
        completed = []
        for interval in intervals:
            if (not isinstance(interval, dict) or type(interval.get('complete')) is not bool
                    or any(type(interval.get(k)) is not int for k in FIELDS)
                    or interval['start_block'] > interval['end_block']
                    or interval['start_time'] > interval['end_time']):
                raise ValueError('Invalid saved verified-content rectangle')
            completed.append(dict(interval, address=need['address'], asset=need['asset']))
        if missing_rectangles(requested, completed) != check['missing']:
            raise ValueError('Saved missing intervals are not the canonical exact coverage difference')
        for interval in check['missing']:
            if (not isinstance(interval, dict)
                    or any(type(interval.get(k)) is not int for k in FIELDS)
                    or not (need['start_block'] <= interval['start_block'] <= interval['end_block'] <= need['end_block']
                            and need['start_time'] <= interval['start_time'] <= interval['end_time'] <= need['end_time'])):
                raise ValueError('Saved cache difference escapes the exact source rectangle')
            if len(expected) < maximum:
                expected.append(dict(need, **{k: interval[k] for k in FIELDS}))
    if len(expected) < maximum and len(source) != len(ordered):
        raise ValueError('Cache scan stopped before exhausting sources or filling its bound')
    if plan['needs'] != expected:
        raise ValueError('Scheduled needs differ from the original ordered cache difference')


def _verify_selection_cache(cache, plan):
    """Every historically skipped source portion must still be proved complete.

    The saved cache report is not a trusted certificate. Current gaps may shrink
    after additional success, but may not exceed its recorded missing union.
    """
    if plan.get('selection_policy') != POST_CACHE_SELECTION:
        return  # Keep existing saved-plan recovery on its original path.
    from stage1d_window import missing_rectangles
    for need, check in zip(plan['source_needs'], plan['cache_checks']):
        current = cache.fetch_interval(need['address'], need['asset'], need['start_block'], need['end_block'],
            start_time=need['start_time'], end_time=need['end_time'], global_end_time=need['end_time'])
        if current.fact_conflicts:
            raise ValueError('Existing physical fact conflict requires reconciliation before acquisition')
        saved_missing = [dict(interval, address=need['address'], asset=need['asset'], complete=True)
                         for interval in check['missing']]
        for interval in current.coverage[0]['uncovered_intervals']:
            required = dict(interval, address=need['address'], asset=need['asset'])
            if missing_rectangles(required, saved_missing):
                raise ValueError('Saved cache selection omitted a currently unproved source gap')


def prepare(work, boundary_path, query_name, output, *, maximum=20):
    """Snapshot current input; no candidate/neighbor creation or network operation."""
    work = Path(work).resolve()
    boundary_path = inside(work.parent, boundary_path)
    boundary = read(boundary_path)
    query, _ = frozen_query(work, query_name)
    if boundary.get('batch_freeze_sha256') != sha(active_batch_path(work)):
        raise ValueError('Boundary belongs to another freeze')
    entry = next(q for q in boundary['queries'] if q['query_name'] == query_name)
    if entry['query_id'] != query['query_id'] or entry['scope_hash'] != query['scope_hash']:
        raise ValueError('Boundary query identity differs')
    checked(work, {'path': entry['collection_path'], 'sha256': entry['collection_sha256']})
    for label in boundary['labels']:
        checked(work, label)
    folder = inside(work, output)
    from stage1d_acquisition import CachedIntervals
    if type(maximum) is not int or not 1 <= maximum <= 20:
        raise ValueError('One query can select at most twenty actual missing needs')
    ordered = _ordered_needs(query, entry)
    source_needs = []
    cache = CachedIntervals(work)
    cache.bind_scope(Scope.from_policy(query))
    selected, cache_checks = [], []
    for need in ordered:
        if len(selected) >= maximum: break
        source_needs.append(need)
        found = cache.fetch_interval(need['address'], need['asset'], need['start_block'], need['end_block'],
            start_time=need['start_time'], end_time=need['end_time'], global_end_time=need['end_time'])
        if found.fact_conflicts:
            raise ValueError('Existing physical fact conflict requires reconciliation before acquisition')
        missing = found.coverage[0]['uncovered_intervals']
        cache_checks.append({'need': need, 'coverage': found.coverage, 'missing': missing})
        for interval in missing:
            if len(selected) >= maximum: break
            selected.append(dict(need, **{k: interval[k] for k in FIELDS}))
    immutable(folder / 'boundary_input.json', boundary)
    plan = {'version': VERSION, 'query_name': query_name, 'query_id': query['query_id'],
            'scope_hash': query['scope_hash'], 'freeze_sha256': sha(active_batch_path(work)),
            'boundary_snapshot': ref(work, folder / 'boundary_input.json'),
            'original_boundary_sha256': sha(boundary_path), 'needs': selected,
             'source_needs': source_needs, 'cache_checks': cache_checks,
             'selection_policy': POST_CACHE_SELECTION,
             'ordered_source_count': len(ordered), 'ordered_source_sha256': digest(ordered),
            'route_dependencies': gate_dependencies(work), 'maximum_needs': maximum,
            'ordering': 'STATE_DEPTH_THEN_ARRIVAL_BLOCK_TX_INDEX_TIME_EVENT_ID_ADDRESS',
            'cache_order': 'CURRENT_INTERVALS_AND_ADMITTED_LEGACY_THEN_SHARED_RPC_SUCCESS_THEN_NETWORK'}
    _validate_selection(query, entry, plan)
    immutable(folder / 'plan.json', plan)
    return plan


def _verified_legacy_point(work, plan, member, envelope):
    """Recheck admitted copied point bytes without claiming an old HTTP status.

    The legacy import is a separate saved format, never a Transfers-page or
    missing-raw exception. No old absolute provenance path is accessed.
    """
    schema = 'stage1d-legacy-rpc-point-admission-v1'
    methods = {'eth_getTransactionByHash', 'eth_getTransactionReceipt', 'eth_getBlockByNumber'}
    request, response = envelope.get('request', {}), envelope.get('response', {})
    if (plan.get('method') not in methods or set(plan) != {'method', 'params'}
            or envelope.get('schema') != schema or envelope.get('provider_alias') != PROVIDER
            or envelope.get('evidence_kind') != 'REAL_CHAIN_LEGACY_RAW_REUSE'
            or envelope.get('original_http_status_not_preserved') is not True
            or envelope.get('http_status') is not None or envelope.get('import_is_new_provider_dispatch') is not False
            or envelope.get('response_complete') is not True or envelope.get('status') != 'SUCCESS_VALIDATED'
            or member.get('cache_hit') is not True or member.get('local_import') is not True
            or member.get('new_network_requests') != 0 or member.get('new_alchemy_cu') != 0
            or {k: request.get(k) for k in ('method', 'params')} != plan
            or TransfersRuntime().rpc_result_status(request, response) != 'SUCCESS_VALIDATED'
            or member.get('result', response.get('result')) != response.get('result')):
        raise ValueError('Admitted legacy point request/result binding differs')
    source = envelope.get('legacy_source', {})
    raw_sha, manifest_sha = source.get('raw_sha256'), source.get('manifest_sha256')
    if any(not isinstance(h, str) or len(h) != 64 or any(c not in '0123456789abcdef' for c in h)
           for h in (raw_sha, manifest_sha)):
        raise ValueError('Admitted legacy copied SHA invalid')
    raw_name = 'raw/legacy_rpc/' + raw_sha + '.json'
    manifest_name = 'private/stage1d_legacy_import/manifests/' + manifest_sha + '.json'
    if (envelope.get('raw_path') != raw_name or envelope.get('manifest_path') != manifest_name
            or envelope.get('raw_body_sha256') != raw_sha or member.get('legacy_raw_sha256') != raw_sha):
        raise ValueError('Admitted legacy copied paths differ')
    raw = checked(work, {'path': raw_name, 'sha256': raw_sha})
    manifest = checked(work, {'path': manifest_name, 'sha256': manifest_sha})
    if (type(source.get('raw_bytes')) is not int or type(source.get('manifest_bytes')) is not int
            or raw.stat().st_size != source['raw_bytes'] or manifest.stat().st_size != source['manifest_bytes']
            or raw.stat().st_size > 16 * 1024 * 1024 or read(raw) != response):
        raise ValueError('Admitted legacy copied bytes/response differ')
    admission_path = checked(work, {'path': member['admission_path'], 'sha256': member['admission_sha256']})
    admission = read(admission_path)
    key = logical_key(TransfersRuntime().rpc_identity(PROVIDER, plan))
    if (admission.get('schema') != schema or admission.get('status') != 'ADMISSIBLE_POINT_PENDING_ROOT_APPLY'
            or admission.get('freeze_sha256') != FREEZE_SHA or admission.get('logical_key') != key
            or admission.get('legacy_source') != source or admission.get('plan') != plan
            or admission.get('request_sha256') != digest({'chain_id': 1, **plan})
            or admission.get('coverage_complete') is not False or not isinstance(admission.get('need'), dict)
            or not isinstance(admission.get('normalization'), dict)
            or any(admission.get(k) != 0 for k in ('new_network_requests', 'new_rpc_operations',
                                                  'new_alchemy_cu', 'new_bigquery_scanned_bytes'))):
        raise ValueError('Admitted legacy durable admission differs')
    admission_key = digest({'logical_key': key, 'need': admission['need'], 'raw_sha256': raw_sha})
    if (member['admission_path'] != 'private/stage1d_legacy_import/admissions/' + admission_key + '.json'
            or member['artifact_path'] != 'private/stage1d_legacy_import/envelopes/' + key + '_' + raw_sha + '.json'
            or envelope.get('strict_normalizer') != admission['normalization'].get('normalizer')):
        raise ValueError('Admitted legacy receipt/admission identity differs')
    ordinal = source.get('manifest_ordinal')
    entries = read(manifest).get('entries')
    if type(ordinal) is not int or ordinal < 0 or not isinstance(entries, list) or ordinal >= len(entries):
        raise ValueError('Admitted legacy manifest ordinal invalid')
    entry = entries[ordinal]
    if (not isinstance(entry, dict) or str(entry.get('file', '')).lower() != raw_sha + '.json'
            or str(entry.get('response_sha256', '')).lower() != raw_sha
            or entry.get('bytes') != source['raw_bytes'] or entry.get('method') != plan['method']
            or str(entry.get('request_sha256', '')).lower() != admission['request_sha256']):
        raise ValueError('Admitted legacy manifest request/payload binding differs')
    return deepcopy(response['result'])


def verified_member(work, plan, member):
    """Derive a response from exact original raw body, never a success Boolean."""
    path = checked(work, {'path': member['artifact_path'], 'sha256': member['artifact_sha256']})
    envelope = read(path)
    if envelope.get('evidence_kind') == 'REAL_CHAIN_LEGACY_RAW_REUSE':
        return _verified_legacy_point(work, plan, member, envelope)
    request, response = envelope.get('request', {}), envelope.get('response', {})
    if (envelope.get('provider_alias') != PROVIDER or envelope.get('evidence_kind') != 'REAL_CHAIN'
            or envelope.get('http_status') != 200 or envelope.get('response_complete') is not True
            or envelope.get('status') != 'SUCCESS_VALIDATED'
            or {k: request.get(k) for k in ('method', 'params')} != plan
            or TransfersRuntime().rpc_result_status(request, response) != 'SUCCESS_VALIDATED'
            or member.get('result', response.get('result')) != response.get('result')):
        raise ValueError('RPC raw member request/result binding differs')
    source = path
    compressed = envelope.get('decoding') == 'BOUNDED_ZSTD_FROM_SAVED_HTTP200_BYTES'
    if 'original_artifact' in envelope:
        if not compressed or envelope.get('new_http_requests') != 0:
            raise ValueError('Unknown saved-response recovery form')
        source = checked(work, envelope['original_artifact'])
        original = read(source)
        if original['request'] != request or original['raw_body_sha256'] != envelope['raw_body_sha256']:
            raise ValueError('Recovered response original binding differs')
    raw = source.parent / 'response_body.bin'
    if not raw.is_file() and plan['method'] != METHOD and member.get('cache_hit') is True:
        # Keep the already accepted ordinary RPC cache contract. Its immutable
        # envelope SHA and exact successful request/response remain evidence;
        # a new Transfers page NEVER receives this inherited exception.
        return deepcopy(response['result'])
    if not raw.is_file() or sha(raw) != envelope['raw_body_sha256']:
        raise ValueError('CACHE_RAW_PROOF_MISSING_OR_CHANGED')
    body = raw.read_bytes()
    if compressed:
        import io
        from compression import zstd
        with zstd.ZstdFile(io.BytesIO(body)) as stream:
            body = stream.read(8388609)
    if len(body) > 8388608:
        raise ValueError('Saved RPC body exceeds the bounded transport response')
    responses = json.loads(body)
    matches = [r for r in responses if isinstance(r, dict) and type(r.get('id')) is type(request['id']) and r.get('id') == request['id']] if isinstance(responses, list) else []
    if len(matches) != 1 or matches[0] != response:
        raise ValueError('Original batch bytes do not establish exactly this member')
    return deepcopy(response['result'])


def cached_member(work, plan):
    """Read the same successful request identity; missing provenance never retries."""
    path = retry_path(work)
    if not path.exists():
        return None
    identity = TransfersRuntime().rpc_identity(PROVIDER, plan)
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as database:
        row = database.execute('SELECT success_payload,success_receipt FROM read_requests WHERE logical_key=? AND state=?',
                               (logical_key(identity), 'SUCCESS')).fetchone()
    if row is None:
        return None
    value, receipt = json.loads(row[0]), json.loads(row[1])
    member = dict(receipt, result=value, status='SUCCESS_VALIDATED', cache_hit=True)
    verified_member(work, plan, member)
    envelope_path = inside(work, member['artifact_path'])
    envelope = read(envelope_path)
    original = inside(work, envelope['original_artifact']['path']) if envelope.get('original_artifact') else envelope_path
    receipt_path = original.parent / 'receipt.json'
    if receipt_path.exists():
        receipt = read(receipt_path)
        if receipt.get('raw_body_sha256') == envelope['raw_body_sha256'] and receipt.get('utc'):
            member['origin_received_at_seconds'] = datetime.fromisoformat(receipt['utc'].replace('Z', '+00:00')).timestamp()
    return member


def restore_chain(work, need, pages, page_size=1000):
    chain = TransferPageChain(need, page_size=page_size)
    for page in pages:
        value = verified_member(work, page['plan'], page)
        member = dict(page, status='SUCCESS_VALIDATED', result=value,
                      origin_received_at_seconds=page.get('cursor_origin_seconds'))
        chain.append(page['plan'], member, received_at_seconds=page['received_at_seconds'],
                     requested_at_seconds=page['requested_at_seconds'])
    return chain


def restore_state_chain(work, state):
    """Recheck original headers and legacy page identities before narrowed pages."""
    if state.get('timestamp_bracket') is None:
        return restore_chain(work, state['need'], state['pages'])
    headers = {}
    query, _ = frozen_query(work, state['need']['query_name'], scope_id=state['need']['scope_id'], scope_hash=state['need']['scope_hash'])
    for number, member in state['timestamp_headers'].items():
        block = int(number)
        if str(block) != number or block in headers:
            raise ValueError('Canonical unique saved timestamp header number required')
        plan = {'method': 'eth_getBlockByNumber', 'params': [hex(block), False]}
        headers[block] = verified_member(work, plan, member)
        for side in ('start', 'end'):
            if block == query[side + '_block'] and query.get(side + '_block_hash'):
                if headers[block]['hash'].lower() != query[side + '_block_hash'].lower():
                    raise ValueError('Saved bracket conflicts with frozen endpoint hash')
    chain = TransferPageChain(state['need'], timestamp_bracket=state['timestamp_bracket'], bracket_headers=headers)
    prior = state.get('prior_timestamp_mapping_state')
    if prior is not None:
        old = read(checked(work, prior))
        if old['need'] != state['need'] or old.get('timestamp_bracket') is not None:
            raise ValueError('Historical page identity changed during timestamp mapping')
        legacy = restore_chain(work, old['need'], old['pages'])
        first, last = state['timestamp_bracket']['first_block'], state['timestamp_bracket']['last_block']
        if not chain.empty_time_intersection:
            last_seen = int(legacy.rows[-1]['blockNum'], 16) if legacy.rows else first
            if legacy.closed or legacy.rows and last_seen > last:
                chain.rows = [deepcopy(r) for r in legacy.rows if first <= int(r['blockNum'], 16) <= last]
                chain.closed = True
                chain.legacy_superset_summary = legacy.summary()
            elif legacy.rows:
                chain.page_start_block = max(first, last_seen)
                chain.rows = [deepcopy(r) for r in legacy.rows if first <= int(r['blockNum'], 16) < chain.page_start_block]
                chain.legacy_prefix_summary = legacy.summary()
                from stage1d_alchemy_transfers import _row_core
                chain._seen_rows = {r['uniqueId']: _row_core(r) for r in chain.rows}
                chain._required_legacy_boundary_rows = {r['uniqueId']: _row_core(r) for r in legacy.rows
                    if int(r['blockNum'], 16) == chain.page_start_block}
    for page in state['pages']:
        value = verified_member(work, page['plan'], page)
        member = dict(page, status='SUCCESS_VALIDATED', result=value,
                      origin_received_at_seconds=page.get('cursor_origin_seconds'))
        chain.append(page['plan'], member, received_at_seconds=page['received_at_seconds'],
                     requested_at_seconds=page['requested_at_seconds'])
    return chain


def _ensure_timestamp_mapping(work, query, state, state_path, access):
    """Finite cache-first historical header reads, using the same RpcAccess ledger."""
    from stage1d_timestamp_bracket import resolve_timestamp_bracket, header_timestamp
    if state.get('timestamp_bracket') is not None:
        return restore_state_chain(work, state)
    if state['pages']:
        legacy = restore_chain(work, state['need'], state['pages'])
        if legacy.closed:
            # Already exhausted original evidence needs neither a new index
            # request nor header discovery. Preserve the original callback
            # ordering and request identity before any missing point binding.
            return legacy
    else:
        # A successful exact old first page can predate this mutable state.
        # Reuse its request identity before asking for any new header. Partial
        # first-page evidence remains a prefix for the new narrowed request.
        legacy = TransferPageChain(state['need'])
        request = legacy.next_plan(time.time())
        member = cached_member(work, request)
        if member is not None:
            verified_member(work, request, member)
            legacy.append(request, member, received_at_seconds=time.time(), requested_at_seconds=None)
            state['pages'] = legacy.summary()['pages']
            atomic_json(state_path, state)
            if legacy.closed:
                return legacy
    state.setdefault('timestamp_headers', {})
    state.setdefault('timestamp_header_selectors_submitted', 0)
    previous = {digest(item['plan']): item['member'] for item in state['bindings']}
    def get_header(block):
        request = {'method': 'eth_getBlockByNumber', 'params': [hex(block), False]}
        member = state['timestamp_headers'].get(str(block)) or previous.get(digest(request))
        if member is None:
            member = cached_member(work, request)
        if member is None:
            state['timestamp_header_selectors_submitted'] += 1
            atomic_json(state_path, state)
            response = access.call_batch([request], query['name'], 'current_need_exact_timestamp_block_bracket', capability=False)
            immutable(Path(state_path).parent / ('timestamp_header_' + str(block) + '_' + digest(response) + '.json'), response)
            if len(response.get('members', [])) != 1:
                raise ValueError('Exactly one historical timestamp header response required')
            member = response['members'][0]
            if member.get('status') != 'SUCCESS_VALIDATED':
                state['failures'].append({'plan': request, 'member': member, 'phase': 'TIMESTAMP_BRACKET'})
                atomic_json(state_path, state)
                raise ValueError('TIMESTAMP_HEADER_BLOCKED_SAME_REQUEST_PRESERVED')
        value = verified_member(work, request, member)
        header_timestamp(block, value)
        for side in ('start', 'end'):
            if block == query[side + '_block'] and query.get(side + '_block_hash'):
                if value['hash'].lower() != query[side + '_block_hash'].lower():
                    raise ValueError('Timestamp bracket header conflicts with frozen endpoint hash')
        state['timestamp_headers'][str(block)] = member
        atomic_json(state_path, state)
        return value
    proof = resolve_timestamp_bracket(state['need'], get_header)
    if state['pages']:
        # Old raw/envelopes and their exact global-range request identities stay
        # unchanged. Reuse a complete superset or its ordered prefix; only a
        # final possibly-partial block is requested again under the new bound.
        original = deepcopy(state)
        archived = Path(state_path).parent / ('before_timestamp_mapping_' + digest(original) + '.json')
        immutable(archived, original)
        state['prior_timestamp_mapping_state'] = ref(work, archived)
        state['pages'] = []
        state.pop('point_cache_admission', None)
    state['timestamp_bracket'] = proof
    chain = restore_state_chain(work, state)
    allowed = {digest(p) for p in binding_plans(chain)}
    state['bindings'] = [item for item in state['bindings'] if digest(item['plan']) in allowed]
    state['timestamp_mapping_authority'] = 'STAGE1D_WINDOW_ROLE_SEMANTIC_CLOSURE_V1'
    atomic_json(state_path, state)
    return chain


def binding_plans(chain):
    tops = [r for r in chain.rows if r['category'] == 'external']
    transactions = sorted({r['hash'].lower() for r in tops})
    blocks = sorted({int(r['blockNum'], 16) for r in tops})
    return ([{'method': 'eth_getTransactionByHash', 'params': [tx]} for tx in transactions]
            + [{'method': 'eth_getTransactionReceipt', 'params': [tx]} for tx in transactions]
            + [{'method': 'eth_getBlockByNumber', 'params': [hex(block), False]} for block in blocks])


def normalized_state(work, state):
    chain = restore_state_chain(work, state)
    transactions, receipts, headers = {}, {}, {}
    allowed = {digest(p) for p in binding_plans(chain)}
    seen = set()
    for item in state['bindings']:
        plan = item['plan']
        key = digest(plan)
        if key not in allowed or key in seen:
            raise ValueError('Unknown or duplicate exact binding request')
        seen.add(key)
        value = verified_member(work, plan, item['member'])
        method, key = plan['method'], plan['params'][0]
        target = headers if method == 'eth_getBlockByNumber' else receipts if method == 'eth_getTransactionReceipt' else transactions
        target[int(key, 16) if method == 'eth_getBlockByNumber' else key] = value
    result = normalize_chain(chain, transactions=transactions, receipts=receipts, headers=headers)
    # No internal event gets an invented trace position or ancestry certificate.
    for gap in result['gaps']:
        gap['needed_range'] = state['need']
    return result


def emit_interval(work, state):
    normalized = normalized_state(work, state)
    proof = {'version': VERSION, 'state': state, 'normalized': normalized}
    key = digest(proof)
    folder = Path(work) / 'private/stage1d_transfers_acquisition/proofs' / key
    immutable(folder / 'proof.json', proof)
    immutable(folder / 'events.json', normalized['events'])
    need = state['need']
    complete = normalized['positive_native_index_complete']
    record = {'acquisition_adapter': VERSION, 'provider': 'Alchemy', 'evidence_id': key,
              'addresses': [need['address']], 'asset': NATIVE, **{k: need[k] for k in FIELDS},
              'complete': complete, 'native_scope_complete': complete, 'all_asset_export_complete': False,
              'coverage_capability': 'NATIVE_INDEX_ONLY', 'normalization_gaps': normalized['gaps'],
              'native_decision_path': ref(work, folder / 'proof.json')['path'], 'native_decision_sha256': sha(folder / 'proof.json'),
              'events_path': ref(work, folder / 'events.json')['path'], 'events_sha256': sha(folder / 'events.json'),
              'query_id_at_acquisition': need['query_id'], 'scope_id_at_acquisition': need['scope_id'],
              'coverage_basis': normalized['index_chain'].get('closure_basis', 'VERIFIED_EXHAUSTED_TRANSFERS_CHAIN_AND_EXACT_TOP_RPC_BINDINGS'),
              'context_complete': False}
    verify_interval_record(work, record)
    destination = Path(work) / 'derived/stage1d/intervals' / ('alchemy_' + key + '.coverage.json')
    immutable(destination, record)
    return {'coverage_record': ref(work, destination), 'complete': complete, 'events': len(normalized['events']), 'gaps': normalized['gaps']}


def verify_interval_record(work, record):
    if record.get('acquisition_adapter') != VERSION or record.get('asset') != NATIVE or record.get('coverage_capability') != 'NATIVE_INDEX_ONLY':
        raise ValueError('Exact native Transfers cache capability required')
    proof = read(checked(work, {'path': record['native_decision_path'], 'sha256': record['native_decision_sha256']}))
    if proof.get('version') != VERSION or digest(proof) != record['evidence_id']:
        raise ValueError('Transfers proof identity changed')
    state = proof['state']
    _, scope = frozen_query(work, state['need']['query_name'], scope_id=state['need'].get('scope_id'), scope_hash=state['need'].get('scope_hash'))
    need = bind_need_to_scope(state['need'], scope)
    verify_gate_dependencies(work, state['route_dependencies'])
    if record['addresses'] != [need['address']] or any(record[k] != need[k] for k in FIELDS):
        raise ValueError('Transfers coverage rectangle differs from proved request')
    regenerated = normalized_state(work, state)
    events = read(checked(work, {'path': record['events_path'], 'sha256': record['events_sha256']}))
    if regenerated != proof['normalized'] or events != regenerated['events']:
        raise ValueError('Events or gaps do not regenerate from original RPC bytes')
    complete = regenerated['positive_native_index_complete']
    if (record.get('complete') is not complete or record.get('native_scope_complete') is not complete
            or record.get('all_asset_export_complete') is not False or record.get('context_complete') is not False
            or record['normalization_gaps'] != regenerated['gaps']):
        raise ValueError('Transfers completeness claim exceeds its raw evidence')
    return regenerated


def _save_binding(work, state, state_path, request, member, known):
    verified_member(work, request, member)
    key = digest(request)
    if key not in known:
        state['bindings'].append({'plan': request, 'member': member})
        known.add(key)
        atomic_json(state_path, state)


def _acquire_bindings(work, state, state_path, requests, access, query_name, counters, maximum):
    """Cache first; five ordinary selectors or one receipt, with durable members.

    counters['members'] retains the old single-call scheduling unit: individual
    missing point selectors submitted, not invoices or HTTP/retry attempts.
    Every actual fee and retry remains owned by the unchanged RpcAccess.
    """
    known = {digest(item['plan']) for item in state['bindings']}
    prior_bound_count = len(known)
    cache_success_count = 0
    pending = []
    for request in requests:
        if digest(request) in known:
            continue
        if request['method'] not in ('eth_getTransactionByHash', 'eth_getTransactionReceipt', 'eth_getBlockByNumber'):
            raise ValueError('Only the existing exact point binding methods may be grouped')
        member = cached_member(work, request)
        if member is None:
            pending.append(request)
        else:
            _save_binding(work, state, state_path, request, member, known)
            cache_success_count += 1
    if len(pending) > 200:
        # This is a routing trigger after exact successful-cache subtraction,
        # not a truncation limit, a hard-resource stop, or permission to discard
        # the closed index and its still-unbound physical transactions.
        state['status'] = 'BATCH_BINDING_REQUIRED'
        state['batch_binding_required'] = {
            'schema_version': 'stage1d-missing-point-batch-route-v1',
            'missing_selector_count': len(pending), 'missing_selectors': pending,
            'requested_selector_count': len(requests), 'prior_bound_selector_count': prior_bound_count,
            'successful_cache_selectors_reused_this_pass': cache_success_count,
            'bound_selector_count_after_cache': len(known),
            'threshold': 200, 'threshold_unit': 'EXACT_INDIVIDUAL_POINT_SELECTORS_AFTER_SUCCESS_CACHE',
            'preferred_route': 'CLASSIC_BIGQUERY_TRANSACTION_AND_FULL_TRACE_FAMILY',
            'point_network_dispatched_for_missing_set': False,
            'needs_preserved': True, 'hard_resource_block': False,
            'retained_requirement': 'Normal/failed/zero-value top rows, real Gas, full internal ancestors and siblings'}
        atomic_json(state_path, state)
        return
    while pending:
        remaining = maximum - counters['members']
        if remaining <= 0:
            state['status'] = 'SCHEDULING_BINDING_BOUND_PAUSED'
            atomic_json(state_path, state)
            return
        batch = pending[:1]
        if batch[0]['method'] != 'eth_getTransactionReceipt':
            for request in pending[1:min(5, remaining)]:
                if request['method'] == 'eth_getTransactionReceipt':
                    break
                batch.append(request)
        counters['members'] += len(batch)
        counters['dispatches'] += 1
        try:
            response = access.call_batch(batch, query_name, 'recovery_candidate_exact_top_binding', capability=False)
            immutable(Path(state_path).parent / ('binding_call_' + digest(batch) + '_' + digest(response) + '.json'), response)
            if len(response.get('members', [])) != len(batch):
                raise ValueError('Exact binding batch member count differs')
            blocked = False
            # RpcAccess returns input order, independently of server JSON order.
            # Verify each successful envelope against its exact input selector.
            for request, member in zip(batch, response['members']):
                if member.get('status') == 'SUCCESS_VALIDATED':
                    _save_binding(work, state, state_path, request, member, known)
                else:
                    state['failures'].append({'plan': request, 'member': member})
                    state['status'] = 'RPC_MEMBER_BLOCKED'
                    atomic_json(state_path, state)
                    blocked = True
            if blocked:
                return
        except Exception as exc:
            # A later reserve/setup/deadline error can follow earlier successes
            # already settled by RpcAccess. Recover only those same-key saved
            # successes; this path never dispatches or grants another retry.
            recover_errors = []
            for request in batch:
                if digest(request) in known:
                    continue
                try:
                    member = cached_member(work, request)
                    if member is not None:
                        _save_binding(work, state, state_path, request, member, known)
                except Exception as recovery_error:
                    recover_errors.append({'plan': request, 'error_class': type(recovery_error).__name__,
                                           'detail': str(recovery_error)})
            state['failures'].append({'plans': batch, 'error_class': type(exc).__name__, 'detail': str(exc),
                                      'same_key_cache_recovery_errors': recover_errors})
            state['status'] = 'RAW_OR_RESOURCE_BLOCKED_SAME_REQUEST_PRESERVED'
            atomic_json(state_path, state)
            raise
        pending = pending[len(batch):]


def acquire(work, plan_path, access, *, clock=time.time, maximum_pages=100, maximum_binding_calls=2000,
            point_cache_admitter=None):
    """Root invokes this serial worker. Stops preserve all responses and same keys.

    Work maxima are per-invocation scheduling bounds, not new resource budgets.
    Expired cursors produce a persisted overlap proposal, never an automatic
    differently keyed request. A later root decision can adopt that proposal.
    """
    from stage1d_acquisition import CachedIntervals
    work = Path(work).resolve()
    if not isinstance(access.runtime, TransfersRuntime) or Path(access.w).resolve() != work:
        raise ValueError('The current shared TransfersRuntime RpcAccess is required')
    if type(maximum_pages) is not int or maximum_pages < 1 or type(maximum_binding_calls) is not int or maximum_binding_calls < 1:
        raise ValueError('Positive finite scheduling bounds required')
    plan_path = inside(work, plan_path)
    plan = read(plan_path)
    if plan['version'] != VERSION or not 0 <= len(plan['needs']) <= 20:
        raise ValueError('Frozen single-query batch of one to twenty needs required')
    query, scope = frozen_query(work, plan['query_name'])
    boundary = read(checked(work, plan['boundary_snapshot']))
    entry = next(q for q in boundary['queries'] if q['query_name'] == query['name'])
    _validate_selection(query, entry, plan)
    for need in plan['needs']:
        if not any(need['address'] == old['address'] and need['asset'] == old['asset']
                   and old['start_block'] <= need['start_block'] <= need['end_block'] <= old['end_block']
                   and old['start_time'] <= need['start_time'] <= need['end_time'] <= old['end_time'] for old in plan['source_needs']):
            raise ValueError('Cache-subtracted need extends beyond the current boundary')
    verify_gate_dependencies(work, plan['route_dependencies'])
    cached = CachedIntervals(work)
    cached.bind_scope(scope)
    _verify_selection_cache(cached, plan)
    results, pages = [], 0
    binding_counters = {'members': 0, 'dispatches': 0}
    for need in plan['needs']:
        need = bind_need_to_scope(need, scope)
        cached_result = cached.fetch_interval(need['address'], need['asset'], need['start_block'], need['end_block'],
            start_time=need['start_time'], end_time=need['end_time'], global_end_time=scope.end_time)
        if cached_result.complete:
            results.append({'need': need, 'status': 'CURRENT_OR_ADMITTED_INTERVAL_CACHE_COMPLETE'})
            continue
        folder = work / 'private/stage1d_transfers_acquisition/needs' / digest(need)
        state_path = folder / 'state.json'
        state = read(state_path) if state_path.exists() else {'version': VERSION, 'need': need, 'pages': [],
            'bindings': [], 'failures': [], 'route_dependencies': plan['route_dependencies'], 'plan_sources': [], 'status': 'READY'}
        if state['need'] != need:
            raise ValueError('Existing same-key acquisition need changed')
        plan_ref = ref(work, plan_path)
        if plan_ref not in state['plan_sources']:
            state['plan_sources'].append(plan_ref)
        atomic_json(state_path, state)
        try:
            chain = _ensure_timestamp_mapping(work, query, state, state_path, access)
            while not chain.closed:
                if pages >= maximum_pages:
                    state['status'] = 'SCHEDULING_PAGE_BOUND_PAUSED'; break
                try:
                    request = chain.next_plan(clock())
                except ValueError as exc:
                    if 'PAGE_KEY_' not in str(exc): raise
                    state['status'] = str(exc)
                    state['overlap_recovery_proposal'] = chain.recovery_range()
                    atomic_json(state_path, state); break
                requested_at = clock()
                member = cached_member(work, request)
                if member is None:
                    response = access.call_batch([request], query['name'], 'recovery_candidate_transfer_page', capability=False)
                    immutable(folder / ('call_' + str(len(state['pages'])).zfill(5) + '_' + digest(response) + '.json'), response)
                    if len(response.get('members', [])) != 1:
                        raise ValueError('One Transfers member required')
                    member = response['members'][0]
                if member.get('status') != 'SUCCESS_VALIDATED':
                    state['failures'].append({'plan': request, 'member': member}); state['status'] = 'RPC_MEMBER_BLOCKED'; break
                verified_member(work, request, member)
                chain.append(request, member, received_at_seconds=clock(), requested_at_seconds=requested_at)
                state['pages'] = chain.summary()['pages']; pages += 1
                state['status'] = 'INDEX_EXHAUSTED' if chain.closed else 'INDEX_PAGINATION_PENDING'
                atomic_json(state_path, state)
            if chain.closed:
                # Bracketing may already have the exact transaction block head.
                # Reuse its original member even in offline test/cache adapters.
                known = {digest(item['plan']) for item in state['bindings']}
                for request in binding_plans(chain):
                    if request['method'] == 'eth_getBlockByNumber':
                        member = state.get('timestamp_headers', {}).get(str(int(request['params'][0], 16)))
                        if member is not None:
                            _save_binding(work, state, state_path, request, member, known)
                if point_cache_admitter is not None and binding_plans(chain):
                    summary = chain.summary()
                    chain_sha = digest(summary)
                    admission = state.get('point_cache_admission')
                    if admission is not None:
                        if admission['chain_sha256'] != chain_sha:
                            raise ValueError('Point cache admission belongs to another closed page chain')
                    else:
                        try:
                            audit_ref = point_cache_admitter(query, need, summary)
                            if audit_ref is None:
                                raise ValueError('Point cache callback must return an audit reference')
                            audit_ref = json.loads(json.dumps(audit_ref, allow_nan=False))
                            state['point_cache_admission'] = {'chain_sha256': chain_sha, 'audit_ref': audit_ref}
                            atomic_json(state_path, state)
                        except Exception as exc:
                            state['failures'].append({'point_cache_admission_error': type(exc).__name__,
                                'detail': str(exc), 'chain_sha256': chain_sha, 'point_network_dispatched': False})
                            state['status'] = 'POINT_CACHE_ADMISSION_BLOCKED'
                            atomic_json(state_path, state)
                            raise
                _acquire_bindings(work, state, state_path, binding_plans(chain), access,
                                  query['name'], binding_counters, maximum_binding_calls)
            normalized = normalized_state(work, state)
            if normalized['positive_native_index_complete']:
                state['status'] = 'NATIVE_CANDIDATE_INDEX_COMPLETE'
            elif chain.closed and all(digest(p) in {digest(x['plan']) for x in state['bindings']} for p in binding_plans(chain)):
                state['status'] = 'INTERNAL_GAP_TO_CLASSIC_BQ_OR_COMPACT_DUNE'
            atomic_json(state_path, state)
            result = emit_interval(work, state)
            results.append(dict(result, need=need, status=state['status'],
                acquisition_state=ref(work, state_path),
                timestamp_bracket=state.get('timestamp_bracket'),
                timestamp_header_selectors_submitted=state.get('timestamp_header_selectors_submitted', 0),
                timestamp_header_count_unit='EXACT_SELECTORS_SUBMITTED_TO_SHARED_ACCESSOR_NOT_BILLED_OPERATIONS',
                batch_binding_required=state.get('batch_binding_required')))
            if state['status'] not in ('NATIVE_CANDIDATE_INDEX_COMPLETE', 'INTERNAL_GAP_TO_CLASSIC_BQ_OR_COMPACT_DUNE'):
                break
        except Exception as exc:
            state['failures'].append({'error_class': type(exc).__name__, 'detail': str(exc)})
            state['status'] = 'RAW_OR_RESOURCE_BLOCKED_SAME_REQUEST_PRESERVED'
            atomic_json(state_path, state)
            results.append({'need': need, 'status': state['status'], 'error_class': type(exc).__name__, 'detail': str(exc)})
            # A hard resource/capability/raw-proof block never advances to another
            # query/need to circumvent the same blocking condition.
            break
    receipt = {'version': VERSION, 'plan': ref(work, plan_path), 'results': results,
               'pages_this_invocation': pages, 'binding_calls_this_invocation': binding_counters['members'],
               'binding_batch_calls_this_invocation': binding_counters['dispatches'],
               'binding_calls_unit': 'INDIVIDUAL_POINT_SELECTORS_SUBMITTED_NOT_BILLED_ATTEMPTS',
               'context_complete': False, 'no_budget_reset': True}
    atomic_json(plan_path.parent / 'acquisition_result.json', receipt)
    return receipt
