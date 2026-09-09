"""Finite classic-BQ binding for exhausted discovery indexes, executed by root.

Preparation and verification are offline. execute_prepared is the only online
entry and delegates every operation/reservation to the existing BQ gateways.
Date partition background is retained; discovery claims cover exact rectangles.
"""
from collections import defaultdict
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

from collector import Event, NATIVE, Scope
from context_ledger_r3 import integer, normalize_rows, trace_path
import stage1d_bq_context_prepare as h
import stage1d_transfers_acquisition as transfers
from stage1d_alchemy_transfers import bind_need_to_scope
from stage1d_closure_scope import active_batch_path, batch_path_for_sha

VERSION = 'stage1d-batch-binding-route-v1'
PREP_SCHEMA = 'stage1d-batch-binding-preparation-v1'
SOURCE_TYPE = 'CLASSIC_BIGQUERY_FULL_TRANSACTION_FAMILY_V1'
FIELDS = ('start_block', 'end_block', 'start_time', 'end_time')


def needed_day_chunks(rectangles, days=7):
    if type(days) is not int or days not in (1, 7):
        raise ValueError('Only approved one/seven day partition splits')
    intervals = []
    for row in rectangles:
        if any(type(row[k]) is not int for k in FIELDS) or row['start_time'] > row['end_time']:
            raise ValueError('Exact noninverted rectangle required')
        lo = datetime.fromtimestamp(row['start_time'], timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        hi = datetime.fromtimestamp(row['end_time'], timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        intervals.append((lo, hi))
    merged = []
    for lo, hi in sorted(intervals):
        if merged and lo <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(hi, merged[-1][1]))
        else:
            merged.append((lo, hi))
    result = []
    for lo, hi in merged:
        while lo < hi:
            stop = min(hi, lo + timedelta(days=days))
            result.append([lo.strftime('%Y-%m-%d'), stop.strftime('%Y-%m-%d')])
            lo = stop
    return result


def build_binding_sql(ranges, fields, start, end):
    sql = h.build_sql(ranges, fields, 'transaction_and_trace', start, end)
    if fields[h.TR].get('input') == 'STRING':
        # The existing canonical output already contains input_data. Select the
        # provider's real trace input only when actual table metadata proves it.
        if sql.count('trace_scope AS (SELECT ') != 1 or sql.count('NULL AS input_data') != 1:
            raise ValueError('Reviewed full-tree SQL shape changed')
        sql = sql.replace('trace_scope AS (SELECT ', 'trace_scope AS (SELECT t.input,', 1)
        sql = sql.replace('NULL AS input_data', 't.input AS input_data', 1)
    return sql


def _query_rectangles(work, document, freeze_path):
    query = next(q for q in h.read(freeze_path)['queries'] if q['query_id'] == document['query_id'])
    if query['name'] not in ('txphish_src001', 'txphish_src002', 'xscam_src001'):
        raise ValueError('Batch discovery cannot expand LI.FI or other queries')
    scope = Scope.from_policy(query)
    if document['scope_hash'] != scope.scope_hash or document['freeze_sha256'] != h.sha(freeze_path):
        raise ValueError('Needed document differs from exact authorized scope')
    rectangles = [bind_need_to_scope(row, scope) for row in document['need_rectangles']]
    if not rectangles or len({r['address'] for r in rectangles}) > 64:
        raise ValueError('One finite native batch with at most 64 addresses required')
    if len({h.digest(r) for r in rectangles}) != len(rectangles):
        raise ValueError('Duplicate exact rectangle')
    rectangles.sort(key=lambda r: (r['address'], *(r[k] for k in FIELDS)))
    ranges = sorted({(r['address'], r['start_block'], r['end_block']) for r in rectangles})
    ranges = [dict(zip(('address', 'start_block', 'end_block'), r)) for r in ranges]
    return query, rectangles, ranges


def prepare(work, needed_path, output, config, schema_evidence, *, chunk_days=7):
    """Prepare explicit current exact needs; root supplies SHA-bound source deps."""
    work = Path(work).resolve()
    freeze = active_batch_path(work)
    needed_path = transfers.inside(work, needed_path)
    document = h.read(needed_path)
    query, rectangles, ranges = _query_rectangles(work, document, freeze)
    if not {h.TX, h.TR}.issubset(config['tables']):
        raise ValueError('Existing classic table allowlist must include transactions and traces')
    fields = h.schemas(work, schema_evidence, {h.TX, h.TR})
    if not document.get('scope_dependencies'):
        raise ValueError('Current need source dependencies required')
    dependencies = [h.dep(work, freeze), h.dep(work, needed_path)]
    for item in document['scope_dependencies']:
        transfers.checked(work, item)
        dependencies.append(h.dep(work, item['path']))
    out = transfers.inside(work, output)
    chunks = needed_day_chunks(rectangles, chunk_days)
    manifest = {'schema_version': PREP_SCHEMA, 'query_name': query['name'], 'query_id': query['query_id'],
        'scope_hash': query['scope_hash'], 'freeze_sha256': h.sha(freeze), 'need_rectangles': rectangles,
        'needed_ranges': ranges, 'scope_dependencies': dependencies, 'chunk_days': chunk_days,
        'date_chunks': chunks, 'plans': [], 'actual_query_executed': False,
        'date_domain_basis': 'EXACT_CURRENT_NEED_TIMESTAMPS_WITH_WHOLE_DAY_LEDGER_BACKGROUND',
        'full_context_claimed': False}
    for index, (start, end) in enumerate(chunks):
        folder = out / 'transaction_and_trace' / str(index).zfill(3)
        folder.mkdir(parents=True, exist_ok=True)
        sql = build_binding_sql(ranges, fields, start, end)
        sql_path = folder / 'query.sql'
        if sql_path.exists() and sql_path.read_text(encoding='utf-8') != sql:
            raise ValueError('Prepared SQL identity changed')
        sql_path.write_text(sql, encoding='utf-8', newline='\n')
        spec = {'schema_version': 'stage1d-bigquery-dryrun-spec-v1', 'batch_binding_sql_version': VERSION,
            'query_id': query['query_id'], 'scope_hash': query['scope_hash'], 'sql_path': h.dep(work, sql_path)['path'],
            'sql_sha256': h.sha(sql_path), 'schema_evidence': schema_evidence, 'scope_dependencies': dependencies,
            'template': 'transaction_and_trace', 'canonical_columns': h.columns_for('transaction_and_trace'),
            'date_start_inclusive': start, 'date_end_exclusive': end, 'needed_ranges': ranges,
            'need_rectangles': rectangles, 'actual_query_executed': False}
        h.save(folder / 'dryrun_spec.json', spec)
        manifest['plans'].append(dict(h.dep(work, folder / 'dryrun_spec.json'), template='transaction_and_trace',
                                     date_start_inclusive=start, date_end_exclusive=end))
    h.save(out / 'PREPARATION.json', manifest)
    return manifest


def prepare_from_states(work, state_paths, output, config, schema_evidence, *, chunk_days=7):
    """Snapshot closed immutable index/member evidence before mutable state resumes."""
    out = transfers.inside(work, output)
    snapshots, needs = [], []
    for value in state_paths:
        state = h.read(transfers.inside(work, value))
        chain = transfers.restore_state_chain(work, state)
        if not chain.closed:
            raise ValueError('Exhausted index page chain required before batch binding')
        transfers.normalized_state(work, state)
        snapshot = out / 'acquisition_snapshots' / (h.digest(state) + '.json')
        h.save(snapshot, state)
        snapshots.append(h.dep(work, snapshot))
        if state['need'] not in needs:
            needs.append(state['need'])
    if not needs or len({(r['query_id'], r['scope_hash']) for r in needs}) != 1:
        raise ValueError('One nonempty current query batch required')
    document = {'query_id': needs[0]['query_id'], 'scope_hash': needs[0]['scope_hash'],
        'freeze_sha256': h.sha(active_batch_path(work)), 'need_rectangles': needs, 'scope_dependencies': snapshots}
    h.save(out / 'CURRENT_NEEDED_RECTANGLES.json', document)
    return prepare(work, out / 'CURRENT_NEEDED_RECTANGLES.json', out, config, schema_evidence, chunk_days=chunk_days)


def verified_preparation(work, preparation_path):
    path = transfers.inside(work, preparation_path)
    manifest = h.read(path)
    if manifest.get('schema_version') != PREP_SCHEMA or manifest.get('actual_query_executed') is not False:
        raise ValueError('Exact batch preparation contract required')
    freeze = batch_path_for_sha(work, manifest['freeze_sha256'])
    query, rectangles, ranges = _query_rectangles(work, manifest, freeze)
    if manifest['need_rectangles'] != rectangles or manifest['needed_ranges'] != ranges:
        raise ValueError('Prepared exact rectangles changed')
    chunks = needed_day_chunks(rectangles, manifest['chunk_days'])
    if manifest['date_chunks'] != chunks or len(manifest['plans']) != len(chunks):
        raise ValueError('Prepared partition family incomplete')
    for dependency in manifest['scope_dependencies']:
        transfers.checked(work, dependency)
    all_fields = None
    for item, (start, end) in zip(manifest['plans'], chunks):
        spec = h.read(transfers.checked(work, item))
        fields = h.schemas(work, spec['schema_evidence'], {h.TX, h.TR})
        if all_fields is not None and fields != all_fields:
            raise ValueError('Schema changed within one prepared family')
        all_fields = fields
        if (spec.get('batch_binding_sql_version') != VERSION or spec.get('template') != 'transaction_and_trace'
            or spec['canonical_columns'] != h.columns_for('transaction_and_trace')
            or spec['needed_ranges'] != ranges or spec['need_rectangles'] != rectangles
            or spec['scope_dependencies'] != manifest['scope_dependencies']
            or spec['query_id'] != query['query_id'] or spec['scope_hash'] != query['scope_hash']
            or [spec['date_start_inclusive'], spec['date_end_exclusive']] != [start, end]
            or [item['date_start_inclusive'], item['date_end_exclusive']] != [start, end]):
            raise ValueError('Spec differs from exact full-tree partition family')
        actual = transfers.checked(work, {'path': spec['sql_path'], 'sha256': spec['sql_sha256']}).read_text(encoding='utf-8')
        if actual != build_binding_sql(ranges, fields, start, end):
            raise ValueError('SQL no longer exports the complete touched transaction family')
    return manifest, all_fields


def validate_transaction_rows(rows):
    """Pure exact family/tree proof. NULL root is a gap until separately bound."""
    groups, gaps, proofs = defaultdict(list), [], {}
    for row in rows:
        if row.get('tx_hash'):
            groups[row['tx_hash']].append(row)
    for tx, family in sorted(groups.items()):
        try:
            tops = [r for r in family if r.get('record_type') == 'transaction']
            traces = [r for r in family if r.get('record_type') == 'trace']
            if len(tops) != 1 or not traces:
                raise ValueError('Exactly one top row and a nonempty full trace tree required')
            top = tops[0]
            identity = tuple(top.get(k) for k in ('block_number', 'block_hash', 'block_time', 'tx_index'))
            if any(v is None for v in identity) or type(top.get('success')) is not bool:
                raise ValueError('Top physical identity/status incomplete')
            tree = {}
            for row in traces:
                if tuple(row.get(k) for k in ('block_number', 'block_hash', 'block_time', 'tx_index')) != identity:
                    raise ValueError('Trace physical identity differs from transaction')
                path = trace_path(row.get('trace_address'))
                if path is None or path in tree:
                    raise ValueError('Unbound NULL or duplicate trace position')
                if type(row.get('success')) is not bool or row['success'] and row.get('error'):
                    raise ValueError('Trace exact execution status missing/conflicting')
                integer(row['value_raw']); integer(row['subtraces'])
                tree[path] = row
            if () not in tree:
                raise ValueError('Explicit or independently bound root required')
            root = tree[()]
            if (root['from_address'] != top['from_address'] or root['to_address'] != (top.get('to_address') or top.get('created_address'))
                or integer(root['value_raw']) != integer(top['value_raw']) or root['success'] != top['success']):
                raise ValueError('Root is not the actual top execution')
            for path, row in tree.items():
                if any(path[:length] not in tree for length in range(len(path))):
                    raise ValueError('Missing full-tree ancestor')
                children = {p[-1] for p in tree if len(p) == len(path) + 1 and p[:-1] == path}
                if children != set(range(integer(row['subtraces']))):
                    raise ValueError('Missing sibling or wrong exact subtraces count')
            normalized = normalize_rows(family)
            if normalized['conflicts']:
                raise ValueError('Independent physical normalizer rejects transaction family')
            proofs[tx] = {'tx_hash': tx, 'full_tree': True, 'root_and_all_ancestors_present': True,
                'exact_child_indices_verified': True, 'trace_rows': len(traces), 'raw_family_sha256': h.digest(family)}
        except (ValueError, KeyError, TypeError) as exc:
            gaps.append({'reason': 'TRANSACTION_FAMILY_BINDING_INCOMPLETE', 'tx_hash': tx, 'detail': str(exc)})
    normalized = normalize_rows(rows)
    gaps.extend(normalized['conflicts'])
    from stage1d_bq_fee_tree_guard import inspect_rows
    review = inspect_rows(rows, normalized)
    gaps.extend(review['conflicts'] + review['top_gaps'] + review['internal_gaps'])
    return {'normalized': normalized, 'full_tree_proofs': proofs, 'gaps': gaps,
            'fee_gaps': review['fee_gaps'], 'fee_review': review, 'full_context_claimed': False}


def _bound_rows(work, manifest, job_states, root_bindings=None):
    if set(job_states) != {p['path'] for p in manifest['plans']}:
        raise ValueError('Every prepared partition must have exactly one completed saved job')
    rows, evidence = [], []
    for spec in manifest['plans']:
        page_rows, source = h.verified_export(work, job_states[spec['path']], spec)
        rows.extend(page_rows); evidence.append(source)
    # Disjoint date partitions must not duplicate any physical canonical row.
    identities = set()
    for row in rows:
        key = (row.get('record_type'), row.get('block_hash'), row.get('tx_hash'), row.get('trace_address'))
        if key in identities and row.get('tx_hash'):
            raise ValueError('Duplicate physical row across partition family')
        identities.add(key)
    requests_by_tx = {} if root_bindings is None else {b['tx_hash']: b for b in root_bindings}
    families = defaultdict(list)
    for row in rows:
        if row.get('record_type') == 'trace':
            families[row['tx_hash']].append(row)
    replacements, bindings, root_gaps = {}, [], []
    from stage1d_bq_root_binding import _bind
    for tx, family in sorted(families.items()):
        if not any(r.get('trace_address') is None for r in family):
            continue
        block = integer(family[0]['block_number'])
        plans = [{'method': 'eth_getTransactionByHash', 'params': [tx]},
                 {'method': 'eth_getTransactionReceipt', 'params': [tx]},
                 {'method': 'eth_getBlockByNumber', 'params': [hex(block), False]}]
        unresolved = {'reason': 'ROOT_EQUIVALENCE_UNRESOLVED_GAP', 'tx_hash': tx,
                      'root_equivalence_requests': plans,
                      'detail': 'Complete saved tx/receipt/header proof does not establish this NULL root'}
        if root_bindings is None:
            members = [transfers.cached_member(work, p) for p in plans]
        else:
            saved = requests_by_tx.get(tx, {}).get('requests', [])
            if saved and [r['plan'] for r in saved] != plans:
                raise ValueError('Saved root RPC selectors changed')
            members = [r['member'] for r in saved]
        if len(members) != 3 or any(m is None for m in members):
            root_gaps.append(unresolved)
            continue
        try:
            values = [transfers.verified_member(work, p, m) for p, m in zip(plans, members)]
            ids = sorted({e for row in family for e in row['evidence_ids']}
                         | {'sha256:' + m['artifact_sha256'] for m in members})
            result = _bind(family, *values, ids)
            binding = {'tx_hash': tx, 'binding': result['root_binding'],
                       'requests': [{'plan': p, 'member': m} for p, m in zip(plans, members)]}
            if root_bindings is not None and binding != requests_by_tx[tx]:
                raise ValueError('Saved root proof does not regenerate')
            replacements[tx] = result['rows']; bindings.append(binding)
        except (ValueError, KeyError, TypeError) as exc:
            if root_bindings is not None and tx in requests_by_tx:
                raise ValueError('Persisted root binding failed revalidation') from exc
            root_gaps.append(unresolved)
    if root_bindings is not None and {b['tx_hash'] for b in bindings} != set(requests_by_tx):
        raise ValueError('Unknown saved root binding')
    result_rows = [r for r in rows if not (r.get('record_type') == 'trace' and r['tx_hash'] in replacements)]
    result_rows.extend(r for tx in sorted(replacements) for r in replacements[tx])
    return result_rows, bindings, root_gaps


def _events(rows, validated):
    stamps = {r['tx_hash']: int(datetime.fromisoformat(r['block_time'].replace('Z', '+00:00')).timestamp())
              for r in rows if r.get('tx_hash') and r.get('block_time')}
    result, gaps = [], []
    normalized = validated['normalized']
    items = [dict(t, event_id='eip155:1:tx:' + t['tx_hash'] + ':top', flow_kind='top', trace_address=[])
             for t in normalized['transactions']]
    items += [f for f in normalized['flows'] if f['flow_kind'] != 'top' and f['tx_hash'] in validated['full_tree_proofs']]
    for item in items:
        try:
            if type(item.get('success')) is not bool or not item.get('recipient'):
                raise ValueError('Unbound top endpoint/status')
            event = Event(event_id=item['event_id'], tx_hash=item['tx_hash'], sender=item['sender'], recipient=item['recipient'],
                asset=NATIVE, amount_raw=integer(item['amount_raw']), block=integer(item['block_number']),
                tx_index=integer(item['tx_index']), timestamp=stamps[item['tx_hash']], kind=item['flow_kind'],
                trace_address=json.dumps(item.get('trace_address', []), separators=(',', ':')) if item['flow_kind'] == 'internal' else None,
                success=item['success'], provenance=';'.join(item['evidence_ids']), block_hash=item['block_hash'],
                gas_raw=integer(item['fee_raw']) if item.get('fee_raw') is not None else None,
                gas_used=integer(item['gas_used']) if item.get('gas_used') is not None else None,
                gas_price=integer(item['effective_gas_price']) if item.get('effective_gas_price') is not None else None)
            result.append(asdict(event))
        except (ValueError, KeyError, TypeError) as exc:
            gaps.append({'reason': 'CANDIDATE_EVENT_BINDING_INCOMPLETE', 'tx_hash': item.get('tx_hash'), 'detail': str(exc)})
    return sorted(result, key=lambda e: Event(**e).stable_key()), gaps


def _result(work, preparation_path, job_states, root_bindings=None):
    manifest, _ = verified_preparation(work, preparation_path)
    rows, bindings, gaps = _bound_rows(work, manifest, job_states, root_bindings)
    validated = validate_transaction_rows(rows)
    events, event_gaps = _events(rows, validated)
    gaps += validated['gaps'] + event_gaps
    # Persisted JSON uses arrays for the strict normalizer's internal tuples.
    return json.loads(json.dumps({'rows': rows, 'root_bindings': bindings, 'validated': validated, 'events': events,
            'gaps': gaps, 'native_complete': not gaps, 'full_context_claimed': False}))


def import_completed(work, preparation_path, job_states):
    result = _result(work, preparation_path, job_states)
    preparation = h.dep(work, preparation_path)
    proof = {'version': VERSION, 'preparation': preparation, 'job_states': {
        k: h.dep(work, v) for k, v in sorted(job_states.items())}, 'result': result}
    key = h.digest(proof)
    folder = transfers.inside(work, 'private/stage1d_batch_binding/proofs/' + key)
    h.save(folder / 'proof.json', proof)
    h.save(folder / 'events.json', result['events'])
    h.save(folder / 'ledger_rows.json', result['rows'])
    manifest = h.read(transfers.checked(work, preparation))
    records = []
    # These saved artifacts are immutable within this single import operation.
    proof_ref = h.dep(work, folder / 'proof.json')
    events_ref = h.dep(work, folder / 'events.json') if manifest['need_rectangles'] else None
    for need in manifest['need_rectangles']:
        complete = result['native_complete']
        record = {'acquisition_adapter': VERSION, 'provider': 'CLASSIC_BIGQUERY', 'evidence_id': key,
            'addresses': [need['address']], 'asset': NATIVE, **{k: need[k] for k in FIELDS},
            'complete': complete, 'native_scope_complete': complete, 'all_asset_export_complete': False,
            'coverage_capability': 'NATIVE_INDEX_ONLY', 'normalization_gaps': result['gaps'],
            'native_decision_path': proof_ref['path'], 'native_decision_sha256': proof_ref['sha256'],
            'events_path': events_ref['path'], 'events_sha256': events_ref['sha256'],
            'query_id_at_acquisition': need['query_id'], 'scope_id_at_acquisition': need['scope_id'],
            'coverage_basis': 'COMPLETE_CLASSIC_TX_TRACE_PAGES_EXACT_BLOCK_AND_TIME_INTERSECTION', 'context_complete': False}
        destination = transfers.inside(work, 'derived/stage1d/intervals/bq_binding_' + key + '_' + h.digest(need) + '.coverage.json')
        h.save(destination, record)
        records.append(h.dep(work, destination))
    return {'status': 'NATIVE_BATCH_BINDING_COMPLETE' if result['native_complete'] else 'BATCH_BINDING_PARTIAL_WITH_EXPLICIT_GAPS',
            'coverage_records': records, 'events': len(result['events']), 'gaps': result['gaps'],
            'fee_gaps': result['validated']['fee_gaps'], 'proof': proof_ref,
            'ledger_rows': h.dep(work, folder / 'ledger_rows.json'), 'full_context_claimed': False}


def verify_interval_record(work, record, *, memo=None):
    if (record.get('acquisition_adapter') != VERSION or record.get('asset') != NATIVE
        or record.get('coverage_capability') != 'NATIVE_INDEX_ONLY'):
        raise ValueError('Exact native classic batch capability required')
    # Caller-owned operation-local memo; never shared by an implicit global cache.
    key = ('BATCH_BINDING_PROOF', str(Path(work).resolve()),
           record['native_decision_path'], record['native_decision_sha256'])
    if memo is None or key not in memo:
        proof = h.read(transfers.checked(work, {'path': key[2], 'sha256': key[3]}))
        proof_identity = h.digest(proof)
        if proof['version'] != VERSION or proof_identity != record['evidence_id']:
            raise ValueError('Batch proof identity changed')
        prep = transfers.checked(work, proof['preparation'])
        states = {k: transfers.checked(work, v) for k, v in proof['job_states'].items()}
        result = _result(work, prep, states, proof['result']['root_bindings'])
        if result != proof['result']:
            raise ValueError('Batch result differs from original verified rows')
        value = (proof, h.read(prep), result, proof_identity)
        if memo is not None:
            memo[key] = value
    proof, manifest, result, proof_identity = memo[key] if memo is not None else value
    matches = [n for n in manifest['need_rectangles'] if record['addresses'] == [n['address']]
               and all(record[k] == n[k] for k in FIELDS)]
    if len(matches) != 1 or record.get('evidence_id') != proof_identity:
        raise ValueError('Coverage exceeds an exact prepared request rectangle')
    events_key = ('BATCH_BINDING_EVENTS', key, record['events_path'], record['events_sha256'])
    if memo is None or memo.get(events_key) != proof_identity:
        if h.read(transfers.checked(work, {'path': record['events_path'], 'sha256': record['events_sha256']})) != result['events']:
            raise ValueError('Candidate cache differs from verified physical rows')
        if memo is not None:
            memo[events_key] = proof_identity
    if (record.get('complete') is not result['native_complete'] or record.get('native_scope_complete') is not result['native_complete']
        or record.get('all_asset_export_complete') is not False or record.get('context_complete') is not False
        or record['normalization_gaps'] != result['gaps']):
        raise ValueError('Completeness claim exceeds verified native evidence')
    return result


def execute_prepared(work, preparation_path, config_path, *, execute=False):
    """Root-only real gateway sequence; incomplete jobs preserve the same plan."""
    manifest, _ = verified_preparation(work, preparation_path)
    if execute is not True:
        return {'status': 'PREPARED_NO_NETWORK', 'plans': manifest['plans'], 'needs_preserved': True}
    from stage1d_bigquery_probe import probe
    from stage1d_bigquery_jobs import execute_plan
    config = h.read(transfers.inside(work, config_path))
    states = {}
    for spec in manifest['plans']:
        folder = transfers.inside(work, spec['path']).parent
        plan_path = folder / 'execution_plan.json'
        if not plan_path.exists():
            receipt = probe(work, manifest['query_name'], config_path, 'dry-run', spec_path=spec['path'])
            if receipt.get('status') != 'SUCCESS_VALIDATED':
                return {'status': 'BATCH_DRY_RUN_BLOCKED', 'response': receipt, 'spec': spec, 'needs_preserved': True}
            h.bind_execution_plan(work, spec['path'], receipt['artifact_path'], plan_path)
        state = execute_plan(work, manifest['query_name'], config, plan_path)
        if state.get('state') != 'COMPLETE_EXPORTED':
            return {'status': 'BATCH_JOB_INCOMPLETE_SAME_PLAN_PRESERVED', 'response': state,
                    'execution_plan': h.dep(work, plan_path), 'needs_preserved': True}
        state_path = transfers.inside(work, state['rows_path']).parent / 'job.json'
        if h.read(state_path) != state:
            raise ValueError('Completed gateway state differs from saved original job')
        states[spec['path']] = h.dep(work, state_path)['path']
    result = import_completed(work, preparation_path, states)
    result['job_states'] = states
    return result


def verified_transaction_family(work, preparation_path, job_states, tx_hash):
    result = _result(work, preparation_path, job_states)
    proof = result['validated']['full_tree_proofs'].get(tx_hash)
    if proof is None:
        raise ValueError('Requested transaction has no complete independently bound full tree')
    return {'source_type': SOURCE_TYPE, 'tx_hash': tx_hash,
        'rows': [r for r in result['rows'] if r.get('tx_hash') == tx_hash], 'full_tree_proof': proof,
        'preparation_dependency': h.dep(work, preparation_path), 'job_states': job_states,
        'root_bindings': [b for b in result['root_bindings'] if b['tx_hash'] == tx_hash]}


def collect_portable_dependencies(work, preparation_path, job_states, tx_hash):
    """Collect the finite protocol dependency fields, then independently verify.

    A scope snapshot's bytes are an input. Its descriptive historical paths
    are not a recursive file manifest. No arbitrary JSON walk or path-specific
    exclusion is used; all actual page/spec/root proof edges remain required.
    """
    from stage1d_bq_portable import verify_transaction_family_documents, ROOT_BINDINGS_PATH
    family = verified_transaction_family(work, preparation_path, job_states, tx_hash)
    documents = {}
    def add(path, expected=None, size=None):
        path = transfers.inside(work, path)
        name = path.relative_to(Path(work).resolve()).as_posix()
        before = path.stat(); data = path.read_bytes(); after = path.stat()
        if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
            raise ValueError('Portable dependency changed during read: ' + name)
        if expected is not None and hashlib.sha256(data).hexdigest() != expected:
            raise ValueError('Portable dependency hash changed: ' + name)
        if size is not None and (type(size) is not int or len(data) != size):
            raise ValueError('Portable dependency byte count changed: ' + name)
        if name in documents and documents[name] != data:
            raise ValueError('One portable path has conflicting original bytes: ' + name)
        documents[name] = data
        return data
    def dependency(ref):
        if not isinstance(ref, dict) or not ref.get('sha256'):
            raise ValueError('Exact portable protocol dependency SHA required')
        return add(ref['path'], ref['sha256'], ref.get('bytes'))
    def document(path, expected=None):
        return json.loads(add(path, expected).decode('utf-8-sig'))
    def source_envelope(path, expected):
        envelope = document(path, expected)
        # Original schema/dry/page HTTP payloads are direct protocol fields.
        # Their bytes remain leaves, even when a provider payload contains a
        # column literally named path, sha256 or any historical provenance.
        for ref in envelope.get('raw_sources', []): dependency(ref)
        return envelope

    manifest = document(preparation_path)
    for ref in manifest['scope_dependencies']: dependency(ref)
    if set(job_states) != {spec['path'] for spec in manifest['plans']}:
        raise ValueError('Every exact prepared job is required')
    for expected_spec in manifest['plans']:
        spec = json.loads(dependency(expected_spec).decode('utf-8-sig'))
        add(spec['sql_path'], spec['sql_sha256'])
        for ref in spec['scope_dependencies']: dependency(ref)
        for ref in spec['schema_evidence']: source_envelope(ref['path'], ref['sha256'])
        state_path = job_states[expected_spec['path']]
        state = document(state_path)
        plan = document(state['plan_path'], state['plan_sha256'])
        # Check both incoming spec bindings; the pure verifier enforces equality.
        add(plan['dry_spec_path'], plan['dry_spec_sha256'])
        source_envelope(plan['dry_receipt_path'], plan['dry_receipt_sha256'])
        add(transfers.inside(work, state_path).parent / 'terminal_job.json', state['terminal_job_sha256'])
        add(state['rows_path'], state['rows_sha256'])
        for page in state['pages']:
            add(page['rows_path'], page['rows_sha256'])
            response = page['response_receipt']
            source_envelope(response['artifact_path'], response['artifact_sha256'])

    for binding in family['root_bindings']:
        for request in binding['requests']:
            member = request['member']
            envelope = document(member['artifact_path'], member['artifact_sha256'])
            if envelope.get('evidence_kind') == 'REAL_CHAIN_LEGACY_RAW_REUSE':
                source = envelope['legacy_source']
                add(member['admission_path'], member['admission_sha256'])
                add(envelope['raw_path'], source['raw_sha256'], source['raw_bytes'])
                add(envelope['manifest_path'], source['manifest_sha256'], source['manifest_bytes'])
                # Old absolute legacy_source locators describe the acquisition;
                # the copied payload/manifest/admission above are the inputs.
            else:
                original = envelope.get('original_artifact')
                if original is not None: dependency(original)
                original_path = original['path'] if original else member['artifact_path']
                raw = transfers.inside(work, original_path).parent / 'response_body.bin'
                if raw.exists(): add(raw, envelope['raw_body_sha256'])
                # The established successful-cache envelope-only contract is
                # checked by the independent verifier when an old body is absent.
    root_bytes = (json.dumps({'schema_version': 'stage1d-portable-root-rpc-bindings-v1',
        'tx_hash': tx_hash, 'root_bindings': family['root_bindings']}, sort_keys=True, separators=(',', ':')) + '\n').encode()
    if ROOT_BINDINGS_PATH in documents and documents[ROOT_BINDINGS_PATH] != root_bytes:
        raise ValueError('Derived root-binding metadata collides with original dependency')
    documents[ROOT_BINDINGS_PATH] = root_bytes
    verify_transaction_family_documents(preparation_path, job_states, tx_hash, documents)
    return documents


def superset_admissibility(current_need, source):
    """No request-hash equality shortcut; require real SQL/domain/page proofs."""
    required = ('sql_predicate_verified', 'schema_verified', 'complete_page_chain', 'values_committed',
                'full_transaction_families_verified')
    missing = [k for k in required if source.get(k) is not True]
    contained = any(r.get('address') == current_need.get('address') and r.get('asset', NATIVE) == current_need.get('asset', NATIVE)
                    and all(type(r.get(k)) is int for k in FIELDS)
                    and r['start_block'] <= current_need['start_block'] <= current_need['end_block'] <= r['end_block']
                    and r['start_time'] <= current_need['start_time'] <= current_need['end_time'] <= r['end_time']
                    for r in source.get('verified_rectangles', []))
    if not contained:
        missing.append('EXACT_CURRENT_NEED_CONTAINED_IN_VERIFIED_SOURCE_RECTANGLE')
    return {'eligible_complete_superset': not missing, 'missing_proofs': missing,
            'request_hash_equality_required': False, 'metadata_alone_is_full': False,
            'positive_facts_require_individual_physical_source_verification': True}


def verified_superset(work, preparation_path, job_states, current_need):
    """Read actual saved SQL/schema/pages before admitting an older wider family.

    This is an evidence adapter only. It neither copies legacy code/results nor
    installs coverage. Bare query-ledger metadata is never accepted as a family.
    """
    _, current_scope = transfers.frozen_query(work, current_need['query_name'])
    need = bind_need_to_scope(current_need, current_scope)
    manifest = h.read(transfers.inside(work, preparation_path))
    if manifest.get('schema_version') == PREP_SCHEMA:
        result = _result(work, preparation_path, job_states)
        rectangles = manifest['need_rectangles']
        rows = result['rows']; validated = result['validated']
        complete = result['native_complete']
    elif manifest.get('schema_version') == 'stage1d-classic-context-preparation-v1':
        # bind_roots=True also independently regenerates each old SQL template;
        # no request-hash comparison to the current narrower demand is needed.
        result = h.normalize_complete_family(work, preparation_path, 'transaction_and_trace', job_states, bind_roots=True)
        frozen = h.read(batch_path_for_sha(work, manifest['freeze_sha256']))
        query = next(q for q in frozen['queries'] if q['query_id'] == manifest['query_id'])
        scope = Scope.from_policy(query)
        rectangles = [dict(r, asset=NATIVE, start_time=scope.start_time, end_time=scope.end_time)
                      for r in manifest['needed_ranges']]
        rows = result['rows']; validated = validate_transaction_rows(rows)
        complete = not validated['gaps'] and all(any(c['address'] == r['address']
            and c['start_block'] == r['start_block'] and c['end_block'] == r['end_block']
            and c['data_type'] == kind and c['status'] == 'COMPLETE' for c in result['coverage'])
            for r in manifest['needed_ranges'] for kind in (h.TOP, h.INTERNAL))
    else:
        raise ValueError('Metadata without original SQL, parameters and full page evidence cannot establish superset coverage')
    decision = superset_admissibility(need, {'sql_predicate_verified': True, 'schema_verified': True,
        'complete_page_chain': True, 'values_committed': True, 'full_transaction_families_verified': complete,
        'verified_rectangles': rectangles})
    return dict(decision, preparation_dependency=h.dep(work, preparation_path), current_need=need,
                rows=rows, normalized=validated['normalized'], full_tree_proofs=validated['full_tree_proofs'],
                gaps=validated['gaps'], full_context_claimed=False, coverage_installed=False)
