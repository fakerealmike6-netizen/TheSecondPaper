"""Finite local bridge into existing Stage1D context round files.

This module performs no provider call, active DB write, candidate replay, model
construction, or solver call. Root supplies SHA-bound stable collection/labels.
The output directory is explicit and immutable; install/execute is root-owned.
"""
from collections import defaultdict
from contextlib import closing
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha
import json
import sqlite3

import stage1d_bq_context_prepare as bq
from context_access_r4 import db_path
from context_ledger_r3 import EvidenceConflict, integer, normalize_rows
from stage1d_context import necessary_context_windows
from stage1d_context_coverage import merge_coverage
from stage1d_context_online import _cached_rpc, _load_context, _merge, _merge_rows, _merge_rpc
from stage1d_context_recovery import _union

KINDS = (bq.TOP, bq.INTERNAL, bq.FEES)
QUERIES = {'txphish_src001', 'txphish_src002'}
XSCAM = {'xscam_src001'}

def query_group(query_names=None):
    """Two fixed current TxPhish consumers, or only the fixed current XScam."""
    if query_names is None:
        names = set(QUERIES)
    else:
        if not isinstance(query_names, (tuple, list, set, frozenset)):
            raise ValueError('Explicit query-name collection required')
        names = set(query_names)
        if len(names) != len(query_names):
            raise ValueError('Duplicate query consumer')
    if names == QUERIES:
        return names, 'txphish_src002', 'SHARED', 'EXISTING_RUNTIME_SHARED_CURRENT_POLICY_ALL_FOUR_QUERIES'
    if names == XSCAM:
        return names, 'xscam_src001', 'xscam_src001', 'EXISTING_RUNTIME_ORIGINAL_SINGLE_QUERY'
    raise ValueError('Only both original TxPhish queries or the original single XScam are allowed; LI.FI is excluded')


def check_bundle_group(bundle, query_names=None):
    result = query_group(query_names)
    names, owner, clock, _ = result
    if names == XSCAM and set(bundle.get('consumers', {})) != names:
        raise ValueError('XScam bundle requires its one exact current consumer')
    for field, expected in (('sql_owner', owner), ('clock_query', clock)):
        if (field in bundle or names == XSCAM) and bundle.get(field) != expected:
            raise ValueError('Bundle original SQL owner or clock attribution differs')
    if 'query_names' in bundle and set(bundle['query_names']) != names:
        raise ValueError('Explicit selected query group differs from prepared bundle')
    return result


def check_one_day_replan(work, dependency, names, frozen_path, config):
    """Only a complete actual seven-day audit permits an explicit full one-day plan."""
    if names not in (QUERIES, XSCAM) or not isinstance(dependency, dict):
        raise ValueError('One-day replan requires a SHA-bound actual full-plan audit')
    receipt = bq.read(bq.checked(work, dependency))
    if (receipt.get('status') != 'SINGLE_JOB_REQUIRES_ONE_DAY_REPLAN'
            or receipt.get('route') != 'BQ_FULL_SCOPE_ONE_DAY_REPLAN'
            or receipt.get('actual_query_executed') is not False):
        raise ValueError('One-day replan was not justified by an actual oversize seven-day audit')
    previous_path = bq.checked(work, receipt['source_preparation'])
    previous = bq.read(previous_path)
    check_bundle_group(previous, names)
    if previous.get('chunk_days', 7) != 7:
        raise ValueError('No recursive subdivision below the original seven-to-one-day split')
    if bq.sha(bq.checked(work, previous['frozen_inputs'])) != bq.sha(frozen_path):
        raise ValueError('One-day plan must retain identical frozen current candidates and labels')
    current = audit_actual_dryruns(work, previous_path, [j['execution_plan'] for j in receipt['jobs']],
                                  config, query_names=names)
    if current['status'] != 'SINGLE_JOB_REQUIRES_ONE_DAY_REPLAN':
        raise ValueError('Current full-plan audit no longer permits this one-day route; preserve its resource gap')
    if any(j.get('existing_state') is not None or j.get('already_booked') is not None for j in current['jobs']):
        raise ValueError('Existing BQ jobs or scan risk cannot be replaced by a daily replan; retain original job identities')
    return previous



def gaps_for_kinds(row, coverage, kinds=KINDS):
    """Existing strict coverage interval semantics, with explicit provider kinds."""
    start, end = row['ledger_start_block'], row['ledger_end_block']
    gaps = []
    for kind in kinds:
        cursor = start
        intervals = _union((max(start, r['start_block']), min(end, r['end_block']))
            for r in coverage if r.get('address') == row['address']
            and r.get('data_type') == kind and r.get('status') == 'COMPLETE'
            and r.get('pagination_complete') is True and r.get('evidence_ids')
            and (not r.get('provider_frozen_scope') or
                 r.get('date_domain_verified') is True and r.get('block_domain_verified') is True)
            and r['start_block'] <= end and r['end_block'] >= start)
        for lo, hi in intervals:
            if cursor < lo:
                gaps.append([cursor, lo - 1])
            cursor = max(cursor, hi + 1)
        if cursor <= end:
            gaps.append([cursor, end])
    return _union(gaps)


def merge_address_ranges(rows):
    """Only overlap/adjacency union; no filling successful islands or new neighbors."""
    addresses = defaultdict(list)
    for row in rows:
        addresses[row['address']].append((row['start_block'], row['end_block']))
    return [{'address': address, 'start_block': lo, 'end_block': hi}
            for address in sorted(addresses) for lo, hi in _union(addresses[address])]


def address_batches(rows):
    addresses = sorted({r['address'] for r in rows})
    return [[r for r in rows if r['address'] in set(addresses[i:i + 64])]
            for i in range(0, len(addresses), 64)]


def checked_freeze(work):
    path = active_batch_path(work)
    if path.name == 'BATCH_QUERY_FREEZE.json' and bq.sha(path) != bq.FREEZE_SHA:
        raise ValueError('The original four-query freeze changed')
    return {q['name']: q for q in bq.read(path)['queries']}


def point_needs(plan, collection, ledger):
    requests = []
    for row in plan['rows']:
        for key in ('before_anchor_block', 'after_anchor_block'):
            block = row[key]
            requests.extend([{'method': 'eth_getBalance', 'params': [row['address'], hex(block)]},
                             {'method': 'eth_getBlockByNumber', 'params': [hex(block), False]}])
    for event in collection.get('candidate_events', []) + collection.get('context_events', []) + list(ledger):
        tx = event.get('tx_hash', event.get('hash'))
        if tx:
            requests.append({'method': 'eth_getTransactionReceipt', 'params': [tx.lower()]})
    return [json.loads(key) for key in sorted({json.dumps(r, sort_keys=True) for r in requests})]


def cached_points(work, requests, headers, balances, receipts):
    found = _cached_rpc(work, requests)
    identities = set()
    for request, value, evidence in found:
        _merge_rpc(headers, balances, receipts, request, value, evidence)
        identities.add(bq.digest(request))
    missing = []
    for request in requests:
        method, params = request['method'], request['params']
        present = (integer(params[0]) in headers if method == 'eth_getBlockByNumber' else
                   params[0] + ':' + str(integer(params[1])) in balances if method == 'eth_getBalance' else
                   params[0] in receipts)
        if bq.digest(request) not in identities and not present:
            missing.append(request)
    return missing


def prepare_current(work, frozen_inputs_path, output, config, schema_evidence, *,
                    query_names=None, chunk_days=7, repartition_receipt=None):
    """Root runs only after current candidates/labels settle; prepares shared SQL.

    frozen_inputs: {freeze_sha256, candidates_frozen:true, labels_frozen:true,
      queries:{name:{collection:{path,sha256}, labels:{path,sha256}}}}.
    These declarations bind existing facts; candidate completeness is not promoted.
    """
    work = Path(work).resolve()
    out = bq.inside(work, output)
    frozen_path = bq.inside(work, frozen_inputs_path)
    frozen = bq.read(frozen_path)
    names, owner_name, clock_query, clock_basis = query_group(query_names)
    if type(chunk_days) is not int or chunk_days not in (1, 7):
        raise ValueError('Only full-scope seven-day or approved one-day partitioning is allowed')
    prior_bundle = None
    if chunk_days == 1:
        prior_bundle = check_one_day_replan(work, repartition_receipt, names, frozen_path, config)
    elif repartition_receipt is not None:
        raise ValueError('Repartition receipt is only for the explicit one-day replan')
    if (frozen.get('freeze_sha256') != bq.sha(active_batch_path(work)) or frozen.get('candidates_frozen') is not True
            or frozen.get('labels_frozen') is not True or set(frozen['queries']) != names):
        raise ValueError('Explicit same-freeze stable allowed query-group inputs required')
    queries = checked_freeze(work)
    consumers, union, dependencies = {}, [], [bq.dep(work, frozen_path)]
    for name in sorted(names):
        query = queries[name]
        inputs = frozen['queries'][name]
        collection = bq.read(bq.checked(work, inputs['collection']))
        labels = bq.read(bq.checked(work, inputs['labels']))
        if collection.get('query_id') != query['query_id']:
            raise EvidenceConflict('Stable collection belongs to another frozen query')
        if not isinstance(labels, dict):
            raise ValueError('Existing address-to-identity label snapshot required')
        dependencies.extend(inputs.values())
        folder = out / name
        folder.mkdir(parents=True, exist_ok=True)
        root = work / 'derived/stage1d/queries' / name / 'context'
        if not root.is_dir():
            raise ValueError('Current context directory is missing; root must identify it explicitly')
        headers, balances, receipts, ledger, coverage, prior, sources = _load_context(work, root, folder)
        ledger = merge_bound_rows(ledger, [])
        plan = necessary_context_windows(query, collection, ledger, labels, prior_rows=prior)
        for row in plan['rows']:
            if not query['start_block'] <= row['ledger_start_block'] <= row['ledger_end_block'] <= query['end_block']:
                raise EvidenceConflict('Known ordinary context escaped unchanged query scope')
        requests = point_needs(plan, collection, ledger)
        missing_rpc = cached_points(work, requests, headers, balances, receipts)
        needed, protocol = [], []
        for row in plan['rows']:
            needed.extend({'address': row['address'], 'start_block': lo, 'end_block': hi}
                          for lo, hi in gaps_for_kinds(row, coverage))
            protocol.extend({'address': row['address'], 'start_block': lo, 'end_block': hi,
                             'type': 'NEEDS_PROTOCOL_NATIVE_EVIDENCE'}
                            for lo, hi in gaps_for_kinds(row, coverage, (bq.PROTOCOL,)))
        for filename, value in [('headers.json', headers), ('balances.json', balances),
                                ('receipts.json', receipts), ('ledger_rows.json', ledger),
                                ('coverage.json', coverage), ('context_plan.json', plan)]:
            bq.save(folder / filename, value)
        for source in sources:
            dependencies.append({'path': source['snapshot_path'], 'sha256': source['sha256']})
        consumer = {'query_id': query['query_id'], 'scope_hash': query['scope_hash'],
                    'query_name': name, 'inputs': inputs, 'context_sources': sources,
                    'prepared_context': {n: bq.dep(work, folder / (n + '.json')) for n in
                        ('headers', 'balances', 'receipts', 'ledger_rows', 'coverage', 'context_plan')},
                    'needed_ranges': needed, 'missing_rpc': missing_rpc, 'protocol_gaps': protocol,
                    'candidate_completeness_unchanged': True}
        consumers[name] = consumer
        union.extend(needed)
    owner = queries[owner_name]
    if any(q['start_block'] < owner['start_block'] or q['end_block'] > owner['end_block']
           or bq.date_chunks(q, chunk_days) != bq.date_chunks(owner, chunk_days) for q in (queries[n] for n in names)):
        raise ValueError('No existing owner freeze contains the entire allowed query-group domain')
    shared = merge_address_ranges(union)
    if prior_bundle is not None and prior_bundle['shared_ranges'] != shared:
        raise ValueError('One-day replan must preserve exactly the same current ordinary-account demand')
    if repartition_receipt is not None:
        dependencies.append(repartition_receipt)
    families = []
    for index, batch in enumerate(address_batches(shared)):
        folder = out / ('shared_' + str(index).zfill(3))
        needed = {'query_id': owner['query_id'], 'scope_hash': owner['scope_hash'],
                  'freeze_sha256': bq.sha(active_batch_path(work)), 'scope_dependencies': dependencies,
                  'needed_ranges': batch, 'consumer_query_ids': [consumers[n]['query_id'] for n in sorted(consumers)]}
        bq.save(folder / 'NEEDED_RANGES.json', needed)
        bq.prepare(work, folder / 'NEEDED_RANGES.json', folder / 'sql', config, schema_evidence,
                   templates=('transaction_and_trace',), chunk_days=chunk_days)
        families.append(bq.dep(work, folder / 'sql/PREPARATION.json'))
    bundle = {'schema_version': 'stage1d-existing-context-local-preparation-v1',
              'freeze_sha256': bq.sha(active_batch_path(work)), 'frozen_inputs': bq.dep(work, frozen_path),
              'sql_owner': owner['name'], 'clock_query': clock_query, 'clock_basis': clock_basis,
              'query_names': sorted(names), 'chunk_days': chunk_days,
              'repartition_receipt': repartition_receipt,
              'consumers': consumers, 'shared_ranges': shared, 'families': families,
              'new_candidate_neighbors': 0, 'actual_query_executed': False, 'formal_model_built': False}
    bq.save(out / 'SHARED_CONTEXT_PREPARATION.json', bundle)
    return bundle


def budget_snapshot_ro(work):
    """Current authority/month guard, existing actuals and unresolved risk, read-only."""
    from stage1d_recovery_policy import RecoveryLedger
    path = Path(db_path(Path(work))).resolve()
    ledger = object.__new__(RecoveryLedger)
    ledger.path = str(path)
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as database:
        database.execute('BEGIN')
        snapshot = ledger.snapshot(database)
        amounts = {row[0]: {'reserved': int(Decimal(row[1])), 'actual': None if row[2] is None else int(Decimal(row[2]))}
                   for row in database.execute("SELECT job,reserved,actual FROM amounts WHERE unit='bigquery_bytes'")}
    return snapshot, amounts


def audit_actual_dryruns(work, bundle_path, execution_plans, config, *, query_names=None):
    """No submission. Require every remaining real dry-run's aggregate upper bound.

    Existing deterministic job reservations are already in the current snapshot;
    they must not be counted again. Root re-runs this gate just before dispatch.
    """
    from stage1d_bigquery_probe import dry_spec
    from stage1d_recovery_policy import bq_reservation
    work = Path(work).resolve()
    queries = {q['query_id']: q for q in checked_freeze(work).values()}
    bundle = bq.read(bq.inside(work, bundle_path))
    if bundle.get('freeze_sha256') != bq.sha(active_batch_path(work)):
        raise ValueError('Aggregate gate requires this original shared preparation')
    names, owner_name, clock_query, _ = check_bundle_group(bundle, query_names)
    expected = set()
    for dependency in bundle['families']:
        family = bq.read(bq.checked(work, dependency))
        expected.update((spec['path'], spec['sha256']) for spec in family['plans']
                        if spec['template'] == 'transaction_and_trace')
    supplied = set()
    for dependency in execution_plans:
        plan = bq.read(bq.checked(work, dependency))
        supplied.add((plan['dry_spec_path'], plan['dry_spec_sha256']))
    if supplied != expected:
        raise ValueError('Aggregate gate requires ALL remaining prepared specs, with no subset or extras')
    snapshot, amounts = budget_snapshot_ro(work)
    jobs, total, oversize = {}, 0, []
    for dependency in execution_plans:
        plan = bq.read(bq.checked(work, dependency))
        spec_dep = {'path': plan['dry_spec_path'], 'sha256': plan['dry_spec_sha256']}
        spec = bq.read(bq.checked(work, spec_dep))
        dry = bq.read(bq.checked(work, {'path': plan['dry_receipt_path'], 'sha256': plan['dry_receipt_sha256']}))
        dry_spec(work, spec, config, queries[spec['query_id']])
        if queries[spec['query_id']]['name'] != owner_name:
            raise ValueError('Spec must retain this allowed query group original SQL owner')
        identity, result = dry.get('identity', {}), dry.get('result', {})
        if (dry.get('status') != 'SUCCESS_VALIDATED' or result.get('kind') != 'DRY_RUN'
                or result.get('actual_query_executed') is not False
                or identity.get('project') != config['project']
                or identity.get('sql_sha256') != spec['sql_sha256']
                or identity.get('spec_sha256') != spec_dep['sha256']
                or identity.get('scope_hash') != spec['scope_hash']):
            raise ValueError('Each actual dry-run must bind exact SQL, project, spec and freeze')
        estimate = result['estimated_processed_bytes']
        if type(estimate) is not int or estimate < 0:
            raise ValueError('Exact actual dry-run estimate required')
        digest = bq.digest({'sql_sha256': spec['sql_sha256'], 'project': config['project'], 'location': 'US'})
        if digest in jobs:
            continue
        state_path = work / 'private/stage1d_bigquery_jobs' / digest / 'job.json'
        state = bq.read(state_path) if state_path.exists() else None
        booked = amounts.get('bq_scan_' + digest)
        if state:
            if (state['sql_sha256'] != spec['sql_sha256'] or state['job_id'] != 'stage1d_recovery_' + digest[:48]
                    or state['plan_sha256'] != dependency['sha256']):
                raise ValueError('Existing deterministic job identity must not change')
            bound = state['maximum_bytes_billed']
            if type(bound) is not int or bound < estimate or bound > 100 * 1073741824:
                raise ValueError('Existing job bound invalid for same dry-run')
            if booked and booked['reserved'] != bound:
                raise ValueError('Existing risk reservation does not match saved job')
            if not booked and state['state'] != 'PREPARED':
                raise ValueError('Dispatched existing job lacks its shared scan ledger record')
        else:
            if booked:
                raise ValueError('Orphan existing scan risk requires explicit root reconciliation')
            if names in (QUERIES, XSCAM):
                # Calculate the entire remaining plan before deciding a route.
                # An oversize seven-day form is never submitted at a capped
                # artificial bound. One-day rebuilding is an explicit later step.
                from decimal import ROUND_CEILING
                from stage1d_recovery_policy import AUTH as RECOVERY_AUTH, BQ_TOTAL, BQ_JOB
                gib = 1073741824
                row = snapshot['bigquery_bytes']
                if row.get('authorization_id') != RECOVERY_AUTH or int(row['cap']) != BQ_TOTAL:
                    raise ValueError('Original adopted BigQuery authority required')
                rounded = max(10 * 1024 * 1024, int((Decimal(estimate) * Decimal('1.10') / gib)
                              .to_integral_value(rounding=ROUND_CEILING)) * gib)
                if estimate > BQ_JOB:
                    bound = rounded
                    oversize.append(spec_dep)
                else:
                    # Pure per-job upper formula, before the aggregate current
                    # room check; no ledger/snapshot amount is altered.
                    bound = min(rounded, BQ_JOB)
            else:
                bound = bq_reservation(estimate, snapshot)['maximum_bytes_billed']
        additional = 0 if booked else bound
        total += additional
        jobs[digest] = {'sql_sha256': spec['sql_sha256'], 'execution_plan': dependency,
                        'estimated_bytes': estimate, 'maximum_bytes_billed': bound if estimate <= 100 * 1073741824 else None,
                        'unsplit_required_upper_bound_bytes': bound,
                        'existing_state': state['state'] if state else None,
                        'already_booked': booked, 'additional_upper_bound_bytes': additional}
    remaining = snapshot['bigquery_bytes']['remaining']
    fits = remaining is not None and total <= int(Decimal(remaining))
    status = 'ACTUAL_DRYRUN_AGGREGATE_FITS' if fits else 'HARD_RESOURCE_GAP'
    route = 'CLASSIC_BIGQUERY_CURRENT_DEMAND' if fits else 'COMPACT_DUNE_CURRENT_DEMAND'
    if fits and oversize:
        if bundle.get('chunk_days', 7) == 7:
            status, route = 'SINGLE_JOB_REQUIRES_ONE_DAY_REPLAN', 'BQ_FULL_SCOPE_ONE_DAY_REPLAN'
        else:
            status, route = 'HARD_PLATFORM_CAPABILITY_GAP', 'COMPACT_DUNE_CURRENT_DEMAND'
    return {'status': status, 'route': route, 'query_names': sorted(names),
            'oversize_spec_dependencies': oversize,
            'original_context_gaps_preserved': True, 'automatic_replan_or_dune_query_executed': False,
            'source_preparation': bq.dep(work, bundle_path),
            'jobs': list(jobs.values()), 'new_upper_bound_total_bytes': total,
            'current_bigquery_snapshot': snapshot['bigquery_bytes'], 'actual_query_executed': False,
            'requires_immediate_root_single_writer_recheck': True, 'clock_query': clock_query}


def project_rows(rows, ranges):
    """Keep whole actual transaction families touching current consumer demand."""
    def touched(row):
        block = integer(row.get('block_number', row.get('blockNumber')))
        addresses = {row.get(k) for k in ('from_address', 'to_address', 'created_address', 'refund_address', 'from', 'to')}
        return any(r['address'] in addresses and r['start_block'] <= block <= r['end_block'] for r in ranges)
    hashes = {r['tx_hash'] for r in rows if r.get('tx_hash') and touched(r)}
    return [deepcopy(r) for r in rows if r.get('tx_hash') in hashes or not r.get('tx_hash') and touched(r)]


def merge_bound_rows(old, added):
    """Strict same-fact replacement of an old NULL root; never infer internal path."""
    old, added = deepcopy(old), deepcopy(added)
    # Binding receipts belong to provenance. Keep their exact SHA-keyed copies
    # while comparing the unchanged physical fields; two actual jobs can prove
    # the same root with different original-row provenance hashes.
    proofs, primary = defaultdict(dict), {}
    for row in list(old) + added:
        binding = row.get('root_position_binding', {})
        if binding.get('status') == 'ROOT_EQUIVALENT':
            tx = row['tx_hash']
            primary.setdefault(tx, deepcopy(binding))
            proofs[tx].update(deepcopy(row.pop('root_position_binding_proofs', {})))
            proofs[tx][bq.digest(binding)] = deepcopy(binding)
    for row in list(old) + added:
        binding = row.get('root_position_binding', {})
        if binding.get('status') != 'ROOT_EQUIVALENT':
            continue
        for index in range(len(old) - 1, -1, -1):
            prior = old[index]
            if (prior.get('record_type') == 'trace' and prior.get('tx_hash') == row['tx_hash']
                    and prior.get('trace_address') is None):
                check = deepcopy(row)
                check['trace_address'] = None
                check.pop('root_position_binding', None)
                _merge(prior, check)  # Reject any old physical disagreement.
                row['evidence_ids'] = sorted(set(row.get('evidence_ids', []) + prior.get('evidence_ids', [])))
                del old[index]
    for row in old + added:
        if row.get('tx_hash') in proofs and row.get('record_type') == 'trace' and row.get('trace_address') in ('[]', []):
            row.pop('root_position_binding', None)
    merged = _merge_rows(old, added)
    for row in merged:
        tx = row.get('tx_hash')
        if tx in proofs and row.get('record_type') == 'trace' and row.get('trace_address') in ('[]', []):
            row['root_position_binding'] = primary[tx]
            row['root_position_binding_proofs'] = proofs[tx]
    return merged


def materialize_saved_trace_point(work, state_path, spec_dependency, tx_hash, output):
    """Previously paid trace-only job: exact current RPC binding, zero coverage."""
    from stage1d_bq_root_binding import bind_saved_export_root
    work = Path(work).resolve()
    result = bind_saved_export_root(work, state_path, spec_dependency, tx_hash)
    folder = bq.inside(work, output)
    if result['status'] == 'ROOT_EQUIVALENT_BOUND':
        # Top/receipt/header are already SHA-checked current cache facts. Preserve
        # the new material as the original four traces; do not issue fee coverage.
        bq.save(folder / 'ledger_rows.json', result['rows'])
        bq.save(folder / 'coverage.json', [])
    receipt = {k: v for k, v in result.items() if k not in ('rows', 'current_top_row')}
    receipt.update(job_state=bq.dep(work, state_path), spec=spec_dependency,
                   new_provider_requests=0, coverage_added=[], formal_model_built=False)
    bq.save(folder / 'BQ_POINT_MATERIAL_RECEIPT.json', receipt)
    return receipt


def materialize_family(work, bundle_path, family_dependency, job_states, output, *, query_names=None):
    """Verify all seven-day chunks; write standard round inputs, never build model.

    output is a fresh staging directory. Root may feed these files directly into
    build_document only at its authorized final common-context step. Recompute
    necessary windows after merge; new context demand remains explicit.
    """
    work = Path(work).resolve()
    bundle = bq.read(bq.inside(work, bundle_path))
    bq.checked(work, bundle['frozen_inputs'])
    if bundle.get('freeze_sha256') != bq.sha(active_batch_path(work)):
        raise ValueError('Context bundle does not bind original freeze')
    names, _, _, _ = check_bundle_group(bundle, query_names)
    if set(bundle['consumers']) != names:
        raise ValueError('Context bundle does not bind this exact allowed query group')
    if family_dependency not in bundle['families']:
        raise ValueError('Family was not in this shared current-demand preparation')
    manifest_path = bq.checked(work, family_dependency)
    result = bq.normalize_complete_family(work, manifest_path, 'transaction_and_trace', job_states, bind_roots=True)
    if result['normalized']['conflicts']:
        raise EvidenceConflict('BQ native physical conflicts prevent ledger admission')
    queries = checked_freeze(work)
    outputs = {}
    for name, consumer in bundle['consumers'].items():
        collection = bq.read(bq.checked(work, consumer['inputs']['collection']))
        labels = bq.read(bq.checked(work, consumer['inputs']['labels']))
        current = {key: bq.read(bq.checked(work, value)) for key, value in consumer['prepared_context'].items()}
        headers = {int(key): value for key, value in current['headers'].items()}
        ledger = merge_bound_rows(current['ledger_rows'], project_rows(result['rows'], consumer['needed_ranges']))
        coverage = list(current['coverage'])
        for record in result['coverage']:
            for needed in consumer['needed_ranges']:
                lo, hi = max(record['start_block'], needed['start_block']), min(record['end_block'], needed['end_block'])
                if record['address'] == needed['address'] and lo <= hi:
                    coverage = merge_coverage(coverage, [dict(record, start_block=lo, end_block=hi)])
        plan = necessary_context_windows(queries[name], collection, ledger, labels,
                                         prior_rows=current['context_plan']['rows'])
        for row in plan['rows']:
            if not queries[name]['start_block'] <= row['ledger_start_block'] <= row['ledger_end_block'] <= queries[name]['end_block']:
                raise EvidenceConflict('New physical context escaped frozen query scope')
        rpc_missing = cached_points(work, point_needs(plan, collection, ledger), headers, current['balances'], current['receipts'])
        remaining = [{'address': row['address'], 'start_block': lo, 'end_block': hi}
                     for row in plan['rows'] for lo, hi in gaps_for_kinds(row, coverage)]
        folder = bq.inside(work, output) / name
        for filename, value in [('headers.json', headers), ('balances.json', current['balances']),
                                ('receipts.json', current['receipts']), ('ledger_rows.json', ledger),
                                ('coverage.json', coverage), ('context_plan.json', plan)]:
            bq.save(folder / filename, value)
        material = {'query_id': queries[name]['query_id'], 'scope_hash': queries[name]['scope_hash'],
                    'source_preparation': bq.dep(work, bundle_path), 'family': family_dependency,
                    'job_states': {key: bq.dep(work, path) for key, path in job_states.items()},
                    'remaining_bq_ranges': remaining, 'missing_rpc': rpc_missing,
                    'provider_gaps': result['gaps'], 'protocol_gaps_preserved': True,
                    'full_context_claimed': False, 'formal_model_built': False,
                    'round_files': {n: bq.dep(work, folder / (n + '.json')) for n in
                        ('headers', 'balances', 'receipts', 'ledger_rows', 'coverage', 'context_plan')}}
        bq.save(folder / 'BQ_MATERIAL_RECEIPT.json', material)
        outputs[name] = material
    return outputs
