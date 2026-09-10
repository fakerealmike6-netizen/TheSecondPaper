"""Compact, complete canonical holder logs from Dune; token discovery only.

SQL/page originals remain Dune evidence. Historical headers bind physical order;
no fabricated RPC response, native ledger completeness, or conversion certificate.
"""
from pathlib import Path
from dataclasses import asdict
import re
from collector import Scope
from context_access_r3 import read, sha
from page_attempts import atomic_json
from page_contract import initial_progress, validate_page
from provider_dune import sql_time
from stage1d_closure_scope import active_batch
from stage1d_cost_request_guard import validate_candidate_entry, _allowed
from stage1d_window import missing_rectangles
from stage1d_finite_state_rpc import WETH, TRANSFER, DEPOSIT, WITHDRAWAL, topic_address
from stage1d_finite_state_route import prepare as prepare_points, execute as execute_points
from stage1d_weth_log_index import (TOKEN, digest, _inside, _dependency, _checked,
    _actual_need, _env, _header, normalize_canonical_logs)

ADAPTER = 'stage1d-dune-canonical-weth-log-index-v1'
FIELDS = ('block_number','block_hash','timestamp','tx_hash','tx_index','log_index',
          'contract_address','topic0','topic1','topic2','topic3','data')
BOUNDS = ('start_block','end_block','start_time','end_time')


def build_sql(need):
    holder = topic_address(need['address'])
    if need.get('asset') != TOKEN or any(type(need.get(k)) is not int for k in BOUNDS):
        raise ValueError('Exact canonical holder asset and integer bounds required')
    if need['start_block'] > need['end_block'] or need['start_time'] > need['end_time']:
        raise ValueError('Inverted holder interval')
    fields = []
    for key in FIELDS:
        source = '"index"' if key == 'log_index' else key
        if key == 'timestamp': source = 'CAST(to_unixtime(block_time) AS bigint)'
        value = ('CAST(' + source + ' AS varchar)' if key in
                 ('block_number','timestamp','tx_index','log_index') else
                 "'0x' || lower(to_hex(" + source + '))')
        fields.append(value + ' AS ' + key)
    lo, hi = sql_time(need['start_time']), sql_time(need['end_time'])
    return ('-- Stage1D exact canonical WETH holder logs, all successful raw log rows\n'
        + 'SELECT ' + ',\n       '.join(fields) + '\nFROM ethereum.logs\n'
        + f"WHERE block_date BETWEEN DATE '{lo[:10]}' AND DATE '{hi[:10]}'\n"
        + f"  AND block_time BETWEEN TIMESTAMP '{lo}' AND TIMESTAMP '{hi}'\n"
        + f"  AND block_number BETWEEN {need['start_block']} AND {need['end_block']}\n"
        + f'  AND contract_address = {WETH}\n'
        + f'  AND ((topic0 = {TRANSFER} AND (topic1 = {holder} OR topic2 = {holder}))\n'
        + f'       OR (topic0 IN ({DEPOSIT},{WITHDRAWAL}) AND topic1 = {holder}))\n'
        + 'ORDER BY block_number, tx_index, "index"\n')


def _integer(value):
    # SQL explicitly exports varchar, so neither rounded JSON floats nor a new
    # upstream type is silently admitted as an exact scientific observation.
    if not isinstance(value, str) or not re.fullmatch(r'0|[1-9][0-9]*', value):
        raise ValueError('Exact nonnegative decimal string required')
    return int(value)


def _hex(value, size):
    if not isinstance(value, str) or not re.fullmatch('0x[0-9a-f]{'+str(size*2)+'}', value):
        raise ValueError('Exact normalized fixed-width hex required')
    return value


def canonical_rows(rows, need):
    """Validate Dune columns without manufacturing any provider envelope."""
    result = []
    holder = topic_address(need['address'])
    for row in rows:
        if set(row) != set(FIELDS): raise ValueError('Canonical Dune log schema differs')
        number, timestamp, ti, li = [_integer(row[k]) for k in
            ('block_number','timestamp','tx_index','log_index')]
        if not (need['start_block'] <= number <= need['end_block'] and
                need['start_time'] <= timestamp <= need['end_time']):
            raise ValueError('Dune row outside exact frozen holder interval')
        if row['contract_address'] != WETH: raise ValueError('Wrong canonical emitter')
        topics = [_hex(row['topic0'],32), _hex(row['topic1'],32)]
        if topics[0] == TRANSFER:
            topics.append(_hex(row['topic2'],32))
            if holder not in topics[1:]: raise ValueError('Transfer unrelated to frozen holder')
        elif topics[0] in (DEPOSIT,WITHDRAWAL):
            if topics[1] != holder or row['topic2'] is not None:
                raise ValueError('Conversion log holder/topic shape differs')
        else: raise ValueError('Unsupported canonical log signature')
        if row['topic3'] is not None or any(t[2:26] != '0'*24 for t in topics[1:]):
            raise ValueError('Canonical indexed address encoding differs')
        result.append(dict(address=WETH, blockNumber=hex(number),
            blockHash=_hex(row['block_hash'],32), transactionHash=_hex(row['tx_hash'],32),
            transactionIndex=hex(ti), logIndex=hex(li), removed=False,
            topics=topics, data=_hex(row['data'],32)))
    return result


def verify_pages(work, job_ref, sql_sha):
    """Re-read all original pages, including the certified terminal empty page."""
    work = Path(work).resolve()
    job = read(_checked(work,job_ref))
    if job.get('sql_sha256') != sql_sha or job.get('state') != 'QUERY_STATE_COMPLETED':
        raise ValueError('Dune job SQL/completion differs')
    status = job.get('status_response') or {}
    if status.get('execution_id') != job.get('execution_id') or status.get('state') != 'QUERY_STATE_COMPLETED':
        raise ValueError('Dune final status identity differs')
    metadata = status.get('result_metadata') or {}
    progress = initial_progress(metadata.get('total_row_count'))
    rows, seen, refs = [], [], [job_ref]
    while not progress['complete']:
        offset = progress['next_offset']; seen.append(str(offset))
        entry = job.get('r4_verified_pages',{}).get(str(offset))
        if entry is None: raise ValueError('Incomplete original Dune page chain')
        page_ref = dict(path=entry['page_path'],sha256=entry['page_sha256'])
        receipt_ref = dict(path=entry['receipt_path'],sha256=entry['receipt_sha256'])
        page, receipt = [read(_checked(work,r)) for r in (page_ref,receipt_ref)]
        if receipt.get('evidence_kind') != 'REAL_PROVIDER':
            raise ValueError('Original Dune transport evidence required')
        raw_ref = dict(path=receipt['raw_path'],sha256=receipt['sha256'])
        if read(_checked(work,raw_ref)) != page: raise ValueError('Dune page differs from original bytes')
        params = receipt.get('parameters',{})
        if params.get('offset') != offset or set(params) != {'offset','limit'}:
            raise ValueError('Dune page selector differs')
        progress = validate_page(page,execution_id=job['execution_id'],offset=offset,
            limit=params.get('limit'),progress=progress,status_metadata=metadata,
            receipt=receipt,parameters=params)
        rows.extend(page['result']['rows']); refs.extend([page_ref,receipt_ref,raw_ref])
    if set(seen) != set(job.get('r4_verified_pages',{})):
        raise ValueError('Extra or overlapping Dune pages')
    return rows, refs, progress


def _snapshot(work, path):
    path = _inside(work,path); expected = sha(path)
    saved = work/'private/stage1d_finite_requirements/evidence'/(expected+path.suffix)
    if not saved.exists():
        saved.parent.mkdir(parents=True,exist_ok=True); saved.write_bytes(path.read_bytes())
    if sha(saved) != expected or sha(path) != expected: raise ValueError('Immutable source changed')
    return dict(path=saved.relative_to(work).as_posix(),sha256=expected,
                original_path=path.relative_to(work).as_posix())


def _domain(work, query, need, collection_path):
    scope = Scope.from_policy(query)
    if query.get('scope_hash') != scope.scope_hash or query.get('scope_id') != scope.scope_id:
        raise ValueError('Current query scope identity differs')
    collection = read(collection_path); _actual_need(query,collection,need)
    entry = dict(query_name=query['name'],query_id=query['query_id'],scope_hash=query['scope_hash'],
        collection_path=collection_path.relative_to(work).as_posix(),collection_sha256=sha(collection_path),
        needed_ranges=[need])
    return validate_candidate_entry(work,query,entry)


def acquire(work, query, need, collection_path, *, admit_headers=None):
    from stage1d_acquisition import save_sql
    from stage1d_runtime import execute_sql
    work = Path(work).resolve(); collection_path = _inside(work,collection_path)
    if query not in active_batch(work)['queries']: raise ValueError('Exact active query required')
    guard = _domain(work,query,need,collection_path)
    ident = digest(dict(scope=query['scope_hash'],need=need,graph=sha(collection_path),adapter=ADAPTER))
    folder = work/'private/stage1d_dune_weth_logs'/ident; folder.mkdir(parents=True,exist_ok=True)
    state_path = folder/'STATE.json'
    ep = work/'derived/stage1d/intervals'/(ident+'.events.json')
    cp = ep.with_name(ident+'.coverage.json')
    if cp.exists(): verify_interval_record(work,read(cp)); return read(state_path)
    inputs = [_snapshot(work,collection_path),_snapshot(work,collection_path.parent/'label_snapshot.json')]
    state = read(state_path) if state_path.exists() else dict(schema_version=ADAPTER,
        query=query,need=need,inputs=inputs,guard=guard,headers={},requirements=[],new_rpc_operations=0)
    if any(state[k] != v for k,v in (('query',query),('need',need),('inputs',inputs),('guard',guard))):
        raise ValueError('Preserved Dune holder requirement changed')
    atomic_json(state_path,state)
    try:
        # The compact descriptor binds the full immutable graph without passing
        # a >64 MiB graph as a point-importer's bounded JSON requirement file.
        descriptor = folder/'DEMAND.json'
        demand = dict(query_id=query['query_id'],scope_id=query['scope_id'],scope_hash=query['scope_hash'],
            need=need,inputs=inputs,guard=guard)
        if descriptor.exists() and read(descriptor) != demand: raise ValueError('Demand identity changed')
        if not descriptor.exists(): atomic_json(descriptor,demand)
        dep = _dependency(work,descriptor)
        freeze_path = save_sql(work,build_sql(need),'candidate',query,
            [dict(need,kind='candidate',query_id=query['query_id'])],[dep],addresses=[need['address']])
        state['freeze_ref'] = _dependency(work,freeze_path)
        state['sql_ref'] = _dependency(work,freeze_path.parent/'query.sql')
        _domain(work,query,need,collection_path)  # Actual dispatch boundary.
        state['sql_execution'] = execute_sql(work,freeze_path,query['name'],'canonical_weth_holder_logs')
        atomic_json(state_path,state)
        if state['sql_execution']['status'] != 'COMPLETED_EXPORTED':
            raise ValueError('Dune holder export remains '+state['sql_execution']['status'])
        job = _inside(work,Path(state['sql_execution']['job_folder'])/'job.json')
        state['job_ref'] = _snapshot(work,job)
        rows, refs, progress = verify_pages(work,state['job_ref'],read(freeze_path)['sql_sha256'])
        state.update(page_progress=progress,page_evidence=refs)
        logs = canonical_rows(rows,need)
        plans = [dict(method='eth_getBlockByNumber',params=[hex(b),False]) for b in
            sorted({int(log['blockNumber'],16) for log in logs}) if str(b) not in state['headers']]
        if plans:
            if admit_headers is not None:
                state['existing_header_admission'] = admit_headers(query,plans,dep)
            req = prepare_points(work,query,plans,purpose='CANONICAL_WETH_DISCOVERY',
                requirement_evidence=[dep],role_snapshot=inputs[1],missing_fields=['MISSING_UNIQUE_WETH_LOG_HEADERS'])
            outcome = execute_points(work,req)
            state['requirements'].append(_dependency(work,req))
            state['new_rpc_operations'] += outcome['actual_operations_this_call']
            for member in outcome['results']:
                env = _env(work,member,member['plan']); number = int(env['response']['result']['number'],16)
                state['headers'][str(number)] = {k:member[k] for k in ('result','artifact_path','artifact_sha256')}
            atomic_json(state_path,state)
            if outcome['status'] != 'COMPLETE': raise ValueError('WETH header binding remains '+outcome['status'])
        events, observed = _normalize_bound(rows,logs,state['headers'],need,work)
        atomic_json(ep,[asdict(e) for e in events])
        state.update(status='COMPLETE',observed_log_count=len(observed),ordinary_transfer_events=len(events))
        atomic_json(state_path,state)
        proof = folder/('PROOF_'+digest(state)+'.json')
        if proof.exists() and read(proof) != state: raise ValueError('Immutable holder proof changed')
        if not proof.exists(): atomic_json(proof,state)
        record = dict(evidence_id=ident,addresses=[need['address']],asset=TOKEN,
            **{k:need[k] for k in BOUNDS},complete=True,normalization_gaps=[],provider='Dune',
            acquisition_adapter=ADAPTER,events_path=ep.relative_to(work).as_posix(),events_sha256=sha(ep),
            state_path=proof.relative_to(work).as_posix(),state_sha256=sha(proof),
            query_id_at_acquisition=query['query_id'],scope_id_at_acquisition=query['scope_id'],
            scope_hash_at_acquisition=query['scope_hash'],native_gas_ledger_complete=False,
            log_scope='ALL_CANONICAL_HOLDER_TRANSFER_DEPOSIT_WITHDRAWAL')
        verify_interval_record(work,record); atomic_json(cp,record)
    except (ValueError,RuntimeError,KeyError,TypeError) as exc:
        state.update(status='PARTIAL',reason=str(exc)); atomic_json(state_path,state)
    return state


def _normalize_bound(rows, logs, members, need, work):
    headers = {}
    for key,member in members.items():
        plan = dict(method='eth_getBlockByNumber',params=[hex(int(key)),False])
        headers[int(key)] = _env(work,member,plan)['response']['result']
    for row in rows:
        number = _integer(row['block_number'])
        if number not in headers: raise ValueError('Dune log historical header gap')
        if int(_header(headers[number],number)['timestamp'],16) != _integer(row['timestamp']):
            raise ValueError('Dune timestamp differs from historical header')
    return normalize_canonical_logs(logs,headers,need)


def verify_interval_record(work, record):
    work = Path(work).resolve()
    state = read(_checked(work,dict(path=record['state_path'],sha256=record['state_sha256'])))
    if state.get('schema_version') != ADAPTER or state.get('status') != 'COMPLETE':
        raise ValueError('Completed canonical Dune holder proof required')
    need, query = state['need'], state['query']
    if (record.get('complete') is not True or record.get('normalization_gaps') != []
        or record.get('provider') != 'Dune' or record.get('acquisition_adapter') != ADAPTER
        or record.get('native_gas_ledger_complete') is not False or record.get('asset') != TOKEN
        or record.get('addresses') != [need['address']] or any(record[k] != need[k] for k in BOUNDS)
        or record.get('log_scope') != 'ALL_CANONICAL_HOLDER_TRANSFER_DEPOSIT_WITHDRAWAL'
        or any(record[k+'_at_acquisition'] != query[k] for k in ('query_id','scope_id','scope_hash'))):
        raise ValueError('Exact token-only holder coverage required')
    if len(state['inputs']) != 2: raise ValueError('Graph and role snapshots required')
    collection = read(_checked(work,state['inputs'][0])); _checked(work,state['inputs'][1])
    scope = _actual_need(query,collection,need)
    if query.get('scope_hash') != scope.scope_hash or query.get('scope_id') != scope.scope_id:
        raise ValueError('Historical query scope identity differs')
    # Frozen original role/cost decisions authorize the original acquisition.
    # Historical coverage remains reusable after later roles or scopes change.
    if collection.get('cost_boundary_overlay') or 'cost_boundary' in collection['states'][0]:
        allowed, _, _ = _allowed(collection,scope)
        if missing_rectangles(need,allowed.get((need['address'],TOKEN),())):
            raise ValueError('Frozen holder need exceeds allowed role/cost decisions')
    expected = digest(dict(scope=query['scope_hash'],need=need,graph=state['inputs'][0]['sha256'],adapter=ADAPTER))
    if record['evidence_id'] != expected: raise ValueError('Dune holder identity changed')
    freeze = read(_checked(work,state['freeze_ref']))
    sql = _checked(work,state['sql_ref'])
    if (sql.read_text(encoding='utf8') != build_sql(need) or sha(sql) != freeze.get('sql_sha256')
        or freeze.get('kind') != 'candidate' or freeze.get('scope_hash') != query['scope_hash']
        or freeze.get('scope_id') != query['scope_id'] or freeze.get('query_ids') != [query['query_id']]
        or freeze.get('addresses') != [need['address']]
        or freeze.get('intervals') != [dict(need,kind='candidate',query_id=query['query_id'])]
        or freeze.get('export_plan') != dict(all_pages_required=True,no_limit_or_sampling=True)):
        raise ValueError('Exact full holder SQL freeze differs')
    for ref in freeze['dependencies']: _checked(work,ref)
    rows, refs, progress = verify_pages(work,state['job_ref'],freeze['sql_sha256'])
    if state['page_progress'] != progress or state['page_evidence'] != refs: raise ValueError('Page evidence changed')
    events, observed = _normalize_bound(rows,canonical_rows(rows,need),state['headers'],need,work)
    if (len(observed) != state['observed_log_count'] or len(events) != state['ordinary_transfer_events']
        or read(_checked(work,dict(path=record['events_path'],sha256=record['events_sha256']))) != [asdict(e) for e in events]):
        raise ValueError('Canonical holder normalization changed')
    return True
