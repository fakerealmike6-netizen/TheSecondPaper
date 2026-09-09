"""Two finite Etherscan account endpoint checks over an existing context gap.

Uses the accepted EtherscanProvider page/retry adapter and the active Runtime
ledger/session. No calls occur on import. Raw internal rows are deliberately not
promoted to canonical call-tree events by this capability harness.
"""
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request

from exact_fields_r4 import exact_uint
from network import NoRedirect
from page_attempts import atomic_json
from provider_etherscan import EtherscanProvider, classify_response
from read_retry_r4 import ReadRetryStore
from stage1d_costs import AUTH, inside, canonical
from stage1d_closure_scope import batch_for_scope, batch_path_for_sha

PROBE_AUTH = 'STAGE1D_MULTI_PROVIDER_ROUTING_V1'
RAW_READ_LIMIT = 4 * 1024 * 1024
ACTIONS = ('txlist', 'txlistinternal')


class AccountHTTPFailure(RuntimeError):
    def __init__(self, code, headers):
        super().__init__('Official account endpoint returned an HTTP error')
        self.code, self.headers = code, headers


def read(path): return json.loads(Path(path).read_bytes())
def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def prepare_plan(work, source_freeze, query_id, address, start_block, end_block):
    """Pure preparation: exact existing context interval, no query expansion."""
    work = Path(work).resolve(); source_freeze = inside(work, source_freeze)
    frozen = read(source_freeze)
    if frozen.get('schema_version') != 'stage1d-sql-freeze-v1' or frozen.get('authorization_id') != AUTH or frozen.get('kind') != 'context':
        raise ValueError('Existing Stage1D context SQL freeze required')
    if sha(source_freeze.parent / 'query.sql') != frozen['sql_sha256']:
        raise ValueError('Existing SQL changed')
    for dependency in frozen['dependencies']:
        if sha(inside(work, dependency['path'])) != dependency['sha256']: raise ValueError('Existing SQL dependency changed')
    batch = batch_for_scope(work,frozen['scope_id'],frozen['scope_hash'])
    from stage1d_closure_scope import active_batch_path
    batch_path = active_batch_path(work)
    if read(batch_path)!=batch:batch_path=work/'private/BATCH_QUERY_FREEZE.json'
    query = next((q for q in batch['queries'] if q['query_id'] == query_id), None)
    if not query or query_id not in frozen['query_ids'] or len(batch['queries']) != 4:
        raise ValueError('The existing four-query domain must remain fixed')
    if not re.fullmatch('0x[0-9a-f]{40}', address): raise ValueError('Exact lowercase address required')
    start_block = exact_uint(start_block); end_block = exact_uint(end_block)
    if start_block > end_block: raise ValueError('Inverted interval')
    interval = {'address': address, 'start_block': start_block, 'end_block': end_block, 'query_id': query_id, 'kind': 'context'}
    if interval not in frozen['intervals']: raise ValueError('Probe must exactly reuse a frozen necessary context interval')
    if frozen['scope_id'] != query['scope_id'] or frozen['scope_hash'] != query['scope_hash']:
        raise ValueError('Existing context/query scope identities differ')
    return {'schema_version': 'stage1d-etherscan-account-probe-v1', 'authorization_id': PROBE_AUTH,
        'scope_authorization_id': AUTH, 'query_id': query_id, 'query_name': query['name'],
        'scope_id': query['scope_id'], 'scope_hash': query['scope_hash'], 'interval': interval,
        'source_freeze': {'path': source_freeze.relative_to(work).as_posix(), 'sha256': sha(source_freeze)},
        'batch_freeze_sha256': sha(batch_path), 'actions': list(ACTIONS), 'page': 1, 'page_size': 1000,
        'rpc_operations_initial': 2, 'max_total_attempts_per_endpoint': 3,
        'original_raw_bytes_bound_per_attempt': RAW_READ_LIMIT,
        'not_a_nametag_or_blockrange_internal_probe': True, 'canonical_internal_events_admitted': False,
        'selection_basis': 'Existing required context interval selected independently of amount/model output; two current account endpoints only.'}


def verify_plan(work, plan):
    work = Path(work).resolve()
    source = plan['source_freeze']
    if sha(inside(work, source['path'])) != source['sha256']: raise ValueError('Probe source freeze hash changed')
    interval = plan['interval']
    expected = prepare_plan(work, source['path'], plan['query_id'], interval['address'], interval['start_block'], interval['end_block'])
    if expected != plan: raise ValueError('Immutable probe selection changed')
    return plan


def parameters(plan, action):
    if action not in ACTIONS: raise ValueError('Only two necessary account endpoints')
    r = plan['interval']
    return {'chainid': '1', 'module': 'account', 'action': action, 'address': r['address'],
        'startblock': r['start_block'], 'endblock': r['end_block'], 'page': 1, 'offset': 1000, 'sort': 'asc'}


def inspect_rows(params, body):
    """Pure physical selection/schema check; no traceId canonicalization."""
    classification = classify_response(body)
    if classification['outcome'] != 'SUCCESS': return None
    values = body['result']
    if len(values) > params['offset']: raise ValueError('Provider returned more than frozen maximum')
    previous = None; seen = set(); missing = set()
    for row in values:
        if not isinstance(row, dict): raise ValueError('Malformed account row')
        block = exact_uint(row.get('blockNumber')); exact_uint(row.get('timeStamp')); exact_uint(row.get('value'))
        if not params['startblock'] <= block <= params['endblock']: raise ValueError('Returned block outside frozen interval')
        if previous is not None and block < previous: raise ValueError('Returned rows do not follow frozen ascending order')
        previous = block
        tx = str(row.get('hash', '')).lower()
        if not re.fullmatch('0x[0-9a-f]{64}', tx): raise ValueError('Transaction identity invalid')
        addresses = {str(row.get(k, '')).lower() for k in ('from', 'to', 'contractAddress')}
        if params['address'] not in addresses: raise ValueError('Returned row does not involve the queried account')
        if row.get('isError') not in ('0', '1', 0, 1): missing.add('isError')
        if params['action'] == 'txlist':
            identity = tx
            for field in ('transactionIndex', 'gasUsed', 'gasPrice', 'blockHash', 'txreceipt_status'):
                if row.get(field) in (None, ''): missing.add(field)
        else:
            trace_id = row.get('traceId')
            if trace_id in (None, ''): missing.add('provider_traceId')
            identity = (tx, str(trace_id))
            missing.update(('verified_canonical_trace_path', 'complete_call_ancestors', 'transaction_position_from_receipt'))
        if identity in seen: raise ValueError('Duplicate provider row identity within page')
        seen.add(identity)
    ended = len(values) < params['offset']
    return {'endpoint_status': 'CURRENT_ENDPOINT_RETURNED_VALID_PAGE', 'row_count': len(values),
        'provider_enumeration_exhausted': ended, 'next_page': None if ended else 2,
        'full_chain_ledger_complete': False, 'canonical_internal_events_admitted': False,
        'detail_gaps': sorted(missing), 'empty_success': not values,
        'scope': {'address': params['address'], 'start_block': params['startblock'], 'end_block': params['endblock']},
        'coverage_basis': 'Provider page1 enumeration only; a full page requires subsequent pagination. Explorer rows do not prove complete fees/ancestors.'}


class BudgetedAccountTransport:
    """Small adapter over the same ledger and single writer; never old Network."""
    def __init__(self, work, plan, runtime, deadline, *, transport=None, clock=time.time, sleep=time.sleep):
        self.w = Path(work).resolve(); self.plan = verify_plan(self.w, plan)
        self.runtime, self.deadline, self.transport = runtime, deadline, transport
        self.clock, self.sleep = clock, sleep
        self.ledger = runtime.ledger_factory(self.w / 'private/shared_budget_r4.sqlite')
        self.active = None
        self.plan_id = hashlib.sha256(canonical(plan)).hexdigest()
        self.directory = self.w / 'private/stage1d_etherscan_probe' / self.plan_id
        self.directory.mkdir(parents=True, exist_ok=True)
        plan_path = self.directory / 'plan.json'
        if plan_path.exists() and read(plan_path) != plan: raise ValueError('Persisted probe plan conflict')
        if not plan_path.exists(): atomic_json(plan_path, plan)

    def reserve(self, claim, params):
        if params != parameters(self.plan, params.get('action')): raise ValueError('Unexpected account selector')
        self.runtime.require_gate(self.w)
        if self.transport is None:
            if not (self.w / 'private/network_worker.lock').exists(): raise RuntimeError('Use the current single-writer session')
            if not os.environ.get('ETHERSCAN_API_KEY'): raise RuntimeError('Configured Etherscan credential absent')
        if self.clock() + 30 > self.deadline: raise RuntimeError('Insufficient remaining same-query online clock')
        if self.runtime.raw_risk(self.w) + RAW_READ_LIMIT > (self.runtime.raw_limit(self.w) if getattr(self.runtime,'raw_limit',None) else 536870912): raise RuntimeError('Unchanged cumulative raw budget')
        snapshot = self.ledger.snapshot()
        if any(r.get('overrun') or r.get('actual_exceeded_reservation') for r in snapshot.values()): raise RuntimeError('Existing resource halt')
        job = 'stage1d_etherscan_' + claim['attempt_id']
        self.ledger.reserve(job, 'etherscan', 'Current finite account ' + params['action'], {'rpc_operations': 1})
        try:
            if getattr(self.runtime,'reserve_raw',None):self.runtime.reserve_raw(self.w,'etherscan_'+claim['attempt_id'],RAW_READ_LIMIT+65536)
        except Exception:
            self.ledger.settle(job,{'rpc_operations':0});raise
        self.active = {'claim': dict(claim), 'params': dict(params), 'job': job}
        atomic_json(self.directory / 'attempts' / claim['attempt_id'] / 'dispatch.json',
            {'job': job, 'params': params, 'plan_sha256': self.plan_id, 'request_url_persisted': False,
             'rpc_operations': 1, 'utc_epoch': self.clock(), 'retry_of': claim.get('retry_of')})
        return {'job': job, 'rpc_operations': 1, 'provider': 'ETHERSCAN_V2_ACCOUNT', 'original_risk_released': False}

    def __call__(self, params):
        if self.active is None or params != self.active['params']: raise RuntimeError('A budgeted dispatch intent is required')
        active = self.active; self.active = None
        # Conservative2/s spacing also survives a new adapter instance.
        last_path = self.w / 'private/stage1d_etherscan_last_dispatch.json'
        previous = read(last_path)['epoch'] if last_path.exists() else 0
        self.sleep(max(0, 0.55 - (self.clock() - previous)))
        atomic_json(last_path, {'epoch': self.clock(), 'plan_sha256': self.plan_id})
        raw, status, headers, failure = b'', None, {}, None
        try:
            if self.transport is not None:
                status, raw, headers = self.transport(dict(params), RAW_READ_LIMIT + 1)
            else:
                key = os.environ['ETHERSCAN_API_KEY']
                url = 'https://api.etherscan.io/v2/api?' + urllib.parse.urlencode({**params, 'apikey': key})
                request = urllib.request.Request(url, headers={'Accept': 'application/json'})
                try:
                    with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
                        status, raw, headers = response.status, response.read(RAW_READ_LIMIT + 1), dict(response.headers)
                except urllib.error.HTTPError as exc:
                    with exc: status, raw, headers = exc.code, exc.read(RAW_READ_LIMIT + 1), dict(exc.headers)
        except Exception as exc:
            failure = exc
            if isinstance(getattr(exc, 'partial', None), bytes): raw = exc.partial[:RAW_READ_LIMIT]
        if not isinstance(raw, bytes): raw, failure = b'', ValueError('Non-bytes provider response')
        if len(raw) > RAW_READ_LIMIT: raw, failure = raw[:RAW_READ_LIMIT], ValueError('Response exceeded reserved raw read bound')
        if any(v.encode() in raw for k, v in os.environ.items() if any(t in k.upper() for t in ('API_KEY','TOKEN','SECRET')) and len(v) >= 12):
            raw, failure = b'', ValueError('Credential echo withheld')
        aid = active['claim']['attempt_id']; raw_path = self.w / 'raw/etherscan_stage1d' / (aid + '.bin')
        raw_path.parent.mkdir(parents=True, exist_ok=True); raw_path.write_bytes(raw)
        receipt = {'schema_version': 'stage1d-etherscan-original-http-v1', 'provider': 'etherscan',
            'operation': params['action'], 'params': params, 'attempt_id': aid, 'logical_key': active['claim']['logical_key'],
            'job': active['job'], 'http_status': status, 'error_class': type(failure).__name__ if failure else None,
            'raw_path': raw_path.relative_to(self.w).as_posix(), 'raw_bytes': len(raw), 'sha256': sha(raw_path),
            'retry_after': headers.get('Retry-After', headers.get('retry-after')), 'plan_sha256': self.plan_id,
            'request_url_persisted': False, 'evidence_kind': 'REAL_PROVIDER' if self.transport is None else 'SYNTHETIC_TRANSPORT'}
        atomic_json(self.w / 'logs' / ('stage1d_etherscan_' + aid + '.json'), receipt)
        self.ledger.settle(active['job'], {'rpc_operations': 1})
        if getattr(self.runtime,'close_raw',None):self.runtime.close_raw(self.w,'etherscan_'+aid,self.w/'logs'/('stage1d_etherscan_'+aid+'.json'))
        if failure is not None:
            atomic_json(self.w / 'private/context_uncertainty' / ('stage1d_etherscan_' + aid + '.json'),
                {'additional_raw_risk_bytes': max(0, RAW_READ_LIMIT - len(raw)), 'basis': 'Unknown bounded response prefix', 'receipt': aid})
        if failure is not None: raise failure
        if status != 200:
            raise AccountHTTPFailure(status, headers)
        body = json.loads(raw, parse_float=Decimal)
        inspect_rows(params, body)  # Invalid facts never enter successful page cache.
        return body


def probe_in_session(work, plan, runtime, deadline, *, transport=None, clock=time.time, sleep=time.sleep):
    budgeted = BudgetedAccountTransport(work, plan, runtime, deadline, transport=transport, clock=clock, sleep=sleep)
    store = getattr(runtime,'retry_store',ReadRetryStore)(Path(work) / 'private/read_retry_r4.sqlite', clock=clock)
    provider = EtherscanProvider(budgeted, budgeted.directory / 'page_cache', page_size=1000, max_pages=1,
        retry_store=store, deadline=deadline, attempt_hook=budgeted.reserve, clock=clock, sleep=sleep)
    results = []
    for action in ACTIONS:
        params = parameters(plan, action)
        try:
            body, receipt, nbytes, requests, hits = provider._page(params)
            original = read(Path(work) / 'logs' / ('stage1d_etherscan_' + receipt['attempt_id'] + '.json'))
            original_raw = inside(Path(work).resolve(), original['raw_path']).read_bytes()
            if (original.get('http_status') != 200 or original.get('error_class') or original.get('params') != params
                    or original.get('plan_sha256') != budgeted.plan_id or len(original_raw) != original['raw_bytes']
                    or hashlib.sha256(original_raw).hexdigest() != original['sha256']
                    or canonical(json.loads(original_raw, parse_float=Decimal)) != canonical(body)):
                raise ValueError('Original successful response/receipt differs from cached page')
            result = inspect_rows(params, body)
            result.update(action=action, original_page_cache_receipt=receipt, original_http_receipt=original,
                actual_operations_this_call=requests, cache_hits=hits)
        except Exception as exc:
            result = {'action': action, 'endpoint_status': 'FAILED_OR_DEFERRED', 'reason': getattr(exc, 'reason', type(exc).__name__),
                'counters': getattr(exc, 'counters', {}), 'full_chain_ledger_complete': False, 'canonical_internal_events_admitted': False}
        results.append(result)
    report = {'schema_version': 'stage1d-etherscan-capability-result-v1', 'plan_sha256': budgeted.plan_id,
        'query_id': plan['query_id'], 'scope_hash': plan['scope_hash'], 'results': results,
        'status': 'TWO_ENDPOINT_PAGES_VALIDATED' if all(r['endpoint_status'] == 'CURRENT_ENDPOINT_RETURNED_VALID_PAGE' for r in results) else 'PARTIAL_CAPABILITY_EVIDENCE',
        'no_global_permission_inference': True, 'no_canonical_internal_events_admitted': True,
        'snapshot': budgeted.ledger.snapshot()}
    return report


def run_probe(work, plan_path):
    from stage1d_runtime import Runtime
    work = Path(work).resolve(); plan = verify_plan(work, read(inside(work, plan_path))); runtime = Runtime()
    with runtime.session(work, plan['query_name'], 'necessary_etherscan_account_capability') as timing:
        return probe_in_session(work, plan, runtime, timing['deadline'])
