"""R3 context-only access: inherited budget, durable clocks, one network writer.

No provider is contacted by import or migration. Credentials exist only in the
HTTP request. Existing R2 financial schema and page guards are carried forward;
new R3 journal identities distinguish the revision without another grant.
"""
import argparse
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import urllib.error
import urllib.request
import uuid

from budget_r2 import RevisionLedger, AUTH
from budget_r1 import consistent_backup
from dune_r2 import RevisionLive, TERMINAL
from network import NoRedirect
from page_attempts import AttemptStore, atomic_json
from legacy_guard_r4 import reject_legacy_workspace

R3_AUTH = 'STAGE1B_R3_CONTEXT_FIRST_V1'
PROBES = ('atomic_simple_transfer', 'harmony_high_branch')
RAW_CAP = 536870912
RPC_MAX = 8 * 1024 * 1024
DUNE_MAX = 16 * 1024 * 1024
CLOCK_CAP = Decimal(3600)


def now():
    return datetime.now(timezone.utc).isoformat()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def identity(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def new_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as handle:
        handle.write(canonical(value))


def db_path(work):
    return Path(work) / 'private/shared_budget_r3.sqlite'


def readonly_snapshot(path):
    # Bypass the constructor: even CREATE TABLE IF NOT EXISTS is a write API.
    ledger = RevisionLedger.__new__(RevisionLedger)
    ledger.path = str(path)
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)) as db:
        db.execute('BEGIN')
        return ledger.snapshot(db)


def migrate(work, baseline):
    reject_legacy_workspace(work, 'context_access_r3.migrate')
    work, baseline = Path(work).resolve(), Path(baseline).resolve()
    if work == baseline or work.parent != baseline.parent:
        raise ValueError('A distinct sibling revision workspace is required')
    policy = read(work / 'configs/STAGE1B_R3_POLICY.json')
    if policy.get('authorization_id') != R3_AUTH or policy['dune'].get('authorization_id') != AUTH:
        raise ValueError('R3 continuation authority and inherited Dune authority required')
    state = read(baseline / 'RUN_STATE.json')
    if state.get('checkpoint') != 'CHECKPOINT_1B_R2_REACHED' or state.get('new_network_submissions_allowed') is not False:
        raise RuntimeError('R2 must be stopped at its checkpoint')
    if (baseline / 'private/network_worker.lock').exists():
        raise RuntimeError('An old writer lock remains')
    private = work / 'private'
    private.mkdir(parents=True, exist_ok=True)
    receipt_path = private / 'BUDGET_MIGRATION_R3.json'
    if receipt_path.exists():
        receipt = read(receipt_path)
        if receipt['baseline_run_id'] != baseline.name or not db_path(work).exists():
            raise RuntimeError('Migration receipt disagrees with workspace')
        return receipt
    for sibling in work.parent.glob('*/private/shared_budget_r3.sqlite'):
        if sibling.resolve() != db_path(work):
            raise RuntimeError('Another R3 budget writer exists; resume it rather than duplicate grants')
    target = db_path(work)
    if target.exists():
        raise RuntimeError('Unfinished migration requires inspection; no overwrite')
    snapshots = private / 'migration_snapshots'
    snapshots.mkdir(exist_ok=True)
    sources = {'ledger': baseline / 'private/shared_budget_r2.sqlite',
               'attempts': baseline / 'private/dune_request_attempts.sqlite'}
    before = readonly_snapshot(sources['ledger'])
    records = {name: consistent_backup(source, snapshots / (name + '.sqlite'))
               for name, source in sources.items()}
    consistent_backup(snapshots / 'ledger.sqlite', target)
    consistent_backup(snapshots / 'attempts.sqlite', private / 'dune_request_attempts.sqlite')
    with closing(sqlite3.connect(snapshots / 'ledger.sqlite')) as old, closing(sqlite3.connect(target)) as db:
        tables = [row[0] for row in old.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        equality = {}
        for table in tables:
            a = old.execute('SELECT * FROM ' + table + ' ORDER BY rowid').fetchall()
            b = db.execute('SELECT * FROM ' + table + ' ORDER BY rowid').fetchall()
            equality[table] = {'rows': len(a), 'identical': a == b}
            if a != b:
                raise RuntimeError('Inherited financial table differs')
        db.execute('CREATE TABLE r3_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        db.execute('CREATE TABLE r3_journal(n INTEGER PRIMARY KEY,kind TEXT,payload TEXT,utc TEXT)')
        db.execute('CREATE TABLE r3_rpc_requests(identity TEXT PRIMARY KEY,job TEXT,provider TEXT,plan TEXT,capability INTEGER,status TEXT,receipt TEXT)')
        for key, value in {'authorization_id': R3_AUTH, 'inherited_dune_authorization_id': AUTH,
                           'baseline_run_id': baseline.name, 'source_ledger_sha256': records['ledger']['source_sha256_before']}.items():
            db.execute('INSERT INTO r3_meta VALUES(?,?)', (key, value))
        db.execute('INSERT INTO r3_journal(kind,payload,utc) VALUES(?,?,?)',
                   ('CONTINUATION_NOT_NEW_GRANT', json.dumps({'cap': '100', 'source_risk': before['dune_credits']['cumulative_risk']}), now()))
        db.commit()
    for name in ('dune_user_confirmation.json', 'dune_rate_evidence.json', 'current_dune_usage.json'):
        destination = private / name
        if destination.exists():
            raise RuntimeError('Pre-existing copied configuration needs inspection')
        destination.write_bytes((baseline / 'private' / name).read_bytes())
    status_path = baseline.parents[3] / state['delivery_directory'] / '02_FINAL_STATUS.json'
    baseline_status = read(status_path)
    resource = {'schema_version': 'r3-inherited-resource-v1', 'source_run_id': baseline.name,
                'source_status_sha256': sha(status_path),
                'inherited_candidate_online_seconds': baseline_status['online_seconds_cumulative_upper'],
                'inherited_raw_risk_bytes': baseline_status['raw_resource_summary']['cumulative_conservative_raw_occupancy_bytes'],
                'context_seconds_at_start': {p: '0' for p in PROBES},
                'context_clock_is_new_explicit_workstream': True}
    new_json(private / 'INHERITED_RESOURCE_R3.json', resource)
    after = readonly_snapshot(target)
    if before != after:
        raise RuntimeError('Migration altered cumulative financial state')
    receipt = {'schema_version': 'stage1b-r3-budget-migration-v1', 'authorization_id': R3_AUTH,
               'inherited_authorization_id': AUTH, 'baseline_run_id': baseline.name, 'run_id': work.name,
               'created_at_utc': now(), 'source_backups': records, 'historical_tables': equality,
               'initial_snapshot': after, 'source_unchanged': all(sha(sources[k]) == v['source_sha256_before'] for k, v in records.items()),
               'new_allowance_granted': False, 'single_writer': 'private/shared_budget_r3.sqlite',
               'inherited_resource_sha256': sha(private / 'INHERITED_RESOURCE_R3.json')}
    new_json(receipt_path, receipt)
    return receipt


def require_gate(work):
    reject_legacy_workspace(work, 'context_access_r3.require_gate/session')
    work = Path(work)
    gate = read(work / 'CONTINUATION_GATE_R3.json')
    if gate.get('status') != 'PASS' or gate.get('run_id') != work.name:
        raise RuntimeError('R3 context continuation gate is missing')
    hashes = gate.get('source_sha256', {})
    if not hashes or 'context_access_r3.py' not in hashes:
        raise RuntimeError('R3 gate must bind actual access source')
    for name, expected in hashes.items():
        if sha(work / 'src' / name) != expected:
            raise RuntimeError('Gate-bound source changed')


def raw_risk(work):
    work = Path(work)
    used = int(read(work / 'private/INHERITED_RESOURCE_R3.json')['inherited_raw_risk_bytes'])
    for path in (work / 'raw').rglob('*'):
        if path.is_file():
            used += path.stat().st_size
    for path in (work / 'private/context_uncertainty').glob('*.json'):
        used += int(read(path)['additional_raw_risk_bytes'])
    return used


def clock_usage(work, probe):
    if probe not in PROBES:
        raise ValueError('Unknown fixed query')
    used = Decimal(0)
    for path in (Path(work) / 'private/context_sessions').glob('*.json'):
        record = read(path)
        if probe not in record['probes']:
            continue
        if not record.get('closed'):
            raise RuntimeError('Unresolved context clock; no restart reset')
        used += Decimal(str(record['elapsed_seconds']))
    return used


@contextmanager
def session(work, probe, label):
    work = Path(work).resolve()
    require_gate(work)
    probes = list(PROBES) if probe == 'SHARED' else [probe]
    remaining = min(CLOCK_CAP - clock_usage(work, p) for p in probes)
    if remaining < 35:
        raise RuntimeError('Context time limit exhausted')
    if not re.fullmatch('[a-zA-Z0-9_-]{1,100}', label):
        raise ValueError('Unsafe work-unit label')
    record_path = work / 'private/context_sessions' / (label + '.json')
    if record_path.exists():
        raise RuntimeError('Work-unit clock already exists; use an explicit resume identity')
    lock = work / 'private/network_worker.lock'
    new_json(lock, {'run_id': work.name, 'pid': os.getpid(), 'label': label, 'started_at_utc': now()})
    started = time.monotonic()
    record = {'label': label, 'probes': probes, 'started_at_utc': now(), 'closed': False,
              'remaining_before_seconds': str(remaining), 'elapsed_seconds': None,
              'inherited_candidate_clock_unchanged': True}
    atomic_json(record_path, record)
    try:
        yield {'remaining': remaining, 'started': started, 'record': record}
    finally:
        record.update(closed=True, elapsed_seconds=round(time.monotonic() - started, 6), ended_at_utc=now())
        atomic_json(record_path, record)
        lock.unlink()


class ContextDune(RevisionLive):
    def __init__(self, work):
        reject_legacy_workspace(work, 'context_access_r3.ContextDune')
        self.w = Path(work).resolve()
        if not db_path(self.w).exists():
            raise RuntimeError('R3 migration required before access')
        self.db = RevisionLedger(db_path(self.w))
        self.confirm = read(self.w / 'private/dune_user_confirmation.json')
        if self.confirm.get('status') != 'USER_CONFIRMED' or self.confirm.get('authorization_id') != AUTH or str(self.confirm.get('execution_cap_credits')) != '20':
            raise RuntimeError('Inherited account cap20 confirmation missing')
        if self.confirm.get('payment_method_added') is not False or self.confirm.get('extra_credits_enabled') is not False:
            raise RuntimeError('Payment changes are not authorized')
        self.account_context_ref = self.confirm['account_context_ref']
        self.attempts = AttemptStore(self.w / 'private/dune_request_attempts.sqlite')

    def require_gate(self):
        require_gate(self.w)

    def call(self, op, execution=None, payload=None, params=None):
        require_gate(self.w)
        if not (self.w / 'private/network_worker.lock').exists():
            raise RuntimeError('Use the R3 clocked single-writer entry point')
        if raw_risk(self.w) + DUNE_MAX > RAW_CAP:
            raise RuntimeError('Cumulative raw risk cap')
        body, receipt = super().call(op, execution, payload, params)
        if receipt.get('error_class'):
            extra = max(0, DUNE_MAX - int(receipt.get('raw_bytes', 0)))
            new_json(self.w / 'private/context_uncertainty' / (receipt['request_id'] + '.json'),
                     {'additional_raw_risk_bytes': extra, 'basis': 'Bounded response read with unresolved prefix', 'receipt': receipt['request_id']})
        return body, receipt

    def submit(self, sqlpath, label, kind='context', freeze_manifest=None, performance='medium'):
        if kind != 'context':
            raise ValueError('R3 context entry point cannot start candidate or label campaigns')
        result = super().submit(sqlpath, label, kind, freeze_manifest, performance)
        with self.db.connection() as db:
            db.execute('INSERT INTO r3_journal(kind,payload,utc) VALUES(?,?,?)',
                       ('DUNE_CONTEXT_JOB', json.dumps({'label': label, 'job_folder': str(Path(result['job_folder']).relative_to(self.w)), 'run_id': self.w.name}), now()))
        return result


def dune_usage(work, label='startup_usage'):
    reject_legacy_workspace(work, 'context_access_r3.dune_usage')
    with session(work, 'SHARED', label):
        return ContextDune(work).usage()


def execute_dune(work, probe, freeze, label, resume=False):
    reject_legacy_workspace(work, 'context_access_r3.execute_dune')
    work, freeze = Path(work).resolve(), Path(freeze).resolve()
    if not freeze.is_relative_to(work):
        raise ValueError('Freeze must be inside R3 workspace')
    frozen = read(freeze)
    if frozen.get('r3_context_scope') is not True:
        raise ValueError('Explicit finite R3 context freeze required')
    with session(work, probe, label) as timing:
        live = ContextDune(work)
        if resume:
            folder = work / 'private/dune_r2_jobs' / frozen['sql_sha256']
            state = read(folder / 'job.json')
            if state.get('sql_sha256') != sha(freeze.parent / 'query.sql') or not state.get('execution_id'):
                raise RuntimeError('No verified saved execution for resume')
        else:
            result = live.submit(freeze.parent / 'query.sql', label, freeze_manifest=freeze)
            folder = Path(result['job_folder'])
            if not result.get('execution_id'):
                raise RuntimeError('Submission uncertain; no automatic retry')
        timing['record']['job_folder'] = str(folder.relative_to(work))
        while True:
            state = read(folder / 'job.json')
            if state['state'] in TERMINAL:
                break
            if time.monotonic() - timing['started'] + 35 > float(timing['remaining']):
                raise RuntimeError('Context clock exhausted; execution retained')
            time.sleep(5)
            live.poll(folder)
        if state['state'] != 'QUERY_STATE_COMPLETED':
            if state.get('execution_cost_credits') is not None:
                live.settle(folder)
            return {'status': state['state'], 'job_folder': str(folder)}
        progress = live.export_progress(folder, state)
        while not progress['complete']:
            if time.monotonic() - timing['started'] + 35 > float(timing['remaining']):
                raise RuntimeError('Context clock exhausted before export; metadata retained')
            result = live.export(folder, limit=1000, offset=progress['next_offset'])
            progress = result['progress']
        # The inherited initial progress for an empty set is not complete until
        # the server's zero-row page is verified, as enforced by page_contract.
        return {'status': 'COMPLETED_EXPORTED', 'job_folder': str(folder), 'settlement': live.settle(folder)}


def validate_rpc(plan):
    if not isinstance(plan, dict) or set(plan) != {'method', 'params'}:
        raise ValueError('Exact RPC method/params required')
    method, params = plan['method'], plan['params']
    if not isinstance(params, list):
        raise ValueError('RPC params must be a list')
    def hx(value, size):
        return isinstance(value, str) and re.fullmatch('0x[0-9a-fA-F]{' + str(size) + '}', value)
    def block(value):
        if isinstance(value, str):
            return bool(re.fullmatch(r'0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)', value))
        return isinstance(value, dict) and set(value) == {'blockHash', 'requireCanonical'} and hx(value['blockHash'], 64) and value['requireCanonical'] is True
    good = False
    if method == 'eth_chainId':
        good = params == []
    elif method in ('eth_getBalance', 'eth_getCode'):
        good = len(params) == 2 and hx(params[0], 40) and block(params[1])
    elif method in ('eth_getBlockByNumber', 'eth_getBlockByHash'):
        good = len(params) == 2 and params[1] is False and (block(params[0]) and isinstance(params[0], str) if method.endswith('Number') else hx(params[0], 64))
    elif method in ('eth_getTransactionReceipt', 'eth_getTransactionByHash'):
        good = len(params) == 1 and hx(params[0], 64)
    elif method == 'debug_traceTransaction':
        good = len(params) == 2 and hx(params[0], 64) and params[1] == {'tracer': 'callTracer', 'tracerConfig': {'withLog': True}}
    if not good:
        raise ValueError('Unsupported or unfixed historical RPC request')
    return json.loads(json.dumps(plan))


def rpc_result_status(request, response):
    if not isinstance(response, dict) or response.get('jsonrpc') != '2.0' or response.get('id') != request['id'] or type(response.get('id')) is not type(request['id']) or ('result' in response) == ('error' in response):
        return 'INVALID_RPC_BINDING'
    if 'error' in response:
        return 'RPC_ERROR'
    result, method = response['result'], request['method']
    if result is None:
        return 'NULL_RESULT_NOT_EVIDENCE'
    if method == 'eth_chainId':
        return 'SUCCESS_VALIDATED' if result == '0x1' else 'WRONG_CHAIN'
    if method in ('eth_getBalance', 'eth_getCode'):
        pattern = r'0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)' if method == 'eth_getBalance' else r'0x(?:[0-9a-fA-F]{2})*'
        return 'SUCCESS_VALIDATED' if isinstance(result, str) and re.fullmatch(pattern, result) else 'INVALID_RPC_RESULT'
    if not isinstance(result, dict):
        return 'INVALID_RPC_RESULT'
    if method.startswith('eth_getBlock'):
        key = 'number' if method.endswith('Number') else 'hash'
        if str(result.get(key, '')).lower() != request['params'][0].lower() or not re.fullmatch('0x[0-9a-fA-F]{64}', str(result.get('hash', ''))):
            return 'INVALID_RPC_BINDING'
    elif method in ('eth_getTransactionReceipt', 'eth_getTransactionByHash'):
        key = 'transactionHash' if method == 'eth_getTransactionReceipt' else 'hash'
        if str(result.get(key, '')).lower() != request['params'][0].lower():
            return 'INVALID_RPC_BINDING'
    return 'SUCCESS_VALIDATED'


class RpcAccess:
    def __init__(self, work, transport=None):
        reject_legacy_workspace(work, 'context_access_r3.RpcAccess')
        self.w = Path(work).resolve()
        if not db_path(self.w).exists():
            raise RuntimeError('R3 migration required')
        self.ledger = RevisionLedger(db_path(self.w))
        self.transport = transport
        with self.ledger.connection() as db:
            db.execute('CREATE TABLE IF NOT EXISTS r3_rpc_retries(original_identity TEXT PRIMARY KEY,retry_identity TEXT UNIQUE,reason TEXT,utc TEXT)')

    def permission(self):
        permission = read(self.w / 'private/alchemy_permission_r3.json')
        if permission.get('status') not in ('USER_CONFIRMED', 'INDEPENDENTLY_VERIFIED') or permission.get('paid_overage_authorized') is not False or permission.get('existing_account_authorized') is not True or not permission.get('evidence'):
            raise RuntimeError('Existing Alchemy permission, included allowance and no extra-spend authority required')
        remaining = permission.get('included_compute_units_remaining')
        if isinstance(remaining, bool) or not isinstance(remaining, int) or remaining < 1:
            raise RuntimeError('Current included compute-unit allowance required')
        if not permission.get('method_cu_upper_bounds') or not permission.get('rate_evidence'):
            raise RuntimeError('Applicable method CU upper bounds required')
        # Confirmation is applied once, using observed *remaining*, not a fresh
        # stage grant. Repeated calls cannot refill the source allowance.
        fingerprint = identity(permission)
        with self.ledger.connection() as db:
            previous = db.execute("SELECT value FROM r3_meta WHERE key='alchemy_permission_sha256'").fetchone()
            if previous and previous[0] != fingerprint:
                raise RuntimeError('Changed provider evidence needs explicit reconciliation')
            if not previous:
                snapshot = self.ledger.snapshot(db)['alchemy_cu']
                total = Decimal(snapshot['actual']) + Decimal(snapshot['reserved']) + remaining
                db.execute("UPDATE limits SET available=?,evidence=? WHERE unit='alchemy_cu'", (str(total), 'R3 current included allowance; evidence ' + fingerprint))
                db.execute('INSERT INTO r3_meta VALUES(?,?)', ('alchemy_permission_sha256', fingerprint))
        return permission

    def call_batch(self, plans, probe, label, capability=False, retry_of=None, retry_reason=None):
        if not isinstance(plans, list) or not 1 <= len(plans) <= 50:
            raise ValueError('Finite 1..50 individual RPC operations required')
        plans = [validate_rpc(plan) for plan in plans]
        provider = 'ALCHEMY_ETH_MAINNET_EXISTING' if self.transport is None else 'SYNTHETIC_TRANSPORT'
        identities = [identity({'provider': provider, 'chain': 1, 'plan': plan}) for plan in plans]
        if len(set(identities)) != len(identities):
            raise ValueError('Duplicate RPC operation in batch')
        if retry_of is not None:
            if (not isinstance(retry_of, list) or len(retry_of) != len(plans) or len(plans) > 5
                    or len(set(retry_of)) != len(retry_of) or not retry_reason
                    or not all(isinstance(key, str) and re.fullmatch('[0-9a-f]{64}', key) for key in retry_of)):
                raise ValueError('An explicit one-time timeout retry needs 1..5 exact original identities and a reason')
            identities = [identity({'provider': provider, 'chain': 1, 'plan': plan,
                                    'retry_of': old, 'retry_number': 1}) for old, plan in zip(retry_of, plans)]
        elif retry_reason is not None:
            raise ValueError('Retry reason without an original attempted request')
        permission = self.permission()
        rates = permission['method_cu_upper_bounds']
        if any(type(rates.get(p['method'])) is not int or rates[p['method']] < 0 for p in plans):
            raise ValueError('Every RPC operation requires a nonnegative integer CU bound')
        cu = sum(rates[plan['method']] for plan in plans)
        if self.transport is None and not os.environ.get('ALCHEMY_API_KEY'):
            raise RuntimeError('Configured Alchemy credential absent')
        if raw_risk(self.w) + RPC_MAX + 1 > RAW_CAP:
            raise RuntimeError('Cumulative raw risk cap')
        job = 'rpc_r3_' + identity(identities)[:24]
        requests = [{'jsonrpc': '2.0', 'id': job + '_' + str(i), **plan} for i, plan in enumerate(plans)]
        directory = self.w / 'raw/rpc_r3' / job
        with session(self.w, probe, label):
            with self.ledger.connection() as db:
                db.execute('BEGIN IMMEDIATE')
                if db.execute("SELECT 1 FROM r3_rpc_requests WHERE status='DISPATCH_INTENT'").fetchone():
                    raise RuntimeError('Unresolved prior RPC dispatch; no automatic retry')
                used = db.execute('SELECT count(*) FROM r3_rpc_requests WHERE capability=1 AND provider=?', (provider,)).fetchone()[0]
                if capability and used + len(plans) > 5:
                    raise RuntimeError('Selected provider capability limit of five operations')
                if not capability:
                    proved = db.execute("SELECT plan FROM r3_rpc_requests WHERE capability=1 AND status='SUCCESS_VALIDATED' AND provider=?", (provider,)).fetchall()
                    methods = {json.loads(row[0])['method'] for row in proved}
                    if not {'eth_chainId', 'eth_getBalance'}.issubset(methods):
                        raise RuntimeError('Mainnet and historical-state capability not yet demonstrated')
                if retry_of is not None:
                    for old, plan in zip(retry_of, plans):
                        row = db.execute('SELECT provider,plan,status,receipt FROM r3_rpc_requests WHERE identity=?', (old,)).fetchone()
                        if not row or row[0] != provider or json.loads(row[1]) != plan or row[2] != 'TRANSPORT_ERROR_NO_RETRY':
                            raise RuntimeError('Retry requires the exact original failed provider request; successful data must be reused')
                        if db.execute('SELECT 1 FROM r3_rpc_retries WHERE original_identity=? OR retry_identity=?', (old, old)).fetchone():
                            raise RuntimeError('Only one documented optimized retry per original timeout')
                        receipt_path = (self.w / row[3]).resolve()
                        if not receipt_path.is_relative_to(self.w):
                            raise RuntimeError('Timeout receipt path escape')
                        prior = read(receipt_path)
                        if prior.get('error_class') not in ('TimeoutError', 'IncompleteRead') or prior.get('raw_bytes') != 0 or prior.get('http_status') is not None:
                            raise RuntimeError('Only a documented timeout or interrupted response with zero persisted body is eligible for this bounded read-only retry')
                for key in identities:
                    if db.execute('SELECT 1 FROM r3_rpc_requests WHERE identity=?', (key,)).fetchone():
                        raise RuntimeError('Logical RPC already attempted; reuse saved evidence')
            self.ledger.reserve(job, 'alchemy', 'Finite R3 historical context ' + label, {'rpc_operations': len(plans), 'alchemy_cu': cu})
            with self.ledger.connection() as db:
                for key, plan in zip(identities, plans):
                    db.execute('INSERT INTO r3_rpc_requests VALUES(?,?,?,?,?,?,?)',
                               (key, job, provider, json.dumps(plan), int(capability), 'DISPATCH_INTENT', None))
                if retry_of is not None:
                    for original, new in zip(retry_of, identities):
                        db.execute('INSERT INTO r3_rpc_retries VALUES(?,?,?,?)', (original, new, retry_reason, now()))
            directory.mkdir(parents=True, exist_ok=False)
            new_json(directory / 'dispatch_intent.json', {'requests': requests, 'provider_alias': 'ALCHEMY_ETH_MAINNET_EXISTING',
                     'rpc_operations': len(plans), 'cu_upper_bound_not_actual': cu, 'permission_sha256': identity(permission), 'utc': now(),
                     'retry_of': retry_of, 'retry_reason': retry_reason, 'old_attempt_risk_released': False})
            started, status, body, error = time.monotonic(), None, b'', None
            try:
                if self.transport is None:
                    # Never persist, hash, print, or follow redirects of this URL.
                    url = 'https://eth-mainnet.g.alchemy.com/v2/' + os.environ['ALCHEMY_API_KEY']
                    request = urllib.request.Request(url, data=canonical(requests), headers={'Content-Type': 'application/json'}, method='POST')
                    try:
                        with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
                            status, body = response.status, response.read(RPC_MAX + 1)
                    except urllib.error.HTTPError as exc:
                        with exc:
                            status, body = exc.code, exc.read(RPC_MAX + 1)
                else:
                    status, body = self.transport(requests, RPC_MAX + 1)
            except Exception as exc:
                error = type(exc).__name__
            if not isinstance(body, bytes):
                body, error = b'', 'NON_BYTES_RESPONSE'
            if any(value.encode() in body for key, value in os.environ.items() if any(part in key.upper() for part in ('API_KEY', 'TOKEN', 'SECRET')) and len(value) >= 12):
                body, error = b'', 'CREDENTIAL_ECHO_WITHHELD'
            overflow = len(body) > RPC_MAX
            if overflow:
                body, error = body[:RPC_MAX], 'RESPONSE_TOO_LARGE'
            (directory / 'response_body.bin').write_bytes(body)
            try:
                decoded = json.loads(body) if not error else None
            except (ValueError, UnicodeError):
                decoded = None
            common = 'TRANSPORT_ERROR_NO_RETRY' if error else 'HTTP_ERROR' if status != 200 else 'INVALID_JSON_RPC_BATCH' if not isinstance(decoded, list) or len(decoded) != len(requests) else None
            members = []
            for index, request in enumerate(requests):
                matches = [row for row in decoded if isinstance(row, dict) and row.get('id') == request['id']] if isinstance(decoded, list) else []
                response = matches[0] if len(matches) == 1 else None
                state = common or rpc_result_status(request, response)
                envelope = {'provider_alias': 'ALCHEMY_ETH_MAINNET_EXISTING', 'evidence_kind': 'REAL_CHAIN' if self.transport is None else 'SYNTHETIC_TRANSPORT',
                            'http_status': status, 'request': request, 'response': response, 'response_complete': not error,
                            'raw_body_sha256': hashlib.sha256(body).hexdigest(), 'status': state}
                path = directory / ('envelope_' + str(index) + '.json')
                new_json(path, envelope)
                members.append({'identity': identities[index], 'method': request['method'], 'status': state,
                                'artifact_path': path.relative_to(self.w).as_posix(), 'artifact_sha256': sha(path)})
            self.ledger.settle(job, {'rpc_operations': len(plans), 'alchemy_cu': None})
            receipt = {'schema_version': 'stage1b-r3-rpc-receipt-v1', 'job': job, 'utc': now(),
                       'provider_alias': 'ALCHEMY_ETH_MAINNET_EXISTING', 'http_status': status, 'error_class': error,
                       'elapsed_seconds': round(time.monotonic() - started, 6), 'members': members,
                       'rpc_operations_actual': len(plans), 'cu_upper_bound_not_actual': cu, 'cu_actual': None,
                       'raw_path': (directory / 'response_body.bin').relative_to(self.w).as_posix(), 'raw_bytes': len(body),
                       'raw_sha256': hashlib.sha256(body).hexdigest(), 'no_automatic_retry': True,
                       'retry_of': retry_of, 'retry_reason': retry_reason, 'old_attempt_risk_released': False,
                       'evidence_kind': 'REAL_CHAIN' if self.transport is None else 'SYNTHETIC_TRANSPORT'}
            receipt_path = directory / 'receipt.json'
            new_json(receipt_path, receipt)
            if error:
                new_json(self.w / 'private/context_uncertainty' / (job + '.json'),
                         {'additional_raw_risk_bytes': max(0, RPC_MAX + 1 - len(body)), 'basis': error})
            with self.ledger.connection() as db:
                for member in members:
                    db.execute('UPDATE r3_rpc_requests SET status=?,receipt=? WHERE identity=?',
                               (member['status'], receipt_path.relative_to(self.w).as_posix(), member['identity']))
            return receipt


def ledger_snapshot(work):
    return readonly_snapshot(db_path(work))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('migrate', 'snapshot', 'usage', 'dune', 'rpc'))
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--baseline', type=Path)
    parser.add_argument('--probe', default='SHARED')
    parser.add_argument('--label', default='startup_usage')
    parser.add_argument('--freeze', type=Path)
    parser.add_argument('--plans', type=Path)
    parser.add_argument('--capability', action='store_true')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--retry-of', type=Path, help='JSON list of explicitly selected failed RPC identities')
    parser.add_argument('--retry-reason', help='One documented reason for a split timeout retry')
    args = parser.parse_args()
    # R3 APIs retain their historical behavior; the active R4 CLI cannot fall
    # back to the superseded permanent-error cache/no-retry transport.
    r4_cli = (args.work / 'configs/STAGE1B_R4_POLICY.json').exists()
    if r4_cli:
        from context_access_r4 import migrate, RpcAccess, db_path, readonly_snapshot
        from dune_r4 import execute_dune, dune_usage
        ledger_snapshot = lambda work: readonly_snapshot(db_path(work))
    if args.action == 'migrate': result = migrate(args.work, args.baseline)
    elif args.action == 'snapshot': result = ledger_snapshot(args.work)
    elif args.action == 'usage': result = dune_usage(args.work, args.label)
    elif args.action == 'dune': result = execute_dune(args.work, args.probe, args.freeze, args.label, args.resume)
    elif r4_cli:
        if args.retry_of or args.retry_reason:
            raise ValueError('R4 recovers the exact persisted read identity; R3 one-off retry selectors cannot grant attempts')
        result = RpcAccess(args.work).call_batch(read(args.plans), args.probe, args.label, args.capability)
    else: result = RpcAccess(args.work).call_batch(read(args.plans), args.probe, args.label, args.capability,
                    read(args.retry_of) if args.retry_of else None, args.retry_reason)
    print(json.dumps(result, indent=2, default=str))
    if r4_cli:
        complete = (args.action in ('migrate', 'snapshot')
                    or args.action == 'usage' and result.get('http_status') == 200
                    or args.action == 'dune' and result.get('status') == 'COMPLETED_EXPORTED'
                    or args.action == 'rpc' and result.get('status') == 'COMPLETE')
        raise SystemExit(0 if complete else 1)
