"""R4 persistent, costed, bounded read access. Importing performs no I/O.

The financial ledger is an exact continuation of R3. Request identity excludes
run and batch names; every dispatch has a separate immutable cost reservation.
"""
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import sqlite3
import time
import urllib.error
import urllib.request
import uuid

from budget_r2 import AUTH, RevisionLedger
from budget_r1 import consistent_backup
from context_access_r3 import (PROBES, RAW_CAP, RPC_MAX, CLOCK_CAP, RpcAccess as R3RpcAccess,
    canonical, identity, new_json, now, read, readonly_snapshot, rpc_result_status, sha, validate_rpc)
from context_access_r3 import raw_risk as old_raw_risk, clock_usage as old_clock_usage
from network import NoRedirect
from page_attempts import atomic_json
from read_retry_r4 import ReadRetryStore, classify_failure, logical_key

R4_AUTH = 'STAGE1B_R4_ALL_FIXES_AND_READ_RETRY_V1'

def db_path(work): return Path(work) / 'private/shared_budget_r4.sqlite'
def retry_path(work): return Path(work) / 'private/read_retry_r4.sqlite'
def rpc_identity(provider, plan): return {'provider': provider, 'chain': 1, **validate_rpc(plan)}

def require_gate(work):
    work = Path(work).resolve()
    gate = read(work / 'REPAIR_GATE_R4.json')
    if gate.get('status') != 'PASS' or gate.get('run_id') != work.name:
        raise RuntimeError('R4 repair gate required before live requests')
    if set(gate.get('closed_findings', [])) != {'F%02d' % n for n in range(1, 10)} or gate.get('E01') != 'PASS':
        raise RuntimeError('All nine repairs and bounded read recovery are required')
    hashes = gate.get('source_sha256', {})
    if not {'context_access_r4.py', 'read_retry_r4.py'}.issubset(hashes):
        raise RuntimeError('Gate must bind active access code')
    for name, expected in hashes.items():
        path = (work / 'src' / name).resolve()
        if not path.is_relative_to(work / 'src') or sha(path) != expected:
            raise RuntimeError('Gate-bound source changed')

def raw_risk(work):
    work = Path(work)
    used = int(read(work / 'private/INHERITED_RESOURCE_R4.json')['inherited_raw_risk_bytes'])
    used += sum(p.stat().st_size for p in (work / 'raw').rglob('*') if p.is_file())
    used += sum(int(read(p)['additional_raw_risk_bytes']) for p in (work / 'private/context_uncertainty').glob('*.json'))
    return used

def clock_usage(work, probe):
    if probe not in PROBES: raise ValueError('Unknown fixed query')
    used = Decimal(read(Path(work) / 'private/INHERITED_RESOURCE_R4.json')['context_seconds_at_start'][probe])
    for path in (Path(work) / 'private/context_sessions_r4').glob('*.json'):
        row = read(path)
        if probe in row['probes']:
            if not row.get('closed'): raise RuntimeError('Unresolved context clock; restart cannot reset it')
            used += Decimal(str(row['elapsed_seconds']))
    return used

@contextmanager
def session(work, probe, label, *, clock=time.time, monotonic=time.monotonic, synthetic=False):
    work = Path(work).resolve()
    if not synthetic: require_gate(work)
    probes = list(PROBES) if probe == 'SHARED' else [probe]
    remaining = min(CLOCK_CAP - clock_usage(work, p) for p in probes)
    if remaining <= 0: raise RuntimeError('Cumulative context clock exhausted')
    if not isinstance(label, str) or not label or len(label) > 100: raise ValueError('Work label required')
    lock = work / 'private/network_worker.lock'
    new_json(lock, {'run_id': work.name, 'pid': os.getpid(), 'label': label, 'started_at_utc': now()})
    started = monotonic()
    path = work / 'private/context_sessions_r4' / (uuid.uuid4().hex + '.json')
    record = {'label': label, 'probes': probes, 'closed': False, 'elapsed_seconds': None,
              'remaining_before_seconds': str(remaining), 'started_at_utc': now(), 'synthetic': synthetic}
    atomic_json(path, record)
    try:
        yield {'remaining': remaining, 'started': started, 'deadline': clock() + float(remaining), 'record': record}
    finally:
        record.update(closed=True, elapsed_seconds=round(monotonic() - started, 6), ended_at_utc=now())
        atomic_json(path, record)
        lock.unlink()

def _import_rpc(work, baseline, store):
    """Keep every old attempt and cost; cache only validated semantic results."""
    source = baseline / 'private/shared_budget_r3.sqlite'
    with closing(sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        requests = [dict(r) for r in db.execute('SELECT * FROM r3_rpc_requests ORDER BY rowid')]
        retries = {r['retry_identity']: r['original_identity'] for r in db.execute('SELECT * FROM r3_rpc_retries')}
    rows = []
    for row in requests:
        if row['provider'] not in {'ALCHEMY_ETH_MAINNET_EXISTING', 'SYNTHETIC_TRANSPORT'}:
            raise RuntimeError('Unrecognized historical RPC provider')
        plan = json.loads(row['plan']); key = rpc_identity(row['provider'], plan)
        legacy_selector = {'provider': row['provider'], 'chain': 1, 'plan': plan}
        if row['identity'] in retries:
            legacy_selector.update(retry_of=retries[row['identity']], retry_number=1)
        if identity(legacy_selector) != row['identity']:
            raise RuntimeError('Old RPC logical identity and stored plan disagree')
        receipt, payload, artifact = {}, None, None
        if row['receipt']:
            old_path = (baseline / row['receipt']).resolve()
            if not old_path.is_relative_to(baseline): raise ValueError('Old RPC receipt escapes known baseline')
            receipt = read(old_path)
            source_dir = old_path.parent
            intent = read(source_dir / 'dispatch_intent.json')
            if receipt.get('job') != row['job'] or source_dir.name != row['job']:
                raise RuntimeError('Old RPC job/receipt identity differs')
            target_dir = work / 'private/inherited_rpc_r3' / source_dir.name
            if not target_dir.exists(): shutil.copytree(source_dir, target_dir)
            members = receipt.get('members', [])
            member = next((m for m in members if m.get('identity') == row['identity']), None)
            if member:
                old_env = (baseline / member['artifact_path']).resolve()
                if not old_env.is_relative_to(baseline) or sha(old_env) != member['artifact_sha256']:
                    raise RuntimeError('Old RPC member provenance differs')
                artifact = target_dir / old_env.name
                if sha(artifact) != member['artifact_sha256']:
                    raise RuntimeError('Copied historical RPC envelope bytes differ')
                env = read(artifact)
                env_request = env.get('request', {})
                if ({k: env_request.get(k) for k in ('method', 'params')} != plan
                    or env_request not in intent.get('requests', [])
                    or member.get('method') != plan['method'] or member.get('status') != row['status']
                    or env.get('status') != row['status']
                    or env.get('evidence_kind') != ('SYNTHETIC_TRANSPORT' if row['provider'] == 'SYNTHETIC_TRANSPORT' else 'REAL_CHAIN')
                    or env.get('provider_alias') != 'ALCHEMY_ETH_MAINNET_EXISTING'
                    or env.get('raw_body_sha256') != receipt.get('raw_sha256')):
                    raise RuntimeError('Old RPC plan/member/envelope binding disagrees')
                if row['status'] == 'SUCCESS_VALIDATED':
                    if rpc_result_status(env['request'], env['response']) != 'SUCCESS_VALIDATED':
                        raise RuntimeError('Old successful RPC binding does not validate')
                    payload = env['response']['result']
        if row['status'] == 'SUCCESS_VALIDATED' and artifact is None:
            raise RuntimeError('Successful historical read is missing its evidence')
        if row['status'] == 'SUCCESS_VALIDATED': outcome, error_class = 'SUCCESS', None
        else:
            env = read(artifact) if artifact else {}
            failure = classify_failure(http_status=receipt.get('http_status'),
                provider_error=(env.get('response') or {}).get('error'), category=receipt.get('error_class'))
            outcome, error_class = failure['outcome'], failure['error_class']
            if row['status'] == 'DISPATCH_INTENT': outcome, error_class = 'UNRESOLVED', 'PROCESS_INTERRUPTED_READ'
        imported_receipt = {'legacy_identity': row['identity'], 'legacy_job': row['job'],
            'legacy_receipt_sha256': sha(baseline / row['receipt']) if row['receipt'] else None,
            'artifact_path': artifact.relative_to(work).as_posix() if artifact else None,
            'artifact_sha256': sha(artifact) if artifact else None, 'legacy_status': row['status']}
        imported = store.import_attempt(key, 'r3-rpc:' + row['identity'], outcome=outcome, payload=payload,
            receipt=imported_receipt, error_class=error_class,
            accounting={'legacy_job': row['job'], 'preserved_in_financial_ledger': True, 'new_charge': False})
        rows.append({**imported, 'legacy_identity': row['identity'], 'legacy_status': row['status'], 'outcome': outcome})
    return rows

def migrate(work, baseline):
    work, baseline = Path(work).resolve(), Path(baseline).resolve()
    if work == baseline or work.parent != baseline.parent: raise ValueError('Distinct sibling revision required')
    policy = read(work / 'configs/STAGE1B_R4_POLICY.json')
    if policy.get('authorization_id') != R4_AUTH or policy['dune']['authorization_id'] != AUTH:
        raise ValueError('R4 continuation and inherited cumulative authority required')
    state = read(baseline / 'RUN_STATE.json')
    if state.get('phase') != 'CHECKPOINT_1B_R3_REACHED' or state.get('new_network_submissions_allowed') is not False:
        raise RuntimeError('Baseline R3 writer must be stopped')
    if (baseline / 'private/network_worker.lock').exists(): raise RuntimeError('Old network writer lock remains')
    receipt_path = work / 'private/BUDGET_MIGRATION_R4.json'
    if receipt_path.exists():
        receipt = read(receipt_path)
        if receipt['baseline_run_id'] != baseline.name or not db_path(work).exists(): raise RuntimeError('Migration disagreement')
        return receipt
    for sibling in work.parent.glob('*/private/shared_budget_r4.sqlite'):
        if sibling.resolve() != db_path(work): raise RuntimeError('Another R4 writer exists; resume it')
    if db_path(work).exists(): raise RuntimeError('Interrupted migration requires reconciliation, never overwrite')
    private = work / 'private'; private.mkdir(parents=True, exist_ok=True)
    snapshots = private / 'migration_snapshots'; snapshots.mkdir(exist_ok=True)
    sources = {'ledger': baseline / 'private/shared_budget_r3.sqlite', 'attempts': baseline / 'private/dune_request_attempts.sqlite'}
    before = readonly_snapshot(sources['ledger'])
    records = {k: consistent_backup(p, snapshots / (k + '.sqlite')) for k, p in sources.items()}
    consistent_backup(snapshots / 'ledger.sqlite', db_path(work))
    consistent_backup(snapshots / 'attempts.sqlite', private / 'dune_request_attempts.sqlite')
    with closing(sqlite3.connect(snapshots / 'ledger.sqlite')) as old, closing(sqlite3.connect(db_path(work))) as db:
        tables = [r[0] for r in old.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        equality = {}
        for table in tables:
            if not table.replace('_', '').isalnum(): raise ValueError('Unsafe table identifier')
            a = old.execute('SELECT * FROM ' + table + ' ORDER BY rowid').fetchall()
            b = db.execute('SELECT * FROM ' + table + ' ORDER BY rowid').fetchall()
            equality[table] = {'rows': len(a), 'identical': a == b}
            if a != b: raise RuntimeError('Inherited financial table changed')
        db.execute('CREATE TABLE r4_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        db.execute('CREATE TABLE r4_journal(n INTEGER PRIMARY KEY,kind TEXT,payload TEXT,utc TEXT)')
        db.execute('CREATE TABLE r4_rpc_attempts(attempt_id TEXT PRIMARY KEY,logical_key TEXT,job TEXT,capability INTEGER,receipt TEXT)')
        for key, value in {'authorization_id': R4_AUTH, 'inherited_authorization_id': AUTH, 'baseline_run_id': baseline.name}.items():
            db.execute('INSERT INTO r4_meta VALUES(?,?)', (key, value))
        db.execute('INSERT INTO r4_journal(kind,payload,utc) VALUES(?,?,?)',
            ('CONTINUATION_NOT_NEW_GRANT', json.dumps({'source_risk': before['dune_credits']['cumulative_risk']}), now()))
        db.commit()
    for name in ('dune_user_confirmation.json', 'dune_rate_evidence.json', 'current_dune_usage.json', 'alchemy_permission_r3.json'):
        (private / name).write_bytes((baseline / 'private' / name).read_bytes())
    if (baseline / 'private/dune_r2_jobs').exists(): shutil.copytree(baseline / 'private/dune_r2_jobs', private / 'dune_r2_jobs')
    inherited = read(baseline / 'private/INHERITED_RESOURCE_R3.json')
    resource = {'schema_version': 'r4-inherited-resource-v1', 'source_run_id': baseline.name,
        'inherited_raw_risk_bytes': old_raw_risk(baseline),
        'inherited_candidate_online_seconds': inherited['inherited_candidate_online_seconds'],
        'context_seconds_at_start': {p: str(old_clock_usage(baseline, p) + Decimal(inherited['context_seconds_at_start'][p])) for p in PROBES},
        'context_clock_is_new_explicit_workstream': False, 'no_clock_or_allowance_reset': True}
    new_json(private / 'INHERITED_RESOURCE_R4.json', resource)
    rows = _import_rpc(work, baseline, ReadRetryStore(retry_path(work)))
    new_json(private / 'RPC_ATTEMPT_MIGRATION_R4.json', {'rows': rows, 'attempts_imported': len(rows), 'old_costs_released': False})
    after = readonly_snapshot(db_path(work))
    if before != after: raise RuntimeError('Migration changed cumulative financial state')
    receipt = {'schema_version': 'stage1b-r4-budget-migration-v1', 'authorization_id': R4_AUTH,
        'baseline_run_id': baseline.name, 'run_id': work.name, 'source_backups': records,
        'historical_tables': equality, 'initial_snapshot': after, 'inherited_resource': resource,
        'rpc_attempts_imported': len(rows), 'new_allowance_granted': False,
        'source_unchanged': all(sha(sources[k]) == r['source_sha256_before'] for k, r in records.items()),
        'single_writer': 'private/shared_budget_r4.sqlite', 'created_at_utc': now()}
    new_json(receipt_path, receipt)
    return receipt

class RpcAccess(R3RpcAccess):
    def __init__(self, work, transport=None, *, clock=time.time, monotonic=time.monotonic, sleep=time.sleep, rng=random.random):
        self.w = Path(work).resolve()
        if not db_path(self.w).exists(): raise RuntimeError('R4 budget migration required')
        self.ledger = RevisionLedger(db_path(self.w)); self.transport = transport
        self.clock, self.monotonic, self.sleep = clock, monotonic, sleep
        self.store = ReadRetryStore(retry_path(self.w), clock=clock, rng=rng)

    def _transport(self, requests):
        if self.transport is not None:
            result = self.transport(requests, RPC_MAX + 1)
            return (*result, {}) if len(result) == 2 else result
        key = os.environ.get('ALCHEMY_API_KEY')
        if not key: raise RuntimeError('Configured Alchemy credential absent')
        request = urllib.request.Request('https://eth-mainnet.g.alchemy.com/v2/' + key,
            data=canonical(requests), headers={'Content-Type': 'application/json'}, method='POST')
        try:
            with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
                return response.status, response.read(RPC_MAX + 1), {'Retry-After': response.headers.get('Retry-After')}
        except urllib.error.HTTPError as exc:
            with exc: return exc.code, exc.read(RPC_MAX + 1), {'Retry-After': exc.headers.get('Retry-After')}

    def call_batch(self, plans, probe, label, capability=False, *, deadline=None):
        if not isinstance(plans, list) or not 1 <= len(plans) <= 50: raise ValueError('Finite 1..50 read operations required')
        plans = [validate_rpc(p) for p in plans]
        provider = 'ALCHEMY_ETH_MAINNET_EXISTING' if self.transport is None else 'SYNTHETIC_TRANSPORT'
        identities = [rpc_identity(provider, p) for p in plans]
        if len({logical_key(k) for k in identities}) != len(plans): raise ValueError('Duplicate batch member')
        permission = self.permission(); rates = permission['method_cu_upper_bounds']
        if any(type(rates.get(p['method'])) is not int or rates[p['method']] < 0 for p in plans): raise ValueError('Method CU bound required')
        if self.transport is None and not os.environ.get('ALCHEMY_API_KEY'): raise RuntimeError('Configured Alchemy credential absent')
        if not capability:
            with self.ledger.connection() as db:
                proved = db.execute("SELECT plan FROM r3_rpc_requests WHERE capability=1 AND status='SUCCESS_VALIDATED' AND provider=?", (provider,)).fetchall()
            with self.store._db() as db:
                proved_new = db.execute("SELECT identity_json FROM read_requests WHERE state='SUCCESS'").fetchall()
            methods = {json.loads(r[0])['method'] for r in proved}
            methods |= {json.loads(r[0])['method'] for r in proved_new if json.loads(r[0]).get('provider') == provider}
            if not {'eth_chainId', 'eth_getBalance'}.issubset(methods): raise RuntimeError('Mainnet and historical-state capability required')
        done = {}; attempt_receipts = []; deferred = {}; pending = list(range(len(plans)))
        with session(self.w, probe, label, clock=self.clock, monotonic=self.monotonic, synthetic=self.transport is not None) as timing:
            deadline = min(timing['deadline'], deadline) if deadline is not None else timing['deadline']
            while pending:
                claimed = []; wait_until = []
                for index in pending:
                    claim = self.store.claim(identities[index], deadline=deadline)
                    if claim['state'] == 'CACHE_HIT':
                        cached_path = (self.w / claim['receipt'].get('artifact_path', '')).resolve()
                        if not cached_path.is_relative_to(self.w) or not cached_path.is_file() or sha(cached_path) != claim['receipt'].get('artifact_sha256'):
                            raise RuntimeError('Successful RPC cache provenance differs')
                        done[index] = {'identity': claim['logical_key'], 'method': plans[index]['method'], 'status': 'SUCCESS_VALIDATED',
                            'cache_hit': True, 'result': claim['payload'], **claim['receipt']}
                    elif claim['state'] == 'CLAIMED': claimed.append((index, claim))
                    elif claim['state'] == 'DEFERRED':
                        wait_until.append(claim['next_eligible_at']); deferred[index] = claim
                    else: done[index] = {'identity': claim['logical_key'], 'method': plans[index]['method'], 'status': claim['state'], 'complete': False}
                if not claimed:
                    if wait_until and min(wait_until) < deadline and self.clock() < deadline:
                        self.sleep(min(30, max(0, min(wait_until) - self.clock()))); pending = [i for i in pending if i not in done]; continue
                    break
                if raw_risk(self.w) + RPC_MAX + 1 > RAW_CAP:
                    for _, claim in claimed: self.store.abandon_before_dispatch(claim['attempt_id'], 'RAW_BUDGET_DEFERRED')
                    raise RuntimeError('Cumulative raw risk cap')
                if self.clock() + 30 > deadline:
                    for index, claim in claimed:
                        self.store.abandon_before_dispatch(claim['attempt_id'], 'DEADLINE_DEFERRED')
                        deferred[index] = {'state': 'DEFERRED', 'next_eligible_at': self.clock(), 'deadline': deadline}
                    break
                batch = 'rpc_r4_' + uuid.uuid4().hex; directory = self.w / 'raw/rpc_r4' / batch
                directory.mkdir(parents=True, exist_ok=False)
                # Each individual operation has its own durable cost identity.
                # Reserve all members before dispatching any member.
                reserved = []
                try:
                    if capability:
                        with self.ledger.connection() as db:
                            old_plans = db.execute('SELECT plan FROM r3_rpc_requests WHERE capability=1 AND provider=?', (provider,)).fetchall()
                            new_keys = {r[0] for r in db.execute('SELECT DISTINCT logical_key FROM r4_rpc_attempts WHERE capability=1')}
                        # The five-operation capability scope is about distinct
                        # selectors; retries retain the same selector and cost.
                        old_keys = {logical_key(rpc_identity(provider, json.loads(r[0]))) for r in old_plans}
                        if len(old_keys | new_keys | {c['logical_key'] for _, c in claimed}) > 5:
                            raise RuntimeError('Selected-provider capability scope exhausted')
                    for index, claim in claimed:
                        job = 'rpc_r4_' + claim['attempt_id']
                        self.ledger.reserve(job, 'alchemy', 'R4 bounded read: ' + plans[index]['method'], {'rpc_operations': 1, 'alchemy_cu': rates[plans[index]['method']]})
                        reserved.append((index, claim, job))
                    requests = [{'jsonrpc': '2.0', 'id': claim['attempt_id'], **plans[index]} for index, claim, _ in reserved]
                    new_json(directory / 'dispatch_intent.json', {'requests': requests, 'provider_alias': provider,
                        'logical_keys': [c['logical_key'] for _, c, _ in reserved], 'utc': now(), 'old_attempt_risk_released': False})
                    for index, claim, job in reserved:
                        self.store.mark_dispatched(claim['attempt_id'], accounting={'job': job, 'rpc_operations': 1,
                            'alchemy_cu_upper_bound_not_actual': rates[plans[index]['method']]})
                        with self.ledger.connection() as db:
                            db.execute('INSERT INTO r4_rpc_attempts VALUES(?,?,?,?,NULL)', (claim['attempt_id'], claim['logical_key'], job, int(capability)))
                except Exception:
                    # No transport occurred. Reservations already made remain
                    # conservative, explicitly settled zero only with this proof.
                    for _, claim, job in reserved:
                        attempts = self.store.attempts(identities[next(i for i,c,j in reserved if j == job)])
                        if next(a for a in attempts if a['attempt_id'] == claim['attempt_id'])['dispatched_at'] is None:
                            self.ledger.settle(job, {'rpc_operations': 0, 'alchemy_cu': 0})
                    for _, claim in claimed:
                        try: self.store.abandon_before_dispatch(claim['attempt_id'], 'DISPATCH_SETUP_FAILED')
                        except ValueError: pass
                    raise
                status, body, headers, failure = None, b'', {}, None
                try: status, body, headers = self._transport(requests)
                except Exception as exc: failure = classify_failure(exception=exc)
                if not isinstance(body, bytes): body, failure = b'', classify_failure(category='NON_BYTES_RESPONSE')
                if any(v.encode() in body for k,v in os.environ.items() if any(p in k.upper() for p in ('API_KEY','TOKEN','SECRET')) and len(v) >= 12):
                    body, failure = b'', classify_failure(category='CREDENTIAL_ECHO_WITHHELD')
                if len(body) > RPC_MAX: body, failure = body[:RPC_MAX], classify_failure(category='RESPONSE_TOO_LARGE')
                (directory / 'response_body.bin').write_bytes(body)
                if failure is None and status != 200: failure = classify_failure(http_status=status, headers=headers)
                try: decoded = json.loads(body) if failure is None else None
                except (ValueError, UnicodeError): decoded = None
                if failure is None and not isinstance(decoded, list): failure = classify_failure(category='INVALID_JSON_RPC_BATCH')
                valid_ids = {r['id'] for r in requests}
                unknown_ids = any(not isinstance(r, dict) or type(r.get('id')) is not str or r['id'] not in valid_ids for r in decoded) if isinstance(decoded, list) else False
                members = []
                for index, claim, job in reserved:
                    request = next(r for r in requests if r['id'] == claim['attempt_id'])
                    matches = [r for r in decoded if isinstance(r, dict) and type(r.get('id')) is str and r['id'] == request['id']] if isinstance(decoded, list) else []
                    response = matches[0] if len(matches) == 1 else None
                    member_failure = failure
                    if member_failure is None:
                        if len(matches) == 0: member_failure = classify_failure(category='UNRECEIVED_BATCH_MEMBER')
                        elif len(matches) > 1: member_failure = classify_failure(category='DUPLICATE_RPC_RESPONSE_ID')
                        elif 'error' in response: member_failure = classify_failure(provider_error=response['error'], headers=headers)
                        elif rpc_result_status(request, response) != 'SUCCESS_VALIDATED': member_failure = classify_failure(category=rpc_result_status(request, response))
                    outcome = member_failure['outcome'] if member_failure else 'SUCCESS'
                    env = {'provider_alias': provider, 'evidence_kind': 'REAL_CHAIN' if self.transport is None else 'SYNTHETIC_TRANSPORT',
                        'http_status': status, 'request': request, 'response': response, 'response_complete': failure is None,
                        'raw_body_sha256': hashlib.sha256(body).hexdigest(), 'status': 'SUCCESS_VALIDATED' if outcome == 'SUCCESS' else member_failure['error_class']}
                    artifact = directory / ('envelope_' + str(index) + '.json'); new_json(artifact, env)
                    member_receipt = {'artifact_path': artifact.relative_to(self.w).as_posix(), 'artifact_sha256': sha(artifact),
                        'attempt_id': claim['attempt_id'], 'attempt_no': claim['attempt_no'], 'retry_of': claim['retry_of'], 'job': job}
                    self.ledger.settle(job, {'rpc_operations': 1, 'alchemy_cu': None})
                    finished = self.store.finish(claim['attempt_id'], outcome, payload=response['result'] if outcome == 'SUCCESS' else None,
                        receipt=member_receipt, error_class=member_failure['error_class'] if member_failure else None,
                        retry_after=member_failure.get('retry_after') if member_failure else None)
                    members.append({**member_receipt, **finished, 'method': plans[index]['method']})
                    if outcome == 'SUCCESS': done[index] = {**member_receipt, 'identity': claim['logical_key'], 'method': plans[index]['method'], 'status': 'SUCCESS_VALIDATED', 'cache_hit': False, 'result': response['result']}
                    elif outcome == 'PERMANENT_FAILURE': done[index] = {**member_receipt, 'identity': claim['logical_key'], 'method': plans[index]['method'], 'status': 'PERMANENT_FAILURE', 'error_class': member_failure['error_class'], 'complete': False}
                receipt = {'batch': batch, 'http_status': status, 'error_class': failure['error_class'] if failure else None,
                    'raw_bytes': len(body), 'raw_body_sha256': hashlib.sha256(body).hexdigest(), 'members': members,
                    'unknown_response_ids_observed': unknown_ids, 'physical_duplicate_results_do_not_deduplicate_fees': True,
                    'rpc_operations': len(reserved), 'alchemy_cu_is_upper_bound_not_invoice': True, 'utc': now()}
                new_json(directory / 'receipt.json', receipt); attempt_receipts.append(receipt)
                if failure and status is None:
                    new_json(self.w / 'private/context_uncertainty' / (batch + '.json'), {'additional_raw_risk_bytes': max(0, RPC_MAX - len(body)), 'basis': 'Uncertain bounded response prefix', 'receipt': batch})
                with self.ledger.connection() as db:
                    for _, claim, _ in reserved: db.execute('UPDATE r4_rpc_attempts SET receipt=? WHERE attempt_id=?', ((directory / 'receipt.json').relative_to(self.w).as_posix(), claim['attempt_id']))
                pending = [i for i in pending if i not in done]
        for i in range(len(plans)):
            if i not in done: done[i] = {'identity': logical_key(identities[i]), 'method': plans[i]['method'], 'status': 'DEFERRED', 'complete': False, **deferred.get(i, {})}
        members = [done[i] for i in range(len(plans))]
        return {'status': 'COMPLETE' if all(m['status'] == 'SUCCESS_VALIDATED' for m in members) else 'PARTIAL',
            'members': members, 'attempt_batches': len(attempt_receipts), 'actual_operations_this_call': sum(r['rpc_operations'] for r in attempt_receipts),
            'snapshot': self.ledger.snapshot()}

def main(argv=None):
    parser = argparse.ArgumentParser(); parser.add_argument('action', choices=('migrate', 'snapshot', 'rpc'))
    parser.add_argument('--work', required=True, type=Path); parser.add_argument('--baseline', type=Path)
    parser.add_argument('--plan', type=Path); parser.add_argument('--probe', default='SHARED'); parser.add_argument('--label', default='r4_rpc')
    parser.add_argument('--capability', action='store_true'); args = parser.parse_args(argv)
    if args.action == 'migrate': result = migrate(args.work, args.baseline)
    elif args.action == 'snapshot': result = readonly_snapshot(db_path(args.work))
    else: result = RpcAccess(args.work).call_batch(read(args.plan), args.probe, args.label, capability=args.capability)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result.get('status') == 'PARTIAL' else 0

if __name__ == '__main__': raise SystemExit(main())
