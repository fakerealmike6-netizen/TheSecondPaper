"""R4 Dune reads with durable bounded recovery and cumulative accounting.

No network calls occur on import. SQL POST is never transport-retried. Every
page recovery retains the original attempt and adds a costed dispatch intent.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
import hashlib
import json
import os
from pathlib import Path
import re
import random
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from budget_r2 import RevisionLedger, AUTH
from budget import valid
from context_access_r4 import (db_path, retry_path, require_gate, raw_risk, session,
                              runtime_budget_ledger, runtime_budget_warning)
from context_access_r3 import RAW_CAP, DUNE_MAX, new_json, now, read, sha
from context_queries_r3 import verify_frozen_scope
from dune_r2 import RevisionLive, TERMINAL
from network import NoRedirect
from page_attempts import AttemptStore, RequestBlocked, atomic_json
from page_contract import initial_progress, exact_count, validate_page, PageContractError
from read_retry_r4 import ReadRetryStore, classify_failure, logical_key


def _portable(value):
    return json.loads(json.dumps(value, default=str))


class ContextDuneR4(RevisionLive):
    def __init__(self, work, transport=None, *, clock=time.time, sleeper=time.sleep,
                 rng=random.random, deadline=None, runtime=None):
        self.w = Path(work).resolve()
        if not db_path(self.w).exists(): raise RuntimeError('R4 budget migration required')
        self.db = runtime_budget_ledger(self.w, runtime)
        self.confirm = read(self.w / 'private/dune_user_confirmation.json')
        if (self.confirm.get('status') != 'USER_CONFIRMED' or self.confirm.get('authorization_id') != AUTH
                or str(self.confirm.get('execution_cap_credits')) != '20'
                or self.confirm.get('payment_method_added') is not False
                or self.confirm.get('extra_credits_enabled') is not False):
            raise RuntimeError('Inherited cap20 confirmation and no additional payment required')
        self.account_context_ref = self.confirm['account_context_ref']
        self.attempts = AttemptStore(self.w / 'private/dune_request_attempts.sqlite')
        self.transport, self.clock, self.sleeper, self.deadline = transport, clock, sleeper, deadline
        self.reads = getattr(runtime,'retry_store',ReadRetryStore)(retry_path(self.w), clock=clock, rng=rng)
        self.runtime = runtime
        with self.db.connection() as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS r4_dune_exports(
              attempt_id TEXT PRIMARY KEY, job TEXT NOT NULL, increment TEXT NOT NULL,
              upper_after TEXT NOT NULL, dispatched INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS r4_dune_export_baselines(
              job TEXT PRIMARY KEY, inherited_export_risk TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS r4_dune_sql(
              job TEXT PRIMARY KEY, root_job TEXT NOT NULL, retry_number INTEGER NOT NULL,
              folder TEXT NOT NULL, state TEXT NOT NULL, reason TEXT,
              UNIQUE(root_job,retry_number));''')

    def require_gate(self):
        if self.transport is None:
            (self.runtime.require_gate if self.runtime is not None else require_gate)(self.w)

    def active_raw_risk(self):
        return (self.runtime.raw_risk if self.runtime is not None else raw_risk)(self.w)

    def _budget_snapshot(self):
        snapshot = self.db.snapshot()
        self.last_budget_warning = runtime_budget_warning(self.w, self.runtime, snapshot)
        return snapshot

    def ensure_not_halted(self):
        # Parent checks self.db.snapshot(): the injected ledger supplies its cap,
        # and all inherited non-Dune resource halt checks remain effective.
        super().ensure_not_halted()
        if self.runtime is not None: self._budget_snapshot()

    def job_caps(self, state):
        if getattr(self.runtime, 'ledger_factory', None) is None: return super().job_caps(state)
        active = self.db.snapshot()['dune_credits']
        cap = self.db.execution_cap(state.get('logical_job_id')) if hasattr(self.db, 'execution_cap') else Decimal(20)
        return {'execution': cap, 'export': None, 'logical': Decimal(active['cap']),
                'authorization_id': active['authorization_id']}

    def observe_execution_charge(self, state, body, receipt):
        super().observe_execution_charge(state, body, receipt)
        if self.runtime is not None: self._budget_snapshot()

    def usage(self):
        if getattr(self.runtime, 'ledger_factory', None) is None: return super().usage()
        body, receipt = self.call('usage')
        if receipt.get('http_status') != 200 or not isinstance(body, dict):
            raise RuntimeError('Usage unavailable; receipt retained')
        today = datetime.now(timezone.utc).date().isoformat()
        current = next((p for p in body.get('billing_periods', []) if p['start_date'] <= today < p['end_date']), None)
        if current is None: raise RuntimeError('Current billing period absent')
        remaining = valid(current['credits_included'], 'dune_credits') - valid(current['credits_used'], 'dune_credits')
        cap = self.db.snapshot()['dune_credits']['cap']
        self.db.confirm('dune_credits', str(max(Decimal(0), remaining)),
            'Current included remaining ' + receipt['request_id'] + '; authorized cumulative' + cap
            + ' unchanged; no precise job delta inference')
        atomic_json(self.w / 'private/current_dune_usage.json', {'observed_at_utc': receipt['utc'],
            'receipt': receipt, 'billing_period': current, 'included_remaining_credits': str(remaining),
            'account_context_ref': self.account_context_ref, 'not_final_job_settlement': True})
        return {'http_status': 200, 'included_remaining_credits': str(remaining),
                'usage_request_id': receipt['request_id'], 'cumulative': self._budget_snapshot()['dune_credits']}

    def _prepare_raw(self, request_id=None):
        pending=getattr(self,'_pending_raw_id',None)
        if pending:return pending
        request_id=request_id or 'dune_recovery_'+uuid.uuid4().hex
        if getattr(self.runtime,'reserve_raw',None) and self.runtime.reserve_raw(self.w,request_id,DUNE_MAX+65536):
            self._pending_raw_id=request_id
            return request_id
        return None

    def _cancel_raw_before_transport(self, reason):
        pending=getattr(self,'_pending_raw_id',None)
        if not pending:return
        receipt=self.w/'logs'/(pending+'_not_dispatched.json')
        atomic_json(receipt,{'request_id':pending,'http_transport_called':False,'reason':reason,'utc':now()})
        self.runtime.close_raw(self.w,pending,receipt);self._pending_raw_id=None

    def _preflight(self):
        self.require_gate()
        if self.transport is None:
            if not (self.w / 'private/network_worker.lock').exists(): raise RuntimeError('Use R4 clocked single-writer entry point')
            if not os.environ.get('DUNE_API_KEY'): raise RuntimeError('Configured Dune credential absent')
        if self.active_raw_risk() + (0 if getattr(self,'_pending_raw_id',None) else DUNE_MAX) > (self.runtime.raw_limit(self.w) if getattr(self.runtime, 'raw_limit', None) else RAW_CAP): raise RuntimeError('Cumulative raw risk cap')

    def _inside(self, path):
        path = Path(path).resolve()
        if not path.is_relative_to(self.w): raise ValueError('Dune artifact outside revision')
        return path

    def call(self, op, execution=None, payload=None, params=None):
        self.require_gate()
        if self.transport is None and not (self.w / 'private/network_worker.lock').exists():
            raise RuntimeError('Use R4 clocked single-writer entry point')
        if self.active_raw_risk() + (0 if getattr(self,'_pending_raw_id',None) else DUNE_MAX) > (self.runtime.raw_limit(self.w) if getattr(self.runtime, 'raw_limit', None) else RAW_CAP): raise RuntimeError('Cumulative raw risk cap')
        if self.deadline is not None and self.clock() + 30 > self.deadline:
            raise RuntimeError('Context clock insufficient for bounded request')
        if op == 'execute' and not getattr(self, '_active_sql_dispatch', False): raise RuntimeError('Use guarded submit')
        if op == 'results' and not getattr(self, '_active_export_dispatch', False): raise RuntimeError('Use guarded export')
        if execution is not None and not re.fullmatch('[A-Z0-9]{26}', execution): raise ValueError('Invalid execution identity')
        paths = {'usage': '/usage', 'execute': '/sql/execute',
                 'status': '/execution/' + str(execution) + '/status',
                 'results': '/execution/' + str(execution) + '/results'}
        if op not in paths: raise ValueError('Unsupported Dune operation')
        if op == 'execute' and (set(payload or {}) != {'sql', 'performance'} or payload['performance'] not in ('small', 'medium')):
            raise ValueError('Only Small/Medium read-only SQL')
        if params and (op != 'results' or set(params) != {'limit', 'offset'}
                       or type(params['limit']) is not int or not 1 <= params['limit'] <= 1000
                       or type(params['offset']) is not int or params['offset'] < 0): raise ValueError('Fixed page parameters required')
        rid = 'dune_r4_' + op + '_' + uuid.uuid4().hex
        rid=self._prepare_raw(rid) or rid
        started, status, raw, error, headers = self.clock(), None, b'', None, {}
        try:
            if self.transport is not None:
                status, raw, headers = self.transport(op, execution, payload, params, DUNE_MAX)
            else:
                url = 'https://api.dune.com/api/v1' + paths[op]
                if params: url += '?' + urllib.parse.urlencode(params)
                data = json.dumps(payload or {}).encode() if op in ('usage', 'execute') else None
                request = urllib.request.Request(url, data=data, headers={'X-Dune-API-Key': os.environ['DUNE_API_KEY'], 'Content-Type': 'application/json'})
                try:
                    with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
                        status, raw, headers = response.status, response.read(DUNE_MAX), dict(response.headers)
                except urllib.error.HTTPError as exc:
                    with exc: status, raw, headers = exc.code, exc.read(DUNE_MAX), dict(exc.headers)
        except Exception as exc:
            failure = classify_failure(exc)
            error = failure['error_class']
            if isinstance(getattr(exc, 'partial', None), bytes): raw = exc.partial[:DUNE_MAX]
        if not isinstance(raw, bytes): raw, error = b'', 'NON_BYTES_RESPONSE'
        if len(raw) >= DUNE_MAX: raw, error = raw[:DUNE_MAX], 'RAW_RESPONSE_AT_RESERVED_READ_LIMIT'
        if any(v.encode() in raw for k, v in os.environ.items() if any(s in k.upper() for s in ('API_KEY', 'TOKEN', 'SECRET')) and len(v) >= 12):
            raw, error = b'', 'CREDENTIAL_ECHO_WITHHELD'
        path = self.w / 'raw/dune' / (rid + '.json'); path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(raw)
        receipt = {'request_id': rid, 'operation': op, 'execution_id': execution, 'utc': now(),
                   'elapsed_seconds': max(0, self.clock() - started), 'http_status': status, 'error_class': error,
                   'parameters': params, 'raw_path': path.relative_to(self.w).as_posix(), 'raw_bytes': len(raw),
                   'sha256': hashlib.sha256(raw).hexdigest(), 'retry_after': headers.get('Retry-After', headers.get('retry-after')),
                   'evidence_kind': 'REAL_PROVIDER' if self.transport is None else 'SYNTHETIC_TRANSPORT'}
        atomic_json(self.w / 'logs' / (rid + '.json'), receipt)
        if error:
            new_json(self.w / 'private/context_uncertainty' / (rid + '.json'),
                     {'additional_raw_risk_bytes': max(0, DUNE_MAX - len(raw)), 'basis': error, 'receipt': rid})
        try: body = json.loads(raw, parse_float=Decimal) if not error else None
        except (ValueError, UnicodeError): body = None
        if getattr(self,'_pending_raw_id',None):
            self.runtime.close_raw(self.w,rid,self.w/'logs'/(rid+'.json'));self._pending_raw_id=None
        return body, receipt

    def read_identity(self, op, state, params=None, sequence=None):
        identity = {'provider': 'DUNE_EXISTING_ACCOUNT', 'account_context_ref': self.account_context_ref,
                    'chain': 1, 'method': 'GET_' + op.upper(), 'execution_id': state['execution_id']}
        if op == 'results':
            identity.update(parameters=dict(params), columns=(state.get('status_response') or {}).get('result_metadata', {}).get('column_names'))
        else: identity['observation_sequence'] = sequence
        return identity

    def _claim(self, identity):
        while True:
            if self.deadline is not None and self.clock() + 30 > self.deadline:
                return {'state': 'DEFERRED', 'logical_key': logical_key(identity), 'deadline': self.deadline,
                        'next_eligible_at': self.clock(), 'reason': 'INSUFFICIENT_REMAINING_REQUEST_TIME'}
            self._preflight()
            claim = self.reads.claim(identity, deadline=self.deadline)
            if claim['state']=='CLAIMED':
                try:self._prepare_raw()
                except Exception:
                    self.reads.abandon_before_dispatch(claim['attempt_id'],'RAW_RESOURCE_DEFERRED');raise
            if claim['state'] != 'DEFERRED': return claim
            delay = max(0, claim['next_eligible_at'] - self.clock())
            if not delay or delay > 60 or self.deadline is not None and self.clock() + delay + 30 > self.deadline:
                return claim
            self.sleeper(delay)

    @staticmethod
    def _failure(receipt, body):
        if isinstance(body, dict) and body.get('error'):
            provider = classify_failure(provider_error=body['error'])
            if provider['error_class'] == 'AUTH_ENTITLEMENT_OR_INVALID_REQUEST': return provider
        failure = classify_failure(http_status=receipt.get('http_status'), category=receipt.get('error_class'),
                                   headers={'Retry-After': receipt.get('retry_after')})
        if receipt.get('http_status') == 200 and not receipt.get('error_class'):
            failure = classify_failure(provider_error=body.get('error')) if isinstance(body, dict) and body.get('error') else {'outcome': 'PERMANENT_FAILURE', 'error_class': 'INVALID_RESPONSE_SCHEMA', 'retry_after': None}
        return failure

    def _legacy_artifact(self, row, kind, folder):
        key, expected = kind + '_path', row.get(kind + '_sha')
        candidate = folder / ('page_' + str(row['offset']) + ('_receipt.json' if kind == 'receipt' else '.json'))
        if not candidate.exists():
            old = Path(row.get(key) or '')
            if old.is_absolute() and old.resolve().is_relative_to(self.w): candidate = old
        if not candidate.is_file() or not expected or sha(candidate) != expected:
            raise RequestBlocked('Historical Dune artifact identity mismatch')
        return candidate

    def _old_page_attempt(self, state, offset):
        matches = []
        for row in self.attempts.rows():
            identity = json.loads(row['identity'])
            if identity.get('execution_id') == state['execution_id'] and identity.get('operation') == 'results' and identity['parameters'].get('offset') == offset:
                matches.append((row, identity))
        if len(matches) > 1: raise RequestBlocked('Ambiguous historical page identity')
        return matches[0] if matches else (None, None)

    def import_page_attempt(self, folder, state, params, progress):
        row, old_identity = self._old_page_attempt(state, params['offset'])
        if row is None: return None
        if old_identity['parameters'] != params: raise RequestBlocked('Historical page parameters cannot change during recovery')
        payload, receipt = None, {}
        if row.get('response_sha'):
            response_path = self._legacy_artifact(row, 'response', folder)
            receipt_path = self._legacy_artifact(row, 'receipt', folder)
            payload, receipt = read(response_path), read(receipt_path)
        if row['state'] == 'SUCCESS_VALIDATED':
            validate_page(payload, execution_id=state['execution_id'], offset=params['offset'], limit=params['limit'],
                          progress=progress, status_metadata=state['status_response']['result_metadata'], receipt=receipt, parameters=params)
            outcome, error = 'SUCCESS', None
        else:
            failure = self._failure(receipt, payload)
            if not receipt and row.get('error'):
                failure = classify_failure(category=row['error'])
            outcome, error = failure['outcome'], failure['error_class']
            if row['state'] == 'DISPATCH_INTENT': outcome, error = 'UNRESOLVED', 'PROCESS_INTERRUPTED_READ'
        return self.reads.import_attempt(self.read_identity('results', state, params), 'dune-attempt:' + row['attempt_id'],
            outcome=outcome, payload=_portable({'body': payload, 'receipt': receipt}) if outcome == 'SUCCESS' else None,
            receipt={'legacy_attempt_id': row['attempt_id'], 'legacy_state': row['state'], 'bytes_retained': True},
            accounting={'legacy_job': state['logical_job_id'], 'legacy_export_risk_preserved': True, 'new_charge': False},
            error_class=error, retry_after=receipt.get('retry_after'))

    def migrate_legacy_reads(self):
        """Import only copied, identified job artifacts; never scan old projects."""
        imported = []
        for jobpath in sorted((self.w / 'private/dune_r2_jobs').glob('*/job.json')):
            state = read(jobpath)
            if not state.get('execution_id') or not (state.get('status_response') or {}).get('result_metadata'):
                continue
            progress = initial_progress(exact_count(state['status_response']['result_metadata']['total_row_count'], 'total_row_count'))
            while not progress['complete']:
                row, identity = self._old_page_attempt(state, progress['next_offset'])
                if row is None: break
                result = self.import_page_attempt(jobpath.parent, state, identity['parameters'], progress)
                imported.append({**result, 'legacy_attempt_id': row['attempt_id'], 'legacy_state': row['state']})
                if row['state'] != 'SUCCESS_VALIDATED': break
                page, receipt = read(self._legacy_artifact(row, 'response', jobpath.parent)), read(self._legacy_artifact(row, 'receipt', jobpath.parent))
                progress = validate_page(page, execution_id=state['execution_id'], offset=identity['parameters']['offset'],
                    limit=identity['parameters']['limit'], progress=progress, status_metadata=state['status_response']['result_metadata'], receipt=receipt, parameters=identity['parameters'])
        return {'schema_version': 'r4-dune-read-migration-v1', 'attempts': imported, 'original_attempt_rows_changed': False,
                'original_cost_or_risk_released': False, 'network_requests': 0}

    def _verified_pages(self, folder, state):
        md = (state.get('status_response') or {}).get('result_metadata') or {}
        progress = initial_progress(exact_count(md.get('total_row_count'), 'total_row_count'))
        pages = []
        while not progress['complete']:
            offset = progress['next_offset']; overlay = state.get('r4_verified_pages', {}).get(str(offset))
            if overlay:
                p, r = self._inside(self.w / overlay['page_path']), self._inside(self.w / overlay['receipt_path'])
                if sha(p) != overlay['page_sha256'] or sha(r) != overlay['receipt_sha256']: raise RequestBlocked('R4 cached page identity mismatch')
                page, receipt = read(p), read(r)
            else:
                old, _ = self._old_page_attempt(state, offset)
                if old is None or old['state'] != 'SUCCESS_VALIDATED': break
                p, r = self._legacy_artifact(old, 'response', folder), self._legacy_artifact(old, 'receipt', folder)
                page, receipt = read(p), read(r)
            params = receipt.get('parameters') or {}
            progress = validate_page(page, execution_id=state['execution_id'], offset=offset, limit=params.get('limit'),
                progress=progress, status_metadata=md, receipt=receipt, parameters=params)
            pages.append((page, receipt, offset))
        return progress, pages

    def export_progress(self, folder, state): return self._verified_pages(self._inside(folder), state)[0]

    def export_envelope(self, state, limit, folder=None):
        if type(limit) is not int or not 1 <= limit <= 1000: raise ValueError('Page limit1..1000')
        rate = self.rate_evidence(); md = (state.get('status_response') or {}).get('result_metadata') or {}
        rows, size = exact_count(md.get('total_row_count'), 'total_row_count'), exact_count(md.get('total_result_set_bytes'), 'total_result_set_bytes')
        cols = md.get('column_names')
        if not isinstance(cols, list) or not cols: raise ValueError('Full column metadata required')
        observations = [md] + [page['result']['metadata'] for page, _, _ in self._verified_pages(folder, state)[1]] if folder is not None else [md]
        sizes = [size] + [exact_count(x[k], k) for x in observations for k in ('total_result_set_bytes', 'result_set_bytes') if k in x]
        points = [rows * len(cols)] + [exact_count(x['datapoint_count'], 'datapoint_count') for x in observations if 'datapoint_count' in x]
        size = max(sizes); datapoints = max(Decimal(max(points)), (Decimal(size) / 100).to_integral_value(rounding=ROUND_CEILING))
        per = max(Decimal(1), max(Decimal(size) * 20 / 1000000, datapoints / 1000).to_integral_value(rounding=ROUND_CEILING))
        base = per * max(1, (rows + limit - 1) // limit)
        basis = {'rate_evidence': rate, 'result_metadata': md, 'per_request_upper_credits': str(per),
                 'full_export_upper_credits': str(base), 'maximum_pages': max(1, (rows + limit - 1) // limit),
                 'page_limit': limit, 'server_metadata_observations': observations, 'is_actual': False,
                 'basis': 'Maximum documented byte/datapoint model, conservative per-request whole-result bound; not final billing.'}
        return base, basis

    def _reserve_page(self, state, claim, base, basis):
        job = state['logical_job_id']; per = Decimal(basis['per_request_upper_credits'])
        with self.db.connection() as db:
            db.execute('INSERT OR IGNORE INTO r4_dune_export_baselines SELECT job,export_risk FROM r2_components WHERE job=?', (job,))
            inherited = Decimal(db.execute('SELECT inherited_export_risk FROM r4_dune_export_baselines WHERE job=?', (job,)).fetchone()[0])
            old = db.execute('SELECT upper_after FROM r4_dune_exports WHERE attempt_id=?', (claim['attempt_id'],)).fetchone()
            # Revised server metadata also bounds every previous request to
            # this immutable execution, including an interrupted export.
            extras = sum((max(Decimal(r[0]), per) if Decimal(r[0]) else Decimal(0)
                          for r in db.execute('SELECT increment FROM r4_dune_exports WHERE job=?', (job,))), Decimal(0))
        if old: return Decimal(old[0])
        increment = per if claim['attempt_no'] > 1 else Decimal(0)
        upper = max(base, inherited) + extras + increment
        self.db.reserve_export(job, upper, {**basis, 'additional_recovery_risk': str(extras + increment),
                                          'current_attempt_id': claim['attempt_id'], 'old_risk_released': False})
        with self.db.connection() as db:
            upper = Decimal(db.execute('SELECT export_risk FROM r2_components WHERE job=?', (job,)).fetchone()[0])
            db.execute('INSERT INTO r4_dune_exports VALUES(?,?,?,?,0)', (claim['attempt_id'], job, str(increment), str(upper)))
        if self.runtime is not None: self._budget_snapshot()
        return upper

    def poll(self, folder):
        folder = self._inside(folder); state = read(folder / 'job.json')
        if state.get('state') in TERMINAL:
            return {'status': 'TERMINAL_CACHE_REUSED', 'state': state['state'], 'execution_id': state['execution_id']}
        sequence = state.get('r4_status_sequence', 0)
        identity = self.read_identity('status', state, sequence=sequence)
        while True:
            claim = self._claim(identity)
            if claim['state'] == 'CACHE_HIT':
                body, receipt = claim['payload']['body'], claim['payload']['receipt']
            elif claim['state'] != 'CLAIMED': return {**claim, 'state': state['state'], 'retry_status': claim['state']}
            else:
                self.reads.mark_dispatched(claim['attempt_id'], accounting={'cost': 'STATUS_GET_NO_EXECUTION_OR_EXPORT', 'old_risk_released': False})
                body, receipt = self.call('status', state['execution_id'])
                good = receipt.get('http_status') == 200 and not receipt.get('error_class') and isinstance(body, dict) and body.get('execution_id') == state['execution_id'] and body.get('state') in TERMINAL | {'QUERY_STATE_PENDING', 'QUERY_STATE_EXECUTING'}
                if not good:
                    failure = self._failure(receipt, body)
                    self.reads.finish(claim['attempt_id'], failure['outcome'], error_class=failure['error_class'], retry_after=failure.get('retry_after'), receipt=receipt)
                    state.update(latest_status_receipt=receipt, latest_status_response=_portable(body)); atomic_json(folder / 'job.json', state)
                    continue
                self.reads.finish(claim['attempt_id'], 'SUCCESS', payload=_portable({'body': body, 'receipt': receipt}), receipt=receipt)
            state.update(status_response=_portable(body), status_receipt=receipt, state=body['state'], r4_status_sequence=sequence + 1)
            self.observe_execution_charge(state, body, receipt)
            atomic_json(folder / 'job.json', state)
            return {'http_status': 200, 'state': state['state'], 'execution_id': state['execution_id'], 'observation_sequence': sequence,
                    'execution_cost_credits': state.get('execution_cost_credits'), 'cumulative': self._budget_snapshot()['dune_credits']}

    def export(self, folder, limit=1000, offset=0):
        if type(limit) is not int or not 1 <= limit <= 1000: raise ValueError('Page limit1..1000')
        self.require_gate(); self.ensure_not_halted(); folder = self._inside(folder); state = read(folder / 'job.json')
        if state.get('state') != 'QUERY_STATE_COMPLETED': raise RuntimeError('Execution not completed')
        progress = self.export_progress(folder, state)
        if progress['complete']: return {'cache_reused': True, 'progress': progress, 'export_status': 'COMPLETED_DECLARED_RESULT_ROWS'}
        self.pending_job(state)
        if type(offset) is not int or offset != progress['next_offset']: raise ValueError('Only exact next page allowed')
        if progress['page_size'] not in (None, limit): raise ValueError('Page size frozen')
        prior_limit = state.get('r4_page_limit')
        if prior_limit not in (None, limit): raise RequestBlocked('Page parameters remain fixed across retry/restart')
        selection = {'execution_id': state['execution_id'], 'limit': limit,
                     'columns': state['status_response']['result_metadata'].get('column_names'),
                     'column_types': state['status_response']['result_metadata'].get('column_types')}
        if state.get('r4_page_selection', selection) != selection: raise RequestBlocked('Fixed page columns/execution changed')
        state['r4_page_selection'] = selection
        state['r4_page_limit'] = limit; atomic_json(folder / 'job.json', state)
        params = {'limit': limit, 'offset': offset}; identity = self.read_identity('results', state, params)
        self.import_page_attempt(folder, state, params, progress)
        base, basis = self.export_envelope(state, limit, folder)
        while True:
            claim = self._claim(identity)
            if claim['state'] == 'CACHE_HIT': body, receipt = claim['payload']['body'], claim['payload']['receipt']
            elif claim['state'] != 'CLAIMED':
                state['r4_export_retry_status'] = claim; atomic_json(folder / 'job.json', state)
                return {'export_status': claim['state'], 'retry': claim, 'progress': progress}
            else:
                try: upper = self._reserve_page(state, claim, base, basis)
                except BaseException:
                    self.reads.abandon_before_dispatch(claim['attempt_id'], 'BUDGET_OR_EVIDENCE_DEFERRED')
                    self._cancel_raw_before_transport('BUDGET_OR_EVIDENCE_DEFERRED');raise
                state.update(reserved_export=str(upper), r4_export_envelope=basis); atomic_json(folder / 'job.json', state)
                accounting = {'logical_job_id': state['logical_job_id'], 'export_upper_after': str(upper), 'old_attempt_risk_released': False}
                self.reads.mark_dispatched(claim['attempt_id'], accounting=accounting)
                with self.db.connection() as db: db.execute('UPDATE r4_dune_exports SET dispatched=1 WHERE attempt_id=?', (claim['attempt_id'],))
                self._active_export_dispatch = True
                try: body, receipt = self.call('results', state['execution_id'], params=params)
                finally: self._active_export_dispatch = False
                directory = folder / 'r4_reads' / claim['attempt_id']
                atomic_json(directory / 'response.json', body); atomic_json(directory / 'receipt.json', receipt)
                try:
                    validate_page(body, execution_id=state['execution_id'], offset=offset, limit=limit, progress=progress,
                                  status_metadata=state['status_response']['result_metadata'], receipt=receipt, parameters=params)
                except PageContractError:
                    failure = self._failure(receipt, body)
                    self.reads.finish(claim['attempt_id'], failure['outcome'], error_class=failure['error_class'], retry_after=failure.get('retry_after'), receipt=receipt)
                    continue
                self.reads.finish(claim['attempt_id'], 'SUCCESS', payload=_portable({'body': body, 'receipt': receipt}), receipt=receipt)
            validated = validate_page(body, execution_id=state['execution_id'], offset=offset, limit=limit, progress=progress,
                                      status_metadata=state['status_response']['result_metadata'], receipt=receipt, parameters=params)
            # Distinct immutable recovery artifacts preserve failed R3 page files.
            directory = folder / 'r4_verified' / logical_key(identity)
            for name, value in (('page.json', body), ('receipt.json', receipt)):
                target = directory / name
                if target.exists() and read(target) != _portable(value): raise RequestBlocked('Cached recovery artifact conflict')
                if not target.exists(): atomic_json(target, value)
            state.setdefault('r4_verified_pages', {})[str(offset)] = {'page_path': (directory / 'page.json').relative_to(self.w).as_posix(),
                'receipt_path': (directory / 'receipt.json').relative_to(self.w).as_posix(), 'page_sha256': sha(directory / 'page.json'),
                'receipt_sha256': sha(directory / 'receipt.json'), 'logical_read_key': logical_key(identity)}
            state.update(verified_export_progress=validated, export_status='COMPLETED_DECLARED_RESULT_ROWS' if validated['complete'] else 'PARTIAL_CONTIGUOUS_EXPORT')
            atomic_json(folder / 'job.json', state)
            # A larger valid server metadata envelope increases risk immediately.
            base_after, basis_after = self.export_envelope(state, limit, folder)
            with self.db.connection() as db:
                per_after = Decimal(basis_after['per_request_upper_credits'])
                inherited_row = db.execute('SELECT inherited_export_risk FROM r4_dune_export_baselines WHERE job=?', (state['logical_job_id'],)).fetchone()
                inherited = Decimal(inherited_row[0]) if inherited_row else Decimal(0)
                extras = sum((max(Decimal(r[0]), per_after) if Decimal(r[0]) else Decimal(0)
                              for r in db.execute('SELECT increment FROM r4_dune_exports WHERE job=?', (state['logical_job_id'],))), Decimal(0))
            self.db.reserve_export(state['logical_job_id'], max(base_after, inherited) + extras,
                                   {**basis_after, 'inherited_export_risk_preserved': str(inherited), 'additional_recovery_risk': str(extras)}, observed=True)
            with self.db.connection() as db:
                retained = db.execute('SELECT export_risk FROM r2_components WHERE job=?', (state['logical_job_id'],)).fetchone()[0]
            return {'export_status': state['export_status'], 'progress': validated, 'cache_reused': claim['state'] == 'CACHE_HIT',
                    'upper_not_actual': retained, 'cumulative': self._budget_snapshot()['dune_credits']}

    def _unknown_posts(self):
        for row in self.attempts.unresolved(self.account_context_ref):
            if json.loads(row['identity']).get('operation') == 'execute': return True
        with self.db.connection() as db:
            return bool(db.execute("SELECT 1 FROM r4_dune_sql WHERE state='SUBMITTING_OR_UNCERTAIN'").fetchone())

    def submit(self, sqlpath, label, kind='context', freeze_manifest=None, performance='medium', *, _retry=None):
        self.require_gate(); self.ensure_not_halted()
        allowed_kinds = self.runtime.allowed_kinds if self.runtime is not None else ('context',)
        if kind not in allowed_kinds or performance not in ('small', 'medium'): raise ValueError('Only authorized finite Small/Medium work')
        sqlpath = self._inside(sqlpath); freeze_manifest = self._inside(freeze_manifest)
        (self.runtime.verify_sql_freeze if self.runtime is not None else verify_frozen_scope)(freeze_manifest, self.w, sql_path=sqlpath)
        self._preflight()
        if self.deadline is not None and self.clock() + 30 > self.deadline: raise RuntimeError('Context clock insufficient before SQL dispatch')
        sql = sqlpath.read_text(encoding='utf-8'); digest = hashlib.sha256(sql.encode()).hexdigest(); frozen = read(freeze_manifest)
        if self._unknown_posts(): raise RequestBlocked('Unknown SQL creation remains; recover its execution, never re-POST')
        usage = read(self.w / 'private/current_dune_usage.json')['billing_period']; today = datetime.now(timezone.utc).date().isoformat()
        if not usage['start_date'] <= today < usage['end_date']: raise RuntimeError('Current included allowance evidence required')
        retry_number = 1 if _retry else 0
        folder = self.w / 'private/dune_r2_jobs' / (digest + ('_r4_retry1' if _retry else ''))
        if folder.exists(): raise RequestBlocked('SQL already submitted; reuse saved execution')
        job = 'dune_r4:' + digest + (':retry1' if _retry else '')
        root_job = _retry['root_job'] if _retry else job
        with self.db.connection() as db:
            if db.execute('SELECT 1 FROM r4_dune_sql WHERE root_job=? AND retry_number=?', (root_job, retry_number)).fetchone(): raise RequestBlocked('Only one SQL retry per original execution')
        execution_cap = self.db.execution_cap() if hasattr(self.db, 'execution_cap') else Decimal(20)
        self._prepare_raw()
        try:self.db.reserve_dune_job(job, label, str(execution_cap), '0')
        except Exception:
            self._cancel_raw_before_transport('SQL_BUDGET_DEFERRED');raise
        folder.mkdir(parents=True); (folder / 'query.sql').write_text(sql, encoding='utf-8', newline='\n')
        state = {'logical_job_id': job, 'query_label': label, 'kind': kind, 'sql_sha256': digest, 'performance': performance,
            'reserved_execution': str(execution_cap), 'reserved_export': '0', 'export_requests': 0, 'export_offsets': [],
            'state': 'SUBMITTING_OR_UNCERTAIN', 'account_context_ref': self.account_context_ref, 'authorization_id': AUTH,
            'scope_freeze_path': freeze_manifest.relative_to(self.w).as_posix(), 'scope_freeze_sha256': sha(freeze_manifest),
            'export_plan': frozen['export_plan'], 'root_job': root_job, 'retry_number': retry_number, 'retry_reason': _retry.get('reason') if _retry else None}
        if getattr(self.runtime, 'ledger_factory', None) is not None:
            active = self.db.snapshot()['dune_credits']
            state.update(cumulative_budget_authorization_id=active['authorization_id'], cumulative_cap_credits=active['cap'])
            if hasattr(self.db, 'submission_policy'):
                state.update(submission_execution_cap_credits=str(execution_cap), submission_policy=self.db.submission_policy(job))
        atomic_json(folder / 'job.json', state)
        with self.db.connection() as db:
            db.execute('INSERT INTO r4_dune_sql VALUES(?,?,?,?,?,?)', (job, root_job, retry_number, folder.relative_to(self.w).as_posix(), state['state'], state['retry_reason']))
        if self.runtime is not None: self._budget_snapshot()
        self._active_sql_dispatch = True
        try: body, receipt = self.call('execute', payload={'sql': sql, 'performance': performance})
        finally: self._active_sql_dispatch = False
        atomic_json(folder / 'submit_response.json', body); atomic_json(folder / 'submit_receipt.json', receipt)
        state.update(submit_response=_portable(body), submit_receipt=receipt)
        if receipt.get('http_status') == 200 and not receipt.get('error_class') and isinstance(body, dict) and re.fullmatch('[A-Z0-9]{26}', str(body.get('execution_id', ''))):
            state.update(execution_id=body['execution_id'], state=body.get('state', 'QUERY_STATE_PENDING'))
            self.observe_execution_charge(state, body, receipt)
            with self.db.connection() as db: db.execute('UPDATE r4_dune_sql SET state=? WHERE job=?', ('ACCEPTED_EXECUTION_ID', job))
        atomic_json(folder / 'job.json', state)
        return {'job_folder': str(folder), 'execution_id': state.get('execution_id'), 'state': state['state']}

    def retry_failed(self, folder, sqlpath, label, freeze_manifest, reason, retry_class):
        state = read(self._inside(folder) / 'job.json')
        if state.get('state') != 'QUERY_STATE_FAILED' or not state.get('execution_id') or self.db.final_execution_cost(state['logical_job_id']) is None:
            raise RequestBlocked('Only known terminal failed SQL may receive one reasoned retry')
        if state.get('retry_number', 0) or state.get('retry_of'): raise RequestBlocked('Retry lineage already consumed')
        error = (state.get('status_response') or {}).get('error') or {}
        message = json.dumps(error).lower()
        if any(x in message for x in ('credit limit', 'credit cost', 'max credit', 'execution cap')): raise RequestBlocked('Account cap failure does not authorize repeated SQL')
        if not isinstance(reason, str) or not reason.strip(): raise ValueError('Concrete optimization/transient reason required')
        if retry_class == 'OPTIMIZED_SQL':
            if sha(self._inside(sqlpath)) == state['sql_sha256']: raise ValueError('Optimization retry must have changed SQL')
        elif retry_class == 'CONFIRMED_TRANSIENT':
            if not classify_failure(provider_error=error)['retryable']: raise ValueError('Terminal error does not evidence a transient failure')
        else: raise ValueError('Explicit supported SQL retry reason class required')
        return self.submit(sqlpath, label, freeze_manifest=freeze_manifest,
            _retry={'root_job': state.get('root_job', state['logical_job_id']), 'reason': reason})

    def settle(self, folder):
        folder = self._inside(folder); state = read(folder / 'job.json')
        actual = self.db.final_execution_cost(state['logical_job_id'])
        if actual is None or state.get('state') not in TERMINAL: raise RuntimeError('Terminal known charge required')
        exported = state.get('state') == 'QUERY_STATE_COMPLETED' and self.export_progress(folder, state)['complete']
        with self.db.connection() as db:
            recovery = db.execute('SELECT 1 FROM r4_dune_exports WHERE job=? AND increment!=?', (state['logical_job_id'], '0')).fetchone()
        if recovery:
            return {'settlement_status': 'EXPORTED_WITH_RETAINED_ATTEMPT_RISK' if exported else 'PARTIAL_WITH_RETAINED_ATTEMPT_RISK',
                    'known_execution_actual': str(actual), 'export_actual': None, 'old_attempt_risk_released': False,
                    'cumulative': self._budget_snapshot()['dune_credits']}
        if state.get('request_set_closed'): return {'settlement_status': state['settlement_status'], 'already_closed': True, 'known_execution_actual': str(actual)}
        if state['state'] == 'QUERY_STATE_COMPLETED' and not exported: raise RuntimeError('Incomplete export retains open risk')
        receipt = state.get('status_receipt') or state['submit_receipt']
        evidence = {'request_set_closed': True, 'no_unknown_attempts': True, 'source_receipt_sha256': receipt['sha256']}
        if exported:
            _, basis = self.export_envelope(state, self.export_progress(folder, state)['page_size'], folder)
            evidence.update(full_page_chain_verified=True, rate_evidence=basis['rate_evidence'], metering_basis=basis)
        status = self.db.close_job(state['logical_job_id'], exported=exported, evidence=evidence)
        state.update(request_set_closed=True, settlement_status=status); atomic_json(folder / 'job.json', state)
        return {'settlement_status': status, 'known_execution_actual': str(actual), 'export_actual': None if exported else '0', 'cumulative': self._budget_snapshot()['dune_credits']}


ContextDune = ContextDuneR4


def execute_dune(work, probe, freeze, label, resume=False):
    work, freeze = Path(work).resolve(), Path(freeze).resolve()
    verify_frozen_scope(freeze, work)
    with session(work, probe, label) as timing:
        live = ContextDuneR4(work, deadline=timing['deadline'])
        if resume:
            folder = work / 'private/dune_r2_jobs' / read(freeze)['sql_sha256']
            state = read(folder / 'job.json')
            if not state.get('execution_id'): raise RequestBlocked('Unknown SQL creation cannot be resubmitted')
        else:
            result = live.submit(freeze.parent / 'query.sql', label, freeze_manifest=freeze)
            folder = Path(result['job_folder'])
            if not result.get('execution_id'): return result
        timing['record']['job_folder'] = folder.relative_to(work).as_posix()
        while read(folder / 'job.json')['state'] not in TERMINAL:
            result = live.poll(folder)
            if result.get('retry_status'): return result
            if read(folder / 'job.json')['state'] not in TERMINAL:
                if time.time() + 35 > timing['deadline']: return {'status': 'DEFERRED_CONTEXT_CLOCK', 'job_folder': str(folder)}
                time.sleep(5)
        state = read(folder / 'job.json')
        if state['state'] != 'QUERY_STATE_COMPLETED': return {'status': state['state'], 'settlement': live.settle(folder), 'job_folder': str(folder)}
        progress = live.export_progress(folder, state)
        while not progress['complete']:
            result = live.export(folder, offset=progress['next_offset'])
            if result.get('retry'): return result
            progress = result['progress']
        return {'status': 'COMPLETED_EXPORTED', 'job_folder': str(folder), 'settlement': live.settle(folder)}


def dune_usage(work, label='r4_usage'):
    with session(work, 'SHARED', label) as timing: return ContextDuneR4(work, deadline=timing['deadline']).usage()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('dune', 'usage'))
    parser.add_argument('--work', type=Path, required=True); parser.add_argument('--probe', default='SHARED')
    parser.add_argument('--freeze', type=Path); parser.add_argument('--label', default='r4_context'); parser.add_argument('--resume', action='store_true')
    args = parser.parse_args(argv)
    result = dune_usage(args.work, args.label) if args.action == 'usage' else execute_dune(args.work, args.probe, args.freeze, args.label, args.resume)
    print(json.dumps(result, indent=2, default=str))
    return 0 if (result.get('status') == 'COMPLETED_EXPORTED' or args.action == 'usage' and result.get('http_status') == 200) else 1


if __name__ == '__main__': raise SystemExit(main())
