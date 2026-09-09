"""Bounded classic BigQuery jobs and temporary result pages on the shared ledger.

Uses the existing official ADC transport. Each call has one data HTTP dispatch,
at most one authentication dispatch, and retained read/fee evidence. SQL creation
is never retried after uncertain submission; recovery reads the same job ID.
"""
from datetime import datetime, timezone
from contextlib import closing
import hashlib
import json
from pathlib import Path
from stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha
import re
import sqlite3
import time

from context_access_r3 import read, sha, now, canonical
from context_access_r4 import db_path, retry_path
from page_attempts import atomic_json
from read_retry_r4 import ReadRetryStore, logical_key, _process_alive
from stage1d_bigquery_probe import GoogleSdk, config_check, inside, RESPONSE_BOUND
from stage1d_bq_auth_recovery import classify_bigquery_failure
from stage1d_bq_sql_guard import sql_code
from stage1d_runtime import Runtime


def request_spec(project, operation, selectors, body=None):
    if not re.fullmatch(r'[a-z][a-z0-9-]{4,61}[a-z0-9]', project):
        raise ValueError('Existing project ID required')
    base = '/projects/' + project
    selectors = dict(selectors)
    if operation == 'jobs.list':
        if set(selectors) - {'minCreationTime', 'maxCreationTime', 'pageToken', 'allUsers'}:
            raise ValueError('Unexpected monthly job-list selector')
        return 'GET', base + '/jobs', dict(selectors, projection='full', maxResults=100), None
    if operation in ('jobs.get', 'jobs.getQueryResults'):
        job_id = selectors.pop('job_id')
        if not re.fullmatch(r'[A-Za-z0-9_\-]{1,1024}', job_id):
            raise ValueError('Fixed job ID required')
        selectors.pop('poll_sequence', None)
        allowed = {'pageToken', 'startIndex', 'maxResults', 'timeoutMs'} if operation.endswith('getQueryResults') else set()
        if set(selectors) - allowed:
            raise ValueError('Unexpected job read selector')
        suffix = '/queries/' if operation.endswith('getQueryResults') else '/jobs/'
        return 'GET', base + suffix + job_id, dict(selectors, location='US'), None
    if operation == 'jobs.insert':
        if selectors or not isinstance(body, dict):
            raise ValueError('Exact job body required')
        reference = body.get('jobReference', {})
        configuration = body.get('configuration', {})
        if reference.get('projectId') != project or reference.get('location') != 'US':
            raise ValueError('Job project/location changed')
        if set(configuration) != {'query'} or set(configuration['query']) - {
                'query', 'useLegacySql', 'useQueryCache', 'maximumBytesBilled', 'parameterMode', 'queryParameters'}:
            raise ValueError('Only read query temporary results are allowed')
        if configuration['query'].get('useLegacySql') is not False:
            raise ValueError('GoogleSQL required')
        sql = configuration['query'].get('query')
        if not isinstance(sql, str):
            raise ValueError('Exact SQL text required')
        text = sql_code(sql).strip()
        if not re.match(r'(?:SELECT|WITH)\b', text, re.I) or ';' in text or re.search(
                r'\b(?:INSERT|DELETE|UPDATE|CREATE|DROP|ALTER|MERGE|CALL|EXPORT|EXECUTE|EXTERNAL_QUERY)\b', text, re.I):
            raise ValueError('One read-only non-federated query required')
        if not re.fullmatch(r'[A-Za-z0-9_\-]{1,1024}', str(reference.get('jobId', ''))):
            raise ValueError('Fixed job ID required')
        if not re.fullmatch(r'[0-9]+', str(configuration['query'].get('maximumBytesBilled'))):
            raise ValueError('Maximum billed bytes required')
        return 'POST', base + '/jobs', None, body
    raise ValueError('Unsupported BigQuery operation')


def call(work, query_name, config, operation, selectors, *, body=None, runtime=None, sdk_factory=GoogleSdk, clock_query=None):
    if clock_query not in (None, query_name, 'SHARED'):
        raise ValueError('Clock owner must be original query or existing SHARED')
    work = Path(work).resolve()
    config_check(work, config)
    method, path, params, data = request_spec(config['project'], operation, selectors, body)
    runtime = runtime or Runtime()
    identity = {'provider': 'GOOGLE_BIGQUERY_EXISTING_ADC', 'method': operation,
                'project': config['project'], 'location': 'US', 'selectors': selectors}
    if body is not None:
        identity['body_sha256'] = hashlib.sha256(canonical(body)).hexdigest()
    store = runtime.retry_store(retry_path(work))
    ledger = runtime.ledger_factory(db_path(work))
    with runtime.session(work, clock_query or query_name, 'recovery_' + operation.replace('.', '_'),
                         synthetic=sdk_factory is not GoogleSdk) as timing:
        while True:
            claim = store.claim(identity, deadline=timing['deadline'])
            if claim['state'] == 'CACHE_HIT':
                receipt = claim['receipt']
                if sha(inside(work, receipt['artifact_path'])) != receipt['artifact_sha256']:
                    raise ValueError('BigQuery saved read changed')
                return {'status': 'SUCCESS_VALIDATED', 'result': claim['payload'], 'cache_hit': True, **receipt}
            if claim['state'] == 'DEFERRED' and operation != 'jobs.insert':
                delay = max(0, claim['next_eligible_at'] - time.time())
                if time.time() + delay + 60 >= timing['deadline']:
                    return {'status': 'DEFERRED', 'identity': identity}
                time.sleep(min(delay, 30))
                continue
            if claim['state'] != 'CLAIMED':
                return {'status': claim['state'], 'identity': identity}
            if (operation == 'jobs.insert' and claim['attempt_no'] != 1
                    and any(attempt.get('dispatched_at') is not None for attempt in store.attempts(identity))):
                store.abandon_before_dispatch(claim['attempt_id'], 'UNCERTAIN_SQL_REQUIRES_SAME_JOB_GET')
                return {'status': 'SUBMISSION_RECOVERY_REQUIRES_JOB_GET', 'identity': identity}
            job = 'bq_http_' + claim['attempt_id']
            try:
                raw_limit = runtime.raw_limit(work)
                if runtime.raw_risk(work) + RESPONSE_BOUND + 1 > raw_limit:
                    raise RuntimeError('Current disk resource cap reached')
                if time.time() + 120 > timing['deadline']:
                    raise RuntimeError('Online clock cannot cover bounded HTTP')
                ledger.reserve(job, 'bigquery', operation, {'rpc_operations': 2})
            except Exception as exc:
                store.abandon_before_dispatch(claim['attempt_id'], 'BIGQUERY_RESOURCE_DEFERRED')
                return {'status': 'BIGQUERY_RESOURCE_DEFERRED', 'dispatched': False,
                        'identity': identity, 'error_class': type(exc).__name__,
                        'attempt_id': claim['attempt_id']}
            try:
                sdk = sdk_factory(config)
            except Exception as exc:
                ledger.settle(job, {'rpc_operations': 0})
                store.abandon_before_dispatch(claim['attempt_id'], 'BIGQUERY_ADC_LOCAL_SETUP_FAILED')
                return {'status': 'SDK_ADC_SETUP_FAILED', 'error_class': type(exc).__name__, 'dispatched': False}
            try:
                runtime.reserve_raw(work, job, 3 * RESPONSE_BOUND + 65536)
            except Exception as exc:
                sdk.close()
                ledger.settle(job, {'rpc_operations': 0})
                store.abandon_before_dispatch(claim['attempt_id'], 'BIGQUERY_DISK_RESERVATION_DEFERRED')
                return {'status': 'BIGQUERY_DISK_RESERVATION_DEFERRED', 'dispatched': False,
                        'identity': identity, 'error_class': type(exc).__name__,
                        'attempt_id': claim['attempt_id']}
            store.mark_dispatched(claim['attempt_id'], accounting={'job': job, 'rpc_operations': 2,
                'ethereum_methods': 0, 'bigquery_metadata_or_results': 1, 'auth_upper': 1})
            result = None
            failure = None
            try:
                result = sdk.client._connection.api_request(method=method, path=path,
                    query_params=params, data=data, timeout=(10, 60))
                if not isinstance(result, dict):
                    raise ValueError('BigQuery response object required')
            except Exception as exc:
                status = getattr(exc, 'code', None)
                failure = classify_bigquery_failure(exc,
                    http_status=int(status) if isinstance(status, int) and not isinstance(status, bool) else None,
                    transport_evidence=sdk.transport_evidence)
                if getattr(sdk, 'auth_retry_after', None):
                    failure['retry_after'] = sdk.auth_retry_after
            finally:
                sdk.close()
            directory = work / 'raw/stage1d_bigquery_jobs' / claim['attempt_id']
            directory.mkdir(parents=True, exist_ok=False)
            sources = []
            for index, response in enumerate(sdk.transport_evidence):
                destination = directory / ('response_' + str(index) + '.bin')
                destination.write_bytes(response['body'])
                sources.append({'path': destination.relative_to(work).as_posix(), 'sha256': sha(destination),
                    'bytes': destination.stat().st_size, 'complete': response['complete'], 'http_status': response['status']})
            network = sdk.network_evidence
            if len(network) > 2 or len(sources) > 1:
                raise RuntimeError('Bounded SDK unexpectedly dispatched more requests; preserve reservation')
            artifact = directory / 'receipt.json'
            envelope = {'status': 'SUCCESS_VALIDATED' if failure is None else failure['error_class'],
                'identity': identity, 'attempt_id': claim['attempt_id'], 'attempt_no': claim['attempt_no'],
                'result': result, 'raw_sources': sources, 'failure': failure,
                'network_evidence': network, 'auth_bodies_or_headers_saved': False,
                'ethereum_methods': 0, 'utc': now()}
            atomic_json(artifact, envelope)
            runtime.close_raw(work, job, artifact)
            ledger.settle(job, {'rpc_operations': len(network)})
            receipt = {'artifact_path': artifact.relative_to(work).as_posix(), 'artifact_sha256': sha(artifact)}
            store.finish(claim['attempt_id'], failure['outcome'] if failure else 'SUCCESS', payload=result,
                         receipt=receipt, error_class=failure['error_class'] if failure else None,
                         retry_after=failure.get('retry_after') if failure else None)
            if failure is None:
                return {'status': 'SUCCESS_VALIDATED', 'result': result, 'cache_hit': False, **receipt}
            if operation == 'jobs.insert' or not failure.get('retryable'):
                return {'status': failure['error_class'], 'failure': failure, **receipt}


def metadata_complete(job):
    """Only settled known job types can close a monthly metadata observation."""
    statistics = job.get('statistics', {})
    configuration = job.get('configuration', {})
    if (not re.fullmatch(r'[0-9]+', str(statistics.get('creationTime')))
            or job.get('status', {}).get('state') != 'DONE'):
        return False
    if 'query' in configuration or statistics.get('query'):
        return bool(re.fullmatch(r'[0-9]+', str(statistics.get('query', {}).get('totalBytesBilled'))))
    return any(kind in configuration for kind in ('load', 'copy', 'extract'))


def monthly_usage(jobs, start_ms, end_ms):
    """Use full job metadata and avoid script parent/child double-counting."""
    identities = set()
    total = 0
    pending = []
    records = []
    counted_parents = set()
    for job in jobs:
        reference = job.get('jobReference', {})
        statistics = job.get('statistics', {})
        query = statistics.get('query', {})
        billed = query.get('totalBytesBilled')
        created = statistics.get('creationTime')
        if (query.get('statementType') == 'SCRIPT' and created is not None
                and start_ms <= int(created) < end_ms and job.get('status', {}).get('state') == 'DONE'
                and re.fullmatch(r'[0-9]+', str(billed))):
            counted_parents.add((reference.get('projectId'), reference.get('jobId'), reference.get('location', 'US')))
    for job in jobs:
        reference = job.get('jobReference', {})
        identity = (reference.get('projectId'), reference.get('jobId'), reference.get('location', 'US'))
        if identity in identities:
            continue
        identities.add(identity)
        statistics = job.get('statistics', {})
        created = statistics.get('creationTime')
        if created is None:
            pending.append({'job': reference, 'reason': 'CREATION_TIME_MISSING'})
            continue
        if not start_ms <= int(created) < end_ms:
            continue
        query = statistics.get('query', {})
        parent = statistics.get('parentJobId')
        if parent and (reference.get('projectId'), parent, reference.get('location', 'US')) in counted_parents:
            records.append({'job': reference, 'counted': False, 'reason': 'INCLUDED_IN_KNOWN_TERMINAL_SCRIPT_PARENT_TOTAL'})
            continue
        if 'query' not in job.get('configuration', {}) and not query:
            if not any(kind in job.get('configuration', {}) for kind in ('load', 'copy', 'extract')):
                pending.append({'job': reference, 'reason': 'JOB_TYPE_UNRESOLVED'})
            continue
        billed = query.get('totalBytesBilled')
        if job.get('status', {}).get('state') != 'DONE' or billed is None:
            pending.append({'job': reference, 'reason': 'UNSETTLED_OR_MISSING_BILLING'})
        else:
            if not re.fullmatch(r'[0-9]+', str(billed)):
                raise ValueError('Invalid billed-byte metadata')
            total += int(billed)
            records.append({'job': reference, 'billed_bytes': int(billed), 'creation_time_ms': int(created), 'counted': True})
    return {'known_billed_bytes': total, 'pending_jobs': pending, 'records': records,
            'complete_settlement': not pending, 'parent_script_not_double_counted': True}


def month_inventory(work, query_name, config):
    work = Path(work).resolve()
    stamp = datetime.now(timezone.utc)
    start = stamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_ms, end_ms = int(start.timestamp() * 1000), int(stamp.timestamp() * 1000)
    inventory_path = work / 'private/stage1d_bigquery_monthly_inventory_state.json'
    previous = read(inventory_path) if inventory_path.exists() else {}
    # A failed list/page resumes the same frozen range and logical request key.
    # Only a successfully closed listing permits a later normal observation.
    if (previous.get('project') == config['project'] and previous.get('month') == start.strftime('%Y-%m')
            and previous.get('list_complete') is False):
        start_ms, end_ms = previous['start_ms'], previous['end_ms']
    inventory = {'project': config['project'], 'month': start.strftime('%Y-%m'),
                 'start_ms': start_ms, 'end_ms': end_ms, 'list_complete': False}
    atomic_json(inventory_path, inventory)
    selector = {'minCreationTime': str(start_ms), 'maxCreationTime': str(end_ms), 'allUsers': True}
    pages = []
    jobs = []
    tokens = set()
    while True:
        response = call(work, query_name, config, 'jobs.list', selector)
        pages.append({k: v for k, v in response.items() if k != 'result'})
        if response['status'] != 'SUCCESS_VALIDATED':
            return {'status': 'MONTH_METADATA_PARTIAL', 'pages': pages, 'user_unused_confirmation_retained': True}
        document = response['result']
        jobs.extend(document.get('jobs', []))
        token = document.get('nextPageToken')
        if not token:
            break
        if token in tokens:
            raise ValueError('BigQuery monthly list page cycle')
        tokens.add(token)
        selector = dict(selector, pageToken=token)
    inventory['list_complete'] = True
    atomic_json(inventory_path, inventory)
    detailed = []
    for job in jobs:
        # jobs.list projection=full does not guarantee all billing statistics.
        reference = job['jobReference']
        if metadata_complete(job):
            detailed.append(job)
            continue
        key = hashlib.sha256(canonical({'project': config['project'], 'job_id': reference['jobId'],
                                        'location': 'US'})).hexdigest()
        metadata_path = work / 'private/stage1d_bigquery_monthly_metadata' / (key + '.json')
        observation = read(metadata_path) if metadata_path.exists() else {'poll_sequence': 0}
        response = call(work, query_name, config, 'jobs.get',
                        {'job_id': reference['jobId'], 'poll_sequence': observation['poll_sequence']})
        pages.append({k: v for k, v in response.items() if k != 'result'})
        observation['last_read'] = {k: v for k, v in response.items() if k != 'result'}
        if response['status'] == 'SUCCESS_VALIDATED':
            result = response['result']
            if result.get('jobReference', {}).get('jobId') != reference['jobId']:
                raise ValueError('Monthly metadata returned a different job')
            detailed.append(result)
            if not metadata_complete(result):
                observation['poll_sequence'] += 1
        else:
            detailed.append(job)
        atomic_json(metadata_path, observation)
    usage = monthly_usage(detailed, start_ms, end_ms)
    result = {'status': 'MONTH_METADATA_COMPLETE' if usage['complete_settlement'] else 'MONTH_METADATA_PARTIAL',
              'month': start.strftime('%Y-%m'), 'as_of_utc': datetime.fromtimestamp(end_ms / 1000, timezone.utc).isoformat(), 'project': config['project'],
              'start_ms': start_ms, 'end_ms': end_ms, 'jobs_listed': len(jobs), 'usage': usage, 'pages': pages,
              'fee_query_scans': 0, 'all_users_requested': True}
    atomic_json(Path(work) / 'private/stage1d_bigquery_monthly_usage.json', result)
    return result


def decode_rows(document):
    """Preserve REST INTEGER/NUMERIC values as strings and reject truncation."""
    schema = document.get('schema', {}).get('fields', [])
    rows = document.get('rows', [])
    if rows and not schema:
        raise ValueError('Result schema missing')
    def value(cell, field):
        raw = cell.get('v')
        if raw is None:
            return None
        if field.get('mode') == 'REPEATED':
            return [value(item, dict(field, mode='NULLABLE')) for item in raw]
        if field['type'] in ('RECORD', 'STRUCT'):
            return convert(raw, field.get('fields', []))
        if field['type'] in ('BOOLEAN', 'BOOL'):
            if type(raw) is bool:
                return raw
            if isinstance(raw, str) and raw in ('true', 'false'):
                return raw == 'true'
            raise ValueError('Invalid boolean')
        return raw
    def convert(row, fields):
        cells = row.get('f', [])
        if len(cells) != len(fields):
            raise ValueError('Result row arity differs from schema')
        return {field['name']: value(cell, field) for cell, field in zip(cells, fields)}
    return [convert(row, schema) for row in rows]


def undispatched_submission(work, config, body_sha256):
    """Read existing attempt evidence; absence is valid only for this harness.

    The harness persists mark_dispatched before every HTTP call. A live or young
    in-flight owner is inconclusive. A dead, expired, never-dispatched owner can
    use ordinary retry-store recovery without erasing any old attempt.
    """
    identity = {'provider': 'GOOGLE_BIGQUERY_EXISTING_ADC', 'method': 'jobs.insert',
                'project': config['project'], 'location': 'US', 'selectors': {},
                'body_sha256': body_sha256}
    path = Path(retry_path(work))
    if not path.exists():
        return {'basis': 'NO_RETRY_DATABASE_BEFORE_HARNESS_DISPATCH', 'logical_key': logical_key(identity)}
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as database:
        database.row_factory = sqlite3.Row
        if not database.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='read_attempts'").fetchone():
            return {'basis': 'NO_RETRY_TABLE_BEFORE_HARNESS_DISPATCH', 'logical_key': logical_key(identity)}
        attempts = [dict(row) for row in database.execute(
            'SELECT * FROM read_attempts WHERE logical_key=? ORDER BY attempt_no,created_at',
            (logical_key(identity),))]
    if any(row.get('dispatched_at') is not None for row in attempts):
        return None
    for row in attempts:
        if row.get('outcome') == 'IN_FLIGHT' and (
                _process_alive(row['owner_pid']) or time.time() < row['created_at'] + 60):
            return None
    return {'basis': 'BOUND_ATTEMPTS_NEVER_DISPATCHED_NO_LIVE_OWNER',
            'logical_key': logical_key(identity), 'attempt_ids': [row['attempt_id'] for row in attempts]}


def execute_plan(work, query_name, config, plan_path, *, clock_query=None):
    """Execute a SHA-bound successful dry-run plan, then close all result pages."""
    from stage1d_bigquery_probe import dry_spec
    from stage1d_recovery_policy import bq_reservation
    if clock_query not in (None, query_name, 'SHARED'):
        raise ValueError('Clock owner must be original query or existing SHARED')
    work = Path(work).resolve()
    plan_path = inside(work, plan_path)
    plan = read(plan_path)
    query = next(q for q in active_batch(work)['queries'] if q['name'] == query_name)
    spec_path = inside(work, plan['dry_spec_path'])
    dry_receipt = inside(work, plan['dry_receipt_path'])
    if sha(spec_path) != plan['dry_spec_sha256'] or sha(dry_receipt) != plan['dry_receipt_sha256']:
        raise ValueError('Frozen BQ plan evidence changed')
    spec = read(spec_path)
    sql = dry_spec(work, spec, config, query)
    dry = read(dry_receipt)
    if dry.get('status') != 'SUCCESS_VALIDATED' or dry.get('result', {}).get('kind') != 'DRY_RUN':
        raise ValueError('Actual successful dry-run required')
    if dry['identity'].get('sql_sha256') != spec['sql_sha256']:
        raise ValueError('Dry-run SQL identity differs')
    estimate = dry['result']['estimated_processed_bytes']
    ledger = Runtime().ledger_factory(db_path(work))
    digest = hashlib.sha256(canonical({'sql_sha256': spec['sql_sha256'],
        'project': config['project'], 'location': 'US'})).hexdigest()
    job_id = 'stage1d_recovery_' + digest[:48]
    folder = work / 'private/stage1d_bigquery_jobs' / digest
    state_path = folder / 'job.json'
    if state_path.exists():
        state = read(state_path)
        if state['job_id'] != job_id or state['sql_sha256'] != spec['sql_sha256']:
            raise ValueError('Existing job differs from logical SQL identity')
    else:
        reservation = bq_reservation(estimate, ledger.snapshot())
        bound = reservation['maximum_bytes_billed']
        if not isinstance(bound, int):
            raise ValueError('Exact integer BQ reservation required')
        folder.mkdir(parents=True, exist_ok=False)
        state = {'job_id': job_id, 'sql_sha256': spec['sql_sha256'],
                 'plan_path': plan_path.relative_to(work).as_posix(), 'plan_sha256': sha(plan_path),
                 'estimate_bytes': estimate, 'maximum_bytes_billed': bound,
                 'reservation_rule': reservation,
                 'scan_ledger_job': 'bq_scan_' + digest, 'state': 'PREPARED', 'pages': [],
                 'poll_sequence': 0, 'created_at_utc': now()}
        atomic_json(state_path, state)
    if state['state'] == 'COMPLETE_EXPORTED':
        output = inside(work, state['rows_path'])
        if sha(output) != state['rows_sha256']:
            raise ValueError('Saved BQ complete rows changed')
        return state
    body = {'jobReference': {'projectId': config['project'], 'jobId': job_id, 'location': 'US'},
            'configuration': {'query': {'query': sql, 'useLegacySql': False, 'useQueryCache': True,
                 'maximumBytesBilled': str(state['maximum_bytes_billed'])}}}
    body_sha256 = hashlib.sha256(canonical(body)).hexdigest()
    if state['state'] == 'SUBMISSION_INTENT':
        if state.get('submission_body_sha256') != body_sha256:
            raise ValueError('Interrupted submission body changed')
        proof = undispatched_submission(work, config, body_sha256)
        if proof is not None:
            state['undispatched_recovery'] = proof
            state['state'] = 'PREPARED'
            atomic_json(state_path, state)
    if state['state'] == 'PREPARED':
        from stage1d_cost_request_guard import validate_bq_candidate_request
        validate_bq_candidate_request(work, query, spec)
        with ledger.connection() as database:
            reserved = database.execute('SELECT reserved FROM amounts WHERE job=? AND unit=?',
                (state['scan_ledger_job'], 'bigquery_bytes')).fetchone()
        if reserved is None:
            ledger.reserve(state['scan_ledger_job'], 'bigquery', 'classic necessary ledger scan',
                           {'bigquery_bytes': state['maximum_bytes_billed']})
        elif int(reserved[0]) != state['maximum_bytes_billed']:
            raise ValueError('Existing scan reservation differs')
        state['state'] = 'SUBMISSION_INTENT'
        state['submission_body_sha256'] = body_sha256
        atomic_json(state_path, state)
        response = call(work, query_name, config, 'jobs.insert', {}, body=body, clock_query=clock_query)
        state['submission_receipt'] = {k: v for k, v in response.items() if k != 'result'}
        state['state'] = ('SUBMITTED' if response['status'] == 'SUCCESS_VALIDATED' else
                          'PREPARED' if response.get('dispatched') is False else 'SUBMISSION_UNCERTAIN')
        atomic_json(state_path, state)
        if state['state'] == 'PREPARED':
            return state
    # A resumed uncertain submission only gets this same job. No alternate ID.
    while state['state'] not in ('DONE', 'QUERY_FAILED'):
        response = call(work, query_name, config, 'jobs.get',
                        {'job_id': job_id, 'poll_sequence': state['poll_sequence']}, clock_query=clock_query)
        state['last_job_read'] = {k: v for k, v in response.items() if k != 'result'}
        atomic_json(state_path, state)
        if response['status'] != 'SUCCESS_VALIDATED':
            return state
        job = response['result']
        if job.get('jobReference', {}).get('jobId') != job_id:
            raise ValueError('Provider returned different BQ job')
        if job.get('status', {}).get('state') != 'DONE':
            state['poll_sequence'] += 1
            atomic_json(state_path, state)
            with Runtime().session(work, clock_query or query_name, 'bigquery_normal_status_wait'):
                time.sleep(5)
            continue
        terminal_path = folder / 'terminal_job.json'
        atomic_json(terminal_path, job)
        state['terminal_job_sha256'] = sha(terminal_path)
        state['state'] = 'QUERY_FAILED' if job.get('status', {}).get('errorResult') else 'DONE'
        billed = job.get('statistics', {}).get('query', {}).get('totalBytesBilled')
        if billed is not None and re.fullmatch(r'[0-9]+', str(billed)):
            with ledger.connection() as database:
                actual = database.execute('SELECT actual FROM amounts WHERE job=? AND unit=?',
                    (state['scan_ledger_job'], 'bigquery_bytes')).fetchone()
            if actual[0] is None:
                ledger.settle(state['scan_ledger_job'], {'bigquery_bytes': int(billed)})
            elif int(actual[0]) != int(billed):
                raise ValueError('Final BQ billed-byte evidence changed')
            state['actual_billed_bytes'] = int(billed)
        else:
            state['actual_billed_bytes'] = None
            state['scan_reservation_preserved'] = True
        atomic_json(state_path, state)
    if state['state'] == 'QUERY_FAILED':
        return state
    token = state['pages'][-1].get('next_page_token') if state['pages'] else None
    closed = bool(state['pages'] and not token)
    seen = {p.get('page_token') for p in state['pages'] if p.get('page_token')}
    while not closed:
        selectors = {'job_id': job_id, 'maxResults': 1000, 'timeoutMs': 10000}
        if token:
            selectors['pageToken'] = token
        response = call(work, query_name, config, 'jobs.getQueryResults', selectors, clock_query=clock_query)
        if response['status'] != 'SUCCESS_VALIDATED':
            state['last_export_read'] = {k: v for k, v in response.items() if k != 'result'}
            atomic_json(state_path, state)
            return state
        document = response['result']
        if document.get('jobComplete') is not True:
            raise ValueError('DONE job result unexpectedly incomplete')
        if document.get('jobReference', {}).get('jobId') != job_id:
            raise ValueError('Result page job identity differs')
        total = document.get('totalRows')
        if not re.fullmatch(r'[0-9]+', str(total)):
            raise ValueError('Result totalRows missing')
        if 'total_rows' in state and state['total_rows'] != int(total):
            raise ValueError('Result totalRows changed across pages')
        state['total_rows'] = int(total)
        if document.get('schema'):
            if 'schema' in state and state['schema'] != document['schema']:
                raise ValueError('Result schema changed')
            state['schema'] = document['schema']
        document = dict(document, schema=state.get('schema', {}))
        decoded = decode_rows(document)
        destination = folder / ('rows_' + str(len(state['pages'])).zfill(6) + '.jsonl')
        encoded_rows = ''.join(json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n' for row in decoded)
        if destination.exists():
            if destination.read_bytes() != encoded_rows.encode('utf-8'):
                raise ValueError('Interrupted page bytes differ from verified response')
        else:
            destination.write_text(encoded_rows, encoding='utf-8', newline='\n')
        next_token = document.get('pageToken')
        if next_token and (next_token == token or next_token in seen):
            raise ValueError('BQ result page cycle')
        if token:
            seen.add(token)
        state['pages'].append({'page_token': token, 'next_page_token': next_token, 'row_count': len(decoded),
            'rows_path': destination.relative_to(work).as_posix(), 'rows_sha256': sha(destination),
            'response_receipt': {k: v for k, v in response.items() if k != 'result'}})
        atomic_json(state_path, state)
        token = next_token
        closed = not token
    if sum(p['row_count'] for p in state['pages']) != state['total_rows']:
        raise ValueError('BQ pages ended before exact totalRows')
    output = folder / 'all_rows.jsonl'
    temporary = folder / 'all_rows.jsonl.partial'
    with temporary.open('wb') as target:
        for page in state['pages']:
            source = inside(work, page['rows_path'])
            if sha(source) != page['rows_sha256']:
                raise ValueError('Saved page changed')
            with source.open('rb') as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    target.write(chunk)
    temporary.replace(output)
    state.update(state='COMPLETE_EXPORTED', rows_path=output.relative_to(work).as_posix(),
                 rows_sha256=sha(output), complete_page_chain=True, completed_at_utc=now())
    atomic_json(state_path, state)
    return state
