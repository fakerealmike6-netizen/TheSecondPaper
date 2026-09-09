"""Finite Stage1D BigQuery schema/dry-run reads; no actual query execution API."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
from stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha
import re
import time
from decimal import Decimal

from context_access_r3 import read, sha, canonical
from context_access_r4 import db_path, retry_path
from page_attempts import atomic_json
from read_retry_r4 import ReadRetryStore, logical_key, retry_after_seconds
from stage1d_bq_auth_recovery import classify_bigquery_failure
from stage1d_bq_sql_guard import sql_code
from stage1d_runtime import Runtime, RAW_LIMIT

AUTH = 'STAGE1D_MULTI_PROVIDER_ROUTING_V1'
JOB_SCAN_CAP = 1073741824
TOTAL_SCAN_CAP = 5368709120
RESPONSE_BOUND = 8388608
AUTH_RESPONSE_BOUND = 65536
TABLE = re.compile(r'[a-z][a-z0-9-]*\.[A-Za-z_][A-Za-z_0-9]*\.[A-Za-z_][A-Za-z_0-9]*')


def inside(work, value):
    path = Path(value); path = path if path.is_absolute() else work/path
    path = path.resolve()
    if not path.is_relative_to(work): raise ValueError('BigQuery evidence path escapes work')
    return path


def config_check(work, config):
    if config.get('authorization_id') != AUTH: raise ValueError('Current routing authority required')
    source = config['authority_source']; path = inside(work, source['path'])
    if sha(path) != source['sha256'] or AUTH not in path.read_text(encoding='utf-8-sig'):
        raise ValueError('Routing authority evidence differs')
    if not re.fullmatch('[a-z][a-z0-9-]{4,61}[a-z0-9]', config.get('project', '')):
        raise ValueError('Configured existing BigQuery project required')
    if config.get('location') != 'US': raise ValueError('This finite mainnet harness expects the existing US location')
    if not config.get('tables') or any(not TABLE.fullmatch(t) for t in config['tables']):
        raise ValueError('Exact known table allowlist required')


def dry_spec(work, spec, config, query):
    path = inside(work, spec['sql_path'])
    if sha(path) != spec['sql_sha256']: raise ValueError('Dry-run SQL changed')
    sql = path.read_text(encoding='utf-8')
    text = sql_code(sql).strip()
    if not re.match(r'(?:SELECT|WITH)\b', text, re.I) or ';' in text or re.search(
            r'\b(?:INSERT|DELETE|UPDATE|CREATE|DROP|ALTER|MERGE|CALL|EXPORT|EXECUTE|EXTERNAL_QUERY)\b', text, re.I):
        raise ValueError('One read-only non-federated query required')
    referenced = set(re.findall(r'`([^`]+)`', sql))
    if not referenced or not referenced.issubset(set(config['tables'])):
        raise ValueError('Dry-run references outside exact configured tables')
    if spec.get('query_id') != query['query_id'] or spec.get('scope_hash') != query['scope_hash']:
        raise ValueError('Dry-run must bind the unchanged authorized query scope')
    checked = set()
    for dependency in spec.get('schema_evidence', []):
        p = inside(work, dependency['path'])
        if sha(p) != dependency['sha256']: raise ValueError('Schema evidence changed')
        envelope = read(p); response = envelope.get('result', {})
        if envelope.get('status') != 'SUCCESS_VALIDATED' or response.get('kind') != 'SCHEMA':
            raise ValueError('Actual successful table metadata is required before dry-run')
        checked.add(response['table'])
    if not referenced.issubset(checked): raise ValueError('Each referenced table needs actual schema evidence')
    for dependency in spec.get('scope_dependencies', []):
        if sha(inside(work, dependency['path'])) != dependency['sha256']:
            raise ValueError('Frozen context gap dependency changed')
    if not spec.get('scope_dependencies'): raise ValueError('Frozen current context gap dependency required')
    return sql


def dry_result(metadata, snapshot):
    if metadata.get('configuration', {}).get('dryRun') is not True:
        raise ValueError('SDK returned a non-dry-run job')
    statistics = metadata.get('statistics', {})
    value = statistics.get('totalBytesProcessed', statistics.get('query', {}).get('totalBytesProcessed'))
    if isinstance(value, bool) or not re.fullmatch(r'[0-9]+', str(value)):
        raise ValueError('Dry-run exact nonnegative byte estimate missing')
    estimate = int(value); row = snapshot['bigquery_bytes']
    remaining = max(0, int(row['cap']) - int(row['actual']) - int(row['reserved']))
    from stage1d_recovery_policy import AUTH as RECOVERY_AUTH, BQ_TOTAL, BQ_JOB, bq_reservation
    recovery=row.get('authorization_id')==RECOVERY_AUTH
    total_cap,job_cap=(BQ_TOTAL,BQ_JOB) if recovery else (TOTAL_SCAN_CAP,JOB_SCAN_CAP)
    if int(row['cap']) != total_cap: raise ValueError('Unexpected BigQuery project cap')
    if recovery:remaining=0 if row['remaining'] is None else int(Decimal(row['remaining']))
    return {'kind': 'DRY_RUN', 'estimated_processed_bytes': estimate, 'actual_scanned_bytes': 0,
        'maximum_bytes_billed_per_execution': job_cap, 'project_cap_bytes': total_cap,
        'project_remaining_after_existing_risk': remaining,
        'within_single_job_cap': estimate <= job_cap, 'within_project_cap_remaining': estimate <= remaining,
        'provider_allowance_confirmed_in_ledger': row.get('confirmed_allowance') is not None,
        'actual_query_executed': False, 'estimate_is_not_actual_billing': True,
        'minimum_single_job_cap_if_over': estimate if estimate > job_cap else None,
        'metadata': metadata}


class GoogleSdk:
    """Normal OAuth ADC and official SDK; bounded HTTP, SDK retries disabled."""
    rpc_operation_reservation = 2  # At most one auth HTTP plus one data HTTP.
    def __init__(self, config):
        import google.auth
        from google.auth.transport.requests import AuthorizedSession, Request
        from google.cloud import bigquery
        import requests
        def deny_adc_setup_network(*args, **kwargs):
            raise RuntimeError('ADC setup network is outside dispatched bounded operation')
        credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'],
                                             request=deny_adc_setup_network)
        owner = self; self.transport_evidence = []
        self.credential_type = {'module': type(credentials).__module__, 'qualname': type(credentials).__qualname__}
        self.network_evidence = []; self.auth_retry_after = None
        self.auth_http_calls = 0; self.data_http_calls = 0
        class RecordingAdapter(requests.adapters.HTTPAdapter):
            def __init__(self, kind):
                self.kind = kind
                super().__init__(max_retries=0)
            def send(self, request, **kwargs):
                counter = 'auth_http_calls' if self.kind == 'AUTH' else 'data_http_calls'
                if getattr(owner, counter) >= 1:
                    raise RuntimeError('SDK internal repeated HTTP suppressed before dispatch')
                setattr(owner, counter, getattr(owner, counter) + 1)
                started = time.monotonic()
                item = {'kind': self.kind, 'method': request.method, 'started_at': time.time(),
                    'http_status': None, 'elapsed_seconds': None, 'adapter_retries': 0,
                    'redirects_followed': 0, 'request_details_recorded': False,
                    'response_body_recorded': False}
                owner.network_evidence.append(item)
                try:
                    response = super().send(request, **kwargs)
                    item['http_status'] = response.status_code
                    return response
                except Exception as exc:
                    item['exception_type'] = {'module': type(exc).__module__, 'qualname': type(exc).__qualname__}
                    raise
                finally:
                    item['elapsed_seconds'] = max(0.0, time.monotonic() - started)
        class AuthSession(requests.Session):
            def request(self, method, url, **kwargs):
                kwargs.update(stream=True, allow_redirects=False)
                response = super().request(method, url, **kwargs)
                body = b''; body_started = time.monotonic()
                try:
                    for chunk in response.iter_content(chunk_size=16384):
                        body += chunk[:AUTH_RESPONSE_BOUND + 1 - len(body)]
                        if len(body) > AUTH_RESPONSE_BOUND:
                            raise ValueError('Bounded auth response exceeded')
                    response._content = body; response._content_consumed = True
                    if owner.network_evidence:
                        owner.network_evidence[-1]['response_bytes_consumed_not_stored'] = len(body)
                    # Do not let OAuth's own retry loop consume hidden attempts.
                    # Request converts this typed HTTP error into TransportError;
                    # the ordinary persisted logical retry policy handles it.
                    if response.status_code in {408, 429, 500, 502, 503, 504}:
                        owner.auth_retry_after = retry_after_seconds(response.headers.get('Retry-After'), time.time())
                        raise requests.exceptions.HTTPError('Transient auth HTTP response', response=response)
                    return response
                finally:
                    if owner.network_evidence:
                        owner.network_evidence[-1]['body_read_elapsed_seconds'] = max(0.0, time.monotonic() - body_started)
                    response.close()
        self.auth_http = AuthSession()
        for scheme in ('http://', 'https://'):
            self.auth_http.mount(scheme, RecordingAdapter('AUTH'))
        class BoundedSession(AuthorizedSession):
            def request(self, method, url, *args, **kwargs):
                kwargs['stream'] = True
                kwargs['allow_redirects'] = False
                item = {'status': None, 'body': b'', 'complete': False, 'retry_after': None}
                owner.transport_evidence.append(item)
                response = super().request(method, url, *args, **kwargs)
                item.update(status=response.status_code, retry_after=response.headers.get('Retry-After'))
                body_started = time.monotonic()
                try:
                    for chunk in response.iter_content(chunk_size=65536):
                        room = RESPONSE_BOUND + 1 - len(item['body'])
                        item['body'] += chunk[:room]
                        if len(item['body']) > RESPONSE_BOUND: raise ValueError('BoundedBigQueryResponseTooLarge')
                    item['complete'] = True
                    response._content = item['body']; response._content_consumed = True
                    return response
                finally:
                    if owner.network_evidence:
                        owner.network_evidence[-1]['body_read_elapsed_seconds'] = max(0.0, time.monotonic() - body_started)
                    response.close()
        self.http = BoundedSession(credentials, refresh_status_codes=(), max_refresh_attempts=0,
                                   auth_request=Request(self.auth_http))
        for scheme in ('http://', 'https://'):
            self.http.mount(scheme, RecordingAdapter('BIGQUERY_DATA'))
        self.client = bigquery.Client(project=config['project'], location=config['location'],
                                     credentials=credentials, _http=self.http)
        self.bq = bigquery

    def schema(self, table):
        value = self.client.get_table(table, retry=None, timeout=30)
        reference = value.to_api_repr().get('tableReference', {})
        if '.'.join(reference.get(k, '') for k in ('projectId', 'datasetId', 'tableId')) != table:
            raise ValueError('Actual BigQuery metadata table identity differs')
        return {'kind': 'SCHEMA', 'table': table, 'metadata': value.to_api_repr(),
            'schema': [field.to_api_repr() for field in value.schema], 'location': value.location,
            'table_type': value.table_type, 'partition': value.time_partitioning.to_api_repr() if value.time_partitioning else None,
            'range_partition': value.range_partitioning.to_api_repr() if value.range_partitioning else None,
            'clustering_fields': value.clustering_fields, 'num_rows_metadata_not_download': value.num_rows}

    def dry_run(self, sql, key):
        job_config = self.bq.QueryJobConfig(dry_run=True, use_query_cache=False, use_legacy_sql=False,
                                           maximum_bytes_billed=getattr(self, 'maximum_bytes_billed', JOB_SCAN_CAP))
        job = self.client.query(sql, job_config=job_config, job_id='stage1d_dry_'+key,
                                retry=None, job_retry=None, timeout=30)
        # QueryJob.to_api_repr serializes submission fields, not returned statistics.
        # The exact server body is retained by BoundedSession; read official SDK statistics explicitly.
        return {**job.to_api_repr(), 'statistics': {'totalBytesProcessed': str(job.total_bytes_processed)},
                'status': {'state': job.state}, 'server_statistics_via_official_sdk_properties': True}

    def close(self):
        try: self.client.close()
        finally: self.auth_http.close()


def probe(work, query_name, config_path, action, *, table=None, spec_path=None, runtime=None,
          sdk_factory=GoogleSdk, clock=time.time, sleep=time.sleep, clock_query=None):
    if clock_query not in (None, query_name, 'SHARED'):
        raise ValueError('Clock owner must be original query or existing SHARED')
    work = Path(work).resolve(); config_path = inside(work, config_path); config = read(config_path)
    config_check(work, config)
    query = next(q for q in active_batch(work)['queries'] if q['name'] == query_name)
    sql = None
    if action == 'schema':
        if table not in config['tables']: raise ValueError('Table outside finite metadata allowlist')
        selectors = {'table': table}
    elif action == 'dry-run':
        spec_path = inside(work, spec_path); spec = read(spec_path); sql = dry_spec(work, spec, config, query)
        from stage1d_cost_request_guard import validate_bq_candidate_request
        validate_bq_candidate_request(work, query, spec)
        selectors = {'sql_sha256': spec['sql_sha256'], 'spec_sha256': sha(spec_path), 'scope_hash': query['scope_hash']}
    else: raise ValueError('Only schema and dry-run are implemented')
    if sdk_factory is GoogleSdk:
        # Missing runtime libraries are a local setup issue, not a dispatched API attempt.
        from google.cloud import bigquery
        import google.auth
    runtime = runtime or Runtime(); store = getattr(runtime,'retry_store',ReadRetryStore)(retry_path(work), clock=clock)
    ledger = runtime.ledger_factory(db_path(work))
    identity = {'provider': 'GOOGLE_BIGQUERY_EXISTING_ADC', 'method': 'tables.get' if action == 'schema' else 'dry_run',
        'project': config['project'], 'location': config['location'], **selectors}
    with runtime.session(work, clock_query or query_name, 'bigquery_'+action.replace('-', '_'), synthetic=sdk_factory is not GoogleSdk) as timing:
        while True:
            claim = store.claim(identity, deadline=timing['deadline'])
            if claim['state'] == 'CACHE_HIT':
                artifact = inside(work, claim['receipt']['artifact_path'])
                if sha(artifact) != claim['receipt']['artifact_sha256']: raise ValueError('BigQuery cache evidence changed')
                return {'status': 'SUCCESS_VALIDATED', 'cache_hit': True, 'result': claim['payload'], **claim['receipt']}
            if claim['state'] == 'DEFERRED' and claim['next_eligible_at']+30 < timing['deadline']:
                sleep(min(30, max(0, claim['next_eligible_at']-clock()))); continue
            if claim['state'] != 'CLAIMED': return {'status': claim['state'], 'complete': False}
            job = 'bq_read_'+claim['attempt_id']; reserved = False
            operation_bound = getattr(sdk_factory, 'rpc_operation_reservation', 1)
            try:
                if runtime.raw_risk(work)+RESPONSE_BOUND+1 > (runtime.raw_limit(work) if getattr(runtime, 'raw_limit', None) else RAW_LIMIT): raise RuntimeError('Cumulative raw risk cap')
                if clock()+30*operation_bound > timing['deadline']: raise RuntimeError('Online time insufficient for metadata read')
                ledger.reserve(job, 'bigquery', 'Stage1D '+action, {'rpc_operations': operation_bound}); reserved = True
            except Exception:
                if reserved: ledger.settle(job, {'rpc_operations': 0})
                store.abandon_before_dispatch(claim['attempt_id'], 'BIGQUERY_READ_RESOURCE_DEFERRED'); raise
            try: sdk = sdk_factory(config)
            except Exception as exc:
                ledger.settle(job, {'rpc_operations': 0})
                store.abandon_before_dispatch(claim['attempt_id'], 'BIGQUERY_SDK_ADC_SETUP_FAILED')
                return {'status': 'SDK_ADC_SETUP_FAILED', 'error_class': type(exc).__name__,
                    'messages_withheld': True, 'bigquery_api_dispatched': False}
            raw_reserved=False
            try:
                if getattr(runtime,'reserve_raw',None):raw_reserved=runtime.reserve_raw(work,job,3*RESPONSE_BOUND+65536)
                sdk.maximum_bytes_billed=int(ledger.snapshot()['bigquery_bytes'].get('single_job_cap_bytes',JOB_SCAN_CAP))
            except Exception:
                sdk.close();ledger.settle(job,{'rpc_operations':0})
                store.abandon_before_dispatch(claim['attempt_id'],'BIGQUERY_RAW_RESOURCE_DEFERRED')
                if raw_reserved:
                    local_receipt=work/'logs'/(job+'_not_dispatched.json')
                    atomic_json(local_receipt,{'job':job,'http_transport_called':False,'reason':'LOCAL_STORAGE_SETUP_FAILED'})
                    runtime.close_raw(work,job,local_receipt)
                raise
            store.mark_dispatched(claim['attempt_id'], accounting={'job': job, 'rpc_operations': operation_bound,
                'bigquery_bytes': 0, 'basis': 'Pre-dispatch bound for metadata/dry-run plus bounded auth; no scanned data query'})
            result = None; failure = None
            try:
                result = sdk.schema(table) if action == 'schema' else dry_result(sdk.dry_run(sql, logical_key(identity)), ledger.snapshot())
                if action == 'schema' and (result.get('kind') != 'SCHEMA' or result.get('table') != table):
                    raise ValueError('Schema response table identity differs')
            except Exception as exc:
                code = getattr(exc, 'code', None)
                failure = classify_bigquery_failure(exc, http_status=int(code) if isinstance(code, int) and not isinstance(code, bool) else None,
                    transport_evidence=getattr(sdk, 'transport_evidence', []))
                if getattr(sdk, 'auth_retry_after', None):
                    failure['retry_after'] = sdk.auth_retry_after
            finally:
                if sdk is not None: sdk.close()
            directory = work/'raw/stage1d_bigquery'/claim['attempt_id']; directory.mkdir(parents=True, exist_ok=False)
            transport = getattr(sdk, 'transport_evidence', []) if sdk else []
            raw_sources = []
            for i, response in enumerate(transport):
                body_path = directory/('response_'+str(i)+'.bin'); body_path.write_bytes(response['body'])
                raw_sources.append({'path': body_path.relative_to(work).as_posix(), 'sha256': sha(body_path),
                    'bytes': len(response['body']), 'http_status': response['status'], 'complete': response['complete']})
                if not response['complete']:
                    atomic_json(work/'private/context_uncertainty'/('bigquery_'+claim['attempt_id']+'.json'),
                        {'additional_raw_risk_bytes': max(0, RESPONSE_BOUND-len(response['body'])),
                         'basis': 'Uncertain bounded BigQuery metadata/dry-run response',
                         'receipt_path': (directory/'receipt.json').relative_to(work).as_posix(),
                         'provider': 'GOOGLE_BIGQUERY_EXISTING_ADC'})
            if len(transport) > 1:
                # SDK retries were explicitly disabled; keep every received byte and the unknown reservation.
                atomic_json(directory/'unexpected_transport_count.json', {'count': len(transport), 'raw_sources': raw_sources})
                raise RuntimeError('Unexpected SDK data retry; preserve attempt unknown for review')
            artifact = directory/'receipt.json'
            network = getattr(sdk, 'network_evidence', None)
            actual_operations = len(network) if network is not None else 1
            if actual_operations > operation_bound: raise RuntimeError('Bounded SDK HTTP count exceeded reservation')
            envelope = {'status': 'SUCCESS_VALIDATED' if failure is None else failure['error_class'],
                'identity': identity, 'attempt_id': claim['attempt_id'], 'attempt_no': claim['attempt_no'],
                'job': job, 'result': result, 'raw_sources': raw_sources, 'config_sha256': sha(config_path),
                'actual_query_executed': False, 'sdk_error_messages_withheld': True,
                'failure_classification': failure, 'network_evidence': network,
                'credential_type': getattr(sdk, 'credential_type', None),
                'network_operation_accounting': {'reserved': operation_bound, 'observed': actual_operations,
                    'basis': 'Actual bounded adapter dispatches' if network is not None else 'Injected SDK logical-operation fixture',
                    'auth_payloads_or_headers_saved': False}}
            atomic_json(artifact, envelope); ledger.settle(job, {'rpc_operations': actual_operations})
            if getattr(runtime, 'close_raw', None): runtime.close_raw(work, job, artifact)
            receipt = {'artifact_path': artifact.relative_to(work).as_posix(), 'artifact_sha256': sha(artifact), 'job': job}
            retry_after = failure.get('retry_after') if failure else None
            if retry_after is None and transport:
                retry_after = transport[-1].get('retry_after')
            store.finish(claim['attempt_id'], failure['outcome'] if failure else 'SUCCESS', payload=result,
                receipt=receipt, error_class=failure['error_class'] if failure else None,
                retry_after=retry_after)
            if failure is None: return {'status': 'SUCCESS_VALIDATED', 'cache_hit': False, 'result': result, **receipt}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('schema', 'dry-run')); parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--query', required=True); parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--table'); parser.add_argument('--spec', type=Path); args = parser.parse_args()
    result = probe(args.work, args.query, args.config, args.action, table=args.table, spec_path=args.spec)
    print(json.dumps({k: v for k, v in result.items() if k != 'result'}, indent=2))


if __name__ == '__main__': main()
