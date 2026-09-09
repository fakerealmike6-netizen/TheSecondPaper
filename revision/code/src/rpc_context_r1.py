"""Finite, explicitly planned PublicNode evidence calls. No retries or discovery.

The caller owns the cumulative raw-data review and the offline-repair gate.
Every request (including each batch member) consumes one inherited RPC unit.
Injected transports are for offline tests; they must never produce REAL_CHAIN
catalogue candidates. A transport exception is an attempted operation, not zero.
"""
import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
import urllib.error
import urllib.request

from budget_r1 import AUTH, RevisionLedger, now
from legacy_guard_r4 import reject_legacy_workspace, reject_unscoped_legacy_transport

ENDPOINT = 'https://ethereum-rpc.publicnode.com'
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
RAW_CAP = 512 * 1024 * 1024
TIMEOUT_SECONDS = 30
ROLES = {'eth_getCode': 'historical_code', 'debug_traceTransaction': 'trace',
         'eth_getTransactionReceipt': 'receipt', 'eth_getTransactionByHash': 'transaction',
         'eth_getBalance': 'balance_context'}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def bytes_sha(value):
    return hashlib.sha256(value).hexdigest()


def write_new(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as handle:
        handle.write(data)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def http_transport(requests, max_bytes, *, work=None):
    """One HTTP attempt; read at most the maximum plus an overflow sentinel."""
    reject_unscoped_legacy_transport('rpc_context_r1.http_transport', work=work, module_file=__file__)
    request = urllib.request.Request(ENDPOINT, data=encoded(requests), method='POST',
                                     headers={'Content-Type': 'application/json'})
    opener = urllib.request.build_opener(NoRedirect())
    try:
        with opener.open(request, timeout=TIMEOUT_SECONDS) as response:
            return int(response.status), response.read(max_bytes + 1)
    except urllib.error.HTTPError as error:
        # HTTPError includes the redirect rejection; do not follow Location.
        with error:
            return int(error.code), error.read(max_bytes + 1)


def validate_plan(method, params):
    if method not in ROLES or not isinstance(params, list):
        raise ValueError('Unsupported explicit RPC method or params')
    params = json.loads(json.dumps(params))
    def hex_exact(value, size):
        return isinstance(value, str) and re.fullmatch('0x[0-9a-fA-F]{' + str(size) + '}', value)
    if method in ('eth_getCode', 'eth_getBalance'):
        if len(params) != 2 or not hex_exact(params[0], 40):
            raise ValueError('One exact address and historical block required')
        tag = params[1]
        if isinstance(tag, dict):
            if (set(tag) - {'blockHash', 'requireCanonical'} or not hex_exact(tag.get('blockHash'), 64)
                    or ('requireCanonical' in tag and tag['requireCanonical'] is not True)):
                raise ValueError('Exact canonical EIP-1898 block hash required')
        elif not isinstance(tag, str) or not re.fullmatch(r'0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)', tag):
            raise ValueError('Latest/pending and unfixed block tags are forbidden')
    elif method == 'debug_traceTransaction':
        if (len(params) != 2 or not hex_exact(params[0], 64)
                or params[1] != {'tracer': 'callTracer', 'tracerConfig': {'withLog': True}}):
            raise ValueError('Only exact transaction callTracer withLog is authorized')
    elif len(params) != 1 or not hex_exact(params[0], 64):
        raise ValueError('One exact transaction hash required')
    params[0] = params[0].lower()
    if method in ('eth_getCode', 'eth_getBalance'):
        if isinstance(params[1], dict):
            params[1]['blockHash'] = params[1]['blockHash'].lower()
            # Omission and true mean the same permitted canonical request here.
            params[1]['requireCanonical'] = True
        else:
            params[1] = hex(int(params[1], 16))
    return {'method': method, 'params': params}


def response_status(request, response):
    if (not isinstance(response, dict) or response.get('jsonrpc') != '2.0'
            or type(response.get('id')) is not type(request['id']) or response.get('id') != request['id']
            or ('result' in response) == ('error' in response)):
        return 'INVALID_RPC_BINDING'
    if 'error' in response:
        return 'RPC_ERROR'
    value = response['result']
    method = request['method']
    if value is None:
        return 'NULL_RESULT_NOT_EVIDENCE'
    if method in ('eth_getCode', 'eth_getBalance'):
        pattern = r'0x(?:[0-9a-fA-F]{2})*' if method == 'eth_getCode' else r'0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)'
        if not isinstance(value, str) or not re.fullmatch(pattern, value):
            return 'INVALID_RPC_RESULT'
    elif not isinstance(value, dict):
        return 'INVALID_RPC_RESULT'
    elif method in ('eth_getTransactionReceipt', 'eth_getTransactionByHash'):
        key = 'transactionHash' if method == 'eth_getTransactionReceipt' else 'hash'
        if str(value.get(key, '')).lower() != request['params'][0].lower():
            return 'INVALID_RPC_BINDING'
    return 'SUCCESS_VALIDATED'


class RpcContextClient:
    def __init__(self, work, *, transport=None, max_response_bytes=MAX_RESPONSE_BYTES):
        reject_legacy_workspace(work, 'rpc_context_r1.RpcContextClient')
        self.work = Path(work).resolve()
        ledger_path = self.work / 'private/shared_budget_r1.sqlite'
        if not ledger_path.is_file():
            raise RuntimeError('Initialized revision ledger is required; no new allowance is created')
        # Inspect read-only BEFORE invoking the ledger constructor which creates tables.
        with closing(sqlite3.connect(ledger_path.as_uri() + '?mode=ro', uri=True)) as db:
            meta = dict(db.execute('SELECT key,value FROM r1_meta'))
            cap = db.execute("SELECT cap FROM limits WHERE unit='rpc_operations'").fetchone()
            if (meta.get('authorization_id') != AUTH or not meta.get('legacy_snapshot_sha256')
                    or not cap or int(cap[0]) != 500):
                raise RuntimeError('Inherited RPC ledger/authorization/cumulative cap is unavailable')
        self.ledger = RevisionLedger(ledger_path)
        if type(max_response_bytes) is not int or not 0 < max_response_bytes <= MAX_RESPONSE_BYTES:
            raise ValueError('Response bound must be between 1 byte and 8 MiB')
        self.max_bytes = int(max_response_bytes)
        self.transport = (lambda requests, bound: http_transport(requests, bound, work=self.work)) if transport is None else transport
        self.evidence_kind = 'REAL_CHAIN' if transport is None else 'SYNTHETIC_TRANSPORT'
        with self.ledger.connection() as db:
            db.execute('CREATE TABLE IF NOT EXISTS rpc_context_intents(identity TEXT PRIMARY KEY,job TEXT UNIQUE,request_json TEXT,review_sha256 TEXT,review_cumulative_bytes INTEGER,status TEXT,raw_bytes INTEGER,utc TEXT,receipt_path TEXT)')

    def call(self, method, params, resource_review):
        return self.call_batch([{'method': method, 'params': params}], resource_review, batch=False)

    def call_batch(self, plans, resource_review, *, batch=True):
        """One finite plan, max four explicit members. Never enumerate neighbors."""
        reject_legacy_workspace(self.work, 'rpc_context_r1.RpcContextClient.call_batch')
        if not isinstance(plans, list) or not 1 <= len(plans) <= 4:
            raise ValueError('A finite plan of 1 to 4 requests is required')
        plans = [validate_plan(p['method'], p['params']) for p in plans]
        if len({digest(p) for p in plans}) != len(plans):
            raise ValueError('Duplicate logical requests in batch')
        review = dict(resource_review)
        reviewed_plans = review.get('requests', [{'method': review.get('method'), 'params': review.get('params')}])
        reviewed_plans = [validate_plan(p.get('method'), p.get('params')) for p in reviewed_plans]
        raw_so_far = review.get('cumulative_raw_bytes')
        if (review.get('schema_version') != 'rpc-resource-review-1' or review.get('approved') is not True
                or not review.get('review_id') or review.get('offline_repair_gate_passed') is not True
                or review.get('cumulative_raw_cap_bytes') != RAW_CAP or reviewed_plans != plans
                or type(raw_so_far) is not int or raw_so_far < 0
                or raw_so_far + self.max_bytes + 1 > RAW_CAP):
            raise RuntimeError('Fresh exact-plan cumulative resource/gate review is required')
        identity = digest({'provider': ENDPOINT, 'chain_id': 1, 'plans': plans})
        job = 'rpc_r1_' + identity[:24]
        requests = [{'jsonrpc': '2.0', 'id': job + '_' + str(i), **p} for i, p in enumerate(plans)]
        request_body = requests if batch else requests[0]
        directory = self.work / 'raw/rpc_context_r1' / job
        with self.ledger.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM rpc_context_intents WHERE identity=?', (identity,)).fetchone():
                raise RuntimeError('Logical plan already dispatched, blocked or uncertain; automatic retry prohibited')
            old = db.execute('SELECT request_json,review_cumulative_bytes,raw_bytes,status FROM rpc_context_intents').fetchall()
            for previous, previous_total, previous_size, state in old:
                # Reordering or switching batch/single cannot replay an earlier member.
                existing = json.loads(previous)
                if any(p in existing for p in plans):
                    raise RuntimeError('Logical RPC request already recorded; automatic retry prohibited')
                if state in ('INTENT_RECORDED', 'DISPATCH_INTENT'):
                    raise RuntimeError('Prior unknown dispatch requires explicit offline reconciliation')
                if raw_so_far < previous_total + (previous_size or 0):
                    raise RuntimeError('Cumulative raw review omits prior RPC payloads')
            db.execute('INSERT INTO rpc_context_intents VALUES(?,?,?,?,?,?,?,?,?)',
                       (identity, job, json.dumps(plans), digest(review), raw_so_far, 'INTENT_RECORDED', None, now(), None))
        directory.mkdir(parents=True, exist_ok=False)
        write_new(directory / 'intent.json', encoded({'job_id': job, 'identity_sha256': identity, 'request': request_body,
                  'resource_review': review, 'evidence_kind': self.evidence_kind, 'recorded_utc': now()}))
        try:
            self.ledger.reserve(job, 'publicnode', 'Explicit finite historical context RPC', {'rpc_operations': len(requests)})
        except Exception:
            self._state(identity, 'BLOCKED_BEFORE_DISPATCH', 0, None)
            raise
        self._state(identity, 'DISPATCH_INTENT', None, None)
        write_new(directory / 'dispatch_intent.json', encoded({'job_id': job, 'request': request_body,
                  'operation_reservation': len(requests), 'dispatch_intent_utc': now(), 'retry_allowed': False}))
        started = time.monotonic()
        status, raw, error = None, b'', None
        try:
            status, raw = self.transport(request_body, self.max_bytes)
            if not isinstance(raw, bytes):
                raise TypeError('Transport body must be bytes')
        except Exception as exc:
            error = type(exc).__name__
            raw = b''
        # The HTTP attempt occurred even if a timeout or transport error followed.
        self.ledger.settle(job, {'rpc_operations': len(requests)})
        overflow = len(raw) > self.max_bytes
        observed_lower_bound = len(raw)
        # A failed read may have consumed an unknown prefix. Retain the whole
        # read bound as resource risk; zero saved bytes is not zero acquisition.
        raw_resource_risk = self.max_bytes + 1 if error else observed_lower_bound
        raw = raw[:self.max_bytes]
        write_new(directory / 'response_body.bin', raw)
        parse_error = None
        decoded = None
        if not overflow and error is None:
            try:
                decoded = json.loads(raw)
            except (ValueError, UnicodeError) as exc:
                parse_error = type(exc).__name__
        common_state = ('RESPONSE_TOO_LARGE' if overflow else 'TRANSPORT_ERROR_NO_RETRY' if error
                        else 'HTTP_ERROR' if status != 200 else 'INVALID_JSON' if parse_error else None)
        responses = decoded if batch and isinstance(decoded, list) else [decoded] if not batch else []
        if common_state is None and (len(responses) != len(requests)
                or any(not isinstance(r, dict) for r in responses)
                or len({str(r.get('id')) for r in responses}) != len(responses)):
            common_state = 'INVALID_RPC_BINDING'
        members = []
        for i, request in enumerate(requests):
            matches = [r for r in responses if isinstance(r, dict) and r.get('id') == request['id']]
            response = matches[0] if len(matches) == 1 else None
            member_state = common_state or response_status(request, response)
            envelope = {'http_status': status, 'request': request, 'response': response,
                        'evidence_kind': self.evidence_kind, 'body_sha256': bytes_sha(raw),
                        'response_complete': not overflow and error is None}
            artifact = directory / ('envelope_' + str(i) + '.json')
            content = encoded(envelope)
            write_new(artifact, content)
            record = {'role': ROLES[request['method']], 'evidence_kind': self.evidence_kind,
                      'status': member_state, 'chain_id': 1, 'origin_url': ENDPOINT,
                      'artifact_path': artifact.relative_to(self.work).as_posix(),
                      'artifact_sha256': bytes_sha(content), 'request': request,
                      'payload_sha256': digest(response['result']) if member_state == 'SUCCESS_VALIDATED' else None}
            members.append(record)
        final_state = common_state or ('SUCCESS_VALIDATED' if all(m['status'] == 'SUCCESS_VALIDATED' for m in members) else 'RPC_MEMBERS_INCOMPLETE')
        receipt = {'schema_version': 'rpc-context-receipt-1', 'job_id': job, 'provider': 'publicnode',
                   'endpoint': ENDPOINT, 'evidence_kind': self.evidence_kind, 'status': final_state,
                   'request': request_body, 'started_utc': json.loads((directory / 'dispatch_intent.json').read_bytes())['dispatch_intent_utc'],
                   'finished_utc': now(), 'elapsed_seconds': round(time.monotonic() - started, 6),
                   'http_status': status, 'error_class': error, 'json_error_class': parse_error,
                   'rpc_operations_attempted': len(requests), 'rpc_operations_settled': len(requests),
                   'monetary_cost': None, 'monetary_cost_status': 'NOT_METERED_NO_PAID_ENTITLEMENT_INFERRED',
                   'raw_path': (directory / 'response_body.bin').relative_to(self.work).as_posix(),
                   'raw_bytes_saved': len(raw), 'observed_body_bytes_lower_bound': observed_lower_bound,
                   'raw_bytes_accounted_as_resource_risk': raw_resource_risk,
                   'raw_accounting_status': 'UPPER_BOUND_RESERVED_NOT_ACTUAL' if error else 'READ_BYTES_OBSERVED',
                   'raw_sha256': bytes_sha(raw), 'truncated': overflow, 'response_complete': not overflow and error is None,
                   'response_byte_limit': self.max_bytes, 'automatic_retry_allowed': False,
                   'resource_review_sha256': digest(review), 'cumulative_raw_bytes_before': raw_so_far,
                   'cumulative_raw_bytes_saved_after': raw_so_far + len(raw),
                   'cumulative_received_raw_bytes_lower_bound_after': raw_so_far + observed_lower_bound, 'members': members,
                   'cumulative_raw_resource_risk_after': raw_so_far + raw_resource_risk,
                   'intent_sha256': bytes_sha((directory / 'intent.json').read_bytes()),
                   'dispatch_intent_sha256': bytes_sha((directory / 'dispatch_intent.json').read_bytes())}
        receipt_path = directory / 'receipt.json'
        receipt_content = encoded(receipt)
        write_new(receipt_path, receipt_content)
        write_new(directory / 'receipt.json.sha256', (bytes_sha(receipt_content) + '  receipt.json\n').encode())
        self._state(identity, final_state, raw_resource_risk, receipt_path.relative_to(self.work).as_posix())
        return receipt

    def _state(self, identity, state, raw_bytes, receipt):
        with self.ledger.connection() as db:
            db.execute('UPDATE rpc_context_intents SET status=?,raw_bytes=?,receipt_path=? WHERE identity=?',
                       (state, raw_bytes, receipt, identity))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--method', choices=tuple(ROLES), required=True)
    parser.add_argument('--params-json-file', type=Path, required=True)
    parser.add_argument('--resource-check-file', type=Path, required=True)
    args = parser.parse_args()
    work = args.work.resolve()
    for path in (args.params_json_file, args.resource_check_file):
        if not path.resolve().is_relative_to(work):
            parser.error('Parameters and resource review must remain inside the revision workspace')
    receipt = RpcContextClient(work).call(args.method, json.loads(args.params_json_file.read_text(encoding='utf-8-sig')),
                                        json.loads(args.resource_check_file.read_text(encoding='utf-8-sig')))
    print(json.dumps({k: receipt[k] for k in ('job_id', 'status', 'http_status', 'rpc_operations_attempted',
                                             'raw_bytes_saved', 'truncated', 'raw_sha256')}, sort_keys=True))


if __name__ == '__main__':
    main()
