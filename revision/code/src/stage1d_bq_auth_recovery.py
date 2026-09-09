"""Staged exact Google-auth classification and append-only correction evidence.

This module makes no network requests and never touches a resource ledger.  Its
adoption API changes only the mutable request state after all safety/evidence
checks; original finalized attempt rows remain byte-for-byte unchanged.
"""
from pathlib import Path
from contextlib import closing
import hashlib
import json
import sqlite3
import time

from read_retry_r4 import classify_failure, logical_key

SCHEMA = 'stage1d-bq-auth-transport-classification-review-v1'
CORRECTED = 'GOOGLE_AUTH_TRANSPORT_ERROR'
TYPE = {'module': 'google.auth.exceptions', 'qualname': 'TransportError'}


def canonical(value): return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
def digest(value): return hashlib.sha256(canonical(value).encode()).hexdigest()
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def read(path): return json.loads(path.read_text(encoding='utf8'))


def exception_identity(exception):
    return {'module': type(exception).__module__, 'qualname': type(exception).__qualname__}


def classify_bigquery_failure(exception, *, http_status=None, transport_evidence=()):
    """Only a typed Google-auth failure with the exact no-response shape is new.

    Permission, invalid credentials, HTTP rejection and schema errors retain the
    inherited rules.  The SDK exception message is never persisted or returned.
    """
    result = classify_failure(exception, http_status=http_status)
    typed = any({'module': cls.__module__, 'qualname': cls.__qualname__} == TYPE
                for cls in type(exception).__mro__)
    no_response = len(transport_evidence) == 1 and (
        transport_evidence[0].get('status') is None and
        transport_evidence[0].get('complete') is False and
        transport_evidence[0].get('body') == b'')
    if typed and http_status is None and no_response:
        result = {'retryable': True, 'error_class': CORRECTED, 'outcome': 'RETRYABLE_FAILURE',
            'retry_after': transport_evidence[0].get('retry_after'),
            'classification_basis': 'TYPED_GOOGLE_AUTH_TRANSPORT_ERROR_WITH_NO_HTTP_RESPONSE',
            'original_generic_error_class': type(exception).__name__}
    return dict(result, exception_type=exception_identity(exception))


def inside(work, value):
    work = Path(work).resolve(); lexical = Path(value)
    lexical = lexical if lexical.is_absolute() else work / lexical
    # Validate the supplied path before resolving it: otherwise an internal
    # symlink resolving to another internal path would hide the link itself.
    if not lexical.is_relative_to(work) or '..' in lexical.parts:
        raise ValueError('Correction evidence must be inside the project work tree')
    walk = work
    for part in lexical.relative_to(work).parts:
        walk = walk / part
        if walk.is_symlink() or getattr(walk, 'is_junction', lambda:False)():
            raise ValueError('Linked correction evidence is not accepted')
    path = lexical.resolve()
    if not path.is_relative_to(work): raise ValueError('Correction evidence must be inside the project work tree')
    walk = work
    for part in path.relative_to(work).parts:
        walk = walk / part
        if walk.is_symlink() or getattr(walk, 'is_junction', lambda:False)():
            raise ValueError('Linked correction evidence is not accepted')
    return path


def binding(work, path):
    path = inside(work, path)
    return {'path': path.relative_to(Path(work).resolve()).as_posix(), 'sha256': sha(path), 'bytes': path.stat().st_size}


def verify_binding(work, item):
    path = inside(work, item['path'])
    if sha(path) != item['sha256'] or path.stat().st_size != item['bytes']:
        raise ValueError('Correction evidence SHA/size differs')
    return path


def original_failure(work, attempt):
    if attempt['outcome'] != 'PERMANENT_FAILURE' or attempt['error_class'] != 'TransportError':
        raise ValueError('Only the exact misclassified original failure is eligible')
    if attempt['dispatched_at'] is None or attempt['finished_at'] is None:
        raise ValueError('Only a finalized dispatched read may have its classification reviewed')
    receipt = json.loads(attempt['receipt_json'] or '{}')
    path = inside(work, receipt['artifact_path'])
    if sha(path) != receipt['artifact_sha256']: raise ValueError('Original failure receipt changed')
    envelope = read(path); identity = envelope.get('identity', {})
    if identity.get('provider') != 'GOOGLE_BIGQUERY_EXISTING_ADC' or identity.get('method') not in {'tables.get', 'dry_run'}:
        raise ValueError('Only these explicitly read-only BigQuery operations are eligible')
    if logical_key(identity) != attempt['logical_key'] or envelope.get('attempt_id') != attempt['attempt_id'] or envelope.get('attempt_no') != attempt['attempt_no']:
        raise ValueError('Original attempt/receipt identity differs')
    if envelope.get('status') != 'TransportError' or envelope.get('actual_query_executed') is not False or envelope.get('result') is not None:
        raise ValueError('Original receipt is not the specific failed metadata/dry-run read')
    raw = envelope.get('raw_sources', [])
    if len(raw) != 1 or raw[0].get('http_status') is not None or raw[0].get('bytes') != 0 or raw[0].get('complete') is not False:
        raise ValueError('Original failure is not the verified no-HTTP-response case')
    rawpath = inside(work, raw[0]['path'])
    if rawpath.stat().st_size != 0 or sha(rawpath) != raw[0]['sha256']:
        raise ValueError('Original zero-length transport evidence changed')
    accounting = json.loads(attempt['accounting_json'] or '{}')
    if accounting.get('rpc_operations') != 1 or accounting.get('bigquery_bytes') != 0 or accounting.get('job') != envelope.get('job'):
        raise ValueError('Original resource accounting is missing or differs')
    return envelope, binding(work, path), binding(work, rawpath)


def verified_diagnosis(work, diagnosis_binding, attempt, receipt_binding):
    value = read(verify_binding(work, diagnosis_binding))
    if value.get('schema_version') != 'stage1d-sdk-exception-type-diagnosis-v1' or value.get('status') != 'EVIDENCE_VERIFIED':
        raise ValueError('A completed explicit exception-type diagnosis is required')
    if value.get('exception_type') != TYPE or value.get('attempt_id') != attempt['attempt_id'] or value.get('original_receipt_sha256') != receipt_binding['sha256']:
        raise ValueError('Exception-type diagnosis belongs to a different failure')
    if not value.get('type_evidence_basis') or not value.get('reviewer'):
        raise ValueError('Retrospective type evidence must state its basis and reviewer')
    if value.get('type_evidence_kind') == 'BOUND_SDK_STATIC_INFERENCE':
        if value.get('historical_module_directly_recorded') is not False or not value.get('type_source_evidence'):
            raise ValueError('Static inference must retain the missing historical module limitation and bound sources')
        for item in value['type_source_evidence']:
            verify_binding(work, item)
    # Older receipts store only TransportError, not its module. They cannot by
    # themselves prove this class. The new append-only diagnosis must record
    # the actual contemporaneous observation/source review explicitly.
    return value


def prepare_review(work, retry_database, attempt_id, *, diagnosis, environment_evidence):
    """Read-only preparation; returns a concrete review without adopting it."""
    work = Path(work).resolve(); dbpath = inside(work, retry_database)
    with closing(sqlite3.connect(dbpath.as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        row = db.execute('SELECT * FROM read_attempts WHERE attempt_id=?', (attempt_id,)).fetchone()
        if row is None: raise ValueError('Original attempt is absent')
        attempt = dict(row)
        request = dict(db.execute('SELECT * FROM read_requests WHERE logical_key=?', (attempt['logical_key'],)).fetchone())
        attempts = [dict(r) for r in db.execute("SELECT * FROM read_attempts WHERE logical_key=? AND outcome!='ABANDONED_BEFORE_DISPATCH' ORDER BY attempt_no,created_at", (attempt['logical_key'],))]
    envelope, receipt, raw = original_failure(work, attempt)
    diagnosis_binding = binding(work, diagnosis)
    verified_diagnosis(work, diagnosis_binding, attempt, receipt)
    evidence = [binding(work, path) for path in environment_evidence]
    if not evidence: raise ValueError('Observed recovery/environment change evidence is required')
    if request['state'] != 'PERMANENT_FAILURE' or request['success_payload'] is not None or request['success_receipt'] is not None:
        raise ValueError('A successful or other request state cannot be unlocked by this repair')
    if not attempts or attempts[-1]['attempt_id'] != attempt_id or len(attempts) >= 3:
        raise ValueError('Only the last failed read with remaining original attempts can be reviewed')
    return {'schema_version': SCHEMA, 'status': 'EVIDENCE_VERIFIED',
        'logical_key': attempt['logical_key'], 'identity': envelope['identity'], 'attempt_id': attempt_id,
        'original_request': request, 'original_request_sha256': digest(request),
        'original_attempt_sha256': digest(attempt), 'original_attempts_sha256': digest(attempts),
        'original_attempts_count': len(attempts), 'original_total_attempts_limit': 3,
        'remaining_attempts': 3-len(attempts), 'corrected_classification': 'RETRYABLE_FAILURE',
        'corrected_error_class': CORRECTED, 'diagnosis_evidence': diagnosis_binding,
        'original_receipt': receipt, 'original_raw_response': raw, 'environment_evidence': evidence,
        'original_attempt_record_is_immutable': True, 'resources_or_clocks_reset': False,
        'resources_or_unknown_raw_risk_released': False, 'new_network_request_created': False,
        'adoption_required_before_claim': True}


def adopt_review(work, retry_database, review_path, *, now=None):
    """Safe-point adoption only; leaves all original finalized attempts intact.

    Caller must stop normal research workers before invoking. Any active read or
    unclosed Stage1D session rejects adoption. Calling twice with the same bound
    receipt is harmless and cannot re-enable a subsequently exhausted request.
    """
    work = Path(work).resolve(); dbpath = inside(work, retry_database)
    review_file = inside(work, review_path); review = read(review_file)
    if review.get('schema_version') != SCHEMA or review.get('status') != 'EVIDENCE_VERIFIED':
        raise ValueError('An evidence-verified review is required')
    if review.get('original_total_attempts_limit') != 3 or review.get('corrected_classification') != 'RETRYABLE_FAILURE' or review.get('corrected_error_class') != CORRECTED:
        raise ValueError('The inherited three-attempt policy cannot change')
    if not review.get('environment_evidence'):
        raise ValueError('Observed recovery/environment change evidence is required')
    for item in [review['original_receipt'], review['original_raw_response'], review['diagnosis_evidence']] + review['environment_evidence']:
        verify_binding(work, item)
    correction_id = sha(review_file)
    when = time.time() if now is None else float(now)
    with closing(sqlite3.connect(dbpath, timeout=15, isolation_level=None)) as db:
        db.row_factory = sqlite3.Row; db.execute('BEGIN IMMEDIATE')
        try:
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='read_classification_corrections'").fetchone()
            if exists:
                old = db.execute('SELECT * FROM read_classification_corrections WHERE correction_id=?', (correction_id,)).fetchone()
                if old:
                    # Do not touch READY/SUCCESS/EXHAUSTED aggregates on replay.
                    db.rollback(); return {'status':'ALREADY_APPLIED','correction_id':correction_id,'receipt':json.loads(old['receipt_json'])}
            if (work / 'private/network_worker.lock').exists(): raise RuntimeError('Active research writer prevents correction adoption')
            if any(read(path).get('closed') is not True for path in (work / 'private/stage1d_sessions').glob('*.json')):
                raise RuntimeError('Unclosed online session prevents correction adoption')
            if db.execute("SELECT 1 FROM read_attempts WHERE outcome='IN_FLIGHT' LIMIT 1").fetchone():
                raise RuntimeError('In-flight reads cannot be interrupted or concurrently unlocked')
            attempt_row = db.execute('SELECT * FROM read_attempts WHERE attempt_id=?', (review['attempt_id'],)).fetchone()
            if attempt_row is None: raise ValueError('Original failure is missing')
            attempt = dict(attempt_row); envelope, receipt, raw = original_failure(work, attempt)
            verified_diagnosis(work, review['diagnosis_evidence'], attempt, receipt)
            for item in [review['original_receipt'], review['original_raw_response']] + review['environment_evidence']:
                verify_binding(work, item)
            if receipt != review['original_receipt'] or raw != review['original_raw_response'] or envelope['identity'] != review['identity']:
                raise ValueError('Bound original evidence identity changed')
            key = logical_key(review['identity'])
            if key != review['logical_key'] or key != attempt['logical_key']: raise ValueError('Logical request identity cannot change')
            request = dict(db.execute('SELECT * FROM read_requests WHERE logical_key=?', (key,)).fetchone())
            attempts = [dict(r) for r in db.execute("SELECT * FROM read_attempts WHERE logical_key=? AND outcome!='ABANDONED_BEFORE_DISPATCH' ORDER BY attempt_no,created_at", (key,))]
            if digest(request) != review['original_request_sha256'] or digest(attempt) != review['original_attempt_sha256'] or digest(attempts) != review['original_attempts_sha256']:
                raise ValueError('Request or finalized attempt history changed after review')
            if request['state'] != 'PERMANENT_FAILURE' or request['success_payload'] is not None or request['success_receipt'] is not None:
                raise ValueError('Only the reviewed failure aggregate can be unlocked')
            if not attempts or attempts[-1]['attempt_id'] != attempt['attempt_id'] or len(attempts) != review['original_attempts_count'] or len(attempts) >= 3:
                raise ValueError('No original retry capacity remains')
            if review.get('remaining_attempts') != 3-len(attempts): raise ValueError('Remaining attempts cannot be increased')
            eligible = max(float(request['next_eligible_at']), when + 1.0)
            record = {'schema_version':'stage1d-read-classification-correction-receipt-v1',
                'correction_id':correction_id,'logical_key':key,'attempt_id':attempt['attempt_id'],
                'review':binding(work,review_file),'adopted_at':when,'previous_request':request,
                'new_request_state':'READY','next_eligible_at':eligible,'total_attempts_limit':3,
                'counted_old_attempts':len(attempts),'next_attempt_no':len(attempts)+1,
                'next_retry_of':attempt['attempt_id'],'original_attempts_sha256':digest(attempts),
                'old_outcome_retained':'PERMANENT_FAILURE','corrected_interpretation':'RETRYABLE_FAILURE',
                'resource_or_clock_or_raw_risk_mutation':False,'new_request_dispatched':False}
            db.execute('CREATE TABLE IF NOT EXISTS read_classification_corrections(correction_id TEXT PRIMARY KEY,logical_key TEXT NOT NULL,attempt_id TEXT NOT NULL,created_at REAL NOT NULL,receipt_json TEXT NOT NULL)')
            db.execute('INSERT INTO read_classification_corrections VALUES(?,?,?,?,?)', (correction_id,key,attempt['attempt_id'],when,canonical(record)))
            db.execute("UPDATE read_requests SET state='READY',next_eligible_at=? WHERE logical_key=? AND state='PERMANENT_FAILURE'", (eligible,key))
            current = [dict(r) for r in db.execute("SELECT * FROM read_attempts WHERE logical_key=? AND outcome!='ABANDONED_BEFORE_DISPATCH' ORDER BY attempt_no,created_at", (key,))]
            if digest(current) != review['original_attempts_sha256']: raise RuntimeError('Original attempt mutation detected')
            db.commit(); return {'status':'APPLIED','correction_id':correction_id,'receipt':record}
        except BaseException:
            db.rollback(); raise
