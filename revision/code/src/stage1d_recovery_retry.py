"""One additional three-read allowance for proved pre-pause transient failures.

The old request identity, attempt chain, cost and next eligible time survive.
This does not authorize repeat SQL creation or grant fresh attempts on restart.
"""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import threading

from context_access_r3 import read, sha, now, canonical
from page_attempts import atomic_json
from read_retry_r4 import ReadRetryStore, logical_key

RECOVERY_ID = 'STAGE1D_RECOVERY_TRANSPORT_V1'
ALLOWED = {'eth_getBalance', 'eth_getBlockByNumber', 'eth_getTransactionReceipt',
           'eth_getTransactionByHash', 'eth_getCode', 'txlist', 'txlistinternal'}


def eligible(identity, attempts):
    return (identity.get('method') in ALLOWED and len(attempts) == 3
            and [a['attempt_no'] for a in attempts] == [1, 2, 3]
            and all(a['outcome'] == 'RETRYABLE_FAILURE' and a.get('finished_at') is not None
                    and a.get('dispatched_at') is not None for a in attempts))


def _transport_record(work, path, expected_sha):
    path = Path(work) / path
    if not path.resolve().is_relative_to(Path(work).resolve()) or sha(path) != expected_sha:
        raise ValueError('Changed recovery transport receipt')
    record = read(path)
    expected = {'ordinary_batch_size': 5, 'receipt_batch_size': 1,
                'connect_timeout_seconds': 10, 'read_timeout_seconds': 60,
                'proxy': 'http://127.0.0.1:7890', 'hidden_retries': 0}
    if any(record.get('settings', {}).get(k) != v for k, v in expected.items()):
        raise ValueError('Actual recovery transport changes required')
    if not record.get('source_changes'):
        raise ValueError('Changed transport source evidence required')
    for source in record['source_changes']:
        target = (Path(work) / source['path']).resolve()
        expected = source['new_sha256']
        corrections = Path(work) / 'private/stage1d_authority/TRANSPORT_SOURCE_CORRECTIONS.json'
        if corrections.exists():
            for correction in read(corrections)['corrections']:
                if correction['path'] == source['path'] and correction['from_sha256'] == expected:
                    test = (Path(work) / correction['test_receipt']['path']).resolve()
                    if (not test.is_relative_to(Path(work).resolve())
                            or sha(test) != correction['test_receipt']['sha256']
                            or read(test).get('status') != 'PASS' or not correction.get('reason')):
                        raise ValueError('Transport correction lacks passed SHA-bound verification')
                    expected = correction['to_sha256']
        if not target.is_relative_to(Path(work).resolve()) or sha(target) != expected:
            raise ValueError('Active recovery transport source differs')
        if source['old_sha256'] == source['new_sha256']:
            raise ValueError('Unchanged transport is not a recovery grant')
    return record


def adopt_grants(work, baseline_retry_db, transport_path):
    from stage1d_recovery_policy import effective, POLICY_SHA
    work = Path(work).resolve()
    if effective(work) is None:
        raise ValueError('Recovery policy must already be adopted')
    if (work / 'private/network_worker.lock').exists():
        raise RuntimeError('Retry grant adoption requires stopped writer')
    baseline = Path(baseline_retry_db).resolve()
    snapshot = work.parent / 'recovery/PRE_RECOVERY_SNAPSHOT.json'
    manifest = read(snapshot)
    expected = next(r for r in manifest['files'] if r['path'] == 'code/private/read_retry_r4.sqlite')
    if sha(baseline) != expected['backup_sha256']:
        raise ValueError('Exact preserved pre-recovery retry database required')
    transport_path = Path(transport_path).resolve()
    relative = transport_path.relative_to(work).as_posix()
    transport_sha = sha(transport_path)
    _transport_record(work, relative, transport_sha)
    with closing(sqlite3.connect(baseline.as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        originals = []
        for row in db.execute('SELECT logical_key,identity_json FROM read_requests'):
            identity = json.loads(row['identity_json'])
            attempts = [dict(a) for a in db.execute("SELECT * FROM read_attempts WHERE logical_key=? AND outcome!='ABANDONED_BEFORE_DISPATCH' ORDER BY attempt_no,created_at", (row['logical_key'],))]
            if eligible(identity, attempts):
                originals.append((row['logical_key'], identity, attempts))
    granted = []
    target_db = work / 'private/read_retry_r4.sqlite'
    with closing(sqlite3.connect(target_db)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN IMMEDIATE')
        db.execute('CREATE TABLE IF NOT EXISTS stage1d_recovery_read_grants(recovery_id TEXT,logical_key TEXT,proof_json TEXT,PRIMARY KEY(recovery_id,logical_key))')
        for key, identity, old in originals:
            actual = [dict(a) for a in db.execute("SELECT * FROM read_attempts WHERE logical_key=? AND outcome!='ABANDONED_BEFORE_DISPATCH' ORDER BY attempt_no,created_at", (key,))]
            if actual[:3] != old:
                raise ValueError('Old retry attempts changed')
            existing = db.execute('SELECT proof_json FROM stage1d_recovery_read_grants WHERE recovery_id=? AND logical_key=?', (RECOVERY_ID, key)).fetchone()
            if existing:
                proof = json.loads(existing[0])
                if proof['original_attempts_sha256'] != hashlib.sha256(canonical(old)).hexdigest():
                    raise ValueError('Existing recovery grant differs')
            else:
                if len(actual) != 3:
                    continue
                proof = {'recovery_id': RECOVERY_ID, 'logical_key': key, 'method': identity['method'],
                    'policy_sha256': POLICY_SHA, 'original_attempts_sha256': hashlib.sha256(canonical(old)).hexdigest(),
                    'original_attempt_ids': [a['attempt_id'] for a in old],
                    'baseline_database_sha256': expected['backup_sha256'],
                    'transport_receipt_path': relative, 'transport_receipt_sha256': transport_sha,
                    'additional_attempts': 3, 'total_attempts': 6, 'old_attempts_and_fees_preserved': True, 'utc': now()}
                db.execute('INSERT INTO stage1d_recovery_read_grants VALUES(?,?,?)', (RECOVERY_ID, key, json.dumps(proof, sort_keys=True)))
            granted.append(proof)
        db.commit()
    receipt = {'recovery_id': RECOVERY_ID, 'granted_keys': len(granted), 'grants': granted,
               'same_request_identity': True, 'new_failure_keys_remain_three': True, 'network_requests': 0}
    output = work / 'private/stage1d_authority/RECOVERY_RETRY_GRANTS.json'
    if output.exists():
        if read(output) != receipt:
            raise ValueError('Recovery grant receipt cannot change on restart')
    else:
        atomic_json(output, receipt)
    return receipt


class RecoveryReadRetryStore(ReadRetryStore):
    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)
        self._recovery_mutex = threading.RLock()

    def _limit(self, key):
        with self._db() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='stage1d_recovery_read_grants'").fetchone():
                return 3
            row = db.execute('SELECT proof_json FROM stage1d_recovery_read_grants WHERE recovery_id=? AND logical_key=?', (RECOVERY_ID, key)).fetchone()
            if not row:
                return 3
            proof = json.loads(row[0])
            old = [dict(a) for a in db.execute("SELECT * FROM read_attempts WHERE logical_key=? AND outcome!='ABANDONED_BEFORE_DISPATCH' ORDER BY attempt_no,created_at LIMIT 3", (key,))]
        from stage1d_recovery_policy import effective, POLICY_SHA
        work = self.path.resolve().parent.parent
        if (effective(work) is None or proof['policy_sha256'] != POLICY_SHA
                or proof['total_attempts'] != 6 or proof['additional_attempts'] != 3
                or hashlib.sha256(canonical(old)).hexdigest() != proof['original_attempts_sha256']):
            raise ValueError('Immutable pre-pause retry grant proof differs')
        _transport_record(work, proof['transport_receipt_path'], proof['transport_receipt_sha256'])
        return 6

    def claim(self, identity, **kwargs):
        with self._recovery_mutex:
            self.total_attempts = self._limit(logical_key(identity))
            try:
                return super().claim(identity, **kwargs)
            finally:
                self.total_attempts = 3

    def finish(self, attempt_id, outcome, **kwargs):
        with self._recovery_mutex:
            with self._db() as db:
                row = db.execute('SELECT logical_key FROM read_attempts WHERE attempt_id=?', (attempt_id,)).fetchone()
            if row is None:
                raise ValueError('Unknown retry attempt')
            self.total_attempts = self._limit(row[0])
            try:
                return super().finish(attempt_id, outcome, **kwargs)
            finally:
                self.total_attempts = 3
