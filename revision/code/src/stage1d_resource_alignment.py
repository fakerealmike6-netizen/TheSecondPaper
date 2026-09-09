"""Explicit resource alignment on the one continued Stage1D ledger.

Old limits, the cumulative500 authority, money facts, and unknown attempts are
preserved. New executions reserve50; every pre-adoption R2 execution retains20.
The account usage screenshot is provenance only, never project settlement.
"""
from decimal import Decimal
import hashlib
import json
import re
import sqlite3
from contextlib import closing
from pathlib import Path

from budget import valid
from budget_r2 import RevisionLedger, AUTH as LEGACY_AUTH, now
from page_attempts import atomic_json
from stage1d_budget import Stage1DLedger, AUTH as PREVIOUS_AUTH, SCOPE_AUTH, _inside, canonical, sha

AUTH = 'STAGE1D_RESOURCE_ALIGNMENT_50_2000_RPC10000_V1'
CAP = Decimal('2000')
WARNING = Decimal('1600')
EXECUTION_CAP = Decimal('50')
RPC_CAP = Decimal('10000')
CU_CAP = Decimal('1000000')
EXPECTED_FILE = 'private/STAGE1D_RESOURCE_ALIGNMENT_EXPECTED.json'


def _source(work, proof):
    expected = json.loads(_inside(work, EXPECTED_FILE).read_text(encoding='utf8'))
    if (expected.get('schema_version') != 'stage1d-resource-alignment-expected-v1'
            or expected.get('authorization_id') != AUTH
            or not re.fullmatch('[0-9a-f]{64}', str(expected.get('source_sha256', '')))
            or type(expected.get('source_bytes')) is not int or expected['source_bytes'] <= 0):
        raise ValueError('Private expected resource authority binding required')
    path = _inside(work, proof['source_path'])
    if (proof.get('source_sha256') != expected['source_sha256']
            or proof.get('source_bytes') != expected['source_bytes']
            or path.stat().st_size != expected['source_bytes'] or sha(path) != expected['source_sha256']):
        raise ValueError('Exact authorized resource attachment SHA and bytes required')
    return path


def _validate_proof(work, proof):
    required = {'authorization_id': AUTH, 'status': 'USER_CONFIRMED',
        'cumulative_cap_credits': '2000', 'warning_credits': '1600', 'execution_cap_credits': '50',
        'rpc_operations_cap': '10000', 'alchemy_cu_cap': '1000000',
        'payment_method_added': False, 'extra_credits_enabled': False}
    if any(proof.get(k) != v for k, v in required.items()):
        raise ValueError('Exact explicit50/2000/1600/RPC10000/CU1000000 authority required')
    source = _source(work, proof)
    frozen = Path(work) / 'private/BATCH_QUERY_FREEZE.json'
    batch = json.loads(frozen.read_text(encoding='utf8'))
    if (batch.get('authorization_id') != SCOPE_AUTH or len(batch.get('queries', [])) != 4
            or len({q['query_id'] for q in batch['queries']}) != 4):
        raise ValueError('Unchanged original four-query scope freeze required')
    if (proof.get('scope_authorization_id', SCOPE_AUTH) != SCOPE_AUTH
            or proof.get('batch_freeze_sha256', sha(frozen)) != sha(frozen)
            or proof.get('previous_authorization_id', PREVIOUS_AUTH) != PREVIOUS_AUTH):
        raise ValueError('Resource alignment cannot replace scope or previous ledger authority')
    return dict(proof, source_path=source.relative_to(Path(work).resolve()).as_posix(),
                scope_authorization_id=SCOPE_AUTH, batch_freeze_sha256=sha(frozen),
                previous_authorization_id=PREVIOUS_AUTH)


def _old_tables(db):
    tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
              if not r[0].startswith('stage1d_resource_') and r[0] != 'stage1d_journal']
    result = {}
    for table in sorted(tables):
        if not re.fullmatch('[A-Za-z_][A-Za-z_0-9]*', table):
            raise ValueError('Unsafe ledger table name')
        rows = db.execute('SELECT * FROM ' + table + ' ORDER BY rowid').fetchall()
        result[table] = {'rows': len(rows), 'sha256': hashlib.sha256(canonical(rows)).hexdigest()}
    return result


def is_aligned(path):
    """Read-only factory probe; malformed adopted evidence never falls back."""
    path = Path(path).resolve()
    if not path.is_file():
        return False
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'stage1d_resource_authorizations' not in tables:
            if any(t.startswith('stage1d_resource_') for t in tables):
                raise ValueError('Partial resource authorization state; cannot fall back')
            return False
        rows = db.execute('SELECT authorization_id,source_sha256,proof_json FROM stage1d_resource_authorizations').fetchall()
        if len(rows) != 1 or rows[0][0] != AUTH:
            raise ValueError('Malformed resource authority state; cannot fall back')
        proof = json.loads(rows[0][2])
        if _validate_proof(path.parent.parent, proof) != proof or rows[0][1] != proof['source_sha256']:
            raise ValueError('Resource authority evidence changed; cannot fall back')
        if not {'stage1d_resource_receipts', 'stage1d_resource_job_policies'} <= tables:
            raise ValueError('Incomplete resource adoption state; cannot fall back')
        receipt = db.execute('SELECT receipt_json FROM stage1d_resource_receipts WHERE authorization_id=?', (AUTH,)).fetchone()
        if not receipt or json.loads(receipt[0]).get('proof') != proof:
            raise ValueError('Resource adoption receipt differs; cannot fall back')
        return True


class Stage1DResourceLedger(Stage1DLedger):
    """Runtime ledger factory; construction never adopts or grants resources."""

    def _amended(self, db):
        # Keep the entire previous authority valid and independently readable.
        previous = super()._amended(db)
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='stage1d_resource_authorizations'").fetchone():
            raise RuntimeError('Explicit resource alignment has not been adopted')
        row = db.execute('SELECT proof_json FROM stage1d_resource_authorizations WHERE authorization_id=?', (AUTH,)).fetchone()
        if not row:
            raise RuntimeError('Current explicit resource authority absent')
        work = Path(self.path).resolve().parent.parent
        proof = json.loads(row[0])
        if _validate_proof(work, proof) != proof:
            raise ValueError('Stored resource authorization differs from bound evidence')
        return proof

    def snapshot(self, db=None):
        if db is None:
            with self.connection() as cx:
                return self.snapshot(cx)
        self._amended(db)
        out = super().snapshot(db)
        dune = out['dune_credits']
        risk = Decimal(dune['cumulative_risk'])
        allowance = dune['confirmed_allowance']
        effective = None if allowance is None else min(CAP, Decimal(allowance))
        remaining = None if effective is None else str(max(Decimal(0), effective - risk))
        dune.pop('warning_at_400', None)
        dune.update(cap=str(CAP), cumulative_cap=str(CAP), authorization_id=AUTH,
            warning_threshold=str(WARNING), warning_at_1600=risk >= WARNING,
            warning_threshold_reached=risk >= WARNING, warning_requires_pause=False,
            remaining=remaining, remaining_for_new_jobs=remaining,
            overrun=bool(dune.get('halt_reason')) or effective is not None and risk > effective,
            new_execution_cap_credits=str(EXECUTION_CAP),
            amendment_basis='Replacement cumulative2000 across every inherited/current actual and unknown risk; not an additional pool. Provider allowance is unchanged.')
        for unit, cap in (('rpc_operations', RPC_CAP), ('alchemy_cu', CU_CAP)):
            row = out[unit]
            used = Decimal(row['actual']) + Decimal(row['reserved'])
            row.update(legacy_cap=row['cap'], legacy_confirmed_allowance=row['confirmed_allowance'],
                legacy_allowance_evidence=row['evidence'], cap=str(cap), confirmed_allowance=str(cap),
                remaining=str(max(Decimal(0), cap - used)), overrun=used > cap,
                authorization_id=AUTH, effective_limit_basis='EXPLICIT_CONTINUED_PROJECT_RESOURCE_AUTHORITY',
                provider_account_remaining_not_inferred=True)
        return out

    def submission_policy(self, job=None, db=None):
        if db is None:
            with self.connection() as cx:
                return self.submission_policy(job, cx)
        self._amended(db)
        if job is None:
            row = db.execute('SELECT source_sha256,proof_json FROM stage1d_resource_authorizations WHERE authorization_id=?', (AUTH,)).fetchone()
            return {'authorization_id': AUTH, 'execution_cap_credits': str(EXECUTION_CAP),
                    'scope_authorization_id': SCOPE_AUTH, 'policy': 'NEW_EXECUTION_ONLY',
                    'authorization_source_sha256': row[0],
                    'authorization_proof_sha256': hashlib.sha256(canonical(json.loads(row[1]))).hexdigest()}
        row = db.execute('SELECT policy_json FROM stage1d_resource_job_policies WHERE job=?', (job,)).fetchone()
        if not row:
            raise ValueError('Existing job has no adopted submission-cap proof; inherited whole-job risk needs separate reconciliation')
        return json.loads(row[0])

    def execution_cap(self, job=None, db=None):
        return Decimal(self.submission_policy(job, db)['execution_cap_credits'])

    def reserve_dune_job(self, job, purpose, execution_estimate='50', export_estimate='0'):
        er, xr = valid(execution_estimate, 'dune_credits'), valid(export_estimate, 'dune_credits')
        if er != EXECUTION_CAP:
            raise ValueError('Unknown newly submitted execution must reserve50')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            snap = self._authorized(db)
            if db.execute('SELECT 1 FROM jobs WHERE id=?', (job,)).fetchone():
                raise ValueError('Existing job must resume its original submission policy and reservation')
            if db.execute("SELECT 1 FROM r2_components WHERE origin='R2_NEW' AND status IN ('RESERVED','EXECUTING','UNKNOWN_EXECUTION')").fetchone():
                raise RuntimeError('At most one in-flight SQL execution')
            remaining = snap['dune_credits']['remaining']
            if remaining is None or er + xr > Decimal(remaining):
                raise RuntimeError('Insufficient cumulative2000/provider allowance')
            policy = dict(self.submission_policy(db=db), job=job, submitted_at_utc=now())
            db.execute('INSERT INTO jobs VALUES(?,?,?,?,?)', (job, 'dune', purpose, now(), 'RESERVED'))
            db.execute('INSERT INTO amounts VALUES(?,?,?,NULL)', (job, 'dune_credits', str(er + xr)))
            db.execute('INSERT INTO r2_components VALUES(?,?,?,?,?,?,?,?)',
                       (job, 'R2_NEW', str(er), None, str(xr), None, 'RESERVED', '{}'))
            db.execute('INSERT INTO stage1d_resource_job_policies VALUES(?,?,?)',
                       (job, json.dumps(policy, sort_keys=True), now()))
            self._record(db, job, 'RESOURCE_ALIGNED_SUBMISSION_POLICY', policy)

    def observe_execution(self, job, cost, evidence, terminal=False):
        cost = valid(cost, 'dune_credits')
        if not evidence or not evidence.get('sha256'):
            raise ValueError('Verified execution receipt required')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE'); self._amended(db)
            row = db.execute('SELECT origin,execution_risk,execution_known,export_risk FROM r2_components WHERE job=?', (job,)).fetchone()
            if not row or row[0] != 'R2_NEW':
                raise ValueError('Inherited whole-job risk requires separate evidence-bearing reconciliation')
            cap = self.execution_cap(job, db)
            known = max(cost, Decimal(row[2] or '0'))
            risk = known if terminal else max(Decimal(row[1]), known)
            db.execute('UPDATE r2_components SET execution_risk=?,execution_known=?,status=? WHERE job=?',
                       (str(risk), str(known), 'EXECUTION_KNOWN' if terminal else 'EXECUTING', job))
            db.execute("UPDATE amounts SET reserved=? WHERE job=? AND unit='dune_credits' AND actual IS NULL",
                       (str(risk + Decimal(row[3])), job))
            self._record(db, job, 'EXECUTION_OBSERVED', {'cost': str(cost), 'terminal': terminal, 'receipt': evidence,
                'submission_policy': self.submission_policy(job, db)})
            if known > cap:
                db.execute('INSERT OR REPLACE INTO r2_meta VALUES(?,?)', ('halt', 'EXECUTION_CAP' + str(cap) + '_EXCEEDED:' + str(known)))
            if Decimal(self.snapshot(db)['dune_credits']['cumulative_risk']) > CAP:
                db.execute('INSERT OR REPLACE INTO r2_meta VALUES(?,?)', ('halt', 'CUMULATIVE2000_EXCEEDED'))

    def reserve_export(self, job, upper, evidence, *, observed=False):
        # The inherited500 controller's checks dispatch through our snapshot and
        # _authorized methods. No old fixed cap is used in its reserve arithmetic.
        return super().reserve_export(job, upper, evidence, observed=observed)


def adopt(work, proof):
    """Explicit append-only authority adoption; no provider calls or limit edits."""
    work = Path(work).resolve()
    proof = _validate_proof(work, proof)
    if (work / 'private/network_worker.lock').exists():
        raise RuntimeError('Stop the single writer before resource adoption')
    path = work / 'private/shared_budget_r4.sqlite'
    if not path.is_file():
        raise ValueError('Continued shared ledger required; no second pool')
    previous = Stage1DLedger(path)
    receipt_path = work / 'private/stage1d_authority/RESOURCE_ALIGNMENT_ADOPTION_RECEIPT.json'
    with previous.connection() as db:
        db.execute('BEGIN IMMEDIATE')
        previous._amended(db)
        before_tables = _old_tables(db)
        before = previous.snapshot(db)
        db.execute('CREATE TABLE IF NOT EXISTS stage1d_resource_authorizations(authorization_id TEXT PRIMARY KEY,source_sha256 TEXT,proof_json TEXT,utc TEXT)')
        db.execute('CREATE TABLE IF NOT EXISTS stage1d_resource_receipts(authorization_id TEXT PRIMARY KEY,receipt_json TEXT)')
        db.execute('CREATE TABLE IF NOT EXISTS stage1d_resource_job_policies(job TEXT PRIMARY KEY,policy_json TEXT,utc TEXT)')
        existing = db.execute('SELECT proof_json FROM stage1d_resource_authorizations WHERE authorization_id=?', (AUTH,)).fetchone()
        if existing:
            if canonical(json.loads(existing[0])) != canonical(proof):
                raise ValueError('Existing resource adoption cannot change evidence or scope')
            saved = db.execute('SELECT receipt_json FROM stage1d_resource_receipts WHERE authorization_id=?', (AUTH,)).fetchone()
            if not saved:
                raise ValueError('Adoption receipt missing; preserve incomplete evidence')
            result = json.loads(saved[0])
        else:
            if db.execute('SELECT 1 FROM stage1d_resource_authorizations').fetchone():
                raise ValueError('Different resource alignment requires another explicit policy')
            db.execute('INSERT INTO stage1d_resource_authorizations VALUES(?,?,?,?)',
                       (AUTH, proof['source_sha256'], json.dumps(proof, sort_keys=True), now()))
            legacy_jobs = []
            for row in db.execute("SELECT * FROM r2_components WHERE origin='R2_NEW' ORDER BY job").fetchall():
                policy = {'job': row[0], 'authorization_id': PREVIOUS_AUTH,
                    'execution_cap_credits': '20', 'policy': 'PRE_ADOPTION_SUBMISSION_CAP_PRESERVED',
                    'evidence_basis': 'Original continued R2 and cumulative500 policies both reserve20; this row predates resource alignment.',
                    'component_sha256_at_adoption': hashlib.sha256(canonical(list(row))).hexdigest(),
                    'alignment_authorization_id': AUTH, 'new_reservation_added': False}
                db.execute('INSERT INTO stage1d_resource_job_policies VALUES(?,?,?)',
                           (row[0], json.dumps(policy, sort_keys=True), now()))
                legacy_jobs.append(policy)
            current = object.__new__(Stage1DResourceLedger); current.path = str(path)
            after = current.snapshot(db)
            if before_tables != _old_tables(db):
                raise ValueError('An old ledger, limit, authority, or evidence table changed')
            stable = ('actual', 'reserved', 'cumulative_risk', 'known_actual_lower_bound', 'legacy_risk', 'r2_risk',
                      'export_upper_not_actual', 'unknown_or_pending_risk', 'execution_uncertainty_risk',
                      'execution_peak_discrepancy_risk', 'confirmed_allowance')
            if any(before['dune_credits'][k] != after['dune_credits'][k] for k in stable):
                raise ValueError('Money facts or confirmed provider allowance changed')
            for unit in before:
                if unit not in ('dune_credits', 'rpc_operations', 'alchemy_cu') and before[unit] != after[unit]:
                    raise ValueError('Other resource policy changed')
                if any(before[unit][k] != after[unit][k] for k in ('actual', 'reserved', 'actual_exceeded_reservation')):
                    raise ValueError('Historical resource consumption changed')
            result = {'schema_version': 'stage1d-resource-alignment-adoption-v1', 'authorization_id': AUTH,
                'proof': proof, 'before_snapshot': before, 'after_snapshot': after,
                'old_tables_before': before_tables, 'old_tables_after': _old_tables(db),
                'legacy_submission_policies': legacy_jobs, 'old_limits_and_authorities_unchanged': True,
                'all_consumption_and_unknown_risk_unchanged': True, 'new_allowance_pool_added': False,
                'account_usage_snapshot_used_as_project_settlement': False,
                'provider_confirmed_allowance_unchanged': True,
                'cap_semantics': 'Replacement totals2000/RPC10000/CU1000000 include all inherited use; only new execution submissions reserve50.',
                'network_requests': 0, 'utc': now()}
            payload = json.dumps(result, sort_keys=True)
            db.execute('INSERT INTO stage1d_resource_receipts VALUES(?,?)', (AUTH, payload))
            db.execute('CREATE TABLE IF NOT EXISTS stage1d_journal(n INTEGER PRIMARY KEY,kind TEXT,payload TEXT,utc TEXT)')
            db.execute('INSERT INTO stage1d_journal(kind,payload,utc) VALUES(?,?,?)',
                       ('RESOURCE_TOTALS_ALIGNED_WITH_NEW_AUTHORITY', payload, now()))
    atomic_json(receipt_path, result)
    return result


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--proof', type=Path, required=True)
    args = parser.parse_args()
    proof_path = _inside(args.work, args.proof)
    print(json.dumps(adopt(args.work, json.loads(proof_path.read_text(encoding='utf8'))), indent=2))
