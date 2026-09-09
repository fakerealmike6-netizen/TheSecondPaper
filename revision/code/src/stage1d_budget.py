"""Evidence-bound Stage1D cumulative500 amendment on the existing single ledger.

This opt-in controller leaves RevisionLedger's 100/80 defaults and every old
money/limit/meta row intact. The amendment is a replacement cumulative cap, not
an added allowance, and does not confirm the provider account's paid allowance.
"""
from decimal import Decimal
import hashlib, json, re, sqlite3
from pathlib import Path
from budget import valid
from budget_r2 import RevisionLedger, AUTH as INHERITED_AUTH, now
from page_attempts import atomic_json

AUTH='STAGE1D_DUNE_CUMULATIVE500_V1'
SCOPE_AUTH='STAGE1D_BATCH01_REFERENCE_FULL_V1'
CAP=Decimal('500');WARNING=Decimal('400')
EXPECTED_FILE='private/STAGE1D_BUDGET_AUTHORITY_EXPECTED.json'

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),default=str).encode()
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def _inside(work,value):
    work=Path(work).resolve();lexical=work/Path(value);path=lexical.resolve()
    if not path.is_relative_to(work):raise ValueError('Copy the authorized attachment inside this Stage1D workspace first')
    walk=work
    for part in lexical.relative_to(work).parts:
        walk=walk/part
        if walk.is_symlink() or getattr(walk,'is_junction',lambda:False)():raise ValueError('Linked budget evidence is not accepted')
    return path

def _source(work,proof):
    expected_path=_inside(work,EXPECTED_FILE)
    expected=json.loads(expected_path.read_text(encoding='utf8'))
    if expected.get('schema_version')!='stage1d-budget-authority-expected-v1' or expected.get('authorization_id')!=AUTH or not re.fullmatch('[0-9a-f]{64}',str(expected.get('source_sha256',''))) or type(expected.get('source_bytes'))is not int or expected['source_bytes']<=0:
        raise ValueError('Private expected authority binding is absent or malformed')
    path=_inside(work,proof['source_path'])
    if proof.get('source_sha256')!=expected['source_sha256'] or proof.get('source_bytes')!=expected['source_bytes'] or path.stat().st_size!=expected['source_bytes'] or sha(path)!=expected['source_sha256']:
        raise ValueError('Exact authorized incremental attachment SHA and byte count required')
    return path

def _validate_proof(work,proof):
    expected={'authorization_id':AUTH,'status':'USER_CONFIRMED','cumulative_cap_credits':'500',
              'warning_credits':'400','execution_cap_credits':'20','payment_method_added':False,'extra_credits_enabled':False}
    if any(proof.get(k)!=v for k,v in expected.items()):raise ValueError('Explicit cumulative500/warning400/cap20 authority required')
    source=_source(work,proof)
    batch_path=Path(work)/'private/BATCH_QUERY_FREEZE.json';batch=json.loads(batch_path.read_text(encoding='utf8'))
    if batch.get('authorization_id')!=SCOPE_AUTH or len(batch.get('queries',[]))!=4 or len({q['query_id'] for q in batch['queries']})!=4:
        raise ValueError('Original authorized four-query scope freeze required')
    if proof.get('scope_authorization_id',SCOPE_AUTH)!=SCOPE_AUTH or proof.get('batch_freeze_sha256',sha(batch_path))!=sha(batch_path):
        raise ValueError('Budget amendment cannot replace the query/scope authority')
    return dict(proof,source_path=source.relative_to(Path(work).resolve()).as_posix(),scope_authorization_id=SCOPE_AUTH,batch_freeze_sha256=sha(batch_path))

def _old_tables(db):
    tables=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            if not r[0].startswith('stage1d_budget_') and r[0]!='stage1d_journal']
    result={}
    for table in sorted(tables):
        if not re.fullmatch('[a-zA-Z_][a-zA-Z_0-9]*',table):raise ValueError('Unsafe existing ledger table name')
        rows=db.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall()
        result[table]={'rows':len(rows),'sha256':hashlib.sha256(canonical(rows)).hexdigest()}
    return result

class Stage1DLedger(RevisionLedger):
    def __init__(self,path):
        if not Path(path).is_file():raise ValueError('Existing amended shared ledger required; no new pool')
        super().__init__(path)
        with self.connection() as db:self._amended(db)

    def _amended(self,db):
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='stage1d_budget_authorizations'").fetchone():
            raise RuntimeError('Explicit amendment has not been applied')
        row=db.execute('SELECT proof_json FROM stage1d_budget_authorizations WHERE authorization_id=?',(AUTH,)).fetchone()
        active=db.execute("SELECT value FROM stage1d_budget_meta WHERE key='active_authorization_id'").fetchone()
        if not row or not active or active[0]!=AUTH:raise RuntimeError('Active cumulative500 authority is absent')
        proof=json.loads(row[0]);work=Path(self.path).resolve().parent.parent
        _source(work,proof)
        if proof.get('authorization_id')!=AUTH or proof.get('scope_authorization_id')!=SCOPE_AUTH or proof.get('cumulative_cap_credits')!='500' or proof.get('warning_credits')!='400' or proof.get('execution_cap_credits')!='20':
            raise RuntimeError('Amended budget policy differs from authorized constants')
        if sha(work/'private/BATCH_QUERY_FREEZE.json')!=proof['batch_freeze_sha256']:raise ValueError('Bound four-query freeze changed after the amendment')
        return proof

    def snapshot(self,db=None):
        if db is None:
            with self.connection() as cx:return self.snapshot(cx)
        self._amended(db);out=super().snapshot(db);d=out['dune_credits']
        risk=Decimal(d['cumulative_risk']);allowance=d['confirmed_allowance'];effective=None if allowance is None else min(CAP,Decimal(allowance))
        remaining=None if effective is None else str(max(Decimal(0),effective-risk))
        d.pop('warning_at_80',None)
        d.update(cap=str(CAP),authorization_id=AUTH,cumulative_cap=str(CAP),warning_threshold=str(WARNING),
                 warning_at_400=risk>=WARNING,warning_threshold_reached=risk>=WARNING,warning_requires_pause=False,
                 remaining=remaining,remaining_for_new_jobs=remaining,overrun=bool(d.get('halt_reason')) or effective is not None and risk>effective,
                 amendment_basis='Replacement total cap500 includes every inherited/current actual and unresolved reservation; provider allowance is unchanged')
        return out

    def _authorized(self,db):
        self._amended(db)
        if not db.execute("SELECT 1 FROM r2_meta WHERE key='authorization_id' AND value=?",(INHERITED_AUTH,)).fetchone():raise RuntimeError('Original ledger continuation identity missing')
        snap=self.snapshot(db)
        if any(v['overrun'] or v['actual_exceeded_reservation'] for v in snap.values()):raise RuntimeError('Cumulative budget halted')
        return snap

    def reserve_dune_job(self,job,purpose,execution_estimate='20',export_estimate='0'):
        er=valid(execution_estimate,'dune_credits');xr=valid(export_estimate,'dune_credits')
        if er!=20:raise ValueError('Unknown new execution must reserve20')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE');snap=self._authorized(db)
            if db.execute("SELECT 1 FROM r2_components WHERE origin='R2_NEW' AND status IN ('RESERVED','EXECUTING','UNKNOWN_EXECUTION')").fetchone():raise RuntimeError('At most one in-flight SQL execution')
            remaining=snap['dune_credits']['remaining']
            if remaining is None or er+xr>Decimal(remaining):raise RuntimeError('Insufficient cumulative500/provider allowance')
            db.execute('INSERT INTO jobs VALUES(?,?,?,?,?)',(job,'dune',purpose,now(),'RESERVED'))
            db.execute('INSERT INTO amounts VALUES(?,?,?,NULL)',(job,'dune_credits',str(er+xr)))
            db.execute('INSERT INTO r2_components VALUES(?,?,?,?,?,?,?,?)',(job,'R2_NEW',str(er),None,str(xr),None,'RESERVED','{}'))

    def observe_execution(self,job,cost,evidence,terminal=False):
        cost=valid(cost,'dune_credits')
        if not evidence or not evidence.get('sha256'):raise ValueError('Verified execution receipt required')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE');self._amended(db)
            row=db.execute('SELECT origin,execution_risk,execution_known,export_risk FROM r2_components WHERE job=?',(job,)).fetchone()
            if not row or row[0]!='R2_NEW':raise ValueError('Inherited risk requires separate evidence-bearing reconciliation')
            known=max(cost,Decimal(row[2] or '0'));risk=known if terminal else max(Decimal(row[1]),known)
            db.execute('UPDATE r2_components SET execution_risk=?,execution_known=?,status=? WHERE job=?',(str(risk),str(known),'EXECUTION_KNOWN' if terminal else 'EXECUTING',job))
            db.execute("UPDATE amounts SET reserved=? WHERE job=? AND unit='dune_credits' AND actual IS NULL",(str(risk+Decimal(row[3])),job))
            self._record(db,job,'EXECUTION_OBSERVED',{'cost':str(cost),'terminal':terminal,'receipt':evidence})
            if known>20:db.execute('INSERT OR REPLACE INTO r2_meta VALUES(?,?)',('halt','EXECUTION_CAP20_EXCEEDED:'+str(known)))
            if Decimal(self.snapshot(db)['dune_credits']['cumulative_risk'])>CAP:db.execute('INSERT OR REPLACE INTO r2_meta VALUES(?,?)',('halt','CUMULATIVE500_EXCEEDED'))

    def reserve_export(self,job,upper,evidence,*,observed=False):
        upper=valid(upper,'dune_credits')
        if not evidence or not evidence.get('rate_evidence') or not evidence.get('result_metadata'):raise ValueError('Full result metadata and applicable rate evidence required')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE');self._amended(db);snap=self.snapshot(db)
            row=db.execute('SELECT origin,execution_risk,export_risk,status FROM r2_components WHERE job=?',(job,)).fetchone()
            amount=db.execute("SELECT actual FROM amounts WHERE job=? AND unit='dune_credits'",(job,)).fetchone()
            if not row or row[0]!='R2_NEW' or not amount or amount[0] is not None:raise ValueError('New open job required')
            if row[3] in ('RESERVED','EXECUTING','UNKNOWN_EXECUTION'):raise RuntimeError('Wait for reliable terminal execution charge')
            upper=max(upper,Decimal(row[2]));delta=upper-Decimal(row[2]);remaining=snap['dune_credits']['remaining']
            if not observed:
                self._authorized(db)
                if remaining is None or delta>Decimal(remaining):raise RuntimeError('Necessary full export does not fit cumulative500/provider allowance')
            db.execute('UPDATE r2_components SET export_risk=?,evidence=? WHERE job=?',(str(upper),json.dumps(evidence,default=str),job))
            db.execute("UPDATE amounts SET reserved=? WHERE job=? AND unit='dune_credits'",(str(Decimal(row[1])+upper),job))
            self._record(db,job,'EXPORT_ENVELOPE',{'upper':str(upper),'observed':observed,'evidence':evidence,'authorization_id':AUTH})
            if observed and (remaining is None or delta>Decimal(remaining)):db.execute('INSERT OR REPLACE INTO r2_meta VALUES(?,?)',('halt','OBSERVED_EXPORT_RISK_EXCEEDS_REMAINING_POOL'))

def amend(work,proof):
    """Explicit root-only local amendment; proof points at the archived attachment.

    Safe to repeat: AUTH is unique and returns its original receipt. Neither
    duplication nor a new runtime/session grants another500. No API is called.
    """
    work=Path(work).resolve();proof=_validate_proof(work,proof)
    if (work/'private/network_worker.lock').exists():raise RuntimeError('Stop the single network writer before budget amendment')
    path=work/'private/shared_budget_r4.sqlite'
    if not path.is_file():raise ValueError('The continued shared ledger must exist')
    legacy=RevisionLedger(path);receipt_path=work/'private/stage1d_authority/CUMULATIVE500_AMENDMENT_RECEIPT.json'
    with legacy.connection() as db:
        db.execute('BEGIN IMMEDIATE')
        if not db.execute("SELECT 1 FROM r2_meta WHERE key='authorization_id' AND value=?",(INHERITED_AUTH,)).fetchone():raise ValueError('Original continued ledger identity required')
        oldtables=_old_tables(db);before=legacy.snapshot(db)
        db.execute('CREATE TABLE IF NOT EXISTS stage1d_budget_authorizations(authorization_id TEXT PRIMARY KEY,source_sha256 TEXT,proof_json TEXT,receipt_json TEXT,utc TEXT)')
        db.execute('CREATE TABLE IF NOT EXISTS stage1d_budget_meta(key TEXT PRIMARY KEY,value TEXT)')
        existing=db.execute('SELECT proof_json,receipt_json FROM stage1d_budget_authorizations WHERE authorization_id=?',(AUTH,)).fetchone()
        if existing:
            if canonical(json.loads(existing[0]))!=canonical(proof):raise ValueError('An existing amendment cannot change evidence or scopes')
            result=json.loads(existing[1])
            active=db.execute("SELECT value FROM stage1d_budget_meta WHERE key='active_authorization_id'").fetchone()
            if not active or active[0]!=AUTH:raise ValueError('Active amendment changed')
        else:
            if db.execute('SELECT 1 FROM stage1d_budget_authorizations').fetchone():raise ValueError('Another amendment requires a separate explicit policy')
            db.execute('INSERT INTO stage1d_budget_authorizations VALUES(?,?,?,?,?)',(AUTH,proof['source_sha256'],json.dumps(proof,sort_keys=True),'{}',now()))
            db.execute('INSERT INTO stage1d_budget_meta VALUES(?,?)',('active_authorization_id',AUTH))
            controller=object.__new__(Stage1DLedger);controller.path=str(path);after=controller.snapshot(db)
            if _old_tables(db)!=oldtables:raise ValueError('An inherited ledger table changed during the cap amendment')
            if any(before[u]!=after[u] for u in before if u!='dune_credits'):raise ValueError('Other provider budgets changed')
            stable=('actual','reserved','cumulative_risk','known_actual_lower_bound','legacy_risk','r2_risk','export_upper_not_actual','unknown_or_pending_risk','execution_uncertainty_risk','execution_peak_discrepancy_risk','confirmed_allowance')
            if any(before['dune_credits'][k]!=after['dune_credits'][k] for k in stable):raise ValueError('Money facts or provider allowance changed')
            result={'schema_version':'stage1d-cumulative500-amendment-v1','authorization_id':AUTH,'proof':proof,
                    'before_snapshot':before,'after_snapshot':after,'old_tables_before':oldtables,'old_tables_after':_old_tables(db),
                    'all_old_money_and_unknown_risk_unchanged':True,'old_limits_and_r2_meta_unchanged':True,'new_allowance_added':False,
                    'cap_semantics':'500 total across old and current executions, exports and unknown risks; not additional500',
                    'network_requests':0,'utc':now()}
            payload=json.dumps(result,sort_keys=True)
            db.execute('UPDATE stage1d_budget_authorizations SET receipt_json=? WHERE authorization_id=?',(payload,AUTH))
            db.execute('CREATE TABLE IF NOT EXISTS stage1d_journal(n INTEGER PRIMARY KEY,kind TEXT,payload TEXT,utc TEXT)')
            db.execute('INSERT INTO stage1d_journal(kind,payload,utc) VALUES(?,?,?)',('CUMULATIVE_CAP_REPLACED_WITH500',payload,now()))
    atomic_json(receipt_path,result)
    return result
