"""Stage1B-R1 persistent risk accounting, retaining original unknown consumption.

Evidence-bearing final/upper-bound settlement is distinct from measured cost.
One authorization can fund one revision ledger. No credentials are stored here.
"""
import hashlib,json,sqlite3
from pathlib import Path
from decimal import Decimal
from datetime import datetime,timezone
from contextlib import closing
from budget import Ledger,valid
from dune_cap_exception import CAP5_AUTH,binding,require_cap5_confirmation

AUTH='STAGE1B_R1_INCREMENTAL_10_V1'
def now():return datetime.now(timezone.utc).isoformat()
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def claim_authorization(registry,ledger_path,authorization_id=AUTH):
    """Cross-run idempotency; repeated delivery cannot create a second grant."""
    p=Path(registry);p.parent.mkdir(parents=True,exist_ok=True)
    with closing(sqlite3.connect(p,timeout=20)) as db, db:
        db.execute('CREATE TABLE IF NOT EXISTS claims(id TEXT PRIMARY KEY,ledger TEXT,utc TEXT)')
        db.execute('BEGIN IMMEDIATE')
        identity=str(Path(ledger_path).resolve())
        row=db.execute('SELECT ledger FROM claims WHERE id=?',(authorization_id,)).fetchone()
        if row and row[0]!=identity:raise RuntimeError('Authorization already belongs to another revision ledger; resume that ledger')
        db.execute('INSERT OR IGNORE INTO claims VALUES(?,?,?)',(authorization_id,identity,now()))

def consistent_backup(source,destination):
    source=Path(source).resolve();destination=Path(destination).resolve()
    if destination.exists():raise FileExistsError('Backup is immutable; use existing verified snapshot')
    destination.parent.mkdir(parents=True,exist_ok=True)
    before=sha(source)
    with closing(sqlite3.connect(source.as_uri()+'?mode=ro',uri=True,timeout=20)) as src:
        with closing(sqlite3.connect(destination)) as dst:src.backup(dst)
    after=sha(source)
    if before!=after:raise RuntimeError('Original ledger changed while snapshotting; do not migrate')
    with closing(sqlite3.connect(destination)) as db:
        if db.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise RuntimeError('Backup integrity failure')
    return {'source_sha256_before':before,'source_sha256_after':after,'backup_sha256':sha(destination),'method':'SQLite read-only connection backup API; incorporates committed WAL state','at_utc':now()}

class RevisionLedger(Ledger):
    def __init__(self,path):
        super().__init__(path)
        with self.connection() as db:
            db.execute('CREATE TABLE IF NOT EXISTS r1_meta(key TEXT PRIMARY KEY,value TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS dune_risk(job TEXT PRIMARY KEY,bucket TEXT,initial_reserved TEXT,execution_known TEXT,export_actual TEXT,total_upper TEXT,accounting_status TEXT,evidence TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS observations(id INTEGER PRIMARY KEY,job TEXT,kind TEXT,payload TEXT,utc TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS counted_actions(id TEXT PRIMARY KEY,kind TEXT,utc TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS dune_cap_exceptions(authorization_id TEXT PRIMARY KEY,binding TEXT NOT NULL,used_job TEXT UNIQUE,used_utc TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS dune_job_caps(job TEXT PRIMARY KEY,execution_cap TEXT NOT NULL,export_cap TEXT,logical_cap TEXT NOT NULL,authorization_id TEXT)')
    def initialize(self,legacy_snapshot,legacy_jobs,authorization_evidence):
        """Import the consistent old ledger once, preserving all non-Dune units."""
        digest=sha(legacy_snapshot)
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT value FROM r1_meta WHERE key=?',('legacy_snapshot_sha256',)).fetchone()
            if old:
                if old[0]!=digest:raise RuntimeError('Different legacy ledger version')
                return False
            if db.execute('SELECT count(*) FROM jobs').fetchone()[0]:raise RuntimeError('Destination not empty')
            with closing(sqlite3.connect(Path(legacy_snapshot).resolve().as_uri()+'?mode=ro',uri=True)) as src:
                for table in ('jobs','amounts'):
                    for row in src.execute('SELECT * FROM '+table):db.execute('INSERT INTO '+table+' VALUES('+','.join('?' for _ in row)+')',row)
                for row in src.execute('SELECT * FROM limits'):db.execute('INSERT OR REPLACE INTO limits VALUES(?,?,?,?)',row)
            db.execute("UPDATE limits SET cap='20' WHERE unit='dune_credits'")
            # Meta remains exhausted even though the old request-unit cap had room.
            db.execute("UPDATE limits SET cap='1' WHERE unit='meta_requests'")
            for job,reserved,actual in db.execute("SELECT job,reserved,actual FROM amounts WHERE unit='dune_credits'").fetchall():
                state=legacy_jobs.get(job,{})
                known=state.get('known_execution_cost_credits',state.get('execution_cost_credits'))
                if known is not None:known=str(valid(known,'dune_credits'))
                status='FINAL_ACTUAL' if actual is not None else 'UNKNOWN_RESERVED'
                db.execute('INSERT INTO dune_risk VALUES(?,?,?,?,?,?,?,?)',(job,'LEGACY_STAGE1B',reserved,known,None,None,status,json.dumps({'source':'immutable legacy ledger and saved job receipts','snapshot_sha256':digest})))
            entries={'legacy_snapshot_sha256':digest,'authorization_id':AUTH,'legacy_cap':'10','new_cap':'10','parent_cap':'20','authorization_evidence':json.dumps(authorization_evidence),'initialized_utc':now()}
            for key,value in entries.items():db.execute('INSERT INTO r1_meta VALUES(?,?)',(key,value))
            return True
    def snapshot(self,db=None):
        if db is None:
            with self.connection() as cx:return self.snapshot(cx)
        out=super().snapshot(db)
        exists=db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dune_risk'").fetchone()
        if not exists:return out
        buckets={b:{'cap':'10','risk':'0','known_execution':'0','final_actual':'0','bounded_not_final':'0','unknown_reserved':'0'} for b in ('LEGACY_STAGE1B','R1_NEW')}
        for job,b,initial,known,export,upper,status,evidence in db.execute('SELECT * FROM dune_risk'):
            amount=db.execute("SELECT reserved,actual FROM amounts WHERE job=? AND unit='dune_credits'",(job,)).fetchone()
            risk=Decimal(amount[1] if amount[1] is not None else amount[0]);d=buckets[b]
            d['risk']=str(Decimal(d['risk'])+risk)
            d['known_execution']=str(Decimal(d['known_execution'])+Decimal(known or '0'))
            key='final_actual' if amount[1] is not None else 'bounded_not_final' if upper is not None else 'unknown_reserved'
            d[key]=str(Decimal(d[key])+risk)
        for d in buckets.values():d['remaining']=str(max(Decimal(0),Decimal(d['cap'])-Decimal(d['risk'])))
        halt=db.execute("SELECT value FROM r1_meta WHERE key='halt'").fetchone()
        out['dune_credits']['buckets']=buckets
        out['dune_credits']['remaining_for_new_jobs']=str(min(Decimal(buckets['R1_NEW']['remaining']),Decimal(out['dune_credits']['remaining'] or '0')))
        out['dune_credits']['halt_reason']=None if not halt else halt[0]
        if halt:out['dune_credits']['overrun']=True
        return out
    def reserve(self,job,provider,purpose,amounts):
        if provider=='metasleuth' or any(u.startswith('meta_') for u in amounts):raise RuntimeError('R1 authorizes zero additional MetaSleuth calls')
        if 'dune_credits' not in amounts:return super().reserve(job,provider,purpose,amounts)
        if set(amounts)!={'dune_credits'} or provider!='dune':raise ValueError('Dune reservation unit mismatch')
        n=valid(amounts['dune_credits'],'dune_credits')
        if n>2 or n<=0:raise ValueError('Dune logical execution plus all exports cap2')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE');snap=self.snapshot(db)
            if not db.execute("SELECT 1 FROM r1_meta WHERE key='authorization_id' AND value=?",(AUTH,)).fetchone():raise RuntimeError('R1 grant absent')
            if any(d['overrun'] or d['actual_exceeded_reservation'] for d in snap.values()):raise RuntimeError('Stage halted after overrun')
            if n>Decimal(snap['dune_credits']['remaining_for_new_jobs']):raise RuntimeError('Insufficient R1/parent/provider allowance')
            db.execute('INSERT INTO jobs VALUES(?,?,?,?,?)',(job,provider,purpose,now(),'RESERVED'))
            db.execute('INSERT INTO amounts VALUES(?,?,?,NULL)',(job,'dune_credits',str(n)))
            db.execute('INSERT INTO dune_risk VALUES(?,?,?,?,?,?,?,?)',(job,'R1_NEW',str(n),None,None,None,'UNKNOWN_RESERVED','{}'))
    def reserve_dune_job(self,job,purpose,execution_estimate,export_estimate):
        execution=valid(execution_estimate,'dune_credits');export=valid(export_estimate,'dune_credits')
        if execution>1:raise ValueError('Execution cap1')
        return self.reserve(job,'dune',purpose,{'dune_credits':execution+export})
    def job_caps(self,job,db=None):
        if db is None:
            with self.connection() as cx:return self.job_caps(job,cx)
        row=db.execute('SELECT execution_cap,export_cap,logical_cap,authorization_id FROM dune_job_caps WHERE job=?',(job,)).fetchone()
        return {'execution':Decimal(row[0]),'export':None if row[1] is None else Decimal(row[1]),'logical':Decimal(row[2]),'authorization_id':row[3]} if row else {'execution':Decimal(1),'export':None,'logical':Decimal(2),'authorization_id':None}
    def register_cap5_exception(self,control):
        frozen=binding(control);encoded=json.dumps(frozen,sort_keys=True)
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT binding FROM dune_cap_exceptions WHERE authorization_id=?',(CAP5_AUTH,)).fetchone()
            if row:
                if row[0]!=encoded:raise RuntimeError('One-time exception binding cannot be changed')
                return False
            if not db.execute("SELECT 1 FROM r1_meta WHERE key='authorization_id' AND value=?",(AUTH,)).fetchone():raise RuntimeError('Existing R1 budget grant absent')
            db.execute('INSERT INTO dune_cap_exceptions VALUES(?,?,NULL,NULL)',(CAP5_AUTH,encoded));return True
    def reserve_cap5_exception(self,control,purpose):
        frozen=require_cap5_confirmation(control);encoded=json.dumps(frozen,sort_keys=True)
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            auth=db.execute('SELECT binding,used_job FROM dune_cap_exceptions WHERE authorization_id=?',(CAP5_AUTH,)).fetchone()
            if not auth or auth[0]!=encoded:raise RuntimeError('Exception not registered with this exact binding')
            if auth[1] is not None:raise RuntimeError('One-time exception already consumed; restart does not restore the slot')
            snap=self.snapshot(db)
            if any(v['overrun'] or v['actual_exceeded_reservation'] for v in snap.values()):raise RuntimeError('Stage halted')
            if Decimal(snap['dune_credits']['remaining_for_new_jobs'])<6:raise RuntimeError('Full six-credit exception reservation does not fit existing R1/parent/provider allowance')
            for job in frozen['prior_failed_job_ids']:
                row=db.execute('SELECT bucket FROM dune_risk WHERE job=?',(job,)).fetchone()
                if not row or row[0]!='R1_NEW':raise RuntimeError('Exception must retain its two original R1 job bindings')
            job=CAP5_AUTH
            db.execute('INSERT INTO jobs VALUES(?,?,?,?,?)',(job,'dune',purpose,now(),'RESERVED'))
            db.execute('INSERT INTO amounts VALUES(?,?,?,NULL)',(job,'dune_credits','6'))
            db.execute('INSERT INTO dune_risk VALUES(?,?,?,?,?,?,?,?)',(job,'R1_NEW','6',None,None,None,'UNKNOWN_RESERVED',encoded))
            db.execute('INSERT INTO dune_job_caps VALUES(?,?,?,?,?)',(job,'5','1','6',CAP5_AUTH))
            db.execute('UPDATE dune_cap_exceptions SET used_job=?,used_utc=? WHERE authorization_id=?',(job,now(),CAP5_AUTH))
            return job
    def void_unsubmitted(self,job,evidence):
        if not isinstance(evidence,dict) or evidence.get('dispatch_attempt_count')!=0 or evidence.get('request_journal_verified') is not True or evidence.get('job_state')!='PLANNED' or not evidence.get('unsubmitted_state_sha256'):
            raise ValueError('Proven unsubmitted planned request required')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT initial_reserved,execution_known,export_actual,total_upper FROM dune_risk WHERE job=?',(job,)).fetchone()
            amount=db.execute("SELECT actual FROM amounts WHERE job=? AND unit='dune_credits'",(job,)).fetchone()
            if not row or row[1] is not None or row[2] is not None or row[3] is not None or amount[0] is not None or self.job_caps(job,db)['authorization_id'] is not None:raise ValueError('Consumed exception or known charge cannot be voided')
            db.execute("UPDATE amounts SET reserved='0' WHERE job=? AND unit='dune_credits'",(job,))
            db.execute("UPDATE dune_risk SET accounting_status='VOID_UNSUBMITTED',evidence=? WHERE job=?",(json.dumps(evidence),job))
            db.execute("UPDATE jobs SET status='VOID_UNSUBMITTED' WHERE id=?",(job,))
            db.execute('INSERT INTO observations(job,kind,payload,utc) VALUES(?,?,?,?)',(job,'VOID_UNSUBMITTED',json.dumps(evidence),now()))
    def settle(self,job,amounts):
        if 'dune_credits' in amounts:
            if set(amounts)!={'dune_credits'} or amounts['dune_credits'] is not None:
                raise ValueError('Dune final/upper settlement requires reconcile and evidence')
            # Unknown settlement is allowed but releases no risk.
            return super().settle(job,amounts)
        return super().settle(job,amounts)
    def observe_execution(self,job,cost,evidence):
        cost=valid(cost,'dune_credits')
        if not evidence:raise ValueError('Execution evidence required')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            r=db.execute('SELECT execution_known,total_upper FROM dune_risk WHERE job=?',(job,)).fetchone()
            if not r:raise ValueError('Unknown job')
            # Preserve observations; a smaller delayed value cannot erase known use.
            known=max(cost,Decimal(r[0] or 0))
            db.execute('UPDATE dune_risk SET execution_known=? WHERE job=?',(str(known),job))
            db.execute('INSERT INTO observations(job,kind,payload,utc) VALUES(?,?,?,?)',(job,'EXECUTION',json.dumps({'cost':str(cost),'evidence':evidence}),now()))
            caps=self.job_caps(job,db)
            if cost>caps['execution']:db.execute('INSERT OR REPLACE INTO r1_meta VALUES(?,?)',('halt','EXECUTION_CAP_EXCEEDED:'+str(cost)))
            amount=db.execute("SELECT reserved,actual FROM amounts WHERE job=? AND unit='dune_credits'",(job,)).fetchone()
            risk=Decimal(amount[1] if amount[1] is not None else amount[0])
            if known>risk or cost>caps['execution'] or (r[1] is not None and known>Decimal(r[0] or 0)):
                # Retain the export part of the previous bound/reservation;
                # replace, rather than add twice, the execution component.
                prior_execution=Decimal(r[0] or 0) if r[1] is not None else min(caps['execution'],risk)
                increased=max(risk,known+max(Decimal(0),risk-prior_execution))
                if amount[1] is None:db.execute("UPDATE amounts SET reserved=? WHERE job=? AND unit='dune_credits'",(str(increased),job))
                else:
                    # Contradictory final cost is retained as evidence; restore
                    # uncertain risk rather than silently rewriting an actual.
                    db.execute("UPDATE amounts SET reserved=?,actual=NULL WHERE job=? AND unit='dune_credits'",(str(increased),job))
                db.execute("UPDATE dune_risk SET accounting_status='OVERRUN_RECORDED_HALTED',total_upper=NULL WHERE job=?",(job,))
                db.execute('INSERT OR REPLACE INTO r1_meta VALUES(?,?)',('halt','KNOWN_EXECUTION_INVALIDATES_RISK:'+str(known)))
    def reconcile(self,job,*,total_actual=None,export_actual=None,total_upper=None,evidence):
        """Close requests then finalize actual or release only a proven bound.

        Unsupported estimates never reach amounts.actual or lower reservations.
        Final receipts may supersede a prior nonfinal bound asynchronously.
        """
        if (total_actual is None)==(total_upper is None):raise ValueError('Exactly one final actual or proven bound required')
        if not isinstance(evidence,dict) or not evidence.get('request_set_closed') or not evidence.get('source_receipt_sha256'):raise ValueError('Closed request-set and source evidence required')
        actual=None if total_actual is None else valid(total_actual,'dune_credits')
        upper=None if total_upper is None else valid(total_upper,'dune_credits')
        exp=None if export_actual is None else valid(export_actual,'dune_credits')
        if upper is not None:
            needed=('account_context_ref','applicable_rate_evidence','metered_units_evidence','all_attempts_included','rounding_and_minimum_evidence','unknown_attempt_risk_included')
            if any(not evidence.get(k) for k in needed):raise ValueError('An estimate lacks applicable metering/rounding/all-attempt evidence')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            r=db.execute('SELECT initial_reserved,execution_known,accounting_status FROM dune_risk WHERE job=?',(job,)).fetchone()
            if not r:raise ValueError('Unknown job')
            old=db.execute("SELECT reserved,actual FROM amounts WHERE job=? AND unit='dune_credits'",(job,)).fetchone()
            value=actual if actual is not None else upper;known=Decimal(r[1] or 0)
            caps=self.job_caps(job,db)
            if value<known or (exp is not None and value<known+exp) or (actual is not None and exp is not None and known+exp!=actual):raise ValueError('Accounting contradicts observed execution/export cost')
            if old[1] is not None:
                if actual is not None and Decimal(old[1])==actual:return
                raise ValueError('Final accounting cannot be rewritten')
            status='FINAL_ACTUAL' if actual is not None else 'BOUNDED_ACCOUNTING_NOT_FINAL'
            if value>Decimal(r[0]) or value>caps['logical'] or (actual is not None and value>Decimal(old[0])) or (exp is not None and caps['export'] is not None and exp>caps['export']):
                status='OVERRUN_RECORDED_HALTED';db.execute('INSERT OR REPLACE INTO r1_meta VALUES(?,?)',('halt','ACCOUNTING_OVERRUN:'+str(value)))
            if actual is not None:db.execute("UPDATE amounts SET actual=? WHERE job=? AND unit='dune_credits'",(str(actual),job))
            else:db.execute("UPDATE amounts SET reserved=? WHERE job=? AND unit='dune_credits'",(str(upper),job))
            db.execute('UPDATE dune_risk SET export_actual=?,total_upper=?,accounting_status=?,evidence=? WHERE job=?',(None if exp is None else str(exp),None if upper is None else str(upper),status,json.dumps(evidence),job))
            db.execute('UPDATE jobs SET status=? WHERE id=?',('SETTLED' if actual is not None else 'UNKNOWN_RESERVED',job))
            db.execute('INSERT INTO observations(job,kind,payload,utc) VALUES(?,?,?,?)',(job,status,json.dumps({'value':str(value),'export_actual':None if exp is None else str(exp),'evidence':evidence}),now()))
    def count_action(self,identity,kind,cap):
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM counted_actions WHERE id=?',(identity,)).fetchone():raise RuntimeError('Action already dispatched or uncertain')
            if db.execute('SELECT count(*) FROM counted_actions WHERE kind=?',(kind,)).fetchone()[0]>=cap:raise RuntimeError('Metadata/resource action cap reached')
            db.execute('INSERT INTO counted_actions VALUES(?,?,?)',(identity,kind,now()))
    def detailed_jobs(self):
        with self.connection() as db:
            names=[d[0] for d in db.execute('SELECT * FROM dune_risk').description]
            return [dict(zip(names,r)) for r in db.execute('SELECT * FROM dune_risk ORDER BY bucket,job')]
