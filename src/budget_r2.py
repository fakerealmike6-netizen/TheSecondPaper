"""R2 cumulative accounting. Historical rows are copied unchanged, never regranted.

New execution and export components replace one another atomically in one risk
amount; an observed cost is not added on top of its containing reservation.
"""
import hashlib,json,sqlite3
from pathlib import Path
from decimal import Decimal
from datetime import datetime,timezone
from contextlib import closing
from budget import Ledger,valid
from budget_r1 import consistent_backup

AUTH='STAGE1B_R2_CAP20_CUMULATIVE100_V1'
CAP=Decimal('100')
def now():return datetime.now(timezone.utc).isoformat()
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

class RevisionLedger(Ledger):
    def __init__(self,path):
        super().__init__(path)
        with self.connection() as db:
            db.execute('CREATE TABLE IF NOT EXISTS r2_meta(key TEXT PRIMARY KEY,value TEXT)')
            db.execute('''CREATE TABLE IF NOT EXISTS r2_components(job TEXT PRIMARY KEY,
              origin TEXT NOT NULL,execution_risk TEXT NOT NULL,execution_known TEXT,
              export_risk TEXT NOT NULL,export_actual TEXT,status TEXT NOT NULL,evidence TEXT NOT NULL)''')
            db.execute('CREATE TABLE IF NOT EXISTS r2_observations(n INTEGER PRIMARY KEY,job TEXT,kind TEXT,payload TEXT,utc TEXT)')
    def initialize(self,source_sha256,evidence):
        if evidence.get('status')!='USER_CONFIRMED' or str(evidence.get('execution_cap_credits'))!='20' or evidence.get('authorization_id')!=AUTH:
            raise ValueError('Explicit R2 cap20 cumulative100 user confirmation required')
        if evidence.get('payment_method_added') is not False or evidence.get('extra_credits_enabled') is not False:raise ValueError('No extra payment authorization')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute("SELECT value FROM r2_meta WHERE key='source_sha256'").fetchone()
            if old:
                if old[0]!=source_sha256:raise RuntimeError('Different immutable migration input')
                return False
            tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            for job,reserved,actual in db.execute("SELECT job,reserved,actual FROM amounts WHERE unit='dune_credits'").fetchall():
                history=db.execute('SELECT execution_known,export_actual,bucket,accounting_status,evidence FROM dune_risk WHERE job=?',(job,)).fetchone() if 'dune_risk' in tables else None
                known=history[0] if history else None
                # A historical whole-job risk remains whole; no speculative release.
                db.execute('INSERT INTO r2_components VALUES(?,?,?,?,?,?,?,?)',(job,'INHERITED',reserved,known,'0',history[1] if history else None,'INHERITED_UNCHANGED',json.dumps({'source_sha256':source_sha256,'historical_risk':history})))
            for k,v in {'source_sha256':source_sha256,'authorization_id':AUTH,'cap':'100','warning':'80','execution_cap':'20','authorization_evidence':json.dumps(evidence),'initialized_utc':now()}.items():db.execute('INSERT INTO r2_meta VALUES(?,?)',(k,v))
            db.execute("UPDATE limits SET cap='100' WHERE unit='dune_credits'")
            return True
    def snapshot(self,db=None):
        if db is None:
            with self.connection() as cx:return self.snapshot(cx)
        out=Ledger.snapshot(self,db);d=out['dune_credits'];risk=Decimal(d['actual'])+Decimal(d['reserved'])
        tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'r2_components' not in tables:return out
        known=Decimal(0);legacy=Decimal(0);new=Decimal(0);bounded=Decimal(0);unknown=Decimal(0);execution_uncertainty=Decimal(0);peak_discrepancy=Decimal(0)
        for job,origin,er,ek,xr,xa,status,evidence in db.execute('SELECT * FROM r2_components'):
            amount=db.execute("SELECT reserved,actual FROM amounts WHERE job=? AND unit='dune_credits'",(job,)).fetchone()
            value=Decimal(amount[1] if amount[1] is not None else amount[0])
            if origin=='INHERITED':legacy+=value
            else:new+=value
            if origin=='R2_NEW':
                # execution_known deliberately retains the largest observation.
                # A running estimate can exceed the terminal provider charge;
                # classify that retained excess as risk, never final actual.
                terminal=self.final_execution_cost(job,db)
                jobknown=(terminal if terminal is not None else Decimal(0))+Decimal(xa or '0')
                difference=max(Decimal(0),Decimal(er)-(terminal if terminal is not None else Decimal(0)))
                execution_uncertainty+=difference
                if terminal is not None:peak_discrepancy+=difference
            else:jobknown=Decimal(amount[1]) if amount[1] is not None else Decimal(ek or '0')+Decimal(xa or '0')
            known+=jobknown
            residual=max(Decimal(0),value-jobknown)
            if origin=='R2_NEW':
                export_bound=min(residual,max(Decimal(0),Decimal(xr)-Decimal(xa or '0'))) if status=='BOUNDED_ACCOUNTING_NOT_FINAL' else Decimal(0)
                bounded+=export_bound;unknown+=residual-export_bound
            elif amount[1] is None:
                inherited=json.loads(evidence).get('historical_risk') if origin=='INHERITED' else None
                proven_bound=status=='BOUNDED_ACCOUNTING_NOT_FINAL' or (inherited is not None and inherited[3]=='BOUNDED_ACCOUNTING_NOT_FINAL')
                if proven_bound:bounded+=residual
                else:unknown+=residual
        halt=db.execute("SELECT value FROM r2_meta WHERE key='halt'").fetchone()
        d.update(authorization_id=AUTH,cumulative_risk=str(risk),known_actual_lower_bound=str(known),legacy_risk=str(legacy),r2_risk=str(new),export_upper_not_actual=str(bounded),unknown_or_pending_risk=str(unknown),execution_uncertainty_risk=str(execution_uncertainty),execution_peak_discrepancy_risk=str(peak_discrepancy),known_actual_basis='Inherited reliable charges plus latest terminal R2 execution observations; provisional observed peaks are retained uncertainty',warning_at_80=risk>=80,warning_requires_pause=False,remaining_for_new_jobs=d['remaining'],historical_subbucket_limits_apply_to_new_jobs=False,halt_reason=halt[0] if halt else None)
        if halt:d['overrun']=True
        return out
    def final_execution_cost(self,job,db=None):
        """Read-only terminal charge classification; never updates a risk row."""
        if db is None:
            with self.connection() as cx:return self.final_execution_cost(job,cx)
        for (payload,) in db.execute("SELECT payload FROM r2_observations WHERE job=? AND kind='EXECUTION_OBSERVED' ORDER BY n DESC",(job,)):
            observed=json.loads(payload)
            if observed.get('terminal') is True:return valid(observed['cost'],'dune_credits')
        return None
    def _authorized(self,db):
        if not db.execute("SELECT 1 FROM r2_meta WHERE key='authorization_id' AND value=?",(AUTH,)).fetchone():raise RuntimeError('R2 authorization not migrated')
        snap=self.snapshot(db)
        if any(v['overrun'] or v['actual_exceeded_reservation'] for v in snap.values()):raise RuntimeError('Cumulative budget halted')
        return snap
    def reserve(self,job,provider,purpose,amounts):
        if provider=='metasleuth' or any(u.startswith('meta_') for u in amounts):raise RuntimeError('R2 authorizes zero new MetaSleuth calls')
        if 'dune_credits' in amounts:raise ValueError('Use reserve_dune_job to separate execution/export components')
        return Ledger.reserve(self,job,provider,purpose,amounts)
    def reserve_dune_job(self,job,purpose,execution_estimate='20',export_estimate='0'):
        er=valid(execution_estimate,'dune_credits');xr=valid(export_estimate,'dune_credits')
        if er!=20:raise ValueError('Unknown new execution must reserve20')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE');snap=self._authorized(db)
            if db.execute("SELECT 1 FROM r2_components WHERE origin='R2_NEW' AND status IN ('RESERVED','EXECUTING','UNKNOWN_EXECUTION')").fetchone():raise RuntimeError('At most one in-flight SQL execution')
            remaining=snap['dune_credits']['remaining']
            if remaining is None or er+xr>Decimal(remaining):raise RuntimeError('Insufficient cumulative100/provider allowance')
            db.execute('INSERT INTO jobs VALUES(?,?,?,?,?)',(job,'dune',purpose,now(),'RESERVED'))
            db.execute('INSERT INTO amounts VALUES(?,?,?,NULL)',(job,'dune_credits',str(er+xr)))
            db.execute('INSERT INTO r2_components VALUES(?,?,?,?,?,?,?,?)',(job,'R2_NEW',str(er),None,str(xr),None,'RESERVED','{}'))
    def _record(self,db,job,kind,payload):db.execute('INSERT INTO r2_observations(job,kind,payload,utc) VALUES(?,?,?,?)',(job,kind,json.dumps(payload,default=str),now()))
    def observe_execution(self,job,cost,evidence,terminal=False):
        cost=valid(cost,'dune_credits')
        if not evidence or not evidence.get('sha256'):raise ValueError('Verified execution receipt required')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT origin,execution_risk,execution_known,export_risk FROM r2_components WHERE job=?',(job,)).fetchone()
            if not row:raise ValueError('Unknown job')
            if row[0]!='R2_NEW':raise ValueError('Inherited risk requires a separate evidence-bearing reconciliation')
            known=max(cost,Decimal(row[2] or '0'));risk=known if terminal else max(Decimal(row[1]),known)
            db.execute('UPDATE r2_components SET execution_risk=?,execution_known=?,status=? WHERE job=?',(str(risk),str(known),'EXECUTION_KNOWN' if terminal else 'EXECUTING',job))
            db.execute("UPDATE amounts SET reserved=? WHERE job=? AND unit='dune_credits' AND actual IS NULL",(str(risk+Decimal(row[3])),job))
            self._record(db,job,'EXECUTION_OBSERVED',{'cost':str(cost),'terminal':terminal,'receipt':evidence})
            if known>20:db.execute('INSERT OR REPLACE INTO r2_meta VALUES(?,?)',('halt','EXECUTION_CAP20_EXCEEDED:'+str(known)))
            if Decimal(self.snapshot(db)['dune_credits']['cumulative_risk'])>100:db.execute('INSERT OR REPLACE INTO r2_meta VALUES(?,?)',('halt','CUMULATIVE100_EXCEEDED'))
    def reserve_export(self,job,upper,evidence,*,observed=False):
        upper=valid(upper,'dune_credits')
        if not evidence or not evidence.get('rate_evidence') or not evidence.get('result_metadata'):raise ValueError('Full result metadata and applicable rate evidence required')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE');snap=self.snapshot(db)
            row=db.execute('SELECT origin,execution_risk,export_risk,status FROM r2_components WHERE job=?',(job,)).fetchone()
            amount=db.execute("SELECT actual FROM amounts WHERE job=? AND unit='dune_credits'",(job,)).fetchone()
            if not row or row[0]!='R2_NEW' or amount[0] is not None:raise ValueError('New open job required')
            if row[3] in ('RESERVED','EXECUTING','UNKNOWN_EXECUTION'):raise RuntimeError('Wait for reliable terminal execution charge')
            # Never discard an already dispatched/unknown export envelope.
            upper=max(upper,Decimal(row[2]));delta=upper-Decimal(row[2]);remaining=snap['dune_credits']['remaining']
            if not observed:
                self._authorized(db)
                if remaining is None or delta>Decimal(remaining):raise RuntimeError('Necessary full export does not fit cumulative100; retain execution metadata')
            db.execute('UPDATE r2_components SET export_risk=?,evidence=? WHERE job=?',(str(upper),json.dumps(evidence,default=str),job))
            db.execute("UPDATE amounts SET reserved=? WHERE job=? AND unit='dune_credits'",(str(Decimal(row[1])+upper),job))
            self._record(db,job,'EXPORT_ENVELOPE',{'upper':str(upper),'observed':observed,'evidence':evidence})
            if observed and (remaining is None or delta>Decimal(remaining)):db.execute('INSERT OR REPLACE INTO r2_meta VALUES(?,?)',('halt','OBSERVED_EXPORT_RISK_EXCEEDS_REMAINING_POOL'))
    def close_job(self,job,*,exported,evidence):
        if not evidence.get('request_set_closed') or not evidence.get('source_receipt_sha256') or not evidence.get('no_unknown_attempts'):raise ValueError('Verified closed request set required')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT origin,execution_risk,execution_known,export_risk,status FROM r2_components WHERE job=?',(job,)).fetchone()
            if not row or row[0]!='R2_NEW' or row[2] is None or row[4] in ('RESERVED','EXECUTING'):raise ValueError('Terminal known execution required')
            status='BOUNDED_ACCOUNTING_NOT_FINAL' if exported else 'FINAL_ACTUAL'
            if exported and (not evidence.get('full_page_chain_verified') or not evidence.get('rate_evidence')):raise ValueError('Full page chain and metering evidence required')
            if not exported and Decimal(row[3])!=0:raise ValueError('Existing export risk cannot become no-export actual')
            if not exported:db.execute("UPDATE amounts SET actual=? WHERE job=? AND unit='dune_credits'",(row[2],job))
            db.execute('UPDATE r2_components SET status=?,export_actual=?,evidence=? WHERE job=?',(status,None if exported else '0',json.dumps(evidence,default=str),job))
            db.execute('UPDATE jobs SET status=? WHERE id=?',('UNKNOWN_RESERVED' if exported else 'SETTLED',job))
            self._record(db,job,status,evidence)
            return status
    def detailed_jobs(self):
        with self.connection() as db:
            cur=db.execute('SELECT * FROM r2_components ORDER BY origin,job');names=[x[0] for x in cur.description]
            return [dict(zip(names,row)) for row in cur]

def migrate_revision(work,baseline,confirmation):
    """Read-only consistent backups; callers first confirm old workers stopped.

    Immutable source attempts keep original absolute receipt paths. Portability
    packagers must separately map those files; original identities stay intact.
    """
    work=Path(work).resolve();baseline=Path(baseline).resolve();private=work/'private';private.mkdir(parents=True,exist_ok=True)
    state=json.loads((baseline/'RUN_STATE.json').read_text(encoding='utf-8'))
    if state.get('checkpoint')!='CHECKPOINT_1B_R1_REACHED' or state.get('new_network_submissions_allowed') is not False:raise RuntimeError('Baseline worker stop checkpoint not established')
    # An identical authorization may not fund a second revision database.
    for sibling in work.parent.glob('*/private/shared_budget_r2.sqlite'):
        if sibling.resolve()==(private/'shared_budget_r2.sqlite').resolve():continue
        with closing(sqlite3.connect(sibling.resolve().as_uri()+'?mode=ro',uri=True)) as db:
            if db.execute("SELECT name FROM sqlite_master WHERE name='r2_meta'").fetchone() and db.execute("SELECT 1 FROM r2_meta WHERE key='authorization_id' AND value=?",(AUTH,)).fetchone():raise RuntimeError('Authorization already migrated in another revision; resume it')
    receipt_path=private/'BUDGET_MIGRATION.json'
    if receipt_path.exists():
        receipt=json.loads(receipt_path.read_text(encoding='utf-8'))
        if receipt['baseline_run']!=baseline.name:raise RuntimeError('Different baseline')
        ledger=RevisionLedger(private/'shared_budget_r2.sqlite')
        ledger.initialize(receipt['ledger_snapshot']['source_sha256_before'],confirmation)
        return receipt
    snapshots=private/'migration_snapshots';snapshots.mkdir(exist_ok=True)
    sources={'ledger':baseline/'private/shared_budget_r1.sqlite','attempts':baseline/'private/dune_request_attempts.sqlite'}
    receipts={}
    for name,path in sources.items():receipts[name]=consistent_backup(path,snapshots/(name+'.sqlite'))
    consistent_backup(snapshots/'ledger.sqlite',private/'shared_budget_r2.sqlite')
    consistent_backup(snapshots/'attempts.sqlite',private/'dune_request_attempts.sqlite')
    ledger=RevisionLedger(private/'shared_budget_r2.sqlite');ledger.initialize(receipts['ledger']['source_sha256_before'],confirmation)
    with closing(sqlite3.connect(snapshots/'ledger.sqlite')) as old,ledger.connection() as new:
        names=[r[0] for r in old.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
        comparisons={}
        for table in names:
            before=old.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall();after=new.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall()
            if table=='limits':comparisons[table]={'historical_rows':len(before),'only_dune_cap_changed':all(a==b or (a[0]=='dune_credits' and b[0]=='dune_credits' and a[2:]==b[2:] and b[1]=='100') for a,b in zip(before,after))}
            else:
                if before!=after:raise RuntimeError('Historical table changed: '+table)
                comparisons[table]={'rows':len(before),'identical':True}
    report={'authorization_id':AUTH,'baseline_run':baseline.name,'new_run':work.name,'at_utc':now(),'old_checkpoint_evidence':{'path':str(baseline/'RUN_STATE.json'),'sha256':sha(baseline/'RUN_STATE.json'),'new_network_submissions_allowed':False},'old_worker_process_check':'Get-Process python,python3,pythonw returned no processes before migration; no command-line access claimed','ledger_snapshot':receipts['ledger'],'attempts_snapshot':receipts['attempts'],'historical_table_comparison':comparisons,'source_files_unchanged':all(sha(sources[k])==v['source_sha256_after'] for k,v in receipts.items()),'initial_snapshot':ledger.snapshot(),'legacy_job_and_page_identity_policy':'Exact inherited job/execution/page keys and attempt states, immutable historical receipt paths, no release of old whole-job risk','single_writer':str(private/'shared_budget_r2.sqlite')}
    from page_attempts import atomic_json
    atomic_json(receipt_path,report);atomic_json(private/'dune_user_confirmation.json',confirmation)
    for name in ('dune_rate_evidence.json','current_dune_usage.json'):
        source=baseline/'private'/name
        if not (private/name).exists():(private/name).write_bytes(source.read_bytes())
    return report
