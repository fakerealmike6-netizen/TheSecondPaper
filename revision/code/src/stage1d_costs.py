"""Evidence-bound terminal execution reconciliation for authorized Stage1D jobs.

No network or reconciliation occurs on import. Historical observations and export
components remain intact. The inherited provisional peak is never called actual.
"""
from decimal import Decimal
import hashlib, json
from pathlib import Path
from dune_r4 import ContextDuneR4, TERMINAL, _portable
from context_access_r3 import now, read, sha
from page_attempts import atomic_json

AUTH='STAGE1D_BATCH01_REFERENCE_FULL_V1'

def canonical(value):return json.dumps(_portable(value),sort_keys=True,separators=(',',':')).encode()
def inside(root,value):
    path=(root/Path(value)).resolve()
    if not path.is_relative_to(root):raise ValueError('Cost evidence path escapes Stage1D workspace')
    current=root
    for piece in path.relative_to(root).parts:
        current=current/piece
        if current.is_symlink() or getattr(current,'is_junction',lambda:False)():raise ValueError('Linked cost evidence is not accepted')
    return path

class Stage1DDune(ContextDuneR4):
    def _verified_terminal(self,state,body,receipt):
        if not isinstance(body,dict) or body.get('state') not in TERMINAL:raise ValueError('Reliable terminal execution is required for reconciliation')
        if receipt.get('http_status')!=200 or receipt.get('error_class') or body.get('error') and body.get('state')=='QUERY_STATE_COMPLETED':raise ValueError('Reliable HTTP 200 terminal receipt required')
        if receipt.get('operation') not in ('status','execute'):raise ValueError('Execution cost must come from execution/status receipt')
        if not state.get('execution_id') or body.get('execution_id')!=state['execution_id']:raise ValueError('Terminal execution identity mismatch')
        if receipt.get('operation')=='status' and receipt.get('execution_id')!=state['execution_id']:raise ValueError('Receipt execution identity mismatch')
        if state.get('state')!=body['state']:raise ValueError('Saved job terminal state mismatch')
        value=body.get('execution_cost_credits')
        if isinstance(value,bool) or value is None:raise ValueError('Exact terminal execution fee required')
        cost=Decimal(str(value))
        cap=self.db.execution_cap(state['logical_job_id']) if hasattr(self.db,'execution_cap') else Decimal(20)
        if state.get('submission_execution_cap_credits') is not None:
            if Decimal(state['submission_execution_cap_credits'])!=cap or state.get('submission_policy')!=self.db.submission_policy(state['logical_job_id']):
                raise ValueError('Saved submission cap differs from immutable ledger policy')
        if not cost.is_finite() or cost<0 or cost>cap:raise ValueError('Terminal execution cost conflicts with submission cap'+str(cap))
        if Decimal(str(state.get('execution_cost_credits') or 0))>cap:raise ValueError('Historical cap violation cannot be erased')
        freeze=inside(self.w,state['scope_freeze_path'])
        if sha(freeze)!=state.get('scope_freeze_sha256'):raise ValueError('SQL freeze byte identity mismatch')
        frozen=read(freeze)
        if frozen.get('schema_version')!='stage1d-sql-freeze-v1' or frozen.get('authorization_id')!=AUTH:raise ValueError('Historical or unauthorized SQL freeze cannot release risk')
        folder=inside(self.w,'private/dune_r2_jobs/'+state['sql_sha256']+('_r4_retry1' if state.get('retry_number') else ''))
        sql=folder/'query.sql'
        if sha(sql)!=state['sql_sha256'] or frozen.get('sql_sha256')!=state['sql_sha256']:raise ValueError('Exact submitted SQL identity mismatch')
        from stage1d_runtime import Runtime
        Runtime().verify_sql_freeze(freeze,self.w,sql_path=sql)
        raw=inside(self.w,receipt['raw_path']);raw_bytes=raw.read_bytes()
        if hashlib.sha256(raw_bytes).hexdigest()!=receipt.get('sha256') or len(raw_bytes)!=receipt.get('raw_bytes'):raise ValueError('Original response SHA/size missing or mismatched')
        parsed=json.loads(raw_bytes,parse_float=Decimal)
        if canonical(parsed)!=canonical(body):raise ValueError('Terminal body is not the SHA-bound original response')
        receipt_path=inside(self.w,'logs/'+receipt['request_id']+'.json')
        if canonical(read(receipt_path))!=canonical(receipt):raise ValueError('Persisted request receipt differs')
        with self.db.connection() as db:
            component=db.execute('SELECT * FROM r2_components WHERE job=?',(state['logical_job_id'],)).fetchone()
            if not component or component[1]!='R2_NEW':raise ValueError('Inherited ledger component cannot be reconciled by Stage1D')
            if Decimal(component[3] or '0')>cap:raise ValueError('Historical component cap violation cannot be erased')
            amount=db.execute("SELECT actual FROM amounts WHERE job=? AND unit='dune_credits'",(state['logical_job_id'],)).fetchone()
            if not amount or amount[0] is not None and Decimal(amount[0])!=cost+Decimal(component[5] or '0'):raise ValueError('Settled actual conflicts with terminal evidence; actual is retained')
            if db.execute("SELECT value FROM r2_meta WHERE key='halt'").fetchone():raise ValueError('Existing budget halt requires separate conflict handling')
            existing=db.execute("SELECT payload FROM r2_observations WHERE job=? AND kind='EXECUTION_OBSERVED' ORDER BY n DESC",(state['logical_job_id'],)).fetchall()
            for (payload,) in existing:
                previous=json.loads(payload)
                if previous.get('terminal'):
                    old_receipt=previous.get('receipt',{})
                    if old_receipt.get('utc') and receipt.get('utc') and old_receipt['utc']>receipt['utc']:raise ValueError('An older terminal receipt cannot supersede newer billing evidence')
                    break
        evidence={'authorization_id':AUTH,'sql_freeze_path':freeze.relative_to(self.w).as_posix(),'sql_freeze_sha256':sha(freeze),
                  'sql_sha256':sha(sql),'execution_id':state['execution_id'],'terminal_state':body['state'],'terminal_cost_credits':str(cost),
                  'raw_path':raw.relative_to(self.w).as_posix(),'raw_sha256':sha(raw),'receipt_path':receipt_path.relative_to(self.w).as_posix(),
                  'receipt_sha256':sha(receipt_path),'request_id':receipt['request_id'],'http_status':200}
        identity=hashlib.sha256(canonical({'job':state['logical_job_id'],'evidence':evidence})).hexdigest()
        return cost,evidence,identity

    def _rows(self,db,job):
        component=db.execute('SELECT * FROM r2_components WHERE job=?',(job,)).fetchone()
        amount=db.execute("SELECT * FROM amounts WHERE job=? AND unit='dune_credits'",(job,)).fetchone()
        return {'component':list(component),'amount':list(amount),'job':list(db.execute('SELECT * FROM jobs WHERE id=?',(job,)).fetchone())}

    def _protected_history(self,db,job,observation_max=None):
        records={table:db.execute('SELECT * FROM '+table+' WHERE '+key+'!=? ORDER BY rowid',(job,)).fetchall()
                 for table,key in (('amounts','job'),('r2_components','job'),('jobs','id'))}
        if observation_max is None:observation_max=db.execute('SELECT COALESCE(MAX(n),0) FROM r2_observations').fetchone()[0]
        records['old_observations']=db.execute('SELECT * FROM r2_observations WHERE n<=? ORDER BY n',(observation_max,)).fetchall()
        return {'observation_max':observation_max,'tables':{name:{'rows':len(rows),'sha256':hashlib.sha256(canonical(rows)).hexdigest()} for name,rows in records.items()}}

    def observe_execution_charge(self,state,body,receipt):
        if not isinstance(body,dict) or body.get('state') not in TERMINAL:
            # Normal progress observations never release peak/unknown risk.
            return super().observe_execution_charge(state,body,receipt)
        cost,evidence,identity=self._verified_terminal(state,body,receipt)
        job=state['logical_job_id'];path=self.w/'private/stage1d_cost_reconciliation'/(identity+'.json')
        with self.db.connection() as db:
            db.execute('CREATE TABLE IF NOT EXISTS stage1d_cost_reconciliations(identity TEXT PRIMARY KEY,job TEXT,payload TEXT,utc TEXT)')
            old=db.execute('SELECT payload FROM stage1d_cost_reconciliations WHERE identity=?',(identity,)).fetchone()
            before=self._rows(db,job);before_snapshot=self.db.snapshot(db);protected=self._protected_history(db,job)
        if old:
            result=json.loads(old[0])
            if Decimal(before['component'][2])!=cost:raise ValueError('A reconciled execution component was changed after its receipt')
            atomic_json(path,result)
            state.update(reserved_execution=str(cost),stage1d_terminal_cost_reconciliation={'path':path.relative_to(self.w).as_posix(),'sha256':sha(path),'identity':identity})
            return result
        # Preserve existing accounting and native observation history first.
        super().observe_execution_charge(state,body,receipt)
        with self.db.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            current=self._rows(db,job);component=current['component'];amount=current['amount']
            if component[1]!='R2_NEW':raise ValueError('Only this authorized new job component may change')
            if component[4:6]!=before['component'][4:6] or amount[3]!=before['amount'][3]:raise ValueError('Concurrent export/actual change requires a fresh audit')
            expected=cost+Decimal(component[4])
            if amount[3] is not None and Decimal(amount[3])!=cost+Decimal(component[5] or '0'):raise ValueError('Already settled actual charge conflicts with reliable terminal fee; actual is retained')
            restored_status=before['component'][6] if before['component'][6] in ('BOUNDED_ACCOUNTING_NOT_FINAL','FINAL_ACTUAL') else component[6]
            db.execute('UPDATE r2_components SET execution_risk=?,status=? WHERE job=?',(str(cost),restored_status,job))
            db.execute("UPDATE amounts SET reserved=? WHERE job=? AND unit='dune_credits' AND actual IS NULL",(str(expected),job))
            after=self._rows(db,job);after_snapshot=self.db.snapshot(db)
            protected_after=self._protected_history(db,job,protected['observation_max'])
            if protected_after!=protected:raise ValueError('Protected historical fee rows or observation history changed')
            if before['component'][3] is not None and Decimal(after['component'][3])<Decimal(before['component'][3]):raise ValueError('Peak execution observation must be retained')
            result={'schema_version':'stage1d-terminal-execution-cost-reconciliation-v1','identity':identity,'job':job,'evidence':evidence,
                    'before':before,'after_inherited_observation':current,'after':after,'before_snapshot':before_snapshot,'after_snapshot':after_snapshot,
                    'protected_historical_rows_before':protected,'protected_historical_rows_after':protected_after,
                    'released_execution_risk_credits':str(Decimal(current['component'][2])-cost),
                    'net_cumulative_risk_reduction_credits':str(Decimal(before_snapshot['dune_credits']['cumulative_risk'])-Decimal(after_snapshot['dune_credits']['cumulative_risk'])),
                    'historical_peak_and_observations_retained':True,'export_components_and_actual_unchanged':True,'inherited_jobs_unchanged':True,
                    'basis':'Latest SHA-bound HTTP200 terminal actual execution charge replaces only this authorized Stage1D execution component. Provisional peak remains observation history.',
                    'network_requests':0,'utc':now()}
            payload=json.dumps(result,sort_keys=True)
            db.execute('INSERT INTO stage1d_cost_reconciliations VALUES(?,?,?,?)',(identity,job,payload,now()))
            db.execute('CREATE TABLE IF NOT EXISTS stage1d_journal(n INTEGER PRIMARY KEY,kind TEXT,payload TEXT,utc TEXT)')
            db.execute('INSERT INTO stage1d_journal(kind,payload,utc) VALUES(?,?,?)',('RELIABLE_TERMINAL_EXECUTION_COMPONENT_RECONCILED',payload,now()))
            self.db._record(db,job,'STAGE1D_TERMINAL_EXECUTION_RECONCILED',result)
        atomic_json(path,result)
        state.update(reserved_execution=str(cost),stage1d_terminal_cost_reconciliation={'path':path.relative_to(self.w).as_posix(),'sha256':sha(path),'identity':identity})
        return result

def reconcile_existing(work,folder):
    """Explicit caller-only local reconciliation; no API, retry or new grant."""
    work=Path(work).resolve();folder=inside(work,folder)
    state=read(folder/'job.json')
    if state.get('state') not in TERMINAL:raise ValueError('Only terminal existing jobs can be reconciled')
    receipt=state.get('status_receipt') or state.get('submit_receipt')
    body=state.get('status_response') or state.get('submit_response')
    if not receipt or not body:raise ValueError('Existing terminal response/receipt is missing')
    live=Stage1DDune(work)
    result=live.observe_execution_charge(state,body,receipt)
    atomic_json(folder/'job.json',state)
    return result
