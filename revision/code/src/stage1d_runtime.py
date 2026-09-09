"""Stage1D policy adapter over the accepted durable R4 transports.

One continued financial ledger and four persistent per-query 3-hour clocks. Imports do
not access providers. Old stage entrypoints keep their old gates and defaults.
"""
from contextlib import contextmanager, closing
from decimal import Decimal
import hashlib, json, os, re, shutil, sqlite3, time, uuid
from pathlib import Path
from stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha
from budget_r1 import consistent_backup
from budget_r2 import RevisionLedger
from context_access_r3 import read, sha, now, canonical, readonly_snapshot
from context_access_r4 import RpcAccess
from dune_r4 import ContextDuneR4, TERMINAL
from page_attempts import atomic_json, RequestBlocked

AUTH = 'STAGE1D_BATCH01_REFERENCE_FULL_V1'
LIMIT_SECONDS = 10800
RAW_LIMIT = 536870912

def migrate(work, source):
    work, source = Path(work).resolve(), Path(source).resolve()
    receipt = work/'private/STAGE1D_LEDGER_MIGRATION.json'
    if receipt.exists():
        old = read(receipt)
        if old['source_workspace'] != str(source): raise ValueError('Different migration source')
        return old
    state = read(source/'RUN_STATE.json')
    if state.get('network_writer') != 'STOPPED' or state.get('new_live_data_requests_allowed') is not False:
        raise RuntimeError('Inherited writer must be stopped')
    if (source/'private/network_worker.lock').exists(): raise RuntimeError('Inherited worker lock')
    source_db = source/'private/shared_budget_r4.sqlite'
    baseline_db = work/'private/ledger/shared_budget_r4.sqlite'
    if sha(source_db) != sha(baseline_db): raise RuntimeError('Live ledger differs from accepted baseline: reconcile first')
    target = work/'private/shared_budget_r4.sqlite'
    if target.exists(): raise RuntimeError('Interrupted migration: reconcile, never reset')
    before = readonly_snapshot(source_db)
    files = {}
    for name in ('shared_budget_r4.sqlite','dune_request_attempts.sqlite','read_retry_r4.sqlite'):
        files[name] = consistent_backup(source/'private'/name,work/'private'/name)
    for name in ('dune_user_confirmation.json','dune_rate_evidence.json','current_dune_usage.json','alchemy_permission_r3.json'):
        shutil.copyfile(source/'private'/name,work/'private'/name)
    after = readonly_snapshot(target)
    if before != after: raise RuntimeError('Budget migration changed amounts')
    with closing(sqlite3.connect(source_db.resolve().as_uri()+'?mode=ro',uri=True)) as a, closing(sqlite3.connect(target)) as b:
        equality={}
        for (table,) in a.execute("SELECT name FROM sqlite_master WHERE type='table'"):
            if not re.fullmatch('[A-Za-z_0-9]+',table): raise ValueError('Unsafe table name')
            old=a.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall()
            new=b.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall()
            if old!=new: raise RuntimeError('Historical rows changed')
            equality[table]={'rows':len(old),'identical':True}
        b.execute('CREATE TABLE IF NOT EXISTS stage1d_journal(n INTEGER PRIMARY KEY,kind TEXT,payload TEXT,utc TEXT)')
        b.execute('INSERT INTO stage1d_journal(kind,payload,utc) VALUES(?,?,?)',('CONTINUATION_NOT_NEW_GRANT',json.dumps(before),now()))
        b.commit()
    inherited_raw = {p.relative_to(work).as_posix():{'bytes':p.stat().st_size,'sha256':sha(p)} for p in (work/'raw').rglob('*') if p.is_file()}
    result={'authorization_id':AUTH,'source_workspace':str(source),'source_state_sha256':sha(source/'RUN_STATE.json'),
        'source_files':files,'historical_tables':equality,'initial_snapshot':after,'inherited_raw_files':inherited_raw,
        'online_seconds_cap':LIMIT_SECONDS,'clock_scope':'PER_QUERY_ALL_VARIANTS_USER_CLARIFICATION',
        'new_dune_allowance':False,'created_at_utc':now()}
    atomic_json(receipt,result)
    return result

class Runtime:
    allowed_kinds=('candidate','context','frontier_labels')
    warning_threshold=Decimal('400')
    def ledger_factory(self, path):
        from stage1d_recovery_policy import RecoveryLedger, is_adopted
        if is_adopted(path): return RecoveryLedger(path)
        from stage1d_budget import Stage1DLedger
        from stage1d_resource_alignment import Stage1DResourceLedger, is_aligned
        return Stage1DResourceLedger(path) if is_aligned(path) else Stage1DLedger(path)
    def warn_budget(self, work, snapshot):
        threshold=Decimal(snapshot['dune_credits'].get('warning_threshold',str(self.warning_threshold)))
        if Decimal(snapshot['dune_credits']['cumulative_risk']) < threshold:return
        path=Path(work)/('private/stage1d_authority/WARNING_'+str(threshold)+'.json')
        if not path.exists():atomic_json(path,{'warning_only':True,'automatic_stop':False,'utc':now(),'snapshot':snapshot})
    def __init__(self, expected_headers=None):
        from stage1d_rpc import Stage1DRpcValidation
        self.rpc_validation=Stage1DRpcValidation(expected_headers or {})
    def validate_rpc(self, plan):
        return self.rpc_validation.validate_rpc(plan)
    def rpc_identity(self, provider, plan):
        return self.rpc_validation.rpc_identity(provider,plan)
    def rpc_result_status(self, request, response):
        return self.rpc_validation.rpc_result_status(request,response)
    def require_gate(self, work):
        work=Path(work); gate=read(work/'STAGE1D_PREFLIGHT_GATE.json')
        if gate.get('status')!='PASS' or gate.get('authorization_id')!=AUTH:
            raise RuntimeError('Stage1D preflight gate required')
        for name,expected in gate['source_sha256'].items():
            path=(work/'src'/name).resolve()
            if not path.is_relative_to((work/'src').resolve()) or sha(path)!=expected:
                raise RuntimeError('Gate bound source changed: '+name)
        if not gate.get('old_and_new_tests_passed'): raise RuntimeError('Tests required before collection')
        if (work/'private/stage1d_authority/CLOSURE_SCOPE_ADOPTION.json').exists():
            active=active_batch_path(work)
            if gate.get('closure_authorization_id')!='STAGE1D_WINDOW_ROLE_SEMANTIC_CLOSURE_V1' or gate.get('closure_batch_sha256')!=sha(active) or gate.get('window_semantic_controlled_gate_passed') is not True:
                raise RuntimeError('Current window/role/semantic end-to-end gate required')
    def resource_cap(self, work, key, legacy):
        from stage1d_recovery_policy import resource_cap
        return resource_cap(work, key, legacy)
    def retry_store(self, path, **kwargs):
        from stage1d_recovery_policy import effective
        from read_retry_r4 import ReadRetryStore
        if effective(Path(path).resolve().parent.parent):
            from stage1d_recovery_retry import RecoveryReadRetryStore
            return RecoveryReadRetryStore(path, **kwargs)
        return ReadRetryStore(path, **kwargs)
    def raw_limit(self, work):
        return self.resource_cap(work, 'new_raw_logical_unique_bytes_hard', RAW_LIMIT)
    def reserve_raw(self, work, request_id, bound):
        from stage1d_recovery_policy import reserve_raw
        return reserve_raw(work, request_id, bound)
    def close_raw(self, work, request_id, receipt):
        from stage1d_recovery_policy import close_raw
        return close_raw(work, request_id, receipt)
    def raw_risk(self, work):
        from stage1d_recovery_policy import effective, storage_summary
        if effective(work): return storage_summary(work)['storage_risk_bytes']
        work=Path(work); migration=read(work/'private/STAGE1D_LEDGER_MIGRATION.json')
        inherited=migration['inherited_raw_files']
        total=sum(p.stat().st_size for p in (work/'raw').rglob('*') if p.is_file() and p.relative_to(work).as_posix() not in inherited)
        total+=sum(int(read(p)['additional_raw_risk_bytes']) for p in (work/'private/context_uncertainty').glob('*.json'))
        return total
    def clock_used(self, work, query=None):
        total=Decimal(0)
        for p in (Path(work)/'private/stage1d_sessions').glob('*.json'):
            r=read(p)
            if query is not None and r['query'] not in (query,'SHARED'):continue
            if not r['closed']: raise RuntimeError('Unresolved online clock requires measured recovery')
            total+=Decimal(str(r['elapsed_seconds']))
        return total
    @contextmanager
    def session(self,work,probe,label,*,clock=time.time,monotonic=time.monotonic,synthetic=False):
        work=Path(work).resolve()
        if not synthetic:self.require_gate(work)
        scopes=active_batch(work)['queries']
        if probe!='SHARED' and probe not in {q['name'] for q in scopes}: raise ValueError('Unauthorized query')
        names=[q['name'] for q in scopes] if probe=='SHARED' else [probe]
        limit=self.resource_cap(work,'per_query_online_hard_seconds_all_modes',LIMIT_SECONDS)
        remaining=min(Decimal(limit)-self.clock_used(work,q) for q in names)
        if remaining<=0:raise RuntimeError('Per-query cumulative Stage1D online time exhausted')
        lock=work/'private/network_worker.lock'
        with lock.open('x',encoding='utf8') as f:json.dump({'pid':os.getpid(),'authorization_id':AUTH,'utc':now(),'query':probe},f)
        path=work/'private/stage1d_sessions'/(uuid.uuid4().hex+'.json');started=monotonic()
        record={'query':probe,'label':label,'closed':False,'started_at_utc':now(),'remaining_before_seconds':str(remaining),'synthetic':synthetic}
        atomic_json(path,record)
        try: yield {'deadline':clock()+float(remaining),'remaining':remaining,'record':record}
        finally:
            record.update(closed=True,elapsed_seconds=round(monotonic()-started,6),ended_at_utc=now())
            atomic_json(path,record);lock.unlink()
    def verify_sql_freeze(self, freeze_path, work_root, *, sql_path=None):
        root=Path(work_root).resolve();path=Path(freeze_path).resolve();sql_path=Path(sql_path).resolve()
        if not path.is_relative_to(root) or not sql_path.is_relative_to(root):raise ValueError('SQL freeze path escapes stage')
        frozen=read(path)
        if frozen.get('schema_version')!='stage1d-sql-freeze-v1' or frozen.get('authorization_id')!=AUTH:
            raise ValueError('Stage1D scoped SQL freeze required')
        if sha(sql_path)!=frozen.get('sql_sha256'):raise ValueError('Frozen SQL changed')
        if not frozen.get('scope_id') or not frozen.get('scope_hash'):raise ValueError('SQL freeze must bind an exact historical or current scope')
        batch=batch_for_scope(root, frozen['scope_id'], frozen['scope_hash'])
        byid={q['query_id']:q for q in batch['queries']}
        for qid in frozen['query_ids']:
            if qid not in byid:raise ValueError('Query outside authorized four')
        sql=sql_path.read_text(encoding='utf8')
        if not sql.startswith('--') or re.search(r'\b(INSERT|DELETE|DROP|ALTER|CREATE|UPDATE|MERGE)\s+(INTO|FROM|TABLE|VIEW|SCHEMA)',sql,re.I):raise ValueError('Read only SQL required')
        for dependency in frozen['dependencies']:
            p=(root/dependency['path']).resolve()
            if not p.is_relative_to(root) or sha(p)!=dependency['sha256']:raise ValueError('SQL input dependency mismatch')
        if not frozen.get('export_plan',{}).get('all_pages_required'):raise ValueError('Full pagination required')
        if frozen['kind'] in ('candidate','context'):
            if 'block_date BETWEEN DATE' not in sql or frozen['kind']=='candidate' and 'block_time BETWEEN TIMESTAMP' not in sql:
                raise ValueError('Explicit date and timestamp coverage required')
            for interval in frozen['intervals']:
                q=byid[interval['query_id']]
                if interval['kind']=='candidate' and not q['start_block']<=interval['start_block']<=interval['end_block']<=q['end_block']:
                    raise ValueError('Candidate blocks outside scope')
                if interval['end_block']<interval['start_block']:raise ValueError('Inverted interval')
        elif frozen['kind']=='frontier_labels':
            if not 1<=len(frozen['addresses'])<=100:raise ValueError('Finite actual-frontier labels')
            from labels_policy import full_address
            addresses=[full_address(a) for a in frozen['addresses']]
            if len(addresses)!=len(set(addresses)):raise ValueError('Duplicate frozen actual-frontier labels')
        else:raise ValueError('Unknown SQL purpose')
        return frozen

def rpc(work, plans, query, label):
    return RpcAccess(work,runtime=Runtime()).call_batch(plans,query,label)

def execute_sql(work,freeze_path,query,label):
    from stage1d_export_reconcile import Stage1DPageDune as Stage1DDune
    work=Path(work).resolve();freeze_path=Path(freeze_path).resolve();runtime=Runtime()
    frozen=runtime.verify_sql_freeze(freeze_path,work,sql_path=freeze_path.parent/'query.sql')
    existing=work/'private/dune_r2_jobs'/frozen['sql_sha256']
    if (existing/'job.json').exists():
        state=read(existing/'job.json')
        if state.get('state')=='QUERY_STATE_COMPLETED' and state.get('request_set_closed'):
            cached=Stage1DDune(work,runtime=runtime)
            if cached.export_progress(existing,state)['complete']:
                settlement=None
                if (existing/'stage1d_export_plan.json').exists():
                    from stage1d_export_reconcile import verify_plan
                    verify_plan(work,existing)
                    with cached.db.connection() as db:
                        table=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='stage1d_export_reconciliations'").fetchone()
                        reconciled=bool(table and db.execute('SELECT 1 FROM stage1d_export_reconciliations WHERE job=?',(state['logical_job_id'],)).fetchone())
                    if not reconciled:
                        # A crash after closing pages must not strand their old
                        # planning envelope. This is offline single-writer work.
                        lock=work/'private/network_worker.lock'
                        with lock.open('x',encoding='utf8') as handle:
                            json.dump({'pid':os.getpid(),'authorization_id':AUTH,'utc':now(),'kind':'OFFLINE_CACHED_EXPORT_SETTLEMENT'},handle)
                        try:settlement=cached.settle(existing)
                        finally:lock.unlink()
                return {'status':'COMPLETED_EXPORTED','job_folder':str(existing),'cache_reused':True,'new_requests':0,'online_clock_increment':0,'settlement_recovery':settlement}
    with runtime.session(work,query,label) as timing:
        live=Stage1DDune(work,deadline=timing['deadline'],runtime=runtime)
        folder=work/'private/dune_r2_jobs'/frozen['sql_sha256']
        if (folder/'job.json').exists():
            state=read(folder/'job.json')
            if not state.get('execution_id'):raise RequestBlocked('Unknown SQL submission: preserve risk and recover execution identity')
        else:
            submitted=live.submit(freeze_path.parent/'query.sql',label,kind=frozen['kind'],freeze_manifest=freeze_path,performance='medium')
            folder=Path(submitted['job_folder'])
            if not submitted.get('execution_id'):return submitted
        timing['record']['job_folder']=folder.relative_to(work).as_posix()
        while read(folder/'job.json')['state'] not in TERMINAL:
            r=live.poll(folder)
            if r.get('retry_status'):return {**r,'job_folder':str(folder)}
            if read(folder/'job.json')['state'] not in TERMINAL:
                if time.time()+35>timing['deadline']:return {'status':'DEFERRED_CLOCK','job_folder':str(folder)}
                time.sleep(5)
        state=read(folder/'job.json')
        if state['state']!='QUERY_STATE_COMPLETED':return {'status':state['state'],'job_folder':str(folder),'settlement':live.settle(folder)}
        progress=live.export_progress(folder,state)
        while not progress['complete']:
            r=live.export(folder,offset=progress['next_offset'])
            if r.get('retry'):return {**r,'job_folder':str(folder)}
            progress=r['progress']
        return {'status':'COMPLETED_EXPORTED','job_folder':str(folder),'settlement':live.settle(folder)}

def result_rows(work, folder):
    work=Path(work);folder=Path(folder);live=ContextDuneR4(work,runtime=Runtime())
    state=read(folder/'job.json');progress=live.export_progress(folder,state)
    if not progress['complete']:raise ValueError('Incomplete export is not complete ledger')
    rows=[]
    for off,entry in sorted(state.get('r4_verified_pages',{}).items(),key=lambda x:int(x[0])):
        path=work/entry['page_path']
        if sha(path)!=entry['page_sha256']:raise ValueError('Page identity changed')
        rows.extend(read(path)['result']['rows'])
    return rows
