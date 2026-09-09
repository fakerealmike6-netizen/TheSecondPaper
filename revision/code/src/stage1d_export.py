"""Exact native-scope export of an already completed Stage1D Dune execution.

The provider's original all-asset metadata is retained. Only native top/internal
rows are downloaded, with two SQL-proven NULL columns reconstructed. This does
not certify token context, alter SQL, or reset the shared financial/read ledgers.
No request, ledger write, or import-time work occurs in the pure proof functions.
"""
from decimal import Decimal, ROUND_CEILING
import hashlib, json, os, re, urllib.error, urllib.parse, urllib.request, uuid
from pathlib import Path
from collector import NATIVE, Scope
from context_access_r3 import DUNE_MAX, new_json, now, read, sha
from dune_r4 import _portable
from network import NoRedirect
from page_attempts import atomic_json, RequestBlocked
from page_contract import PageContractError, exact_count, initial_progress, schema_of, validate_next_uri
from read_retry_r4 import classify_failure, logical_key
from stage1d_costs import Stage1DDune, AUTH, inside, canonical

FILTER = "event_kind IN ('top','internal')"
FULL_COLUMNS = ['event_kind','tx_hash','sender','recipient','amount_raw','block_number','tx_index',
                'block_time','log_index','trace_address','success','contract_address','gas_used',
                'gas_price','trace_type','call_type']
COLUMNS = [c for c in FULL_COLUMNS if c not in ('log_index','contract_address')]
DOCS = ['https://docs.dune.com/api-reference/executions/endpoint/get-execution-result',
        'https://docs.dune.com/api-reference/executions/filtering',
        'https://docs.dune.com/api-reference/executions/pagination']

def bound_receipt(work, body, receipt):
    work=Path(work).resolve()
    raw=inside(work,receipt['raw_path']);data=raw.read_bytes()
    if sha(raw)!=receipt.get('sha256') or len(data)!=receipt.get('raw_bytes'):
        raise ValueError('Original response byte identity differs')
    if canonical(json.loads(data,parse_float=Decimal))!=canonical(body):
        raise ValueError('Response body differs from original raw evidence')
    if canonical(read(inside(work,'logs/'+receipt['request_id']+'.json')))!=canonical(receipt):
        raise ValueError('Original persisted receipt differs')

def projection_proof(work, folder):
    """Read-only proof; fails closed for non-native, changed SQL or unbound status."""
    from stage1d_runtime import Runtime
    from stage1d_acquisition import interval_sql
    work=Path(work).resolve();folder=inside(work,folder);state=read(folder/'job.json')
    if state.get('state')!='QUERY_STATE_COMPLETED' or not re.fullmatch('[A-Z0-9]{26}',state.get('execution_id','')):
        raise ValueError('An existing completed execution is required')
    freeze=inside(work,state['scope_freeze_path'])
    if sha(freeze)!=state.get('scope_freeze_sha256'):raise ValueError('SQL freeze changed')
    f=Runtime().verify_sql_freeze(freeze,work,sql_path=folder/'query.sql')
    if f['kind']!='candidate' or not f.get('intervals') or len(f['query_ids'])!=1:
        raise ValueError('Only exact native candidate interval SQL is eligible')
    q=next(q for q in read(work/'private/BATCH_QUERY_FREEZE.json')['queries'] if q['query_id']==f['query_ids'][0])
    scope=Scope.from_policy(q)
    if q.get('seed_asset')!=NATIVE or q.get('seed_event',{}).get('asset')!=NATIVE:
        raise ValueError('Native seed asset required')
    if scope.scope_hash!=q.get('scope_hash') or scope.scope_hash!=f.get('scope_hash') or scope.scope_id!=f.get('scope_id'):
        raise ValueError('Actual frozen scope identity differs')
    # No instance-specific conversion is implemented by this collector/context
    # path; refuse any added declaration instead of importing old WETH evidence.
    if any(q.get(k) for k in ('certified_conversions','protocol_conversions','conversion_edges','weth_certification')):
        raise ValueError('Cross-asset conversion requires a different complete export')
    intervals=f['intervals'];first=intervals[0]
    keys=('start_block','end_block','start_time','end_time')
    if any(i.get('asset')!=NATIVE or i.get('query_id')!=q['query_id'] or i.get('kind')!='candidate'
           or any(i[k]!=first[k] for k in keys) for i in intervals):
        raise ValueError('Single exact native rectangle required')
    if sorted(set(f['addresses']))!=sorted({i['address'] for i in intervals}):raise ValueError('Frozen interval addresses differ')
    sql=interval_sql(f['addresses'],*(first[k] for k in keys))
    if hashlib.sha256(sql.encode()).hexdigest()!=state['sql_sha256'] or sha(folder/'query.sql')!=state['sql_sha256']:
        raise ValueError('SQL must exactly regenerate from the native adapter, not a textual NULL guess')
    body=state['status_response'];receipt=state['status_receipt'];bound_receipt(work,body,receipt)
    if receipt.get('operation')!='status' or receipt.get('http_status')!=200 or receipt.get('error_class') or body.get('execution_id')!=state['execution_id'] or body.get('state')!='QUERY_STATE_COMPLETED':
        raise ValueError('SHA-bound HTTP200 completed status required')
    md=body['result_metadata'];sch=schema_of(md)
    if sch.get('column_names')!=FULL_COLUMNS or len(sch.get('column_types',[]))!=16:raise ValueError('Original 16-column schema differs')
    total=exact_count(md.get('total_row_count'),'total_row_count');exact_count(md.get('total_result_set_bytes'),'total_result_set_bytes')
    if total>25000:raise ValueError('Finite per-query candidate cap exceeded')
    types=[sch['column_types'][FULL_COLUMNS.index(c)] for c in COLUMNS]
    source={name:sha(Path(__file__).parent/name) for name in ('collector.py','provider_dune.py','stage1d_acquisition.py','stage1d_context.py','stage1d_export.py')}
    return {'schema_version':'stage1d-native-export-projection-v1','authorization_id':AUTH,
            'execution_id':state['execution_id'],'logical_job_id':state['logical_job_id'],
            'scope_id':scope.scope_id,'scope_hash':scope.scope_hash,'query_id':q['query_id'],
            'sql_sha256':state['sql_sha256'],'sql_freeze_sha256':sha(freeze),
            'original_status_receipt_sha256':receipt['sha256'],'original_all_asset_metadata':md,
            'asset':NATIVE,'filters':FILTER,'columns':COLUMNS,'column_types':types,'limit':max(total,1),
            'reconstructed_constants':{'log_index':None,'contract_address':None},'source_sha256':source,
            'all_asset_export_complete':False,'native_rows_unfiltered_by_amount_success_time_or_address':True,
            'proof':'Exact regenerated SQL: top/internal both cast log_index and contract_address NULL; native Collector propagation and native LP context exclude ERC20. Labels precede fetch. Native zero/failed top rows retain gas. No certified cross-asset continuation.',
            'excluded_context':'Optional ERC20 rows and their all-asset physical-conflict checks are not certified by this native-only export.',
            'official_documentation':DOCS}

def export_bound(proof, rate, observations=()):
    md=proof['original_all_asset_metadata'];n=exact_count(md['total_row_count'],'total_row_count')
    size=exact_count(md['total_result_set_bytes'],'total_result_set_bytes')
    points=n*len(COLUMNS)
    for observed in observations:
        for k in ('result_set_bytes','total_result_set_bytes'):
            if k in observed:size=max(size,exact_count(observed[k],k))
        if 'datapoint_count' in observed:points=max(points,exact_count(observed['datapoint_count'],'datapoint_count'))
    points=max(Decimal(points),(Decimal(size)/100).to_integral_value(rounding=ROUND_CEILING))
    per=max(Decimal(1),max(Decimal(size)*20/1000000,points/1000).to_integral_value(rounding=ROUND_CEILING))
    return per,{'rate_evidence':rate,'result_metadata':md,'projection_scope_hash':proof['scope_hash'],
                'columns':COLUMNS,'filters':FILTER,'per_request_upper_credits':str(per),
                'result_metadata_observations':list(observations),'is_actual':False,
                'basis':'Conservative original whole-result byte bound and projected rows*14/1000 datapoint bound; each dispatched page/retry reserves this entire upper independently.'}

def parameters(proof,offset):return {'limit':proof['limit'],'offset':offset,'columns':','.join(COLUMNS),'filters':FILTER}

def validate_native_page(body,receipt,proof,progress):
    offset=progress['next_offset'];limit=proof['limit'];params=parameters(proof,offset)
    if progress.get('complete') or type(offset)is not int or offset<0 or not 1<=limit<=25000:
        raise PageContractError('Exact next finite page required')
    if receipt.get('http_status')!=200 or receipt.get('error_class') or receipt.get('execution_id')!=proof['execution_id'] or receipt.get('parameters')!=params:
        raise PageContractError('HTTP200 exact projected request receipt required')
    if not isinstance(body,dict) or body.get('execution_id')!=proof['execution_id'] or body.get('state')!='QUERY_STATE_COMPLETED' or body.get('error'):
        raise PageContractError('Completed same-execution page required')
    result=body.get('result',{});rows=result.get('rows');md=result.get('metadata')
    if not isinstance(rows,list) or not isinstance(md,dict) or not all(isinstance(r,dict) for r in rows) or len(rows)>limit:
        raise PageContractError('Finite rows and metadata required')
    total=exact_count(md.get('total_row_count'),'total_row_count')
    if total>proof['original_all_asset_metadata']['total_row_count'] or progress.get('total') not in (None,total) or exact_count(md.get('row_count'),'row_count')!=len(rows):
        raise PageContractError('Projected row count is inconsistent')
    expected={'column_names':COLUMNS,'column_types':proof['column_types']}
    if schema_of(md)!=expected:raise PageContractError('Complete exact projected column schema required')
    for r in rows:
        if set(r)!=set(COLUMNS) or r.get('event_kind') not in ('top','internal'):
            raise PageContractError('Projection returned a non-native row or changed columns')
    end=offset+len(rows);nxt=body.get('next_offset');terminal=end==total
    if end>total:raise PageContractError('Page exceeds projected total')
    if terminal:
        if nxt is not None or body.get('next_uri') is not None:raise PageContractError('Terminal page advertises continuation')
    elif not rows or type(nxt)is not int or nxt!=end or nxt<=offset:
        raise PageContractError('Missing, repeating or noncontiguous continuation')
    validate_next_uri(body.get('next_uri'),proof['execution_id'],nxt,limit,params)
    return {'next_offset':None if terminal else nxt,'observed_rows':end,'total':total,'complete':terminal,
            'successful_pages':progress.get('successful_pages',0)+1,'page_size':limit,'schema':expected}

def verified_native_pages(work,folder):
    """Pure offline replay, re-proving SQL/scope and every SHA-bound raw page."""
    work=Path(work).resolve();folder=inside(work,folder);proof=projection_proof(work,folder)
    root=folder/'stage1d_native_export';saved=read(root/'decision.json')
    if saved['proof']!=proof:raise ValueError('Immutable native projection proof changed')
    progress=initial_progress();pages=[]
    for record in read(root/'progress.json').get('pages',[]):
        if record['offset']!=progress['next_offset']:raise ValueError('Saved native page sequence differs')
        pp=inside(work,record['page_path']);rp=inside(work,record['receipt_path'])
        if sha(pp)!=record['page_sha256'] or sha(rp)!=record['receipt_sha256']:raise ValueError('Saved native page changed')
        body,receipt=read(pp),read(rp);bound_receipt(work,body,receipt)
        progress=validate_native_page(body,receipt,proof,progress);pages.append((body,receipt,record['offset']))
    saved_progress=read(root/'progress.json')['progress']
    if saved_progress!=progress:raise ValueError('Saved native completeness differs from raw page chain')
    return proof,progress,pages

def native_result_rows(work,folder):
    proof,progress,pages=verified_native_pages(work,folder)
    if not progress['complete']:raise ValueError('Partial native export cannot certify native ledger coverage')
    return [dict(r,**proof['reconstructed_constants']) for body,_,_ in pages for r in body['result']['rows']]

class Stage1DNativeExport(Stage1DDune):
    """Explicit opt-in native GET bridge; inherited export/SQL methods unchanged."""
    def _guard_prior(self,state):
        if state.get('export_requests') or state.get('export_offsets') or state.get('r4_verified_pages'):
            raise RequestBlocked('Previously dispatched full-column exports require separate attempt reconciliation')
        with self.reads._db() as db:
            for row in db.execute('SELECT r.identity_json,a.dispatched_at FROM read_requests r JOIN read_attempts a USING(logical_key) WHERE a.dispatched_at IS NOT NULL'):
                identity=json.loads(row[0])
                if identity.get('execution_id')==state['execution_id'] and identity.get('method')=='GET_RESULTS' and identity.get('parameters',{}).get('filters')!=FILTER:
                    raise RequestBlocked('Cannot reset dispatched all-asset attempt budget through projection')
        for row in self.attempts.rows():
            i=json.loads(row['identity'])
            if i.get('execution_id')==state['execution_id'] and i.get('operation')=='results':
                raise RequestBlocked('Historical page attempt must be reconciled before projection')

    def prepare(self,folder):
        folder=self._inside(folder);state=read(folder/'job.json');proof=projection_proof(self.w,folder)
        root=folder/'stage1d_native_export';decision=root/'decision.json'
        if decision.exists():
            if read(decision)['proof']!=proof:raise RequestBlocked('Native projection decision is immutable')
        else:
            self._guard_prior(state)
            self._verified_terminal(state,state['status_response'],state['status_receipt'])
            per,basis=export_bound(proof,self.rate_evidence())
            atomic_json(decision,{'proof':proof,'previous_full_column_selection':state.get('r4_page_selection'),
                'previous_full_column_limit':state.get('r4_page_limit'),'previous_export_envelope':state.get('r4_export_envelope'),
                'previous_export_retry_status':state.get('r4_export_retry_status'),
                'per_request_upper_credits':str(per),'initial_metering_basis':basis,'utc':now(),
                'reason':'No all-column export fits the shared remainder; native asset projection retains every required native ledger, amount and gas row. Original all-asset execution remains intact.'})
        if not (root/'progress.json').exists():atomic_json(root/'progress.json',{'pages':[],'progress':initial_progress()})
        return verified_native_pages(self.w,folder)

    def _reserve_native(self,state,claim,per,basis):
        job=state['logical_job_id']
        with self.db.connection() as db:
            db.execute('CREATE TABLE IF NOT EXISTS stage1d_native_export_attempts(attempt_id TEXT PRIMARY KEY,job TEXT,per TEXT,upper_after TEXT,status TEXT)')
            old=db.execute('SELECT per,upper_after,status FROM stage1d_native_export_attempts WHERE attempt_id=?',(claim['attempt_id'],)).fetchone()
            current=Decimal(db.execute('SELECT export_risk FROM r2_components WHERE job=?',(job,)).fetchone()[0])
            upper=Decimal(old[1]) if old else current+per
            if not old:db.execute('INSERT INTO stage1d_native_export_attempts VALUES(?,?,?,?,?)',(claim['attempt_id'],job,str(per),str(upper),'INTENT'))
        self.db.reserve_export(job,upper,{**basis,'attempt_id':claim['attempt_id'],'old_export_risk_released':False})
        with self.db.connection() as db:db.execute('UPDATE stage1d_native_export_attempts SET status=? WHERE attempt_id=?',('RESERVED',claim['attempt_id']))
        return upper

    def _observe_native_bound(self,state,per,basis):
        with self.db.connection() as db:
            entries=db.execute('SELECT attempt_id,per,upper_after,status FROM stage1d_native_export_attempts WHERE job=?',(state['logical_job_id'],)).fetchall()
            current=Decimal(db.execute('SELECT export_risk FROM r2_components WHERE job=?',(state['logical_job_id'],)).fetchone()[0])
        # Unknown/interrupted reserved attempts remain charged at a full upper.
        active=[r for r in entries if r[3] in ('RESERVED','DISPATCHED') or r[3]=='INTENT' and Decimal(r[2])<=current]
        delta=sum((max(Decimal(0),per-Decimal(r[1])) for r in active),Decimal(0))
        if delta:
            self.db.reserve_export(state['logical_job_id'],current+delta,{**basis,'all_prior_attempt_envelopes_increased':True},observed=True)
            with self.db.connection() as db:
                for r in active:db.execute('UPDATE stage1d_native_export_attempts SET per=? WHERE attempt_id=?',(str(max(per,Decimal(r[1]))),r[0]))

    def _call_native(self,execution,params):
        if not getattr(self,'_active_native_dispatch',False):raise RuntimeError('Use the reserved native export entry point')
        self._preflight()
        if self.deadline is not None and self.clock()+30>self.deadline:raise RuntimeError('Insufficient clock for bounded native GET')
        if not re.fullmatch('[A-Z0-9]{26}',execution) or set(params)!={'limit','offset','columns','filters'} or params['columns']!=','.join(COLUMNS) or params['filters']!=FILTER or type(params['limit'])is not int or not 1<=params['limit']<=25000 or type(params['offset'])is not int or params['offset']<0:
            raise ValueError('Only exact finite native projection parameters allowed')
        rid='dune_stage1d_native_'+uuid.uuid4().hex;started=self.clock();status=None;raw=b'';headers={};error=None
        rid=self._prepare_raw(rid) or rid
        try:
            if self.transport is not None:status,raw,headers=self.transport('results',execution,None,params,DUNE_MAX)
            else:
                url='https://api.dune.com/api/v1/execution/'+execution+'/results?'+urllib.parse.urlencode(params)
                request=urllib.request.Request(url,headers={'X-Dune-API-Key':os.environ['DUNE_API_KEY'],'Content-Type':'application/json'})
                try:
                    with urllib.request.build_opener(NoRedirect()).open(request,timeout=30) as response:status,raw,headers=response.status,response.read(DUNE_MAX),dict(response.headers)
                except urllib.error.HTTPError as exc:
                    with exc:status,raw,headers=exc.code,exc.read(DUNE_MAX),dict(exc.headers)
        except Exception as exc:
            error=classify_failure(exc)['error_class']
            if isinstance(getattr(exc,'partial',None),bytes):raw=exc.partial[:DUNE_MAX]
        if not isinstance(raw,bytes):raw,error=b'','NON_BYTES_RESPONSE'
        if len(raw)>=DUNE_MAX:raw,error=raw[:DUNE_MAX],'RAW_RESPONSE_AT_RESERVED_READ_LIMIT'
        if any(v.encode() in raw for k,v in os.environ.items() if any(s in k.upper() for s in ('API_KEY','TOKEN','SECRET')) and len(v)>=12):raw,error=b'','CREDENTIAL_ECHO_WITHHELD'
        path=self.w/'raw/dune'/(rid+'.json');path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
        receipt={'request_id':rid,'operation':'results','execution_id':execution,'parameters':params,'http_status':status,
                 'error_class':error,'raw_path':path.relative_to(self.w).as_posix(),'raw_bytes':len(raw),'sha256':sha(path),
                 'utc':now(),'elapsed_seconds':max(0,self.clock()-started),'retry_after':headers.get('Retry-After',headers.get('retry-after')),
                 'evidence_kind':'REAL_PROVIDER' if self.transport is None else 'SYNTHETIC_TRANSPORT'}
        atomic_json(self.w/'logs'/(rid+'.json'),receipt)
        if error:new_json(self.w/'private/context_uncertainty'/(rid+'.json'),{'additional_raw_risk_bytes':max(0,DUNE_MAX-len(raw)),'basis':error,'receipt':rid})
        try:body=json.loads(raw,parse_float=Decimal) if not error else None
        except (ValueError,UnicodeError):body=None
        if getattr(self,'_pending_raw_id',None):
            self.runtime.close_raw(self.w,rid,self.w/'logs'/(rid+'.json'));self._pending_raw_id=None
        return body,receipt

    def export_native_next(self,folder):
        self.require_gate();self.ensure_not_halted();folder=self._inside(folder);state=read(folder/'job.json')
        proof,progress,pages=self.prepare(folder)
        if progress['complete']:return {'status':'COMPLETED_NATIVE_SCOPE_EXPORTED','progress':progress,'cache_reused':True,'all_asset_export_complete':False}
        self.pending_job(state);params=parameters(proof,progress['next_offset']);identity=self.read_identity('results',state,params)
        identity['columns']=COLUMNS;identity['native_scope_hash']=proof['scope_hash']
        root=folder/'stage1d_native_export'
        while True:
            per,basis=export_bound(proof,self.rate_evidence(),[p['result']['metadata'] for p,_,_ in pages]);claim=self._claim(identity)
            if claim['state']=='CACHE_HIT':body,receipt=claim['payload']['body'],claim['payload']['receipt']
            elif claim['state']!='CLAIMED':return {'status':claim['state'],'retry':claim,'progress':progress,'all_asset_export_complete':False}
            else:
                try:upper=self._reserve_native(state,claim,per,basis)
                except BaseException:
                    self.reads.abandon_before_dispatch(claim['attempt_id'],'BUDGET_OR_EVIDENCE_DEFERRED')
                    self._cancel_raw_before_transport('BUDGET_OR_EVIDENCE_DEFERRED');raise
                self.reads.mark_dispatched(claim['attempt_id'],accounting={'job':state['logical_job_id'],'export_upper_after':str(upper),'old_risk_released':False})
                with self.db.connection() as db:db.execute('UPDATE stage1d_native_export_attempts SET status=? WHERE attempt_id=?',('DISPATCHED',claim['attempt_id']))
                self._active_native_dispatch=True
                try:body,receipt=self._call_native(state['execution_id'],params)
                finally:self._active_native_dispatch=False
                attempt=root/'attempts'/claim['attempt_id'];atomic_json(attempt/'response.json',body);atomic_json(attempt/'receipt.json',receipt)
                if isinstance(body,dict) and isinstance(body.get('result',{}).get('metadata'),dict):
                    # Observed larger metering survives even a rejected row schema.
                    actual_per,actual_basis=export_bound(proof,self.rate_evidence(),[p['result']['metadata'] for p,_,_ in pages]+[body['result']['metadata']])
                    self._observe_native_bound(state,actual_per,actual_basis)
                try:validate_native_page(body,receipt,proof,progress)
                except PageContractError:
                    failure=self._failure(receipt,body);self.reads.finish(claim['attempt_id'],failure['outcome'],error_class=failure['error_class'],retry_after=failure.get('retry_after'),receipt=receipt)
                    continue
                self.reads.finish(claim['attempt_id'],'SUCCESS',payload=_portable({'body':body,'receipt':receipt}),receipt=receipt)
            bound_receipt(self.w,body,receipt);validated=validate_native_page(body,receipt,proof,progress)
            target=root/'verified'/logical_key(identity)
            for name,value in (('page.json',body),('receipt.json',receipt)):
                path=target/name
                if path.exists() and canonical(read(path))!=canonical(value):raise RequestBlocked('Native cached artifact conflict')
                if not path.exists():atomic_json(path,value)
            saved=read(root/'progress.json');saved['pages'].append({'offset':params['offset'],'page_path':(target/'page.json').relative_to(self.w).as_posix(),
                'receipt_path':(target/'receipt.json').relative_to(self.w).as_posix(),'page_sha256':sha(target/'page.json'),
                'receipt_sha256':sha(target/'receipt.json'),'logical_read_key':logical_key(identity)})
            saved['progress']=validated;atomic_json(root/'progress.json',saved)
            result={'status':'COMPLETED_NATIVE_SCOPE_EXPORTED' if validated['complete'] else 'PARTIAL_NATIVE_SCOPE_EXPORT',
                    'progress':validated,'cache_reused':claim['state']=='CACHE_HIT','all_asset_export_complete':False,
                    'export_actual':None,'cumulative':self.db.snapshot()['dune_credits'],'original_job_metadata_unchanged':True}
            atomic_json(root/'EXPORT_RECEIPT.json',result)
            return result

    def reconcile_native_upper(self,folder):
        """Explicit local adjustment for one fully known successful GET only.

        No actual fee is invented. A failed/unknown/page-2 chain is ineligible;
        original export risk from before this request is arithmetically retained.
        The metadata must identify the full projected response, including its
        full byte count, so neither raw HTTP bytes nor account deltas are used.
        """
        folder=self._inside(folder);proof,progress,pages=verified_native_pages(self.w,folder)
        if not progress['complete'] or len(pages)!=1 or pages[0][2]!=0:
            raise ValueError('Only a single complete projected GET can be reconciled')
        body,receipt,_=pages[0];md=body['result']['metadata'];n=len(body['result']['rows'])
        count=exact_count(md.get('datapoint_count'),'datapoint_count')
        byte_count=exact_count(md.get('total_result_set_bytes'),'total_result_set_bytes')
        if md['total_row_count']!=n or md['row_count']!=n or exact_count(md.get('result_set_bytes'),'result_set_bytes')!=byte_count or count<n*len(COLUMNS) or n and byte_count==0:
            raise ValueError('Full projected metadata is ambiguous; retain the original upper')
        points=max(Decimal(count),Decimal(n*len(COLUMNS)),(Decimal(byte_count)/100).to_integral_value(rounding=ROUND_CEILING))
        upper=max(Decimal(1),max(Decimal(byte_count)*20/1000000,points/1000).to_integral_value(rounding=ROUND_CEILING))
        state=read(folder/'job.json');job=state['logical_job_id'];rate=self.rate_evidence()
        evidence={'execution_id':proof['execution_id'],'scope_hash':proof['scope_hash'],'sql_sha256':proof['sql_sha256'],
                  'receipt':receipt,'complete_projected_metadata':md,'original_all_asset_metadata':proof['original_all_asset_metadata'],
                  'rate_evidence':rate,'documented_projection_endpoint':DOCS[0],'is_actual':False}
        identity=hashlib.sha256(canonical(evidence)).hexdigest();path=folder/'stage1d_native_export/EXPORT_UPPER_RECONCILIATION.json'
        with self.reads._db() as db:
            readrows=db.execute('SELECT a.attempt_id,a.outcome,a.receipt_json,r.identity_json FROM read_attempts a JOIN read_requests r USING(logical_key) WHERE a.dispatched_at IS NOT NULL').fetchall()
        relevant=[r for r in readrows if json.loads(r[3]).get('execution_id')==proof['execution_id'] and json.loads(r[3]).get('method')=='GET_RESULTS']
        if len(relevant)!=1 or relevant[0][1]!='SUCCESS' or canonical(json.loads(relevant[0][2]))!=canonical(receipt):
            raise ValueError('Failed, unknown or multiple GET attempts retain their original risk')
        with self.db.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('CREATE TABLE IF NOT EXISTS stage1d_export_upper_reconciliation(identity TEXT PRIMARY KEY,job TEXT,payload TEXT)')
            old=db.execute('SELECT payload FROM stage1d_export_upper_reconciliation WHERE identity=?',(identity,)).fetchone()
            if old:
                result=json.loads(old[0])
                current=db.execute('SELECT export_risk FROM r2_components WHERE job=?',(job,)).fetchone()[0]
                if current!=result['after']['export_risk']:raise ValueError('Reconciled export risk changed')
            else:
                attempts=db.execute('SELECT attempt_id,per,upper_after,status FROM stage1d_native_export_attempts WHERE job=?',(job,)).fetchall()
                if len(attempts)!=1 or attempts[0][0]!=relevant[0][0] or attempts[0][3]!='DISPATCHED':
                    raise ValueError('Additional reservation/unknown intent retains its risk')
                attempt=attempts[0];original=Decimal(attempt[1]);baseline=Decimal(attempt[2])-original
                component=db.execute('SELECT * FROM r2_components WHERE job=?',(job,)).fetchone()
                amount=db.execute("SELECT * FROM amounts WHERE job=? AND unit='dune_credits'",(job,)).fetchone()
                if component[1]!='R2_NEW' or amount[3] is not None or Decimal(component[4])!=baseline+original or baseline<0:
                    raise ValueError('Shared ledger changed or actual settled; preserve all risk')
                if db.execute("SELECT value FROM r2_meta WHERE key='halt'").fetchone():raise ValueError('Existing budget halt cannot be reconciled away')
                retained=baseline+min(original,upper)
                protected=self._protected_history(db,job);before={'component':list(component),'amount':list(amount),'export_risk':component[4]}
                db.execute('UPDATE r2_components SET export_risk=? WHERE job=?',(str(retained),job))
                db.execute("UPDATE amounts SET reserved=? WHERE job=? AND unit='dune_credits'",(str(Decimal(component[2])+retained),job))
                after_component=db.execute('SELECT * FROM r2_components WHERE job=?',(job,)).fetchone()
                after_amount=db.execute("SELECT * FROM amounts WHERE job=? AND unit='dune_credits'",(job,)).fetchone()
                if component[:4]+component[5:]!=after_component[:4]+after_component[5:] or amount[3]!=after_amount[3] or protected!=self._protected_history(db,job,protected['observation_max']):
                    raise ValueError('Execution, actual or historical fee records changed')
                result={'schema_version':'stage1d-projected-export-upper-reconciliation-v1','identity':identity,'job':job,'before':before,
                        'after':{'component':list(after_component),'amount':list(after_amount),'export_risk':str(retained)},
                        'initial_this_request_upper':str(original),'verified_this_request_upper':str(upper),
                        'prior_export_risk_preserved':str(baseline),'released_this_request_upper_difference':str(original-min(original,upper)),
                        'export_actual':None,'execution_component_unchanged':True,'protected_historical_rows':protected,'evidence':evidence,'network_requests':0,'utc':now()}
                payload=json.dumps(result,sort_keys=True)
                db.execute('INSERT INTO stage1d_export_upper_reconciliation VALUES(?,?,?)',(identity,job,payload))
                db.execute('CREATE TABLE IF NOT EXISTS stage1d_journal(n INTEGER PRIMARY KEY,kind TEXT,payload TEXT,utc TEXT)')
                db.execute('INSERT INTO stage1d_journal(kind,payload,utc) VALUES(?,?,?)',('COMPLETE_NATIVE_GET_UPPER_RECONCILED',payload,now()))
                self.db._record(db,job,'STAGE1D_COMPLETE_NATIVE_GET_UPPER_RECONCILED',result)
        atomic_json(path,result)
        return result
