"""Guarded R2 Dune transport. One SQL execution, persistent no-retry page journal.

Uses the existing trial applicability evidence. Server metering fields, never
compressed HTTP bytes or rounded account usage differences, bound all exports.
"""
import argparse,hashlib,json,os,re,time,uuid,urllib.request,urllib.error,urllib.parse
from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal,ROUND_CEILING
from dune_live import Live,read,dump
from budget import valid
from budget_r2 import RevisionLedger,AUTH,migrate_revision
from network import NoRedirect
from page_attempts import AttemptStore,RequestBlocked
from page_contract import exact_count,validate_page,initial_progress,PageContractError
from legacy_guard_r4 import reject_legacy_workspace

TERMINAL={'QUERY_STATE_COMPLETED','QUERY_STATE_FAILED','QUERY_STATE_CANCELLED','QUERY_STATE_EXPIRED'}
MAX_RAW_BYTES=16*1024*1024
class RevisionLive(Live):
    def __init__(self,work):
        reject_legacy_workspace(work, 'dune_r2.RevisionLive')
        self.w=Path(work).resolve();self.db=RevisionLedger(self.w/'private/shared_budget_r2.sqlite')
        self.confirm=read(self.w/'private/dune_user_confirmation.json')
        if self.confirm.get('status')!='USER_CONFIRMED' or str(self.confirm.get('execution_cap_credits'))!='20' or self.confirm.get('authorization_id')!=AUTH:raise RuntimeError('R2 USER_CONFIRMED cap20 required')
        if self.confirm.get('payment_method_added') is not False or self.confirm.get('extra_credits_enabled') is not False:raise RuntimeError('No payment changes authorized')
        self.account_context_ref=self.confirm.get('account_context_ref','INHERITED_DUNE_ACCOUNT_USER_CONFIRMED_20260906T104844Z')
        self.attempts=AttemptStore(self.w/'private/dune_request_attempts.sqlite')
    def require_gate(self):
        gate=read(self.w/'CONTINUATION_GATE.json')
        if gate.get('status')!='PASS' or gate.get('run_id')!=self.w.name:raise RuntimeError('Continuation gate absent/failed')
        source=gate.get('source_sha256',{})
        if not source:raise RuntimeError('Gate source identities absent')
        for name,digest in source.items():
            if hashlib.sha256((self.w/'src'/name).read_bytes()).hexdigest()!=digest:raise RuntimeError('Gate code changed; revalidate before dispatch')
    def job_caps(self,state):return {'execution':Decimal(20),'export':None,'logical':Decimal(100),'authorization_id':AUTH}
    def pending_job(self,state):
        rows=[r for r in self.db.rows() if r['request_id']==state['logical_job_id'] and r['unit']=='dune_credits']
        if len(rows)!=1 or rows[0]['actual'] is not None or state.get('request_set_closed'):raise RuntimeError('Export requires an open unique reservation')
        return rows[0]
    def call(self,op,execution=None,payload=None,params=None):
        if op=='execute':
            if not getattr(self,'_active_sql_dispatch',False):raise RuntimeError('Use guarded submit')
            if set(payload or {})!={'sql','performance'} or payload['performance'] not in ('small','medium'):raise ValueError('Only Small/Medium; no account-cap bypass')
        if op=='results' and not getattr(self,'_active_export_dispatch',False):raise RuntimeError('Use guarded export')
        if execution is not None and not re.fullmatch('[A-Z0-9]{26}',execution):raise ValueError('Invalid execution ID')
        paths={'usage':'/usage','execute':'/sql/execute','status':f'/execution/{execution}/status','results':f'/execution/{execution}/results'}
        if op not in paths:raise ValueError('Unsupported operation')
        if params:
            if op!='results' or set(params)!={'limit','offset'} or isinstance(params['limit'],bool) or isinstance(params['offset'],bool) or not 1<=params['limit']<=1000 or params['offset']<0:raise ValueError('Unsafe page parameters')
        baseline=self.w/'baseline/FINAL_RESOURCE_ADDENDUM.json';mapping=self.w/'manifests/R1_INPUT_MAPPING.json'
        if baseline.exists() and mapping.exists():
            prior=read(baseline)['bytes']['conservative_physical_directory_upper_with_unknown_read_reserve']
            reused={v['destination'] for v in read(mapping) if v.get('status')=='COPIED_IDENTICAL' and v.get('destination','').startswith('raw/')}
            new_bytes=sum(p.stat().st_size for p in (self.w/'raw').rglob('*') if p.is_file() and p.relative_to(self.w).as_posix() not in reused)
            if prior+new_bytes+MAX_RAW_BYTES>536870912:raise RuntimeError('Cumulative raw capacity insufficient for next bounded response')
        url='https://api.dune.com/api/v1'+paths[op]
        if params:url+='?'+urllib.parse.urlencode(params)
        data=json.dumps(payload or {}).encode() if op in ('usage','execute') else None
        req=urllib.request.Request(url,data=data,headers={'X-Dune-API-Key':os.environ['DUNE_API_KEY'],'Content-Type':'application/json'})
        stamp=datetime.now(timezone.utc).isoformat();t=time.monotonic();status=None;error=None;raw=b'';rid='dune_r2_'+op+'_'+uuid.uuid4().hex
        try:
            with urllib.request.build_opener(NoRedirect()).open(req,timeout=30) as response:status=response.status;raw=response.read(MAX_RAW_BYTES)
        except urllib.error.HTTPError as exc:status=exc.code;raw=exc.read(MAX_RAW_BYTES)
        except Exception as exc:error=type(exc).__name__
        if len(raw)>=MAX_RAW_BYTES:error='RAW_RESPONSE_AT_RESERVED_READ_LIMIT'
        if any(v.encode() in raw for k,v in os.environ.items() if any(x in k.upper() for x in ('API_KEY','TOKEN')) and len(v)>12):raw=b'';error='CREDENTIAL_ECHO_WITHHELD'
        path=self.w/'raw/dune'/f'{rid}.json';path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
        receipt={'request_id':rid,'operation':op,'execution_id':execution,'utc':stamp,'elapsed_seconds':round(time.monotonic()-t,6),'http_status':status,'error_class':error,'parameters':params,'raw_path':path.relative_to(self.w).as_posix(),'raw_bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
        dump(self.w/'logs'/f'{rid}.json',receipt)
        try:body=None if error else json.loads(raw,parse_float=Decimal)
        except Exception:body=None
        return body,receipt
    def usage(self):
        body,receipt=self.call('usage')
        if receipt.get('http_status')!=200 or not isinstance(body,dict):raise RuntimeError('Usage unavailable; receipt retained')
        today=datetime.now(timezone.utc).date().isoformat();current=next((p for p in body.get('billing_periods',[]) if p['start_date']<=today<p['end_date']),None)
        if current is None:raise RuntimeError('Current billing period absent')
        remaining=valid(current['credits_included'],'dune_credits')-valid(current['credits_used'],'dune_credits')
        self.db.confirm('dune_credits',str(max(Decimal(0),remaining)),'Current included remaining '+receipt['request_id']+'; cumulative100 unchanged; no precise job delta inference')
        dump(self.w/'private/current_dune_usage.json',{'observed_at_utc':receipt['utc'],'receipt':receipt,'billing_period':current,'included_remaining_credits':str(remaining),'account_context_ref':self.account_context_ref,'not_final_job_settlement':True})
        return {'http_status':200,'included_remaining_credits':str(remaining),'usage_request_id':receipt['request_id'],'cumulative':self.db.snapshot()['dune_credits']}
    def rate_evidence(self):
        rate=read(self.w/'private/dune_rate_evidence.json')
        if rate.get('account_context_ref')!=self.account_context_ref or rate.get('status') not in ('USER_CONFIRMED','INDEPENDENTLY_VERIFIED'):raise RuntimeError('Rate account applicability missing')
        if rate.get('plan')=='Plus trial' and rate.get('credit_economics_tier')!='Free':raise RuntimeError('Trial Free economics evidence required')
        if str(rate.get('export_credits_per_decimal_MB_for_budget'))!='20':raise RuntimeError('Documented20/decimalMB evidence required')
        return rate
    def observe_execution_charge(self,state,body,receipt):
        if not isinstance(body,dict) or body.get('execution_cost_credits') is None:return
        cost=valid(body['execution_cost_credits'],'dune_credits');known=max(cost,Decimal(state.get('execution_cost_credits') or '0'))
        # Only a reliable terminal receipt releases the unknown execution part.
        terminal=body.get('state') in TERMINAL
        self.db.observe_execution(state['logical_job_id'],cost,receipt,terminal=terminal)
        state.update(execution_cost_credits=str(known),known_execution_cost_credits=str(known),reserved_execution=str(known if terminal else max(Decimal(20),known)))
        state.setdefault('execution_charge_observations',[]).append({'request_id':receipt['request_id'],'utc':receipt.get('utc'),'state':body.get('state'),'execution_cost_credits':str(cost),'terminal':terminal})
    def submit(self,sqlpath,label,kind='candidate',freeze_manifest=None,performance='medium'):
        self.require_gate();self.ensure_not_halted()
        if kind not in ('candidate','frontier_labels','context'):raise ValueError('Invalid job kind')
        if performance not in ('small','medium'):raise ValueError('Large not authorized')
        sqlpath=Path(sqlpath).resolve();sqlpath.relative_to(self.w);sql=sqlpath.read_text(encoding='utf-8');digest=hashlib.sha256(sql.encode()).hexdigest()
        if not sql.lstrip().startswith('--') or re.search(r'\b(INSERT|DELETE|DROP|ALTER|CREATE|UPDATE|MERGE)\s+(INTO|FROM|TABLE|VIEW|SCHEMA)',sql,re.I):raise ValueError('Reviewed read-only SQL required')
        if freeze_manifest is None:raise RuntimeError('Frozen scope/export plan required')
        freeze_manifest=Path(freeze_manifest).resolve();freeze_manifest.relative_to(self.w);frozen=read(freeze_manifest)
        if not isinstance(frozen,dict) or not frozen.get('export_plan'):raise RuntimeError('Frozen manifest must include export_plan before execution')
        if frozen.get('sql_sha256') not in (None,digest):raise RuntimeError('SQL differs from frozen plan')
        if kind=='candidate':
            from dune_batch_r1 import verify_frozen
            verify_frozen(freeze_manifest)
            if frozen.get('full_sql_sha256')!=digest:raise RuntimeError('Candidate SQL differs from verified scope batch')
        usage=read(self.w/'private/current_dune_usage.json');period=usage['billing_period'];today=datetime.now(timezone.utc).date().isoformat()
        if not period['start_date']<=today<period['end_date']:raise RuntimeError('Account period changed; usage/entitlement recheck needed')
        if self.attempts.unresolved(self.account_context_ref):raise RequestBlocked('Unknown request remains; no replacement SQL')
        folder=self.w/'private/dune_r2_jobs'/digest
        if folder.exists():raise RequestBlocked('SQL already planned/submitted; use saved job and pages')
        job='dune_r2:'+digest;self.db.reserve_dune_job(job,label,'20','0');folder.mkdir(parents=True)
        (folder/'query.sql').write_text(sql,encoding='utf-8',newline='\n')
        state={'logical_job_id':job,'query_label':label,'kind':kind,'sql_sha256':digest,'performance':performance,'reserved_execution':'20','reserved_export':'0','state':'PLANNED','export_requests':0,'export_offsets':[],'account_context_ref':self.account_context_ref,'authorization_id':AUTH,'scope_freeze_path':freeze_manifest.relative_to(self.w).as_posix(),'scope_freeze_sha256':hashlib.sha256(freeze_manifest.read_bytes()).hexdigest(),'export_plan':frozen['export_plan'],'planned_at_utc':datetime.now(timezone.utc).isoformat()}
        dump(folder/'job.json',state)
        identity=AttemptStore.identity(self.account_context_ref,job,'SQL:'+digest,'execute',{'sql_sha256':digest,'performance':performance});aid=self.attempts.dispatch(identity)
        state.update(state='SUBMITTING_OR_UNCERTAIN',submit_attempt_id=aid);dump(folder/'job.json',state)
        try:
            self._active_sql_dispatch=True;body,receipt=self.call('execute',payload={'sql':sql,'performance':performance})
            self.attempts.save_response(aid,body,receipt,folder/'submit_response.json',folder/'submit_receipt.json')
            state.update(submit_response=body,submit_receipt=receipt)
            if receipt.get('http_status')==200 and isinstance(body,dict) and re.fullmatch('[A-Z0-9]{26}',str(body.get('execution_id',''))):
                state.update(execution_id=body['execution_id'],state=body.get('state','QUERY_STATE_PENDING'));self.attempts.validated(aid,{'complete':True,'next_offset':None})
                self.observe_execution_charge(state,body,receipt)
            else:self.attempts.mark(aid,'UNKNOWN_TRANSPORT' if receipt.get('error_class') else 'INVALID_RESPONSE','Submit lacks verified accepted execution')
            dump(folder/'job.json',state)
        except BaseException as exc:
            if self.attempts.get(identity)['state']=='DISPATCH_INTENT':self.attempts.mark(aid,'UNKNOWN_TRANSPORT',type(exc).__name__)
            raise
        finally:self._active_sql_dispatch=False
        return {'job_folder':str(folder),'execution_id':state.get('execution_id'),'state':state['state'],'http_status':receipt.get('http_status')}
    def poll(self,folder):
        folder=Path(folder);state=read(folder/'job.json');body,receipt=self.call('status',state['execution_id'])
        state.update(latest_status_response=body,latest_status_receipt=receipt)
        if receipt.get('http_status')==200 and not receipt.get('error_class') and isinstance(body,dict) and body.get('execution_id')==state['execution_id'] and body.get('state'):
            state.update(status_response=body,status_receipt=receipt,state=body['state']);self.observe_execution_charge(state,body,receipt)
        dump(folder/'job.json',state)
        return {'http_status':receipt.get('http_status'),'execution_id':state['execution_id'],'state':state['state'],'execution_cost_credits':state.get('execution_cost_credits'),'result_metadata':(body or {}).get('result_metadata') if isinstance(body,dict) else None,'cumulative':self.db.snapshot()['dune_credits']}
    def export_progress(self,folder,state):
        folder=Path(folder);offsets=state.get('export_offsets',[])
        if len(offsets)!=state.get('export_requests',0) or len(offsets)!=len(set(offsets)):raise RuntimeError('Inconsistent page attempts')
        md=(state.get('status_response') or {}).get('result_metadata') or {};progress=initial_progress(exact_count(md.get('total_row_count'),'total_row_count'))
        for offset in offsets:
            if progress['complete'] or offset!=progress['next_offset']:raise RuntimeError('Noncontiguous/postterminal page')
            page=read(folder/f'page_{offset}.json');receipt=read(folder/f'page_{offset}_receipt.json');params=receipt.get('parameters') or {};limit=params.get('limit')
            if params.get('offset')!=offset or isinstance(limit,bool) or not isinstance(limit,int) or not 1<=limit<=1000:raise RuntimeError('Invalid saved pagination')
            identity=AttemptStore.identity(self.account_context_ref,state['logical_job_id'],state['execution_id'],'results',params)
            if not self.attempts.get(identity):raise RequestBlocked('R2 page lacks durable attempt identity')
            self.attempts.cached(identity)
            progress=validate_page(page,execution_id=state['execution_id'],offset=offset,limit=limit,progress=progress,status_metadata=md,receipt=receipt,parameters=params)
        return progress
    def export_envelope(self,state,limit,folder=None):
        if isinstance(limit,bool) or not isinstance(limit,int) or not 1<=limit<=1000:raise ValueError('Page limit1..1000')
        rate=self.rate_evidence();md=(state.get('status_response') or {}).get('result_metadata') or {}
        rows=exact_count(md.get('total_row_count'),'total_row_count');size=exact_count(md.get('total_result_set_bytes'),'total_result_set_bytes');cols=md.get('column_names')
        if not isinstance(cols,list) or not cols:raise ValueError('Full column metadata required')
        pages=max(1,(rows+limit-1)//limit);observations=[{'source':'status','metadata':md,'receipt_sha256':(state.get('status_receipt') or {}).get('sha256')}]
        if state.get('export_requests',0):
            if folder is None:raise RuntimeError('Saved page metadata required')
            self.export_progress(folder,state)
            for offset in state['export_offsets']:
                receipt=read(Path(folder)/f'page_{offset}_receipt.json');page=read(Path(folder)/f'page_{offset}.json')
                observations.append({'source':'verified_page','offset':offset,'metadata':page['result']['metadata'],'receipt_sha256':receipt.get('sha256'),'saved_page_sha256':hashlib.sha256((Path(folder)/f'page_{offset}.json').read_bytes()).hexdigest()})
        sizes=[size];points=[rows*len(cols)]
        for obs in observations:
            for key in ('total_result_set_bytes','result_set_bytes'):
                if key in obs['metadata']:sizes.append(exact_count(obs['metadata'][key],key))
            if 'datapoint_count' in obs['metadata']:points.append(exact_count(obs['metadata']['datapoint_count'],'datapoint_count'))
        size=max(sizes);datapoints=max(Decimal(max(points)),(Decimal(size)/100).to_integral_value(rounding=ROUND_CEILING))
        byte_cost=Decimal(size)*20/1000000;point_cost=datapoints/1000
        per_request=max(Decimal(1),max(byte_cost,point_cost).to_integral_value(rounding=ROUND_CEILING));upper=per_request*pages
        basis={'rate_evidence':rate,'result_metadata':md,'observations':observations,'maximum_server_bytes':size,'maximum_datapoints':str(datapoints),'server_byte_fields_disagree':len(set(sizes))>1,'byte_scheme_credits':str(byte_cost),'datapoint_scheme_credits':str(point_cost),'maximum_pages':pages,'page_limit':limit,'per_request_upper_credits':str(per_request),'full_export_upper_credits':str(upper),'is_actual':False,'basis':'Each request conservatively bounded by entire server result metadata; maximum of documented Free20/decimalMB and datapoint evidence; whole-credit upward rounding is conservative reservation, not documented minimum or bill. No compressed HTTP bytes.'}
        return upper,basis
    def export(self,folder,limit=1000,offset=0):
        self.require_gate();self.ensure_not_halted();folder=Path(folder);state=read(folder/'job.json');self.pending_job(state)
        if state.get('state')!='QUERY_STATE_COMPLETED':raise RuntimeError('Execution not completed')
        progress=self.export_progress(folder,state)
        if progress['complete']:return {'cache_reused':True,'progress':progress,'export_status':'COMPLETED_DECLARED_RESULT_ROWS'}
        if isinstance(offset,bool) or not isinstance(offset,int) or offset!=progress['next_offset']:raise ValueError('Only exact next page allowed')
        if progress['page_size'] not in (None,limit):raise ValueError('Page size frozen')
        upper,basis=self.export_envelope(state,limit,folder);self.db.reserve_export(state['logical_job_id'],upper,basis)
        state.update(reserved_export=str(upper),r2_export_envelope=basis);dump(folder/'job.json',state)
        params={'limit':limit,'offset':offset};identity=AttemptStore.identity(self.account_context_ref,state['logical_job_id'],state['execution_id'],'results',params);aid=self.attempts.dispatch(identity)
        try:
            state['export_offsets'].append(offset);state['export_requests']+=1;dump(folder/'job.json',state)
            self._active_export_dispatch=True;body,receipt=self.call('results',state['execution_id'],params=params)
            self.attempts.save_response(aid,body,receipt,folder/f'page_{offset}.json',folder/f'page_{offset}_receipt.json')
            validated=validate_page(body,execution_id=state['execution_id'],offset=offset,limit=limit,progress=progress,status_metadata=state['status_response']['result_metadata'],receipt=receipt,parameters=params)
            self.attempts.validated(aid,validated);state['verified_export_progress']=self.export_progress(folder,state)
            state['export_status']='COMPLETED_DECLARED_RESULT_ROWS' if validated['complete'] else 'PARTIAL_CONTIGUOUS_EXPORT'
            upper,basis=self.export_envelope(state,limit,folder);self.db.reserve_export(state['logical_job_id'],upper,basis,observed=True);state.update(reserved_export=str(upper),r2_export_envelope=basis,last_export_receipt=receipt)
        except BaseException as exc:
            if self.attempts.get(identity)['state']!='SUCCESS_VALIDATED':self.attempts.mark(aid,'UNKNOWN_TRANSPORT' if not locals().get('receipt',{}).get('http_status') or locals().get('receipt',{}).get('error_class') else 'INVALID_RESPONSE',type(exc).__name__)
            state.update(export_status='EXPORT_FAILED_OR_UNCERTAIN',export_gap=type(exc).__name__);dump(folder/'job.json',state);raise
        finally:self._active_export_dispatch=False
        dump(folder/'job.json',state)
        return {'http_status':receipt['http_status'],'raw_bytes':receipt['raw_bytes'],'rows':len(body['result']['rows']),'export_status':state['export_status'],'progress':validated,'upper_not_actual':str(upper),'cumulative':self.db.snapshot()['dune_credits']}
    def settle(self,folder):
        folder=Path(folder);state=read(folder/'job.json')
        terminal_actual=self.db.final_execution_cost(state['logical_job_id'])
        reported={'known_execution_actual':None if terminal_actual is None else str(terminal_actual),
            'observed_execution_peak_retained':state.get('execution_cost_credits'),
            'execution_peak_discrepancy_risk':None if terminal_actual is None else str(max(Decimal(0),Decimal(state.get('execution_cost_credits') or '0')-terminal_actual))}
        if state.get('request_set_closed'):return {'settlement_status':state['settlement_status'],'already_closed':True,**reported}
        if state.get('state') not in TERMINAL or state.get('execution_cost_credits') is None:raise RuntimeError('Terminal known execution required')
        if self.attempts.unresolved(self.account_context_ref):raise RequestBlocked('Unknown attempts retain full risk')
        receipt=state.get('status_receipt') or state.get('submit_receipt');evidence={'request_set_closed':True,'no_unknown_attempts':True,'source_receipt_sha256':receipt['sha256']}
        exported=bool(state.get('export_requests'))
        if exported:
            progress=self.export_progress(folder,state)
            if not progress['complete']:raise RuntimeError('Incomplete stream cannot close as complete')
            upper,basis=self.export_envelope(state,progress['page_size'],folder);self.db.reserve_export(state['logical_job_id'],upper,basis,observed=True)
            evidence.update(full_page_chain_verified=True,rate_evidence=basis['rate_evidence'],metering_basis=basis)
        status=self.db.close_job(state['logical_job_id'],exported=exported,evidence=evidence)
        state.update(request_set_closed=True,settlement_status=status,settlement_evidence=evidence,known_export_cost_credits=None if exported else '0');dump(folder/'job.json',state)
        return {'settlement_status':status,**reported,'export_actual':None if exported else '0','cumulative':self.db.snapshot()['dune_credits']}

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=('usage','submit','poll','status','export','settle','snapshot','migrate'));p.add_argument('--work',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--baseline',type=Path);p.add_argument('--confirmation',type=Path);p.add_argument('--sql',type=Path);p.add_argument('--label');p.add_argument('--kind',default='candidate');p.add_argument('--freeze',type=Path);p.add_argument('--performance',default='medium');p.add_argument('--folder',type=Path);p.add_argument('--limit',type=int,default=1000);p.add_argument('--offset',type=int,default=0);a=p.parse_args()
    if a.action=='migrate':result=migrate_revision(a.work,a.baseline,read(a.confirmation))
    else:
        live=RevisionLive(a.work)
        if a.action=='usage':result=live.usage()
        elif a.action=='submit':result=live.submit(a.sql,a.label,a.kind,a.freeze,a.performance)
        elif a.action in ('poll','status'):result=live.poll(a.folder)
        elif a.action=='export':result=live.export(a.folder,a.limit,a.offset)
        elif a.action=='settle':result=live.settle(a.folder)
        else:result=live.db.snapshot()
    print(json.dumps(result,indent=2,default=str))
if __name__=='__main__':main()
