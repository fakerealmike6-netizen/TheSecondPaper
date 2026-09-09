"""Single-worker R1 transport: explicit grant, repair gate, retained legacy risks."""
import argparse,hashlib,json,uuid,re
from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal,ROUND_CEILING
from dune_live import Live,read,dump
from budget_r1 import RevisionLedger,AUTH
from page_attempts import AttemptStore
from legacy_guard_r4 import reject_legacy_workspace
from dune_cap_exception import CAP5_AUTH,CONFIRMED,RESTORED,TERMINAL,read_control,ordinary_sql_allowed,verify_submitted_binding,binding

class RevisionLive(Live):
    def __init__(self,work):
        reject_legacy_workspace(work, 'dune_r1.RevisionLive')
        self.w=Path(work).resolve();self.db=RevisionLedger(self.w/'private/shared_budget_r1.sqlite')
        self.confirm=read(self.w/'private/dune_user_confirmation.json')
        assert self.confirm['status']=='USER_CONFIRMED' and self.confirm['execution_cap_credits']=='1'
        assert self.confirm['payment_method_added'] is False and self.confirm['extra_credits_enabled'] is False
        self.account_context_ref='INHERITED_DUNE_ACCOUNT_USER_CONFIRMED_20260906T104844Z'
        self.attempts=AttemptStore(self.w/'private/dune_request_attempts.sqlite')
    def call(self,op,execution=None,payload=None,params=None):
        if op=='execute' and not getattr(self,'_active_sql_dispatch',None):raise RuntimeError('Execute requires the guarded submit entry point')
        if op=='usage':self.db.count_action('usage:'+uuid.uuid4().hex,'usage',6)
        return super().call(op,execution,payload,params)
    def job_caps(self,state):return self.db.job_caps(state['logical_job_id'])
    def control(self):return read_control(self.w)
    def _update_exception_control(self,state,terminal=False,unknown=False):
        if self.job_caps(state)['authorization_id']!=CAP5_AUTH:return
        control=self.control()
        if control is None or control.get('authorization_id')!=CAP5_AUTH:raise RuntimeError('Exception control is missing')
        if control.get('status') in (RESTORED,'USER_CONFIRMED_KEEP5_ORDINARY_SQL_PAUSED'):return
        control.update(status='PENDING_RESTORE_1' if terminal or unknown else 'EXCEPTION_INFLIGHT',sql_submissions_paused=True,
            additional_attempt_used=True,exception_job_id=state['logical_job_id'],exception_execution_id=state.get('execution_id'),
            last_observed_exception_state=state.get('state'),unknown_attempt=bool(unknown),updated_at_utc=datetime.now(timezone.utc).isoformat())
        if terminal or unknown:control['ordinary_sql_requires_restore_confirmation']=True
        dump(self.w/'private/dune_cap5_control.json',control)
    def usage(self):
        body,r=self.call('usage')
        if not isinstance(body,dict) or r.get('http_status')!=200:raise RuntimeError('Current usage unavailable; receipt retained')
        today=datetime.now(timezone.utc).date().isoformat()
        current=next((p for p in body.get('billing_periods',[]) if p['start_date']<=today<p['end_date']),None)
        if current is None:raise RuntimeError('No current billing period')
        remaining=Decimal(str(current['credits_included']))-Decimal(str(current['credits_used']))
        # Conservative account allowance: prior risk is not reset by this read.
        self.db.confirm('dune_credits',str(max(Decimal(0),remaining)),'Current included remaining; '+r['request_id']+'; legacy and R1 grant unchanged')
        saved={'observed_at_utc':r['utc'],'receipt':r,'billing_period':current,'included_remaining_credits':str(remaining),'account_context_ref':self.account_context_ref,'root_keys':sorted(body),'not_final_job_settlement':True}
        dump(self.w/'private/current_dune_usage.json',saved)
        return {'http_status':r['http_status'],'included_remaining_credits':str(remaining),'root_keys':sorted(body),'current_period_fields':sorted(current),'usage_request_id':r['request_id']}
    def require_gate(self):
        gate=read(self.w/'REPAIR_GATE.json')
        if gate.get('status')!='PASS' or gate.get('run_id')!=self.w.name:raise RuntimeError('Repair gate absent or failed')
        for name,digest in gate.get('source_sha256',{}).items():
            if hashlib.sha256((self.w/'src'/name).read_bytes()).hexdigest()!=digest:raise RuntimeError('Repair gate code has changed; revalidate before new network work')
        if not gate.get('source_sha256'):raise RuntimeError('Gate source identity absent')
    def observe_execution_charge(self,state,body,receipt):
        super().observe_execution_charge(state,body,receipt)
        if isinstance(body,dict) and body.get('execution_cost_credits') is not None:
            self.db.observe_execution(state['logical_job_id'],body['execution_cost_credits'],receipt)
        if isinstance(body,dict) and body.get('state') in TERMINAL:self._update_exception_control(state|{'state':body['state']},terminal=True)
    def rate_evidence(self):
        path=self.w/'private/dune_rate_evidence.json'
        if not path.exists():raise RuntimeError('Current account export metering applicability unresolved; keep all risks and continue offline')
        evidence=read(path)
        if evidence.get('account_context_ref')!=self.account_context_ref or evidence.get('status') not in ('USER_CONFIRMED','INDEPENDENTLY_VERIFIED'):
            raise RuntimeError('Account rate evidence identity not verified')
        if evidence.get('plan') not in ('Free','Analyst','Plus','Plus trial'):raise RuntimeError('No supported conservative export rate envelope for this plan')
        if evidence.get('plan')=='Plus trial' and evidence.get('credit_economics_tier')!='Free':raise RuntimeError('Trial must use documented Free economics')
        return evidence
    def export_envelope(self,state,limit,folder=None):
        """Conservative whole-credit ceiling for each possible result request.

        Both published byte and datapoint schemes are covered. Before dispatch,
        full result statistics bound each planned page. After dispatch, include
        every verified page's server metadata; page/result byte fields need not
        agree. This intentionally loose envelope is never an actual bill.
        No compression/HTTP payload estimate or usage delta is used to settle.
        """
        rate=self.rate_evidence();md=(state.get('status_response') or {}).get('result_metadata') or {}
        from page_contract import exact_count
        rows=exact_count(md.get('total_row_count'),'total_row_count')
        size=exact_count(md.get('total_result_set_bytes'),'total_result_set_bytes')
        cols=md.get('column_names')
        if not isinstance(cols,list) or not cols:raise RuntimeError('Full result column count absent')
        if isinstance(limit,bool) or not isinstance(limit,int) or not 1<=limit<=50:raise ValueError('Page limit')
        pages=max(1,(rows+limit-1)//limit)
        observations=[{'source':'status','metadata':md,'source_receipt_sha256':(state.get('status_receipt') or {}).get('sha256')}]
        if state.get('export_requests',0):
            if folder is None:raise RuntimeError('Submitted pages require verified saved metadata for an export envelope')
            folder=Path(folder);self.export_progress(folder,state)
            for offset in state['export_offsets']:
                pagefile=folder/f'page_{offset}.json';receiptfile=folder/f'page_{offset}_receipt.json'
                receipt=read(receiptfile)
                observations.append({'source':'verified_page','offset':offset,'metadata':read(pagefile)['result']['metadata'],
                    'source_receipt_sha256':receipt.get('sha256'),'saved_receipt_sha256':hashlib.sha256(receiptfile.read_bytes()).hexdigest(),
                    'saved_page_sha256':hashlib.sha256(pagefile.read_bytes()).hexdigest(),'request_id':receipt.get('request_id')})
        sizes=[];points=[rows*len(cols)]
        for observation in observations:
            metadata=observation['metadata']
            for key in ('result_set_bytes','total_result_set_bytes'):
                if key in metadata:sizes.append(exact_count(metadata[key],observation['source']+' '+key))
            if 'datapoint_count' in metadata:points.append(exact_count(metadata['datapoint_count'],observation['source']+' datapoint_count'))
        size=max([size]+sizes)
        datapoints=max(Decimal(max(points)),(Decimal(size)/100).to_integral_value(rounding=ROUND_CEILING))
        documented=max(Decimal(size)*20/1000000,datapoints/1000)
        per_page=max(Decimal(1),documented.to_integral_value(rounding=ROUND_CEILING))
        bound=per_page*pages
        return bound,{'basis':'upper envelope of both published non-enterprise schemes; conservative whole-credit rounding per request; maximum available status and verified-page server metadata bounds each planned page; HTTP body size is provenance only',
            'rate_evidence':rate,'result_metadata':md,'server_metadata_observations':observations,'maximum_server_bytes':size,
            'maximum_datapoints':str(datapoints),'server_byte_fields_disagree':len(set(sizes))>1,
            'byte_scheme_credits':str(Decimal(size)*20/1000000),'datapoint_scheme_credits':str(datapoints/1000),
            'page_limit':limit,'maximum_pages':pages,'per_page_bound_credits':str(per_page),'full_export_bound_credits':str(bound),'is_actual':False}
    def envelope_evidence(self,state,basis,reason):
        receipt=state.get('status_receipt') or state.get('submit_receipt') or {}
        return {'request_set_closed':True,'source_receipt_sha256':receipt.get('sha256'),'account_context_ref':self.account_context_ref,
            'applicable_rate_evidence':basis['rate_evidence'],
            'metered_units_evidence':{'status':basis['result_metadata'],'observations':basis['server_metadata_observations'],
                'maximum_server_bytes':basis['maximum_server_bytes'],'maximum_datapoints':basis['maximum_datapoints'],
                'server_byte_fields_disagree':basis['server_byte_fields_disagree']},
            'all_attempts_included':True,'rounding_and_minimum_evidence':basis['basis'],'unknown_attempt_risk_included':reason}
    def retain_expanded_export_risk(self,folder,state,upper,basis):
        """A verified page exceeded its preflight cap: stop and retain full risk.

        No future page is dispatched. Closing this local request set is distinct
        from claiming that all result rows were acquired. The bound remains an
        upper, including the conservative planned-page envelope, never a bill.
        """
        cost=Decimal(str(state['execution_cost_credits']));caps=self.job_caps(state)
        if cost+upper<=caps['logical'] and (caps['export'] is None or upper<=caps['export']):return
        if not state.get('export_requests'):raise RuntimeError('Documented export risk envelope exceeds the authorized job/export cap; no export')
        amounts=[r for r in self.db.rows() if r['request_id']==state['logical_job_id'] and r['unit']=='dune_credits']
        if len(amounts)!=1:raise RuntimeError('Cannot record expanded risk without the existing unique reservation')
        retained=max(Decimal(amounts[0]['reserved']),cost+upper)
        violation={'reason':'VERIFIED_EXPORT_METADATA_EXCEEDS_AUTHORIZED_ENVELOPE','logical_job_id':state['logical_job_id'],
            'execution_id':state['execution_id'],'execution_actual_credits':str(cost),'export_upper_credits':str(upper),
            'retained_total_upper_credits':str(retained),'is_actual':False,'subsequent_submit_export_allowed':False}
        # Durable halt precedes reconciliation or any state rewrite. A disk
        # failure cannot grant another page or silently release the old reserve.
        dump(self.w/'private/dune_live_halt.json',violation)
        evidence=self.envelope_evidence(state,basis,'Verified submitted pages only; remaining pages permanently abandoned locally; entire prior reservation and expanded envelope retained')
        self.db.reconcile(state['logical_job_id'],total_upper=str(retained),evidence=evidence)
        state.update(request_set_closed=True,export_bound_cap_violation=violation,r1_export_envelope=basis,
            total_upper_credits=str(retained),known_export_cost_credits=None,settlement_status='BOUNDED_ACCOUNTING_HALTED',settlement_evidence=evidence)
        dump(Path(folder)/'job.json',state)
        raise RuntimeError('Verified page metadata expanded the export envelope; full risk recorded and all later SQL/export halted')
    def submit(self,sqlpath,label,kind='candidate',freeze_manifest=None,exception_authorization_id=None):
        self.require_gate();self.ensure_not_halted()
        if kind not in ('candidate','frontier_labels','context'):raise ValueError('Unsupported logical job kind')
        sql=Path(sqlpath).read_text(encoding='utf-8');digest=hashlib.sha256(sql.encode()).hexdigest();job='dune_r1:'+digest
        if not sql.lstrip().startswith('--') or re.search(r'\b(INSERT|DELETE|DROP|ALTER|CREATE|UPDATE|MERGE)\s+(INTO|TABLE|VIEW|SCHEMA)',sql,re.I):raise ValueError('Only reviewed SELECT SQL')
        if freeze_manifest is None or not Path(freeze_manifest).is_file():raise RuntimeError('Frozen queue/scope manifest required before submit')
        control=self.control();exception=exception_authorization_id is not None
        if exception:
            if exception_authorization_id!=CAP5_AUTH or kind!='frontier_labels':raise RuntimeError('Cap5 is solely the named nine-address frontier-label exception')
            if control is None:raise RuntimeError('Persistent exception authorization absent')
            frozen=verify_submitted_binding(control,self.account_context_ref,sql,freeze_manifest)
            # The explicit retry remains linked to both failed submissions.
            prior=[]
            for p in (self.w/'private/dune_r1_jobs').glob('*/job.json'):
                old=read(p)
                if old.get('logical_job_id') in frozen['prior_failed_job_ids']:prior.append(old)
            if len(prior)!=2 or any(p.get('state')!='QUERY_STATE_FAILED' for p in prior) or sorted(p.get('execution_id','') for p in prior)!=frozen['original_failed_execution_ids']:
                raise RuntimeError('Both original failed execution contexts must verify before the explicit retry')
            if not any(p.get('sql_sha256')==digest for p in prior):raise RuntimeError('The exception must explicitly retry the already recorded optimized SQL')
            job=CAP5_AUTH
        else:ordinary_sql_allowed(control)
        usage=read(self.w/'private/current_dune_usage.json')
        period=usage['billing_period'];today=datetime.now(timezone.utc).date().isoformat()
        if not period['start_date']<=today<period['end_date']:raise RuntimeError('Current billing period must be checked once; no grant reset')
        folder=self.w/'private/dune_r1_jobs'/(hashlib.sha256(CAP5_AUTH.encode()).hexdigest() if exception else digest)
        if folder.exists():raise RuntimeError('SQL already planned/submitted/uncertain; use saved state')
        if self.attempts.unresolved(self.account_context_ref):raise RuntimeError('Earlier request unknown; no new job')
        if exception:
            self.db.register_cap5_exception(control)
            self.db.reserve_cap5_exception(control,label)
        else:self.db.reserve_dune_job(job,label,'1','1')
        folder.mkdir(parents=True)
        (folder/'query.sql').write_text(sql,encoding='utf-8',newline='\n')
        state={'logical_job_id':job,'query_label':label,'kind':kind,'sql_sha256':digest,'performance':'medium','reserved_execution':'5' if exception else '1','reserved_export':'1','combined_reserved':'6' if exception else '2','state':'PLANNED','export_requests':0,'export_offsets':[],'account_context_ref':self.account_context_ref,'authorization_id':CAP5_AUTH if exception else AUTH,'scope_freeze_sha256':hashlib.sha256(Path(freeze_manifest).read_bytes()).hexdigest(),'scope_freeze_path':Path(freeze_manifest).resolve().relative_to(self.w.resolve()).as_posix(),'planned_at_utc':datetime.now(timezone.utc).isoformat()}
        if exception:state['exception_binding']=frozen
        dump(folder/'job.json',state)
        if kind=='frontier_labels':self.db.count_action(job,'frontier_label_sql',3)
        identity=AttemptStore.identity(self.account_context_ref,job,'SQL:'+digest,'execute',{'sql_sha256':digest,'performance':'medium'})
        aid=self.attempts.dispatch(identity);state.update(state='SUBMITTING_OR_UNCERTAIN',submit_attempt_id=aid);dump(folder/'job.json',state)
        try:
            if exception:self._update_exception_control(state)
            self._active_sql_dispatch={'job':job,'authorization_id':state['authorization_id'],'sql_sha256':digest}
            body,r=self.call('execute',payload={'sql':sql,'performance':'medium'})
            self.attempts.save_response(aid,body,r,folder/'submit_response.json',folder/'submit_receipt.json')
            state['submit_receipt']=r;state['submit_response']=body
            if r.get('http_status')==200 and isinstance(body,dict) and re.fullmatch('[A-Z0-9]{26}',str(body.get('execution_id',''))):
                state['execution_id']=body['execution_id'];state['state']=body.get('state');self.attempts.validated(aid,{'complete':True,'next_offset':None})
            else:
                self.attempts.mark(aid,'UNKNOWN_TRANSPORT' if r.get('error_class') else 'INVALID_RESPONSE','SQL submit lacks verified accepted execution')
                if exception:self._update_exception_control(state,unknown=True)
            self.observe_execution_charge(state,body,r);dump(folder/'job.json',state)
        except BaseException as exc:
            if self.attempts.get(identity)['state']=='DISPATCH_INTENT':self.attempts.mark(aid,'UNKNOWN_TRANSPORT',type(exc).__name__)
            if exception:self._update_exception_control(state,unknown=True)
            raise
        finally:self._active_sql_dispatch=None
        return {'job_folder':str(folder),'execution_id':state.get('execution_id'),'state':state['state'],'http_status':r['http_status']}
    def submit_cap5(self,sqlpath,label,freeze_manifest):
        return self.submit(sqlpath,label,'frontier_labels',freeze_manifest,CAP5_AUTH)
    def export(self,folder,limit,offset):
        self.require_gate();state=read(Path(folder)/'job.json')
        upper,basis=self.export_envelope(state,limit,folder)
        cost=Decimal(str(state.get('execution_cost_credits')))
        caps=self.job_caps(state)
        if cost+upper>caps['logical'] or (caps['export'] is not None and upper>caps['export']):self.retain_expanded_export_risk(folder,state,upper,basis)
        state['r1_export_envelope']=basis;dump(Path(folder)/'job.json',state)
        result=super().export(folder,limit,offset)
        saved=read(Path(folder)/'job.json')
        if saved.get('export_status') in ('COMPLETED_DECLARED_RESULT_ROWS','PARTIAL_CONTIGUOUS_EXPORT'):
            after_upper,after_basis=self.export_envelope(saved,limit,folder)
            self.retain_expanded_export_risk(folder,saved,after_upper,after_basis)
            saved['r1_export_envelope']=after_basis;dump(Path(folder)/'job.json',saved)
        return result
    def settle(self,folder):
        folder=Path(folder);state=read(folder/'job.json');cost=state.get('execution_cost_credits')
        if state.get('state') not in ('QUERY_STATE_COMPLETED','QUERY_STATE_FAILED','QUERY_STATE_CANCELLED') or cost is None:raise RuntimeError('Final execution cost unknown; keep reservation')
        execution=Decimal(str(cost));receipt=state.get('status_receipt') or state.get('submit_receipt')
        if not receipt:raise RuntimeError('Terminal execution receipt absent')
        self._update_exception_control(state,terminal=True)
        evidence={'request_set_closed':True,'source_receipt_sha256':receipt['sha256'],'account_context_ref':self.account_context_ref}
        if state.get('export_requests',0)==0:
            self.db.reconcile(state['logical_job_id'],total_actual=str(execution),export_actual='0',evidence=evidence)
            state['settlement_status']='FINAL_ACTUAL';state['known_export_cost_credits']='0';state['total_actual_credits']=str(execution)
        else:
            progress=self.export_progress(folder,state)
            if not progress['complete']:raise RuntimeError('Incomplete or unknown page attempts retain risk')
            upper,basis=self.export_envelope(state,progress['page_size'],folder)
            self.retain_expanded_export_risk(folder,state,upper,basis)
            evidence=self.envelope_evidence(state,basis,'No unknown attempts; verified full page chain and closed result stream')
            self.db.reconcile(state['logical_job_id'],total_upper=str(execution+upper),evidence=evidence)
            state['r1_export_envelope']=basis
            state['settlement_status']='BOUNDED_ACCOUNTING_NOT_FINAL';state['known_export_cost_credits']=None;state['total_upper_credits']=str(execution+upper)
        state['settlement_evidence']=evidence;state['request_set_closed']=True;dump(folder/'job.json',state)
        return {'settlement_status':state['settlement_status'],'actual':state.get('total_actual_credits'),'upper':state.get('total_upper_credits'),'export_actual':state['known_export_cost_credits']}

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['usage','submit','submit-cap5','poll','export','settle']);p.add_argument('--work',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--sql',type=Path);p.add_argument('--label');p.add_argument('--kind',default='candidate');p.add_argument('--freeze',type=Path);p.add_argument('--folder',type=Path);p.add_argument('--limit',type=int,default=50);p.add_argument('--offset',type=int,default=0);a=p.parse_args();live=RevisionLive(a.work)
    if a.action=='usage':result=live.usage()
    elif a.action=='submit':result=live.submit(a.sql,a.label,a.kind,a.freeze)
    elif a.action=='submit-cap5':result=live.submit_cap5(a.sql,a.label,a.freeze)
    elif a.action=='poll':result=live.poll(a.folder)
    elif a.action=='export':result=live.export(a.folder,a.limit,a.offset)
    else:result=live.settle(a.folder)
    if result is not None:print(json.dumps(result,indent=2,default=str))
if __name__=='__main__':main()
