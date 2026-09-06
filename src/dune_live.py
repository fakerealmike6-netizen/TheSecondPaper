"""Explicit one-step Dune transport. No automatic SQL retry or browser control."""
import argparse, hashlib, json, os, re, time, uuid, urllib.request, urllib.error
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
from budget import Ledger
from network import NoRedirect

def dump(p,v):p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(v,indent=2,ensure_ascii=False,default=str)+'\n',encoding='utf-8')
def read(p):return json.loads(p.read_text(encoding='utf-8'))
class Live:
    def __init__(self,work):
        self.w=Path(work);self.db=Ledger(self.w/'private/shared_budget.sqlite')
        self.confirm=read(self.w/'private/dune_user_confirmation.json')
        assert self.confirm['status']=='USER_CONFIRMED' and self.confirm['execution_cap_credits']=='1'
        assert self.confirm['payment_method_added'] is False and self.confirm['extra_credits_enabled'] is False
    def ensure_not_halted(self):
        if (self.w/'private/dune_live_halt.json').exists():
            raise RuntimeError('Dune submissions/exports halted after a recorded execution-cap violation')
        if any(x['overrun'] or x['actual_exceeded_reservation'] for x in self.db.snapshot().values()):
            raise RuntimeError('Stage halted after recorded budget overrun')
    def pending_job(self,state):
        rows=[r for r in self.db.rows() if r['request_id']==state['logical_job_id'] and r['unit']=='dune_credits']
        if len(rows)!=1 or rows[0]['actual'] is not None or rows[0]['status'] not in ('RESERVED','UNKNOWN_RESERVED'):
            raise RuntimeError('Dune job has no pending shared reservation; settled jobs cannot export')
        if Decimal(rows[0]['reserved'])!=Decimal('2'):
            raise RuntimeError('Dune transport requires its entire 2-credit logical-job reservation')
        return rows[0]
    def observe_execution_charge(self,state,body,receipt):
        if not isinstance(body,dict) or body.get('execution_cost_credits') is None:return
        try:cost=Decimal(str(body['execution_cost_credits']))
        except Exception:raise RuntimeError('Invalid reported execution charge; retain reservation')
        if not cost.is_finite() or cost<0:raise RuntimeError('Invalid reported execution charge; retain reservation')
        # Retain the exact observation even when a later status/HTTP error
        # omits the charge. Running and final observations remain separate.
        state['execution_cost_credits']=str(cost)
        state['known_execution_cost_credits']=str(cost)
        state.setdefault('execution_charge_observations',[]).append({'request_id':receipt['request_id'],'utc':receipt.get('utc'),
            'state':body.get('state'),'execution_cost_credits':str(cost)})
        if cost>Decimal('1'):
            violation={'reason':'EXECUTION_CAP_EXCEEDED','execution_cap_credits':'1','observed_execution_cost_credits':str(cost),
                       'logical_job_id':state['logical_job_id'],'execution_id':state.get('execution_id'),
                       'receipt_request_id':receipt['request_id'],'utc':receipt.get('utc'),
                       'subsequent_submit_export_allowed':False,'unknown_export_charge_must_remain_reserved':True}
            state['execution_cap_violation']=violation
            dump(self.w/'private/dune_live_halt.json',violation)
    def export_progress(self,folder,state):
        """Replay only saved page receipts; failed/uncertain pages never end a stream."""
        offsets=state.get('export_offsets',[])
        if len(offsets)!=state.get('export_requests',0) or len(offsets)!=len(set(offsets)):
            raise RuntimeError('Inconsistent export attempt history; retain reservation')
        response=state.get('status_response') or {}
        md=response.get('result_metadata') or {}
        total=md.get('total_row_count',md.get('row_count'))
        if total is None:raise RuntimeError('Total export rows unknown')
        total=int(total)
        if total<0:raise RuntimeError('Invalid total export rows')
        progress={'page_size':None,'next_offset':0,'observed_rows':0,'complete':False,'successful_pages':0}
        for offset in offsets:
            if progress['complete'] or offset!=progress['next_offset']:
                raise RuntimeError('Overlapping, noncontiguous or post-terminal export history')
            pagefile=folder/f'page_{offset}.json';receiptfile=folder/f'page_{offset}_receipt.json'
            if not pagefile.exists() or not receiptfile.exists():
                raise RuntimeError('A submitted export page remains uncertain; no replacement or continuation')
            page=read(pagefile);receipt=read(receiptfile);params=receipt.get('parameters') or {}
            size=params.get('limit')
            if receipt.get('http_status')!=200 or receipt.get('error_class') or not isinstance(page,dict):
                raise RuntimeError('A submitted export page failed or remains uncertain')
            if params.get('offset')!=offset or not isinstance(size,int) or isinstance(size,bool) or not 1<=size<=50:
                raise RuntimeError('Missing/invalid saved pagination parameters')
            if progress['page_size'] is not None and size!=progress['page_size']:
                raise RuntimeError('Page size changed within one logical export')
            progress['page_size']=size
            result=page.get('result') or {};rows=result.get('rows')
            if page.get('state') not in (None,'QUERY_STATE_COMPLETED') or not isinstance(rows,list) or len(rows)>size:
                raise RuntimeError('Saved page does not certify successful result rows')
            if page.get('execution_id') not in (None,state['execution_id']):
                raise RuntimeError('Export page execution identity mismatch')
            end=offset+len(rows);nxt=page.get('next_offset')
            if end>total:raise RuntimeError('Export rows exceed declared full result count')
            if nxt is not None and (not isinstance(nxt,int) or isinstance(nxt,bool) or nxt!=end or nxt<=offset):
                raise RuntimeError('Result cursor creates overlap or a gap')
            if end==total:
                progress['complete']=True;progress['next_offset']=None
            elif nxt is None or not rows:
                raise RuntimeError('Missing cursor before declared export completion; keep incomplete')
            else:progress['next_offset']=nxt
            progress['observed_rows']=end;progress['successful_pages']+=1
        return progress
    def call(self,op,execution=None,payload=None,params=None):
        if execution is not None and not re.fullmatch('[A-Z0-9]{26}',execution):raise ValueError('Invalid execution ID')
        paths={'usage':'/usage','execute':'/sql/execute','status':f'/execution/{execution}/status','results':f'/execution/{execution}/results','cancel':f'/execution/{execution}/cancel'}
        if op not in paths:raise ValueError('Unsupported operation')
        if params and (op!='results' or set(params)!={'limit','offset'}):raise ValueError('Unsafe result parameters')
        if params and not (1<=params['limit']<=50 and params['offset']>=0):raise ValueError('Unsafe pagination')
        url='https://api.dune.com/api/v1'+paths[op]
        if params:url+='?'+urllib.parse.urlencode(params)
        data=json.dumps(payload or {}).encode() if op in ('usage','execute','cancel') else None
        req=urllib.request.Request(url,data=data,headers={'X-Dune-API-Key':os.environ['DUNE_API_KEY'],'Content-Type':'application/json'})
        stamp=datetime.now(timezone.utc).isoformat();t=time.monotonic();status=None;err=None;raw=b'';rid='dune_live_'+op+'_'+uuid.uuid4().hex
        try:
            with urllib.request.build_opener(NoRedirect()).open(req,timeout=30) as r:status=r.status;raw=r.read()
        except urllib.error.HTTPError as e:status=e.code;raw=e.read()
        except Exception as e:err=type(e).__name__
        if any(v.encode() in raw for k,v in os.environ.items() if any(x in k.upper() for x in ('API_KEY','TOKEN')) and len(v)>12):raw=b'';err='CREDENTIAL_ECHO_WITHHELD'
        path=self.w/'raw/dune'/f'{rid}.json';path.parent.mkdir(exist_ok=True,parents=True);path.write_bytes(raw)
        receipt={'request_id':rid,'operation':op,'execution_id':execution,'utc':stamp,'elapsed_seconds':round(time.monotonic()-t,6),'http_status':status,'error_class':err,'parameters':params,'raw_path':path.relative_to(self.w).as_posix(),'raw_bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
        dump(self.w/'logs'/f'{rid}.json',receipt)
        try:body=json.loads(raw,parse_float=Decimal)
        except Exception:body=None
        return body,receipt
    def submit(self,sqlpath,label):
        self.ensure_not_halted()
        sql=Path(sqlpath).read_text(encoding='utf-8');digest=hashlib.sha256(sql.encode()).hexdigest();job='dune_live:'+digest
        if not sql.lstrip().startswith('--') or any(x in sql.upper() for x in ('INSERT INTO','DELETE FROM','DROP TABLE')):raise ValueError('Only reviewed read SQL')
        folder=self.w/'private/dune_live_jobs'/digest
        if folder.exists():raise RuntimeError('SQL previously submitted/uncertain; do not resubmit')
        body,receipt=self.call('usage')
        if not body or receipt['http_status']!=200:raise RuntimeError('Current included allowance unavailable')
        periods=body['billing_periods'];today=datetime.now(timezone.utc).date().isoformat()
        current=next(p for p in periods if p['start_date']<=today<p['end_date'])
        remaining=Decimal(str(current['credits_included']))-Decimal(str(current['credits_used']))
        spent=Decimal(self.db.snapshot()['dune_credits']['actual'])
        self.db.confirm('dune_credits',str(remaining+spent),'Current usage '+receipt['request_id']+'; USER_CONFIRMED account cap1 and no payment method/extra credits; no evidence of key/account mismatch')
        self.db.reserve_dune_job(job,label,'1','1')
        folder.mkdir(parents=True);(folder/'query.sql').write_text(sql,encoding='utf-8')
        state={'logical_job_id':job,'query_label':label,'sql_sha256':digest,'performance':'medium','engine_evidence':'Prior project Stage0B1L2 Small NOT_AVAILABLE_WITH_SUBSCRIPTION, Medium successful; no Large','reserved_execution':'1','reserved_export':'1','combined_reserved':'2','usage_before':current,'usage_before_receipt':receipt,'state':'SUBMITTING_OR_UNCERTAIN','export_requests':0,'export_offsets':[]}
        dump(folder/'job.json',state)
        body,r=self.call('execute',payload={'sql':sql,'performance':'medium'})
        state['submit_receipt']=r;state['submit_response']=body
        if body and body.get('execution_id'):state['execution_id']=body['execution_id'];state['state']=body.get('state')
        self.observe_execution_charge(state,body,r)
        dump(folder/'job.json',state)
        print(json.dumps({'job_folder':str(folder),'http_status':r['http_status'],'body':body},default=str))
    def poll(self,folder):
        folder=Path(folder);state=read(folder/'job.json');body,r=self.call('status',state['execution_id'])
        state['latest_status_response']=body;state['latest_status_receipt']=r
        if r['http_status']==200 and isinstance(body,dict) and body.get('state'):
            state['status_response']=body;state['status_receipt']=r;state['state']=body['state']
            self.observe_execution_charge(state,body,r)
        dump(folder/'job.json',state);print(json.dumps(body,default=str))
    def export(self,folder,limit,offset):
        self.ensure_not_halted()
        if not isinstance(limit,int) or isinstance(limit,bool) or not 1<=limit<=50 or not isinstance(offset,int) or isinstance(offset,bool) or offset<0:
            raise ValueError('Invalid export limit/offset')
        folder=Path(folder);state=read(folder/'job.json');response=state.get('status_response') or {}
        self.pending_job(state)
        if response.get('state')!='QUERY_STATE_COMPLETED':raise RuntimeError('Execution incomplete')
        progress=self.export_progress(folder,state)
        if progress['complete']:raise RuntimeError('Export already complete; use cached pages')
        if offset!=progress['next_offset']:raise RuntimeError('Only the exact next export cursor is authorized')
        if progress['page_size'] is not None and limit!=progress['page_size']:raise RuntimeError('Export page size is frozen')
        cost=response.get('execution_cost_credits')
        if cost is None:raise RuntimeError('Execution cost unknown')
        if Decimal(str(cost))>1:
            self.observe_execution_charge(state,response,state.get('status_receipt') or {'request_id':'saved_status','utc':None})
            dump(folder/'job.json',state)
            raise RuntimeError('Execution cost exceeded cap; future Dune submissions/exports halted')
        md=response.get('result_metadata') or {};rows=md.get('total_row_count',md.get('row_count'));size=md.get('total_result_set_bytes',md.get('result_set_bytes'));cols=len(md.get('column_names',[]))
        if rows is None or size is None or cols==0:raise RuntimeError('Full export size unknown')
        # Conservative maximum of both currently documented billing schemes.
        export_max=max(Decimal(int(rows)*cols)/1000,Decimal(int(size))/1000000*20,Decimal(int(size))/100000)
        export_max+=Decimal('0.02')*((int(rows)+limit-1)//limit+1)
        if Decimal(str(cost))+export_max>2:raise RuntimeError('Entire paginated export cannot fit combined 2-credit job; preserve execution without export')
        state['full_export_conservative_reservation']=str(export_max);state['export_offsets'].append(offset);state['export_requests']+=1;dump(folder/'job.json',state)
        body,r=self.call('results',state['execution_id'],params={'limit':limit,'offset':offset})
        dump(folder/f'page_{offset}.json',body);dump(folder/f'page_{offset}_receipt.json',r)
        state['last_export_receipt']=r;state['last_next_offset']=body.get('next_offset') if isinstance(body,dict) else None
        try:
            state['verified_export_progress']=self.export_progress(folder,state)
            state['export_status']='COMPLETED_DECLARED_RESULT_ROWS' if state['verified_export_progress']['complete'] else 'PARTIAL_CONTIGUOUS_EXPORT'
        except RuntimeError as ex:
            state['export_status']='EXPORT_FAILED_OR_UNCERTAIN';state['export_gap']=str(ex)
        dump(folder/'job.json',state)
        print(json.dumps({'http_status':r['http_status'],'raw_bytes':r['raw_bytes'],'state':None if not body else body.get('state'),'rows':None if not body else len(body.get('result',{}).get('rows',[])),'metadata':None if not body else body.get('result',{}).get('metadata'),'next_offset':None if not body else body.get('next_offset')},default=str))
    def settle(self,folder):
        folder=Path(folder);state=read(folder/'job.json');cost=state.get('execution_cost_credits')
        self.pending_job(state)
        if cost is None:raise RuntimeError('Execution charge unknown; retain reservation')
        if state['state'] not in ('QUERY_STATE_COMPLETED','QUERY_STATE_FAILED','QUERY_STATE_CANCELLED'):raise RuntimeError('Execution not terminal')
        self.observe_execution_charge(state,{'execution_cost_credits':cost,'state':state['state']},
                                      state.get('status_receipt') or {'request_id':'saved_terminal_status','utc':None})
        if state['export_requests']==0:actual=Decimal(str(cost));basis='official execution_cost_credits; no results requested'
        else:
            body,r=self.call('usage');state['usage_after_receipt']=r
            # Account usage can be rounded, delayed, or include unrelated
            # activity. Even delta>=execution cannot certify export charges.
            # This bounded transport has no final export-charge endpoint:
            # retain the whole logical reservation and expose known execution
            # cost separately, never replace unknown export cost with zero.
            if isinstance(body,dict) and r['http_status']==200:
                prior=state['usage_before']
                after=next((p for p in body.get('billing_periods',[]) if p['start_date']==prior['start_date']),None)
                if after is not None:
                    state['usage_after']=after
                    state['account_usage_delta_observed']=str(Decimal(str(after['credits_used']))-Decimal(str(prior['credits_used'])))
            self.db.settle(state['logical_job_id'],{'dune_credits':None})
            state['settlement_status']='UNKNOWN_EXPORT_CHARGE_RESERVED'
            state['known_execution_cost_credits']=str(Decimal(str(cost)))
            state['known_export_cost_credits']=None
            state['known_total_charge_lower_bound_credits']=str(Decimal(str(cost)))
            state['combined_pending_reservation_credits']='2'
            state['cost_basis']='Official exact execution charge known; export charge finality unavailable. Rounded/delayed account delta is observational only; whole 2-credit reservation retained.'
            state.pop('settled_credits',None)
            dump(folder/'job.json',state)
            print(json.dumps({'settled':None,'settlement_status':state['settlement_status'],'known_execution_credits':str(cost),
                              'known_export_credits':None,'snapshot':self.db.snapshot()['dune_credits']}))
            return
        self.db.settle(state['logical_job_id'],{'dune_credits':str(actual)})
        state['settled_credits']=str(actual);state['settlement_status']='SETTLED_EXECUTION_ONLY';state['cost_basis']=basis;dump(folder/'job.json',state)
        print(json.dumps({'settled':str(actual),'state':state['state'],'snapshot':self.db.snapshot()['dune_credits']}))

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['submit','poll','export','settle']);p.add_argument('--sql');p.add_argument('--label');p.add_argument('--folder');p.add_argument('--limit',type=int,default=50);p.add_argument('--offset',type=int,default=0);a=p.parse_args();live=Live(Path(__file__).resolve().parents[1])
    if a.action=='submit':live.submit(a.sql,a.label)
    elif a.action=='poll':live.poll(a.folder)
    elif a.action=='export':live.export(a.folder,a.limit,a.offset)
    else:live.settle(a.folder)
if __name__=='__main__':main()
