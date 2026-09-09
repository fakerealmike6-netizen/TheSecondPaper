"""Allowlisted transports; credentials used only in memory and never logged."""
import os,json,hashlib,datetime,urllib.request,urllib.parse,urllib.error,time,uuid
from pathlib import Path
from budget import Ledger
from legacy_guard_r4 import reject_legacy_workspace

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs): return None

class Network:
    def __init__(self, work):
        reject_legacy_workspace(work, 'network.Network')
        self.work=Path(work); self.ledger=Ledger(self.work/'private/shared_budget.sqlite'); self.last=0
    def call(self,provider,operation,params=None,payload=None,amounts=None,metadata=False):
        reject_legacy_workspace(self.work, 'network.Network.call')
        params=params or {}; payload=payload or {}; job=provider+'_'+operation+'_'+uuid.uuid4().hex
        if provider=='etherscan':
            if any(x in params for x in ('apikey','api_key')): raise ValueError('Pass no credentials in params')
            if not os.environ.get('ETHERSCAN_API_KEY'): raise RuntimeError('Credential absent')
            url='https://api.etherscan.io/v2/api?'+urllib.parse.urlencode({**params,'apikey':os.environ['ETHERSCAN_API_KEY']})
            req=urllib.request.Request(url,headers={'Accept':'application/json'})
        elif provider=='dune' and operation=='usage':
            req=urllib.request.Request('https://api.dune.com/api/v1/usage',data=b'{}',method='POST',headers={'Content-Type':'application/json','X-Dune-API-Key':os.environ['DUNE_API_KEY']})
        elif provider=='metasleuth' and operation=='batch_labels':
            req=urllib.request.Request('https://aml.blocksec.com/address-label/api/v3/batch-labels',data=json.dumps(payload).encode(),method='POST',headers={'Content-Type':'application/json','API-KEY':os.environ['METASLEUTH_API_KEY']})
        elif provider=='publicnode':
            req=urllib.request.Request('https://ethereum-rpc.publicnode.com',data=json.dumps(payload).encode(),method='POST',headers={'Content-Type':'application/json'})
        else: raise ValueError('Provider/operation not allowlisted')
        if not metadata: self.ledger.reserve(job,provider,operation,amounts or {'rpc_operations':1})
        time.sleep(max(0,0.36-(time.monotonic()-self.last)))
        start=datetime.datetime.now(datetime.timezone.utc).isoformat(); status=None; error=None; raw=b''
        try:
            with urllib.request.build_opener(NoRedirect()).open(req,timeout=30) as r: status=r.status; raw=r.read()
        except urllib.error.HTTPError as ex: status=ex.code; raw=ex.read()
        except Exception as ex: error=type(ex).__name__
        self.last=time.monotonic()
        # Refuse to persist an unexpected response echoing authentication material.
        if any(os.environ.get(k) and os.environ[k].encode() in raw for k in ('ETHERSCAN_API_KEY','DUNE_API_KEY','METASLEUTH_API_KEY','ALCHEMY_API_KEY')):
            raw=b''; error='RESPONSE_WITHHELD_CREDENTIAL_ECHO'
        dest=self.work/'raw'/provider; dest.mkdir(parents=True,exist_ok=True)
        rp=dest/(job+'.json'); rp.write_bytes(raw)
        try: body=json.loads(raw)
        except Exception: body=None
        evidence={'request_id':job,'provider':provider,'operation':operation,'utc':start,'http_status':status,'error_class':error,'params':params,'payload':payload,'raw_path':str(rp.relative_to(self.work)),'raw_bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'metadata':metadata}
        (self.work/'logs'/('request_'+job+'.json')).write_text(json.dumps(evidence,indent=2),encoding='utf-8')
        if not metadata:
            actual=amounts or {'rpc_operations':1}
            if provider=='metasleuth' and (body is None or body.get('code')!=200000):
                self.ledger.settle(job,{u:None for u in actual})
            else: self.ledger.settle(job,actual)
        return body,evidence

def preflight(work):
    reject_legacy_workspace(work, 'network.preflight')
    net=Network(work); p=Path(work)/'private/provider_preflight.json'
    if p.exists(): return json.loads(p.read_text())
    out={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'credential_presence':{k:bool(os.environ.get(k)) for k in ('DUNE_API_KEY','ETHERSCAN_API_KEY','METASLEUTH_API_KEY','ALCHEMY_API_KEY')},'dune_sql_enabled':False,'bigquery_enabled':False,'alchemy_enabled':False}
    if os.environ.get('DUNE_API_KEY'):
        body,e=net.call('dune','usage',metadata=True)
        out['dune']={'evidence':e,'billing_periods':None if not body else body.get('billing_periods',body.get('billingPeriods')),'enforceable_job_cap':None,'paid_overage_disabled':None,'status':'BLOCKED_EXECUTION_CAP_AND_OVERAGE_UNCONFIRMED'}
    if os.environ.get('ETHERSCAN_API_KEY'):
        body,e=net.call('etherscan','getapilimit',{'module':'getapilimit','action':'getapilimit'},metadata=True)
        out['etherscan']={'evidence':e,'usage':body,'status':'ALLOWANCE_UNCONFIRMED'}
        if body and str(body.get('status'))=='1' and isinstance(body.get('result'),dict):
            available=int(body['result']['creditsAvailable']); net.ledger.confirm('rpc_operations',max(0,available-1),'Etherscan current getapilimit; reserve one metadata operation; no additional paid overage enabled by this task')
            out['etherscan']['status']='CURRENT_ALLOWANCE_CONFIRMED'
    p.write_text(json.dumps(out,indent=2),encoding='utf-8'); return out

if __name__=='__main__':
    import sys
    out=preflight(Path(sys.argv[1]))
    print(json.dumps(out,indent=2))
