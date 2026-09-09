"""Finite public protocol documentation evidence; no account or credential read."""
from pathlib import Path
import sys,json,hashlib,urllib.request,urllib.error,time,ssl
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path.insert(0,str(C/'src'))
from stage1d_runtime import Runtime
from page_attempts import atomic_json
from context_access_r3 import read,sha,now
from network import NoRedirect
sources={
 'dln_deployment':'https://docs.debridge.com/dln-details/overview/deployed-contracts',
 'dln_source':'https://raw.githubusercontent.com/debridge-finance/dln-contracts/main/contracts/DLN/DlnSource.sol',
 'dln_order_lib':'https://raw.githubusercontent.com/debridge-finance/dln-contracts/main/contracts/libraries/DlnOrderLib.sol',
}
runtime=Runtime();runtime.require_gate(C);out=[]
for name,url in sources.items():
 root=C/'raw/stage1d_public_protocol_sources'/name;root.mkdir(parents=True,exist_ok=True)
 journal=root/'REQUEST_JOURNAL.json';j=read(journal) if journal.exists() else {'url':url,'attempts':[],'new_budget_pool':False,'research_rpc_or_sql_request':False}
 if j['url']!=url:raise ValueError('Public source identity changed')
 if j.get('status')=='SUCCESS':
  if sha(root/j['body_path'])!=j['body_sha256']:raise ValueError('Successful source body changed')
  out.append({'name':name,'status':'CACHE_HIT','journal':journal.relative_to(C).as_posix()});continue
 if j.get('status')=='PERMANENT_FAILURE' or len(j['attempts'])>=3:
  out.append({'name':name,'status':j['status']});continue
 with runtime.session(C,'txphish_src001','public_role_source_'+name):
  while len(j['attempts'])<3:
   n=len(j['attempts'])+1;ident='public_role_'+hashlib.sha256(url.encode()).hexdigest()+'_'+str(n)
   runtime.reserve_raw(C,ident,4*1024*1024+65536)
   attempt={'attempt':n,'request_url':url,'started_at_utc':now(),'transport_called':False}
   j['attempts'].append(attempt);atomic_json(journal,j)
   try:
    opener=urllib.request.build_opener(NoRedirect)
    request=urllib.request.Request(url,headers={'Accept':'text/html,text/plain,application/json','User-Agent':'Stage1D-Protocol-Evidence/1.0'})
    attempt['transport_called']=True;atomic_json(journal,j)
    with opener.open(request,timeout=40) as response:
     data=response.read(4*1024*1024+1);attempt['http_status']=response.status
     attempt['content_type']=response.headers.get('Content-Type');attempt['final_url']=response.geturl()
    if len(data)>4*1024*1024:raise ValueError('Public document exceeds frozen response bound')
    if attempt['http_status']!=200 or attempt['final_url']!=url:raise ValueError('Unbound public-source response')
    body=root/('body_'+str(n)+'.txt');body.write_bytes(data)
    attempt.update(status='SUCCESS',body_path=body.name,body_sha256=sha(body),bytes=len(data))
    j.update(status='SUCCESS',body_path=body.name,body_sha256=sha(body),source_url=url,source_kind='PUBLIC_PROTOCOL_DOCUMENT')
   except urllib.error.HTTPError as exc:
    attempt.update(status='PERMANENT_FAILURE' if exc.code in (400,401,403,404) else 'TEMPORARY_FAILURE',http_status=exc.code,error_class='HTTP_'+str(exc.code))
    j['status']=attempt['status']
   except (urllib.error.URLError,TimeoutError,ConnectionError,ssl.SSLError) as exc:
    attempt.update(status='TEMPORARY_FAILURE',error_class=type(exc).__name__);j['status']='TEMPORARY_FAILURE'
   except Exception as exc:
    attempt.update(status='PERMANENT_FAILURE',error_class=type(exc).__name__);j['status']='PERMANENT_FAILURE'
   attempt['ended_at_utc']=now();atomic_json(journal,j)
   receipt=root/('receipt_'+str(n)+'.json');atomic_json(receipt,attempt);runtime.close_raw(C,ident,receipt)
   if j['status'] in ('SUCCESS','PERMANENT_FAILURE'):break
   if n<3:time.sleep(2 if n==1 else 5)
 out.append({'name':name,'status':j['status'],'journal':journal.relative_to(C).as_posix(),'attempts':len(j['attempts'])})
 print(json.dumps(out[-1]),flush=True)
atomic_json(R/'PUBLIC_ROLE_SOURCE_FETCH.json',out)
