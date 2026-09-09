"""Finite historical point/log requirements through the continued RPC pool.

Plans are frozen against a current query and actual requirement evidence before
dispatch. Cached successful selectors are independently checked and subtracted.
This module never makes an unbound log into coverage or silently caps a need.
"""
from pathlib import Path
from contextlib import closing
import hashlib,json,sqlite3
from stage1d_finite_state_rpc import FiniteStateRuntime,METHOD_RATES,key,validate
from stage1d_closure_scope import active_batch
from context_access_r4 import RpcAccess
from read_retry_r4 import logical_key
from page_attempts import atomic_json

SCHEMA='stage1d-finite-state-requirement-v1'
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def read(path):return json.loads(Path(path).read_text(encoding='utf8'))
def inside(work,relative):
 if not isinstance(relative,str) or ':' in relative or '\\' in relative or relative.startswith('/') or any(v in ('','..','.') for v in relative.split('/')):raise ValueError('Safe work-relative path required')
 p=(work/relative).resolve()
 if not p.is_relative_to(work) or not p.is_file() or p.is_symlink():raise ValueError('Requirement evidence missing/outside work')
 return p
def validate_plan_list(plans):
 if not isinstance(plans,list) or not plans:raise ValueError('Nonempty finite requirement required')
 runtime=FiniteStateRuntime([p for p in plans if p.get('method') in METHOD_RATES])
 checked=[runtime.validate_rpc(p) for p in plans]
 if len({key(p) for p in checked})!=len(checked):raise ValueError('Duplicate selector is not a new requirement')
 return checked
def prepare(work,query,plans,*,purpose,requirement_evidence,role_snapshot,missing_fields):
 work=Path(work).resolve();queries=active_batch(work)['queries']
 if query not in queries:raise ValueError('Exact active query required')
 if purpose not in ('HISTORICAL_TECHNICAL_ROLE','CANONICAL_WETH_INSTANCE','CANONICAL_WETH_DISCOVERY','NECESSARY_ASSET_LEDGER'):raise ValueError('Finite current closure purpose required')
 checked=validate_plan_list(plans);dependencies=[]
 for ref in requirement_evidence+[role_snapshot]:
  p=inside(work,ref['path'])
  if sha(p)!=ref['sha256']:raise ValueError('Requirement source changed')
  saved=work/'private/stage1d_finite_requirements/evidence'/(ref['sha256']+p.suffix)
  if saved.exists() and sha(saved)!=ref['sha256']:raise ValueError('Immutable requirement conflict')
  if not saved.exists():saved.parent.mkdir(parents=True,exist_ok=True);saved.write_bytes(p.read_bytes())
  if sha(p)!=ref['sha256'] or sha(saved)!=ref['sha256']:raise ValueError('Requirement changed during snapshot')
  dependencies.append({'path':saved.relative_to(work).as_posix(),'sha256':ref['sha256'],'original_path':ref['path']})
 record={'schema_version':SCHEMA,'authorization_id':'STAGE1D_WINDOW_ROLE_SEMANTIC_CLOSURE_V1',
  'query_name':query['name'],'query_id':query['query_id'],'scope_id':query['scope_id'],'scope_hash':query['scope_hash'],
  'purpose':purpose,'plans':checked,'dependencies':dependencies,'role_snapshot_sha256':role_snapshot['sha256'],
  'missing_fields':list(missing_fields),'route':'ALCHEMY_EXISTING_FINITE_STATE','additional_budget':False}
 digest=hashlib.sha256(key(record).encode()).hexdigest();path=work/'private/stage1d_finite_requirements'/(digest+'.json')
 if path.exists() and read(path)!=record:raise ValueError('Immutable finite requirement conflict')
 if not path.exists():atomic_json(path,record)
 return path
def verify(work,path):
 work=Path(work).resolve();path=Path(path).resolve()
 if not path.is_relative_to(work):raise ValueError('Finite requirement outside work')
 r=read(path)
 if r.get('schema_version')!=SCHEMA or r.get('authorization_id')!='STAGE1D_WINDOW_ROLE_SEMANTIC_CLOSURE_V1' or r.get('additional_budget') is not False:raise ValueError('Current continued authority required')
 if path.stem!=hashlib.sha256(key(r).encode()).hexdigest():raise ValueError('Immutable requirement identity changed')
 queries=[q for q in active_batch(work)['queries'] if q['name']==r['query_name']]
 if len(queries)!=1 or any(queries[0][k]!=r[k] for k in ('query_id','scope_id','scope_hash')):raise ValueError('Finite selectors claim old/different scope')
 for dep in r['dependencies']:
  if sha(inside(work,dep['path']))!=dep['sha256']:raise ValueError('Finite requirement source changed')
 validate_plan_list(r['plans']);return r
def cached(work,plans):
 work=Path(work).resolve();runtime=FiniteStateRuntime([p for p in plans if p.get('method') in METHOD_RATES])
 result=[];missing=[];dbpath=work/'private/read_retry_r4.sqlite'
 if not dbpath.exists():return [],list(plans)
 with closing(sqlite3.connect(dbpath.as_uri()+'?mode=ro',uri=True)) as db:
  for plan in plans:
   ident=runtime.rpc_identity('ALCHEMY_ETH_MAINNET_EXISTING',plan)
   row=db.execute("SELECT success_payload,success_receipt FROM read_requests WHERE logical_key=? AND state='SUCCESS'",(logical_key(ident),)).fetchone()
   if row is None:missing.append(plan);continue
   value,receipt=map(json.loads,row);path=inside(work,receipt['artifact_path'])
   if sha(path)!=receipt['artifact_sha256']:raise ValueError('Cached successful RPC artifact changed')
   env=read(path);request=env.get('request',{});response=env.get('response',{})
   if {k:request.get(k) for k in ('method','params')}!=plan or runtime.rpc_result_status(request,response)!='SUCCESS_VALIDATED' or response.get('result')!=value:raise ValueError('Cached RPC selector/binding changed')
   result.append({'plan':plan,'result':value,'artifact_path':receipt['artifact_path'],'artifact_sha256':receipt['artifact_sha256'],'cache_hit':True})
 return result,missing
def execute(work,path,*,allow_reviewed_point_remainder=False,batch_route_review=None):
 work=Path(work).resolve();r=verify(work,path);success,missing=cached(work,r['plans'])
 out={'schema_version':'stage1d-finite-state-outcome-v1','requirement_id':Path(path).stem,
  'requirement_path':Path(path).relative_to(work).as_posix(),'requirement_sha256':sha(path),
  'query_name':r['query_name'],'purpose':r['purpose'],'cached_success_count':len(success),
  'missing_selector_count':len(missing),'missing_selectors':missing,'actual_operations_this_call':0,
  'results':success,'batches':[]}
 if len(missing)>200 and not allow_reviewed_point_remainder:
  out['status']='BATCH_BINDING_REQUIRED';return out
 if allow_reviewed_point_remainder:
  if not batch_route_review or sha(inside(work,batch_route_review['path']))!=batch_route_review['sha256']:raise ValueError('Actual immutable batch-route review required')
  review=read(inside(work,batch_route_review['path']))
  if review.get('requirement_id')!=Path(path).stem or review.get('remaining_point_plans')!=missing or not review.get('provider_comparison'):raise ValueError('Batch review must bind exact remaining selectors and provider comparison')
  out['batch_route_review']=batch_route_review
 runtime=FiniteStateRuntime([p for p in r['plans'] if p.get('method') in METHOD_RATES])
 runtime.require_gate(work)
 rates=work/'private/stage1d_authority/ALCHEMY_FINITE_STATE_RATE_EVIDENCE.json'
 if any(p['method'] in METHOD_RATES for p in missing):
  evidence=read(rates)
  if evidence.get('rates',evidence.get('method_cu_upper_bounds'))!=METHOD_RATES:raise ValueError('Saved exact finite-state rate evidence required')
  out['finite_state_rate_evidence']={'path':rates.relative_to(work).as_posix(),'sha256':sha(rates)}
 access=RpcAccess(work,runtime=runtime)
 for start in range(0,len(missing),50):
  group=missing[start:start+50];res=access.call_batch(group,r['query_name'],r['purpose'].lower())
  out['actual_operations_this_call']+=res['actual_operations_this_call']
  out['batches'].append({k:v for k,v in res.items() if k!='snapshot'})
  for plan,member in zip(group,res['members']):
   if member['status']=='SUCCESS_VALIDATED':out['results'].append({**member,'plan':plan})
  # Keep all original selectors and durable failures; one finite invocation
  # does not bypass the normal per-key retry controller or invent a new alias.
  by_plan={key(v['plan']):v for v in out['results']}
  out['results']=[by_plan[key(p)] for p in r['plans'] if key(p) in by_plan]
  out['status']='COMPLETE' if len(out['results'])==len(r['plans']) else 'PARTIAL'
  atomic_json(work/'derived/stage1d/finite_state'/(Path(path).stem+'.json'),out)
 by_plan={key(v['plan']):v for v in out['results']}
 out['results']=[by_plan[key(p)] for p in r['plans'] if key(p) in by_plan]
 out['status']='COMPLETE' if len(out['results'])==len(r['plans']) else 'PARTIAL'
 atomic_json(work/'derived/stage1d/finite_state'/(Path(path).stem+'.json'),out)
 return out
