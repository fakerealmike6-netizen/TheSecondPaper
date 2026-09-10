"""Single-writer exact paid discovery-family binding after successful raw reuse."""
from pathlib import Path
from collections import Counter
import argparse,json,sys,os,time,uuid,sqlite3
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
from stage1d_runtime import Runtime,rpc
from stage1d_finite_state_route import cached
from stage1d_paid_candidate_domains import input_documents
from stage1d_cost_request_guard import validate_candidate_entry
from stage1d_bq_context_prepare import digest,dep,checked,inside
from prepare_current_weth import safe_point
from read_retry_r4 import logical_key

def partition_old_raw(plans,audit,validate_rpc):
 """Keep ambiguous old responses local; unrelated exact misses can proceed."""
 dispatchable=[];quarantined=[]
 for plan in plans:
  validate_rpc(plan)
  status=audit.get(digest(plan))
  if status=='NO_EXACT_LEGACY_REQUEST_MATCH':dispatchable.append(plan)
  elif status=='LEGACY_RESPONSE_VARIANTS_QUARANTINED':
   quarantined.append(dict(plan=plan,status=status,new_request_dispatched=False,
    disposition='PRESERVE_ORIGINAL_VARIANTS_FOR_SEPARATE_LOCAL_REVIEW'))
  else:raise ValueError('Current old raw needs unresolved-evidence disposition')
 return dispatchable,quarantined

p=argparse.ArgumentParser();p.add_argument('--folder',required=True);p.add_argument('--legacy-folder',required=True)
p.add_argument('--maximum-batches',type=int,default=8);p.add_argument('--execute',action='store_true');a=p.parse_args()
if not 1<=a.maximum_batches<=100:raise ValueError('Finite scheduling batches required')
runtime=Runtime();runtime.require_gate(C);safe_point(C)
if list((R/'operations').glob('*DRIVER.lock')):raise ValueError('An outer writer is active')
folder=inside(C,a.folder);metadata=read(folder/'ENTRY_RECEIPT.json')
if metadata['source_sha256']!={p.name:sha(p) for p in (C/'src').glob('*.py')}:raise ValueError('Source gate changed')
proof_path=checked(C,metadata['receipt']['proof']);proof=read(proof_path);q,col=input_documents(C,proof)
entry=next(e for e in read(R/'BOUNDARY_AND_REQUIREMENTS.json')['queries'] if e['query_name']==q['name'])
guard=validate_candidate_entry(C,q,entry)
if digest(entry)!=metadata['exact_current_entry_sha256']:raise ValueError('Discovery graph/queue advanced')
for kind,name in (('collection','collection.json'),('labels','label_snapshot.json')):
 if sha(C/'derived/stage1d/queries'/q['name']/name)!=proof['inputs'][kind]['sha256']:raise ValueError('Current graph/role advanced')
result=read(checked(C,proof['admission']))['result'];_,plans=cached(C,result['point_binding_requests'])
old=inside(C,a.legacy_folder);original=read(old/'DEMAND.json');audit={};refs=[]
view=read(folder/'LEGACY_DEMAND_VIEW.json')
if original['query_id']!=q['query_id'] or original['scope_hash']!=q['scope_hash'] or original['original_paid_admission']['sha256']!=view['report']['sha256']:
 raise ValueError('Old raw audit is not for this exact current discovery demand')
for path in sorted(old.glob('BATCH_*.json')):
 doc=read(path);refs.append(dep(C,path))
 for row in doc['results']:
  plan=row.get('plan') or {k:row['need'][k] for k in ('method','params')}
  audit[digest(plan)]=row['status']
plans,legacy_quarantined=partition_old_raw(plans,audit,runtime.validate_rpc)
ordered=[];seen=set();wanted={digest(v):v for v in plans}
for gap in result['binding_gaps']:
 for plan in gap.get('needed_rpc',[]):
  key=digest(plan)
  if key in wanted and key not in seen:ordered.append(plan);seen.add(key)
if seen!=set(wanted):raise ValueError('Current paid family binding reason absent')
blocked=[];dispatchable=[]
with sqlite3.connect((C/'private/read_retry_r4.sqlite').resolve().as_uri()+'?mode=ro',uri=True) as db:
 for plan in ordered:
  key=logical_key(runtime.rpc_identity('ALCHEMY_ETH_MAINNET_EXISTING',plan))
  row=db.execute('SELECT state FROM read_requests WHERE logical_key=?',(key,)).fetchone()
  if row and row[0] in ('PERMANENT_FAILURE','RETRIES_EXHAUSTED'):
   blocked.append(dict(plan=plan,logical_key=key,state=row[0],new_attempts_granted=False))
  else:dispatchable.append(plan)
ordered=dispatchable
review=dict(query=q['name'],scope_hash=q['scope_hash'],discovery_proof=dep(C,proof_path),admission=proof['admission'],
 exact_current_guard=guard,legacy_demand=dep(C,old/'DEMAND.json'),legacy_batches=refs,
 missing_point_plans=ordered,original_terminal_keys_retained_as_gaps=blocked,
 old_response_variants_retained_as_gaps=legacy_quarantined,
 source_script_sha256=sha(Path(__file__)),
 provider_comparison={'saved_sources':'Exact successes and whitelisted raw subtracted first.',
 'BigQuery':'Original full SQL domains/page families retained. Eligible top and fee operands reused; no new SQL.',
 'Alchemy':'Only current required physical family bindings, full headers where needed; existing pacing and original attempts.',
 'Dune':'No repeated ordinary-ledger scan; compact evidence role remains.', 'Etherscan':'Nonblocking backup not needed for this bounded selector difference.'},
 additional_budget=False,counters_reset=False,full_context_claimed=False,created_at_utc=now())
out=C/'private/integration_paid_candidate_binding_execution'/q['name']/uuid.uuid4().hex;out.mkdir(parents=True)
atomic_json(out/'ROUTE_REVIEW.json',review)
print(json.dumps(dict(status='EXACT_CANDIDATE_REMAINDER_READY',query=q['name'],points=len(ordered),
 by_method=dict(Counter(v['method'] for v in ordered)),terminal_key_gaps=len(blocked),
 legacy_variant_gaps=len(legacy_quarantined),review=dep(C,out/'ROUTE_REVIEW.json'),new_requests=0)),flush=True)
if not a.execute:sys.exit(0)
if read(R/'REPAIR_CONTROL.json').get('research_network_allowed') is not True:raise ValueError('Research control is closed')
lock=R/'operations/CURRENT_PAID_CANDIDATE_BINDING_DRIVER.lock';token=dict(pid=os.getpid(),id=out.name)
with lock.open('x',encoding='utf8') as f:json.dump(token,f)
for k in ('HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy'):os.environ[k]='http://127.0.0.1:7890'
counts=Counter();operations=0;processed=0;start=time.perf_counter();status='SCHEDULING_LIMIT_SAFE_POINT'
try:
 for offset in range(0,min(len(ordered),a.maximum_batches*25),25):
  if read(R/'REPAIR_CONTROL.json').get('research_network_allowed') is not True:status='ROOT_CONTROL_CLOSED_AT_SAFE_POINT';break
  selected=ordered[offset:offset+25]
  r=rpc(C,selected,q['name'],'current_paid_discovery_family_exact_binding')
  atomic_json(out/('BATCH_'+str(offset).zfill(5)+'.json'),r)
  counts.update(v['status'] for v in r['members']);operations+=r['actual_operations_this_call'];processed+=len(selected)
  progress=dict(status='EXACT_PAID_CANDIDATE_BINDING_IN_PROGRESS',query=q['name'],processed=processed,total=len(ordered),
   states=dict(counts),actual_operations=operations,seconds=time.perf_counter()-start)
  atomic_json(out/'STATUS.json',progress);print(json.dumps(progress),flush=True)
  if r.get('status')!='COMPLETE':status='PARTIAL_SAME_KEYS_PRESERVED';break
 else:
  if processed==len(ordered):status='EXACT_REMAINDER_FINISHED'
except Exception as exc:
 status='BLOCKED_SAME_KEYS_PRESERVED';atomic_json(out/'FAILURE.json',dict(error_class=type(exc).__name__,reason=str(exc)));raise
finally:
 final=dict(status=status,query=q['name'],processed=processed,total=len(ordered),states=dict(counts),actual_operations=operations,
  seconds=time.perf_counter()-start,remaining_not_dispatched=len(ordered)-processed,
  legacy_variant_gaps=len(legacy_quarantined),original_terminal_key_gaps=len(blocked),
  completion_basis='SELECTED_UNAMBIGUOUS_MISSING_BINDINGS_ONLY',
  current_discovery_not_yet_replayed=True,route_review=dep(C,out/'ROUTE_REVIEW.json'))
 atomic_json(out/'STATUS.json',final)
 if read(lock)!=token:raise ValueError('Root guard changed')
 lock.unlink();print(json.dumps(final),flush=True)
