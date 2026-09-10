"""Root-only exact paid-source binding difference, after the current source union.

Use the existing RPC pacing, attempts, clocks and pool. No new bulk ledger SQL.
The outer scheduling count can be resumed against the same immutable plan.
"""
from pathlib import Path
from collections import Counter
import argparse,json,sys,os,time,uuid,sqlite3
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
from stage1d_runtime import Runtime,rpc
from stage1d_finite_state_route import cached
from stage1d_closure_scope import active_batch
from stage1d_closure_context import _missing_points,_points
from stage1d_bq_context_prepare import digest,dep,checked,inside
from prepare_current_weth import safe_point
from read_retry_r4 import logical_key

def main():
 p=argparse.ArgumentParser();p.add_argument('--union',required=True);p.add_argument('--legacy-folder',required=True)
 p.add_argument('--maximum-batches',type=int,default=8);p.add_argument('--execute',action='store_true')
 a=p.parse_args()
 if not 1<=a.maximum_batches<=100:raise ValueError('Finite scheduling batches required')
 runtime=Runtime();runtime.require_gate(C);safe_point(C)
 folder=inside(C,a.union);receipt=read(folder/'RECEIPT.json');qname=receipt['query']
 q=next(q for q in active_batch(C)['queries'] if q['name']==qname)
 if receipt['source_sha256']!={p.name:sha(p) for p in (C/'src').glob('*.py')}:raise ValueError('Source gate changed')
 for name in ('collection.json','label_snapshot.json'):
  if sha(folder/name)!=sha(C/'derived/stage1d/queries'/qname/name):raise ValueError('Current graph/role advanced; recompute its union')
 gaps=read(checked(C,receipt['gaps']));mat=read(checked(C,receipt['material']))
 plans=_missing_points(gaps['bq_binding_points'],*_points(mat))
 _,plans=cached(C,plans)
 old=inside(C,a.legacy_folder);original=read(old/'DEMAND.json');audit={};refs=[]
 if original['query_id']!=q['query_id'] or original['scope_hash']!=q['scope_hash']:raise ValueError('Old raw demand scope differs')
 for path in sorted(old.glob('BATCH_*.json')):
  doc=read(path);refs.append(dep(C,path))
  for row in doc['results']:
   plan=row.get('plan') or {k:row['need'][k] for k in ('method','params')}
   audit[digest(plan)]=row['status']
 for plan in plans:
  runtime.validate_rpc(plan)
  if audit.get(digest(plan))!='NO_EXACT_LEGACY_REQUEST_MATCH':
   raise ValueError('Current exact old raw needs adoption or separate unresolved-evidence disposition')
 # Complete a family's binding before moving to another family. Shared header
 # selectors stay unique; no wholesale header prefetch for later unrelated work.
 paid=read(folder/'PAID_ADMISSION.json');ordered=[];seen=set();wanted={digest(v):v for v in plans}
 for gap in paid['binding_gaps']:
  for plan in gap.get('needed_rpc',[]):
   key=digest(plan)
   if key in wanted and key not in seen:ordered.append(plan);seen.add(key)
 if seen!=set(wanted):raise ValueError('Union remainder not represented by current paid binding reasons')
 blocked=[];dispatchable=[]
 with sqlite3.connect((C/'private/read_retry_r4.sqlite').resolve().as_uri()+'?mode=ro',uri=True) as db:
  for plan in ordered:
   key=logical_key(runtime.rpc_identity('ALCHEMY_ETH_MAINNET_EXISTING',plan))
   row=db.execute('SELECT state FROM read_requests WHERE logical_key=?',(key,)).fetchone()
   if row and row[0] in ('PERMANENT_FAILURE','RETRIES_EXHAUSTED'):
    blocked.append(dict(plan=plan,logical_key=key,state=row[0],new_attempts_granted=False))
   else:dispatchable.append(plan)
 ordered=dispatchable
 proof=dict(query=qname,scope_hash=q['scope_hash'],source_union=dep(C,folder/'RECEIPT.json'),
  original_paid_admission=dep(C,folder/'PAID_ADMISSION.json'),legacy_demand=dep(C,old/'DEMAND.json'),legacy_batches=refs,
  missing_point_plans=ordered,original_terminal_keys_retained_as_gaps=blocked,provider_comparison={
   'saved_sources':'Current Dune/context rounds, all named paid BQ families, RPC successes, and exact whitelisted old raw were merged first.',
   'BigQuery':'Existing complete paid page families retained. Eligible ordinary top/fee operands use their verified BQ fields; the same full-root checks retain a separate exact full header and reject contradictory RPC counterevidence. Unsupported/missing ordinary fields keep explicit point gaps. No repeat paid SQL.',
   'Dune':'Existing coverage and physical facts are in the source union. No new bulk ordinary-ledger scan routed here.',
   'Alchemy':'Only the exact remaining physical binding selectors through the existing controller; not a per-index-row default.',
   'Etherscan':'Nonblocking backup remains unnecessary unless the current route actually blocks.'},
  additional_budget=False,counters_reset=False,full_context_claimed=False,created_at_utc=now())
 out=C/'private/integration_paid_binding_execution'/qname/uuid.uuid4().hex;out.mkdir(parents=True)
 atomic_json(out/'ROUTE_REVIEW.json',proof)
 print(json.dumps(dict(status='EXACT_REMAINDER_READY',query=qname,points=len(ordered),
  by_method=dict(Counter(v['method'] for v in ordered)),terminal_key_gaps=len(blocked),review=dep(C,out/'ROUTE_REVIEW.json'),new_requests=0)),flush=True)
 if not a.execute:return
 if read(R/'REPAIR_CONTROL.json').get('research_network_allowed') is not True:raise ValueError('Research control is closed')
 guard=R/'operations/CURRENT_PAID_BINDING_RPC_DRIVER.lock';token=dict(pid=os.getpid(),id=out.name)
 with guard.open('x',encoding='utf8') as f:json.dump(token,f)
 for k in ('HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy'):os.environ[k]='http://127.0.0.1:7890'
 counts=Counter();operations=0;processed=0;start=time.perf_counter();status='SCHEDULING_LIMIT_SAFE_POINT'
 try:
  for offset in range(0,min(len(ordered),a.maximum_batches*25),25):
   if read(R/'REPAIR_CONTROL.json').get('research_network_allowed') is not True:
    status='ROOT_CONTROL_CLOSED_AT_SAFE_POINT';break
   selected=ordered[offset:offset+25]
   result=rpc(C,selected,qname,'current_paid_family_exact_binding_remainder')
   atomic_json(out/('BATCH_'+str(offset).zfill(5)+'.json'),result)
   counts.update(v['status'] for v in result['members']);operations+=result['actual_operations_this_call'];processed+=len(selected)
   progress=dict(status='EXACT_PAID_BINDING_IN_PROGRESS',query=qname,processed=processed,total=len(ordered),
    states=dict(counts),actual_operations=operations,seconds=time.perf_counter()-start)
   atomic_json(out/'STATUS.json',progress);print(json.dumps(progress),flush=True)
   if result.get('status')!='COMPLETE':status='PARTIAL_SAME_KEYS_PRESERVED';break
  else:
   if processed==len(ordered):status='EXACT_REMAINDER_FINISHED'
 except Exception as exc:
  status='BLOCKED_SAME_KEYS_PRESERVED';atomic_json(out/'FAILURE.json',dict(error_class=type(exc).__name__,reason=str(exc)))
  raise
 finally:
  final=dict(status=status,query=qname,processed=processed,total=len(ordered),states=dict(counts),
   actual_operations=operations,seconds=time.perf_counter()-start,remaining_not_dispatched=len(ordered)-processed,
   current_context_not_yet_reassembled=True,route_review=dep(C,out/'ROUTE_REVIEW.json'))
  atomic_json(out/'STATUS.json',final)
  if read(guard)!=token:raise ValueError('Root writer guard changed')
  guard.unlink();print(json.dumps(final),flush=True)

if __name__=='__main__':main()
