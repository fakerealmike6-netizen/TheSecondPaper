"""Dispatch only the exact current difference after saved-data and legacy audit."""
from pathlib import Path
from collections import Counter
import sys,os,json,argparse,time
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C),str(R.parents[1]/'runtime_packages')]
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
import stage1d_bq_context_prepare as b
from stage1d_runtime import Runtime,rpc
from stage1d_context_online import _cached_rpc
from stage1d_closure_context import _missing_points,_points
from stage1d_closure_scope import active_batch

p=argparse.ArgumentParser();p.add_argument('--query',required=True);p.add_argument('--demand',required=True);p.add_argument('--revision',required=True);a=p.parse_args()
runtime=Runtime();runtime.require_gate(C)
if (C/'private/network_worker.lock').exists():raise ValueError('Another writer is active')
folder=b.inside(C,a.demand);demand=read(folder/'REQUIREMENTS.json');audit=read(folder/'LEGACY_AUDIT.json');material=read(folder/'MATERIAL.json')
q=next(q for q in active_batch(C)['queries'] if q['name']==a.query)
if q['query_id']!=demand['query_id'] or q['scope_hash']!=demand['scope_hash']:raise ValueError('Current query/window identity differs')
for ref in audit['binding_refs']:b.checked(C,ref)
if audit['source_sha256']!={p.name:sha(p) for p in (C/'src').glob('*.py')}:raise ValueError('Demand audit source gate changed')
reqs=_missing_points(demand['point_requests'],*_points(material))
cached={b.digest(p) for p,_,_ in _cached_rpc(C,reqs)}
pending=[p for p in reqs if b.digest(p) not in cached]
audit_index={b.digest({k:r['need'][k] for k in ('method','params')}):r for r in audit['rows'] if 'need' in r}
for plan in pending:
    entry=audit_index.get(b.digest(plan))
    if not entry or entry['status']=='ADMISSIBLE_POINT_PENDING_ROOT_APPLY':raise ValueError('Saved admissible evidence must be applied before new request')
    if plan['method'] not in {'eth_getBlockByNumber','eth_getTransactionByHash','eth_getTransactionReceipt','eth_getBalance'}:raise ValueError('Use the finite WETH route for other current selectors')
    runtime.validate_rpc(plan)
priority={'eth_getBlockByNumber':0,'eth_getTransactionByHash':1,'eth_getTransactionReceipt':2,'eth_getBalance':3}
pending.sort(key=lambda p:(priority[p['method']],b.digest(p)))
out=C/'private/integration_repair_point_execution'/a.query/a.revision;out.mkdir(parents=True,exist_ok=True)
binding={'demand':b.dep(C,folder/'REQUIREMENTS.json'),'audit':b.dep(C,folder/'LEGACY_AUDIT.json'),'query':a.query,'scope_hash':q['scope_hash']}
if (out/'BINDING.json').exists() and read(out/'BINDING.json')!=binding:raise ValueError('Immutable execution binding changed')
atomic_json(out/'BINDING.json',binding)
attempt=now().replace(':','').replace('+','_');rounddir=out/attempt;rounddir.mkdir()
atomic_json(rounddir/'PENDING.json',pending)
os.environ['HTTP_PROXY']=os.environ['HTTPS_PROXY']='http://127.0.0.1:7890'
print(json.dumps({'phase':'EXACT_POINT_DIFFERENCE','query':a.query,'pending':len(pending),'by_method':dict(Counter(p['method'] for p in pending))}),flush=True)
counts=Counter();actual=0;done=0;failure=None;started=time.perf_counter()
for index in range(0,len(pending),25):
    selected=pending[index:index+25]
    # Runtime applies the same persisted maximum attempts, ordinary batch<=5,
    # receipt batch=1 and existing pacing. No query or selector replacement.
    try:
        result=rpc(C,selected,a.query,'repair_exact_context_'+a.query)
        atomic_json(rounddir/(str(index)+'.json'),result)
        counts.update(m['status'] for m in result['members']);actual+=result['actual_operations_this_call'];done+=len(selected)
    except Exception as exc:
        failure={'error_class':type(exc).__name__,'reason':str(exc),'current_batch':selected,'remaining_plans':pending[index:]}
        atomic_json(rounddir/'FAILURE.json',failure)
        break
    status={'status':'EXACT_POINTS_IN_PROGRESS','query':a.query,'processed':done,'total':len(pending),'states':dict(counts),'actual_operations_this_invocation':actual,'seconds':time.perf_counter()-started}
    atomic_json(out/'STATUS.json',status);print(json.dumps(status),flush=True)
status={'status':'EXACT_POINT_BATCH_FINISHED' if not failure else 'EXACT_POINT_BATCH_BLOCKED','query':a.query,'processed':done,'total':len(pending),'states':dict(counts),'actual_operations_this_invocation':actual,'seconds':time.perf_counter()-started,'failure':failure,'attempt_path':rounddir.relative_to(C).as_posix()}
atomic_json(out/'STATUS.json',status);print(json.dumps({k:v for k,v in status.items() if k!='failure'}),flush=True)
