"""ROOT-only bounded evidence reads; use the existing ledger/runtime/transport.

No discovery, role adoption, collector replay, retry reset or new provider.
"""
from pathlib import Path
from dataclasses import asdict
import argparse, hashlib, json, os, sys, uuid

R=Path(__file__).resolve().parents[1];C=R/'code'
sys.path[:0]=[str(C/'src'),str(C),str(R.parents[1]/'runtime_packages')]
AUTH='STAGE1D_UNKNOWN_CONTRACT_OR_RATE20_BOUNDARY_V1'
SNAP_SHA='d577d3cf9bce7fc5e780a31c12de2de60c6eb6010a767e0440d3e41c004feea1'
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
from stage1d_runtime import Runtime
from stage1d_closure_scope import active_batch
from stage1d_acquisition import Labels,acquire_labels
from collector import Event,State
from read_retry_r4 import logical_key

def inside(root,rel):
    p=(root/rel).resolve()
    if not p.is_relative_to(root.resolve()):raise ValueError('Path escapes current revision')
    return p

def validate_preparation(prep,expected,runtime):
    path=inside(R,prep)
    if sha(path)!=expected:raise ValueError('Exact prepared evidence demand changed')
    doc=read(path)
    if doc['authorization_id']!=AUTH or doc['initial_snapshot_ref']['sha256']!=SNAP_SHA:
        raise ValueError('Different authority or initial screen')
    for rel,h in doc['source_and_label_sha256'].items():
        if sha(inside(R,rel))!=h:raise ValueError('Current source/role/label changed; prepare a new evidence difference')
    rows=doc['next_batch']['requests']
    if not 1<=len(rows)<=100:raise ValueError('Finite prepared batch required')
    seen=set()
    for row in rows:
        p=runtime.validate_rpc(row['request'])
        if p['method'] not in ('eth_getCode','eth_getBlockByNumber'):
            raise ValueError('Historical code or missing exact header only')
        key=logical_key(runtime.rpc_identity('ALCHEMY_ETH_MAINNET_EXISTING',p))
        if key!=row['logical_key'] or key in seen:raise ValueError('Exact shared request identity differs')
        seen.add(key)
        if not row['query_names'] or set(row['query_names'])-{'txphish_src001','txphish_src002','xscam_src001','lifi_src001'}:
            raise ValueError('Unknown clock owner')
    return path,doc,rows

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=('code','labels'))
    parser.add_argument('--preparation');parser.add_argument('--sha256')
    parser.add_argument('--query',choices=('txphish_src001','txphish_src002','xscam_src001'))
    parser.add_argument('--maximum-batches',type=int,default=1)
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args()
    if not args.execute:
        print(json.dumps({'mode':args.mode,'execution':False,'authority':AUTH}));return
    runtime=Runtime();runtime.require_gate(C);runtime.clock_used(C)
    if (C/'private/network_worker.lock').exists():raise RuntimeError('Wait for existing normal writer')
    snapshot=R/'reports/unknown_cost_boundary_v1/inputs/INITIAL_SNAPSHOT.json'
    if sha(snapshot)!=SNAP_SHA:raise ValueError('Initial screen changed')
    doc=read(snapshot)
    guard=R/'operations/CURRENT_COST_EVIDENCE_DRIVER.lock'
    token={'pid':os.getpid(),'id':uuid.uuid4().hex,'authority':AUTH}
    with guard.open('x',encoding='utf8') as f:json.dump(token,f)
    out=R/'operations'/('unknown_cost_'+args.mode+'_'+token['id']+'.json')
    record={'authorization_id':AUTH,'started_at_utc':now(),'mode':args.mode,
        'initial_snapshot':{'path':snapshot.relative_to(R).as_posix(),'sha256':SNAP_SHA},
        'script_sha256':sha(Path(__file__)),'budget_reset':False,'retry_reset':False,'results':[]}
    atomic_json(out,record)
    try:
        if args.mode=='code':
            if not args.preparation or not args.sha256:raise ValueError('Exact preparation SHA required')
            p,prepared,rows=validate_preparation(args.preparation,args.sha256,runtime)
            record['preparation']={'path':p.relative_to(R).as_posix(),'sha256':args.sha256}
            from context_access_r4 import RpcAccess
            for key in ('HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy'):os.environ[key]='http://127.0.0.1:7890'
            access=RpcAccess(C,runtime=runtime)
            cursor=0
            while cursor<len(rows):
                owners=rows[cursor]['query_names'];probe=owners[0] if len(owners)==1 else 'SHARED'
                batch=[]
                while cursor<len(rows) and len(batch)<5:
                    item=rows[cursor];owner=item['query_names'][0] if len(item['query_names'])==1 else 'SHARED'
                    if owner!=probe:break
                    batch.append(item);cursor+=1
                result=access.call_batch([r['request'] for r in batch],probe,'unknown_cost_historical_code_or_header',capability=False)
                record['results'].append({'needs':batch,'query_clock':probe,'result':result});atomic_json(out,record)
                print(json.dumps({'members':cursor,'planned':len(rows),'status':result['status'],
                    'actual_operations_this_call':result.get('actual_operations_this_call')}),flush=True)
                if result['status']!='COMPLETE':break
        else:
            if not args.query or not 1<=args.maximum_batches<=3:raise ValueError('Finite query/label batch required')
            query=next(q for q in active_batch(C)['queries'] if q['name']==args.query)
            sources=[C/'STAGE1D_PREFLIGHT_GATE.json',C/'private/STAGE1D_CLOSURE_BATCH.json',
                C/'src/stage1d_acquisition.py',C/'src/stage1d_role_adoption.py',C/'src/stage1d_task_boundaries.py',
                C/'private/stage1d_roles/CURRENT.json',C/'private/stage1d_roles/AUTHORITIES.json',
                C/'private/stage1d_roles/USER_TASK_BOUNDARIES.json',
                C/'private/stage1d_inputs/address_registry.csv.gz',C/'private/stage1d_inputs/HISTORICAL_LABEL_SUCCESS.json']
            sources+=sorted((C/'derived/stage1d/labels').glob('*.json'))
            record['current_filter_inputs']=[{'path':p.relative_to(R).as_posix(),'sha256':sha(p)} for p in sources if p.exists()]
            labels=Labels(C);filtered=[]
            for row in doc['state_screen_rows']:
                if row['query_name']!=args.query or not row['new_cost_screen_required']:continue
                s=State(**dict(row['state'],arrival=Event(**row['state']['arrival'])))
                identity=labels.resolve_state(s)
                if identity.get('kind')=='UNKNOWN' and identity.get('branch_action') in (None,'NORMAL_ACCOUNT_EXPAND'):
                    filtered.append({'state':asdict(s),'identity':identity})
            record.update(query_name=args.query,actual_initial_unfinished_states=len(filtered))
            atomic_json(out,record)
            for _ in range(args.maximum_batches):
                result=acquire_labels(C,query,{'states':filtered})
                record['results'].append(result);atomic_json(out,record)
                print(json.dumps({k:result.get(k) for k in ('status','new_addresses','labels_updated')}),flush=True)
                if result.get('status')!='COMPLETED_EXPORTED' or not result.get('labels_updated'):break
        record.update(status='EVIDENCE_BATCH_RETURNED',ended_at_utc=now());atomic_json(out,record)
        print(json.dumps({'receipt':out.relative_to(R).as_posix(),'sha256':sha(out)}),flush=True)
    except Exception as exc:
        record.update(status='BLOCKED_OR_FAILED_SAME_KEYS_PRESERVED',error_class=type(exc).__name__,ended_at_utc=now())
        atomic_json(out,record);raise
    finally:
        if read(guard)!=token:raise RuntimeError('Guard identity changed; preserve it')
        guard.unlink()

if __name__=='__main__':main()
