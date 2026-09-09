"""ROOT-only bounded evidence reads; use the existing ledger/runtime/transport.

No discovery, role adoption, collector replay, retry reset or new provider.
"""
from pathlib import Path
from dataclasses import asdict
import argparse, hashlib, json, os, sys, uuid

R=Path(__file__).resolve().parents[2] if Path(__file__).resolve().parent.parent.name=='staging' else Path(__file__).resolve().parents[1]
C=R/'code'
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
CURRENT_SCHEMA='stage1d-current-pending-historical-code-preparation-v1'
INITIAL_SCHEMA='stage1d-initial-screen-historical-code-preparation-v1'
CURRENT_HELPER='staging/unknown_cost_future_code_preparation/prepare_future_historical_code.py'
INITIAL_HELPER='staging/unknown_cost_code_preparation/prepare_historical_code.py'
QUERY_NAMES=('txphish_src002','txphish_src001','xscam_src001','lifi_src001')
FAILURES={'LOOKUP_FAILED','ACCESS_BLOCKED','ROLE_CONFLICT'}

def inside(root,rel):
    p=(root/rel).resolve()
    if not p.is_relative_to(root.resolve()):raise ValueError('Path escapes current revision')
    return p

def verify_ref(root,ref):
    p=inside(root,ref['path'])
    if sha(p)!=ref['sha256'] or ('bytes' in ref and p.stat().st_size!=ref['bytes']):
        raise ValueError('Prepared input SHA/bytes changed: '+ref['path'])
    return p

def _recheck_current_files(prep,expected,doc):
    """Recheck frozen inputs without reparsing the four graphs per RPC batch."""
    verify_ref(R,{'path':prep,'sha256':expected})
    sources=doc['source_and_current_input_sha256'];refs={}
    for ref in [dict(path=p,sha256=h) for p,h in sources.items()]+doc['input_refs']:
        previous=refs.get(ref['path'])
        if previous is not None and previous['sha256']!=ref['sha256']:
            raise ValueError('Conflicting prepared input SHA references')
        refs[ref['path']]=ref
    for ref in refs.values():verify_ref(R,ref)
    # A newly added label or task-boundary file is also a changed lookup input,
    # even when every previously known file retains identical bytes.
    live=set((C/'derived/stage1d/labels').glob('*.json'))
    live.update(C/'private/stage1d_roles'/n for n in ('CURRENT.json','AUTHORITIES.json','USER_TASK_BOUNDARIES.json'))
    for path in live:
        if path.exists() and path.relative_to(R).as_posix() not in sources:
            raise ValueError('New current role/label input was not in the prepared source version')

def _current_preparation(doc):
    """Bind selected future demands to saved pending states, never a new graph."""
    from collector import Scope
    from stage1d_unknown_cost_boundary import validate_decision,state_key,validate_policy
    from stage1d_closure_scope import active_batch_path
    sources=doc.get('source_and_current_input_sha256')
    if (doc.get('source_and_current_inputs_stable') is not True or not isinstance(sources,dict)
            or doc.get('status')!='PREPARED_OFFLINE_NOT_EXECUTED'
            or doc.get('initial_snapshot_modified') is not False
            or doc.get('original_group_key_and_runtime_logical_key_preserved') is not True):
        raise ValueError('Current pending preparation is not a frozen source-bound demand')
    for rel,h in sources.items():verify_ref(R,{'path':rel,'sha256':h})
    refs=doc.get('input_refs')
    if not isinstance(refs,list) or not refs:raise ValueError('All current input references required')
    for ref in refs:verify_ref(R,ref)
    bound={r['path']:r['sha256'] for r in refs}
    def source(path):
        relative=path.relative_to(R).as_posix()
        if sources.get(relative)!=sha(path) or bound.get(relative)!=sources[relative]:
            raise ValueError('Required current source is not doubly bound: '+relative)
        return read(path)
    policy=source(C/'private/stage1d_roles/UNKNOWN_COST_POLICY.json')
    current=source(C/'private/stage1d_roles/UNKNOWN_COST_CURRENT.json')
    policy_sha=sha(C/'private/stage1d_roles/UNKNOWN_COST_POLICY.json')
    if current.get('schema_version')!='stage1d-unknown-cost-registry-v1' or current.get('policy_ref',{}).get('sha256')!=policy_sha:
        raise ValueError('Current policy pointer differs')
    initial=current.get('initial_snapshot_ref',{})
    initial_path=verify_ref(R,dict(initial,path=(Path('code')/initial.get('path','')).as_posix()))
    if initial.get('sha256')!=SNAP_SHA or initial_path!=inside(R,'reports/unknown_cost_boundary_v1/inputs/INITIAL_SNAPSHOT.json'):
        raise ValueError('Current pending work cannot replace the original initial snapshot')
    if (sources.get(CURRENT_HELPER)!=doc.get('helper_sha256')
            or sources.get(INITIAL_HELPER)!=doc.get('original_helper_sha256')):
        raise ValueError('Reviewed preparation helper sources differ')
    batch=source(active_batch_path(C));scopes={q['name']:Scope.from_policy(q) for q in batch['queries']}
    if set(scopes)!=set(QUERY_NAMES) or set(doc.get('current_collection_refs',{}))!=set(QUERY_NAMES):
        raise ValueError('Exactly four active query/collection bindings required')
    collections={}
    for name in QUERY_NAMES:
        scoped=scopes[name];validate_policy(policy,scope=scoped)
        path=C/'derived/stage1d/queries'/name/'collection.json'
        ref=doc['current_collection_refs'][name]
        if ref['path']!=path.relative_to(R).as_posix() or verify_ref(R,ref)!=path:
            raise ValueError('Current collection alias is required')
        co=source(path);collections[name]=co
        cp=co.get('cost_boundary_policy',{})
        if (co.get('query_id')!=scoped.query_id or cp.get('schema_version')!='stage1d-unknown-cost-policy-v1' or cp.get('enabled') is not True
                or cp.get('authorization_id')!=AUTH or cp.get('policy_sha256')!=policy_sha):
            raise ValueError('Current saved graph has another cost policy/query')
    groups={g['group_key']:g for g in doc.get('code_groups',[])}
    if len(groups)!=len(doc.get('code_groups',[])):raise ValueError('Duplicate physical code group')
    for request in doc['next_batch']['requests']:
        g=groups.get(request.get('first_group_key'))
        if not g or g.get('input_origin')!='CURRENT_SAVED_PENDING_STATES' or g.get('future_new_states_included') is not True:
            raise ValueError('Request lacks its current pending physical group')
        if g.get('block_binding_status')=='BLOCK_HASH_CONFLICT':raise ValueError('Conflicting block cannot be dispatched')
        if g.get('label_status',{}).get('online_label_status')!='SUCCESS':raise ValueError('Current label success missing')
        address=g['address'];block=g['arrival_block']
        group_key=hashlib.sha256(json.dumps(['eip155:1',address,block],sort_keys=True,
            separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
        if g.get('chain_id')!='eip155:1' or g['group_key']!=group_key:
            raise ValueError('Original physical group key differs')
        method=request['request']['method']
        expected={'method':method,'params':[address,hex(block)] if method=='eth_getCode' else [hex(block),False]}
        if request['request']!=expected:raise ValueError('Request selector differs from its exact physical group')
        if doc.get('current_search_status',{}).get(address,{}).get('status') in FAILURES:
            raise ValueError('Existing finite identity failure cannot be nominated again')
        owners=[]
        for member in g['states']:
            name=member['query_name'];co=collections[name];scoped=scopes[name]
            index=member['current_row_index']
            if type(index) is not int or index<0:raise ValueError('Exact current row index required')
            row=co['states'][index];state=row['state'];decision=row['cost_boundary']
            validate_decision(state,scoped,decision,policy_sha)
            if (decision['action']!='PENDING' or decision['reason'] not in ('TYPE_UNRESOLVED','IDENTITY_CHECK_PENDING')
                    or decision['decision_sha256']!=member['saved_decision_sha256']
                    or state_key(state,scoped)!=member['state_key']
                    or member['query_id']!=scoped.query_id or member['scope_hash']!=scoped.scope_hash
                    or member['collection_ref']!=doc['current_collection_refs'][name]
                    or state['address']!=address or state['arrival']['block']!=block
                    or state['depth']>=scoped.max_depth or state['local_end']<=state['arrival']['timestamp']
                    or any(v.get('status') in FAILURES for v in decision.get('identity_checks',{}).values())
                    or decision.get('code_status') in ('CODE_PRESENT','NO_RUNTIME_CODE_AT_BLOCK','NO_CODE_AT_BLOCK')):
                raise ValueError('Current member no longer requires this finite code/header request')
            if decision not in co.get('cost_boundary_decisions',[]):raise ValueError('Saved decision ledger differs')
            identity=member.get('current_identity',{})
            if (identity.get('kind')!='UNKNOWN' or identity.get('branch_action') not in (None,'NORMAL_ACCOUNT_EXPAND')
                    or identity.get('status') not in ('LOCAL_FROZEN','SUCCESS_EMPTY_CACHED')):
                raise ValueError('Prepared member is already a role/boundary or failed lookup')
            owners.append(name)
        if request['query_names']!=sorted(set(owners),key=QUERY_NAMES.index):raise ValueError('Prepared query clock owner changed')
    return sources

def validate_preparation(prep,expected,runtime):
    path=inside(R,prep)
    if sha(path)!=expected:raise ValueError('Exact prepared evidence demand changed')
    doc=read(path)
    if doc['authorization_id']!=AUTH:
        raise ValueError('Different authority or initial screen')
    if doc.get('schema_version')==CURRENT_SCHEMA:
        sources=_current_preparation(doc)
        if doc.get('next_batch',{}).get('maximum_rpc_members') not in (5,100):
            raise ValueError('Current prepared member limit must be 5 or 100')
    else:
        if doc.get('schema_version')!=INITIAL_SCHEMA or doc['initial_snapshot_ref']['sha256']!=SNAP_SHA:
            raise ValueError('Different authority or initial screen')
        sources=doc['source_and_label_sha256']
    for rel,h in sources.items():
        if sha(inside(R,rel))!=h:raise ValueError('Current source/role/label changed; prepare a new evidence difference')
    rows=doc['next_batch']['requests']
    if not 1<=len(rows)<=min(100,doc['next_batch'].get('maximum_rpc_members',100)):raise ValueError('Finite prepared batch required')
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

def _current_cache(rows,runtime):
    """Existing success contract, exact readonly SQL; no retry-store creation."""
    import importlib.util,sqlite3
    from contextlib import closing
    path=inside(R,INITIAL_HELPER)
    spec=importlib.util.spec_from_file_location('_current_dispatch_cache_reader',path)
    base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base)
    inputs=base.Inputs(R);dbpath=C/'private/read_retry_r4.sqlite'
    def lookup(db):
        cache=base.ReadOnlyCache(C,db,runtime,inputs,logical_key)
        return [cache.lookup(row['request']) for row in rows]
    if not dbpath.exists():return lookup(None)
    with closing(sqlite3.connect(dbpath.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.execute('PRAGMA query_only=ON');db.execute('BEGIN');return lookup(db)

def execute_code_preparation(prep,expected,runtime,record,out,*,access_factory=None):
    p,prepared,rows=validate_preparation(prep,expected,runtime)
    if prepared['schema_version']==CURRENT_SCHEMA:_recheck_current_files(prep,expected,prepared)
    runtime.require_gate(C)
    if (C/'private/network_worker.lock').exists():raise RuntimeError('Wait for existing normal writer')
    record['preparation']={'path':p.relative_to(R).as_posix(),'sha256':expected,
                           'schema_version':prepared['schema_version']}
    if prepared['schema_version']==CURRENT_SCHEMA:
        observations=_current_cache(rows,runtime);pending=[]
        for row,cached in zip(rows,observations,strict=True):
            if cached['logical_key']!=row['logical_key']:raise ValueError('Current cache request key differs')
            if cached['state']=='SUCCESS_ENVELOPE_SHA_AND_RUNTIME_VALIDATED':
                record.setdefault('cache_reuse',[]).append({'need':row,'cache':cached,'new_request':False})
            elif cached['state']=='NO_CURRENT_EXACT_REQUEST':pending.append(row)
            else:raise RuntimeError('Existing failed/inflight/blocked request preserved; no automatic renomination')
        rows=pending
        atomic_json(out,record)
        if not rows:return
    # Validation and current success lookup precede even RpcAccess construction.
    if access_factory is None:
        from context_access_r4 import RpcAccess
        for key in ('HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy'):os.environ[key]='http://127.0.0.1:7890'
        access_factory=RpcAccess
    access=access_factory(C,runtime=runtime)
    cursor=0
    while cursor<len(rows):
        if prepared['schema_version']==CURRENT_SCHEMA:
            _recheck_current_files(prep,expected,prepared);runtime.require_gate(C)
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
            execute_code_preparation(args.preparation,args.sha256,runtime,record,out)
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
