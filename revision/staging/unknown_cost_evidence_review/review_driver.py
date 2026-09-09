"""Bounded synthetic review: execute only extracted driver functions with fake IO providers."""
from pathlib import Path
from dataclasses import dataclass, asdict
from contextlib import redirect_stdout
from unittest.mock import patch
import ast, hashlib, io, json, os, sys, tempfile, time, types, uuid

HERE=Path(__file__).resolve().parent
R=HERE.parents[1]
SOURCE=R/'scripts/execute_unknown_cost_evidence.py'
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def read(path):return json.loads(Path(path).read_bytes())
def atomic(path,value):
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    Path(path).write_text(json.dumps(value,sort_keys=True),encoding='utf8')
def key(identity):return hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest()
def identity(provider,p):return {'provider':provider,'chain':1,**p}
def check(ok,label):
    if not ok:raise AssertionError(label)
    checks.append(label)

@dataclass
class Event:
    event_id:str
@dataclass
class State:
    address:str
    arrival:Event

started=time.perf_counter();source_before=sha(SOURCE);checks=[];dispatches=[];label_calls=[]
tree=ast.parse(SOURCE.read_text(encoding='utf8'))
functions=ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef)],type_ignores=[])

with tempfile.TemporaryDirectory(prefix='synthetic_',dir=HERE) as temp:
    root=Path(temp);work=root/'code';(work/'private').mkdir(parents=True);(root/'operations').mkdir()
    snap=root/'reports/unknown_cost_boundary_v1/inputs/INITIAL_SNAPSHOT.json'
    snapshot={'state_screen_rows':[
        {'query_name':'txphish_src002','new_cost_screen_required':new,
         'state':{'address':a,'arrival':{'event_id':a}}}
        for a,new in [('unknown',True),('hold',True),('service',True),('supported',True),('finished',False)]]}
    atomic(snap,snapshot)
    metadata=root/'code/local_metadata.json';atomic(metadata,{'version':1})
    gate=work/'STAGE1D_PREFLIGHT_GATE.json';atomic(gate,{'status':'SYNTHETIC_PASS'})
    roles=work/'private/stage1d_roles/CURRENT.json';atomic(roles,{'role':'SYNTHETIC_ONLY'})
    class Runtime:
        def require_gate(self,w):
            check(Path(w)==work,'runtime gate uses exact work')
        def clock_used(self,w):return 0
        def validate_rpc(self,p):return p
        def rpc_identity(self,provider,p):return identity(provider,p)
    class Labels:
        def __init__(self,w):pass
        def resolve_state(self,state):
            if state.address=='hold':return {'kind':'UNKNOWN','branch_action':'USER_REQUESTED_BRANCH_HOLD'}
            if state.address=='service':return {'kind':'SERVICE'}
            if state.address=='supported':return {'kind':'SUPPORTED_PROTOCOL'}
            if state.address=='finished':raise AssertionError('Previously completed state rechecked')
            return {'kind':'UNKNOWN','status':'LOCAL_FROZEN'}
    def acquire_labels(w,q,collection):
        label_calls.append(collection)
        return {'status':'NO_NEW_LABEL_OPPORTUNITY','labels_updated':False}
    statuses=[]
    class RpcAccess:
        def __init__(self,w,runtime=None):
            if type(runtime) is not Runtime:raise AssertionError('Runtime not injected')
        def call_batch(self,plans,probe,label,capability=False):
            dispatches.append({'plans':plans,'probe':probe,'capability':capability})
            return {'status':statuses.pop(0) if statuses else 'COMPLETE','actual_operations_this_call':len(plans)}
    module=types.ModuleType('context_access_r4');module.RpcAccess=RpcAccess
    env=dict(Path=Path,asdict=asdict,argparse=__import__('argparse'),hashlib=hashlib,json=json,os=os,sys=sys,uuid=uuid,
        R=root,C=work,AUTH='TEST_AUTH',SNAP_SHA=sha(snap),read=read,sha=sha,now=lambda:'SYNTHETIC',atomic_json=atomic,
        Runtime=Runtime,Labels=Labels,State=State,Event=Event,logical_key=key,acquire_labels=acquire_labels,
        active_batch=lambda _: {'queries':[{'name':'txphish_src002','query_id':'Q2'}]},__file__=str(SOURCE))
    exec(compile(functions,str(SOURCE),'exec'),env)
    rows=[]
    for i,owners in enumerate([['txphish_src002']]*7+[['txphish_src001','txphish_src002']]*6):
        request={'method':'eth_getBlockByNumber','params':[hex(i+1),False]}
        rows.append({'request':request,'logical_key':key(identity('ALCHEMY_ETH_MAINNET_EXISTING',request)), 'query_names':owners})
    prepared={'authorization_id':'TEST_AUTH','initial_snapshot_ref':{'sha256':sha(snap)},
        'source_and_label_sha256':{'code/local_metadata.json':sha(metadata)},'next_batch':{'requests':rows}}
    prep=root/'prepared.json';atomic(prep,prepared)
    def run(args):
        with patch.object(sys,'argv',[str(SOURCE),*args]),patch.dict(sys.modules,{'context_access_r4':module}),patch.dict(os.environ,{},clear=False),redirect_stdout(io.StringIO()):
            env['main']()
    args=['code','--execute','--preparation','prepared.json','--sha256',sha(prep)]
    run(args)
    check([len(d['plans']) for d in dispatches]==[5,2,5,1],'same-owner consecutive batches are bounded by five')
    check([d['probe'] for d in dispatches]==['txphish_src002','txphish_src002','SHARED','SHARED'],'shared requests retain existing SHARED clock')
    check(all(d['capability'] is False for d in dispatches),'ordinary calls never gain capability budget')
    first=[p for d in dispatches for p in d['plans']];dispatches.clear();statuses.append('PARTIAL');run(args)
    check(len(dispatches)==1 and dispatches[0]['plans']==first[:5],'partial result stops later batches and preserves exact request selectors')
    check(not (root/'operations/CURRENT_COST_EVIDENCE_DRIVER.lock').exists(),'driver guard is released after normal partial return')
    dispatches.clear();atomic(metadata,{'version':2})
    try:run(args)
    except ValueError:pass
    else:raise AssertionError('Changed preparation source accepted')
    check(not dispatches,'changed bound source blocks all dispatch')
    atomic(metadata,{'version':1});(work/'private/network_worker.lock').write_text('{}')
    try:run(args)
    except RuntimeError:pass
    else:raise AssertionError('Existing writer accepted')
    check(not dispatches,'existing network lock blocks all dispatch')
    (work/'private/network_worker.lock').unlink()
    run(['labels','--execute','--query','txphish_src002'])
    check([s['state']['address'] for s in label_calls[0]['states']]==['unknown'],'label wrapper keeps only initially unfinished and current expandable UNKNOWN states')
    outputs=[read(p) for p in (root/'operations').glob('unknown_cost_labels_*.json')]
    check(len(outputs)==1,'one immutable labels receipt generated')
    refs={x['path']:x['sha256'] for x in outputs[0]['current_filter_inputs']}
    check(refs[gate.relative_to(root).as_posix()]==sha(gate) and refs[roles.relative_to(root).as_posix()]==sha(roles),'labels receipt binds actual current gate and role inputs')
    missing_label_source_binding=False

receipt={'status':'PASS','checks':checks,'check_count':len(checks),'network_attempts':0,
    'production_execution':False,'actual_production_db_reads':False,'source_sha256_before':source_before,
    'source_sha256_after':sha(SOURCE),'source_stable':sha(SOURCE)==source_before,'seconds':time.perf_counter()-started,
    'observed_gap':{'labels_receipt_does_not_bind_current_filter_sources':missing_label_source_binding},
    'limitation':'Extracted driver entry functions with synthetic runtime/access; original runtime accounting reviewed read-only, not reexecuted.'}
atomic(HERE/'REVIEW_TEST_RECEIPT.json',receipt)
print(json.dumps(receipt,ensure_ascii=False))
