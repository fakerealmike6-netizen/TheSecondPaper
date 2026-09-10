"""Offline positive/negative checks of the actual root dispatch partition."""
from pathlib import Path
from copy import deepcopy
import ast
import hashlib
import json
import socket

R=Path(__file__).resolve().parents[1]
def denied(*args,**kwargs):raise RuntimeError('Offline partition checks')
socket.socket.connect=denied
socket.create_connection=denied
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
source=R/'scripts/execute_paid_candidate_binding.py'
tree=ast.parse(source.read_text(encoding='utf-8'))
function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='partition_old_raw')
namespace={'digest':digest}
exec(compile(ast.Module(body=[function],type_ignores=[]),str(source),'exec'),namespace)
partition=namespace['partition_old_raw']
def validate(plan):
 if plan.get('method')!='eth_getBlockByNumber' or plan.get('params')!=['0x123',False] and plan.get('params')!=['0x124',False]:
  raise ValueError('Unexpected selector')
a={'method':'eth_getBlockByNumber','params':['0x123',False]}
b={'method':'eth_getBlockByNumber','params':['0x124',False]}
cases=[]
def expect_error(name,plans,audit):
 try:partition(plans,audit,validate)
 except ValueError:cases.append({'case':name,'status':'PASS'})
 else:raise AssertionError(name)
plans=[a,b];before=deepcopy(plans)
allowed,blocked=partition(plans,{digest(a):'NO_EXACT_LEGACY_REQUEST_MATCH',digest(b):'LEGACY_RESPONSE_VARIANTS_QUARANTINED'},validate)
assert allowed==[a] and len(blocked)==1 and blocked[0]['plan']==b
assert blocked[0]['new_request_dispatched'] is False and plans==before
cases.append({'case':'mixed_demand_keeps_ambiguity_separate_and_preserves_input','status':'PASS'})
allowed,blocked=partition(plans,{digest(p):'NO_EXACT_LEGACY_REQUEST_MATCH' for p in plans},validate)
assert allowed==plans and blocked==[]
cases.append({'case':'all_exact_misses_keep_their_original_selectors','status':'PASS'})
allowed,blocked=partition([b],{digest(b):'LEGACY_RESPONSE_VARIANTS_QUARANTINED'},validate)
assert allowed==[] and len(blocked)==1
cases.append({'case':'all_ambiguous_means_no_dispatch','status':'PASS'})
expect_error('absent_audit_rejects',[a],{})
expect_error('unrecognized_disposition_rejects',[a],{digest(a):'UNREVIEWED'})
expect_error('unsubtracted_success_rejects',[a],{digest(a):'IMPORTED_POINT_SUCCESS'})
invalid={'method':'eth_sendTransaction','params':[]}
expect_error('runtime_selector_validator_still_applies',[invalid],{digest(invalid):'NO_EXACT_LEGACY_REQUEST_MATCH'})
def ref(path):return {'path':path.relative_to(R).as_posix(),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'bytes':path.stat().st_size}
report={'status':'PASS','cases':cases,'source':ref(source),'script':ref(Path(__file__)),
        'new_external_requests':0,'production_source_gate_changed':False,
        'scope':'Actual dispatch-partition function plus conservative negative checks; real CLI review is separate.'}
payload=json.dumps(report,sort_keys=True,indent=2).encode()+b'\n'
target=R/'reports/candidate_legacy_partition'/hashlib.sha256(payload).hexdigest()/'CHECK_RECEIPT.json'
target.parent.mkdir(parents=True,exist_ok=True)
if target.exists() and target.read_bytes()!=payload:raise ValueError('Immutable check receipt differs')
if not target.exists():target.write_bytes(payload)
print(json.dumps({'status':'PASS','checks':len(cases),'receipt':ref(target),'new_external_requests':0}),flush=True)
