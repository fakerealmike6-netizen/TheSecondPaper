"""Freshly validate unchanged actual certificates against a new tested source gate."""
from pathlib import Path
import sys,json,socket,hashlib,copy
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from stage1d_runtime import Runtime
from stage1d_semantic_units import FiniteSemanticResolver
from stage1d_semantic_catalogue import load_current_resolver,SCHEMA,_bound
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
def blocked(*a,**k):raise RuntimeError('Gate rebind uses saved evidence only')
socket.socket.connect=blocked;socket.create_connection=blocked
runtime=Runtime();runtime.require_gate(C)
if (C/'private/network_worker.lock').exists():raise RuntimeError('Wait for current writer to complete')
pointer=C/'private/stage1d_semantics/CURRENT.json'
if not pointer.exists():raise ValueError('No actual certificate catalogue to rebind')
original=read(pointer);oldsha=sha(pointer)
if original.get('schema_version')!=SCHEMA or original.get('evidence_kind')!='REAL_CHAIN':raise ValueError('Actual finite catalogue required')
gatepath=C/'private/stage1d_semantics/SEMANTIC_CAPABILITY_GATE.json';gate=read(gatepath);gatesha=sha(gatepath)
units=[_bound(C,x['unit']) for x in original['instances']];shared=_bound(C,original['shared_evidence_context'])
resolver=FiniteSemanticResolver(units,shared,capability_gate=gate,work_root=C)
def exact_copy(source,target):
 target.parent.mkdir(parents=True,exist_ok=True)
 if target.exists() and target.read_bytes()!=source.read_bytes():raise ValueError('Immutable evidence copy conflict')
 if not target.exists():target.write_bytes(source.read_bytes())
 if sha(source)!=sha(target):raise ValueError('Evidence byte copy differs')
with runtime.session(C,'SHARED','current_actual_semantic_source_gate_rebind'):
 savedgate=C/'private/stage1d_semantics/gates'/(gatesha+'.json');exact_copy(gatepath,savedgate)
 oldpath=R/'snapshot/semantic_catalogues'/(oldsha+'.json');exact_copy(pointer,oldpath)
 catalogue=copy.deepcopy(original);catalogue['capability_gate']={'path':savedgate.relative_to(C).as_posix(),'sha256':gatesha}
 key=hashlib.sha256(json.dumps(catalogue,sort_keys=True,separators=(',',':')).encode()).hexdigest()
 version=C/'private/stage1d_semantics/catalogues'/(key+'.json')
 if version.exists() and read(version)!=catalogue:raise ValueError('Versioned catalogue changed')
 if not version.exists():atomic_json(version,catalogue)
 atomic_json(pointer,catalogue)
 current=load_current_resolver(C)
 if current.identity!=resolver.identity:raise ValueError('Live resolver differs from freshly checked original physical certificates')
 receipt={'status':'ACTUAL_CERTIFICATES_REVALIDATED_NEW_SOURCE_GATE','utc':now(),
  'prior_catalogue':{'path':oldpath.relative_to(R).as_posix(),'sha256':oldsha},
  'catalogue':{'path':version.relative_to(C).as_posix(),'sha256':sha(version)},
  'unit_ids':sorted(current.units),'same_physical_certificate_bytes':True,'new_external_requests':0,'solver_runs':0}
 out=R/'operations'/('semantic_gate_rebind_'+gatesha+'.json')
 if out.exists() and read(out).get('catalogue')!=receipt['catalogue']:raise ValueError('Gate rebind receipt conflict')
 if not out.exists():atomic_json(out,receipt)
print(json.dumps({'status':receipt['status'],'units':len(current.units),'new_external_requests':0,'receipt':out.relative_to(R).as_posix()}))
