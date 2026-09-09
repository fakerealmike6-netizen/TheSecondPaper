"""Freeze the completed zero-hop facts and assemble their existing native ledger."""
from pathlib import Path
import sys,json,socket,time,hashlib
R=Path(__file__).resolve().parents[1]; C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
from stage1d_runtime import Runtime
from stage1d_closure_scope import active_batch_path
from stage1d_closure_context import assemble_current
def blocked(*a,**k):raise RuntimeError('Local assembly cannot access network')
socket.socket.connect=blocked;socket.create_connection=blocked
runtime=Runtime();runtime.require_gate(C)
base=C/'derived/stage1d/queries/lifi_src001'
collection=read(base/'collection.json')
if collection['status']!='COMPLETED_WITHIN_DECLARED_SCOPE' or collection['unresolved_frontier']:
    raise ValueError('Only completed unchanged zero-hop candidate facts can be frozen here')
source_inventory={p.name:sha(p) for p in (C/'src').glob('*.py')}
key=hashlib.sha256(json.dumps({'collection':sha(base/'collection.json'),
    'labels':sha(base/'label_snapshot.json'),'source':source_inventory},sort_keys=True,separators=(',',':')).encode()).hexdigest()
out=base/'closure_context'/key
out.mkdir(parents=True,exist_ok=True)
def save(name,value):
    p=out/name
    if p.exists() and read(p)!=value:raise ValueError('Immutable current context evidence conflict: '+name)
    if not p.exists():atomic_json(p,value)
    return {'path':p.relative_to(C).as_posix(),'sha256':sha(p)}
refs={}
for dest,old in [('collection','collection.json'),('labels','label_snapshot.json')]:
    data=(base/old).read_bytes(); p=out/(dest+'.json')
    if p.exists() and p.read_bytes()!=data:raise ValueError('Snapshot conflict')
    if not p.exists():p.write_bytes(data)
    if sha(p)!=sha(base/old):raise ValueError('Live inputs changed during zero-hop snapshot')
    refs[dest]={'path':p.relative_to(C).as_posix(),'sha256':sha(p)}
freeze=save('CANDIDATES_AND_LABELS_FREEZE.json',{'freeze_sha256':sha(active_batch_path(C)),
    'candidates_frozen':True,'labels_frozen':True,'queries':{'lifi_src001':refs},
    'basis':'Current unchanged completed original zero-hop scope; no service promotion or extra expansion'})
old=base/'context/round_500_full'
names=[('events','ledger_rows.json'),('headers','headers.json'),('balances','balances.json'),
       ('receipts','receipts.json'),('coverage','coverage.json')]
source_refs=[{'path':(old/name).relative_to(C).as_posix(),'sha256':sha(old/name)} for _,name in names]
material={k:read(old/name) for k,name in names}
material.update(evidence_kind='REAL_EVIDENCE_BOUND',source_refs=source_refs)
material_ref=save('MATERIAL.json',material)
with runtime.session(C,'lifi_src001','current_zero_hop_context_assembly'):
    result=assemble_current(C,freeze,'lifi_src001',material_ref)
result_ref=save('ASSEMBLY.json',result)
receipt=save('ASSEMBLY_RECEIPT.json',{'status':result['status'],
    'completion_status':result['context_result']['completion_status'],
    'missing_points':result['missing_points'],'gaps':result['context_result']['evidence_gaps'],
    'raw_rows':len(material['events']),'selected_rows':len(result['requirements']['selected_events']),
    'frozen_inputs':freeze,'material':material_ref,'assembly':result_ref,
    'source_sha256':source_inventory,
    'new_external_requests':0,'methods_executed':0,'model_registered':False,
    'qualification':'Full context is only within unchanged declared zero-hop model scope. UNKNOWN does not become a service. Final seven-method comparison is pending.'})
atomic_json(base/'CURRENT_CLOSURE_CONTEXT.json',receipt)
print(json.dumps({'completion_status':result['context_result']['completion_status'],
    'missing_points':len(result['missing_points']),'gaps':len(result['context_result']['evidence_gaps']),
    'receipt':receipt}),flush=True)
