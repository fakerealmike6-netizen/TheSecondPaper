"""Assemble one current, explicitly frozen query from admitted real material."""
from pathlib import Path
import sys,json,argparse,time
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha
from page_attempts import atomic_json
import stage1d_bq_context_prepare as b
from stage1d_closure_context import assemble_current
from offline_operation import offline_operation

p=argparse.ArgumentParser();p.add_argument('--query',required=True);p.add_argument('--bundle',required=True);p.add_argument('--material',required=True);a=p.parse_args()
bp=b.inside(C,a.bundle);bundle=read(bp);consumer=bundle['consumers'][a.query]
mp=b.inside(C,a.material);material=read(mp);base=C/'derived/stage1d/queries'/a.query
source={p.name:sha(p) for p in (C/'src').glob('*.py')}
for key,file in (('collection','collection.json'),('labels','label_snapshot.json')):
    if sha(base/file)!=consumer['inputs'][key]['sha256']:raise ValueError('Prepared current candidate/role binding changed')
freeze=bundle['frozen_inputs'];b.checked(C,freeze)
identity={'frozen_inputs':freeze,'material':b.dep(C,mp),'source_sha256':source}
out=base/'closure_context'/b.digest(identity);out.mkdir(parents=True,exist_ok=True)
def save(name,data):
    path=out/name
    if path.exists() and read(path)!=data:raise ValueError('Immutable actual assembly changed: '+name)
    if not path.exists():atomic_json(path,data)
    return b.dep(C,path)
started=time.perf_counter()
with offline_operation(C,'repaired_context_assembly_'+a.query):
    try:
        result=assemble_current(C,freeze,a.query,identity['material'])
    except Exception as exc:
        failure={'status':'ASSEMBLY_ERROR_BLOCKED','error_class':type(exc).__name__,'reason':str(exc),**identity,'new_requests':0,'methods_executed':0}
        save('ASSEMBLY_FAILURE.json',failure);print(json.dumps({k:failure[k] for k in ('status','error_class','reason')}),flush=True);raise
    result_ref=save('ASSEMBLY.json',result)
    receipt=save('ASSEMBLY_RECEIPT.json',dict(identity,status=result['status'],completion_status=result['context_result']['completion_status'],
        assembly=result_ref,missing_points=result['missing_points'],gaps=result['context_result']['evidence_gaps'],
        raw_rows=len(material['events']),selected_rows=len(result['requirements']['selected_events']),
        new_external_requests=0,new_online_sessions=0,methods_executed=0,model_registered=False,
        qualification='Current actual source-bound context; model intervals and seven-method comparison remain pending.'))
    atomic_json(base/'CURRENT_CLOSURE_CONTEXT.json',receipt)
print(json.dumps({'query':a.query,'status':result['status'],'completion_status':result['context_result']['completion_status'],
    'missing_points':len(result['missing_points']),'gaps':len(result['context_result']['evidence_gaps']),
    'receipt':receipt,'seconds':time.perf_counter()-started}),flush=True)
