"""Root-only finite label enrichment on actual current arrivals."""
from pathlib import Path
import sys, json, argparse, copy, traceback
R=Path(__file__).resolve().parents[1]; C=R/'code'
sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
from stage1d_closure_scope import active_batch
from stage1d_acquisition import acquire_labels
from stage1d_runtime import Runtime
parser=argparse.ArgumentParser();parser.add_argument('--query',required=True);parser.add_argument('--maximum-batches',type=int,default=3)
a=parser.parse_args();Runtime().require_gate(C)
if not 1<=a.maximum_batches<=3:raise ValueError('Finite label scheduling bound required')
query=next(q for q in active_batch(C)['queries'] if q['name']==a.query)
source=C/'derived/stage1d/queries'/a.query/'collection.json'
collection=read(source)
filtered=copy.copy(collection)
filtered['states']=[s for s in collection['states'] if not s['identity'].get('task_boundary_ids')]
record={'schema_version':'stage1d-closure-current-label-batches-v1','started_at_utc':now(),
 'query_name':a.query,'collection':{'path':source.relative_to(C).as_posix(),'sha256':sha(source)},
 'excluded_user_task_boundary_arrivals':len(collection['states'])-len(filtered['states']),
 'source_arrivals_only':True,'pool_reset':False,'results':[]}
folder=R/'operations';folder.mkdir(exist_ok=True)
out=folder/('labels_'+a.query+'_'+record['started_at_utc'].replace(':','').replace('+','_')+'.json')
atomic_json(out,record)
for i in range(a.maximum_batches):
 try:
  result=acquire_labels(C,query,filtered)
 except Exception as exc:
  record['results'].append({'status':'BLOCKED_OR_FAILED','error_class':type(exc).__name__,'traceback':traceback.format_exc()})
  atomic_json(out,record)
  print(json.dumps({'query':a.query,'status':'BLOCKED_OR_FAILED','error_class':type(exc).__name__}),flush=True)
  raise
 record['results'].append(result);atomic_json(out,record)
 print(json.dumps({'query':a.query,'batch':i+1,**{k:result.get(k) for k in ('status','new_addresses','labels_updated','new_requests','cache_reused')}}),flush=True)
 if result.get('status')!='COMPLETED_EXPORTED' or not result.get('labels_updated'):break
record['ended_at_utc']=now();atomic_json(out,record)
print(json.dumps({'receipt':out.relative_to(R).as_posix(),'sha256':sha(out)}),flush=True)
