"""Admit full newly completed context families without resubmitting jobs."""
from pathlib import Path
import sys,json,argparse,socket,time
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha
from page_attempts import atomic_json
import stage1d_bq_context_prepare as b
from stage1d_final_context_prepare import materialize_family
from stage1d_current_context_paid_bq import admit_current
from stage1d_runtime import Runtime
def blocked(*a,**k):raise RuntimeError('Offline completed BQ admission')
socket.socket.connect=blocked;socket.create_connection=blocked
p=argparse.ArgumentParser();p.add_argument('--bundle',required=True);p.add_argument('--query',required=True);p.add_argument('--revision',required=True);a=p.parse_args()
Runtime().require_gate(C)
if (C/'private/network_worker.lock').exists():raise ValueError('Wait for existing writer')
bundle_path=b.inside(C,a.bundle);bundle=read(bundle_path);consumer=bundle['consumers'][a.query]
out=C/'private/integration_repair_context'/a.query/a.revision
if out.exists():raise ValueError('Use new immutable material revision')
out.mkdir(parents=True);sources=[];outputs=[];started=time.perf_counter()
for i,family in enumerate(bundle['families']):
 states={}
 for spec in read(b.checked(C,family))['plans']:
  expected=read(b.checked(C,spec))
  config=read(C/'private/STAGE1D_BIGQUERY_CONFIG_RECOVERY.json')
  digest=b.digest({'sql_sha256':expected['sql_sha256'],'project':config['project'],'location':'US'})
  state=C/'private/stage1d_bigquery_jobs'/digest/'job.json'
  if read(state)['state']!='COMPLETE_EXPORTED':raise ValueError('Saved family not fully read back')
  states[spec['path']]=state
 sources.append({'preparation_ref':family,'job_states':{k:b.dep(C,v) for k,v in states.items()}})
 outputs.append(materialize_family(C,bundle_path,family,states,out/('family_'+str(i)),query_names=[a.query]))
atomic_json(out/'CURRENT_JOB_SOURCES.json',{'sources':sources})
# Binding gaps must be explicit exact-point needs, never another whole scan.
admission=admit_current(C,a.query,consumer['inputs']['collection'],consumer['inputs']['labels'],sources,reuse_success_cache=True)
atomic_json(out/'CURRENT_PAID_ADMISSION.json',admission)
summary={'status':'NEW_PAID_CONTEXT_MATERIALIZED','query':a.query,'outputs':outputs,'admission':b.dep(C,out/'CURRENT_PAID_ADMISSION.json'),
 'rows':len(admission['rows']),'coverage_rows':len(admission['coverage']),'paid_binding_gaps':len(admission['binding_gaps']),
 'binding_points':len(admission['point_binding_requests']),'new_requests':0,'seconds':time.perf_counter()-started}
atomic_json(out/'MATERIALIZATION_RECEIPT.json',summary)
print(json.dumps({k:v for k,v in summary.items() if k!='outputs'}),flush=True)
