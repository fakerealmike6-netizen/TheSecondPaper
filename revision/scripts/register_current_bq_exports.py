"""Version source inventory after real complete BQ exports; never re-own jobs."""
from pathlib import Path
import sys,argparse,json,socket
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
def denied(*a,**k):raise RuntimeError('Saved job registration is offline')
socket.socket.connect=denied;socket.create_connection=denied
from prepare_current_weth import safe_point
from stage1d_runtime import Runtime
import stage1d_bq_context_prepare as h
from stage1d_current_context_paid_bq import _load_source
p=argparse.ArgumentParser();p.add_argument('--operation',required=True)
p.add_argument('--base-inventory',default='private/integration_repair_context/ALL_FIVE_PAID_JOB_SOURCES.json');a=p.parse_args()
Runtime().require_gate(C);safe_point(C)
operation=(R/a.operation).resolve()
if not operation.is_relative_to(R):raise ValueError('Current root operation receipt required')
saved=h.read(operation);base=h.inside(C,a.base_inventory);original=h.read(base)
prep=saved['preparation'];h.checked(C,prep)
states=saved['result']['job_states']
new=dict(preparation_ref=prep,job_states={key:h.dep(C,path) for key,path in states.items()})
verified=_load_source(C,new)
if verified['manifest']['query_name']!=saved['query']:raise ValueError('Original job owner differs from completed operation')
sources=original['sources'];key=lambda s:h.digest(dict(preparation_ref=s['preparation_ref'],job_states=s['job_states']))
if key(new) in {key(s) for s in sources}:raise ValueError('Successful source already registered; reuse original inventory')
document=dict(schema_version='stage1d-current-paid-source-inventory-v1',sources=[*sources,new],
 prior_inventory_ref=h.dep(C,base),completed_operation_ref=dict(path=operation.relative_to(R).as_posix(),sha256=h.sha(operation)),
 original_job_owner_preserved=True,new_external_requests=0)
output=C/'private/integration_repair_context/source_inventories'/(h.digest(document)+'.json')
h.save(output,document)
print(json.dumps(dict(status='COMPLETE_ORIGINAL_BQ_EXPORT_REGISTERED',source_count=len(document['sources']),
 new_original_query_name=verified['manifest']['query_name'],inventory=h.dep(C,output),new_external_requests=0)),flush=True)
