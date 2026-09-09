"""Root-only resume of an immutable current BigQuery preparation."""
from pathlib import Path
import sys,json,argparse,traceback,os
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C),str(R.parents[1]/'runtime_packages')]
# Preserve the same local proxy route used by the original successful BQ run.
os.environ['HTTP_PROXY']=os.environ['HTTPS_PROXY']='http://127.0.0.1:7890'
from stage1d_batch_binding_route import execute_prepared,verified_preparation
from stage1d_runtime import Runtime
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
p=argparse.ArgumentParser();p.add_argument('--preparation',required=True);a=p.parse_args()
Runtime().require_gate(C)
manifest,_=verified_preparation(C,a.preparation)
record={'schema_version':'stage1d-closure-bq-execution-v1','started_at_utc':now(),
 'query':manifest['query_name'],'preparation':{'path':a.preparation,'sha256':sha(C/a.preparation)},'pool_reset':False}
folder=R/'operations';folder.mkdir(exist_ok=True)
out=folder/('bq_'+manifest['query_name']+'_'+record['started_at_utc'].replace(':','').replace('+','_')+'.json')
atomic_json(out,record)
try:
 result=execute_prepared(C,a.preparation,'private/STAGE1D_BIGQUERY_CONFIG_RECOVERY.json',execute=True)
 record['result']=result;record['ended_at_utc']=now();atomic_json(out,record)
 print(json.dumps({'query':manifest['query_name'],'status':result.get('status'),
  'result_fields':list(result),'receipt':out.relative_to(R).as_posix(),'sha256':sha(out)}),flush=True)
except Exception as exc:
 record.update(ended_at_utc=now(),status='BLOCKED_OR_FAILED',error_class=type(exc).__name__,traceback=traceback.format_exc())
 atomic_json(out,record)
 print(json.dumps({'query':manifest['query_name'],'status':'BLOCKED_OR_FAILED','error_class':type(exc).__name__,
  'receipt':out.relative_to(R).as_posix(),'sha256':sha(out)}),flush=True)
 raise
