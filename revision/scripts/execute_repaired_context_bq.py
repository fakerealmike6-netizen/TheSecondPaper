"""Current singleton context: all actual dry-runs before aggregate-gated scans."""
from pathlib import Path
import sys,os,argparse,json
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C),str(R.parents[1]/'runtime_packages')]
os.environ['HTTP_PROXY']=os.environ['HTTPS_PROXY']='http://127.0.0.1:7890'
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
import stage1d_bq_context_prepare as bq
import stage1d_final_context_prepare as f
from stage1d_bigquery_probe import probe
from stage1d_bigquery_jobs import execute_plan
from stage1d_runtime import Runtime

def main():
 p=argparse.ArgumentParser();p.add_argument('--bundle',required=True);p.add_argument('--query',required=True);p.add_argument('--mode',choices=('dry-run','execute'),required=True);a=p.parse_args()
 Runtime().require_gate(C)
 bundle_path=bq.inside(C,a.bundle);bundle=read(bundle_path)
 names,owner,clock,_=f.check_bundle_group(bundle,[a.query])
 for name in names:
  for field,filename in (('collection','collection.json'),('labels','label_snapshot.json')):
   if sha(C/'derived/stage1d/queries'/name/filename)!=bundle['consumers'][name]['inputs'][field]['sha256']:raise ValueError('Current graph/role binding changed before dispatch')
 output=bundle_path.parent/'EXECUTION';output.mkdir(exist_ok=True)
 config_path='private/STAGE1D_BIGQUERY_CONFIG_RECOVERY.json';config=read(C/config_path)
 specs=[]
 for family in bundle['families']:
  specs.extend(p for p in read(bq.checked(C,family))['plans'] if p['template']=='transaction_and_trace')
 plans=[]
 for i,spec in enumerate(specs):
  target=output/('plan_'+str(i)+'.json')
  if a.mode=='dry-run':
   result=probe(C,owner,config_path,'dry-run',spec_path=spec['path'],clock_query=clock)
   atomic_json(output/('DRY_RESULT_'+str(i)+'.json'),result)
   print(json.dumps({'phase':'ACTUAL_DRYRUN','index':i,'status':result['status'],'estimated_bytes':(result.get('result') or {}).get('estimated_processed_bytes')}),flush=True)
   if result['status']!='SUCCESS_VALIDATED':
    atomic_json(output/'ROUTE_STATUS.json',{'status':'DRYRUN_BLOCKED','index':i,'result':result,'remaining_specs':specs[i:],'new_scan_submitted':False});return
   bq.bind_execution_plan(C,spec['path'],result['artifact_path'],target)
  plans.append(bq.dep(C,target))
 audit=f.audit_actual_dryruns(C,bundle_path,plans,config,query_names=[a.query])
 atomic_json(output/'AGGREGATE_AUDIT.json',audit)
 print(json.dumps({'phase':'AGGREGATE','status':audit['status'],'upper_bytes':audit['new_upper_bound_total_bytes']}),flush=True)
 if a.mode!='execute' or audit['status']!='ACTUAL_DRYRUN_AGGREGATE_FITS':return
 for i,plan in enumerate(plans):
  # Recheck saved complete global audit and current room without redoing any
  # successful scan/dry-run or assigning a new job identity.
  current=f.audit_actual_dryruns(C,bundle_path,plans,config,query_names=[a.query])
  if current['status']!='ACTUAL_DRYRUN_AGGREGATE_FITS':atomic_json(output/'ROUTE_STATUS.json',current);return
  result=execute_plan(C,owner,config,plan['path'],clock_query=clock)
  atomic_json(output/('JOB_RESULT_'+str(i)+'.json'),result)
  print(json.dumps({'phase':'COMPLETE_JOB_READBACK','index':i,'state':result['state'],'rows':result.get('total_rows'),'pages':len(result.get('pages',[])),'actual_billed_bytes':result.get('actual_billed_bytes')}),flush=True)
  if result['state']!='COMPLETE_EXPORTED':return
 atomic_json(output/'ROUTE_STATUS.json',{'status':'ALL_CONTEXT_JOBS_FULLY_READ_BACK','utc':now(),'job_count':len(plans),'context_model_built':False})

if __name__=='__main__':main()
