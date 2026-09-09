"""Review-only fault injection at METHOD output boundary; frozen sources unchanged.
Real immutable input/hidden/freezes are used. We inject output defects, not input
or accounting data. Then call the actual runner CLI main to test propagation.
"""
from pathlib import Path
from unittest.mock import patch
from contextlib import redirect_stdout,redirect_stderr
import copy,sys,json,io,socket
R=Path(__file__).parent;ROOT=R.parent/'baseline/min';sys.path.insert(0,str(ROOT/'src'));sys.dont_write_bytecode=True
import run_stage1c as runner
original=runner.dispatch
sample='controlled-v1/normal_mixing/00'
def deny(*a,**k):raise RuntimeError('REVIEW_NETWORK_DISABLED')
socket.create_connection=deny;socket.socket.connect=deny;socket.getaddrinfo=deny

def mutate(case,doc,method):
 out=original(doc,method)
 if case=='full_missing_output' and method=='FULL_INTERVAL':
  out=copy.deepcopy(out);out.update(addresses={},events={},joint_by_asset={},positive_addresses=[])
 elif case in ('haircut_missing_witness_bad_point','haircut_bad_point_with_valid_witness') and method=='HAIRCUT':
  out=copy.deepcopy(out)
  if case=='haircut_missing_witness_bad_point':out['allocation_raw']=None
  for section in ('addresses','joint_by_asset'):
   for row in out[section].values():row['point_raw']='999';row['source_amount_raw']='999'
 elif case=='balance_ablation_non_nested' and method=='BALANCE_INFORMATION_REMOVED':
  out=copy.deepcopy(out)
  for row in out['joint_by_asset'].values():row['lower_raw']='2';row['upper_raw']='2'
 return out
reports=[]
for case in ('normal_positive_control','full_missing_output','haircut_missing_witness_bad_point','haircut_bad_point_with_valid_witness','balance_ablation_non_nested'):
 dest=R/'gate_probe_outputs_final'/case
 args=['run_stage1c.py','--tree',str(ROOT),'--kind','min','--selection',sample,'--output',str(dest)]
 log=io.StringIO();err=io.StringIO()
 with patch.object(runner,'dispatch',side_effect=lambda doc,method:mutate(case,doc,method)),patch.object(sys,'argv',args),redirect_stdout(log),redirect_stderr(err):
  code=runner.main()
 p=dest/'samples/controlled-v1_normal_mixing_00'
 idx=json.loads((dest/'RESULTS_INDEX.json').read_text()) if (dest/'RESULTS_INDEX.json').exists() else {}
 ev=json.loads((p/'EVALUATION.json').read_text()) if (p/'EVALUATION.json').exists() else {}
 ab=json.loads((p/'ABLATIONS.json').read_text()) if (p/'ABLATIONS.json').exists() else None
 (dest/'probe_stdout.log').write_text(log.getvalue());(dest/'probe_stderr.log').write_text(err.getvalue())
 reports.append({'case':case,'cli_exit_code':code,'run_status':idx.get('status'),'passed':idx.get('passed'),'evaluation_passed':ev.get('passed'),'comparison_count':len(ev.get('comparisons',[])),'evaluation_errors':ev.get('errors'),'full_metrics':ev.get('address_metrics',{}).get('FULL_INTERVAL'),'haircut_audit':ev.get('haircut_full_assignment_audit'),'ablation_comparison':ab,'expected_exit_code':0 if case=='normal_positive_control' else 1})
(R/'evaluation_gate_probe_results.json').write_text(json.dumps({'scope':'Synthetic output-fault injection; no evidence of corrupted saved Stage1C output','tests':reports,'reproduced_fail_open_cases':sum(r['case']!='normal_positive_control' and r['cli_exit_code']==0 for r in reports),'source_modified':False},indent=2))
print(json.dumps(reports,indent=2))
