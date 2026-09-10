"""Finish a failed local atomic state write from an already saved acceptance."""
from pathlib import Path
import sys,json,re,sqlite3,argparse,socket
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha
from page_attempts import atomic_json
import stage1d_bq_context_prepare as b
from offline_operation import offline_operation
p=argparse.ArgumentParser();p.add_argument('--job',required=True);a=p.parse_args()
folder=b.inside(C,a.job);state=read(folder/'job.json')
body=read(folder/'submit_response.json');receipt=read(folder/'submit_receipt.json')
with offline_operation(C,'RECOVER_SAVED_DUNE_ACCEPTANCE'):
    if state.get('execution_id') or state['state']!='SUBMITTING_OR_UNCERTAIN':raise ValueError('Only this interrupted local transition is admitted')
    raw=b.checked(C,{'path':receipt['raw_path'],'sha256':receipt['sha256']})
    if read(raw)!=body or receipt.get('http_status')!=200 or receipt.get('error_class') or not re.fullmatch('[A-Z0-9]{26}',body.get('execution_id','')):raise ValueError('Exact complete saved acceptance required')
    db=sqlite3.connect((C/'private/shared_budget_r4.sqlite').resolve().as_uri()+'?mode=ro',uri=True)
    try:found=db.execute('SELECT state FROM r4_dune_sql WHERE job=?',(state['logical_job_id'],)).fetchone()
    finally:db.close()
    if found!=('ACCEPTED_EXECUTION_ID',):raise ValueError('Original acceptance ledger transition is not complete')
    out=R/'operations/integration_repair/dune_local_acceptance'/state['sql_sha256'];out.mkdir(parents=True,exist_ok=False)
    (out/'BEFORE_JOB.json').write_bytes((folder/'job.json').read_bytes())
    state.update(submit_response=body,submit_receipt=receipt,execution_id=body['execution_id'],state=body.get('state','QUERY_STATE_PENDING'))
    atomic_json(folder/'job.json',state)
    atomic_json(out/'RECOVERY.json',{'status':'LOCAL_ACCEPTANCE_STATE_RESTORED','job':b.dep(C,folder/'job.json'),'execution_id':body['execution_id'],'new_requests':0,'new_post':False,'ledger_and_retry_counters_unchanged':True})
    print(json.dumps({'status':'LOCAL_ACCEPTANCE_STATE_RESTORED','execution_id':body['execution_id'],'new_requests':0}),flush=True)
