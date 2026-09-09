"""Serial R2 job lifecycle with persisted cumulative online-time accounting.

One invocation handles one frozen work unit. It never retries an uncertain
submission/page, changes the probe scope, or resets historical resource use.
"""
import argparse,json,time,os
from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal
from page_attempts import atomic_json
from legacy_guard_r4 import reject_legacy_workspace

def now():return datetime.now(timezone.utc).isoformat()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8'))
def clock_usage(work,probe):
    base=read(work/'baseline/FINAL_RESOURCE_ADDENDUM.json')
    used=Decimal(base['per_probe_cumulative_upper_seconds'])
    for p in (work/'private/online_sessions').glob('*.json'):
        v=read(p)
        if v['probe'] not in (probe,'SHARED'):continue
        if not v.get('closed'):raise RuntimeError('Unresolved previous online session; inspect before new job')
        used+=Decimal(str(v['elapsed_seconds']))
    return used

def execute(work,probe,freeze,label,kind='candidate',performance='medium'):
    reject_legacy_workspace(work, 'pilot_actions_r2.execute')
    from dune_r2 import RevisionLive
    work=Path(work).resolve();freeze=Path(freeze).resolve()
    if not freeze.is_relative_to(work):raise ValueError('Frozen work must be inside revision')
    policy=read(work/'configs/STAGE1B_R2_POLICY.json')
    probes=[p['name'] for p in policy['query_pilots']]
    if probe not in probes+['SHARED']:raise ValueError('Only fixed pilots or shared necessary context')
    names=probes if probe=='SHARED' else [probe]
    remaining=min(Decimal(5400)-clock_usage(work,p) for p in names)
    if remaining<=30:raise RuntimeError('CUMULATIVE_ONLINE_TIME_LIMIT')
    old_bytes=read(work/'baseline/FINAL_RESOURCE_ADDENDUM.json')['bytes']['conservative_physical_directory_upper_with_unknown_read_reserve']
    base_raw={r['destination'] for r in read(work/'manifests/R1_INPUT_MAPPING.json') if r['status']=='COPIED_IDENTICAL' and r['destination'].startswith('raw/')}
    new_bytes=sum(p.stat().st_size for p in (work/'raw').rglob('*') if p.is_file() and p.relative_to(work).as_posix() not in base_raw)
    if old_bytes+new_bytes+16*1024*1024>536870912:raise RuntimeError('CUMULATIVE_RAW_RESPONSE_LIMIT')
    sessions=work/'private/online_sessions';sessions.mkdir(parents=True,exist_ok=True)
    session=sessions/(label+'.json')
    if session.exists():raise RuntimeError('Work unit already attempted; inspect durable job, do not repeat')
    lock=work/'private/network_worker.lock'
    with lock.open('x') as h:h.write(json.dumps({'pid':os.getpid(),'label':label,'started_at_utc':now()}))
    start=time.monotonic();record={'label':label,'probe':probe,'kind':kind,'started_at_utc':now(),'remaining_before_seconds':str(remaining),'closed':False,'worker_pid':os.getpid()}
    atomic_json(session,record)
    try:
        live=RevisionLive(work)
        result=live.submit(freeze.parent/'query.sql',label,kind=kind,freeze_manifest=freeze,performance=performance)
        folder=Path(result['job_folder']);record['job_folder']=str(folder);atomic_json(session,record)
        if not result.get('execution_id'):raise RuntimeError('SUBMISSION_UNCERTAIN_NO_AUTOMATIC_RETRY')
        while True:
            state=read(folder/'job.json')
            if state['state'] in ('QUERY_STATE_COMPLETED','QUERY_STATE_FAILED','QUERY_STATE_CANCELLED','QUERY_STATE_EXPIRED'):break
            if time.monotonic()-start+35>float(remaining):
                record['status']='ONLINE_LIMIT_WITH_PENDING_EXECUTION';raise RuntimeError('Online time exhausted; execution retained')
            time.sleep(5);result=live.poll(folder)
            print(json.dumps({'label':label,'poll':result},default=str),flush=True)
        if state['state']!='QUERY_STATE_COMPLETED':
            if state.get('execution_cost_credits') is not None:record['settlement']=live.settle(folder)
            record['status']=state['state'];return record
        offset=0
        while True:
            if time.monotonic()-start+35>float(remaining):raise RuntimeError('Online time exhausted before next page')
            result=live.export(folder,limit=1000,offset=offset)
            print(json.dumps({'label':label,'export':result},default=str),flush=True)
            progress=result.get('progress',result)
            if progress.get('complete'):break
            offset=progress['next_offset']
        record['settlement']=live.settle(folder)
        record['status']='COMPLETED_EXPORTED'
        return record
    except Exception as exc:
        record.update(status=record.get('status','STOPPED_REQUIRES_INSPECTION'),exception_type=type(exc).__name__,reason=str(exc))
        raise
    finally:
        record.update(closed=True,ended_at_utc=now(),elapsed_seconds=round(time.monotonic()-start,6),clock_includes_entire_submit_to_stop_interval=True)
        atomic_json(session,record)
        if lock.resolve().is_relative_to(work):lock.unlink()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);p.add_argument('--probe',required=True);p.add_argument('--freeze',type=Path,required=True);p.add_argument('--label',required=True);p.add_argument('--kind',default='candidate');a=p.parse_args()
    print(json.dumps(execute(a.work,a.probe,a.freeze,a.label,a.kind),indent=2,default=str))
