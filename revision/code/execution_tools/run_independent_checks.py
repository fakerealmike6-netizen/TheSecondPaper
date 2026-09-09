"""Execute supplied independent algorithms offline after official method timing."""
from pathlib import Path
import argparse,concurrent.futures,hashlib,json,os,platform,subprocess,sys,time
REV=Path(__file__).resolve().parent
NAMES=('independent_controlled_check.py','independent_baseline_check.py','check_statistics_and_metadata.py')
GUARD='''import json,runpy,socket,sys
from pathlib import Path
script,receipt,*arguments=sys.argv[1:]
attempts=[]
def blocked(*args,**kwargs):
 attempts.append('OUTBOUND_BLOCKED')
 raise RuntimeError('INDEPENDENT_CHECK_NETWORK_FORBIDDEN')
socket.create_connection=blocked;socket.socket.connect=blocked;socket.socket.connect_ex=blocked
socket.getaddrinfo=blocked;socket.gethostbyname=blocked;socket.gethostbyname_ex=blocked
sys.argv=[script,*arguments]
try:runpy.run_path(script,run_name='__main__')
finally:Path(receipt).write_text(json.dumps({'network_attempts':attempts,'network_blocked':True}),encoding='utf-8')
'''
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--tree',type=Path,default=REV/'code');parser.add_argument('--batch',type=Path,default=REV/'batch_final');parser.add_argument('--reports',type=Path,default=REV/'reports_final');parser.add_argument('--baseline-tree',type=Path,default=REV/'baseline/min');parser.add_argument('--output',type=Path,default=REV/'independent_results');parser.add_argument('--scripts-root',type=Path,default=REV/'independent_checks');a=parser.parse_args()
    out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
    env={k:v for k,v in os.environ.items() if not any(x in k.upper() for x in ('API_KEY','TOKEN','CREDENTIAL','PASSWORD','SECRET','AUTHORIZATION','ACCESS_KEY','PRIVATE_KEY'))}
    env.update(PYTHONDONTWRITEBYTECODE='1',PYTHONUTF8='1',PYTHONIOENCODING='utf-8')
    common=['--tree',str(a.tree.resolve()),'--batch',str(a.batch.resolve()),'--reports',str(a.reports.resolve()),'--baseline-tree',str(a.baseline_tree.resolve()),'--output-dir',str(out)]
    inputs=[a.tree/'EXPERIMENT_FREEZE.json',a.tree/'REVISION_FREEZE.json',a.tree/'EXPERIMENT_INPUTS.json',a.tree/'controlled_v1/MANIFEST.json',a.batch/'RESULTS_INDEX.json',a.reports/'STATISTICS.json']
    inputs += [root/name for root in (a.tree,a.baseline_tree) for name in ('private/ledger/shared_budget_r4.sqlite','private/FINAL_BUDGET_SNAPSHOT_R4.json','private/CUMULATIVE_REQUEST_COST_ROWS_R4.json')]
    before={str(p.resolve()):sha(p) for p in inputs}
    def run(name):
        source=a.scripts_root.resolve()/name;guard=out/(name+'.guard.json')
        command=[sys.executable,'-B','-c',GUARD,str(source),str(guard),*common]
        start=time.perf_counter();completed=subprocess.run(command,cwd=REV,env=env,capture_output=True)
        stdout=out/(name+'.stdout.txt');stderr=out/(name+'.stderr.txt')
        stdout.write_bytes(completed.stdout);stderr.write_bytes(completed.stderr)
        guard_result=json.loads(guard.read_text(encoding='utf-8')) if guard.exists() else {'missing':True}
        receipt={'script':name,'script_sha256':sha(source),'command_display':[sys.executable,'-B','<recorded_network_guard>',str(source),*common],
                 'actual_argument_vector':command,'guard_source_sha256':hashlib.sha256(GUARD.encode()).hexdigest(),'return_code':completed.returncode,
                 'elapsed_seconds':time.perf_counter()-start,'stdout':stdout.name,'stdout_sha256':sha(stdout),'stderr':stderr.name,'stderr_sha256':sha(stderr),
                 'guard':guard_result,'passed':completed.returncode==0 and guard_result.get('network_blocked') is True and not guard_result.get('network_attempts')}
        (out/(name+'.execution.json')).write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
        print(json.dumps({'script':name,'return_code':completed.returncode,'passed':receipt['passed']},ensure_ascii=False),flush=True)
        return receipt
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:rows=list(pool.map(run,NAMES))
    after={str(p.resolve()):sha(p) for p in inputs}
    expected_outputs=('independent_controlled_results.json','independent_baseline_results.json','statistics_metadata_checks.json')
    result={'schema_version':'stage1c-r1-independent-check-execution-v1','passed':all(r['passed'] for r in rows) and before==after,
            'runtime':{'platform':platform.platform(),'python':platform.python_version(),'windows_executed':platform.system()=='Windows','linux_executed':False},
            'timing_scope':'Independent verification executed only after official method timing ended; these elapsed times are not algorithm benchmark measurements.',
            'source_adaptation_manifest_sha256':sha(a.scripts_root/'ADAPTATION_MANIFEST.json'),'runs':rows,'checked_input_hashes_before':before,
            'checked_inputs_unchanged':before==after,'output_hashes':{name:sha(out/name) for name in expected_outputs if (out/name).is_file()},
            'external_review_status':'PENDING_REVIEW; internal rerun of supplied external independent algorithms, not a new independent external acceptance'}
    (out/'INDEPENDENT_CHECK_EXECUTION_RECEIPT.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'passed':result['passed'],'receipt':str(out/'INDEPENDENT_CHECK_EXECUTION_RECEIPT.json')}))
    return 0 if result['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
