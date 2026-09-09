from pathlib import Path
import sys,os,json,unittest,socket,time,traceback
R=Path(__file__).resolve().parents[1];C=R/'code';os.chdir(C)
sys.path[:0]=[str(C/'src'),str(C/'tests'),str(C)]
(R/'checks').mkdir(exist_ok=True)
attempts=[]
def blocked(*a,**k):attempts.append('BLOCKED_NETWORK');raise RuntimeError('Offline test network forbidden')
socket.socket.connect=blocked;socket.create_connection=blocked
names=['test_stage1d_runtime','test_stage1d_window','test_stage1d_acquisition_cache','test_stage1d_native_candidate','test_stage1d_native_candidate_batch','test_stage1d_native_candidate_split','test_stage1d_bigquery_probe','test_stage1d_bigquery_jobs','test_stage1d_context_recovery','test_stage1d_context_online','test_stage1d_candidate_batch','test_stage1d_experiments','test_stage1d_query_window_semantic']
start=time.perf_counter();suite=unittest.defaultTestLoader.loadTestsFromNames(names)
with (R/'checks/WINDOW_ROUTING_TESTS.txt').open('w',encoding='utf-8') as log:result=unittest.TextTestRunner(stream=log,verbosity=2).run(suite)
summary={'status':'PASS' if result.wasSuccessful() and not attempts else 'FAIL','tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),'attempted_network_calls':len(attempts),'actual_external_requests':0,'seconds':time.perf_counter()-start}
(R/'checks/WINDOW_ROUTING_TESTS.json').write_text(json.dumps(summary,indent=2),encoding='utf-8');print(json.dumps(summary))
for case,detail in result.failures+result.errors:print(str(case)+'\n'+detail)
raise SystemExit(0 if summary['status']=='PASS' else 1)
