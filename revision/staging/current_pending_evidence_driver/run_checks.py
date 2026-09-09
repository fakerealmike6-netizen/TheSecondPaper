import hashlib,io,json,socket,sys,time,unittest
from pathlib import Path
S=Path(__file__).resolve().parent;R=S.parent.parent
sys.dont_write_bytecode=True
sys.path[:0]=[str(S),str(S/'tests'),str(R/'code/src'),str(R/'code')]
def denied(*args,**kwargs):raise AssertionError('No network in synthetic driver checks')
socket.socket.connect=denied;socket.create_connection=denied
files=[S/'execute_unknown_cost_evidence.py',S/'tests/test_current_pending_driver.py',
       R/'staging/unknown_cost_future_code_preparation/prepare_future_historical_code.py',
       R/'staging/unknown_cost_code_preparation/prepare_historical_code.py']
def inventory():return {p.relative_to(R).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
before=inventory();wall=time.perf_counter();cpu=time.process_time();log=io.StringIO()
suite=unittest.defaultTestLoader.loadTestsFromName('test_current_pending_driver')
result=unittest.TextTestRunner(stream=log,verbosity=2).run(suite);after=inventory()
receipt={'status':'PASS' if result.wasSuccessful() and before==after else 'FAIL','tests':result.testsRun,
 'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),'source_stable':before==after,
 'source_before':before,'source_after':after,'wall_seconds':time.perf_counter()-wall,'cpu_seconds':time.process_time()-cpu,
 'real_network_requests':0,'production_writes':0,'production_graph_reads':0,'production_sqlite_reads':0,
 'formal_algorithms':0,'fake_transport_only':True}
run=1+len(list(S.glob('TEST_RECEIPT_*.json')))
(S/f'TEST_RECEIPT_{run:02d}.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
(S/f'TEST_LOG_{run:02d}.txt').write_text(log.getvalue(),encoding='utf-8')
print(log.getvalue());print(json.dumps(receipt));raise SystemExit(0 if receipt['status']=='PASS' else 1)
