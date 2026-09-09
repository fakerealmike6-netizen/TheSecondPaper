"""Finite affected tests; scratch isolated and external sockets forbidden."""
from pathlib import Path
import hashlib,importlib,json,socket,sys,tempfile,time,unittest
S=Path(__file__).resolve().parent;R=S.parents[1];C=R/'code'
PATHS=[S/'src',S/'tests',R/'staging/unknown_cost_request_guard_candidate/src',
       R/'staging/unknown_cost_boundary_core_candidate/src',
       R/'staging/unknown_cost_boundary_context_candidate/src',
       R/'staging/unknown_cost_boundary_registry_candidate/src',C/'src',C]
sys.dont_write_bytecode=True;sys.path[:0]=[str(p) for p in PATHS]
tempfile.tempdir=str(S/'scratch');network=[]
def forbidden(*args,**kwargs):network.append('BLOCKED_SOCKET_ATTEMPT');raise AssertionError('Offline tests prohibit sockets')
socket.create_connection=forbidden;socket.socket.connect=forbidden;socket.socket.connect_ex=forbidden
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
sources=list((S/'src').glob('*.py'))+[PATHS[2]/'stage1d_cost_request_guard.py',PATHS[3]/'stage1d_unknown_cost_boundary.py',
          PATHS[4]/'stage1d_cost_boundary_context.py',PATHS[5]/'stage1d_unknown_cost_registry.py']
before={str(p.relative_to(R)):sha(p) for p in sources}
names=['test_bq_cost_guard_entries','test_cost_request_guard','test_stage1d_bigquery_probe',
       'test_stage1d_bigquery_jobs','test_stage1d_bigquery_jobs_recovery','test_stage1d_batch_binding_route','test_bq_context_prepare']
suite=unittest.TestSuite();seen=set()
def add(value):
    if isinstance(value,unittest.TestSuite):
        for test in value:add(test)
    elif value.id() not in seen:seen.add(value.id());suite.addTest(value)
for name in names:add(unittest.defaultTestLoader.loadTestsFromModule(importlib.import_module(name)))
started=time.perf_counter();cpu=time.process_time()
with (S/'TEST_LOG.txt').open('w',encoding='utf8') as out:
    result=unittest.TextTestRunner(stream=out,verbosity=2).run(suite)
after={str(p.relative_to(R)):sha(p) for p in sources}
receipt={'status':'PASS' if result.wasSuccessful() and not network and before==after else 'FAIL',
    'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),
    'network_attempts':len(network),'source_sha256':before,'source_stable':before==after,
    'production_writes':False,'scratch':str(S/'scratch'),'seconds':time.perf_counter()-started,'cpu_seconds':time.process_time()-cpu}
(S/'TEST_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf8');print(json.dumps(receipt))
if not result.wasSuccessful():
    for test,trace in result.failures+result.errors:print(test.id(),trace)
raise SystemExit(0 if receipt['status']=='PASS' else 1)
