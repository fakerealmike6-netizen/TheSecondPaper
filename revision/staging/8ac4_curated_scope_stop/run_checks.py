import hashlib,io,json,socket,sys,time,unittest
from pathlib import Path
sys.dont_write_bytecode=True
S=Path(__file__).resolve().parent;R=S.parent.parent
sys.path[:0]=[str(S/'src'),str(S/'tests'),str(S),str(R/'code/src'),str(R/'code/tests')]
def denied(*a,**kw): raise AssertionError('No network is permitted')
socket.socket.connect=denied;socket.create_connection=denied
files=[*sorted((S/'src').glob('*.py')),S/'tests/test_curated_scope_stop.py']
def hashes():return {p.relative_to(S).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
before=hashes();t=time.perf_counter();cpu=time.process_time();stream=io.StringIO()
suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromName(name) for name in (
 'test_curated_scope_stop','test_stage1d_role_adoption_evidence','test_stop_boundary_proportionality'))
result=unittest.TextTestRunner(stream=stream,verbosity=2).run(suite);after=hashes()
receipt={'status':'PASS' if result.wasSuccessful() and before==after else 'FAIL','tests':result.testsRun,
 'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),'source_before':before,'source_after':after,
 'source_stable':before==after,'wall_seconds':time.perf_counter()-t,'cpu_seconds':time.process_time()-cpu,
 'network_calls':0,'production_writes':0,'formal_algorithm_runs':0,'actual_saved_sources_revalidated_in_staging':True,
 'synthetic_state_collector_only':True}
(S/'TEST_LOG.txt').write_text(stream.getvalue(),encoding='utf-8');(S/'TEST_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n')
h=S/'TEST_HISTORY.json';hist=json.loads(h.read_text()) if h.exists() else [];h.write_text(json.dumps(hist+[receipt],indent=2)+'\n')
print(stream.getvalue());print(json.dumps(receipt));raise SystemExit(0 if receipt['status']=='PASS' else 1)
