"""Run only affected synthetic cache tests with staged source precedence."""
from pathlib import Path
import hashlib,json,socket,sys,time,unittest
S=Path(__file__).resolve().parent;R=S.parents[1];C=R/'code'
sys.dont_write_bytecode=True
def blocked(*a,**k):raise RuntimeError('Controlled staging checks cannot request network')
socket.socket.connect=blocked;socket.create_connection=blocked
sys.path[:0]=[str(S/'src'),str(R/'staging/compact_gap_sequence/src'),str(C/'src'),str(S/'tests')]
names=('test_stage1d_normalization_gap_suffix','test_stage1d_acquisition_cache','test_stage1d_cache_memo','test_stage1d_compact_gap_cache')
modules=[__import__(name) for name in names]
import collector,stage1d_acquisition,stage1d_gap_sequence
assert Path(stage1d_acquisition.__file__).resolve()==S/'src/stage1d_acquisition.py'
assert Path(collector.__file__).resolve()==R/'staging/compact_gap_sequence/src/collector.py'
assert Path(stage1d_gap_sequence.__file__).resolve()==R/'staging/compact_gap_sequence/src/stage1d_gap_sequence.py'
paths={Path(m.__file__).resolve() for m in list(sys.modules.values()) if getattr(m,'__file__',None) and Path(m.__file__).resolve().is_relative_to(R)}
before={str(p.relative_to(R)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths if p.is_file()}
started=time.perf_counter();cpu=time.process_time()
suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromModule(m) for m in modules)
result=unittest.TextTestRunner(verbosity=2).run(suite)
after={str(p.relative_to(R)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths if p.is_file()}
receipt={'status':'PASS' if result.wasSuccessful() and before==after else 'FAIL_OR_SOURCE_CHANGED',
 'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),
 'wall_seconds':time.perf_counter()-started,'cpu_seconds':time.process_time()-cpu,
 'source_sha256':before,'source_stable':before==after,'million_matrix':modules[-1].MATRIX_MEASUREMENTS,
 'network_calls':0,'real_collector_runs':0,'synthetic_collector_runs_only':True,'active_writes':0}
(S/'CHECKS.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf8')
print(json.dumps({k:v for k,v in receipt.items() if k!='source_sha256'}))
raise SystemExit(receipt['status']!='PASS')
