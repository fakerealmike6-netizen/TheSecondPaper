from pathlib import Path
import hashlib,json,socket,sys,time,unittest
S=Path(__file__).resolve().parent;R=S.parents[1]
sys.path[:0]=[str(S),str(R/'staging/unknown_cost_boundary_core_candidate/src'),
    str(R/'staging/unknown_cost_boundary_context_candidate/src'),str(R/'code/src')]
def blocked(*a,**k):raise AssertionError('Synthetic report checks must be offline')
socket.socket.connect=blocked;socket.create_connection=blocked
import test_report
started=time.perf_counter();cpu=time.process_time()
sources={}
for module in tuple(sys.modules.values()):
 p=getattr(module,'__file__',None)
 if p and Path(p).resolve().is_relative_to(R):
  p=Path(p).resolve();sources[str(p.relative_to(R))]=hashlib.sha256(p.read_bytes()).hexdigest()
suite=unittest.defaultTestLoader.loadTestsFromModule(test_report)
result=unittest.TextTestRunner(verbosity=2).run(suite)
stable=all(hashlib.sha256((R/p).read_bytes()).hexdigest()==h for p,h in sources.items())
receipt={'status':'PASS' if result.wasSuccessful() and stable else 'FAIL','tests':result.testsRun,
 'failures':len(result.failures),'errors':len(result.errors),'skips':len(result.skipped),
 'wall_seconds':time.perf_counter()-started,'cpu_seconds':time.process_time()-cpu,
 'source_sha256':sources,'sources_stable':stable,'network_calls':0,'production_reads':'SOURCE_ONLY',
 'production_writes':0,'scope':'Synthetic report helper only; not production Registry/context or full suite'}
(S/'TEST_RECEIPT.json').write_text(json.dumps(receipt,sort_keys=True,indent=2)+'\n',encoding='utf-8')
raise SystemExit(0 if receipt['status']=='PASS' else 1)
