from pathlib import Path
import hashlib,json,socket,sys,time,unittest
S=Path(__file__).resolve().parent;R=S.parents[1]
sys.path[:0]=[str(S/'src'),str(S/'tests'),str(R/'code/src')]
def blocked(*a,**k):raise AssertionError('No network in paid BQ admission tests')
socket.socket.connect=blocked;socket.create_connection=blocked
import test_current_context_paid_bq as tests
def sources():
 result={}
 for module in tuple(sys.modules.values()):
  p=getattr(module,'__file__',None)
  if p and Path(p).resolve().is_relative_to(R):
   p=Path(p).resolve();result[str(p.relative_to(R))]=hashlib.sha256(p.read_bytes()).hexdigest()
 return result
before=sources();start=time.perf_counter();cpu=time.process_time()
result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(tests))
stable=all(hashlib.sha256((R/p).read_bytes()).hexdigest()==s for p,s in before.items())
receipt={'status':'PASS' if result.wasSuccessful() and stable else 'FAIL','tests':result.testsRun,'failures':len(result.failures),
 'errors':len(result.errors),'skips':len(result.skipped),'wall_seconds':time.perf_counter()-start,'cpu_seconds':time.process_time()-cpu,
 'sources':sources(),'preloaded_sources_stable':stable,'network_calls':0,'actual_paid_pages_read':False,'production_writes':0}
(S/'TEST_RECEIPT.json').write_text(json.dumps(receipt,sort_keys=True,indent=2)+'\n',encoding='utf-8')
raise SystemExit(0 if receipt['status']=='PASS' else 1)
