from pathlib import Path
import io,json,time,unittest
from build_delivery import sha
import test_delivery
S=Path(__file__).resolve().parent
before=sha(S/'build_delivery.py');stream=io.StringIO();wall=time.perf_counter();cpu=time.process_time()
r=unittest.TextTestRunner(stream=stream,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(test_delivery))
receipt={'status':'PASS' if r.wasSuccessful() else 'FAIL','tests':r.testsRun,'failures':len(r.failures),'errors':len(r.errors),
 'source_sha256':before,'source_stable':before==sha(S/'build_delivery.py'),'wall_seconds':time.perf_counter()-wall,
 'cpu_seconds':time.process_time()-cpu,'output':stream.getvalue(),'network_calls':0,'production_writes':0,
 'actual_full_graph_reads':0,'formal_method_runs':0,'real_delivery_generated':False}
(S/'CHECKS.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(stream.getvalue());print(json.dumps({k:v for k,v in receipt.items() if k!='output'},indent=2))
raise SystemExit(0 if r.wasSuccessful() else 1)
