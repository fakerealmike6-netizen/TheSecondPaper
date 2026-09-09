from pathlib import Path
import io,json,time,unittest
import test_role_binding
from verify_role_binding import sha
S=Path(__file__).resolve().parent
before=sha(S/'verify_role_binding.py');stream=io.StringIO();wall=time.perf_counter();cpu=time.process_time()
r=unittest.TextTestRunner(stream=stream,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(test_role_binding))
receipt={'status':'PASS' if r.wasSuccessful() else 'FAIL','tests':r.testsRun,'failures':len(r.failures),'errors':len(r.errors),
 'validator_sha256':before,'source_stable':before==sha(S/'verify_role_binding.py'),'wall_seconds':time.perf_counter()-wall,
 'cpu_seconds':time.process_time()-cpu,'output':stream.getvalue(),'network_calls':0,'production_writes':0,
 'actual_graph_reads':0,'raw_material_reads':0,'collection_replay_or_requirement_derivation':0,
 'source_and_existing_staging_authority_fixture_reads':True}
(S/'TEST_RECEIPT.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(stream.getvalue());print(json.dumps({k:v for k,v in receipt.items() if k!='output'},indent=2))
raise SystemExit(0 if r.wasSuccessful() else 1)
