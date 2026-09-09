from pathlib import Path
import sys,tempfile,unittest,io,json,time,hashlib
S=Path(__file__).resolve().parent;C=S.parents[1]/'code';(S/'tmp').mkdir(exist_ok=True)
sys.dont_write_bytecode=True;sys.path[:0]=[str(S/'src'),str(S/'tests'),str(C/'src'),str(C/'tests')];tempfile.tempdir=str(S/'tmp')
suite=unittest.TestLoader().loadTestsFromName('test_stage1d_gap_sequence');stream=io.StringIO();wall=time.monotonic();cpu=time.process_time()
result=unittest.TextTestRunner(stream=stream,verbosity=2).run(suite)
receipt={'status':'PASS' if result.wasSuccessful() else 'FAIL','tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'output':stream.getvalue(),'wall_seconds':time.monotonic()-wall,'cpu_seconds':time.process_time()-cpu,'network_calls':0,'production_writes':0,'full_suite_or_formal_method_runs':0,'source_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (S/'src').glob('*.py')}}
(S/'TEST_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8');print(stream.getvalue());print(json.dumps({k:v for k,v in receipt.items() if k!='output'}));raise SystemExit(not result.wasSuccessful())
