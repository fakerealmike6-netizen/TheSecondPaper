from pathlib import Path
import hashlib,io,json,socket,sys,time,unittest
S=Path(__file__).resolve().parent;R=S.parents[1]
sys.dont_write_bytecode=True;sys.path[:0]=[str(S),str(S/'tests'),str(R/'code/src'),str(R/'code')]
network=[]
def denied(*a,**kw):network.append(1);raise AssertionError('Tests are offline')
socket.socket.connect=denied;socket.create_connection=denied
source=S/'prepare_historical_code.py';before=hashlib.sha256(source.read_bytes()).hexdigest();start=time.perf_counter();cpu=time.process_time()
suite=unittest.defaultTestLoader.loadTestsFromName('test_prepare_historical_code');stream=io.StringIO()
result=unittest.TextTestRunner(stream=stream,verbosity=2).run(suite);after=hashlib.sha256(source.read_bytes()).hexdigest()
(S/'TEST_LOG.txt').write_text(stream.getvalue(),encoding='utf-8')
receipt={'status':'PASS' if result.wasSuccessful() and before==after and not network else 'FAIL','tests':result.testsRun,
 'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),'source_before':before,'source_after':after,
 'source_stable':before==after,'network_attempts':len(network),'production_reads':'Runtime validation source only; no production preparation or runtime DB',
 'scratch':str(S),'seconds':time.perf_counter()-start,'cpu_seconds':time.process_time()-cpu}
(S/'TEST_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8');print(json.dumps(receipt));print(stream.getvalue())
raise SystemExit(0 if receipt['status']=='PASS' else 1)
