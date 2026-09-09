"""Staging-only synthetic/affected tests; deny network and bind candidate hashes."""
from pathlib import Path
import hashlib,io,json,socket,sys,time,unittest

S=Path(__file__).resolve().parent
R=S.parents[1]
sys.dont_write_bytecode=True
sys.path[:0]=[str(S/'src'),str(S/'scripts'),str(S/'tests'),str(R/'code/src'),str(R/'code')]
paths=[S/'src/stage1d_transfers_acquisition.py',S/'scripts/execute_current_transfers.py']
def inventory():return {p.relative_to(S).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
network=[]
def blocked(*args,**kw):network.append('ATTEMPT');raise AssertionError('Offline targeted tests prohibit network')
socket.socket.connect=blocked;socket.create_connection=blocked
before=inventory();start=time.perf_counter();cpu=time.process_time()
modules=['test_transfers_post_cache_selection','test_transfers_new_plan_id','test_transfers_acquisition','test_transfers_need_overlap',
         'test_binding_batch','test_transfers_timestamp_mapping','test_current_transfers_driver']
def flatten(suite):
    for item in suite:
        if isinstance(item,unittest.TestSuite):yield from flatten(item)
        else:yield item
unique={test.id():test for name in modules for test in flatten(unittest.defaultTestLoader.loadTestsFromName(name))}
suite=unittest.TestSuite(unique.values())
stream=io.StringIO();result=unittest.TextTestRunner(stream=stream,verbosity=2).run(suite)
(S/'TEST_LOG.txt').write_text(stream.getvalue(),encoding='utf-8')
after=inventory()
receipt={'status':'PASS' if result.wasSuccessful() and before==after and not network else 'FAIL',
 'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),
 'source_before':before,'source_after':after,'source_stable':before==after,'modules':modules,
 'network_attempts':len(network),'seconds':time.perf_counter()-start,'cpu_seconds':time.process_time()-cpu,
 'production_write_allowed':False,'scratch_root':str(S),'formal_methods_run':False}
(S/'TEST_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
print(json.dumps(receipt));print(stream.getvalue()[-10000:])
raise SystemExit(0 if receipt['status']=='PASS' else 1)
