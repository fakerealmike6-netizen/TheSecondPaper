from pathlib import Path
import difflib,hashlib,json,socket,sys,time,unittest
S=Path(__file__).resolve().parent;C=S.parents[1]/'code'
sys.dont_write_bytecode=True;sys.path[:0]=[str(S/'src'),str(S/'tests'),str(C/'src')]
def blocked(*args,**kwargs):raise AssertionError('No network authorized')
socket.socket.connect=blocked;socket.create_connection=blocked
source=S/'src/stage1d_batch_binding_route.py';active=C/'src'/source.name
before=hashlib.sha256(source.read_bytes()).hexdigest();base=hashlib.sha256(active.read_bytes()).hexdigest()
start=time.perf_counter();cpu=time.process_time()
names=['test_portable_dependency_fields','test_stage1d_batch_binding_route','test_stage1d_bq_portable']
suite=unittest.defaultTestLoader.loadTestsFromNames(names)
with (S/'checks/TEST_LOG.txt').open('w',encoding='utf8') as log:
    result=unittest.TextTestRunner(stream=log,verbosity=2).run(suite)
after=hashlib.sha256(source.read_bytes()).hexdigest()
receipt={'status':'PASS' if result.wasSuccessful() and before==after else 'FAIL','tests':result.testsRun,
    'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),
    'wall_seconds':time.perf_counter()-start,'cpu_seconds':time.process_time()-cpu,
    'base_sha256':base,'new_sha256':after,'source_stable':before==after,'network':'BLOCKED',
    'production_data_read':False,'production_modified':False,'test_modules':names,
    'loaded_route_source':sys.modules['stage1d_batch_binding_route'].__file__,
    'portable_verifier_sha256':hashlib.sha256((C/'src/stage1d_bq_portable.py').read_bytes()).hexdigest()}
(S/'checks/TEST_RECEIPT.json').write_text(json.dumps(receipt,indent=2),encoding='utf8')
(S/'MINIMAL_SOURCE_DIFF.patch').write_text(''.join(difflib.unified_diff(active.read_text().splitlines(True),
    source.read_text().splitlines(True),fromfile='a/src/'+source.name,tofile='b/src/'+source.name)),encoding='utf8')
print(json.dumps(receipt,indent=2))
if not result.wasSuccessful():print((S/'checks/TEST_LOG.txt').read_text()[-12000:])
sys.exit(0 if receipt['status']=='PASS' else 1)
