import hashlib, importlib, json, os, socket, sys, time, unittest
from pathlib import Path
S=Path(__file__).resolve().parent
C=S.parents[1]/'code'
sys.dont_write_bytecode=True
sys.path[:0]=[str(S/'src'),str(S/'tests'),str(C/'src')]
os.environ['PYTHONDONTWRITEBYTECODE']='1'
os.environ['TMP']=str(S/'checks');os.environ['TEMP']=str(S/'checks')
def blocked(*args,**kwargs): raise AssertionError('No network authorized in controlled staging tests')
socket.socket.connect=blocked;socket.create_connection=blocked
names=sys.argv[1:] or ['test_stage1d_shared_evidence','test_stage1d_bq_portable',
    'test_stage1d_semantic_units','test_stage1d_multiasset_pipeline','test_stage1d_multiasset_context']
files=list((S/'src').glob('*.py'))
before={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
start=time.perf_counter();cpu=time.process_time()
suite=unittest.defaultTestLoader.loadTestsFromNames(names)
with (S/'checks'/'TEST_LOG.txt').open('w',encoding='utf8') as log:
    result=unittest.TextTestRunner(stream=log,verbosity=2).run(suite)
after={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
receipt={'status':'PASS' if result.wasSuccessful() and before==after else 'FAIL',
 'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),
 'wall_seconds':time.perf_counter()-start,'cpu_seconds':time.process_time()-cpu,
 'network':'BLOCKED','production_data_read':False,'stage_source_stable':before==after,
 'source_sha256':after,'test_modules':names,
 'synthetic_measurements':getattr(sys.modules.get('test_stage1d_shared_evidence'),'MEASUREMENTS',{}),
 'loaded_sources':{n:getattr(m,'__file__',None) for n,m in sys.modules.items() if n in [p.stem for p in files]}}
(S/'checks'/'TEST_RECEIPT.json').write_text(json.dumps(receipt,indent=2),encoding='utf8')
print(json.dumps(receipt,indent=2))
if not result.wasSuccessful():
    print((S/'checks'/'TEST_LOG.txt').read_text(encoding='utf8')[-18000:])
sys.exit(0 if receipt['status']=='PASS' else 1)
