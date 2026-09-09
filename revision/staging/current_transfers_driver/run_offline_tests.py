from pathlib import Path
import hashlib,json,socket,sys,time,unittest
S=Path(__file__).resolve().parent;C=S.parents[1]/'code'
sys.dont_write_bytecode=True;sys.path[:0]=[str(S),str(S/'tests'),str(C/'src')]
def blocked(*args,**kwargs):raise AssertionError('No network authorized')
socket.socket.connect=blocked;socket.create_connection=blocked
p=S/'execute_current_transfers.py';before=hashlib.sha256(p.read_bytes()).hexdigest()
start=time.perf_counter();cpu=time.process_time()
suite=unittest.defaultTestLoader.loadTestsFromNames(['test_current_transfers_driver'])
with (S/'checks/TEST_LOG.txt').open('w',encoding='utf8') as log:
    result=unittest.TextTestRunner(stream=log,verbosity=2).run(suite)
after=hashlib.sha256(p.read_bytes()).hexdigest()
receipt={'status':'PASS' if result.wasSuccessful() and before==after else 'FAIL','tests':result.testsRun,
    'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),
    'wall_seconds':time.perf_counter()-start,'cpu_seconds':time.process_time()-cpu,
    'driver_sha256':after,'driver_stable':before==after,'network':'BLOCKED',
    'real_index_opened':False,'real_provider_data_read':False,'production_modified':False,
    'source_dependencies':{n:hashlib.sha256((C/'src'/n).read_bytes()).hexdigest() for n in
        ('stage1d_transfers_acquisition.py','stage1d_alchemy_transfers.py','stage1d_timestamp_bracket.py',
         'stage1d_legacy_rpc_import.py','context_access_r4.py','stage1d_runtime.py')}}
(S/'checks/TEST_RECEIPT.json').write_text(json.dumps(receipt,indent=2),encoding='utf8')
print(json.dumps(receipt,indent=2))
if not result.wasSuccessful():print((S/'checks/TEST_LOG.txt').read_text()[-10000:])
sys.exit(0 if receipt['status']=='PASS' else 1)
