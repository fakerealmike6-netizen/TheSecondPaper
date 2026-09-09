"""Runs only bounded synthetic cases; all temp writes stay in this staging directory."""
from pathlib import Path
import sys,tempfile,unittest,io,json,hashlib,time
S=Path(__file__).resolve().parent;C=S.parents[1]/'code'
sys.dont_write_bytecode=True
sys.path[:0]=[str(S/'tests'),str(C/'tests'),str(C/'src')]
tempfile.tempdir=str(S/'tmp')
names=['test_stage1d_context_batch_size.ContextBatchSizeTests',
       'test_stage1d_context_online.ContextOnlineTests.test_all_selection_includes_leaf_after_debit_and_resource_gap_is_local',
       'test_context_fixture_asset_guard.FixtureAssetGuardTests']
suite=unittest.TestLoader().loadTestsFromNames(names);stream=io.StringIO();start=time.monotonic();cpu=time.process_time()
result=unittest.TextTestRunner(stream=stream,verbosity=2).run(suite)
receipt={'status':'PASS' if result.wasSuccessful() else 'FAIL','tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'output':stream.getvalue(),'wall_seconds':time.monotonic()-start,'cpu_seconds':time.process_time()-cpu,
         'scope':'Eight targeted synthetic cases only; no network, production files/DB writes, LP, formal algorithm or full suite.',
         'test_names':names,'source_manifest_sha256':hashlib.sha256((S/'SOURCE_MANIFEST.json').read_bytes()).hexdigest()}
(S/'TEST_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
print(stream.getvalue());print(json.dumps({k:v for k,v in receipt.items() if k!='output'}))
raise SystemExit(0 if result.wasSuccessful() else 1)
