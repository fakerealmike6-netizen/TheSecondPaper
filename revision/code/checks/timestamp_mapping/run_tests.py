"""Bounded synthetic affected suites; no production source/data mutation."""
from pathlib import Path
import hashlib
import json
import sys
import time
import unittest

CODE=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(CODE/'src'),str(CODE/'tests')]
names=['test_stage1d_timestamp_bracket','test_transfers_timestamp_mapping',
       'test_stage1d_alchemy_transfers','test_transfers_acquisition','test_binding_batch',
       'test_transfers_need_overlap','test_stage1d_batch_binding_route','test_stage1d_bq_portable',
       'test_stage1d_cache_memo','test_stage1d_acquisition_cache']
files=['src/stage1d_timestamp_bracket.py','src/stage1d_alchemy_transfers.py',
       'src/stage1d_transfers_acquisition.py','src/stage1d_batch_binding_route.py']+['tests/'+n+'.py' for n in names]
before={p:hashlib.sha256((CODE/p).read_bytes()).hexdigest() for p in files}
start,cpu=time.perf_counter(),time.process_time()
with (Path(__file__).parent/'TEST_LOG.txt').open('w',encoding='utf-8') as log:
    result=unittest.TextTestRunner(stream=log,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromNames(names))
receipt={'status':'PASS' if result.wasSuccessful() else 'FAIL','tests':result.testsRun,
    'errors':len(result.errors),'failures':len(result.failures),'skipped':len(result.skipped),
    'wall_seconds':time.perf_counter()-start,'cpu_seconds':time.process_time()-cpu,
    'basis':'SYNTHETIC_AFFECTED_TESTS_ONLY','actual_provider_calls':0,'formal_methods_run':False,
    'sha256':before,'source_stable':all(hashlib.sha256((CODE/p).read_bytes()).hexdigest()==v for p,v in before.items()),
    'test_modules':names}
(Path(__file__).parent/'TEST_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
print(json.dumps(receipt,indent=2))
sys.exit(0 if result.wasSuccessful() else 1)
