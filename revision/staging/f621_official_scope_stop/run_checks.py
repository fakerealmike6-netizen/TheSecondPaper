from pathlib import Path
import hashlib
import io
import json
import time
import unittest
import test_candidate

S = Path(__file__).resolve().parent
C = S.parent.parent / 'code'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
names = ('stage1d_role_adoption.py', 'stage1d_closure_scope.py')
before = {n: sha(C / 'src' / n) for n in names}
stream = io.StringIO(); start = time.perf_counter(); cpu = time.process_time()
result = unittest.TextTestRunner(stream=stream, verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(test_candidate))
receipt = {'status': 'PASS' if result.wasSuccessful() else 'FAIL',
           'tests': result.testsRun, 'failures': len(result.failures), 'errors': len(result.errors),
           'source_sha256': before, 'source_stable': before == {n: sha(C / 'src' / n) for n in names},
           'wall_seconds': time.perf_counter() - start, 'cpu_seconds': time.process_time() - cpu,
           'network_calls': 0, 'production_writes': 0, 'formal_method_runs': 0,
           'bundle_manifest_sha256': sha(S / 'candidate/BUNDLE_MANIFEST.json'), 'output': stream.getvalue()}
(S / 'CHECKS.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print(stream.getvalue()); print(json.dumps({k:v for k,v in receipt.items() if k != 'output'}, indent=2))
raise SystemExit(0 if result.wasSuccessful() and receipt['source_stable'] else 1)
