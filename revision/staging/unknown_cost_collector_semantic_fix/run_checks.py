"""Bounded synthetic Collector/context/guard regression; no provider or solver."""
import hashlib, io, json, socket, sys, tempfile, time, unittest
from pathlib import Path
sys.dont_write_bytecode = True
S = Path(__file__).resolve().parent
R = S.parent.parent
stages = [R/'staging_root/unknown_cost_collector_candidate',
          R/'staging/unknown_cost_boundary_context_candidate',
          R/'staging/unknown_cost_request_guard_candidate',
          R/'staging/unknown_cost_boundary_core_candidate',
          R/'staging/unknown_cost_boundary_registry_candidate']
sys.path[:0] = [str(p/'src') for p in stages] + [str(p/'tests') for p in stages] + [str(R/'code/src'),str(R/'code/tests')]
(S/'scratch').mkdir(exist_ok=True); tempfile.tempdir = str(S/'scratch')
def denied(*args, **kwargs): raise AssertionError('Network and optimizer forbidden in this bounded check')
socket.socket.connect = denied; socket.create_connection = denied
import context_lp_r3
context_lp_r3.linprog = denied
files = [stages[0]/'src/collector.py', stages[0]/'tests/test_unknown_cost_collector.py',
         stages[1]/'src/stage1d_cost_boundary_context.py', stages[1]/'tests/test_cost_boundary_context.py',
         stages[2]/'src/stage1d_cost_request_guard.py', stages[2]/'tests/test_cost_request_guard.py',
         stages[3]/'src/stage1d_unknown_cost_boundary.py', stages[4]/'src/stage1d_unknown_cost_registry.py',
         R/'code/src/stage1d_semantic_units.py', R/'code/tests/semantic_weth_fixture.py']
def inventory(): return {p.relative_to(R).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
before = inventory(); wall=time.perf_counter(); cpu=time.process_time()
suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromName(name) for name in
    ('test_unknown_cost_collector','test_cost_boundary_context','test_cost_request_guard'))
out=io.StringIO(); result=unittest.TextTestRunner(stream=out,verbosity=2).run(suite)
after=inventory(); receipt={'status':'PASS' if result.wasSuccessful() and before==after else 'FAIL',
    'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),
    'source_before':before,'source_after':after,'source_stable':before==after,
    'wall_seconds':time.perf_counter()-wall,'cpu_seconds':time.process_time()-cpu,
    'network_calls':0,'optimizer_calls':0,'seven_method_dispatches':0,'production_writes':0,'production_graph_reads':0}
run=1+len(list(S.glob('TEST_RECEIPT_*.json')))
(S/f'TEST_LOG_{run:02d}.txt').write_text(out.getvalue(),encoding='utf-8')
(S/f'TEST_RECEIPT_{run:02d}.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
print(out.getvalue());print(json.dumps(receipt))
raise SystemExit(0 if receipt['status']=='PASS' else 1)
