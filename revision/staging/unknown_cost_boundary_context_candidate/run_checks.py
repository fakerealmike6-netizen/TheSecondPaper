"""Run only this bounded synthetic context suite, never an optimizer/provider."""
import hashlib
import io
import json
from pathlib import Path
import socket
import sys
import time
import unittest

sys.dont_write_bytecode = True
STAGE = Path(__file__).resolve().parent
REVISION = STAGE.parent.parent
CORE = REVISION / 'staging/unknown_cost_boundary_core_candidate/src'
sys.path[:0] = [str(STAGE / 'src'), str(CORE), str(STAGE / 'tests'),
    str(REVISION / 'code/src'), str(REVISION / 'code/tests')]


def denied(*args, **kwargs):
    raise AssertionError('Network and optimizer are not authorized for this bounded check')


socket.socket.connect = denied
socket.create_connection = denied
import context_lp_r3
context_lp_r3.linprog = denied
sources = list(sorted((STAGE / 'src').glob('*.py'))) + [CORE / 'stage1d_unknown_cost_boundary.py']


def inventory():
    return {str(p.relative_to(REVISION)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}


before = inventory()
wall, cpu = time.perf_counter(), time.process_time()
suite = unittest.defaultTestLoader.loadTestsFromName('test_cost_boundary_context')
output = io.StringIO()
result = unittest.TextTestRunner(stream=output, verbosity=2).run(suite)
after = inventory()
receipt = {'status': 'PASS' if result.wasSuccessful() and before == after else 'FAIL',
    'tests': result.testsRun, 'failures': len(result.failures), 'errors': len(result.errors),
    'skipped': len(result.skipped), 'source_before': before, 'source_after': after,
    'source_stable': before == after, 'wall_seconds': time.perf_counter() - wall,
    'cpu_seconds': time.process_time() - cpu, 'network_calls': 0, 'optimizer_calls': 0,
    'seven_method_dispatches': 0, 'production_writes': 0, 'actual_graph_reads': 0,
    'matrix_construction_and_independent_exact_witness_audit_only': True}
(STAGE / 'TEST_LOG.txt').write_text(output.getvalue(), encoding='utf-8')
(STAGE / 'TEST_RECEIPT.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
print(output.getvalue())
print(json.dumps(receipt))
raise SystemExit(0 if receipt['status'] == 'PASS' else 1)
