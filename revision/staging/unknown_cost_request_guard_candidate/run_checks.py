"""Only the small synthetic request guard suite; all writes stay in staging."""
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
paths = [STAGE / 'src', STAGE / 'tests',
    REVISION / 'staging/unknown_cost_boundary_registry_candidate/src',
    REVISION / 'staging/unknown_cost_boundary_core_candidate/src',
    REVISION / 'staging/unknown_cost_boundary_context_candidate/src',
    REVISION / 'code/src']
sys.path[:0] = [str(p) for p in paths]


def denied(*args, **kwargs):
    raise AssertionError('Network/optimizer is forbidden in this bounded synthetic suite')


socket.socket.connect = denied
socket.create_connection = denied
import context_lp_r3
context_lp_r3.linprog = denied
sources = [STAGE / 'src/stage1d_cost_request_guard.py', STAGE / 'tests/test_cost_request_guard.py',
    paths[2] / 'stage1d_unknown_cost_registry.py', paths[3] / 'stage1d_unknown_cost_boundary.py',
    paths[4] / 'stage1d_cost_boundary_context.py', paths[5] / 'collector.py', paths[5] / 'stage1d_window.py']


def inventory():
    return {str(p.relative_to(REVISION)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}


before = inventory(); wall, cpu = time.perf_counter(), time.process_time()
output = io.StringIO()
suite = unittest.defaultTestLoader.loadTestsFromName('test_cost_request_guard')
result = unittest.TextTestRunner(stream=output, verbosity=2).run(suite)
after = inventory()
receipt = {'status': 'PASS' if result.wasSuccessful() and before == after else 'FAIL',
    'tests': result.testsRun, 'failures': len(result.failures), 'errors': len(result.errors), 'skipped': len(result.skipped),
    'source_before': before, 'source_after': after, 'source_stable': before == after,
    'wall_seconds': time.perf_counter() - wall, 'cpu_seconds': time.process_time() - cpu,
    'network_calls': 0, 'provider_calls': 0, 'optimizer_calls': 0, 'graph_replays': 0,
    'production_reads': 0, 'production_writes': 0, 'test_kind': 'SYNTHETIC_CONTROLLED_WITH_REAL_REGISTRY_AND_CORE'}
(STAGE / 'TEST_LOG.txt').write_text(output.getvalue(), encoding='utf-8')
(STAGE / 'TEST_RECEIPT.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
history = STAGE / 'TEST_HISTORY.json'
old = json.loads(history.read_text()) if history.exists() else []
history.write_text(json.dumps(old + [receipt], indent=2) + '\n', encoding='utf-8')
print(output.getvalue()); print(json.dumps(receipt))
raise SystemExit(0 if receipt['status'] == 'PASS' else 1)
