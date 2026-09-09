"""Staging-only, no solver/network/production mutation; bounded synthetic work."""
from pathlib import Path
import ast, copy, hashlib, importlib.util, io, json, random, socket, sys, time, unittest

S = Path(__file__).resolve().parents[1]
R = S.parents[1]
C = R / 'code'
sys.dont_write_bytecode = True
sys.path[:0] = [str(S / 'src'), str(S / 'tests'), str(C / 'src'), str(C / 'tests')]
started_wall, started_cpu = time.perf_counter(), time.process_time()


def deny(*args, **kwargs):
    raise AssertionError('Network is forbidden in this bounded offline check')


socket.create_connection = deny
socket.socket.connect = deny
socket.socket.connect_ex = deny


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def budget():
    if time.process_time() - started_cpu > 50:
        raise AssertionError('Bounded check safety stop before 60-second CPU budget')


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


base = load('frozen_normalizer_before_child_index', S / 'base/context_ledger_r3.py')
import context_ledger_r3 as current
import test_context_trace_child_index as cases

assert Path(current.__file__).resolve() == (S / 'src/context_ledger_r3.py').resolve()
for path in [S / 'src/context_ledger_r3.py', S / 'tests/test_context_trace_child_index.py']:
    ast.parse(path.read_text(encoding='utf-8'))


def outcome(function, rows):
    try:
        value = function(copy.deepcopy(rows))
        # JSON text equality also binds dict/list insertion order, not only values.
        return {'returned_json': json.dumps(value, ensure_ascii=False, separators=(',', ':'))}
    except (ValueError, KeyError, TypeError) as error:
        return {'exception_type': type(error).__name__, 'exception_message': str(error)}


checks = []
for name, rows in cases.equivalence_cases().items():
    budget()
    old, new = outcome(base.normalize_rows, rows), outcome(current.normalize_rows, rows)
    assert old == new, name
    checks.append({'case': name, 'rows': len(rows), 'exact_return_order_or_exception_equal': True,
                   'outcome_sha256': hashlib.sha256(json.dumps(new, sort_keys=True).encode()).hexdigest()})

randomizer = random.Random(20260909)
for n in range(40):
    rows = cases.family(1) + cases.family(2) + cases.family(3)
    if n % 2 == 0:
        rows.append(copy.deepcopy(rows[2]))
    if n % 3 == 0:
        rows[3]['success'] = False
    if n % 5 == 0:
        rows[1]['subtraces'] = 3
    randomizer.shuffle(rows)
    assert outcome(base.normalize_rows, rows) == outcome(current.normalize_rows, rows), n
    budget()

suite = unittest.defaultTestLoader.loadTestsFromModule(cases)
old_names = [
    'test_fee_dedup_root_trace_and_windows', 'test_failed_value_rolled_back_fee_kept',
    'test_failed_ancestor_rolls_back_success_child', 'test_delegatecall_value_is_not_capacity',
    'test_contract_create_uses_created_recipient_and_root_is_one_value',
    'test_selfdestruct_value_uses_refund_address', 'test_withdrawal_is_a_block_end_actual_credit',
    'test_cross_source_conflict_is_retained', 'test_gas_price_arithmetic_conflict_not_smoothed',
]
suite.addTests(unittest.defaultTestLoader.loadTestsFromNames([
    'test_context_ledger_r3.ContextLedgerR3Tests.' + name for name in old_names]))
stream = io.StringIO()
test_result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
(S / 'checks/TEST_OUTPUT.txt').write_text(stream.getvalue(), encoding='utf-8')
assert test_result.wasSuccessful(), stream.getvalue()
budget()

timings = []
for count, versions in [(100, ('old', 'new')), (300, ('old', 'new')), (600, ('old', 'new')),
                         (4000, ('new',))]:
    rows = [r for number in range(1, count + 1) for r in cases.family(number)]
    for version in versions:
        budget()
        function = base.normalize_rows if version == 'old' else current.normalize_rows
        wall, cpu = time.perf_counter(), time.process_time()
        result = function(rows)
        elapsed_cpu, elapsed_wall = time.process_time() - cpu, time.perf_counter() - wall
        assert not result['conflicts'] and len(result['flows']) == 5 * count
        timings.append({'version': version, 'synthetic_transactions': count,
            'rows': len(rows), 'merged_traces': 5 * count,
            'cpu_seconds': elapsed_cpu, 'wall_seconds': elapsed_wall,
            'flows': len(result['flows']), 'conflicts': len(result['conflicts'])})
budget()

base_sha = sha(S / 'base/context_ledger_r3.py')
receipt = {'status': 'PASS', 'tests': test_result.testsRun, 'failures': len(test_result.failures),
    'errors': len(test_result.errors), 'new_tests': 10, 'existing_pure_normalizer_tests': len(old_names),
    'equivalence_cases': checks, 'deterministic_permuted_exact_comparisons': 40,
    'cpu_seconds': time.process_time() - started_cpu,
    'wall_seconds': time.perf_counter() - started_wall, 'cpu_budget_seconds': 60,
    'safety_stop_threshold_seconds': 50, 'safety_stop_triggered': False,
    'synthetic_timings': timings, 'all_inputs_synthetic': True,
    'network_calls': 0, 'solver_calls': 0, 'production_mutations': 0,
    'actual_bq_rows_read': 0, 'live_worker_observed_or_controlled': False,
    'base_sha256': base_sha, 'new_sha256': sha(S / 'src/context_ledger_r3.py'),
    'active_source_still_base': sha(C / 'src/context_ledger_r3.py') == base_sha,
    'test_sha256': sha(S / 'tests/test_context_trace_child_index.py')}
(S / 'TEST_EVIDENCE.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
print(json.dumps({k: receipt[k] for k in ('status', 'tests', 'cpu_seconds', 'wall_seconds',
    'active_source_still_base', 'synthetic_timings')}, indent=2))
