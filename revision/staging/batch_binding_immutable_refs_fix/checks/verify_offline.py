"""Synthetic staging comparison; never reads production raw, network or solver."""
from pathlib import Path
from collections import Counter
import ast, hashlib, importlib.util, io, json, socket, sys, tempfile, time, unittest
from unittest.mock import patch

S = Path(__file__).resolve().parents[1]
C = S.parents[1] / 'code'
sys.dont_write_bytecode = True
sys.path[:0] = [str(S / 'src'), str(S / 'tests'), str(C / 'src'), str(C / 'tests')]
temp_root = S / 'checks/synthetic_temp'
temp_root.mkdir(exist_ok=True)
tempfile.tempdir = str(temp_root)
start_wall, start_cpu = time.perf_counter(), time.process_time()


def deny(*args, **kwargs):
    raise AssertionError('Network forbidden by staging verification')


socket.create_connection = deny
socket.socket.connect = deny
socket.socket.connect_ex = deny


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


spec = importlib.util.spec_from_file_location('frozen_batch_binding_before_refs', S / 'base/stage1d_batch_binding_route.py')
old = importlib.util.module_from_spec(spec)
spec.loader.exec_module(old)
import stage1d_batch_binding_route as new
import stage1d_bq_context_prepare as h
import test_batch_binding_immutable_refs as cases
assert Path(new.__file__).resolve() == (S / 'src/stage1d_batch_binding_route.py').resolve()
for path in (S / 'src/stage1d_batch_binding_route.py', S / 'tests/test_batch_binding_immutable_refs.py'):
    ast.parse(path.read_text(encoding='utf-8'))


def run_import(module, count, complete):
    with tempfile.TemporaryDirectory() as folder:
        work = Path(folder)
        result, states, _ = cases.fixture(work, count, complete)
        with patch.object(module, '_result', return_value=result), \
             patch.object(h, 'sha', wraps=h.sha) as hashes, patch.object(h, 'dep', wraps=h.dep) as deps:
            output = module.import_completed(work, 'PREPARATION.json', states)
        artifacts = {p.relative_to(work).as_posix(): p.read_bytes() for p in work.rglob('*.json')}
        counts = {'hashes': dict(Counter(Path(call.args[0]).name for call in hashes.call_args_list)),
                  'deps': dict(Counter(Path(call.args[1]).name for call in deps.call_args_list))}
        return output, artifacts, counts


comparisons = []
for count, complete in [(0, True), (1, True), (5, True), (5, False)]:
    before, before_files, before_counts = run_import(old, count, complete)
    after, after_files, after_counts = run_import(new, count, complete)
    assert json.dumps(before, separators=(',', ':')) == json.dumps(after, separators=(',', ':'))
    assert before_files == after_files
    comparisons.append({'needs': count, 'native_complete': complete,
        'exact_return_order_equal': True, 'all_saved_paths_and_bytes_equal': True,
        'artifact_count': len(after_files), 'old_counts': before_counts, 'new_counts': after_counts})


def run_verifier(module):
    with tempfile.TemporaryDirectory() as folder:
        work = Path(folder)
        result, states, _ = cases.fixture(work, 5, False)
        with patch.object(module, '_result', return_value=result):
            output = module.import_completed(work, 'PREPARATION.json', states)
        records = [h.read(work / ref['path']) for ref in output['coverage_records']]
        memo = {}
        with patch.object(module, '_result', return_value=result) as replay, \
             patch.object(h, 'digest', wraps=h.digest) as digests, \
             patch.object(module.transfers, 'sha', wraps=module.transfers.sha) as hashes, \
             patch.object(h, 'read', wraps=h.read) as reads:
            results = [module.verify_interval_record(work, record, memo=memo) for record in records]
        return results, {'result_revalidations': replay.call_count, 'proof_digests': digests.call_count,
            'sha_reads': dict(Counter(Path(call.args[0]).name for call in hashes.call_args_list)),
            'json_reads': dict(Counter(Path(call.args[0]).name for call in reads.call_args_list))}


old_results, old_counts = run_verifier(old)
new_results, new_counts = run_verifier(new)
assert json.dumps(old_results, separators=(',', ':')) == json.dumps(new_results, separators=(',', ':'))
suite = unittest.defaultTestLoader.loadTestsFromModule(cases)
existing = ['test_complete_import_reverifies_and_never_widens_seconds', 'test_missing_job_prevents_import',
            'test_original_multipage_to_cache_and_portable_receiver',
            'test_partial_null_root_proof_is_stable_when_no_rpc_is_claimed']
suite.addTests(unittest.defaultTestLoader.loadTestsFromNames([
    'test_stage1d_batch_binding_route.BatchBindingTests.' + name for name in existing]))
stream = io.StringIO()
test_result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
(S / 'checks/TEST_OUTPUT.txt').write_text(stream.getvalue(), encoding='utf-8')
receipt = {'status': 'PASS' if test_result.wasSuccessful() else 'FAIL', 'tests': test_result.testsRun,
    'failures': len(test_result.failures), 'errors': len(test_result.errors),
    'cpu_seconds': time.process_time() - start_cpu, 'wall_seconds': time.perf_counter() - start_wall,
    'import_exact_comparisons': comparisons,
    'five_record_verifier': {'exact_results_equal': True, 'old_counts': old_counts, 'new_counts': new_counts},
    'network_calls': 0, 'solver_calls': 0, 'production_mutations': 0, 'real_payload_reads': 0,
    'base_sha256': sha(S / 'base/stage1d_batch_binding_route.py'),
    'new_sha256': sha(S / 'src/stage1d_batch_binding_route.py'),
    'test_sha256': sha(S / 'tests/test_batch_binding_immutable_refs.py'),
    'active_source_still_base': sha(C / 'src/stage1d_batch_binding_route.py') == sha(S / 'base/stage1d_batch_binding_route.py')}
(S / 'TEST_EVIDENCE.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
print(json.dumps({key: receipt[key] for key in ('status', 'tests', 'failures', 'errors', 'cpu_seconds',
    'wall_seconds', 'five_record_verifier', 'active_source_still_base')}, indent=2))
if not test_result.wasSuccessful():
    print(stream.getvalue())
    raise SystemExit(1)
