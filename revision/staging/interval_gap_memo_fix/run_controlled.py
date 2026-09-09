"""Small synthetic equivalence/timing; neither current graph nor raw is read."""
from pathlib import Path
import hashlib, importlib, io, json, socket, statistics, sys, time, unittest

HERE = Path(__file__).resolve().parent
R = HERE.parents[1]
sys.dont_write_bytecode = True
sys.path[:0] = [str(HERE/'src'), str(HERE/'tests'), str(R/'code/src')]
def blocked(*args, **kwargs): raise RuntimeError('No network in synthetic control')
socket.create_connection = blocked
socket.socket.connect = blocked
source = HERE/'src/stage1d_acquisition.py'
before = hashlib.sha256(source.read_bytes()).hexdigest()
tests = importlib.import_module('test_stage1d_normalization_gap_suffix')
start, cpu = time.perf_counter(), time.process_time()
output = io.StringIO()
result = unittest.TextTestRunner(stream=output, verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(tests))
(HERE/'checks/TEST_LOG.txt').write_text(output.getvalue(), encoding='utf8')
bench = {'scope':'SYNTHETIC_SUFFIX_ONLY_NOT_REAL_QUERY_PROFILE', 'base_items':300, 'matching_records':12,
         'gaps_per_record':400, 'suffix_entries_including_duplicates':4800, 'repetitions':5}
base = [{'reason':'UNCOLLECTED_ADDRESS_INTERVAL', 'detail':str(i)} for i in range(300)]
gaps = [{'reason':'ROOT_EQUIVALENCE_UNRESOLVED_GAP' if i%2 else 'TRANSACTION_FAMILY_BINDING_INCOMPLETE',
         'detail':str(i)} for i in range(400)]
records = [{'normalization_gaps':gaps} for _ in range(12)]
for name, fn in [('original', tests.original_suffix), ('indexed', tests.acquisition._normalization_gap_suffix)]:
    timings = []
    for _ in range(5):
        tick = time.perf_counter(); values = fn(base, records); timings.append(time.perf_counter()-tick)
    bench[name+'_seconds'] = timings; bench[name+'_median_seconds'] = statistics.median(timings)
    bench[name+'_output_sha256'] = hashlib.sha256(tests.canonical(values).encode()).hexdigest()
bench['exact_outputs_equal'] = bench['original_output_sha256'] == bench['indexed_output_sha256']
bench['timing_is_acceptance_threshold'] = False
after = hashlib.sha256(source.read_bytes()).hexdigest()
receipt = {'status':'PASS' if result.wasSuccessful() and bench['exact_outputs_equal'] and before==after else 'FAIL',
           'tests_run':result.testsRun, 'errors':len(result.errors), 'failures':len(result.failures), 'skipped':len(result.skipped),
           'wall_seconds_including_small_benchmark':time.perf_counter()-start,'cpu_seconds':time.process_time()-cpu,
           'source_sha256':after,'source_stable':before==after,'loaded_source':str(Path(tests.acquisition.__file__).resolve()),
           'current_collector_runs':0,'network_calls':0,'production_writes':0,'actual_raw_or_current_graph_read':False,
           'benchmark':bench}
(HERE/'checks/TEST_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf8')
print(json.dumps(receipt))
raise SystemExit(0 if receipt['status']=='PASS' else 1)
