"""Private read-only LI.FI native assembly check. No network/solve/register."""
import hashlib
import json
from pathlib import Path
import sys
import time
from unittest.mock import patch

work = Path(sys.argv[1]).resolve()
staged = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(staged/'src'), str(work/'src')]
from stage1d_closure_context import requirements, assemble
from stage1d_closure_scope import active_batch

sources = {}
def read(path):
    data = path.read_bytes()
    sources[path.relative_to(work).as_posix()] = {'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}
    return json.loads(data)

query = next(q for q in active_batch(work)['queries'] if q['name']=='lifi_src001')
base = work/'derived/stage1d/queries/lifi_src001'
collection, labels = read(base/'collection.json'), read(base/'label_snapshot.json')
folder = base/'context/round_500_full'
material = {key:read(folder/name) for key,name in
    [('events','ledger_rows.json'),('headers','headers.json'),('balances','balances.json'),
     ('receipts','receipts.json'),('coverage','coverage.json')]}
wall, cpu = time.perf_counter(), time.process_time()
with patch('socket.create_connection', side_effect=AssertionError('Network forbidden')), patch('socket.socket.connect', side_effect=AssertionError('Network forbidden')):
    needed = requirements(query, collection, labels, material)
    result = assemble(query, collection, labels, material)
report = {'status':'PASS' if result['context_result']['completion_status']=='FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE' and not result['missing_points'] else 'EXPLICIT_GAPS',
    'scope':'PRIVATE_ACTUAL_READONLY_ASSEMBLY_NOT_PUBLIC_SYNTHETIC_TEST',
    'actual_readonly_assembly':True, 'scope_hash':query['scope_hash'], 'input_sources':sources,
    'source_sha256':hashlib.sha256((staged/'src/stage1d_closure_context.py').read_bytes()).hexdigest(),
    'dependencies':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in
        [work/'src/stage1d_context.py',work/'src/context_ledger_r3.py',work/'src/stage1d_multiasset_context.py']},
    'point_requests':needed['point_requests'],
    'native_receipt_reuse':needed['native_receipt_point_requests_satisfied_by_ledger'],
    'selected_raw_rows':len(needed['selected_events']),
    'completion_status':result['context_result']['completion_status'], 'missing_points':result['missing_points'],
    'gaps':result['context_result']['evidence_gaps'],
    'reconciliation':result['context_result']['ledger_reconciliation'],
    'accounts':len(result['context_result']['model_input']['accounts']),
    'objective_groups':result['context_result']['context_plan']['objective_groups'],
    'wall_seconds':time.perf_counter()-wall,'cpu_seconds':time.process_time()-cpu,
    'network_requests':0,'solver_calls':0,'registered':False,'production_state_writes':0,
    'qualification':'FULL is only the existing declared account/block context result. Current UNKNOWN depth-zero role is not a verified first-service label or method/external acceptance.'}
Path(sys.argv[2]).write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
print(json.dumps({k:report[k] for k in ('status','completion_status','missing_points','gaps','selected_raw_rows','accounts')}))
