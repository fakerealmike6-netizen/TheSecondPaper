"""Private dynamic check; requirements only, never assembly/freeze/solver."""
from pathlib import Path
from collections import Counter
import sys,json,hashlib,time
from unittest.mock import patch
work=Path(sys.argv[1]).resolve();staged=Path(__file__).resolve().parent
sys.path[:0]=[str(staged/'src'),str(work/'src')]
from stage1d_closure_context import requirements
from stage1d_closure_scope import active_batch
from stage1d_context import necessary_context_windows
sources={}
def read(path):
    data=path.read_bytes();sources[path.relative_to(work).as_posix()]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}
    return json.loads(data)
folder=work/'derived/stage1d/queries/txphish_src002'
collection=read(folder/'collection.json');labels=read(folder/'label_snapshot.json')
material={'events':[],'headers':[],'balances':[],'receipts':[],'coverage':[]}
for name in ('txphish_src002','txphish_src001'):
    folder=work/'derived/stage1d/queries'/name/'context/round_500_all_01'
    for key,file in [('events','ledger_rows.json'),('headers','headers.json'),('balances','balances.json'),('receipts','receipts.json'),('coverage','coverage.json')]:
        value=read(folder/file);material[key]+=list(value.values()) if isinstance(value,dict) else value
query=next(q for q in active_batch(work)['queries'] if q['name']=='txphish_src002')
base_plan=necessary_context_windows(query,collection,(),labels)
wall,cpu=time.perf_counter(),time.process_time()
with patch('socket.create_connection',side_effect=AssertionError('offline')),patch('socket.socket.connect',side_effect=AssertionError('offline')):
    needed=requirements(query,collection,labels,material)
assert needed['context_plan']['rows']==base_plan['rows']
report={'status':'REQUIREMENTS_COMPUTED','scope':'CURRENT_DYNAMIC_REQUIREMENTS_ONLY_NOT_CONTEXT_MODEL_OR_FREEZE',
    'source_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (staged/'src').glob('*.py')},
    'inputs':sources,'binding':needed['binding'],'candidate_status':collection['status'],
    'accounts':len(needed['context_plan']['rows']),'selected_rows':len(needed['selected_events']),
    'background_asset_projection':needed['background_asset_projection'],
    'point_requests_by_method':dict(Counter(r['method'] for r in needed['point_requests'])),
    'reusable_native_receipts':len(needed['native_receipt_point_requests_satisfied_by_ledger']),
    'new_neighbors':needed['new_neighbors'],'native_account_ranges_unchanged':True,
    'no_point_request_executed':True,'global_point_cache_checked':False,
    'network_requests':0,'solver_calls':0,'registration_calls':0,'assembly_calls':0,
    'wall_seconds':time.perf_counter()-wall,'cpu_seconds':time.process_time()-cpu,
    'inputs_unchanged':all(hashlib.sha256((work/p).read_bytes()).hexdigest()==ref['sha256'] for p,ref in sources.items())}
(staged/'TX2_READONLY_REQUIREMENTS.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
print(json.dumps({k:v for k,v in report.items() if k not in ('source_sha256','inputs','binding','background_asset_projection')}))
