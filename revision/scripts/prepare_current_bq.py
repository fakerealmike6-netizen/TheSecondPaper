"""Freeze current ordinary cache-minus rectangles for one classic-BQ batch."""
from pathlib import Path
import sys,json,hashlib,argparse,shutil
R=Path(__file__).resolve().parents[1]; C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha
from page_attempts import atomic_json
from stage1d_closure_scope import active_batch,active_batch_path
from stage1d_batch_binding_route import prepare,verified_preparation
from stage1d_alchemy_transfers import bind_need_to_scope
from collector import Scope,NATIVE
parser=argparse.ArgumentParser();parser.add_argument('--query',required=True);parser.add_argument('--maximum-addresses',type=int,default=64)
parser.add_argument('--address-offset',type=int,default=0)
a=parser.parse_args()
if not 1<=a.maximum_addresses<=64 or a.address_offset<0:raise ValueError('Finite BQ address batch bound and nonnegative queue offset required')
b=read(R/'BOUNDARY_AND_REQUIREMENTS.json');q=next(q for q in active_batch(C)['queries'] if q['name']==a.query)
entry=next(q for q in b['queries'] if q['query_name']==a.query);scope=Scope.from_policy(q)
cp=C/entry['collection_path']
if sha(cp)!=entry['collection_sha256']:raise ValueError('Current boundary graph changed')
collection=read(cp);addresses=[];needs=[];queue_addresses=[]
for need in entry['needed_ranges']:
 if need['asset']!=NATIVE:continue
 need=bind_need_to_scope(need,scope)
 fronts=[s for s in entry['frontier'] if s.get('reason')=='INTERVAL_INCOMPLETE' and s['state']['address']==need['address'] and s['state']['asset']==NATIVE and s['state'].get('protocol_context')=='ordinary' and s['state']['depth']<scope.max_depth]
 if not fronts:raise ValueError('No current ordinary frontier for BQ requirement')
 if need['address'] not in queue_addresses:queue_addresses.append(need['address'])
 if queue_addresses.index(need['address'])<a.address_offset:continue
 if need['address'] not in addresses:
  if len(addresses)==a.maximum_addresses:continue
  addresses.append(need['address'])
 needs.append(need)
if not needs:
 print(json.dumps({'status':'NO_CURRENT_NATIVE_NEEDS','query':a.query}));sys.exit(0)
token=hashlib.sha256(json.dumps(needs,sort_keys=True,separators=(',',':')).encode()).hexdigest()
out=C/'private/stage1d_closure_bq'/a.query/token
out.mkdir(parents=True,exist_ok=True)
deps=[]
for name,p in [('collection',cp),('labels',C/entry['label_snapshot']['path'])]:
 target=C/'private/stage1d_current_input_snapshots'/(sha(p)+'.json');target.parent.mkdir(parents=True,exist_ok=True)
 if not target.exists():shutil.copyfile(p,target)
 if sha(target)!=sha(p):raise ValueError('Immutable source snapshot conflict')
 deps.append({'path':target.relative_to(C).as_posix(),'sha256':sha(target)})
need_path=out/'CURRENT_NEEDED_RECTANGLES.json'
doc={'query_id':q['query_id'],'scope_hash':q['scope_hash'],'freeze_sha256':sha(active_batch_path(C)),
 'need_rectangles':needs,'scope_dependencies':deps,'selection_basis':('CURRENT_COLLECTOR_FRONTIER_ORDER_FIRST_64_ORDINARY_ADDRESSES' if a.address_offset==0 else 'CURRENT_COLLECTOR_FRONTIER_ORDER_DISJOINT_ORDINARY_ADDRESS_SLICE'),
 'current_success_cache_already_subtracted_by_collector':True,'stopped_protocol_history_requested':False}
if a.address_offset:
 doc['queue_slice']={'first_address_ordinal_zero_based':a.address_offset,'maximum_addresses':a.maximum_addresses,
  'previous_address_needs_preserved':True,'reason':'Other independent current frontier addresses; original blocked request keys are retained, not changed or retried'}
if need_path.exists() and read(need_path)!=doc:raise ValueError('Immutable current need identity changed')
atomic_json(need_path,doc)
config_path=C/'private/STAGE1D_BIGQUERY_CONFIG_RECOVERY.json'
schemas=read(C/'private/stage1d_bq_minimal_context_20260908_v2/dryrun_spec.json')['schema_evidence']
classic_tx=C/'raw/stage1d_bigquery/read-r4-PRIVATE_LITERAL_1/receipt.json'
schemas.append({'path':classic_tx.relative_to(C).as_posix(),'sha256':sha(classic_tx),'bytes':classic_tx.stat().st_size})
manifest=prepare(C,need_path,out,read(config_path),schemas,chunk_days=7)
verified_preparation(C,out/'PREPARATION.json')
print(json.dumps({'status':'PREPARED_NO_NEW_EXTERNAL_REQUESTS','query':a.query,'addresses':len(addresses),
 'rectangles':len(needs),'date_chunks':manifest['date_chunks'],'preparation_path':(out/'PREPARATION.json').relative_to(C).as_posix(),
 'sha256':sha(out/'PREPARATION.json')}),flush=True)
