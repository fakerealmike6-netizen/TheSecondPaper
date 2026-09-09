"""Offline replay of current observations; no provider / budget writer."""
from pathlib import Path
from dataclasses import asdict
import sys,json,time,hashlib,socket,argparse
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from stage1d_closure_scope import active_batch,active_batch_path
from stage1d_acquisition import CachedIntervals,Labels
from stage1d_semantic_catalogue import load_current_resolver
from collector import Collector,Event,Scope,Limits
from stage1d_runtime import Runtime
from context_access_r3 import sha,read
from page_attempts import atomic_json
from stage1d_native_candidate_batch import archive_query_files
def blocked(*a,**k):raise RuntimeError('Offline replay cannot open a network connection')
socket.socket.connect=blocked;socket.create_connection=blocked
parser=argparse.ArgumentParser();parser.add_argument('--query');args=parser.parse_args()
batch=active_batch(C);started=time.perf_counter();provider=CachedIntervals(C);labels=Labels(C);resolver=load_current_resolver(C)
result_path=R/'BOUNDARY_AND_REQUIREMENTS.json';prior=read(result_path) if result_path.exists() else None
names=[args.query] if args.query else ['txphish_src001','txphish_src002','xscam_src001','lifi_src001']
result={'authorization_id':batch['authorization_id'],'batch_freeze_sha256':sha(active_batch_path(C)),
 'labels':[{'path':p.relative_to(C).as_posix(),'sha256':sha(p)} for p in sorted((C/'derived/stage1d/labels').glob('*.json'))],
 'queries':[q for q in prior['queries'] if q['query_name'] not in names] if prior else [],'new_external_requests':0}
limits=Limits(max_events=100000,max_expanded_addresses=5000,max_online_seconds=43200)
for name in names:
 q=next(q for q in batch['queries'] if q['name']==name);scope=Scope.from_policy(q)
 qdir=C/'derived/stage1d/queries'/name;qdir.mkdir(parents=True,exist_ok=True)
 old=read(qdir/'collection.json');oldhash=sha(qdir/'collection.json')
 archived=archive_query_files(C,q)
 provider.pending=[];t=time.perf_counter()
 c=asdict(Collector(provider,labels,limits,semantic_resolver=resolver).run(scope,Event(**q['seed_event'])))
 byaddr={};state_bindings=[]
 for s in c['states']:
  addr=s['state']['address'];identity=s['identity']
  byaddr.setdefault(addr,[])
  if identity not in byaddr[addr]:byaddr[addr].append(identity)
  state_bindings.append({'address':addr,'arrival_id':s['state']['arrival']['event_id'],'depth':s['state']['depth'],'asset':s['state']['asset'],'identity':identity})
 snapshot={}
 for a,rs in byaddr.items():
  if len(rs)==1:snapshot[a]=rs[0]
  else:snapshot[a]={'kind':'UNKNOWN','status':'HISTORICAL_ROLE_VARIES_BY_ARRIVAL','historical_role_bindings':rs,'context_conservative_account_ledger_required':True}
 labelpath=qdir/'label_snapshot.json';atomic_json(labelpath,snapshot)
 canonical=lambda x:hashlib.sha256(json.dumps(x,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
 binding={'schema_version':'stage1d-actual-role-replay-v1','query_id':q['query_id'],'scope_hash':scope.scope_hash,
 'prior_collection_sha256':oldhash,'actual_state_roles_sha256':canonical(state_bindings),'label_snapshot_sha256':sha(labelpath),
 'label_snapshot_canonical_hash':canonical(snapshot),'addresses':len(snapshot),'states':len(state_bindings),
 'all_state_identities_retained':True,'source_manifest_sha256':canonical(result['labels']),
 'rule_source_sha256':sha(C/'src/stage1d_role_adoption.py'),'new_technical_certificates_adopted':len(labels.technical_roles.records) if labels.technical_roles else 0}
 binding.update(task_boundary_source_sha256=getattr(labels.task_boundaries,'identity',None),
  previous_source_snapshots=archived,
  task_boundary_rule_source_sha256=sha(C/'src/stage1d_task_boundaries.py'),
  actual_user_task_boundary_ids=sorted({v for row in state_bindings for v in row['identity'].get('task_boundary_ids',[])}))
 c['metrics'].update(label_snapshot_hash=binding['label_snapshot_canonical_hash'],actual_state_roles_sha256=binding['actual_state_roles_sha256'],label_snapshot_version='ACTUAL_REPLAYED_ROLES_V1')
 atomic_json(qdir/'collection.json',c);atomic_json(qdir/'pending_intervals.json',provider.pending)
 binding['collection_sha256']=sha(qdir/'collection.json');atomic_json(qdir/'ROLE_ADOPTION_AND_REPLAY.json',binding)
 row={'query_name':name,'query_id':q['query_id'],'scope_id':scope.scope_id,'scope_hash':scope.scope_hash,
 'collection_path':(qdir/'collection.json').relative_to(C).as_posix(),'collection_sha256':sha(qdir/'collection.json'),
 'label_snapshot':{'path':labelpath.relative_to(C).as_posix(),'sha256':sha(labelpath)},
 'event_count':len(c['candidate_events']),'state_count':len(c['states']),'prior_event_count':len(old['candidate_events']),
 'needed_ranges':[{**r,'query_id':q['query_id'],'query_name':name,'scope_id':scope.scope_id,'scope_hash':scope.scope_hash,
 'direction':'OUTGOING','gap_reason':'UNCOLLECTED_ADDRESS_INTERVAL','fact_type':'POSITIVE_NATIVE_CANDIDATE_INDEX' if r['asset'].startswith('native:') else 'POSITIVE_WETH_CANDIDATE_INDEX'} for r in provider.pending],
 'frontier':c['unresolved_frontier'],'frontier_count':len(c['unresolved_frontier']),'status':c['status'],'seconds':time.perf_counter()-t,'role_replay':binding}
 result['queries'].append(row);atomic_json(result_path,result)
 print(json.dumps({k:row[k] for k in ('query_name','event_count','state_count','frontier_count','status','seconds')}),flush=True)
print(json.dumps({'status':'OFFLINE_REPLAY_SAVED','seconds':time.perf_counter()-started,'new_external_requests':0}))
