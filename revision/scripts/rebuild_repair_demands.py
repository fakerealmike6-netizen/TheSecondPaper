"""Real current context demand minus saved points, then exact legacy audit."""
from pathlib import Path
from collections import Counter
import argparse,sys,json,socket,copy,time,sqlite3
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
import stage1d_bq_context_prepare as bq
from stage1d_closure_scope import active_batch,active_batch_path
from stage1d_closure_context import requirements,_points,_missing_points,project_necessary_families,_project
from stage1d_final_context_prepare import merge_bound_rows
from stage1d_context import _event
from stage1d_context_online import _cached_rpc,_merge_rpc
from stage1d_legacy_rpc_import import LegacyPointImporter,serializable_preparation,exact_request_sha
from stage1d_runtime import Runtime
def blocked(*a,**k):raise RuntimeError('Saved-data admission uses no network')
socket.socket.connect=blocked;socket.create_connection=blocked

def main():
 p=argparse.ArgumentParser();p.add_argument('--query',required=True);p.add_argument('--bundle',required=True);p.add_argument('--revision',required=True);p.add_argument('--apply-audit');p.add_argument('--supplement-material');a=p.parse_args()
 Runtime().require_gate(C)
 if (C/'private/network_worker.lock').exists():raise ValueError('Wait for single-writer safe point')
 q=next(q for q in active_batch(C)['queries'] if q['name']==a.query)
 out=C/'private/integration_repair_demands'/a.query/a.revision
 if a.apply_audit:
  audit_path=bq.inside(C,a.apply_audit);audit=read(audit_path)
  for ref in audit['binding_refs']:bq.checked(C,ref)
  if audit['source_sha256']!={p.name:sha(p) for p in (C/'src').glob('*.py')}:raise ValueError('Audit source changed')
  importer=LegacyPointImporter(C,R/'recovery/legacy_metadata_index/legacy_metadata.sqlite')
  needs=[r['need'] for r in audit['rows'] if r['status']=='ADMISSIBLE_POINT_PENDING_ROOT_APPLY']
  claim=out/'APPLY_CLAIM.json';claim.parent.mkdir(parents=True,exist_ok=True)
  with claim.open('x',encoding='utf8') as f:json.dump({'audit':bq.dep(C,audit_path),'needs':len(needs)},f)
  result=importer.apply_many(q,needs)
  atomic_json(out/'LEGACY_APPLY.json',result)
  print(json.dumps({'status':'LEGACY_APPLY_SAVED','states':dict(Counter(r['status'] for r in result['results'])),'new_requests':0}));return
 if out.exists():raise ValueError('Immutable demand revision already exists')
 out.mkdir(parents=True)
 started=time.perf_counter();bundle_path=bq.inside(C,a.bundle);bundle=read(bundle_path);consumer=bundle['consumers'][a.query]
 for key,filename in (('collection','collection.json'),('labels','label_snapshot.json')):
  if sha(C/'derived/stage1d/queries'/a.query/filename)!=consumer['inputs'][key]['sha256']:raise ValueError('Preparation is stale for current graph/roles')
 collection=read(bq.checked(C,consumer['inputs']['collection']));labels=read(bq.checked(C,consumer['inputs']['labels']))
 material={key:read(bq.checked(C,consumer['prepared_context'][name])) for key,name in
  (('events','ledger_rows'),('headers','headers'),('balances','balances'),('receipts','receipts'),('coverage','coverage'))}
 material['headers']={int(k):v for k,v in material['headers'].items()}
 material['transactions']={};material['evidence_kind']='REAL_EVIDENCE_BOUND'
 refs=[bq.dep(C,bundle_path),*consumer['inputs'].values(),*consumer['prepared_context'].values()]
 material['source_refs']=refs
 paid_points=[]
 if consumer.get('paid_source_admission'):
  paid=read(bq.checked(C,consumer['paid_source_admission']))
  paid_points=paid['point_binding_requests']
  refs.append(consumer['paid_source_admission'])
 if a.supplement_material:
  from stage1d_context_coverage import merge_coverage
  sp=bq.inside(C,a.supplement_material);supplement=read(sp)
  material['events']=merge_bound_rows(material['events'],supplement['events'])
  material['coverage']=merge_coverage(material['coverage'],supplement['coverage'])
  refs.append(bq.dep(C,sp))
 point_index=C/'private/integration_repair_context/ALL_FIVE_PAID_JOB_SOURCES.json'
 if point_index.exists():
  from stage1d_bq_root_binding import bind_saved_export_root
  point_source=read(point_index)['trace_only_point_source']
  bound=bind_saved_export_root(C,bq.checked(C,point_source['job']),point_source['spec'],point_source['tx_hash'])
  atomic_json(out/'TRACE_ONLY_POINT_ADMISSION.json',{k:v for k,v in bound.items() if k not in ('rows','current_top_row')})
  if bound['status']=='ROOT_EQUIVALENT_BOUND':
   before=requirements(q,collection,labels,material)
   selected=_project(bound['rows']+[bound['current_top_row']],before['context_plan'],before['necessary_transaction_family_projection']['mandatory_transaction_ids'])
   proof={'basis':'ONLY_NEW_POINT_ROWS_TOUCHING_CURRENT_UNIFIED_REQUIREMENTS','selected_rows':len(selected),'coverage_added':0}
   material['events']=merge_bound_rows(material['events'],selected)
   refs.extend([point_source['job'],point_source['spec'],bq.dep(C,out/'TRACE_ONLY_POINT_ADMISSION.json')])
   material['trace_only_point_projection']=proof
 first=requirements(q,collection,labels,material)
 first['point_requests']=list({bq.digest(p):p for p in first['point_requests']+paid_points}.values())
 if any(p['method']=='eth_call' for p in first['point_requests']):raise ValueError('Use the existing finite WETH consumer for non-native current demands')
 hits=_cached_rpc(C,first['point_requests'])
 for plan,value,evidence in hits:
  if plan['method']=='eth_getTransactionByHash':material['transactions'][plan['params'][0]]={'request':plan,'response':{'result':value},'evidence_ids':evidence}
  else:_merge_rpc(material['headers'],material['balances'],material['receipts'],plan,value,evidence)
 needed=requirements(q,collection,labels,material)
 needed['point_requests']=list({bq.digest(p):p for p in needed['point_requests']+paid_points}.values())
 needed['paid_current_root_binding_requests']=paid_points
 missing=_missing_points(needed['point_requests'],*_points(material))
 atomic_json(out/'MATERIAL.json',material);atomic_json(out/'REQUIREMENTS.json',needed)
 binding_refs=[bq.dep(C,out/'REQUIREMENTS.json'),bq.dep(C,active_batch_path(C)),*consumer['inputs'].values()]
 blocks={}
 for row in collection.get('candidate_events',[])+needed['selected_events']:
  e=_event(row)
  if e.get('tx_hash') and e.get('block') is not None:
   if e['tx_hash'] in blocks and blocks[e['tx_hash']]!=e['block']:raise ValueError('Conflicting transaction block')
   blocks[e['tx_hash']]=e['block']
 importer=LegacyPointImporter(C,R/'recovery/legacy_metadata_index/legacy_metadata.sqlite')
 # The user's repair work order explicitly continues the original frozen raw
 # whitelist. This is the task-specific exception to generic root governance;
 # the importer enforces exact metadata matches, groups, paths and byte hashes.
 raw_inside_project=True
 audit=[]
 for request in sorted(missing,key=lambda p:p['method']!='eth_getBlockByNumber'):
  method,params=request['method'],request['params']
  block=int(params[0],16) if method=='eth_getBlockByNumber' else int(params[1],16) if method=='eth_getBalance' else blocks.get(params[0])
  if block is None:audit.append({'plan':request,'status':'CURRENT_EXPECTED_BLOCK_EVIDENCE_MISSING'});continue
  need=dict(request,expected_block=block,query_id=q['query_id'],scope_id=q['scope_id'],scope_hash=q['scope_hash'],reason='CURRENT_REPAIRED_CLOSURE_NECESSARY_POINT',evidence_refs=binding_refs)
  if raw_inside_project:
   audit.append(serializable_preparation(importer.prepare_one(q,need)))
  else:
   # Inspect only the already migrated metadata index. Outside-root bytes need
   # the current clean-room exception reconciled before original raw admission.
   db=sqlite3.connect(importer.index.as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row
   try:matches=[dict(v) for v in db.execute('SELECT manifest_path,ordinal,payload_path,response_sha256,declared_bytes FROM rpc_entries WHERE method=? AND request_sha256=?',(method,exact_request_sha(request)))]
   finally:db.close()
   variants=sorted({r['response_sha256'] for r in matches})
   status='NO_EXACT_LEGACY_REQUEST_MATCH' if not matches else 'LEGACY_RESPONSE_VARIANTS_QUARANTINED' if len(variants)>1 else 'EXACT_LEGACY_METADATA_MATCH_OUTSIDE_PROJECT_PERMISSION_UNRESOLVED'
   audit.append({'status':status,'need':need,'matches':matches,'original_payload_read':False,'new_requests':0})
 result={'status':'LEGACY_EXACT_CURRENT_DEMAND_AUDITED','query':a.query,'rows':audit,'binding_refs':binding_refs,
  'source_sha256':{p.name:sha(p) for p in (C/'src').glob('*.py')},'new_requests':0,'time_utc':now()}
 atomic_json(out/'LEGACY_AUDIT.json',result)
 summary={'status':'CURRENT_NEEDS_AFTER_SAVED_MATERIAL_AND_CACHE','query':a.query,'material':bq.dep(C,out/'MATERIAL.json'),
  'requirements':bq.dep(C,out/'REQUIREMENTS.json'),'legacy_audit':bq.dep(C,out/'LEGACY_AUDIT.json'),
  'point_requests_before_cached_headers':len(first['point_requests']),'current_cache_hits':len(hits),
  'point_requests_after_cache_materialization':len(needed['point_requests']),'remaining_points':len(missing),
  'remaining_by_method':dict(Counter(p['method'] for p in missing)),'legacy_audit_states':dict(Counter(r['status'] for r in audit)),
  'native_ledger_ranges':len(consumer['needed_ranges']),'points_are_not_ledger_coverage':True,
  'new_requests':0,'new_online_sessions':0,'seconds':time.perf_counter()-started}
 atomic_json(out/'DEMAND_RECEIPT.json',summary);print(json.dumps(summary),flush=True)

if __name__=='__main__':main()
