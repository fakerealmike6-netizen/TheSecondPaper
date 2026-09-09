"""Adopt the user's incremental stop rule at a request-free safe point."""
from pathlib import Path
import sys,json,hashlib,shutil,socket
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
from stage1d_closure_scope import active_batch,active_batch_path
from stage1d_context import necessary_context_windows
from stage1d_task_boundaries import TaskBoundaries
if (C/'private/network_worker.lock').exists():raise RuntimeError('Wait for the normal in-flight request to finish')
socket.create_connection=lambda *a,**k:(_ for _ in ()).throw(RuntimeError('Local adoption cannot request network'))
socket.socket.connect=socket.create_connection
identifier='STAGE1D_STOP_BOUNDARY_PROPORTIONALITY_V1'
source=Path('D:/chrome下载')/(identifier+'.txt');data=source.read_bytes()
target=R/'authorization'/(identifier+'.txt')
if target.exists() and target.read_bytes()!=data:raise ValueError('Incremental authorization bytes changed')
target.parent.mkdir(exist_ok=True);target.write_bytes(data)
decision_path=C/'private/stage1d_roles/USER_TASK_BOUNDARIES.json'
decision=read(decision_path);TaskBoundaries(C)
addresses={d['address'] for d in decision['boundaries']}
record={'schema_version':'stage1d-stop-proportionality-adoption-v1','supplement_id':identifier,
 'authorization_source':{'path':target.relative_to(R).as_posix(),'bytes':len(data),'sha256':sha(target)},
 'adopted_at_utc':now(),'safe_point_network_lock_absent':True,
 'normal_inflight_requests_cancelled':0,'new_external_requests':0,'existing_boundary_decisions':
 {'path':decision_path.relative_to(C).as_posix(),'sha256':sha(decision_path),'ids':[d['boundary_id'] for d in decision['boundaries']]},
 'separate_decisions':['UNSUPPORTED_PROTOCOL_SCOPE_STOP','USER_EXPLICIT_SCOPE_STOP','SUPPORTED_INSTANCE_CONTINUATION','EXACT_ZERO_UPPER_BOUND_PROOF'],
 'existing_decisions_reopened':False,'historical_unknowns_promoted_to_verified':False,
 'no_pending_stop_decision_awaits_full_semantic_certification':True,
 'queries':[],'semantic_amount_and_zero_upper_bound_certificates_unchanged':True,
 'source_pool_attempts_and_clock_reset':False,'final_stop':'CHECKPOINT_1D_REACHED','external_acceptance':'PENDING_REVIEW'}
old=R/'CURRENT_FINITE_REQUIREMENTS.json'
if old.exists():
 hist=R/'authorization/requirement_history'/(sha(old)+'.json');hist.parent.mkdir(exist_ok=True)
 if not hist.exists():shutil.copyfile(old,hist)
 record['obsolete_stop_investigation_requirements_preserved']={'path':hist.relative_to(R).as_posix(),'sha256':sha(hist),
  'disposition':'UNSUBMITTED_STOP_ONLY_REQUESTS_SUPERSEDED_KEEP_ORIGINAL_EVIDENCE'}
for q in active_batch(C)['queries']:
 folder=C/'derived/stage1d/queries'/q['name'];collection=read(folder/'collection.json');labels=read(folder/'label_snapshot.json')
 pending=read(folder/'pending_intervals.json')
 relevant=addresses&{s['state']['address'] for s in collection['states']}
 if any(n['address'] in relevant for n in pending):raise ValueError('Stopped address remains in actual pending queue')
 plan=necessary_context_windows(q,collection,label_snapshot=labels)
 if any(r['address'] in relevant for r in plan['rows']):raise ValueError('Stopped platform remains in current context ledger plan')
 ref={'path':(folder/'collection.json').relative_to(C).as_posix(),'sha256':sha(folder/'collection.json')}
 preview=C/'private/stage1d_current_context_plans'/q['name']/(ref['sha256']+'.json');preview.parent.mkdir(parents=True,exist_ok=True)
 payload={'schema_version':'stage1d-current-context-plan-binding-v1','collection':ref,
  'labels':{'path':(folder/'label_snapshot.json').relative_to(C).as_posix(),'sha256':sha(folder/'label_snapshot.json')},
  'freeze_sha256':sha(active_batch_path(C)),'plan':plan,'candidates_and_labels_final_frozen':False,
  'existing_necessary_ordinary_ledger_material_still_required':True,
  'raw_entering_and_return_events_and_gas_not_deleted':True}
 if preview.exists() and read(preview)!=payload:raise ValueError('Immutable current context plan conflict')
 atomic_json(preview,payload)
 record['queries'].append({'query_name':q['name'],'graph':ref,'context_plan':{'path':preview.relative_to(C).as_posix(),'sha256':sha(preview)},
  'stopped_addresses_in_pending':0,'stopped_platform_full_ledger_plans':0,
  'boundaries_still_effective':sorted(relevant),
  'old_context_and_experiments_must_rebind_current_hashes':True})
record['generic_direct_source_stop_route']='LOCAL_MINIMAL_PATCH_PENDING; EXISTING_USER_BOUNDARIES_ALREADY_EFFECTIVE'
out=R/'STOP_BOUNDARY_PROPORTIONALITY_ADOPTION.json';atomic_json(out,record)
atomic_json(C/'private/stage1d_authority/STOP_BOUNDARY_PROPORTIONALITY_ADOPTION.json',record)
print(json.dumps({'status':'ADOPTED_EXISTING_BOUNDARIES_CONFIRMED','source_bytes':len(data),'source_sha256':sha(target),
 'context_plans':len(record['queries']),'new_external_requests':0,'cancelled_inflight':0,'receipt_sha256':sha(out)}),flush=True)
