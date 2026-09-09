from pathlib import Path
import hashlib,json,shutil
R=Path(__file__).resolve().parents[1];C=R/'code';S=C/'src'
changes=[]
def edit(name,pairs,imports=True):
 p=S/name;s=p.read_text(encoding='utf-8');before=hashlib.sha256(p.read_bytes()).hexdigest()
 if all(new in s for old,new,count in pairs):
  changes.append({'path':name,'status':'ALREADY_INSTALLED','sha256':before});return
 for old,new,count in pairs:
  assert s.count(old)==count,(name,old,s.count(old),count)
  s=s.replace(old,new)
 if imports:
  marker='from pathlib import Path'
  assert marker in s
  s=s.replace(marker,marker+'\nfrom stage1d_closure_scope import active_batch, active_batch_path, batch_for_scope, batch_path_for_sha',1)
 p.write_text(s,encoding='utf-8',newline='\n');changes.append({'path':name,'before_sha256':before,'after_sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
shutil.copyfile(R/'staging_root/stage1d_closure_scope.py',S/'stage1d_closure_scope.py')
edit('stage1d_runtime.py',[
 ("scopes=read(work/'private/STAGE1D_QUERY_SCOPES.json')['queries']","scopes=active_batch(work)['queries']",1),
 ("batch=read(root/'private/BATCH_QUERY_FREEZE.json')","batch=batch_for_scope(root, frozen['scope_id'], frozen['scope_hash'])",1),
 ("if not gate.get('old_and_new_tests_passed'): raise RuntimeError('Tests required before collection')", "if not gate.get('old_and_new_tests_passed'): raise RuntimeError('Tests required before collection')\n        if (work/'private/stage1d_authority/CLOSURE_SCOPE_ADOPTION.json').exists():\n            active=active_batch_path(work)\n            if gate.get('closure_authorization_id')!='STAGE1D_WINDOW_ROLE_SEMANTIC_CLOSURE_V1' or gate.get('closure_batch_sha256')!=sha(active) or gate.get('window_semantic_controlled_gate_passed') is not True:\n                raise RuntimeError('Current window/role/semantic end-to-end gate required')",1),
])
edit('stage1d_acquisition.py',[("read(work/'private/BATCH_QUERY_FREEZE.json')","active_batch(work)",1),("read(a.work/'private/BATCH_QUERY_FREEZE.json')","active_batch(a.work)",1)])
for file,old,new in [
 ('stage1d_bigquery_probe.py',"read(work/'private/BATCH_QUERY_FREEZE.json')","active_batch(work)"),
 ('stage1d_bigquery_jobs.py',"read(work / 'private/BATCH_QUERY_FREEZE.json')","active_batch(work)"),
 ('stage1d_candidate_batch.py',"read(a.work/'private/BATCH_QUERY_FREEZE.json')","active_batch(a.work)"),
 ('stage1d_context_online.py',"frozen=work/'private/BATCH_QUERY_FREEZE.json';bounds=None","frozen=active_batch_path(work);bounds=None"),
 ('stage1d_context_recovery.py',"frozen = work / 'private/BATCH_QUERY_FREEZE.json'","frozen = active_batch_path(work)"),
 ('stage1d_native_candidate_batch.py',"read(Path(work) / 'private/BATCH_QUERY_FREEZE.json')","active_batch(work)"),
 ('stage1d_owner_roles.py',"read(work / 'private/BATCH_QUERY_FREEZE.json')","active_batch(work)"),
 ]:
 edit(file,[(old,new,1)])
# CLI entrypoints consume the new active scope; old evidence verifiers retain their version.
for file,old,new in [
 ('stage1d_context_online.py',"read(a.work/'private/BATCH_QUERY_FREEZE.json')","active_batch(a.work)"),
 ('stage1d_context_recovery.py',"read(args.work / 'private/BATCH_QUERY_FREEZE.json')","active_batch(args.work)"),
 ('stage1d_native_candidate_batch.py',"read(a.work / 'private/BATCH_QUERY_FREEZE.json')","active_batch(a.work)"),
 ]:edit(file,[(old,new,1)],False)
edit('stage1d_native_candidate_batch.py',[("{'path': 'private/BATCH_QUERY_FREEZE.json', 'sha256': sha(work / 'private/BATCH_QUERY_FREEZE.json')}","{'path': active_batch_path(work).relative_to(work).as_posix(), 'sha256': sha(active_batch_path(work))}",1)],False)
edit('stage1d_native_candidate.py',[("read(work / 'private/BATCH_QUERY_FREEZE.json')","batch_for_scope(work, frozen['scope_id'], frozen['scope_hash'])",1)])
edit('stage1d_bq_context_prepare.py',[
 ("freeze_path = work / 'private/BATCH_QUERY_FREEZE.json'\n    if sha(freeze_path) != FREEZE_SHA:","freeze_path = active_batch_path(work)\n    if freeze_path.name == 'BATCH_QUERY_FREEZE.json' and sha(freeze_path) != FREEZE_SHA:",1),
 ("'freeze_sha256': FREEZE_SHA, 'needed_ranges': ranges","'freeze_sha256': sha(freeze_path), 'needed_ranges': ranges",1),
 ("freeze_path = Path(work) / 'private/BATCH_QUERY_FREEZE.json'\n    if sha(freeze_path) != FREEZE_SHA or manifest['freeze_sha256'] != FREEZE_SHA:","freeze_path = batch_path_for_sha(work, manifest['freeze_sha256'])\n    if sha(freeze_path) != manifest['freeze_sha256']:",1),
])
edit('stage1d_final_context_prepare.py',[("path = Path(work) / 'private/BATCH_QUERY_FREEZE.json'\n    if bq.sha(path) != bq.FREEZE_SHA:","path = active_batch_path(work)\n    if path.name == 'BATCH_QUERY_FREEZE.json' and bq.sha(path) != bq.FREEZE_SHA:",1)])
edit('stage1d_transfers_acquisition.py',[
 ("def frozen_query(work, name):\n    path = Path(work) / 'private/BATCH_QUERY_FREEZE.json'\n    if sha(path) != FREEZE_SHA:\n        raise ValueError('Original four-query freeze changed')\n    query = next(q for q in read(path)['queries'] if q['name'] == name)","def frozen_query(work, name, *, scope_id=None, scope_hash=None):\n    path = active_batch_path(work)\n    if path.name == 'BATCH_QUERY_FREEZE.json' and sha(path) != FREEZE_SHA:\n        raise ValueError('Original four-query freeze changed')\n    batch = batch_for_scope(work, scope_id, scope_hash) if scope_id is not None else read(path)\n    query = next(q for q in batch['queries'] if q['name'] == name)",1),
 ("if boundary.get('batch_freeze_sha256') != FREEZE_SHA:","if boundary.get('batch_freeze_sha256') != sha(active_batch_path(work)):",1),
 ("'scope_hash': query['scope_hash'], 'freeze_sha256': FREEZE_SHA,","'scope_hash': query['scope_hash'], 'freeze_sha256': sha(active_batch_path(work)),",1),
 ("_, scope = frozen_query(work, state['need']['query_name'])","_, scope = frozen_query(work, state['need']['query_name'], scope_id=state['need'].get('scope_id'), scope_hash=state['need'].get('scope_hash'))",1),
])
edit('stage1d_experiments.py',[("normalized['window_mode']!='ARRIVAL_90D'","normalized['window_mode'] not in {'ARRIVAL_90D','QUERY_ARRIVAL_WINDOW_SECONDS_V1'}",1)],False)
(R/'WINDOW_ROUTING_INSTALLATION.json').write_text(json.dumps(changes,indent=2),encoding='utf-8')
print(json.dumps({'changed_entries':len(changes),'network_requests':0}))
