"""Install only reviewed, tested staging files at a writer-safe boundary."""
from pathlib import Path
import sys,json,shutil,argparse
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
from stage1d_runtime import Runtime
p=argparse.ArgumentParser();group=p.add_mutually_exclusive_group()
group.add_argument('--context-only',action='store_true');group.add_argument('--actual-semantic',action='store_true');group.add_argument('--trace-index',action='store_true');group.add_argument('--hotspots',action='store_true');a=p.parse_args()
if (C/'private/network_worker.lock').exists():raise RuntimeError('Normal in-flight request must finish first')
Runtime().require_gate(C)
files=[]
if a.hotspots:
 stage=R/'staging/batch_binding_immutable_refs_fix'
 if sha(stage/'INSTALLATION_MANIFEST.json')!='PRIVATE_LITERAL_3':raise ValueError('Reviewed immutable refs manifest changed')
 for row in read(stage/'INSTALLATION_MANIFEST.json')['files']:files.append((stage,row))
 stage=R/'staging/interval_gap_memo_fix'
 if sha(stage/'INSTALLATION_MANIFEST.json')!='PRIVATE_LITERAL_1':raise ValueError('Reviewed gap memo manifest changed')
 manifest=read(stage/'INSTALLATION_MANIFEST.json')
 files.append((stage,manifest['source']))
 files.append((stage,{'path':manifest['test']['path'],'base_sha256':None,'new_sha256':manifest['test']['sha256']}))
elif a.trace_index:
 stage=R/'staging/context_trace_child_index_fix'
 if sha(stage/'INSTALLATION_MANIFEST.json')!='PRIVATE_LITERAL_6':raise ValueError('Reviewed trace index manifest changed')
 for row in read(stage/'INSTALLATION_MANIFEST.json')['files']:files.append((stage,row))
elif a.actual_semantic:
 stage=R/'staging/semantic_emitter_fix'
 manifest=read(stage/'SOURCE_MANIFEST.json')
 if sha(stage/'SOURCE_MANIFEST.json')!='PRIVATE_LITERAL_2':raise ValueError('Reviewed semantic manifest changed')
 for row in manifest['installation']:files.append((stage,row))
 stage=R/'staging/portable_dependency_fields'
 files.extend([(stage,{'path':'src/stage1d_batch_binding_route.py','base_sha256':'PRIVATE_LITERAL_5','new_sha256':'PRIVATE_LITERAL_7'}),
  (stage,{'path':'tests/test_portable_dependency_fields.py','base_sha256':None,'new_sha256':'PRIVATE_LITERAL_8'})])
 stage=R/'staging/context_background_asset_fix'
 if sha(stage/'INSTALLATION_MANIFEST.json')!='PRIVATE_LITERAL_4':raise ValueError('Reviewed background asset manifest changed')
 for row in read(stage/'INSTALLATION_MANIFEST.json')['files']:files.append((stage,row))
elif a.context_only:
 stage=R/'staging/closure_context_family_fix'
 manifest=read(stage/'INSTALLATION_MANIFEST.json')
 for row in manifest.get('files',manifest.get('install_files',[])):files.append((stage,row))
else:
 for dirname,mname,key in [('semantic_shared_evidence','INSTALLATION_MANIFEST.json','files'),
                          ('stop_boundary_proportionality','SOURCE_MANIFEST.json','install_files')]:
  stage=R/'staging'/dirname
  for row in read(stage/mname)[key]:files.append((stage,row))
 stage=R/'staging/legacy_rpc_scope_migration';base=read(stage/'BASE_SHA256.json')
 for rel in ['src/stage1d_legacy_rpc_import.py','tests/test_stage1d_legacy_rpc_closure_scope.py']:
  files.append((stage,{'path':rel,'base_sha256':base.get(rel),'new_sha256':sha(stage/rel)}))
if not files or len({r['path'] for _,r in files})!=len(files):raise ValueError('Nonempty conflict-free exact installation inventory required')
for stage,row in files:
 rel=Path(row['path']);source=stage/rel;target=C/rel
 if rel.is_absolute() or '..' in rel.parts or rel.parts[0] not in ('src','tests'):raise ValueError('Unsafe source target')
 if sha(source)!=row['new_sha256']:raise ValueError('Staged tested source changed: '+str(rel))
 if (sha(target) if target.exists() else None)!=row['base_sha256']:raise ValueError('Active source base mismatch: '+str(rel))
stamp=now().replace(':','').replace('+','_');backup=R/'snapshot'/('source_adoption_'+stamp);backup.mkdir(parents=True)
gate=C/'STAGE1D_PREFLIGHT_GATE.json';shutil.copyfile(gate,backup/'PRIOR_PREFLIGHT_GATE.json')
for rel in ('private/stage1d_semantics/SEMANTIC_CAPABILITY_GATE.json','private/stage1d_semantics/CURRENT.json'):
 source=C/rel
 if source.exists():
  saved=backup/rel;saved.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source,saved)
oldgate=read(gate);oldgate.update(status='PENDING_AFFECTED_TESTS',source_adoption_started_at_utc=now())
atomic_json(gate,oldgate)
receipt={'status':'INSTALLED_PENDING_AFFECTED_TESTS','safe_point_inflight_cancelled':0,'new_external_requests':0,
 'prior_gate':{'path':(backup/'PRIOR_PREFLIGHT_GATE.json').relative_to(R).as_posix(),'sha256':sha(backup/'PRIOR_PREFLIGHT_GATE.json')},'files':[]}
for stage,row in files:
 target=C/row['path'];source=stage/row['path']
 if target.exists():
  saved=backup/row['path'];saved.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(target,saved)
 target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source,target)
 if sha(target)!=row['new_sha256']:raise ValueError('Installed source bytes differ')
 receipt['files'].append({**row,'staging_source':source.relative_to(R).as_posix()})
atomic_json(backup/'INSTALLATION_RECEIPT.json',receipt)
atomic_json(R/'CURRENT_LOCAL_PATCH_ADOPTION.json',{'receipt':str((backup/'INSTALLATION_RECEIPT.json').relative_to(R)),
 'sha256':sha(backup/'INSTALLATION_RECEIPT.json'),'prior_gate':receipt['prior_gate']})
print(json.dumps({'status':receipt['status'],'files':len(files),'prior_gate':receipt['prior_gate'],'receipt_sha256':sha(backup/'INSTALLATION_RECEIPT.json')}),flush=True)
