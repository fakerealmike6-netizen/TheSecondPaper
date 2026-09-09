"""Install one reviewed coordinated storage patch, preserving the prior gate."""
from pathlib import Path
import sys, json, shutil, argparse
R=Path(__file__).resolve().parents[1]; C=R/'code'
sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read, sha, now
from page_attempts import atomic_json
from stage1d_runtime import Runtime
p=argparse.ArgumentParser(); p.add_argument('--manifest',type=Path,required=True)
p.add_argument('--sha256',required=True); a=p.parse_args()
manifest=a.manifest.resolve()
if not manifest.is_relative_to(R/'staging') or sha(manifest)!=a.sha256:
    raise ValueError('Exact reviewed staging manifest required')
doc=read(manifest); files=doc['installation']
if (C/'private/network_worker.lock').exists(): raise RuntimeError('Wait for the existing writer safe point')
Runtime().require_gate(C)
if not files or len({x['target'] for x in files})!=len(files): raise ValueError('Unique install targets required')
for row in files:
    source=R/row['source']; target=R/row['target']
    if not source.resolve().is_relative_to(R/'staging'): raise ValueError('Staging source required')
    if not (target.resolve().is_relative_to(C/'src') or target.resolve().is_relative_to(C/'tests') or
            target==R/'scripts/replay_current_scope.py'): raise ValueError('Undeclared install target')
    if sha(source)!=row['new_sha256']: raise ValueError('Candidate changed: '+row['source'])
    if (sha(target) if target.exists() else None)!=row['base_sha256']: raise ValueError('Base changed: '+row['target'])
stamp=now().replace(':','').replace('+','_'); backup=R/'snapshot'/('compact_gap_adoption_'+stamp)
backup.mkdir(parents=True)
gate=C/'STAGE1D_PREFLIGHT_GATE.json'
for rel in ['code/STAGE1D_PREFLIGHT_GATE.json','code/private/stage1d_semantics/SEMANTIC_CAPABILITY_GATE.json',
            'code/private/stage1d_semantics/CURRENT.json']+[x['target'] for x in files]:
    source=R/rel
    if source.exists():
        saved=backup/rel; saved.parent.mkdir(parents=True,exist_ok=True); shutil.copyfile(source,saved)
        if sha(source)!=sha(saved): raise ValueError('Backup verification failed')
shutil.copyfile(manifest,backup/'REVIEWED_INSTALLATION_MANIFEST.json')
prior={'path':(backup/'code/STAGE1D_PREFLIGHT_GATE.json').relative_to(R).as_posix(),
       'sha256':sha(backup/'code/STAGE1D_PREFLIGHT_GATE.json')}
pending=read(gate); pending.update(status='PENDING_AFFECTED_TESTS',source_adoption_started_at_utc=now())
atomic_json(gate,pending)
for row in files:
    target=R/row['target']; target.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(R/row['source'],target)
    if sha(target)!=row['new_sha256']: raise ValueError('Installed bytes differ')
receipt={'status':'INSTALLED_PENDING_AFFECTED_TESTS','prior_gate':prior,'files':files,
         'new_external_requests':0,'inflight_cancelled':0,'created_at_utc':now()}
atomic_json(backup/'INSTALLATION_RECEIPT.json',receipt)
atomic_json(R/'CURRENT_LOCAL_PATCH_ADOPTION.json',{'receipt':(backup/'INSTALLATION_RECEIPT.json').relative_to(R).as_posix(),
    'sha256':sha(backup/'INSTALLATION_RECEIPT.json'),'prior_gate':prior})
print(json.dumps({'status':receipt['status'],'files':len(files),'prior_gate':prior}),flush=True)
