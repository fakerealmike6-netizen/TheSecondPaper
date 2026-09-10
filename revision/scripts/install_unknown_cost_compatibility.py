"""Repair only the two observed legacy interfaces while the source gate is closed."""
from pathlib import Path
import json,shutil,sys
R=Path(__file__).resolve().parents[1]; C=R/'code';S=R/'staging/unknown_cost_legacy_compatibility'
sys.path[:0]=[str(C/'src'),str(R/'staging/8ac4_curated_scope_stop')]
from context_access_r3 import sha,read,now
from page_attempts import atomic_json
from adopt_8ac4 import safe_point
safe_point(C)
if read(C/'STAGE1D_PREFLIGHT_GATE.json')['status']!='PENDING_AFFECTED_TESTS':raise ValueError('Expected closed pending gate')
bases={'src/collector.py':'PRIVATE_LITERAL_2',
 'src/stage1d_context.py':'PRIVATE_LITERAL_1',
 'src/stage1d_cost_boundary_context.py':'PRIVATE_LITERAL_3',
 'tests/test_cost_legacy_compatibility.py':None}
for rel,old in bases.items():
 if (sha(C/rel) if (C/rel).exists() else None)!=old:raise ValueError('Changed production base '+rel)
 if not (S/rel).is_file():raise ValueError('Missing compatibility candidate')
backup=R/'snapshot'/('unknown_cost_compatibility_'+now().replace(':','').replace('+','_'));backup.mkdir(parents=True)
for rel in bases:
 if (C/rel).exists():
  out=backup/rel;out.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(C/rel,out)
  if sha(C/rel)!=sha(out):raise ValueError('Backup differs')
prior_test=read(C/'checks/closure_preflight/CURRENT_TEST_RECEIPT.json')
rows=[{'target':'code/'+rel,'source':(S/rel).relative_to(R).as_posix(),'base_sha256':old,'new_sha256':sha(S/rel)} for rel,old in bases.items()]
for row in rows:
 target=R/row['target'];target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(R/row['source'],target)
 if sha(target)!=row['new_sha256']:raise ValueError('Installed source differs')
receipt={'status':'COMPATIBILITY_REPAIR_INSTALLED_GATE_REMAINS_PENDING','files':rows,'observed_failure_receipt':prior_test,
 'baseline':read(R/'reports/unknown_cost_boundary_v1/SOURCE_INSTALLATION_RECEIPT.json')['prior_gate'],
 'created_at_utc':now(),'new_external_requests':0}
atomic_json(backup/'INSTALLATION_RECEIPT.json',receipt)
atomic_json(R/'reports/unknown_cost_boundary_v1/COMPATIBILITY_INSTALLATION_RECEIPT.json',receipt)
print(json.dumps({'status':receipt['status'],'files':len(rows),'source_sha256':{r['target']:r['new_sha256'] for r in rows}}))
