"""Local source identity comparison; no model run or input mutation."""
from pathlib import Path
import sys,json,socket
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
def denied(*a,**k):raise RuntimeError('Method source comparison is offline')
socket.socket.connect=denied;socket.create_connection=denied
from context_access_r3 import sha,now
import stage1d_bq_context_prepare as h
saved=C/'private/integration_method_source_versions/07104dbb97b5da62403daa2c8f57b839124469cfd785462dbb0c5c1a5d29e908'
rows=[]
for p in sorted((saved/'src').glob('*.py')):
 current=C/'src'/p.name
 rows.append(dict(module=p.name,saved_sha256=sha(p),current_sha256=sha(current) if current.exists() else None,
  byte_identical=current.exists() and sha(p)==sha(current)))
new=sorted(p.name for p in (C/'src').glob('*.py') if not (saved/'src'/p.name).exists())
core={'lp_model.py','lp_oracle.py','lp_run.py','context_lp_r3.py','context_ledger_r3.py',
 'stage1d_experiments.py','stage1d_context.py','stage1d_multiasset_context.py',
 'stage1d_semantic_units.py','weth_component.py','weth_evidence.py'}
selected=[r for r in rows if r['module'] in core]
if {r['module'] for r in selected}!=core:raise ValueError('Explicit core comparison module absent')
doc=dict(status='LOCAL_SOURCE_IDENTITY_COMPARISON',utc=now(),saved_source_index=h.dep(C,saved/'SOURCE_INDEX.json'),
 current_gate=h.dep(C,C/'STAGE1D_PREFLIGHT_GATE.json'),modules=rows,added_modules=new,
 explicit_core_modules=selected,explicit_core_modules_byte_identical=all(r['byte_identical'] for r in selected),
 complete_transitive_dependency_equivalence_not_claimed=True,model_reruns=0,new_external_requests=0,
 frozen_input_mutated=False,final_offline_restore_validation_still_required=True)
out=R/'reports/completed_method_source_comparison'/h.sha(C/'STAGE1D_PREFLIGHT_GATE.json')/'COMPARISON.json'
h.save(out,doc)
print(json.dumps(dict(checked_saved_modules=len(rows),changed_existing_modules=[r['module'] for r in rows if not r['byte_identical']],
 added_modules=new,explicit_core_modules_byte_identical=doc['explicit_core_modules_byte_identical'],
 report=dict(path=out.relative_to(R).as_posix(),sha256=sha(out)),new_external_requests=0)),flush=True)
