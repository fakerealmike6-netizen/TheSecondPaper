"""Export an exact candidate-family selector view to the existing old-raw importer.

The importer accepts current-input refs, physical family rows, and exact point
selectors. This is explicitly a discovery demand, never a context/full claim.
"""
from pathlib import Path
import sys,json,argparse,socket
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
def denied(*a,**k):raise RuntimeError('Candidate demand export has no network authority')
socket.socket.connect=denied;socket.create_connection=denied
from stage1d_runtime import Runtime
from stage1d_cost_request_guard import validate_candidate_entry
from stage1d_paid_candidate_domains import input_documents
from prepare_current_weth import safe_point
import stage1d_bq_context_prepare as h
p=argparse.ArgumentParser();p.add_argument('--folder',required=True);a=p.parse_args()
Runtime().require_gate(C);safe_point(C)
folder=h.inside(C,a.folder);metadata=h.read(folder/'ENTRY_RECEIPT.json')
if metadata['source_sha256']!={p.name:h.sha(p) for p in (C/'src').glob('*.py')}:raise ValueError('Source gate changed')
proof_path=h.checked(C,metadata['receipt']['proof']);proof=h.read(proof_path)
q,col=input_documents(C,proof)
entry=next(e for e in h.read(R/'BOUNDARY_AND_REQUIREMENTS.json')['queries'] if e['query_name']==q['name'])
validate_candidate_entry(C,q,entry)
if h.digest(entry)!=metadata['exact_current_entry_sha256']:raise ValueError('Discovery demand advanced')
for kind,name in (('collection','collection.json'),('labels','label_snapshot.json')):
 if h.sha(C/'derived/stage1d/queries'/q['name']/name)!=proof['inputs'][kind]['sha256']:raise ValueError('Current role/graph changed')
admission=h.read(h.checked(C,proof['admission']));result=admission['result']
doc=dict(schema_version='stage1d-paid-candidate-binding-legacy-input-v1',query_id=q['query_id'],scope_hash=q['scope_hash'],
 current_input_refs=proof['inputs'],rows=result['rows'],point_binding_requests=result['point_binding_requests'],
 binding_gaps=result['binding_gaps'],original_discovery_proof=h.dep(C,proof_path),
 original_discovery_admission=proof['admission'],demand_basis=proof['demand_basis'],
 full_context_claimed=False,reference_paths_used_to_select_demand=False,new_external_requests=0)
out=R/'reports/paid_current_material'/q['name']/('discovery_'+folder.name)/'ADMISSION.json'
h.save(out,doc)
receipt=dict(status='EXACT_DISCOVERY_POINT_DEMAND_EXPORTED',query=q['name'],requests=len(doc['point_binding_requests']),
 report=dict(path=out.relative_to(R).as_posix(),sha256=h.sha(out)),proof=h.dep(C,proof_path),new_external_requests=0)
h.save(folder/'LEGACY_DEMAND_VIEW.json',receipt);print(json.dumps(receipt),flush=True)
