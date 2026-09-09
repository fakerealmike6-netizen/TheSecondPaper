"""Root adoption of the independently checked saved actual 0.036 ETH Deposit."""
from pathlib import Path
import sys,json,socket
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
from stage1d_runtime import Runtime
from stage1d_semantic_units import FiniteSemanticResolver
from stage1d_semantic_catalogue import load_current_resolver,SCHEMA
def blocked(*a,**k):raise RuntimeError('Saved actual semantic adoption cannot request network')
socket.socket.connect=blocked;socket.create_connection=blocked
runtime=Runtime();runtime.require_gate(C)
expected={'stage1d_semantic_units.py':'efe34dc5b429ec6203cbad35a860f51178a577e1c4941ef475cededa5a9770ee',
 'stage1d_batch_binding_route.py':'f92aa2707a11914dec61728e905b3c20a592e5d1babcc3c639f82b3612ed8ac1'}
if any(sha(C/'src'/name)!=value for name,value in expected.items()):
    raise ValueError('Reviewed actual semantic sources are not installed')
stage=R/'staging/actual_weth_ae_bundle/staged_certified_ae'
hashes={'UNIT.json':'d3746d13eb737aea7914e605463e1ad37cd89686860a058f445d8a37ecfca816',
 'SHARED_EVIDENCE_CONTEXTS.json':'b38865784610b206ee45232c8d31474c53d52844c71eae0a614f0527cafb1c25'}
for name,value in hashes.items():
    if sha(stage/name)!=value:raise ValueError('Saved independently checked actual evidence changed')
unit=read(stage/'UNIT.json');shared=read(stage/'SHARED_EVIDENCE_CONTEXTS.json')
gate_path=C/'private/stage1d_semantics/SEMANTIC_CAPABILITY_GATE.json';gate=read(gate_path)
pointer=C/'private/stage1d_semantics/CURRENT.json'
if pointer.exists():
    existing=load_current_resolver(C)
    if unit['unit_id'] in existing.units and existing.units[unit['unit_id']]==unit:
        print(json.dumps({'status':'ALREADY_ADOPTED_NO_REPEAT','unit_id':unit['unit_id']}));sys.exit(0)
    raise ValueError('Existing catalogue must be explicitly merged; never overwrite its entries')
with runtime.session(C,'SHARED','actual_canonical_deposit_saved_evidence_adoption'):
    # Fresh receiver validates original pooled bytes, the full BQ page chain,
    # source/runtime, call/log proof, and strict real-vs-synthetic separation.
    resolver=FiniteSemanticResolver([unit],shared,capability_gate=gate,work_root=C)
    folder=C/'private/stage1d_semantics/instances'/hashes['UNIT.json'];folder.mkdir(parents=True,exist_ok=True)
    refs={}
    for name,value in hashes.items():
        target=folder/name;original=stage/name
        if target.exists() and sha(target)!=value:raise ValueError('Immutable actual certificate copy conflict')
        if not target.exists():target.write_bytes(original.read_bytes())
        if sha(target)!=value:raise ValueError('Actual original bytes not copied exactly')
        refs[name]={'path':target.relative_to(C).as_posix(),'sha256':value}
    catalogue={'schema_version':SCHEMA,'evidence_kind':'REAL_CHAIN',
        'instances':[{'unit':refs['UNIT.json'],'context_id':unit['evidence_context_id']}],
        'shared_evidence_context':refs['SHARED_EVIDENCE_CONTEXTS.json'],
        'capability_gate':{'path':gate_path.relative_to(C).as_posix(),'sha256':sha(gate_path)},
        'authorization_id':'STAGE1D_WINDOW_ROLE_SEMANTIC_CLOSURE_V1',
        'new_budget_pool':False,'target_source_amount_result':None}
    version=folder/'CATALOGUE.json';atomic_json(version,catalogue)
    atomic_json(pointer,catalogue)
    actual=load_current_resolver(C)
    if actual.identity!=resolver.identity:raise ValueError('Live adopted resolver differs from reviewed actual certificate')
    receipt={'schema_version':'stage1d-actual-semantic-adoption-v1','adopted_at_utc':now(),
        'unit_id':unit['unit_id'],'instance_status':unit['certification_status'],
        'operation_amount_raw':unit['input']['amount_raw'],'operation_input_asset':unit['input']['asset'],
        'operation_output_asset':unit['output']['asset'],'is_target_source_amount_result':False,
        'catalogue':{'path':version.relative_to(C).as_posix(),'sha256':sha(version)},
        'saved_prior_gate_failure':{'path':'staging/actual_weth_ae_bundle/current_gate_failure_v2/RESULT.json',
            'sha256':'3090383d304df6994fb45646b0da0d5535c8cd0f6f0a70c64f72aef30529b8de'},
        'actual_query_memberships':'PENDING_CURRENT_COLLECTOR_REPLAY',
        'physical_evidence_shared':True,'source_variables_shared':False,
        'all_receipt_logs_retained':True,'normal_funds_and_gas_requirements_unchanged':True,
        'new_external_requests':0,'solver_runs':0}
    atomic_json(folder/'ADOPTION_RECEIPT.json',receipt)
    atomic_json(R/'ACTUAL_AE_SEMANTIC_ADOPTION.json',receipt)
print(json.dumps({'status':'ACTUAL_CERTIFICATE_ADOPTED_PENDING_REPLAY','unit_id':unit['unit_id'],
    'operation_amount_raw':unit['input']['amount_raw'],'new_external_requests':0}),flush=True)
