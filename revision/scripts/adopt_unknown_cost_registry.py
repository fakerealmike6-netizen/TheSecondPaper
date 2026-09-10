"""Activate the fixed initial cost policy and verified immutable evidence catalog offline."""
from pathlib import Path
import argparse, json, shutil, sys
R=Path(__file__).resolve().parents[1]; C=R/'code'; U=R/'reports/unknown_cost_boundary_v1'
sys.path[:0]=[str(C/'src'),str(R/'staging/8ac4_curated_scope_stop')]
from context_access_r3 import sha, read, now
from page_attempts import atomic_json
from adopt_8ac4 import safe_point

def prepare():
    paths=[R/'staging/unknown_cost_registry_inputs/outputs/verified_v2/READY_MANIFEST.json',
        R/'staging/unknown_cost_online_identity_inputs/outputs/verified_v1/READY_MANIFEST.json',
        U/'identity_checks/WEB_IDENTITY_INDEX.json']
    expected=['PRIVATE_LITERAL_3',
        'PRIVATE_LITERAL_1',
        'PRIVATE_LITERAL_2']
    for p,h in zip(paths,expected):
        if sha(p)!=h:raise ValueError('Final evidence index changed')
    evidence,online,web=map(read,paths)
    pp=U/'preparations/UNKNOWN_COST_POLICY.candidate.json'
    if sha(pp)!='PRIVATE_LITERAL_4':raise ValueError('Fixed policy changed')
    policy=read(pp)
    doc={'schema_version':'stage1d-unknown-cost-registry-v1',
         'policy_ref':{'path':'private/stage1d_roles/UNKNOWN_COST_POLICY.json','sha256':sha(pp)},
         'initial_snapshot_ref':policy['initial_snapshot_ref'],
         'identity_checks':online['descriptors']+web['identity_checks'],
         'historical_codes':evidence['historical_codes'],'activity_observations':evidence['activity_observations']}
    for key in ('identity_checks','historical_codes','activity_observations'):
        if len({v['path'] for v in doc[key]})!=len(doc[key]):raise ValueError('Duplicate descriptor')
        for ref in doc[key]:
            path=(C/ref['path']).resolve()
            if not path.is_relative_to(R) or sha(path)!=ref['sha256']:raise ValueError('Descriptor changed')
    out=U/'preparations/UNKNOWN_COST_CURRENT.candidate.json';atomic_json(out,doc)
    atomic_json(U/'preparations/REGISTRY_INPUT_MANIFEST.json',{'sources':[{'path':p.relative_to(R).as_posix(),'sha256':sha(p)} for p in paths],
        'policy_sha256':sha(pp),'current_sha256':sha(out),'new_external_requests':0})
    print(json.dumps({'current_sha256':sha(out),'identity_checks':len(doc['identity_checks']),'historical_codes':len(doc['historical_codes'])}))

def apply(expected):
    from stage1d_runtime import Runtime
    safe_point(C);Runtime().require_gate(C)
    if (R/'operations/CURRENT_COST_EVIDENCE_DRIVER.lock').exists():raise ValueError('Evidence writer active')
    source=U/'preparations/UNKNOWN_COST_CURRENT.candidate.json'
    if sha(source)!=expected:raise ValueError('Current descriptor changed')
    inputs=read(U/'preparations/REGISTRY_INPUT_MANIFEST.json')
    for ref in inputs['sources']:
        if sha(R/ref['path'])!=ref['sha256']:raise ValueError('Input changed before adoption')
    policy=U/'preparations/UNKNOWN_COST_POLICY.candidate.json'
    if sha(policy)!=inputs['policy_sha256']:raise ValueError('Policy changed')
    for src,name in [(policy,'UNKNOWN_COST_POLICY.json'),(source,'UNKNOWN_COST_CURRENT.json')]:
        target=C/'private/stage1d_roles'/name
        if target.exists():
            if sha(target)!=sha(src):raise ValueError('Do not overwrite another policy/catalog')
        else:
            with target.open('xb') as f:f.write(src.read_bytes())
        if sha(target)!=sha(src):raise ValueError('Adopted bytes differ')
    from stage1d_acquisition import Labels
    from stage1d_closure_scope import active_batch
    from collector import Scope
    labels=Labels(C);registry=labels.cost_boundary_resolver
    scopes=[]
    for q in active_batch(C)['queries']:
        s=Scope.from_policy(q);p=registry.policy_for_scope(s)
        if not p or not p['enabled']:raise ValueError('Policy not active for current query')
        scopes.append({'name':q['name'],'scope_hash':s.scope_hash})
    receipt={'status':'COST_POLICY_AND_EVIDENCE_ACTIVE_REPLAY_PENDING','created_at_utc':now(),
        'authorization_id':'STAGE1D_UNKNOWN_CONTRACT_OR_RATE20_BOUNDARY_V1','policy_sha256':sha(policy),
        'current_sha256':sha(source),'registry_identity':registry.identity,'registry_identity_document':registry.identity_document,
        'scopes':scopes,'new_external_requests':0,'existing_raw_and_ledgers_modified':False,
        'new_discovery_requires_current_replayed_collection':True,'checkpoint_claimed':False}
    atomic_json(U/'REGISTRY_ADOPTION_RECEIPT.json',receipt)
    print(json.dumps({k:receipt[k] for k in ('status','registry_identity','new_external_requests')}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--prepare',action='store_true');p.add_argument('--apply-sha256');a=p.parse_args()
    if a.prepare==bool(a.apply_sha256):p.error('Choose prepare or apply SHA')
    prepare() if a.prepare else apply(a.apply_sha256)
