"""Assemble reviewed narrow candidates, then install at the sole-writer safe point."""
from pathlib import Path
import argparse, hashlib, json, shutil, sys
R=Path(__file__).resolve().parents[1]; C=R/'code'; S=R/'staging/unknown_cost_coordinated_install'
sys.path[:0]=[str(C/'src'),str(R/'staging/8ac4_curated_scope_stop')]
from context_access_r3 import sha, read, now
from page_attempts import atomic_json
from adopt_8ac4 import safe_point
from stage1d_runtime import Runtime

def prepare():
    rows=[]; refs=[]
    for folder, filename in [('unknown_cost_boundary_core_candidate','INSTALLATION_MANIFEST.json'),
        ('unknown_cost_boundary_registry_candidate','INSTALLATION_MANIFEST.json'),
        ('unknown_cost_boundary_context_candidate','INSTALLATION_MANIFEST.json'),
        ('unknown_cost_request_guard_candidate','INSTALLATION_MANIFEST.json'),
        ('8ac4_curated_scope_stop','INSTALLATION_MANIFEST.json'),
        ('bq_cost_guard_entry_candidate','INSTALLATION_FILES.json')]:
        root=R/'staging'/folder; mp=root/filename; doc=read(mp)
        refs.append({'path':mp.relative_to(R).as_posix(),'sha256':sha(mp)})
        for row in doc.get('files',doc.get('installation_files',[])):
            rel=row.get('path',row.get('source',row.get('candidate_path')))
            target=row.get('production_target','code/'+row.get('target',row.get('install_path',rel)))
            rows.append({'source':(root/rel).relative_to(R).as_posix(),'target':target,
                'base_sha256':row['base_sha256'],'new_sha256':row.get('new_sha256',row.get('sha256'))})
    root=R/'staging_root/unknown_cost_collector_candidate'
    for rel,base in [('src/collector.py','93f9641458ab03d2f344837b162f67c70bc2b74843dafa93037f8701307b74b1'),
        ('src/stage1d_transfers_acquisition.py','e9ccb925c68445047d9a5f1a9e11d044c98c2df4ed9e7af8fc0773b11f212796'),
        ('tests/test_unknown_cost_collector.py',None)]:
        rows.append({'source':(root/rel).relative_to(R).as_posix(),'target':'code/'+rel,
            'base_sha256':base,'new_sha256':sha(root/rel)})
    root=R/'staging/transfers_post_cache_selection_candidate'
    for rel,target,base in [('src/execute_current_transfers.py','scripts/execute_current_transfers.py','7cb422547cffec22c6fa4780d651cf66c8f0e27459c706d85aafb9a85439e4c0'),
        ('tests/test_transfers_post_cache_selection.py','code/tests/test_transfers_post_cache_selection.py',None)]:
        source=root/rel
        if not source.exists() and rel.endswith('execute_current_transfers.py'): source=root/'scripts/execute_current_transfers.py'
        rows.append({'source':source.relative_to(R).as_posix(),'target':target,'base_sha256':base,'new_sha256':sha(source)})
    if len({r['target'] for r in rows})!=len(rows): raise ValueError('Overlapping installation targets')
    S.mkdir(parents=True,exist_ok=True)
    for row in rows:
        source=R/row['source']; target=R/row['target']
        if sha(source)!=row['new_sha256'] or (sha(target) if target.exists() else None)!=row['base_sha256']:
            raise ValueError('Source/base differs: '+row['target'])
        saved=S/'files'/row['target']; saved.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(source,saved); row['reviewed_origin']=row['source']; row['source']=saved.relative_to(R).as_posix()
    doc={'authorization_id':'STAGE1D_UNKNOWN_CONTRACT_OR_RATE20_BOUNDARY_V1','status':'PREPARED_NOT_INSTALLED',
         'installation':rows,'candidate_manifests':refs,'new_external_requests':0}
    atomic_json(S/'INSTALLATION_MANIFEST.json',doc)
    print(json.dumps({'manifest':str(S/'INSTALLATION_MANIFEST.json'),'sha256':sha(S/'INSTALLATION_MANIFEST.json'),'files':len(rows)}))

def install(digest):
    mp=S/'INSTALLATION_MANIFEST.json'
    if sha(mp)!=digest: raise ValueError('Exact prepared manifest required')
    doc=read(mp); safe_point(C); Runtime().require_gate(C)
    if (R/'operations/CURRENT_COST_EVIDENCE_DRIVER.lock').exists(): raise ValueError('Evidence writer active')
    for ref in doc['candidate_manifests']:
        if sha(R/ref['path'])!=ref['sha256']: raise ValueError('Candidate manifest changed')
    for row in doc['installation']:
        source=(R/row['source']).resolve(); target=(R/row['target']).resolve()
        if not source.is_relative_to(S) or not (target.is_relative_to(C/'src') or target.is_relative_to(C/'tests') or target==R/'scripts/execute_current_transfers.py'):
            raise ValueError('Undeclared path')
        if sha(source)!=row['new_sha256'] or (sha(target) if target.exists() else None)!=row['base_sha256']: raise ValueError('Source/base changed')
    backup=R/'snapshot'/('unknown_cost_source_adoption_'+now().replace(':','').replace('+','_')); backup.mkdir(parents=True)
    preserved=['code/STAGE1D_PREFLIGHT_GATE.json','code/private/stage1d_semantics/SEMANTIC_CAPABILITY_GATE.json','code/private/stage1d_semantics/CURRENT.json']+[r['target'] for r in doc['installation']]
    for rel in preserved:
        source=R/rel
        if source.exists():
            saved=backup/rel;saved.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source,saved)
            if sha(source)!=sha(saved):raise ValueError('Backup differs')
    prior={'path':(backup/'code/STAGE1D_PREFLIGHT_GATE.json').relative_to(R).as_posix(),'sha256':sha(backup/'code/STAGE1D_PREFLIGHT_GATE.json')}
    gate=read(C/'STAGE1D_PREFLIGHT_GATE.json');gate.update(status='PENDING_AFFECTED_TESTS',source_adoption_started_at_utc=now());atomic_json(C/'STAGE1D_PREFLIGHT_GATE.json',gate)
    for row in doc['installation']:
        target=R/row['target'];target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(R/row['source'],target)
        if sha(target)!=row['new_sha256']:raise ValueError('Installed source differs')
    receipt=dict(status='INSTALLED_PENDING_AFFECTED_TESTS',prior_gate=prior,installation_manifest={'path':mp.relative_to(R).as_posix(),'sha256':digest},files=doc['installation'],new_external_requests=0,inflight_cancelled=0,created_at_utc=now())
    atomic_json(backup/'INSTALLATION_RECEIPT.json',receipt)
    atomic_json(R/'reports/unknown_cost_boundary_v1/SOURCE_INSTALLATION_RECEIPT.json',receipt)
    print(json.dumps({'status':receipt['status'],'prior_gate':prior,'files':len(doc['installation'])}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--prepare',action='store_true');p.add_argument('--install-sha256');a=p.parse_args()
    if a.prepare == bool(a.install_sha256):p.error('Choose prepare or exact install SHA')
    prepare() if a.prepare else install(a.install_sha256)
