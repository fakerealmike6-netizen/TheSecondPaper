"""Append-only execution identity; the Stage1C scientific freeze is immutable."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,re

PARENT_COMMIT='d88b839361b8fa93308430641baf7af423d76ae8'
PARENT_TREE='cef2f09aaa6033e9cb7393d6dc5cad6119a4f450'
PARENT_FREEZE_SHA256='e22250a3689e0cbb5837c000bd332fcb74171fc58f3bc013a6a340a0ec4d6e06'
ALLOWED_CHANGED_SOURCE={'src/run_stage1c.py','src/stage1c_reports.py','src/validate_stage1c.py'}
EXECUTION_POLICY_KEYS=('schema_version','stage','checkpoint','authorization_id','baseline_commit','baseline_tree',
    'baseline_run','external_acceptance','repairs','integration_review','validation_contract','experiments',
    'required_faults','required_positive_controls','required_additional_integration_controls','network',
    'next_batch','publication','delivery')
def file_hash(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def read(path):return json.loads(Path(path).read_text(encoding='utf-8'))
def inventory(tree):
    return [{'path':p.relative_to(tree).as_posix(),'sha256':file_hash(p)} for folder in ('src','tests')
            for p in sorted((tree/folder).rglob('*.py')) if '__pycache__' not in p.parts]
def _parent_unchanged(tree,parent):
    if file_hash(tree/'EXPERIMENT_FREEZE.json')!=PARENT_FREEZE_SHA256:raise ValueError('Accepted parent freeze identity changed')
    current={r['path']:r['sha256'] for r in inventory(tree)}
    changes=[]
    for row in parent['source_inventory']:
        name=row['path']
        if name not in current:raise ValueError('Inherited source/test removed: '+name)
        if current[name]!=row['sha256']:
            if name not in ALLOWED_CHANGED_SOURCE:raise ValueError('Unapproved inherited implementation/test change: '+name)
            changes.append(name)
    return changes
def verify_revision(tree,parent):
    path=tree/'REVISION_FREEZE.json';rev=read(path)
    if rev.get('schema_version')!='stage1c-r1-execution-freeze-1.0':raise ValueError('Unknown revision freeze schema')
    if rev.get('parent_commit')!=PARENT_COMMIT or rev.get('parent_tree')!=PARENT_TREE:raise ValueError('Wrong revision parent')
    if rev.get('parent_freeze_sha256')!=file_hash(tree/'EXPERIMENT_FREEZE.json'):raise ValueError('Parent scientific freeze changed')
    if rev.get('source_inventory')!=inventory(tree):raise ValueError('Revision source/test identity changed')
    if rev.get('changed_inherited_source')!=_parent_unchanged(tree,parent):raise ValueError('Inherited change inventory mismatch')
    private_policy=tree/'configs/STAGE1C_R1_POLICY.json'
    if private_policy.exists() and file_hash(private_policy)!=rev.get('private_authorization_policy_sha256'):raise ValueError('Private authorization policy changed')
    for row in rev['additional_execution_inputs']:
        p=tree/row['path']
        if not p.resolve().is_relative_to(tree.resolve()) or file_hash(p)!=row['sha256']:raise ValueError('Revision execution policy changed')
    return {'version':rev['version'],'freeze_sha256':file_hash(path),'parent_freeze_sha256':rev['parent_freeze_sha256'],
            'source_inventory_sha256':hashlib.sha256(json.dumps(rev['source_inventory'],sort_keys=True,separators=(',',':')).encode()).hexdigest()}
def freeze_revision(tree,version):
    tree=Path(tree);parent=read(tree/'EXPERIMENT_FREEZE.json')
    changed=_parent_unchanged(tree,parent)
    if not re.fullmatch(r'stage1c-r1-[A-Za-z0-9][A-Za-z0-9._-]*',version):raise ValueError('Explicit safe stage1c-r1 version required')
    policy=tree/'configs/STAGE1C_R1_POLICY.json'
    policy_value=read(policy)
    if policy_value.get('authorization_id')!='STAGE1C_R1_OUTPUT_CONTRACT_AND_INTEGRATION_V1':raise ValueError('Wrong revision authorization')
    # The authoritative full policy remains private and unchanged. Public replay
    # needs the exact execution contract, not historical account billing figures.
    effective=tree/'configs/STAGE1C_R1_EXECUTION_POLICY.json'
    policy_bytes=(json.dumps({k:policy_value[k] for k in EXECUTION_POLICY_KEYS},sort_keys=True,indent=2)+'\n').encode()
    if effective.exists() and effective.read_bytes()!=policy_bytes:raise ValueError('Existing execution policy cannot be replaced')
    if not effective.exists():effective.write_bytes(policy_bytes)
    # Verify immutable scientific identities before allowing an execution freeze.
    for name,key in [('EXPERIMENT_INPUTS.json','inputs_manifest_sha256'),('configs/STAGE1C_EFFECTIVE_POLICY.json','effective_policy_sha256'),('METHOD_SPEC_EFFECTIVE.md','method_spec_sha256'),('controlled_v1/MANIFEST.json','controlled_manifest_sha256')]:
        if file_hash(tree/name)!=parent[key]:raise ValueError('Scientific identity changed: '+name)
    value={'schema_version':'stage1c-r1-execution-freeze-1.0','version':version,'stage':'Stage1C-R1',
           'frozen_at_utc':datetime.now(timezone.utc).isoformat(),'parent_commit':PARENT_COMMIT,'parent_tree':PARENT_TREE,
           'parent_freeze_sha256':file_hash(tree/'EXPERIMENT_FREEZE.json'),'source_inventory':inventory(tree),
           'changed_inherited_source':changed,'additional_execution_inputs':[{'path':'configs/STAGE1C_R1_EXECUTION_POLICY.json','sha256':file_hash(effective)}],
           'private_authorization_policy_sha256':file_hash(policy),
           'scientific_methods_and_inputs_changed':False,'warmups':parent['warmups'],'timed_repetitions':parent['timed_repetitions'],
           'method_versions_inherited':parent['method_versions'],'checkpoint':'CHECKPOINT_1C_R1_REACHED'}
    path=tree/'REVISION_FREEZE.json'
    if path.exists():
        prior=read(path)
        if not re.fullmatch(r'stage1c-r1-[A-Za-z0-9][A-Za-z0-9._-]*',prior['version']):raise ValueError('Unsafe prior revision version')
        if prior['version']==version:raise ValueError('Revision version exists; use a new execution version')
        history=tree/'revision_freeze_history'/f"{prior['version']}.json"
        history.parent.mkdir(exist_ok=True)
        with history.open('xb') as out:out.write(path.read_bytes())
    path.write_text(json.dumps(value,sort_keys=True,indent=2)+'\n',encoding='utf-8')
    return value
