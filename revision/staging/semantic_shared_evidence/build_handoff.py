"""Write only staged handoff metadata; never install or touch active files."""
from pathlib import Path
import difflib, hashlib, json
S=Path(__file__).resolve().parent;C=S.parents[1]/'code'
def sha(raw):return hashlib.sha256(raw).hexdigest()
files=[];diff=[]
for source in sorted((S/'src').glob('*.py')):
    active=C/'src'/source.name;old=active.read_bytes() if active.exists() else None;new=source.read_bytes()
    files.append({'path':'src/'+source.name,'action':'ADD' if old is None else 'REPLACE_AFTER_BASE_SHA_CHECK',
        'base_sha256':sha(old) if old is not None else None,'new_sha256':sha(new),'bytes':len(new)})
    diff.extend(difflib.unified_diff(old.decode().splitlines(True) if old else [],new.decode().splitlines(True),
        fromfile='a/src/'+source.name if old else '/dev/null',tofile='b/src/'+source.name))
test=S/'tests/test_stage1d_shared_evidence.py'
files.append({'path':'tests/'+test.name,'action':'ADD','base_sha256':None,'new_sha256':sha(test.read_bytes()),'bytes':test.stat().st_size})
manifest={'status':'STAGED_READY_NOT_INSTALLED','production_modified':False,'files':files,
 'new_test_dependencies':['test_stage1d_bq_portable.py','test_stage1d_batch_binding_route.py','test_bq_context_prepare.py',
    'semantic_weth_fixture.py','test_stage1d_multiasset_pipeline.py'],
 'installation_requirements':['Check each current active base SHA before copying.',
    'Install only listed src files and the one new test; existing test copies are staging fixtures, unchanged.',
    'Include src/stage1d_shared_evidence.py in live capability gate source inventory; refresh complete gate after source install.',
    'Adopt explicit shared catalogue entry and keep whole serialized shared pool in frozen input closure.',
    'Do not copy runtime ValidationSession or VerifiedEvidenceContext objects into portable artifacts.'],
 'test_receipt':json.loads((S/'checks/TEST_RECEIPT.json').read_text()),
 'related_closure_context_staging_modified':False}
(S/'INSTALLATION_MANIFEST.json').write_text(json.dumps(manifest,indent=2),encoding='utf8')
(S/'MINIMAL_SOURCE_DIFF.patch').write_text(''.join(diff),encoding='utf8',newline='\n')
print(json.dumps({'files':files,'status':manifest['status']},indent=2))
