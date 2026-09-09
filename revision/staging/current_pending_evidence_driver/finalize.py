import difflib,hashlib,json
from pathlib import Path
S=Path(__file__).resolve().parent;R=S.parent.parent
def ref(p):
    raw=p.read_bytes();return {'path':p.relative_to(R).as_posix(),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
candidate=S/'execute_unknown_cost_evidence.py';base=S/'BASE_execute_unknown_cost_evidence.py'
receipt=S/'TEST_RECEIPT_03.json';test=json.loads(receipt.read_text())
assert test['status']=='PASS' and test['tests']==5 and test['source_stable']
assert test['source_after'][candidate.relative_to(R).as_posix()]==ref(candidate)['sha256']
(S/'DIFF.patch').write_text(''.join(difflib.unified_diff(base.read_text().splitlines(True),candidate.read_text().splitlines(True),
    fromfile='scripts/execute_unknown_cost_evidence.py',tofile='candidate/execute_unknown_cost_evidence.py')),encoding='utf-8')
manifest={'schema_version':'stage1d-current-pending-evidence-driver-candidate-v1','status':'READY_STAGED_NOT_INSTALLED',
    'installation_files':[dict(ref(candidate),production_target='scripts/execute_unknown_cost_evidence.py',base_sha256=ref(base)['sha256'])],
    'tests':5,'test_receipt':ref(receipt),'source_dependencies':test['source_after'],
    'real_network_requests':0,'production_writes':0,'production_graph_reads':0,'production_sqlite_reads':0,
    'new_budget_or_retry_allowance':False,'new_request_identity':False,'new_labels_mode':False,
    'artifacts':[ref(p) for p in sorted(S.iterdir()) if p.is_file() and p.name!='MANIFEST.json']}
(S/'MANIFEST.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'manifest':ref(S/'MANIFEST.json'),'candidate':ref(candidate),'base_sha256':ref(base)['sha256']},indent=2))
