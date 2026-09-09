"""Freeze only this staging handoff; no installation or production mutation."""
import difflib, hashlib, json
from pathlib import Path
S=Path(__file__).resolve().parent; R=S.parent.parent; C=R/'code'
ROOT=R/'staging_root/unknown_cost_collector_candidate'
CTX=R/'staging/unknown_cost_boundary_context_candidate'
def ref(p):
    data=p.read_bytes();return {'path':p.relative_to(R).as_posix(),'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
def write(p,v):p.write_text(json.dumps(v,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
receipt=S/'TEST_RECEIPT_03.json'; checked=json.loads(receipt.read_text())
assert checked['status']=='PASS' and checked['tests']==42 and checked['source_stable']
files=[];diff=[]
for path,base,target in [(ROOT/'src/collector.py',S/'base/collector.py','src/collector.py'),
                         (ROOT/'tests/test_unknown_cost_collector.py',S/'base/test_unknown_cost_collector.py','tests/test_unknown_cost_collector.py'),
                         (CTX/'src/stage1d_cost_boundary_context.py',S/'base/stage1d_cost_boundary_context.py','src/stage1d_cost_boundary_context.py')]:
    item=ref(path);assert checked['source_after'][item['path']]==item['sha256']
    item.update(production_target='code/'+target,base_staged_sha256=ref(base)['sha256'],
                production_base_sha256=ref(C/target)['sha256'] if (C/target).exists() else None)
    files.append(item)
    diff.extend(difflib.unified_diff(base.read_text().splitlines(True),path.read_text().splitlines(True),
                                    fromfile='base/'+target,tofile='candidate/'+target))
(S/'DIFF.patch').write_text(''.join(diff),encoding='utf-8')
review={
 'status':'PASS_READ_ONLY_SOURCE_REVIEW','source':ref(ROOT/'src/stage1d_transfers_acquisition.py'),
 'checks':[
  {'check':'prepare cost guard before cache/plan write or provider dispatch','lines':[276,277],'result':'PASS'},
  {'check':'acquire revalidates current collection/policy/Registry and compares immutable plan guard','lines':[795,796,798],'result':'PASS'},
  {'check':'guard precedes first timestamp header mapping and all page/point requests','lines':[829,885],'result':'PASS'},
  {'check':'same address STOP/PENDING cannot authorize ordinary interval; independent legal arrival union remains eligible','result':'PASS_SOURCE_AND_GUARD_TESTS'},
  {'check':'existing immutable request keys/counters/session identities are not changed by reviewed guard wiring','result':'PASS_SOURCE_ONLY'}],
 'limitations':['Read-only source review; no production/dynamic graph/provider execution.',
  'The worker remains governed by root single-writer discipline; no concurrent role/policy mutation is permitted.'],
 'network_calls':0,'production_writes':0}
write(S/'TRANSFERS_REVIEW.json',review)
(S/'HANDOFF.md').write_text('''# Collector cost and finite semantic supplement

READY_STAGED_NOT_INSTALLED. Install through the root coordinated source gate.

Changed only the root staged Collector, its test file, and the staged cost context helper. No semantic resolver source or request guard changed.

An explicitly disabled real Registry now yields saved version-bound BYPASS decisions. Context validation and the current request guard reverify the same registry/policy; no policy-removal shortcut was introduced. The old absent-policy output remains unchanged.

For a STOP/PENDING state, the existing validated finite catalogue supplies only its already certified native legs to the existing exact state resolver. This runs after prior user/service/unsupported-protocol/component/depth stops. Each accepted unit retains exact membership, physical conflict checks, one physical hop and its independent output arrival. Normal interval discovery stays stopped; the cost decision is never changed into a holder-wide BYPASS. Saved continuation metadata explicitly denies ordinary discovery and ledger completeness claims.

Context accepts a cost-stopped holder dependency only after matching exact saved state, decision hash, catalogue identity, current query/scope, strict legal order, portable unit certificate, physical native candidate and semantic membership. It preserves both required asset ledgers and Gas. The actual required_windows fixture keeps only observed block spans (ends at block 4, not global block 20). It does not assert balances, receipts or complete context have been acquired. The existing conditional outside-reservoir mathematics and all seven methods are unchanged.

42 bounded synthetic checks PASS: 12 Collector (six new), original 16 context, original 14 request guard; current role-bound Registry afe79fa... is included. Network, optimizers, seven-method dispatch and production graph reads were denied/not invoked. One initial legacy malformed-unit error was corrected without weakening the expected fail-closed behavior; all run receipts are retained. No full regression suite was run here.

Run: python -B staging/unknown_cost_collector_semantic_fix/run_checks.py (from revision root).
See INSTALLATION_MANIFEST.json, TEST_RECEIPT_03.json and TRANSFERS_REVIEW.json.
''',encoding='utf-8')
context_manifest=json.loads((CTX/'INSTALLATION_MANIFEST.json').read_text())
for item in context_manifest['installation_files']:
    if item['path']=='src/stage1d_cost_boundary_context.py':
        item.update(sha256=ref(CTX/item['path'])['sha256'],bytes=(CTX/item['path']).stat().st_size,
                    previous_staged_sha256=ref(S/'base/stage1d_cost_boundary_context.py')['sha256'])
context_manifest['finite_semantic_supplement']={
 'receipt':ref(receipt),'tests':42,'status':'PASS',
 'collector_sha256':files[0]['sha256'],
 'note':'Original 16-test receipt remains historical; source-current validation is the bound 42-test supplement.'}
write(CTX/'INSTALLATION_MANIFEST.json',context_manifest)
manifest={'schema_version':'stage1d-cost-collector-finite-semantic-supplement-v1',
 'status':'READY_STAGED_NOT_INSTALLED','installation_files':files,
 'current_context_installation_manifest':ref(CTX/'INSTALLATION_MANIFEST.json'),
 'test_receipt':ref(receipt),'tests':42,'failures':0,'errors':0,'skipped':0,
 'dependencies':checked['source_after'],
 'artifacts':[ref(p) for p in sorted(S.iterdir()) if p.is_file() and p.name!='INSTALLATION_MANIFEST.json'],
 'network_calls':0,'production_writes':0,'production_graph_reads':0,'optimizer_calls':0,
 'ordinary_discovery_exemption':False,'main_checkpoint_claimed':False,
 'no_new_semantic_source_module':True}
write(S/'INSTALLATION_MANIFEST.json',manifest)
print(json.dumps({'manifest':ref(S/'INSTALLATION_MANIFEST.json'),
                  'context_manifest':ref(CTX/'INSTALLATION_MANIFEST.json'),'files':files},indent=2))
