"""One offline current-source regression run; gate is minted only on real PASS."""
from pathlib import Path
import sys,os,json,time,hashlib,socket,unittest,tempfile,traceback,argparse
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C/'tests'),str(C)]
os.chdir(C);os.environ['PYTHONDONTWRITEBYTECODE']='1';os.environ['PYTHONPATH']=str(C/'src')+os.pathsep+str(C/'tests')
tmp=C/'test_scratch/closure_preflight';tmp.mkdir(parents=True,exist_ok=True)
tempfile.tempdir=str(tmp);os.environ['TEMP']=str(tmp);os.environ['TMP']=str(tmp)
attempts=[]
def blocked(*a,**k):attempts.append('socket connection blocked');raise RuntimeError('Closure preflight network is disabled')
socket.socket.connect=blocked;socket.create_connection=blocked
from context_access_r3 import read,sha,now
from page_attempts import atomic_json
from stage1d_closure_scope import active_batch_path
parser=argparse.ArgumentParser();parser.add_argument('--mint-gate',action='store_true');parser.add_argument('--modules',nargs='*');parser.add_argument('--baseline-gate',type=Path);a=parser.parse_args()
folder=C/'checks/closure_preflight';folder.mkdir(parents=True,exist_ok=True)
started=now();stamp=time.perf_counter()
source_paths=list((C/'src').glob('*.py'))+list((C/'tests').glob('*.py'))
before={p.relative_to(C).as_posix():sha(p) for p in source_paths}
modules=a.modules or [p.stem for p in sorted((C/'tests').glob('test*.py'))]
test_suites=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromName(m) for m in modules)
log=folder/('TEST_LOG_'+started.replace(':','').replace('+','_')+'.txt')
with log.open('w',encoding='utf8') as stream:result=unittest.TextTestRunner(stream=stream,verbosity=2).run(test_suites)
after={p.relative_to(C).as_posix():sha(p) for p in source_paths}
changed=[p for p in before if before[p]!=after[p]]
receipt={'schema_version':'stage1d-closure-preflight-v1','status':'PASS' if result.wasSuccessful() and not result.skipped and not changed and not attempts else 'FAIL',
 'started_at_utc':started,'ended_at_utc':now(),'tests_run':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),
 'skipped':len(result.skipped),'failure_details':[{'test':str(t),'traceback':v} for t,v in result.failures+result.errors],
 'skip_details':[{'test':str(t),'reason':v} for t,v in result.skipped],
 'seconds':time.perf_counter()-stamp,'network_attempts_blocked':len(attempts),'actual_external_requests':0,
 'source_sha256':before,'source_changed_during_tests':changed,'modules':modules,'log':{'path':log.relative_to(C).as_posix(),'sha256':sha(log)}}
if a.baseline_gate:
 baseline_path=a.baseline_gate.resolve()
 if not baseline_path.is_relative_to(R) or not baseline_path.is_file():raise ValueError('Prior gate must be preserved inside this revision')
 baseline=read(baseline_path);prior_ref=baseline['test_receipt'];prior_path=C/prior_ref['path']
 if baseline.get('status')!='PASS' or sha(prior_path)!=prior_ref['sha256'] or read(prior_path).get('status')!='PASS':raise ValueError('Prior regression gate is not verified PASS')
 receipt['validation_scope']='AFFECTED_TESTS_AFTER_PRIOR_REGRESSION_PASS' if a.modules else 'FULL_REGRESSION'
 receipt['prior_gate']={'path':baseline_path.relative_to(R).as_posix(),'sha256':sha(baseline_path),'test_receipt':prior_ref}
 receipt['source_changes_since_prior_gate']=sorted(p.name for p in (C/'src').glob('*.py') if baseline['source_sha256'].get(p.name)!=sha(p))
receipt_path=folder/('TEST_RECEIPT_'+started.replace(':','').replace('+','_')+'.json');atomic_json(receipt_path,receipt)
atomic_json(folder/'CURRENT_TEST_RECEIPT.json',{'status':receipt['status'],'path':receipt_path.relative_to(C).as_posix(),'sha256':sha(receipt_path)})
if a.mint_gate:
 if a.modules and not a.baseline_gate:raise ValueError('An incremental source gate must retain its prior passed regression evidence')
 if receipt['status']!='PASS':raise RuntimeError('Tests failed/changed/skipped/network attempted: gate stays closed')
 from stage1d_semantic_units import REQUIRED_CAPABILITIES,REQUIRED_GATE_SOURCES,SEMANTIC_VERSION,validate_live_capability_gate
 required=set(REQUIRED_GATE_SOURCES)|{'stage1d_task_boundaries.py','stage1d_finite_state_route.py','stage1d_finite_state_rpc.py','stage1d_weth_log_index.py'}
 missing=required-set(p.name for p in (C/'src').glob('*.py'))
 if missing:raise ValueError('Actual pipeline source missing: '+repr(sorted(missing)))
 source_sha={p.name:sha(p) for p in (C/'src').glob('*.py')}
 semantic={'status':'PASS','semantic_version':SEMANTIC_VERSION,'capabilities':{k:True for k in REQUIRED_CAPABILITIES},
  'source_sha256':source_sha,'test_evidence':[{'path':receipt_path.relative_to(C).as_posix(),'sha256':sha(receipt_path),'status':'PASS'}]}
 validate_live_capability_gate(semantic,C)
 sempath=C/'private/stage1d_semantics/SEMANTIC_CAPABILITY_GATE.json';atomic_json(sempath,semantic)
 gate={'status':'PASS','authorization_id':'STAGE1D_BATCH01_REFERENCE_FULL_V1',
  'closure_authorization_id':'STAGE1D_WINDOW_ROLE_SEMANTIC_CLOSURE_V1','closure_batch_sha256':sha(active_batch_path(C)),
  'old_and_new_tests_passed':True,'window_semantic_controlled_gate_passed':True,'source_sha256':source_sha,
  'test_receipt':{'path':receipt_path.relative_to(C).as_posix(),'sha256':sha(receipt_path)},
  'semantic_gate':{'path':sempath.relative_to(C).as_posix(),'sha256':sha(sempath)},
  'resource_pool':'CONTINUED_MIGRATED_RECOVERY_POOL_NOT_NEW','clock':'PER_QUERY_ALL_MODES_43200',
  'user_boundary':{'path':'private/stage1d_roles/USER_TASK_BOUNDARIES.json','sha256':sha(C/'private/stage1d_roles/USER_TASK_BOUNDARIES.json')},
  'source_previous_revision':read(R/'CONTINUATION_RECEIPT.json'),'created_at_utc':now()}
 if a.baseline_gate:gate['incremental_validation_after_prior_gate']=receipt['prior_gate'];gate['source_changes_since_prior_gate']=receipt['source_changes_since_prior_gate']
 atomic_json(C/'STAGE1D_PREFLIGHT_GATE.json',gate)
 from stage1d_runtime import Runtime
 Runtime().require_gate(C)
print(json.dumps({k:receipt[k] for k in ('status','tests_run','failures','errors','skipped','seconds','network_attempts_blocked','source_changed_during_tests')}),flush=True)
if receipt['status']!='PASS':sys.exit(1)
