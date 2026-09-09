from pathlib import Path
from copy import deepcopy
import sys,json,hashlib,shutil
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path.insert(0,str(C/'src'))
from collector import Scope
from stage1d_closure_scope import *
from page_attempts import atomic_json
assert not (C/'private/network_worker.lock').exists()
old=read(C/HISTORICAL);assert sha(C/HISTORICAL)==OLD_SHA
new=deepcopy(old)
new.update(schema_version='stage1d-window-semantic-batch-query-freeze-v1',authorization_id=AUTH,
 historical_batch_sha256=OLD_SHA,clock_policy='PER_QUERY_ALL_MODES_CUMULATIVE_43200_SECONDS_SAME_RECOVERY_POOL',
 policy_sha256=POLICY_SHA,scope_version='STAGE1D_QUERY_WINDOW_SEMANTICS_V2',supported_semantics_version='STAGE1D_CANONICAL_WETH_1TO1_V1')
changes=[]
for q in new['queries']:
 prior=deepcopy(q);w=VALUES[q['name']];mode='REFERENCE_FULL' if w is None else MODE
 q.update(window_mode=mode,primary_window_mode=mode,local_window_seconds=w,primary_local_window_seconds=w,
          fallback_window_mode=None,fallback_local_window_seconds=None,scope_id=None,
          window_controls_observation_only=True,lp_fund_age_constraint=False,
          supported_semantics_version='STAGE1D_CANONICAL_WETH_1TO1_V1')
 scope=Scope.from_policy(q);q.update(scope=scope.freeze_dict(),scope_id=scope.scope_id,scope_hash=scope.scope_hash)
 q['execution_priority']={'txphish_src001':1,'txphish_src002':2,'xscam_src001':3,'lifi_src001':4}[q['name']]
 changes.append({'query':q['name'],'W_seconds':w,'display_ceil_days':None if w is None else (w+86399)//86400,
 'old_scope_hash':prior['scope_hash'],'new_scope_hash':q['scope_hash'],'new_scope_id':q['scope_id'],
 'global_times_blocks_seed_depth_unchanged':True,'old_FULL_retained':True,'short_window_is_not_FULL':w is not None})
authority=C/'private/stage1d_authority'
for src,name in [('config/ACTIVE_POLICY.json','STAGE1D_WINDOW_SEMANTIC_POLICY.json'),('config/QUERY_SCOPES.json','STAGE1D_WINDOW_SEMANTIC_QUERY_CONFIG.json')]:
 target=authority/name
 if target.exists():assert target.read_bytes()==(R/'authorization/package'/src).read_bytes()
 else:shutil.copyfile(R/'authorization/package'/src,target)
def ref(p):return {'path':p.relative_to(C).as_posix(),'sha256':sha(p)}
atomic_json(C/CURRENT,new)
atomic_json(C/PROOF,{'authorization_id':AUTH,'current_batch':ref(C/CURRENT),
 'historical_batch':ref(C/HISTORICAL),'policy':ref(authority/'STAGE1D_WINDOW_SEMANTIC_POLICY.json'),
 'query_config':ref(authority/'STAGE1D_WINDOW_SEMANTIC_QUERY_CONFIG.json'),
 'new_budget_pool':False,'reset_counters':False,'lp_fund_age_constraint':False,
 'changes':changes,'authority_note':'Current collection scope supersedes report pause; Recovery resource/retry authority unchanged.'})
assert active_batch_path(C)==C/CURRENT
atomic_json(R/'WINDOW_SCOPE_ADOPTION.json',{'status':'ADOPTED_LOCAL_ONLY','queries':changes,'active_batch_sha256':sha(C/CURRENT),'new_requests':0})
print(json.dumps(changes,indent=2))
