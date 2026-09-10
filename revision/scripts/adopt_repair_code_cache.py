"""Adopt the five pending saved successes; prove idempotence without requests."""
from pathlib import Path
import sys,socket,json,time,traceback
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path.insert(0,str(C/'src'))
def blocked(*a,**k):raise RuntimeError('Offline saved-code admission')
socket.socket.connect=blocked;socket.create_connection=blocked
from stage1d_code_cache_admission import admit_operation
from context_access_r3 import sha
from page_attempts import atomic_json
p=R/'operations/unknown_cost_code_e8faf9ccbaaf4f129f6cb0fec4747e8c.json'
out=R/'operations/integration_repair';out.mkdir(exist_ok=True)
ref={'path':'../'+p.relative_to(R).as_posix(),'sha256':sha(p)}
dbs=[C/'private/shared_budget_r4.sqlite',C/'private/read_retry_r4.sqlite',C/'private/dune_request_attempts.sqlite']
before={p.name:sha(p) for p in dbs};start=time.perf_counter()
try:
    result=admit_operation(C,ref,apply=True)
    atomic_json(out/'F03_ADOPTION.json',result)
    repeated=admit_operation(C,ref,apply=True)
    atomic_json(out/'F03_IDEMPOTENT_REUSE.json',repeated)
    after={p.name:sha(p) for p in dbs}
    if before!=after:raise ValueError('Offline adoption changed shared accounting')
    if any(r['new_descriptor'] for r in repeated['admitted']):raise ValueError('Adoption is not idempotent')
    summary={'status':'SUCCESSFUL_CACHE_ADOPTED_REPLAY_REQUIRED','admitted':len(result['admitted']),
        'gaps':result['gaps'],'affected_queries':result['affected_queries'],
        'affected_states':len(result['affected_states']),'catalog':result['current_catalog_sha256'],
        'idempotent':True,'database_sha_before':before,'database_sha_after':after,
        'seconds':time.perf_counter()-start,'new_requests':0,'new_online_sessions':0}
    atomic_json(out/'F03_ADOPTION_RECEIPT.json',summary);print(json.dumps(summary),flush=True)
except Exception:
    (out/'F03_ADOPTION_FAILURE.txt').write_text(traceback.format_exc(),encoding='utf-8');raise
