"""Existing local ledger only; no account lookup, constructors, or counter edits."""
from pathlib import Path
import sys,json,sqlite3,socket
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from stage1d_recovery_policy import RecoveryLedger
from stage1d_runtime import Runtime
from stage1d_closure_scope import active_batch
from context_access_r3 import sha,now
from page_attempts import atomic_json
def blocked(*a,**k):raise RuntimeError('Local snapshot has no network')
socket.socket.connect=blocked;socket.create_connection=blocked
path=C/'private/shared_budget_r4.sqlite'
ledger=object.__new__(RecoveryLedger);ledger.path=path
with sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True) as db:
    db.execute('PRAGMA query_only=ON');db.execute('BEGIN');snapshot=ledger.snapshot(db);db.rollback()
runtime=Runtime();stamp=now()
clocks={q['name']:str(runtime.clock_used(C,q['name'])) for q in active_batch(C)['queries']}
record={'created_at_utc':stamp,'snapshot':snapshot,'source_path':path.relative_to(C).as_posix(),
    'live_account_usage':None,'new_external_requests':0,'counters_reset':False,
    'per_query_seconds_all_modes':clocks,'per_query_cap_seconds':43200,
    'alchemy_meter_note':'Ledger actual zero means no posted exact CU value, not zero account use'}
out=R/'operations'/('local_resource_snapshot_'+stamp.replace(':','').replace('+','_')+'.json');atomic_json(out,record)
print(json.dumps({'path':out.relative_to(R).as_posix(),'sha256':sha(out),'clocks':clocks,
    'bigquery_bytes':snapshot['bigquery_bytes']['actual'],'dune_risk':snapshot['dune_credits']['cumulative_risk'],
    'rpc_operations':{k:snapshot['rpc_operations'][k] for k in ('actual','reserved','remaining')},
    'alchemy_cu_reservation':snapshot['alchemy_cu']['reserved'],'live_account_usage':None,'new_external_requests':0}))
