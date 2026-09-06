"""One specific user-authorized nine-address retry; no new stage budget grant."""
import hashlib,json,re
from pathlib import Path
from datetime import datetime

CAP5_AUTH='STAGE1B_R1_FRONTIER_LABEL_CAP5_ONCE_V1'
PENDING='PENDING_USER_CONFIRMATION'
CONFIRMED='USER_CONFIRMED_CAP5'
RESTORED='RESTORED_1_CONFIRMED'
TERMINAL={'QUERY_STATE_COMPLETED','QUERY_STATE_FAILED','QUERY_STATE_CANCELLED'}

def require_timestamp(value):
    if not isinstance(value,str):raise RuntimeError('Timestamped manual user confirmation required')
    try:t=datetime.fromisoformat(value.replace('Z','+00:00'))
    except ValueError as exc:raise RuntimeError('Invalid confirmation timestamp') from exc
    if t.tzinfo is None:raise RuntimeError('Confirmation time must include timezone')

def read_control(work):
    p=Path(work)/'private/dune_cap5_control.json'
    return json.loads(p.read_text(encoding='utf-8')) if p.exists() else None

def binding(control):
    if control.get('authorization_id')!=CAP5_AUTH:raise RuntimeError('Unknown one-time exception authorization')
    if control.get('queue_count')!=9:raise RuntimeError('Exception is only the original nine-address queue')
    for key in ('queue_sha256','sql_sha256'):
        if not re.fullmatch('[0-9a-f]{64}',str(control.get(key,''))):raise RuntimeError('Exact queue and SQL identities required')
    ids=control.get('prior_failed_job_ids');executions=control.get('original_failed_execution_ids')
    if not isinstance(ids,list) or len(ids)!=2 or len(set(ids))!=2 or not all(isinstance(x,str) and x for x in ids):raise RuntimeError('Exactly two prior failed logical jobs required')
    if not isinstance(executions,list) or len(executions)!=2 or len(set(executions))!=2 or not all(re.fullmatch('[A-Z0-9]{26}',str(x)) for x in executions):raise RuntimeError('Exactly two prior failed executions required')
    account=control.get('account_context_ref')
    if not isinstance(account,str) or not account:raise RuntimeError('Account context binding required')
    for key,value in [('exception_execution_cap_credits','5'),('exception_export_cap_credits','1'),('exception_total_cap_credits','6'),('new_stage_budget_unchanged','10'),('parent_risk_cap_unchanged','20')]:
        if str(control.get(key))!=value:raise RuntimeError('Exception cannot change stage budgets or per-job caps')
    return dict(authorization_id=CAP5_AUTH,account_context_ref=account,queue_sha256=control['queue_sha256'],sql_sha256=control['sql_sha256'],queue_count=9,
                prior_failed_job_ids=sorted(ids),original_failed_execution_ids=sorted(executions),execution_cap='5',export_cap='1',logical_cap='6')

def require_cap5_confirmation(control):
    bind=binding(control)
    if control.get('status')!=CONFIRMED or control.get('confirmation_source')!='USER_CONFIRMED' or str(control.get('account_execution_cap_confirmed_credits'))!='5':
        raise RuntimeError('Account cap5 is not yet manually confirmed; all SQL remains paused')
    require_timestamp(control.get('confirmation_received_utc'))
    if control.get('payment_method_added') is not False or control.get('extra_credits_enabled') is not False:raise RuntimeError('No paid overage is authorized')
    return bind

def ordinary_sql_allowed(control):
    if control is None:return
    binding(control)
    if control.get('status')!=RESTORED or control.get('sql_submissions_paused') is not False or str(control.get('account_execution_cap_confirmed_credits'))!='1' or control.get('restore_confirmation_source')!='USER_CONFIRMED':
        raise RuntimeError('Ordinary SQL paused until explicit user-confirmed restoration of cap1')
    require_timestamp(control.get('restore_received_utc'))

def verify_submitted_binding(control,account,sql,queue_path):
    bind=require_cap5_confirmation(control)
    if account!=bind['account_context_ref'] or hashlib.sha256(sql.encode()).hexdigest()!=bind['sql_sha256'] or hashlib.sha256(Path(queue_path).read_bytes()).hexdigest()!=bind['queue_sha256']:
        raise RuntimeError('Exception account, original queue or exact retry SQL differs from authorization')
    return bind
