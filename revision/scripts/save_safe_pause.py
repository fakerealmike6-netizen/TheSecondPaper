"""Record a user-requested safe pause from local, read-only runtime state."""
from pathlib import Path
from collections import Counter
import json,sys,sqlite3,shutil
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src')]
from context_access_r3 import read,sha,now
from page_attempts import atomic_json

stamp=now();out=R/'reports'/('safe_pause_'+stamp.replace(':','').replace('+','_'))
out.mkdir(parents=True,exist_ok=False)
locks=[p.relative_to(R).as_posix() for p in (C/'private/network_worker.lock',R/'operations/CURRENT_COST_EVIDENCE_DRIVER.lock') if p.exists()]
sessions=[]
for rel in ('private/stage1d_sessions','private/context_sessions_r4'):
 for p in (C/rel).glob('*.json'):
  doc=read(p)
  if doc.get('closed') is not True:sessions.append({'path':p.relative_to(C).as_posix(),'sha256':sha(p),'closed':doc.get('closed')})
with sqlite3.connect((C/'private/read_retry_r4.sqlite').as_uri()+'?mode=ro',uri=True) as db:
 db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
 inflight_requests=db.execute("SELECT count(*) FROM read_requests WHERE state IN ('IN_FLIGHT','INFLIGHT')").fetchone()[0]
 inflight_attempts=db.execute("SELECT count(*) FROM read_attempts WHERE outcome IN ('IN_FLIGHT','INFLIGHT')").fetchone()[0]
 db.rollback()
if locks or sessions or inflight_requests or inflight_attempts:
 atomic_json(out/'PAUSE_NEEDS_ATTENTION.json',dict(locks=locks,unclosed_sessions=sessions,inflight_requests=inflight_requests,inflight_attempts=inflight_attempts))
 raise RuntimeError('Local work is not quiescent; do not cancel/reset it')

refs=[]
def ref(p):return {'path':p.relative_to(R).as_posix(),'sha256':sha(p),'bytes':p.stat().st_size}
sources=list((C/'src').glob('*.py'))+list((C/'tests').glob('*.py'))
sources += [C/'STAGE1D_PREFLIGHT_GATE.json',C/'private/stage1d_roles/UNKNOWN_COST_POLICY.json',
 C/'private/stage1d_roles/UNKNOWN_COST_CURRENT.json',C/'private/stage1d_roles/CURRENT.json',
 C/'private/stage1d_roles/AUTHORITIES.json',C/'private/stage1d_roles/USER_TASK_BOUNDARIES.json',
 C/'private/stage1d_semantics/CURRENT.json',C/'private/stage1d_semantics/SEMANTIC_CAPABILITY_GATE.json']
for p in sources:
 row=ref(p);target=out/'source_snapshot'/p.relative_to(C);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,target)
 if sha(target)!=row['sha256']:raise ValueError('Pause source copy differs')
 row['saved_path']=target.relative_to(R).as_posix();refs.append(row)
queries=[]
for name in ('txphish_src002','txphish_src001','xscam_src001','lifi_src001'):
 p=C/'derived/stage1d/queries'/name/'collection.json';before=sha(p);doc=read(p)
 queries.append({'name':name,'collection':ref(p),'status':doc['status'],'events':len(doc['candidate_events']),
  'states':len(doc['states']),'frontier':len(doc['unresolved_frontier']),
  'cost_decisions':dict(Counter(x['action'] for x in doc.get('cost_boundary_decisions',[])))})
 if sha(p)!=before:raise ValueError('Query changed while pausing')
jobs=[]
for p in (C/'private/stage1d_bigquery_jobs').glob('*/state.json'):
 doc=read(p);jobs.append({'path':p.relative_to(C).as_posix(),'sha256':sha(p),'state':doc.get('state'),'job_id':doc.get('job_id')})
resource=R/'operations/local_resource_snapshot_2026-09-09T143109.835842_0000.json'
pending100=R/'reports/unknown_cost_boundary_v1/preparations/historical_code_current_pending_v2.json'
last5=R/'operations/unknown_cost_code_e8faf9ccbaaf4f129f6cb0fec4747e8c.json'
record={'status':'PAUSED_BY_USER','resume_condition':'EXPLICIT_USER_RESUME_REQUIRED','paused_at_utc':stamp,
 'run_id':'20260908T165701+0800_stage1d','active_revision':str(R),'active_code':str(C),
 'legacy_active_run_pointer_preserved':True,'controlled_writer_lock_count':0,'unclosed_measured_sessions':0,
 'local_read_requests_inflight':0,'local_read_attempts_inflight':0,'system_wide_process_inventory':'UNAVAILABLE_WINDOWS_CIM_ACCESS_DENIED',
 'owned_tool_sessions_all_completed':True,'subagents_safe_stop_confirmed':['policy_audit','performance_audit','legacy_inventory'],
 'last_completed_evidence_batch':ref(last5),'last_batch_actual_operations':5,'last_batch_registry_admission':'PENDING_NOT_IN_CURRENT_CATALOG',
 'prepared_100_member_batch':ref(pending100),'prepared_100_member_batch_submitted':False,
 'queries':queries,'resource_snapshot':ref(resource),'source_snapshot':refs,'local_bigquery_job_markers':jobs,
 'live_remote_state_checked':False,'new_external_requests_for_pause':0,'inflight_cancelled':0,'counters_reset':False,
 'source_gate':ref(C/'STAGE1D_PREFLIGHT_GATE.json'),'cost_adoption_report':ref(R/'reports/unknown_cost_boundary_v1/UNKNOWN_COST_BOUNDARY_ADOPTION.json'),
 'stage_checkpoint_reached':False,'external_review':'PENDING_REVIEW',
 'uncompleted':['Admit five new successful historical-code results into a versioned cost catalog; increment script NOT_IMPLEMENTED_PAUSED.',
 '100-member current-pending preparation is complete but has never been dispatched.',
 'Resolve remaining identity/type evidence within the existing pool after explicit resume; no failed-key or finite-lookup allowance reset.',
 'Paid-BQ context adapter remains staging only; final NULL-endpoint hunk has 16 synthetic PASS but independent review remains pending. No real paid-page admission performed.',
 'Complete necessary account ledgers/Gas/balances, final context, seven-method comparison, final regression and three delivery packages/publication.']}
atomic_json(out/'PAUSE_STATE.json',record)
lines=['# Stage1D 安全暂停','', '状态：PAUSED_BY_USER；等待用户明确恢复。未达到 CHECKPOINT_1D_REACHED。','',
 '受控请求与本地回放已结束；无 writer lock、未关闭计量 session 或本地 IN_FLIGHT 记录。Windows 全系统进程枚举无权限，未声称完成系统级审计；未远程轮询。','',
 '| Query | 事件 | 状态 | 前沿 |','|---|---:|---:|---:|']
for q in queries:lines.append(f"| {q['name']} | {q['events']} | {q['states']} | {q['frontier']} |")
lines += ['', '971 的 18 个 hold 仍保留。成本规则、四图、真实队列和必要 context 计划已同步；正式两类成本停止均为 0，身份/类型 PENDING 共 1349 个到达状态、381 个地址。', '',
 '最后新增 5 次历史代码读取已完成并保留原件，尚未编入当前证据目录。下一批 100 个请求只完成准备，未发送。', '',
 'TxPhish SRC002 仅发现已闭合；正常账本、Gas、余额、最终 context、金额与七方法及交付仍未完成。', '',
 '当前源码、测试及关键活动配置已备份；大图、原始数据、缓存和持久账本原地保留，并以 SHA 绑定。费用、计时、尝试次数未清零。暂停操作新增外部请求 0。', '',
 '待恢复事项详见 PAUSE_STATE.json；恢复必须延续当前 revision/code，不能从旧 main 覆盖。']
(out/'PAUSE_STATUS.md').write_text('\n'.join(lines)+'\n',encoding='utf8')
atomic_json(R/'PAUSED_STATE.json',{'status':'PAUSED_BY_USER','resume_condition':'EXPLICIT_USER_RESUME_REQUIRED','state':ref(out/'PAUSE_STATE.json'),'report':ref(out/'PAUSE_STATUS.md')})
print(json.dumps({'status':'PAUSED_BY_USER','report':str(out/'PAUSE_STATUS.md'),'state_sha256':sha(out/'PAUSE_STATE.json'),'source_files_saved':len(refs),'new_external_requests':0}))
