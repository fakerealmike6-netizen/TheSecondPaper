"""Bounded report construction only. No project imports, tests, DB or network.

This creates the initial observation. After root edits the JSON, use render_matrix.py
only: rerunning this initial builder would replace editorial updates.
"""
from pathlib import Path
import ast, csv, hashlib, json, re, time
from datetime import datetime, timezone

OUT = Path(__file__).resolve().parent
R = OUT.parents[1]
C = R / 'code'
ROOT = R.parents[3]
P = ROOT / '03_workspaces/stage1D/runs/20260908T165701+0800_stage1d/revisions/closure_20260909T142445+0800/bootstrap/package'
START = datetime.now(timezone.utc).isoformat()
CPU = time.process_time()
refs = {}
source_bytes = {}

def rel(path):
    path = Path(path).resolve()
    if not path.is_relative_to(ROOT):
        raise ValueError('Report input escapes project')
    return path.relative_to(ROOT).as_posix()

def ref(path, kind, *, expected=None, symbols=()):
    path = Path(path).resolve()
    name = rel(path)
    if name in refs:
        value = refs[name]
        if symbols:
            value['symbols'] = sorted(set(value.get('symbols', []) + list(symbols)))
        return name
    before = path.stat()
    data = path.read_bytes()
    after = path.stat()
    digest = hashlib.sha256(data).hexdigest()
    if expected and digest != expected:
        raise ValueError('Explicit evidence hash differs: ' + name)
    value = {'path': name, 'sha256': digest, 'bytes': len(data), 'kind': kind,
             'observation_started_utc': START,
             'stable_during_single_read': (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns),
             'whole_run_frozen': False}
    if kind == 'CURRENT_SOURCE':
        source_bytes[name] = data
        value['tree_role'] = ('PRODUCTION_CODE_OBSERVATION' if path.is_relative_to(C)
                              else 'ROOT_DRIVER_OBSERVATION' if path.is_relative_to(R/'scripts')
                              else 'STAGED_SOURCE_OR_HELPER_OBSERVATION')
        tree = ast.parse(data.decode('utf-8-sig'))
        value['symbols'] = list(symbols)
        value['definitions'] = [{'symbol': n.name, 'line': n.lineno, 'end_line': n.end_lineno}
                                for n in ast.walk(tree)
                                if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in symbols]
    refs[name] = value
    return name

def ev(name, kind='CURRENT_SAVED_RUN_RECEIPT'):
    return ref(R / name, kind)

def src(name, *symbols):
    return ref(C / 'src' / name, 'CURRENT_SOURCE', symbols=symbols)

def readj(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))

contract_refs = [ref(P / 'STAGE1D_EXECUTION_PROMPT.md', 'VERIFIED_PACKAGE_CONTRACT'),
                 ref(P / 'notes/ACCEPTANCE_AND_OUTPUTS.md', 'VERIFIED_PACKAGE_CONTRACT'),
                 ref(P / 'inputs/Stage1D_Paused_Status_For_ChatGPT.md', 'VERIFIED_PACKAGE_CONTRACT'),
                 ref(P / 'inputs/OPTIMIZATION_EVIDENCE_AT_PAUSE.csv', 'VERIFIED_PACKAGE_INHERITED_MATRIX'),
                 ev('authorization/PACKAGE_VALIDATION.json', 'PACKAGE_VALIDATION_RECEIPT')]
prior = list(csv.DictReader((P / 'inputs/OPTIMIZATION_EVIDENCE_AT_PAUSE.csv').open(encoding='utf-8-sig')))
text = (P / 'inputs/Stage1D_Paused_Status_For_ChatGPT.md').read_text(encoding='utf-8-sig')
section = text.split('## 7. 16项工程优化是否实际生效',1)[1].split('### 当前最重',1)[0]
names = {int(a): b.strip() for a,b in re.findall(r'^\| (\d+) \| ([^|]+) \|', section, re.M) if 1 <= int(a) <= 16}
if len(names) != 16 or len(prior) != 16:
    raise ValueError('The exact sixteen-item contract is missing')

E = {}
for key, name in {
 'stop': 'STOP_BOUNDARY_PROPORTIONALITY_ADOPTION.json',
 'stop_impl': 'STOP_BOUNDARY_PROPORTIONALITY_IMPLEMENTATION.json',
 'scope': 'WINDOW_SCOPE_ADOPTION.json',
 'transfer': 'operations/transfers_txphish_src002_53467b261e834ece9c0bf6deaca069fb.json',
 'label69': 'operations/labels_txphish_src001_2026-09-09T081023.029144_0000.json',
 'label0': 'operations/labels_txphish_src002_2026-09-09T081134.482209_0000.json',
 'label1': 'operations/labels_txphish_src002_2026-09-09T091000.422119_0000.json',
 'bq_auth': 'operations/bq_txphish_src001_2026-09-09T081946.394392_0000.json',
 'bq_ae': 'operations/bq_txphish_src001_2026-09-09T083313.343179_0000.json',
 'bq_bind': 'operations/bq_offline_binding_2026-09-09T085918.832717_0000.json',
 'bq_inflight': 'operations/bq_txphish_src001_2026-09-09T091202.881532_0000.json',
 'ae_failure': 'staging/actual_weth_ae_bundle/current_gate_failure_v2/RESULT.json',
 'ae_stage': 'staging/actual_weth_ae_bundle/staged_certified_ae/RESULT.json',
 'ae_receiver': 'staging/actual_weth_ae_bundle/staged_certified_ae/RECEIVER_VALIDATION.json',
 'ae_manifest': 'staging/actual_weth_ae_bundle/MANIFEST.json',
 'ae_handoff': 'staging/actual_weth_ae_bundle/HANDOFF.md',
 'joint_test': 'code/checks/joint_zero/FINAL_TEST_RECEIPT.json',
 'joint_bench': 'code/checks/joint_zero/SYNTHETIC_ON_OFF_BENCHMARK.json',
 'shared_test': 'staging/semantic_shared_evidence/checks/TEST_RECEIPT.json',
 'timestamp_test': 'code/checks/timestamp_mapping/TEST_RECEIPT.json',
 'driver_test': 'staging/current_transfers_driver/checks/TEST_RECEIPT.json',
 'deposit_test': 'staging/semantic_emitter_fix/TEST_EVIDENCE.json',
 'portable_test': 'staging/portable_dependency_fields/checks/TEST_RECEIPT.json',
 'role_test': 'code/checks/role_adoption/TEST_EVIDENCE.json',
 'stop_test': 'staging/stop_boundary_proportionality/TEST_EVIDENCE.json',
}.items():
    kind = 'CONTROLLED_TEST_ALREADY_RUN' if key.endswith('_test') or key == 'joint_bench' else 'CURRENT_SAVED_RUN_RECEIPT'
    if key.startswith('ae_'): kind = 'ACTUAL_SAVED_MATERIAL_OFFLINE_STAGING_RECEIPT'
    if key == 'bq_inflight': kind = 'MUTABLE_INFLIGHT_LOCAL_OBSERVATION_NOT_FINAL'
    E[key] = ev(name, kind)

tx = readj(R / 'operations/transfers_txphish_src002_53467b261e834ece9c0bf6deaca069fb.json')
state_ref = tx['result']['results'][0]['acquisition_state']
E['transfer_state'] = ref(C/state_ref['path'], 'CURRENT_SAVED_RUN_STATE', expected=state_ref['sha256'])
state = readj(C/state_ref['path'])
admission = state['point_cache_admission']['audit_ref']
E['point_admission'] = ref(C/admission['path'], 'CURRENT_SAVED_RUN_RECEIPT', expected=admission['sha256'])
admitted = readj(C/admission['path'])
E['bq_job'] = ev('code/private/stage1d_bigquery_jobs/9077d5eb838b8dbce71c087bfffb075f2021e6c0e0531a4fe243445931bfa2fe/job.json')
job = readj(ROOT/E['bq_job'])
for q in ('txphish_src001','txphish_src002','xscam_src001','lifi_src001'):
    E['replay_'+q] = ev('code/derived/stage1d/queries/'+q+'/ROLE_ADOPTION_AND_REPLAY.json', 'SAVED_REPLAY_RECEIPT_NOT_NEW_GRAPH_AUDIT')
lifi_pointer = readj(C/'derived/stage1d/queries/lifi_src001/CURRENT_CLOSURE_CONTEXT.json')
E['lifi'] = ref(C/lifi_pointer['path'], 'CURRENT_ACTUAL_ASSEMBLY_RECEIPT', expected=lifi_pointer['sha256'])
E['lifi_pointer'] = ev('code/derived/stage1d/queries/lifi_src001/CURRENT_CLOSURE_CONTEXT.json')

source_map = {
 'collector': ('collector.py', 'State','key','run','push'),
 'facts': ('physical_facts.py','PhysicalFactRegistry','add','_view','snapshot','physical_key'),
 'cache': ('stage1d_acquisition.py','CachedIntervals','fetch_interval','Labels','resolve_state','acquire_labels','replay'),
 'context': ('stage1d_context.py','_targets','required_context_windows','exclude_verified_protocol_windows','necessary_context_windows','_native_rows','build_document'),
 'closure_context': ('stage1d_closure_context.py','validate_current','requirements','assemble','assemble_current'),
 'multiasset': ('stage1d_multiasset_context.py','validate_extension','required_windows','build_document'),
 'semantic': ('stage1d_semantic_units.py','certify_instance','validate_semantic_unit','FiniteSemanticResolver','resolve'),
 'catalogue': ('stage1d_semantic_catalogue.py','load_current_resolver'),
 'shared': ('stage1d_shared_evidence.py','BlobPool','SharedEvidenceContexts','SharedContextBuilder','serialize_contexts'),
 'portable': ('stage1d_bq_portable.py','ValidationSession','verify_export_documents','verify_transaction_family_documents','export_family_documents'),
 'owners': ('stage1d_owner_roles.py','owner_sql','reclassify','enrich'),
 'opportunities': ('stage1d_label_opportunities.py','label_budget','snapshot'),
 'boundaries': ('stage1d_task_boundaries.py','TaskBoundaries','resolve'),
 'roles': ('stage1d_role_adoption.py','TechnicalRoles','resolve'),
 'transfers': ('stage1d_transfers_acquisition.py','_merge_overlapping_needs','select_needs','prepare','restore_state_chain','_ensure_timestamp_mapping','_acquire_bindings','acquire'),
 'alchemy': ('stage1d_alchemy_transfers.py','request_plan','TransferPageChain','route_gate'),
 'bracket': ('stage1d_timestamp_bracket.py','resolve_timestamp_bracket','verify_timestamp_bracket'),
 'legacy': ('stage1d_legacy_rpc_import.py','LegacyPointImporter','_current_success','prepare_one','apply_many'),
 'batch': ('stage1d_batch_binding_route.py','prepare','build_binding_sql','validate_transaction_rows','execute_prepared','import_completed','collect_portable_dependencies','superset_admissibility'),
 'bq': ('stage1d_bigquery_jobs.py','call','undispatched_submission','execute_plan'),
 'window': ('stage1d_window.py','missing_rectangles','IntervalContentCache'),
 'experiments': ('stage1d_experiments.py','register_document','freeze_batch','run_batch'),
 'zero': ('stage1c_zero_derivation.py','model_identity','certify_zero_parent','derive_zero','validate_derived_zero'),
 'intervals': ('stage1c_intervals.py','run_interval','solve_all'),
 'lp': ('lp_model.py','_certify','solve_interval'),
 'receiver': ('stage1c_output_contract.py','accept_method_results','interval_row'),
 'runner': ('run_stage1c.py','dispatch','measure','evaluate_query','run'),
 'retry': ('stage1d_recovery_retry.py','adopt_grants','RecoveryReadRetryStore','claim'),
 'bq_context': ('stage1d_bq_context_prepare.py','build_sql','verified_export'),
}
S = {k:src(*v) for k,v in source_map.items()}
S['driver'] = ref(R/'scripts/execute_current_transfers.py','CURRENT_SOURCE', symbols=('CurrentIndexPointCacheAdmission','__call__','main'))
S['batch_export'] = ref(R/'staging/actual_weth_ae_bundle/build_ae_bundle.py','CURRENT_SOURCE',symbols=('build','main'))
S['replay_driver'] = ref(R/'scripts/replay_current_scope.py','CURRENT_SOURCE')
S['labels_driver'] = ref(R/'scripts/acquire_current_labels.py','CURRENT_SOURCE')
S['deposit_staged'] = ref(R/'staging/semantic_emitter_fix/src/stage1d_semantic_units.py','CURRENT_SOURCE',symbols=('_deposit_topic_emitter_proof','_frame_and_log','certify_instance'))
S['portable_staged'] = ref(R/'staging/portable_dependency_fields/src/stage1d_batch_binding_route.py','CURRENT_SOURCE',symbols=('collect_portable_dependencies',))

def row(i, status, scope, entry, sources, evidence, benefit, equivalent, opens, subitems, promote):
    old = prior[i-1]
    return {'item': i, 'optimization': names[i], 'status': status, 'applicable_scope': scope,
            'actual_or_wired_entrypoint': entry,
            'source_refs': [S[k] for k in sources], 'run_refs': [E[k] for k in evidence],
            'observed_benefit': benefit, 'equivalence_evidence': equivalent,
            'open_items': opens, 'subitem_statuses': subitems,
            'inherited_evidence': {'container_ref': contract_refs[3], 'row': i,
               'historical_source_refs': json.loads(old['production_source']),
               'historical_test_refs': json.loads(old['inherited_test_receipts']),
               'historical_run_refs': json.loads(old['saved_actual_evidence']),
               'verification': 'Bindings copied from the verified pause-input matrix; original historical files not re-read or rerun in this task.'},
            'promotion_requirements': promote,
            'avoided_http_requests': None, 'saved_credits': None, 'saved_alchemy_cu': None,
            'final_query_execution_complete': False}

rows = [
row(1,'已实际生效','四query候选；Tx1/Tx2已采用边界；LI仅原深度0',
 'replay_current_scope → Collector.run：SERVICE分支在provider.fetch_interval之前continue；context读取同角色绑定。',
 ['collector','context','closure_context','replay_driver'],['stop','replay_txphish_src001','replay_txphish_src002','lifi'],
 '已保存候选角色回放；stop采用回执中已停止地址的pending与全平台ledger计划均为0。LI实际组装原零跳context，26行，0缺点/0gap，methods_executed=0。',
 '继承候选停止测试；当前角色回执保存state identities及label/collection SHA。不是当前四query最终模型等价实测。',
 ['最终context/sidecar须绑定最终同一角色及输入；服务停止次数不能换算避免请求数。','LI UNKNOWN未变成首服务命中。'],
 {'候选fetch前服务停止':'已实际生效','最终四query context一致性':'已接线未实测','LI首服务识别':'不适用'},
 '最终冻结四query角色/collection/context的同源回执，随后实际方法与接收器回执。'),
row(2,'已实际生效','Tx候选与context需求；已证/用户明确的不支持平台范围边界',
 'Labels.resolve_state → TaskBoundaries/TechnicalRoles；Collector协议边界；necessary_context_windows排除后续账户历史。',
 ['collector','boundaries','roles','context','closure_context'],['stop','stop_impl','stop_test','replay_txphish_src001','replay_txphish_src002'],
 'Dln/Magpie既有用户边界已真实采用；保存进入与停止身份，后续pending/全平台ledger计划0。一般direct-source STOP补丁已装与受影响测试通过。',
 'STOP与受支持实例续接、金额证书、精确0上界分别决策；本项不把名称/行为DEX Trader标签作为协议认证。',
 ['最终context中进入、必要祖先、normal/Gas与其它合法路径完整性仍须最终原件验收。','新技术角色测试不等于当前查询已采用技术证书。'],
 {'既有明确STOP边界':'已实际生效','一般direct-source STOP能力':'已接线未实测','最终金额context闭合':'已接线未实测'},
 '增加实际新角色采用回执或最终context验收；不能只凭源码安装升级子项。'),
row(3,'已实际生效','两Tx当前源到达标签机会；链专属角色/owner继承能力',
 'acquire_current_labels → acquire_labels → label opportunity预算；Labels.resolve_state → 当前角色回放。',
 ['owners','cache','opportunities','labels_driver','replay_driver'],['label69','label0','label1','replay_txphish_src001','replay_txphish_src002','role_test'],
 'Tx1真实COMPLETED_EXPORTED并labels_updated，69个新标签机会；随后Tx2原批0新机会、新批1个机会。69+1是地址机会，不是角色确认数/SQL调用数。',
 'owner补充/失败与成功空结果区分为继承；当前源到达预算路径真实运行。Tx2新标签回执晚于所观察ROLE_ADOPTION_AND_REPLAY，最终需回放。',
 ['最后一次标签结果须回放进候选/最终快照；不把成功空标签写成已证角色。','新技术角色采用数与标签机会不可相加。'],
 {'Dune机会去重及查询':'已实际生效','已有owner链专属解析':'已实际生效','最新标签后的最终快照':'已接线未实测'},
 '最新label结果之后保存真实replay和最终label/context SHA对应。'),
row(4,'已接线未实测','仅canonical WETH Deposit/Withdrawal有限实例；生产完整续接链',
 'load_current_resolver → FiniteSemanticResolver → Collector semantic unit → multiasset context → 独立receiver。',
 ['semantic','catalogue','multiasset','collector','portable','deposit_staged'],['ae_failure','ae_stage','ae_receiver','deposit_test','shared_test'],
 '真实AE材料在现行emitter上失败被明确记录为能力拒绝非data gap；staged Deposit topic唯一发射证明已对真实0.036ETH实例及序列化receiver PASS。回执live_catalogue_adopted=false、solver_runs=0。',
 '完整source/runtime/selector/原receipt/全树绑定；静态继承、不支持delegate、重复Deposit拒绝，普通Transfer保留，Withdrawal不借用新Deposit判据；既有受控测试38PASS。',
 ['Deposit与portable field patch仍待root安装/完整gate；真实unit须采用到catalogue并经当前query frontier/constraint。','不得扩成泛化DSU、任意WETH往来或周边协议认证。'],
 {'有限语义生产链路':'已接线未实测','新Deposit补丁生产接线':'已写未接线','staged真实原件离线证书':'已实际生效','广泛协议DSU':'不适用'},
 '绑定安装源SHA/gate、真实catalogue、当前组件frontier_used与constraint_used回执；分别更新不一并推定。'),
row(5,'已实际生效','每query/asset/arrival/depth/W到达；不跨到达做支配',
 'Collector.push / State.key → seen；范围差集由missing_rectangles处理。',
 ['collector','window'],['replay_txphish_src001','replay_txphish_src002','scope'],
 '确定性到达精确去重已在保存的新scope回放使用；未重新测减少状态的反事实数量。',
 '继承精确状态与窗口回归；保留多次合法到达/不同深度窗口，区间包含合并不是到达支配。',
 ['安全跨到达/深度/W支配没有实现，本轮不新建。'],
 {'精确到达去重':'已实际生效','安全跨到达支配':'没实现'},
 '如以后授权支配，须独立完备证明与同事实反例测试，不能用现有seen冒充。'),
row(6,'已实际生效','当前候选/共享cache物理事件；最终Gas约束另验',
 'CachedIntervals.fetch_interval → PhysicalFactRegistry.add/snapshot；Collector批量校验后接受候选。',
 ['facts','cache','collector','context','multiasset'],['replay_txphish_src001','replay_txphish_src002','bq_bind','lifi'],
 '多个到达/来源保留在同一物理容量；BQ离线补绑定5事件入库不宣称全context。LI现有账户账本实际组装。',
 '继承Registry冲突隔离与来源保留测试；本轮没有重新制造实际冲突；最终多组件同tx费用仅受控验证。',
 ['四query最终normal/failed/zero/Gas与多组件费用约束尚未实际求解验收。','context到达记录数不能当Gas笔数或物理容量累加。'],
 {'候选物理去重与保留':'已实际生效','最终多资产Gas约束':'已接线未实测'},
 '最终四query context/模型与receiver原件验收后更新费用约束子项。'),
row(7,'已实际生效','原生Tx2本次页链、窗口；旧重叠范围/完整页链复用',
 'select_needs/_merge_overlapping_needs → prepare/acquire；restore_state_chain；timestamp bracket → request_plan；CachedIntervals差集。',
 ['transfers','alchemy','bracket','cache','window','driver'],['transfer','transfer_state','point_admission','timestamp_test','driver_test','scope'],
 '真实Tx2 1页/1事件/1need闭合；2端header成功缓存、0新header selector。该need物理边界与逻辑块边界相同，不能称本次W缩短请求范围。旧同输入14→11并集相等为继承。',
 '128个受控影响测试覆盖含等秒、相邻空窗、legacy闭链/prefix、cache优先；静态W传到真实request的接线已执行，但实际时间二分尚无本批缩界实例。',
 ['其它pending/未闭链不得FULL；本批不证明多页resume或空窗真实采集收益。','合并仅同其他字段的精确矩形无扩张并集，不泛化交叉矩形。'],
 {'合并/差集/完整页chain复用':'已实际生效','本次真实timestamp请求入口':'已实际生效','真实二分缩短块界收益':'已接线未实测'},
 '以具体逻辑W/物理bracket/page身份记录新证据，只有同输入对照才能宣称节省。'),
row(8,'已实际生效','当前需求白名单旧raw索引/点准入；旧成功替代继承',
 'CurrentIndexPointCacheAdmission → restore_state_chain + immutable demand证据 → LegacyPointImporter.apply_many → exact缓存。',
 ['legacy','transfers','driver'],['transfer','transfer_state','point_admission'],
 '当前sealed index复用，无重建。Tx2本批准入callback真实执行，3项均NO_EXACT_LEGACY_REQUEST_MATCH，新增导入0字节；随后3个新点成功。旧314导入/784缓存调用是暂停时继承快照，绝不当本轮新增。',
 'closed timestamp/prefix合法，原件/admission/当前scope强绑定；旧研究代码/图/LP/专家路径不入链。',
 ['本次callback未匹配不等于未实现；不能把3次查找称3次替代。','旧BQ 42条缺SQL/参数/完整范围不满足FULL；只准入有完整绑定的事实。'],
 {'旧point准入与旧实际成功替代':'已实际生效','本批当前精确需求查找':'已实际生效','本批旧payload成功命中':'不适用'},
 '后续真实完成回执逐笔计入当前复用，不把既有314重复增加。'),
row(9,'已实际生效','继承固定输入Registry与744次cache对照；当前入口沿用',
 'PhysicalFactRegistry._view revision缓存；CachedIntervals content_memo+events_by_address。',
 ['facts','cache'],['replay_txphish_src001','replay_txphish_src002','timestamp_test'],
 '继承固定输入Registry：X 203.219→10.297s；Tx1 1.622→0.340s；Tx2 0.0872→0.0492s。独立744次cache对照：构造3.048→0.217s，查询4.570→4.669s（查询未加速）。',
 '以上精确canonical输出相等来自包内旧矩阵绑定的保存基准；Registry固定acquisition，cache固定Registry/Collector。不是1075s profile的对比，也非当前新增大图性能。',
 ['源观察SHA不等于当前全图性能freeze；无当前大图重测或额外收益数字。'],
 {'Registry与cache安装/固定输入证明':'已实际生效','当前增强大图性能基准':'已接线未实测'},
 '若测新图，分别冻结共同模块与真实输入并报告构造/查询/CPU/墙钟；不混历史profile。'),
row(10,'已实际生效','候选批界重放；LI原零跳context；最终1+5未执行',
 'acquire后root在批界replay_current_scope；最终assemble_current/register_document/run_batch分离。',
 ['transfers','cache','replay_driver','closure_context','experiments','runner'],['transfer','replay_txphish_src002','lifi'],
 '本次续采后有持久角色/候选回放；LI实际current context已组装26行、methods_executed=0。未因每个前沿执行1+5。',
 '重放仍为确定性重建，非增量图算法；context未重复运行不是实测加速比例。',
 ['Tx/X候选/标签仍变化，最终context与七方法须最后冻结后运行。','不得把阶段等待计作context性能收益。'],
 {'批次候选更新调度':'已实际生效','LI current context组装':'已实际生效','四query最终1+5':'已接线未实测'},
 '最终batch freeze、四query context注册与1+5真实回执。'),
row(11,'已实际生效','实际AE classic BQ日期/账户/全tx+trace family；普通批量部分阻塞',
 'batch.prepare/build_binding_sql → execute_prepared → bigquery.execute_plan → verified_export → import_completed。',
 ['batch','bq_context','bq','portable'],['bq_job','bq_ae','bq_bind','bq_auth','bq_inflight','portable_test'],
 'AE实际job COMPLETE_EXPORTED，1页24行，complete_page_chain=true；目标AE完整9 trace。后续离线原点补绑定5events、0fee_gaps，不追加BQ请求。',
 '严格原SQL/spec/schema、总行/分页自然闭合、原receipt、完整root/ancestor/sibling核验；此时不是全账户连续context FULL。',
 ['普通BQ第一准备集dryrun RETRIES_EXHAUSTED、需求保留；另一正常组处于非冻结观测未知。','必要portable依赖字段补丁仅staged；AE离线staging已独立原件验证，不替代production安装。'],
 {'日期/账户/全tree批量实际入口':'已实际生效','完整四query账本闭合':'已接线未实测','portable字段依赖补丁生产采用':'已写未接线'},
 '普通批量同原spec terminal+完整pages/readback、最终账户范围coverage后更新，不根据在途状态预测。'),
row(12,'已实际生效','Alchemy发现/点状态；classic BQ批量；Dune标签；Etherscan备用',
 'TransfersRuntime/RpcAccess cache优先；missing selectors>200返回BATCH_BINDING_REQUIRED；BQ execute_prepared；Dune acquire_labels。',
 ['alchemy','transfers','batch','bq','cache'],['transfer','label69','label1','bq_job','bq_bind','bq_auth','timestamp_test'],
 '三主平台已分别有真实运行；本次Alchemy仅3缺点，未触发>200门槛。AE BQ不是假装Dune；normal/internal/Gas不同数据能力保持区分。',
 '>200缓存扣减后的真实接口有受控测试，需求完整保留、不当硬预算停止；已有实际BQ调用不自动证明此次由门槛触发。',
 ['>200真实触发→最终缓存闭合未有本次回执；普通批量仍OPEN。','Etherscan非阻塞备用无本轮成功承担请求证据。'],
 {'三主平台分工':'已实际生效','>200门槛真实自动路由':'已接线未实测','Etherscan备用成功路径':'已接线未实测'},
 '真实门槛状态/missing集合/实际BQ回读绑定后更新子项；平台能力不得互相冒充。'),
row(13,'已实际生效','持久计数/成功cache/same-key恢复；旧grant与新失败分别计',
 'RecoveryReadRetryStore；_acquire_bindings持久member；bigquery call/execute_plan提交意图与原job恢复。',
 ['retry','transfers','bq','legacy'],['transfer','transfer_state','point_admission','bq_auth','bq_inflight','bq_job','timestamp_test'],
 '本次page/点成功与2header cache命中保存；普通BQ旧组失败出口RETRIES_EXHAUSTED、needs_preserved=true，未清计数。',
 '未知submit只GET原job及明确未dispatch恢复路径有保存受控测试；不把实际正常job成功称为未知提交恢复实验。',
 ['根观察第一普通BQ auth同键3次耗尽；本任务不读DB重对账，仅用回执确认耗尽出口，3次为root限定观察。','旧76 grant与旧5 TLS新失败属不同集合；无证据可称5个RPC均已恢复。','第二普通BQ组不作成功或失败判定。'],
 {'持久同键计数与缓存':'已实际生效','未知SQL同job恢复真实故障':'已接线未实测','旧5个TLS成功恢复':'已接线未实测'},
 '准确同key attempt/terminal/raw回执才能更新恢复实效；禁止改key/新预算作“恢复”。'),
row(14,'已接线未实测','实际七方法中的四种区间方法；逐asset/variant/query/target-copy；当前四query未求解',
 'run_stage1c.dispatch → run_interval.solve_all joint-first → certify_zero_parent/derive_zero；共同receiver interval_row独立验父证书。',
 ['runner','intervals','lp','zero','receiver','experiments'],['joint_test','joint_bench','shared_test','lifi'],
 '已有135受影响测试PASS。固定合成输入1+5：FULL/NO_PROTOCOL/BALANCE_REMOVED linprog 10→2；NO_CROSS 8→4；域/资产/金额端点相同。当前四query实际skip数量仍null。',
 '每run新model/memo，每copy隔离；精确rational上界0+可行见证+非负同asset真子集+完整identity；负/缺父/错模型/跨asset等拒绝。0下界/float近0/unknown不可跳。',
 ['尚无当前最终query方法回执；合成毫秒收益不能推广到真实图或credits。','Haircut/Poison/其他baseline输出语义不变；不是七种方法都调用LP。'],
 {'七方法入口joint-first与独立接收器':'已接线未实测','合成同输入端点/调用数对照':'已实际生效','当前query实际joint-zero skip':'已接线未实测'},
 '在最终冻结四query输入下1+5运行，保存实际LP计数/父子证明、独立receiver、不可计时原因。'),
row(15,'没实现','采集阶段可靠余额归零/保守局部容量提前停止；本轮不开发新策略',
 'Collector现有停止分支为服务/协议/深度/资源/证据异常；没有金额上界在线剪枝调用。',
 ['collector','context','intervals','zero'],['scope','joint_bench'],
 '无当前实现、调用或收益；离线joint-zero只是等价求解调度。',
 '0下界不是0上界；window筛选/角色STOP/seen/LP子目标skip均不能替代本项证明。',
 ['未来需完整余额、正常流入流出、Gas、祖先、顺序、来源结转、返回与多到达W的保守零上界证据，并获得明确授权。'],
 {'在线余额归零剪枝':'没实现','保守局部容量剪枝':'没实现'},
 '本轮保留没实现；不因数据缺口或资源阈值自动改成不适用/已完成。'),
row(16,'已实际生效','coverage内容memo；实际AE单批导出共享池；最终最小依赖/无损分卷仍待交付',
 'CachedIntervals.content_memo；build_ae_bundle单批documents → SharedContextBuilder → lazy SharedEvidenceContexts → 独立原bytes验证。',
 ['cache','shared','portable','multiasset','batch_export','portable_staged'],['shared_test','ae_failure','ae_stage','ae_receiver','ae_handoff','ae_manifest','portable_test'],
 '真实共享原件JSON 49,147,931B；build_ae_bundle一次物理batch export；saved certify另0次export，每独立receiver一次job验证/一次tx索引。旧pool bytes未删页/未用持久seal。',
 '合成2物理tx共享同BQ family：120132→75350B，30unique blobs；同path不同bytes冲突/缺blob/改页/跨query/序列化重载拒绝或独立验证有58PASS。合成与真实体积不混算。',
 ['真实AE仍staging，production candidate/context/七方法中pool的最终采用尚待回执。','非全链流式，仍有整文件/列表物化；最终MIN/public/Handoff/sidecar/回执未产生。','无损分卷工具继承已装；包体/耗时收益没有最终实测。'],
 {'coverage内容memo':'已实际生效','真实AE共享原件离线导出/验证':'已实际生效','production共享池最终约束使用':'已接线未实测','全链流式':'没实现','最终最小闭包/分卷实包':'已接线未实测'},
 '保留原件，root安装并记录真实frontier/constraint与最终依赖closure、分卷manifest、禁网新目录复验及发布回执。'),
]

for name,data in source_bytes.items():
    path = ROOT/name
    refs[name]['stable_until_builder_end'] = hashlib.sha256(path.read_bytes()).hexdigest() == hashlib.sha256(data).hexdigest()
    refs[name]['freeze_claim'] = 'OBSERVED_BYTES_ONLY_SOURCE_IS_NOT_FINAL_FROZEN'

counts = {}
for item in rows: counts[item['status']] = counts.get(item['status'],0)+1
observation = {
 'schema_version':'stage1d-sixteen-optimization-closure-observation-v1',
 'status':'UPDATABLE_STAGING_OBSERVATION_NOT_FINAL_CLOSURE',
 'authorization_id':'STAGE1D_WINDOW_ROLE_SEMANTIC_CLOSURE_V1',
 'created_at_utc':START,'revision':rel(R),'source_tree':rel(C),
 'contract_refs':contract_refs,
 'contract_locations':['inputs/Stage1D_Paused_Status_For_ChatGPT.md:207-228','notes/ACCEPTANCE_AND_OUTPUTS.md:73'],
 'allowed_statuses':['没实现','已写未接线','已接线未实测','已实际生效','不适用'],
 'status_semantics':'Main status is scoped to applicable_scope/subitem_statuses. 已实际生效 is evidence of the named local mechanism, never all sixteen closed, all queries FULL, final model use or billed savings. Controlled tests alone do not promote current-query execution.',
 'state_counts_not_completion_score':counts,
 'external_acceptance':'PENDING_REVIEW','final_stop':'CHECKPOINT_1D_REACHED',
 'checkpoint_reached_by_this_report':False,
 'whole_run_frozen':False,
 'current_query_solver_run_claim':False,
 'mutable_observation':{'ordinary_bq_second_group_ref':E['bq_inflight'],'terminal_status':'UNKNOWN_NONFROZEN_OBSERVATION',
   'basis':'Only the existing local operation receipt was observed. No remote polling, live job-state inspection or ledger/account reconciliation.',
   'first_group_auth_three_attempts':'Root observed 3 same-key authentication failures. This bounded task independently binds only the saved RETRIES_EXHAUSTED exit, and does not invent attempt evidence.'},
 'measured_fact_extracts':{
   'tx2_transfer':{'closed_needs':1,'pages_this_invocation':tx['result']['pages_this_invocation'],'events':tx['result']['results'][0]['events'],
      'timestamp_header_selectors_submitted':state['timestamp_header_selectors_submitted'],'timestamp_cached_members':sum(x.get('cache_hit') is True for x in state['timestamp_headers'].values()),
      'new_point_selectors_submitted':tx['result']['binding_calls_this_invocation'],
      'point_admission_statuses':[x['status'] for x in admitted['results']],
      'legacy_imported_storage_added_bytes':admitted['imported_storage_added_bytes'],
      'point_selectors_are_billed_attempts':False,'w_shrink_proved_for_this_need':False},
   'ae_bigquery':{'total_rows':job['total_rows'],'pages':len(job['pages']),'complete_page_chain':job['complete_page_chain'],'state':job['state'],
      'target_ae_trace_rows':9,'whole_query_context_full':False},
   'shared_pool':{'bytes':49147931,'sha256':'b38865784610b206ee45232c8d31474c53d52844c71eae0a614f0527cafb1c25',
      'basis':'Existing actual RESULT/MANIFEST; the 49MB raw evidence file was not re-read in this task.'}},
 'rows':rows,
 'inspection_scope':{'new_tests_run':0,'formal_methods_run':0,'network_calls':0,'database_constructors':0,'production_writes':0,
   'bounded_reads':'Exact contract, sixteen-item inherited matrix, explicit current source files/necessary definitions and saved named receipts only; no raw directory scan or whole repository audit.',
   'source_files_observed':len(source_bytes),'observation_cpu_seconds':time.process_time()-CPU},
 'update_rule':'Root must append installation/run refs and update exact affected source hashes plus appropriate subitems. Retain inherited/controlled provenance. A changed source hash alone never promotes status. Re-render with render_matrix.py after editing this JSON.'
}
(OUT/'09_OPTIMIZATION_CLOSURE_MATRIX.json').write_text(json.dumps(observation,ensure_ascii=False,indent=2)+'\n',encoding='utf8')
(OUT/'SOURCE_REFERENCES.json').write_text(json.dumps({'schema_version':'stage1d-optimization-local-source-references-v1',
 'created_at_utc':START,'files':list(refs.values()),'historical_inner_refs_resolved':False,
 'only_named_current_refs_read':True,'large_raw_documents_read':False},ensure_ascii=False,indent=2)+'\n',encoding='utf8')
print(json.dumps({'rows':len(rows),'source_files':len(source_bytes),'named_evidence_and_source_refs':len(refs),'status_counts':counts,
                  'source_changes_during_builder':[p for p,v in refs.items() if v.get('stable_until_builder_end') is False],
                  'new_tests_run':0,'cpu_seconds':time.process_time()-CPU},ensure_ascii=False))
