# F01—F03 修复与同输入影响

状态：三项均 `FIXED_AND_TESTED`；外部验收仍为 `PENDING_REVIEW`。

F01 在真实入口 `context_ledger_r3.normalize_rows` 使用共用的
`exact_fields_r4`：金额缺失、显式 null、无效数值和别名冲突分别报告，
不再补 0。合法十进制/十六进制 0、1 原始单位仍可使用。实际交易/trace
必须给精确非负价值；仅补 Gas/状态的 receipt 独立进入非价值补证路径，
再与对应交易的已知字段核对和合并。未知状态不生成成功转移。

原审查的两笔 100 背景流相消反例没有改金额或顺序。完整资料仍得
`[0,80]`，删除或 null 化两笔金额现在明确拒绝，无法再产生伪造完整的
`[70,80]`。原脚本及 R3 坏行为运行结果保留于
`checks/F01_F02_ORIGINAL_BAD_BEHAVIOR.json`。

F02 在任何失败 frame 过滤之前比较同一交易的顶层与 root：区块、哈希、
交易位置、端点、适用 value 和已知状态。冲突进入 `fact_conflicts`，
相应价值流隔离，LP 拒绝。合法成功交易的失败 nested 子调用仍按回滚处理；
失败交易的实际 Gas 保留。frame success 与整笔 tx_success 分别解释，
没有把正常的 failed nested + successful transaction 当作矛盾。

F03 的 `context_queries_r3.build/freeze` 不再从候选时间推定扩展账本的
UTC 分区日期。新 freeze 绑定完整块域、已保存且经请求/响应/消息体哈希
验证的边界时间证据、日期域与 SQL 逻辑身份。可使用严格包围账本块域的
前/后已知区块时间；缺少边界证据则拒绝冻结，不取消分区过滤。

`verify_frozen_scope(freeze_path, work_root)` 是调用方统一关卡。
`context_ledger_r3.replay_manifest`、`freeze_context_replay_r3.freeze`
均调用它；`dune_r4.ContextDuneR4.submit` 也在预留/派发前实际调用它。
真实 provider coverage 必须同时有 date 和 block 验证结果，
完整导出本身不能替代这两个条件。新 schema 为
`stage1b-r4-context-query-v1`。旧 R3 schema 仅在保存块头证明包围关系、
且 SQL 与完整窗口模板一致时兼容，旧 SQL 不改写。

定向历史检查实际验证了 43 个保存块头、3 个作业的 28 条窗口，以及
69 笔精确正整数价值和 top/root 一致性。所有旧日期域均安全，包含
Harmony 原有的 31 块同日补洞；没有需要重新取得的实际缺失区间。
结果见 `derived/F03_HISTORICAL_CONTEXT_COVERAGE.json`，重放命令为
`python src/audit_context_contracts_r4.py --tree <R3-MIN> --output <new-report>`。

修正版实际从同一保存响应重建两查询：69 个价值事件、55 个锚点、
61 个模型付款者费用全部复用。两个 `model_input.json` 与 R3 原件的
JSON 内容完全相同，账本仍为 27/27 零残差、无事实冲突。输出在
`derived/F01_F03_same_input/`；这没有更改 LP 期望。

新增 23 项应有行为测试包括精确字段/别名、合法 receipt、未知成功状态、
原相消反例、root/nested 正反例、同日/跨日前后/多日/午夜、多账户日期域、
日期篡改后重算哈希仍拒绝、旧 SQL 证据兼容、coverage 双域以及真实 CLI
从保存块头冻结的正反控。原 26 项账本测试和原 3 项查询测试通过。
旧查询正控 fixture 仅补新契约所需的两个合成块头，原行、时间、数值与
断言不变；原件保留，适配 diff 在 `checks/F01_F03_diffs/`。

本次接口调用处检索覆盖当前 `src/` 和 `tests/`。生产网络调用方必须在
提交新上下文 SQL 前执行同一 scope 验证；R4 的 Dune 恢复入口已接入该
关卡。`audit_context_contracts_r4` 在固定历史范围缺失、双域失败、事实
冲突或拒绝结论时实际非零退出；CLI 反控位于 `tests/test_dune_r4.py`。
本文报告的修复和历史检查均未发送网络请求。
