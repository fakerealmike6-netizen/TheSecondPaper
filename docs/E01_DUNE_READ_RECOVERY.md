# E01：Dune 固定读请求恢复

实现状态为 `IMPLEMENTED_AND_SYNTHETICALLY_TESTED`，外部验收为
`PENDING_REVIEW`。本模块的实现与验证没有发送真实网络请求。

`src/dune_r4.py` 的 `ContextDuneR4` 继续使用原统一预算组件与动态完整
导出上界；R4 运行使用同一迁移账本和 `ReadRetryStore`。原 R3 入口
`context_access_r3.py` 仅在工作区存在 R4 policy 时，路由到 R4 RPC 和
Dune 实现。原 R3 Python API、旧测试和只读基线均保留。

固定结果页身份包括 provider 别名、链、账户非秘密引用、execution ID、
offset、limit 与列选择；重新启动或改 label 不产生额外次数。每次派发
先取得持久化 claim，默认最多三次总尝试。Timeout、IncompleteRead、
可恢复 429/503 等错误按共享分类和指数全抖动处理；Retry-After 保留。
剩余上下文时钟不足时返回 DEFERRED。已知权限、参数、身份和完整 schema
错误不会进入相同请求的无条件恢复。原成功页与成功空页直接复用。

每个失败页的原 attempt、原响应/回执与费用占用保留。新恢复页写入独立
`r4_reads` 和 `r4_verified`，不覆盖失败页。新的恢复请求在派发前增加
自己的导出上界；持久化原占用基线使旧风险较大时也不能吞掉这项新增
费用。合成独立反控验证“旧风险 10 + 新请求上界 3”保留为 13；后续
更大的可靠服务器结果元数据还会同时上调旧中断请求的保守上界。
这些都是预算上界，不写作最终收费。成功恢复不释放原未知风险。

状态轮询按固定 execution ID 与持久化 observation sequence 记录。
一次有效 pending/executing 响应结束本次成功观察，随后正常观察继续；
同一次观察发生传输失败才使用三次恢复限制。因此五次正常运行状态
后仍可以取得完成状态。终态直接复用，状态读成功不能解决未知 SQL POST。

新 SQL 在 `verify_frozen_scope` 通过后才预留和派发，检查实际账本块域、
已保存块头时间及 SQL 日期域。只允许 Small/Medium 有限上下文工作。
SQL POST 的未知结果保持原执行预留；重新启动、修改标签或成功读取
其他 execution 都不能重新提交。已知终态失败的原 SQL 最多允许一次
明确优化或已证实瞬态原因的重试，记录原作业关联及独立执行费用；
账户单次上限失败不授权扩大限额。该逻辑没有真实失败 SQL 重试发生。

`migrate_legacy_reads()` 只读取当前修订中明确复制的作业目录和原 attempt
身份，验证已有页/回执哈希后导入。实际三个 R3 成功页已离线迁移，
没有修改原 attempt 行或降低金融账本占用。私有审查回执定位：
`private/DUNE_READ_MIGRATION_R4.json`。

在 Windows 的实际 Python 进程运行 `tests/test_dune_r4.py` 中 24 项测试，
以及独立 `tests/test_r4_retry_integration_review.py` 的 6 项集成检查，
共 30 项通过；该组包含 RPC 独立交叉检查。另有既有 Dune/R3 access、
账本/查询及 F01–F03 的 83 项回归通过。测试使用明确合成传输、临时
SQLite、实际页验证和费用方法，未使用真实凭据或请求。Linux 本子任务
未实际运行，未声称通过。记录位于 `logs/E01_DUNE_R4_TESTS.txt` 和
`logs/F01_F03_E01_EXISTING_AND_NEW_REGRESSION.txt`。

CLI 对 DEFERRED、用尽、失败和不完整结果返回非零；单独的历史检查
CLI 也拒绝将失败 decision 当作成功退出。测试覆盖了这些真实入口。
