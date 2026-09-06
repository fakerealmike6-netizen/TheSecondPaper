# R2-01 / R2-03 修复与接口

`page_attempts.py`使用本地SQLite事务和同步写入，记录账户上下文引用、逻辑作业、execution、operation及完整请求参数。账户引用是非秘密标识，不能是API key。每个stream的offset唯一，limit及其余参数冻结。请求前提交PLANNED→DISPATCH_INTENT；回调失败、进程消失或写盘结果不确定，不恢复为未请求。UNKNOWN_TRANSPORT/INVALID_RESPONSE及尚未结束的DISPATCH_INTENT阻止同账户上下文的新请求；没有内置自动重试或隐式恢复授权。

成功响应及回执通过临时文件、flush/fsync及原子替换保存；契约校验后才提交SUCCESS_VALIDATED并推进next_offset。SQLite锁防止多个进程重复派发。请求计数在回调前增加，失败也计入；持久化attempts记录跨重启保留。DISPATCH_INTENT是“可能已提交”的保守计数，不把本地没有page文件当作服务端未处理。

`page_contract.py`同时供通用adapter与Live续页验证使用：要求明确execution identity和完成状态，rows为对象数组，本页row_count等于实际页长，total_row_count与status及已保存页一致。cursor须连续；达到总数仍有续游标是异常。schema/行字段改变、空页循环、重复offset和不完整终页被拒绝。该endpoint配置允许省略列描述和终页next_offset/next_uri；不允许用自填身份弥补缺失execution/state。

next_uri从不用于发送请求；仅验证HTTPS官方api.dune.com、同一execution的results路径和允许且不变的参数。跨主机、用户信息、非标准端口、fragment、重复参数、过滤或limit变化均拒绝。URL检查不能替代正常平台访问权限。

对旧测试的唯一有意fixture补强：`tests/test_dune_live.py`原正常回调没有result.metadata，现补入其已有实际页长和总数。原断言没有删除或弱化；新增缺失metadata反例明确要求拒绝。原输入形状不是本轮严格分页契约下的有效正常响应。

独立外部反例在旧MIN源码上重新运行，原四项缺陷均复现，证据位于本地checks/provider_baseline。相同Dune故障输入及回调在修正版运行，三个异常200在第一异常页后不再调用，正常两页通过，通用超时导出次数由2降为1，当前两次返回请求计数为[2,0]。这些是合成验证，不能宣称真实provider已验证。

公开回归入口：`python -m unittest discover -s tests -p test_page_recovery.py -v`。包含真实子进程销毁/重建（合成回调）、双进程同页竞争、双adapter竞争、请求前/响应落盘失败、参数换名绕过、正常多页/零结果及异常身份/metadata/cursor/URL。

预算另由R1账本统一管理。此模块不结算、释放或增加额度；未知尝试仍须由上层保留风险占用。旧Live中的导出估计只是提交前包络检查，不构成可释放预算的已核实上界。R1 worker应实例化自己的AttemptStore并绑定同一已批准账户上下文和逻辑job。
