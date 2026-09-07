# R4 旧写入入口隔离

调用方闭合检查发现：旧 Network 和旧 live 构造仍可能在 R4 工作区
创建独立的旧账本，或进入不适用本轮授权的网络流程。现由
`src/legacy_guard_r4.py` 在任何 SQLite 创建、会话写入或请求前拒绝。
状态为 `FIXED_AND_TESTED`，外部验收仍为 `PENDING_REVIEW`。

守卫按明确传入的工作区及其 `configs/STAGE1B_R4_POLICY.json` 或
`configs/STAGE1B_R4_PUBLIC_POLICY.json` 检查；公开 R4 树同样受约束，
`STAGE1B_R3_PUBLIC_POLICY.json` 不触发 R4 守卫。
不会因为源码位于 R4 目录，就拒绝显式无 R4 标记的历史/合成工作区。
基础 Ledger 仅限制该工作区 `private` 下四种旧 `shared_budget` 文件名，
实际 `shared_budget_r4.sqlite` 继续正常使用。通用读取、数学和继承
helper 没有关闭。

覆盖 Network 构造、call、preflight；原 Live、R1/R2 RevisionLive；
R1 RPC 构造与请求；R3 Dune/RPC 构造、迁移、会话关卡、usage 和
execute_dune；R2 migrate_revision；原 pilot_actions_r2.execute。
R4 policy 存在时，旧 context_access_r3 CLI 仍路由到新统一接口。

无工作区参数的旧 PublicNode `http_transport` 新增可选 `work`：
显式传入时仅依据该工作区；省略时检查模块工作区及当前工作目录，
防止 R4 默认入口绕过构造器。历史 RpcContextClient 默认运输函数
已传入自身工作区。原唯一直接合成运输测试只补 `work=self.work`，
响应、读取上限、超时、URL 和所有断言保持原样；基线原件不改。

`tests/test_legacy_guard_r4.py` 的 11 项实际测试验证所有旧 API/CLI
均在写入或运输前拒绝、未出现新旧账本文件、没有运输调用；也验证
显式历史工作区、新 R4 Dune 导出与费用 helper、新 R4 RPC 正常成功。
另有原 12 项 R1 RPC 测试通过。总计 23 项在 Windows Python 实测，
使用合成运输和临时目录，网络请求为 0。Linux 未在本子任务实测。
日志：`logs/LEGACY_GUARD_R4_TESTS.txt`。
