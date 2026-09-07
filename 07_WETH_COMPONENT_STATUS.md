# WETH 独立有限组件：11/12，仍未认证

本轮继续固定区块 20582941 的同一笔 150 ETH canonical WETH9 Deposit。
原历史 `trace:1` 响应序号保持不变；实际调用树路径单独记录为 `[0,0,0,0]`。
未新增另一笔样例，未启用真实转换，未改变 Atomic/Harmony 同资产金额模型。

本轮实际完成 5 个已有授权 RPC 操作：历史 code、区块、交易、receipt 成功；
`debug_traceTransaction` 明确返回 Free 层不提供该方法。该请求未重试，未升级。
另 1 次 Sourcify 官方 v2 公共接口请求取得固定合约的已验证源码与编译匹配材料。

历史 RPC 的 3,124 字节 runtime 与 Sourcify 返回的链上 runtime 逐字节一致。
已验证编译输出的 3,081 字节可执行前缀完全一致，唯一差异为末尾 43 字节的
Solidity 0.4 `bzzr0` CBOR 元数据。适配器只允许这一明确的尾部替换，拒绝
操作码、库或 immutable 替换，并核对变换后完整 runtime 与实际历史 bytes。
这项验证依赖已保存的验证服务编译材料；没有声称本机重新运行了 Solidity 编译器。

真实源码的 deposit/fallback 语义已核查：按 `msg.value` 记入调用者余额并发出
Deposit，没有存入费用或退款。源码及请求保持 Sourcify 身份，未包装成虚构的
Etherscan 响应。原 Etherscan 路径和原 12 项组件谓词继续通过回归。

R2 已保存的 5 行 Dune 调用树经完整分页、交易/区块、路径、祖先成功和子节点
数量检查后复用。适配结果明确标为索引调用树，保留未知 output，不填入虚构
frame logs，也不宣称其为成功的 debug RPC 响应。原 internal 摘录与原始响应的
字节绑定保持；旧请求 envelope 未保存的部分没有事后伪造。

本轮实际复跑原 12 项谓词：**11 项通过，Deposit 与具体 call frame 的日志绑定
仍缺失**。真实认证仍为 `false`，组件状态为
`OBSERVED_LOCAL_DEPOSIT_PATTERN_WITH_GAPS`。不可将 11/12 或代码匹配升级为
已认证的 1:1 转换、退款或周围桥交易证明。

新增 19 项跨来源/字节码/身份/调用树正反例与原 WETH 回归合计 **45/45 通过**。
包括 metadata 正控、执行字节变化拒绝、源码/编译输入冲突、正常函数正文被
字符串冒充的反例、历史 runtime 冲突、代理/链身份错误以及缺日志不得认证。

MIN 的有限闭包包含 16 个输入文件，共 209,170 bytes，附逐文件 SHA-256。
可在无凭据、禁联网环境执行：

```
python -B src/weth_source_adapter_r3.py --input-root derived/weth_context_r3/inputs --output fresh_weth_replay
```

公开包提供相关源码、合成测试和本脱敏报告；真实响应及完整第三方源码只在
私有审查闭包内。外部验收保持 `PENDING_REVIEW`。
