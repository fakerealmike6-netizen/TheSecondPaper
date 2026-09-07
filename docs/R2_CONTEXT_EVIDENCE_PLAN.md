# R2 余额、Gas 与 WETH 最小补证

这是离线接续方案，未提交 Dune/RPC 请求，未改变源码、账本、模型或真实认证状态。固定 WETH 样例继续 6/12 通过、`real_component_certified=false`。本方案的两个 SQL 文件是可审阅输入，不能记成成功采集。

## 可直接补充的一项小范围证据

优先使用 `sql/weth_fixed_call_context.sql`：仅取得固定交易、区块 20582941、UTC 日期 2024-08-22 的完整 Dune traces。日期由原实际 internal 响应的 `timeStamp=1724315183` 推导（2024-08-22T08:26:23Z），不使用猜测日期。取全部调用树行，保留零金额、失败、根与子调用；不以 `value=150 ETH`、成功或 WETH 收款人过滤，否则无法检查祖先失败、子树闭合与退款。列名、真实 `trace_address` 和调用输入字段有 [Dune traces 官方文档](https://docs.dune.com/data-catalog/evm/ethereum/raw/traces) 依据。

完整下载后先按 execution/page/row 总数验证覆盖，再核对固定 tx/block/hash/tx_index；检查唯一 trace 路径及每个父节点声明的 `sub_traces` 与实际直接子节点数量。匹配 credited→WETH 的成功 150 ETH `CALL`，还需祖先全部成功、入口数据为 `0x` 或 `0xd0e30db0`。只有完整子树才能报告观察到的正价值子调用/退款情况；未知值不写 0。排序仅用于展示调用树路径。

这些数据可把旧 `trace:1` 的“响应行序号”与真实调用树路径分开，并提供调用/退款上下文。Dune 原始日志没有调用树路径，不能因同 tx、合约、金额、唯一 Deposit 就自行将日志塞进某个调用 frame。当前验证器要求真实 `debug_traceTransaction` 请求绑定；Dune SQL 不能伪装成该 RPC。保留独立 `DUNE_INDEXED_CALL_CONTEXT` 结果，不擅改 6/12 认证接口、不开启真实转换，更不能认证外围 LI.FI/桥操作。

原 transaction、完整 receipt、internal 响应均已保存并可按原 SHA 复用，不必再付费取完整 transaction/receipt/logs。固定 receipt 具有 Deposit `logIndex=210` 与交易 Gas；重复 Dune logs 查询不会补上 frame-log 绑定。[Dune logs 官方字段](https://docs.dune.com/data-catalog/evm/ethereum/raw/logs) 支持日志及交易位置，不提供对应 call frame。

## 部署与历史代码

`sql/weth_deployment_context_optional.sql` 仅按 canonical WETH 地址查询 `ethereum.creation_traces`，优先级低于两个探针及上述单交易上下文。官方列清单给出 `address`、`code`、部署块/交易；文档示例还使用 `contract_address`，存在例子与列清单不一致，因此 SQL 使用列清单的 `address`，实际服务端编译未实测。[Dune creation traces](https://docs.dune.com/data-catalog/evm/ethereum/raw/creation-traces)

当前本地证据没有固定部署时间，故未伪造日期分区。若主执行器有剩余统一风险预算且认为部署字节值得取得，可在正常 20-credit 执行预留后调用；这不是额外授权或固定费用。部署 code 只能记为部署时的字节，不能当成区块 20582941 的 `eth_getCode`，也不构成源码编译对应证明。当前 source adapter 还要求实际 `getsourcecode`、历史 code、部署验证与受信 catalogue；这些缺口不会因查询 creation 表自动消失。

PublicNode 既有 403 能力未变，不重试。父任务报告 Alchemy key 存在但当前权限/CU 余量未确认，且原采集器禁用；本方案不据 key 存在开启请求。公开 Etherscan 合约网页本次仅作无凭据证据可用性检查，返回 403，未取得源码，不把可见 URL 记成已验证源码。

## 余额与 Gas

当前 Dune 官方余额目录明确：所有 EVM `balances_<chain>.latest/updates/daily_updates` 是 Enterprise add-on 的 gated 数据。现有 Plus trial 不构成该访问权限证据；不提交猜测 SQL、不升级、不用全链流入减流出重建替代可靠余额锚点。[Dune balances 官方目录](https://docs.dune.com/data-catalog/curated/balances/overview)

原 Atomic 6 个、Harmony 4 个地址—资产期初余额仍 `null`，真实观测零余额数为 0。新前沿生成后按最终图重新列所需锚点；日终余额即使将来可得，也不是事件紧前余额，必须用可靠锚点之后的完整价值/费用事件及真实位置重放才可转换。服务首次进入后的全平台账本不在本轮采集范围内。

旧 15 个候选 top 交易已有 Dune `gas_used` 与 `gas_price`，应复用；新增候选交易优先使用本轮批次已有字段，按 tx 物理去重，不能把每条 trace 重复计一笔交易 Gas。只有实际缺字段的新 tx 才需要按其确切 block/date/hash 批量补 `ethereum.transactions` 的 `from,index,success,gas_used,gas_price,block_hash`，不重新查询全体 terminal；失败交易可能付 Gas，金额传播成功筛选不能代替费用上下文完整性。[Dune transactions 官方字段](https://docs.dune.com/data-catalog/evm/ethereum/raw/transactions)

原 Atomic 候选 Gas 合计 5439909182778000 wei、Harmony 1843624551405000 wei；固定 WETH 交易 93860235540657 wei（89811×1045086187）。这些是已有交易费用上下文，不是完整地址历史费用或来源份额。LP 当前未完整强制真实 Gas，来源份额未知；继续使用原条件金额等级，不能因补了 Gas 列就改变该声明。

## 执行与最终记录

根执行器单 writer 统一累计：未知执行费先留 20，可靠执行费返回后调整执行部分，再根据实际完整结果规模为全部导出动态留风险；未决请求不重发。上述查询没有 `LIMIT` 截断证据，预计小结果不是硬大小保证；实际结果超过余量时保存 execution/metadata 并记导出未完成。SQL 生成、实际提交、完整导出、上下文验证、真实组件认证五个状态分别记录。

本次没有选择额外简单 WETH 交易，不消耗最多 1 笔的允许选择，也未按服务命中情况挑样例。完整机器状态及本地来源 SHA 见 `R2_CONTEXT_EVIDENCE_STATUS.json`。本方案不阻塞两个同资产探针及条件金额更新，外部验收保持 `PENDING_REVIEW`。
