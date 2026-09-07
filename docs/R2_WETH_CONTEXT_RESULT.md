# R2 WETH 新增真实调用上下文

根执行器实际完成 Dune execution `ARCHIVED_FIXED_EXECUTION`：5 个 traces 行、1 个完整结果页。离线分析逐项复核 SQL、冻结输入、submit/status/results 原始响应 SHA-256 与字节数、保存页与原始 JSON 等价、execution/schema/总行数/终页关系；已有 `page_contract` 验证 5/5 行完整。原 transaction、receipt、internal 的摘录与原始归档哈希及内容也重新核对通过。

真实 WETH 调用路径是 **`[0,0,0,0]`**。旧 `trace:1` 是历史 internal 响应的第 1 行序号，不能当作链上 `[1]` 路径。两者的实际 sender、recipient、150 ETH 与 call Gas 6874 一致，新增路径独立记录，原事件 ID 和历史数据保持不变。

五行依次是根 CALL、DELEGATECALL、向 credited 地址的 CALL、DELEGATECALL、credited 向 canonical WETH 的 CALL。每个父节点声明的 `sub_traces` 与完整结果中的直接子节点数一致，路径无重复、祖先齐全且成功。两条 DELEGATECALL 的 150 ETH 是调用上下文值，不能与三条 CALL 一起累加成五次独立价值转移。

目标调用 `from=0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5`，`to=0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2`，输入 `0xd0e30db0`，金额 150 ETH，`sub_traces=0`，全部祖先成功。完整索引调用树中目标调用无子调用，未观察到子树内退款。`output_data=null` 保留未知，不写成已证实空输出。已有 receipt 的 Deposit `logIndex=210`、credited 与金额一致。

真实认证仍 **6/12，`NOT_CERTIFIED`**。Dune 索引上下文没有日志到调用 frame 的绑定；现有接口要求实际 RPC 请求绑定、历史运行时代码和已验证源码对应，目前仍缺。没有构造假 `debug_traceTransaction`、改变认证适配器或启用真实转换。组件手续费与认证退款金额保持 `null`，外围 LI.FI/桥交易未认证。

本作业已知执行费用 0.435263158 credits；导出实际账单未知，保留 1 credit 有据上界，二者由根任务统一累计。这里没有将上界当成已付费用。分析过程新增网络请求 0。

完整逐行结果：`derived/weth_context_r2/TRACE_ROWS.json`；证据绑定、树闭合与缺口：`derived/weth_context_r2/ANALYSIS.json`。复跑脚本为 `src/context_evidence_r2.py weth`，显式传入 revision 根、此 job 目录、MIN 内可复用的 baseline WETH 目录和新输出目录即可；不需要凭据。

## 当前余额、Gas 与位置快照

从 `derived/replay_001` 离线生成的清单如下，后续最终图变化必须重新运行 inventory，不能套用这些计数。

| 探针 | candidate / context | 未知余额对 | 已观察 Gas 的候选 tx | Gas 合计 wei | tx_index 缺失 |
|---|---:|---:|---:|---:|---:|
| Atomic | 12 / 10 | 6 / 6 | 12 / 12 | 5976984603492000 | 0 |
| Harmony | 14 / 6 | 13 / 13 | 14 / 14 | 6985169441103000 | 0 |

零余额观测均为 0 个；所有余额保持 `null`。Gas 按候选交易 tx_hash 去重并核对 top 交易 `gas_used × gas_price`，无冲突、不按 trace 重复计费。这是有限候选交易上下文，未覆盖完整地址历史、失败交易全集或来源份额，也未自动加入 LP 约束。

两张图必要顺序检查与保存的 scope 一致；26 个候选事件有 tx_index，但规范记录均缺 block_hash，该缺口单列保留。Atomic 此快照 online_complete=true，Harmony=false；余额/Gas 未闭合使两个图继续 `ASSUMPTION_CONDITIONAL`。完整逐事件位置、逐交易 Gas 和逐余额对清单位于 `derived/context_inventory_replay_001/`。

```text
python -B src/context_evidence_r2.py inventory --replay derived/replay_FINAL --output derived/context_inventory_FINAL
```

本脚本只读图和采集结果，不修改输入、金额规则或任何账本。外部验收保持 `PENDING_REVIEW`。
