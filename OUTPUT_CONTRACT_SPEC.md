# Stage1C-R1 输出验收契约

版本：`stage1c-r1-output-acceptance-v1`。本模块验收已生成的方法返回，修复 C1-V01-A/B 与硬不变量未阻止成功交付的问题；不改变冻结方法、样本、目标、标签或旧实验版本。它不生成实验答案，不调用求解器或 Oracle，不读取 hidden、参考标签或真实来源金额真值。

## 接口与调用边界

`expected_domains(document)` 从共同观察事实生成方法各自的完整输出域，返回 `passed`、结构化 `errors`、`addresses`、`interval_events`、`baseline_events`、`allocation_ports`、`port_facts`、按方法区分的联合资产域，以及输入决定的 Haircut 不适用原因、边界补全和时序可达标记。目标按冻结 `objective_groups` 与实际收款端口交叉核对；不能从某个方法的返回键或 Oracle 答案推断应有目标。

`accept_method_results(document, results, *, expected_identity=None)` 返回确定性验收回执。调用方必须传入已核对 manifest 的 `sample_id`、`query_id`、`input_fact_hash`、`scope_hash`、`label_version`，并传入冻结 `method_versions` 映射进行方法版本核验。文件字节身份由调用方验证；本模块另绑定规范化观察对象身份，二者不可混用。

调用方应先原样保存原生返回与异常诊断，验证原生方法身份后加入规范 envelope，保留原生身份，再调用本契约。验收失败的 query 不得进入认证统计、flat、成功报告或成功交付包；必须向 batch 和 CLI 传播失败。入口集成、native identity 核验、科学指标、Oracle 对照及报告/包离线重验属于 runner/result gate，不能只在测试路径调用本模块。

回执包括 `status`（PASS/FAIL）、`passed`、`errors`、`method_checks`、`hard_invariants`、`expected_domains`、域/审计计数以及输入、方法返回、可信身份与契约源文件的 SHA-256 绑定。错误记录固定为 `{code, method, path, detail}`，保留多个可定位诊断。回执不含时间或随机值，允许相同输入和执行源下离线精确重算。

## 方法型完整输出域

七种方法必须各出现一次，未知、缺失、非法形状和错误 method_id 均失败。区间方法输出每个冻结地址资产组、每个服务入账事件及每个联合资产；三种基线的事件域是全部物理来源端口，包括 seed、background、费用及已连接协议的 input/refund/net/output。地址输出同时保留有定义但不支持的零值项；输出集合另依各方法自己的语义生成，成员须唯一且完全吻合。

区间为精确上下界，Reachability 没有来源金额，Poison 为名义量，Haircut 为比例分摊点。禁止混入其他金额型字段。金额接受有限精确整数、有理数字符串或十进制字符串；bool、float、NaN、Infinity、null、负来源量以及非法表示均失败。端点状态、证书标记、完整 witness、目标事件成员与目标值一致性均核验，并按对应约束模型检查所给 witness 的可行性。契约不通过重新求最优值修补输出。

合法空目标不等于丢失目标。空目标仅当观察中确实没有目标；区间方法对源潜在资产返回经过模型可行性证明的 `[0,0]`，基线联合域为空。零跳和明确零上界继续有效，不以正结果作为样本或输出域筛选条件。

Haircut 的六个冻结缺失余额样本允许 NOT_APPLICABLE，必须有输入确实触发的未知余额、具体账户和事件原因，且金额、allocation 和集合输出为 null。不能用无关的未知余额、空字典或一般错误冒充合法不适用。成功 Haircut 则必须提供全部来源端口的精确 allocation，包括非目标、费用及协议端口；对原约束作完整可行性检查后，每个事件点及别名须等于 allocation，地址和联合点须精确等于其冻结入账端口之和。

H_BMIN_BOUNDARY_V1 按观察时序独立重建并逐项核对，是 baseline 的补全假设，不能写成真实余额。Poison 名义总额可超过共同源或 FULL 上界，Haircut 不必等于隐藏真值，这些都不是验收失败理由。

## 必须使验收失败的硬不变量

- 同图信息放宽：相同变量和等式、余额约束仅放宽的结构关系必须成立；每个地址、事件及联合区间必须包含 FULL 对应区间。
- 独立目标副本：每个地址资产保留完整内部约束和自己的共同源预算，移除跨副本共享身份；副本身份域唯一，单目标区间等于 FULL，独立联合区间包含 FULL，并精确等于各目标副本端点之和。此联合量不能按单一共同源预算截断。
- 协议输入边界：对观察中实际连接的转换检查边界变量、源零续接标记及 gross/refund 守恒元数据。没有已连接协议特征时，地址、事件和联合区间必须与 FULL 完全相同；有连接特征时不凭空增加一般性的端点次序要求。

主要错误码包括 `OUTPUT_DOMAIN`、`RESULT_IDENTITY`、`METHOD_IDENTITY`、`METHOD_VERSION`、`EXACT_NUMBER`、`ALLOCATION_INFEASIBLE`、`ALLOCATION_AUDIT_ERROR`、`POINT_ALLOCATION_MISMATCH`、`POINT_AGGREGATE_MISMATCH`、`BALANCE_ENDPOINT_NESTING`、`TARGET_COPY_EXACT_DECOMPOSITION`、`NO_PROTOCOL_FEATURE_EQUALITY`。具体实际错误码以源文件和回执为准。

## 本模块局部验证

`tests/test_stage1c_output_contract.py` 的 36 项测试在 Windows / Python 3.14.3 实跑通过，失败 0、错误 0、跳过 0。完整日志和源身份见 `checks/OUTPUT_CONTRACT_TESTS.json` 与 `.txt`。测试覆盖四个外部验收旧负控、输出域/身份/类型/精确数/集合畸形、完整 allocation 与点一致性、真实格式合成图、六个冻结合法 NA、合法空目标/零跳/显式零、WETH 端口、硬不变量、禁止求解器/Oracle 调用和不修改输入。另有集成代理负责的边界故障注入和真实 CLI 验证；不以本模块局部测试冒充全套或整批实验完成。

完整公开验证随后暴露出六 NA 单测对根 manifest/保存结果的依赖：验证器的最小执行镜像不含这些路径，导致一项单测错误。已仅修复这一新增测试，改从镜像已有的 `controlled_v1/MANIFEST.json.samples` 取原六图、核观察 SHA 和样本身份、实际生成方法返回，再使用可信 fixture 身份封套验收；不读取保存答案或 hidden，不跳过任何样本，另明确断言全部七个 NA 输出字段为 null。修复后在仅含 src/tests/fixtures/configs/controlled_v1 的全新最小镜像实际通过 36/36，失败/错误/跳过均为 0。最终测试身份与日志以 `checks/OUTPUT_CONTRACT_MINIMAL_MIRROR_TESTS.json` 和 `.txt` 为准；前次完整失败记录保留。

本契约 PASS 是当前执行的内部验收结果，不是外部盲审结论。外部状态仍为 PENDING_REVIEW；整个 R1 交付停止点为 CHECKPOINT_1C_R1_REACHED。
