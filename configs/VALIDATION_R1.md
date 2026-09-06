# Stage1B-R1离线便携验证

解压到新目录后运行；output必须是新建目录，且不能位于输入tree内，也不能是其祖先目录。

```text
python -B src/validate_review_bundle_r1.py --tree . --kind public --output ../new_public_validation
python -B src/validate_review_bundle_r1.py --tree . --kind min --output ../new_min_validation
```

本次最终MIN使用已归档的失败及成功标签切片，两视图均为same raw，运行：

```text
python -B src/validate_review_bundle_r1.py --tree . --kind min --manifest configs/final_validation.json --output ../new_min_final_validation
```

`auto`通过private/reference_replay目录存在与否选择MIN或public。明确使用`min`时，缺少必需真实输入将记录FAIL；public明确SKIP真实重放，不把合成测试称为真实provider验证。每个命令都有stdout、stderr、退出状态；即使某项失败也继续独立项目，最终写validation_receipt.json。

验证器把src/tests/fixtures/configs复制到output的执行镜像。单元测试及其临时目录只修改镜像，不写冻结输入；没有固定105或183测试数量。受控线要求12场景、32组Oracle比较全部通过。MIN另运行reference重放并校验10430关系，缓存与旧Dune四图same-input重放、stop/coverage/候选/LP语义比较，以及原WETH证据哈希和绑定重放。WETH重放成功不等于组件获得真实认证。

所有Python子进程及其Python子进程继承sitecustomize：禁止socket连接和DNS，移除API_KEY/TOKEN/CREDENTIAL/PASSWORD/SECRET/AUTHORIZATION等凭据环境变量，阻止写出output。原始数据输入只接受tree内相对路径，拒绝绝对/盘符/UNC/路径穿越和链接。运行前后核对整个输入tree的文件列表与SHA-256，包含字节级未改动声明。

默认布局在代码DEFAULTS中。MIN需保留原portable输入、raw、baseline_derived的reference identity/summary和derived/bugfix_only_same_input下四图。最终标签与新增图通过可选manifest指定：

```json
{
  "additional_replays": [
    {
      "name": "label_update_same_raw",
      "kind": "dune",
      "policy": "configs/STAGE1B_POLICY.json",
      "events": "private/cache_replay/value_events_normalized.csv.gz",
      "members": "private/reference_replay/reference_query_members.csv",
      "registry": "derived/final_labels/address_registry.csv.gz",
      "jobs": "private/dune_live_jobs",
      "work": ".",
      "expected": "derived/label_update_same_raw/dune_live_replay"
    }
  ],
  "additional_fixed_graphs": [
    {
      "name": "continued_atomic",
      "graph": "derived/continued_observed_graph/atomic_simple_transfer/fixed_graph.json",
      "expected_result": "derived/continued_observed_graph/atomic_simple_transfer/lp/lp_fixed_graph_result.json"
    }
  ]
}
```

把manifest保存在tree内，例如configs/final_validation.json，并增加`--manifest configs/final_validation.json`。它只能选择既定replay类型与tree内输入，不能传入任意命令。reference、policy、events、members、cache_registry、dune_registry、jobs、weth_root、weth_evidence、expected_root可用同名键改变布局。weth_optional允许指定call_trace、historical_code、source_attestation、evidence_bundle、acquisition_catalogue及expected，全部仍须为tree内路径。

失败标签及候选batch的便携重放使用`kind: "batch"`，调用当前`dune_batch_r1.replay`，因此保留失败/未查询/身份冲突标签的独立语义。已有的`configs/validation_failed_lookup_same_raw.json`配置声明旧raw加失败标签的视图，`batch_specs: []`明确表示没有新候选batch；它比较本次label_update_same_raw的候选、stop、coverage、标签及采集缺口、计数和状态，另与bugfix_only_same_input/dune_live_replay比较相同图的LP区间。若标签改变金额图，必须改为对应新图的预期LP目录，不得继续沿用旧LP结果。

新候选batch的配置形如：

```json
{
  "additional_replays": [{
    "name": "candidate_batch_view",
    "kind": "batch",
    "policy": "configs/STAGE1B_POLICY.json",
    "events": "private/cache_replay/value_events_normalized.csv.gz",
    "members": "private/reference_replay/reference_query_members.csv",
    "registry": "derived/final_labels/address_registry.csv.gz",
    "label_manifest": "derived/final_labels/apply_manifest.json",
    "jobs": "private/dune_live_jobs",
    "work": ".",
    "batch_specs": [{
      "folder": "private/candidate_jobs/declared_completed_job",
      "freeze": "derived/candidate_batches/declared_batch/freeze_manifest.json"
    }],
    "expected": "derived/continued_observed_graph",
    "expected_lp": "derived/continued_observed_graph"
  }]
}
```

registry、label_manifest、work和batch_specs均为batch模式必填项。每条batch_specs都是required输入，必须提供folder和freeze；不自动发现、替换或忽略目录。缺文件、原始HTTP哈希/字节数不符、分页不完整、冻结SQL/区间不符、被SavedDuneProvider拒绝的任一声明作业，均使该重放FAIL。失败/未完成的真实SQL作业可保留在审查包的其他目录，不应伪装成required completed batch。

旧项目路径只作为来源元数据保存。便携适配器核对新位置registry的SHA-256等于原label_manifest声明值，显式传入新位置freeze，再使用原job绑定的freeze SHA-256核验；不跟随job中的历史绝对路径，不改写原manifest、job、SQL或响应。每个重放的portable_mapping_receipt.json列出原声明路径、便携路径及两者绑定的hash，并列出逐pilot加载的逻辑作业。内部适配器只替换路径解析，不改变采集或LP规则；所有子进程继续继承禁网、去凭据和输出写入限制。合成格式验证与离线归档重放均不新增真实provider验证声明。

expected_lp可省略，此时从expected的各pilot/lp/lp_fixed_graph_result.json读取。指定时也必须是tree内目录；缺少预期LP不会SKIP或视为成功。原输入树的逐文件hash在验证结束再次核对。公开包仍明确跳过全部私有真实重放。

最终MIN不复制完整标签库。portable_manifest的registry_sha256绑定公开交付以外的私有必要地址切片；预期same raw状态文件仍绑定原完整库。`label_slice_mapping: {"source_apply_manifest": "private/.../source_apply_manifest.json"}`显式开启这种映射。验证器逐项核对原apply文件字节SHA、其byte_identical映射的源/目标SHA及大小、portable_manifest的source_apply_manifest_sha256，以及source_registry_sha256与原apply的registry_sha256。预期状态必须等于这两个原源hash，重放状态必须等于真实切片文件及portable_manifest的hash；任一不符均FAIL，不会笼统忽略hash差异。

该映射核验已归档的来源绑定，不声称在未交付完整库时重新证明所有切片行的全库成员关系。候选事实、服务stop、coverage、frontier、标签缺口、采集计数和LP区间继续严格重放比较。缺少显式映射、伪造源hash、原apply字节变化、错误源文件映射、路径逃逸均有拒绝测试。两个最终视图还设置`required_no_new_candidate_jobs: true`，此时非空batch_specs会直接失败，确保最终验证不会悄悄加入新候选作业。

额外独立整数最大流/有理数见证检查：

```text
python -B src/verify_fixed_graph_independent.py --root . --output ../new_independent_result.json
```

默认读取derived/bugfix_only_same_input/{cache_probe,dune_live_replay}/两探针。可用`--derived-subdir`指定tree内另一同形目录。此检查器不导入原LP/Oracle/SciPy/NumPy；只适用于ETH seed/transfer、未知期初实际余额的已声明固定图，不支持任意转换，也不证明全链完整性。代码可公开；真实图输入不随公开包分发。来源哈希及路径/冲突guard改动见checks/portable_independent_checker_mapping.json与对应diff。
