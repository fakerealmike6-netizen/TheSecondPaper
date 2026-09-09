"""Render the editorial observation JSON only; reads no production input."""
from pathlib import Path
import csv, hashlib, json

folder = Path(__file__).resolve().parent
data = json.loads((folder/'09_OPTIMIZATION_CLOSURE_MATRIX.json').read_text(encoding='utf8'))
rows = data['rows']
if [r['item'] for r in rows] != list(range(1,17)):
    raise ValueError('Exactly the original sixteen rows in order are required')
allowed = set(data['allowed_statuses'])
for row in rows:
    if row['status'] not in allowed or not set(row['subitem_statuses'].values()) <= allowed:
        raise ValueError('A status outside the five explicit states is forbidden')
fields = ['item','optimization','status','applicable_scope','actual_or_wired_entrypoint','source_refs','run_refs',
          'observed_benefit','equivalence_evidence','open_items','subitem_statuses','promotion_requirements',
          'inherited_evidence','avoided_http_requests','saved_credits','saved_alchemy_cu','final_query_execution_complete']
with (folder/'09_OPTIMIZATION_CLOSURE_MATRIX.csv').open('w',encoding='utf-8-sig',newline='') as f:
    writer = csv.DictWriter(f,fieldnames=fields);writer.writeheader()
    for row in rows:
        writer.writerow({k:json.dumps(row[k],ensure_ascii=False,separators=(',',':'))
                         if isinstance(row[k],(dict,list)) else row[k] for k in fields})
lines = ['# 16项优化闭合矩阵：可更新的中间观测',
 '','本文件不是最终闭合回执。各行“已实际生效”只覆盖写明的局部机制；四query最终方法未执行，本次未生成最终包或达到checkpoint。外部验收仍为 PENDING_REVIEW。',
 '',f'观测开始：{data["created_at_utc"]}。源码/普通BQ在途均非最终冻结；在途最终状态记 UNKNOWN_NONFROZEN_OBSERVATION。',
 '', '固定16项来自已核验closure包所含原暂停报告§7，交付名来自 acceptance 第73行。仅使用五状态；源码逐文件SHA/必要函数行号和实际回执SHA详见 SOURCE_REFERENCES.json。',
 '', '| 项 | 优化 | 局部主状态 | 适用范围 / OPEN |','|---|---|---|---|']
for row in rows:
    text = row['applicable_scope']+'；OPEN：'+'；'.join(row['open_items'])
    lines.append(f'| {row["item"]} | {row["optimization"]} | {row["status"]} | {text.replace("|","／")} |')
for row in rows:
    lines += ['',f'## {row["item"]}. {row["optimization"]}', '',row['actual_or_wired_entrypoint'],'',
              '**已观察：** '+row['observed_benefit'],'','**等价/验证边界：** '+row['equivalence_evidence'],'',
              '**子项：** '+'；'.join(k+'＝'+v for k,v in row['subitem_statuses'].items()),'',
              '**更新条件：** '+row['promotion_requirements'],'',
              '源码（完整SHA/行号见refs）：'+'；'.join('`'+Path(p).name+'`' for p in row['source_refs']), '',
              '本轮回执（完整路径/SHA见本项JSON的run_refs）：'+'；'.join('`'+'/'.join(Path(p).parts[-2:])+'`' for p in row['run_refs']), '',
              '继承证据：已核验包 `OPTIMIZATION_EVIDENCE_AT_PAUSE.csv` 第'+str(row['item'])+'项；内部原回执绑定保留在本行JSON中，本次未重新运行或全量读取。']
lines += ['', '## 更新与限制','',
 '先编辑同目录JSON中的受影响行与来源引用，再运行 `python -B render_matrix.py`。root安装补丁后补安装manifest及实际回执，逐项更新SHA和适用范围；源码SHA变化本身不能把“未实测”升为“实际生效”。历史证据不可删除或冒充本轮。', '',
 '本次没有联网、Apps、数据库构造器、采集、collector/context/正式方法执行、测试或全仓审计。仅在本staging目录输出；没有读取49MB原件正文、扫描raw/history或对账账户。', '',
 '不能把角色删减/时间窗代理/合成LP调用差换算HTTP、credits或CU节省；所有金额节省及避免请求计数保留null。余额/局部容量在线剪枝明确没实现。']
(folder/'09_OPTIMIZATION_CLOSURE_MATRIX.md').write_text('\n'.join(lines)+'\n',encoding='utf8')
files=[]
for name in ['09_OPTIMIZATION_CLOSURE_MATRIX.json','09_OPTIMIZATION_CLOSURE_MATRIX.csv','09_OPTIMIZATION_CLOSURE_MATRIX.md','SOURCE_REFERENCES.json','build_observation.py','render_matrix.py']:
    p=folder/name; raw=p.read_bytes();files.append({'path':name,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
(folder/'MANIFEST.json').write_text(json.dumps({'status':'READY_FOR_ROOT_EDITORIAL_UPDATE','rows':16,
    'final_delivery':False,'external_acceptance':'PENDING_REVIEW','files':files},ensure_ascii=False,indent=2)+'\n',encoding='utf8')
print(json.dumps({'rendered_rows':16,'new_tests_run':0,'status':'READY_FOR_ROOT_EDITORIAL_UPDATE'}))
