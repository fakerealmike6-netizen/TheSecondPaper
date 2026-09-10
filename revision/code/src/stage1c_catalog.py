"""Offline query catalogue and small queue from frozen metadata, never outcomes."""
import argparse
import copy
import csv
import hashlib
import json
from pathlib import Path

VERSION = 'STAGE1C_CATALOG_V1'
DEVELOPMENT_INCIDENTS = {'INC_ATOMIC_WALLET_2023', 'INC_HARMONY_BRIDGE_2022'}
DEVELOPMENT_QUERIES = {'PRIVATE_DEVELOPMENT_QUERY_1',
                       'PRIVATE_DEVELOPMENT_QUERY_2'}


def cost_key(row):
    # Deliberately no amount, label-hit, recall, interval or positivity fields.
    return (row['incident_id'] in DEVELOPMENT_INCIDENTS,
            row['cost_window_seconds'] is None,
            row['cost_window_seconds'] if row['cost_window_seconds'] is not None else 0,
            row['cost_reference_entry_events'],
            -row['seed_cached_event_count'], row['query_id'])


def propose(document):
    rows = copy.deepcopy(document['queries'])
    ids = [row['query_id'] for row in rows]
    if len(ids) != len(set(ids)) or len(ids) != document['registered_query_count']:
        raise ValueError('Catalogue query identity/count mismatch')
    for row in rows:
        if row['next_batch_eligible'] and (not row['exact_single_seed'] or row['seed_member_count'] != 1 or row['stratum'] == 'S3'):
            raise ValueError('Unsafe seed incorrectly marked eligible')
    eligible = sorted([row for row in rows if row['next_batch_eligible'] and row['query_id'] not in DEVELOPMENT_QUERIES], key=cost_key)
    # Cover each currently unrepresented incident first; then fill remaining
    # places by the same predeclared cost ordering. No outcome-based quotas.
    primaries = []
    covered = set(DEVELOPMENT_INCIDENTS)
    for row in eligible:
        if row['incident_id'] not in covered:
            primaries.append(row)
            covered.add(row['incident_id'])
            if len(primaries) == 4:
                break
    selected = {row['query_id'] for row in primaries}
    for row in eligible:
        if len(primaries) == 4:
            break
        if row['query_id'] not in selected:
            primaries.append(row)
            selected.add(row['query_id'])
    reserves = [row for row in eligible if row['query_id'] not in selected][:2]
    status = {row['query_id']: ('PRIMARY_PROPOSED', i + 1) for i, row in enumerate(primaries)}
    status.update({row['query_id']: ('RESERVE_PROPOSED', i + 1) for i, row in enumerate(reserves)})
    for row in rows:
        row['queue_role'], row['queue_rank'] = status.get(row['query_id'], ('CURRENT_DEVELOPMENT_PILOT', None) if row['query_id'] in DEVELOPMENT_QUERIES else ('RETAINED_NOT_SELECTED', None))
    return {'schema_version': 'stage1c-next-batch-proposal-v1', 'method_version': VERSION,
            'primary': primaries, 'reserve': reserves,
            'registered_queries_retained': len(rows), 'eligible_unrun_queries': len(eligible),
            'catalog': sorted(rows, key=lambda row: row['query_id']),
            'selection_order': ['New incident coverage before repetitions of an incident',
                'Exactly resolved single event in the currently validated native ETH context adapter',
                'Other-than-development incidents, known window before unknown window, ascending observed reference window seconds',
                'Ascending number of distinct same-asset causal reference entry events of ALL reference statuses',
                'Descending cached seed count; query_id lexical order as final deterministic tie-break'],
            'excluded_selection_features': ['source amount', 'positive lower bound', 'method recall', 'expected label hit', 'number of positive reference addresses'],
            'cost_limits': ['Existing reference event count is a lower bound, not predicted full candidate history size.',
                'Reference windows only propose outer bounds and must not be converted to branch whitelists.',
                'Reference fact cache does not establish complete candidate graph or context availability.',
                'No credit estimate is asserted without an actual provider plan; next collection needs the carried ledger and its own authorization.'],
            's3_policy': 'All 3 S3 query IDs and 21 member records remain registered; unresolved independent injection is not guessed or replaced with one member.',
            'zero_hop_policy': 'Legal zero-hop queries remain eligible when their exact seed structure is supported; no simplicity exclusion.',
            'new_collection_executed': False, 'new_research_requests': 0,
            'final_evaluation_denominator_frozen': False, 'external_acceptance': 'PENDING_REVIEW'}


def write_outputs(document, output, input_sha256=None):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    proposal = propose(document)
    proposal['frozen_input_sha256'] = input_sha256
    proposal['input_sources'] = document['sources']
    rows = proposal.pop('catalog')
    with (output / 'QUERY_CATALOG_CURRENT.csv').open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows({k: json.dumps(v, separators=(',', ':')) if isinstance(v, (list, dict)) else v for k, v in row.items()} for row in rows)
    (output / 'NEXT_BATCH_PROPOSAL.json').write_text(json.dumps(proposal, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    lines = ['# 下一批小队列建议', '', '仅建议，不执行新查询采集；外部验收 PENDING_REVIEW。不是正式总体分母。', '',
             '注册表保留 49 个 query（S2 41、S1 5、S3 3），共 67 个 seed member。当前两例仍为开发/首批验证。', '',
             '先补当前两案以外的案件，再按精确单事件与当前已验证 ETH 上下文结构、已有窗口、参考事件量和缓存排序，最终以 query_id 打破平局。所有参考状态均可用于窗口成本；来源金额、标签命中、召回和区间端点没有参与选择。', '',
             '| 角色 | 案件 | query_id | 既有窗口秒 | 已有同资产参考进入事件 | 种子缓存 |',
             '|---|---|---|---:|---:|---:|']
    for role, rr in [('主选', proposal['primary']), ('备选', proposal['reserve'])]:
        for i, row in enumerate(rr, 1):
            lines.append(f"| {role}{i} | {row['incident_id']} | `{row['query_id']}` | {row['cost_window_seconds']} | {row['cost_reference_entry_events']} | {row['seed_cached_event_count']} |")
    lines += ['', '窗口及事件数来自已有参考事实，只是采集成本下界代理，并非完整候选图或已确认的信用点报价。建议不预先排除未知标签、未知终点或无正下界的样本。全局窗口可按参考证据界定，但采集不能沿参考路径白名单进行。', '',
              '所有 S3 保留独立注入未确认状态，本轮不注入；ERC-20 核心与 canonical WETH 组件有支持，但下一批优先当前已实际验证的 ETH 上下文适配，未强行承诺复杂协议续接。零跳合法且仍在目录，不因简单而剔除。', '',
              '两个开发样本已有完整上下文；其余候选仅确认既有参考事件及精确种子缓存，未确认完整主动采集覆盖。本轮新增研究请求 0，未启动上述任何新 query。']
    (output / '08_NEXT_BATCH_PROPOSAL.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return proposal


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    data = args.input.read_bytes()
    result = write_outputs(json.loads(data), args.output, hashlib.sha256(data).hexdigest())
    print(json.dumps({'registered': result['registered_queries_retained'], 'primary': [r['query_id'] for r in result['primary']], 'reserve': [r['query_id'] for r in result['reserve']]}))


if __name__ == '__main__':
    main()
