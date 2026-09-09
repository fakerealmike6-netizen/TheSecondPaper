"""Offline R2 context inventory and exact WETH Dune evidence analysis.

No provider, credentials, budget mutation, certification adapter or LP change.
"""
import argparse
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from event_order import order_observed_transfers
from page_contract import initial_progress, validate_page


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'), parse_float=str)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')


def inside(root, relative):
    path = (root / relative).resolve()
    if not path.is_relative_to(root) or path.is_symlink():
        raise ValueError('Context input leaves declared root or is linked')
    return path


def receipt_payload(root, receipt, expected=None):
    path = inside(root, receipt['raw_path'])
    if receipt.get('http_status') != 200 or receipt.get('error_class'):
        raise ValueError('Context receipt not successful')
    if sha(path) != receipt['sha256'] or path.stat().st_size != receipt['raw_bytes']:
        raise ValueError('Context raw bytes/length do not match receipt')
    payload = read(path)
    if expected is not None and payload != expected:
        raise ValueError('Saved context payload differs from raw response')
    return payload, {'path':receipt['raw_path'], 'sha256':sha(path),
                     'bytes':path.stat().st_size, 'request_id':receipt['request_id']}


def analyze_weth(root, job_folder, baseline_weth_root, output):
    root, job_folder = Path(root).resolve(), Path(job_folder).resolve()
    baseline_weth_root, output = Path(baseline_weth_root).resolve(), Path(output).resolve()
    job = read(job_folder / 'job.json')
    if job['kind'] != 'context' or job['state'] != 'QUERY_STATE_COMPLETED':
        raise ValueError('Only completed context job accepted')
    freeze_path = inside(root, job['scope_freeze_path'])
    freeze = read(freeze_path)
    if sha(freeze_path) != job['scope_freeze_sha256']:
        raise ValueError('Context scope freeze bytes changed')
    sql = (job_folder / 'query.sql').read_text(encoding='utf-8')
    if hashlib.sha256(sql.encode()).hexdigest() != job['sql_sha256'] or sha(job_folder/'query.sql') != freeze['query_file_sha256']:
        raise ValueError('Context SQL differs from frozen request')
    for filename, digest in freeze['input_files'].items():
        if sha(inside(freeze_path.parent, filename)) != digest:
            raise ValueError('Context frozen input hash changed')
    submit, submit_source = receipt_payload(root, job['submit_receipt'], job['submit_response'])
    status, status_source = receipt_payload(root, job['status_receipt'], job['status_response'])
    execution = job['execution_id']
    if submit['execution_id'] != execution or status['execution_id'] != execution or status['state'] != 'QUERY_STATE_COMPLETED':
        raise ValueError('Context submit/status execution binding differs')
    progress, rows, page_sources = initial_progress(), [], []
    for offset in job['export_offsets']:
        receipt = read(job_folder / f'page_{offset}_receipt.json')
        page = read(job_folder / f'page_{offset}.json')
        _, source = receipt_payload(root, receipt, page)
        progress = validate_page(page, execution_id=execution, offset=offset,
                                limit=receipt['parameters']['limit'], progress=progress,
                                status_metadata=status['result_metadata'], receipt=receipt,
                                parameters=receipt['parameters'])
        rows.extend(page['result']['rows'])
        page_sources.append(source | {'saved_page_sha256':sha(job_folder/f'page_{offset}.json')})
    if not progress['complete'] or progress['observed_rows'] != len(rows):
        raise ValueError('Context page chain incomplete')
    archived, archive_sources = {}, []
    for role, filename in [('transaction','WETH_COMPONENT_TRANSACTION_EXCERPT.json'),
                           ('receipt','WETH_COMPONENT_RECEIPT_EXCERPT.json'),
                           ('internal','WETH_COMPONENT_INTERNAL_RESPONSE.json')]:
        path = baseline_weth_root/'evidence_samples'/filename
        excerpt = read(path)
        original = inside(baseline_weth_root, excerpt['evidence']['path'])
        if sha(original) != excerpt['evidence']['sha256']:
            raise ValueError('Original archived WETH hash changed')
        source = read(original)
        expected = excerpt.get('result',excerpt.get('response'))
        actual = next(row['result'] for row in source if row['id']==excerpt['evidence']['rpc_id']) if isinstance(source,list) else source
        if actual != expected:
            raise ValueError('Archived WETH excerpt not equal to original payload')
        archived[role] = expected
        archive_sources.append({'role':role,'excerpt_sha256':sha(path),'original_sha256':sha(original),'original_path':excerpt['evidence']['path']})
    policy = read(root/'configs/STAGE1B_R2_POLICY.json')['weth_component']
    tx, receipt = archived['transaction'], archived['receipt']
    num = lambda value: int(value,16) if isinstance(value,str) and value.startswith('0x') else int(value)
    by_path = {}
    for row in rows:
        path = tuple(row['trace_address'])
        if path in by_path or not all(type(i) is int and i >= 0 for i in path):
            raise ValueError('Duplicate or invalid Dune trace path')
        if (row['tx_hash'] != policy['tx_hash'] or row['block_number'] != policy['block_number']
                or row['block_hash'] != tx['blockHash'] or row['block_hash'] != receipt['blockHash']
                or row['tx_index'] != num(tx['transactionIndex']) or row['tx_index'] != num(receipt['transactionIndex'])):
            raise ValueError('Dune context contradicts exact archived identity')
        by_path[path] = row
    if () not in by_path:
        raise ValueError('Dune call tree root missing')
    closure = []
    for path,row in sorted(by_path.items()):
        children = sorted(p[-1] for p in by_path if len(p)==len(path)+1 and p[:-1]==path)
        complete = children == list(range(row['sub_traces'])) and (not path or path[:-1] in by_path)
        closure.append({'trace_address':list(path),'declared_children':row['sub_traces'],'observed_child_indices':children,'complete':complete})
        if not complete:
            raise ValueError('Dune call subtree is not closed')
    matched = [(p,r) for p,r in by_path.items() if r['from_address']==policy['credited_address']
               and r['to_address']==policy['contract'] and num(r['value_raw'])==num(policy['amount_raw'])
               and r['type']=='call' and r['call_type']=='call']
    if len(matched) != 1:
        raise ValueError('Exact canonical WETH CALL is not unique')
    target_path,target = matched[0]
    ancestors = [by_path[target_path[:i]] for i in range(len(target_path)+1)]
    if not all(r['success'] is True and r['tx_success'] is True and r['error'] is None for r in ancestors):
        raise ValueError('WETH call or ancestor not successful')
    children = [r for p,r in by_path.items() if len(p)>len(target_path) and p[:len(target_path)]==target_path]
    deposit = [r for r in receipt['logs'] if num(r['logIndex'])==policy['deposit_log_index']]
    deposit_matches = (len(deposit)==1 and deposit[0]['address']==policy['contract']
                       and deposit[0]['transactionHash']==policy['tx_hash']
                       and deposit[0]['topics'][0]=='0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
                       and '0x'+deposit[0]['topics'][1][-40:]==policy['credited_address']
                       and num(deposit[0]['data'])==num(policy['amount_raw']))
    if not deposit_matches:
        raise ValueError('Archived exact Deposit conflicts with WETH context')
    legacy_matches = [r for r in archived['internal']['result'] if r['from']==target['from_address']
                      and r['to']==target['to_address'] and r['value']==target['value_raw']]
    if len(legacy_matches)!=1 or num(legacy_matches[0]['gasUsed'])!=num(target['gas_used']):
        raise ValueError('Indexed legacy WETH input does not match Dune call')
    baseline = read(baseline_weth_root/'replayed_component_verification.json')
    result = {'schema_version':'r2-weth-dune-context-analysis-1','status':'VERIFIED_INDEXED_CONTEXT_WITH_CERTIFICATION_GAPS',
              'created_at_utc':datetime.now(timezone.utc).isoformat(),'execution_id':execution,
              'job_file_sha256':sha(job_folder/'job.json'),'sql_sha256':job['sql_sha256'],
              'freeze_manifest_sha256':sha(freeze_path),'sources':[submit_source,status_source,*page_sources],
              'archive_sources':archive_sources,'page_contract':progress,'raw_rows':len(rows),
              'normalized_distinct_trace_paths':len(by_path),'tree_closure':closure,
              'legacy_indexed_row_ordinal':1,'legacy_event_id':f"eip155:1:tx:{policy['tx_hash']}:trace:1",
              'actual_dune_call_tree_path':list(target_path),'legacy_and_actual_paths_interchangeable':False,
              'canonical_call':target,'all_ancestors_successful':True,
              'supported_deposit_selector_observed':target['input_data'] in ('0x','0xd0e30db0'),
              'observed_descendant_count':len(children),'observed_descendant_value_calls':sum(num(r['value_raw'])>0 and r['call_type']=='call' for r in children),
              'observed_subtree_refunds':[] if not children else None,
              'no_descendant_call_in_complete_indexed_tree':not children,
              'output_data_unknown_preserved':target['output_data'] is None,
              'deposit_log_semantic_identity_matches':deposit_matches,'deposit_log_index':policy['deposit_log_index'],
              'log_to_call_frame_binding_established':False,
              'delegatecall_context_rows':sum(r['call_type']=='delegatecall' for r in rows),
              'trace_values_summed_as_independent_transfers':False,
              'baseline_checks_passed':sum(baseline['checks'].values()),'baseline_checks_total':len(baseline['checks']),
              'baseline_real_certification_value':baseline.get('real_component_certified',baseline['semantic_unit']['certified']),
              'baseline_real_certification_preserved':baseline.get('real_component_certified',baseline['semantic_unit']['certified']) is False,
              'real_component_certified':False,'real_conversion_enabled':False,
              'remaining_blockers':['NO_AUTHORIZED_DUNE_TO_RPC_BINDING_ADAPTER','LOG_TO_CALL_FRAME_BINDING_MISSING','HISTORICAL_RUNTIME_AT_FIXED_BLOCK_MISSING','VERIFIED_SOURCE_RUNTIME_PROVENANCE_MISSING'],
              'surrounding_transaction_certified':False,'component_fee_raw':None,'certified_refund_raw':None,
              'known_execution_credits':job['execution_cost_credits'],'known_export_credits':None,
              'export_risk_upper_credits':job['reserved_export'],'network_requests_by_analysis':0,
              'external_acceptance_status':'PENDING_REVIEW'}
    write(output/'ANALYSIS.json',result)
    write(output/'TRACE_ROWS.json',rows)
    return result


def inventory(replay, output):
    replay,output = Path(replay).resolve(),Path(output).resolve()
    results=[]
    for folder in sorted(replay.iterdir()):
        if not folder.is_dir() or not (folder/'collection.json').is_file():
            continue
        collection,graph,scope = [read(folder/name) for name in ('collection.json','fixed_graph.json','model_scope.json')]
        candidates=collection['candidate_events']; context=collection['context_events']
        rows_by_tx=defaultdict(list)
        for event in candidates+context:
            if event['kind']=='top':rows_by_tx[event['tx_hash']].append(event)
        gas_rows=[]
        for tx in sorted({e['tx_hash'] for e in candidates}):
            facts=rows_by_tx[tx]
            known={(e.get('gas_used'),e.get('gas_price'),e.get('gas_raw')) for e in facts
                   if e.get('gas_used') is not None and e.get('gas_price') is not None}
            row={'tx_hash':tx,'status':'MISSING_TOP_TRANSACTION_GAS','gas_used':None,'gas_price':None,'gas_raw':None}
            if len(known)>1:
                row['status']='CONFLICTING_TOP_TRANSACTION_GAS'
            elif known:
                used,price,fee=next(iter(known));product=int(used)*int(price)
                row.update(status='OBSERVED_TOP_TRANSACTION_GAS' if fee in (None,product) else 'GAS_PRODUCT_CONFLICT',gas_used=str(used),gas_price=str(price),gas_raw=str(product))
            gas_rows.append(row)
        physical=graph['physical_fact_manifest']
        if sorted(e['event_id'] for e in physical)!=sorted(e['event_id'] for e in candidates):
            raise ValueError('Graph physical facts differ from candidate membership')
        ordered,audit=order_observed_transfers(physical)
        balances=[{'address_asset':key,'balance_raw':value,'status':'UNKNOWN_NO_ANCHOR' if value is None else 'EXPLICIT_GRAPH_VALUE'} for key,value in sorted(graph['initial_balances'].items())]
        event_rows=[{'event_id':e['event_id'],'tx_hash':e['tx_hash'],'kind':e['kind'],'block':e['block'],'tx_index':e['tx_index'],'log_index':e.get('log_index'),'trace_address':e.get('trace_address'),'execution_index':e.get('execution_index'),'block_hash':e.get('block_hash')} for e in ordered]
        item={'name':folder.name,'query_id':collection['query_id'],'input_hashes':{name:sha(folder/name) for name in ('collection.json','fixed_graph.json','model_scope.json')},
              'candidate_events':len(candidates),'context_events':len(context),'candidate_kind_counts':dict(Counter(e['kind'] for e in candidates)),
              'balance_pairs':len(balances),'unknown_balance_pairs':sum(r['balance_raw'] is None for r in balances),'observed_zero_balance_pairs':sum(r['balance_raw'] in (0,'0') for r in balances),
              'balance_rows':balances,'gas_rows':gas_rows,'candidate_distinct_transactions':len(gas_rows),
              'candidate_gas_known_transactions':sum(r['status']=='OBSERVED_TOP_TRANSACTION_GAS' for r in gas_rows),
              'candidate_transaction_gas_total_wei':str(sum(int(r['gas_raw']) for r in gas_rows if r['status']=='OBSERVED_TOP_TRANSACTION_GAS')),
              'candidate_transactions_missing_gas':sum(r['status']=='MISSING_TOP_TRANSACTION_GAS' for r in gas_rows),
              'gas_fact_conflicts':[r for r in gas_rows if 'CONFLICT' in r['status']],
              'gas_source_share_known':False,'gas_is_complete_address_history':False,'gas_automatically_enforced_by_lp':False,
              'order_audit':audit,'order_audit_matches_saved_scope':audit==scope['order_validation'],
              'missing_transaction_indices':sum(e['tx_index'] is None for e in candidates),'missing_block_hashes':sum(not e.get('block_hash') for e in candidates),
              'event_position_rows':event_rows,'scope_status':scope['status'],'online_complete':scope['online_complete'],
              'unchanged_limits':['UNKNOWN_BALANCES_NOT_ZERO','DUNE_BALANCE_SOURCE_GATED_NO_ENTITLEMENT','GAS_CONTEXT_NOT_SOURCE_ATTRIBUTION','NO_REAL_WORLD_CONSERVATIVE_GUARANTEE']}
        write(output/folder.name/'INVENTORY.json',item);results.append(item)
    summary={'schema_version':'r2-context-inventory-1','created_at_utc':datetime.now(timezone.utc).isoformat(),
             'replay_source':replay.name,'query_count':len(results),'queries':results,
             'status':'COMPLETED_WITH_RECORDED_GAPS','network_requests':0,'model_inputs_modified':False,
             'external_acceptance_status':'PENDING_REVIEW'}
    write(output/'SUMMARY.json',summary)
    return summary


def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='command',required=True)
    weth=sub.add_parser('weth')
    for name in ('root','job-folder','baseline-weth-root','output'):weth.add_argument('--'+name,type=Path,required=True)
    inv=sub.add_parser('inventory');inv.add_argument('--replay',type=Path,required=True);inv.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    result=(analyze_weth(args.root,args.job_folder,args.baseline_weth_root,args.output) if args.command=='weth' else inventory(args.replay,args.output))
    print(json.dumps({k:result[k] for k in ('status','raw_rows','query_count','actual_dune_call_tree_path') if k in result}))


if __name__=='__main__':main()
