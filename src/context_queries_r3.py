"""Freeze complete native context SQL for the already closed R2 candidate graph.

This queries each account's whole first/last blocks and the intervening window.
It does not expand candidates or query service histories. Selected transaction
trees include ancestors so local frame success cannot conceal a rollback.
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re


COLS = {'record_type':'varchar', 'block_number':'bigint', 'block_hash':'varchar',
        'block_time':'varchar', 'tx_hash':'varchar', 'tx_index':'bigint',
        'from_address':'varchar', 'to_address':'varchar', 'value_raw':'varchar',
        'success':'boolean', 'gas_used':'varchar', 'effective_gas_price':'varchar',
        'gas_limit':'varchar', 'input_data':'varchar', 'trace_address':'varchar',
        'trace_type':'varchar', 'call_type':'varchar', 'subtraces':'bigint',
        'error':'varchar', 'tx_success':'boolean', 'created_address':'varchar',
        'refund_address':'varchar', 'withdrawal_index':'bigint', 'fee_recipient':'varchar'}


def select(values, source):
    return 'SELECT\n  ' + ',\n  '.join(f"CAST({values.get(k, 'NULL')} AS {v}) AS {k}" for k,v in COLS.items()) + '\n' + source


def hx(value):
    return "concat('0x',lower(to_hex(" + value + ')))'


def build(query):
    rows = query['rows']
    if not rows or len({r['address'] for r in rows}) != len(rows):
        raise ValueError('Distinct nonterminal account windows required')
    for r in rows:
        if not re.fullmatch('0x[0-9a-f]{40}', r['address']) or r['role'] != 'NON_TERMINAL_MODEL_ACCOUNT' or r['ledger_end_block'] < r['ledger_start_block']:
            raise ValueError('Invalid frozen context window')
    low, high = min(r['ledger_start_block'] for r in rows), max(r['ledger_end_block'] for r in rows)
    start = datetime.fromtimestamp(min(r['first_candidate_timestamp'] for r in rows), timezone.utc).date().isoformat()
    end = datetime.fromtimestamp(max(r['last_candidate_timestamp'] for r in rows), timezone.utc).date().isoformat()
    values = ',\n  '.join(f"({r['address']},{r['ledger_start_block']},{r['ledger_end_block']})" for r in rows)
    def guard(alias):
        return f"{alias}.block_date BETWEEN DATE '{start}' AND DATE '{end}' AND {alias}.block_number BETWEEN {low} AND {high}"
    # Full tree rows may mention counterparties, but only active frozen accounts
    # enter the ledger. The query neither labels nor recursively scans them.
    cte = f'''-- R3 finite complete native context; no candidate expansion; all required rows, no SQL LIMIT.
WITH scope(address, first_block, last_block) AS (VALUES
  {values}
), top_keys AS (
 SELECT DISTINCT t.hash FROM ethereum.transactions t JOIN scope s
 ON t.block_number BETWEEN s.first_block AND s.last_block AND (t."from"=s.address OR t."to"=s.address)
 WHERE {guard('t')}
), trace_keys AS (
 SELECT DISTINCT t.tx_hash AS hash FROM ethereum.traces t JOIN scope s
 ON t.block_number BETWEEN s.first_block AND s.last_block
 AND (t."from"=s.address OR t."to"=s.address OR t.address=s.address OR t.refund_address=s.address)
 WHERE {guard('t')} AND t.tx_hash IS NOT NULL
), tx_keys AS (SELECT hash FROM top_keys UNION SELECT hash FROM trace_keys)
'''
    tx = {'record_type':"'transaction'", 'block_number':'t.block_number','block_hash':hx('t.block_hash'), 'block_time':'t.block_time',
          'tx_hash':hx('t.hash'), 'tx_index':'t."index"', 'from_address':hx('t."from"'), 'to_address':hx('t."to"'),
          'value_raw':'t.value','success':'t.success','gas_used':'t.gas_used','effective_gas_price':'t.gas_price','gas_limit':'t.gas_limit','input_data':hx('t.data')}
    tr = {'record_type':"'trace'",'block_number':'t.block_number','block_hash':hx('t.block_hash'),'block_time':'t.block_time',
          'tx_hash':hx('t.tx_hash'),'tx_index':'t.tx_index','from_address':hx('t."from"'),'to_address':hx('t."to"'),
          'value_raw':'t.value','success':'t.success','trace_address':'json_format(CAST(t.trace_address AS JSON))',
          'trace_type':'t.type','call_type':'t.call_type','subtraces':'t.sub_traces','error':'t.error','tx_success':'t.tx_success',
          'created_address':hx('t.address'),'refund_address':hx('t.refund_address')}
    wd = {'record_type':"'withdrawal'",'block_number':'t.block_number','block_time':'t.block_time','to_address':hx('t.address'),
          'value_raw':'t.amount * UINT256 \'1000000000\'', 'withdrawal_index':'t."index"','success':'true'}
    bl = {'record_type':"'fee_recipient'",'block_number':'t.number','block_time':'t.time','block_hash':hx('t.hash'),'fee_recipient':hx('t.miner')}
    reward = dict(tr,record_type="'protocol_credit'")
    parts = [select(tx, f"FROM ethereum.transactions t JOIN tx_keys k ON t.hash=k.hash WHERE {guard('t')}"),
             select(tr, f"FROM ethereum.traces t JOIN tx_keys k ON t.tx_hash=k.hash WHERE {guard('t')}"),
             select(wd, f"FROM ethereum.withdrawals t WHERE {guard('t')} AND EXISTS (SELECT 1 FROM scope s WHERE t.address=s.address AND t.block_number BETWEEN s.first_block AND s.last_block)"),
             select(bl, f"FROM ethereum.blocks t WHERE t.date BETWEEN DATE '{start}' AND DATE '{end}' AND t.number BETWEEN {low} AND {high} AND EXISTS (SELECT 1 FROM scope s WHERE t.miner=s.address AND t.number BETWEEN s.first_block AND s.last_block)"),
             select(reward, f"FROM ethereum.traces t WHERE {guard('t')} AND t.tx_hash IS NULL AND EXISTS (SELECT 1 FROM scope s WHERE (t.\"from\"=s.address OR t.\"to\"=s.address OR t.address=s.address OR t.refund_address=s.address) AND t.block_number BETWEEN s.first_block AND s.last_block)")]
    return cte + '\nUNION ALL\n'.join(parts) + '\nORDER BY block_number, tx_index, record_type, tx_hash, trace_address\n'


def freeze(work, query):
    work=Path(work)
    folder=work/'private/context_queries'/query['name']
    folder.mkdir(parents=True, exist_ok=False)
    sql=build(query)
    (folder/'query.sql').write_text(sql,encoding='utf-8',newline='\n')
    digest=hashlib.sha256(sql.encode()).hexdigest()
    manifest={'schema_version':'stage1b-r3-context-query-v1','r3_context_scope':True,'kind':'context','query_id':query['query_id'],
              'sql_sha256':digest,'prepared_at_utc':datetime.now(timezone.utc).isoformat(),'source_graph':query['graph_identity'],
              'account_windows':query['rows'],'candidate_expansion':False,'metasleuth_new_requests':0,
              'schema_sources':['https://docs.dune.com/data-catalog/evm/ethereum/raw/'+x for x in ('transactions','traces','blocks','withdrawals')],
              'schema_access':'Official public docs via web on 2026-09-07; compilation not claimed before execution',
              'coverage_requires_successful_full_export':True,'order_by_is_serialization_only':True,
              'export_plan':{'selection':'ALL_REQUIRED_NATIVE_CONTEXT_AND_SELECTED_TRANSACTION_TREES','columns':len(COLS),'page_limit':1000,
                             'result_size':'OBTAIN_AFTER_EXECUTION_BEFORE_EXPORT','fee_rate_evidence':'Inherited Plus trial Free20credits/decimalMB with conservative documented envelope',
                             'no_export_if_full_required_result_cannot_fit_shared100':True}}
    (folder/'freeze_manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    return folder/'freeze_manifest.json'


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work',type=Path,required=True)
    args=parser.parse_args()
    targets=json.loads((args.work/'derived/CONTEXT_TARGETS.json').read_text(encoding='utf-8-sig'))
    for query in targets['queries']:
        print(freeze(args.work,query))
