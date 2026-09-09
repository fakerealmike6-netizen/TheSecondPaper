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
from exact_fields_r4 import exact_uint


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def load_verified_block_headers(work):
    """Load only request/response/body-bound saved historical block headers."""
    root = Path(work).resolve()
    inherited = root/'baseline/r3'
    headers = load_verified_block_headers(inherited) if inherited.is_dir() else {}
    def inside(path):
        path = (root / path).resolve()
        if not path.is_relative_to(root) or path.is_symlink():
            raise ValueError('Block evidence path escapes work')
        return path
    for receipt_path in sorted((root/'raw/rpc_r3').glob('*/receipt.json')):
        receipt = _read(receipt_path)
        if receipt.get('http_status') != 200 or receipt.get('error_class'):
            continue
        intent_path = receipt_path.parent/'dispatch_intent.json'
        intent = _read(intent_path)
        wire_path = inside(receipt['raw_path'])
        if _sha(wire_path) != receipt['raw_sha256'] or wire_path.stat().st_size != receipt['raw_bytes']:
            raise ValueError('Block evidence raw response identity mismatch')
        wire = _read(wire_path)
        wire = wire if isinstance(wire, list) else [wire]
        responses = {r['id']: r for r in wire}
        requests = {r['id']: r for r in intent['requests']}
        if len(responses) != len(wire) or len(requests) != len(intent['requests']):
            raise ValueError('Duplicate block request/response identity')
        for member in receipt['members']:
            path = inside(member['artifact_path'])
            if _sha(path) != member['artifact_sha256']:
                raise ValueError('Block envelope identity mismatch')
            envelope = _read(path)
            request, response = envelope['request'], envelope['response']
            if requests.get(request['id']) != request or responses.get(request['id']) != response:
                raise ValueError('Block envelope not bound to actual request/response')
            if request['method'] != 'eth_getBlockByNumber' or envelope.get('status') != 'SUCCESS_VALIDATED':
                continue
            result = response.get('result')
            if not isinstance(result, dict) or response.get('error'):
                continue
            number = exact_uint(request['params'][0], 'requested_block')
            if exact_uint(result['number'], 'result_block') != number:
                raise ValueError('Historical block response number mismatch')
            header = {'block_number': number, 'block_hash': result['hash'].lower(),
                      'timestamp': exact_uint(result['timestamp'], 'block_timestamp'),
                      'evidence_ids': sorted('sha256:'+_sha(p) for p in (receipt_path,intent_path,wire_path,path))}
            if number in headers and any(headers[number][k] != header[k] for k in ('block_hash','timestamp')):
                raise ValueError('Historical block header fact conflict')
            headers[number] = header
    return headers


def _header(headers, number):
    value = headers.get(number, headers.get(str(number)))
    if value is None:
        raise ValueError('LEDGER_BOUNDARY_TIME_EVIDENCE_MISSING:' + str(number))
    if exact_uint(value['block_number']) != number or not value.get('block_hash') or not value.get('evidence_ids'):
        raise ValueError('Invalid or unbound ledger block header')
    timestamp = exact_uint(value['timestamp'], 'block_timestamp')
    return {'block_number': number, 'block_hash': value['block_hash'].lower(),
            'timestamp': timestamp, 'date_utc': datetime.fromtimestamp(timestamp, timezone.utc).date().isoformat(),
            'evidence_ids': list(value['evidence_ids'])}


def date_contract(query, block_headers):
    """The numeric ledger window is enclosed by verified block-time evidence.

    Candidate timestamps are deliberately unused. A preceding/following known
    block is allowed as a conservative timestamp bracket, with both identities
    frozen so future replay can validate the inclusion independently.
    """
    windows = []
    for row in query['rows']:
        start, end = exact_uint(row['ledger_start_block']), exact_uint(row['ledger_end_block'])
        if start > end:
            raise ValueError('Inverted ledger range')
        lower_number = start if start in block_headers or str(start) in block_headers else row.get('before_anchor_block', start)
        upper_number = end if end in block_headers or str(end) in block_headers else row.get('after_anchor_block', end)
        lower, upper = _header(block_headers, exact_uint(lower_number)), _header(block_headers, exact_uint(upper_number))
        if not lower['block_number'] <= start <= end <= upper['block_number'] or lower['timestamp'] > upper['timestamp']:
            raise ValueError('Block/time evidence does not enclose ledger window')
        if lower['block_number'] < upper['block_number'] and lower['timestamp'] >= upper['timestamp']:
            raise ValueError('Ethereum block timestamp order conflict')
        windows.append({'address': row['address'], 'start_block': start, 'end_block': end,
                        'lower_header': lower, 'upper_header': upper})
    if not windows:
        raise ValueError('No ledger windows')
    return {'schema_version': 'stage1b-r4-context-date-contract-v1',
            'start_block': min(w['start_block'] for w in windows),
            'end_block': max(w['end_block'] for w in windows),
            'start_date_utc': min(w['lower_header']['date_utc'] for w in windows),
            'end_date_utc': max(w['upper_header']['date_utc'] for w in windows),
            'windows': windows,
            'basis': 'VERIFIED_LEDGER_BLOCK_TIMESTAMP_BRACKETS_NOT_CANDIDATE_TIMESTAMPS'}


def verify_frozen_scope(freeze_path, work_root, *, block_headers=None, sql_path=None):
    """Verify new freezes or immutable R3 SQL with saved compatible evidence.

    Returning PASS asserts both date and number predicates cover each window;
    it does not assert query execution or exported pagination completeness.
    """
    root, freeze_path = Path(work_root).resolve(), Path(freeze_path).resolve()
    if not freeze_path.is_relative_to(root):
        raise ValueError('Context freeze outside replay root')
    frozen = _read(freeze_path)
    if frozen.get('schema_version') not in ('stage1b-r3-context-query-v1','stage1b-r4-context-query-v1'):
        raise ValueError('Unknown context freeze schema')
    sql_path = Path(sql_path).resolve() if sql_path else freeze_path.parent/'query.sql'
    if not sql_path.is_relative_to(root) or _sha(sql_path) != frozen.get('sql_sha256'):
        raise ValueError('Frozen context SQL hash mismatch')
    sql = sql_path.read_text(encoding='utf-8')
    dates = re.findall(r"\b(?:block_date|date) BETWEEN DATE '([0-9-]+)' AND DATE '([0-9-]+)'", sql)
    if not dates or len(set(dates)) != 1:
        raise ValueError('Context SQL partition predicates are absent or inconsistent')
    start_date, end_date = dates[0]
    headers = load_verified_block_headers(root) if block_headers is None else block_headers
    query = {'rows': frozen['account_windows']}
    contract = frozen.get('coverage_contract')
    if contract is not None:
        if contract.get('schema_version') != 'stage1b-r4-context-date-contract-v1':
            raise ValueError('Unknown frozen date contract')
        if (start_date,end_date) != (contract['start_date_utc'],contract['end_date_utc']):
            raise ValueError('SQL dates differ from frozen ledger date domain')
        windows = contract['windows']
        expected_windows = [(r['address'],r['ledger_start_block'],r['ledger_end_block']) for r in query['rows']]
        if [(r['address'],r['start_block'],r['end_block']) for r in windows] != expected_windows:
            raise ValueError('Date contract account windows changed')
        for window in windows:
            for name in ('lower_header','upper_header'):
                saved = window[name]
                actual = _header(headers, exact_uint(saved['block_number']))
                if any(saved[k] != actual[k] for k in ('block_hash','timestamp','date_utc')):
                    raise ValueError('Frozen block-time evidence identity changed')
    else:
        if frozen['schema_version'] != 'stage1b-r3-context-query-v1':
            raise ValueError('New context freeze lacks required date evidence contract')
        contract = date_contract(query, headers)
        windows = contract['windows']
    for window in windows:
        lower,upper=window['lower_header'],window['upper_header']
        if not lower['block_number'] <= window['start_block'] <= window['end_block'] <= upper['block_number'] or lower['timestamp'] > upper['timestamp']:
            raise ValueError('Frozen block headers do not enclose ledger domain')
        if not start_date <= lower['date_utc'] <= upper['date_utc'] <= end_date:
            raise ValueError('SQL_DATE_DOMAIN_DOES_NOT_COVER_LEDGER_BLOCK_DOMAIN')
    # Same allowlisted SQL structure, exact address windows, and number range.
    # A hash alone cannot certify that a malicious/restricted SQL has no holes.
    expected_sql = _build_sql(query, start_date, end_date)
    if sql != expected_sql:
        raise ValueError('Context SQL logical predicates differ from declared complete-window template')
    if frozen.get('sql_logic_sha256', _sha(sql_path)) != hashlib.sha256(expected_sql.encode()).hexdigest():
        raise ValueError('Frozen SQL logical identity mismatch')
    return {'status':'PASS','schema_version':'stage1b-r4-context-coverage-validation-v1',
            'compatibility_mode':'LEGACY_R3_VERIFIED_WITH_SAVED_BLOCKS' if frozen['schema_version']=='stage1b-r3-context-query-v1' else 'NATIVE_R4_DATE_CONTRACT',
            'sql_sha256':_sha(sql_path),'sql_dates':[start_date,end_date],
            'window_count':len(windows),'windows':windows,'date_domain_verified':True,
            'block_domain_verified':True,'network_requests':0}


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


def _build_sql(query, start, end):
    rows = query['rows']
    if not rows or len({r['address'] for r in rows}) != len(rows):
        raise ValueError('Distinct nonterminal account windows required')
    for r in rows:
        if not re.fullmatch('0x[0-9a-f]{40}', r['address']) or r['role'] != 'NON_TERMINAL_MODEL_ACCOUNT' or r['ledger_end_block'] < r['ledger_start_block']:
            raise ValueError('Invalid frozen context window')
    low, high = min(r['ledger_start_block'] for r in rows), max(r['ledger_end_block'] for r in rows)
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


def build(query, *, block_headers=None):
    headers = block_headers if block_headers is not None else query.get('block_headers', {})
    contract = date_contract(query, headers)
    return _build_sql(query, contract['start_date_utc'], contract['end_date_utc'])


def freeze(work, query):
    work=Path(work)
    headers=load_verified_block_headers(work)
    contract=date_contract(query,headers)
    sql=build(query,block_headers=headers)
    folder=work/'private/context_queries'/query['name']
    folder.mkdir(parents=True, exist_ok=False)
    (folder/'query.sql').write_text(sql,encoding='utf-8',newline='\n')
    digest=hashlib.sha256(sql.encode()).hexdigest()
    manifest={'schema_version':'stage1b-r4-context-query-v1','r3_context_scope':True,'kind':'context','query_id':query['query_id'],
              'sql_sha256':digest,'prepared_at_utc':datetime.now(timezone.utc).isoformat(),'source_graph':query['graph_identity'],
              'sql_logic_sha256':digest,'coverage_contract':contract,
              'account_windows':query['rows'],'candidate_expansion':False,'metasleuth_new_requests':0,
              'schema_sources':['https://docs.dune.com/data-catalog/evm/ethereum/raw/'+x for x in ('transactions','traces','blocks','withdrawals')],
              'schema_access':'Official public docs via web on 2026-09-07; compilation not claimed before execution',
              'coverage_requires_successful_full_export':True,'order_by_is_serialization_only':True,
              'export_plan':{'selection':'ALL_REQUIRED_NATIVE_CONTEXT_AND_SELECTED_TRANSACTION_TREES','columns':len(COLS),'page_limit':1000,
                             'result_size':'OBTAIN_AFTER_EXECUTION_BEFORE_EXPORT','fee_rate_evidence':'Inherited Plus trial Free20credits/decimalMB with conservative documented envelope',
                             'no_export_if_full_required_result_cannot_fit_shared100':True}}
    (folder/'freeze_manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    verify_frozen_scope(folder/'freeze_manifest.json',work,block_headers=headers)
    return folder/'freeze_manifest.json'


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work',type=Path,required=True)
    args=parser.parse_args()
    targets=json.loads((args.work/'derived/CONTEXT_TARGETS.json').read_text(encoding='utf-8-sig'))
    for query in targets['queries']:
        print(freeze(args.work,query))
