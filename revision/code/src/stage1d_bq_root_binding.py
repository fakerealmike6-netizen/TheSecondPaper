"""Explicit BQ empty-root equivalence from a full saved family and current RPC.

No network, raw mutation, internal-position inference or coverage certificate.
"""
from copy import deepcopy
from datetime import datetime,timezone
import hashlib,json,re
from pathlib import Path
from context_ledger_r3 import integer,trace_path,normalize_rows

SOURCES=[
 "https://raw.githubusercontent.com/blockchain-etl/ethereum-etl/develop/ethereumetl/mappers/trace_mapper.py",
 "https://raw.githubusercontent.com/blockchain-etl/ethereum-etl/develop/blockchainetl/exporters.py",
 "https://docs.cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv",
]


def _hex(value,size):
    if not isinstance(value,str) or not re.fullmatch('0x[0-9a-fA-F]{'+str(size)+'}',value):
        raise ValueError('Exact physical hexadecimal identity required')
    return value.lower()


def _bind(family,tx,receipt,header,evidence):
    """Pure implementation; public caller must first verify saved export and RPC."""
    rows=deepcopy(family)
    if not rows or len(rows)>100000 or not evidence:raise ValueError('Finite full transaction family and evidence required')
    nulls=[r for r in rows if r.get('trace_address') is None]
    if len(nulls)!=1:raise ValueError('Exactly one unresolved root field required')
    root=nulls[0]
    tx_hash=_hex(tx['hash'],64);block=integer(tx['blockNumber']);index=integer(tx['transactionIndex'])
    block_hash=_hex(tx['blockHash'],64)
    if (integer(header['number'])!=block or _hex(header['hash'],64)!=block_hash
        or _hex(receipt['transactionHash'],64)!=tx_hash or integer(receipt['blockNumber'])!=block
        or integer(receipt['transactionIndex'])!=index or _hex(receipt['blockHash'],64)!=block_hash):
        raise ValueError('Current tx receipt header physical binding conflict')
    hashes=header['transactions']
    if not isinstance(hashes,list) or len({_hex(h,64) for h in hashes})!=len(hashes) or index>=len(hashes) or hashes[index].lower()!=tx_hash:
        raise ValueError('Current complete header transaction ordering required')
    sender=_hex(tx['from'],40);recipient=_hex(tx['to'],40)
    if _hex(receipt['from'],40)!=sender or _hex(receipt['to'],40)!=recipient:
        raise ValueError('Current receipt endpoints conflict')
    amount=integer(tx['value']);integer(tx['gas']);integer(receipt['gasUsed']);integer(receipt['effectiveGasPrice'])
    if not isinstance(tx.get('input'),str) or not re.fullmatch('0x(?:[0-9a-fA-F]{2})*',tx['input']):
        raise ValueError('Strict current transaction calldata required')
    status=integer(receipt['status'])
    if status not in (0,1):raise ValueError('Strict current receipt status required')
    if (root.get('trace_type')!='call' or root.get('call_type')!='call' or root.get('error') and status!=0
        or root.get('success') is not bool(status) or root.get('from_address')!=sender
        or root.get('to_address')!=recipient or integer(root['value_raw'])!=amount):
        raise ValueError('Unique NULL row is not exact ordinary top call equivalent')
    paths={}
    stamp=integer(header['timestamp'])
    for row in rows:
        if (row.get('record_type')!='trace' or row['tx_hash']!=tx_hash
            or integer(row['block_number'])!=block or row['block_hash']!=block_hash or integer(row['tx_index'])!=index
            or type(row.get('success')) is not bool or row.get('success') is True and row.get('error')):
            raise ValueError('Full family transaction identity/status conflict')
        if int(datetime.fromisoformat(row['block_time'].replace('Z','+00:00')).timestamp())!=stamp:
            raise ValueError('Trace timestamp differs from current exact header')
        path=() if row is root else trace_path(row['trace_address'])
        if path is None or row is not root and not path or path in paths:
            raise ValueError('Missing/duplicate root or internal path')
        integer(row['value_raw']);integer(row['subtraces'])
        paths[path]=row
    for path,row in paths.items():
        if any(path[:length] not in paths for length in range(len(path))):
            raise ValueError('Missing actual internal ancestor')
        children={p[-1] for p in paths if len(p)==len(path)+1 and p[:-1]==path}
        if children!=set(range(integer(row['subtraces']))):
            raise ValueError('Whole tree child indices/counts not complete')
    original_sha=hashlib.sha256(json.dumps(root,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    root['trace_address']='[]'
    root['root_position_binding']={'status':'ROOT_EQUIVALENT','original_trace_address':None,
        'original_row_sha256':original_sha,'basis':'FULL_EXPORTED_TRANSACTION_FAMILY_AND_CURRENT_RPC_TOP_RECEIPT_HEADER',
        'source_interpretation':'ETL_EMPTY_LIST_CSV_EMPTY_FIELD_BQ_NULL_COMPATIBILITY','primary_sources':SOURCES,
        'internal_paths_modified':False,'evidence_ids':sorted(set(evidence))}
    for row in rows:
        row['evidence_ids']=sorted(set(row.get('evidence_ids',[])+evidence))
        row['tx_success']=bool(status)
    top=dict(tx,record_type='transaction',gas_used=receipt['gasUsed'],effective_gas_price=receipt['effectiveGasPrice'],
             success=bool(status),evidence_ids=sorted(set(evidence)))
    normalized=normalize_rows([top]+rows)
    if normalized['conflicts']:raise ValueError('Current strict normalizer rejects root equivalence')
    return {'status':'ROOT_EQUIVALENT_BOUND','rows':rows,'current_top_row':top,
            'root_binding':root['root_position_binding'],'full_context_claimed':False,'covered_ranges':0}


def bind_saved_export_root(work,state_path,spec_dependency,tx_hash):
    """Root-only read adapter. Reverify saved BQ chain and read current RPC cache."""
    import stage1d_bq_context_prepare as helper
    from stage1d_context_online import _cached_rpc
    try:
        work=Path(work).resolve();tx_hash=_hex(tx_hash,64)
        spec=helper.read(helper.checked(work,spec_dependency))
        tables={helper.TR,helper.TX} if spec['template']=='transaction_and_trace' else {helper.TR}
        fields=helper.schemas(work,spec['schema_evidence'],tables)
        expected_sql=helper.build_sql(spec['needed_ranges'],fields,spec['template'],spec['date_start_inclusive'],spec['date_end_exclusive'])
        actual_sql=helper.checked(work,{'path':spec['sql_path'],'sha256':spec['sql_sha256']}).read_text(encoding='utf-8')
        if expected_sql!=actual_sql:raise ValueError('Saved SQL is not the reviewed full touched transaction family')
        exported,bq_evidence=helper.verified_export(work,state_path,spec_dependency)
        family=[r for r in exported if r.get('tx_hash')==tx_hash and r.get('record_type')=='trace']
        if not family:raise ValueError('No current full exported trace family for exact hash')
        blocks={integer(row['block_number']) for row in family}
        if len(blocks)!=1:raise ValueError('Family block identity conflict')
        block=blocks.pop()
        plans=[{'method':'eth_getTransactionByHash','params':[tx_hash]},
               {'method':'eth_getTransactionReceipt','params':[tx_hash]},
               {'method':'eth_getBlockByNumber','params':[hex(block),False]}]
        cached=_cached_rpc(work,plans)
        if len(cached)!=3:raise ValueError('Current strict cached tx receipt header is incomplete')
        return _bind(family,cached[0][1],cached[1][1],cached[2][1],[bq_evidence]+[e for _,_,proof in cached for e in proof])
    except (ValueError,KeyError,TypeError,IndexError) as exc:
        return {'status':'ROOT_EQUIVALENCE_UNRESOLVED_GAP','reason':str(exc),'error_class':type(exc).__name__,
                'input_rows_unchanged':True,'covered_ranges':0,'full_context_claimed':False}


def _bind_verified_export_roots(work, spec_dependency, exported, bq_evidence):
    """Private seam: caller MUST have just completed helper.verified_export.

    Reproduce reviewed SQL once, index actual rows once, read the exact current
    point cache in one connection. No repeated whole-export read per transaction.
    The public single-family verifier above remains unchanged.
    """
    from collections import defaultdict
    import stage1d_bq_context_prepare as helper
    from stage1d_context_online import _cached_rpc
    work = Path(work).resolve()
    spec = helper.read(helper.checked(work, spec_dependency))
    tables = {helper.TR, helper.TX} if spec['template'] == 'transaction_and_trace' else {helper.TR}
    fields = helper.schemas(work, spec['schema_evidence'], tables)
    expected = helper.build_sql(spec['needed_ranges'], fields, spec['template'],
                                spec['date_start_inclusive'], spec['date_end_exclusive'])
    actual = helper.checked(work, {'path': spec['sql_path'], 'sha256': spec['sql_sha256']}).read_text(encoding='utf-8')
    if expected != actual:
        raise ValueError('Saved SQL is not the reviewed full touched transaction family')
    families = defaultdict(list)
    for row in exported:
        if row.get('record_type') == 'trace':
            families[row['tx_hash']].append(row)
    unresolved = {tx: rows for tx, rows in families.items() if any(r.get('trace_address') is None for r in rows)}
    needs, unique, gaps = {}, {}, []
    for tx, rows in sorted(unresolved.items()):
        blocks = {integer(row['block_number']) for row in rows}
        if len(blocks) != 1:
            raise ValueError('Actual transaction family has conflicting block identities')
        block = blocks.pop()
        requests = [{'method': 'eth_getTransactionByHash', 'params': [tx]},
                    {'method': 'eth_getTransactionReceipt', 'params': [tx]},
                    {'method': 'eth_getBlockByNumber', 'params': [hex(block), False]}]
        needs[tx] = requests
        unique.update({helper.digest(r): r for r in requests})
    found = {helper.digest(request): (value, evidence) for request, value, evidence
             in _cached_rpc(work, list(unique.values()))}
    replacements = {}
    for tx, requests in needs.items():
        missing = [r for r in requests if helper.digest(r) not in found]
        if missing:
            gaps.append({'type': 'ROOT_EQUIVALENCE_UNRESOLVED_GAP', 'tx_hash': tx,
                         'reason': 'CURRENT_EXACT_TX_RECEIPT_HEADER_MISSING', 'needed_rpc': missing})
            continue
        values = [found[helper.digest(r)] for r in requests]
        evidence = [bq_evidence] + [e for _, proof in values for e in proof]
        try:
            result = _bind(unresolved[tx], values[0][0], values[1][0], values[2][0], evidence)
            replacements[tx] = result['rows']
        except (ValueError, KeyError, TypeError, IndexError) as exc:
            gaps.append({'type': 'ROOT_EQUIVALENCE_UNRESOLVED_GAP', 'tx_hash': tx,
                         'reason': str(exc), 'error_class': type(exc).__name__})
    rows = [deepcopy(row) for row in exported if not
            (row.get('record_type') == 'trace' and row.get('tx_hash') in replacements)]
    rows.extend(row for tx in sorted(replacements) for row in replacements[tx])
    return {'rows': rows, 'gaps': gaps, 'full_context_claimed': False}
