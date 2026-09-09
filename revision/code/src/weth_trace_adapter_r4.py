"""Offline Dune request-chain and unique EVM log-emitter proof for fixed WETH.

No RPC trace is manufactured. The archived query selects every transaction
trace, including failed, zero-value and non-CALL rows; unsupported executions
are rejected. Derived frames keep logs empty. A separate proof uses complete
CALL/DELEGATECALL storage contexts and the complete request-bound receipt.
"""
from __future__ import annotations
import argparse
import copy
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re

from weth_source_adapter_r3 import indexed_tree, read, write, digest, WETH
from weth_component import DEPOSIT_TOPIC

SCHEMA = 'stage1b-r4-weth-trace-input-v1'
SOURCE_TYPE = 'DUNE_ARCHIVED_SQL_COMPLETE_TRANSACTION_TRACE'


def exact(value):
    if isinstance(value, (bool, float)) or value is None:
        raise ValueError('Exact integer evidence required')
    if isinstance(value, str) and not re.fullmatch(r'(?:0x[0-9a-fA-F]+|[0-9]+)', value):
        raise ValueError('Exact nonnegative integer required')
    answer = int(value, 16) if isinstance(value, str) and value.startswith('0x') else int(value)
    if answer < 0:
        raise ValueError('Negative integer evidence')
    return answer


def normalized_sql(sql):
    return re.sub(r'\s+', ' ', re.sub(r'--[^\n]*', '', sql)).strip().lower()


def validate_policy(policy):
    if (not re.fullmatch(r'0x[0-9a-f]{64}', policy.get('tx_hash', ''))
            or policy.get('contract') != WETH
            or not re.fullmatch(r'0x[0-9a-f]{40}', policy.get('credited_address', ''))
            or type(policy.get('block_number')) is not int or policy['block_number'] < 0
            or type(policy.get('deposit_log_index')) is not int or policy['deposit_log_index'] < 0
            or not isinstance(policy.get('input_trace_address'), list)
            or any(type(n) is not int or n < 0 for n in policy['input_trace_address'])):
        raise ValueError('Exact fixed component identities required')
    exact(policy['amount_raw'])


def expected_sql(scope):
    return f'''SELECT block_number, CAST(block_time AS VARCHAR) AS block_time,
CONCAT('0x', LOWER(TO_HEX(block_hash))) AS block_hash,
CONCAT('0x', LOWER(TO_HEX(tx_hash))) AS tx_hash, tx_index, trace_address,
sub_traces, type, call_type, success, tx_success, error,
CONCAT('0x', LOWER(TO_HEX("from"))) AS from_address,
CONCAT('0x', LOWER(TO_HEX("to"))) AS to_address,
CONCAT('0x', LOWER(TO_HEX(refund_address))) AS refund_address,
CAST(value AS VARCHAR) AS value_raw, CAST(gas AS VARCHAR) AS gas,
CAST(gas_used AS VARCHAR) AS gas_used,
CONCAT('0x', LOWER(TO_HEX("input"))) AS input_data,
CONCAT('0x', LOWER(TO_HEX("output"))) AS output_data
FROM ethereum.traces WHERE block_date = DATE '{scope['block_date_utc']}'
AND block_number = {scope['block_number']} AND tx_hash = {scope['tx_hash']}
ORDER BY trace_address'''


def validate_dune_bundle(manifest_path, policy):
    validate_policy(policy)
    manifest_path = Path(manifest_path).resolve()
    manifest = read(manifest_path)
    if manifest.get('schema_version') != SCHEMA:
        raise ValueError('Unsupported fixed Dune trace manifest')
    required = {'job', 'freeze', 'sql', 'submit', 'submit_receipt', 'status',
                'page', 'page_raw', 'page_receipt', 'cached_internal_excerpt', 'schema_evidence'}
    if set(manifest.get('files', {})) != required:
        raise ValueError('Exact Dune request-chain closure required')
    data = {}
    targets = set()
    for role, item in manifest['files'].items():
        path = Path(item['path'])
        file = (manifest_path.parent / path).resolve()
        if path.is_absolute() or not file.is_relative_to(manifest_path.parent) or file in targets:
            raise ValueError('Unsafe or duplicate Dune input path')
        targets.add(file)
        data[role] = file.read_bytes()
        if digest(data[role]) != item['sha256'] or len(data[role]) != item['bytes']:
            raise ValueError('Dune archived evidence hash/size changed: ' + role)
    return validate_dune_documents(manifest,data,policy,digest(manifest_path.read_bytes()))


def validate_dune_documents(manifest, data, policy, manifest_sha256):
    """Identical scientific checks over original role bytes, usable in a package."""
    from page_contract import initial_progress, validate_page
    validate_policy(policy)
    required={'job','freeze','sql','submit','submit_receipt','status','page','page_raw','page_receipt','cached_internal_excerpt','schema_evidence'}
    if manifest.get('schema_version')!=SCHEMA or set(manifest.get('files',{}))!=required or set(data)!=required:
        raise ValueError('Exact complete Dune closure required')
    payloads,identities={},{}
    for role,item in manifest['files'].items():
        if not isinstance(data[role],bytes) or digest(data[role])!=item['sha256'] or len(data[role])!=item['bytes']:
            raise ValueError('Dune original role bytes changed: '+role)
        identities[role]={'sha256':item['sha256'],'bytes':item['bytes'],'path':item['path']}
        payloads[role]=data[role].decode('utf-8') if role=='sql' else json.loads(data[role])
    job, freeze = payloads['job'], payloads['freeze']
    scope = freeze.get('fixed_scope', {})
    if not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', scope.get('block_date_utc', '')):
        raise ValueError('Exact frozen UTC date required')
    for key in ('tx_hash', 'block_number', 'contract', 'credited_address', 'amount_raw'):
        if scope.get(key) != policy.get(key):
            raise ValueError('Frozen Dune scope differs from fixed component: ' + key)
    if (scope.get('chain_id') != 1 or scope.get('all_call_tree_rows') is not True
            or freeze.get('schema_version') != 'r2-fixed-context-batch-1'
            or freeze.get('no_sql_limit') is not True or job.get('kind') != 'context'):
        raise ValueError('Complete fixed transaction scope required')
    sql_sha = digest(data['sql'])
    if (any(freeze.get(k) != sql_sha for k in ('sql_sha256', 'full_sql_sha256', 'query_file_sha256'))
            or job.get('sql_sha256') != sql_sha
            or job.get('scope_freeze_sha256') != digest(data['freeze'])):
        raise ValueError('Archived SQL/job/freeze identity mismatch')
    for filename, role in [('query.sql', 'sql'), ('cached_internal_excerpt.json', 'cached_internal_excerpt'), ('schema_evidence.json', 'schema_evidence')]:
        if freeze.get('input_files', {}).get(filename) != digest(data[role]):
            raise ValueError('Frozen original input differs: ' + role)
    if normalized_sql(payloads['sql']) != normalized_sql(expected_sql(scope)):
        raise ValueError('SQL must select every type/status/value row of the one exact transaction')
    execution = job.get('execution_id')
    if not execution or job.get('export_offsets') != [0] or job.get('export_requests') != 1:
        raise ValueError('This finite complete one-page archived trace is required')
    def wire(receipt, role, operation):
        if (receipt.get('http_status') != 200 or receipt.get('error_class')
                or receipt.get('operation') != operation or not receipt.get('request_id')
                or receipt.get('sha256') != digest(data[role])
                or receipt.get('raw_bytes') != len(data[role])
                or receipt.get('execution_id') not in (None, execution)):
            raise ValueError('Actual Dune response receipt does not bind: ' + role)
    if job.get('submit_receipt') != payloads['submit_receipt']:
        raise ValueError('Job submit receipt differs from original receipt')
    wire(payloads['submit_receipt'], 'submit', 'execute')
    wire(job['status_receipt'], 'status', 'status')
    wire(payloads['page_receipt'], 'page_raw', 'results')
    if payloads['page_raw'] != payloads['page']:
        raise ValueError('Saved indexed page differs from original wire response')
    def status_identity(value):
        value = dict(value)
        # The collector serializes its parsed Decimal fee as text. Only this
        # known representation change is normalized, with no fields dropped.
        if 'execution_cost_credits' in value:
            value['execution_cost_credits'] = Decimal(str(value['execution_cost_credits']))
        return value
    if (payloads['submit'].get('execution_id') != execution
            or status_identity(payloads['status']) != status_identity(job.get('status_response', {}))
            or payloads['status'].get('execution_id') != execution
            or payloads['status'].get('state') != 'QUERY_STATE_COMPLETED'):
        raise ValueError('Dune execution/completion identity mismatch')
    page_receipt = payloads['page_receipt']
    params = page_receipt.get('parameters')
    if params != {'offset': 0, 'limit': 1000}:
        raise ValueError('Original whole-result page parameters required')
    progress = validate_page(payloads['page'], execution_id=execution, offset=0,
        limit=1000, progress=initial_progress(), status_metadata=payloads['status']['result_metadata'],
        receipt=page_receipt, parameters=params)
    if not progress['complete']:
        raise ValueError('Entire indexed transaction was not exported')
    rows = payloads['page']['result']['rows']
    for row in rows:
        # Missing/unsupported create, selfdestruct, callcode or unknown type is
        # a real proof gap, never a row silently removed before tree closure.
        if row.get('type') != 'call' or row.get('call_type') not in {'call', 'delegatecall'}:
            raise ValueError('Unsupported execution type cannot prove log-emitter closure')
        if str(row.get('block_time', ''))[:10] != scope['block_date_utc']:
            raise ValueError('Frozen UTC date differs from actual indexed block time')
        exact(row['value_raw']); exact(row['tx_index'])
        if any(type(n) is not int or n < 0 for n in row.get('trace_address', [])) or type(row.get('sub_traces')) is not int:
            raise ValueError('Exact indexed positions required')
    tree = indexed_tree(rows, policy)
    return {'rows': rows, 'tree': tree, 'scope': scope, 'execution_id': execution,
        'sql_sha256': sql_sha, 'manifest_sha256': manifest_sha256,
        'source_files': identities, 'page_contract': progress,
        'request_binding_basis': 'Unchanged original frozen SQL, collector job and execute/status/result receipts; no original HTTP wire-body or RPC trace claimed'}


def prove_unique_emitter(policy, rows, transaction, receipt, source_review):
    """Prove the emitter's unique execution context, never populate frame.logs.

This structural proof alone grants no trust. Only the sealed acquisition path
uses it after validating all rows and actual receipt/source request bindings.
"""
    validate_policy(policy)
    indexed_tree(rows, policy)  # full child/ancestor/success checks, no filtering
    if not source_review.get('source_validated') or source_review.get('semantics') != 'CANONICAL_WETH9_DEPOSIT_NO_FEE_NO_REFUND':
        raise ValueError('Independent verified historical source semantics required')
    contract, tx_hash = policy['contract'].lower(), policy['tx_hash'].lower()
    if contract != WETH:
        raise ValueError('Only fixed canonical WETH supported')
    root = next(row for row in rows if row['trace_address'] == [])
    if (transaction.get('hash', '').lower() != tx_hash or receipt.get('transactionHash', '').lower() != tx_hash
            or exact(transaction['blockNumber']) != policy['block_number']
            or exact(receipt['blockNumber']) != policy['block_number']
            or exact(receipt['status']) != 1 or exact(transaction['chainId']) != 1):
        raise ValueError('Successful mainnet transaction/receipt identity required')
    block_hash = transaction['blockHash'].lower()
    tx_index = exact(transaction['transactionIndex'])
    if (receipt['blockHash'].lower() != block_hash or exact(receipt['transactionIndex']) != tx_index
            or root['call_type'] != 'call' or root['from_address'].lower() != transaction['from'].lower()
            or root['to_address'].lower() != transaction['to'].lower()
            or exact(root['value_raw']) != exact(transaction['value'])
            or root['input_data'].lower() != transaction['input'].lower()):
        raise ValueError('Indexed root differs from actual transaction')
    contexts, context_rows = {}, []
    for row in sorted(rows, key=lambda r: (len(r['trace_address']), r['trace_address'])):
        path, call_type = tuple(row['trace_address']), row['call_type']
        if row.get('type') != 'call' or call_type not in {'call', 'delegatecall'}:
            raise ValueError('Unsupported execution context type')
        if (row['block_hash'].lower() != block_hash or exact(row['tx_index']) != tx_index
                or row['tx_hash'].lower() != tx_hash):
            raise ValueError('Indexed physical identity conflict')
        if path and row['from_address'].lower() != contexts[path[:-1]]:
            raise ValueError('Child caller does not match parent execution context')
        address = row['to_address'].lower() if call_type == 'call' else contexts[path[:-1]] if path else None
        if address is None:
            raise ValueError('Root must have a direct CALL context')
        contexts[path] = address
        context_rows.append({'path': list(path), 'call_type': call_type,
            'code_address': row['to_address'].lower(), 'storage_and_log_emitter_address': address})
    weth_contexts = [r for r in context_rows if r['storage_and_log_emitter_address'] == contract]
    weth_code = [r for r in context_rows if r['code_address'] == contract]
    if len(weth_contexts) != 1 or len(weth_code) != 1 or weth_contexts != weth_code:
        raise ValueError('Canonical WETH execution context is not strictly unique')
    target_path = weth_contexts[0]['path']
    if target_path != policy['input_trace_address']:
        raise ValueError('Canonical emitter context is the wrong fixed frame')
    target = next(r for r in rows if r['trace_address'] == target_path)
    if (target['call_type'] != 'call' or target['from_address'].lower() != policy['credited_address'].lower()
            or exact(target['value_raw']) != exact(policy['amount_raw'])
            or target['input_data'].lower() not in {'0x', '0xd0e30db0'} or target['sub_traces'] != 0):
        raise ValueError('Fixed successful leaf deposit entrypoint does not match')
    logs = receipt.get('logs')
    if not isinstance(logs, list):
        raise ValueError('Complete receipt log array required')
    indices = []
    for log in logs:
        index = exact(log['logIndex']); indices.append(index)
        if (log.get('removed') is not False or log.get('transactionHash', '').lower() != tx_hash
                or log.get('blockHash', '').lower() != block_hash or exact(log['blockNumber']) != policy['block_number']
                or exact(log['transactionIndex']) != tx_index):
            raise ValueError('Receipt log identity/removal conflict')
    if len(set(indices)) != len(indices) or indices != sorted(indices):
        raise ValueError('Duplicate/unordered exact receipt log positions')
    emitted = [log for log in logs if log.get('address', '').lower() == contract]
    if len(emitted) != 1:
        raise ValueError('Repeated or missing canonical WETH log cannot map uniquely')
    deposit = emitted[0]
    if (exact(deposit['logIndex']) != policy['deposit_log_index']
            or deposit.get('topics') != [DEPOSIT_TOPIC, '0x' + '0' * 24 + policy['credited_address'][2:].lower()]
            or exact(deposit['data']) != exact(policy['amount_raw'])):
        raise ValueError('Unique emitter log differs from exact fixed Deposit')
    return {'schema_version': 'stage1b-r4-unique-evm-log-emitter-proof-v1',
        'proof_valid': True, 'basis': 'COMPLETE_REQUEST_BOUND_TREE_UNIQUE_STORAGE_LOG_EMITTER_AND_COMPLETE_RECEIPT',
        'tx_hash': tx_hash, 'block_number': policy['block_number'], 'block_hash': block_hash,
        'tx_index': tx_index, 'actual_call_tree_path': target_path, 'log_index': policy['deposit_log_index'],
        'caller': target['from_address'].lower(), 'contract': contract,
        'amount_raw': str(exact(policy['amount_raw'])), 'input': target['input_data'],
        'frame_count': len(rows), 'receipt_log_count': len(logs), 'canonical_weth_log_count': 1,
        'execution_contexts': context_rows, 'frame_logs_fabricated': False,
        'runtime_code_sha256': source_review['runtime_code_sha256'],
        'source_text_sha256': source_review['source_text_sha256'],
        'proof_rule': 'EVM LOG uses the current execution-context address; CALL changes it to callee, DELEGATECALL preserves parent context. All frames are present and successful; the only canonical WETH context is the leaf. The one canonical WETH receipt log must therefore originate in that leaf.',
        'scope': 'Only this isolated deposit; surrounding protocol and cross-chain behavior remain uncertified'}


def native_rows(rows):
    return {'status': 'SUCCESS_VALIDATED_INDEXED_CALLS', 'evidence_provider': SOURCE_TYPE,
        'result': [{'from': r['from_address'], 'to': r['to_address'], 'value': r['value_raw'],
            'isError': '0', 'blockNumber': str(r['block_number']), 'blockHash': r['block_hash'],
            'hash': r['tx_hash'], 'trace_address': r['trace_address']}
            for r in rows if r['trace_address'] and r['call_type'] == 'call']}


def replay_weth_r4(input_root, output):
    from weth_source_adapter_r3 import replay_weth_manifest
    from weth_evidence import load_evidence_context, extend_dune_evidence_context
    from weth_component import verify_component
    input_root, output = Path(input_root).resolve(), Path(output).resolve()
    config = read(input_root / 'R4_INPUT_MANIFEST.json')
    if config.get('schema_version') != 'stage1b-r4-weth-replay-v1' or set(config) != {'schema_version', 'r3_inputs', 'dune_trace_manifest'}:
        raise ValueError('Unsupported R4 WETH replay selection')
    selected = {}
    for role in ('r3_inputs', 'dune_trace_manifest'):
        item = config[role]; path = (input_root / item['path']).resolve()
        if Path(item['path']).is_absolute() or not path.is_relative_to(input_root) or digest(path.read_bytes()) != item['sha256']:
            raise ValueError('R4 input selection identity changed')
        selected[role] = path
    baseline = replay_weth_manifest(selected['r3_inputs'].parent, output / 'r3_baseline_replay')
    context = load_evidence_context(output / 'r3_baseline_replay/EVIDENCE_BUNDLE.json', output / 'r3_baseline_replay/ACQUISITION_CATALOGUE.json')
    r3_manifest = read(selected['r3_inputs'])
    policy = read(selected['r3_inputs'].parent / r3_manifest['files']['policy']['path'])['weth_component']
    context = extend_dune_evidence_context(context, selected['dune_trace_manifest'], policy)
    old_analysis = read(selected['r3_inputs'].parent / r3_manifest['files']['r2_trace_analysis']['path'])
    archive = context.records['trace']['archive_binding']
    if (archive['source_files']['job']['sha256'] != old_analysis['job_file_sha256']
            or archive['source_files']['freeze']['sha256'] != old_analysis['freeze_manifest_sha256']
            or archive['sql_sha256'] != old_analysis['sql_sha256']
            or archive['execution_id'] != old_analysis['execution_id']
            or context.records['trace']['payload'] != read(output/'r3_baseline_replay/INDEXED_CALL_TREE_WITHOUT_LOGS.json')):
        raise ValueError('New adapter must retain the exact original R2 trace source and tree')
    result = verify_component(policy, context.records['transaction']['payload'], context.records['receipt']['payload'],
        context.records['internal']['payload'], call_trace=context.records['trace']['payload'],
        historical_code=context.records['historical_code']['payload'], evidence_context=context)
    result['native_locator']['legacy_event_id'] = baseline['native_locator']['legacy_event_id']
    result['native_locator']['current_native_input_provider'] = SOURCE_TYPE
    result['native_locator']['current_physical_path'] = policy['input_trace_address']
    result['native_locator']['legacy_row_ordinal_does_not_supply_current_path'] = True
    result.update({'schema_version': 'stage1b-r4-weth-result-v1', 'checks_passed': sum(result['checks'].values()),
        'checks_total': len(result['checks']), 'source_review': context.source_review,
        'index_adapter_provenance': SOURCE_TYPE + '; derived tree retains empty frame.logs, no successful RPC trace claimed',
        'source_request_bound_internal_available': True,
        'legacy_internal_binding': 'Original explorer response and row ordinal remain unchanged and unbound to an original request. Current native input is independently derived from the validated Dune CALL row.',
        'legacy_native_locator': baseline['native_locator'],
        'trace_rpc_attempt': baseline['trace_rpc_attempt'], 'fixed_block_identity': baseline['fixed_block_identity'],
        'equivalent_deposit_binding': context.source_review.get('equivalent_deposit_binding'),
        'equivalent_binding_gap': context.source_review.get('equivalent_binding_gap'),
        'r3_baseline_status': baseline['status'], 'r3_baseline_checks_passed': baseline['checks_passed'],
        'real_conversion_enabled': result['real_component_certified'], 'new_network_requests': 0,
        'external_acceptance_status': 'PENDING_REVIEW'})
    # Source/runtime is already verified even if a distinct binding gap remains.
    if not result['real_component_certified']:
        result['semantic_unit']['component_fee_port']['basis'] = 'Verified source has no deposit fee; component activation awaits remaining evidence predicates'
    write(output / 'WETH_COMPONENT_RESULT.json', result)
    write(output / 'DUNE_REQUEST_CHAIN_BINDING.json', context.records['trace']['archive_binding'])
    write(output / 'INDEXED_CALL_TREE_WITHOUT_LOGS.json', context.records['trace']['payload'])
    write(output / 'DUNE_NATIVE_INPUT_ROWS.json', context.records['internal']['payload'])
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = replay_weth_r4(args.input_root, args.output)
    print(json.dumps({k: result[k] for k in ('status', 'checks_passed', 'checks_total', 'real_component_certified', 'gaps')}, indent=2))


if __name__ == '__main__':
    main()
