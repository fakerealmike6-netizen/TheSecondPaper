"""Independent offline check of the fixed WETH five-member original RPC body.

This checks material provenance only. It neither imports nor modifies the WETH
component's twelve certification predicates. The CLI is pinned to the accepted
real transaction/body; synthetic tests exercise the same core with explicit
synthetic identities, without placing private evidence in the public package.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re

MANIFEST = 'derived/weth_shared_wire_r4_r1/manifest.json'
SCHEMA = 'stage1b-r4-r1-weth-shared-wire-v1'
FIXED = {
    'original_body_sha256': 'c22430d52fd472458dc477c28a862986b0e2b54d2d4971770f78287033d006e7',
    'chain_id': 1,
    'tx_hash': '0xfd9f05f2eb3673eebfd4c2b3af50fa8503a3486a5c7f00e92bf5213d774f6b9c',
    'block_number': 20582941,
    'block_hash': '0x8dd954796c85378c2d10e4c67ce1d571ee00516d8b2a9632e49ddcebc058c9b3',
    'transaction_index': 66,
    'weth_contract': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    'runtime_code_sha256': '5566bf50796faf93c9b6f6adacd3b32c70bfe16b48ffc59db6cd144cbdc89739',
    'runtime_code_bytes': 3124,
    'transaction_value_raw': '150000000000000000000',
}
ROLES = ('trace_attempt', 'historical_code', 'block', 'transaction', 'receipt')


class WireValidationError(ValueError):
    pass


def require(condition, message):
    if not condition: raise WireValidationError(message)


def sha(raw): return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'DUPLICATE_JSON_OBJECT_KEY:' + key)
            result[key] = value
        return result
    def bad_constant(value): raise WireValidationError('NONFINITE_JSON_CONSTANT')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=bad_constant)


def uint(value, name):
    if type(value) is int and value >= 0: return value
    if isinstance(value, str) and re.fullmatch(r'0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)', value): return int(value, 16)
    raise WireValidationError('EXACT_UNSIGNED_VALUE_REQUIRED:' + name)


def id_key(value):
    require(type(value) in (str, int) and (type(value) is str and bool(value) or type(value) is int and value >= 0), 'INVALID_RPC_ID')
    return type(value).__name__, value


def by_id(rows, kind):
    require(isinstance(rows, list) and len(rows) == 5 and all(isinstance(r, dict) for r in rows), kind + '_MUST_HAVE_FIVE_MEMBERS')
    result = {}
    for row in rows:
        key = id_key(row.get('id'))
        require(key not in result, kind + '_DUPLICATE_RPC_ID')
        require(row.get('jsonrpc') == '2.0', kind + '_JSONRPC_VERSION')
        result[key] = row
    return result


def no_auth_fields(value):
    forbidden = {'authorization', 'api_key', 'apikey', 'x_api_key', 'x_dune_api_key',
                 'cookie', 'set_cookie', 'password', 'secret', 'access_token', 'refresh_token',
                 'private_key', 'permission_sha256'}
    if isinstance(value, dict):
        for key, child in value.items():
            require(key.lower().replace('-', '_') not in forbidden, 'AUTHENTICATION_FIELD_NOT_REDACTED:' + key)
            no_auth_fields(child)
    elif isinstance(value, list):
        for child in value: no_auth_fields(child)


def artifact(tree, descriptor):
    require(isinstance(descriptor, dict), 'ARTIFACT_DESCRIPTOR_REQUIRED')
    name = descriptor.get('path')
    require(isinstance(name, str) and name and '\\' not in name and ':' not in name, 'INVALID_ARTIFACT_PATH')
    rel = PurePosixPath(name)
    require(not rel.is_absolute() and '..' not in rel.parts, 'ARTIFACT_PATH_ESCAPE')
    root = Path(tree).resolve(); path = root.joinpath(*rel.parts)
    require(path.resolve().is_relative_to(root), 'ARTIFACT_PATH_ESCAPE')
    for parent in [path, *list(path.parents)[:len(rel.parts)]]:
        require(not parent.is_symlink(), 'ARTIFACT_LINK_NOT_ALLOWED')
    require(path.is_file(), 'MISSING_PRIVATE_ARTIFACT:' + name)
    raw = path.read_bytes()
    require(type(descriptor.get('bytes')) is int and descriptor['bytes'] == len(raw), 'ARTIFACT_BYTE_COUNT_MISMATCH:' + name)
    require(descriptor.get('sha256') == sha(raw), 'ARTIFACT_SHA256_MISMATCH:' + name)
    return raw


def expected_plans(fixed):
    tx, block = fixed['tx_hash'], hex(fixed['block_number'])
    return {
        'trace_attempt': ('debug_traceTransaction', [tx, {'tracer': 'callTracer', 'tracerConfig': {'withLog': True}}]),
        'historical_code': ('eth_getCode', [fixed['weth_contract'], block]),
        'block': ('eth_getBlockByNumber', [block, False]),
        'transaction': ('eth_getTransactionByHash', [tx]),
        'receipt': ('eth_getTransactionReceipt', [tx]),
    }


def verify_document(tree, manifest, *, fixed):
    """Validate a supplied closure; fixed is explicit for public synthetic tests.

    The command-line wrapper always supplies the hard-coded real FIXED identity.
    Callers must not treat synthetic test identities as real-chain attestation.
    """
    require(isinstance(manifest, dict) and manifest.get('schema_version') == SCHEMA, 'MANIFEST_SCHEMA')
    require(manifest.get('fixed_identity') == fixed, 'FIXED_WETH_IDENTITY_CHANGED')
    no_auth_fields(manifest)
    if manifest.get('material_status') == 'MISSING_ORIGINAL_BODY':
        require(manifest.get('original_body_available') is False and isinstance(manifest.get('gap_reason'), str) and bool(manifest['gap_reason'].strip()), 'EXPLICIT_MATERIAL_GAP_REQUIRED')
        require(not manifest.get('artifacts'), 'MISSING_STATUS_CONTRADICTS_SUPPLIED_BODY')
        return {'status': 'PASS', 'material_status': 'MISSING_ORIGINAL_BODY', 'original_body_available': False,
                'original_body_sha256': fixed['original_body_sha256'], 'members_verified': 0, 'network_requests': 0,
                'scope': 'Checks explicit gap reporting only; original wire has NOT been verified',
                'gap_reason': manifest['gap_reason'], 'weth_certification_predicates_changed': False}
    require(manifest.get('material_status') == 'AVAILABLE_ORIGINAL_BODY' and manifest.get('original_body_available') is True, 'UNKNOWN_MATERIAL_STATUS')
    items = manifest.get('artifacts')
    require(isinstance(items, dict) and set(items) == {'original_body', 'requests_redacted', 'batch_receipt_redacted', *ROLES}, 'EXACT_ARTIFACT_CLOSURE_REQUIRED')
    require(len({x.get('path') for x in items.values()}) == len(items), 'DUPLICATE_ARTIFACT_TARGET')
    raw = artifact(tree, items['original_body'])
    require(sha(raw) == fixed['original_body_sha256'], 'ORIGINAL_BODY_NOT_FIXED_EXPECTED_BYTES')
    responses = by_id(strict_json(raw), 'RESPONSE')
    request_view = strict_json(artifact(tree, items['requests_redacted']))
    receipt_view = strict_json(artifact(tree, items['batch_receipt_redacted']))
    no_auth_fields(request_view); no_auth_fields(receipt_view); no_auth_fields(list(responses.values()))
    for view, kind in ((request_view, 'request_identity'), (receipt_view, 'batch_receipt')):
        redaction = view.get('redaction')
        require(isinstance(redaction, dict) and redaction.get('kind') == 'DERIVED_REDACTED_VIEW' and redaction.get('not_original_bytes') is True, kind + '_REDACTION_PROVENANCE_REQUIRED')
        require(isinstance(redaction.get('source_original_sha256'), str) and re.fullmatch('[0-9a-f]{64}', redaction['source_original_sha256']), kind + '_SOURCE_HASH_REQUIRED')
        require(items[kind == 'batch_receipt' and 'batch_receipt_redacted' or 'requests_redacted'].get('source_original_sha256') == redaction['source_original_sha256'], kind + '_SOURCE_HASH_DISAGREEMENT')
    requests = by_id(request_view.get('requests'), 'REQUEST')
    require(set(requests) == set(responses), 'REQUEST_RESPONSE_ID_SET_MISMATCH')
    require(uint(request_view.get('rpc_operations'), 'rpc_operations') == 5, 'REQUEST_OPERATION_COUNT_MISMATCH')
    require(receipt_view.get('http_status') == 200 and receipt_view.get('error_class') is None, 'BATCH_HTTP_INCOMPLETE')
    require(receipt_view.get('raw_sha256') == sha(raw) and receipt_view.get('raw_bytes') == len(raw), 'BATCH_ORIGINAL_WIRE_BINDING')
    require(uint(receipt_view.get('rpc_operations_actual'), 'rpc_operations_actual') == 5, 'BATCH_OPERATION_COUNT_MISMATCH')
    require(isinstance(request_view.get('utc'), str) and isinstance(receipt_view.get('utc'), str), 'ORIGINAL_TIMESTAMPS_REQUIRED')
    require(request_view.get('utc') == manifest.get('original_dispatch_utc') and receipt_view.get('utc') == manifest.get('original_receipt_utc'), 'ORIGINAL_TIMESTAMPS_CHANGED')
    members = receipt_view.get('members')
    require(isinstance(members, list) and len(members) == 5, 'BATCH_MEMBER_COUNT_MISMATCH')
    member_by_sha = {m.get('artifact_sha256'): m for m in members if isinstance(m, dict)}
    require(len(member_by_sha) == 5, 'BATCH_MEMBER_HASH_DUPLICATE_OR_INVALID')
    used_ids, values, summary = set(), {}, []
    for role in ROLES:
        desc = items[role]; envelope = strict_json(artifact(tree, desc)); no_auth_fields(envelope)
        request, response = envelope.get('request'), envelope.get('response')
        require(isinstance(request, dict) and isinstance(response, dict), role + '_SPLIT_ENVELOPE_REQUIRED')
        key = id_key(request.get('id'))
        require(key in requests and requests[key] == request and key not in used_ids, role + '_SPLIT_REQUEST_IDENTITY')
        require(responses.get(key) == response, role + '_SPLIT_RESPONSE_DIFFERS_FROM_ORIGINAL_BODY')
        used_ids.add(key)
        method, params = expected_plans(fixed)[role]
        require(request.get('method') == method and request.get('params') == params, role + '_FIXED_HISTORICAL_SELECTOR')
        require(envelope.get('raw_body_sha256') == sha(raw) and envelope.get('http_status') == 200 and envelope.get('response_complete') is True, role + '_SPLIT_WIRE_BINDING')
        require(('result' in response) != ('error' in response), role + '_EXCLUSIVE_RESULT_OR_ERROR')
        state = 'RPC_ERROR' if 'error' in response else 'SUCCESS_VALIDATED'
        require(envelope.get('status') == state, role + '_MEMBER_STATUS_MISMATCH')
        member = member_by_sha.get(desc['sha256'])
        require(member is not None and member.get('method') == method and member.get('status') == state, role + '_BATCH_MEMBER_BINDING')
        require(desc.get('original_artifact_path') == member.get('artifact_path'), role + '_ORIGINAL_SPLIT_PATH_BINDING')
        if state == 'RPC_ERROR':
            error = response['error']
            require(isinstance(error, dict) and type(error.get('code')) is int and isinstance(error.get('message'), str), role + '_MALFORMED_RPC_ERROR')
            require(role == 'trace_attempt', 'NECESSARY_IDENTITY_MEMBER_FAILED:' + role)
        else:
            require(response.get('result') is not None, role + '_NULL_RESULT')
            values[role] = response['result']
        summary.append({'role': role, 'request_id': request['id'], 'method': method, 'status': state,
                        'split_sha256': desc['sha256'], 'request_response_equal_by_id': True})
    require(used_ids == set(responses), 'UNVERIFIED_EXTRA_RPC_MEMBER')
    tx, receipt, block, code = (values.get(role) for role in ('transaction', 'receipt', 'block', 'historical_code'))
    require(all(isinstance(v, dict) for v in (tx, receipt, block)), 'CHAIN_IDENTITY_OBJECTS_REQUIRED')
    require(tx.get('hash') == fixed['tx_hash'] and receipt.get('transactionHash') == fixed['tx_hash'], 'FIXED_TRANSACTION_HASH')
    require(uint(tx.get('chainId'), 'chainId') == fixed['chain_id'], 'CHAIN_ID_MISMATCH')
    require(uint(tx.get('value'), 'transaction.value') == int(fixed['transaction_value_raw']), 'TRANSACTION_VALUE_MISMATCH')
    for value, kind in ((tx, 'transaction'), (receipt, 'receipt')):
        require(uint(value.get('blockNumber'), kind + '.blockNumber') == fixed['block_number'] and value.get('blockHash') == fixed['block_hash'], 'FIXED_BLOCK_IDENTITY:' + kind)
        require(uint(value.get('transactionIndex'), kind + '.transactionIndex') == fixed['transaction_index'], 'FIXED_TRANSACTION_INDEX:' + kind)
    require(receipt.get('from') == tx.get('from') and receipt.get('to') == tx.get('to'), 'TRANSACTION_RECEIPT_ENDPOINTS')
    require(uint(receipt.get('status'), 'receipt.status') == 1, 'FIXED_TRANSACTION_NOT_SUCCESSFUL')
    require(uint(block.get('number'), 'block.number') == fixed['block_number'] and block.get('hash') == fixed['block_hash'], 'HISTORICAL_BLOCK_IDENTITY')
    timestamp = uint(block.get('timestamp'), 'block.timestamp')
    if 'blockTimestamp' in tx: require(uint(tx['blockTimestamp'], 'transaction.blockTimestamp') == timestamp, 'TRANSACTION_BLOCK_TIME_MISMATCH')
    transactions = block.get('transactions')
    require(isinstance(transactions, list) and len(transactions) > fixed['transaction_index'] and transactions[fixed['transaction_index']] == fixed['tx_hash'], 'BLOCK_TRANSACTION_POSITION_MISMATCH')
    logs = receipt.get('logs'); require(isinstance(logs, list), 'RECEIPT_LOGS_REQUIRED')
    log_positions = set()
    for log in logs:
        require(isinstance(log, dict) and log.get('transactionHash') == fixed['tx_hash'] and log.get('blockHash') == fixed['block_hash'], 'RECEIPT_LOG_IDENTITY')
        require(uint(log.get('blockNumber'), 'log.blockNumber') == fixed['block_number'] and uint(log.get('transactionIndex'), 'log.transactionIndex') == fixed['transaction_index'], 'RECEIPT_LOG_POSITION')
        require(log.get('removed') is False, 'RECEIPT_LOG_REMOVED_OR_UNKNOWN')
        position = uint(log.get('logIndex'), 'log.logIndex'); require(position not in log_positions, 'DUPLICATE_RECEIPT_LOG_INDEX'); log_positions.add(position)
        if 'blockTimestamp' in log: require(uint(log['blockTimestamp'], 'log.blockTimestamp') == timestamp, 'RECEIPT_LOG_TIME_MISMATCH')
    require(isinstance(code, str) and re.fullmatch(r'0x(?:[0-9a-fA-F]{2})+', code), 'HISTORICAL_RUNTIME_HEX')
    runtime = bytes.fromhex(code[2:])
    require(len(runtime) == fixed['runtime_code_bytes'] and sha(runtime) == fixed['runtime_code_sha256'], 'HISTORICAL_RUNTIME_IDENTITY')
    return {'status': 'PASS', 'material_status': 'VERIFIED_ORIGINAL_SHARED_BODY', 'original_body_available': True,
            'original_body_sha256': sha(raw), 'original_body_bytes': len(raw), 'members_verified': 5,
            'successful_members': sum(x['status'] == 'SUCCESS_VALIDATED' for x in summary),
            'error_members': sum(x['status'] == 'RPC_ERROR' for x in summary), 'members': summary,
            'tx_hash': fixed['tx_hash'], 'block_number': fixed['block_number'], 'block_hash': fixed['block_hash'],
            'transaction_index': fixed['transaction_index'], 'receipt_logs_verified': len(logs),
            'runtime_code_bytes': len(runtime), 'runtime_code_sha256': sha(runtime),
            'original_dispatch_utc': request_view['utc'], 'original_receipt_utc': receipt_view['utc'],
            'redacted_view_sha256': {role: items[role]['sha256'] for role in ('requests_redacted', 'batch_receipt_redacted')},
            'network_requests': 0, 'weth_certification_predicates_changed': False,
            'scope': 'Original shared HTTP bytes and five split RPC bindings; existing WETH predicate outcome is independent',
            'external_acceptance_status': 'PENDING_REVIEW'}


def verify_tree(tree):
    root = Path(tree).resolve(); path = root / MANIFEST
    require(path.is_file(), 'MISSING_PRIVATE_WETH_WIRE_MANIFEST: public source/test trees do not contain this private closure')
    require(not path.is_symlink(), 'MANIFEST_LINK_NOT_ALLOWED')
    return verify_document(root, strict_json(path.read_bytes()), fixed=FIXED)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tree', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = verify_tree(args.tree)
    except (WireValidationError, OSError, ValueError, KeyError, TypeError) as error:
        result = {'status': 'FAIL', 'material_status': 'MATERIAL_VERIFICATION_FAILED', 'original_body_available': False,
                  'original_body_sha256': FIXED['original_body_sha256'], 'members_verified': 0,
                  'error_type': type(error).__name__, 'reason': str(error), 'network_requests': 0,
                  'weth_certification_predicates_changed': False}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8', newline='\n') as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2); handle.write('\n')
    print(json.dumps({k: result[k] for k in ('status', 'material_status', 'original_body_available', 'members_verified', 'network_requests')}, sort_keys=True))
    return 0 if result['status'] == 'PASS' else 1


if __name__ == '__main__': raise SystemExit(main())
