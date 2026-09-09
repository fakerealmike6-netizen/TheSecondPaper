"""Strict hydrated historical blocks for the explicit Stage1D runtime only.

Importing performs no I/O. Headers are supplied from verified local evidence.
This extends no provider plan, price, retry allowance, or candidate scope.
"""
import hashlib
import json
import re

from context_access_r3 import validate_rpc as legacy_validate_rpc
from context_access_r3 import rpc_result_status as legacy_rpc_result_status


SCHEMA = 'stage1d-hydrated-block-binding-v1'


def _hex(value, length):
    return isinstance(value, str) and re.fullmatch(r'0x[0-9a-fA-F]{' + str(length) + '}', value) is not None


def _quantity(value):
    return isinstance(value, str) and re.fullmatch(r'0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)', value) is not None


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def _hydrated(plan):
    return (isinstance(plan, dict) and plan.get('method') == 'eth_getBlockByNumber'
            and isinstance(plan.get('params'), list) and len(plan['params']) == 2
            and plan['params'][1] is True)


class Stage1DRpcValidation:
    """Runtime hooks bound to exact prior headers, never a moving latest block.

    ``expected_headers`` maps decimal block numbers (int or str) to ordinary
    hash-only eth_getBlockByNumber results. Every full-block request must select
    one of those headers. Legacy false-block and all other supported reads keep
    their exact legacy selector identity and result semantics.
    """
    def __init__(self, expected_headers):
        if not isinstance(expected_headers, dict):
            raise ValueError('Exact prior header mapping required')
        self._headers = {}
        for key, header in expected_headers.items():
            if (not isinstance(header, dict) or not _quantity(header.get('number'))
                    or not _quantity(header.get('timestamp')) or not _hex(header.get('hash'), 64)):
                raise ValueError('Verified fixed header number, hash and timestamp required')
            number = int(header['number'], 16)
            if type(key) not in (int, str) or str(key) != str(number) or number in self._headers:
                raise ValueError('Header mapping key does not match unique block number')
            hashes = header.get('transactions')
            if (not isinstance(hashes, list) or any(not _hex(x, 64) for x in hashes)
                    or len({x.lower() for x in hashes}) != len(hashes)):
                raise ValueError('Prior header must contain the full unique transaction hash list')
            self._headers[number] = {'number': hex(number), 'hash': header['hash'].lower(),
                'timestamp': hex(int(header['timestamp'], 16)),
                'transactions': tuple(x.lower() for x in hashes)}

    def _expected(self, plan):
        if (set(plan) != {'method', 'params'} or not _hydrated(plan)
                or not _quantity(plan['params'][0])):
            raise ValueError('Fixed hydrated historical block required')
        expected = self._headers.get(int(plan['params'][0], 16))
        if expected is None:
            raise ValueError('Hydrated block is outside supplied verified headers')
        return expected

    def validate_rpc(self, plan):
        if (isinstance(plan, dict) and isinstance(plan.get('method'), str)
                and plan['method'].startswith(('debug_', 'trace_'))):
            raise ValueError('Stage1D has no proved existing debug or trace plan entitlement')
        if not _hydrated(plan):
            return legacy_validate_rpc(plan)
        self._expected(plan)
        return json.loads(json.dumps(plan))

    def rpc_identity(self, provider, plan):
        checked = self.validate_rpc(plan)
        result = {'provider': provider, 'chain': 1, **checked}
        if _hydrated(checked):
            expected = self._expected(checked)
            result['hydrated_block_binding'] = {'schema': SCHEMA,
                'block_number': expected['number'], 'block_hash': expected['hash'],
                'timestamp': expected['timestamp'], 'transaction_count': len(expected['transactions']),
                'transaction_hashes_sha256': hashlib.sha256(_canonical(expected['transactions'])).hexdigest()}
        return result

    def rpc_result_status(self, request, response):
        if not _hydrated(request):
            return legacy_rpc_result_status(request, response)
        plan = {key: request.get(key) for key in ('method', 'params')}
        try:
            expected = self._expected(plan)
        except (ValueError, TypeError, KeyError):
            return 'INVALID_RPC_BINDING'
        prior = legacy_rpc_result_status(request, response)
        if prior != 'SUCCESS_VALIDATED':
            return prior
        block = response['result']
        if (str(block.get('hash', '')).lower() != expected['hash']
                or not _quantity(block.get('timestamp'))
                or int(block['timestamp'], 16) != int(expected['timestamp'], 16)):
            return 'INVALID_RPC_BINDING'
        transactions = block.get('transactions')
        if not isinstance(transactions, list) or len(transactions) != len(expected['transactions']):
            return 'INCOMPLETE_HYDRATED_BLOCK'
        for index, (tx, tx_hash) in enumerate(zip(transactions, expected['transactions'])):
            if not isinstance(tx, dict):
                return 'INCOMPLETE_HYDRATED_BLOCK'
            if (str(tx.get('hash', '')).lower() != tx_hash
                    or str(tx.get('blockHash', '')).lower() != expected['hash']
                    or not _quantity(tx.get('blockNumber'))
                    or int(tx['blockNumber'], 16) != int(expected['number'], 16)
                    or not _quantity(tx.get('transactionIndex'))
                    or int(tx['transactionIndex'], 16) != index):
                return 'INVALID_RPC_BINDING'
            if (not _hex(tx.get('from'), 40)
                    or (tx.get('to') is not None and not _hex(tx.get('to'), 40))
                    or 'to' not in tx or not _quantity(tx.get('value'))
                    or not _quantity(tx.get('gas'))
                    or not isinstance(tx.get('input'), str)
                    or re.fullmatch(r'0x(?:[0-9a-fA-F]{2})*', tx['input']) is None):
                return 'INVALID_HYDRATED_TRANSACTION'
        return 'SUCCESS_VALIDATED'
