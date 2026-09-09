"""Pure exact Ethereum timestamp-to-block brackets; caller verifies RPC bytes.

No provider, clock, database or budget implementation. Canonical Ethereum block
timestamps strictly increase; all included boundary blocks remain inclusive.
"""
from copy import deepcopy
import re

VERSION = 'stage1d-timestamp-block-bracket-v1'
FIELDS = ('start_block', 'end_block', 'start_time', 'end_time')
RULE = 'ETHEREUM_CANONICAL_STRICTLY_INCREASING_BLOCK_TIMESTAMPS'


def _bounds(need):
    result = {k: need[k] for k in FIELDS}
    if any(type(v) is not int or v < 0 for v in result.values()):
        raise ValueError('Exact nonnegative block/time integers required')
    if result['start_block'] > result['end_block'] or result['start_time'] > result['end_time']:
        raise ValueError('Inverted logical request')
    return result


def header_timestamp(block, header):
    if type(block) is not int or not isinstance(header, dict):
        raise ValueError('Exact physical header required')
    for key in ('number', 'timestamp'):
        if not isinstance(header.get(key), str) or not re.fullmatch(r'0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)', header[key]):
            raise ValueError('Exact header quantity required')
    if int(header['number'], 16) != block or not re.fullmatch(r'0x[0-9a-fA-F]{64}', str(header.get('hash'))):
        raise ValueError('Historical header block/hash identity differs')
    return int(header['timestamp'], 16)


def resolve_timestamp_bracket(need, get_header):
    bounds = _bounds(need)
    lo, hi, start, end = (bounds[k] for k in FIELDS)
    observed = {}
    def stamp(block):
        if block not in observed:
            observed[block] = deepcopy(get_header(block))
        return header_timestamp(block, observed[block])
    if stamp(hi) < start:
        first, last = hi + 1, hi
    elif stamp(lo) > end:
        first, last = lo, lo - 1
    else:
        left, right = lo, hi
        if stamp(lo) >= start:
            right = lo
        while left < right:
            middle = (left + right) // 2
            if stamp(middle) < start:
                left = middle + 1
            else:
                right = middle
        first = left
        left, right = lo, hi
        if stamp(hi) <= end:
            left = hi
        while left < right:
            middle = (left + right + 1) // 2
            if stamp(middle) > end:
                right = middle - 1
            else:
                left = middle
        last = left
        for block in (first, last, first - 1, last + 1):
            if lo <= block <= hi:
                stamp(block)
    proof = {'schema_version': VERSION, 'logical_bounds': bounds, 'first_block': first,
             'last_block': last, 'empty': first > last, 'proof_headers': sorted(observed), 'timestamp_rule': RULE}
    verify_timestamp_bracket(need, proof, observed)
    return proof


def verify_timestamp_bracket(need, proof, headers):
    bounds = _bounds(need)
    if (proof.get('schema_version') != VERSION or proof.get('logical_bounds') != bounds
        or proof.get('timestamp_rule') != RULE or type(proof.get('empty')) is not bool):
        raise ValueError('Timestamp proof does not bind this exact logical rectangle')
    lo, hi, start, end = (bounds[k] for k in FIELDS)
    first, last = proof.get('first_block'), proof.get('last_block')
    if type(first) is not int or type(last) is not int or not lo <= first <= hi + 1 or not lo - 1 <= last <= hi:
        raise ValueError('Physical timestamp bracket escapes logical block domain')
    included = proof.get('proof_headers')
    if not isinstance(included, list) or not included or included != sorted(set(included)):
        raise ValueError('Unique observed historical header list required')
    observed = {}
    for block in included:
        if type(block) is not int or not lo <= block <= hi or block not in headers:
            raise ValueError('Timestamp proof original header missing')
        observed[block] = header_timestamp(block, headers[block])
    if any(observed[a] >= observed[b] for a, b in zip(included, included[1:])):
        raise ValueError('Observed canonical block timestamps are not strictly increasing')
    def stamp(block):
        if block not in observed:
            raise ValueError('Necessary adjacent timestamp bracket header absent')
        return observed[block]
    if proof['empty']:
        if first != last + 1:
            raise ValueError('Empty window requires adjacent lower/upper insertion positions')
        if first == hi + 1:
            if stamp(hi) >= start:
                raise ValueError('Whole domain is not before the requested time')
        elif last == lo - 1:
            if stamp(lo) <= end:
                raise ValueError('Whole domain is not after the requested time')
        elif not (stamp(last) < start <= end < stamp(first)):
            raise ValueError('Adjacent headers do not prove an empty timestamp intersection')
    else:
        if first > last or not start <= stamp(first) <= stamp(last) <= end:
            raise ValueError('Physical endpoint timestamps outside requested inclusive window')
        if first > lo and stamp(first - 1) >= start:
            raise ValueError('Lower block is not the first timestamp in the request')
        if last < hi and stamp(last + 1) <= end:
            raise ValueError('Upper block is not the last timestamp in the request')
    return deepcopy(proof)
