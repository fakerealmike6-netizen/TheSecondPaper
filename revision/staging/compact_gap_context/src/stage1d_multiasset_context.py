"""Finite canonical ETH/WETH context contract and evidence adapter helpers.

This module performs no I/O, acquisition or attribution. Asset identities are
chain-bound, and an operation is admitted only through its instance validator.
The accepted R3 model consumes the resulting account ledgers directly.
"""
from __future__ import annotations
import copy
import hashlib
import json
from fractions import Fraction as F
from stage1d_gap_sequence import concat_gaps, wrap_gaps, serialize_gaps

SCHEMA = 'stage1d-multiasset-context-model-v1'
SEMANTICS = 'STAGE1D_CANONICAL_WETH_1TO1_V1'
ETH = 'ETH'
NATIVE = 'native:eip155:1'
CONTRACT = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
WETH = 'erc20:eip155:1:' + CONTRACT
ASSETS = frozenset((ETH, WETH))
REGISTRY = {ETH: {'chain_id': 1, 'asset_id': NATIVE, 'decimals': 18},
            WETH: {'chain_id': 1, 'asset_id': WETH, 'contract': CONTRACT, 'decimals': 18}}


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def asset(value):
    value = ETH if value == NATIVE else value
    if value not in ASSETS:
        raise ValueError('Unsupported chain-bound context asset')
    return value



def context_asset(value):
    """Classify raw context only; asset() remains the strict source-model gate."""
    if value in (NATIVE, ETH, WETH):
        return asset(value)
    import re
    if isinstance(value, str) and re.fullmatch(r'erc20:eip155:1:0x[0-9a-f]{40}', value):
        return None  # Exact Ethereum token identity, outside this model's value domain.
    raise ValueError('Malformed or non-Ethereum raw context asset')


def validate_collection_asset_domain(collection, query=None):
    """Reachability and semantic ports never use the background exception."""
    from stage1d_context import _event
    for event in collection.get('candidate_events', []):
        asset(_event(event)['asset'])
    state_records = list(collection.get('states', []))
    state_records.extend(record for record in collection.get('unresolved_frontier', []) if isinstance(record, dict) and isinstance(record.get('state'), dict))
    for record in state_records:
        state = record['state']
        asset(state['asset'])
        if state.get('arrival') is not None:
            asset(_event(state['arrival'])['asset'])
    for unit in collection.get('semantic_units', []):
        for name in ('input', 'output'):
            asset(unit[name]['asset'])
        if unit.get('native_event') and asset(_event(unit['native_event'])['asset']) != ETH:
            raise ValueError('Canonical semantic native leg must remain ETH')
    if query:
        for value in ([query['seed_asset']] if 'seed_asset' in query else []) + ([query['seed_event']['asset']] if query.get('seed_event') else []):
            if asset(value) != ETH:
                raise ValueError('Authorized source seed remains native ETH')


def project_context_assets(rows, *, origin, candidates=()):
    """Return supported values and an exact audit of retained raw background.

    No raw row is modified, zero-filled or interpreted as source allocation.
    Native transactions/trees/fees and complete receipt logs are separate facts.
    """
    from stage1d_context import _event, _hash
    protected = [_event(x) for x in candidates]
    protected_ids = {e.get('event_id') for e in protected if e.get('event_id')}
    protected_logs = {(e.get('tx_hash'), e.get('log_index')) for e in protected if e.get('kind') in ('erc20', 'semantic_log') and e.get('log_index') is not None}
    supported, audit = [], []
    for original in rows:
        event = _event(original)
        if context_asset(event['asset']) is not None:
            supported.append(original)
            continue
        if event.get('event_id') in protected_ids or (event.get('tx_hash'), event.get('log_index')) in protected_logs:
            raise ValueError('Background asset conflicts with a current reachable physical event')
        if (event.get('kind') != 'erc20' or event.get('record_type') not in (None, 'erc20')
                or event.get('trace_address') is not None):
            raise ValueError('Non-native background asset requires an explicit ERC20 event, never a native frame')
        if event.get('chain_id', 'eip155:1') not in ('eip155:1', 1, '1'):
            raise ValueError('Raw context chain disagrees with Ethereum asset identity')
        audit.append({'origin': origin, 'event_id': event.get('event_id'),
            'asset': event['asset'], 'tx_hash': event.get('tx_hash'), 'block': event.get('block'),
            'log_index': event.get('log_index'),
            'raw_row_canonical_sha256': _hash(original),
            'classification': 'EXACT_ETHEREUM_TOKEN_BACKGROUND_OUTSIDE_ETH_WETH_VALUE_DOMAIN',
            'source_value_assigned': False, 'coverage_certified': False,
            'native_transaction_fees_still_required': True,
            'raw_retention': 'Original collection/material row retained under its bound input SHA; receipt logs unchanged'})
    return supported, audit


def key_asset(key):
    if not isinstance(key, str) or '|' not in key:
        raise ValueError('Explicit account|asset required')
    return asset(key.rsplit('|', 1)[1])


def boundary(value):
    return '@outside_reservoir|' + asset(value)


def ordered_operations(tx):
    flows, conversions = tx.get('flows', []), tx.get('conversions', [])
    if not conversions:
        return flows
    items = {x.get('event_id', x.get('id')): x for x in flows + conversions}
    order = tx.get('operation_order')
    if (len(items) != len(flows) + len(conversions) or not isinstance(order, list)
            or len(order) != len(set(order)) or set(order) != set(items)
            or not tx.get('operation_order_basis')):
        raise ValueError('Mixed native/log/conversion operations require complete evidence-bound order')
    return [items[name] for name in order]


def conversion_ports(operation):
    eid = operation['id']
    return [(eid + ':input', operation['asset'], operation['gross_raw']),
            (eid + ':refund', operation['asset'], operation.get('refund_raw', '0')),
            (eid + ':net', operation['asset'], str(F(operation['gross_raw']) - F(operation.get('refund_raw', '0')))),
            (eid + ':output', operation['output_asset'], operation['output_raw'])]


def _integer(value):
    if isinstance(value, (bool, float)):
        raise ValueError('Exact integer raw amount required')
    result = F(value)
    if result < 0 or result.denominator != 1:
        raise ValueError('Nonnegative integer raw amount required')
    return result


def validate_extension(document):
    """Validate registered identity and certified instances, never a bool flag."""
    if document.get('schema_version') != SCHEMA or document.get('asset_registry') != REGISTRY:
        raise ValueError('Unknown multiasset schema or asset registry')
    if document.get('semantic_policy_id') != SEMANTICS or not document.get('semantic_scope_id'):
        raise ValueError('Explicit frozen semantic scope required')
    units = document.get('semantic_units', [])
    by_id = {u['unit_id']: u for u in units}
    if len(by_id) != len(units):
        raise ValueError('Duplicate certified instance identity')
    from stage1d_shared_evidence import context_view
    contexts = context_view(document.get('semantic_evidence_context', {}))
    admitted, consumed = set(), set()
    for tx in document.get('transactions', []):
        physical = {x['event_id'] for x in tx.get('flows', [])}
        for op in tx.get('conversions', []):
            if op.get('kind') != 'conversion' or not op.get('id', '').startswith('semport:'):
                raise ValueError('Semantic port namespace required')
            if {asset(op['asset']), asset(op['output_asset'])} != ASSETS:
                raise ValueError('Only canonical ETH/WETH conversion is supported')
            gross, refund, output = _integer(op['gross_raw']), _integer(op.get('refund_raw', '0')), _integer(op['output_raw'])
            if gross <= 0 or refund != 0 or output != gross:
                raise ValueError('Canonical pure conversion must be exact 1:1 with no hidden refund')
            if key_asset(op['from_account']) != op['asset'] or key_asset(op['to_account']) != op['output_asset']:
                raise ValueError('Conversion account asset mismatch')
            if op['from_account'].rsplit('|', 1)[0] != op['to_account'].rsplit('|', 1)[0]:
                raise ValueError('Canonical conversion credits its actual caller/holder')
            uid = op.get('unit_id')
            if uid not in by_id or uid in admitted:
                raise ValueError('Conversion needs exactly one bound instance')
            unit = by_id[uid]
            if unit.get('virtual_ports') and any(unit['virtual_ports'].get(s) != op['id'] + ':' + s
                                               for s in ('input', 'refund', 'net', 'output')):
                raise ValueError('Ledger semantic port identity differs from certified instance')
            if unit.get('tx_hash') != tx['tx_id'] or unit.get('block_number', unit.get('block')) != tx['block_number'] or unit.get('tx_index') != tx['tx_index']:
                raise ValueError('Instance/ledger transaction identity mismatch')
            if op.get('certified_semantics') == 'SYNTHETIC_CONTROLLED':
                if (document.get('evidence_kind') != 'SYNTHETIC_CONTROLLED'
                        or unit.get('evidence_kind') != 'SYNTHETIC_CONTROLLED'):
                    raise ValueError('Synthetic certificate cannot authorize a real instance')
                if not unit.get('controlled_contract') == 'CANONICAL_1TO1_NO_REFUND':
                    raise ValueError('Explicit controlled operation contract required')
            else:
                from stage1d_semantic_units import validate_semantic_unit
                check = validate_semantic_unit(unit, evidence_context=contexts.get(unit.get('evidence_context_id')))
                if not (check.get('passed') is True or check.get('valid') is True):
                    raise ValueError('Instance evidence revalidation failed: ' + str(check))
            holder = unit.get('holder')
            if holder != op['from_account'].rsplit('|', 1)[0]:
                raise ValueError('Instance actual caller differs from ledger holder')
            expected_kind = 'DEPOSIT' if op['asset'] == ETH else 'WITHDRAWAL'
            if unit.get('kind') != expected_kind:
                raise ValueError('Deposit certificate does not certify withdrawal')
            for side, a in [('input', op['asset']), ('output', op['output_asset'])]:
                port = unit[side]
                if asset(port['asset']) != a or _integer(port['amount_raw']) != gross or port.get('address') != holder:
                    raise ValueError('Exact instance port differs from ledger conversion')
            raw_ids = set(unit.get('raw_consumed_event_ids', []))
            if not raw_ids or raw_ids & physical or raw_ids & consumed:
                raise ValueError('Raw conversion value must be accounted exactly once')
            if set(op.get('raw_consumed_event_ids', [])) != raw_ids:
                raise ValueError('Raw/semantic consumption mapping mismatch')
            consumed |= raw_ids
            admitted.add(uid)
        ordered_operations(tx)
    if admitted != set(by_id):
        raise ValueError('Unused certified instances must remain outside the frozen model')
    return {'passed': True, 'instances': len(admitted), 'raw_consumed_event_ids': sorted(consumed)}


def conversions(document):
    return [op for tx in document.get('transactions', []) for op in tx.get('conversions', [])]


def register_document(document, units, *, evidence_context=None, semantic_scope_id=None, evidence_kind='REAL_EVIDENCE_BOUND'):
    """Register an already aligned real ledger; never fills missing balances."""
    from stage1d_shared_evidence import serialize_contexts
    result = copy.deepcopy(document)
    result.update(schema_version=SCHEMA, asset_registry=copy.deepcopy(REGISTRY),
        semantic_policy_id=SEMANTICS, semantic_units=copy.deepcopy(units),
        semantic_evidence_context=serialize_contexts(evidence_context or {}), evidence_kind=evidence_kind)
    result['semantic_scope_id'] = semantic_scope_id or canonical_hash({'policy': SEMANTICS, 'units': units,
        'query_id': document.get('query_id'), 'scope_hash': document.get('scope_hash')})
    validate_extension(result)
    return result


TOKEN_REQUIRED = ['ALL_CANONICAL_WETH_TRANSFER_LOGS', 'ALL_CANONICAL_WETH_DEPOSIT_WITHDRAWAL_LOGS']


def needs_multiasset(collection):
    validate_collection_asset_domain(collection)
    return bool(collection.get('semantic_units') or any(
        (x.get('asset', NATIVE) if isinstance(x, dict) else getattr(x, 'asset', NATIVE))
        not in (NATIVE, ETH) for x in collection.get('candidate_events', [])))


def required_windows(query, collection, ledger_events=(), label_snapshot=None, *, prior_rows=()):
    """Full-block ledgers on actual account/asset needs, never new neighbors."""
    from stage1d_context import _event, _seed, REQUIRED
    validate_collection_asset_domain(collection, query)
    seed, candidates = _seed(query, collection)
    units = collection.get('semantic_units', [])
    by_id = {e['event_id']: e for e in candidates}
    groups, excluded = {}, set()
    for stop in collection.get('stops', []):
        reason = stop.get('reason')
        if reason not in ('FIRST_IDENTIFIED_SERVICE', 'PROTOCOL_BOUNDARY'): continue
        event = by_id.get(stop.get('entry_event_id'))
        if event is None: raise ValueError('Stopped boundary lacks current candidate evidence')
        address = event['recipient']
        identity = stop.get('identity', {})
        label = (label_snapshot or {}).get(address)
        if not label or label.get('kind', label.get('identity_class')) != identity.get('kind'):
            raise ValueError('Current role snapshot differs from collected boundary')
        excluded.add(address)
        if reason == 'FIRST_IDENTIFIED_SERVICE':
            if identity.get('kind') != 'SERVICE': raise ValueError('Nonservice target')
            groups.setdefault(address + '|' + asset(event['asset']), []).append(event['event_id'])
    # The canonical reserve is not a holder ledger or a new service target.
    excluded.add(CONTRACT)
    modeled = set()
    context_rows, context_projection = project_context_assets(collection.get('context_events', []), origin='collection.context_events', candidates=collection.get('candidate_events', []))
    ledger_rows, ledger_projection = project_context_assets(ledger_events, origin='material.events', candidates=collection.get('candidate_events', []))
    facts = candidates + [_event(x) for x in context_rows] + [_event(x) for x in ledger_rows]
    for e in candidates:
        a = asset(e['asset'])
        for address in [e['recipient']] + ([] if e['event_id'] == seed['event_id'] else [e['sender']]):
            if address not in excluded: modeled.add((address, a))
    for unit in units:
        if unit['holder'] in excluded: raise ValueError('Unsupported/service caller cannot be bypassed by conversion')
        modeled.update((unit['holder'], a) for a in ASSETS)
        facts.append({'event_id': unit['unit_id'], 'sender': unit['holder'], 'recipient': unit['holder'],
                      'asset': ETH, 'block': unit['block_number']})
        facts.append(dict(facts[-1], asset=WETH))
    # ETH fees of token transactions must remain in the same physical ledger.
    # This creates ledger requirements, not fabricated ETH transfer facts.
    token_holders = {address for address, a in modeled if a == WETH}
    modeled.update((address, ETH) for address in token_holders)
    facts.extend(dict(e, asset=ETH) for e in list(facts)
                 if asset(e.get('asset', NATIVE)) == WETH
                 and token_holders & {e.get('sender'), e.get('recipient')})
    rows = {}
    for address, a in sorted(modeled):
        relevant = [e for e in facts if asset(e.get('asset', NATIVE)) == a and address in (e.get('sender'), e.get('recipient'))]
        if not relevant: continue
        blocks = [e['block'] for e in relevant]
        first, last = min(blocks), max(blocks)
        if first <= 0: raise ValueError('Historical before-block anchor required')
        key = address + '|' + a
        rows[key] = {'account_id': key, 'address': address, 'asset': a,
            'ledger_start_block': first, 'ledger_end_block': last, 'before_anchor_block': first-1,
            'after_anchor_block': last, 'anchor_semantics': 'BLOCK_END',
            'required_coverage': list(REQUIRED if a == ETH else TOKEN_REQUIRED),
            'known_physical_event_ids': sorted({e['event_id'] for e in relevant if e.get('event_id')}),
            'scope_role': 'FINITE_CONTEXT_ONLY_NO_NEIGHBOR_EXPANSION'}
    for old in prior_rows:
        asset(old['asset']); key_asset(old['account_id'])
        key = old['account_id']
        if old['address'] in excluded: continue
        if key not in rows: rows[key] = copy.deepcopy(old)
        else:
            row = rows[key]
            row['ledger_start_block'] = min(row['ledger_start_block'], old['ledger_start_block'])
            row['ledger_end_block'] = max(row['ledger_end_block'], old['ledger_end_block'])
            row['before_anchor_block'], row['after_anchor_block'] = row['ledger_start_block']-1, row['ledger_end_block']
    return {'schema_version': SCHEMA, 'query_id': query['query_id'], 'name': query['name'],
        'seed_event_id': seed['event_id'], 'rows': list(rows.values()),
        'objective_groups': {k: sorted(set(v)) for k, v in groups.items()},
        'service_terminals_excluded': sorted({k.rsplit('|',1)[0] for k in groups}),
        'protocol_boundaries_excluded': sorted(excluded - {k.rsplit('|',1)[0] for k in groups}),
        'new_candidate_or_reference_neighbors_added': 0,
        'background_asset_projection': context_projection + ledger_projection,
        'semantic_scope_id': collection.get('semantic_scope_id') or collection.get('metrics',{}).get('semantic_scope_id')
            or canonical_hash({'units':units,'query_id':query['query_id'],'scope_hash':query.get('scope_hash'),
                               'roles':label_snapshot or {}})}


def normalize_weth_balance(value, headers):
    """A holder balanceOf at a fixed block; an ETH reserve balance is rejected."""
    from context_ledger_r3 import integer
    request, response = value.get('request', {}), value.get('response', {})
    params = request.get('params', [])
    if request.get('method') != 'eth_call' or len(params) != 2:
        raise ValueError('WETH holder balance requires exact historical eth_call')
    call, selector = params
    data = call.get('data', '').lower()
    if call.get('to', '').lower() != CONTRACT or len(data) != 74 or not data.startswith('0x70a08231' + '0'*24):
        raise ValueError('Only canonical balanceOf(holder) is a token anchor')
    address = '0x' + data[-40:]
    block = integer(selector)
    header = headers.get(block)
    if not header or integer(header['number']) != block or not header.get('hash'):
        raise ValueError('Historical token balance lacks canonical block identity')
    if response.get('error') or not isinstance(response.get('result'), str) or len(response['result']) != 66:
        raise ValueError('Missing or malformed historical balanceOf result')
    amount = integer(response['result'])
    return {'anchor_id': 'balanceOf:' + WETH + ':' + address + ':' + str(block),
        'account_id': address + '|' + WETH, 'address': address, 'asset': WETH,
        'block_number': block, 'block_hash': header['hash'], 'position': 'BLOCK_END',
        'actual_balance_raw': str(amount), 'evidence_ids': value.get('evidence_ids', []),
        'request_sha256': canonical_hash(request), 'response_sha256': canonical_hash(response)}


def build_document(query, collection, ledger_events, balances=None, block_headers=None,
                   receipts=None, label_snapshot=None, *, coverage=None, context_plan=None):
    """Normalize actual native rows, canonical receipt logs, balances and units.

    Unresolved cross-log/trace ordering or an observed uncertified Withdrawal
    returns a blocked material with the original operation evidence and explicit
    gap. It is never replaced with source-zero normal income.
    """
    from stage1d_context import (_event, _seed, _native_rows, _unwrap, _headers, _balances, _evidence, _hash)
    from context_ledger_r3 import normalize_rows, assemble_model, coverage_complete, integer
    from context_lp_r3 import build_context_model, validate_document
    from stage1d_semantic_units import TRANSFER_TOPIC, WITHDRAWAL_TOPIC, DEPOSIT_TOPIC, validate_semantic_unit
    if collection.get('query_id') != query['query_id']: raise ValueError('Collection/query mismatch')
    if context_plan is not None and any(context_plan.get(k) != query.get(k) for k in ('query_id','seed_event_id')):
        raise ValueError('Context plan belongs to another query/seed')
    material = ledger_events if isinstance(ledger_events, dict) else {'events': list(ledger_events or [])}
    validate_collection_asset_domain(collection, query)
    supported_events, ledger_projection = project_context_assets(material.get('events', []), origin='material.events', candidates=collection.get('candidate_events', []))
    supported_context, context_projection = project_context_assets(collection.get('context_events', []), origin='collection.context_events', candidates=collection.get('candidate_events', []))
    events = [_event(x) for x in supported_events]
    coverage = list(coverage if coverage is not None else material.get('coverage', []))
    units = copy.deepcopy(collection.get('semantic_units', []))
    from stage1d_shared_evidence import context_view
    contexts = context_view(material.get('semantic_evidence_context', collection.get('semantic_evidence_context', {})))
    evidence_kind = material.get('evidence_kind', 'REAL_EVIDENCE_BOUND')
    for unit in units:
        receipt = validate_semantic_unit(unit, evidence_context=contexts)
        if receipt.get('passed') is not True: raise ValueError('Exact instance evidence failed: ' + str(receipt))
        if receipt.get('real_component_certified') is not True and evidence_kind != 'SYNTHETIC_CONTROLLED':
            raise ValueError('Synthetic instance cannot authorize real context')
    plan = required_windows(query, collection, material.get('events', []), label_snapshot, prior_rows=(context_plan or {}).get('rows', []))
    seed, candidates = _seed(query, collection)
    headers = _headers(block_headers or {})
    native_events = [e for e in candidates + [_event(x) for x in supported_context] + events if asset(e['asset']) == ETH]
    certified_legs = {eid:u for u in units for eid in u['raw_consumed_event_ids']}
    for e in native_events:
        if e.get('event_id') in certified_legs:
            original = certified_legs[e['event_id']]['native_event']
            if (any(e.get(k) != original.get(k) for k in ('sender','recipient','block','tx_hash','tx_index'))
                    or integer(e['amount_raw']) != integer(original['amount_raw'])):
                raise ValueError('Original native leg differs from certified physical event')
            e.update(ancestor_success_verified=True, tx_success=True)
    native_events.extend(dict(_event(unit['native_event']), ancestor_success_verified=True, tx_success=True,
        evidence_ids=['REVALIDATED_INSTANCE:' + unit['unit_id']]) for unit in units)
    raw, ids, exclusions, tree_checks, conflicts = _native_rows(native_events)
    receipt_map = {}
    receipt_items = receipts.values() if isinstance(receipts, dict) else receipts or []
    for original in receipt_items:
        response = _unwrap(original); rec = response.get('result', response)
        if not isinstance(rec, dict) or not rec.get('transactionHash'): continue
        txid = rec['transactionHash'].lower()
        if txid in receipt_map and receipt_map[txid][0] != rec: raise ValueError('Conflicting receipt identity')
        receipt_map[txid] = (rec, _evidence(original))
        raw.append(dict(rec, record_type='receipt', evidence_ids=_evidence(original)))
    normalized = normalize_rows(raw)
    normalized['conflicts'].extend(conflicts); normalized['excluded'].extend(exclusions)
    fee_gaps = []
    for tx in normalized['transactions']:
        rec = receipt_map.get(tx['tx_hash'], ({},[]))[0]
        fields = ('blobGasUsed','blobGasPrice')
        if any(rec.get(k) is not None for k in fields):
            if not all(rec.get(k) is not None for k in fields):
                fee_gaps.append({'type':'BLOB_FEE_FIELDS_INCOMPLETE','tx_id':tx['tx_hash']})
            elif tx.get('fee_raw') is None:
                fee_gaps.append({'type':'BASE_EXECUTION_FEE_MISSING_WITH_BLOB_FEE','tx_id':tx['tx_hash']})
            else:
                tx['execution_fee_raw'] = tx['fee_raw']
                tx['blob_fee_raw'] = str(integer(rec['blobGasUsed']) * integer(rec['blobGasPrice']))
                tx['fee_raw'] = str(integer(tx['fee_raw']) + integer(tx['blob_fee_raw']))
        elif rec.get('type') is not None and integer(rec['type']) == 3:
            fee_gaps.append({'type':'BLOB_TRANSACTION_FEE_MISSING','tx_id':tx['tx_hash']})
    for flow in normalized['flows']: flow['event_id'] = ids.get(flow['event_id'], flow['event_id'])
    for fact in normalized['transactions'] + normalized['flows']:
        header = headers.get(fact['block_number'])
        if header and fact.get('block_hash') and header['hash'] != fact['block_hash']:
            normalized['conflicts'].append({'reason': 'CANONICAL_BLOCK_HASH_CONFLICT', 'tx_id': fact['tx_hash']})
    native_plan = copy.deepcopy(plan); native_plan['rows'] = [r for r in plan['rows'] if r['asset'] == ETH]
    native_plan['objective_groups'] = {k:v for k,v in plan['objective_groups'].items() if key_asset(k) == ETH}
    graph = {'scenario_id': query['query_id'], 'target_accounts': plan['service_terminals_excluded'],
        'events': [{'id': e['event_id'], 'kind': 'seed' if e['event_id'] == seed['event_id'] else 'transfer'} for e in candidates if asset(e['asset']) == ETH],
        'physical_fact_manifest': [{'event_id': e['event_id'], 'block': e['block'], 'tx_index': e['tx_index'], 'tx_hash': e['tx_hash']} for e in candidates if asset(e['asset']) == ETH],
        'objective_groups': native_plan['objective_groups']}
    eth_balances = {k:v for k,v in (balances or {}).items() if v.get('request', {}).get('method') != 'eth_call'}
    anchors, balance_gaps = _balances(eth_balances, headers, native_plan['rows'])
    result = assemble_model(graph, native_plan, normalized, anchors, coverage, name=query['name'])
    doc = result['model_input']; doc['gaps'].extend(balance_gaps)
    windows = {r['account_id']: r for r in plan['rows']}; modeled = set(windows)
    token_anchors = {}
    for original in (balances or {}).values():
        if original.get('request', {}).get('method') != 'eth_call': continue
        row = normalize_weth_balance(original, headers)
        old = token_anchors.get((row['account_id'], row['block_number']))
        if old and old['actual_balance_raw'] != row['actual_balance_raw']: raise ValueError('Conflicting balanceOf anchors')
        token_anchors[(row['account_id'], row['block_number'])] = row
    for key, row in windows.items():
        if row['asset'] != WETH: continue
        initial = token_anchors.get((key, row['before_anchor_block']))
        doc['accounts'].append({'account_id': key, 'address': row['address'], 'asset': WETH,
            'initial_actual_balance_raw': initial['actual_balance_raw'] if initial else None,
            'initial_source_raw': '0', 'initial_source_basis': 'Frozen single-source causal scope before first asset arrival; not a claim about all Ethereum history',
            'initial_position': {'phase':'BLOCK_END', 'block_number':row['before_anchor_block']},
            'initial_anchor_id': initial['anchor_id'] if initial else None,
            'evidence_ids': initial['evidence_ids'] if initial else []})
        if not initial: doc['gaps'].append({'type':'INITIAL_WETH_BALANCEOF_MISSING', 'account_id':key, 'required_block':row['before_anchor_block']})
        token_coverage = [c for c in coverage if c.get('asset', WETH) == WETH]
        ok, detail = coverage_complete(token_coverage, row['address'], row['ledger_start_block'], row['ledger_end_block'], TOKEN_REQUIRED)
        if not ok: doc['gaps'].append({'type':'WETH_LEDGER_COVERAGE_MISSING', 'account_id':key, 'missing_types':[k for k,v in detail.items() if not v]})
    txs = {tx['tx_id']:tx for tx in doc['transactions']}
    def tx_for(txid, block, index):
        tx = txs.setdefault(txid, {'tx_id':txid, 'block_number':block, 'tx_index':index, 'flows':[], 'fees':[]})
        if (tx['block_number'],tx['tx_index']) != (block,index): raise ValueError('Physical transaction position conflict')
        return tx
    blocked = list(fee_gaps)
    physical_candidates = {e['event_id']:e for e in candidates}
    log_identity = {(u['tx_hash'], u['exact_log_locator']):u for u in units}
    seen_logs = set(); token_observations = []
    for txid,(rec,evidence) in receipt_map.items():
        block, index = integer(rec['blockNumber']), integer(rec['transactionIndex'])
        if integer(rec['status']) != 1: continue
        header = headers.get(block)
        if not header or header['hash'] != rec['blockHash']:
            blocked.append({'type':'TOKEN_RECEIPT_HEADER_BINDING_MISSING','tx_id':txid}); continue
        for log in rec.get('logs', []):
            if log.get('address','').lower() != CONTRACT: continue
            number = integer(log['logIndex']); identity = (txid,number)
            if identity in seen_logs: raise ValueError('Duplicate physical receipt log')
            seen_logs.add(identity)
            if (log.get('transactionHash') != txid or log.get('blockHash') != rec['blockHash'] or integer(log['blockNumber']) != block
                    or integer(log['transactionIndex']) != index or log.get('removed') is not False):
                raise ValueError('Receipt/log canonical identity conflict')
            topics = log.get('topics', []); topic = topics[0] if topics else None
            if topic not in (TRANSFER_TOPIC, DEPOSIT_TOPIC, WITHDRAWAL_TOPIC): continue
            token_observations.append({'tx_id':txid,'log_index':number,'original_log':copy.deepcopy(log),'evidence_ids':evidence})
            if topic != TRANSFER_TOPIC:
                if identity not in log_identity:
                    holder = ('0x'+topics[1][-40:]).lower() if len(topics)==2 else None
                    if holder + '|' + WETH in modeled if holder else False:
                        blocked.append({'type':'OBSERVED_WETH_OPERATION_LINKAGE_UNRESOLVED','tx_id':txid,'log_index':number,
                            'kind':'WITHDRAWAL' if topic==WITHDRAWAL_TOPIC else 'DEPOSIT','account_id':holder+'|'+WETH,
                            'effect':'Real token debit/credit retained; not classified as unrelated source-zero income'})
                continue
            if len(topics)!=3 or len(log.get('data',''))!=66: raise ValueError('Malformed canonical Transfer')
            sender, receiver = ('0x'+topics[1][-40:]).lower(), ('0x'+topics[2][-40:]).lower()
            amount = integer(log['data']); a,b = sender+'|'+WETH,receiver+'|'+WETH
            eid = 'eip155:1:tx:'+txid+':log:'+str(number)
            if not ({a,b} & modeled): continue
            for candidate in candidates:
                if asset(candidate['asset']) == WETH and candidate.get('log_index') == number and candidate['tx_hash'] == txid and candidate.get('kind') != 'semantic_log':
                    if (candidate['sender'],candidate['recipient'],integer(candidate['amount_raw'])) != (sender,receiver,amount): raise ValueError('Current token candidate differs from receipt log')
                    eid = candidate['event_id']
            if a in modeled:
                role = 'CANDIDATE' if eid in physical_candidates else 'MODELED_INTERNAL' if b in modeled else 'BOUNDARY_OUTFLOW'
            else:
                role = 'BACKGROUND_NORMAL' if (block,index)<(seed['block'],seed['tx_index']) else 'UNKNOWN_EXTERNAL_INCOMING'
            flow = {'event_id':eid,'asset':WETH,'from_account':a,'to_account':b,'amount_raw':str(amount),'role':role,
                    'flow_kind':'erc20_log','log_index':number,'order_basis':'CANONICAL_RECEIPT_LOG_INDEX','evidence_ids':evidence}
            if role=='BACKGROUND_NORMAL':flow['source_zero_basis']='OBSERVED_BEFORE_UNIQUE_SEED_TRANSACTION'
            tx_for(txid,block,index)['flows'].append(flow)
    for unit in units:
        tx = tx_for(unit['tx_hash'],unit['block_number'],unit['tx_index'])
        consumed = set(unit['raw_consumed_event_ids'])
        found = [f for f in tx['flows'] if f['event_id'] in consumed]
        if len(found)!=len(consumed):
            blocked.append({'type':'CONVERSION_NATIVE_LEG_NOT_RECONSTRUCTED','unit_id':unit['unit_id']}); continue
        tx['flows'] = [f for f in tx['flows'] if f['event_id'] not in consumed]
        prefix = unit['virtual_ports']['input'].removesuffix(':input')
        op = {'id':prefix,'kind':'conversion','asset':asset(unit['input']['asset']),'output_asset':asset(unit['output']['asset']),
              'from_account':unit['holder']+'|'+asset(unit['input']['asset']),'to_account':unit['holder']+'|'+asset(unit['output']['asset']),
              'gross_raw':unit['input']['amount_raw'],'refund_raw':'0','output_raw':unit['output']['amount_raw'],
              'certified_semantics':'CERTIFIED_LOCAL_COMPONENT','unit_id':unit['unit_id'],
              'raw_consumed_event_ids':unit['raw_consumed_event_ids'],'evidence_ids':found[0].get('evidence_ids',[]),
              'log_index':unit['exact_log_locator'],'trace_address':unit['call_path']}
        tx.setdefault('conversions',[]).append(op)
    for tx in txs.values():
        ops = tx['flows'] + tx.get('conversions',[])
        token = [o for o in ops if o.get('asset')==WETH or o.get('kind')=='conversion']
        native = [o for o in tx['flows'] if o.get('asset',ETH)==ETH]
        if tx.get('conversions'):
            ordinary_token = [o for o in token if o.get('kind') != 'conversion']
            if native and not ordinary_token:
                # Native CALL value movements use verified trace preorder. A
                # pure canonical component has no unrepresented child value
                # operations, so its atomic replacement occupies that call.
                paths = [(tuple(o.get('trace_address',[])),o) for o in ops]
                conversion_paths = [tuple(o['trace_address']) for o in tx['conversions']]
                if (len({p for p,_ in paths}) != len(paths)
                        or any(p[:len(c)]==c and len(p)>len(c) for p,o in paths if o.get('kind')!='conversion' for c in conversion_paths)):
                    blocked.append({'type':'CONVERSION_TRACE_POSITION_OVERLAP','tx_id':tx['tx_id']})
                else:
                    ordered = [o for _,o in sorted(paths,key=lambda pair:pair[0])]
                    tx['operation_order'] = [o.get('event_id',o.get('id')) for o in ordered]
                    tx['operation_order_basis'] = 'VERIFIED_TRACE_PREORDER_WITH_PURE_ATOMIC_CANONICAL_CALL_REPLACEMENT'
            elif native:
                blocked.append({'type':'MIXED_TRACE_LOG_ORDER_UNRESOLVED','tx_id':tx['tx_id'],
                    'effect':'No guessed cross trace/log order admitted'})
            else:
                ordered = sorted(token,key=lambda o:o['log_index'])
                tx['operation_order'] = [o.get('event_id',o.get('id')) for o in ordered]
                tx['operation_order_basis'] = 'CANONICAL_ORDERED_OPERATION_LOGS_WITH_CERTIFIED_ATOMIC_CALLS'
        elif token and native:
            # With no conversion these two account-asset ledgers commute;
            # preserve each ledger's known internal order and retain that the
            # cross-asset physical order itself was not inferred.
            tx['flows'] = native + sorted(token,key=lambda o:o['log_index'])
            tx['independent_asset_order_proof'] = 'NO_CROSS_ASSET_OPERATION_IN_TX; REPRESENTATIVE_OF_COMMUTING_LEDGER_OPERATIONS_NOT_CLAIMED_CHAIN_ORDER'
        else:
            if token: tx['flows'].sort(key=lambda o:o['log_index'])
    doc['transactions'] = sorted(txs.values(),key=lambda t:(t['block_number'],t['tx_index']))
    reconstructed = {f['event_id'] for tx in doc['transactions'] for f in tx['flows']}
    consumed = {e for u in units for e in u['raw_consumed_event_ids']}
    missing_candidates = set(physical_candidates) - reconstructed - consumed
    if missing_candidates:
        blocked.append({'type':'CURRENT_CANDIDATE_NOT_RECONSTRUCTED','event_ids':sorted(missing_candidates)})
    normalized_txs = {t['tx_hash']:t for t in normalized['transactions']}
    for tx in doc['transactions']:
        observed = normalized_txs.get(tx['tx_id'],{})
        if 'blob_fee_raw' in observed:
            for fee in tx['fees']:
                fee['observed_fee_components'] = {'execution_fee_raw':observed['execution_fee_raw'],
                    'blob_fee_raw':observed['blob_fee_raw'],'sum_raw':observed['fee_raw'],
                    'charge_count':1,'asset':ETH}
    doc['objective_groups'] = plan['objective_groups'];doc['all_service_entries']=sorted(e for es in plan['objective_groups'].values() for e in es)
    # Token anchors attach only at the last observed operation in the bounded
    # block range; missing physical coverage remains an explicit condition.
    for key,row in windows.items():
        if row['asset']!=WETH:continue
        relevant = [tx for tx in doc['transactions'] if any(key in (o.get('from_account'),o.get('to_account')) for o in tx['flows']+tx.get('conversions',[]))]
        close = token_anchors.get((key,row['after_anchor_block']))
        if close and relevant:
            doc['anchors'].append({'anchor_id':close['anchor_id'],'account_id':key,'tx_id':relevant[-1]['tx_id'],
                'when':'post','actual_balance_raw':close['actual_balance_raw'],'evidence_ids':close['evidence_ids'],
                'alignment_basis':'COMPLETE_DECLARED_TOKEN_LEDGER_THROUGH_BLOCK_END_CONDITIONAL_ON_RECORDED_COVERAGE'})
        elif not close:doc['gaps'].append({'type':'CLOSING_WETH_BALANCEOF_MISSING','account_id':key,'required_block':row['after_anchor_block']})
    doc['gaps'].extend(blocked)
    doc['fact_conflicts'].extend(copy.deepcopy(collection.get('fact_conflicts',[])))
    doc['gaps'] = concat_gaps(doc['gaps'], wrap_gaps(collection.get('gaps', []),
        {'type': 'CANDIDATE_ACQUISITION_GAP', 'account_id': None}))
    if collection.get('unresolved_frontier') or str(collection.get('status','')).startswith('INCOMPLETE'):
        doc['gaps'] = concat_gaps(doc['gaps'], [{'type':'CANDIDATE_COLLECTION_INCOMPLETE',
            'account_id':None,'collection_status':collection.get('status')}])
    doc['gaps'] = serialize_gaps(doc['gaps'])
    doc['background_asset_projection'] = context_projection + ledger_projection
    doc['stage1d_context_adapter']={'version':SCHEMA,'input_collection_hash':_hash(collection),
        'query_scope_hash':query.get('scope_hash'),'label_snapshot_hash':_hash(label_snapshot or {}),'context_plan':plan}
    result.update(context_plan=plan, token_operation_observations=token_observations,
        raw_trace_projection_checks=tree_checks, new_network_requests=0, adapter_version=SCHEMA)
    if blocked:
        result.update(model_input=None, blocked_observed_material=doc,
            completion_status='SEMANTIC_OR_ORDER_GAP_MODEL_BLOCKED', evidence_gaps=doc['gaps'])
        return result
    doc=register_document(doc,units,evidence_context=contexts,semantic_scope_id=plan['semantic_scope_id'], evidence_kind=evidence_kind)
    validate_document(doc)
    model=build_context_model(doc)
    result.update(model_input=doc, completion_status=model.metadata['balance_status'], evidence_gaps=doc['gaps'],
        constraint_provenance=model.metadata['constraint_provenance'], semantic_constraint_used_unit_ids=[u['unit_id'] for u in units])
    return result
