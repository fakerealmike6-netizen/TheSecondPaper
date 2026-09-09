"""Stage1D native evidence adapter into the unchanged accepted R3 context LP.

No requests, expert neighbors, source-allocation rules, or new solver occur here.
``necessary_context_windows`` tells the caller which finite account ledgers and
block-end balances are needed. ``build_document`` returns the existing
``assemble_model`` result; its ``model_input`` is the common method input.
Missing evidence is local and explicit. Contradictory physical facts block it.
"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass, fields
from collections import defaultdict
import copy
import hashlib
import json

from context_ledger_r3 import (EvidenceConflict, account, assemble_model,
                               normalize_anchor, normalize_rows, integer, truth)
from context_lp_r3 import build_context_model, validate_document
from stage1d_gap_sequence import (GapSequence, GapAccumulator, gap_count,
    concat_gaps, wrap_gaps, serialize_gaps)

VERSION = 'stage1d-native-context-adapter-v1'
NATIVE = 'native:eip155:1'
REQUIRED = [
    'ALL_TOP_LEVEL_TRANSACTIONS_INCLUDING_ZERO_VALUE_AND_FAILED',
    'ALL_EFFECTIVE_NATIVE_INTERNAL_TRANSFERS_INCLUDING_SELFDESTRUCT',
    'ALL_TRANSACTION_FEES_PAID_BY_ACCOUNT',
    'APPLICABLE_PROTOCOL_NATIVE_CHANGES',
]


def _dict(value):
    if isinstance(value, (GapSequence, GapAccumulator)):
        return serialize_gaps(value)
    return asdict(value) if is_dataclass(value) else dict(value)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=False, default=_dict).encode()).hexdigest()


def _event(value):
    row = _dict(value)
    # Standard Collector.Event objects are the preferred input. Saved raw rows
    # remain acceptable through the same exact native normalizer below.
    aliases = {'block': ('block_number', 'blockNumber'), 'tx_index': ('transaction_index', 'transactionIndex'),
               'sender': ('from_address', 'from'), 'recipient': ('to_address', 'to'),
               'timestamp': ('block_timestamp', 'block_time'), 'tx_hash': ('hash', 'transactionHash'),
               'amount_raw': ('value_raw', 'value'), 'kind': ('event_kind', 'record_type')}
    for key, names in aliases.items():
        if key not in row:
            for name in names:
                if name in row: row[key] = row[name]; break
    if row.get('asset') is None: row['asset'] = row.get('asset_key', NATIVE)
    kind = str(row.get('kind', 'top')).lower()
    row['kind'] = 'top' if kind in ('tx', 'transaction', 'eth_top_level') else kind
    for key in ('block', 'tx_index'):
        if row.get(key) is not None: row[key] = integer(row[key])
    for key in ('sender', 'recipient', 'tx_hash'):
        if row.get(key) is not None: row[key] = row[key].lower()
    return row


def _collection(value):
    # A collection is read-only here. dataclasses.asdict would recursively copy
    # every repeated gap before the compact codec can preserve its references.
    return {field.name: getattr(value, field.name) for field in fields(value)} if is_dataclass(value) else dict(value)


def _needs_multiasset(collection):
    # Historical native context may carry address-only role records. Explicit
    # non-native facts still enter the unchanged strict asset validator.
    explicit = bool(collection.get('semantic_units')) or any(
        _event(e)['asset'] not in (NATIVE, 'ETH') for e in collection.get('candidate_events', []))
    for row in collection.get('states', []) + collection.get('unresolved_frontier', []):
        state = row.get('state', {})
        explicit |= state.get('asset', NATIVE) not in (NATIVE, 'ETH')
        if state.get('arrival') is not None:
            explicit |= _event(state['arrival'])['asset'] not in (NATIVE, 'ETH')
    if explicit:
        from stage1d_multiasset_context import needs_multiasset
        return needs_multiasset(collection)
    return False


def _seed(query, collection):
    wanted = query['seed_event_id']
    candidates = [_event(x) for x in collection.get('candidate_events', [])]
    found = [x for x in candidates if x.get('event_id') == wanted]
    if len(found) != 1: raise ValueError('Exactly one observed frozen seed is required')
    seed = found[0]
    if seed.get('asset') != NATIVE or seed.get('kind') != 'top':
        raise ValueError('Stage1D adapter supports the authorized native top-level seeds')
    if seed.get('success') is not True or integer(seed.get('amount_raw')) <= 0:
        raise EvidenceConflict('Seed must be a successful exact positive physical event')
    for key, field in [('seed_amount_raw', 'amount_raw'), ('seed_tx_hash', 'tx_hash'),
                       ('seed_from', 'sender'), ('seed_to', 'recipient')]:
        if key in query and str(query[key]).lower() != str(seed[field]).lower():
            raise EvidenceConflict('Frozen seed identity disagrees: ' + key)
    return seed, candidates


def _targets(collection, candidates, labels):
    by_id = {e['event_id']: e for e in candidates}
    groups = defaultdict(set)
    for stop in collection.get('stops', []):
        if stop.get('reason') != 'FIRST_IDENTIFIED_SERVICE': continue
        eid = stop.get('entry_event_id')
        if eid not in by_id: raise EvidenceConflict('Service stop has no observed candidate entry')
        e = by_id[eid]
        addr = stop.get('state', {}).get('address', e['recipient']).lower()
        if addr != e['recipient'] or stop.get('identity', {}).get('kind') != 'SERVICE':
            raise EvidenceConflict('Service stop identity/recipient disagrees')
        current = labels.get(addr) if isinstance(labels, dict) else None
        if current is not None:
            role = current.get('kind', current.get('identity_class'))
            if role != 'SERVICE': raise EvidenceConflict('Service stop differs from frozen label snapshot')
        groups[account(addr)].add(eid)
    return {k: sorted(v) for k, v in sorted(groups.items())}


def required_context_windows(query, collection, ledger_events=(), label_snapshot=None):
    """Finite full-block account windows; never add context counterparties.

    Every nonterminal candidate endpoint needs all native changes in the
    contiguous first..last needed blocks, and balances at first-1 and last.
    Existing context involving such accounts is retained; if it connects two
    modeled accounts, both windows cover the fact. This may expose a specific
    need to extend an earlier plan, but never starts another candidate query.
    """
    collection = _collection(collection)
    from stage1d_multiasset_context import needs_multiasset, required_windows
    if _needs_multiasset(collection):
        return required_windows(query, collection, ledger_events, label_snapshot)
    seed, candidates = _seed(query, collection)
    targets = _targets(collection, candidates, label_snapshot or {})
    terminals = {k[:-4] for k in targets}
    if seed['recipient'] in terminals:
        if len(candidates) != 1:
            raise EvidenceConflict('First-service seed cannot have downstream candidate history')
        return {'schema_version': VERSION, 'name': query['name'], 'query_id': query['query_id'],
            'seed_event_id': seed['event_id'], 'rows': [], 'objective_groups': targets,
            'service_terminals_excluded': sorted(terminals), 'seed_is_first_service': True,
            'new_candidate_or_reference_neighbors_added': 0,
            'context_reason': 'Exact frozen source event enters the verified service directly; no platform ledger or balance is needed.'}
    modeled = {e['recipient'] for e in candidates}
    modeled.update(e['sender'] for e in candidates if e['event_id'] != seed['event_id'])
    modeled -= terminals
    facts = candidates + [_event(e) for e in collection.get('context_events', [])] + [_event(e) for e in ledger_events]
    rows = []
    for addr in sorted(modeled):
        needed = [e for e in facts if e.get('asset') == NATIVE and addr in (e.get('sender'), e.get('recipient'))]
        if not needed: continue
        if any(e.get('block') is None for e in needed): raise ValueError('Observed context needs exact block order')
        first, last = min(e['block'] for e in needed), max(e['block'] for e in needed)
        if first <= 0: raise ValueError('Historical before-block anchor must exist')
        rows.append({'account_id': account(addr), 'address': addr, 'asset': 'ETH',
            'ledger_start_block': first, 'ledger_end_block': last,
            'before_anchor_block': first - 1, 'after_anchor_block': last,
            'anchor_semantics': 'BLOCK_END', 'required_coverage': list(REQUIRED),
            'known_physical_event_ids': sorted({e.get('event_id') for e in needed if e.get('event_id')}),
            'timestamp_rule': 'Use exact first/last block headers or whole UTC date bounds with exact block predicates; do not truncate first/last block at arrival time.',
            'scope_role': 'FINITE_CONTEXT_ONLY_NO_NEIGHBOR_EXPANSION'})
    return {'schema_version': VERSION, 'name': query['name'], 'query_id': query['query_id'],
        'seed_event_id': seed['event_id'], 'rows': rows, 'objective_groups': targets,
        'service_terminals_excluded': sorted(terminals),
        'source_free_initialization_is_conditional_scope_assumption': True,
        'new_candidate_or_reference_neighbors_added': 0}


PROTOCOL_KINDS = frozenset({'BRIDGE', 'MIXER', 'UNSUPPORTED_PROTOCOL'})
IDENTITY_KINDS = {'DEX_OR_PROTOCOL': 'UNSUPPORTED_PROTOCOL',
                  'BRIDGE_BOUNDARY': 'BRIDGE', 'MIXER_BOUNDARY': 'MIXER'}


def exclude_verified_protocol_windows(query, collection, plan, label_snapshot):
    """Return a copied plan with only proven stopped protocol ledgers excluded.

    A missing/mismatching label or entry is an evidence conflict, not permission
    to infer a boundary from transaction volume.  This adapter deliberately does
    not edit candidate/context events, raw ledger rows, coverage, or balances.
    Known ordinary-account facts retain their original planned windows.  A seed
    entering a protocol, or a candidate already propagated out of one, requires
    a separately supported acquisition/model interface and is not silently cut.
    """
    collection = _collection(collection)
    if collection.get('query_id') != query['query_id'] or plan.get('query_id') != query['query_id']:
        raise EvidenceConflict('Protocol context plan belongs to another query')
    candidates = [_dict(e) for e in collection.get('candidate_events', [])]
    by_id = {e['event_id']: e for e in candidates}
    if len(by_id) != len(candidates):
        raise EvidenceConflict('Duplicate candidate physical event identity')
    boundaries = {}
    labels = {str(k).lower(): v for k, v in (label_snapshot or {}).items()}
    for stop in collection.get('stops', []):
        if stop.get('reason') != 'PROTOCOL_BOUNDARY':
            continue
        kind = stop.get('identity', {}).get('kind')
        if kind not in PROTOCOL_KINDS:
            raise EvidenceConflict('Protocol stop is not an existing supported boundary kind')
        eid = stop.get('entry_event_id')
        event = by_id.get(eid)
        if event is None:
            raise EvidenceConflict('Protocol stop has no observed candidate entry')
        recipient = str(event.get('recipient', '')).lower()
        addr = str(stop.get('state', {}).get('address', recipient)).lower()
        if not addr or addr != recipient:
            raise EvidenceConflict('Protocol stop recipient disagrees with observed entry')
        current = labels.get(addr)
        if not isinstance(current, dict):
            raise EvidenceConflict('Protocol stop requires its frozen label snapshot')
        current_kind = current.get('kind', current.get('identity_class'))
        current_kind = IDENTITY_KINDS.get(current_kind, current_kind)
        if current_kind != kind:
            raise EvidenceConflict('Protocol stop differs from frozen label snapshot')
        if eid == query['seed_event_id']:
            raise EvidenceConflict('Seed-recipient protocol boundary needs a dedicated supported seed interface')
        if any(str(e.get('sender', '')).lower() == addr and e['event_id'] != query['seed_event_id']
               for e in candidates):
            raise EvidenceConflict('Candidate continuation out of a stopped protocol needs collection replay')
        prior = boundaries.setdefault(addr, {'address': addr, 'kind': kind,
            'entry_event_ids': [], 'label_evidence': copy.deepcopy(current)})
        if prior['kind'] != kind:
            raise EvidenceConflict('Conflicting protocol kinds in observed stops')
        prior['entry_event_ids'].append(eid)
    copied = copy.deepcopy(plan)
    if not boundaries:
        return copied
    for value in boundaries.values():
        value['entry_event_ids'] = sorted(set(value['entry_event_ids']))
    removed = [copy.deepcopy(row) for row in plan['rows']
               if str(row['address']).lower() in boundaries]
    copied['rows'] = [copy.deepcopy(row) for row in plan['rows']
                      if str(row['address']).lower() not in boundaries]
    copied['protocol_boundaries_excluded'] = sorted(boundaries)
    copied['protocol_boundary_entries'] = [boundaries[k] for k in sorted(boundaries)]
    copied['protocol_boundary_plan_adjustment'] = {
        'schema_version': 'stage1d-existing-protocol-boundary-context-v1',
        'original_plan_sha256': _hash(plan),
        'original_continuous_platform_windows': removed,
        'candidate_graph_unchanged': True,
        'ordinary_account_windows_unchanged': True,
        'ordinary_amounts_fees_balances_unchanged': True,
        'raw_ledger_and_anchor_evidence_must_be_retained': True,
        'retained_entry_semantics': 'EXISTING_R3_CANDIDATE_TO_OUTSIDE_RESERVOIR_NON_TARGET',
        'retained_return_semantics': 'EXISTING_R3_UNKNOWN_EXTERNAL_INCOMING_WITH_ACTUAL_AMOUNT',
        'source_transport_assumption': 'Only prior observed boundary exits may return; no new source injection',
        'service_objectives_unchanged': True,
        'collection_coverage_not_promoted': True,
        'method_definition_changed': False}
    return copied


def necessary_context_windows(query, collection, ledger_events=(), label_snapshot=None, *, prior_rows=()):
    """One acquisition/model plan: preserve known ordinary windows, stop proven boundaries."""
    from stage1d_multiasset_context import needs_multiasset, required_windows
    if _needs_multiasset(_collection(collection)):
        return required_windows(query, _collection(collection), ledger_events, label_snapshot, prior_rows=prior_rows)
    plan = required_context_windows(query, collection, ledger_events, label_snapshot)
    if plan.get('seed_is_first_service'):
        return plan
    from stage1d_cost_boundary_context import current_prior_rows
    prior_rows, deferred_prior = current_prior_rows(_collection(collection), plan['rows'], prior_rows)
    by_address = {row['address']: copy.deepcopy(row) for row in plan['rows']}
    for old in prior_rows:
        if old['address'] in plan['service_terminals_excluded']:
            continue
        if old['address'] not in by_address:
            by_address[old['address']] = copy.deepcopy(old)
        else:
            row = by_address[old['address']]
            row['ledger_start_block'] = min(row['ledger_start_block'], old['ledger_start_block'])
            row['ledger_end_block'] = max(row['ledger_end_block'], old['ledger_end_block'])
            row['before_anchor_block'] = row['ledger_start_block'] - 1
            row['after_anchor_block'] = row['ledger_end_block']
            row['known_physical_event_ids'] = sorted(set(row.get('known_physical_event_ids', [])) |
                set(old.get('known_physical_event_ids', [])))
    plan['rows'] = [by_address[address] for address in sorted(by_address)]
    from stage1d_cost_boundary_context import project_plan
    planned = project_plan(query, _collection(collection),
        exclude_verified_protocol_windows(query, collection, plan, label_snapshot))
    if deferred_prior:
        planned['cost_boundary_context']['deferred_prior_context_rows'] = deferred_prior
    return planned


def _evidence(row):
    value = row.get('evidence_ids') or row.get('provenance')
    if isinstance(value, str):
        try: parsed = json.loads(value)
        except (ValueError, TypeError): parsed = [value]
        value = parsed if isinstance(parsed, list) else [value]
    return sorted(set(value or ['supplied_content_sha256:' + _hash(row)]))


def _path(value):
    if value is None: return None
    if isinstance(value, (tuple, list)): return [integer(v) for v in value]
    value = str(value).strip()
    if value.startswith('['): return [integer(x) for x in json.loads(value)]
    if not value: return []
    return [integer(x) for x in value.replace('_', ',').split(',')]


def _native_rows(events):
    events = list(events)
    raw, original_ids, exclusions, tree_checks, tree_conflicts = [], {}, [], [], []
    full_tree = {}
    for value in events:
        original = _dict(value)
        if original.get('record_type') != 'trace' or original.get('trace_address') is None: continue
        key = (original['tx_hash'].lower(), tuple(_path(original['trace_address'])))
        if key in full_tree:
            physical = ('from_address', 'to_address', 'value_raw', 'call_type', 'success', 'error', 'subtraces')
            if any(full_tree[key].get(k) != original.get(k) for k in physical):
                tree_conflicts.append({'reason': 'RAW_TRACE_STRUCTURE_CONFLICT', 'tx_id': key[0], 'trace_address': list(key[1])})
        else: full_tree[key] = original
    omitted = {key for key, row in full_tree.items()
        if str(row.get('call_type', '')).lower() in ('staticcall', 'delegatecall', 'callcode')
        and all(row.get(k) is None for k in ('value_raw', 'amount_raw', 'value'))}
    rolled_back = {key for key in full_tree if any(
        ancestor and (truth(ancestor.get('success')) is False or ancestor.get('error'))
        for ancestor in (full_tree.get((key[0], key[1][:n])) for n in range(len(key[1]))))}
    projected_out = omitted | rolled_back
    for (txhash, path), original in full_tree.items():
        children = [key for key in full_tree if key[0] == txhash and len(key[1]) == len(path) + 1 and key[1][:-1] == path]
        if original.get('subtraces') is not None:
            expected = integer(original['subtraces'])
            if expected != len(children):
                tree_conflicts.append({'reason': 'TRACE_TREE_CHILD_COUNT_CONFLICT', 'tx_id': txhash,
                    'trace_address': list(path), 'expected_children': expected, 'observed_children': len(children),
                    'evidence_ids': _evidence(original)})
        if any(key in projected_out for key in children) or (txhash, path) in projected_out:
            tree_checks.append({'tx_id': txhash, 'trace_address': list(path),
                'observed_original_subtraces': original.get('subtraces'), 'original_children': len(children),
                'normalizer_projection_children': sum(key not in projected_out for key in children),
                'omitted_nonvalue_node': (txhash, path) in omitted,
                'omitted_rolled_back_node': (txhash, path) in rolled_back,
                'evidence_ids': _evidence(original), 'original_amount_not_modified': True})
    for value in events:
        original = _dict(value)
        # Full R3 evidence rows already express transaction/trace semantics,
        # effective gas price, failure ancestors, creation and refund endpoints.
        # Do not reduce them to the narrower acquisition Event interface.
        if original.get('record_type') is not None:
            key = (original.get('tx_hash', '').lower(), tuple(_path(original['trace_address']))) if original.get('record_type') == 'trace' and original.get('trace_address') is not None else None
            if key in omitted:
                exclusions.append({'reason': 'CALL_CONTEXT_VALUE_NOT_PHYSICAL_TRANSFER',
                    'row': copy.deepcopy(original), 'evidence_ids': _evidence(original),
                    'missing_call_context_value_not_filled': original.get('value_raw') is None})
                continue
            if key in rolled_back:
                exclusions.append({'reason': 'FAILED_FRAME_OR_ANCESTOR_ROLLBACK', 'row': copy.deepcopy(original),
                                   'evidence_ids': _evidence(original), 'basis': 'COMPLETE_ORIGINAL_TRACE_STRUCTURE'})
                continue
            projected = dict(original, evidence_ids=_evidence(original))
            if key in full_tree and projected_out:
                # Check original structure before presenting a tree projection
                # without nonvalue NULL-valued nodes to the legacy normalizer.
                # Projection counts describe this supplied representation, not
                # a changed assertion about the original physical trace tree.
                ancestors = [full_tree.get((key[0], key[1][:n])) for n in range(len(key[1]))]
                if ancestors and all(a and truth(a.get('success')) is True and not a.get('error') for a in ancestors):
                    projected['ancestor_success_verified'] = True
                if projected.get('subtraces') is not None:
                    projected['original_subtraces_evidence'] = projected['subtraces']
                    projected['subtraces'] = sum(k not in projected_out and k[0] == key[0] and len(k[1]) == len(key[1]) + 1 and k[1][:-1] == key[1] for k in full_tree)
            raw.append(projected)
            continue
        e = _event(value)
        if e['asset'] != NATIVE: continue
        kind = e.get('kind', 'top')
        if kind in ('withdrawal', 'protocol_credit', 'fee_recipient', 'block', 'coverage'):
            raw.append(dict(e, record_type=kind)); continue
        if kind not in ('top', 'internal', 'trace', 'call', 'create', 'selfdestruct', 'suicide'):
            continue
        path = _path(e.get('trace_address'))
        if kind == 'internal' and path is None:
            raise ValueError('UNRESOLVED_CHAIN_ORDER: internal transfer lacks trace path')
        trace = kind != 'top'
        row = {'record_type': 'trace' if trace else 'transaction',
            'tx_hash': e['tx_hash'], 'block_number': e['block'], 'tx_index': e.get('tx_index'),
            'block_hash': e.get('block_hash'), 'sender': e['sender'], 'recipient': e.get('recipient'),
            'amount_raw': e.get('amount_raw'), 'success': e.get('success'),
            'evidence_ids': _evidence(e)}
        if trace:
            # The inherited Dune adapter's effective-success SQL excludes failed
            # transaction/ancestor frames. Generic trace inputs must provide
            # their own ancestry evidence; a mere successful-looking child is
            # not upgraded by this adapter.
            verified = (e.get('ancestor_success_verified') is True or
                        any(str(item).startswith('DUNE_INDEX_stage1b-dune-index-adapter-1.0') for item in _evidence(e)))
            row.update(trace_address=path, trace_type=e.get('trace_type', 'call'),
                call_type=e.get('call_type', 'call'), ancestor_success_verified=verified,
                tx_success=e.get('tx_success', e.get('success') if verified else None))
        else:
            row.update(gas_raw=e.get('gas_raw'), gas_used=e.get('gas_used'),
                       gas_price=e.get('gas_price'), input=e.get('input', e.get('input_data')))
        raw.append(row)
        if e.get('event_id'):
            canonical = f"eip155:1:tx:{e['tx_hash']}:" + ('top' if not trace or not path else 'trace:' + ','.join(map(str, path)))
            old = original_ids.get(canonical)
            if old is not None and old != e['event_id']:
                raise EvidenceConflict('One physical event has incompatible public input identities')
            original_ids[canonical] = e['event_id']
    return raw, original_ids, exclusions, tree_checks, tree_conflicts


def _unwrap(value):
    if isinstance(value, dict) and 'response' in value: return value['response']
    return value


def _headers(headers):
    found = {}
    values = headers.values() if isinstance(headers, dict) else headers or []
    for value in values:
        wrapped = _unwrap(value)
        result = wrapped.get('result', wrapped) if isinstance(wrapped, dict) else None
        if not isinstance(result, dict) or result.get('number') is None: continue
        block = integer(result['number'])
        if block in found and found[block].get('hash') != result.get('hash'):
            raise EvidenceConflict('Conflicting canonical block headers')
        found[block] = result
    return found


def _balances(balances, headers, windows):
    normalized, gaps = [], []
    entries = balances.items() if isinstance(balances, dict) else [(None, b) for b in balances or []]
    for key, value in entries:
        if isinstance(value, dict) and value.get('anchor_includes_seed'):
            raise EvidenceConflict('Anchor already includes seed; double injection rejected')
        if isinstance(value, dict) and value.get('initial_position', {}).get('phase') in ('EVENT_PRE', 'TRANSACTION_PRE'):
            raise ValueError('This adapter accepts historical BLOCK_END balances only')
        if isinstance(value, dict) and value.get('request'):
            request, response = value['request'], value.get('response', {})
            selector = request['params'][1]
            block = integer(selector) if not isinstance(selector, dict) else None
            header = next((h for h in headers.values() if h.get('hash') == selector.get('blockHash')), None) if isinstance(selector, dict) else headers.get(block)
        else:
            if isinstance(key, tuple): addr, block = key
            elif key is not None: addr, block = key.rsplit(':', 1)
            else: addr, block = value['address'], value['block_number']
            block = integer(block)
            request = {'method': 'eth_getBalance', 'params': [addr, hex(block)]}
            response = _unwrap(value)
            if not isinstance(response, dict): response = {'result': response}
            if 'actual_balance_raw' in response and 'result' not in response:
                response = dict(response, result=hex(integer(response['actual_balance_raw'])))
            header = headers.get(block)
        if response.get('error') or response.get('result') is None:
            gaps.append({'type': 'HISTORICAL_BALANCE_REQUEST_UNAVAILABLE', 'account_id': account(request['params'][0]),
                'block_number': block, 'provider_error': copy.deepcopy(response.get('error'))})
            continue
        normalized.append(normalize_anchor(request, response, header, _evidence(value) if isinstance(value, dict) else ['balance_content_sha256:' + _hash({'key':str(key), 'value':value})]))
    return normalized, gaps


def _seed_service_document(query, collection, seed, plan, labels):
    """Use the existing simple observed-graph interface for a direct seed sink.

    Its seed variable is fixed to the physical seed amount. No synthetic
    transfer, measured platform balance, or additional source is introduced.
    """
    from lp_model import build_model
    fact = {k: v for k, v in seed.items() if k in EventFields}
    graph = {'schema_version': 'stage1c-controlled-observed-1.0',
        'query_id': query['query_id'], 'scenario_id': query['query_id'], 'name': query['name'],
        'incident_id': query.get('incident_id'), 'source_layer': 'REAL_CHAIN',
        'provenance': 'STAGE1D_EXACT_OBSERVED_SEED_FIRST_SERVICE',
        'scope': 'FROZEN_ORDERED_OBSERVED_GRAPH', 'target_accounts': [seed['recipient']],
        'initial_balances': {}, 'objective_groups': plan['objective_groups'],
        'all_service_entries': [seed['event_id']], 'zero_hop': True,
        'events': [{'id': seed['event_id'], 'kind': 'seed', 'order': 1, 'asset': 'ETH',
            'from': seed['sender'], 'to': seed['recipient'], 'amount_raw': str(seed['amount_raw']),
            'order_basis': 'Sole exact observed source event at its canonical block and transaction index',
            'evidence_ids': _evidence(seed)}],
        'physical_fact_manifest': [fact],
        'sampling': {'chain_id': 1, 'strict_time_order': True, 'first_identified_service_stops_branch': True,
                     'lp_path_hop_limit': None, 'lp_fund_age_limit': None},
        'fact_conflicts': copy.deepcopy(collection.get('fact_conflicts', [])),
        'gaps': serialize_gaps(collection.get('gaps', [])),
        'construction_notes': ['The existing simple observed schema also supports a real seed-service receipt; this is not a synthetic sample.',
            'The service has an absorbing received-source accumulator, not a measured or zero-filled platform balance.',
            'The seed payer fee precedes source injection outside the modeled recipient; it does not debit the received seed amount.'],
        'stage1d_context_adapter': {'version': VERSION, 'input_collection_hash': _hash(collection),
            'label_snapshot_hash': _hash(labels or {}), 'context_plan': plan,
            'ordinary_native_graph_requires_weth_certification': False},
        'outside_seed_payer_fee_evidence': {'payer': seed['sender'], 'gas_raw': seed.get('gas_raw'),
            'evidence_ids': _evidence(seed), 'used_as_recipient_debit': False}}
    if collection.get('unresolved_frontier'):
        graph['gaps'] = serialize_gaps(concat_gaps(graph['gaps'], [
            {'type': 'UNRESOLVED_FRONTIER_AT_SERVICE_SEED',
             'frontier': copy.deepcopy(collection['unresolved_frontier'])}]))
    if not graph['fact_conflicts']: build_model(graph)
    status = 'EVIDENCE_CONFLICT_MODEL_BLOCKED' if graph['fact_conflicts'] else 'PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS' if gap_count(graph['gaps']) else 'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE'
    return {'model_input': graph, 'context_plan': plan, 'completion_status': status,
        'account_ledgers': [], 'balance_anchors': [], 'ledger_reconciliation': [],
        'constraint_provenance': [], 'fact_conflicts': graph['fact_conflicts'],
        'evidence_gaps': graph['gaps'], 'adapter_version': VERSION, 'new_network_requests': 0}


# Only the already accepted canonical physical Event fields may cross into a
# generic graph manifest; context-only annotations are kept separately.
from collector import Event as _CollectorEvent
EventFields = set(_CollectorEvent.__dataclass_fields__)


def build_document(query, collection, ledger_events, balances=None, block_headers=None,
                   receipts=None, label_snapshot=None, *, coverage=None, context_plan=None):
    """Return accepted R3 ``model_input`` plus reconciliation and exact gaps.

    ``ledger_events`` is a list of Collector.Event/dicts, or a mapping with
    ``events`` and explicit R3 ``coverage`` records. ``balances`` is keyed by
    ``address:block`` (integer/hex block), with JSON-RPC result objects or saved
    request/response envelopes. Receipt/header mappings use standard RPC dicts.
    Contradictions raise EvidenceConflict or return the inherited BLOCKED result;
    callers must preserve failure instead of substituting a no-target model.
    """
    collection = _collection(collection)
    from stage1d_multiasset_context import needs_multiasset, build_document as build_multiasset_document
    if _needs_multiasset(collection):
        return build_multiasset_document(query, collection, ledger_events, balances, block_headers,
            receipts, label_snapshot, coverage=coverage, context_plan=context_plan)
    if collection.get('query_id') != query['query_id']:
        raise EvidenceConflict('Collection belongs to another query')
    if isinstance(ledger_events, dict):
        if coverage is None: coverage = ledger_events.get('coverage', [])
        ledger_events = ledger_events.get('events', [])
    ledger_events = list(ledger_events or [])
    seed, candidates = _seed(query, collection)
    if context_plan is not None and any(context_plan.get(key) != expected for key, expected in
            [('query_id', query['query_id']), ('seed_event_id', query['seed_event_id'])]):
        raise EvidenceConflict('Supplied acquisition context plan belongs to another query or seed')
    plan = necessary_context_windows(query, collection, ledger_events, label_snapshot,
        prior_rows=(context_plan or {}).get('rows', []))
    if context_plan is not None and context_plan.get('objective_groups') != plan['objective_groups']:
        raise EvidenceConflict('Supplied acquisition context plan has different frozen service objectives')
    if plan.get('seed_is_first_service'):
        return _seed_service_document(query, collection, seed, plan, label_snapshot)
    native = [e for e in candidates if e.get('asset') == NATIVE]
    if len(native) != len(candidates):
        raise ValueError('Native context requires unsupported cross-asset candidates to remain explicit boundaries')
    graph = {'scenario_id': query['query_id'], 'target_accounts': plan['service_terminals_excluded'],
        'events': [{'id': e['event_id'], 'kind': 'seed' if e['event_id'] == seed['event_id'] else 'transfer'} for e in candidates],
        'physical_fact_manifest': [{'event_id': e['event_id'], 'block': e['block'], 'tx_index': e.get('tx_index'), 'tx_hash': e['tx_hash']} for e in candidates],
        'objective_groups': plan['objective_groups']}
    raw, ids, semantic_exclusions, tree_checks, tree_conflicts = _native_rows(candidates + list(collection.get('context_events', [])) + ledger_events)
    receipt_items = receipts.items() if isinstance(receipts, dict) else [(None, x) for x in receipts or []]
    for key, value in receipt_items:
        response = _unwrap(value)
        rec = response.get('result', response) if isinstance(response, dict) else None
        if not isinstance(rec, dict) or not rec.get('transactionHash'): continue
        if key is not None and str(key).startswith('0x') and key.lower() != rec['transactionHash'].lower():
            raise EvidenceConflict('Receipt belongs to another requested transaction')
        raw.append(dict(rec, record_type='receipt', evidence_ids=_evidence(value)))
    normalized = normalize_rows(raw)
    normalized['excluded'].extend(semantic_exclusions)
    normalized['conflicts'].extend(tree_conflicts)
    for flow in normalized['flows']:
        flow['event_id'] = ids.get(flow['event_id'], flow['event_id'])
    headers = _headers(block_headers or {})
    for fact in normalized['transactions'] + normalized['flows']:
        header = headers.get(fact['block_number'])
        if header and fact.get('block_hash') and header.get('hash') != fact['block_hash']:
            normalized['conflicts'].append({'reason': 'RPC_EVENT_BLOCK_IDENTITY_CONFLICT', 'tx_id': fact['tx_hash'], 'block_number': fact['block_number']})
    anchors, balance_gaps = _balances(balances or {}, headers, plan['rows'])
    # A query may explicitly carry an attempted initial anchor; do not silently
    # reinterpret an after-seed balance as a pre-seed source-free state.
    for row in query.get('initial_anchors', []):
        if row.get('anchor_includes_seed') or integer(row['block_number']) >= seed['block'] and row.get('address') == seed['recipient']:
            raise EvidenceConflict('Initial seed-recipient anchor includes the seed block')
    result = assemble_model(graph, plan, normalized, anchors, list(coverage or []), name=query['name'])
    document = result['model_input']
    document['gaps'].extend(balance_gaps)
    if collection.get('fact_conflicts'):
        document['fact_conflicts'].extend(copy.deepcopy(collection['fact_conflicts']))
    if collection.get('unresolved_frontier') or str(collection.get('status', '')).startswith('INCOMPLETE'):
        document['gaps'].append({'type': 'CANDIDATE_COLLECTION_INCOMPLETE', 'account_id': None,
            'collection_status': collection.get('status'), 'unresolved_frontier': copy.deepcopy(collection.get('unresolved_frontier', [])),
            'effect': 'This observed model is conditional; it is not completion of the original frozen query/scope.'})
    document['gaps'] = serialize_gaps(concat_gaps(document['gaps'], wrap_gaps(
        collection.get('gaps', []), {'type': 'CANDIDATE_ACQUISITION_GAP'},
        account_from='address', account_suffix='|ETH')))
    missing = set(e['event_id'] for e in candidates) - {f['event_id'] for t in document['transactions'] for f in t['flows']}
    if missing:
        document['fact_conflicts'].append({'reason': 'CANDIDATE_PHYSICAL_INPUT_NOT_RECONSTRUCTED', 'event_ids': sorted(missing)})
    document['assumptions'] = [x.replace('R2 candidate scope', 'Stage1D candidate scope') for x in document['assumptions']]
    document['assumptions'].append('Missing actual ledger facts and missing gas remain explicit evidence conditions; no missing amount or balance is filled with zero, and conditional observed-ledger bounds are not full-chain strict bounds.')
    document['stage1d_context_adapter'] = {'version': VERSION, 'input_collection_hash': _hash(collection),
        'query_scope_hash': query.get('scope_hash'), 'label_snapshot_hash': _hash(label_snapshot or {}),
        'ordinary_native_graph_requires_weth_certification': False, 'context_plan': plan}
    if document['fact_conflicts']:
        result['completion_status'] = 'EVIDENCE_CONFLICT_MODEL_BLOCKED'
    else:
        validate_document(document)
        try: build_context_model(document)
        except ValueError as exc:
            if 'EVIDENCE_CONFLICT_MODEL_BLOCKED' not in str(exc): raise
            document['fact_conflicts'].append({'reason': str(exc)})
            result['completion_status'] = 'EVIDENCE_CONFLICT_MODEL_BLOCKED'
        else:
            if gap_count(document['gaps']): result['completion_status'] = 'PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS'
    result['evidence_gaps'] = document['gaps']
    result['fact_conflicts'] = document['fact_conflicts']
    result['context_plan'] = plan
    result['adapter_version'] = VERSION
    result['raw_trace_projection_checks'] = tree_checks
    result['new_network_requests'] = 0
    from stage1d_cost_boundary_context import annotate_result
    return annotate_result(result, plan)
