"""Offline final-context bridge for explicitly frozen current query materials.

No collection, cache mutation, network, solver call or execution freeze occurs.
The root supplies checked existing facts and controls acquisition/registration.
"""
from __future__ import annotations
import copy
import hashlib
import json
from pathlib import Path

from collector import Scope
from context_ledger_r3 import integer
from stage1d_context import _event, _hash, _headers, _unwrap, build_document, necessary_context_windows
from stage1d_multiasset_context import (ETH, WETH, NATIVE, CONTRACT, asset, context_asset,
                                      project_context_assets, validate_collection_asset_domain)
from stage1d_finite_state_rpc import balance_plan, logs_plan
from stage1d_gap_sequence import concat_gaps, serialize_gaps
from stage1d_raw_fields import validate_raw_member, transaction_type, exact_trace_path

SCHEMA = 'stage1d-closure-final-context-v1'
ROLE_FIELDS = ('kind', 'actor', 'branch_action', 'technical_role_status',
               'role_certificate_ids', 'task_boundary_id', 'task_boundary_ids',
               'task_boundary_authority_ids', 'task_boundary_source_sha256',
               'branch_hold_reason', 'branch_hold_temporary', 'branch_hold_resume_condition',
               'branch_hold_chain_verified_role_claim', 'branch_hold_source_zero_claim',
               'branch_hold_identity_reclassification')


def _checked(work, ref):
    root = Path(work).resolve()
    if not isinstance(ref, dict) or not isinstance(ref.get('path'), str):
        raise ValueError('Explicit current input path and SHA-256 required')
    path = (root / ref['path']).resolve()
    if not path.is_relative_to(root) or path.is_symlink() or not path.is_file():
        raise ValueError('Current input escaped work')
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != ref.get('sha256'):
        raise ValueError('Current frozen input SHA-256 differs')
    return json.loads(data), {'path': path.relative_to(root).as_posix(), 'sha256': ref['sha256']}


def validate_current(query, collection, labels):
    """Bind adopted roles and regenerated scope, without silently replaying labels."""
    validate_collection_asset_domain(collection, query)
    metrics = collection.get('metrics', {})
    frozen_scope = metrics.get('scope_freeze')
    if not isinstance(frozen_scope, dict):
        raise ValueError('Current collection needs its actual serialized scope freeze')
    scope = Scope(**frozen_scope)
    if (collection.get('query_id') != query['query_id'] or scope.query_id != query['query_id']
            or scope.scope_hash != query.get('scope_hash') or scope.scope_id != query.get('scope_id')
            or metrics.get('scope_hash') != scope.scope_hash or metrics.get('scope_id') != scope.scope_id):
        raise ValueError('Current collection/query scope binding differs')
    if not isinstance(labels, dict):
        raise ValueError('Explicit current address-to-identity snapshot required')
    adopted = []
    for record in collection.get('states', []):
        state, identity = record['state'], record['identity']
        address = state['address']
        frozen = labels.get(address)
        if not isinstance(frozen, dict):
            raise ValueError('Current adopted role missing from label snapshot: ' + address)
        fields = [k for k in ROLE_FIELDS if k in identity or k in frozen]
        if any(identity.get(k) != frozen.get(k) for k in fields):
            raise ValueError('Current adopted role differs from label snapshot: ' + address)
        adopted.append({'state': state, 'identity': identity})
    result = {'scope': scope.freeze_dict(), 'scope_hash': scope.scope_hash,
            'collection_canonical_sha256': _hash(collection), 'label_canonical_sha256': _hash(labels),
            'adopted_roles_sha256': _hash(adopted)}
    from stage1d_cost_boundary_context import validate_overlay
    cost = validate_overlay(query, collection)
    if cost is not None:
        result.update(cost_boundary_policy_sha256=cost['policy']['policy_sha256'],
                      cost_boundary_decisions_sha256=cost['decisions_sha256'])
    return result


def load_frozen_inputs(work, frozen_inputs_ref, query_name):
    """Read the same explicit candidates/labels freeze used by BQ preparation."""
    from stage1d_closure_scope import active_batch_path
    root = Path(work).resolve()
    frozen, dependency = _checked(root, frozen_inputs_ref)
    current = active_batch_path(root)
    if (frozen.get('freeze_sha256') != hashlib.sha256(current.read_bytes()).hexdigest()
            or frozen.get('candidates_frozen') is not True or frozen.get('labels_frozen') is not True):
        raise ValueError('Explicit current stable candidates and labels freeze required')
    queries = [q for q in json.loads(current.read_bytes())['queries'] if q['name'] == query_name]
    if len(queries) != 1 or query_name not in frozen.get('queries', {}):
        raise ValueError('Query absent from current freeze')
    refs = frozen['queries'][query_name]
    collection, cr = _checked(root, refs['collection'])
    labels, lr = _checked(root, refs['labels'])
    query = queries[0]
    binding = validate_current(query, collection, labels)
    binding.update(freeze_sha256=frozen['freeze_sha256'], dependencies=[dependency, cr, lr])
    return query, collection, labels, binding


def _unique(requests):
    return [json.loads(k) for k in sorted({_hashable(r) for r in requests})]


def _hashable(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def _rows(material):
    rows = list(material.get('events', []))
    values = material.get('transactions', {})
    for original in values.values() if isinstance(values, dict) else values:
        wrapped = _unwrap(original)
        value = wrapped.get('result', wrapped)
        if value is not None:
            # A top transaction remains physical evidence, including value=0.
            rows.append(dict(value, record_type='transaction', evidence_ids=original.get('evidence_ids', [])))
    return rows


def _project(rows, plan, mandatory=()):
    # Select physical transaction identities first. Full original trace trees
    # (including nonvalue frames, zero/failed top rows and creation/refunds) are
    # evidence dependencies, not extra modeled accounts or candidate neighbors.
    normalized = [(original, _event(validate_raw_member(original, physical=True))) for original in rows]
    def touched(original, event):
        addresses = {event.get('sender'), event.get('recipient')}
        addresses.update(original.get(k) for k in
                         ('created_address', 'refund_address', 'fee_recipient', 'address'))
        return any((context_asset(event.get('asset', NATIVE)) == asset(row.get('asset', NATIVE))
                   and row['address'] in addresses
                   or asset(row.get('asset', NATIVE)) == ETH and row['address'] == event.get('sender'))
                   and row['ledger_start_block'] <= event['block'] <= row['ledger_end_block']
                   for row in plan['rows'])
    matched = set(mandatory) | {event['tx_hash'] for original, event in normalized
               if event.get('tx_hash') and touched(original, event)}
    return [copy.deepcopy(original) for original, event in normalized
            if event.get('tx_hash') in matched
            or not event.get('tx_hash') and touched(original, event)]


def project_necessary_families(collection, rows, plan):
    """One transaction-family projection for prepare, assembly and dispatch.

    Membership comes from the current model windows and explicit dependencies;
    once selected, all supplied members of that family remain evidence.
    """
    candidates = collection.get('candidate_events', [])
    mandatory = {_event(r)['tx_hash'] for r in candidates if _event(r).get('tx_hash')}
    for key in ('semantic_units', 'context_dependencies', 'boundary_dependencies', 'balance_alignment_dependencies'):
        dependencies = collection.get(key, [])
        if isinstance(dependencies, dict):
            dependencies = dependencies.values()
        for item in dependencies:
            if isinstance(item, dict) and item.get('tx_hash'):
                mandatory.add(item['tx_hash'].lower())
    combined = list(collection.get('context_events', [])) + list(rows)
    selected = _project(combined, plan, mandatory)
    def storage_order(original):
        event=_event(original)
        path=exact_trace_path(original.get('trace_address',original.get('traceAddress')))
        # Canonical storage only: missing positions remain missing in the fact.
        return (event.get('block') if event.get('block') is not None else -1,
                event.get('tx_index') if event.get('tx_index') is not None else -1,
                event.get('tx_hash') or '',event.get('kind')!='top',
                path if path is not None else (-1,),_hashable(original))
    selected.sort(key=storage_order)
    selected_ids = {_event(r)['tx_hash'] for r in selected if _event(r).get('tx_hash')}
    all_ids = {_event(r)['tx_hash'] for r in combined if _event(r).get('tx_hash')}
    return selected, {
        'basis': 'CURRENT_CANDIDATES_ACCOUNT_ASSET_BLOCK_WINDOWS_AND_EXPLICIT_DEPENDENCIES',
        'mandatory_transaction_ids': sorted(mandatory),
        'selected_transaction_ids': sorted(selected_ids | mandatory),
        'excluded_transaction_ids': sorted(all_ids - selected_ids - mandatory),
        'excluded_reason': 'NO_CURRENT_WINDOW_TOUCH_OR_EXPLICIT_TRANSACTION_DEPENDENCY',
        'original_rows_retained': True,
        'planning_only_not_http_or_cost_savings': True,
    }


def _native_receipt_reuse(facts, plan, material, mandatory):
    """Reuse only the existing native normalization/coverage contract, not logs."""
    from stage1d_context import _native_rows
    from context_ledger_r3 import normalize_rows, coverage_complete
    headers = _headers(material.get('headers', {}))
    native_windows = [r for r in plan['rows'] if asset(r.get('asset', NATIVE)) == ETH]
    covered = {r['account_id']: coverage_complete(material.get('coverage', []), r['address'],
               r['ledger_start_block'], r['ledger_end_block'], r['required_coverage'])[0]
               for r in native_windows}
    families = {}
    for original in facts:
        event = _event(original)
        if context_asset(event.get('asset', NATIVE)) == ETH and event.get('tx_hash'):
            families.setdefault(event['tx_hash'], []).append(original)
    reusable = {}
    uncertain = {'TRACE_PATH_MISSING_NO_HASH_ORDER', 'ANCESTOR_SUCCESS_EVIDENCE_MISSING',
                 'ROOT_SUCCESS_EVIDENCE_MISSING', 'TRACE_SUCCESS_EVIDENCE_MISSING',
                 'NON_NATIVE_OR_UNSUPPORTED_RECORD'}
    for txid, family in families.items():
        if txid in mandatory:
            continue
        raw, _, excluded, _, conflicts = _native_rows(family)
        normalized = normalize_rows(raw)
        if conflicts or normalized['conflicts']:
            continue
        if any(e.get('reason') in uncertain or e.get('reason') == 'FAILED_OR_UNKNOWN_TOP_VALUE'
               and e.get('success') is None for e in excluded + normalized['excluded']):
            continue
        txs = [t for t in normalized['transactions'] if t['tx_hash'] == txid]
        if len(txs) != 1:
            continue
        tx = txs[0]
        if (tx['success'] not in (True, False) or tx['tx_index'] is None or not tx.get('block_hash')
                or tx['gas_used'] is None or tx['effective_gas_price'] is None or tx['fee_raw'] is None
                or integer(tx['fee_raw']) != tx['gas_used'] * tx['effective_gas_price']):
            continue
        header = headers.get(tx['block_number'])
        if not header or header.get('hash', '').lower() != tx['block_hash']:
            continue
        touched = {tx['sender'], tx['recipient']}
        for flow in normalized['flows']:
            touched.update((flow.get('sender'), flow.get('recipient')))
        windows = [r for r in native_windows if r['address'] in touched
                   and r['ledger_start_block'] <= tx['block_number'] <= r['ledger_end_block']]
        if not windows or not all(covered[r['account_id']] for r in windows):
            continue
        payer_modeled = any(r['address'] == tx['sender'] for r in windows)
        tops = [r for r in family if _event(r).get('kind') == 'top']
        # Existing native fee normalization is execution-gas based. Do not use
        # it to certify unknown/known blob fees for a modeled payer. Outside
        # payers are explicitly excluded by the existing assemble_model rule.
        if any(any(r.get(k) is not None for k in ('blobGasUsed', 'blobGasPrice', 'blob_gas_used',
                'blob_gas_price', 'receipt_blob_gas_used', 'receipt_blob_gas_price',
                'maxFeePerBlobGas', 'blobVersionedHashes')) for r in tops):
            continue
        types = [t for r in tops if (t := transaction_type(r)) is not None]
        if any(t not in (0, 1, 2) for t in types) or payer_modeled and (not types or len(set(types)) != 1):
            continue
        reusable[txid] = {'basis':'EXISTING_STRICT_NATIVE_NORMALIZER_AND_COMPLETE_CURRENT_LEDGER',
            'normalizer':'stage1d_context._native_rows + context_ledger_r3.normalize_rows/coverage_complete',
            'current_account_ids':[r['account_id'] for r in windows], 'fee_payer_modeled':payer_modeled,
            'execution_fee_raw':tx['fee_raw'], 'raw_family_canonical_sha256':_hash(family),
            'full_receipt_or_token_logs_certified':False, 'ledger_coverage_promoted':False}
    return reusable


def requirements(query, collection, labels, material=None):
    """Return exact current demand; neither log success nor point success is coverage."""
    binding = validate_current(query, collection, labels)
    material = material or {}
    # Derive account/asset membership from current reachability first. A shared
    # provider page cannot add accounts or extend these finite block windows.
    plan = necessary_context_windows(query, collection, (), labels)
    selected, family_projection = project_necessary_families(collection, _rows(material), plan)
    requests, token_ledgers, observed_logs = [], [], []
    for row in plan['rows']:
        for field in ('before_anchor_block', 'after_anchor_block'):
            block = row[field]
            requests.append(balance_plan(row['address'], block) if asset(row.get('asset', NATIVE)) == WETH
                            else {'method': 'eth_getBalance', 'params': [row['address'], hex(block)]})
            requests.append({'method': 'eth_getBlockByNumber', 'params': [hex(block), False]})
        if asset(row.get('asset', NATIVE)) == WETH:
            token_ledgers.append({'account_id': row['account_id'], 'address': row['address'], 'asset': WETH,
                'start_block': row['ledger_start_block'], 'end_block': row['ledger_end_block'],
                'required_coverage': row['required_coverage'],
                'requests': [logs_plan(row['address'], row['ledger_start_block'], row['ledger_end_block'], kind)
                             for kind in ('TRANSFER_OUT', 'TRANSFER_IN', 'DEPOSIT', 'WITHDRAWAL')],
                'coverage_status': 'NOT_INFERRED_FROM_POINT_OR_LOG_ROWS'})
    original_facts = collection.get('candidate_events', []) + selected
    facts, background_projection = project_context_assets(original_facts, origin='current.context_or_selected_material', candidates=collection.get('candidate_events', []))
    # A foreign token value does not enter the source model or select accounts.
    # Its physical transaction can still pay ETH fees in a current native window.
    for original in original_facts:
        event = _event(original)
        if context_asset(event['asset']) is None and any(
                asset(row['asset']) == ETH and row['address'] in (event.get('sender'), event.get('recipient'))
                and event.get('block') is not None and row['ledger_start_block'] <= event['block'] <= row['ledger_end_block']
                for row in plan['rows']):
            facts.append(original)  # point identity only; never a native/token value row

    # Log rows only nominate exact already-observed tx/header dependencies. The
    # adapter consumes the full bound receipt and never assumes this is all logs.
    for original in material.get('weth_logs', []):
        event = _event(original)
        if original.get('address', '').lower() != CONTRACT:
            raise ValueError('Unexpected token in canonical WETH log material')
        if any(row['start_block'] <= event['block'] <= row['end_block']
               and ('0x'+'0'*24+row['address'][2:]) in [t.lower() for t in original.get('topics', [])[1:]]
               for row in token_ledgers):
            facts.append(original)
            observed_logs.append(copy.deepcopy(original))
    # Existing exact physical top rows already supply the transaction position,
    # endpoints and integer value to the strict adapter. Do not fetch the same
    # top transaction merely because it came from a verified ledger provider.
    top_present = set()
    for original in facts:
        event = _event(original)
        if (context_asset(event.get('asset', NATIVE)) == ETH and event.get('kind') == 'top'
                and all(event.get(k) is not None for k in ('tx_hash', 'sender', 'block', 'tx_index', 'amount_raw'))):
            top_present.add(event['tx_hash'])
    mandatory_receipts = {u['tx_hash'].lower() for u in collection.get('semantic_units', [])}
    mandatory_receipts.update(_event(r)['tx_hash'] for r in facts
                              if context_asset(_event(r).get('asset', NATIVE)) == WETH and _event(r).get('tx_hash'))
    mandatory_receipts.update(log['transactionHash'].lower() for log in observed_logs)
    # Preserve observed canonical token semantics even if the current candidate
    # representation has not yet acquired its token event/unit membership.
    values = material.get('receipts', {})
    for original in values.values() if isinstance(values, dict) else values:
        value = _unwrap(original); value = value.get('result', value)
        if isinstance(value, dict) and any(log.get('address', '').lower() == CONTRACT for log in value.get('logs', [])):
            mandatory_receipts.add(value['transactionHash'].lower())
    native_reuse = _native_receipt_reuse(facts, plan, material, mandatory_receipts)
    relevant_txs = set(family_projection['mandatory_transaction_ids'])
    for original in facts:
        event = _event(original)
        if event.get('tx_hash'):
            relevant_txs.add(event['tx_hash'])
            if event['tx_hash'] not in native_reuse:
                requests.append({'method': 'eth_getTransactionReceipt', 'params': [event['tx_hash']]})
            if event['tx_hash'] not in top_present:
                requests.append({'method': 'eth_getTransactionByHash', 'params': [event['tx_hash']]})
        if event.get('block') is not None:
            requests.append({'method': 'eth_getBlockByNumber', 'params': [hex(event['block']), False]})
    for tx in relevant_txs - {_event(r).get('tx_hash') for r in facts}:
        requests.extend([{'method':'eth_getTransactionReceipt','params':[tx]},
                         {'method':'eth_getTransactionByHash','params':[tx]}])
    return {'schema_version': SCHEMA, 'query_id': query['query_id'], 'scope_hash': query['scope_hash'],
            'binding': binding, 'context_plan': plan, 'selected_events': selected,
            'necessary_transaction_family_projection': family_projection,
            'background_asset_projection': background_projection,
            'point_requests': _unique(requests), 'weth_ledger_requirements': token_ledgers,
            'observed_weth_logs_requiring_full_materialization': observed_logs,
            'relevant_transaction_ids': sorted(relevant_txs),
            'native_receipt_point_requests_satisfied_by_ledger': native_reuse,
            'new_neighbors': 0, 'new_requests_executed': 0}


def _points(material):
    """Preserve exact eth_call envelopes; never route them into receipt storage."""
    headers = _headers(material.get('headers', {}))
    balances, receipts, transactions = {}, {}, {}
    for kind in ('balances', 'receipts', 'transactions'):
        values = material.get(kind, {})
        for original in values.values() if isinstance(values, dict) else values:
            if kind == 'balances':
                request = original.get('request', {})
                method, params = request.get('method'), request.get('params', [])
                if method == 'eth_getBalance' and len(params) == 2:
                    key = params[0].lower()+':'+str(integer(params[1]))
                elif method == 'eth_call':
                    from stage1d_finite_state_rpc import validate
                    validate({'method': method, 'params': params})
                    key = 'weth_balanceOf:' + _hash({'method': method, 'params': params})
                else:
                    raise ValueError('Exact balance request envelope required')
                target, value = balances, copy.deepcopy(original)
            else:
                value = _unwrap(original)
                value = value.get('result', value)
                if not isinstance(value, dict):
                    raise ValueError('Successful original transaction/receipt required')
                key = value['transactionHash' if kind == 'receipts' else 'hash'].lower()
                request = original.get('request')
                if request and (request.get('method') != ('eth_getTransactionReceipt' if kind == 'receipts' else 'eth_getTransactionByHash')
                                or request.get('params') != [key]):
                    raise ValueError('Original transaction/receipt request identity differs')
                target = receipts if kind == 'receipts' else transactions
                value = copy.deepcopy(original)
            if key in target and _hash(target[key]) != _hash(value):
                raise ValueError('Conflicting point evidence at the same exact selector')
            target[key] = value
    return headers, balances, receipts, transactions


def _missing_points(requests, headers, balances, receipts, transactions):
    balance_index = {}
    for value in balances.values():
        req = {k: value['request'][k] for k in ('method', 'params')}
        response = value.get('response', {})
        if not response.get('error') and response.get('result') is not None:
            balance_index[_hash(req)] = value
    missing = []
    for request in requests:
        method, params = request['method'], request['params']
        present = (_hash(request) in balance_index if method in ('eth_getBalance', 'eth_call') else
                   integer(params[0]) in headers if method == 'eth_getBlockByNumber' else
                   params[0] in receipts if method == 'eth_getTransactionReceipt' else params[0] in transactions)
        if not present:
            missing.append(request)
    return missing


def assemble(query, collection, labels, material, *, binding=None):
    """Return a checked document or explicit blocked result; never run methods."""
    original_material_sha256 = _hash(material)
    trace_identity_proofs = []
    for supplement in material.get('provider_trace_supplements', []):
        from stage1d_trace_provider_identity import reconcile
        material, proofs = reconcile(material, supplement)
        trace_identity_proofs.extend(proofs)
    needed = requirements(query, collection, labels, material)
    checked = needed['binding']
    if binding is not None and any(binding.get(k) != checked[k] for k in checked):
        raise ValueError('Assembly inputs changed after explicit current freeze')
    headers, balances, receipts, transactions = _points(material)
    missing = _missing_points(needed['point_requests'], headers, balances, receipts, transactions)
    required_txs = set(needed['relevant_transaction_ids'])
    # Shared facts are projected into each query's independent source model.
    # Unrelated shared receipts must not contribute fees, logs or new gaps.
    receipts = {k: v for k, v in receipts.items() if k in required_txs}
    required_balances = {_hash(r) for r in needed['point_requests'] if r['method'] in ('eth_getBalance', 'eth_call')}
    balances = {k: v for k, v in balances.items()
                if _hash({field: v['request'][field] for field in ('method', 'params')}) in required_balances}
    supported_events, _ = project_context_assets(needed['selected_events'], origin='material.events', candidates=collection.get('candidate_events', []))
    facts = {'events': supported_events, 'coverage': copy.deepcopy(material.get('coverage', [])),
             'semantic_evidence_context': copy.deepcopy(material.get('semantic_evidence_context',
                                                                       collection.get('semantic_evidence_context', {}))),
             'evidence_kind': material.get('evidence_kind', 'REAL_EVIDENCE_BOUND')}
    missing_txs = {r['params'][0] for r in missing
                   if r['method'] in ('eth_getTransactionByHash', 'eth_getTransactionReceipt')}
    incomplete_logs = [log for log in needed['observed_weth_logs_requiring_full_materialization']
                       if log['transactionHash'].lower() in missing_txs]
    if incomplete_logs:
        # A visible normal transfer/operation is not silently dropped while
        # declaring only a generic account-coverage gap.
        result = {'model_input': None, 'context_plan': needed['context_plan'],
            'completion_status': 'OBSERVED_WETH_LEDGER_MATERIALIZATION_BLOCKED',
            'evidence_gaps': [{'type':'OBSERVED_WETH_LOG_POINT_MATERIAL_MISSING', 'original_log':log}
                              for log in incomplete_logs],
            'blocked_observed_material': {'collection': {
                key: serialize_gaps(value) if key == 'gaps' else copy.deepcopy(value)
                for key, value in collection.items()}, 'material':copy.deepcopy(material)}}
    else:
        projected_collection = dict(collection, context_events=[])
        result = build_document(query, projected_collection, facts, balances, headers, receipts, labels,
                                coverage=facts['coverage'], context_plan=needed['context_plan'])
    if missing:
        gaps = [{'type':'FINAL_CONTEXT_POINT_REQUEST_MISSING', 'request':copy.deepcopy(request)}
                for request in missing]
        previous_gaps = result.get('evidence_gaps', [])
        result['evidence_gaps'] = serialize_gaps(concat_gaps(previous_gaps, gaps))
        if result.get('model_input'):
            # Preserve an alias only when it represented the same original
            # sequence; missing points are appended exactly once in either case.
            doc_gaps = result['model_input'].get('gaps', [])
            result['model_input']['gaps'] = (result['evidence_gaps'] if doc_gaps is previous_gaps
                else serialize_gaps(concat_gaps(doc_gaps, gaps)))
            if result['completion_status'] == 'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE':
                result['completion_status'] = 'PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS'
    document = result.get('model_input')
    if document is not None:
        document['background_asset_projection'] = copy.deepcopy(needed['background_asset_projection'])
    context_evidence = {'schema_version': SCHEMA, 'binding': copy.deepcopy(binding or checked),
        'material_canonical_sha256': _hash(material), 'context_plan': result['context_plan'],
        'background_asset_projection': copy.deepcopy(needed['background_asset_projection']),
        'coverage': facts['coverage'], 'gaps': result.get('evidence_gaps', []),
        'missing_points': missing, 'unresolved_frontier': collection.get('unresolved_frontier', []),
        'candidate_acquisition_status': collection.get('status'), 'adapter_status': result['completion_status'],
        'point_success_does_not_certify_ledger': True, 'new_requests_executed': 0}
    if trace_identity_proofs:
        context_evidence['original_material_canonical_sha256'] = original_material_sha256
        context_evidence['trace_provider_identity_proofs'] = trace_identity_proofs
        result['trace_provider_identity_proofs'] = trace_identity_proofs
    if result.get('cost_boundary_scope') is not None:
        context_evidence['cost_boundary_scope'] = copy.deepcopy(result['cost_boundary_scope'])
        context_evidence['cost_boundary_context'] = copy.deepcopy(needed['context_plan']['cost_boundary_context'])
    runnable = isinstance(document, dict) and bool(document) and 'BLOCKED' not in result['completion_status']
    if runnable:
        from stage1c_output_contract import expected_domains
        domains = expected_domains(document)
        if not domains['passed']:
            raise ValueError('Final common-method input failed domain acceptance: '+str(domains['errors']))
    status = result['completion_status']
    mapped = ('PARTIAL_CONTEXT_WITH_EXPLICIT_ASSUMPTIONS' if status == 'PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS'
              else status) if runnable else None
    if runnable and result.get('cost_boundary_scope', {}).get('conditional_assumptions'):
        mapped = 'PARTIAL_CONTEXT_WITH_EXPLICIT_ASSUMPTIONS'
    registration = None if not runnable else {'query_id': query['query_id'], 'document': document,
        'scope': checked['scope'], 'label_snapshot': copy.deepcopy(labels),
        'context_evidence': context_evidence, 'context_status': mapped,
        'dependencies': copy.deepcopy((binding or {}).get('dependencies', []))}
    return {'schema_version': SCHEMA, 'query_id': query['query_id'], 'scope_hash': query['scope_hash'],
        'status': 'RUNNABLE_CONDITIONAL_CONTEXT' if runnable else 'MODEL_BLOCKED',
        'context_result': result, 'context_evidence': context_evidence, 'requirements': needed,
        'registration_arguments': registration, 'missing_points': missing,
        'acquisition_status_must_be_supplied_by_root': True, 'methods_executed': 0}


def assemble_current(work, frozen_inputs_ref, query_name, material_ref):
    query, collection, labels, binding = load_frozen_inputs(work, frozen_inputs_ref, query_name)
    material, dependency = _checked(work, material_ref)
    binding['dependencies'].append(dependency)
    if material.get('evidence_kind', 'REAL_EVIDENCE_BOUND') != 'REAL_EVIDENCE_BOUND':
        raise ValueError('Production final assembler cannot accept synthetic material')
    return assemble(query, collection, labels, material, binding=binding)
