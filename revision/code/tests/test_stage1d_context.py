import copy
import hashlib
import json
import unittest

from collector import Event
from context_ledger_r3 import EvidenceConflict
from context_lp_r3 import build_context_model, run_context_document, validate_document
from stage1d_context import REQUIRED, build_document, required_context_windows

A, B, S, T, X = ['0x' + s * 40 for s in '12345']


def event(n, sender, receiver, amount, block, index=0, fee=0, success=True):
    tx = '0x' + format(n, '064x')
    return Event('eip155:1:tx:' + tx + ':top', tx, sender, receiver,
                 'native:eip155:1', amount, block, index, 1000 + block,
                 gas_raw=fee, success=success, provenance='SYNTHETIC_CONTEXT_FACT')


def base(second=True):
    seed = event(1, S, A, 80, 10)
    out = event(2, A, T, 90, 11)
    candidates = [seed, out] if second else [seed]
    query = {'query_id': 'synthetic:new:context', 'name': 'new_context',
             'seed_event_id': seed.event_id, 'seed_amount_raw': '80'}
    collection = {'query_id': query['query_id'], 'status': 'COMPLETED',
        'candidate_events': candidates, 'context_events': [], 'gaps': [],
        'unresolved_frontier': [], 'stops': [{'reason': 'FIRST_IDENTIFIED_SERVICE',
            'entry_event_id': out.event_id, 'state': {'address': T},
            'identity': {'kind': 'SERVICE'}}] if second else []}
    return query, collection, seed, out


def anchors(entries):
    balances = {addr + ':' + str(block): {'result': hex(value)} for addr, block, value in entries}
    headers = {block: {'number': hex(block), 'hash': '0x' + format(block, '064x')}
               for addr, block, value in entries}
    return balances, headers


def complete(plan):
    return [{'address': row['address'], 'start_block': row['ledger_start_block'],
        'end_block': row['ledger_end_block'], 'data_type': kind, 'status': 'COMPLETE',
        'pagination_complete': True, 'evidence_ids': ['SYNTHETIC_FULL_NATIVE_SCOPE']}
        for row in plan['rows'] for kind in REQUIRED]


def raw_transaction(observed, **extra):
    return dict({'record_type': 'transaction', 'tx_hash': observed.tx_hash,
        'block_number': observed.block, 'tx_index': observed.tx_index,
        'from_address': observed.sender, 'to_address': observed.recipient,
        'value_raw': str(observed.amount_raw), 'success': observed.success,
        'gas_used': '2', 'effective_gas_price': '1', 'input_data': '0x',
        'evidence_ids': ['SYNTHETIC_RAW_R3_LEDGER']}, **extra)


def raw_trace(observed, path, subtraces=0, **extra):
    return dict({'record_type': 'trace', 'tx_hash': observed.tx_hash,
        'block_number': observed.block, 'tx_index': observed.tx_index,
        'from_address': observed.sender, 'to_address': observed.recipient,
        'value_raw': str(observed.amount_raw), 'success': observed.success,
        'trace_address': path, 'trace_type': 'call', 'call_type': 'call',
        'subtraces': subtraces, 'tx_success': observed.success,
        'evidence_ids': ['SYNTHETIC_RAW_R3_TRACE']}, **extra)


class Stage1DContextTests(unittest.TestCase):
    def test_known_balances_reach_existing_solver(self):
        q, c, seed, out = base()
        b, h = anchors([(A, 9, 20), (A, 11, 10)])
        result = build_document(q, c, [], b, h, {}, {T: {'kind': 'SERVICE'}},
            coverage=complete(required_context_windows(q, c)))
        doc = result['model_input']
        self.assertEqual(result['completion_status'], 'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')
        validate_document(doc)
        solved = run_context_document(doc)
        interval = solved['variants']['BEST_AVAILABLE_CONTEXT']['all_service_joint']
        self.assertEqual((interval['lower_raw'], interval['upper_raw']), ('70', '80'))
        self.assertTrue(solved['structural_nesting']['nested'])

    def test_seed_included_anchor_is_rejected(self):
        q, c, _, _ = base()
        with self.assertRaises(EvidenceConflict):
            build_document(q, c, [], {A + ':9': {'result': '0x64', 'anchor_includes_seed': True}})

    def test_declared_initial_seed_block_is_rejected(self):
        q, c, _, _ = base()
        q['initial_anchors'] = [{'address': A, 'block_number': 10}]
        with self.assertRaises(EvidenceConflict): build_document(q, c, [])

    def test_unknown_initial_balance_is_local(self):
        q, c, seed, _ = base(False)
        internal = event(3, A, B, 50, 11)
        out = event(4, B, T, 30, 12)
        c['candidate_events'] += [internal, out]
        c['stops'] = [{'reason': 'FIRST_IDENTIFIED_SERVICE', 'entry_event_id': out.event_id,
                      'state': {'address': T}, 'identity': {'kind': 'SERVICE'}}]
        b, h = anchors([(A, 9, 20)])
        doc = build_document(q, c, [], b, h)['model_input']
        accounts = {a['account_id']: a for a in doc['accounts']}
        self.assertEqual(accounts[A + '|ETH']['initial_actual_balance_raw'], '20')
        self.assertIsNone(accounts[B + '|ETH']['initial_actual_balance_raw'])
        self.assertEqual(build_context_model(doc).metadata['known_initial_balances'], 1)

    def test_known_gas_is_paid_once_by_actual_payer(self):
        q, c, _, _ = base()
        out = event(2, A, T, 90, 11, fee=2)
        c['candidate_events'][1] = out
        c['context_events'] = [out]
        b, h = anchors([(A, 9, 20), (A, 11, 8)])
        doc = build_document(q, c, [out], b, h)['model_input']
        fees = [f for tx in doc['transactions'] for f in tx['fees']]
        self.assertEqual(len(fees), 1)
        self.assertEqual((fees[0]['payer_account'], fees[0]['amount_raw']), (A + '|ETH', '2'))
        self.assertIn(fees[0]['fee_id'], build_context_model(doc).event_variables)

    def test_failed_transaction_has_no_value_but_keeps_fee(self):
        q, c, _, _ = base()
        failed = event(3, A, X, 5000, 10, index=1, fee=3, success=False)
        b, h = anchors([(A, 9, 20), (A, 11, 7)])
        doc = build_document(q, c, [failed], b, h)['model_input']
        transaction = next(tx for tx in doc['transactions'] if tx['tx_id'] == failed.tx_hash)
        self.assertEqual(transaction['flows'], [])
        self.assertEqual(transaction['fees'][0]['amount_raw'], '3')
        self.assertNotIn(failed.event_id, build_context_model(doc).event_variables)

    def test_preseed_normal_income_is_retained_with_causal_zero_basis(self):
        q, c, seed, _ = base()
        c['candidate_events'][0] = event(1, S, A, 80, 10, index=1)
        normal = event(5, X, A, 100, 10, index=0)
        b, h = anchors([(A, 9, 20), (A, 11, 110)])
        doc = build_document(q, c, [normal], b, h)['model_input']
        flow = next(f for tx in doc['transactions'] for f in tx['flows'] if f['event_id'] == normal.event_id)
        self.assertEqual(flow['role'], 'BACKGROUND_NORMAL')
        self.assertTrue(flow['source_zero_basis'])
        self.assertEqual(run_context_document(doc)['variants']['BEST_AVAILABLE_CONTEXT']['all_service_joint']['lower_raw'], '0')

    def test_unknown_postseed_income_keeps_external_source_variable(self):
        q, c, seed, _ = base()
        boundary = event(6, A, X, 10, 10, index=1)
        returning = event(7, X, A, 10, 10, index=2)
        b, h = anchors([(A, 9, 20), (A, 11, 10)])
        doc = build_document(q, c, [boundary, returning], b, h)['model_input']
        flows = {f['event_id']: f for tx in doc['transactions'] for f in tx['flows']}
        self.assertEqual(flows[returning.event_id]['role'], 'UNKNOWN_EXTERNAL_INCOMING')
        self.assertNotIn('source_zero_basis', flows[returning.event_id])
        self.assertEqual(flows[boundary.event_id]['role'], 'BOUNDARY_OUTFLOW')
        self.assertIn(returning.event_id, build_context_model(doc).event_variables)

    def test_balance_conflict_blocks_without_relaxing_known_input(self):
        q, c, _, _ = base()
        b, h = anchors([(A, 9, 20), (A, 11, 999)])
        result = build_document(q, c, [], b, h)
        self.assertEqual(result['completion_status'], 'EVIDENCE_CONFLICT_MODEL_BLOCKED')
        self.assertEqual(result['model_input']['accounts'][0]['initial_actual_balance_raw'], '20')
        with self.assertRaises(ValueError): build_context_model(result['model_input'])

    def test_no_target_is_legal_with_observed_seed(self):
        q, c, _, _ = base(False)
        b, h = anchors([(A, 9, 0), (A, 10, 80)])
        doc = build_document(q, c, [], b, h)['model_input']
        self.assertEqual(doc['objective_groups'], {})
        result = run_context_document(doc)
        self.assertEqual(result['variants']['BEST_AVAILABLE_CONTEXT']['all_service_joint']['status'], 'NO_OBSERVED_TARGET')

    def test_partial_collection_remains_conditional_and_retains_frontier(self):
        q, c, _, _ = base()
        c.update(status='INCOMPLETE_RESOURCE_LIMIT', unresolved_frontier=[{'address': A}])
        b, h = anchors([(A, 9, 20), (A, 11, 10)])
        result = build_document(q, c, [], b, h)
        self.assertEqual(result['completion_status'], 'PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS')
        gap = next(x for x in result['evidence_gaps'] if x['type'] == 'CANDIDATE_COLLECTION_INCOMPLETE')
        self.assertEqual(gap['unresolved_frontier'], [{'address': A}])
        self.assertTrue(build_context_model(result['model_input']))

    def test_windows_exclude_service_and_expand_known_context(self):
        q, c, _, _ = base()
        known = event(8, X, A, 15, 12)
        plan = required_context_windows(q, c, [known])
        self.assertEqual([x['address'] for x in plan['rows']], [A])
        self.assertEqual((plan['rows'][0]['before_anchor_block'], plan['rows'][0]['after_anchor_block']), (9, 12))
        self.assertEqual(plan['rows'][0]['required_coverage'], REQUIRED)

    def test_receipt_physical_conflict_is_not_silently_used(self):
        q, c, _, out = base()
        receipt = {'transactionHash': out.tx_hash, 'blockNumber': hex(11), 'transactionIndex': hex(0),
            'from': A, 'to': T, 'status': '0x1', 'gasUsed': '0x2', 'effectiveGasPrice': '0x1'}
        result = build_document(q, c, [], receipts={out.tx_hash: receipt})
        self.assertEqual(result['completion_status'], 'EVIDENCE_CONFLICT_MODEL_BLOCKED')

    def test_missing_balance_response_is_not_zero(self):
        q, c, _, _ = base()
        doc = build_document(q, c, [], {A + ':9': {'error': {'message': 'missing historical state'}}})['model_input']
        self.assertIsNone(doc['accounts'][0]['initial_actual_balance_raw'])
        self.assertTrue(any(g['type'] == 'HISTORICAL_BALANCE_REQUEST_UNAVAILABLE' for g in doc['gaps']))

    def test_verified_service_seed_uses_original_event_and_all_methods(self):
        from run_stage1c import METHODS, dispatch, normalized
        from stage1c_output_contract import accept_method_results
        q, c, seed, _ = base(False)
        c['stops'] = [{'reason': 'FIRST_IDENTIFIED_SERVICE', 'entry_event_id': seed.event_id,
                      'state': {'address': A}, 'identity': {'kind': 'SERVICE'}}]
        labels = {A: {'kind': 'SERVICE'}}
        plan = required_context_windows(q, c, label_snapshot=labels)
        self.assertEqual(plan['rows'], [])
        doc = build_document(q, c, [], label_snapshot=labels)['model_input']
        self.assertEqual(doc['initial_balances'], {})
        self.assertEqual(doc['events'][0]['id'], seed.event_id)
        self.assertEqual(doc['objective_groups'], {A + '|ETH': [seed.event_id]})
        expected = {'sample_id': doc['scenario_id'], 'query_id': doc['query_id'],
            'input_fact_hash': hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest(),
            'scope_hash': 'f' * 64, 'label_version': 'SYNTHETIC_FROZEN_SERVICE_LABEL',
            'method_versions': {method: 'context-adapter-test-v1' for method in METHODS}}
        results = {}
        for method in METHODS:
            value = normalized(dispatch(doc, method))
            value.update({k: expected[k] for k in ('sample_id', 'query_id', 'input_fact_hash', 'scope_hash', 'label_version')})
            value.update(method_id=method, method_version=expected['method_versions'][method])
            results[method] = value
        accepted = accept_method_results(doc, results, expected_identity=expected)
        self.assertTrue(accepted['passed'], accepted)

    def test_service_seed_rejects_downstream_candidate_history(self):
        q, c, seed, _ = base()
        c['stops'].append({'reason': 'FIRST_IDENTIFIED_SERVICE', 'entry_event_id': seed.event_id,
                          'state': {'address': A}, 'identity': {'kind': 'SERVICE'}})
        with self.assertRaises(EvidenceConflict): build_document(q, c, [])

    def test_verified_internal_trace_keeps_original_physical_id(self):
        q, c, _, _ = base()
        tx = '0x' + format(20, '064x')
        internal = Event('eip155:1:tx:' + tx + ':trace:0_1', tx, A, T,
            'native:eip155:1', 90, 11, 0, 1011, kind='internal', trace_address='0_1',
            provenance='DUNE_INDEX_stage1b-dune-index-adapter-1.0')
        c['candidate_events'][1] = internal
        c['stops'][0]['entry_event_id'] = internal.event_id
        b, h = anchors([(A, 9, 20), (A, 11, 10)])
        doc = build_document(q, c, [], b, h)['model_input']
        self.assertIn(internal.event_id, build_context_model(doc).event_variables)
        self.assertEqual(doc['objective_groups'][T + '|ETH'], [internal.event_id])

    def test_unverified_successful_child_does_not_gain_ancestry_evidence(self):
        q, c, _, _ = base()
        tx = '0x' + format(20, '064x')
        internal = Event('eip155:1:tx:' + tx + ':trace:0', tx, A, T,
            'native:eip155:1', 90, 11, 0, 1011, kind='internal', trace_address='0',
            provenance='CHILD_SUCCESS_ONLY')
        c['candidate_events'][1] = internal
        c['stops'][0]['entry_event_id'] = internal.event_id
        result = build_document(q, c, [])
        self.assertEqual(result['completion_status'], 'EVIDENCE_CONFLICT_MODEL_BLOCKED')
        self.assertTrue(any(x['reason'] == 'ANCESTOR_SUCCESS_EVIDENCE_MISSING' for x in result['normalization_exclusions']))

    def test_real_receipt_supplies_missing_fee(self):
        q, c, _, _ = base()
        out = event(2, A, T, 90, 11, fee=None)
        c['candidate_events'][1] = out
        receipt = {'transactionHash': out.tx_hash, 'blockNumber': hex(11), 'transactionIndex': '0x0',
            'from': A, 'to': T, 'status': '0x1', 'gasUsed': '0x2', 'effectiveGasPrice': '0x1'}
        b, h = anchors([(A, 9, 20), (A, 11, 8)])
        doc = build_document(q, c, [], b, h, {out.tx_hash: receipt})['model_input']
        self.assertEqual(doc['transactions'][-1]['fees'][0]['amount_raw'], '2')
        self.assertFalse(doc['fact_conflicts'])

    def test_missing_fee_is_a_specific_gap_and_not_a_zero_fee_record(self):
        q, c, _, _ = base()
        out = event(2, A, T, 90, 11, fee=None)
        c['candidate_events'][1] = out
        doc = build_document(q, c, [])['model_input']
        self.assertEqual(doc['transactions'][-1]['fees'], [])
        self.assertTrue(any(g['type'] == 'ACTUAL_TRANSACTION_FEE_MISSING' and g['account_id'] == A + '|ETH' for g in doc['gaps']))

    def test_conflicting_header_cannot_bind_balance(self):
        q, c, _, _ = base()
        b, h = anchors([(A, 9, 20), (A, 11, 10)])
        h['duplicate'] = {'number': '0x9', 'hash': '0x' + 'f' * 64}
        with self.assertRaises(EvidenceConflict): build_document(q, c, [], b, h)

    def test_service_stop_must_match_final_label_snapshot(self):
        q, c, _, _ = base()
        with self.assertRaises(EvidenceConflict):
            build_document(q, c, [], label_snapshot={T: {'identity_class': 'UNKNOWN'}})

    def test_raw_r3_effective_price_is_retained_and_paid_once(self):
        q, c, _, _ = base()
        out = event(2, A, T, 90, 11, fee=None)
        c['candidate_events'][1] = out
        b, h = anchors([(A, 9, 20), (A, 11, 8)])
        result = build_document(q, c, [raw_transaction(out)], b, h)
        self.assertFalse(result['fact_conflicts'])
        fees = [fee for tx in result['model_input']['transactions'] for fee in tx['fees']]
        self.assertEqual([(f['payer_account'], f['amount_raw']) for f in fees], [(A + '|ETH', '2')])
        self.assertEqual(result['ledger_reconciliation'][0]['difference_raw'], '0')

    def test_raw_null_staticcall_preserves_evidence_without_filling_amount(self):
        q, c, _, out = base()
        rows = [raw_trace(out, [], 1), raw_trace(out, [0], call_type='staticcall',
                                              value_raw=None, from_address=T, to_address=X)]
        original = copy.deepcopy(rows)
        result = build_document(q, c, rows)
        self.assertFalse(result['fact_conflicts'])
        self.assertEqual(rows, original)
        excluded = [x for x in result['normalization_exclusions']
                    if x['reason'] == 'CALL_CONTEXT_VALUE_NOT_PHYSICAL_TRANSFER']
        self.assertEqual(len(excluded), 1)
        self.assertIsNone(excluded[0]['row']['value_raw'])
        self.assertTrue(excluded[0]['missing_call_context_value_not_filled'])
        self.assertEqual(len([f for tx in result['model_input']['transactions'] for f in tx['flows']]), 2)
        check = next(x for x in result['raw_trace_projection_checks'] if not x['trace_address'])
        self.assertEqual((check['original_children'], check['normalizer_projection_children']), (1, 0))

    def test_true_raw_child_count_conflict_survives_semantic_projection(self):
        q, c, _, out = base()
        rows = [raw_trace(out, [], 2), raw_trace(out, [0], call_type='staticcall', value_raw=None)]
        result = build_document(q, c, rows)
        self.assertEqual(result['completion_status'], 'EVIDENCE_CONFLICT_MODEL_BLOCKED')
        self.assertTrue(any(x['reason'] == 'TRACE_TREE_CHILD_COUNT_CONFLICT' for x in result['fact_conflicts']))

    def test_null_physical_transfer_is_rejected_not_zero_filled(self):
        q, c, _, out = base()
        with self.assertRaises(ValueError):
            build_document(q, c, [raw_transaction(out, value_raw=None)])
        with self.assertRaises(ValueError):
            build_document(q, c, [raw_trace(out, [], value_raw=None)])

    def test_raw_null_failed_ancestor_does_not_release_descendant_value(self):
        q, c, _, out = base()
        rows = [raw_trace(out, [], 1),
                raw_trace(out, [0], 1, call_type='staticcall', value_raw=None,
                          success=False, error='Reverted'),
                raw_trace(out, [0, 0], value_raw='700', from_address=X, to_address=A)]
        result = build_document(q, c, rows)
        self.assertFalse(result['fact_conflicts'])
        self.assertTrue(any(x['reason'] == 'FAILED_FRAME_OR_ANCESTOR_ROLLBACK'
                            for x in result['normalization_exclusions']))
        self.assertEqual(len([f for tx in result['model_input']['transactions'] for f in tx['flows']]), 2)

    def test_raw_creation_and_refund_endpoints_reach_existing_normalizer(self):
        from stage1d_context import _native_rows
        from context_ledger_r3 import normalize_rows
        observed = event(30, A, X, 5, 11)
        rows = [raw_trace(observed, [], 2),
                raw_trace(observed, [0], trace_type='create', created_address=B,
                          to_address=None, value_raw='3'),
                raw_trace(observed, [1], trace_type='selfdestruct', created_address=B,
                          refund_address=A, value_raw='2')]
        raw, _, _, _, conflicts = _native_rows(rows)
        normalized = normalize_rows(raw)
        self.assertFalse(conflicts + normalized['conflicts'])
        flows = {tuple(f['trace_address']): f for f in normalized['flows']}
        self.assertEqual(flows[(0,)]['recipient'], B)
        self.assertEqual((flows[(1,)]['sender'], flows[(1,)]['recipient']), (B, A))

    def test_merged_dune_provenance_preserves_verified_internal_identity(self):
        q, c, _, _ = base()
        tx = '0x' + format(20, '064x')
        internal = Event('eip155:1:tx:' + tx + ':trace:0_1', tx, A, T,
            'native:eip155:1', 90, 11, 0, 1011, kind='internal', trace_address='0_1',
            provenance=json.dumps(['SAVED_CONTEXT', 'DUNE_INDEX_stage1b-dune-index-adapter-1.0']))
        c['candidate_events'][1] = internal
        c['stops'][0]['entry_event_id'] = internal.event_id
        result = build_document(q, c, [])
        self.assertFalse(result['fact_conflicts'])
        self.assertIn(internal.event_id, build_context_model(result['model_input']).event_variables)


if __name__ == '__main__': unittest.main()
