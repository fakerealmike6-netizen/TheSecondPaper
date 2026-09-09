"""Bounded synthetic state/ledger fixtures. No network, LP solve or dispatch."""
from copy import deepcopy
from dataclasses import asdict, replace
import unittest

from collector import Scope, State
from context_ledger_r3 import EvidenceConflict
from context_lp_r3 import audit_context_witness, build_context_model
import stage1d_context as adapter
import stage1d_closure_context as closure
from stage1d_multiasset_context import WETH, required_windows
from stage1d_cost_boundary_context import (AUTH, POLICY_SCHEMA, canonical,
    project_plan, validate_overlay)
from stage1d_unknown_cost_boundary import make_decision
from test_stage1d_context import A, B, S, T, X, event, anchors, complete

POLICY_SHA = 'a' * 64
REF = {'path': 'synthetic/controlled.json', 'sha256': 'b' * 64}


def fixture(action='STOP'):
    seed = event(1, S, A, 80, 10)
    out = event(2, A, X, 50, 11, fee=2)
    service = event(3, A, T, 20, 12)
    q = {'query_id': 'synthetic:cost-context', 'name': 'synthetic_cost',
         'seed_event_id': seed.event_id, 'seed_amount_raw': '80',
         'start_block': 1, 'end_block': 1000, 'start_time_utc': 1000,
         'end_time_utc': 2000, 'max_acquisition_depth': 5,
         'window_mode': 'QUERY_ARRIVAL_WINDOW_SECONDS_V1', 'local_window_seconds': 100}
    scope = Scope.from_policy(q); q.update(scope_id=scope.scope_id, scope_hash=scope.scope_hash)
    labels = {A: {'kind': 'UNKNOWN'}, X: {'kind': 'UNKNOWN'}, T: {'kind': 'SERVICE'}}
    c = {'query_id': q['query_id'], 'status': 'COMPLETED_WITH_COST_BOUNDARIES',
         'candidate_events': [asdict(e) for e in (seed, out, service)], 'context_events': [],
         'states': [], 'stops': [], 'unresolved_frontier': [], 'gaps': [],
         'metrics': {'scope_freeze': scope.freeze_dict(), 'scope_hash': scope.scope_hash, 'scope_id': scope.scope_id},
         'cost_boundary_policy': {'schema_version': POLICY_SCHEMA, 'authorization_id': AUTH,
             'policy_sha256': POLICY_SHA, 'enabled': True}, 'cost_boundary_decisions': []}
    add_state(c, q, seed, 0, labels[A], 'BYPASS')
    add_state(c, q, out, 1, labels[X], action)
    service_state = asdict(State(q['query_id'], T, service.asset, service, 1, scope.local_end(service)))
    service_row = add_state(c, q, service, 1, labels[T], 'BYPASS')
    c['stops'].append({'state': service_state, 'identity': labels[T], 'cost_boundary': service_row['cost_boundary'], 'reason': 'FIRST_IDENTIFIED_SERVICE',
        'entry_event_id': service.event_id})
    return q, c, labels, seed, out, service


def add_state(c, q, arrival, depth, identity, action):
    scope = Scope.from_policy(q)
    state = asdict(State(q['query_id'], arrival.recipient, arrival.asset, arrival, depth, scope.local_end(arrival)))
    reason = {'STOP': 'UNKNOWN_CODE_COST_BOUNDARY', 'PENDING': 'IDENTITY_CHECK_PENDING',
              'CONTINUE': 'COST_BOUNDARY_NOT_PROVED', 'BYPASS': 'HISTORICAL_COMPLETED_STATE_OUTSIDE_INITIAL_FRONTIER'}[action]
    decision = make_decision(state, scope, policy_sha256=POLICY_SHA, action=action,
        reason=reason, evidence_refs=[REF])
    row = {'state': state, 'identity': deepcopy(identity), 'cost_boundary': decision}
    c['states'].append(row); c['cost_boundary_decisions'].append(decision)
    if action in ('STOP', 'PENDING'):
        saved = dict(deepcopy(row), reason=reason)
        if action == 'STOP': saved['entry_event_id'] = arrival.event_id
        c['stops' if action == 'STOP' else 'unresolved_frontier'].append(saved)
    return row


def document(action='STOP', *, return_first=False):
    q, c, labels, seed, out, service = fixture(action)
    returning = event(4, X, A, 10, 10 if return_first else 11, index=1)
    b, h = anchors([(A, 9, 20), (A, 12, 38), (X, 10, 1000)])
    plan = adapter.necessary_context_windows(q, c, [returning], labels)
    result = adapter.build_document(q, c, [returning], b, h, label_snapshot=labels,
        coverage=complete(plan), context_plan=plan)
    return result, (q, c, labels, seed, out, service, returning)


class CostContextTests(unittest.TestCase):
    def test_absent_policy_preserves_old_plan_object(self):
        plan = {'rows': [], 'sentinel': [1, 2]}
        self.assertIs(project_plan({}, {'states': []}, plan), plan)

    def test_actual_native_entry_return_fee_and_shared_reservoir(self):
        result, (q, c, labels, seed, out, service, returning) = document()
        doc = result['model_input']; flows = {f['event_id']: f for t in doc['transactions'] for f in t['flows']}
        self.assertEqual([a['account_id'] for a in doc['accounts']], [A + '|ETH'])
        self.assertEqual(flows[out.event_id]['role'], 'CANDIDATE')
        self.assertIsNone(flows[out.event_id]['to_account'])
        self.assertNotIn('terminal_target', flows[out.event_id])
        self.assertEqual(flows[returning.event_id]['role'], 'UNKNOWN_EXTERNAL_INCOMING')
        self.assertEqual(flows[returning.event_id]['amount_raw'], '10')
        self.assertNotIn('source_zero_basis', flows[returning.event_id])
        self.assertEqual(doc['objective_groups'], {T + '|ETH': [service.event_id]})
        witness = {f['event_id']: '0' for t in doc['transactions'] for f in t['flows']}
        witness.update({f['fee_id']: '0' for t in doc['transactions'] for f in t['fees']})
        witness.update({seed.event_id: '80', out.event_id: '50', returning.event_id: '10', service.event_id: '10'})
        audit = audit_context_witness(doc, witness)
        self.assertTrue(audit['exact_feasible'], audit)
        witness[out.event_id] = '0'  # Return cannot create new source from an empty pool.
        self.assertFalse(audit_context_witness(doc, witness)['exact_feasible'])
        self.assertTrue(result['cost_boundary_scope']['has_cost_boundary'])
        self.assertTrue(result['cost_boundary_scope']['scope_complete'])
        self.assertFalse(result['cost_boundary_scope']['untruncated_scope_strict_interval_claimed'])
        self.assertIn('CONDITIONAL_OBSERVED_SCOPE', doc['assumptions'][-1])
        self.assertEqual(labels[X]['kind'], 'UNKNOWN')
        model = build_context_model(doc)  # Matrix construction only; no optimizer.
        self.assertIn(returning.event_id, model.event_variables)

    def test_return_cannot_precede_any_source_exit(self):
        result, (_, _, _, seed, out, service, returning) = document(return_first=True)
        doc = result['model_input']
        witness = {f['event_id']: '0' for t in doc['transactions'] for f in t['flows']}
        witness.update({f['fee_id']: '0' for t in doc['transactions'] for f in t['fees']})
        witness.update({seed.event_id: '80', out.event_id: '50', returning.event_id: '10', service.event_id: '10'})
        self.assertFalse(audit_context_witness(doc, witness)['exact_feasible'])

    def test_pending_defers_only_its_rows_without_global_model_shutdown(self):
        result, _ = document('PENDING')
        self.assertIsNotNone(result['model_input'])
        self.assertFalse(result['cost_boundary_scope']['has_cost_boundary'])
        self.assertTrue(result['cost_boundary_scope']['has_pending_identity_or_type'])
        self.assertEqual(result['context_plan']['cost_boundary_context']['deferred_account_ids'], [X + '|ETH'])

    def test_same_existing_boundary_expression_has_identical_matrix(self):
        result, (q, c, labels, _, _, _, returning) = document()
        original = deepcopy(c)
        original.pop('cost_boundary_policy'); original.pop('cost_boundary_decisions')
        for row in original['states']: row.pop('cost_boundary', None)
        for row in original['stops']:
            if 'cost_boundary' in row:
                decision = row.pop('cost_boundary')
                if decision['action'] == 'STOP':
                    row['reason'] = 'PROTOCOL_BOUNDARY'
                    row['identity'] = {'kind': 'UNSUPPORTED_PROTOCOL'}
        labels = dict(labels, **{X: {'kind': 'UNSUPPORTED_PROTOCOL'}})
        b, h = anchors([(A, 9, 20), (A, 12, 38), (X, 10, 1000)])
        plan = adapter.necessary_context_windows(q, original, [returning], labels)
        old = adapter.build_document(q, original, [returning], b, h, label_snapshot=labels,
            coverage=complete(plan), context_plan=plan)
        new_model, old_model = build_context_model(result['model_input']), build_context_model(old['model_input'])
        self.assertEqual(new_model.variables, old_model.variables)
        self.assertEqual(new_model.eq, old_model.eq)
        self.assertEqual(new_model.rhs, old_model.rhs)

    def test_disabled_policy_bypass_does_not_remove_windows(self):
        q, c, labels, _, _, _ = fixture('BYPASS')
        c['cost_boundary_policy']['enabled'] = False
        plan = adapter.necessary_context_windows(q, c, [], labels)
        self.assertIn(X, [r['address'] for r in plan['rows']])
        self.assertFalse(plan['cost_boundary_context']['has_cost_boundary'])
        bad = fixture()[1]; bad['cost_boundary_policy']['enabled'] = False
        with self.assertRaisesRegex(EvidenceConflict, 'Disabled'):
            validate_overlay(q, bad)

    def test_stale_semantic_unit_cannot_bypass_all_stopped_holder_arrivals(self):
        q, c, labels, _, _, _ = fixture()
        plan = adapter.required_context_windows(q, c, [], labels)
        c['semantic_units'] = [{'holder': X}]
        with self.assertRaisesRegex(EvidenceConflict, 'SUPPORTED_UNIT_REACHABILITY_OPEN'):
            project_plan(q, c, plan)

    def test_other_arrival_preserves_same_address_normal_ledger(self):
        q, c, labels, _, _, _ = fixture()
        arriving = event(5, A, X, 5, 13)
        onward = event(6, X, B, 1, 14)
        c['candidate_events'] += [asdict(arriving), asdict(onward)]
        add_state(c, q, arriving, 1, labels[X], 'CONTINUE')
        add_state(c, q, onward, 2, {'kind': 'UNKNOWN'}, 'BYPASS')
        before = deepcopy(c)
        plan = adapter.necessary_context_windows(q, c, [], labels)
        self.assertIn(X, [r['address'] for r in plan['rows']])
        self.assertEqual(c, before)
        self.assertTrue(plan['cost_boundary_context']['retained_due_to_other_arrival_or_conversion'])

    def test_other_asset_keeps_real_gas_ledger(self):
        q, c, labels, _, _, _ = fixture()
        token = replace(event(7, A, X, 1, 14), asset=WETH, kind='erc20', log_index=1)
        c['candidate_events'].append(asdict(token))
        add_state(c, q, token, 1, labels[X], 'CONTINUE')
        plan = required_windows(q, c, [], labels)
        self.assertIn(X + '|ETH', [r['account_id'] for r in plan['rows']])
        self.assertIn(X + '|' + WETH, [r['account_id'] for r in plan['rows']])

    def test_token_only_stopped_receiver_does_not_restore_its_auxiliary_gas_account(self):
        q, c, labels, _, out, _ = fixture()
        token = replace(out, asset=WETH, kind='erc20', log_index=1)
        c['candidate_events'] = [asdict(token) if e['event_id'] == out.event_id else e for e in c['candidate_events']]
        c['states'] = [r for r in c['states'] if r['state']['address'] != X]
        c['cost_boundary_decisions'] = [r['cost_boundary'] for r in c['states'] if 'cost_boundary' in r]
        c['stops'] = [r for r in c['stops'] if r['state']['address'] != X]
        add_state(c, q, token, 1, labels[X], 'STOP')
        plan = required_windows(q, c, [], labels)
        self.assertNotIn(X, [r['address'] for r in plan['rows']])
        self.assertIn(A + '|ETH', [r['account_id'] for r in plan['rows']])
        self.assertEqual(set(plan['cost_boundary_context']['deferred_account_ids']), {X + '|ETH', X + '|' + WETH})

    def test_policy_hash_state_list_and_stop_table_tampering_rejected(self):
        q, c, labels, _, _, _ = fixture()
        mutations = [lambda x: x['cost_boundary_policy'].update(schema_version='invented'),
            lambda x: x['cost_boundary_policy'].update(policy_sha256='c' * 64),
            lambda x: x['cost_boundary_decisions'].pop(),
            lambda x: x['states'][1]['cost_boundary'].update(state_key='0' * 64),
            lambda x: x['stops'][0].update(entry_event_id='fake'),
            lambda x: x['stops'].pop(0)]
        for change in mutations:
            bad = deepcopy(c); change(bad)
            with self.subTest(change=change), self.assertRaises((ValueError, EvidenceConflict)):
                validate_overlay(q, bad)

    def test_seed_cost_and_unreplayed_outgoing_fail_closed(self):
        q, c, labels, seed, _, _ = fixture()
        c['candidate_events'].append(asdict(event(9, X, B, 1, 13)))
        with self.assertRaisesRegex(EvidenceConflict, 'continuation requires'):
            adapter.necessary_context_windows(q, c, [], labels)
        q, c, labels, seed, _, _ = fixture()
        c['states'] = []; c['cost_boundary_decisions'] = []; c['stops'] = []
        add_state(c, q, seed, 0, labels[A], 'STOP')
        with self.assertRaisesRegex(EvidenceConflict, 'SEED_RECEIVER_MODEL_OPEN'):
            adapter.necessary_context_windows(q, c, [], labels)

    def test_closure_binds_overlay_separately_from_base_labels(self):
        q, c, labels, _, _, _ = fixture()
        binding = closure.validate_current(q, c, labels)
        self.assertEqual(binding['cost_boundary_policy_sha256'], POLICY_SHA)
        self.assertEqual(binding['cost_boundary_decisions_sha256'], canonical(c['cost_boundary_decisions']))
        self.assertNotIn('branch_action', labels[X])

    def test_closure_pending_registers_only_explicit_conditional_scope(self):
        q, c, labels, _, _, _ = fixture('PENDING')
        returning = event(4, X, A, 10, 11, index=1)
        balances, headers = anchors([(A, 9, 20), (A, 12, 38)])
        plan = adapter.necessary_context_windows(q, c, [returning], labels)
        envelopes = {}
        for key, response in balances.items():
            address, block = key.rsplit(':', 1)
            envelopes[key] = {'request': {'method': 'eth_getBalance', 'params': [address, hex(int(block))]},
                'response': response}
        result = closure.assemble(q, c, labels, {'events': [asdict(returning)],
            'balances': envelopes, 'headers': headers, 'coverage': complete(plan)})
        self.assertEqual(result['status'], 'RUNNABLE_CONDITIONAL_CONTEXT')
        self.assertEqual(result['registration_arguments']['context_status'], 'PARTIAL_CONTEXT_WITH_EXPLICIT_ASSUMPTIONS')
        self.assertTrue(result['context_evidence']['cost_boundary_scope']['has_pending_identity_or_type'])
        self.assertFalse(result['context_evidence']['cost_boundary_scope']['has_cost_boundary'])
        self.assertFalse(result['context_evidence']['cost_boundary_scope']['scope_complete'])
        self.assertTrue(result['missing_points'])  # Missing point evidence stays explicit.

    def test_prior_cost_account_window_never_readded_other_rows_unchanged(self):
        q, c, labels, _, _, _ = fixture()
        old = adapter.required_context_windows(q, c, [], labels)
        for row in old['rows']:
            if row['address'] == X: row.update(ledger_end_block=900, after_anchor_block=900)
        result = adapter.necessary_context_windows(q, c, [], labels, prior_rows=old['rows'])
        self.assertEqual(result['rows'], [row for row in old['rows'] if row['address'] != X])
        self.assertEqual(result['cost_boundary_context']['deferred_context_rows'][0]['ledger_end_block'], 900)

    def test_old_descendant_row_not_readded_but_ordinary_return_and_window_stay(self):
        q, c, labels, _, _, _ = fixture()
        base = adapter.required_context_windows(q, c, [], labels)
        old = deepcopy(base['rows'])
        ordinary = next(r for r in old if r['address'] == A)
        ordinary.update(ledger_end_block=15, after_anchor_block=15)
        obsolete = dict(deepcopy(ordinary), account_id=B + '|ETH', address=B,
            ledger_start_block=12, before_anchor_block=11, ledger_end_block=900, after_anchor_block=900,
            inherited_evidence_ref={'path': 'synthetic/prior.json', 'sha256': 'c' * 64})
        old.append(obsolete)
        returning = event(10, B, A, 10, 14)
        result = adapter.necessary_context_windows(q, c, [returning], labels, prior_rows=old)
        expected_ordinary = deepcopy(ordinary)
        expected_ordinary['known_physical_event_ids'] = sorted(set(ordinary['known_physical_event_ids']) | {returning.event_id})
        self.assertEqual(result['rows'], [expected_ordinary])
        deferred = result['cost_boundary_context']['deferred_prior_context_rows']
        self.assertEqual(deferred[0]['original_row'], obsolete)
        projected = closure._project([asdict(returning)], result)
        self.assertEqual(projected, [asdict(returning)])
        # The old, unenabled contract retains all historical ordinary rows.
        old_c = deepcopy(c); old_c.pop('cost_boundary_policy'); old_c.pop('cost_boundary_decisions')
        for table in ('states', 'stops'):
            for row in old_c[table]: row.pop('cost_boundary', None)
        legacy = adapter.necessary_context_windows(q, old_c, [returning], labels, prior_rows=old)
        self.assertIn(B, [r['address'] for r in legacy['rows']])


if __name__ == '__main__':
    unittest.main()
