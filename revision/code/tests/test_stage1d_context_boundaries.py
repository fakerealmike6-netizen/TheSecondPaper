import copy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

C = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(C / 'src'))
sys.path.insert(0, str(C / 'tests'))
import stage1d_context
from context_ledger_r3 import EvidenceConflict
from context_lp_r3 import build_context_model, run_context_document, validate_document
from test_stage1d_context import A, B, S, T, X, anchors, base, complete, event
from stage1d_context import exclude_verified_protocol_windows


def fixture():
    q, c, seed, _ = base(False)
    out = event(2, A, X, 50, 11, fee=2)
    service = event(3, A, T, 20, 12)
    c['candidate_events'] += [out, service]
    c['stops'] = [
        {'reason': 'PROTOCOL_BOUNDARY', 'entry_event_id': out.event_id,
         'state': {'address': X}, 'identity': {'kind': 'UNSUPPORTED_PROTOCOL'}},
        {'reason': 'FIRST_IDENTIFIED_SERVICE', 'entry_event_id': service.event_id,
         'state': {'address': T}, 'identity': {'kind': 'SERVICE'}}]
    labels = {X: {'kind': 'UNSUPPORTED_PROTOCOL', 'observation_ids': ['SYNTHETIC_ADOPTED']},
              T: {'kind': 'SERVICE'}}
    return q, c, labels, seed, out, service


def build(q, c, labels, rows, balances, headers):
    plan = stage1d_context.necessary_context_windows(q, c, rows, labels)
    return stage1d_context.build_document(q, c, rows, balances, headers,
        label_snapshot=labels, coverage=complete(plan), context_plan=plan)


class ExistingProtocolBoundaryTests(unittest.TestCase):
    def test_only_verified_protocol_window_removed_and_input_unchanged(self):
        q, c, labels, _, _, _ = fixture()
        history = [event(4, X, B, 1234, 1000)]
        plan = stage1d_context.required_context_windows(q, c, history, labels)
        before = copy.deepcopy((q, c, labels, history, plan))
        result = exclude_verified_protocol_windows(q, c, plan, labels)
        self.assertEqual((q, c, labels, history, plan), before)
        self.assertEqual(result['rows'], [r for r in plan['rows'] if r['address'] != X])
        self.assertEqual([r['address'] for r in result['rows']], [A])
        self.assertEqual(result['objective_groups'], plan['objective_groups'])
        self.assertEqual(result['service_terminals_excluded'], [T])
        self.assertEqual(result['protocol_boundaries_excluded'], [X])

    def test_real_amount_fee_known_balance_and_boundary_return_reach_existing_lp(self):
        q, c, labels, seed, out, service = fixture()
        returning = event(4, X, A, 10, 11, index=1)
        balances, headers = anchors([(A, 9, 20), (A, 12, 38), (X, 10, 1000)])
        result = build(q, c, labels, [returning], balances, headers)
        doc = result['model_input']
        self.assertFalse(doc['fact_conflicts'])
        self.assertEqual([a['account_id'] for a in doc['accounts']], [A + '|ETH'])
        self.assertEqual(doc['accounts'][0]['initial_actual_balance_raw'], '20')
        self.assertEqual(len(result['balance_anchors']), 3)  # Unused platform anchor remains evidence.
        flows = {f['event_id']: f for t in doc['transactions'] for f in t['flows']}
        self.assertEqual(flows[out.event_id]['amount_raw'], '50')
        self.assertEqual(flows[out.event_id]['role'], 'CANDIDATE')
        self.assertIsNone(flows[out.event_id]['to_account'])
        self.assertNotIn('terminal_target', flows[out.event_id])
        self.assertEqual(flows[returning.event_id]['role'], 'UNKNOWN_EXTERNAL_INCOMING')
        self.assertEqual(flows[returning.event_id]['amount_raw'], '10')
        self.assertNotIn('source_zero_basis', flows[returning.event_id])
        fees = [f for t in doc['transactions'] for f in t['fees']]
        self.assertEqual([(f['payer_account'], f['amount_raw']) for f in fees],
                         [(A + '|ETH', '2'), (A + '|ETH', '0')])
        self.assertEqual(doc['objective_groups'], {T + '|ETH': [service.event_id]})
        validate_document(doc)
        model = build_context_model(doc)
        self.assertIn(returning.event_id, model.event_variables)
        solved = run_context_document(doc)
        self.assertTrue(solved['all_same_graph_comparisons_passed'])

    def test_unknown_external_return_has_no_new_source_when_no_prior_exit(self):
        q, c, labels, _, _, _ = fixture()
        returning = event(4, X, A, 10, 10, index=1)
        b, h = anchors([(A, 9, 20), (A, 12, 38)])
        doc = build(q, c, labels, [returning], b, h)['model_input']
        model = build_context_model(doc)
        # Exact original LP, not a replacement accounting rule.
        from context_lp_r3 import solve_context_interval
        result = solve_context_interval(model, doc, [returning.event_id])
        self.assertEqual(result['upper_raw'], '0')

    def test_all_seven_frozen_methods_accept_same_boundary_document(self):
        from run_stage1c import METHODS, dispatch, normalized
        q, c, labels, _, _, _ = fixture()
        b, h = anchors([(A, 9, 20), (A, 12, 38), (X, 10, 1000)])
        doc = build(q, c, labels, [event(4, X, A, 10, 11, index=1)], b, h)['model_input']
        original = copy.deepcopy(doc)
        self.assertEqual(len(METHODS), 7)
        for method in METHODS:
            with self.subTest(method=method):
                result = normalized(dispatch(doc, method))
                self.assertEqual(result['status'], 'COMPLETED')
                self.assertEqual(doc, original)

    def test_missing_label_and_changed_label_block(self):
        q, c, labels, _, _, _ = fixture()
        plan = stage1d_context.required_context_windows(q, c, [], labels)
        for variant in ({}, {X: {'kind': 'UNKNOWN'}}):
            with self.assertRaises(EvidenceConflict):
                exclude_verified_protocol_windows(q, c, plan, variant)

    def test_no_protocol_stop_does_not_drop_high_volume_unknown(self):
        q, c, labels, _, _, _ = fixture()
        c['stops'] = [x for x in c['stops'] if x['reason'] != 'PROTOCOL_BOUNDARY']
        labels[X] = {'kind': 'UNKNOWN'}
        plan = stage1d_context.required_context_windows(q, c, [event(4, X, B, 999, 1000)], labels)
        self.assertEqual(exclude_verified_protocol_windows(q, c, plan, labels), plan)

    def test_supported_protocol_label_without_stop_is_not_cut(self):
        q, c, labels, _, _, _ = fixture()
        c['stops'] = [x for x in c['stops'] if x['reason'] != 'PROTOCOL_BOUNDARY']
        labels[X] = {'kind': 'SUPPORTED_PROTOCOL', 'instance_certificate': 'SYNTHETIC'}
        plan = stage1d_context.required_context_windows(q, c, [], labels)
        self.assertEqual(exclude_verified_protocol_windows(q, c, plan, labels), plan)

    def test_supported_protocol_conflicting_with_stale_stop_blocks(self):
        q, c, labels, _, _, _ = fixture()
        plan = stage1d_context.required_context_windows(q, c, [], labels)
        labels[X] = {'kind': 'SUPPORTED_PROTOCOL', 'instance_certificate': 'SYNTHETIC'}
        with self.assertRaises(EvidenceConflict): exclude_verified_protocol_windows(q, c, plan, labels)

    def test_candidate_continuation_from_stopped_protocol_is_not_silently_deleted(self):
        q, c, labels, _, _, _ = fixture()
        c['candidate_events'].append(event(4, X, A, 10, 11, index=1))
        plan = stage1d_context.required_context_windows(q, c, [], labels)
        with self.assertRaises(EvidenceConflict): exclude_verified_protocol_windows(q, c, plan, labels)

    def test_known_balance_conflict_remains_blocked(self):
        q, c, labels, _, _, _ = fixture()
        b, h = anchors([(A, 9, 20), (A, 12, 999)])
        result = build(q, c, labels, [], b, h)
        self.assertEqual(result['completion_status'], 'EVIDENCE_CONFLICT_MODEL_BLOCKED')
        self.assertEqual(result['model_input']['accounts'][0]['initial_actual_balance_raw'], '20')

    def test_no_service_target_is_legal_and_protocol_does_not_become_target(self):
        q, c, labels, _, _, _ = fixture()
        c['candidate_events'].pop()
        c['stops'].pop()
        b, h = anchors([(A, 9, 20), (A, 11, 48)])
        doc = build(q, c, labels, [], b, h)['model_input']
        self.assertEqual(doc['objective_groups'], {})
        self.assertEqual(run_context_document(doc)['variants']['BEST_AVAILABLE_CONTEXT']['all_service_joint']['status'], 'NO_OBSERVED_TARGET')





class ProtocolBoundaryIntegrationTests(unittest.TestCase):
    def _facts(self, harness):
        from dataclasses import asdict
        from page_attempts import atomic_json
        from test_stage1d_context import raw_transaction
        q, c, labels, seed, out, service = fixture()
        c['candidate_events'] = [asdict(e) for e in c['candidate_events']]
        c['states'] = [{'state': {'address': a}} for a in (A, X, T)]
        harness.c = c
        harness.seed, harness.out = seed, service
        atomic_json(harness.qdir / 'collection.json', c)
        returning = event(4, X, A, 10, 11, index=1)
        atomic_json(harness.root / 'ledger_rows.json', [raw_transaction(returning)])
        old = stage1d_context.required_context_windows(harness.q, c, [returning], labels)
        router = next(r for r in old['rows'] if r['address'] == X)
        router.update(ledger_end_block=99, after_anchor_block=99)
        atomic_json(harness.root / 'context_plan.json', old)
        return labels, returning, (harness.root / 'context_plan.json').read_bytes()

    def test_online_plans_and_builds_from_one_label_snapshot_and_never_readds_protocol(self):
        import stage1d_context_online as online
        from test_stage1d_context_online import ContextOnlineTests
        from page_attempts import atomic_json
        h = ContextOnlineTests(); h.setUp(); self.addCleanup(h.doCleanups)
        labels, returning, old = self._facts(h)
        b, headers = anchors([(A, 9, 20), (A, 12, 38), (X, 10, 1000)])
        for number, header in headers.items():
            header['timestamp'] = hex(1693612790 + (number - 10) * 12)
        atomic_json(h.root / 'balances.json', b)
        atomic_json(h.root / 'headers.json', headers)
        h.capacity = 0
        calls = []
        def lookup(address):
            calls.append(address)
            return labels.get(address, {'kind': 'UNKNOWN'})
        with patch.object(online, 'Labels', return_value=lookup):
            result = h.invoke(output_variant='round_boundary', account_selection='all')
        folder = h.w / result['context_folder']
        plan = json.loads((folder / 'context_plan.json').read_text())
        doc = json.loads((folder / 'model_input.json').read_text())
        self.assertEqual(sorted(calls), sorted([A, X, T]))
        self.assertEqual([A], [r['address'] for r in plan['rows']])
        self.assertEqual([X], plan['protocol_boundaries_excluded'])
        self.assertEqual(plan['rows'], doc['stage1d_context_adapter']['context_plan']['rows'])
        self.assertEqual(labels.get(X), json.loads((folder / 'label_snapshot.json').read_text())[X])
        self.assertEqual(old, (h.root / 'context_plan.json').read_bytes())
        self.assertEqual(3, len(json.loads((folder / 'balances.json').read_text())))
        self.assertEqual('20', doc['accounts'][0]['initial_actual_balance_raw'])
        self.assertEqual(1, len(h.sqls))
        self.assertNotIn('(' + X + ',', h.sqls[0])
        self.assertIn('(' + A + ',10,12)', h.sqls[0])
        flow = next(f for tx in doc['transactions'] for f in tx['flows'] if f['event_id'] == returning.event_id)
        self.assertEqual(('UNKNOWN_EXTERNAL_INCOMING', '10'), (flow['role'], flow['amount_raw']))

    def test_recovery_preserves_prior_ordinary_window_and_binds_same_label_for_model(self):
        import stage1d_context_recovery as recovery
        from test_stage1d_context_recovery import RunTests
        from page_attempts import atomic_json
        h = RunTests(); h.setUp(); self.addCleanup(h.doCleanups)
        labels, returning, _ = self._facts(h)
        old = json.loads((h.root / 'context_plan.json').read_text())
        ordinary = next(r for r in old['rows'] if r['address'] == A)
        ordinary.update(ledger_end_block=15, after_anchor_block=15)
        atomic_json(h.root / 'context_plan.json', old)
        b, _ = anchors([(A, 9, 20), (A, 15, 38), (X, 10, 1000)])
        atomic_json(h.root / 'balances.json', b)
        calls = []
        def lookup(address):
            calls.append(address)
            return labels.get(address, {'kind': 'UNKNOWN'})
        with patch.object(recovery, 'Labels', return_value=lookup):
            result = h.invoke(max_batches=1)
        folder = h.w / result['context_folder']
        plan = json.loads((folder / 'context_plan.json').read_text())
        final_plan = json.loads((folder / 'post_export_context_requirements.json').read_text())
        doc = json.loads((folder / 'model_input.json').read_text())
        self.assertEqual(sorted(calls), sorted([A, X, T]))
        self.assertEqual([ordinary], plan['rows'])
        self.assertEqual([ordinary], final_plan['rows'])
        self.assertEqual(plan['rows'], doc['stage1d_context_adapter']['context_plan']['rows'])
        self.assertEqual(3, len(json.loads((folder / 'balances.json').read_text())))
        self.assertEqual('20', doc['accounts'][0]['initial_actual_balance_raw'])
        self.assertEqual(1, len(h.calls))
        sql = (h.calls[0].parent / 'query.sql').read_text()
        self.assertNotIn('(' + X + ',', sql)
        self.assertIn('(' + A + ',10,15)', sql)
        freeze = json.loads(h.calls[0].read_text())
        self.assertTrue(any(x['path'].endswith('/label_snapshot.json') for x in freeze['dependencies']))
        self.assertEqual(h.before, h.sentinel.read_bytes())


if __name__ == '__main__': unittest.main()
