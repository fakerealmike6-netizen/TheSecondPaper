"""R2A synthetic external counterexample and evidence-order positive controls."""
from dataclasses import asdict, replace
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from collector import Collector, Event, FetchResult, NATIVE, Scope
from cache_probe import fixed_graph
from dune_observed_replay import live_fixed_graph
from event_order import order_observed_transfers, ORDER_UNRESOLVED
from lp_model import build_model, solve_interval

S, A, B, T, X, Y = ['0x' + format(i, '040x') for i in range(1, 7)]


def event(i, sender, recipient, amount, block, index):
    tx = '0x' + format(i, '064x')
    return Event('eip155:1:tx:' + tx + ':top', tx, sender, recipient,
                 NATIVE, amount, block, index, block * 12)


class Provider:
    replay_only = True

    def __init__(self, events):
        self.events = events

    def fetch_interval(self, address, asset, start_block, end_block, **kw):
        return FetchResult([e for e in self.events if address in (e.sender, e.recipient)
                            and start_block <= e.block <= end_block],
                           [{'complete': True, 'basis': 'SYNTHETIC_R2_ORDER_CONTROL'}],
                           True, cache_hits=1)


def shared_balance_case(late_hash, late_index=None, target_index=None):
    seed = event(1, S, A, 100, 1, 0)
    early = event(2, A, B, 10, 2, 0)
    later = event(late_hash, A, B, 90, 3, late_index)
    target = event(30, B, T, 100, 3, target_index)
    result = Collector(Provider([seed, early, later, target]),
                       lambda address: {'kind': 'SERVICE' if address == T else 'UNKNOWN',
                                        'status': 'NO_LABEL'}).run(
        Scope('synthetic-r2-order', 'synthetic-r2-order', 1, 4, 12, 48, 3), seed)
    return result, seed, target


class EventOrderR2Tests(unittest.TestCase):
    def test_unknown_shared_balance_order_rejected_for_both_hash_presentations_and_entries(self):
        audits = []
        for late_hash in (20, 40):
            result, seed, _ = shared_balance_case(late_hash)
            for builder in (fixed_graph, live_fixed_graph):
                with self.subTest(late_hash=late_hash, builder=builder.__name__):
                    graph, scope = builder(result, seed)
                    self.assertEqual(graph['scope'], ORDER_UNRESOLVED)
                    self.assertEqual(scope['status'], ORDER_UNRESOLVED)
                    self.assertEqual(len(scope['unresolved_order_pairs']), 1)
                    self.assertFalse(scope['order_validation']['hash_order_is_chain_evidence'])
                    with self.assertRaisesRegex(ValueError, 'order'):
                        build_model(graph)
                    audits.append(scope['order_validation']['status'])
        self.assertEqual(len(set(audits)), 1)

    def test_known_incoming_first_keeps_upper_100_for_both_hash_presentations_and_entries(self):
        for late_hash in (20, 40):
            result, seed, target = shared_balance_case(late_hash, 0, 1)
            for builder in (fixed_graph, live_fixed_graph):
                with self.subTest(late_hash=late_hash, builder=builder.__name__):
                    graph, scope = builder(result, seed)
                    self.assertFalse(scope['order_validation']['unresolved_order_pairs'])
                    solved = solve_interval(build_model(graph), [target.event_id])
                    self.assertEqual(solved['upper_raw'], '100')

    def test_known_outgoing_first_keeps_upper_10_for_both_hash_presentations_and_entries(self):
        for late_hash in (20, 40):
            result, seed, target = shared_balance_case(late_hash, 1, 0)
            for builder in (fixed_graph, live_fixed_graph):
                with self.subTest(late_hash=late_hash, builder=builder.__name__):
                    graph, scope = builder(result, seed)
                    solved = solve_interval(build_model(graph), [target.event_id])
                    self.assertEqual(solved['upper_raw'], '10')

    def test_one_missing_transaction_index_is_still_unresolved(self):
        for indices in ((None, 1), (0, None)):
            result, seed, _ = shared_balance_case(20, *indices)
            for builder in (fixed_graph, live_fixed_graph):
                graph, _ = builder(result, seed)
                with self.assertRaises(ValueError):
                    build_model(graph)

    def test_unknown_independent_balance_operations_need_no_chain_relative_order(self):
        left = event(40, A, B, 10, 3, None)
        right = event(20, X, Y, 20, 3, None)
        ordered, audit = order_observed_transfers([asdict(left), asdict(right)])
        self.assertEqual(audit['status'], 'NECESSARY_ORDER_ESTABLISHED')
        self.assertEqual(audit['independent_unknown_pairs'], 1)
        self.assertEqual({e['event_id'] for e in ordered}, {left.event_id, right.event_id})

    def test_verified_execution_positions_override_trace_hash_presentation(self):
        tx = '0x' + '9' * 64
        # Lexical trace locator presentation is intentionally the reverse of
        # the verified execution positions. Neither locator alone is evidence.
        incoming = replace(event(9, A, B, 90, 3, 0), tx_hash=tx,
                           event_id='eip155:1:tx:' + tx + ':trace:9', kind='internal',
                           trace_address='9', execution_index=1)
        outgoing = replace(event(9, B, T, 100, 3, 0), tx_hash=tx,
                           event_id='eip155:1:tx:' + tx + ':trace:1', kind='internal',
                           trace_address='1', execution_index=2)
        ordered, audit = order_observed_transfers([asdict(outgoing), asdict(incoming)])
        self.assertEqual(audit['status'], 'NECESSARY_ORDER_ESTABLISHED')
        self.assertEqual([e['event_id'] for e in ordered], [incoming.event_id, outgoing.event_id])
        _, missing = order_observed_transfers([asdict(replace(e, execution_index=None))
                                               for e in (outgoing, incoming)])
        self.assertEqual(missing['status'], ORDER_UNRESOLVED)

    def test_receipt_log_positions_remain_supported(self):
        tx = '0x' + '9' * 64
        token = 'erc20:eip155:1:' + X
        incoming = Event('eip155:1:tx:' + tx + ':log:9', tx, A, B, token,
                         90, 3, 0, 36, 'erc20', 9)
        outgoing = Event('eip155:1:tx:' + tx + ':log:10', tx, B, T, token,
                         100, 3, 0, 36, 'erc20', 10)
        ordered, audit = order_observed_transfers([asdict(outgoing), asdict(incoming)])
        self.assertEqual(audit['status'], 'NECESSARY_ORDER_ESTABLISHED')
        self.assertEqual([e['log_index'] for e in ordered], [9, 10])


if __name__ == '__main__':
    unittest.main()
