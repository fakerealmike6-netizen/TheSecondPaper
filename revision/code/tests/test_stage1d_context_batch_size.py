"""Synthetic outer RPC batching; inherited durable retries remain authoritative."""
import copy
import json
import unittest
from unittest.mock import patch

import stage1d_context_online as online
import test_stage1d_context_online as support


class ContextBatchSizeTests(unittest.TestCase):
    setUp = support.ContextOnlineTests.setUp
    budget = support.ContextOnlineTests.budget
    rpc = support.ContextOnlineTests.rpc
    execute = support.ContextOnlineTests.execute
    invoke = support.ContextOnlineTests.invoke

    def prepare_many(self):
        self.capacity = 200
        plan = online.required_context_windows(self.q, self.c)
        row = next(r for r in plan['rows'] if r['address'] == self.q['seed_to'])
        plan['rows'] = [row] + [dict(copy.deepcopy(row), address='0x'+format(i+200, '040x')) for i in range(26)]
        # Batch sizing operates on the resolved necessary-account plan. The
        # boundary adapter itself is covered by the separate integration suite.
        p = patch.object(online, 'necessary_context_windows', return_value=plan)
        p.start(); self.addCleanup(p.stop)
        p = patch.object(online, 'build_document', return_value={'completion_status':'PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS'})
        p.start(); self.addCleanup(p.stop)

    def test_explicit_ten_splits_in_original_order_and_reuses_success_after_restart(self):
        self.prepare_many()
        result = self.invoke(fetch_ledger=False, account_selection='all', fetch_anchor_headers=False,
                             rpc_batch_size=10, output_variant='round_small')
        recorded = json.loads((self.root/'round_small/rpc_results.json').read_text())
        flattened = [p for batch in self.calls for p in batch]
        self.assertGreater(len(flattened), 50)
        self.assertEqual(recorded['plans'], flattened)
        self.assertTrue(all(len(batch) <= 10 for batch in self.calls))
        self.assertEqual(10, recorded['rpc_batch_size'])
        self.assertEqual(len(flattened), len({json.dumps(p, sort_keys=True) for p in flattened}))
        first_calls = len(self.calls)
        first_anchors = result['known_balance_anchors']
        again = self.invoke(fetch_ledger=False, account_selection='all', fetch_anchor_headers=False,
                            rpc_batch_size=10, output_variant='round_reuse')
        self.assertEqual(first_calls, len(self.calls))
        self.assertEqual(first_anchors, again['known_balance_anchors'])
        metadata = json.loads((self.root/'round_small/context_result.json').read_text())['context_acquisition_metadata']
        self.assertEqual(10, metadata['rpc_batch_size'])

    def test_default_retains_previous_fifty(self):
        self.prepare_many()
        self.invoke(fetch_ledger=False, account_selection='all', fetch_anchor_headers=False,
                    output_variant='round_default')
        self.assertEqual(50, len(self.calls[0]))
        self.assertTrue(all(len(batch) <= 50 for batch in self.calls))
        self.assertEqual(50, json.loads((self.root/'round_default/rpc_results.json').read_text())['rpc_batch_size'])

    def test_invalid_size_rejected_before_any_request(self):
        for value in (0, -1, 51, True, '10', 10.0, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.invoke(rpc_batch_size=value, output_variant='round_invalid')
        self.assertEqual([], self.calls)
        self.assertFalse((self.root/'round_invalid').exists())

    def test_remaining_budget_smaller_than_ten_keeps_specific_gap(self):
        self.prepare_many(); self.capacity = 3
        result = self.invoke(fetch_ledger=False, account_selection='all', fetch_anchor_headers=False,
                             rpc_batch_size=10, output_variant='round_limited')
        self.assertEqual([3], [len(batch) for batch in self.calls])
        self.assertTrue(result['resource_gaps'])
        recorded = json.loads((self.root/'round_limited/rpc_results.json').read_text())
        gap = recorded['resource_gaps'][0]
        self.assertEqual(recorded['plans'][3:], gap['unprocessed_plans'])


if __name__ == '__main__':
    unittest.main()
