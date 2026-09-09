"""Synthetic tests for exact context partition recovery, with no network."""
import copy
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import stage1d_context_recovery as recovery
import test_stage1d_context as facts
from context_ledger_r3 import EvidenceConflict, coverage_complete
from page_attempts import atomic_json
from stage1d_context import REQUIRED


def headers(first, last):
    return {n: {'number': hex(n), 'hash': '0x' + format(1000 - n, '064x'),
                'timestamp': hex(1660000000 + n * 86400)} for n in range(first, last + 1)}


def row(address, lo, hi):
    return {'address': address, 'ledger_start_block': lo, 'ledger_end_block': hi,
            'before_anchor_block': lo - 1, 'after_anchor_block': hi, 'asset': 'ETH'}


def complete(address, lo, hi):
    return [{'address': address, 'start_block': lo, 'end_block': hi, 'data_type': kind,
             'status': 'COMPLETE', 'pagination_complete': True, 'evidence_ids': ['SYNTHETIC']} for kind in REQUIRED]


class PartitionTests(unittest.TestCase):
    def test_full_108_day_union_exact_and_day95_is_retained(self):
        original = [row(facts.A, 10, 118), row(facts.B, 105, 105)]
        plan = recovery.make_plan(original, headers(9, 118))
        got = {facts.A: set(), facts.B: set()}
        for batch in plan['batches']:
            self.assertLessEqual(batch['date_domain']['elapsed_days'], 14)
            for r in batch['rows']:
                blocks = set(range(r['ledger_start_block'], r['ledger_end_block'] + 1))
                self.assertFalse(got[r['address']] & blocks)
                got[r['address']] |= blocks
        self.assertEqual(set(range(10, 119)), got[facts.A])
        self.assertEqual({105}, got[facts.B])
        self.assertEqual([], plan['resource_gaps'])

    def test_partial_success_islands_only_gaps_requeried(self):
        old = complete(facts.A, 12, 14) + complete(facts.A, 17, 18)
        plan = recovery.make_plan([row(facts.A, 10, 20)], headers(9, 20), old)
        actual = [(r['ledger_start_block'], r['ledger_end_block']) for b in plan['batches'] for r in b['rows']]
        self.assertEqual([(10, 11), (15, 16), (19, 20)], actual)

    def test_all_four_coverage_types_and_full_export_are_required(self):
        coverage = complete(facts.A, 10, 20)
        self.assertEqual([], recovery.missing_intervals(row(facts.A, 10, 20), coverage))
        self.assertEqual([[10, 20]], recovery.missing_intervals(row(facts.A, 10, 20), coverage[:-1]))
        coverage[-1]['pagination_complete'] = False
        self.assertEqual([[10, 20]], recovery.missing_intervals(row(facts.A, 10, 20), coverage))
        coverage[-1]['pagination_complete'] = True
        coverage[-1].update(provider_frozen_scope=True, date_domain_verified=False)
        self.assertEqual([[10, 20]], recovery.missing_intervals(row(facts.A, 10, 20), coverage))

    def test_context_coverage_checker_accepts_adjacent_shards(self):
        coverage = complete(facts.A, 10, 14) + complete(facts.A, 15, 20)
        self.assertEqual([], recovery.missing_intervals(row(facts.A, 10, 20), coverage))
        passed, detail = coverage_complete(coverage, facts.A, 10, 20, REQUIRED)
        self.assertTrue(passed)

    def test_missing_headers_are_specific_gap_and_other_account_continues(self):
        plan = recovery.make_plan([row(facts.A, 10, 11), row(facts.B, 20, 21)], headers(9, 11))
        self.assertEqual([facts.A], [r['address'] for b in plan['batches'] for r in b['rows']])
        self.assertEqual(facts.B, plan['resource_gaps'][0]['address'])

    def test_sparse_headers_do_not_silently_query_longer_span(self):
        all_headers = headers(9, 118)
        plan = recovery.make_plan([row(facts.A, 10, 118)], {9: all_headers[9], 118: all_headers[118]})
        self.assertEqual([], plan['batches'])
        self.assertEqual('CONTEXT_PARTITION_HEADER_DENSITY_GAP', plan['resource_gaps'][0]['type'])

    def test_physical_header_identity_and_timestamp_conflicts_rejected(self):
        good = headers(9, 20)
        bad = copy.deepcopy(good); bad[10]['number'] = '0xb'
        with self.assertRaises(EvidenceConflict): recovery.make_plan([row(facts.A, 10, 20)], bad)
        bad = copy.deepcopy(good); bad[10]['timestamp'] = bad[9]['timestamp']
        with self.assertRaises(EvidenceConflict): recovery.make_plan([row(facts.A, 10, 20)], bad)
        self.assertTrue(recovery.make_plan([row(facts.A, 10, 20)], good)['batches'])

    def test_amounts_and_hash_sort_do_not_set_batch_order(self):
        rows = [row(facts.B, 20, 22), row(facts.A, 10, 45)]
        a = recovery.make_plan(rows, headers(9, 45))
        b = recovery.make_plan(list(reversed(rows)), headers(9, 45))
        self.assertEqual(a['batches'], b['batches'])

    def test_frozen_global_boundary_is_exact_and_conflicts_fail(self):
        h = headers(9, 40)
        q = {'start_block': 10, 'end_block': 40, 'start_block_hash': h[10]['hash'],
             'end_block_hash': h[40]['hash'], 'scope': {'start_block': 10, 'end_block': 40,
             'start_time': int(h[10]['timestamp'], 16), 'end_time': int(h[40]['timestamp'], 16)}}
        mixed = recovery.add_frozen_boundaries(q, {24: h[24]})
        self.assertEqual({10, 24, 40}, set(mixed))
        self.assertEqual(h[40], mixed[40])
        with self.assertRaises(EvidenceConflict):
            recovery.add_frozen_boundaries(q, {40: dict(h[40], hash='0x' + 'e' * 64)})


class RunTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.w = Path(self.tmp.name)
        self.q, self.c, self.seed, self.out = facts.base()
        self.out = facts.event(2, facts.A, facts.T, 90, 40)
        self.c['candidate_events'] = [asdict(self.seed), asdict(self.out)]
        self.c['states'] = [{'state': {'address': facts.A}}, {'state': {'address': facts.T}}]
        self.q.update(seed_to=facts.A, start_block=10, end_block=40, scope_id='synthetic:full', scope_hash='f' * 64)
        self.qdir = self.w / 'derived/stage1d/queries' / self.q['name']
        self.root = self.qdir / 'context'; self.root.mkdir(parents=True)
        atomic_json(self.qdir / 'collection.json', self.c)
        atomic_json(self.w / 'private/BATCH_QUERY_FREEZE.json', {'queries': [self.q]})
        atomic_json(self.w / 'private/STAGE1D_POLICY.json', {'new_batch_resource_limits': {'context_events_per_query': 50000}})
        atomic_json(self.root / 'headers.json', headers(9, 40))
        atomic_json(self.root / 'balances.json', facts.anchors([(facts.A, 9, 20), (facts.A, 40, 10)])[0])
        self.sentinel = self.w / 'private/existing_budget_and_clock.json'
        atomic_json(self.sentinel, {'risk': '198.499226044', 'clock': '123.456', 'rpc_used': 500})
        self.before = self.sentinel.read_bytes()
        self.calls = []
        self.execution_status = 'COMPLETED_EXPORTED'
        self.rows = []
        self.label_patch = patch.object(recovery, 'Labels', return_value=lambda a: {'kind': 'SERVICE' if a == facts.T else 'UNKNOWN'})
        self.execute_patch = patch.object(recovery, 'execute_sql', side_effect=self.execute)
        self.rows_patch = patch.object(recovery, 'result_rows', side_effect=lambda *a: self.rows)
        for p in (self.label_patch, self.execute_patch, self.rows_patch): p.start(); self.addCleanup(p.stop)

    def execute(self, work, freeze, query, label):
        self.calls.append(freeze)
        return {'status': self.execution_status, 'job_folder': 'synthetic:job'}

    def invoke(self, **kw): return recovery.run_recovery(self.w, self.q, **kw)

    def test_max_batches_resume_only_missing_shards_preserves_old_files(self):
        old = (self.root / 'headers.json').read_bytes()
        one = self.invoke(max_batches=1)
        self.assertEqual(1, len(self.calls)); self.assertTrue(one['remaining_account_intervals'])
        two = self.invoke(max_batches=20)
        self.assertEqual(3, len(self.calls)); self.assertEqual([], two['remaining_account_intervals'])
        self.assertEqual(old, (self.root / 'headers.json').read_bytes())
        self.assertEqual(self.before, self.sentinel.read_bytes())
        self.invoke(max_batches=20)
        self.assertEqual(3, len(self.calls))
        self.assertEqual(3, len({p.parent.name for p in self.calls}))

    def test_plan_only_never_calls_paid_transport(self):
        result = self.invoke(max_batches=0)
        self.assertEqual([], self.calls)
        self.assertEqual(3, result['planned_batches'])
        self.assertEqual(self.before, self.sentinel.read_bytes())

    def test_failed_export_claims_no_coverage_and_stops_for_resume(self):
        self.execution_status = 'DEFERRED_CLOCK'
        result = self.invoke(max_batches=3)
        self.assertEqual(1, len(self.calls))
        coverage = json.loads((self.w / result['context_folder'] / 'coverage.json').read_text())
        self.assertEqual([], coverage)
        self.assertEqual([[10, 40]], result['remaining_account_intervals'][0]['intervals'])

    def test_terminal_failed_sql_not_submitted_again(self):
        self.execution_status = 'QUERY_STATE_FAILED'
        result = self.invoke(max_batches=1)
        freeze = self.calls[0]
        atomic_json(self.w / 'private/dune_r2_jobs' / freeze.parent.name / 'job.json', {'state': 'QUERY_STATE_FAILED', 'execution_id': 'synthetic'})
        self.execution_status = 'COMPLETED_EXPORTED'
        result = self.invoke(max_batches=20)
        self.assertEqual(3, len(self.calls))
        self.assertEqual('EXISTING_TERMINAL_FAILURE_RETAINED', result['outcomes'][0]['status'])
        self.assertEqual([[10, 24]], result['remaining_account_intervals'][0]['intervals'])

    def test_full_export_validation_failure_does_not_certify_coverage(self):
        with patch.object(recovery, 'result_rows', side_effect=ValueError('Incomplete verified pages')):
            result = self.invoke(max_batches=3)
        self.assertEqual(1, len(self.calls))
        self.assertEqual('CONTEXT_EXPORT_OR_PHYSICAL_VALIDATION_FAILED', result['outcomes'][0]['status'])
        self.assertEqual([[10, 40]], result['remaining_account_intervals'][0]['intervals'])

    def test_duplicate_physical_rows_do_not_duplicate_or_reset_cap(self):
        event = facts.raw_transaction(self.seed)
        atomic_json(self.root / 'ledger_rows.json', [event])
        self.rows = [event, dict(event, evidence_ids=['SECOND_SYNTHETIC'])]
        result = self.invoke(max_batches=1)
        self.assertEqual(1, result['ledger_rows'])
        self.assertEqual(self.before, self.sentinel.read_bytes())

    def test_cap_retains_all_exported_rows_and_blocks_further_queries(self):
        atomic_json(self.w / 'private/STAGE1D_POLICY.json', {'new_batch_resource_limits': {'context_events_per_query': 1}})
        self.rows = [facts.raw_transaction(self.seed), facts.raw_transaction(self.out)]
        result = self.invoke(max_batches=10)
        self.assertEqual(1, len(self.calls)); self.assertEqual(2, result['ledger_rows'])
        self.assertEqual('CONTEXT_RESOURCE_LIMIT_MODEL_BLOCKED', result['context_status'])
        self.assertEqual(2, len(json.loads((self.w / result['context_folder'] / 'ledger_rows.json').read_text())))
        self.assertFalse((self.w / result['context_folder'] / 'model_input.json').exists())

    def test_different_frozen_query_is_rejected(self):
        with self.assertRaises(ValueError): recovery.run_recovery(self.w, dict(self.q, scope_hash='e' * 64))
        self.assertEqual([], self.calls)

    def test_context_sql_preserves_whole_block_traces_fees_and_protocol_rows(self):
        self.invoke(max_batches=1)
        sql = (self.calls[0].parent / 'query.sql').read_text()
        self.assertIn(f'({facts.A},10,24)', sql)
        self.assertIn('ethereum.withdrawals', sql)
        self.assertIn('ethereum.blocks', sql)
        self.assertIn('JOIN tx_keys k ON t.tx_hash=k.hash', sql)
        self.assertNotIn('block_time BETWEEN', sql)
        self.assertNotIn('INTERVAL', sql)
        self.assertIn('gas_price', sql)


if __name__ == '__main__': unittest.main()
