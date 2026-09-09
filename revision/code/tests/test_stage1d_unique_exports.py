"""Actual R4 transport/retry/ledger paths, using only synthetic responses."""
from decimal import Decimal
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
CODE = BASE if (BASE / 'src/stage1d_costs.py').is_file() else BASE.parents[2] / 'code'
sys.path.insert(0, str(BASE / 'src'))
sys.path.append(str(CODE / 'tests'))
from stage1d_export_reconcile import (AUTH, KNOWN_COLUMNS, PLAN_POLICY, Stage1DPageDune,
    analyze, canonical, digest, prepare_known_plan, rows, unique_set_upper, verify_plan)
import test_dune_r4 as support
from test_stage1d_budget_access import Runtime, amend_synthetic
from page_attempts import atomic_json


class UniqueExportsTests(unittest.TestCase):
    def setUp(self):
        self.s = support.DuneR4Tests(); self.s.setUp(); self.addCleanup(self.s.tearDown)
        self.w = self.s.w; self.folder = self.s.folder
        rate_path = self.w / 'private/dune_rate_evidence.json'
        atomic_json(rate_path, json.loads(rate_path.read_bytes()) | {'datapoint_scheme_credits_per_1000': '1'})
        self.configure()
        batch = json.loads((self.w / 'private/BATCH_QUERY_FREEZE.json').read_bytes())
        def write_fixture(path, value):
            atomic_json(path, batch if Path(path).name == 'BATCH_QUERY_FREEZE.json' else value)
        with patch('test_stage1d_budget_access.atomic_json', side_effect=write_fixture):
            amend_synthetic(self, self.w)
        self.runtime = Runtime()
        self.live = Stage1DPageDune(self.w, self.s.transport, clock=self.s.clock,
            sleeper=self.s.clock.sleep, rng=lambda: 0, runtime=self.runtime)

    def configure(self, total=1500, size=100000, kind='candidate'):
        sql = b"SELECT 'synthetic frozen ordinary complete rows'"
        (self.folder / 'query.sql').write_bytes(sql)
        sql_sha = digest(sql)
        freeze = {'schema_version': 'stage1d-sql-freeze-v1', 'authorization_id': AUTH,
            'query_ids': ['synthetic:q0'], 'sql_sha256': sql_sha, 'dependencies': []}
        freeze_path = 'private/stage1d_sql/synthetic/freeze_manifest.json'
        atomic_json(self.w / freeze_path, freeze)
        atomic_json(self.w / 'private/BATCH_QUERY_FREEZE.json', {'schema_version': 'stage1d-batch-query-freeze-v1',
            'authorization_id': AUTH, 'queries': [{'query_id': 'synthetic:q' + str(i)} for i in range(4)]})
        self.s.md = {'column_names': KNOWN_COLUMNS[kind], 'column_types': ['varchar'] * len(KNOWN_COLUMNS[kind]),
            'total_row_count': total, 'row_count': total, 'total_result_set_bytes': size,
            'result_set_bytes': size, 'datapoint_count': total * len(KNOWN_COLUMNS[kind])}
        body = {'execution_id': self.s.execution, 'state': 'QUERY_STATE_COMPLETED',
            'execution_cost_credits': '.5', 'result_metadata': self.s.md}
        raw_path = 'raw/dune/synthetic_original_status.json'
        atomic_json(self.w / raw_path, body)
        raw = (self.w / raw_path).read_bytes()
        receipt = {'request_id': 'synthetic_original_status', 'operation': 'status', 'execution_id': self.s.execution,
            'http_status': 200, 'error_class': None, 'parameters': None, 'raw_path': raw_path,
            'raw_bytes': len(raw), 'sha256': digest(raw), 'evidence_kind': 'SYNTHETIC_TRANSPORT'}
        atomic_json(self.w / 'logs/synthetic_original_status.json', receipt)
        self.s.state.update(sql_sha256=sql_sha, scope_freeze_path=freeze_path,
            scope_freeze_sha256=digest((self.w / freeze_path).read_bytes()), kind=kind,
            status_response=body, status_receipt=receipt)
        self.s.save()
        self.s.responses['results'] = [self.s.response(200, self.page(offset, min(1000, total - offset))) for offset in range(0, total, 1000)] if total < 10000 else []

    def page(self, offset, count, value='x'):
        total = self.s.md['total_row_count']
        body = {'execution_id': self.s.execution, 'state': 'QUERY_STATE_COMPLETED', 'result': {
            'metadata': self.s.md | {'row_count': count, 'result_set_bytes': count * 100,
                'datapoint_count': count * len(self.s.md['column_names'])},
            'rows': [dict.fromkeys(self.s.md['column_names'], value) for _ in range(count)]}}
        if offset + count < total: body['next_offset'] = offset + count
        return body

    def export_all(self):
        progress = self.live.export_progress(self.folder, self.s.state_now())
        while not progress['complete']:
            result = self.live.export(self.folder, offset=progress['next_offset'])
            self.assertNotIn('retry', result)
            progress = result['progress']
        return result

    def test_one_set_reserved_once_then_settle_reconciles_exact_pages_and_cache_keeps_it(self):
        first = self.live.export(self.folder)
        plan = verify_plan(self.w, self.folder)
        self.assertEqual(first['upper_not_actual'], plan['full_export_upper_credits'])
        self.assertEqual(plan['hypothetical_retries_reserved'], 0)
        second = self.live.export(self.folder, offset=first['progress']['next_offset'])
        self.assertEqual(second['upper_not_actual'], first['upper_not_actual'])
        outcome = self.live.settle(self.folder)
        self.assertEqual(outcome['export_upper_reconciliation']['status'], 'RECONCILED_UPPER_NOT_ACTUAL', outcome)
        proof = analyze(self.w, self.folder)
        self.assertEqual(proof['current_export_upper'], '24')
        self.assertEqual(proof['verified_response_export_upper'], '24')
        self.assertEqual(len(self.s.calls), 2)
        self.assertTrue(self.live.export(self.folder)['cache_reused'])
        self.assertEqual(self.live.settle(self.folder)['export_upper_reconciliation']['status'], 'ALREADY_RECONCILED')
        self.assertEqual(self.live.export_envelope(self.s.state_now(), 1000, self.folder)[0], Decimal(24))
        self.assertEqual(len(self.s.calls), 2)

    def test_actual_retry_reserves_one_extra_and_all_failed_risk_remains(self):
        self.s.responses['results'].insert(0, TimeoutError('synthetic failed actual dispatch'))
        self.export_all()
        plan = verify_plan(self.w, self.folder)
        component = next(r for r in rows(self.live.db.path, 'r2_components') if r['job'] == self.s.job)
        self.assertEqual(Decimal(component['export_risk']), Decimal(plan['full_export_upper_credits']) + Decimal(plan['actual_retry_request_upper_credits']))
        self.assertEqual(len(self.s.calls), 3)
        outcome = self.live.settle(self.folder)
        self.assertEqual(outcome['settlement_status'], 'EXPORTED_WITH_RETAINED_ATTEMPT_RISK')
        self.assertNotIn('export_upper_reconciliation', outcome)
        self.assertEqual(next(r for r in rows(self.live.db.path, 'r2_components') if r['job'] == self.s.job), component)

    def test_second_reservation_failure_prevents_retry_dispatch_and_no_hypothetical_stack(self):
        self.s.responses['results'].insert(0, TimeoutError('synthetic failed actual dispatch'))
        original = self.live.db.reserve_export; seen = []
        def reserve(job, upper, evidence, **kwargs):
            seen.append(str(upper))
            if len(seen) > 1: raise RuntimeError('Synthetic insufficient remaining pool')
            return original(job, upper, evidence, **kwargs)
        with patch.object(self.live.db, 'reserve_export', side_effect=reserve):
            with self.assertRaisesRegex(RuntimeError, 'remaining pool'): self.live.export(self.folder)
        plan = verify_plan(self.w, self.folder)
        self.assertEqual(seen[0], plan['full_export_upper_credits'])
        self.assertEqual(len(self.s.calls), 1)
        component = next(r for r in rows(self.live.db.path, 'r2_components') if r['job'] == self.s.job)
        self.assertEqual(component['export_risk'], plan['full_export_upper_credits'])

    def test_giant_result_blocked_without_a_get_even_with_remaining_credits(self):
        self.configure(total=9464042, size=3463997248, kind='context')
        with self.assertRaisesRegex(ValueError, 'raw cap'): self.live.export(self.folder)
        self.assertFalse(self.s.calls)
        self.assertFalse((self.folder / 'stage1d_export_plan.json').exists())

    def test_byte_evidence_tamper_blocks_before_dispatch(self):
        prepare_known_plan(self.w, self.folder)
        path = self.folder / 'stage1d_export_byte_evidence.json'; path.write_bytes(path.read_bytes() + b' ')
        with self.assertRaisesRegex(ValueError, 'source changed'): self.live.export(self.folder)
        self.assertFalse(self.s.calls)

    def test_observed_byte_margin_excess_raises_risk_and_blocks_next_get(self):
        self.configure(total=2, size=100)
        self.s.responses['results'] = [self.s.response(200, self.page(0, 2, 'x' * 6000))]
        with self.assertRaisesRegex(RuntimeError, 'exceeded declared byte margin'): self.live.export(self.folder)
        self.assertEqual(len(self.s.calls), 1)
        plan = verify_plan(self.w, self.folder)
        component = next(r for r in rows(self.live.db.path, 'r2_components') if r['job'] == self.s.job)
        self.assertGreater(Decimal(component['export_risk']), Decimal(plan['full_export_upper_credits']))
        with self.assertRaisesRegex(RuntimeError, 'Previous response'): self.live.export(self.folder)
        self.assertEqual(len(self.s.calls), 1)

    def test_known_plan_is_immutable_and_labels_escape_margin_is_explicit(self):
        self.configure(total=2, size=100, kind='frontier_labels')
        first = prepare_known_plan(self.w, self.folder)
        self.assertEqual(first, prepare_known_plan(self.w, self.folder))
        evidence = json.loads((self.folder / 'stage1d_export_byte_evidence.json').read_bytes())
        self.assertEqual(evidence['metadata_byte_multiplier'], 6)
        self.assertFalse(evidence['is_actual'])

    def test_heterogeneous_pages_use_sum_bound_not_invalid_global_max(self):
        md = {'total_row_count': 1500, 'total_result_set_bytes': 1000000, 'column_names': list(range(24))}
        upper, retry, pages = unique_set_upper(md, 1000, 1000000)
        self.assertEqual(upper, Decimal(57))
        self.assertEqual(retry, Decimal(24))
        self.assertEqual(pages, 2)


if __name__ == '__main__': unittest.main()
