"""Synthetic persistent exact computational split tests; no provider request."""
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from context_access_r3 import sha
from page_attempts import atomic_json
from stage1d_export import FULL_COLUMNS
from stage1d_native_candidate_batch import prepare, acquire
from stage1d_native_candidate_split import divide, proof_of_union, register, schedule, verify
import test_stage1d_native_candidate_batch as batch_tests
from test_stage1d_native_candidate import rectangle, raw_event, A, B, T, NATIVE


class ComputationSplitTests(unittest.TestCase):
    def setUp(self):
        self.fixture = batch_tests.NativeBatchTests(); self.fixture.setUp()
        self.work = self.fixture.work; self.query = self.fixture.query; self.cp = self.fixture.cp

    def tearDown(self): self.fixture.tearDown()

    def completed(self, group, total=30000, failure=None):
        prepared = prepare(self.work, self.query, group, self.cp, len(group))
        sid = prepared['proof']['native_sql_sha256']; job = self.work / 'private/dune_r2_jobs' / sid
        body = {'execution_id': sid[:26].upper(), 'state': 'QUERY_STATE_FAILED' if failure else 'QUERY_STATE_COMPLETED',
            'execution_cost_credits': '0.9625'}
        if failure: body['error'] = {'type': failure, 'message': 'synthetic failure'}
        else: body['result_metadata'] = {'column_names': FULL_COLUMNS, 'column_types': ['synthetic'] * 16,
            'total_row_count': total, 'total_result_set_bytes': total * 200}
        raw = self.work / 'raw/dune' / ('status_' + sid + '.json'); atomic_json(raw, body)
        receipt = {'request_id': 'status_' + sid, 'operation': 'status', 'http_status': 200, 'error_class': None,
            'raw_path': raw.relative_to(self.work).as_posix(), 'raw_bytes': raw.stat().st_size, 'sha256': sha(raw)}
        atomic_json(self.work / 'logs' / (receipt['request_id'] + '.json'), receipt)
        state = {'state': body['state'], 'execution_id': body['execution_id'], 'sql_sha256': sid,
            'scope_freeze_path': Path(prepared['freeze_path']).relative_to(self.work).as_posix(),
            'scope_freeze_sha256': sha(Path(prepared['freeze_path'])), 'status_response': body, 'status_receipt': receipt,
            'export_requests': 0, 'export_offsets': []}
        atomic_json(job / 'job.json', state)
        (job / 'query.sql').write_bytes(Path(prepared['freeze_path']).with_name('query.sql').read_bytes())
        return job, prepared

    def test_integer_midpoint_exact_union_retains_all_time_and_identity_fields(self):
        parent = rectangle(lo=11, hi=20, start=T + 95 * 86400, end=T + 107 * 86400)
        mode, children, mid = divide([parent])
        self.assertEqual(mode, 'INTEGER_BLOCK_BISECTION'); self.assertEqual(mid, 15)
        self.assertEqual([(r[0]['start_block'], r[0]['end_block']) for r in children], [(11, 15), (16, 20)])
        for child in children:
            for key in ['address', 'asset', 'start_time', 'end_time']: self.assertEqual(child[0][key], parent[key])
        self.assertTrue(proof_of_union([parent], mode, children, mid)['exact_parent_union'])
        bad = copy.deepcopy(children); bad[1][0]['start_block'] = 15
        with self.assertRaises(ValueError): proof_of_union([parent], mode, bad, mid)

    def test_groups_preserve_original_order_without_block_or_time_change(self):
        group = [rectangle(lo=n, hi=n + 1) for n in (10, 20, 30, 40, 50)]
        mode, children, mid = divide(group)
        self.assertEqual(mode, 'ORIGINAL_RECTANGLE_GROUP_BISECTION'); self.assertIsNone(mid)
        self.assertEqual(children, [group[:2], group[2:]])
        self.assertEqual(children[0] + children[1], group)

    def test_real_terminal_resource_evidence_required_not_timeout_or_small_result(self):
        for total, failure in [(25000, None), (0, 'TIMEOUT'), (0, 'USER_CANCELED')]:
            job, _ = self.completed([rectangle()], total, failure)
            with self.assertRaises(ValueError): register(self.work, self.query, job)
        job, _ = self.completed([rectangle()], failure='FAILED_TYPE_RESOURCES_CAP_REACHED')
        proof = register(self.work, self.query, job)
        self.assertEqual(verify(self.work, self.query, proof['path'])['trigger'], 'VERIFIED_SINGLE_EXECUTION_RESOURCE_CAP')

    def test_persistent_split_replaces_parent_after_restart_without_writing_coverage(self):
        parent = rectangle(lo=10, hi=19); job, _ = self.completed([parent])
        before = (job / 'job.json').read_bytes(); ref = register(self.work, self.query, job)
        self.assertEqual(register(self.work, self.query, job), ref)
        first = schedule(self.work, self.query, [parent], 32)
        self.assertEqual(first['selected'], [dict(parent, end_block=14)])
        self.assertEqual(first, schedule(self.work, self.query, [parent], 32))
        self.assertEqual(before, (job / 'job.json').read_bytes())
        self.assertFalse((self.work / 'derived/stage1d/intervals').exists())
        self.assertFalse(list(self.work.glob('**/*.sqlite')))

    def test_only_uncached_right_intersection_remains_and_freeze_binds_plan(self):
        parent = rectangle(lo=10, hi=19); job, _ = self.completed([parent]); ref = register(self.work, self.query, job)
        right = dict(parent, start_block=15)
        decision = schedule(self.work, self.query, [right])
        self.assertEqual(decision['selected'], [right])
        child = prepare(self.work, self.query, decision['selected'], self.cp, 1, decision['dependencies'])
        freeze = json.loads(Path(child['freeze_path']).read_text())
        self.assertIn({'path': ref['path'], 'sha256': ref['sha256']}, freeze['dependencies'])
        self.assertEqual(child['proof']['computation_split_dependencies'], decision['dependencies'])

    def test_nested_failure_uses_quarter_not_original_parent(self):
        parent = rectangle(lo=10, hi=25); job, _ = self.completed([parent]); register(self.work, self.query, job)
        left = schedule(self.work, self.query, [parent])['selected']
        left_job, _ = self.completed(left); register(self.work, self.query, left_job)
        selected = schedule(self.work, self.query, [parent])
        self.assertEqual(selected['selected'], [dict(parent, end_block=13)])
        self.assertEqual(len(selected['dependencies']), 2)

    def test_single_block_gap_does_not_starve_other_pending_address(self):
        single = rectangle(lo=10, hi=10); job, _ = self.completed([single]); ref = register(self.work, self.query, job)
        self.assertEqual(ref['mode'], 'UNSPLITTABLE_SINGLE_BLOCK')
        other = rectangle(B, 20, 21)
        plan = schedule(self.work, self.query, [single, other])
        self.assertEqual(plan['selected'], [other]); self.assertEqual(len(plan['resource_gaps']), 1)
        empty = schedule(self.work, self.query, [single]); self.assertFalse(empty['selected'])
        self.assertEqual(empty['resource_gaps'][0]['reason'], 'UNSPLITTABLE_NATIVE_SINGLE_BLOCK_RESOURCE_GAP')

    def test_blocked_subset_not_recombined_into_an_overlapping_parent(self):
        single = rectangle(lo=10, hi=10); other = rectangle(lo=20, hi=21)
        group_job, _ = self.completed([single, other]); register(self.work, self.query, group_job)
        single_job, _ = self.completed([single]); register(self.work, self.query, single_job)
        plan = schedule(self.work, self.query, [single, other], 32)
        self.assertEqual(plan['selected'], [other]); self.assertTrue(plan['resource_gaps'])

    def test_plan_tamper_or_outside_scope_cannot_create_new_coverage(self):
        parent = rectangle(lo=10, hi=19); job, _ = self.completed([parent]); ref = register(self.work, self.query, job)
        path = self.work / ref['path']; proof = json.loads(path.read_text()); proof['children_groups'][0][0]['end_block'] = 16
        atomic_json(path, proof)
        with self.assertRaises(ValueError): schedule(self.work, self.query, [parent])
        self.assertFalse((self.work / 'derived/stage1d/intervals').exists())

    def test_scope_time_intersection_does_not_expand_pending(self):
        parent = rectangle(lo=10, hi=19, end=T + 100); job, _ = self.completed([parent]); register(self.work, self.query, job)
        remaining = rectangle(lo=12, hi=17, start=T + 10, end=T + 30)
        plan = schedule(self.work, self.query, [remaining])
        self.assertEqual(plan['selected'], [rectangle(lo=12, hi=14, start=T + 10, end=T + 30)])

    def test_acquire_persists_valid_size_trigger_after_preexport_stop(self):
        parent = rectangle(lo=10, hi=19); job, prepared = self.completed([parent])
        with patch('stage1d_native_candidate_batch.execute_sql', side_effect=ValueError('Declared query/context row resource cap blocks this result')):
            result = acquire(self.work, self.query, [parent], self.cp, 1)
        self.assertEqual(result['status'], 'ACQUISITION_PARTIAL')
        self.assertEqual(result['computation_split_plan']['mode'], 'INTEGER_BLOCK_BISECTION')
        self.assertFalse(result['complete_native_index']); self.assertIsNone(result['propagated_candidate_cap_exceeded'])
        self.assertEqual(schedule(self.work, self.query, [parent])['selected'][0]['end_block'], 14)


if __name__ == '__main__': unittest.main(verbosity=2)
