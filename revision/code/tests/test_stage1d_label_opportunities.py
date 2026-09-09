"""Synthetic continued100+500 opportunities; no quota pool or network reset."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from context_access_r3 import sha
from page_attempts import atomic_json
from stage1d_label_opportunities import AUTH, AUTH_PATH, SCOPE_AUTH, label_budget, snapshot
import stage1d_acquisition as acquisition


class LabelOpportunityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.work = Path(self.tmp.name).resolve()
        self.queries = [dict(query_id='query-' + str(i), name='query_' + str(i), scope_id='scope-' + str(i), scope_hash=str(i) * 64,
                             seed_event={'chain_id': 'eip155:1'}) for i in range(4)]
        atomic_json(self.work / 'private/BATCH_QUERY_FREEZE.json', {'authorization_id': SCOPE_AUTH, 'queries': self.queries})
        self.policy = self.work / 'private/STAGE1D_EFFECTIVE_POLICY.json'
        atomic_json(self.policy, {'labels': {'max_new_external_distinct_addresses_this_batch': 100}})
        atomic_json(self.work / 'private/stage1d_inputs/source_rules.json', {'table_schema': {}})

    def tearDown(self): self.tmp.cleanup()

    def amend(self):
        value = {'schema_version': 'stage1d-label-opportunity-amendment-v1', 'authorization_id': AUTH,
            'status': 'USER_CONFIRMED', 'source_kind': 'USER_MESSAGE', 'source_text': 'Synthetic user grants500 additional opportunities',
            'previous_cap': 100, 'additional_opportunities': 500, 'total_cap': 600,
            'same_batch_distinct_chain_address_pool': True, 'scope_authorization_id': SCOPE_AUTH,
            'other_resource_caps_unchanged': True, 'batch_freeze_sha256': sha(self.work / 'private/BATCH_QUERY_FREEZE.json')}
        path = self.work / AUTH_PATH; atomic_json(path, value)
        atomic_json(self.policy, {'labels': {'max_new_external_distinct_addresses_this_batch': 600,
            'opportunity_amendment': {'path': AUTH_PATH, 'sha256': sha(path), 'authorization_id': AUTH}}})
        return path

    def cohort(self, addresses, owner=0, complete=True, chain=None):
        row = {'query_id': self.queries[owner]['query_id'], 'addresses': addresses, 'previous_batch_distinct': 0}
        if chain is not None: row['chain_id'] = chain
        identity = hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest()
        path = self.work / 'private/stage1d_label_cohorts' / (identity + '.json'); atomic_json(path, row)
        if complete:
            atomic_json(self.work / 'derived/stage1d/labels' / path.name,
                {'rows': [{'address': a, 'lookup_status': 'COMPLETED_FOUR_TABLE_OPPORTUNITY'} for a in addresses]})
        return path

    def test_legacy100_and_verified600_keep_same100_used(self):
        self.cohort(['0x' + format(i + 1, '040x') for i in range(100)])
        old = snapshot(self.work); self.assertEqual((old['cap'], old['confirmed_used_distinct'], old['remaining']), (100, 100, 0))
        self.amend(); new = snapshot(self.work)
        self.assertEqual((new['cap'], new['confirmed_used_distinct'], new['reserved_not_confirmed_distinct'], new['remaining']), (600, 100, 0, 500))
        self.assertFalse(new['new_pool_created'])

    def test_wrong_sha_unbound_increase_and_wrong_scope_fail_closed(self):
        atomic_json(self.policy, {'labels': {'max_new_external_distinct_addresses_this_batch': 600}})
        with self.assertRaises(ValueError): label_budget(self.work)
        path = self.amend(); row = json.loads(path.read_text()); row['additional_opportunities'] = 600; atomic_json(path, row)
        with self.assertRaises(ValueError): label_budget(self.work)
        self.amend(); batch = self.work / 'private/BATCH_QUERY_FREEZE.json'; batch.write_text('{}')
        with self.assertRaises(ValueError): label_budget(self.work)

    def test_no_repeat_adoption_or_perquery_refresh(self):
        self.amend(); addresses = ['0x' + format(i + 1, '040x') for i in range(100)]
        self.cohort(addresses, owner=0); self.cohort(addresses[:50], owner=1)
        before = snapshot(self.work); self.amend(); after = snapshot(self.work)
        self.assertEqual(before, after); self.assertEqual(after['allocated_distinct'], 100)
        self.assertEqual(after['remaining'], 500)
        self.cohort(['0x' + format(i + 1001, '040x') for i in range(500)], owner=2)
        self.assertEqual(snapshot(self.work)['remaining'], 0)

    def test_same_chain_address_casefold_but_different_chain_is_distinct(self):
        self.amend(); a = '0x' + 'a' * 40
        self.cohort([a], owner=0); self.cohort([a.upper()], owner=1)
        self.assertEqual(snapshot(self.work)['confirmed_used_distinct'], 1)
        self.cohort([a], owner=2, chain='eip155:2')
        self.assertEqual(snapshot(self.work)['confirmed_used_distinct'], 2)

    def test_unsubmitted_allocation_reserved_not_reported_as_used(self):
        self.amend(); self.cohort(['pending-A', 'pending-B'], complete=False)
        result = snapshot(self.work)
        self.assertEqual(result['allocated_distinct'], 2); self.assertEqual(result['confirmed_used_distinct'], 0)
        self.assertEqual(result['reserved_not_confirmed_distinct'], 2); self.assertEqual(result['remaining'], 598)
        self.assertEqual(result, snapshot(self.work))

    def test_real_acquire_path_uses600_and_before_post_is_reserved(self):
        self.cohort(['used-' + str(i) for i in range(100)]); q = self.queries[1]
        collection = {'states': [{'state': {'address': 'fresh-address'}}]}
        with patch.object(acquisition, 'execute_sql') as execute:
            result = acquisition.acquire_labels(self.work, q, collection)
        execute.assert_not_called(); self.assertEqual(result['status'], 'NO_NEW_LABEL_OPPORTUNITY')
        self.amend()
        with patch('frontier_labels_r2.optimized_sql', return_value='-- synthetic\nSELECT 1'), \
             patch.object(acquisition, 'execute_sql', side_effect=RuntimeError('before POST')) as execute:
            with self.assertRaises(RuntimeError): acquisition.acquire_labels(self.work, q, collection)
        self.assertEqual(execute.call_count, 1)
        after = snapshot(self.work)
        self.assertEqual(after['confirmed_used_distinct'], 100); self.assertEqual(after['reserved_not_confirmed_distinct'], 1)
        self.assertEqual(after['remaining'], 499)
        latest = [json.loads(p.read_text()) for p in (self.work / 'private/stage1d_label_cohorts').glob('*.json') if json.loads(p.read_text()).get('quota_authorization_id') == AUTH]
        self.assertEqual(len(latest), 1)

    def test_submitted_existing_failed_job_is_used_without_new_pool(self):
        path = self.cohort(['failed-A'], complete=False); self.amend()
        frozen = self.work / 'private/stage1d_sql' / ('a' * 64) / 'freeze_manifest.json'
        atomic_json(frozen, {'kind': 'frontier_labels', 'sql_sha256': 'a' * 64,
            'dependencies': [{'path': path.relative_to(self.work).as_posix(), 'sha256': sha(path)}]})
        job = self.work / 'private/dune_r2_jobs' / ('a' * 64) / 'job.json'
        atomic_json(job, {'state': 'SUBMISSION_UNKNOWN'})
        self.assertEqual(snapshot(self.work)['confirmed_used_distinct'], 0)
        atomic_json(job, {'state': 'QUERY_STATE_FAILED', 'execution_id': 'SYNTHETIC_CONFIRMED_SUBMIT'})
        self.assertEqual(snapshot(self.work)['confirmed_used_distinct'], 1)
        self.assertEqual(snapshot(self.work)['reserved_not_confirmed_distinct'], 0)

    def test_successful_empty_results_are_not_requeried_after_expansion(self):
        self.cohort(['empty-address']); self.amend()
        class Unknown:
            registry = {}
            def __init__(self, work): pass
            def __call__(self, address): return {'kind': 'UNKNOWN', 'status': 'UNQUERIED'}
        with patch.object(acquisition, 'Labels', Unknown), patch.object(acquisition, 'execute_sql') as execute:
            result = acquisition.acquire_labels(self.work, self.queries[2], {'states': [{'state': {'address': 'empty-address'}}]})
        execute.assert_not_called(); self.assertEqual(result['status'], 'NO_NEW_LABEL_OPPORTUNITY')
        self.assertEqual(result['label_opportunity_budget']['confirmed_used_distinct'], 1)


if __name__ == '__main__': unittest.main(verbosity=2)
