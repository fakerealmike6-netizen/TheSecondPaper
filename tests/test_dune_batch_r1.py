"""Synthetic-only frozen union/page replay controls; no transport is imported."""
import copy
import csv
import json
from pathlib import Path
import sys
import tempfile
import unittest

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE / 'src'))
from dune_batch_r1 import prepare, verify_frozen, load_completed_batch, BatchSavedDuneProvider, select_intervals, replay, label_identity, collect_with_labels
from collector import Event, Scope
from dune_observed_replay import read_json, sha, write_json, parse_interval_sql


class DuneBatchR1Tests(unittest.TestCase):
    def setUp(self):
        scratch = BASE / '.testtmp'; scratch.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=scratch); self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.data = read_json(BASE / 'fixtures/fault/dune_batch/synthetic_plan.json')
        write_json(self.work / 'frontier.json', self.data['frontier'])
        write_json(self.work / 'policy.json', self.data['policy'])
        with (self.work / 'registry.csv').open('w', newline='', encoding='utf-8') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(self.data['registry'][0])); writer.writeheader(); writer.writerows(self.data['registry'])
        write_json(self.work / 'labels.json', {'registry_path': 'registry.csv', 'registry_sha256': sha(self.work / 'registry.csv'), 'evidence_kind': 'SYNTHETIC_ONLY'})
        self.freeze = self.work / 'prepared/freeze_manifest.json'
        self.manifest = prepare(self.work / 'frontier.json', 'synthetic_probe', self.work / 'labels.json',
                                self.work / 'policy.json', self.work, self.freeze.parent)
        self.job = self.work / 'jobs/batch'; self.job.mkdir(parents=True)
        (self.work / 'old_jobs').mkdir()

    def raw(self, name, value, **fields):
        path = self.work / 'raw' / (name + '.json'); write_json(path, value)
        return {'http_status': 200, 'error_class': None, 'raw_path': path.relative_to(self.work).as_posix(),
                'raw_bytes': path.stat().st_size, 'sha256': sha(path), **fields}

    def saved_job(self, rows, page_limit=50):
        execution = 'SYNTHETIC_EXECUTION'
        submit = {'execution_id': execution, 'state': 'QUERY_STATE_PENDING'}
        status = {'execution_id': execution, 'state': 'QUERY_STATE_COMPLETED', 'result_metadata': {'total_row_count': len(rows)}}
        job = {'kind': 'candidate', 'state': 'QUERY_STATE_COMPLETED', 'execution_id': execution,
               'logical_job_id': 'synthetic-batch-job', 'sql_sha256': self.manifest['full_sql_sha256'],
               'scope_freeze_path': str(self.freeze), 'scope_freeze_sha256': sha(self.freeze),
               'submit_response': submit, 'status_response': status,
               'submit_receipt': self.raw('submit', submit, operation='execute'),
               'status_receipt': self.raw('status', status, operation='status', execution_id=execution)}
        write_json(self.job / 'job.json', job)
        (self.job / 'query.sql').write_bytes((self.freeze.parent / 'query.sql').read_bytes())
        for offset in range(0, max(1, len(rows)), page_limit):
            values = rows[offset:offset+page_limit]
            page = {'execution_id': execution, 'state': 'QUERY_STATE_COMPLETED',
                    'result': {'rows': values, 'metadata': {'total_row_count': len(rows), 'row_count': len(values)}}}
            if offset + len(values) < len(rows):
                page['next_offset'] = offset + len(values)
            receipt = self.raw('page_' + str(offset), page, operation='results', execution_id=execution, parameters={'offset': offset, 'limit': page_limit})
            write_json(self.job / ('page_%d.json' % offset), page); write_json(self.job / ('page_%d_receipt.json' % offset), receipt)
        return job

    def row(self, i=0, **changes):
        return self.data['row'] | changes | {'interval_id': self.manifest['intervals'][i]['interval_id']}

    def test_exact_full_fragments_no_limit_and_round_trip_verify(self):
        self.assertEqual(len(verify_frozen(self.freeze)['intervals']), 2)
        sql = (self.freeze.parent / 'query.sql').read_text()
        self.assertNotRegex(sql, r'\bLIMIT\b|\bTOP\b')
        for interval in self.manifest['intervals']:
            parsed = parse_interval_sql((self.freeze.parent / interval['fragment_path']).read_text())
            self.assertEqual(parsed, interval['scope'])
        with self.assertRaises(FileExistsError):
            prepare(self.work / 'frontier.json', 'synthetic_probe', self.work / 'labels.json', self.work / 'policy.json', self.work, self.freeze.parent)

    def test_scope_and_frozen_sql_tampering_rejected(self):
        manifest = read_json(self.freeze); manifest['intervals'][0]['scope']['start_block'] = 99
        write_json(self.freeze, manifest)
        with self.assertRaisesRegex(ValueError, 'scope or label altered'):
            verify_frozen(self.freeze)
        write_json(self.freeze, self.manifest)
        path = self.freeze.parent / 'query.sql'; path.write_text(path.read_text() + '\nLIMIT 1')
        with self.assertRaisesRegex(ValueError, 'union SQL'):
            verify_frozen(self.freeze)

    def test_bounds_cannot_widen_shorten_or_reset_arrival_window(self):
        registry = {r['address']: r for r in self.data['registry']}
        for field, value in [('start_block', 99), ('end_block', 111), ('end_time', 2001), ('end_time', 1999), ('start_time', 999)]:
            frontier = copy.deepcopy(self.data['frontier']); frontier[0][field] = value
            with self.assertRaises(ValueError):
                select_intervals(frontier, self.data['policy'], 'synthetic_probe', registry)

    def test_service_stops_but_empty_and_unqueried_unknown_continue(self):
        registry = {r['address']: r for r in self.data['registry']}
        selected, _ = select_intervals(self.data['frontier'], self.data['policy'], 'synthetic_probe', registry)
        self.assertEqual(len(selected), 2)
        registry[self.data['frontier'][0]['address']] = {'identity_class': 'SERVICE'}
        selected, excluded = select_intervals(self.data['frontier'], self.data['policy'], 'synthetic_probe', registry)
        self.assertEqual(len(selected), 1); self.assertEqual(excluded[0]['reason'], 'FROZEN_LABEL_BOUNDARY')

    def test_failed_lookup_context_row_remains_gap_and_unknown_frontier(self):
        registry = {r['address']: dict(r, lookup_status='LOOKUP_FAILED_RESOURCE_CAP') for r in self.data['registry']}
        selected, excluded = select_intervals(self.data['frontier'], self.data['policy'], 'synthetic_probe', registry)
        self.assertEqual(len(selected), 2); self.assertEqual(excluded, [])
        for address in registry:
            identity = label_identity(registry, address)
            self.assertEqual(identity['kind'], 'UNKNOWN'); self.assertEqual(identity['status'], 'LOOKUP_FAILED')
            self.assertEqual(identity['lookup_status'], 'LOOKUP_FAILED_RESOURCE_CAP')

    def test_pure_actor_conflict_continues_ordinary_transfer_and_keeps_distinct_gap(self):
        registry = {r['address']: dict(r) for r in self.data['registry']}
        first, second = list(registry)
        registry[first].update(identity_class='CONFLICTED_IDENTITY', actor='Disputed actors', lookup_status='COMPLETED')
        registry[second].update(identity_class='SERVICE', actor='Synthetic service', lookup_status='COMPLETED')
        selected, _ = select_intervals(self.data['frontier'], self.data['policy'], 'synthetic_probe', registry)
        self.assertEqual([r['scope']['address'] for r in selected], [first])
        self.assertEqual(label_identity(registry, first)['kind'], 'UNKNOWN')
        self.assertEqual(label_identity(registry, first)['status'], 'LABEL_CONFLICT')
        self.saved_job([self.row()])
        provider = BatchSavedDuneProvider(self.work / 'old_jobs', self.work, batch_specs=[(self.job, self.freeze)])
        tx = '0x' + 'f' * 64
        seed = Event('eip155:1:tx:' + tx + ':top', tx, '0x' + 'c' * 40, first, 'native:eip155:1', 10, 100, 0, 1000)
        result = collect_with_labels(provider, registry, Scope.from_policy(self.data['policy']['query_pilots'][0]), seed)
        self.assertEqual(len(result.candidate_events), 2)
        self.assertEqual(result.stops[0]['reason'], 'FIRST_IDENTIFIED_SERVICE')
        self.assertTrue(any(g['reason'] == 'LABEL_CONFLICT' and g['continues_as_unknown'] for g in result.gaps))
        self.assertEqual(result.fact_conflicts, [])

    def test_complete_batch_proves_empty_subinterval(self):
        self.saved_job([self.row()])
        jobs = load_completed_batch(self.job, self.work)
        self.assertEqual([j['exported_rows'] for j in jobs], [1, 0])
        self.assertTrue(all(j['export_complete'] for j in jobs))
        self.assertEqual(jobs[1]['zero_row_completeness_basis'], 'ENTIRE_FROZEN_BATCH_RESULT_PAGINATION_COMPLETE')
        provider = BatchSavedDuneProvider(self.work / 'old_jobs', self.work, batch_specs=[(self.job, self.freeze)])
        scope = jobs[1]['scope']
        result = provider.fetch_interval(**scope, global_end_time=2000)
        self.assertTrue(result.complete); self.assertEqual(result.events, []); self.assertEqual(result.real_requests, 0)

    def test_zero_total_still_requires_an_actual_successful_terminal_page(self):
        self.saved_job([])
        self.assertEqual(len(load_completed_batch(self.job, self.work)), 2)
        (self.job / 'page_0.json').unlink()
        with self.assertRaises(OSError):
            load_completed_batch(self.job, self.work)

    def test_missing_page_next_uri_or_execution_conflict_never_complete(self):
        self.saved_job([self.row(), self.row(1)], page_limit=1)
        (self.job / 'page_1.json').unlink()
        with self.assertRaises(OSError):
            load_completed_batch(self.job, self.work)
        self.saved_job([self.row()])
        page = read_json(self.job / 'page_0.json'); page['next_uri'] = 'https://outside.invalid/unsafe'
        receipt = self.raw('page_0', page, operation='results', execution_id='SYNTHETIC_EXECUTION', parameters={'offset': 0, 'limit': 50})
        write_json(self.job / 'page_0.json', page); write_json(self.job / 'page_0_receipt.json', receipt)
        with self.assertRaises(ValueError):
            load_completed_batch(self.job, self.work)

    def test_original_raw_hash_and_submission_binding_required(self):
        job = self.saved_job([self.row()])
        (self.work / job['submit_receipt']['raw_path']).write_text('{}')
        with self.assertRaisesRegex(ValueError, 'receipt does not verify'):
            load_completed_batch(self.job, self.work)

    def test_raw_decimal_receipt_matches_root_exact_decimal_persistence(self):
        job = self.saved_job([self.row()])
        job['status_response']['execution_cost_credits'] = '0.123456789123456789'
        path = self.work / job['status_receipt']['raw_path']
        wire = json.dumps(job['status_response']).replace('"0.123456789123456789"', '0.123456789123456789')
        path.write_text(wire, encoding='utf-8')
        job['status_receipt'].update(raw_bytes=path.stat().st_size, sha256=sha(path))
        write_json(self.job / 'job.json', job)
        self.assertEqual(len(load_completed_batch(self.job, self.work)), 2)
        job['status_response']['execution_cost_credits'] = '0.123456789123456788'
        write_json(self.job / 'job.json', job)
        with self.assertRaisesRegex(ValueError, 'execution binding differs'):
            load_completed_batch(self.job, self.work)

    def test_unknown_interval_and_out_of_scope_row_rejected(self):
        self.saved_job([self.row() | {'interval_id': 'foreign'}])
        with self.assertRaisesRegex(ValueError, 'unknown frozen interval'):
            load_completed_batch(self.job, self.work)
        self.saved_job([self.row(block_number=111)])
        with self.assertRaisesRegex(ValueError, 'violates its frozen interval'):
            load_completed_batch(self.job, self.work)

    def test_overlapping_intervals_deduplicate_physical_fact_and_conflicts_quarantine(self):
        self.saved_job([self.row(), self.row(1)])
        provider = BatchSavedDuneProvider(self.work / 'old_jobs', self.work, batch_specs=[(self.job, self.freeze)])
        self.assertEqual(len(provider.fact_snapshot['events']), 1)
        self.saved_job([self.row(), self.row(1, amount_raw='11')])
        provider = BatchSavedDuneProvider(self.work / 'old_jobs', self.work, batch_specs=[(self.job, self.freeze)])
        result = provider.fetch_interval(**self.manifest['intervals'][0]['scope'], global_end_time=2000)
        self.assertFalse(result.complete); self.assertTrue(result.fact_conflicts); self.assertEqual(result.events, [])

    def test_full_offline_replay_uses_exact_seed_and_preserves_unqueried_label_gap(self):
        self.saved_job([self.row(), self.row(1)])
        tx = '0x' + 'f' * 64; eid = 'eip155:1:tx:' + tx + ':top'
        policy = copy.deepcopy(self.data['policy']); policy['query_pilots'][0]['seed_event_id'] = eid
        write_json(self.work / 'replay_policy.json', policy)
        event = {'event_id': eid, 'tx_hash': tx, 'from_address': '0x' + 'c' * 40,
                 'to_address': self.data['registry'][0]['address'], 'asset_key': 'native:eip155:1',
                 'amount_raw': '10', 'block_number': '100', 'block_timestamp': '1970-01-01T00:16:40Z',
                 'transaction_index': '0', 'transaction_status': 'SUCCESS', 'chain_id': '1',
                 'event_type': 'ETH_TOP_LEVEL', 'log_index': '', 'trace_address': '', 'raw_evidence_sha256': 'SYNTHETIC'}
        member = {'query_id': 'synthetic-query', 'seed_rule': 'S2', 'seed_event_id': eid, 'seed_match_status': 'EXACT_EVENT_MATCH',
                  'seed_amount_raw': '10', 'seed_asset': event['asset_key'], 'seed_from': event['from_address'],
                  'seed_to': event['to_address'], 'seed_time': event['block_timestamp'], 'seed_tx_hash': tx}
        for filename, row in [('events.csv', event), ('members.csv', member)]:
            with (self.work / filename).open('w', newline='', encoding='utf-8') as handle:
                writer = csv.DictWriter(handle, fieldnames=list(row)); writer.writeheader(); writer.writerow(row)
        result = replay(self.work / 'replay_policy.json', self.work / 'events.csv', self.work / 'members.csv',
                        self.work / 'labels.json', self.work, self.work / 'old_jobs', self.work / 'jobs', self.work, self.work / 'replayed')
        self.assertEqual(result[0]['candidate_event_count'], 2)
        self.assertEqual(result[0]['unresolved_frontier_count'], 0)
        self.assertEqual(result[0]['unqueried_label_addresses'], [self.data['registry'][1]['address']])
        self.assertEqual(result[0]['status'], 'PARTIAL')
        self.assertEqual(result[0]['replay_network_requests'], 0)
        self.assertEqual(result[0]['live_logical_job_count'], 1)
        self.assertEqual(result[0]['live_exported_rows'], 2)
        self.assertEqual(result[0]['live_normalized_distinct_events'], 1)


if __name__ == '__main__':
    unittest.main()
