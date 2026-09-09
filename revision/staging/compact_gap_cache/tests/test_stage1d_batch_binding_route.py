"""Portable synthetic tests: no production data, DB or external service."""
import copy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

CODE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(CODE / 'src'), str(CODE / 'tests')]
import test_bq_context_prepare as fixtures
import stage1d_batch_binding_route as b
import stage1d_bq_context_prepare as h
import stage1d_transfers_acquisition as t
from collector import NATIVE, Scope


class BatchBindingTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.PreparationTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.addCleanup(self.fixture.tearDown)
        self.w, self.q = self.fixture.w, self.fixture.q
        self.scope = Scope.from_policy(self.q)
        self.need = dict(address=self.fixture.a, asset=NATIVE, query_id=self.q['query_id'],
            direction='OUTGOING', fact_type='POSITIVE_NATIVE_CANDIDATE_INDEX', gap_reason='SYNTHETIC_UNBOUND_INDEX',
            query_name=self.q['name'], scope_id=self.q['scope_id'], scope_hash=self.q['scope_hash'],
            start_block=100, end_block=1000, start_time=self.scope.start_time+3600,
            end_time=self.scope.start_time+7200)
        document = dict(query_id=self.q['query_id'], scope_hash=self.q['scope_hash'],
            freeze_sha256=h.sha(self.w/'private/BATCH_QUERY_FREEZE.json'), need_rectangles=[self.need],
            scope_dependencies=[h.dep(self.w, 'CURRENT.json')])
        h.save(self.w/'NEEDS.json', document)
        self.config = self.fixture.config
        self.evidence = self.fixture.evidence

    def prepare(self):
        return b.prepare(self.w, 'NEEDS.json', 'batch', self.config, self.evidence)

    def family(self, success=True, amount='3', children=1):
        def row(kind, **kw):
            r = dict.fromkeys(h.columns_for('transaction_and_trace'))
            r.update(record_type=kind, block_number='100', block_hash='0x'+'a'*64,
                block_time='2024-08-21T01:30:00Z', tx_hash='0x'+'b'*64, tx_index='0',
                from_address=self.fixture.a, to_address=self.fixture.b, value_raw=amount,
                success=success, evidence_ids=['SYNTHETIC_FULL_PAGE'], input_data='0x')
            r.update(kw)
            return r
        top = row('transaction', gas_used='21000', effective_gas_price='2', gas_limit='50000', transaction_type='2')
        root = row('trace', trace_address='[]', trace_type='call', call_type='call', subtraces=str(children))
        rows = [top, root]
        if children:
            rows.append(row('trace', trace_address='[0]', trace_type='call', call_type='call', subtraces='0',
                from_address=self.fixture.b, to_address='0x'+'3'*40, value_raw='1'))
        return rows

    def test_exact_w_dates_not_whole_global_block_time_domain(self):
        manifest = self.prepare()
        self.assertEqual(manifest['date_chunks'], [['2024-08-21', '2024-08-22']])
        self.assertEqual(len(manifest['plans']), 1)
        self.assertEqual(b.verified_preparation(self.w, 'batch/PREPARATION.json')[0], manifest)
        self.assertEqual(manifest['need_rectangles'][0]['end_time'], self.need['end_time'])
        self.assertEqual(manifest['needed_ranges'][0]['end_block'], 1000)

    def test_sql_actual_trace_input_only_when_schema_proves_field(self):
        fields = copy.deepcopy(self.fixture.fields)
        original = b.build_binding_sql(self.fixture.ranges, fields, '2024-08-21', '2024-08-22')
        self.assertIn('NULL AS input_data', original)
        fields[h.TR]['input'] = 'STRING'
        actual = b.build_binding_sql(self.fixture.ranges, fields, '2024-08-21', '2024-08-22')
        self.assertIn('trace_scope AS (SELECT t.input,', actual)
        self.assertEqual(actual.count('t.input AS input_data'), 2)
        self.assertNotIn('value >', actual)
        self.assertIn('t.receipt_gas_used', actual)

    def test_nonadjacent_days_do_not_create_unrequested_date_bridge(self):
        later = dict(self.need, start_time=self.need['start_time']+3*86400, end_time=self.need['end_time']+3*86400)
        self.assertEqual(b.needed_day_chunks([self.need, later]),
            [['2024-08-21', '2024-08-22'], ['2024-08-24', '2024-08-25']])

    def test_full_tree_normal_internal_and_failed_zero_gas_kept(self):
        rows = self.family()
        v = b.validate_transaction_rows(rows)
        self.assertFalse(v['gaps'])
        self.assertEqual(len(v['full_tree_proofs']), 1)
        events, gaps = b._events(rows, v)
        self.assertFalse(gaps)
        self.assertEqual({r['kind'] for r in events}, {'top', 'internal'})
        self.assertEqual(events[0]['gas_raw'], 42000)
        rows = self.family(False, '0', 0)
        v = b.validate_transaction_rows(rows)
        events, gaps = b._events(rows, v)
        self.assertFalse(gaps)
        self.assertEqual(len(events), 1)
        self.assertFalse(events[0]['success'])
        self.assertEqual(events[0]['amount_raw'], 0)
        self.assertEqual(events[0]['gas_raw'], 42000)

    def test_null_root_or_missing_sibling_or_ancestor_cannot_full(self):
        for mutate in (lambda r: r[1].update(trace_address=None), lambda r: r.pop(),
                       lambda r: r[2].update(trace_address='[0,0]'),
                       lambda r: r[2].update(block_time='2024-08-21T01:30:01Z')):
            rows = self.family(); mutate(rows)
            result = b.validate_transaction_rows(rows)
            self.assertFalse(result['full_tree_proofs'])
            self.assertTrue(result['gaps'])

    def test_missing_fee_keeps_fee_gap_without_faking_full_context(self):
        rows = self.family(); rows[0]['effective_gas_price'] = None
        result = b.validate_transaction_rows(rows)
        self.assertTrue(result['fee_gaps'])
        self.assertFalse(result['full_context_claimed'])

    def test_cache_threshold_after_successes_preserves_exact_missing(self):
        requests = [{'method': 'eth_getTransactionByHash', 'params': ['0x'+format(i, '064x')]} for i in range(203)]
        state = {'bindings': [], 'status': 'PREPARED'}
        counters = {'members': 0, 'dispatches': 0}
        access = Mock()
        member = {'fixture': 'already verified by synthetic seam'}
        def save(work, state, state_path, request, member, known):
            state['bindings'].append({'plan': request, 'member': member}); known.add(t.digest(request))
        with patch.object(t, 'cached_member', side_effect=lambda w, p: member if p in requests[:2] else None), \
             patch.object(t, '_save_binding', side_effect=save):
            t._acquire_bindings(self.w, state, self.w/'state.json', requests, access, self.q['name'], counters, 10000)
        self.assertEqual(state['status'], 'BATCH_BINDING_REQUIRED')
        self.assertEqual(state['batch_binding_required']['missing_selectors'], requests[2:])
        self.assertEqual(len(state['bindings']), 2)
        self.assertEqual(state['batch_binding_required']['successful_cache_selectors_reused_this_pass'], 2)
        self.assertEqual(counters, {'members': 0, 'dispatches': 0})
        access.call_batch.assert_not_called()

    def test_two_hundred_is_not_a_batch_trigger(self):
        requests = [{'method': 'eth_getTransactionByHash', 'params': ['0x'+format(i, '064x')]} for i in range(200)]
        state = {'bindings': [], 'status': 'PREPARED'}
        with patch.object(t, 'cached_member', return_value=None):
            t._acquire_bindings(self.w, state, self.w/'state.json', requests, Mock(), self.q['name'], {'members': 0, 'dispatches': 0}, 0)
        self.assertEqual(state['status'], 'SCHEDULING_BINDING_BOUND_PAUSED')
        self.assertNotIn('batch_binding_required', state)

    def test_complete_import_reverifies_and_never_widens_seconds(self):
        manifest = self.prepare()
        h.save(self.w/'job.json', {'synthetic': 'gateway fixture seam'})
        states = {manifest['plans'][0]['path']: 'job.json'}
        with patch.object(h, 'verified_export', return_value=(self.family(), 'SYNTHETIC_FULL_PAGE')):
            result = b.import_completed(self.w, 'batch/PREPARATION.json', states)
            self.assertEqual(result['status'], 'NATIVE_BATCH_BINDING_COMPLETE')
            record = h.read(self.w/result['coverage_records'][0]['path'])
            self.assertEqual(record['end_time'], self.need['end_time'])
            self.assertFalse(record['context_complete'])
            b.verify_interval_record(self.w, record)
            record['end_time'] += 1
            with self.assertRaises(ValueError): b.verify_interval_record(self.w, record)

    def test_missing_job_prevents_import(self):
        self.prepare()
        with self.assertRaises(ValueError): b.import_completed(self.w, 'batch/PREPARATION.json', {})

    def test_prepare_and_default_worker_do_not_dispatch(self):
        self.prepare()
        with patch('stage1d_bigquery_probe.probe', side_effect=AssertionError('network forbidden')):
            result = b.execute_prepared(self.w, 'batch/PREPARATION.json', 'unused')
        self.assertEqual(result['status'], 'PREPARED_NO_NETWORK')

    def test_worker_same_saved_plan_stops_on_incomplete_job(self):
        self.prepare(); h.save(self.w/'config.json', self.config)
        h.save(self.w/'batch/transaction_and_trace/000/execution_plan.json', {'fixture': 'pre-existing plan'})
        with patch('stage1d_bigquery_probe.probe', side_effect=AssertionError('must resume original plan')), \
             patch('stage1d_bigquery_jobs.execute_plan', return_value={'state': 'SUBMITTED'}) as execute:
            result = b.execute_prepared(self.w, 'batch/PREPARATION.json', 'config.json', execute=True)
        execute.assert_called_once()
        self.assertEqual(result['status'], 'BATCH_JOB_INCOMPLETE_SAME_PLAN_PRESERVED')
        self.assertTrue(result['needs_preserved'])

    def test_superset_requires_real_predicate_pages_values_not_requesthash(self):
        source = dict(sql_predicate_verified=True, schema_verified=True, complete_page_chain=True,
            values_committed=True, full_transaction_families_verified=True,
            request_hash='different historical demand', verified_rectangles=[dict(self.need, end_time=self.need['end_time']+1)])
        self.assertTrue(b.superset_admissibility(self.need, source)['eligible_complete_superset'])
        source['values_committed'] = False; source.pop('sql_predicate_verified')
        result = b.superset_admissibility(self.need, source)
        self.assertFalse(result['eligible_complete_superset'])
        self.assertIn('values_committed', result['missing_proofs'])
        self.assertFalse(result['metadata_alone_is_full'])

    def saved_export(self, manifest, rows):
        """Actual REST f/v BOOLEAN rows, two pages, original raw/SQL/dry/job chain."""
        spec_dep = manifest['plans'][0]; spec = h.read(self.w/spec_dep['path'])
        project = 'synthetic-test-project'
        h.save(self.w/'dry.json', {'status': 'SUCCESS_VALIDATED', 'result': {'kind': 'DRY_RUN',
            'actual_query_executed': False, 'estimated_processed_bytes': 0}, 'identity': {'project': project,
            'sql_sha256': spec['sql_sha256'], 'spec_sha256': spec_dep['sha256'], 'scope_hash': spec['scope_hash']}})
        h.bind_execution_plan(self.w, spec_dep['path'], 'dry.json', 'batch/transaction_and_trace/000/execution_plan.json')
        digest = h.digest({'sql_sha256': spec['sql_sha256'], 'project': project, 'location': 'US'})
        folder = self.w/'private/stage1d_bigquery_jobs'/digest
        job_id = 'stage1d_recovery_'+digest[:48]
        reference = {'jobId': job_id, 'projectId': project, 'location': 'US'}
        h.save(folder/'terminal_job.json', {'jobReference': reference, 'status': {'state': 'DONE'},
            'configuration': {'query': {'query': (self.w/spec['sql_path']).read_text(encoding='utf-8'), 'useLegacySql': False}}})
        columns = h.columns_for('transaction_and_trace')
        schema = {'fields': [{'name': k, 'type': 'BOOLEAN' if k in ('success', 'tx_success') else 'STRING'} for k in columns]}
        canonical = [{k: row[k] for k in columns} for row in rows]
        pages, combined = [], b''
        for index, chunk in enumerate((canonical[:1], canonical[1:])):
            token, next_token = (None, 'second') if index == 0 else ('second', None)
            body = {'jobReference': reference, 'jobComplete': True, 'totalRows': str(len(canonical)), 'schema': schema,
                'rows': [{'f': [{'v': str(row[k]).lower() if type(row[k]) is bool else row[k]} for k in columns]} for row in chunk]}
            if next_token: body['pageToken'] = next_token
            h.save(folder/f'raw{index}.json', body)
            h.save(folder/f'envelope{index}.json', {'status': 'SUCCESS_VALIDATED', 'identity': {
                'method': 'jobs.getQueryResults', 'project': project, 'location': 'US',
                'selectors': {'job_id': job_id, 'pageToken': token}}, 'result': body,
                'raw_sources': [dict(h.dep(self.w, folder/f'raw{index}.json'), complete=True, http_status=200)]})
            data = b''.join((json.dumps(row, sort_keys=True)+'\n').encode() for row in chunk)
            combined += data
            (folder/f'rows{index}.jsonl').write_bytes(data)
            pages.append({'page_token': token, 'next_page_token': next_token, 'row_count': len(chunk),
                'rows_path': h.dep(self.w, folder/f'rows{index}.jsonl')['path'], 'rows_sha256': h.sha(folder/f'rows{index}.jsonl'),
                'response_receipt': {'artifact_path': h.dep(self.w, folder/f'envelope{index}.json')['path'],
                    'artifact_sha256': h.sha(folder/f'envelope{index}.json')}})
        (folder/'all_rows.jsonl').write_bytes(combined)
        plan_path = self.w/'batch/transaction_and_trace/000/execution_plan.json'
        state = {'state': 'COMPLETE_EXPORTED', 'complete_page_chain': True, 'job_id': job_id,
            'plan_path': h.dep(self.w, plan_path)['path'], 'plan_sha256': h.sha(plan_path), 'sql_sha256': spec['sql_sha256'],
            'terminal_job_sha256': h.sha(folder/'terminal_job.json'), 'schema': schema, 'total_rows': len(canonical),
            'rows_path': h.dep(self.w, folder/'all_rows.jsonl')['path'], 'rows_sha256': h.sha(folder/'all_rows.jsonl'), 'pages': pages}
        h.save(folder/'job.json', state)
        return state, {spec_dep['path']: h.dep(self.w, folder/'job.json')['path']}

    def test_original_multipage_to_cache_and_portable_receiver(self):
        manifest = self.prepare()
        state, states = self.saved_export(manifest, self.family())
        result = b.import_completed(self.w, 'batch/PREPARATION.json', states)
        record = h.read(self.w/result['coverage_records'][0]['path'])
        b.verify_interval_record(self.w, record)
        from stage1d_bq_portable import verify_transaction_family_documents
        docs = b.collect_portable_dependencies(self.w, 'batch/PREPARATION.json', states, '0x'+'b'*64)
        checked = verify_transaction_family_documents('batch/PREPARATION.json', states, '0x'+'b'*64, docs)
        self.assertEqual(len(checked['rows']), 3)
        self.assertEqual(checked['jobs'][0]['page_count'], 2)
        original = state['pages'][1]['response_receipt']['artifact_path']
        docs[original] += b' '
        with self.assertRaises(ValueError):
            verify_transaction_family_documents('batch/PREPARATION.json', states, '0x'+'b'*64, docs)

    def test_worker_complete_real_saved_pages_imports_and_cache_filters_seconds(self):
        manifest = self.prepare(); h.save(self.w/'config.json', self.config)
        state, states = self.saved_export(manifest, self.family())
        with patch('stage1d_bigquery_probe.probe', side_effect=AssertionError('existing exact dry plan')), \
             patch('stage1d_bigquery_jobs.execute_plan', return_value=state):
            result = b.execute_prepared(self.w, 'batch/PREPARATION.json', 'config.json', execute=True)
        self.assertEqual(result['status'], 'NATIVE_BATCH_BINDING_COMPLETE')
        from stage1d_acquisition import CachedIntervals
        cache = CachedIntervals(self.w); cache.bind_scope(self.scope)
        inside = cache.fetch_interval(self.need['address'], NATIVE, 100, 1000,
            start_time=self.need['start_time'], end_time=self.need['end_time'], global_end_time=self.scope.end_time)
        self.assertTrue(inside.complete)
        self.assertEqual(len(inside.events), 1)
        outside = cache.fetch_interval(self.need['address'], NATIVE, 100, 1000,
            start_time=self.need['end_time']+1, end_time=self.need['end_time']+100, global_end_time=self.scope.end_time)
        self.assertFalse(outside.complete)
        self.assertEqual(len(outside.events), 0)

    def test_partial_null_root_proof_is_stable_when_no_rpc_is_claimed(self):
        manifest = self.prepare(); rows = self.family(); rows[1]['trace_address'] = None
        state, states = self.saved_export(manifest, rows)
        with patch.object(t, 'cached_member', side_effect=[None, {'synthetic': 'present but incomplete trio'}, None]):
            result = b.import_completed(self.w, 'batch/PREPARATION.json', states)
        self.assertEqual(result['status'], 'BATCH_BINDING_PARTIAL_WITH_EXPLICIT_GAPS')
        record = h.read(self.w/result['coverage_records'][0]['path'])
        with patch.object(t, 'cached_member', side_effect=AssertionError('proof must recheck fixed inputs, not current DB')):
            b.verify_interval_record(self.w, record)

    def test_superset_adapter_rechecks_actual_family_and_rejects_metadata(self):
        manifest = self.prepare()
        state, states = self.saved_export(manifest, self.family())
        narrower = dict(self.need, start_time=self.need['start_time']+1, end_time=self.need['end_time']-1)
        with patch.object(t, 'FREEZE_SHA', h.sha(self.w/'private/BATCH_QUERY_FREEZE.json')):
            result = b.verified_superset(self.w, 'batch/PREPARATION.json', states, narrower)
            self.assertTrue(result['eligible_complete_superset'])
            self.assertFalse(result['coverage_installed'])
            h.save(self.w/'metadata.json', {'request_sha256': 'known-old-request', 'parameter_metadata': [
                {'name': 'hashes', 'value_count': 103, 'values_committed': False}]})
            with self.assertRaisesRegex(ValueError, 'Metadata without original SQL'):
                b.verified_superset(self.w, 'metadata.json', {}, narrower)
            raw_path = self.w/state['pages'][1]['response_receipt']['artifact_path']
            raw_path.write_bytes(raw_path.read_bytes()+b' ')
            with self.assertRaises(ValueError):
                b.verified_superset(self.w, 'batch/PREPARATION.json', states, narrower)


if __name__ == '__main__':
    unittest.main()
