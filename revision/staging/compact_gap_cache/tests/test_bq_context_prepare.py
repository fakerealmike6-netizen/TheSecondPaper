import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

STAGE = Path(__file__).resolve().parents[1]
CODE = STAGE
sys.path[:0] = [str(STAGE), str(CODE / 'src')]
import stage1d_bq_context_prepare as p


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=STAGE)
        self.w = Path(self.temp.name)
        (self.w / 'private').mkdir()
        from collector import Scope
        query = {'name':'txphish_src001','query_id':'qry:synthetic:bq-prepare',
                 'start_block':100,'end_block':1000,'start_time_utc':'2024-08-21T00:00:00Z',
                 'end_time_utc':'2024-09-07T00:00:00Z','max_acquisition_depth':13,'window_mode':'REFERENCE_FULL'}
        scope = Scope.from_policy(query)
        query.update(scope_id=scope.scope_id,scope_hash=scope.scope_hash,scope=__import__('dataclasses').asdict(scope))
        query.update(start_block_hash='0x'+'c'*64,end_block_hash='0x'+'d'*64)
        p.save(self.w / 'private/BATCH_QUERY_FREEZE.json', {'queries':[query]})
        freezer = patch.object(p, 'FREEZE_SHA', p.sha(self.w / 'private/BATCH_QUERY_FREEZE.json'))
        freezer.start()
        self.addCleanup(freezer.stop)
        self.q = next(q for q in p.read(self.w / 'private/BATCH_QUERY_FREEZE.json')['queries'] if q['name'] == 'txphish_src001')
        self.a, self.b = '0x' + '1' * 40, '0x' + '2' * 40
        self.ranges = [{'address': self.a, 'start_block': self.q['start_block'], 'end_block': self.q['end_block']}]
        p.save(self.w / 'CURRENT.json', {'current_labels_and_collection': 'synthetic_fixture_only'})
        self.needed = {'query_id': self.q['query_id'], 'scope_hash': self.q['scope_hash'],
                       'freeze_sha256': p.FREEZE_SHA, 'needed_ranges': self.ranges,
                       'scope_dependencies': [p.dep(self.w, 'CURRENT.json')]}
        p.save(self.w / 'NEEDED_RANGES.json', self.needed)
        common = {'block_number': 'INTEGER', 'block_hash': 'STRING', 'block_timestamp': 'TIMESTAMP',
                  'transaction_index': 'INTEGER', 'from_address': 'STRING', 'to_address': 'STRING', 'value': 'NUMERIC'}
        self.fields = {p.TR: dict(common, transaction_hash='STRING', status='INTEGER', trace_address='STRING',
            trace_type='STRING', call_type='STRING', subtraces='INTEGER', error='STRING'),
            p.TX: dict(common, hash='STRING', gas='INTEGER', input='STRING', receipt_status='INTEGER',
                       receipt_gas_used='INTEGER', receipt_effective_gas_price='INTEGER')}
        self.evidence = []
        for index, (table, fields) in enumerate(self.fields.items()):
            filename = f'schema{index}.json'
            p.save(self.w / filename, {'status': 'SUCCESS_VALIDATED', 'result': {'kind': 'SCHEMA', 'table': table,
                'partition': {'field': 'block_timestamp', 'type': 'DAY'},
                'schema': [{'name': n, 'type': t} for n, t in fields.items()]}})
            self.evidence.append(p.dep(self.w, filename))
        self.config = {'tables': [p.TX, p.TR]}

    def tearDown(self):
        self.temp.cleanup()

    def prepare(self, **kw):
        return p.prepare(self.w, 'NEEDED_RANGES.json', 'prepared', self.config, self.evidence, **kw)

    def row(self, kind, **kw):
        row = dict.fromkeys(p.COLUMNS)
        row.update(record_type=kind, block_number=str(self.q['start_block']), block_hash='0x'+'a'*64,
                   tx_hash='0x'+'b'*64, tx_index='0', from_address=self.a, to_address=self.b,
                   value_raw='0', success=False)
        if kind == 'transaction': row['transaction_type'] = '2'  # Explicit nonblob synthetic fixture.
        row.update(kw)
        return row

    def normalized(self, rows, **kw):
        m = self.prepare(templates=('transaction_and_trace',))
        job_states = {s['path']: 'mock-unexecuted-state' for s in m['plans']}
        with patch.object(p, 'verified_export', return_value=(rows, 'SYNTHETIC_TEST_ONLY')):
            return p.normalize_complete_family(self.w, 'prepared/PREPARATION.json', 'transaction_and_trace', job_states)

    def test_prepares_two_forms_three_seven_day_chunks_no_network(self):
        m = self.prepare()
        self.assertEqual(len(m['plans']), 6)
        self.assertFalse(m['actual_query_executed'])
        self.assertEqual(p.date_chunks(self.q), [('2024-08-21', '2024-08-28'), ('2024-08-28', '2024-09-04'), ('2024-09-04', '2024-09-08')])

    def test_limit64_and_exact_scope(self):
        d = copy.deepcopy(self.needed)
        d['needed_ranges'] = [dict(self.ranges[0], address='0x' + str(i).zfill(40)) for i in range(65)]
        with self.assertRaises(ValueError): p.needed_ranges(self.q, d)
        d['needed_ranges'] = [dict(self.ranges[0], start_block=self.q['start_block']-1)]
        with self.assertRaises(ValueError): p.needed_ranges(self.q, d)

    def test_original_freeze_hash_required(self):
        (self.w / 'private/BATCH_QUERY_FREEZE.json').write_text('{}')
        with self.assertRaises(ValueError): self.prepare()

    def test_schema_or_partition_missing_stops_before_sql(self):
        with self.assertRaises(ValueError): p.schemas(self.w, self.evidence[:1], {p.TX, p.TR})
        j = p.read(self.w/'schema0.json'); j['result']['partition'] = None
        (self.w/'schema0.json').write_text(json.dumps(j))
        with self.assertRaises(ValueError): p.schemas(self.w, [p.dep(self.w,'schema0.json')], {p.TR})

    def test_ranges_bound_changed_dependencies(self):
        (self.w/'CURRENT.json').write_text('{}')
        with self.assertRaises(ValueError): self.prepare()

    def test_two_forms_no_amount_or_success_filters_full_tree(self):
        sql = p.build_sql(self.ranges, self.fields, 'transaction_and_trace', '2024-08-21', '2024-08-28')
        self.assertNotIn('SELECT t.*', sql)
        self.assertNotIn('value >', sql)
        self.assertNotIn('status=1 AND', sql)
        self.assertIn('UNION DISTINCT', sql)
        self.assertIn('k.block_number=t.block_number AND k.tx_hash=t.transaction_hash', sql)
        self.assertNotIn('INNER JOIN requested', sql)

    def test_effective_price_absence_not_gas_price_substitution(self):
        fields = copy.deepcopy(self.fields); del fields[p.TX]['receipt_effective_gas_price']
        fields[p.TX]['gas_price'] = 'INTEGER'
        sql = p.build_sql(self.ranges, fields, 'transaction_and_trace', '2024-08-21', '2024-08-28')
        self.assertNotIn('t.gas_price', sql)
        self.assertIn('NULL AS effective_gas_price', sql)

    def test_unsupported_form_or_hourly_chunks_refused(self):
        with self.assertRaises(ValueError): self.prepare(templates=('invented_third_form',))
        with self.assertRaises(ValueError): p.date_chunks(self.q, 2)

    def test_missing_partition_family_cannot_claim_coverage(self):
        m = self.prepare(templates=('trace_only',))
        states = {s['path']: 'mock' for s in m['plans'][:-1]}
        with self.assertRaises(ValueError): p.normalize_complete_family(self.w, 'prepared/PREPARATION.json', 'trace_only', states)

    def test_failed_zero_tx_kept_and_fee_calculated(self):
        top = self.row('transaction', gas_used='21000', effective_gas_price='2000000000')
        trace = self.row('trace', trace_address='[]', trace_type='call', call_type='call', subtraces='0')
        result = self.normalized([top, trace])
        self.assertEqual(result['normalized']['transactions'][0]['fee_raw'], '42000000000000')
        self.assertEqual(result['normalized']['flows'], [])
        self.assertEqual({r['data_type'] for r in result['coverage']}, {p.TOP, p.INTERNAL, p.FEES})
        self.assertEqual(result['gaps'][0]['data_type'], p.PROTOCOL)
        self.assertFalse(result['full_context_claimed'])

    def test_missing_fee_operands_keep_fee_gap(self):
        result = self.normalized([self.row('transaction'), self.row('trace', trace_address='[]', trace_type='call', call_type='call', subtraces='0')])
        self.assertIn(p.FEES, {r['data_type'] for r in result['gaps']})

    def test_missing_status_keeps_top_internal_fee_gaps(self):
        result = self.normalized([self.row('transaction', success=None)])
        self.assertEqual(result['coverage'], [])

    def test_incomplete_tree_conflict_prevents_all_coverage(self):
        result = self.normalized([self.row('transaction', success=True), self.row('trace', success=True,
                     trace_address='[]', trace_type='call', call_type='call', subtraces='1')])
        self.assertEqual(result['coverage'], [])
        self.assertTrue(result['normalized']['conflicts'])

    def test_orphan_trace_cannot_claim_internal_complete(self):
        result = self.normalized([self.row('transaction', success=True),
            self.row('trace', success=True, trace_address='[]', trace_type='call', call_type='call', subtraces='0'),
            self.row('trace', success=True, trace_address='[0,0]', trace_type='call', call_type='call', subtraces='0')])
        self.assertNotIn(p.INTERNAL, {r['data_type'] for r in result['coverage']})

    def test_page_raw_chain_verified_and_tampering_refused(self):
        m = self.prepare(templates=('trace_only',)); spec_dep = m['plans'][0]; spec = p.read(self.w/spec_dep['path'])
        p.save(self.w/'dry.json', {'status': 'SUCCESS_VALIDATED', 'result': {'kind': 'DRY_RUN'},
            'identity': {'project': 'synthetic-test-project', 'sql_sha256': spec['sql_sha256'],
                         'spec_sha256': spec_dep['sha256'], 'scope_hash': spec['scope_hash']}})
        job_id = 'stage1d_recovery_' + p.digest({'sql_sha256': spec['sql_sha256'],
                        'project': 'synthetic-test-project', 'location': 'US'})[:48]
        plan = {'dry_spec_path': spec_dep['path'], 'dry_spec_sha256': spec_dep['sha256'],
                'dry_receipt_path': 'dry.json', 'dry_receipt_sha256': p.sha(self.w/'dry.json')}
        p.save(self.w/'plan.json', plan)
        schema = {'fields': [{'name': name, 'type': 'STRING'} for name in p.COLUMNS]}
        document = {'jobReference': {'jobId': job_id}, 'jobComplete': True, 'totalRows': '0', 'rows': [], 'schema': schema}
        p.save(self.w/'raw.json', document)
        p.save(self.w/'receipt.json', {'status': 'SUCCESS_VALIDATED', 'identity': {'method': 'jobs.getQueryResults',
            'selectors': {'job_id': job_id}}, 'result': document,
            'raw_sources': [dict(p.dep(self.w,'raw.json'), complete=True, http_status=200)]})
        (self.w/'page.jsonl').write_bytes(b'')
        (self.w/'all.jsonl').write_bytes(b'')
        p.save(self.w/'terminal_job.json', {'jobReference': {'jobId': job_id}, 'status': {'state': 'DONE'},
            'configuration': {'query': {'query': (self.w/spec['sql_path']).read_text(encoding='utf-8')}}})
        state = {'state': 'COMPLETE_EXPORTED', 'complete_page_chain': True, 'job_id': job_id,
            'plan_path': 'plan.json', 'plan_sha256': p.sha(self.w/'plan.json'), 'sql_sha256': spec['sql_sha256'],
            'terminal_job_sha256': p.sha(self.w/'terminal_job.json'), 'schema': schema, 'total_rows': 0,
            'rows_path': 'all.jsonl', 'rows_sha256': p.sha(self.w/'all.jsonl'), 'pages': [
                {'page_token': None, 'next_page_token': None, 'row_count': 0, 'rows_path': 'page.jsonl',
                 'rows_sha256': p.sha(self.w/'page.jsonl'), 'response_receipt': {
                     'artifact_path': 'receipt.json', 'artifact_sha256': p.sha(self.w/'receipt.json')}}]}
        p.save(self.w/'job.json', state)
        self.assertEqual(p.verified_export(self.w, 'job.json', spec_dep)[0], [])
        (self.w/'raw.json').write_text('{}')
        with self.assertRaises(ValueError): p.verified_export(self.w, 'job.json', spec_dep)

    def test_fake_dry_run_never_binds(self):
        m = self.prepare(templates=('trace_only',)); spec = m['plans'][0]
        p.save(self.w/'fake.json', {'status': 'SUCCESS_VALIDATED', 'result': {'kind': 'DRY_RUN'}})
        with self.assertRaises(ValueError): p.bind_execution_plan(self.w, spec['path'], 'fake.json', 'execute.json')

    def test_config_copy_retains_original(self):
        base = {'tables': [p.TR], 'authority_source': {'sha256': 'original'}}
        result = p.recovery_config(base)
        self.assertEqual(base['tables'], [p.TR]); self.assertIn(p.TX, result['tables'])
        self.assertEqual(result['authority_source'], base['authority_source'])

    def test_partial_export_rejected(self):
        p.save(self.w/'job.json', {'state': 'SUBMITTED'})
        with self.assertRaises(ValueError): p.verified_export(self.w, 'job.json', {})


if __name__ == '__main__':
    unittest.main()
