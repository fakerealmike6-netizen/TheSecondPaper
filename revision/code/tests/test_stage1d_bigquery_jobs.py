import unittest
from stage1d_bigquery_jobs import request_spec, monthly_usage, decode_rows


class BigQueryJobsTests(unittest.TestCase):
    def test_only_explicit_read_job_endpoints(self):
        with self.assertRaises(ValueError):
            request_spec('ethereum-paper-analysis', 'tables.insert', {})
        with self.assertRaises(ValueError):
            request_spec('ethereum-paper-analysis', 'jobs.get', {'job_id': '../other'})
        spec = request_spec('ethereum-paper-analysis', 'jobs.getQueryResults', {'job_id': 'fixed', 'pageToken': 'page'})
        self.assertEqual(spec[2]['location'], 'US')

    def test_persistent_tables_cannot_be_created(self):
        body = {'jobReference': {'projectId': 'ethereum-paper-analysis', 'location': 'US'},
                'configuration': {'query': {'query': 'SELECT 1', 'useLegacySql': False,
                    'maximumBytesBilled': '100', 'destinationTable': {}}}}
        with self.assertRaises(ValueError):
            request_spec('ethereum-paper-analysis', 'jobs.insert', {}, body)

    def test_month_and_script_deduplication(self):
        def job(identifier, created, billed, statement='SELECT'):
            return {'jobReference': {'projectId': 'p', 'jobId': identifier},
                    'configuration': {'query': {}}, 'status': {'state': 'DONE'},
                    'statistics': {'creationTime': str(created),
                       'query': {'totalBytesBilled': str(billed), 'statementType': statement}}}
        child = job('child', 20, 100)
        child['statistics']['parentJobId'] = 'parent'
        result = monthly_usage([job('old', 5, 900), job('parent', 20, 100, 'SCRIPT'), child, child], 10, 30)
        self.assertEqual(result['known_billed_bytes'], 100)
        self.assertTrue(result['complete_settlement'])
        parent_only = monthly_usage([job('parent', 20, 100, 'SCRIPT')], 10, 30)
        self.assertEqual(parent_only['known_billed_bytes'], 100)
        self.assertTrue(parent_only['complete_settlement'])
        unknown = job('unknown', 20, 0)
        del unknown['statistics']['query']['totalBytesBilled']
        self.assertFalse(monthly_usage([unknown], 10, 30)['complete_settlement'])

    def test_exact_integer_and_empty_page(self):
        document = {'schema': {'fields': [{'name': 'value_raw', 'type': 'STRING'}, {'name': 'status', 'type': 'BOOLEAN'}]},
                    'rows': [{'f': [{'v': '3879688898937200589030'}, {'v': 'true'}]}]}
        self.assertEqual(decode_rows(document), [{'value_raw': '3879688898937200589030', 'status': True}])
        self.assertEqual(decode_rows({'rows': []}), [])
        with self.assertRaises(ValueError):
            decode_rows({'schema': document['schema'], 'rows': [{'f': [{'v': '1'}]}]})


if __name__ == '__main__':
    unittest.main()
