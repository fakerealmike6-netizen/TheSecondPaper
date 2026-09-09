"""Synthetic SDK metadata and dry-run governance; no external requests."""
from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
import unittest

from page_attempts import atomic_json
from read_retry_r4 import ReadRetryStore
import stage1d_bigquery_probe as probe

TABLE = 'synthetic-project.ethereum.transactions_by_from_address'


class Ledger:
    def __init__(self): self.jobs = {}
    def reserve(self, job, provider, purpose, amounts): self.jobs[job] = dict(amounts)
    def settle(self, job, amounts): self.jobs[job].update(amounts)
    def snapshot(self):
        return {'bigquery_bytes': {'cap': str(probe.TOTAL_SCAN_CAP), 'actual': '10', 'reserved': '20',
                                  'confirmed_allowance': None}}


class Runtime:
    def __init__(self, ledger): self.ledger = ledger; self.time = 1000.0
    def ledger_factory(self, path): return self.ledger
    def raw_risk(self, work):
        return sum(json.loads(p.read_text())['additional_raw_risk_bytes'] for p in
                   (work/'private/context_uncertainty').glob('*.json'))
    @contextmanager
    def session(self, work, query, label, synthetic=False):
        lock = work/'private/network_worker.lock'
        with lock.open('x') as stream: stream.write('synthetic single writer')
        try: yield {'deadline': self.time+1000}
        finally: lock.unlink()
    def sleep(self, amount): self.time += amount


class ProbeTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup); self.work = Path(temp.name)
        self.authority = self.work/'private/authority.txt'; self.authority.parent.mkdir()
        self.authority.write_text(probe.AUTH)
        self.config = {'authorization_id': probe.AUTH, 'authority_source': {'path': 'private/authority.txt',
            'sha256': probe.sha(self.authority)}, 'project': 'synthetic-project', 'location': 'US', 'tables': [TABLE]}
        atomic_json(self.work/'private/config.json', self.config)
        self.query = {'name': 'synthetic_query', 'query_id': 'synthetic:q', 'scope_hash': 'f'*64}
        atomic_json(self.work/'private/BATCH_QUERY_FREEZE.json', {'queries': [self.query]})
        self.ledger = Ledger(); self.runtime = Runtime(self.ledger); self.calls = []
        parent = self
        class Sdk:
            def __init__(self, config): self.transport_evidence = []
            def schema(self, table):
                parent.calls.append(table)
                self.transport_evidence.append({'status': 200, 'body': b'{}', 'complete': True, 'retry_after': None})
                return {'kind': 'SCHEMA', 'table': table, 'schema': [{'name': 'block_timestamp', 'type': 'TIMESTAMP'}]}
            def close(self): pass
        self.Sdk = Sdk

    def invoke(self, factory=None):
        return probe.probe(self.work, 'synthetic_query', 'private/config.json', 'schema', table=TABLE,
                           runtime=self.runtime, sdk_factory=factory or self.Sdk,
                           clock=lambda: self.runtime.time, sleep=self.runtime.sleep)

    def test_schema_success_cache_reused_without_another_operation(self):
        first = self.invoke(); second = self.invoke()
        self.assertEqual('SUCCESS_VALIDATED', first['status']); self.assertTrue(second['cache_hit'])
        self.assertEqual([TABLE], self.calls); self.assertEqual(1, sum(j['rpc_operations'] for j in self.ledger.jobs.values()))
        (self.work/first['artifact_path']).write_text('{}')
        with self.assertRaises(ValueError): self.invoke()

    def test_three_attempts_and_unknown_raw_risk_survive_restart(self):
        parent = self
        class Timeout(self.Sdk):
            def schema(self, table):
                parent.calls.append(table)
                self.transport_evidence.append({'status': None, 'body': b'', 'complete': False, 'retry_after': None})
                raise TimeoutError('synthetic provider detail must not persist')
        first = self.invoke(Timeout); second = self.invoke(Timeout)
        self.assertEqual('RETRIES_EXHAUSTED', first['status']); self.assertEqual(first['status'], second['status'])
        self.assertEqual(3, len(self.calls)); self.assertEqual(3, sum(j['rpc_operations'] for j in self.ledger.jobs.values()))
        self.assertEqual(3*probe.RESPONSE_BOUND, self.runtime.raw_risk(self.work))
        for p in (self.work/'raw').rglob('*.json'): self.assertNotIn('synthetic provider detail', p.read_text())

    def test_sdk_adc_failure_is_not_dispatched_or_charged(self):
        def missing(config): raise RuntimeError('secret detail')
        result = self.invoke(missing)
        self.assertEqual('SDK_ADC_SETUP_FAILED', result['status'])
        self.assertFalse(result['bigquery_api_dispatched']); self.assertEqual(0, sum(j['rpc_operations'] for j in self.ledger.jobs.values()))

    def test_shared_worker_lock_blocks_before_provider(self):
        (self.work/'private/network_worker.lock').write_text('existing writer')
        with self.assertRaises(FileExistsError): self.invoke()
        self.assertEqual([], self.calls)

    def test_dry_run_estimate_preserves_caps_and_never_claims_allowance(self):
        result = probe.dry_result({'configuration': {'dryRun': True}, 'statistics':
                                  {'totalBytesProcessed': str(probe.JOB_SCAN_CAP+1)}}, self.ledger.snapshot())
        self.assertFalse(result['within_single_job_cap']); self.assertTrue(result['within_project_cap_remaining'])
        self.assertFalse(result['actual_query_executed']); self.assertEqual(0, result['actual_scanned_bytes'])
        self.assertFalse(result['provider_allowance_confirmed_in_ledger'])
        with self.assertRaises(ValueError): probe.dry_result({'configuration': {'dryRun': False}}, self.ledger.snapshot())

    def test_dry_sql_requires_actual_schema_and_current_scope_dependencies(self):
        first = self.invoke(); sql = self.work/'private/query.sql'
        sql.write_text('SELECT block_timestamp FROM `'+TABLE+'` WHERE block_timestamp >= TIMESTAMP(\'2022-01-01\')')
        dep = self.work/'private/context_gap.json'; dep.write_text('{"synthetic":true}')
        spec = {'sql_path': 'private/query.sql', 'sql_sha256': probe.sha(sql), 'query_id': self.query['query_id'],
            'scope_hash': self.query['scope_hash'], 'schema_evidence': [{'path': first['artifact_path'], 'sha256': first['artifact_sha256']}],
            'scope_dependencies': [{'path': 'private/context_gap.json', 'sha256': probe.sha(dep)}]}
        self.assertEqual(sql.read_text(), probe.dry_spec(self.work, spec, self.config, self.query))
        with self.assertRaises(ValueError): probe.dry_spec(self.work, spec|{'schema_evidence': []}, self.config, self.query)
        with self.assertRaises(ValueError): probe.dry_spec(self.work, spec|{'scope_hash': '0'*64}, self.config, self.query)
        dep.write_text('{}')
        with self.assertRaises(ValueError): probe.dry_spec(self.work, spec, self.config, self.query)



if __name__ == '__main__': unittest.main()
