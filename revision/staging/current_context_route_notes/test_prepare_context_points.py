"""Synthetic-only cache/readonly boundary checks; no production fixture or API."""
import hashlib
import json
from contextlib import closing
from pathlib import Path
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
import prepare_context_points as driver

class Runtime:
    def rpc_identity(self, provider, plan):
        return {'provider': provider, 'chain': 1, **plan}
    def rpc_result_status(self, request, response):
        return 'SUCCESS_VALIDATED' if request['id'] == response['id'] else 'INVALID_RPC_BINDING'

class ReadOnlyCacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        self.addCleanup(self.tmp.cleanup)
        self.work = Path(self.tmp.name)
        self.method = {'method': 'eth_call', 'params': [{'to': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 'data': '0x70a08231'+'0'*24+'1'*40}, '0x2']}
        retry = types.ModuleType('read_retry_r4')
        retry.logical_key = driver.digest
        self.patch = patch.dict(sys.modules, {'read_retry_r4': retry})
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def fixture(self, *, state='SUCCESS', bad_sha=False):
        (self.work/'private').mkdir()
        env = {'request': {'id': 1, **self.method}, 'response': {'id': 1, 'result': '0x'+'0'*63+'8'}}
        raw = driver.encoded(env)
        (self.work/'point.json').write_bytes(raw)
        receipt = {'artifact_path': 'point.json', 'artifact_sha256': '0'*64 if bad_sha else hashlib.sha256(raw).hexdigest()}
        identity = Runtime().rpc_identity(driver.PROVIDER, self.method)
        with closing(sqlite3.connect(self.work/'private/read_retry_r4.sqlite')) as db:
            db.execute('CREATE TABLE read_requests(logical_key TEXT,identity_json TEXT,state TEXT,success_payload TEXT,success_receipt TEXT)')
            db.execute('INSERT INTO read_requests VALUES(?,?,?,?,?)', (driver.digest(identity), json.dumps(identity), state, json.dumps(env['response']['result']), json.dumps(receipt)))
            db.commit()

    def test_missing_database_is_not_created(self):
        out = driver.cached_points(self.work, [self.method], Runtime())
        self.assertEqual(next(iter(out.values()))['status'], 'NO_CURRENT_CACHE_DATABASE')
        self.assertFalse((self.work/'private').exists())

    def test_exact_eth_call_hit_is_readonly_and_no_coverage(self):
        self.fixture()
        before = (self.work/'private/read_retry_r4.sqlite').read_bytes()
        out = driver.cached_points(self.work, [self.method], Runtime())
        row = out[driver.digest(self.method)]
        self.assertEqual(row['status'], 'SUCCESS_ENVELOPE_SHA_AND_RUNTIME_VALIDATED')
        self.assertIs(row['coverage_claimed'], False)
        self.assertEqual(before, (self.work/'private/read_retry_r4.sqlite').read_bytes())

    def test_changed_artifact_hard_rejected(self):
        self.fixture(bad_sha=True)
        with self.assertRaisesRegex(ValueError, 'SHA changed'):
            driver.cached_points(self.work, [self.method], Runtime())

    def test_inflight_is_not_retried_or_reset(self):
        self.fixture(state='IN_FLIGHT')
        with self.assertRaisesRegex(ValueError, 'remains in flight'):
            driver.cached_points(self.work, [self.method], Runtime())

    def test_changed_params_do_not_reuse(self):
        self.fixture()
        other = dict(self.method, params=[self.method['params'][0], '0x3'])
        out = driver.cached_points(self.work, [other], Runtime())
        self.assertEqual(out[driver.digest(other)]['status'], 'NO_CURRENT_EXACT_REQUEST')

if __name__ == '__main__':
    unittest.main()
