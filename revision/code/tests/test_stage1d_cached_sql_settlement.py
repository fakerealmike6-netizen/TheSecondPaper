"""Recover an interrupted closed export without network or a refreshed clock."""
import json
import sqlite3
import tempfile
from contextlib import contextmanager, closing
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from stage1d_runtime import execute_sql


class ClosedCacheSettlementTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.work=Path(self.tmp.name);self.folder=self.work/'private/dune_r2_jobs/synthetic'
        self.folder.mkdir(parents=True);self.freeze=self.work/'freeze.json';self.freeze.write_text('{}')
        self.state={'state':'QUERY_STATE_COMPLETED','request_set_closed':True,'logical_job_id':'synthetic-job'}
        (self.folder/'job.json').write_text(json.dumps(self.state))
        (self.folder/'stage1d_export_plan.json').write_text('{}')
        self.database=self.work/'private/shared_budget_r4.sqlite';self.settled=0
        @contextmanager
        def connection():
            with closing(sqlite3.connect(self.database)) as db:
                with db:yield db
        def settle(folder):
            self.assertTrue((self.work/'private/network_worker.lock').exists())
            self.settled+=1
            with connection() as db:
                db.execute('CREATE TABLE stage1d_export_reconciliations(job TEXT)')
                db.execute('INSERT INTO stage1d_export_reconciliations VALUES(?)',('synthetic-job',))
            return {'network_requests':0,'verified_pages_reconciled':True}
        self.live=SimpleNamespace(db=SimpleNamespace(connection=connection),settle=settle,
            export_progress=lambda folder,state:{'complete':True})
        self.runtime=SimpleNamespace(verify_sql_freeze=lambda *a,**k:{'sql_sha256':'synthetic'})

    def invoke(self):
        with patch('stage1d_runtime.Runtime',return_value=self.runtime),patch('stage1d_export_reconcile.Stage1DPageDune',return_value=self.live):
            return execute_sql(self.work,self.freeze,'synthetic-query','synthetic')

    def test_closed_before_reconcile_crash_recovers_once_without_online_session(self):
        with patch('stage1d_export_reconcile.verify_plan',return_value={'verified':True}) as verify:
            first=self.invoke();second=self.invoke()
        self.assertEqual(self.settled,1)
        self.assertEqual(verify.call_count,2)
        self.assertEqual(first['online_clock_increment'],0)
        self.assertEqual(first['new_requests'],0)
        self.assertTrue(first['settlement_recovery']['verified_pages_reconciled'])
        self.assertIsNone(second['settlement_recovery'])
        self.assertFalse((self.work/'private/network_worker.lock').exists())

    def test_changed_export_plan_cannot_be_accepted_as_closed_cache(self):
        with patch('stage1d_export_reconcile.verify_plan',side_effect=ValueError('changed proof')):
            with self.assertRaisesRegex(ValueError,'changed proof'):self.invoke()
        self.assertEqual(self.settled,0)

    def test_active_writer_is_not_cancelled_or_replaced_by_cache_recovery(self):
        lock=self.work/'private/network_worker.lock';lock.write_text('existing normal writer')
        with patch('stage1d_export_reconcile.verify_plan',return_value={}):
            with self.assertRaises(FileExistsError):self.invoke()
        self.assertEqual(lock.read_text(),'existing normal writer')
        self.assertEqual(self.settled,0)

    def test_offline_reconciliation_error_cleans_its_own_lock_only(self):
        def fail(folder):raise ValueError('accounting conflict')
        self.live.settle=fail
        with patch('stage1d_export_reconcile.verify_plan',return_value={}):
            with self.assertRaisesRegex(ValueError,'accounting conflict'):self.invoke()
        self.assertFalse((self.work/'private/network_worker.lock').exists())
