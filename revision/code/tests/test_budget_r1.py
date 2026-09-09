import concurrent.futures,json,sqlite3,tempfile,unittest
from pathlib import Path
from decimal import Decimal
from budget import Ledger
from budget_r1 import RevisionLedger,claim_authorization,consistent_backup,AUTH

class BudgetR1Tests(unittest.TestCase):
    def setUp(self):
        temp_root=Path(__file__).resolve().parents[1]/'.test_tmp';temp_root.mkdir(exist_ok=True)
        self.tmp=tempfile.TemporaryDirectory(dir=temp_root);self.p=Path(self.tmp.name)
        old=Ledger(self.p/'old.sqlite');old.confirm('dune_credits','2500','SYNTHETIC INCLUDED');old.confirm('rpc_operations','500','SYNTHETIC PUBLIC')
        for i in range(5):old.reserve_dune_job('old'+str(i),'synthetic','1','1');old.settle('old'+str(i),{'dune_credits':None})
        old.reserve('rpc','public','synthetic',{'rpc_operations':4});old.settle('rpc',{'rpc_operations':4})
        self.receipt=consistent_backup(self.p/'old.sqlite',self.p/'snapshot.sqlite')
        self.db=RevisionLedger(self.p/'new.sqlite');self.jobs={'old'+str(i):{'execution_cost_credits':'0.1'} for i in range(5)}
        self.db.initialize(self.p/'snapshot.sqlite',self.jobs,{'kind':'SYNTHETIC_TEST'})
    def tearDown(self):self.tmp.cleanup()
    def evidence(self):return {'request_set_closed':True,'source_receipt_sha256':'a'*64,'account_context_ref':'synthetic','applicable_rate_evidence':'synthetic known rate','metered_units_evidence':'synthetic all saved requests','all_attempts_included':True,'rounding_and_minimum_evidence':'synthetic no minimum','unknown_attempt_risk_included':True}
    def test_migration_idempotent_legacy_and_other_units(self):
        self.assertFalse(self.db.initialize(self.p/'snapshot.sqlite',self.jobs,{}))
        s=self.db.snapshot();self.assertEqual(s['rpc_operations']['actual'],'4');self.assertEqual(s['dune_credits']['buckets']['LEGACY_STAGE1B']['risk'],'10')
        self.assertEqual(s['dune_credits']['remaining_for_new_jobs'],'10');self.assertEqual(len(self.db.detailed_jobs()),5)
        self.assertEqual(self.receipt['source_sha256_before'],self.receipt['source_sha256_after'])
    def test_cross_run_authorization_claim_cannot_duplicate(self):
        claim_authorization(self.p/'claims.sqlite',self.p/'new.sqlite');claim_authorization(self.p/'claims.sqlite',self.p/'new.sqlite')
        with self.assertRaises(RuntimeError):claim_authorization(self.p/'claims.sqlite',self.p/'another.sqlite')
    def test_concurrent_new_bucket_and_restart(self):
        def reserve(i):
            try:RevisionLedger(self.p/'new.sqlite').reserve_dune_job('new'+str(i),'synthetic','1','1');return True
            except RuntimeError:return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:r=list(pool.map(reserve,range(12)))
        self.assertEqual(sum(r),5);self.assertEqual(RevisionLedger(self.p/'new.sqlite').snapshot()['dune_credits']['reserved'],'20')
    def test_unknown_and_estimates_do_not_release(self):
        self.db.reserve_dune_job('n','synthetic','1','1');self.db.observe_execution('n','.2',{'fake':True})
        with self.assertRaises(ValueError):self.db.reconcile('n',total_upper='.4',evidence={'request_set_closed':True,'source_receipt_sha256':'a'*64})
        self.assertEqual(self.db.snapshot()['dune_credits']['buckets']['R1_NEW']['risk'],'2')
    def test_bound_not_actual_then_asynchronous_actual(self):
        self.db.reserve_dune_job('n','synthetic','1','1');self.db.observe_execution('n','.2',{'fake':True})
        self.db.reconcile('n',total_upper='.6',evidence=self.evidence());j=next(x for x in self.db.detailed_jobs() if x['job']=='n')
        self.assertEqual(j['accounting_status'],'BOUNDED_ACCOUNTING_NOT_FINAL');self.assertIsNone(j['export_actual']);self.assertEqual(self.db.snapshot()['dune_credits']['actual'],'0')
        self.db.reconcile('n',total_actual='.4',export_actual='.2',evidence=self.evidence())
        s=self.db.snapshot()['dune_credits'];self.assertEqual(s['actual'],'0.4');self.assertEqual(s['reserved'],'10');self.assertEqual(s['buckets']['R1_NEW']['risk'],'0.4')
        self.db.reconcile('n',total_actual='.4',export_actual='.2',evidence=self.evidence())
    def test_legacy_release_never_increases_new_cap(self):
        self.db.reconcile('old0',total_upper='.2',evidence=self.evidence())
        self.assertEqual(self.db.snapshot()['dune_credits']['remaining_for_new_jobs'],'10')
    def test_full_overrun_recorded_and_halted(self):
        self.db.reserve_dune_job('n','synthetic','1','1');self.db.observe_execution('n','.5',{'fake':True})
        self.db.reconcile('n',total_actual='2.123456789123456789',evidence=self.evidence())
        self.assertEqual(self.db.snapshot()['dune_credits']['actual'],'2.123456789123456789')
        with self.assertRaises(RuntimeError):self.db.reserve_dune_job('next','synthetic','1','1')
    def test_execution_cap_violation_and_nonfinite(self):
        self.db.reserve_dune_job('n','synthetic','1','1')
        for v in ['NaN','Infinity','-1']:
            with self.assertRaises(ValueError):self.db.observe_execution('n',v,{'fake':True})
        self.db.observe_execution('n','1.000000001',{'fake':True})
        self.assertTrue(self.db.snapshot()['dune_credits']['overrun'])
    def test_usage_metadata_dispatched_count_persists(self):
        for i in range(6):self.db.count_action('u'+str(i),'usage',6)
        with self.assertRaises(RuntimeError):RevisionLedger(self.p/'new.sqlite').count_action('u6','usage',6)
        with self.assertRaises(RuntimeError):self.db.count_action('u0','usage',6)
    def test_no_meta_grant_or_float_bytes(self):
        with self.assertRaises(RuntimeError):self.db.reserve('meta','metasleuth','forbidden',{'meta_requests':1})
        with self.assertRaises(ValueError):self.db.reserve('bq','bigquery','synthetic',{'bigquery_bytes':1.0})
    def test_lower_provider_allowance_still_applies(self):
        self.db.confirm('dune_credits','11','SYNTHETIC decreased allowance; no reset of jobs')
        with self.assertRaises(RuntimeError):self.db.reserve_dune_job('n','synthetic','1','1')
        self.assertEqual(self.db.snapshot()['dune_credits']['reserved'],'10')
    def test_direct_parent_settle_cannot_release_dune(self):
        self.db.reserve_dune_job('n','synthetic','1','1');self.db.observe_execution('n','.2',{'fake':True})
        with self.assertRaises(ValueError):self.db.settle('n',{'dune_credits':'0'})
        self.db.settle('n',{'dune_credits':None})
        self.assertEqual(self.db.snapshot()['dune_credits']['buckets']['R1_NEW']['risk'],'2')
    def test_later_execution_invalidates_nonfinal_bound(self):
        self.db.reserve_dune_job('n','synthetic','1','1');self.db.observe_execution('n','.2',{'fake':True})
        self.db.reconcile('n',total_upper='.4',evidence=self.evidence())
        self.db.observe_execution('n','.6',{'fake':'later contradictory actual execution'})
        s=self.db.snapshot()['dune_credits'];self.assertTrue(s['overrun']);self.assertEqual(s['buckets']['R1_NEW']['risk'],'0.8')
        self.assertEqual(s['buckets']['R1_NEW']['known_execution'],'0.6')
    def test_upper_cannot_undercount_explicit_export_actual(self):
        self.db.reserve_dune_job('n','synthetic','1','1');self.db.observe_execution('n','.2',{'fake':True})
        with self.assertRaises(ValueError):self.db.reconcile('n',total_upper='.5',export_actual='5',evidence=self.evidence())
        self.assertEqual(self.db.snapshot()['dune_credits']['buckets']['R1_NEW']['risk'],'2')

if __name__=='__main__':unittest.main()
