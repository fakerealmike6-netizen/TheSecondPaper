import json,sqlite3,tempfile,unittest
from pathlib import Path
from decimal import Decimal
from budget import Ledger
from budget_r2 import RevisionLedger,AUTH

def confirmation():return {'status':'USER_CONFIRMED','execution_cap_credits':'20','authorization_id':AUTH,'payment_method_added':False,'extra_credits_enabled':False,'account_context_ref':'synthetic'}
class BudgetR2Tests(unittest.TestCase):
    def setUp(self):
        base=Path(__file__).resolve().parents[1]/'checks/r2_test_tmp';base.mkdir(parents=True,exist_ok=True)
        self.tmp=tempfile.TemporaryDirectory(dir=base);self.p=Path(self.tmp.name);self.db=RevisionLedger(self.p/'budget.sqlite')
        self.db.confirm('dune_credits','2500','synthetic account allowance')
        with self.db.connection() as db:
            db.execute("INSERT INTO jobs VALUES('legacy','dune','synthetic','2020','UNKNOWN_RESERVED')")
            db.execute("INSERT INTO amounts VALUES('legacy','dune_credits','16.433088236',NULL)")
        self.db.initialize('a'*64,confirmation())
    def tearDown(self):self.tmp.cleanup()
    def cost(self,job,value,terminal=True):self.db.observe_execution(job,value,{'sha256':'b'*64,'request_id':'synthetic'},terminal=terminal)
    def envelope(self):return {'rate_evidence':{'synthetic':True},'result_metadata':{'total_row_count':100}}
    def test_migration_idempotent_preserves_old_risk_and_removes_subbuckets(self):
        self.assertFalse(self.db.initialize('a'*64,confirmation()));self.assertEqual(self.db.snapshot()['dune_credits']['cumulative_risk'],'16.433088236')
        self.db.reserve_dune_job('new','synthetic');s=self.db.snapshot()['dune_credits']
        self.assertEqual(s['cumulative_risk'],'36.433088236');self.assertEqual(s['cap'],'100');self.assertFalse(s['historical_subbucket_limits_apply_to_new_jobs'])
    def test_terminal_known_cost_replaces20_before_export_invoice(self):
        self.db.reserve_dune_job('new','synthetic');self.cost('new','.4')
        self.assertEqual(self.db.snapshot()['dune_credits']['r2_risk'],'0.4')
        self.db.reserve_export('new','3',self.envelope());self.assertEqual(self.db.snapshot()['dune_credits']['r2_risk'],'3.4')
    def test_nonterminal_observation_does_not_release_cap20(self):
        self.db.reserve_dune_job('new','synthetic');self.cost('new','.1',False)
        self.assertEqual(self.db.snapshot()['dune_credits']['r2_risk'],'20')
        with self.assertRaises(RuntimeError):self.db.reserve_dune_job('another','synthetic')
    def test_warning80_does_not_pause_but100_does(self):
        self.db.reserve_dune_job('one','synthetic');self.cost('one','1');self.db.reserve_export('one','64',self.envelope())
        self.assertTrue(self.db.snapshot()['dune_credits']['warning_at_80'])
        with self.assertRaises(RuntimeError):self.db.reserve_dune_job('two','synthetic')
        self.db.reserve_export('one','65',self.envelope())
        self.assertFalse(self.db.snapshot()['dune_credits']['overrun'])
    def test_new_export_above1_allowed_and_unknown_amount_not_actual(self):
        self.db.reserve_dune_job('one','synthetic');self.cost('one','2');self.db.reserve_export('one','6',self.envelope())
        status=self.db.close_job('one',exported=True,evidence={'request_set_closed':True,'source_receipt_sha256':'b'*64,'no_unknown_attempts':True,'full_page_chain_verified':True,'rate_evidence':{'synthetic':True}})
        self.assertEqual(status,'BOUNDED_ACCOUNTING_NOT_FINAL');self.assertIsNone(self.db.rows()[-1]['actual'])
        self.assertEqual(self.db.snapshot()['dune_credits']['known_actual_lower_bound'],'2')
    def test_old_risk_not_released_by_execution_observation(self):
        with self.assertRaises(ValueError):self.cost('legacy','.01')
        self.assertEqual(self.db.snapshot()['dune_credits']['legacy_risk'],'16.433088236')
    def test_nonfinite_and_negative_rejected(self):
        self.db.reserve_dune_job('new','synthetic')
        for value in ('NaN','Infinity','-1',True):
            with self.assertRaises(ValueError):self.cost('new',value)
    def test_execution_cap_violation_records_full_and_halts(self):
        self.db.reserve_dune_job('new','synthetic');self.cost('new','21')
        self.assertEqual(self.db.snapshot()['dune_credits']['r2_risk'],'21')
        with self.assertRaises(RuntimeError):self.db.reserve_dune_job('another','synthetic')
    def test_observed_export_expansion_is_recorded_even_over100(self):
        self.db.reserve_dune_job('new','synthetic');self.cost('new','1');self.db.reserve_export('new','90',self.envelope(),observed=True)
        self.assertEqual(self.db.snapshot()['dune_credits']['cumulative_risk'],'107.433088236');self.assertTrue(self.db.snapshot()['dune_credits']['overrun'])
    def test_zero_meta_authorization(self):
        with self.assertRaises(RuntimeError):self.db.reserve('meta','metasleuth','synthetic',{'meta_requests':1})
    def test_duplicate_job_cannot_create_second_reservation(self):
        self.db.reserve_dune_job('new','synthetic');self.cost('new','1')
        with self.assertRaises(sqlite3.IntegrityError):self.db.reserve_dune_job('new','synthetic')
        self.assertEqual(self.db.snapshot()['dune_credits']['r2_risk'],'1')
    def test_lower_terminal_cost_is_actual_while_peak_difference_stays_reserved(self):
        self.db.reserve_dune_job('new','synthetic');self.cost('new','.466848396',False)
        self.assertEqual(self.db.snapshot()['dune_credits']['known_actual_lower_bound'],'0')
        self.cost('new','.200617648');self.db.reserve_export('new','1',self.envelope())
        self.db.close_job('new',exported=True,evidence={'request_set_closed':True,'source_receipt_sha256':'b'*64,'no_unknown_attempts':True,'full_page_chain_verified':True,'rate_evidence':{'synthetic':True}})
        before=self.db.rows();snap=self.db.snapshot()['dune_credits'];self.assertEqual(self.db.rows(),before)
        self.assertEqual(snap['known_actual_lower_bound'],'0.200617648');self.assertEqual(snap['execution_peak_discrepancy_risk'],'0.266230748');self.assertEqual(snap['export_upper_not_actual'],'1');self.assertEqual(snap['r2_risk'],'1.466848396')
        self.assertEqual(Decimal(snap['known_actual_lower_bound'])+Decimal(snap['export_upper_not_actual'])+Decimal(snap['unknown_or_pending_risk']),Decimal(snap['cumulative_risk']))
if __name__=='__main__':unittest.main()
