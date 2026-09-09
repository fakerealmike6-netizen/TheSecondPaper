"""Synthetic explicit amendment tests; no live ledger or provider access."""
import hashlib, json, sys, unittest
from pathlib import Path
from decimal import Decimal
BASE=Path(__file__).resolve().parents[1];sys.path.insert(0,str(BASE/'src'))
import test_budget_r2
from budget_r1 import consistent_backup
from budget_r2 import RevisionLedger
from page_attempts import atomic_json
import stage1d_budget as b

class Stage1DBudgetTests(unittest.TestCase):
    def setUp(self):
        self.base=test_budget_r2.BudgetR2Tests();self.base.setUp();self.w=self.base.p
        (self.w/'private').mkdir();self.path=self.w/'private/shared_budget_r4.sqlite';consistent_backup(self.base.db.path,self.path)
        self.synthetic=b'SYNTHETIC explicit replacement cumulative500 cap20 warn400; no network, no actual authorization.'
        self.source_sha=hashlib.sha256(self.synthetic).hexdigest();self.source_bytes=len(self.synthetic)
        atomic_json(self.w/b.EXPECTED_FILE,{'schema_version':'stage1d-budget-authority-expected-v1','authorization_id':b.AUTH,'source_sha256':self.source_sha,'source_bytes':self.source_bytes})
        source=self.w/'private/authorized_synthetic.txt';source.write_bytes(self.synthetic)
        atomic_json(self.w/'private/BATCH_QUERY_FREEZE.json',{'authorization_id':b.SCOPE_AUTH,'queries':[{'query_id':'synthetic:'+str(i)} for i in range(4)]})
        self.proof={'authorization_id':b.AUTH,'source_path':source.relative_to(self.w).as_posix(),'source_sha256':self.source_sha,'source_bytes':self.source_bytes,
                    'status':'USER_CONFIRMED','cumulative_cap_credits':'500','warning_credits':'400','execution_cap_credits':'20','payment_method_added':False,'extra_credits_enabled':False}

    def tearDown(self):
        self.base.tearDown()

    def amended(self):b.amend(self.w,self.proof);return b.Stage1DLedger(self.path)
    def cost(self,ledger,job,n,terminal=True):ledger.observe_execution(job,n,{'sha256':'b'*64},terminal)
    def ev(self):return {'rate_evidence':{'synthetic':True},'result_metadata':{'total_row_count':100}}
    def oldrows(self):
        old=RevisionLedger(self.path)
        with old.connection() as db:return b._old_tables(db)

    def test_amend_keeps_every_old_table_and_is_idempotent_not_plus500(self):
        before=self.oldrows();old=RevisionLedger(self.path).snapshot();receipt=b.amend(self.w,self.proof)
        self.assertEqual(before,self.oldrows());self.assertEqual(receipt,b.amend(self.w,self.proof));self.assertEqual(old,RevisionLedger(self.path).snapshot())
        live=b.Stage1DLedger(self.path);s=live.snapshot()['dune_credits']
        self.assertEqual(s['cap'],'500');self.assertEqual(s['remaining'],'483.566911764');self.assertEqual(s['cumulative_risk'],'16.433088236')
        with live.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM stage1d_budget_authorizations').fetchone()[0],1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM stage1d_journal WHERE kind='CUMULATIVE_CAP_REPLACED_WITH500'").fetchone()[0],1)

    def test_above100_legal_below500_and_observe_has_no_old100_halt(self):
        live=self.amended();live.reserve_dune_job('first','synthetic');self.cost(live,'first','1')
        live.reserve_export('first','150',self.ev());self.assertGreater(Decimal(live.snapshot()['dune_credits']['cumulative_risk']),100)
        live.reserve_dune_job('second','synthetic');self.cost(live,'second','2')
        self.assertIsNone(live.snapshot()['dune_credits']['halt_reason']);self.assertFalse(live.snapshot()['dune_credits']['overrun'])

    def test_400_warning_only_then_500_reservation_rejected(self):
        live=self.amended();live.reserve_dune_job('first','synthetic');self.cost(live,'first','1')
        live.reserve_export('first','385',self.ev());s=live.snapshot()['dune_credits']
        self.assertTrue(s['warning_at_400']);self.assertFalse(s['warning_requires_pause']);self.assertFalse(s['overrun'])
        live.reserve_dune_job('second','still permitted');self.cost(live,'second','1')
        with self.assertRaises(RuntimeError):live.reserve_export('first','483',self.ev())
        self.assertFalse(live.snapshot()['dune_credits']['overrun'])

    def test_execution20_remains_strict_and_known_overcap_halts(self):
        live=self.amended()
        for n in ('1','5','19','21'):
            with self.assertRaises(ValueError):live.reserve_dune_job('bad'+n,'bad',n)
        live.reserve_dune_job('one','synthetic');self.cost(live,'one','21')
        self.assertTrue(live.snapshot()['dune_credits']['overrun']);self.assertEqual(live.snapshot()['dune_credits']['halt_reason'],'EXECUTION_CAP20_EXCEEDED:21')
        with self.assertRaises(RuntimeError):live.reserve_dune_job('two','bad')

    def test_observed_above500_is_retained_and_halts(self):
        live=self.amended();live.reserve_dune_job('one','synthetic');self.cost(live,'one','1')
        live.reserve_export('one','490',self.ev(),observed=True)
        s=live.snapshot()['dune_credits'];self.assertEqual(s['cumulative_risk'],'507.433088236');self.assertTrue(s['overrun'])
        with self.assertRaises(RuntimeError):live.reserve_dune_job('two','blocked')

    def test_unknown_peak_export_and_provider_allowance_never_reset(self):
        old=RevisionLedger(self.path);old.reserve_dune_job('oldnew','synthetic');self.cost(old,'oldnew','10',False);self.cost(old,'oldnew','0.677478310')
        old.reserve_export('oldnew','3',self.ev());before=old.snapshot();live=self.amended();after=live.snapshot()
        self.assertEqual(after['dune_credits']['execution_peak_discrepancy_risk'],'9.322521690')
        for k in ('reserved','actual','execution_peak_discrepancy_risk','unknown_or_pending_risk','confirmed_allowance'):self.assertEqual(before['dune_credits'][k],after['dune_credits'][k])
        live.confirm('dune_credits','49','synthetic lower actual provider allowance')
        with self.assertRaises(RuntimeError):live.reserve_dune_job('another','provider allowance binds')

    def test_missing_bad_attachment_or_wrong_scope_fails_without_money_changes(self):
        before=self.oldrows()
        for key,value in [('source_sha256','0'*64),('cumulative_cap_credits','1000'),('payment_method_added',True),('source_path','../outside.txt')]:
            proof=dict(self.proof);proof[key]=value
            with self.assertRaises(ValueError):b.amend(self.w,proof)
            self.assertEqual(before,self.oldrows())
        with self.assertRaises(RuntimeError):b.Stage1DLedger(self.path)

    def test_metasleuth_and_other_provider_limits_unchanged(self):
        live=self.amended();before=RevisionLedger(self.path).snapshot();after=live.snapshot()
        for unit in before:
            if unit!='dune_credits':self.assertEqual(before[unit],after[unit])
        with self.assertRaises(RuntimeError):live.reserve('meta','metasleuth','blocked',{'meta_requests':1})
        with self.assertRaises(ValueError):live.reserve('bq','bigquery','over single job',{'bigquery_bytes':1073741825})

    def test_active_writer_and_changed_authorization_refused(self):
        lock=self.w/'private/network_worker.lock';lock.write_text('synthetic')
        with self.assertRaises(RuntimeError):b.amend(self.w,self.proof)
        lock.unlink();live=self.amended()
        (self.w/'private/authorized_synthetic.txt').write_bytes(b'changed')
        with self.assertRaises(ValueError):live.snapshot()

if __name__=='__main__':unittest.main(verbosity=2)
