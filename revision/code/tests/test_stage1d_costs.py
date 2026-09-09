"""Synthetic ledger-only tests for evidence-bound terminal execution adjustment."""
import copy,json,sys,unittest
from pathlib import Path
from decimal import Decimal
BASE=Path(__file__).resolve().parents[1];sys.path.insert(0,str(BASE/'src'))
import test_budget_r2
from budget_r1 import consistent_backup
from page_attempts import atomic_json
from context_access_r3 import sha
from stage1d_costs import Stage1DDune,reconcile_existing,AUTH

class Stage1DCostTests(unittest.TestCase):
    def setUp(self):
        self.base=test_budget_r2.BudgetR2Tests();self.base.setUp();self.w=self.base.p
        (self.w/'private').mkdir();consistent_backup(self.base.db.path,self.w/'private/shared_budget_r4.sqlite')
        atomic_json(self.w/'private/dune_user_confirmation.json',test_budget_r2.confirmation())
        self.live=Stage1DDune(self.w,transport=lambda *a: self.fail('No network permitted'))
        self.live.db.reserve_dune_job('old-stage-job','historical');self.live.db.observe_execution('old-stage-job','10',{'sha256':'b'*64},False)
        self.live.db.observe_execution('old-stage-job','0.677478310',{'sha256':'c'*64},True)
        self.sql='-- Synthetic Stage1D test only\nSELECT 1\n'
        import hashlib
        self.sqlhash=hashlib.sha256(self.sql.encode()).hexdigest();self.job='dune_r4:'+self.sqlhash
        self.folder=self.w/'private/dune_r2_jobs'/self.sqlhash;self.folder.mkdir(parents=True);(self.folder/'query.sql').write_text(self.sql,encoding='utf-8',newline='\n')
        self.freeze=self.w/'private/synthetic_scope.json';atomic_json(self.w/'private/BATCH_QUERY_FREEZE.json',{'queries':[{'query_id':'synthetic:one','scope_id':'synthetic:scope','scope_hash':'1'*64}]})
        atomic_json(self.freeze,{'schema_version':'stage1d-sql-freeze-v1','authorization_id':AUTH,'sql_sha256':self.sqlhash,
            'query_ids':['synthetic:one'],'scope_id':'synthetic:scope','scope_hash':'1'*64,'dependencies':[],'kind':'frontier_labels','addresses':['0x'+'1'*40],'export_plan':{'all_pages_required':True}})
        self.live.db.reserve_dune_job(self.job,'new Stage1D test');self.live.db.observe_execution(self.job,'4.811471549',{'sha256':'d'*64},False)
        self.live.db.observe_execution(self.job,'3.764852942',{'sha256':'e'*64},True)
        self.live.db.reserve_export(self.job,'2',{'rate_evidence':{'synthetic':True},'result_metadata':{'total_row_count':2}})
        self.live.db.close_job(self.job,exported=True,evidence={'request_set_closed':True,'source_receipt_sha256':'e'*64,'no_unknown_attempts':True,'full_page_chain_verified':True,'rate_evidence':{'synthetic':True}})
        self.state={'logical_job_id':self.job,'execution_id':'A'*26,'sql_sha256':self.sqlhash,'state':'QUERY_STATE_COMPLETED',
            'execution_cost_credits':'4.811471549','reserved_execution':'4.811471549','scope_freeze_path':self.freeze.relative_to(self.w).as_posix(),
            'scope_freeze_sha256':sha(self.freeze),'retry_number':0,'request_set_closed':True,'settlement_status':'BOUNDED_ACCOUNTING_NOT_FINAL'}
        self.body={'execution_id':'A'*26,'state':'QUERY_STATE_COMPLETED','execution_cost_credits':'3.764852942'}
        self.receipt=self.make_receipt(self.body)
        self.state.update(status_response=self.body,status_receipt=self.receipt);atomic_json(self.folder/'job.json',self.state)

    def tearDown(self):self.base.tearDown()

    def make_receipt(self,body):
        raw=self.w/'raw/dune/synthetic_terminal.json';raw.parent.mkdir(parents=True,exist_ok=True);raw.write_text(json.dumps(body),encoding='utf-8')
        receipt={'request_id':'synthetic_terminal','operation':'status','execution_id':'A'*26,'http_status':200,'error_class':None,
            'raw_path':raw.relative_to(self.w).as_posix(),'raw_bytes':raw.stat().st_size,'sha256':sha(raw),'utc':'2026-09-08T10:00:00+00:00'}
        atomic_json(self.w/'logs/synthetic_terminal.json',receipt);return receipt

    def rows(self):
        with self.live.db.connection() as db:
            return {table:db.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall() for table in ('amounts','r2_components','r2_observations','jobs')}

    def test_reliable_terminal_releases_only_new_peak_excess_and_keeps_old_risk(self):
        before=self.rows();snap=self.live.db.snapshot()['dune_credits']
        result=self.live.observe_execution_charge(self.state,self.body,self.receipt)
        after=self.rows();end=self.live.db.snapshot()['dune_credits']
        self.assertEqual(result['released_execution_risk_credits'],'1.046618607')
        self.assertEqual(Decimal(snap['cumulative_risk'])-Decimal(end['cumulative_risk']),Decimal('1.046618607'))
        self.assertEqual(end['execution_peak_discrepancy_risk'],'9.322521690')
        for table in ('amounts','r2_components','jobs'):
            self.assertEqual([r for r in before[table] if r[0]!=self.job],[r for r in after[table] if r[0]!=self.job])
        component=next(r for r in after['r2_components'] if r[0]==self.job)
        self.assertEqual(component[2:7],('3.764852942','4.811471549','2',None,'BOUNDED_ACCOUNTING_NOT_FINAL'))
        self.assertEqual(after['r2_observations'][:len(before['r2_observations'])],before['r2_observations'])
        self.assertEqual(self.state['execution_cost_credits'],'4.811471549')
        self.assertTrue((self.w/self.state['stage1d_terminal_cost_reconciliation']['path']).is_file())

    def test_explicit_reconcile_is_idempotent_without_new_observations(self):
        first=reconcile_existing(self.w,self.folder);before=self.rows();second=reconcile_existing(self.w,self.folder)
        self.assertEqual(first,second);self.assertEqual(before,self.rows())
        with self.live.db.connection() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM stage1d_journal').fetchone()[0],1)

    def test_running_does_not_release_and_explicit_reconcile_refuses(self):
        self.state['state']='QUERY_STATE_EXECUTING';body=dict(self.body,state='QUERY_STATE_EXECUTING',execution_cost_credits='1')
        self.live.observe_execution_charge(self.state,body,self.receipt)
        component=next(r for r in self.rows()['r2_components'] if r[0]==self.job)
        self.assertEqual(component[2],'4.811471549')
        atomic_json(self.folder/'job.json',self.state)
        with self.assertRaises(ValueError):reconcile_existing(self.w,self.folder)

    def test_missing_raw_binding_or_persisted_receipt_refuses_without_mutation(self):
        for bad in (dict(self.receipt,sha256='0'*64),dict(self.receipt,raw_path='raw/missing.json'),dict(self.receipt,http_status=503)):
            before=self.rows()
            with self.assertRaises((ValueError,FileNotFoundError)):self.live.observe_execution_charge(self.state,self.body,bad)
            self.assertEqual(before,self.rows())

    def test_old_freeze_and_wrong_authority_refuse_without_mutation(self):
        for key,value in [('schema_version','r4-context-freeze-v1'),('authorization_id','OLD_AUTH')]:
            frozen=json.loads(self.freeze.read_text());frozen[key]=value;atomic_json(self.freeze,frozen);self.state['scope_freeze_sha256']=sha(self.freeze)
            before=self.rows()
            with self.assertRaises(ValueError):self.live.observe_execution_charge(self.state,self.body,self.receipt)
            self.assertEqual(before,self.rows())

    def test_body_identity_or_cost_disagrees_with_original_raw(self):
        for body in (dict(self.body,execution_id='B'*26),dict(self.body,execution_cost_credits='0.001')):
            before=self.rows()
            with self.assertRaises(ValueError):self.live.observe_execution_charge(self.state,body,self.receipt)
            self.assertEqual(before,self.rows())

    def test_over_cap_or_nonfinite_terminal_is_not_reconciled(self):
        for value in ('20.1','NaN','Infinity','-1',True):
            with self.subTest(value=value):
                body=dict(self.body,execution_cost_credits=value);receipt=self.make_receipt(body);before=self.rows()
                with self.assertRaises(ValueError):self.live.observe_execution_charge(self.state,body,receipt)
                self.assertEqual(before,self.rows())

    def test_bound_sql_or_dependency_mutation_refuses(self):
        (self.folder/'query.sql').write_text('-- Changed\nSELECT 2',encoding='utf-8');before=self.rows()
        with self.assertRaises(ValueError):self.live.observe_execution_charge(self.state,self.body,self.receipt)
        self.assertEqual(before,self.rows())

    def test_ledger_inherited_origin_cannot_receive_reconciliation(self):
        self.state['logical_job_id']='legacy';before=self.rows()
        with self.assertRaises(ValueError):self.live.observe_execution_charge(self.state,self.body,self.receipt)
        self.assertEqual(before,self.rows())

if __name__=='__main__':unittest.main(verbosity=2)
