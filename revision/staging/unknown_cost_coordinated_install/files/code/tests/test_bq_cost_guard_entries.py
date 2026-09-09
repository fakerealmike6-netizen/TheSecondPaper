"""Actual BQ entry controls with real synthetic cost Registry and no provider."""
from contextlib import contextmanager
import hashlib,json
from pathlib import Path
import tempfile,unittest
from unittest.mock import Mock,patch

import stage1d_batch_binding_route as route
import stage1d_bigquery_probe as probe
import stage1d_bigquery_jobs as jobs
import stage1d_cost_request_guard as guard
import stage1d_recovery_policy as recovery
from bq_cost_fixture import Fixture,write
from test_stage1d_bigquery_jobs_recovery import Ledger

ROOT=Path(__file__).resolve().parents[1]
TABLE='synthetic-project.ethereum.transactions'

class BqCostEntries(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory(prefix='bq_cost_',dir=Path(__file__).resolve().parent);self.addCleanup(temp.cleanup)
        self.w=Path(temp.name)/'code';self.w.mkdir();self.f=Fixture(self.w);self.f.save()
        write(self.w,'private/BATCH_QUERY_FREEZE.json',{'queries':[self.f.q]})
        authority=self.w/'authority.txt';authority.write_text(probe.AUTH)
        self.config={'authorization_id':probe.AUTH,'authority_source':{'path':'authority.txt','sha256':probe.sha(authority)},
            'project':'synthetic-project','location':'US','tables':[TABLE]}
        write(self.w,'config.json',self.config)
        self.need=dict(self.f.rectangle(self.f.b),query_name=self.f.q['name'],query_id=self.f.q['query_id'],
            scope_id=self.f.scope.scope_id,scope_hash=self.f.scope.scope_hash,direction='OUTGOING',
            fact_type='POSITIVE_NATIVE_CANDIDATE_INDEX',gap_reason='SYNTHETIC_STOPPED_DOMAIN')
        sql=self.w/'query.sql';sql.write_text('SELECT block_number FROM `'+TABLE+'`')
        schema=write(self.w,'schema.json',{'status':'SUCCESS_VALIDATED','result':{'kind':'SCHEMA','table':TABLE}})
        scope=write(self.w,'scope.json',{'synthetic_fixed_scope':True})
        self.spec={'schema_version':'stage1d-bigquery-dryrun-spec-v1','batch_binding_sql_version':route.VERSION,
            'query_id':self.f.q['query_id'],'scope_hash':self.f.q['scope_hash'],'sql_path':'query.sql','sql_sha256':jobs.sha(sql),
            'schema_evidence':[schema],'scope_dependencies':[scope],'need_rectangles':[self.need]}
        write(self.w,'spec.json',self.spec)
        write(self.w,'dry.json',{'status':'SUCCESS_VALIDATED','result':{'kind':'DRY_RUN','estimated_processed_bytes':100},
            'identity':{'sql_sha256':self.spec['sql_sha256']}})
        write(self.w,'plan.json',{'dry_spec_path':'spec.json','dry_spec_sha256':jobs.sha(self.w/'spec.json'),
            'dry_receipt_path':'dry.json','dry_receipt_sha256':jobs.sha(self.w/'dry.json')})
        self.ledger=Ledger(self.w);self.calls=[]
        self.runtime=type('Runtime',(),{'ledger_factory':lambda _,path:self.ledger})

    @contextmanager
    def environment(self):
        ledger=self.ledger
        class Runtime:
            def ledger_factory(self,path):return ledger
        with patch.object(jobs,'Runtime',Runtime),patch.object(jobs,'call',side_effect=self.call), \
             patch.object(recovery,'bq_reservation',return_value={'maximum_bytes_billed':110}):yield

    def call(self,work,query,config,operation,selectors,**kwargs):
        self.calls.append((operation,dict(selectors),kwargs))
        if operation=='jobs.insert':return {'status':'SUCCESS_VALIDATED','result':{}}
        job=selectors['job_id']
        if operation=='jobs.get':return {'status':'SUCCESS_VALIDATED','result':{
            'jobReference':{'projectId':self.config['project'],'jobId':job,'location':'US'},
            'status':{'state':'DONE'},'statistics':{'query':{'totalBytesBilled':'100'}}}}
        return {'status':'SUCCESS_VALIDATED','result':{'jobComplete':True,'jobReference':{'jobId':job},
            'totalRows':'0','schema':{'fields':[]},'rows':[]}}

    def run_plan(self):return jobs.execute_plan(self.w,self.f.q['name'],self.config,'plan.json')

    def prepared_state(self):
        with self.environment(),self.assertRaisesRegex(ValueError,'arrival union'):self.run_plan()
        path=next((self.w/'private/stage1d_bigquery_jobs').glob('*/job.json'))
        state=json.loads(path.read_bytes());self.assertEqual(state['state'],'PREPARED');return path,state

    def install_existing_state(self,status):
        path,state=self.prepared_state();state['state']=status
        body={'jobReference':{'projectId':self.config['project'],'jobId':state['job_id'],'location':'US'},
            'configuration':{'query':{'query':(self.w/'query.sql').read_text(),'useLegacySql':False,'useQueryCache':True,
                'maximumBytesBilled':str(state['maximum_bytes_billed'])}}}
        state['submission_body_sha256']=hashlib.sha256(jobs.canonical(body)).hexdigest()
        write(self.w,path.relative_to(self.w).as_posix(),state)
        self.ledger.reserve(state['scan_ledger_job'],'bigquery','SYNTHETIC_ALREADY_RESERVED',{'bigquery_bytes':110})
        return path,state

    def test_prepare_rejects_stopped_before_schema_sql_or_plans(self):
        doc={'query_id':self.f.q['query_id'],'scope_hash':self.f.q['scope_hash'],
             'freeze_sha256':jobs.sha(self.w/'private/BATCH_QUERY_FREEZE.json'),'need_rectangles':[self.need]}
        write(self.w,'NEEDS.json',doc)
        with patch.object(route.h,'schemas') as schemas,patch.object(route,'build_binding_sql') as sql, \
             self.assertRaisesRegex(ValueError,'arrival union'):
            route.prepare(self.w,'NEEDS.json','new_batch',{},[])
        schemas.assert_not_called();sql.assert_not_called();self.assertFalse((self.w/'new_batch').exists())

    def test_probe_rejects_stopped_before_sdk_auth_retry_or_ledger(self):
        runtime=Mock();sdk=Mock(side_effect=AssertionError('SDK/auth must never start'))
        with self.assertRaisesRegex(ValueError,'arrival union'):
            probe.probe(self.w,self.f.q['name'],'config.json','dry-run',spec_path='spec.json',runtime=runtime,sdk_factory=sdk)
        sdk.assert_not_called();runtime.retry_store.assert_not_called();runtime.ledger_factory.assert_not_called()
        self.assertFalse((self.w/'private/read_retry_r4.sqlite').exists())

    def test_prepared_rejects_before_scan_reserve_or_insert(self):
        with patch.object(self.ledger,'reserve',wraps=self.ledger.reserve) as reserve:
            self.prepared_state()
        reserve.assert_not_called();self.assertEqual(self.calls,[])

    def test_submitted_and_uncertain_jobs_finish_same_job_despite_current_stop(self):
        for status in ('SUBMITTED','SUBMISSION_UNCERTAIN'):
            with self.subTest(status=status):
                # Reuse only this synthetic job's original pages and identity.
                if self.calls:self.calls.clear()
                path=next((self.w/'private/stage1d_bigquery_jobs').glob('*/job.json'),None)
                if path:
                    state=json.loads(path.read_bytes());state.update(state=status,pages=[],poll_sequence=0)
                    for key in ('actual_billed_bytes','total_rows'):state.pop(key,None)
                    write(self.w,path.relative_to(self.w).as_posix(),state)
                else:_,state=self.install_existing_state(status)
                with self.environment(),patch.object(guard,'validate_bq_candidate_request',side_effect=AssertionError('No new-request guard during existing readback')):
                    final=self.run_plan();again=self.run_plan()
                self.assertEqual(final['state'],'COMPLETE_EXPORTED');self.assertEqual(final,again)
                self.assertEqual([x[0] for x in self.calls],['jobs.get','jobs.getQueryResults'])
                self.assertTrue(all(x[1]['job_id']==state['job_id'] for x in self.calls))

    def test_undispatched_intent_returns_to_prepared_then_rechecks_stop(self):
        path,state=self.install_existing_state('SUBMISSION_INTENT')
        with self.environment(),patch.object(jobs,'undispatched_submission',return_value={'basis':'SYNTHETIC_NO_DISPATCH_PROOF'}), \
             patch.object(self.ledger,'reserve',wraps=self.ledger.reserve) as reserve,self.assertRaisesRegex(ValueError,'arrival union'):
            self.run_plan()
        reserve.assert_not_called();self.assertEqual(self.calls,[])
        self.assertEqual(json.loads(path.read_bytes())['state'],'PREPARED')

    def test_uncertain_dispatched_intent_only_gets_original_job(self):
        _,state=self.install_existing_state('SUBMISSION_INTENT')
        with self.environment(),patch.object(jobs,'undispatched_submission',return_value=None), \
             patch.object(guard,'validate_bq_candidate_request',side_effect=AssertionError('Original dispatched intent must finish')):
            final=self.run_plan()
        self.assertEqual(final['state'],'COMPLETE_EXPORTED');self.assertEqual([x[0] for x in self.calls],['jobs.get','jobs.getQueryResults'])
        self.assertTrue(all(x[1]['job_id']==state['job_id'] for x in self.calls))

    def test_classic_context_without_discovery_markers_is_not_cut(self):
        for key in ('batch_binding_sql_version','need_rectangles'):self.spec.pop(key)
        write(self.w,'spec.json',self.spec)
        plan=json.loads((self.w/'plan.json').read_bytes());plan['dry_spec_sha256']=jobs.sha(self.w/'spec.json');write(self.w,'plan.json',plan)
        with self.environment():final=self.run_plan()
        self.assertEqual(final['state'],'COMPLETE_EXPORTED');self.assertEqual([x[0] for x in self.calls],['jobs.insert','jobs.get','jobs.getQueryResults'])

if __name__=='__main__':unittest.main()
