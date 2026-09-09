"""Immutable label cohort recovery with synthetic dispatch; no network/ledger."""
import json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
BASE=Path(__file__).resolve().parents[1];sys.path.insert(0,str(BASE/'src'))
import stage1d_acquisition as acquire
from page_attempts import atomic_json

class Stage1DLabelRecoveryTests(unittest.TestCase):
    def setUp(self):
        temp=BASE/'.test_tmp';temp.mkdir(exist_ok=True);self.tmp=tempfile.TemporaryDirectory(dir=temp);self.work=Path(self.tmp.name)
        self.query={'query_id':'synthetic:source1','name':'source1','scope_id':'synthetic:scope1','scope_hash':'1'*64}
        self.collection={'states':[{'state':{'address':'address-A'}},{'state':{'address':'address-B'}}]}
        atomic_json(self.work/'private/stage1d_inputs/source_rules.json',{'table_schema':{}})
        atomic_json(self.work/'private/BATCH_QUERY_FREEZE.json',{'queries':[self.query]})
        self.calls=[];self.outcome='RAISE_BEFORE_SUBMIT'
        self.patches=[patch('frontier_labels_r2.optimized_sql',side_effect=lambda addrs,schema:'-- Synthetic four-table labels\nSELECT '+','.join(addrs)),
            patch('frontier_labels_r1.parse_result_rows',side_effect=lambda data,addresses:addresses),
            patch('frontier_labels_r1.observations_from_parsed',side_effect=lambda parsed,*args:([],[{'address':a} for a in parsed])),
            patch('labels_policy.resolve_address',side_effect=lambda observations,prior,**kwargs:dict(prior)),
            patch.object(acquire,'execute_sql',side_effect=self.execute),patch.object(acquire,'result_rows',return_value=[])]
        for p in self.patches:p.start()

    def tearDown(self):
        for p in reversed(self.patches):p.stop()
        self.tmp.cleanup()

    def execute(self,work,freeze,query,label):
        self.calls.append({'freeze':Path(freeze),'query':query})
        if self.outcome=='RAISE_BEFORE_SUBMIT':raise RuntimeError('SYNTHETIC_BUDGET_REFUSAL_BEFORE_POST')
        frozen=acquire.read(freeze);folder=self.work/'private/dune_r2_jobs'/frozen['sql_sha256'];folder.mkdir(parents=True,exist_ok=True)
        if not (folder/'job.json').exists():atomic_json(folder/'job.json',{'execution_id':'SYNTHETIC_EXISTING_EXECUTION'})
        return {'status':self.outcome,'job_folder':str(folder)}

    def first_refusal(self):
        with self.assertRaises(RuntimeError):acquire.acquire_labels(self.work,self.query,self.collection)
        paths=list((self.work/'private/stage1d_label_cohorts').glob('*.json'));self.assertEqual(len(paths),1)
        return paths[0],paths[0].read_bytes(),self.calls[0]['freeze']

    def test_not_submitted_cohort_recovers_same_freeze_without_new_opportunity(self):
        cohort,original,freeze=self.first_refusal();frozen_bytes=freeze.read_bytes();self.outcome='COMPLETED_EXPORTED'
        result=acquire.acquire_labels(self.work,self.query,self.collection)
        self.assertTrue(result['cohort_reused']);self.assertEqual(result['new_addresses'],0)
        self.assertEqual(result['opportunities_spent_now'],0);self.assertEqual(result['opportunities_reused'],2);self.assertTrue(result['labels_updated'])
        self.assertEqual(self.calls[-1]['freeze'],freeze);self.assertEqual(cohort.read_bytes(),original);self.assertEqual(freeze.read_bytes(),frozen_bytes)
        self.assertEqual(len(list((self.work/'private/stage1d_label_cohorts').glob('*.json'))),1)

    def test_failed_existing_execution_is_reused_and_not_replaced(self):
        cohort,original,freeze=self.first_refusal();self.outcome='QUERY_STATE_FAILED'
        failed=acquire.acquire_labels(self.work,self.query,self.collection)
        job=Path(failed['job_folder'])/'job.json';before=job.read_bytes()
        again=acquire.acquire_labels(self.work,self.query,self.collection)
        self.assertEqual(again['status'],'QUERY_STATE_FAILED');self.assertTrue(again['cohort_reused'])
        self.assertEqual(job.read_bytes(),before);self.assertEqual(self.calls[-1]['freeze'],freeze);self.assertEqual(cohort.read_bytes(),original)

    def test_unknown_submission_identity_never_dispatches_post(self):
        _,_,freeze=self.first_refusal();folder=self.work/'private/dune_r2_jobs'/acquire.read(freeze)['sql_sha256']
        folder.mkdir(parents=True);atomic_json(folder/'job.json',{'state':'SUBMITTING_OR_UNCERTAIN'})
        count=len(self.calls);result=acquire.acquire_labels(self.work,self.query,self.collection)
        self.assertEqual(result['status'],'UNRESOLVED_EXISTING_SUBMISSION');self.assertEqual(len(self.calls),count)
        self.assertEqual(result['new_addresses'],0);self.assertFalse(result['labels_updated'])

    def test_successful_empty_opportunity_is_cached_not_requeried(self):
        self.outcome='COMPLETED_EXPORTED';first=acquire.acquire_labels(self.work,self.query,self.collection)
        self.assertTrue(first['labels_updated']);count=len(self.calls)
        second=acquire.acquire_labels(self.work,self.query,self.collection)
        self.assertEqual(second['status'],'NO_NEW_LABEL_OPPORTUNITY');self.assertEqual(len(self.calls),count)

    def test_successful_empty_is_cached_despite_unresolved_historical_claim(self):
        self.outcome='COMPLETED_EXPORTED';acquire.acquire_labels(self.work,self.query,self.collection);count=len(self.calls)
        class HistoricalUnknown:
            registry={}
            def __init__(self,work):pass
            def __call__(self,address):return {'kind':'UNKNOWN','status':'UNQUERIED','reason':'HISTORICAL_EVIDENCE_GAP'}
        with patch.object(acquire,'Labels',HistoricalUnknown):result=acquire.acquire_labels(self.work,self.query,self.collection)
        self.assertEqual(result['status'],'NO_NEW_LABEL_OPPORTUNITY');self.assertEqual(len(self.calls),count)

    def test_cross_query_recovery_keeps_original_cohort_query_owner(self):
        _,_,freeze=self.first_refusal();self.outcome='COMPLETED_EXPORTED'
        other=dict(self.query,query_id='synthetic:source2',name='source2',scope_id='synthetic:scope2')
        result=acquire.acquire_labels(self.work,other,self.collection)
        self.assertTrue(result['cohort_reused']);self.assertEqual(self.calls[-1],{'freeze':freeze,'query':'source1'})

    def test_cohort_changed_on_disk_is_rejected(self):
        cohort,_,_=self.first_refusal();record=acquire.read(cohort);record['previous_batch_distinct']=99;atomic_json(cohort,record)
        with self.assertRaises(ValueError):acquire.acquire_labels(self.work,self.query,self.collection)

    def test_crash_before_sql_freeze_preserves_cohort_on_reconstruction(self):
        with patch.object(acquire,'save_sql',side_effect=RuntimeError('SYNTHETIC_CRASH_BEFORE_SQL_FREEZE')):
            with self.assertRaises(RuntimeError):acquire.acquire_labels(self.work,self.query,self.collection)
        cohort=next((self.work/'private/stage1d_label_cohorts').glob('*.json'));before=cohort.read_bytes();self.outcome='COMPLETED_EXPORTED'
        result=acquire.acquire_labels(self.work,self.query,self.collection)
        self.assertTrue(result['labels_updated']);self.assertEqual(result['new_addresses'],0);self.assertEqual(cohort.read_bytes(),before)

    def test_successful_recovery_triggers_run_query_label_replay(self):
        old={'states':[]};final={'status':'COMPLETE','candidate_events':[]}
        query=dict(self.query,window_mode='REFERENCE_FULL',max_acquisition_depth=1)
        with (patch.object(acquire,'replay',side_effect=[(old,[{'pending':True}]),(final,[]),(final,[])]) as replay,
             patch.object(acquire,'acquire_labels',return_value={'status':'COMPLETED_EXPORTED','new_addresses':0,'labels_updated':True}),
             patch.object(acquire,'acquire_pending') as pending):
            result=acquire.run_query(self.work,query,max_batches=1)
        self.assertEqual(replay.call_count,3);pending.assert_not_called();self.assertEqual(result['collection_status'],'COMPLETE')

if __name__=='__main__':unittest.main(verbosity=2)
