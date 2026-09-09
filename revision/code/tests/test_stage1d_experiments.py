"""Synthetic new-source acceptance and failure propagation; no provider I/O."""
import copy, json, shutil, sys, tempfile, unittest
from pathlib import Path
from unittest.mock import patch

BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE/'src'))
import stage1d_experiments as batch
import run_stage1c as inherited
from collector import Scope

def declared():
    return [{'name':'new_source_'+str(i),'query_id':'synthetic:stage1d:'+str(i),'incident_id':'SYNTHETIC',
             'seed_rule':'SYNTHETIC','seed_event_id':'seed','seed_amount_raw':'2'} for i in range(4)]

def scope(query,mode='REFERENCE_FULL',scope_id=None):
    return Scope(query['query_id'],query['name'],1,108,0,108*86400,13,
                 None if mode=='REFERENCE_FULL' else 90*86400,mode,scope_id).freeze_dict()

def document(query,kind='ordinary'):
    doc={'scenario_id':query['name'],'query_id':query['query_id'],'index':9,
         'initial_balances':{'A|ETH':'2','T|ETH':'0'},'target_accounts':['T'],
         'objective_groups':{'T|ETH':['enter']},
         'events':[{'id':'seed','kind':'seed','order':1,'to':'A','asset':'ETH','amount_raw':'2'},
                   {'id':'enter','kind':'transfer','order':2,'from':'A','to':'T','asset':'ETH','amount_raw':'3'}]}
    if kind in ('zero_service','zero_nonservice'):
        doc['events']=doc['events'][:1];doc['initial_balances']={'A|ETH':'0'}
        doc['target_accounts']=['A'] if kind=='zero_service' else []
        doc['objective_groups']={'A|ETH':['seed']} if kind=='zero_service' else {}
    return doc

def register(tree,path,query,kind='ordinary',partial=False,mode='REFERENCE_FULL'):
    doc=document(query,kind)
    if partial:doc.update(assumptions=['Unfinished acquisition is outside this conditional observed graph.'],gaps=[{'code':'UNFINISHED_FRONTIER'}])
    return batch.register_document(tree,path,query['query_id'],doc,scope(query,mode),
             {'observations':[{'address':a,'kind':'SERVICE'} for a in doc['target_accounts']]},
             {'gaps':doc.get('gaps',[]),'source':'synthetic known actual balances'},
             'ACQUISITION_PARTIAL' if partial else 'COMPLETE_FULL_SCOPE' if mode=='REFERENCE_FULL' else 'COMPLETE_REDUCED_SCOPE',
             'PARTIAL_CONTEXT_WITH_EXPLICIT_ASSUMPTIONS' if partial else 'NO_OBSERVED_TARGET' if kind=='zero_nonservice' else 'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')

class Stage1DExperimentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_root=BASE/'.test_tmp';cls.temp_root.mkdir(exist_ok=True)
        cls.shared=tempfile.TemporaryDirectory(prefix='stage1d_shared_',dir=cls.temp_root)
        cls.master=Path(cls.shared.name)/'tree';cls.master.mkdir()
        cls.master_batch=cls.master/'batch';batch.initialize_batch(cls.master,cls.master_batch,declared())
        cls.rows=[]
        for q,kind in zip(declared(),('ordinary','zero_nonservice','zero_service','ordinary')):
            cls.rows.append(register(cls.master,cls.master_batch,q,kind,partial=q['name'].endswith('3')))
        batch.freeze_batch(cls.master,cls.master_batch)
        cls.good=batch.run_batch(cls.master,cls.master_batch,cls.master/'results')

    @classmethod
    def tearDownClass(cls):cls.shared.cleanup()

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='stage1d_case_',dir=self.temp_root)
        self.tree=Path(self.temp.name)/'tree';shutil.copytree(self.master,self.tree)
        self.batch=self.tree/'batch';self.results=self.tree/'results'

    def tearDown(self):self.temp.cleanup()

    def gate(self):return batch.validate_saved_batch(self.tree,self.batch,self.results)

    def sample(self,index=0):
        rows=batch.read(self.results/'RESULTS_INDEX.json')['method_results_index']
        row=next(r for r in rows if r['query_id']==declared()[index]['query_id'])
        return self.results/row['path']

    def new_tree(self):
        tree=Path(self.temp.name)/'new';tree.mkdir();path=tree/'batch';batch.initialize_batch(tree,path,declared());return tree,path

    def test_new_query_domain_and_same_seven_method_contract_pass(self):
        gate=self.gate();self.assertTrue(gate['passed'],gate)
        summary=batch.read(self.results/'RESULTS_INDEX.json')
        self.assertEqual(summary['query_count'],4);self.assertEqual(summary['model_count'],4)
        self.assertIsNone(summary['real_amount_accuracy']);self.assertFalse(summary['all_full_scopes_complete'])
        for entry in summary['method_results_index']:self.assertEqual(set(entry['method_statuses']),set(inherited.METHODS))

    def test_exact_collector_scope_identity_reaches_every_method(self):
        methods=batch.read(self.sample()/'METHOD_RESULTS.json');row=next(r for r in batch.read(self.batch/'EXPERIMENT_INPUTS.json')['models'] if r['query_id']==declared()[0]['query_id'])
        self.assertEqual(row['scope_hash'],Scope(**scope(declared()[0])).scope_hash)
        for value in methods.values():self.assertEqual(value['scope_hash'],row['scope_hash'])

    def test_legal_service_zero_hop_is_seed_target(self):
        methods=batch.read(self.sample(2)/'METHOD_RESULTS.json')
        self.assertEqual(methods['FULL_INTERVAL']['events']['seed']['lower_raw'],'2')
        self.assertEqual(methods['FULL_INTERVAL']['addresses']['A|ETH']['upper_raw'],'2')
        self.assertTrue(self.gate()['passed'])

    def test_reference_zero_hop_nonservice_has_no_fabricated_target(self):
        methods=batch.read(self.sample(1)/'METHOD_RESULTS.json')
        self.assertEqual(methods['FULL_INTERVAL']['addresses'],{})
        self.assertEqual(methods['FULL_INTERVAL']['events'],{})
        self.assertEqual(methods['FULL_INTERVAL']['joint_by_asset']['ETH']['upper_raw'],'0')
        self.assertEqual(methods['HAIRCUT']['joint_by_asset'],{})

    def test_partial_and_notstarted_preserve_four_predeclared_queries(self):
        tree,path=self.new_tree();register(tree,path,declared()[0],partial=True);batch.freeze_batch(tree,path)
        value=batch.run_batch(tree,path,tree/'results')
        self.assertTrue(value['passed']);self.assertEqual(len(value['query_progress']),4)
        self.assertEqual(value['model_count'],1);self.assertFalse(value['all_full_scopes_complete'])
        self.assertEqual(sum(q['unavailable'][0]['acquisition_status']=='NOT_STARTED' for q in value['query_progress'] if q['unavailable']),3)
        self.assertTrue(batch.validate_saved_batch(tree,path,tree/'results')['passed'])

    def unavailable(self,tree,path,status='EVIDENCE_CONFLICT_MODEL_BLOCKED'):
        evidence_path=tree/'evidence/actual_failure.json'
        batch.write(evidence_path,{'schema_version':'synthetic-context-evidence-v1','status':status,
                                   'gaps':[{'code':'SYNTHETIC_MISSING_BALANCE'}],'synthetic_only':True})
        evidence={'schema_version':'synthetic-unavailable-evidence-v1','observed_status':status,
                  'replay_dependencies':[{'path':'evidence/actual_failure.json'}]}
        value=batch.record_unavailable(tree,path,declared()[0]['query_id'],status,'Synthetic bounded context remains unavailable',evidence,scope(declared()[0]))
        return evidence_path,value

    def test_unavailable_replay_dependencies_are_normalized_and_frozen(self):
        tree,path=self.new_tree();evidence,value=self.unavailable(tree,path)
        expected=[{'path':'evidence/actual_failure.json','sha256':batch.file_hash(evidence)}]
        self.assertEqual(value['dependencies'],expected);self.assertEqual(value['evidence']['replay_dependencies'],expected)
        self.assertEqual(value['evidence_sha256'],batch.digest(batch.canonical(value['evidence'])))
        batch.freeze_batch(tree,path);summary=batch.run_batch(tree,path,tree/'results')
        self.assertEqual(summary['query_count'],4);self.assertEqual(summary['model_count'],0)
        self.assertTrue(batch.validate_saved_batch(tree,path,tree/'results')['passed'])
        self.assertFalse(summary['all_full_scopes_complete'])

    def test_unavailable_missing_or_changed_dependency_rejects_before_freeze(self):
        tree,path=self.new_tree();evidence,_=self.unavailable(tree,path)
        batch.write(evidence,{'status':'PASS'})
        with self.assertRaises(ValueError):batch.freeze_batch(tree,path)
        self.assertFalse((path/'EXECUTION_FREEZE.json').exists())
        evidence.unlink()
        with self.assertRaises((ValueError,FileNotFoundError)):batch.freeze_batch(tree,path)

    def test_unavailable_dependency_mutation_and_deletion_fail_run_saved_report_and_cli(self):
        tree,path=self.new_tree();evidence,_=self.unavailable(tree,path);batch.freeze_batch(tree,path)
        batch.run_batch(tree,path,tree/'results');original=evidence.read_bytes()
        for changed in (b'{"passed":true,"status":"PASS"}',None):
            if changed is None:evidence.unlink()
            else:evidence.write_bytes(changed)
            with self.assertRaises((ValueError,FileNotFoundError)):batch.verify_freeze(tree,path)
            with self.assertRaises((ValueError,FileNotFoundError)):batch.run_batch(tree,path,tree/'invalid_replay')
            self.assertFalse((tree/'invalid_replay').exists())
            gate=batch.validate_saved_batch(tree,path,tree/'results');self.assertFalse(gate['passed'])
            self.assertFalse(batch.write_report(tree,path,tree/'results',tree/'rejected.md')['passed'])
            argv=['stage1d_experiments','--tree',str(tree),'--batch','batch','--action','gate','--results',str(tree/'results'),'--output',str(tree/'failed_gate.json')]
            with patch.object(sys,'argv',argv):self.assertEqual(batch.main(),1)
            evidence.parent.mkdir(parents=True,exist_ok=True);evidence.write_bytes(original)

    def test_unavailable_freeform_pass_and_external_status_without_dependencies_refused(self):
        tree,path=self.new_tree()
        for status in ('NOT_STARTED','ERROR','EVIDENCE_CONFLICT_MODEL_BLOCKED'):
            for evidence in ('PASS',{'passed':True},{'schema_version':'unbound-status-v1','status':'PASS'}):
                with self.subTest(status=status,evidence=evidence):
                    with self.assertRaises(ValueError):batch.record_unavailable(tree,path,declared()[0]['query_id'],status,'Unverified',evidence)
        with self.assertRaises(ValueError):batch.record_unavailable(tree,path,declared()[0]['query_id'],'ERROR','Unavailable')

    def test_structural_notstarted_is_recomputed_against_current_registration(self):
        tree,path=self.new_tree();q=declared()[0]
        value=batch.record_unavailable(tree,path,q['query_id'],'NOT_STARTED','No registered observation')
        self.assertEqual(value['evidence_basis'],'CURRENT_BATCH_REGISTRATION_ABSENCE')
        self.assertEqual(value['dependencies'][0]['path'],'batch/QUERY_DOMAIN.json')
        register(tree,path,q)
        with self.assertRaises(ValueError):batch.freeze_batch(tree,path)

    def test_unavailable_explicit_dependencies_compatible_and_disagreement_refused(self):
        tree,path=self.new_tree();a=tree/'a.json';b=tree/'b.json';batch.write(a,{'reason':'synthetic'});batch.write(b,{'reason':'other'})
        evidence={'schema_version':'synthetic-unavailable-v1','replay_dependencies':[{'path':'a.json'}]}
        with self.assertRaises(ValueError):batch.record_unavailable(tree,path,declared()[0]['query_id'],'ERROR','Mismatch',evidence,dependencies=[{'path':'b.json'}])
        value=batch.record_unavailable(tree,path,declared()[0]['query_id'],'ERROR','Same evidence',evidence,dependencies=[{'path':'a.json'}])
        self.assertEqual(value['dependencies'][0]['sha256'],batch.file_hash(a))

    def test_package_gate_fails_when_bound_unavailable_evidence_is_missing(self):
        from stage1d_validation import validate_package
        tree,path=self.new_tree();shutil.copytree(BASE/'src',tree/'src',ignore=shutil.ignore_patterns('__pycache__'))
        evidence,_=self.unavailable(tree,path);batch.freeze_batch(tree,path);batch.run_batch(tree,path,tree/'results')
        evidence.unlink()
        result=validate_package(tree,'batch','results',Path(self.temp.name)/'unavailable_package_bad','min')
        self.assertFalse(result['passed'])
        self.assertEqual(next(c['exit_code'] for c in result['commands'] if c['name']=='stage1d_saved_gate'),1)
        self.assertEqual(next(c['exit_code'] for c in result['commands'] if c['name']=='stage1d_fresh_seven_method_replay'),1)

    def test_full_and_reduced_are_distinct_scope_rows_without_merging_sources(self):
        tree,path=self.new_tree();q=declared()[0]
        first=register(tree,path,q,partial=True);second=register(tree,path,q,mode='ARRIVAL_90D')
        self.assertNotEqual(first['sample_id'],second['sample_id']);self.assertNotEqual(first['scope_hash'],second['scope_hash'])
        batch.freeze_batch(tree,path);self.assertEqual(len(batch.verify_freeze(tree,path)[1]['models']),2)

    def test_registered_seed_identity_and_amount_are_checked(self):
        for field,value in [('query_id','different'),('amount_raw','100')]:
            with self.subTest(field=field):
                tree=Path(self.temp.name)/('seed_'+field);tree.mkdir();path=tree/'batch';batch.initialize_batch(tree,path,declared())
                doc=document(declared()[0])
                if field=='query_id':doc[field]=value
                else:doc['events'][0][field]=value
                with self.assertRaises(ValueError):batch.register_document(tree,path,declared()[0]['query_id'],doc,scope(declared()[0]),{}, {},'COMPLETE','FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')

    def test_wrong_scope_hash_and_missing_explicit_mode_rejected(self):
        tree,path=self.new_tree();q=declared()[0]
        for bad in (dict(scope(q),scope_hash='0'*64),{k:v for k,v in scope(q).items() if k!='window_mode'}):
            with self.assertRaises(ValueError):batch.register_document(tree,path,q['query_id'],document(q),bad,{}, {},'COMPLETE','FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')

    def test_fact_mutation_and_late_registration_cannot_change_freeze(self):
        row=batch.read(self.batch/'EXPERIMENT_INPUTS.json')['models'][0]
        observed=self.tree/row['observed_path'];doc=batch.read(observed);doc['events'][0]['amount_raw']='900';batch.write(observed,doc)
        self.assertFalse(self.gate()['passed'])
        with self.assertRaises(ValueError):register(self.tree,self.batch,declared()[0])

    def test_forged_pass_does_not_replace_common_output_contract(self):
        folder=self.sample();values=batch.read(folder/'METHOD_RESULTS.json');values['FULL_INTERVAL']['addresses']={};batch.write(folder/'METHOD_RESULTS.json',values)
        batch.write(folder/'OUTPUT_CONTRACT.json',{'passed':True,'status':'PASS'})
        self.assertFalse(self.gate()['passed'])

    def test_native_identity_is_rebuilt_from_raw_returns(self):
        folder=self.sample();values=batch.read(folder/'RAW_METHOD_RETURNS.json')
        values['POISON']['first_return']['query_or_sample_id']='FORGED_OTHER_SOURCE'
        batch.write(folder/'RAW_METHOD_RETURNS.json',values)
        receipt=self.gate();self.assertFalse(receipt['passed'])
        self.assertTrue(any('RECOMPUTED_NATIVE_IDENTITY_FAILED:POISON' in q['errors'] for q in receipt['queries']))

    def test_output_domain_omission_cannot_remove_fourth_source(self):
        value=batch.read(self.results/'RESULTS_INDEX.json');value['query_progress']=value['query_progress'][:3];value['query_count']=3
        batch.write(self.results/'RESULTS_INDEX.json',value);self.assertFalse(self.gate()['passed'])

    def test_flat_report_tampering_is_recomputed(self):
        path=self.results/'PAIRED_RESULTS.csv';text=path.read_text(encoding='utf-8');path.write_text(text.replace('QUERY_ACCEPTED','FORGED'),encoding='utf-8')
        self.assertFalse(self.gate()['passed'])

    def test_false_full_completion_and_truth_claim_rejected(self):
        value=batch.read(self.results/'RESULTS_INDEX.json');value['all_full_scopes_complete']=True;value['real_amount_accuracy']='1'
        batch.write(self.results/'RESULTS_INDEX.json',value);gate=self.gate();self.assertFalse(gate['passed'])
        self.assertIn('FALSE_COMPLETION_CLAIM',{e['code'] for e in gate['errors']})

    def test_warmup_and_five_actual_method_timings(self):
        data=batch.read(self.sample()/'EFFICIENCY.json')
        for profile in data['profiles'].values():
            self.assertEqual(len(profile['repetitions']),6)
            self.assertEqual(sum(r['warmup'] for r in profile['repetitions']),1)

    def test_method_exception_propagates_to_batch_gate_report_and_cli(self):
        tree,path=self.new_tree();register(tree,path,declared()[0]);batch.freeze_batch(tree,path)
        original=inherited.dispatch
        def failing(doc,method):
            if method=='POISON':raise RuntimeError('SYNTHETIC_INJECTED_METHOD_FAILURE')
            return original(doc,method)
        with patch.object(inherited,'dispatch',failing):value=batch.run_batch(tree,path,tree/'results')
        self.assertFalse(value['passed'])
        self.assertFalse(batch.validate_saved_batch(tree,path,tree/'results')['passed'])
        report=tree/'report.md';self.assertFalse(batch.write_report(tree,path,tree/'results',report)['passed'])
        self.assertIn('rejected',report.read_text(encoding='utf-8'))
        argv=['stage1d_experiments','--tree',str(tree),'--batch','batch','--action','gate','--results',str(tree/'results'),'--output',str(tree/'cli_gate.json')]
        with patch.object(sys,'argv',argv):self.assertEqual(batch.main(),1)
        raw=batch.read(tree/'results'/value['method_results_index'][0]['path']/'RAW_METHOD_RETURNS.json')
        self.assertEqual(len(raw['POISON']['failed_attempts']),6)

    def test_no_target_status_cannot_hide_registered_targets(self):
        tree,path=self.new_tree();q=declared()[0]
        with self.assertRaises(ValueError):batch.register_document(tree,path,q['query_id'],document(q),scope(q),{}, {},'COMPLETE','NO_OBSERVED_TARGET')

    def test_final_package_guard_replays_and_rejects_forged_method_pass(self):
        from stage1d_validation import validate_package
        tree,path=self.new_tree()
        shutil.copytree(BASE/'src',tree/'src',ignore=shutil.ignore_patterns('__pycache__'))
        register(tree,path,declared()[0],partial=True);batch.freeze_batch(tree,path)
        value=batch.run_batch(tree,path,tree/'results')
        first=validate_package(tree,'batch','results',Path(self.temp.name)/'validation_good','public')
        self.assertTrue(first['passed'],first)
        self.assertTrue(first['network_disabled']);self.assertTrue(first['credentials_removed_from_child_environment'])
        folder=tree/'results'/value['method_results_index'][0]['path']
        methods=batch.read(folder/'METHOD_RESULTS.json');methods['POISON']['status']='ERROR';batch.write(folder/'METHOD_RESULTS.json',methods)
        second=validate_package(tree,'batch','results',Path(self.temp.name)/'validation_bad','public')
        self.assertFalse(second['passed'])
        self.assertEqual(next(c['exit_code'] for c in second['commands'] if c['name']=='stage1d_saved_gate'),1)

    def reference_rows(self):
        rows=[]
        for row in batch.read(self.batch/'EXPERIMENT_INPUTS.json')['models']:
            # Independently authored synthetic reference positives; an empty
            # reference domain is intentional even when the method finds T.
            addresses=['A|ETH'] if row['query_id']==declared()[2]['query_id'] else []
            events=['seed'] if addresses else []
            rows.append({'query_id':row['query_id'],'scope_id':row['scope_id'],'label_version':row['label_version'],'reference_version':'SYNTHETIC_REFERENCE_V1',
                 'positive_address_assets':addresses,'positive_event_ids':events,
                 'full_reference_positive_address_assets':addresses,'full_reference_positive_event_ids':events})
        return rows

    def test_empty_positive_domain_remains_undefined_and_outside_is_not_negative(self):
        receipt=batch.evaluate_reference(self.tree,self.batch,self.results,self.reference_rows(),self.tree/'reference')
        first=next(r for r in receipt['rows'] if r['query_id']==declared()[0]['query_id'])
        for method in first['methods'].values():
            metric=method['scope_address_positive_recall']
            self.assertIsNone(metric['recall']);self.assertTrue(metric['no_positive_denominator'])
            self.assertIsNone(metric['false_positive_count']);self.assertIsNone(method['real_amount_accuracy'])
        zero=next(r for r in receipt['rows'] if r['query_id']==declared()[2]['query_id'])
        self.assertEqual(zero['methods']['FULL_INTERVAL']['scope_address_positive_recall']['recall'],'1')

    def test_reference_wrong_scope_label_or_missing_query_is_rejected(self):
        refs=self.reference_rows();refs[0]['label_version']='OTHER_LABEL_SNAPSHOT'
        with self.assertRaises(ValueError):batch.evaluate_reference(self.tree,self.batch,self.results,refs,self.tree/'reference')
        with self.assertRaises(ValueError):batch.evaluate_reference(self.tree,self.batch,self.results,self.reference_rows()[:3],self.tree/'reference')

if __name__=='__main__':unittest.main(verbosity=2)
