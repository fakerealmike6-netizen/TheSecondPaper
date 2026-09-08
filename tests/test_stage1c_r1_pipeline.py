"""Saved-result, report, CLI and immutable-revision integration boundaries."""
import copy,io,json,sys,tempfile,unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch
BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE/'src'))
import run_stage1c as runner
import stage1c_result_gate as gate
import stage1c_reports as reports
import stage1c_revision as revision
from test_stage1c_runner import observed,hidden

class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document=observed()
        cls.returns={m:runner.normalized(runner.dispatch(copy.deepcopy(cls.document),m)) for m in runner.METHODS}

    def setUp(self):
        scratch=BASE/'test_scratch';scratch.mkdir(exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(prefix='r1_pipeline_',dir=scratch)
        self.addCleanup(self.temp.cleanup)
        self.tree=Path(self.temp.name);self.out=self.tree/'batch'
        runner.write(self.tree/'observed.json',self.document)
        runner.write(self.tree/'hidden.json',hidden())
        runner.write(self.tree/'EXPERIMENT_FREEZE.json',{'unit':'integration'})
        self.frozen={'method_versions':{m:'unit-v1' for m in runner.METHODS}}
        self.row={'sample_id':'runner-unit','query_id':'runner-unit','kind':'controlled','family':'normal_mixing',
            'incident_id':'SYNTHETIC','source_layer':'SYNTHETIC','zero_hop':False,'observed_path':'observed.json',
            'hidden_path':'hidden.json','label_version':'unit-labels','scope_hash':'unit-scope',
            'input_fact_hash':runner.file_hash(self.tree/'observed.json')}
        self.patch=patch.object(runner,'verify_freeze',return_value=(self.frozen,[self.row]))
        self.patch.start();self.addCleanup(self.patch.stop)

    def run_case(self,change=None):
        # Real runner repeats returns and evaluates with the real contract/Oracle;
        # only algorithm recomputation is cached to keep these boundary tests local.
        def dispatch(doc,method):
            result=copy.deepcopy(self.returns[method])
            if change:change(method,result)
            return result
        with patch.object(runner,'dispatch',side_effect=dispatch),redirect_stdout(io.StringIO()):
            code=runner.run(self.tree,self.out,'public','all')
        return code

    def test_saved_success_is_recomputed_and_then_tampering_fails(self):
        self.assertTrue(self.run_case()['passed'])
        self.assertTrue(gate.validate_saved_batch(self.tree,self.out)['passed'])
        path=self.out/'samples/runner-unit/METHOD_RESULTS.json'
        data=runner.read(path);data['HAIRCUT']['joint_by_asset']['ETH']['point_raw']='999'
        runner.write(path,data)
        receipt=gate.validate_saved_batch(self.tree,self.out)
        self.assertFalse(receipt['passed'])
        self.assertIn('RECOMPUTED_OUTPUT_CONTRACT_FAILED',{e['code'] for e in receipt['queries'][0]['errors']})

    def test_saved_scientific_comparison_order_is_not_a_numeric_difference(self):
        self.run_case();actual=runner.evaluate_query
        def permuted(*args,**kwargs):
            contract,evaluation,ablations,timing=actual(*args,**kwargs)
            evaluation['comparisons'].reverse()
            return contract,evaluation,ablations,timing
        with patch.object(runner,'evaluate_query',side_effect=permuted):
            self.assertTrue(gate.validate_saved_batch(self.tree,self.out)['passed'])
        def changed(*args,**kwargs):
            contract,evaluation,ablations,timing=permuted(*args,**kwargs)
            evaluation['comparisons'][0]['width_raw']='999'
            return contract,evaluation,ablations,timing
        with patch.object(runner,'evaluate_query',side_effect=changed):
            self.assertFalse(gate.validate_saved_batch(self.tree,self.out)['passed'])

    def test_forged_success_flags_do_not_certify_missing_output(self):
        self.run_case(lambda m,v:v.update(addresses={},events={},joint_by_asset={}) if m=='FULL_INTERVAL' else None)
        index=runner.read(self.out/'RESULTS_INDEX.json');index.update(passed=True,status='COMPLETED')
        for row in index['method_results_index']:row.update(passed=True,contract_passed=True,evaluation_passed=True)
        runner.write(self.out/'RESULTS_INDEX.json',index)
        self.assertFalse(gate.validate_saved_batch(self.tree,self.out)['passed'])

    def test_empty_batch_cannot_pass_vacuously(self):
        self.run_case();index=runner.read(self.out/'RESULTS_INDEX.json')
        index.update(method_results_index=[],sample_count=0,controlled_count=0)
        runner.write(self.out/'RESULTS_INDEX.json',index)
        receipt=gate.validate_saved_batch(self.tree,self.out)
        self.assertFalse(receipt['passed'])
        self.assertIn('FROZEN_QUERY_DOMAIN_MISMATCH',{e['code'] for e in receipt['errors']})

    def test_index_status_must_match_verified_method_status(self):
        self.run_case();index=runner.read(self.out/'RESULTS_INDEX.json')
        index['method_results_index'][0]['method_statuses']['HAIRCUT']='ERROR'
        runner.write(self.out/'RESULTS_INDEX.json',index)
        self.assertFalse(gate.validate_saved_batch(self.tree,self.out)['passed'])

    def test_report_failure_has_no_certified_metrics_and_cli_exits_one(self):
        self.run_case(lambda m,v:v.update(allocation_raw=None) if m=='HAIRCUT' else None)
        dest=self.tree/'reports'
        with patch('sys.argv',['reports','--tree',str(self.tree),'--results',str(self.out),'--output',str(dest)]),redirect_stdout(io.StringIO()):
            status=reports.main()
        self.assertEqual(status,1)
        stats=runner.read(dest/'STATISTICS.json')
        self.assertFalse(stats['certified_statistics_generated'])
        self.assertIsNone(stats['controlled']['query_count'])
        self.assertFalse(runner.read(dest/'REPORT_ACCEPTANCE.json')['passed'])
        self.assertEqual((self.out/'PAIRED_RESULTS.csv').read_text().strip(),'status')
        self.assertTrue((self.out/'samples/runner-unit/RAW_METHOD_RETURNS.json').is_file())

    def test_native_identity_failure_propagates_to_method_and_query_status(self):
        summary=self.run_case(lambda m,v:v.update(input_fact_hash='wrong-native-hash') if m=='HAIRCUT' else None)
        self.assertFalse(summary['passed'])
        receipt=runner.read(self.out/'samples/runner-unit/OUTPUT_CONTRACT.json')
        self.assertEqual(receipt['status'],'FAIL')
        self.assertFalse(receipt['method_checks']['HAIRCUT']['passed'])

    def test_suspect_report_cannot_overwrite_a_prior_report(self):
        dest=self.tree/'reports';dest.mkdir();(dest/'prior.txt').write_text('prior')
        with self.assertRaisesRegex(ValueError,'fresh'):
            reports.generate_reports(self.tree,self.out,dest)
        self.assertEqual((dest/'prior.txt').read_text(),'prior')

    def test_explicit_selection_is_stable_and_rejects_unknown_or_duplicates(self):
        rows=[{'sample_id':'a','kind':'controlled'},{'sample_id':'b','kind':'real'}]
        self.assertEqual([r['sample_id'] for r in runner.select_rows(rows,'ids:b,a')],['b','a'])
        for value in ('ids:a,a','ids:unknown','ids:'):
            with self.assertRaises(ValueError):runner.select_rows(rows,value)

    def test_scientific_freeze_is_never_recreated(self):
        before=(self.tree/'EXPERIMENT_FREEZE.json').read_bytes()
        with self.assertRaisesRegex(ValueError,'immutable'):runner.freeze(self.tree,'new')
        self.assertEqual((self.tree/'EXPERIMENT_FREEZE.json').read_bytes(),before)

    def test_inherited_test_or_algorithm_change_is_rejected(self):
        (self.tree/'src').mkdir();(self.tree/'tests').mkdir()
        path=self.tree/'src/stage1c_baselines.py';path.write_text('accepted')
        parent={'source_inventory':[{'path':'src/stage1c_baselines.py','sha256':runner.file_hash(path)}]}
        with patch.object(revision,'PARENT_FREEZE_SHA256',runner.file_hash(self.tree/'EXPERIMENT_FREEZE.json')):
            self.assertEqual(revision._parent_unchanged(self.tree,parent),[])
            path.write_text('changed algorithm')
            with self.assertRaisesRegex(ValueError,'Unapproved'):revision._parent_unchanged(self.tree,parent)

    def test_effective_revision_policy_keeps_contract_and_excludes_account_budget(self):
        (self.tree/'configs').mkdir()
        private={k:{} for k in revision.EXECUTION_POLICY_KEYS}
        private.update(authorization_id='STAGE1C_R1_OUTPUT_CONTRACT_AND_INTEGRATION_V1',budget={'private_billing_example':'do not publish'})
        runner.write(self.tree/'configs/STAGE1C_R1_POLICY.json',private)
        parent={'warmups':1,'timed_repetitions':5,'method_versions':self.frozen['method_versions'],'source_inventory':[]}
        for name,key in [('EXPERIMENT_INPUTS.json','inputs_manifest_sha256'),('configs/STAGE1C_EFFECTIVE_POLICY.json','effective_policy_sha256'),('METHOD_SPEC_EFFECTIVE.md','method_spec_sha256'),('controlled_v1/MANIFEST.json','controlled_manifest_sha256')]:
            runner.write(self.tree/name,{'immutable':name});parent[key]=runner.file_hash(self.tree/name)
        runner.write(self.tree/'EXPERIMENT_FREEZE.json',parent)
        with patch.object(revision,'PARENT_FREEZE_SHA256',runner.file_hash(self.tree/'EXPERIMENT_FREEZE.json')):
            frozen=revision.freeze_revision(self.tree,'stage1c-r1-unit')
            self.assertNotIn('budget',runner.read(self.tree/'configs/STAGE1C_R1_EXECUTION_POLICY.json'))
            self.assertEqual(runner.read(self.tree/'configs/STAGE1C_R1_POLICY.json'),private)
            self.assertTrue(revision.verify_revision(self.tree,parent))
            private['budget']['private_billing_example']='changed'
            runner.write(self.tree/'configs/STAGE1C_R1_POLICY.json',private)
            with self.assertRaisesRegex(ValueError,'Private authorization'):revision.verify_revision(self.tree,parent)
            (self.tree/'configs/STAGE1C_R1_POLICY.json').unlink()
            self.assertTrue(revision.verify_revision(self.tree,parent))
            with self.assertRaisesRegex(ValueError,'safe'):revision.freeze_revision(self.tree,'stage1c-r1-../../escape')

if __name__=='__main__':unittest.main()
