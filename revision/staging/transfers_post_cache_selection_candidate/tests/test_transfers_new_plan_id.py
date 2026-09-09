"""Synthetic CLI path tests; the root driver is imported from staging/scripts."""
from pathlib import Path
from types import SimpleNamespace
import contextlib,io,json,tempfile,unittest
from unittest.mock import patch
import execute_current_transfers as driver
import stage1d_transfers_acquisition as a


class DriverPlanTests(unittest.TestCase):
    def review(self,*args):
        with contextlib.redirect_stdout(io.StringIO()) as buf,patch.object(driver,'run_root',side_effect=AssertionError('No execution allowed')):
            driver.main(list(args))
        return json.loads(buf.getvalue())

    def test_default_and_resume_paths_unchanged_new_path_explicit(self):
        default=driver.plan_output('txphish_src001','a'*64,20)
        self.assertEqual(default,'private/stage1d_transfers_acquisition/batches/current_txphish_src001_'+'a'*24+'_n20')
        self.assertEqual(driver.plan_output('txphish_src001','a'*64,20,'batch_02'),default+'_new_batch_02')
        self.assertIsNone(self.review()['new_plan_id'])
        self.assertEqual(self.review('--plan','private/old/plan.json')['plan'],'private/old/plan.json')
        self.assertEqual(self.review('--new-plan-id','batch_02')['new_plan_id'],'batch_02')

    def test_cli_modes_mutually_exclusive_and_safe_short_ids(self):
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):
            driver.main(['--plan','old.json','--new-plan-id','n2'])
        for val in ('','../outside','a/b','a\\b','C:\\bad','a.b','a b','_first','x'*49,'闈濧SCII'):
            with self.subTest(val=val),self.assertRaises(ValueError):driver.safe_plan_id(val)
        for val in ('a','b_02','A-4','9'*48):self.assertEqual(driver.safe_plan_id(val),val)

    def test_injected_conflicting_mode_rejected_before_any_io(self):
        with patch.object(driver,'load_api',side_effect=AssertionError('Must not load production')):
            with self.assertRaisesRegex(ValueError,'mutually exclusive'):
                driver.run_root(SimpleNamespace(plan='old.json',new_plan_id='new'))

    def test_new_id_creates_once_default_and_explicit_plan_resume_without_prepare(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]) as td:
            revision=Path(td);code=revision/'code';code.mkdir();(code/'private').mkdir()
            boundary=revision/'BOUNDARY_AND_REQUIREMENTS.json';a.atomic_json(boundary,{'synthetic':True})
            base=dict(query='txphish_src002',boundary=boundary.name,maximum_needs=20,maximum_pages=2,
                      maximum_binding_selectors=200,plan=None,new_plan_id='batch02')
            prepared=[];acquired=[]
            def prepare(work,boundary_path,query,output,maximum):
                prepared.append(output);value={'query_name':query,'original_boundary_sha256':a.sha(boundary_path)}
                a.atomic_json(code/output/'plan.json',value);return value
            def acquire(work,path,*args,**kw):acquired.append(path);return {'results':[]}
            runtime=SimpleNamespace(require_gate=lambda code:None,clock_used=lambda code:None)
            def execute(args):
                with contextlib.redirect_stdout(io.StringIO()):
                    return driver.run_root(args,api=a,runtime_factory=lambda:runtime,
                        access_factory=lambda *a,**kw:object(),importer_factory=lambda *a,**kw:object())
            with patch.object(driver,'REVISION',revision),patch.object(driver,'sealed_index',return_value={'synthetic':True}),\
                    patch.object(a,'prepare',side_effect=prepare),patch.object(a,'acquire',side_effect=acquire),patch.dict(driver.os.environ,{}):
                result=execute(SimpleNamespace(**base));first=result['plan']['path']
                self.assertTrue(first.endswith('_new_batch02/plan.json'));self.assertEqual(len(prepared),1)
                with self.assertRaisesRegex(ValueError,'already exists'):execute(SimpleNamespace(**base))
                self.assertEqual(len(prepared),1);self.assertEqual(len(acquired),1)
                resume=base|{'new_plan_id':None,'plan':first};execute(SimpleNamespace(**resume))
                self.assertEqual(len(prepared),1)
                default=driver.plan_output(base['query'],a.sha(boundary),20)
                a.atomic_json(code/default/'plan.json',{'query_name':base['query'],'original_boundary_sha256':a.sha(boundary)})
                execute(SimpleNamespace(**(base|{'new_plan_id':None})))
                self.assertEqual(len(prepared),1);self.assertEqual(len(acquired),3)

if __name__=='__main__':unittest.main()
