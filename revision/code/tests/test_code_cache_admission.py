"""Saved-code adoption, negative bindings, and unchanged shared accounting."""
import json,tempfile,unittest
from pathlib import Path
from copy import deepcopy
from test_stage1d_unknown_cost_registry import Fixture,PROVIDER,A
from stage1d_code_cache_admission import admit_operation
from stage1d_runtime import Runtime
from stage1d_transfers_acquisition import cached_member
from stage1d_unknown_cost_registry import Registry,CURRENT_PATH
from read_retry_r4 import logical_key
from context_access_r3 import sha


class AdmissionTests(unittest.TestCase):
    def setup_case(self,root):
        f=Fixture(root,n=0)
        snapshot=json.loads((f.work/f.snapshot_ref['path']).read_bytes())
        snapshot['state_screen_rows'][0]['query_name']=f.query['name']
        f.current['initial_snapshot_ref']=f.write(f.snapshot_ref['path'],snapshot)
        f.current['historical_codes']=[];f.commit()
        plan={'method':'eth_getCode','params':[A,'0x1']}
        key=logical_key(Runtime().rpc_identity(PROVIDER,plan))
        member=cached_member(f.work,plan)
        op={'results':[{'needs':[{'request':plan,'logical_key':key,'query_names':['q1']}],
                       'result':{'members':[member]}}]}
        return f,op

    def test_successful_adoption_idempotence_and_real_registry_consumer(self):
        with tempfile.TemporaryDirectory() as tmp:
            f,op=self.setup_case(tmp);ref=f.write('private/op.json',op)
            db=f.work/'private/read_retry_r4.sqlite';before=sha(db)
            prior=Registry(f.work).identity
            first=admit_operation(f.work,ref,apply=True)
            self.assertEqual(first['gaps'],[]);self.assertTrue(first['admitted'][0]['new_descriptor'])
            self.assertEqual(first['admitted'][0]['code_result'],'0x')
            self.assertNotEqual(Registry(f.work).identity,prior)
            second=admit_operation(f.work,ref,apply=True)
            self.assertFalse(second['admitted'][0]['new_descriptor'])
            self.assertEqual(first['current_catalog_sha256'],second['current_catalog_sha256'])
            self.assertEqual(sha(db),before)
            r,labels=f.resolver();decision=r.resolve(f.state,f.scope,labels.resolve_state(f.state))
            self.assertEqual(decision['action'],'CONTINUE')
            self.assertEqual(decision['base_identity']['kind'],'UNKNOWN')

    def test_wrong_key_or_envelope_is_rejected_without_catalog_change(self):
        for defect in ('key','envelope','body'):
            with self.subTest(defect=defect),tempfile.TemporaryDirectory() as tmp:
                f,op=self.setup_case(tmp);before=sha(f.work/CURRENT_PATH)
                if defect=='key':op['results'][0]['needs'][0]['logical_key']='0'*64
                if defect=='envelope':op['results'][0]['result']['members'][0]['artifact_sha256']='0'*64
                if defect=='body':(f.work/'raw/test/response_body.bin').write_text('[]')
                ref=f.write('private/op.json',op)
                with self.assertRaises(ValueError):admit_operation(f.work,ref,apply=True)
                self.assertEqual(sha(f.work/CURRENT_PATH),before)

    def test_validation_only_does_not_activate_catalog(self):
        with tempfile.TemporaryDirectory() as tmp:
            f,op=self.setup_case(tmp);ref=f.write('private/op.json',op);before=sha(f.work/CURRENT_PATH)
            result=admit_operation(f.work,ref)
            self.assertEqual(result['status'],'VALIDATED_NOT_ADOPTED')
            self.assertEqual(sha(f.work/CURRENT_PATH),before)


if __name__=='__main__':unittest.main()
