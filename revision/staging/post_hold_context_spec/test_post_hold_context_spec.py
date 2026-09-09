"""Small synthetic tests for the root-only preparation wrapper."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import prepare_post_hold_context_spec as builder

class Driver:
    SCHEMA='stage1d-current-context-point-preparation-v1'
    @staticmethod
    def inside(root,path):
        result=(root/path).resolve()
        if not result.is_relative_to(root.resolve()):raise ValueError('escaped')
        return result
    @staticmethod
    def checked(root,ref):
        data=(root/ref['path']).read_bytes()
        if hashlib.sha256(data).hexdigest()!=ref['sha256']:raise ValueError('SHA changed')
        return json.loads(data)
    @staticmethod
    def source_inventory(work):return {'controlled.py':'a'*64}
    @staticmethod
    def derive(*args,**kwargs):raise ValueError('SYNTHETIC_STRICT_NULL_ROOT_OR_FACT_BLOCK')

class PostHoldSpecTests(unittest.TestCase):
    def ref(self,work,name,value,field='events'):
        p=work/name;p.write_text(json.dumps(value),encoding='utf-8')
        return {'path':name,'sha256':builder.sha(p),'bytes':p.stat().st_size,'field':field,'query_name':'txphish_src001'}

    def test_raw_rows_preserve_null_duplicate_foreign_asset_and_gas(self):
        with tempfile.TemporaryDirectory() as tmp:
            work=Path(tmp)
            raw={'record_type':'trace','trace_address':None,'tx_hash':'0x'+'1'*64,'value_raw':'0','gas_used':'3'}
            foreign={'kind':'token','asset':'erc20:eip155:1:0x'+'2'*40,'amount_raw':'500','opaque_extra':{'original':True}}
            first=self.ref(work,'old.json',[raw,foreign]);second=self.ref(work,'bq.json',[raw])
            got=builder.merge_material(work,{'old_materials':[first],'shared_bq_ledger':second},Driver)
            self.assertEqual(got['events'],[raw,foreign,raw]);self.assertIsNone(got['events'][0]['trace_address'])
            self.assertFalse(got['full_context_claimed']);self.assertFalse(got['old_model_input_imported'])

    def test_changed_material_sha_and_old_model_field_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            work=Path(tmp);ref=self.ref(work,'x.json',[]);wrong=dict(ref,sha256='0'*64)
            with self.assertRaisesRegex(ValueError,'SHA'):builder.merge_material(work,{'old_materials':[],'shared_bq_ledger':wrong},Driver)
            with self.assertRaisesRegex(ValueError,'old model'):builder.merge_material(work,{'old_materials':[dict(ref,field='model_input')],'shared_bq_ledger':ref},Driver)

    def test_existing_writer_lock_is_not_removed_or_recovered(self):
        with tempfile.TemporaryDirectory() as tmp:
            work=Path(tmp);(work/'private').mkdir();lock=work/'private/network_worker.lock';lock.write_text('existing')
            with self.assertRaisesRegex(ValueError,'Existing writer'):builder.safe_point(work)
            self.assertEqual(lock.read_text(),'existing')

    def test_strict_requirements_failure_saves_spec_and_no_retry_or_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            work=Path(tmp);(work/'private').mkdir();batch=work/'private/batch.json'
            batch.write_text(json.dumps({'queries':[{'name':n,'query_id':n,'scope_hash':'s'} for n in builder.NAMES]}))
            ref=self.ref(work,'old.json',[]);reviewed={'old_materials':[ref],'shared_bq_ledger':ref}
            entries={'collection':{'path':'dummy','sha256':'0'*64},'labels':{'path':'dummy','sha256':'0'*64},'role_replay':{'path':'dummy','sha256':'0'*64}}
            with patch('stage1d_closure_scope.active_batch_path',return_value=batch),patch('stage1d_runtime.Runtime.require_gate'),patch.object(builder,'current_entry',side_effect=lambda *a:dict(entries)):
                got=builder.prepare(work,reviewed,'synthetic',Driver,check_cache=True)
                self.assertEqual(got['status'],'REQUIREMENTS_BLOCKED_INPUTS_PRESERVED')
                self.assertEqual(got['blocked_phase'],'CURRENT_REQUIREMENTS_AND_EXACT_CACHE')
                out=work/'private/current_context_points/synthetic'
                self.assertTrue((out/'SPEC.json').is_file())
                result=json.loads((out/'PREPARATION_RESULT.json').read_bytes())
                self.assertFalse(result['automatic_alternate_input_or_retry']);self.assertFalse(result['full_context_claimed'])
                with self.assertRaises(FileExistsError):builder.prepare(work,reviewed,'synthetic',Driver,check_cache=True)

if __name__=='__main__':unittest.main()
