"""Synthetic dispatch guard checks; methods/registration never run."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import root_final_methods as driver

class FinalDriverGuards(unittest.TestCase):
    def test_prepare_only_plan_cannot_execute(self):
        with patch.object(driver,'prepare',side_effect=AssertionError('Must reject before loading APIs')):
            with self.assertRaisesRegex(ValueError,'four terminal'):driver.execute_once('.',{'final_dispositions_supplied':False},'p','s',True)

    def test_root_final_flag_is_required(self):
        with patch.object(driver,'prepare',side_effect=AssertionError('Must reject before loading APIs')):
            with self.assertRaisesRegex(ValueError,'root explicit'):driver.execute_once('.',{'final_dispositions_supplied':True},'p','s',False)

    def test_terminal_domain_cannot_omit_queries_or_evidence(self):
        names=['one','two','three','four']
        value={'schema_version':driver.DISPOSITIONS,'final_dispositions':True,'queries':{
            n:{'disposition':'NOT_RUN','reason':'Explicit synthetic terminal failure','evidence':[{'path':'synthetic.json','sha256':'0'*64}]} for n in names}}
        self.assertEqual(set(driver.final_mapping(value,names)),set(names))
        for defect in ('missing_query','missing_evidence','missing_reason'):
            changed=copy.deepcopy(value)
            if defect=='missing_query':changed['queries'].pop('four')
            elif defect=='missing_evidence':changed['queries']['one']['evidence']=[]
            else:changed['queries']['one']['reason']=''
            with self.subTest(defect=defect),self.assertRaises(ValueError):driver.final_mapping(changed,names)

    def test_changed_source_or_input_stops_before_claim(self):
        plan={'final_dispositions_supplied':True,'driver_sha256':driver.sha(driver.__file__),
              'final_dispositions_ref':{'path':'x','sha256':'x'}}
        with patch.object(driver,'prepare',return_value={'source_inventory_sha256':'new'}),patch.object(driver,'claim_once',side_effect=AssertionError('Claim not allowed')):
            with self.assertRaisesRegex(ValueError,'source or inputs'):driver.execute_once('.',plan,'plan','old',True)

    def test_existing_claim_or_output_blocks_reinitialization(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            work=Path(tmp)
            claim=driver.claim_once(work,'reviewed-plan','batch','results')
            initial=claim.read_bytes()
            with self.assertRaisesRegex(ValueError,'already exist'):driver.claim_once(work,'another-plan','other-batch','other-results')
            self.assertEqual(claim.read_bytes(),initial)
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            work=Path(tmp);(work/'results').mkdir()
            with self.assertRaisesRegex(ValueError,'already exist'):driver.claim_once(work,'plan','batch','results')
            self.assertFalse((work/driver.CLAIM).exists())

    def test_escaped_dependency_is_rejected(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            with self.assertRaisesRegex(ValueError,'escaped'):driver.inside(Path(tmp),'../outside.json')

if __name__=='__main__':unittest.main()
