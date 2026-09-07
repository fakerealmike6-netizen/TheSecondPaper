"""F09 validation outcomes cannot be overwritten by component data."""
import contextlib, io, tempfile, unittest
from pathlib import Path
from validation_result_r4 import CHECK_SCHEMA, check_row, row_failed, receipt_failed, skip_row
from validate_review_bundle_r1 import Validator
from validation_faults_r4 import run_case


class ValidationStatusR4Tests(unittest.TestCase):
    def test_every_reserved_business_field_cannot_override_failure(self):
        v=Validator.__new__(Validator);v.commands=[]
        result=v.check('actual_check',False,**{'name':'false_name','status':'PASS','passed':True,
                       'check_status':'PASS','schema_version':'invented','required':False})
        self.assertIs(result,False)
        row=v.commands[-1]
        self.assertEqual((row['name'],row['status'],row['check_status'],row['passed']),('actual_check','FAIL','FAIL',False))
        self.assertEqual(row['component_status'],'PASS')
        self.assertEqual(row['detail']['name'],'false_name')
        self.assertEqual(row['schema_version'],CHECK_SCHEMA)
        self.assertEqual(len(v.validation_failures()),1)

    def test_unknown_or_truthy_nonboolean_is_not_pass(self):
        for value in (None,1,'PASS',[],{},object()):
            self.assertTrue(row_failed(check_row('unknown',value)))

    def test_passed_replay_and_uncertified_business_state_are_separate(self):
        row=check_row('weth_replay',True,status='OBSERVED_LOCAL_DEPOSIT_PATTERN_WITH_GAPS',real_component_certified=False)
        self.assertEqual(row['status'],'PASS');self.assertEqual(row['component_status'],'OBSERVED_LOCAL_DEPOSIT_PATTERN_WITH_GAPS')
        self.assertFalse(row['real_component_certified']);self.assertFalse(row_failed(row))

    def test_required_skip_unknown_schema_and_inconsistent_status_fail(self):
        self.assertTrue(row_failed(skip_row('required','not run',True)))
        self.assertFalse(row_failed(skip_row('optional','public excludes private',False)))
        self.assertTrue(row_failed({'name':'x','status':'PASS'}))
        self.assertTrue(row_failed(check_row('x',True)|{'status':'business'}))

    def test_legacy_receipts_require_explicit_schema_and_unknown_is_ambiguous(self):
        self.assertFalse(row_failed({'name':'old','status':'PASS'},legacy_schema='stage1b-r3-portable-validation-v1'))
        self.assertTrue(row_failed({'name':'old','status':'OBSERVED_LOCAL_DEPOSIT_PATTERN_WITH_GAPS'},legacy_schema='stage1b-r3-portable-validation-v1'))
        self.assertTrue(receipt_failed({'schema_version':'unknown','status':'PASS','commands':[{'status':'PASS'}]}))
        self.assertTrue(receipt_failed({'schema_version':'stage1b-r3-portable-validation-v1','status':'PASS','commands':[]}))

    def test_missing_required_command_is_explicit_failure(self):
        v=Validator.__new__(Validator);v.commands=[];v.check('unrelated',True)
        self.assertEqual(len(v.validation_failures(['unit_tests'])),1)
        self.assertEqual(v.commands[-1]['missing_check'],'unit_tests')

    def test_real_validator_cli_mismatch_fails_then_normal_gap_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for mode in ('weth_mismatch','child_nonzero','missing_result','malformed_result','normal_expected_gap'):
                with self.subTest(mode=mode):
                    row=run_case(root/mode,mode)
                    self.assertTrue(row['passed'],row)
                    if mode=='weth_mismatch':
                        self.assertEqual(row['weth_check']['check_status'],'FAIL')
                        self.assertIs(row['weth_check']['passed'],False)
                        self.assertEqual(row['weth_check']['component_status'],'OBSERVED_LOCAL_DEPOSIT_PATTERN_WITH_GAPS')

    def test_r4_final_validator_cli_preserves_failure_and_normal_gap(self):
        with tempfile.TemporaryDirectory() as tmp:
            for mode in ('weth_mismatch','child_nonzero','missing_result','malformed_result','normal_expected_gap'):
                with self.subTest(mode=mode):
                    row=run_case(Path(tmp)/mode,mode,'r4')
                    self.assertTrue(row['passed'],row)
                    if mode=='weth_mismatch':
                        self.assertEqual(row['weth_check']['check_status'],'FAIL')
                        self.assertIs(row['weth_check']['passed'],False)


if __name__=='__main__':unittest.main()
