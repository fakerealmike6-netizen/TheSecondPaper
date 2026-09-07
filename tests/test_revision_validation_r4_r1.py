"""Exercise actual revision validator failures and the normal expected-gap control."""
from pathlib import Path
import tempfile,unittest
from validation_faults_r4_r1 import run_case

class RevisionValidationControls(unittest.TestCase):
    def test_actual_revision_cli_faults_and_positive_control(self):
        for mode in ('weth_mismatch','child_nonzero','missing_result','malformed_result','normal_expected_gap'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                result=run_case(Path(directory)/'case',mode)
                self.assertTrue(result['passed'],result)
                self.assertEqual(result['process_exit_code'],0 if mode=='normal_expected_gap' else 1)

if __name__=='__main__':unittest.main()
