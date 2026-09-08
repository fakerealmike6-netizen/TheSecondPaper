"""Replay receipts reject changed inputs and forged endpoints; no private data."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from context_lp_r3 import run_context_document
from stage1c_real_replay import same_input_comparison
from test_context_lp_r3 import example


class FinalRealReplayTests(unittest.TestCase):
    def setUp(self):
        self.ledger = {'model_input': example(), 'source_manifest': {'files': [{'path': 'a', 'sha256': 'a'*64, 'bytes': 1}]}}
        self.amounts = run_context_document(self.ledger['model_input'])

    def test_exact_replay_compares_witnesses(self):
        self.assertTrue(same_input_comparison(self.ledger, self.ledger, self.amounts, self.amounts)['passed'])

    def test_only_provenance_path_relocation_allowed(self):
        renamed = copy.deepcopy(self.ledger)
        renamed['source_manifest']['files'][0]['path'] = 'new/a'
        self.assertTrue(same_input_comparison(self.ledger, renamed, self.amounts, self.amounts)['passed'])
        renamed['source_manifest']['files'][0]['sha256'] = 'b'*64
        self.assertFalse(same_input_comparison(self.ledger, renamed, self.amounts, self.amounts)['passed'])

    def test_changed_initial_balance_cannot_reuse_saved_answers(self):
        changed = copy.deepcopy(self.ledger)
        changed['model_input']['accounts'][0]['initial_actual_balance_raw'] = '100'
        result = same_input_comparison(self.ledger, changed, self.amounts, self.amounts)
        self.assertFalse(result['passed'])
        self.assertFalse(result['evidence_to_ledger_equal'])

    def test_forged_endpoint_rejected(self):
        forged = copy.deepcopy(self.amounts)
        forged['variants']['BEST_AVAILABLE_CONTEXT']['all_service_joint']['lower_raw'] = '71'
        self.assertFalse(same_input_comparison(self.ledger, self.ledger, self.amounts, forged)['passed'])

    def test_missing_private_inputs_cli_fails_and_writes_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = Path(__file__).resolve().parents[1] / 'src/stage1c_real_replay.py'
            result = subprocess.run([sys.executable, str(script), '--tree', str(root/'missing'), '--output', str(root/'out')], capture_output=True)
            self.assertEqual(result.returncode, 1)
            receipt = json.loads((root/'out/REAL_REPLAY_RECEIPT.json').read_text(encoding='utf-8'))
            self.assertFalse(receipt['passed'])
            self.assertEqual(receipt['status'], 'ERROR')


if __name__ == '__main__':
    unittest.main()
