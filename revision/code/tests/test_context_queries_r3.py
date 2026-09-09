import unittest
from context_queries_r3 import build


class ContextScopeTests(unittest.TestCase):
    def fixture(self):
        return {'rows':[{'address':'0x'+'1'*40,'role':'NON_TERMINAL_MODEL_ACCOUNT','ledger_start_block':17000000,'ledger_end_block':17000002,'first_candidate_timestamp':1682000000,'last_candidate_timestamp':1682000024}],
                'block_headers': {number: {'block_number': number, 'block_hash': '0x'+format(number,'064x'), 'timestamp': timestamp, 'evidence_ids': ['SYNTHETIC_EXISTING_SCOPE_BOUNDARY']} for number,timestamp in ((17000000,1682000000),(17000002,1682000024))}}
    def test_complete_scope_preserves_failed_and_zero(self):
        sql=build(self.fixture())
        self.assertIn('17000000,17000002',sql)
        self.assertNotIn('t.success = true',sql)
        self.assertNotIn('t.value > 0',sql)
        self.assertIn('t.tx_hash=k.hash',sql)
        self.assertIn('t.refund_address=s.address',sql)
        self.assertIn('ethereum.withdrawals',sql)
        self.assertIn('t.miner=s.address',sql)
        self.assertNotIn('\nLIMIT ',sql)
    def test_service_scope_rejected(self):
        q=self.fixture();q['rows'][0]['role']='SERVICE'
        with self.assertRaises(ValueError):build(q)
    def test_duplicate_scope_rejected(self):
        q=self.fixture();q['rows']*=2
        with self.assertRaises(ValueError):build(q)
