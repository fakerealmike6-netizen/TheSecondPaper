"""Production admission preserves duplicate source proofs and protocol scope."""
from copy import deepcopy
import unittest
from test_current_context_paid_bq import PaidContextTests
import stage1d_current_context_paid_bq as paid
from stage1d_protocol_supplement import build_protocol_sql
from context_queries_r3 import _build_sql


class PaidMergeContracts(PaidContextTests):
    # The inherited original paid-source tests also exercise the actual changed
    # admission entry. New assertions use two independently retained proofs.
    def sources(self):
        ref = self.f.prepare(self.f.family())
        one = paid._load_source(self.w, ref)
        two = deepcopy(one)
        for source, digit in ((one, 'a'), (two, 'b')):
            for family in source['families'].values():
                root = next(r for r in family if r.get('trace_address') == '[]')
                root['root_position_binding'] = {
                    'status': 'ROOT_EQUIVALENT', 'original_row_sha256': digit * 64}
        return one, two

    def test_actual_admission_keeps_two_source_proofs(self):
        result = paid._admit_plan(self.f.scope, self.f.plan, self.sources(), paid._Points(self.w, []))
        root = next(r for r in result['rows'] if r.get('trace_address') == '[]')
        self.assertEqual(len(root['root_position_binding_proofs']), 2)
        self.assertEqual(len(result['rows']), 3)

    def test_actual_admission_still_rejects_amount_conflict(self):
        one, two = self.sources()
        for family in two['families'].values():
            for row in family:
                if row.get('trace_address') == '[]': row['value_raw'] = '10'
        with self.assertRaises(ValueError):
            paid._admit_plan(self.f.scope, self.f.plan, [one, two], paid._Points(self.w, []))


class ProtocolProjectionContracts(unittest.TestCase):
    def test_all_three_original_protocol_parts_unchanged(self):
        q = {'rows': [{'address': '0x'+'1'*40, 'role': 'NON_TERMINAL_MODEL_ACCOUNT',
                       'ledger_start_block': 100, 'ledger_end_block': 200}]}
        full = _build_sql(q, '2024-08-21', '2024-09-07')
        sql = build_protocol_sql(q, '2024-08-21', '2024-09-07')
        self.assertEqual(sql.split('\nUNION ALL\n')[1:], full.split('\nUNION ALL\n')[3:])
        self.assertTrue(sql.endswith('\n'.join(full.split('\nUNION ALL\n')[2:][0].split('\n'))+'\nUNION ALL\n'+'\nUNION ALL\n'.join(full.split('\nUNION ALL\n')[3:])))
        self.assertIn("t.amount * UINT256 '1000000000'", sql)
        self.assertIn('t.tx_hash IS NULL', sql)
        self.assertIn('t.miner=s.address', sql)
        self.assertNotIn('JOIN tx_keys', sql)
        self.assertNotIn('LIMIT ', sql)

    def test_invalid_account_scope_remains_rejected(self):
        with self.assertRaises(ValueError):
            build_protocol_sql({'rows': []}, '2024-08-21', '2024-09-07')
