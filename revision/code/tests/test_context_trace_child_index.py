"""Synthetic offline tests: complete physical trees, merge and exclusion semantics."""
import copy
import unittest

from context_ledger_r3 import normalize_rows


A, B, D = ('0x' + c * 40 for c in '123')


def top(number=1, **changes):
    row = dict(record_type='transaction', tx_hash='0x' + format(number, '064x'),
        block_number=10, block_hash='0x' + 'a' * 64, tx_index=number,
        from_address=A, to_address=B, value_raw='10', success=True,
        gas_used=21000, effective_gas_price=2, fee_raw='42000',
        evidence_ids=['synthetic:top:' + str(number)])
    row.update(changes)
    return row


def trace(number, path, subtraces=0, **changes):
    row = top(number)
    row.update(record_type='trace', trace_address=path, trace_type='call',
        call_type='call', subtraces=subtraces,
        evidence_ids=['synthetic:trace:' + str(number) + ':' + str(path)])
    if path:
        row.update(from_address=B, to_address=D, value_raw='1')
    row.update(changes)
    return row


def family(number=1):
    return [top(number), trace(number, [], 2), trace(number, [0], 1),
        trace(number, [0, 0]), trace(number, [1], 1), trace(number, [1, 0])]


def equivalence_cases():
    """Tiny public inputs also used by the private frozen-base comparison."""
    duplicate = family()
    duplicate[1]['subtraces'] = None
    duplicate += [trace(1, [], 2, evidence_ids=['synthetic:second-root']),
                  trace(1, [0], 1, evidence_ids=['synthetic:second-child'])]
    conflict = family() + [trace(1, [0], 1, value_raw='999'),
                           trace(1, [1], 1, success=False)]
    missing = [top(), trace(1, [], 2), trace(1, [0])]
    missing_ancestor = [top(), trace(1, [], 0), trace(1, [0, 0])]
    reverted = [top(), trace(1, [], 1), trace(1, [0], 1, success=False),
                trace(1, [0, 0])]
    failed_top = [top(success=False), trace(1, [], 1, success=False), trace(1, [0])]
    zero_static = [top(value_raw='0'), trace(1, [], 1, value_raw='0'),
                   trace(1, [0], 1, value_raw='0', call_type='staticcall'),
                   trace(1, [0, 0], call_type='delegatecall')]
    create_refund = [top(to_address=None), trace(1, [], 1, trace_type='create',
                        to_address=None, created_address=B),
                    trace(1, [0], trace_type='suicide', from_address=None,
                        to_address=None, created_address=B, refund_address=D)]
    return {
        'empty': [], 'root_only': [top(), trace(1, [])],
        'two_transactions_same_paths': family(1) + family(2),
        'null_root': [top(), trace(1, None, 1), trace(1, [0])],
        'merged_duplicate': duplicate, 'conflicting_duplicate': conflict,
        'missing_children': missing, 'missing_ancestor': missing_ancestor,
        'reverted_ancestor': reverted, 'failed_top': failed_top,
        'zero_static_delegate': zero_static, 'create_selfdestruct': create_refund,
        'reverse_order': list(reversed(family())),
        'cross_tx_interleaved': [r for pair in zip(family(1), family(2)) for r in pair],
        'string_paths': [top(), trace(1, '[]', 1), trace(1, '[0]')],
        'invalid_boolean_path': [top(), trace(1, [True])],
        'invalid_negative_path': [top(), trace(1, [-1])],
        'gas_arithmetic_conflict': [top(fee_raw='41999'), trace(1, [])],
    }


class ContextTraceChildIndexTests(unittest.TestCase):
    def test_same_paths_in_distinct_transactions_do_not_mix(self):
        result = normalize_rows(family(1) + family(2))
        self.assertFalse(result['conflicts'])
        self.assertEqual(len(result['flows']), 10)

    def test_null_root_is_excluded_and_does_not_prove_ancestor(self):
        result = normalize_rows(equivalence_cases()['null_root'])
        reasons = [r['reason'] for r in result['excluded']]
        self.assertIn('TRACE_PATH_MISSING_NO_HASH_ORDER', reasons)
        self.assertIn('ANCESTOR_SUCCESS_EVIDENCE_MISSING', reasons)

    def test_duplicate_merge_counts_each_physical_child_once(self):
        result = normalize_rows(equivalence_cases()['merged_duplicate'])
        self.assertFalse(result['conflicts'])
        self.assertEqual(len(result['flows']), 5)
        child = next(f for f in result['flows'] if f['trace_address'] == [0])
        self.assertIn('synthetic:second-child', child['evidence_ids'])

    def test_duplicate_conflicts_remain_ordered(self):
        result = normalize_rows(equivalence_cases()['conflicting_duplicate'])
        self.assertEqual([r['physical_id'].rsplit(':', 1)[1] for r in result['conflicts']], ['0', '1'])
        self.assertEqual([r['fields'] for r in result['conflicts']], [['amount_raw'], ['success']])

    def test_missing_children_is_explicit_conflict(self):
        result = normalize_rows(equivalence_cases()['missing_children'])
        self.assertEqual([(r['reason'], r['expected_children'], r['observed_children'])
            for r in result['conflicts']], [('TRACE_TREE_CHILD_COUNT_CONFLICT', 2, 1)])

    def test_missing_ancestor_not_repaired_by_index(self):
        result = normalize_rows(equivalence_cases()['missing_ancestor'])
        self.assertIn('ANCESTOR_SUCCESS_EVIDENCE_MISSING', [r['reason'] for r in result['excluded']])
        self.assertEqual(len(result['flows']), 1)

    def test_reverted_frames_count_as_physical_children_but_not_flows(self):
        result = normalize_rows(equivalence_cases()['reverted_ancestor'])
        self.assertFalse(result['conflicts'])
        self.assertEqual(len(result['flows']), 1)
        self.assertEqual(sum(r['reason'] == 'FAILED_FRAME_OR_ANCESTOR_ROLLBACK'
            for r in result['excluded']), 2)
        self.assertEqual(result['transactions'][0]['fee_raw'], '42000')

    def test_failed_zero_and_context_calls_preserve_gas_without_value(self):
        for name in ('failed_top', 'zero_static_delegate'):
            with self.subTest(name=name):
                result = normalize_rows(equivalence_cases()[name])
                self.assertFalse(result['conflicts'])
                self.assertEqual(result['flows'], [])
                self.assertEqual(result['transactions'][0]['fee_raw'], '42000')

    def test_create_and_refund_endpoints_remain_bound(self):
        result = normalize_rows(equivalence_cases()['create_selfdestruct'])
        self.assertFalse(result['conflicts'])
        self.assertEqual([(f['sender'], f['recipient']) for f in result['flows']], [(A, B), (B, D)])

    def test_input_rows_not_changed(self):
        rows = equivalence_cases()['merged_duplicate']
        frozen = copy.deepcopy(rows)
        normalize_rows(rows)
        self.assertEqual(rows, frozen)


if __name__ == '__main__':
    unittest.main()
