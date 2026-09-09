"""Synthetic canonical headers only; no clock, database or external calls."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from stage1d_timestamp_bracket import resolve_timestamp_bracket, verify_timestamp_bracket


class BracketTests(unittest.TestCase):
    def example(self, timestamps, start, end):
        headers = {i+100: {'number': hex(i+100), 'timestamp': hex(stamp), 'hash': '0x'+format(i+100, '064x')}
                   for i, stamp in enumerate(timestamps)}
        need = {'start_block': 100, 'end_block': 99+len(timestamps), 'start_time': start, 'end_time': end}
        calls = []
        def get(block):
            calls.append(block)
            return headers[block]
        proof = resolve_timestamp_bracket(need, get)
        self.assertEqual(verify_timestamp_bracket(need, proof, headers), proof)
        self.assertEqual(len(calls), len(set(calls)))
        return need, proof, headers, calls

    def test_first_last_are_inclusive_and_equal_seconds_keep_whole_block(self):
        n, p, headers, calls = self.example([100, 110, 120, 130, 140], 110, 130)
        self.assertEqual((p['first_block'], p['last_block']), (101, 103))
        self.assertFalse(p['empty'])
        n, p, headers, calls = self.example([100, 110, 120], 110, 110)
        self.assertEqual((p['first_block'], p['last_block']), (101, 101))

    def test_empty_before_after_and_internal_skipped_seconds(self):
        for start, end, expected in [(1, 99, (100, 99)), (201, 210, (102, 101)), (150, 160, (101, 100))]:
            n, p, headers, calls = self.example([100, 200], start, end)
            self.assertTrue(p['empty'])
            self.assertEqual((p['first_block'], p['last_block']), expected)

    def test_necessary_neighbor_is_required_and_not_a_success_flag(self):
        n, p, headers, calls = self.example([100, 110, 120, 130, 140], 110, 130)
        for block in (100, 101, 103, 104):
            missing = dict(headers); missing.pop(block)
            with self.assertRaises(ValueError): verify_timestamp_bracket(n, p, missing)
        changed = deepcopy(p); changed['first_block'] = 102
        with self.assertRaises(ValueError): verify_timestamp_bracket(n, changed, headers)
        changed = deepcopy(p); changed['empty'] = True
        with self.assertRaises(ValueError): verify_timestamp_bracket(n, changed, headers)

    def test_nonmonotonic_header_or_wrong_block_refused(self):
        n, p, headers, calls = self.example([100, 110, 120], 110, 110)
        altered = deepcopy(headers); altered[102]['timestamp'] = '0x64'
        with self.assertRaises(ValueError): verify_timestamp_bracket(n, p, altered)
        altered = deepcopy(headers); altered[101]['number'] = '0x64'
        with self.assertRaises(ValueError): verify_timestamp_bracket(n, p, altered)

    def test_binary_not_linear_and_full_endpoints_only(self):
        stamps = list(range(1000, 21000, 2))
        n, p, headers, calls = self.example(stamps, 9001, 9101)
        self.assertLessEqual(len(calls), 34)
        self.assertEqual((p['first_block'], p['last_block']), (4101, 4150))
        n, p, headers, calls = self.example(stamps, 1000, 20998)
        self.assertEqual(len(calls), 2)


if __name__ == '__main__':
    unittest.main()
