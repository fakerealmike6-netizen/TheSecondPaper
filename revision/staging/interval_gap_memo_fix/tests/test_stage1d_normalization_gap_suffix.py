"""Portable, synthetic controls for the exact pre-append membership semantics."""
from dataclasses import asdict, replace
import copy
import json
from pathlib import Path
import random
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import stage1d_acquisition as acquisition
from collector import Event, NATIVE, Scope


def original_suffix(original_gaps, matching):
    return [g for r in matching for g in r.get('normalization_gaps', []) if g not in original_gaps]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


class GapSuffixTests(unittest.TestCase):
    def compare(self, base, records):
        before = copy.deepcopy((base, records))
        expected = original_suffix(base, records)
        actual = acquisition._normalization_gap_suffix(base, records)
        self.assertEqual(expected, actual)
        self.assertEqual([id(v) for v in expected], [id(v) for v in actual])
        self.assertEqual(before, (base, records))
        return actual

    def test_suffix_duplicates_survive_within_and_across_records(self):
        a = {'reason': 'ROOT_EQUIVALENCE_UNRESOLVED_GAP', 'tx_hash': 'synthetic:tx'}
        b = copy.deepcopy(a)
        values = self.compare([{'reason': 'UNCOLLECTED_ADDRESS_INTERVAL'}],
                              [{'normalization_gaps': [a, a, b]}, {'normalization_gaps': [b, a]}])
        self.assertEqual(5, len(values))

    def test_original_list_members_suppressed_but_no_incremental_dedup(self):
        existing = {'reason': 'PHYSICAL_FACT_CONFLICT', 'detail': [1, {'x': 0}]}
        x = {'reason': 'ROOT_EQUIVALENCE_UNRESOLVED_GAP', 'tx_hash': 'x'}
        values = self.compare([existing, copy.deepcopy(existing)],
            [{'normalization_gaps': [copy.deepcopy(existing), x, x]}, {'normalization_gaps': [existing, x]}])
        self.assertEqual([x, x, x], values)

    def test_python_nested_numeric_bool_and_dict_order_equality(self):
        base = [{'reason': 'R', 'value': [True, {'n': 0}]},
                {'reason': 1, 'value': [0]}, {'reason': None}, {'without_reason': [False]}]
        records = [{'normalization_gaps': [
            {'value': [1.0, {'n': False}], 'reason': 'R'},
            {'reason': True, 'value': [False]}, {'reason': None}, {'without_reason': [0]},
            {'reason': [], 'value': 'kept'}, {'reason': 'R', 'value': [2]},
        ]}]
        self.assertEqual(2, len(self.compare(base, records)))

    def test_empty_lists_absent_field_and_unhashable_fallback(self):
        values = self.compare([{'reason': ['r'], 'value': 1}],
            [{}, {'normalization_gaps': []}, {'normalization_gaps': [
                {'value': True, 'reason': ['r']}, {'reason': {'nested': 'r'}}, {'reason': {'nested': 'r'}}]}])
        self.assertEqual(2, len(values))

    def test_no_cross_call_memo_when_records_or_base_change(self):
        gap = {'reason': 'R', 'value': 1}
        records = [{'normalization_gaps': [gap, gap]}]
        self.assertEqual(2, len(self.compare([], records)))
        self.assertEqual([], self.compare([copy.deepcopy(gap)], records))
        gap['value'] = 2
        self.assertEqual(2, len(self.compare([{'reason': 'R', 'value': 1}], records)))

    def test_deterministic_small_json_cases_preserve_exact_order(self):
        rng = random.Random(19)
        atoms = [None, 0, 1, False, True, '', 'a', [], [1], {'nested': 2}]
        pool = [{'reason': reason, 'value': value} for reason in ('R', 'S', None, 1, []) for value in atoms]
        for _ in range(40):
            base = rng.choices(pool, k=8)
            records = [{'normalization_gaps': rng.choices(pool, k=12)} for _ in range(3)]
            self.compare(base, records)


class FetchEquivalenceTests(unittest.TestCase):
    def providers(self, *, complete=False, physical_conflict=False):
        a, b = '0x'+'1'*40, '0x'+'2'*40
        event = Event('synthetic:event', '0x'+'a'*64, a, b, NATIVE, 10, 101, 0, 1001)
        gaps = [{'reason': 'ROOT_EQUIVALENCE_UNRESOLVED_GAP', 'tx_hash': event.tx_hash},
                {'reason': 'TRANSACTION_FAMILY_BINDING_INCOMPLETE', 'detail': 'synthetic', 'tx_hash': event.tx_hash}]
        records = []
        for index in range(3):
            one = replace(event, amount_raw=11) if physical_conflict and index == 2 else event
            records.append({'evidence_id': 'synthetic:'+str(index), 'addresses': [a, b], 'asset': NATIVE,
                'start_block': 100, 'end_block': 110, 'start_time': 1000, 'end_time': 1010,
                'complete': complete, 'normalization_gaps': copy.deepcopy(gaps+gaps),
                'events_by_address': {a: [one], b: [one]}, 'events': [one],
                'coverage_sha256': str(index)*64, 'events_sha256': str(index+1)*64})
        result = []
        for name in ('left', 'right'):
            p = acquisition.CachedIntervals.__new__(acquisition.CachedIntervals)
            p.work = Path(__file__).resolve().parent
            p.records = copy.deepcopy(records); p.pending = []; p.scope = None
            p.bind_scope(Scope('synthetic:'+name, 'synthetic', 100, 110, 1000, 1010, 2, None, 'REFERENCE_FULL'))
            result.append(p)
        # Compare the same query binding, then explicitly switch both later.
        result[1].bind_scope(result[0].scope)
        return a, result

    def compare_fetch(self, providers, address):
        args = (address, NATIVE, 100, 110)
        kw = dict(start_time=1000, end_time=1010, global_end_time=1010)
        with patch.object(acquisition, '_normalization_gap_suffix', original_suffix):
            old = providers[0].fetch_interval(*args, **kw)
        new = providers[1].fetch_interval(*args, **kw)
        self.assertEqual(canonical(asdict(old)), canonical(asdict(new)))
        self.assertEqual(providers[0].pending, providers[1].pending)
        return new

    def test_full_fetch_partial_duplicate_gap_coverage_and_query_binding_equal(self):
        address, providers = self.providers()
        value = self.compare_fetch(providers, address)
        self.assertFalse(value.complete)
        self.assertEqual(13, len(value.gaps))  # One missing rectangle + 3 * 4 suffix entries.
        self.assertEqual(3, len(value.coverage[0]['verified_content_intervals']))
        self.compare_fetch(providers, address)
        for p in providers:
            p.bind_scope(Scope('synthetic:other-query', 'synthetic', 100, 110, 1000, 1010, 2, None, 'REFERENCE_FULL'))
        self.compare_fetch(providers, address)

    def test_complete_fetch_retains_original_no_suffix_branch(self):
        address, providers = self.providers(complete=True)
        value = self.compare_fetch(providers, address)
        self.assertTrue(value.complete)
        self.assertEqual([], value.gaps)

    def test_physical_conflicts_quarantine_and_full_output_equal(self):
        address, providers = self.providers(physical_conflict=True)
        value = self.compare_fetch(providers, address)
        self.assertFalse(value.complete)
        self.assertEqual([], value.events)
        self.assertTrue(value.fact_conflicts)
        self.assertTrue(value.quarantined_facts)


if __name__ == '__main__':
    unittest.main()
