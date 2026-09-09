"""Compact gap storage at real cache, registration and offline-review edges."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from collector import FetchResult, NATIVE
from stage1d_gap_sequence import (GapAccumulator, concat_gaps, gap_count,
                                 iter_gaps, overlay_gaps, serialize_gaps)
from stage1d_window import IntervalContentCache
from validate_review_bundle_r1 import gap_projection_multiset
import stage1d_experiments as experiments
import test_stage1d_experiments as fixture


class GapConsumerEdgeTests(unittest.TestCase):
    def repeated(self):
        base = [{'reason':'ROOT_BINDING_REQUIRED', 'proof':{'selectors':[1, 2]}},
                {'reason':'ROOT_BINDING_REQUIRED', 'proof':{'selectors':[1, 2]}}]
        return concat_gaps(*(overlay_gaps(base, {'address':'0xabc', 'arrival_event_id':str(i)})
                             for i in range(100)))

    def test_interval_partial_cache_round_trip_retains_all_occurrences(self):
        gaps = self.repeated()
        request = {'address':'0xabc', 'asset':NATIVE, 'start_block':10,
                   'end_block':12, 'start_time':100, 'end_time':120}
        with tempfile.TemporaryDirectory() as temp:
            cache = IntervalContentCache(None, temp, replay_only=True)
            original = FetchResult(events=[], complete=False, gaps=gaps)
            digest = cache.store(request, original)
            wire = json.loads((Path(temp)/(digest+'.json')).read_bytes())
            self.assertIsInstance(wire['result']['gaps'], dict)
            self.assertEqual(list(iter_gaps(wire['result']['gaps'])), list(gaps))
            result = cache.fetch_interval(**request, global_end_time=120)
            self.assertFalse(result.complete)
            restored = list(iter_gaps(result.gaps))
            self.assertEqual(restored[0]['reason'], 'UNCOVERED_INTERVAL')
            self.assertEqual(restored[1:], list(gaps))
            self.assertEqual(gap_count(result.to_dict()['gaps']), 201)
            self.assertEqual(result.real_requests, 0)

    def test_empty_compact_cache_does_not_add_a_gap(self):
        empty = serialize_gaps([], force_compact=True)
        request = {'address':'0xabc', 'asset':NATIVE, 'start_block':10,
                   'end_block':12, 'start_time':100, 'end_time':120}
        with tempfile.TemporaryDirectory() as temp:
            cache = IntervalContentCache(None, temp, replay_only=True)
            cache.store(request, FetchResult(events=[], complete=True, gaps=empty))
            result = cache.fetch_interval(**request, global_end_time=120)
            self.assertTrue(result.complete)
            self.assertEqual(gap_count(result.gaps), 0)

    def test_review_multiset_preserves_duplicate_multiplicity(self):
        view = self.repeated()
        fields = ('reason', 'address')
        expected = gap_projection_multiset({'gaps':list(view)}, fields)
        self.assertEqual(expected, gap_projection_multiset({'gaps':serialize_gaps(view)}, fields))
        self.assertEqual(sum(expected.values()), 200)
        shorter = concat_gaps(view[:-1])
        self.assertNotEqual(expected, gap_projection_multiset({'gaps':serialize_gaps(shorter)}, fields))

    def test_review_rejects_wrong_compact_count(self):
        corrupt = serialize_gaps(self.repeated())
        corrupt['expanded_count'] += 1
        with self.assertRaises(ValueError):
            gap_projection_multiset({'gaps':corrupt}, ('reason',))

    def register(self, tree, *, gaps, evidence_gaps, assumptions=None, status='PARTIAL_CONTEXT_WITH_EXPLICIT_ASSUMPTIONS'):
        batch = tree/'batch'
        experiments.initialize_batch(tree, batch, fixture.declared())
        query = fixture.declared()[0]
        document = fixture.document(query)
        document['gaps'] = gaps
        if assumptions is not None: document['assumptions'] = assumptions
        return experiments.register_document(tree, batch, query['query_id'], document,
            fixture.scope(query), {'observations':[{'address':'T', 'kind':'SERVICE'}]},
            {'gaps':evidence_gaps, 'source':'synthetic gap storage check'},
            'ACQUISITION_PARTIAL', status)

    def test_registration_empty_compact_does_not_prove_missing_frontier(self):
        empty = serialize_gaps([], force_compact=True)
        with tempfile.TemporaryDirectory() as temp, self.assertRaisesRegex(ValueError, 'missing frontier or gaps'):
            self.register(Path(temp), gaps=empty, evidence_gaps=empty,
                          assumptions=['Synthetic explicit condition'], status='FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')

    def test_registration_empty_compact_does_not_supply_partial_assumption(self):
        empty = serialize_gaps([], force_compact=True)
        with tempfile.TemporaryDirectory() as temp, self.assertRaisesRegex(ValueError, 'state its limitations'):
            self.register(Path(temp), gaps=empty, evidence_gaps=empty)

    def test_registration_keeps_nonempty_compact_evidence_without_solver(self):
        wire = serialize_gaps(self.repeated())
        with tempfile.TemporaryDirectory() as temp:
            tree = Path(temp)
            row = self.register(tree, gaps=wire, evidence_gaps=wire)
            self.assertEqual(gap_count(row['gaps']), 200)
            saved = json.loads((tree/row['observed_path']).read_bytes())
            self.assertEqual(list(iter_gaps(saved['gaps'])), list(iter_gaps(wire)))
            self.assertEqual(row['method_execution'], 'REGISTERED')


if __name__ == '__main__':
    unittest.main()
