"""Outcome-blind queue ordering and seed safety on public synthetic metadata."""
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from stage1c_catalog import propose, cost_key


def row(qid, incident='INC_NEW', window=100, stratum='S2'):
    return {'query_id': qid, 'incident_id': incident, 'stratum': stratum,
            'next_batch_eligible': stratum != 'S3', 'exact_single_seed': True,
            'seed_member_count': 1, 'cost_window_seconds': window,
            'cost_reference_entry_events': 5, 'seed_cached_event_count': 1,
            'zero_hop_reference': window == 0}


def doc(rows):
    return {'queries': rows, 'registered_query_count': len(rows)}


class CatalogueTests(unittest.TestCase):
    def test_positive_recall_and_amount_fields_cannot_change_queue(self):
        original = doc([row('q'+str(i), 'INC_'+str(i%3), i*100) for i in range(8)])
        changed = copy.deepcopy(original)
        for i, item in enumerate(changed['queries']):
            item.update(source_amount=10**i, lower_bound=1000-i, recall=i/10,
                        latest_registered_reference_positive_addresses=100-i)
        left = propose(original); right = propose(changed)
        self.assertEqual([x['query_id'] for x in left['primary']], [x['query_id'] for x in right['primary']])
        self.assertEqual([x['query_id'] for x in left['reserve']], [x['query_id'] for x in right['reserve']])

    def test_missing_window_is_unknown_not_zero_cost(self):
        self.assertLess(cost_key(row('zero', window=0)), cost_key(row('unknown', window=None)))
        self.assertLess(cost_key(row('known', window=10**9)), cost_key(row('unknown', window=None)))

    def test_legal_zero_hop_not_excluded(self):
        result = propose(doc([row('zero', window=0), row('other')]))
        self.assertEqual(result['primary'][0]['query_id'], 'zero')

    def test_s3_kept_but_not_injected(self):
        multi = row('s3', stratum='S3'); multi['seed_member_count'] = 2
        result = propose(doc([multi, row('safe')]))
        self.assertEqual(len(result['catalog']), 2)
        self.assertEqual([x['query_id'] for x in result['primary']], ['safe'])
        multi['next_batch_eligible'] = True
        with self.assertRaises(ValueError):
            propose(doc([multi]))

    def test_identity_duplicate_rejected_and_tie_is_lexical(self):
        with self.assertRaises(ValueError):
            propose(doc([row('same'), row('same')]))
        result = propose(doc([row('b'), row('a')]))
        self.assertEqual([x['query_id'] for x in result['primary']], ['a', 'b'])


if __name__ == '__main__':
    unittest.main()
