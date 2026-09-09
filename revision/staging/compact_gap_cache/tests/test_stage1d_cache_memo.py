"""Synthetic equivalence of indexed cache reads and original full-list scans."""
from dataclasses import asdict, replace
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import stage1d_acquisition as acquisition
from collector import Collector, Event, NATIVE, Scope

A, B, C, D = ['0x' + str(i) * 40 for i in range(1, 5)]
BASE = 1700000000
DAY = 86400


def event(i, sender=A, recipient=B, day=1):
    tx = '0x' + format(i, '064x')
    return Event('eip155:1:tx:' + tx + ':top', tx, sender, recipient,
                 NATIVE, 100, 100 + day, 0, BASE + day * DAY)


def save(work, name, events, *, shared=None, addresses=(A,), first=0, last=108):
    folder = Path(work) / 'derived/stage1d/intervals'
    folder.mkdir(parents=True, exist_ok=True)
    ep = shared or folder / (name + '.events.json')
    if shared is None:
        ep.write_text(json.dumps([asdict(e) for e in events]), encoding='utf8')
    record = {'evidence_id': name, 'addresses': list(addresses), 'start_block': 100,
              'end_block': 208, 'start_time': BASE + first * DAY,
              'end_time': BASE + last * DAY, 'complete': True, 'normalization_gaps': [],
              'events_path': ep.relative_to(work).as_posix(),
              'events_sha256': hashlib.sha256(ep.read_bytes()).hexdigest()}
    cp = folder / (name + '.coverage.json')
    cp.write_text(json.dumps(record), encoding='utf8')
    return ep, cp


def fetch(provider, address=A, first=0, last=108):
    return provider.fetch_interval(address, NATIVE, 100, 208,
        start_time=BASE + first * DAY, end_time=BASE + last * DAY,
        global_end_time=BASE + 108 * DAY)


class FullScanIndex:
    """Reference scan feeds every saved event into the unchanged predicate."""
    def __init__(self, events): self.events = events
    def get(self, address, default=()): return self.events


def scan_provider(work):
    value = acquisition.CachedIntervals(work)
    for record in value.records:
        record['events_by_address'] = FullScanIndex(record['events'])
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


class CacheMemoTests(unittest.TestCase):
    def workspace(self):
        return tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent)

    def test_shared_content_hashed_parsed_and_constructed_once(self):
        with self.workspace() as tmp:
            ep, _ = save(tmp, 'a', [event(1), event(2, C, D)])
            save(tmp, 'b', [], shared=ep, addresses=(B,))
            with patch.object(acquisition, 'sha', wraps=acquisition.sha) as hs, \
                 patch.object(acquisition, 'read', wraps=acquisition.read) as rd, \
                 patch.object(acquisition, 'Event', wraps=Event) as ctor:
                provider = acquisition.CachedIntervals(tmp)
            self.assertEqual(1, sum(Path(c.args[0]) == ep for c in hs.call_args_list))
            self.assertEqual(1, sum(Path(c.args[0]) == ep for c in rd.call_args_list))
            self.assertEqual(2, ctor.call_count)
            self.assertIs(provider.records[0]['events'], provider.records[1]['events'])
            self.assertEqual(2, len(provider.records))

    def test_shared_file_different_expected_hash_is_not_trusted(self):
        with self.workspace() as tmp:
            ep, _ = save(tmp, 'a', [event(1)])
            _, cp = save(tmp, 'b', [], shared=ep)
            data = json.loads(cp.read_text()); data['events_sha256'] = '0' * 64
            cp.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, 'Cached events changed'):
                acquisition.CachedIntervals(tmp)

    def test_self_transfer_only_once_original_order_preserved(self):
        with self.workspace() as tmp:
            events = [event(3, A, A), event(1), event(4, C, D), event(2, B, A)]
            save(tmp, 'a', events)
            provider = acquisition.CachedIntervals(tmp)
            self.assertEqual([events[i] for i in (0, 1, 3)], provider.records[0]['events_by_address'][A])
            self.assertEqual(canonical(asdict(fetch(scan_provider(tmp)))), canonical(asdict(fetch(provider))))

    def test_overlap_crossquery_provenance_pending_and_scope_equivalent(self):
        with self.workspace() as tmp:
            ep, _ = save(tmp, 'a', [event(1), event(2, A, C, 95)], last=90)
            save(tmp, 'b', [], shared=ep, first=80, last=100)
            indexed, scan = acquisition.CachedIntervals(tmp), scan_provider(tmp)
            scope = Scope('synthetic:later', 'synthetic', 100, 208, BASE, BASE + 108 * DAY, 6, None, 'REFERENCE_FULL')
            for p in (indexed, scan): p.bind_scope(scope)
            for address in (A, B, C):
                self.assertEqual(canonical(asdict(fetch(scan, address))), canonical(asdict(fetch(indexed, address))))
            self.assertEqual(scan.pending, indexed.pending)
            self.assertEqual(2, len(fetch(indexed).coverage[0]['verified_content_intervals']))

    def test_conflicting_shared_versions_keep_quarantine_and_empty_events(self):
        with self.workspace() as tmp:
            ep, _ = save(tmp, 'a', [event(1)])
            save(tmp, 'b', [], shared=ep)
            save(tmp, 'c', [replace(event(1), amount_raw=101)])
            indexed, scan = acquisition.CachedIntervals(tmp), scan_provider(tmp)
            before = copy.deepcopy(indexed.records[0]['events'])
            result = fetch(indexed)
            self.assertEqual(canonical(asdict(fetch(scan))), canonical(asdict(result)))
            self.assertFalse(result.complete); self.assertEqual([], result.events)
            self.assertTrue(result.fact_conflicts); self.assertTrue(result.quarantined_facts)
            self.assertEqual(before, indexed.records[0]['events'])
            self.assertEqual(canonical(asdict(result)), canonical(asdict(fetch(indexed))))

    def test_collector_day95_full_and90_candidate_outputs_equal(self):
        with self.workspace() as tmp:
            ep, _ = save(tmp, 'a', [event(2, A, B, 95), event(3, C, D, 100)])
            save(tmp, 'b', [], shared=ep)
            for mode, cap in (('REFERENCE_FULL', None), ('ARRIVAL_90D', 90 * DAY)):
                scope = Scope('synthetic:q', 'synthetic', 100, 208, BASE, BASE + 108 * DAY, 6, cap, mode)
                values = []
                for provider in (acquisition.CachedIntervals(tmp), scan_provider(tmp)):
                    out = asdict(Collector(provider, lambda a: {'kind': 'SERVICE' if a == B else 'UNKNOWN'}).run(scope, event(1, C, A, 0)))
                    # Wall-clock duration is observational, not candidate semantics.
                    out['metrics'].pop('provider_total_wall_seconds', None)
                    values.append(out)
                self.assertEqual(canonical(values[0]), canonical(values[1]))


if __name__ == '__main__': unittest.main()
