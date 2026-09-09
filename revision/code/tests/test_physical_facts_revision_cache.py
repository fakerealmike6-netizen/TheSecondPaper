"""Synthetic invalidation and ownership counterexamples for registry caching."""
from dataclasses import asdict, replace
import itertools
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from collector import Collector, Event, FetchResult, NATIVE, Scope
from physical_facts import PhysicalFactRegistry, CONFLICT_STATUS

A, B, S, X = ['0x' + str(i) * 40 for i in range(1, 5)]


def event(i, sender=A, recipient=B, **kw):
    tx = '0x' + format(i, '064x')
    return Event('eip155:1:tx:' + tx + ':top', tx, sender, recipient,
                 NATIVE, 7, i, 0, i, **kw)


class RegistryRevisionCacheTests(unittest.TestCase):
    def test_exact_duplicate_preserves_revision_and_cached_group(self):
        registry = PhysicalFactRegistry()
        with patch.object(registry, '_build_view', wraps=registry._build_view) as build:
            first = registry.add(event(5), source='source:a')
            before = registry.snapshot()
            revision = registry.revision
            for _ in range(3):
                self.assertEqual(first, registry.add(event(5), source='source:a'))
                self.assertEqual(before, registry.snapshot())
                self.assertEqual(first, registry.get(first['event_id']))
            self.assertEqual(revision, registry.revision)
            self.assertEqual(1, build.call_count)

    def test_source_and_raw_only_versions_invalidate_and_remain_in_conflicts(self):
        registry = PhysicalFactRegistry()
        value = event(5)
        registry.add(value, source='a', raw={'marker': ['original']})
        revision = registry.revision
        registry.snapshot()
        registry.add(value, source='b', raw={'marker': ['original']})
        self.assertGreater(registry.revision, revision)
        self.assertEqual(['a', 'b'], json.loads(registry.get(value.event_id)['provenance']))
        revision = registry.revision
        registry.add(value, source='b', raw={'marker': ['second']})
        self.assertGreater(registry.revision, revision)
        registry.add(replace(value, amount_raw=8), source='c')
        snapshot = registry.snapshot()
        self.assertFalse(snapshot['events'])
        self.assertEqual(4, len(snapshot['quarantined_versions']))
        self.assertIn({'marker': ['second']}, [v['raw'] for v in snapshot['quarantined_versions']])

    def test_public_get_add_snapshot_do_not_mutate_internal_views(self):
        registry = PhysicalFactRegistry()
        value = event(5)
        returned = registry.add(value)
        returned['amount_raw'] = 100
        registry.get(value.event_id)['recipient'] = X
        good = registry.snapshot()
        good['events'][0]['amount_raw'] = 101
        good['events'].clear()
        self.assertEqual(7, registry.get(value.event_id)['amount_raw'])
        self.assertEqual(B, registry.snapshot()['events'][0]['recipient'])
        raw = {'nested': {'values': ['original']}}
        registry.add(replace(value, amount_raw=8), raw=raw)
        raw['nested']['values'].append('caller mutation')
        before = registry.snapshot()
        result = registry.snapshot(include_events=False)
        result['conflicts'][0]['fields']['amount_raw'].append(999)
        result['quarantined_versions'][0]['facts']['amount_raw'] = 999
        for version in result['quarantined_versions']:
            if 'nested' in version['raw']:
                version['raw']['nested']['values'].append('returned mutation')
        self.assertEqual(before, registry.snapshot())
        nested = next(v['raw'] for v in before['quarantined_versions'] if 'nested' in v['raw'])
        self.assertEqual(['original'], nested['nested']['values'])

    def test_internal_shared_views_are_immutable(self):
        registry = PhysicalFactRegistry()
        registry.add(event(5), raw={'nested': [1, {'value': 2}]})
        view = registry._view(next(iter(registry.groups.values())))
        with self.assertRaises(TypeError): view[0]['amount_raw'] = 100
        with self.assertRaises(TypeError): view[2][0]['raw']['nested'][1]['value'] = 100
        with self.assertRaises(AttributeError): view[2][0]['raw']['nested'].append(100)

    def test_alias_bridge_invalidates_both_group_views(self):
        registry = PhysicalFactRegistry()
        a, b = event(5), event(6)
        registry.add(a); registry.add(b)
        self.assertEqual(2, len(registry.snapshot()['events']))
        self.assertFalse(registry.snapshot(include_events=False)['conflicts'])
        revision = registry.revision
        registry.add(asdict(b) | {'event_id': a.event_id})
        self.assertGreater(registry.revision, revision)
        self.assertEqual(1, len(registry.groups))
        self.assertIsNone(registry.get(a.event_id)); self.assertIsNone(registry.get(b.event_id))
        full = registry.snapshot(); quick = registry.snapshot(include_events=False)
        self.assertEqual(full, quick)
        self.assertEqual(3, len(quick['quarantined_versions']))

    def test_enrichment_and_late_conflict_recompute_only_changed_group(self):
        registry = PhysicalFactRegistry()
        with patch.object(registry, '_build_view', wraps=registry._build_view) as build:
            a = replace(event(5), tx_index=None)
            registry.add(a); registry.add(event(6))
            registry.snapshot()
            registry.add(replace(a, tx_index=2, gas_used=2, gas_price=3, gas_raw=6))
            self.assertEqual(2, registry.get(a.event_id)['tx_index'])
            self.assertEqual(3, build.call_count)
            registry.add(replace(a, tx_index=3))
            quick = registry.snapshot(include_events=False)
            full = registry.snapshot()
            self.assertEqual(quick['conflicts'], full['conflicts'])
            self.assertEqual(quick['quarantined_versions'], full['quarantined_versions'])
            self.assertFalse(quick['events']); self.assertEqual(1, len(full['events']))
            self.assertEqual(4, build.call_count)

    def test_conflicts_and_provenance_remain_order_invariant_after_warm_reads(self):
        a = event(5, provenance='first')
        versions = [a, replace(a, provenance='second', tx_index=None),
                    replace(a, provenance='third', amount_raw=8)]
        outputs = []
        for order in itertools.permutations(versions):
            registry = PhysicalFactRegistry()
            for value in order:
                registry.add(value)
                registry.snapshot(include_events=False); registry.snapshot()
            outputs.append(json.dumps(registry.snapshot(), sort_keys=True))
        self.assertEqual(1, len(set(outputs)))

    def test_collector_uses_conflict_only_checks_without_losing_context(self):
        seed = event(1, X, A)
        values = [event(2, A, S), replace(event(3, A, B), amount_raw=0),
                  event(4, A, B, success=False, gas_raw=6, gas_used=2, gas_price=3)]
        class Provider:
            replay_only = True
            def fetch_interval(self, *args, **kwargs):
                return FetchResult(values, [{'complete': True}], True, cache_hits=1)
        original = PhysicalFactRegistry.snapshot
        calls = []
        def observed(registry, **kwargs):
            calls.append(kwargs)
            return original(registry, **kwargs)
        with patch.object(PhysicalFactRegistry, 'snapshot', observed):
            result = Collector(Provider(), lambda a: {'kind': 'SERVICE' if a == S else 'UNKNOWN'}).run(
                Scope('synthetic:q', 'synthetic', 1, 10, 1, 10, 3), seed)
        self.assertTrue(calls); self.assertTrue(all(c == {'include_events': False} for c in calls))
        self.assertEqual(2, result.metrics['candidate_event_count'])
        self.assertEqual({'ZERO_VALUE_CONTEXT', 'FAILED_VALUE_TRANSFER_GAS_CONTEXT'},
                         {e['context_reason'] for e in result.context_events})
        self.assertEqual(6, next(e['gas_raw'] for e in result.context_events if not e['success']))

    def test_late_conflict_still_revokes_prior_stop_and_coverage(self):
        seed = event(1, X, A)
        ab, ba, first = event(2, A, B), event(3, B, A), event(5, A, S)
        class Provider:
            replay_only = True
            def fetch_interval(self, address, asset, start_block, end_block, **kwargs):
                values = [ab, first] if address == A and start_block == 1 else [ba] if address == B else [replace(first, amount_raw=8)]
                return FetchResult(values, [{'complete': True}], True, cache_hits=1)
        result = Collector(Provider(), lambda a: {'kind': 'SERVICE' if a == S else 'UNKNOWN'}).run(
            Scope('synthetic:q', 'synthetic', 1, 10, 1, 10, 5), seed)
        self.assertEqual(CONFLICT_STATUS, result.status)
        self.assertFalse(result.candidate_events); self.assertFalse(result.stops)
        self.assertTrue(result.invalidated_evidence['stops'])
        self.assertFalse(any(c['complete'] for c in result.coverage))


if __name__ == '__main__': unittest.main()
