"""Synthetic new-window regression; no real identities, credentials or network."""
import dataclasses
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from collector import Collector, Event, FetchResult, Limits, NATIVE, Scope, strictly_after
from dune_observed_replay import parse_interval_sql
from provider_dune import DuneProvider, build_scope_interval_sql
from stage1d_window import IntervalContentCache, missing_rectangles, record_window_fallback

DAY = 86400
BASE = 1700000000
A, B, C, S, X = ['0x' + str(i) * 40 for i in range(1, 6)]


def event(i, sender, recipient, day, **kw):
    tx = '0x' + format(i, '064x')
    return Event('eip155:1:tx:' + tx + ':top', tx, sender, recipient,
                 NATIVE, 100, 100 + int(day * DAY), 0, BASE + int(day * DAY), **kw)


def scope(mode='REFERENCE_FULL', days=108, depth=6):
    return Scope.from_policy({'query_id': 'synthetic:q', 'name': 'synthetic',
        'start_block': 100, 'end_block': 100 + days * DAY,
        'start_time_utc': BASE, 'end_time_utc': BASE + days * DAY,
        'max_acquisition_depth': depth, 'window_mode': mode,
        'local_window_seconds': None if mode == 'REFERENCE_FULL' else 90 * DAY})


def labels(address):
    return {'kind': 'SERVICE', 'status': 'RESOLVED'} if address == S else {'kind': 'UNKNOWN'}


class Provider:
    def __init__(self, events=(), complete=True):
        self.events, self.complete, self.requests = list(events), complete, []
    def fetch_interval(self, address, asset, start_block, end_block, *, start_time, end_time, global_end_time):
        request = dict(address=address, asset=asset, start_block=start_block, end_block=end_block,
                       start_time=start_time, end_time=min(end_time, global_end_time))
        self.requests.append(request)
        events = [e for e in self.events if address in (e.sender, e.recipient)
                  and start_block <= e.block <= end_block and start_time <= e.timestamp <= request['end_time']]
        return FetchResult(events, [request | {'complete': self.complete}], self.complete, real_requests=1)


class WindowTests(unittest.TestCase):
    def test_day95_only_full(self):
        seed = event(1, X, A, 0)
        outgoing = event(2, A, S, 95)
        full = Collector(Provider([outgoing]), labels).run(scope(), seed)
        reduced = Collector(Provider([outgoing]), labels).run(scope('ARRIVAL_90D'), seed)
        self.assertEqual([seed.event_id, outgoing.event_id], [e['event_id'] for e in full.candidate_events])
        self.assertEqual([seed.event_id], [e['event_id'] for e in reduced.candidate_events])
        self.assertEqual('REFERENCE_FULL', full.metrics['window_mode'])

    def test_day20_arrival_to107_is_not_seed_plus90(self):
        seed, middle, terminal = event(1, X, A, 0), event(2, A, B, 20), event(3, B, S, 107)
        result = Collector(Provider([middle, terminal]), labels).run(scope('ARRIVAL_90D'), seed)
        self.assertEqual(3, len(result.candidate_events))
        self.assertEqual(BASE + 108 * DAY, [s['state']['local_end'] for s in result.states if s['state']['address'] == B][0])

    def test_short_equivalent_content_but_distinct_scopes(self):
        seed, terminal = event(1, X, A, 0), event(2, A, S, 10)
        scopes = [scope(m, days=17) for m in ('REFERENCE_FULL', 'ARRIVAL_90D')]
        providers = [Provider([terminal]), Provider([terminal])]
        results = [Collector(p, labels).run(s, seed) for p, s in zip(providers, scopes)]
        self.assertEqual(results[0].candidate_events, results[1].candidate_events)
        self.assertEqual(providers[0].requests, providers[1].requests)
        self.assertNotEqual(scopes[0].scope_id, scopes[1].scope_id)
        self.assertNotEqual(scopes[0].scope_hash, scopes[1].scope_hash)
        sqls = [build_scope_interval_sql(s, A, NATIVE, 100, start_time=BASE) for s in scopes]
        self.assertEqual(sqls[0], sqls[1])

    def test_explicit_none_is_unlimited(self):
        self.assertIsNone(scope().local_window_seconds)
        self.assertEqual(BASE + 108 * DAY, scope().local_end(BASE))

    def test_legacy_freeze_and_hash_remain_old(self):
        old = dict(query_id='old', name='old', start_block=1, end_block=9,
                   start_time=1, end_time=9, max_depth=2, local_window_seconds=90 * DAY)
        policy = dict(query_id='old', name='old', start_block=1, end_block=9,
                      start_time_utc=1, end_time_utc=9, max_acquisition_depth=2,
                      local_window_seconds=None)
        parsed = Scope.from_policy(policy)
        self.assertEqual(old, parsed.freeze_dict())
        self.assertEqual(hashlib.sha256(json.dumps(old, sort_keys=True).encode()).hexdigest(), parsed.scope_hash)

    def test_unresolved_blocks_fail_before_collection(self):
        with self.assertRaises(ValueError):
            dataclasses.replace(scope(), end_block=None)

    def test_wrong_explicit_modes_rejected(self):
        with self.assertRaises(ValueError):
            dataclasses.replace(scope(), window_mode='WHATEVER')
        with self.assertRaises(ValueError):
            dataclasses.replace(scope(), window_mode='ARRIVAL_90D', local_window_seconds=None)

    def test_primary_package_fields_are_resolved(self):
        value = scope().freeze_dict()
        policy = {k: value[k] for k in ('query_id', 'name', 'start_block', 'end_block')}
        policy.update(start_time_utc=BASE, end_time_utc=BASE + 108 * DAY,
                      max_acquisition_depth=6, primary_window_mode='REFERENCE_FULL',
                      primary_local_window_seconds=None)
        self.assertEqual(scope().freeze_dict(), Scope.from_policy(policy).freeze_dict())

    def test_cross_day_sql_dates_blocks_and_replay_match(self):
        s = scope()
        sql = build_scope_interval_sql(s, A, NATIVE, 100, start_time=BASE)
        recovered = parse_interval_sql(sql)
        self.assertEqual(BASE + 108 * DAY, recovered['end_time'])
        self.assertEqual(s.end_block, recovered['end_block'])
        self.assertEqual(2, sql.count('block_date BETWEEN'))
        with self.assertRaises(ValueError):
            parse_interval_sql(sql.replace("DATE '2024-03-01'", "DATE '2024-02-29'"))

    def test_sql_rejects_scope_escape(self):
        with self.assertRaises(ValueError):
            build_scope_interval_sql(scope(), A, NATIVE, 100, start_time=BASE,
                                     end_time=BASE + 109 * DAY)

    def test_hash_is_not_execution_order(self):
        seed = event(9, X, A, 0)
        after = event(1, A, S, 1)
        self.assertTrue(strictly_after(after, seed))
        result = Collector(Provider([after]), labels).run(scope(), seed)
        self.assertEqual(2, len(result.candidate_events))

    def test_reverse_time_not_allowed(self):
        seed = event(1, X, A, 1)
        after = dataclasses.replace(event(2, A, S, 2), timestamp=BASE)
        result = Collector(Provider([after]), labels).run(scope(), seed)
        self.assertEqual(1, len(result.candidate_events))

    def test_zero_hop_service_and_unknown(self):
        seed = event(1, X, S, 0)
        p = Provider()
        result = Collector(p, labels).run(scope(depth=0, days=0), seed)
        self.assertEqual('FIRST_IDENTIFIED_SERVICE', result.stops[0]['reason'])
        self.assertEqual([], p.requests)
        result = Collector(p, lambda _: {'kind': 'UNKNOWN'}).run(scope(depth=0, days=0), seed)
        self.assertEqual('DECLARED_DEPTH_BOUNDARY', result.stops[0]['reason'])
        self.assertEqual(0, result.metrics['service_address_count'])

    def test_mode_checkpoint_cannot_cross_reuse(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            path = Path(tmp) / 'checkpoint.json'
            seed = event(1, X, A, 0)
            Collector(Provider(), labels, checkpoint_path=path).run(scope(), seed)
            with self.assertRaises(ValueError):
                Collector(Provider(), labels, checkpoint_path=path).run(scope('ARRIVAL_90D'), seed)
            saved = json.loads(path.read_text())
            self.assertEqual(scope().scope_hash, saved['scope_hash'])

    def test_bound_dune_sql_and_saved_replay_use_full_end(self):
        calls = []
        def execute(sql, job):
            calls.append(parse_interval_sql(sql))
            return {'execution_id': 'SYNTHETIC_WINDOW_EXECUTION', 'state': 'QUERY_STATE_COMPLETED'}
        def export(*_):
            return {'execution_id': 'SYNTHETIC_WINDOW_EXECUTION', 'state': 'QUERY_STATE_COMPLETED',
                    'result': {'rows': [], 'metadata': {'row_count': 0, 'total_row_count': 0}}}
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            seed = event(1, X, A, 0)
            result = Collector(DuneProvider(execute, export, tmp), labels).run(scope(), seed)
            self.assertEqual('COMPLETED_WITHIN_DECLARED_SCOPE', result.status)
            self.assertEqual(scope().end_time, calls[0]['end_time'])
            manifests = list(Path(tmp).glob('*/scope_requests/*.json'))
            self.assertEqual(1, len(manifests))
            self.assertEqual(scope().scope_hash, json.loads(manifests[0].read_text())['scope_hash'])
            replay = Collector(DuneProvider(None, None, tmp, replay_only=True), labels).run(scope(), seed)
            self.assertEqual(result.candidate_events, replay.candidate_events)
            self.assertEqual(0, replay.metrics['real_requests'])

    def test_new_scope_restart_keeps_exhausted_clock(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            path = Path(tmp) / 'checkpoint.json'
            frozen = scope()
            path.write_text(json.dumps({'query_id': frozen.query_id, 'scope': frozen.freeze_dict(),
                                        'cumulative_online_seconds': 10800}))
            provider = Provider()
            result = Collector(provider, labels, Limits(max_online_seconds=10800), checkpoint_path=path).run(frozen, event(1, X, A, 0))
            self.assertEqual([], provider.requests)
            self.assertEqual('INCOMPLETE_RESOURCE_LIMIT', result.status)
            self.assertEqual(10800, result.metrics['cumulative_online_collection_seconds'])


class CacheTests(unittest.TestCase):
    def request(self, start=0, end=108):
        return dict(address=A, asset=NATIVE, start_block=100, end_block=100 + 108 * DAY,
                    start_time=BASE + start * DAY, end_time=BASE + end * DAY)

    def test_old90_is_partial_full(self):
        missing = missing_rectangles(self.request(), [self.request(end=90) | {'complete': True}])
        self.assertEqual(BASE + 90 * DAY + 1, missing[0]['start_time'])
        self.assertEqual(BASE + 108 * DAY, missing[0]['end_time'])

    def test_adjacent_crossday_complete_union(self):
        first = self.request(end=90) | {'complete': True}
        second = self.request(start=90) | {'complete': True}
        self.assertEqual([], missing_rectangles(self.request(), [first, second]))

    def test_one_second_hole_not_complete(self):
        first = self.request(end=90) | {'complete': True}
        second = self.request(start=90) | {'complete': True, 'start_time': BASE + 90 * DAY + 2}
        gap = missing_rectangles(self.request(), [first, second])
        self.assertEqual(1, len(gap))
        self.assertEqual(gap[0]['start_time'], gap[0]['end_time'])

    def test_block_hole_and_failed_empty_not_coverage(self):
        first = self.request() | {'complete': True, 'end_block': 1000}
        second = self.request() | {'complete': True, 'start_block': 1002}
        gap = missing_rectangles(self.request(), [first, second])
        self.assertEqual(1001, gap[0]['start_block'])
        self.assertEqual(1001, gap[0]['end_block'])
        self.assertEqual([self.request()], missing_rectangles(self.request(), [self.request() | {'complete': False}]))

    def test_only_missing_interval_fetched_then_offline_shared(self):
        late = event(2, A, S, 95)
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            provider = Provider([late])
            cache = IntervalContentCache(provider, tmp)
            cache.store(self.request(end=90), FetchResult([], complete=True))
            cache.bind_scope(scope())
            result = cache.fetch_interval(**self.request(), global_end_time=scope().end_time)
            self.assertTrue(result.complete)
            self.assertEqual(1, len(provider.requests))
            self.assertEqual(BASE + 90 * DAY + 1, provider.requests[0]['start_time'])
            offline = IntervalContentCache(None, tmp, replay_only=True)
            replay = offline.fetch_interval(**self.request(), global_end_time=scope().end_time)
            self.assertEqual(result.events, replay.events)
            self.assertTrue(replay.complete)
            self.assertEqual(0, replay.real_requests)

    def test_overlap_dedup_keeps_background(self):
        outgoing, background = event(2, A, S, 95), event(3, X, A, 94)
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            cache = IntervalContentCache(None, tmp, replay_only=True)
            cache.store(self.request(), FetchResult([outgoing, background], complete=True))
            cache.store(self.request(start=90), FetchResult([outgoing], complete=True))
            result = cache.fetch_interval(**self.request(), global_end_time=scope().end_time)
            self.assertTrue(result.complete)
            self.assertEqual(2, len(result.events))
            self.assertIn(background.event_id, {e.event_id for e in result.events})

    def test_physical_conflict_blocks_complete(self):
        outgoing = event(2, A, S, 95)
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            cache = IntervalContentCache(None, tmp, replay_only=True)
            cache.store(self.request(), FetchResult([outgoing], complete=True))
            cache.store(self.request(start=90), FetchResult([dataclasses.replace(outgoing, amount_raw=999)], complete=True))
            result = cache.fetch_interval(**self.request(), global_end_time=scope().end_time)
            self.assertFalse(result.complete)
            self.assertEqual([], result.events)
            self.assertTrue(result.fact_conflicts)

    def test_tampered_content_is_not_a_cache_hit(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            cache = IntervalContentCache(None, tmp, replay_only=True)
            digest = cache.store(self.request(), FetchResult([], complete=True))
            (Path(tmp) / (digest + '.json')).write_text('{}')
            with self.assertRaises(ValueError):
                cache.fetch_interval(**self.request(), global_end_time=scope().end_time)


class FallbackTests(unittest.TestCase):
    def args(self):
        resources = dict(budget_ledger_sha256='synthetic-ledger', attempt_state_sha256='synthetic-attempts',
                         online_seconds=100, raw_bytes=1234)
        return dict(authorized_query_id='synthetic:q', completed_intervals=[],
            pending_frontier=[{'address': A}], resource_evidence={'frontier_count': 800,
              'online_seconds': 100, 'remaining_risk': '2', 'estimated_remaining_cost': '20',
              'blocking_resource': 'DUNE_RISK', 'actual_full_attempts': 2, 'evidence_refs': ['synthetic-measurement']},
            attempted_recovery=['reuse successful pages', 'measured smaller partition'],
            shortened_intervals=[{'arrival_time': BASE, 'full_end': BASE + 108 * DAY,
                                  'reduced_end': BASE + 90 * DAY, 'uncollected_seconds_saved': 18 * DAY}],
            resources_before=resources, resources_after=dict(resources),
            retained_context_ids=['synthetic-background', 'synthetic-gas'])

    def test_record_before_switch_keeps_resource_and_full_status(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            record = record_window_fallback(Path(tmp) / 'decision.json', scope(), scope('ARRIVAL_90D'), **self.args())
            self.assertEqual('PARTIAL_FULL_SCOPE', record['full_status'])
            self.assertEqual('NOT_STARTED', record['reduced_status'])
            self.assertFalse(record['reset_resource_counters'])
            self.assertEqual(self.args()['retained_context_ids'], record['retained_context_ids'])

    def test_counter_reset_rejected(self):
        args = self.args(); args['resources_after']['online_seconds'] = 0
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp, self.assertRaises(ValueError):
            record_window_fallback(Path(tmp) / 'decision.json', scope(), scope('ARRIVAL_90D'), **args)

    def test_result_based_trigger_rejected(self):
        args = self.args(); args['resource_evidence']['positive_lower_bound'] = 0
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp, self.assertRaises(ValueError):
            record_window_fallback(Path(tmp) / 'decision.json', scope(), scope('ARRIVAL_90D'), **args)

    def test_single_timeout_rejected(self):
        args = self.args(); args['resource_evidence']['actual_full_attempts'] = 1
        args['attempted_recovery'] = ['single timeout']
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp, self.assertRaises(ValueError):
            record_window_fallback(Path(tmp) / 'decision.json', scope(), scope('ARRIVAL_90D'), **args)

    def test_no_effect_fallback_rejected(self):
        args = self.args(); args['shortened_intervals'][0]['uncollected_seconds_saved'] = 0
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp, self.assertRaises(ValueError):
            record_window_fallback(Path(tmp) / 'decision.json', scope(), scope('ARRIVAL_90D'), **args)


if __name__ == '__main__':
    unittest.main()
