"""Synthetic filesystem and exact rectangle tests; no production or provider."""
from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from collector import Event, Scope, State
import stage1d_cost_request_guard as guard
from stage1d_unknown_cost_boundary import AUTH, AUTHORITY_SHA256, POLICY_SCHEMA, make_decision, state_key
from stage1d_unknown_cost_registry import Registry, SCHEMA as REGISTRY_SCHEMA

STAGE = Path(__file__).resolve().parents[1]
NATIVE = guard.NATIVE
A, B, S, T = ['0x' + x * 40 for x in '1234']


def write(work, rel, value):
    path = work / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False).encode()
    path.write_bytes(raw)
    return {'path': rel, 'sha256': hashlib.sha256(raw).hexdigest()}


def event(number, sender, recipient, block, timestamp=None, *, asset=NATIVE):
    tx = '0x' + format(number, '064x')
    return Event('eip155:1:tx:' + tx + ':top', tx, sender, recipient,
        asset, 80, block, 0, timestamp if timestamp is not None else 1000 + block,
        success=True, provenance='SYNTHETIC_CONTROLLED_REQUEST_GUARD')


class Fixture:
    def __init__(self, work):
        self.work = work
        self.q = {'query_id': 'synthetic:guard', 'name': 'synthetic_guard',
            'start_block': 1, 'end_block': 1000, 'start_time_utc': 1000,
            'end_time_utc': 2000, 'max_acquisition_depth': 5,
            'window_mode': 'QUERY_ARRIVAL_WINDOW_SECONDS_V1', 'local_window_seconds': 100}
        self.scope = Scope.from_policy(self.q)
        self.q.update(scope_id=self.scope.scope_id, scope_hash=self.scope.scope_hash)
        self.seed = event(1, S, A, 10)
        self.stopped = event(2, A, B, 11)
        self.a = asdict(State(self.scope.query_id, A, NATIVE, self.seed, 0, self.scope.local_end(self.seed)))
        self.b = asdict(State(self.scope.query_id, B, NATIVE, self.stopped, 1, self.scope.local_end(self.stopped)))
        self.policy = {'schema_version': POLICY_SCHEMA, 'authorization_id': AUTH, 'enabled': True,
            'threshold_daily_strict_gt': 20, 'authority_source_sha256': AUTHORITY_SHA256,
            'query_scope_hashes': {self.scope.query_id: self.scope.scope_hash,
                'synthetic:q2': '2' * 64, 'synthetic:q3': '3' * 64, 'synthetic:q4': '4' * 64}}
        self.policy_ref = write(work, 'private/stage1d_roles/UNKNOWN_COST_POLICY.json', self.policy)
        initial = {'authorization_id': AUTH, 'authority_sha256': AUTHORITY_SHA256,
            'query_snapshots': [{'query': self.q}], 'state_screen_rows': [
                {'state': self.a, 'scope_hash': self.scope.scope_hash,
                 'state_key': state_key(self.a, self.scope), 'new_cost_screen_required': False,
                 'initial_unfinished_arrival': False}]}
        self.initial_ref = write(work, 'private/synthetic_initial.json', initial)
        self.current = {'schema_version': REGISTRY_SCHEMA, 'policy_ref': self.policy_ref,
            'initial_snapshot_ref': self.initial_ref, 'identity_checks': [],
            'historical_codes': [], 'activity_observations': []}
        write(work, 'private/stage1d_roles/UNKNOWN_COST_CURRENT.json', self.current)
        self.registry = Registry(work)
        self.c = {'query_id': self.scope.query_id, 'states': [], 'stops': [], 'unresolved_frontier': [],
            'gaps': [], 'candidate_events': [asdict(self.seed), asdict(self.stopped)],
            'metrics': {'scope_hash': self.scope.scope_hash, 'scope_id': self.scope.scope_id,
                'scope_freeze': self.scope.freeze_dict(), 'cost_boundary_resolver_sha256': self.registry.identity},
            'cost_boundary_policy': self.registry.policy_for_scope(self.scope), 'cost_boundary_decisions': []}
        # Real Registry.resolve over the bound initial snapshot produces this
        # historical BYPASS; no synthetic evidence is passed off as code/identity.
        self.add(self.a, decision=self.registry.resolve(self.a, self.scope, {'kind': 'UNKNOWN'}))
        self.add(self.b, 'STOP')

    def add(self, state, action='CONTINUE', *, identity=None, decision=None):
        identity = identity or {'kind': 'UNKNOWN'}
        if decision is None:
            decision = make_decision(state, self.scope, policy_sha256=self.policy_ref['sha256'],
                action=action, reason={'CONTINUE': 'CONTROLLED_ALLOWED', 'STOP': 'UNKNOWN_CODE_COST_BOUNDARY',
                    'PENDING': 'IDENTITY_CHECK_PENDING', 'BYPASS': 'CONTROLLED_PRIORITY'}[action],
                evidence_refs=[self.initial_ref], base_identity=identity)
        row = {'state': deepcopy(state), 'identity': deepcopy(identity), 'cost_boundary': decision}
        self.c['states'].append(row); self.c['cost_boundary_decisions'].append(decision)
        if decision['action'] in ('STOP', 'PENDING'):
            saved = dict(deepcopy(row), reason=decision['reason'])
            if decision['action'] == 'STOP': saved['entry_event_id'] = state['arrival']['event_id']
            self.c['stops' if decision['action'] == 'STOP' else 'unresolved_frontier'].append(saved)
        return row

    def rectangle(self, state, **changes):
        return dict(address=state['address'], asset=state['asset'], start_block=state['arrival']['block'],
            end_block=self.scope.end_block, start_time=state['arrival']['timestamp'],
            end_time=state['local_end'], **{}) | changes

    def save(self, needs=None):
        ref = write(self.work, 'derived/stage1d/queries/' + self.q['name'] + '/collection.json', self.c)
        return {'query_name': self.q['name'], 'query_id': self.scope.query_id,
            'scope_id': self.scope.scope_id, 'scope_hash': self.scope.scope_hash,
            'collection_path': ref['path'], 'collection_sha256': ref['sha256'],
            'needed_ranges': needs if needs is not None else [self.rectangle(self.a)]}


class CostRequestGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='synthetic_guard_', dir=STAGE)
        self.work = Path(self.temp.name) / 'code'; self.work.mkdir()
        self.f = Fixture(self.work)

    def tearDown(self):
        self.temp.cleanup()

    def check(self, entry):
        return guard.validate_candidate_entry(self.work, self.f.q, entry)

    def test_actual_registry_historical_bypass_allows_no_writes(self):
        entry = self.f.save()
        before = {str(p.relative_to(self.work)): p.read_bytes() for p in self.work.rglob('*') if p.is_file()}
        result = self.check(entry)
        self.assertEqual(result['status'], 'CURRENT_COST_STATE_REQUEST_DOMAIN_VERIFIED')
        self.assertEqual(result['registry_identity_sha256'], self.f.registry.identity)
        self.assertEqual(result['allowed_state_count'], 1)
        self.assertFalse(result['provider_coverage_claimed'])
        self.assertEqual(before, {str(p.relative_to(self.work)): p.read_bytes() for p in self.work.rglob('*') if p.is_file()})

    def test_stop_or_pending_only_range_rejected(self):
        for action in ('STOP', 'PENDING'):
            with self.subTest(action=action):
                f = self.f; f.c['states'] = f.c['states'][:1]
                f.c['cost_boundary_decisions'] = f.c['cost_boundary_decisions'][:1]
                f.c['stops'] = []; f.c['unresolved_frontier'] = []
                f.add(f.b, action)
                with self.assertRaisesRegex(ValueError, 'arrival union'):
                    self.check(f.save([f.rectangle(f.b)]))

    def test_another_legal_arrival_allows_only_its_intersection(self):
        f = self.f; incoming = event(3, A, B, 50)
        state = asdict(State(f.scope.query_id, B, NATIVE, incoming, 2, f.scope.local_end(incoming)))
        f.add(state)
        self.check(f.save([f.rectangle(state)]))
        with self.assertRaisesRegex(ValueError, 'arrival union'):
            self.check(f.save([f.rectangle(f.b)]))

    def test_union_is_exact_not_bounding_envelope(self):
        f = self.f
        first, second = event(3, A, B, 20, 1040), event(4, A, B, 60, 1100)
        s1 = asdict(State(f.scope.query_id, B, NATIVE, first, 2, f.scope.local_end(first)))
        s2 = asdict(State(f.scope.query_id, B, NATIVE, second, 2, f.scope.local_end(second)))
        f.add(s1); f.add(s2)
        # At blocks >=60, the two adjacent/overlapping time slices form an exact union.
        self.check(f.save([f.rectangle(s1, start_block=60, end_time=s2['local_end'])]))
        # Earlier blocks after the first state's local end remain a real hole.
        with self.assertRaisesRegex(ValueError, 'arrival union'):
            self.check(f.save([f.rectangle(s1, end_time=s2['local_end'])]))

    def test_pending_only_api_checks_all_lists_and_exact_fields(self):
        f = self.f; entry = f.save(); entry['pending_intervals'] = entry.pop('needed_ranges')
        self.check(entry)
        entry['needed_ranges'] = [f.rectangle(f.b)]
        with self.assertRaises(ValueError): self.check(entry)
        for changed in ({'start_block': True}, {'end_time': 2001}, {'asset': guard.WETH},
                        {'query_id': 'other'}, {'chain_id': 'eip155:2'}, {'direction': 'INCOMING'}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                self.check(f.save([f.rectangle(f.a, **changed)]))
        entry = f.save(); entry['pending_intervals'] = 1
        with self.assertRaisesRegex(ValueError, 'must be a list'): self.check(entry)

    def test_bypass_cannot_override_existing_hold_depth_or_protocol(self):
        f = self.f
        for identity, depth, context in (({'kind': 'UNKNOWN', 'branch_action': 'USER_REQUESTED_BRANCH_HOLD'}, 1, 'ordinary'),
                ({'kind': 'SERVICE'}, 1, 'ordinary'), ({'kind': 'UNKNOWN'}, 5, 'ordinary'),
                ({'kind': 'UNKNOWN'}, 1, 'component')):
            with self.subTest(identity=identity, depth=depth, context=context):
                f.c['states'] = f.c['states'][:1]; f.c['cost_boundary_decisions'] = f.c['cost_boundary_decisions'][:1]
                f.c['stops'] = []; f.c['unresolved_frontier'] = []
                state = dict(f.b, depth=depth, protocol_context=context)
                row = f.add(state, 'BYPASS', identity=identity)
                if identity.get('branch_action'):
                    f.c['stops'].append(dict(deepcopy(row), reason='USER_REQUESTED_BRANCH_HOLD'))
                with self.assertRaisesRegex(ValueError, 'arrival union'):
                    self.check(f.save([f.rectangle(state)]))

    def test_stale_collection_policy_registry_and_label_version_rejected(self):
        f = self.f; entry = f.save()
        f.c['extra'] = 'changed'; f.save()
        with self.assertRaisesRegex(ValueError, 'stale'): self.check(entry)
        entry = f.save(); f.c['cost_boundary_policy']['enabled'] = False
        with self.assertRaisesRegex(ValueError, 'policy metadata'): self.check(f.save())
        f.c['cost_boundary_policy'] = f.registry.policy_for_scope(f.scope)
        f.c['metrics']['cost_boundary_resolver_sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'registry identity'): self.check(f.save())
        f.c['metrics']['cost_boundary_resolver_sha256'] = f.registry.identity
        entry = f.save()
        write(self.work, 'derived/stage1d/labels/new.json', {'rows': []})
        with self.assertRaisesRegex(ValueError, 'registry identity'): self.check(entry)

    def test_cross_table_or_decision_hash_tampering_rejected(self):
        f = self.f; f.c['stops'][0]['reason'] = 'TYPE_UNRESOLVED'
        with self.assertRaises(ValueError): self.check(f.save())
        f.c['stops'][0]['reason'] = 'UNKNOWN_CODE_COST_BOUNDARY'
        f.c['states'][0]['cost_boundary']['action'] = 'CONTINUE'
        with self.assertRaises(ValueError): self.check(f.save())

    def test_old_scope_unchanged_but_removing_adopted_registry_rejected(self):
        f = self.f; entry = f.save()
        for name in ('UNKNOWN_COST_CURRENT.json', 'UNKNOWN_COST_POLICY.json'):
            (self.work / 'private/stage1d_roles' / name).unlink()
        with self.assertRaisesRegex(ValueError, 'now-missing'): self.check(entry)
        f.c.pop('cost_boundary_policy'); f.c.pop('cost_boundary_decisions')
        for row in f.c['states']: row.pop('cost_boundary')
        f.save()
        self.assertEqual(self.check({'old_entry': True})['status'], 'NOT_APPLICABLE_UNCHANGED')
        old_query = {k: v for k, v in f.q.items() if k not in ('scope_hash', 'scope_id')}
        self.assertEqual(guard.validate_candidate_entry(self.work, old_query, {})['status'], 'NOT_APPLICABLE_UNCHANGED')

    def test_only_current_alias_scope_and_atomic_input_stability(self):
        f = self.f; entry = f.save()
        with self.assertRaisesRegex(ValueError, 'current collection alias'):
            self.check(dict(entry, collection_path='private/old_collection.json'))
        f.c['metrics']['scope_freeze']['local_window_seconds'] = 90
        with self.assertRaisesRegex(ValueError, 'frozen scope'): self.check(f.save())
        f.c['metrics']['scope_freeze'] = f.scope.freeze_dict(); entry = f.save()
        original = guard.missing_rectangles
        def mutate(request, completed):
            result = original(request, completed)
            write(self.work, 'derived/stage1d/labels/race.json', {'rows': []})
            return result
        with patch.object(guard, 'missing_rectangles', mutate), self.assertRaisesRegex(ValueError, 'changed during'):
            self.check(entry)

    def test_registry_source_tamper_and_current_collection_race(self):
        f = self.f; entry = f.save()
        source = self.work / f.initial_ref['path']; original_bytes = source.read_bytes()
        source.write_bytes(original_bytes + b' ')
        with self.assertRaisesRegex(ValueError, 'original bytes changed'): self.check(entry)
        source.write_bytes(original_bytes)
        original = guard.missing_rectangles
        def mutate(request, completed):
            result = original(request, completed)
            current = self.work / entry['collection_path']
            current.write_bytes(current.read_bytes() + b' ')
            return result
        with patch.object(guard, 'missing_rectangles', mutate), self.assertRaisesRegex(ValueError, 'collection changed during'):
            self.check(entry)

    def test_declared_decision_child_raw_sha_rechecked_without_provider_parse(self):
        f = self.f
        child = write(self.work, 'private/synthetic_page_raw.json', {'controlled': 'original'})
        first = f.c['states'][0]
        d = make_decision(first['state'], f.scope, policy_sha256=f.policy_ref['sha256'],
            action='BYPASS', reason='CONTROLLED_BOUND_CHILD', evidence_refs=[child])
        first['cost_boundary'] = d; f.c['cost_boundary_decisions'][0] = d
        entry = f.save(); self.check(entry)
        (self.work / child['path']).write_bytes(b'changed original')
        with self.assertRaisesRegex(ValueError, 'original bytes changed'): self.check(entry)

    def test_bq_discovery_shapes_guard_but_plain_context_unchanged(self):
        f = self.f; f.save()
        doc = {'query_id': f.q['query_id'], 'scope_hash': f.q['scope_hash'], 'need_rectangles': [f.rectangle(f.a)]}
        for extra in ({}, {'schema_version': 'stage1d-batch-binding-preparation-v1'},
            {'schema_version': 'stage1d-bigquery-dryrun-spec-v1', 'batch_binding_sql_version': 'stage1d-batch-binding-route-v1'}):
            with self.subTest(extra=extra):
                self.assertEqual(guard.validate_bq_candidate_request(self.work, f.q, doc | extra)['allowed_state_count'], 1)
        for extra in ({'batch_binding_sql_version': 'invented'}, {'schema_version': 'stage1d-classic-context-preparation-v1'},
                      {'schema_version': 'stage1d-bigquery-dryrun-spec-v1'}, {'need_rectangles': [f.rectangle(f.b)]}):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                guard.validate_bq_candidate_request(self.work, f.q, doc | extra)
        for schema in ('stage1d-classic-context-preparation-v1', 'stage1d-bigquery-dryrun-spec-v1'):
            result = guard.validate_bq_candidate_request(self.work, f.q, {'schema_version': schema, 'needed_ranges': []})
            self.assertEqual(result['status'], 'NOT_BATCH_DISCOVERY_UNCHANGED')

    def test_legacy_bq_discovery_without_policy_or_current_collection_is_unchanged(self):
        f = self.f
        doc = {'query_id': f.q['query_id'], 'scope_hash': f.q['scope_hash'], 'need_rectangles': [f.rectangle(f.a)]}
        with self.assertRaises(ValueError): guard.validate_bq_candidate_request(self.work, f.q, doc)
        for name in ('UNKNOWN_COST_CURRENT.json', 'UNKNOWN_COST_POLICY.json'):
            (self.work / 'private/stage1d_roles' / name).unlink()
        result = guard.validate_bq_candidate_request(self.work, f.q, doc)
        self.assertEqual(result['status'], 'NOT_APPLICABLE_UNCHANGED')


if __name__ == '__main__':
    unittest.main()
