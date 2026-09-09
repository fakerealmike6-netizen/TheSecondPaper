"""Entirely synthetic guard fixture; no production paths/data."""
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
    return Event('eip155:1:tx:' + tx + ':top', tx, sender, recipient, asset, 80, block, 0, timestamp if timestamp is not None else 1000 + block, success=True, provenance='SYNTHETIC_CONTROLLED_REQUEST_GUARD')

class Fixture:

    def __init__(self, work):
        self.work = work
        self.q = {'query_id': 'synthetic:guard', 'name': 'txphish_src001', 'start_block': 1, 'end_block': 1000, 'start_time_utc': 1000, 'end_time_utc': 2000, 'max_acquisition_depth': 5, 'window_mode': 'QUERY_ARRIVAL_WINDOW_SECONDS_V1', 'local_window_seconds': 100}
        self.scope = Scope.from_policy(self.q)
        self.q.update(scope_id=self.scope.scope_id, scope_hash=self.scope.scope_hash)
        self.seed = event(1, S, A, 10)
        self.stopped = event(2, A, B, 11)
        self.a = asdict(State(self.scope.query_id, A, NATIVE, self.seed, 0, self.scope.local_end(self.seed)))
        self.b = asdict(State(self.scope.query_id, B, NATIVE, self.stopped, 1, self.scope.local_end(self.stopped)))
        self.policy = {'schema_version': POLICY_SCHEMA, 'authorization_id': AUTH, 'enabled': True, 'threshold_daily_strict_gt': 20, 'authority_source_sha256': AUTHORITY_SHA256, 'query_scope_hashes': {self.scope.query_id: self.scope.scope_hash, 'synthetic:q2': '2' * 64, 'synthetic:q3': '3' * 64, 'synthetic:q4': '4' * 64}}
        self.policy_ref = write(work, 'private/stage1d_roles/UNKNOWN_COST_POLICY.json', self.policy)
        initial = {'authorization_id': AUTH, 'authority_sha256': AUTHORITY_SHA256, 'query_snapshots': [{'query': self.q}], 'state_screen_rows': [{'state': self.a, 'scope_hash': self.scope.scope_hash, 'state_key': state_key(self.a, self.scope), 'new_cost_screen_required': False, 'initial_unfinished_arrival': False}]}
        self.initial_ref = write(work, 'private/synthetic_initial.json', initial)
        self.current = {'schema_version': REGISTRY_SCHEMA, 'policy_ref': self.policy_ref, 'initial_snapshot_ref': self.initial_ref, 'identity_checks': [], 'historical_codes': [], 'activity_observations': []}
        write(work, 'private/stage1d_roles/UNKNOWN_COST_CURRENT.json', self.current)
        self.registry = Registry(work)
        self.c = {'query_id': self.scope.query_id, 'states': [], 'stops': [], 'unresolved_frontier': [], 'gaps': [], 'candidate_events': [asdict(self.seed), asdict(self.stopped)], 'metrics': {'scope_hash': self.scope.scope_hash, 'scope_id': self.scope.scope_id, 'scope_freeze': self.scope.freeze_dict(), 'cost_boundary_resolver_sha256': self.registry.identity}, 'cost_boundary_policy': self.registry.policy_for_scope(self.scope), 'cost_boundary_decisions': []}
        self.add(self.a, decision=self.registry.resolve(self.a, self.scope, {'kind': 'UNKNOWN'}))
        self.add(self.b, 'STOP')

    def add(self, state, action='CONTINUE', *, identity=None, decision=None):
        identity = identity or {'kind': 'UNKNOWN'}
        if decision is None:
            decision = make_decision(state, self.scope, policy_sha256=self.policy_ref['sha256'], action=action, reason={'CONTINUE': 'CONTROLLED_ALLOWED', 'STOP': 'UNKNOWN_CODE_COST_BOUNDARY', 'PENDING': 'IDENTITY_CHECK_PENDING', 'BYPASS': 'CONTROLLED_PRIORITY'}[action], evidence_refs=[self.initial_ref], base_identity=identity)
        row = {'state': deepcopy(state), 'identity': deepcopy(identity), 'cost_boundary': decision}
        self.c['states'].append(row)
        self.c['cost_boundary_decisions'].append(decision)
        if decision['action'] in ('STOP', 'PENDING'):
            saved = dict(deepcopy(row), reason=decision['reason'])
            if decision['action'] == 'STOP':
                saved['entry_event_id'] = state['arrival']['event_id']
            self.c['stops' if decision['action'] == 'STOP' else 'unresolved_frontier'].append(saved)
        return row

    def rectangle(self, state, **changes):
        return dict(address=state['address'], asset=state['asset'], start_block=state['arrival']['block'], end_block=self.scope.end_block, start_time=state['arrival']['timestamp'], end_time=state['local_end'], **{}) | changes

    def save(self, needs=None):
        ref = write(self.work, 'derived/stage1d/queries/' + self.q['name'] + '/collection.json', self.c)
        return {'query_name': self.q['name'], 'query_id': self.scope.query_id, 'scope_id': self.scope.scope_id, 'scope_hash': self.scope.scope_hash, 'collection_path': ref['path'], 'collection_sha256': ref['sha256'], 'needed_ranges': needs if needs is not None else [self.rectangle(self.a)]}
