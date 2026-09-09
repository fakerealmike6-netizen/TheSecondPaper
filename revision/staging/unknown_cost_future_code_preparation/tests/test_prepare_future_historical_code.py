from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
import tempfile
import unittest

from collector import Event, Scope, State
import stage1d_unknown_cost_boundary as core
import prepare_future_historical_code as f

POLICY = 'f' * 64
A = '0x' + 'a' * 40
P = '0x' + 'b' * 40
HEADER = '0x' + 'c' * 64


def inputs():
    scopes = {name: Scope(name, name, 1, 100, 0, 864000, 9, 864000)
              for name in f.QUERY_NAMES}
    collections = {name: dict(query_id=name, states=[], cost_boundary_decisions=[],
                             cost_boundary_policy=dict(schema_version=core.POLICY_SCHEMA,
                                 authorization_id=core.AUTH, enabled=True, policy_sha256=POLICY))
                   for name in f.QUERY_NAMES}
    refs = {name: dict(path='synthetic/' + name + '.json', sha256='1' * 64)
            for name in f.QUERY_NAMES}
    return collections, scopes, refs


def add(data, query, *, number=1, block=10, address=A, action='PENDING', reason='TYPE_UNRESOLVED', **details):
    co, scopes, refs = data
    event = Event('event-' + str(number), '0x' + format(number, '064x'), P, address,
                  core.NATIVE, 10, block, 0, block, block_hash=HEADER)
    state = asdict(State(query, address, core.NATIVE, event, 1, scopes[query].local_end(event)))
    decision = core.make_decision(state, scopes[query], policy_sha256=POLICY,
                                 action=action, reason=reason, **details)
    co[query]['states'].append(dict(state=state, identity=dict(kind='UNKNOWN'), cost_boundary=decision))
    co[query]['cost_boundary_decisions'].append(decision)
    return state


class PreparationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = f.load_base(Path(__file__).resolve().parents[3])

    def run_derive(self, data, *, resolver=None, cache_state='NO_CURRENT_EXACT_REQUEST', search=None):
        self.resolved, self.looked_up = [], []
        def resolve(state):
            self.resolved.append(state['arrival']['event_id'])
            return resolver(state) if resolver else dict(kind='UNKNOWN', status='LOCAL_FROZEN')
        def lookup(request):
            self.looked_up.append(deepcopy(request))
            value = dict(request=request, logical_key=self.base.digest(request), state=cache_state,
                         existing_attempt_count=2, retry_reset=False)
            if request['method'] == 'eth_getBlockByNumber':
                value.update(state='SUCCESS_ENVELOPE_SHA_AND_RUNTIME_VALIDATED', result={'hash': HEADER},
                             artifact_ref=dict(path='synthetic/header.json', sha256='2' * 64))
            return value
        statuses = {r['state']['address']: dict(online_label_status='SUCCESS')
                    for co in data[0].values() for r in co['states']}
        return f.derive_current(*data, POLICY, resolve, lookup, statuses, 5,
                                base=self.base, core=core, search_status=search)

    def test_future_pending_added_prior_completed_not_reaudited_and_source_unchanged(self):
        data = inputs()
        add(data, 'txphish_src002', number=1, action='BYPASS', reason='INITIAL_STATE_ALREADY_COMPLETED')
        future = add(data, 'txphish_src002', number=2, block=11)
        original = deepcopy(data)
        result = self.run_derive(data)
        self.assertEqual(self.resolved, ['event-2'])
        self.assertEqual(result['current_pending_selected_states'], 1)
        group = result['code_groups'][0]
        self.assertEqual(group['group_key'], self.base.digest(['eip155:1', A, 11]))
        self.assertEqual(group['states'][0]['state_key'], core.state_key(future, data[1]['txphish_src002']))
        self.assertEqual(group['states'][0]['graph_arrival_block_hash'], HEADER)
        self.assertEqual(group['header_ref']['path'], 'synthetic/header.json')
        self.assertFalse(result['initial_snapshot_read'])
        self.assertFalse(result['old_completed_states_rechecked'])
        self.assertEqual(data, original)
        self.assertEqual(result['next_batch']['requests'][0]['request'],
                         dict(method='eth_getCode', params=[A, '0xb']))

    def test_failed_search_known_code_and_roles_stay_out_and_invalid_overlay_fails_closed(self):
        data = inputs()
        add(data, 'txphish_src002', number=1, reason='IDENTITY_CHECK_PENDING',
            identity_checks={'exact_search': {'status': 'ACCESS_BLOCKED'}})
        add(data, 'txphish_src002', number=2, reason='IDENTITY_CHECK_PENDING', code_status='CODE_PRESENT')
        hold = '0x' + 'd' * 40
        add(data, 'txphish_src001', number=3, address=hold)
        blocked = '0x' + 'e' * 40
        add(data, 'xscam_src001', number=4, address=blocked)
        def roles(s):
            return dict(kind='UNKNOWN', status='LOCAL_FROZEN', branch_action='USER_REQUESTED_BRANCH_HOLD')
        result = self.run_derive(data, resolver=roles, search={blocked: {'status': 'ACCESS_BLOCKED'}})
        self.assertEqual(self.resolved, ['event-3'])
        self.assertEqual(result['code_groups'], [])
        self.assertEqual(self.looked_up, [])
        self.assertEqual(set(r['reason'] for r in result['excluded_current_states']), {
            'FINITE_IDENTITY_FAILURE_ALREADY_RECORDED', 'CODE_ALREADY_ESTABLISHED_OTHER_IDENTITY_WORK_PENDING',
            'CURRENT_ROLE_OR_USER_BOUNDARY', 'CURRENT_FINITE_IDENTITY_FAILURE_ALREADY_RECORDED'})
        data[0]['txphish_src002']['states'][0]['cost_boundary']['scope_hash'] = '0' * 64
        with self.assertRaises(ValueError): self.run_derive(data)

    def test_shared_exact_block_priority_and_failed_cache_key_never_replaced(self):
        data = inputs()
        add(data, 'txphish_src001', number=1)
        add(data, 'txphish_src002', number=2)
        add(data, 'xscam_src001', number=3, block=11)
        result = self.run_derive(data, cache_state='RETRYABLE_FAILED')
        self.assertEqual(len(result['code_groups']), 2)
        shared = result['code_groups'][0]
        self.assertEqual([s['query_name'] for s in shared['states']], ['txphish_src002', 'txphish_src001'])
        self.assertEqual(shared['group_key'], self.base.digest(['eip155:1', A, 10]))
        self.assertEqual(shared['code_cache']['existing_attempt_count'], 2)
        self.assertFalse(shared['code_cache']['retry_reset'])
        self.assertEqual(result['next_batch']['requests'], [])
        self.assertEqual(sum(r['method'] == 'eth_getCode' for r in self.looked_up), 2)
        self.assertTrue(result['next_batch']['existing_failed_or_inflight_keys_not_replaced'])

    def test_role_binding_uses_corrected_registry_history_and_authority_contract(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temp:
            root = Path(temp); work = root/'code'
            def write(path, value):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(self.base.canonical(value))
                return self.base.sha(path)
            request = root/'operations/hold_request.json'
            request_sha = write(request, {'user_message': 'Hold the branch'})
            role = work/'private/stage1d_roles'
            write(role/'CURRENT.json', {'certificates': []})
            write(role/'AUTHORITIES.json', {'authorities': []})
            alias = work/'derived/stage1d/queries/txphish_src001/collection.json'
            write(alias, {'replayed': True})
            write(role/'USER_TASK_BOUNDARIES.json', {'boundaries': [{
                'pre_adoption_collections': [{'path': alias.relative_to(work).as_posix(), 'sha256': '0'*64}],
                'authority_request_ref': {'path': 'operations/hold_request.json', 'sha256': request_sha}}]})
            first = self.base.Inputs(root)
            self.assertEqual(f.bind_role_refs(work, first)['status'], 'EXISTING_VALIDATOR_PASSED')
            self.assertIn('operations/hold_request.json', first.refs)
            self.assertIn('code/private/stage1d_roles/USER_TASK_BOUNDARIES.json', first.refs)
            self.assertNotIn(alias.relative_to(root).as_posix(), first.refs)
            write(alias, {'replayed': 'next version'})
            second = self.base.Inputs(root); f.bind_role_refs(work, second)
            self.assertEqual(first.refs, second.refs)
            write(request, {'user_message': 'Changed'})
            with self.assertRaises(ValueError): f.bind_role_refs(work, self.base.Inputs(root))


if __name__ == '__main__': unittest.main()
