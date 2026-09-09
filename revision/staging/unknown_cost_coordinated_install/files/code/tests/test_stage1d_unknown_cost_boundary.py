import copy
import hashlib
import json
import unittest
from dataclasses import asdict, replace

from collector import Event, Scope, State
import stage1d_unknown_cost_boundary as c

A = '0x' + 'a' * 40
B = '0x' + 'b' * 40
P = '0x' + 'c' * 40
POLICY_SHA = 'f' * 64


def fixture(days=10, end=None, asset=c.NATIVE, query='q1', max_depth=4):
    q = Scope(query, query, 1, 10000, 0, days * 86400 if end is None else end,
              max_depth, days * 86400)
    arrival = Event('arrival', '0x' + '0' * 64, P, A, asset, 1000, 1, 0, 0)
    s = State(query, A, asset, arrival, 0, q.local_end(arrival))
    return s, q


def events(n, asset=c.NATIVE):
    return [asdict(Event('out' + str(i), '0x' + format(i + 1, '064x'), A, B,
                         asset, 1, i + 2, 0, i + 1,
                         kind='erc20' if asset == c.WETH else 'top',
                         log_index=i if asset == c.WETH else None)) for i in range(n)]


def source(value, path='synthetic/original.json'):
    raw = json.dumps(value, sort_keys=True).encode()
    ref = dict(path=path, sha256=hashlib.sha256(raw).hexdigest())
    return ref, c.verify_source_bytes([(ref, raw)])


def bound_inputs(s, q, code='0x', exact='EXACT_SEARCH_CHECKED', n=0):
    rows = events(n, s.asset)
    ref, sources = source(dict(events=rows, result=code))
    channels = {k: dict(status=status, evidence_refs=[ref] if status != 'UNQUERIED' else [])
                for k, status in [('local', 'LOCAL_CHECKED'), ('online', 'ONLINE_CHECKED_NO_ADOPTABLE_ROLE'), ('exact_search', exact)]}
    ident = c.bind_identity(s, q, channels, sources=sources, evidence_refs=[ref])
    codeproof = c.classify_code(s, q, {'method': 'eth_getCode', 'params': [A, hex(s.arrival.block)]},
                               {'result': code}, sources=sources, evidence_refs=[ref])
    act = c.activity(s, q, rows, sources=sources, evidence_refs=[ref])
    return ident, codeproof, act


class CoreTests(unittest.TestCase):
    def test_exact_rates_equality_truncation_and_short_window(self):
        for end, n, expected in [(864000, 200, False), (864000, 201, True),
                                 (172800, 40, False), (172800, 41, True), (3600, 1, True)]:
            with self.subTest(end=end, n=n):
                s, q = fixture(end=end)
                a = c.activity(s, q, events(n))
                self.assertEqual(a['D_seconds'], end)
                self.assertEqual(a['N_obs'], n)
                self.assertEqual(a['strict_gt20_observed'], expected)
                self.assertEqual(a['rate_truth'], 'TRUE' if expected else 'UNKNOWN')
                self.assertFalse(a['full_coverage'])
                self.assertEqual(a['short_window_rate_unstable'], end < 86400)

    def test_zero_duration_no_infinite_rate(self):
        s, q = fixture(end=0)
        a = c.activity(s, q, [])
        self.assertEqual(a['D_seconds'], 0)
        self.assertIsNone(a['observed_rate_denominator'])
        self.assertEqual(a['rate_truth'], 'UNAVAILABLE')
        self.assertFalse(a['strict_gt20_observed'])

    def test_tx_dedup_multiple_logs_sources_and_real_sender(self):
        s, q = fixture(asset=c.WETH)
        rows = events(1, c.WETH)
        extra = dict(rows[0], event_id='second_log', log_index=17, recipient=P)
        duplicate = dict(extra, provenance='second source')
        a = c.activity(s, q, rows + [extra, duplicate])
        self.assertEqual(a['N_obs'], 1)
        self.assertEqual(a['physical_event_count'], 2)
        self.assertEqual(a['unique_counterpart_count'], 2)

    def test_same_second_order_and_unknown_order(self):
        s, q = fixture(asset=c.WETH)
        arrival = replace(s.arrival, kind='erc20', log_index=5)
        s = replace(s, arrival=arrival)
        good = dict(events(1, c.WETH)[0], tx_hash=arrival.tx_hash, block=1,
                    tx_index=0, timestamp=0, log_index=6)
        unknown = dict(events(1, c.WETH)[0], event_id='unknown', block=1,
                       tx_index=None, timestamp=0)
        a = c.activity(s, q, [good, unknown])
        self.assertEqual(a['N_obs'], 1)
        self.assertEqual(a['excluded_counts']['ORDER_UNKNOWN'], 1)

    def test_failed_zero_self_gas_virtual_malformed_excluded(self):
        s, q = fixture()
        rows = events(9)
        rows[0]['success'] = False
        rows[1]['amount_raw'] = 0
        rows[2]['recipient'] = A
        rows[3]['kind'] = 'gas'
        rows[4]['semantic_virtual'] = True
        rows[5]['amount_raw'] = None
        rows[6]['success'] = '0x1'
        rows[7]['ancestor_success'] = False
        self.assertEqual(c.activity(s, q, rows)['N_obs'], 1)

    def test_position_missing_and_foreign_assets_do_not_count(self):
        s, q = fixture()
        rows = events(4)
        rows[0].update(kind='internal', trace_address=None)
        rows[1].update(kind='internal', trace_address='')
        rows[2].update(kind='erc20', asset=c.WETH, log_index=1)
        self.assertEqual(c.activity(s, q, rows)['N_obs'], 1)

    def test_conflict_isolates_fact_not_other_reliable_transactions(self):
        s, q = fixture()
        rows = events(2)
        conflict = dict(rows[0], amount_raw=2)
        a = c.activity(s, q, rows + [conflict])
        self.assertEqual(a['N_obs'], 1)
        self.assertEqual(len(a['conflicts']), 1)
        bad_arrival = dict(asdict(s.arrival), amount_raw=1)
        unavailable = c.activity(s, q, rows + [bad_arrival])
        self.assertIsNone(unavailable['N_obs'])

    def test_reverted_duplicate_is_not_discarded_before_reconciliation(self):
        s, q = fixture()
        row = events(1)[0]
        a = c.activity(s, q, [row, dict(row, ancestor_success=False)])
        self.assertEqual(a['N_obs'], 0)
        self.assertEqual(len(a['conflicts']), 1)

    def test_missing_log_locator_cannot_prove_physical_token_outflow(self):
        s, q = fixture(asset=c.WETH)
        row = dict(events(1, c.WETH)[0], log_index=None)
        self.assertEqual(c.activity(s, q, [row])['N_obs'], 0)

    def test_policy_body_is_exact_four_scope_strict20_and_version_bound(self):
        s, q = fixture()
        policy = dict(schema_version=c.POLICY_SCHEMA, authorization_id=c.AUTH,
                      enabled=True, threshold_daily_strict_gt=20,
                      authority_source_sha256=c.AUTHORITY_SHA256,
                      query_scope_hashes={q.query_id: q.scope_hash, 'q2': 'a' * 64,
                                          'q3': 'b' * 64, 'q4': 'c' * 64})
        self.assertTrue(c.validate_policy(policy, scope=q))
        for change in [dict(threshold_daily_strict_gt=20.0), dict(enabled=1),
                       dict(schema_version=c.VERSION), dict(authority_source_sha256='a' * 64)]:
            with self.assertRaises(ValueError):
                c.validate_policy(dict(policy, **change), scope=q)

    def test_state_keys_bind_query_asset_arrival_depth_window_scope(self):
        s, q = fixture()
        keys = {c.state_key(s, q)}
        s2, q2 = fixture(query='q2'); keys.add(c.state_key(s2, q2))
        s2, q2 = fixture(asset=c.WETH); keys.add(c.state_key(s2, q2))
        keys.add(c.state_key(replace(s, depth=1), q))
        keys.add(c.state_key(replace(s, arrival=replace(s.arrival, event_id='arrival2')), q))
        s2, q2 = fixture(days=9); keys.add(c.state_key(s2, q2))
        self.assertEqual(len(keys), 6)
        self.assertEqual(c.state_key(s, q), c.state_key(asdict(s), q.freeze_dict()))
        with self.assertRaises(ValueError):
            c.state_key(s, q2)

    def test_previous_observation_survives_candidate_projection(self):
        s, q = fixture(end=3600)
        old = c.activity(s, q, events(1))
        self.assertEqual(c.activity(s, q, [], previous=old)['N_obs'], 1)
        with self.assertRaises(ValueError):
            c.activity(replace(s, depth=1), q, [], previous=old)

    def test_even_hex_historical_type_and_failure(self):
        self.assertEqual(c.code_status('0x'), 'NO_RUNTIME_CODE_AT_BLOCK')
        self.assertEqual(c.code_status('0x00'), 'CODE_PRESENT')
        for value in [None, '0x0', '', '0xgg', True]:
            self.assertEqual(c.code_status(value), 'TYPE_UNRESOLVED')
        s, q = fixture()
        for block in ['latest', '0x2']:
            p = c.classify_code(s, q, {'method': 'eth_getCode', 'params': [A, block]}, {'result': '0x'})
            self.assertEqual(p['status'], 'TYPE_UNRESOLVED')
        p = c.classify_code(s, q, {'method': 'eth_getCode', 'params': [A, '0x1']}, {'error': {'code': -1}})
        self.assertEqual(p['status'], 'TYPE_UNRESOLVED')

    def test_decisions_unknown_code_low_and_no_code_rates(self):
        s, q = fixture()
        original = dict(kind='UNKNOWN', label='DEX Trader', other={'x': 2})
        for bytecode, n, action in [('0x00', 0, 'STOP'), ('0x', 200, 'CONTINUE'), ('0x', 201, 'STOP')]:
            identity, code, act = bound_inputs(s, q, bytecode, n=n)
            d = c.decide(s, q, original, policy_sha256=POLICY_SHA, identity=identity,
                         code=code, observed_activity=act)
            self.assertEqual(d['action'], action)
            self.assertEqual(d['base_identity'], original)
            self.assertNotIn('branch_action', original)
            self.assertTrue(c.validate_decision(asdict(s), q, d, POLICY_SHA))

    def test_exact_search_only_required_to_confirm_stop(self):
        s, q = fixture()
        for code, n, action in [('0x', 2, 'CONTINUE'), ('0x', 201, 'PENDING'), ('0x00', 0, 'PENDING')]:
            i, p, a = bound_inputs(s, q, code, exact='UNQUERIED', n=n)
            d = c.decide(s, q, {'kind': 'UNKNOWN'}, policy_sha256=POLICY_SHA,
                         identity=i, code=p, observed_activity=a)
            self.assertEqual(d['action'], action)

    def test_missing_failed_or_unverified_evidence_never_coststop(self):
        s, q = fixture()
        self.assertEqual(c.decide(s, q, {'kind': 'UNKNOWN'}, policy_sha256=POLICY_SHA,
                                identity={'checked': True})['action'], 'PENDING')
        i, p, a = bound_inputs(s, q, '0x0', n=201)
        self.assertEqual(c.decide(s, q, {'kind': 'UNKNOWN'}, policy_sha256=POLICY_SHA,
                                identity=i, code=p, observed_activity=a)['reason'], 'TYPE_UNRESOLVED')
        with self.assertRaises(ValueError):
            c.verify_source_bytes([({'path': 'x', 'sha256': 'a' * 64}, b'wrong')])

    def test_existing_boundaries_and_support_have_priority(self):
        s, q = fixture()
        for role in [{'kind': 'UNKNOWN', 'branch_action': 'USER_REQUESTED_BRANCH_HOLD'},
                     {'kind': 'SERVICE'}, {'kind': 'BRIDGE'}, {'kind': 'MIXER'},
                     {'kind': 'UNSUPPORTED_PROTOCOL'}, {'kind': 'ORDINARY'},
                     {'kind': 'SUPPORTED_PROTOCOL', 'branch_action': 'SUPPORTED_OPERATION_RESOLVE', 'role_certificate_ids': ['adopted-cert']}]:
            d = c.decide(s, q, role, policy_sha256=POLICY_SHA)
            self.assertEqual(d['action'], 'BYPASS')
            self.assertEqual(d['base_identity'], role)
        with self.assertRaises(ValueError):
            c.decide(s, q, {'kind': 'UNKNOWN'}, policy_sha256=POLICY_SHA, supported_operation=True)
        d = c.decide(s, q, {'kind': 'UNKNOWN'}, policy_sha256=POLICY_SHA,
                     supported_operation={'evidence_refs': [{'path': 'adopted/unit.json', 'sha256': 'a' * 64}]})
        self.assertEqual(d['action'], 'BYPASS')

    def test_flat_structural_validator_rejects_mutation_and_false_identity_claim(self):
        s, q = fixture()
        d = c.make_decision(s, q, policy_sha256=POLICY_SHA, action='PENDING', reason='TYPE_UNRESOLVED')
        self.assertTrue(c.validate_decision(s, q, d, POLICY_SHA))
        bad = copy.deepcopy(d); bad['action'] = 'CONTINUE'
        with self.assertRaises(ValueError):
            c.validate_decision(s, q, bad, POLICY_SHA)
        with self.assertRaises(ValueError):
            c.make_decision(s, q, policy_sha256=POLICY_SHA, action='STOP', reason='SERVICE', evidence_refs=[])
        with self.assertRaises(ValueError):
            c.make_decision(s, q, policy_sha256=POLICY_SHA, action='STOP', reason='UNKNOWN_CODE_COST_BOUNDARY',
                            evidence_refs=[{'path': 'x', 'sha256': 'a' * 64}], source_zero=True)


if __name__ == '__main__':
    unittest.main()
