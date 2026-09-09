"""New controlled ledger tests; these are not real-query adoption evidence."""
import copy
import itertools
import unittest
from fractions import Fraction as F

from context_lp_r3 import build_context_model, audit_context_witness, solve_context_interval
from stage1d_multiasset_context import ETH, WETH, SCHEMA, register_document
from stage1c_intervals import run_interval, build_variant, audit_allocation
from stage1c_baselines import run_baseline
from stage1c_output_contract import expected_domains

A = '0x' + 'a' * 40
B = '0x' + 'b' * 40
S = '0x' + 'c' * 40
SOURCE = '0x' + 'd' * 40


def fixture(withdraw=False):
    def account(address, asset, amount):
        return {'account_id': address + '|' + asset, 'initial_actual_balance_raw': str(amount),
                'initial_source_raw': '0', 'initial_source_basis': 'CONTROLLED_PRE_SEED_STATE',
                'initial_position': {'phase': 'BLOCK_END', 'block_number': 9},
                'evidence_ids': ['controlled-historical-balance:' + address + ':' + asset]}
    def flow(eid, sender, receiver, value, role='CANDIDATE', asset=ETH):
        return {'event_id': eid, 'from_account': sender + '|' + asset, 'to_account': receiver + '|' + asset,
                'amount_raw': str(value), 'role': role, 'asset': asset, 'flow_kind': 'top' if asset == ETH else 'erc20_log',
                'order_basis': 'CONTROLLED_EXACT_EXECUTION_ORDER', 'evidence_ids': ['controlled:' + eid]}
    def fee(eid, payer):
        return {'fee_id': eid, 'payer_account': payer + '|ETH', 'amount_raw': '1',
                'timing': 'TX_BEGIN_NET_FEE', 'evidence_ids': ['controlled-receipt:' + eid]}
    def tx(number, flows, fees=()):
        return {'tx_id': 'tx' + str(number), 'block_number': 10 + number, 'tx_index': 0,
                'flows': flows, 'fees': list(fees)}
    units = []
    def conversion(number, direction, value):
        incoming, outgoing = (ETH, WETH) if direction == 'DEPOSIT' else (WETH, ETH)
        uid = 'controlled-' + direction.lower()
        rawid = 'physical:' + direction.lower()
        unit = {'unit_id': uid, 'kind': direction, 'tx_hash': 'tx' + str(number), 'block_number': 10 + number,
                'tx_index': 0, 'holder': B, 'evidence_kind': 'SYNTHETIC_CONTROLLED',
                'controlled_contract': 'CANONICAL_1TO1_NO_REFUND', 'raw_consumed_event_ids': [rawid],
                'input': {'asset': incoming, 'amount_raw': str(value), 'address': B},
                'output': {'asset': outgoing, 'amount_raw': str(value), 'address': B}}
        units.append(unit)
        op = {'id': 'semport:' + uid, 'kind': 'conversion', 'asset': incoming, 'output_asset': outgoing,
              'gross_raw': str(value), 'refund_raw': '0', 'output_raw': str(value),
              'from_account': B + '|' + incoming, 'to_account': B + '|' + outgoing,
              'unit_id': uid, 'raw_consumed_event_ids': [rawid], 'certified_semantics': 'SYNTHETIC_CONTROLLED',
              'evidence_ids': ['controlled:' + rawid]}
        result = tx(number, [], [fee('fee' + str(number), B)])
        result.update(conversions=[op], operation_order=[op['id']], operation_order_basis='CONTROLLED_ATOMIC_POSITION')
        return result
    transactions = [tx(1, [flow('seed', SOURCE, A, 10, 'SEED')]),
                    tx(2, [flow('ab', A, B, 10)], [fee('fee2', A)]), conversion(3, 'DEPOSIT', 8)]
    target_asset = WETH
    if withdraw:
        transactions.append(conversion(4, 'WITHDRAWAL', 6)); target_asset = ETH
    transactions.append(tx(5 if withdraw else 4, [flow('target', B, S, 5 if withdraw else 10, asset=target_asset)],
                           [fee('fee5' if withdraw else 'fee4', B)]))
    doc = {'query_id': 'controlled-multiasset-withdraw' if withdraw else 'controlled-multiasset-deposit',
           'name': 'controlled-multiasset-withdraw' if withdraw else 'controlled-multiasset-deposit',
           'accounts': [account(A, ETH, 2), account(B, ETH, 2), account(B, WETH, 4)],
           'transactions': transactions, 'objective_groups': {S + '|' + target_asset: ['target']},
           'all_service_entries': ['target'], 'gaps': [], 'fact_conflicts': [],
           'anchors': [{'anchor_id': 'actual-post-deposit-weth', 'account_id': B + '|' + WETH,
                        'tx_id': 'tx3', 'when': 'post', 'actual_balance_raw': '12',
                        'evidence_ids': ['controlled-balanceOf-at-block13']}],
           'assumptions': ['Controlled complete account ledgers with real-format gas and historical balanceOf anchors']}
    return register_document(doc, units, evidence_kind='SYNTHETIC_CONTROLLED')


class MultiassetTests(unittest.TestCase):
    def test_deposit_against_independent_integer_ledger_enumeration(self):
        # Independent finite raw-unit accounting, no model rows/objectives.
        feasible = set()
        for ga, ab, gb, dep, gc, target in itertools.product(range(2), range(11), range(2), range(9), range(2), range(11)):
            if not 0 <= 10-ga-ab <= 1: continue
            if not 0 <= ab-gb-dep <= 3: continue
            if not 0 <= ab-gb-dep-gc <= 2: continue
            if not 0 <= dep-target <= 2: continue
            feasible.add(target)
        result = run_interval(fixture(), joint_zero_optimization=False)
        value = result['events']['target']
        self.assertEqual(result['status'], 'COMPLETED')
        self.assertEqual((F(value['lower_raw']), F(value['upper_raw'])), (min(feasible), max(feasible)))
        self.assertEqual((min(feasible), max(feasible)), (2, 8))
        for endpoint in value['endpoints'].values():
            self.assertTrue(endpoint['independent_audit']['exact_feasible'])

    def test_all_seven_interfaces_and_full_haircut(self):
        doc = fixture()
        self.assertTrue(expected_domains(doc)['passed'])
        for method in ['FULL_INTERVAL', 'NO_CROSS_TARGET_COUPLING', 'NO_PROTOCOL_CONTINUATION', 'BALANCE_INFORMATION_REMOVED']:
            result = run_interval(doc, method)
            self.assertEqual(result['status'], 'COMPLETED', (method, result))
        for method in ['BOUNDED_REACHABILITY', 'POISON', 'HAIRCUT']:
            result = run_baseline(doc, method)
            self.assertEqual(result['status'], 'OK', result)
            self.assertEqual(set(result['joint_by_asset']), {WETH})
            if method == 'HAIRCUT':
                self.assertEqual(F(result['events']['target']['point_raw']), F(125, 27))
                self.assertTrue(audit_allocation(doc, result['allocation_raw'])['exact_feasible'])

    def test_protocol_ablation_retains_real_balances_and_source_boundary(self):
        result = run_interval(fixture(), 'NO_PROTOCOL_CONTINUATION', joint_zero_optimization=False)
        self.assertEqual(result['events']['target']['upper_raw'], '0')
        self.assertEqual(result['modifications']['connected_protocol_count'], 1)
        audit = result['events']['target']['endpoints']['upper']['independent_audit']
        self.assertTrue(audit['exact_feasible'])
        self.assertGreater(F(audit['source_protocol_boundary_raw']), 0)

    def test_symmetric_withdraw(self):
        doc = fixture(True)
        result = run_interval(doc)
        self.assertEqual(result['status'], 'COMPLETED')
        self.assertEqual(result['events']['target']['asset'], ETH)
        haircut = run_baseline(doc, 'HAIRCUT')
        self.assertEqual(haircut['status'], 'OK')
        self.assertTrue(audit_allocation(doc, haircut['allocation_raw'])['exact_feasible'])

    def test_strict_counterexamples(self):
        for mutation in ['wrong_schema', 'wrong_registry', 'wrong_holder', 'wrong_kind', 'duplicate_consumption', 'missing_order', 'double_fee', 'hidden_refund', 'fake_real']:
            doc = fixture()
            if mutation == 'wrong_schema': doc['schema_version'] += 'unknown'
            elif mutation == 'wrong_registry': doc['asset_registry'][WETH]['chain_id'] = 2
            elif mutation == 'wrong_holder': doc['semantic_units'][0]['holder'] = A
            elif mutation == 'wrong_kind': doc['semantic_units'][0]['kind'] = 'WITHDRAWAL'
            elif mutation == 'duplicate_consumption':
                doc['transactions'][2]['flows'] = [{'event_id': 'physical:deposit'}]
            elif mutation == 'missing_order': doc['transactions'][2].pop('operation_order')
            elif mutation == 'double_fee': doc['transactions'][2]['fees'] *= 2
            elif mutation == 'hidden_refund': doc['transactions'][2]['conversions'][0]['refund_raw'] = '1'
            elif mutation == 'fake_real': doc['evidence_kind'] = 'REAL_EVIDENCE_BOUND'
            with self.subTest(mutation=mutation), self.assertRaises((ValueError, KeyError)):
                build_context_model(doc)

    def test_missing_weth_balance_stays_unknown(self):
        doc = fixture(); doc['accounts'][2]['initial_actual_balance_raw'] = None
        doc['anchors'] = []
        self.assertEqual(run_baseline(doc, 'HAIRCUT')['status'], 'NOT_APPLICABLE')
        model = build_context_model(doc)
        self.assertIn(B + '|' + WETH, model.metadata['unknown_initial_balances'])
        self.assertEqual(model.metadata['balance_status'], 'PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS')

    def test_mixed_asset_objective_rejected_and_queries_independent(self):
        doc = fixture(); model = build_context_model(doc)
        with self.assertRaises(ValueError): solve_context_interval(model, doc, ['seed', 'target'])
        other = copy.deepcopy(doc); other['query_id'] = 'another-source-query'
        m2 = build_context_model(other)
        self.assertNotEqual(model.metadata['document_sha256'], m2.metadata['document_sha256'])
        self.assertIsNot(model.variables, m2.variables)
        self.assertEqual(model.metadata['source_potential_raw'], m2.metadata['source_potential_raw'])


if __name__ == '__main__': unittest.main()
