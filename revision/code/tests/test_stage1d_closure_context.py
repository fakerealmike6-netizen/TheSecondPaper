"""Offline final assembly: frozen scope, token point needs and no false success."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from test_stage1d_multiasset_pipeline import pipeline_material
from stage1d_closure_context import requirements, assemble, validate_current, load_frozen_inputs
from stage1d_multiasset_context import WETH, CONTRACT, SCHEMA
from stage1c_output_contract import expected_domains


def fixture(withdraw=False):
    query, collection, ledger, balances, headers, receipts, labels = pipeline_material(withdraw)
    query['scope_id'] = collection['metrics']['scope_id']
    material = {**ledger, 'balances': balances, 'headers': headers, 'receipts': receipts}
    return query, collection, labels, material


class ClosureContextTests(unittest.TestCase):
    def test_final_assembler_keeps_conversion_and_shared_method_domain(self):
        q, c, labels, m = fixture()
        result = assemble(q, c, labels, m)
        self.assertEqual(result['status'], 'RUNNABLE_CONDITIONAL_CONTEXT')
        doc = result['registration_arguments']['document']
        self.assertEqual(doc['schema_version'], SCHEMA)
        self.assertEqual(sum(len(t.get('conversions', [])) for t in doc['transactions']), 1)
        self.assertTrue(expected_domains(doc)['passed'])
        self.assertEqual(result['registration_arguments']['context_status'], 'PARTIAL_CONTEXT_WITH_EXPLICIT_ASSUMPTIONS')
        self.assertEqual(result['methods_executed'], 0)

    def test_balanceof_requests_are_exact_holder_anchors(self):
        q, c, labels, m = fixture()
        needed = requirements(q, c, labels, m)
        token_rows = [r for r in needed['context_plan']['rows'] if r['asset'] == WETH]
        requests = [r for r in needed['point_requests'] if r['method'] == 'eth_call']
        self.assertEqual(len(requests), 2 * len(token_rows))
        for row in token_rows:
            for block in (row['before_anchor_block'], row['after_anchor_block']):
                self.assertIn({'method': 'eth_call', 'params': [{'to': CONTRACT,
                    'data': '0x70a08231'+'0'*24+row['address'][2:]}, hex(block)]}, requests)
        self.assertEqual(len(needed['weth_ledger_requirements'][0]['requests']), 4)
        self.assertEqual(needed['new_requests_executed'], 0)

    def test_missing_token_balance_is_explicit_not_native_substitution(self):
        q, c, labels, m = fixture()
        m['balances'] = {k:v for k,v in m['balances'].items() if v['request']['method'] != 'eth_call'}
        result = assemble(q, c, labels, m)
        self.assertTrue(any(r['method'] == 'eth_call' for r in result['missing_points']))
        self.assertTrue(any(g['type'] == 'INITIAL_WETH_BALANCEOF_MISSING'
                            for g in result['context_evidence']['gaps']))
        self.assertNotEqual(result['context_evidence']['adapter_status'], 'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')

    def test_new_observed_holder_log_requests_only_its_missing_top_and_receipt(self):
        q, c, labels, m = fixture()
        before = requirements(q, c, labels, m)
        self.assertFalse(any(r['method'] == 'eth_getTransactionByHash' for r in before['point_requests']))
        log = copy.deepcopy(next(rec['logs'][0] for rec in m['receipts'].values()
                                 if rec.get('logs') and len(rec['logs'][0].get('topics', [])) == 3))
        log['transactionHash'] = '0x'+'6'*64
        m['weth_logs'] = [log]
        needed = requirements(q, c, labels, m)
        self.assertIn({'method':'eth_getTransactionByHash','params':[log['transactionHash']]}, needed['point_requests'])
        self.assertIn({'method':'eth_getTransactionReceipt','params':[log['transactionHash']]}, needed['point_requests'])
        self.assertEqual(needed['context_plan'], before['context_plan'])
        self.assertEqual(m['coverage'], fixture()[3]['coverage'])
        assembled = assemble(q, c, labels, m)
        self.assertEqual(assembled['status'], 'MODEL_BLOCKED')
        self.assertIsNone(assembled['registration_arguments'])
        self.assertEqual(assembled['context_result']['completion_status'], 'OBSERVED_WETH_LEDGER_MATERIALIZATION_BLOCKED')

    def test_unresolved_conversion_none_never_registers(self):
        q, c, labels, m = fixture(True)
        c['semantic_units'] = [u for u in c['semantic_units'] if u['kind'] == 'DEPOSIT']
        result = assemble(q, c, labels, m)
        self.assertEqual(result['status'], 'MODEL_BLOCKED')
        self.assertIsNone(result['context_result']['model_input'])
        self.assertIsNone(result['registration_arguments'])

    def test_changed_scope_or_adopted_role_rejected(self):
        q, c, labels, m = fixture()
        other = copy.deepcopy(q); other['scope_hash'] = '0'*64
        with self.assertRaisesRegex(ValueError, 'scope'): requirements(other, c, labels, m)
        labels[c['states'][0]['state']['address']] = {'kind': 'SERVICE'}
        with self.assertRaisesRegex(ValueError, 'adopted role'): requirements(q, c, labels, m)

    def test_shared_foreign_rows_and_receipts_do_not_enter_query_model(self):
        q, c, labels, m = fixture()
        expected = assemble(q, c, labels, m)['registration_arguments']['document']
        foreign = copy.deepcopy(m['events'][0])
        foreign.update(event_id='foreign', sender='0x'+'8'*40, recipient='0x'+'9'*40,
                       tx_hash='0x'+'7'*64, block=9999)
        m['events'].append(foreign)
        receipt = copy.deepcopy(next(iter(m['receipts'].values())))
        receipt.update(transactionHash='0x'+'7'*64, blockNumber=hex(9999), logs=[])
        m['receipts']['0x'+'7'*64] = receipt
        m['balances']['foreign:0'] = {'request':{'method':'eth_getBalance','params':['0x'+'9'*40,'0x0']},
                                     'response':{'result':'0x12345'}}
        actual = assemble(q, c, labels, m)['registration_arguments']['document']
        self.assertEqual(expected, actual)

    def test_existing_experiment_registration_accepts_same_multiasset_document(self):
        from stage1d_experiments import initialize_batch, register_document
        q, c, labels, m = fixture()
        result = assemble(q, c, labels, m)
        declared = [q] + [dict(q, name='unused-'+str(i), query_id='unused-'+str(i)) for i in range(3)]
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            root = Path(tmp); batch = root/'batch'
            initialize_batch(root, batch, declared)
            row = register_document(root, batch, acquisition_status='ACQUISITION_PARTIAL',
                                    **result['registration_arguments'])
            doc = json.loads((root/row['observed_path']).read_bytes())
            self.assertEqual(doc['schema_version'], SCHEMA)
            self.assertEqual(sum(len(t.get('conversions', [])) for t in doc['transactions']), 1)
            self.assertEqual(row['scope_hash'], q['scope_hash'])
            self.assertEqual(row['context_status'], 'PARTIAL_CONTEXT_WITH_EXPLICIT_ASSUMPTIONS')
            self.assertEqual(row['method_execution'], 'REGISTERED')

    def test_load_explicit_file_sha_and_current_batch_binding(self):
        q, c, labels, m = fixture()
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            root = Path(tmp)
            def save(name, value):
                p = root/name; p.write_text(json.dumps(value), encoding='utf-8')
                return {'path':name, 'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
            batch = save('batch.json', {'queries':[q]})
            cr, lr = save('collection.json', c), save('labels.json', labels)
            frozen = save('frozen.json', {'freeze_sha256':batch['sha256'], 'candidates_frozen':True,
                'labels_frozen':True, 'queries':{q['name']:{'collection':cr,'labels':lr}}})
            with patch('stage1d_closure_scope.active_batch_path', return_value=root/'batch.json'):
                loaded = load_frozen_inputs(root, frozen, q['name'])
                self.assertEqual(loaded[0], q)
                (root/'labels.json').write_text('{}')
                with self.assertRaisesRegex(ValueError, 'SHA-256'):load_frozen_inputs(root, frozen, q['name'])

    def test_explicit_binding_cannot_be_reused_after_input_change(self):
        q, c, labels, m = fixture()
        binding = validate_current(q, c, labels)
        c['gaps'].append({'type':'retained-new-gap'})
        with self.assertRaisesRegex(ValueError, 'changed after'):assemble(q, c, labels, m, binding=binding)


if __name__ == '__main__':
    unittest.main()
