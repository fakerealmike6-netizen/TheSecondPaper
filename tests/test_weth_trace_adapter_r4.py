"""Synthetic finite request-chain and EVM emitter proof controls; no network."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from weth_trace_adapter_r4 import (WETH, DEPOSIT_TOPIC, SCHEMA, expected_sql,
    validate_dune_bundle, prove_unique_emitter, native_rows)
from weth_evidence import extend_dune_evidence_context, is_verified_context


def fixture():
    tx_hash, block_hash = '0x' + 'a' * 64, '0x' + 'b' * 64
    sender, caller, implementation = ('0x' + c * 40 for c in ('1', '2', '3'))
    policy = {'tx_hash': tx_hash, 'block_number': 123, 'contract': WETH,
        'credited_address': caller, 'amount_raw': '10', 'input_trace_address': [0, 0], 'deposit_log_index': 4}
    common = {'tx_hash': tx_hash, 'block_number': 123, 'block_hash': block_hash,
        'block_time': '2024-01-01 00:00:00.000 UTC', 'tx_index': 2,
        'success': True, 'tx_success': True, 'error': None, 'type': 'call',
        'call_type': 'call', 'value_raw': '10', 'output_data': None,
        'refund_address': None, 'gas': '50', 'gas_used': '20'}
    rows = [{**common, 'trace_address': [], 'sub_traces': 1, 'from_address': sender,
             'to_address': caller, 'input_data': '0x12'},
            {**common, 'trace_address': [0], 'sub_traces': 1, 'from_address': caller,
             'to_address': implementation, 'input_data': '0x12', 'call_type': 'delegatecall'},
            {**common, 'trace_address': [0, 0], 'sub_traces': 0, 'from_address': caller,
             'to_address': WETH, 'input_data': '0xd0e30db0'}]
    tx = {'hash': tx_hash, 'blockNumber': '0x7b', 'blockHash': block_hash, 'transactionIndex': '0x2',
          'chainId': '0x1', 'from': sender, 'to': caller, 'value': '0xa', 'input': '0x12'}
    log = {'address': WETH, 'topics': [DEPOSIT_TOPIC, '0x' + '0' * 24 + caller[2:]], 'data': '0xa',
        'logIndex': '0x4', 'removed': False, 'transactionHash': tx_hash,
        'blockHash': block_hash, 'blockNumber': '0x7b', 'transactionIndex': '0x2'}
    receipt = {'transactionHash': tx_hash, 'blockNumber': '0x7b', 'blockHash': block_hash,
        'transactionIndex': '0x2', 'status': '0x1', 'logs': [log]}
    review = {'source_validated': True, 'semantics': 'CANONICAL_WETH9_DEPOSIT_NO_FEE_NO_REFUND',
        'runtime_code_sha256': 'SYNTHETIC_RUNTIME', 'source_text_sha256': 'SYNTHETIC_SOURCE'}
    return policy, rows, tx, receipt, review


def dump(path, value):
    data = value.encode() if isinstance(value, str) else (json.dumps(value, sort_keys=True) + '\n').encode()
    path.write_bytes(data)
    return {'path': path.name, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}


def bundle(root, modify=None):
    policy, rows, tx, receipt, review = fixture()
    scope = {k: policy[k] for k in ('tx_hash', 'block_number', 'contract', 'credited_address', 'amount_raw')}
    scope.update({'chain_id': 1, 'block_date_utc': '2024-01-01', 'all_call_tree_rows': True})
    raw = {'sql': expected_sql(scope), 'cached_internal_excerpt': {'SYNTHETIC': True},
           'schema_evidence': {'SYNTHETIC': True}}
    if modify:
        modify(raw, rows, scope)
    identities = {role: dump(root / (role + ('.sql' if role == 'sql' else '.json')), value)
                  for role, value in raw.items()}
    sha = identities['sql']['sha256']
    freeze = {'schema_version': 'r2-fixed-context-batch-1', 'fixed_scope': scope,
        'no_sql_limit': True, 'sql_sha256': sha, 'full_sql_sha256': sha, 'query_file_sha256': sha,
        'input_files': {name: identities[role]['sha256'] for name, role in
            [('query.sql', 'sql'), ('cached_internal_excerpt.json', 'cached_internal_excerpt'), ('schema_evidence.json', 'schema_evidence')]}}
    identities['freeze'] = dump(root / 'freeze.json', freeze)
    execution = 'SYNTHETIC_EXECUTION'
    submit = {'execution_id': execution, 'state': 'QUERY_STATE_PENDING'}
    metadata = {'row_count': len(rows), 'total_row_count': len(rows), 'column_names': list(rows[0])}
    status = {'execution_id': execution, 'state': 'QUERY_STATE_COMPLETED', 'result_metadata': metadata,
              'execution_cost_credits': 0.125}
    page = {'execution_id': execution, 'state': 'QUERY_STATE_COMPLETED', 'result': {'rows': rows, 'metadata': metadata}}
    for role, value in [('submit', submit), ('status', status), ('page', page), ('page_raw', page)]:
        identities[role] = dump(root / (role + '.json'), value)
    def wire(role, operation):
        return {'http_status': 200, 'error_class': None, 'operation': operation,
            'request_id': 'SYNTHETIC_' + operation, 'sha256': identities[role]['sha256'],
            'raw_bytes': identities[role]['bytes'], 'execution_id': None if role == 'submit' else execution}
    sr, pr = wire('submit', 'execute'), wire('page_raw', 'results')
    pr['parameters'] = {'offset': 0, 'limit': 1000}
    identities['submit_receipt'] = dump(root / 'submit_receipt.json', sr)
    identities['page_receipt'] = dump(root / 'page_receipt.json', pr)
    job = {'kind': 'context', 'sql_sha256': sha, 'scope_freeze_sha256': identities['freeze']['sha256'],
        'execution_id': execution, 'export_offsets': [0], 'export_requests': 1,
        'submit_receipt': sr, 'status_receipt': wire('status', 'status'),
        'status_response': {**status, 'execution_cost_credits': '0.125'}}
    identities['job'] = dump(root / 'job.json', job)
    manifest = root / 'INPUT.json'
    dump(manifest, {'schema_version': SCHEMA, 'files': identities})
    return manifest, policy, rows, tx, receipt, review


class DuneTraceBindingR4Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_archived_request_to_native_rows_to_emitter_positive(self):
        manifest, policy, rows, tx, receipt, review = bundle(self.root)
        verified = validate_dune_bundle(manifest, policy)
        self.assertTrue(verified['page_contract']['complete'])
        self.assertEqual(native_rows(verified['rows'])['result'][0]['trace_address'], [0, 0])
        proof = prove_unique_emitter(policy, verified['rows'], tx, receipt, review)
        self.assertTrue(proof['proof_valid'])
        self.assertFalse(proof['frame_logs_fabricated'])
        self.assertFalse(is_verified_context(proof))

    def test_all_derived_frame_logs_stay_empty(self):
        from weth_component import frames
        manifest, policy, *_ = bundle(self.root)
        self.assertTrue(all(not frame['logs'] for _, frame in frames(validate_dune_bundle(manifest, policy)['tree'])))

    def test_changed_archived_bytes_rejected(self):
        manifest, policy, *_ = bundle(self.root)
        with (self.root/'page_raw.json').open('ab') as stream: stream.write(b' ')
        with self.assertRaisesRegex(ValueError, 'hash/size'):
            validate_dune_bundle(manifest, policy)

    def test_rehashed_changed_wire_receipt_still_rejected(self):
        manifest, policy, *_ = bundle(self.root)
        data = json.loads((self.root/'page_raw.json').read_text()); data['execution_id'] = 'OTHER'
        doc = json.loads(manifest.read_text()); doc['files']['page_raw'] = dump(self.root/'page_raw.json', data); dump(manifest, doc)
        with self.assertRaisesRegex(ValueError, 'receipt'):
            validate_dune_bundle(manifest, policy)

    def test_filtered_sql_is_not_full_tree_proof(self):
        manifest, policy, *_ = bundle(self.root, lambda raw, rows, scope: raw.update(sql=raw['sql'].replace('ORDER BY', 'AND success = true ORDER BY')))
        with self.assertRaisesRegex(ValueError, 'every type/status/value'):
            validate_dune_bundle(manifest, policy)

    def test_limited_sql_is_not_full_tree_proof(self):
        manifest, policy, *_ = bundle(self.root, lambda raw, rows, scope: raw.update(sql=raw['sql'] + ' LIMIT 3'))
        with self.assertRaisesRegex(ValueError, 'every type/status/value'):
            validate_dune_bundle(manifest, policy)

    def test_unknown_trace_type_rejected_before_filtering(self):
        def modify(raw, rows, scope): rows[1]['type'] = 'create'
        manifest, policy, *_ = bundle(self.root, modify)
        with self.assertRaisesRegex(ValueError, 'Unsupported execution'):
            validate_dune_bundle(manifest, policy)

    def test_wrong_frozen_date_rejected(self):
        def modify(raw, rows, scope): rows[1]['block_time'] = '2024-01-02 00:00:00 UTC'
        manifest, policy, *_ = bundle(self.root, modify)
        with self.assertRaisesRegex(ValueError, 'UTC date'):
            validate_dune_bundle(manifest, policy)

    def test_missing_internal_frame_rejected(self):
        def modify(raw, rows, scope): rows.pop(1)
        manifest, policy, *_ = bundle(self.root, modify)
        with self.assertRaises(ValueError): validate_dune_bundle(manifest, policy)

    def test_bool_path_rejected(self):
        def modify(raw, rows, scope): rows[-1]['trace_address'][-1] = False
        manifest, policy, *_ = bundle(self.root, modify)
        with self.assertRaisesRegex(ValueError, 'Exact indexed'):
            validate_dune_bundle(manifest, policy)

    def test_changed_fee_value_not_representation_rejected(self):
        manifest, policy, *_ = bundle(self.root)
        path = self.root/'job.json'; job = json.loads(path.read_text()); job['status_response']['execution_cost_credits'] = '0.126'
        doc = json.loads(manifest.read_text()); doc['files']['job'] = dump(path, job); dump(manifest, doc)
        with self.assertRaisesRegex(ValueError, 'execution/completion'):
            validate_dune_bundle(manifest, policy)

    def test_plain_context_cannot_self_promote(self):
        manifest, policy, *_ = bundle(self.root)
        with self.assertRaisesRegex(ValueError, 'Validated real'):
            extend_dune_evidence_context({'kind': 'REAL_CHAIN'}, manifest, policy)


class UniqueEmitterR4Tests(unittest.TestCase):
    def setUp(self): self.args = fixture()

    def reject(self):
        with self.assertRaises((ValueError, KeyError)): prove_unique_emitter(*self.args)

    def test_delegatecall_preserves_nonweth_context(self):
        proof = prove_unique_emitter(*self.args)
        self.assertEqual(proof['execution_contexts'][1]['storage_and_log_emitter_address'], self.args[0]['credited_address'])

    def test_fallback_positive(self):
        self.args[1][-1]['input_data'] = '0x'
        self.assertTrue(prove_unique_emitter(*self.args)['proof_valid'])

    def test_same_amount_repeated_deposit_rejected(self):
        logs = self.args[3]['logs']; second = copy.deepcopy(logs[0]); second['logIndex'] = '0x5'; logs.append(second)
        self.reject()

    def test_same_log_index_repeated_rejected(self):
        self.args[3]['logs'].append(copy.deepcopy(self.args[3]['logs'][0])); self.reject()

    def test_second_weth_context_even_different_amount_rejected(self):
        rows = self.args[1]; second = copy.deepcopy(rows[-1]); second.update(trace_address=[0, 1], value_raw='11'); rows[1]['sub_traces'] = 2; rows.append(second)
        self.reject()

    def test_missing_frame_rejected(self):
        self.args[1].pop(1); self.reject()

    def test_unknown_type_rejected(self):
        self.args[1][1]['call_type'] = 'callcode'; self.reject()

    def test_reverted_ancestor_rejected(self):
        self.args[1][1]['success'] = False; self.reject()

    def test_unknown_ancestor_success_rejected(self):
        self.args[1][1]['success'] = None; self.reject()

    def test_wrong_fixed_frame_rejected(self):
        self.args[0]['input_trace_address'] = [0]; self.reject()

    def test_wrong_transaction_log_rejected(self):
        self.args[3]['logs'][0]['transactionHash'] = '0x' + 'c' * 64; self.reject()

    def test_wrong_log_position_rejected(self):
        self.args[3]['logs'][0]['logIndex'] = '0x5'; self.reject()

    def test_wrong_log_block_rejected(self):
        self.args[3]['logs'][0]['blockHash'] = '0x' + 'c' * 64; self.reject()

    def test_removed_log_rejected(self):
        self.args[3]['logs'][0]['removed'] = True; self.reject()

    def test_wrong_root_endpoints_rejected(self):
        self.args[1][0]['to_address'] = '0x' + '4' * 40; self.reject()

    def test_wrong_delegate_caller_context_rejected(self):
        self.args[1][1]['from_address'] = '0x' + '4' * 40; self.reject()

    def test_unknown_source_semantics_rejected(self):
        self.args[4]['source_validated'] = False; self.reject()

    def test_wrong_leaf_entrypoint_rejected(self):
        self.args[1][-1]['input_data'] = '0x12345678'; self.reject()

    def test_wrong_credited_address_rejected(self):
        self.args[0]['credited_address'] = '0x' + '4' * 40; self.reject()

    def test_other_canonical_weth_event_rejected(self):
        self.args[3]['logs'][0]['topics'][0] = '0x' + '0' * 64; self.reject()


if __name__ == '__main__': unittest.main()
