"""R2-04 identity/provenance counterexamples. Fault data are synthetic only."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE / 'src'))
from weth_component import verify_component
from weth_evidence import payload_hash, load_evidence_context


def fixture():
    case = json.loads((BASE / 'fixtures/fault/weth/external_control.json').read_text())
    data = copy.deepcopy(case['input'])
    tx = data['policy']['tx_hash']
    data['evidence_context'] = {'kind': 'SYNTHETIC', 'records': {
        'trace': {'chain_id': 1, 'payload_sha256': payload_hash(data['call_trace']),
                  'request': {'method': 'debug_traceTransaction', 'params': [tx, {'tracer': 'callTracer', 'tracerConfig': {'withLog': True}}]}},
        'historical_code': {'chain_id': 1, 'payload_sha256': payload_hash(data['historical_code']),
                            'request': {'method': 'eth_getCode', 'params': [data['policy']['contract'], hex(data['policy']['block_number'])]}}}}
    return data


class WethIdentityR1Tests(unittest.TestCase):
    def temporary_directory(self):
        folder=BASE/'.testtmp';folder.mkdir(exist_ok=True)
        return tempfile.TemporaryDirectory(dir=folder)

    def assert_rejected(self, data):
        output = verify_component(**data)
        self.assertEqual(output['status'], 'COMPONENT_EVIDENCE_INCONSISTENT')
        self.assertFalse(output['real_component_certified'])
        self.assertFalse(output['synthetic_component_verified'])
        self.assertTrue(output['evidence_bindings']['conflicts'])
        return output

    def test_external_faults_are_now_rejected_with_exact_same_inputs(self):
        for path in sorted((BASE / 'fixtures/fault/weth').glob('external_wrong_*.json')):
            with self.subTest(case=path.name):
                self.assert_rejected(json.loads(path.read_text())['input'])

    def test_second_inspection_faults_rejected_with_exact_same_inputs(self):
        for path in sorted((BASE/'fixtures/fault/weth').glob('second_conflicting_*.json')):
            with self.subTest(case=path.name):
                self.assert_rejected(json.loads(path.read_text())['input'])

    def test_complete_synthetic_never_enables_real_component_or_lp(self):
        output = verify_component(**fixture())
        self.assertEqual(output['status'], 'SYNTHETIC_COMPONENT_VERIFIED_NOT_REAL')
        self.assertTrue(output['synthetic_component_verified'])
        self.assertFalse(output['semantic_unit']['certified'])
        self.assertFalse(output['boundary_comparison']['real_component_use_permitted'])
        self.assertFalse(output['semantic_unit']['next_state']['enabled_for_real_collection'])

    def test_missing_trace_tx_hash_allowed_by_verified_request_payload_binding(self):
        data = fixture()
        del data['call_trace']['transactionHash']
        data['evidence_context']['records']['trace']['payload_sha256'] = payload_hash(data['call_trace'])
        output = verify_component(**data)
        self.assertTrue(output['synthetic_component_verified'])
        self.assertTrue(output['evidence_bindings']['request_bound']['trace'])
        self.assertFalse(output['real_component_certified'])

    def test_missing_trace_hash_without_request_binding_stays_unverified(self):
        data = fixture()
        del data['call_trace']['transactionHash']
        data['evidence_context']['records'].pop('trace')
        output = verify_component(**data)
        self.assertFalse(output['synthetic_component_verified'])
        self.assertFalse(output['checks']['deposit_bound_to_call_frame'])

    def test_missing_frame_global_index_requires_unique_receipt_pattern(self):
        data=fixture();del data['call_trace']['calls'][0]['logs'][0]['logIndex']
        data['evidence_context']['records']['trace']['payload_sha256']=payload_hash(data['call_trace'])
        self.assertTrue(verify_component(**data)['synthetic_component_verified'])
        duplicate=copy.deepcopy(data['receipt']['logs'][0]);duplicate['logIndex']='0x5'
        data['receipt']['logs'].append(duplicate)
        result=verify_component(**data)
        self.assertFalse(result['checks']['deposit_bound_to_call_frame'])
        self.assertFalse(result['real_component_certified'])

    def test_trace_chain_conflict_rejected_even_with_matching_payload_hash(self):
        data = fixture();data['call_trace']['chainId'] = '0x38'
        data['evidence_context']['records']['trace']['payload_sha256'] = payload_hash(data['call_trace'])
        self.assert_rejected(data)

    def test_receipt_block_hash_conflict(self):
        data = fixture();data['tx']['blockHash'] = '0x'+'1'*64;data['receipt']['blockHash'] = '0x'+'2'*64
        self.assert_rejected(data)

    def test_cross_source_hash_conflict_when_tx_receipt_hash_omitted(self):
        data=fixture();data['call_trace']['blockHash']='0x'+'1'*64
        data['internal_response']['result'][0]['blockHash']='0x'+'2'*64
        data['evidence_context']['records']['trace']['payload_sha256']=payload_hash(data['call_trace'])
        self.assert_rejected(data)

    def test_receipt_chain_conflict(self):
        data = fixture();data['receipt']['chainId'] = 2;self.assert_rejected(data)

    def test_internal_chain_and_block_conflicts(self):
        for field, value in [('chainId', 2), ('blockNumber', '801'), ('hash', '0x'+'f'*64)]:
            with self.subTest(field=field):
                data = fixture();data['internal_response']['result'][0][field] = value;self.assert_rejected(data)

    def test_frame_log_wrong_identity_rejected_when_request_hash_recomputed(self):
        for field, value in [('transactionHash', '0x'+'f'*64), ('logIndex', '0x99'), ('blockNumber', '0x321'), ('chainId', 2), ('removed', True)]:
            with self.subTest(field=field):
                data=fixture();data['call_trace']['calls'][0]['logs'][0][field]=value
                data['evidence_context']['records']['trace']['payload_sha256']=payload_hash(data['call_trace'])
                self.assert_rejected(data)

    def test_source_contract_chain_and_historical_block_conflicts(self):
        for field, value in [('chain_id', 2), ('contract', '0x'+'f'*40), ('historical_block_number', 801)]:
            with self.subTest(field=field):
                data=fixture();data['source_attestation'][field]=value;self.assert_rejected(data)

    def test_historical_code_wrong_block_request_rejected(self):
        data=fixture();data['evidence_context']['records']['historical_code']['request']['params'][1]='0x321'
        self.assert_rejected(data)

    def test_wrong_trace_request_tx_rejected_even_when_payload_matches(self):
        data=fixture();data['evidence_context']['records']['trace']['request']['params'][0]='0x'+'f'*64
        self.assert_rejected(data)

    def test_payload_changed_since_request_receipt_rejected(self):
        data=fixture();data['call_trace']['gas']='0x999';self.assert_rejected(data)

    def test_arbitrary_caller_url_hash_code_cannot_certify_real(self):
        data=fixture();data.pop('evidence_context')
        output=verify_component(**data)
        self.assertFalse(output['real_component_certified'])
        self.assertFalse(output['checks']['runtime_matches_attested_source'])

    def test_caller_dict_cannot_self_declare_real_provenance(self):
        data=fixture();data['evidence_context']['kind']='REAL_CHAIN'
        output=verify_component(**data)
        self.assertFalse(output['real_component_certified'])
        self.assertEqual(output['evidence_bindings']['evidence_kind'],'UNVERIFIED')

    def test_synthetic_acquisition_catalogue_cannot_be_loaded_as_real(self):
        with self.temporary_directory() as temp:
            root=Path(temp);(root/'bundle.json').write_text('{}')
            (root/'catalogue.json').write_text(json.dumps({'schema_version':'weth-acquisition-catalogue-1','evidence_kind':'SYNTHETIC'}))
            with self.assertRaisesRegex(ValueError,'REAL_CHAIN'):
                load_evidence_context(root/'bundle.json',root/'catalogue.json')

    def test_unpinned_caller_claims_do_not_make_real_catalogue(self):
        with self.temporary_directory() as temp:
            root=Path(temp);(root/'bundle.json').write_text('{}')
            (root/'catalogue.json').write_text(json.dumps({'schema_version':'weth-acquisition-catalogue-1','evidence_kind':'REAL_CHAIN','acquisition_run_id':'untrusted','reviewer_record_id':'claim','records':{}}))
            with self.assertRaisesRegex(ValueError,'acquisition record'):
                load_evidence_context(root/'bundle.json',root/'catalogue.json')


if __name__ == '__main__':
    unittest.main()
