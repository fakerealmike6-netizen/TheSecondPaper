"""Public synthetic tests for original-body and split-member binding checks."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from verify_weth_shared_wire_r4_r1 import (FIXED, MANIFEST, SCHEMA, ROLES,
    WireValidationError, expected_plans, strict_json, verify_document)


class SyntheticClosure:
    def __init__(self, root):
        self.root=root;self.folder=root/'synthetic';self.folder.mkdir()
        self.fixed={**FIXED,'tx_hash':'0x'+'1'*64,'block_hash':'0x'+'2'*64,'block_number':100,
                    'transaction_index':1,'weth_contract':'0x'+'3'*40,'runtime_code_bytes':4,
                    'runtime_code_sha256':hashlib.sha256(b'\x60\x00\x60\x00').hexdigest(),'transaction_value_raw':'17'}
        self.requests=[{'jsonrpc':'2.0','id':'synthetic-'+str(i),'method':method,'params':params}
                       for i,(method,params) in enumerate(expected_plans(self.fixed).values())]
        tx={'hash':self.fixed['tx_hash'],'blockHash':self.fixed['block_hash'],'blockNumber':'0x64','transactionIndex':'0x1',
            'chainId':'0x1','value':'0x11','from':'0x'+'4'*40,'to':'0x'+'5'*40,'blockTimestamp':'0xa'}
        log={'address':self.fixed['weth_contract'],'transactionHash':self.fixed['tx_hash'],'blockHash':self.fixed['block_hash'],
             'blockNumber':'0x64','transactionIndex':'0x1','blockTimestamp':'0xa','logIndex':'0x7','removed':False,'data':'0x','topics':[]}
        receipt={'transactionHash':self.fixed['tx_hash'],'blockHash':self.fixed['block_hash'],'blockNumber':'0x64','transactionIndex':'0x1',
                 'from':tx['from'],'to':tx['to'],'status':'0x1','logs':[log]}
        results=[{'error':{'code':-32600,'message':'SYNTHETIC method is not available'}}, {'result':'0x60006000'},
                 {'result':{'number':'0x64','hash':self.fixed['block_hash'],'timestamp':'0xa','transactions':['0x'+'9'*64,self.fixed['tx_hash']]}},
                 {'result':tx},{'result':receipt}]
        self.responses=[{'jsonrpc':'2.0','id':request['id'],**value} for request,value in zip(self.requests,results)]
        self.envelopes={role:{'request':copy.deepcopy(req),'response':copy.deepcopy(resp),'http_status':200,'response_complete':True,
                              'status':'RPC_ERROR' if 'error' in resp else 'SUCCESS_VALIDATED','evidence_kind':'SYNTHETIC_TEST'}
                        for role,req,resp in zip(ROLES,self.requests,self.responses)}
        self.request_extra={};self.batch_extra={};self.members_reverse=False

    def mutate_response(self, role, callback):
        index=ROLES.index(role);callback(self.responses[index])
        self.envelopes[role]['response']=copy.deepcopy(self.responses[index])

    def write(self):
        def save(name,value,raw=False):
            path=self.folder/name;data=value if raw else (json.dumps(value,sort_keys=True,indent=2)+'\n').encode()
            path.write_bytes(data)
            return {'path':path.relative_to(self.root).as_posix(),'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}
        body=json.dumps(self.responses).encode();self.fixed['original_body_sha256']=hashlib.sha256(body).hexdigest()
        artifacts={'original_body':save('body.bin',body,True)};members=[]
        for index,role in enumerate(ROLES):
            self.envelopes[role]['raw_body_sha256']=self.fixed['original_body_sha256']
            desc=save(role+'.json',self.envelopes[role]);desc['original_artifact_path']='raw/synthetic/envelope_'+str(index)+'.json'
            artifacts[role]=desc
            members.append({'artifact_path':desc['original_artifact_path'],'artifact_sha256':desc['sha256'],
                            'method':self.envelopes[role]['request']['method'],'status':self.envelopes[role]['status']})
        def redaction():return {'kind':'DERIVED_REDACTED_VIEW','not_original_bytes':True,'source_original_sha256':'a'*64,'omitted_fields':['permission_sha256']}
        requests={'requests':self.requests,'rpc_operations':5,'utc':'2026-09-07T01:00:00+00:00','redaction':redaction(),**self.request_extra}
        batch={'members':list(reversed(members)) if self.members_reverse else members,'rpc_operations_actual':5,'http_status':200,'error_class':None,
               'raw_sha256':self.fixed['original_body_sha256'],'raw_bytes':len(body),'utc':'2026-09-07T01:00:01+00:00','redaction':redaction(),**self.batch_extra}
        for role,name,value in [('requests_redacted','requests.json',requests),('batch_receipt_redacted','batch.json',batch)]:
            artifacts[role]=save(name,value);artifacts[role]['source_original_sha256']='a'*64
        self.manifest={'schema_version':SCHEMA,'fixed_identity':self.fixed,'material_status':'AVAILABLE_ORIGINAL_BODY','original_body_available':True,
                       'artifacts':artifacts,'original_dispatch_utc':requests['utc'],'original_receipt_utc':batch['utc']}
        return self.manifest

    def verify(self):return verify_document(self.root,self.write(),fixed=self.fixed)


class SharedWireR4R1Tests(unittest.TestCase):
    def setUp(self):
        parent=Path(__file__).resolve().parents[1]/'checks/wire_synthetic_tmp';parent.mkdir(parents=True,exist_ok=True)
        temporary=tempfile.TemporaryDirectory(dir=parent);self.addCleanup(temporary.cleanup);self.root=Path(temporary.name)
        self.fixture=SyntheticClosure(self.root)

    def test_valid_mixed_batch_has_four_successes_one_preserved_error(self):
        with patch('urllib.request.build_opener',side_effect=AssertionError('No network')):
            result=self.fixture.verify()
        self.assertEqual((result['status'],result['members_verified'],result['successful_members'],result['error_members']),('PASS',5,4,1))
        self.assertFalse(result['weth_certification_predicates_changed']);self.assertEqual(result['network_requests'],0)

    def test_ids_bind_independent_request_response_and_receipt_array_order(self):
        self.fixture.requests.reverse();self.fixture.responses.reverse();self.fixture.members_reverse=True
        self.assertEqual(self.fixture.verify()['members_verified'],5)

    def test_duplicate_response_id_is_rejected(self):
        self.fixture.responses[1]['id']=self.fixture.responses[0]['id']
        with self.assertRaisesRegex(WireValidationError,'DUPLICATE_RPC_ID'):self.fixture.verify()

    def test_missing_response_is_not_five_successes(self):
        self.fixture.responses.pop()
        with self.assertRaisesRegex(WireValidationError,'FIVE_MEMBERS'):self.fixture.verify()

    def test_typed_id_mismatch_is_rejected(self):
        self.fixture.responses[0]['id']=0
        with self.assertRaisesRegex(WireValidationError,'ID_SET_MISMATCH'):self.fixture.verify()

    def test_boolean_rpc_id_is_invalid(self):
        self.fixture.responses[0]['id']=False
        with self.assertRaisesRegex(WireValidationError,'INVALID_RPC_ID'):self.fixture.verify()

    def test_split_response_cannot_replace_original_wire_result(self):
        self.fixture.envelopes['transaction']['response']['result']['value']='0x12'
        with self.assertRaisesRegex(WireValidationError,'SPLIT_RESPONSE_DIFFERS'):self.fixture.verify()

    def test_historical_code_selector_cannot_be_latest(self):
        self.fixture.requests[1]['params'][1]='latest';self.fixture.envelopes['historical_code']['request']=copy.deepcopy(self.fixture.requests[1])
        with self.assertRaisesRegex(WireValidationError,'FIXED_HISTORICAL_SELECTOR'):self.fixture.verify()

    def test_transaction_block_identity_is_independently_checked(self):
        self.fixture.mutate_response('transaction',lambda row:row['result'].update(blockHash='0x'+'f'*64))
        with self.assertRaisesRegex(WireValidationError,'FIXED_BLOCK_IDENTITY'):self.fixture.verify()

    def test_block_position_cannot_point_to_another_transaction(self):
        self.fixture.mutate_response('block',lambda row:row['result']['transactions'].__setitem__(1,'0x'+'f'*64))
        with self.assertRaisesRegex(WireValidationError,'BLOCK_TRANSACTION_POSITION'):self.fixture.verify()

    def test_historical_runtime_must_match_fixed_code_identity(self):
        self.fixture.mutate_response('historical_code',lambda row:row.update(result='0x60006001'))
        with self.assertRaisesRegex(WireValidationError,'HISTORICAL_RUNTIME_IDENTITY'):self.fixture.verify()

    def test_batch_receipt_must_bind_the_original_body(self):
        self.fixture.batch_extra['raw_sha256']='f'*64
        with self.assertRaisesRegex(WireValidationError,'BATCH_ORIGINAL_WIRE_BINDING'):self.fixture.verify()

    def test_error_member_cannot_be_marked_success(self):
        self.fixture.envelopes['trace_attempt']['status']='SUCCESS_VALIDATED'
        with self.assertRaisesRegex(WireValidationError,'MEMBER_STATUS_MISMATCH'):self.fixture.verify()

    def test_authentication_fields_are_rejected_in_redacted_views(self):
        self.fixture.request_extra['Authorization']='SYNTHETIC_CREDENTIAL_NOT_A_REAL_KEY'
        with self.assertRaisesRegex(WireValidationError,'AUTHENTICATION_FIELD_NOT_REDACTED'):self.fixture.verify()

    def test_raw_body_tamper_is_rejected_without_rebuilding_from_splits(self):
        manifest=self.fixture.write();path=self.root/manifest['artifacts']['original_body']['path'];path.write_bytes(path.read_bytes()+b' ')
        with self.assertRaisesRegex(WireValidationError,'ARTIFACT_BYTE_COUNT'):verify_document(self.root,manifest,fixed=self.fixture.fixed)

    def test_manifest_cannot_change_fixed_identity_to_accept_other_chain(self):
        manifest=copy.deepcopy(self.fixture.write());manifest['fixed_identity']['chain_id']=10
        with self.assertRaisesRegex(WireValidationError,'FIXED_WETH_IDENTITY_CHANGED'):verify_document(self.root,manifest,fixed=self.fixture.fixed)

    def test_artifact_escape_is_rejected(self):
        manifest=self.fixture.write();manifest['artifacts']['original_body']['path']='../outside.bin'
        with self.assertRaisesRegex(WireValidationError,'PATH_ESCAPE'):verify_document(self.root,manifest,fixed=self.fixture.fixed)

    def test_duplicate_log_position_remains_a_fact_conflict(self):
        self.fixture.mutate_response('receipt',lambda row:row['result']['logs'].append(copy.deepcopy(row['result']['logs'][0])))
        with self.assertRaisesRegex(WireValidationError,'DUPLICATE_RECEIPT_LOG_INDEX'):self.fixture.verify()

    def test_removed_log_not_promoted_to_evidence(self):
        self.fixture.mutate_response('receipt',lambda row:row['result']['logs'][0].update(removed=True))
        with self.assertRaisesRegex(WireValidationError,'REMOVED_OR_UNKNOWN'):self.fixture.verify()

    def test_explicit_material_gap_does_not_claim_wire_available(self):
        manifest={'schema_version':SCHEMA,'fixed_identity':FIXED,'material_status':'MISSING_ORIGINAL_BODY',
                  'original_body_available':False,'gap_reason':'SYNTHETIC finite lookup found no original body'}
        result=verify_document(self.root,manifest,fixed=FIXED)
        self.assertEqual(result['status'],'PASS');self.assertFalse(result['original_body_available']);self.assertEqual(result['members_verified'],0)

    def test_duplicate_json_object_key_is_not_silently_overwritten(self):
        with self.assertRaisesRegex(WireValidationError,'DUPLICATE_JSON_OBJECT_KEY'):strict_json(b'{"id":"a","id":"b"}')

    def test_public_tree_without_private_manifest_fails_cli(self):
        source=Path(__file__).resolve().parents[1]/'src/verify_weth_shared_wire_r4_r1.py';output=self.root/'result.json'
        result=subprocess.run([sys.executable,'-B',str(source),'--tree',str(self.root),'--output',str(output)],capture_output=True,text=True)
        self.assertEqual(result.returncode,1);report=json.loads(output.read_text())
        self.assertEqual(report['status'],'FAIL');self.assertFalse(report['original_body_available']);self.assertEqual(report['members_verified'],0)

    def test_cli_explicit_gap_is_reported_separately_from_certification(self):
        path=self.root/MANIFEST;path.parent.mkdir(parents=True)
        path.write_text(json.dumps({'schema_version':SCHEMA,'fixed_identity':FIXED,'material_status':'MISSING_ORIGINAL_BODY',
                       'original_body_available':False,'gap_reason':'SYNTHETIC missing original file'}))
        source=Path(__file__).resolve().parents[1]/'src/verify_weth_shared_wire_r4_r1.py';output=self.root/'result.json'
        result=subprocess.run([sys.executable,'-B',str(source),'--tree',str(self.root),'--output',str(output)],capture_output=True,text=True)
        self.assertEqual(result.returncode,0);report=json.loads(output.read_text());self.assertEqual(report['material_status'],'MISSING_ORIGINAL_BODY')
        self.assertFalse(report['original_body_available']);self.assertFalse(report['weth_certification_predicates_changed'])

    def test_cli_cannot_overwrite_existing_material_as_output(self):
        path=self.root/MANIFEST;path.parent.mkdir(parents=True);path.write_text('{}');before=path.read_bytes()
        source=Path(__file__).resolve().parents[1]/'src/verify_weth_shared_wire_r4_r1.py'
        result=subprocess.run([sys.executable,'-B',str(source),'--tree',str(self.root),'--output',str(path)],capture_output=True,text=True)
        self.assertNotEqual(result.returncode,0);self.assertEqual(path.read_bytes(),before)


if __name__=='__main__':unittest.main()
