from pathlib import Path
from copy import deepcopy
import importlib.util
import json
import sys
import tempfile
import unittest
from unittest.mock import patch

STAGE=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(STAGE/'src')]
import stage1d_transfers_acquisition as a



class LegacyBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Entirely synthetic fixtures; no private archive or real chain file.
        cls.records=[];cls.inputs={};tx='0x'+'a'*64;block_hash='0x'+'b'*64
        plans=[{'method':'eth_getBlockByNumber','params':['0x100',False]},
               {'method':'eth_getTransactionByHash','params':[tx]},
               {'method':'eth_getTransactionReceipt','params':[tx]}]
        values=[{'number':'0x100','hash':block_hash,'timestamp':'0x6553f100','transactions':[tx]},
                {'hash':tx,'blockNumber':'0x100','blockHash':block_hash,'transactionIndex':'0x0',
                 'from':'0x'+'1'*40,'to':'0x'+'2'*40,'value':'0x1'},
                {'transactionHash':tx,'blockNumber':'0x100','blockHash':block_hash,'transactionIndex':'0x0',
                 'status':'0x1','gasUsed':'0x5208','effectiveGasPrice':'0x2'}]
        encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
        entries=[];responses=[];raw_hashes=[]
        for idx,(plan,value) in enumerate(zip(plans,values)):
            response={'id':idx+100,'jsonrpc':'2.0','result':value};responses.append(response)
            raw=encode(response);rh=a.digest(response);raw_hashes.append(rh)
            cls.inputs['raw/legacy_rpc/'+rh+'.json']=raw
            entries.append({'file':rh.upper()+'.json','bytes':len(raw),'method':plan['method'],
                            'request_sha256':a.digest({'chain_id':1,**plan}).upper(),'response_sha256':rh.upper()})
        manifest={'sealed':True,'entries':entries};mh=a.digest(manifest);mb=encode(manifest)
        mn='private/stage1d_legacy_import/manifests/'+mh+'.json';cls.inputs[mn]=mb
        for idx,(plan,response,rh) in enumerate(zip(plans,responses,raw_hashes)):
            key=a.logical_key(a.TransfersRuntime().rpc_identity(a.PROVIDER,plan))
            source={'raw_path':'D:/SYNTHETIC_ONLY/'+rh.upper()+'.json','raw_sha256':rh,'raw_bytes':len(encode(response)),
                    'manifest_path':'D:/SYNTHETIC_ONLY/cache_manifest.json','manifest_sha256':mh,'manifest_bytes':len(mb),
                    'manifest_ordinal':idx,'same_response_manifest_locations':1,'request_parameters_reconstructed_by_exact_hash':True}
            need={'method':plan['method'],'params':plan['params'],'expected_block':256,'reason':'SYNTHETIC_UNIT_TEST'}
            admission={'schema':'stage1d-legacy-rpc-point-admission-v1','status':'ADMISSIBLE_POINT_PENDING_ROOT_APPLY',
                       'freeze_sha256':a.FREEZE_SHA,'logical_key':key,'legacy_source':source,'plan':plan,
                       'request_sha256':a.digest({'chain_id':1,**plan}),'coverage_complete':False,'need':need,
                       'normalization':{'normalizer':'SYNTHETIC_STRICT_NORMALIZER'},
                       **{k:0 for k in ('new_network_requests','new_rpc_operations','new_alchemy_cu','new_bigquery_scanned_bytes')}}
            admission_key=a.digest({'logical_key':key,'need':need,'raw_sha256':rh})
            an='private/stage1d_legacy_import/admissions/'+admission_key+'.json';cls.inputs[an]=encode(admission)
            en='private/stage1d_legacy_import/envelopes/'+key+'_'+rh+'.json'
            envelope={'schema':admission['schema'],'legacy_source':source,'provider_alias':a.PROVIDER,
                      'evidence_kind':'REAL_CHAIN_LEGACY_RAW_REUSE','original_http_status_not_preserved':True,
                      'import_is_new_provider_dispatch':False,'response_complete':True,'status':'SUCCESS_VALIDATED',
                      'request':dict(plan,id=response['id'],jsonrpc='2.0'),'response':response,
                      'raw_path':'raw/legacy_rpc/'+rh+'.json','raw_body_sha256':rh,'manifest_path':mn,
                      'strict_normalizer':'SYNTHETIC_STRICT_NORMALIZER'}
            cls.inputs[en]=encode(envelope)
            receipt={'artifact_path':en,'artifact_sha256':a.digest(envelope),'admission_path':an,'admission_sha256':a.digest(admission),
                     'legacy_raw_sha256':rh,'local_import':True,'new_network_requests':0,'new_alchemy_cu':0}
            cls.records.append({'plan':plan,'receipt':receipt})

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=STAGE,prefix='legacy_binding_scratch_');self.work=Path(self.tmp.name)
        for name,payload in self.inputs.items():
            path=self.work/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(payload)
        self.row=deepcopy(self.records[0]);self.plan=self.row['plan']
        self.member=dict(self.row['receipt'],cache_hit=True,status='SUCCESS_VALIDATED')
        self.env=json.loads((self.work/self.member['artifact_path']).read_bytes())
        self.member['result']=deepcopy(self.env['response']['result'])

    def tearDown(self):self.tmp.cleanup()

    def save_envelope(self):
        path=self.work/self.member['artifact_path'];path.write_text(json.dumps(self.env,sort_keys=True),encoding='utf-8')
        self.member['artifact_sha256']=a.sha(path)

    def alter_admission(self,change):
        path=self.work/self.member['admission_path'];value=json.loads(path.read_bytes());change(value)
        path.write_text(json.dumps(value,sort_keys=True),encoding='utf-8');self.member['admission_sha256']=a.sha(path)

    def test_three_synthetic_point_methods_accept_without_http200_claim(self):
        original_open=Path.open;opened=[]
        def current_only(path,*args,**kwargs):
            path.resolve().relative_to(self.work.resolve());opened.append(path)
            return original_open(path,*args,**kwargs)
        with patch.object(Path,'open',current_only):
            for row in self.records:
                member=dict(row['receipt'],cache_hit=True,status='SUCCESS_VALIDATED')
                result=a.verified_member(self.work,row['plan'],member)
                env=json.loads((self.work/member['artifact_path']).read_bytes())
                self.assertEqual(result,env['response']['result']);self.assertNotIn('http_status',env)
        self.assertTrue(opened)

    def test_rpc_error_response_rejected(self):
        self.env['response'].pop('result');self.env['response']['error']={'code':-32000,'message':'SYNTHETIC_ERROR'}
        self.save_envelope()
        with self.assertRaisesRegex(ValueError,'request/result'):a.verified_member(self.work,self.plan,self.member)

    def test_no_network_member_exception(self):
        self.member['cache_hit']=False
        with self.assertRaises(ValueError):a.verified_member(self.work,self.plan,self.member)

    def test_transfers_page_cannot_use_legacy_form(self):
        plan={'method':a.METHOD,'params':[{}]};self.env['request'].update(plan);self.save_envelope()
        with self.assertRaises(ValueError):a.verified_member(self.work,plan,self.member)

    def test_unknown_legacy_schema_rejected(self):
        self.env['schema']='unknown';self.save_envelope()
        with self.assertRaises(ValueError):a.verified_member(self.work,self.plan,self.member)

    def test_wrong_plan_rejected(self):
        plan=deepcopy(self.plan);plan['params'][0]='0x1'
        with self.assertRaises(ValueError):a.verified_member(self.work,plan,self.member)

    def test_changed_raw_bytes_rejected(self):
        (self.work/self.env['raw_path']).write_bytes(b'{}')
        with self.assertRaisesRegex(ValueError,'SHA-bound'):a.verified_member(self.work,self.plan,self.member)

    def test_changed_manifest_rejected(self):
        (self.work/self.env['manifest_path']).write_bytes(b'{}')
        with self.assertRaisesRegex(ValueError,'SHA-bound'):a.verified_member(self.work,self.plan,self.member)

    def test_unbound_admission_rejected(self):
        self.member['admission_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'SHA-bound'):a.verified_member(self.work,self.plan,self.member)

    def test_wrong_admission_request_rejected(self):
        self.alter_admission(lambda row:row.update(plan={'method':'eth_getBlockByNumber','params':['0x1',False]}))
        with self.assertRaisesRegex(ValueError,'durable admission'):a.verified_member(self.work,self.plan,self.member)

    def test_wrong_admission_freeze_rejected(self):
        self.alter_admission(lambda row:row.update(freeze_sha256='0'*64))
        with self.assertRaisesRegex(ValueError,'durable admission'):a.verified_member(self.work,self.plan,self.member)

    def test_changed_result_rejected(self):
        self.member['result']['hash']='0x'+'0'*64
        with self.assertRaisesRegex(ValueError,'request/result'):a.verified_member(self.work,self.plan,self.member)

    def test_old_unknown_http_status_must_not_be_forged(self):
        self.env['http_status']=200;self.save_envelope()
        with self.assertRaises(ValueError):a.verified_member(self.work,self.plan,self.member)

    def test_wrong_manifest_ordinal_after_consistent_envelope_edit(self):
        self.env['legacy_source']['manifest_ordinal']+=1;self.save_envelope()
        self.alter_admission(lambda row:row.update(legacy_source=deepcopy(self.env['legacy_source'])))
        with self.assertRaisesRegex(ValueError,'manifest request/payload'):a.verified_member(self.work,self.plan,self.member)

    def test_copied_payload_path_cannot_escape(self):
        self.env['raw_path']='../outside.json';self.save_envelope()
        with self.assertRaisesRegex(ValueError,'copied paths'):a.verified_member(self.work,self.plan,self.member)


if __name__=='__main__':unittest.main(verbosity=2)
