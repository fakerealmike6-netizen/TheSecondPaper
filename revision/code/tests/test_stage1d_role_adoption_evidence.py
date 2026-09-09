"""Synthetic raw-source role proof tests; no public source is fetched."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from stage1d_role_adoption import (SCHEMA, IMPLEMENTATION_SLOT, validate_certificate,
                                  TechnicalRoles, ordinary, sha)

ADDR='0x'+'1'*40; PAYER='0x'+'2'*40; IMPL='0x'+'3'*40
TX='0x'+'a'*64; BH='0x'+'b'*64


def fixture(root, *, proxy=False, mutation=None, supported=False, public_text=False):
    root=Path(root);base=root/'private/stage1d_roles';base.mkdir(parents=True)
    def save(name,value):
        path=base/(name+'.json');path.write_text(json.dumps(value),encoding='utf-8')
        return {'path':path.relative_to(root).as_posix(),'sha256':sha(path)}
    def env(method,params,result):return {'request':{'id':1,'method':method,'params':params},'response':{'id':1,'result':result}}
    tx={'hash':TX,'chainId':'0x1','from':PAYER,'to':ADDR,'value':'0x6','input':'0x' if not supported else '0xdeadbeef'+'0'*64,
        'blockNumber':'0xa','blockHash':BH,'transactionIndex':'0x1'}
    receipt={'transactionHash':TX,'status':'0x1','blockNumber':'0xa','blockHash':BH,'transactionIndex':'0x1'}
    code='0x6000f4' if proxy else '0x6001600055'
    body={'network':'Ethereum','deployments':[{'chain_id':1,'address':ADDR,'role':'Bridge contract'}]}
    raws={'historical_transaction':env('eth_getTransactionByHash',[TX],tx),
          'historical_receipt':env('eth_getTransactionReceipt',[TX],receipt),
          'historical_header':env('eth_getBlockByNumber',['0xa',False],{'number':'0xa','hash':BH,'timestamp':'0x64'}),
          'historical_code':env('eth_getCode',[ADDR,'0xa'],code),
          'official_deployment':{'source_url':'https://official.example/deployments','body':json.dumps(body)}}
    if supported:
        raws['documented_abi']={'source_url':'https://official.example/abi',
            'body':json.dumps([{'type':'function','name':'route','inputs':[{'type':'uint256'}]}])}
    if proxy:
        raws['proxy_storage']=env('eth_getStorageAt',[ADDR,IMPLEMENTATION_SLOT,'0xa'],'0x'+'0'*24+IMPL[2:])
        raws['implementation_code']=env('eth_getCode',[IMPL,'0xa'],'0x6001600055')
    if mutation=='wrong_code_block':raws['historical_code']['request']['params'][1]='0xb'
    if mutation=='receipt_failed':receipt['status']='0x0'
    if mutation=='wrong_recipient':tx['to']=IMPL
    if mutation=='wrong_block_hash':receipt['blockHash']='0x'+'c'*64
    if mutation=='wrong_chain':tx['chainId']='0x38'
    if mutation=='not_official':raws['official_deployment']['source_url']='https://attacker.example/deployments'
    if mutation=='other_chain_record':
        body['deployments'][0]['chain_id']=56;raws['official_deployment']['body']=json.dumps(body)
    if mutation=='name_only':raws['official_deployment']['body']='Ethereum Bridge contract named similarly, no deployed address.'
    if mutation=='wrong_proxy_slot':raws['proxy_storage']['response']['result']='0x'+'0'*64
    if mutation=='claimed_nonproxy':pass
    refs={k:{'kind':k,**save(k,v)} for k,v in raws.items()}
    if public_text:
        text=base/'body_1.txt';text.write_text(raws['official_deployment']['body'],encoding='utf-8')
        url=raws['official_deployment']['source_url'];h=sha(text)
        journal={'url':url,'source_url':url,'status':'SUCCESS','body_path':text.name,'body_sha256':h,
                 'attempts':[{'status':'SUCCESS','transport_called':True,'http_status':200,'request_url':url,'final_url':url,
                              'body_path':text.name,'body_sha256':h,'bytes':text.stat().st_size}]}
        if mutation=='redirected_journal':journal['attempts'][0]['final_url']='https://attacker.example/'
        refs['official_deployment']={'kind':'official_deployment','format':'public_text','path':text.relative_to(root).as_posix(),
                                     'sha256':h,'journal':save('journal',journal)}
    authority={'schema_version':'stage1d-role-authorities-v1','authorities':[{
        'protocol_id':'controlled-bridge','allowed_origins':['https://official.example'],
        'ethereum_markers':['Ethereum'],'deployment_context_markers':['Bridge contract'],
        'deployment_json_pointer':'/deployments/0',
        'technical_role_statuses':['VERIFIED_PROTOCOL_ROLE','VERIFIED_SUPPORTED_COMPONENT_CONTRACT'],
        'documented_methods':[{'selector':'0xdeadbeef','signature':'route(uint256)'}]}]}
    save('AUTHORITIES',authority)
    common={'chain_id':'eip155:1','address':ADDR}
    deployment={**common,'basis':'OFFICIAL_CHAIN_DEPLOYMENT_AND_HISTORICAL_BINDING','source_refs':[refs['official_deployment']]}
    operation={**common,'block':10,'tx_hash':TX,'exact_trace_locator':'top','source_refs':[
        refs[k] for k in ('historical_transaction','historical_receipt','historical_header')]+([refs['documented_abi']] if supported else [])}
    digest=hashlib.sha256(bytes.fromhex(code[2:])).hexdigest()
    code_binding={**common,'code_sha256':digest,'source_refs':[refs['historical_code']]}
    if proxy:
        code_binding.update(is_proxy=mutation!='claimed_nonproxy',implementation_address=IMPL,
            implementation_code_sha256=hashlib.sha256(bytes.fromhex('6001600055')).hexdigest())
        code_binding['source_refs'] += [refs['proxy_storage'],refs['implementation_code']]
    dependencies=[{'kind':k,**save(k,v)} for k,v in [('deployment_binding',deployment),('historical_execution_binding',operation),('code_identity_binding',code_binding)]]
    certificate={'schema_version':SCHEMA,'chain_id':'eip155:1','address':ADDR,'certificate_id':'controlled-cert',
        'start_block':10,'end_block':10,'protocol_id':'controlled-bridge','code_sha256':digest,
        'technical_role_status':'VERIFIED_SUPPORTED_COMPONENT_CONTRACT' if supported else 'VERIFIED_PROTOCOL_ROLE',
        'branch_action':'SUPPORTED_OPERATION_RESOLVE' if supported else 'UNSUPPORTED_PROTOCOL_STOP','dependencies':dependencies}
    if supported:certificate['documented_selector']='0xdeadbeef'
    return certificate


class EvidenceRoleTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent)
        self.root=Path(self.temp.name)
    def tearDown(self):self.temp.cleanup()
    def test_official_stop_does_not_require_creation_or_abi(self):
        c=fixture(self.root)
        result=validate_certificate(self.root,c)
        self.assertEqual(result['verification']['status'],'PASS')
        self.assertFalse(result['verification']['authority']['abi_required'])
        self.assertEqual(c['dependencies'],result['dependencies'])
    def test_supported_operation_keeps_abi_requirement(self):
        c=fixture(self.root,supported=True)
        self.assertEqual(validate_certificate(self.root,c)['verification']['status'],'PASS')
        c['documented_selector']='0x12345678'
        with self.assertRaisesRegex(ValueError,'selector'):validate_certificate(self.root,c)
    def test_proxy_storage_and_code_bound_at_actual_block(self):
        c=fixture(self.root,proxy=True,mutation='claimed_nonproxy')
        self.assertTrue(validate_certificate(self.root,c)['verification']['proxy_implementation_required'])
    def test_two_endpoints_are_not_an_interval_proof(self):
        c=fixture(self.root);c['end_block']=11
        with self.assertRaisesRegex(ValueError,'single-block'):validate_certificate(self.root,c)
    def test_raw_rpc_originals_and_chain_record_required(self):
        mutations=['wrong_code_block','receipt_failed','wrong_recipient','wrong_block_hash','wrong_chain','not_official','other_chain_record','name_only']
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory(dir=self.root) as path:
                c=fixture(path,mutation=mutation)
                with self.assertRaises((ValueError,KeyError)):validate_certificate(path,c)
    def test_wrong_proxy_implementation_rejected(self):
        c=fixture(self.root,proxy=True,mutation='wrong_proxy_slot')
        with self.assertRaisesRegex(ValueError,'storage'):validate_certificate(self.root,c)
    def test_nonempty_source_refs_and_derived_booleans_do_not_certify(self):
        c=fixture(self.root);p=self.root/c['dependencies'][0]['path'];row=json.loads(p.read_text());row['source_refs']=['known official source'];p.write_text(json.dumps(row));c['dependencies'][0]['sha256']=sha(p)
        with self.assertRaisesRegex(ValueError,'raw source'):validate_certificate(self.root,c)
    def test_changed_raw_body_rejected(self):
        c=fixture(self.root);p=self.root/'private/stage1d_roles/historical_code.json';p.write_text('{}')
        with self.assertRaisesRegex(ValueError,'changed'):validate_certificate(self.root,c)
    def test_public_raw_text_binds_successful_request_journal(self):
        c=fixture(self.root,public_text=True)
        self.assertEqual(validate_certificate(self.root,c)['verification']['status'],'PASS')
    def test_unbound_redirect_journal_rejected(self):
        c=fixture(self.root,public_text=True,mutation='redirected_journal')
        with self.assertRaisesRegex(ValueError,'journal'):validate_certificate(self.root,c)
    def test_resolve_keeps_point_scope_and_service_precedence(self):
        c=validate_certificate(self.root,fixture(self.root));roles=TechnicalRoles(self.root);roles.records=[c]
        def state(block):return SimpleNamespace(address=ADDR,arrival=SimpleNamespace(chain_id='eip155:1',block=block))
        self.assertEqual(roles.resolve(state(10),{'kind':'UNKNOWN'})['branch_action'],'UNSUPPORTED_PROTOCOL_STOP')
        self.assertEqual(roles.resolve(state(11),{'kind':'UNKNOWN'})['branch_action'],'NORMAL_ACCOUNT_EXPAND')
        self.assertEqual(roles.resolve(state(10),{'kind':'SERVICE','actor':'custodian'})['actor'],'custodian')
    def test_behavior_tag_never_creates_protocol_role(self):
        self.assertEqual(ordinary({'kind':'UNKNOWN','name':'DEX Trader'})['branch_action'],'NORMAL_ACCOUNT_EXPAND')
    def test_internal_entering_event_needs_only_successful_path_not_component_tree(self):
        c=fixture(self.root);base=self.root/'private/stage1d_roles'
        txfile=base/'historical_transaction.json';raw=json.loads(txfile.read_text());raw['response']['result']['to']=IMPL
        txfile.write_text(json.dumps(raw));tx=raw['response']['result']
        rows=[{'traceAddress':[],'action':{k:tx[k] for k in ('from','to','input','value')},'result':{}},
              {'traceAddress':[0],'action':{'from':IMPL,'to':ADDR,'input':'0x','value':'0x6','callType':'call'},'result':{}}]
        trace=base/'historical_trace.json';trace.write_text(json.dumps({'request':{'method':'trace_transaction','params':[TX]},'response':{'result':rows}}))
        dep=c['dependencies'][1];p=self.root/dep['path'];op=json.loads(p.read_text());op['exact_trace_locator']=[0]
        for ref in op['source_refs']:
            if ref['kind']=='historical_transaction':ref['sha256']=sha(txfile)
        op['source_refs'].append({'kind':'historical_trace','path':trace.relative_to(self.root).as_posix(),'sha256':sha(trace)})
        p.write_text(json.dumps(op));dep['sha256']=sha(p)
        self.assertEqual(validate_certificate(self.root,c)['verification']['status'],'PASS')
        rows[0]['error']='Reverted';trace.write_text(json.dumps({'request':{'method':'trace_transaction','params':[TX]},'response':{'result':rows}}))
        op['source_refs'][-1]['sha256']=sha(trace);p.write_text(json.dumps(op));dep['sha256']=sha(p)
        with self.assertRaisesRegex(ValueError,'ancestors'):validate_certificate(self.root,c)
    def test_creation_route_uses_runtime_artifact_without_deployment_page(self):
        c=fixture(self.root);base=self.root/'private/stage1d_roles'
        def source(kind,value):
            p=base/(kind+'.json');p.write_text(json.dumps(value))
            return {'kind':kind,'path':p.relative_to(self.root).as_posix(),'sha256':sha(p)}
        ctx='0x'+'c'*64;cbh='0x'+'d'*64
        env=lambda m,p,r:{'request':{'method':m,'params':p},'response':{'result':r}}
        refs=[source('creation_transaction',env('eth_getTransactionByHash',[ctx],{'hash':ctx,'chainId':'0x1','from':PAYER,'to':None,'input':'0x6001600055','blockNumber':'0x2','blockHash':cbh,'transactionIndex':'0x1'})),
              source('creation_receipt',env('eth_getTransactionReceipt',[ctx],{'transactionHash':ctx,'contractAddress':ADDR,'status':'0x1','blockNumber':'0x2','blockHash':cbh,'transactionIndex':'0x1'})),
              source('creation_header',env('eth_getBlockByNumber',['0x2',False],{'number':'0x2','hash':cbh})),
              source('verified_implementation',{'source_url':'https://official.example/artifact','body':json.dumps({'contractName':'VerifiedBridge','deployedBytecode':'0x6001600055'})})]
        dep=c['dependencies'][0];p=self.root/dep['path'];value=json.loads(p.read_text())
        value.update(basis='VERIFIED_CONTRACT_CREATION_AND_IMPLEMENTATION',creation_block=2,creation_tx_hash=ctx,source_refs=refs)
        p.write_text(json.dumps(value));dep['sha256']=sha(p)
        registry=base/'AUTHORITIES.json';value=json.loads(registry.read_text());value['authorities'][0]['contract_names']=['VerifiedBridge'];registry.write_text(json.dumps(value))
        self.assertEqual(validate_certificate(self.root,c)['verification']['status'],'PASS')


if __name__=='__main__':unittest.main()
