"""Synthetic finite original-page closure; never follow descriptive live paths."""
import copy,json,unittest
from pathlib import Path
from unittest.mock import patch
import test_stage1d_batch_binding_route as fixtures
import stage1d_batch_binding_route as b
import stage1d_bq_context_prepare as h
from stage1d_bq_portable import verify_transaction_family_documents


class Tests(unittest.TestCase):
    def setUp(self):
        self.f=fixtures.BatchBindingTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.w=self.f.w
        self.stale='derived/old/live_collection.json'
        path=self.w/self.stale;path.parent.mkdir(parents=True);path.write_text('{"current":"changed"}')
        # The source snapshot is current and pinned. Its nested prior locator is
        # descriptive historical data, exactly as a frozen collection snapshot.
        snapshot={'current_snapshot':'CONTROLLED_ONLY','prior_collection':{'path':self.stale,'sha256':'0'*64},
                  'earlier_note':{'artifact_path':'private/absent_history.json','artifact_sha256':'1'*64}}
        (self.w/'CURRENT.json').write_text(json.dumps(snapshot))
        need=h.read(self.w/'NEEDS.json');need['scope_dependencies']=[h.dep(self.w,'CURRENT.json')]
        (self.w/'NEEDS.json').write_text(json.dumps(need))
        for dep in self.f.evidence:
            env=h.read(self.w/dep['path']);env['unrelated_provenance']={'path':self.stale,'sha256':'0'*64}
            rawpath=self.w/(dep['path']+'.provider.json');rawpath.write_text(json.dumps(env['result']))
            env['raw_sources']=[dict(h.dep(self.w,rawpath),http_status=200,complete=True)]
            (self.w/dep['path']).write_text(json.dumps(env));dep.update(h.dep(self.w,dep['path']))
        self.manifest=self.f.prepare()

    def saved(self,rows=None):
        self.state,self.states=self.f.saved_export(self.manifest,rows or self.f.family())
        for page in self.state['pages']:
            ref=page['response_receipt'];path=self.w/ref['artifact_path'];env=h.read(path)
            env['old_local_annotation']={'path':self.stale,'sha256':'0'*64}
            path.write_text(json.dumps(env));ref['artifact_sha256']=h.sha(path)
        (self.w/next(iter(self.states.values()))).write_text(json.dumps(self.state))

    def collect(self):return b.collect_portable_dependencies(self.w,'batch/PREPARATION.json',self.states,'0x'+'b'*64)

    def test_pinned_scope_and_provider_snapshots_do_not_follow_stale_live_provenance(self):
        self.saved();visited=[];inside=b.transfers.inside
        def guarded(work,path):
            result=inside(work,path);visited.append(result)
            if result==self.w/self.stale or result==self.w/'private/absent_history.json':
                raise AssertionError('Historical provenance path was incorrectly accessed')
            return result
        with patch.object(b.transfers,'inside',side_effect=guarded):documents=self.collect()
        self.assertNotIn(self.stale,documents);self.assertNotIn('private/absent_history.json',documents)
        self.assertEqual(documents['CURRENT.json'],(self.w/'CURRENT.json').read_bytes())
        self.assertIn('schema0.json.provider.json',documents)
        result=verify_transaction_family_documents('batch/PREPARATION.json',self.states,'0x'+'b'*64,documents)
        self.assertEqual(result['full_tree_proof']['trace_rows'],2)
        self.assertEqual(result['jobs'][0]['page_count'],2)

    def test_each_real_protocol_dependency_mutation_still_rejects(self):
        self.saved();spec=h.read(self.w/self.manifest['plans'][0]['path'])
        env=h.read(self.w/self.state['pages'][1]['response_receipt']['artifact_path'])
        paths=['CURRENT.json',spec['sql_path'],self.manifest['plans'][0]['path'],'schema0.json',
            'schema0.json.provider.json','dry.json',self.state['plan_path'],
            str((self.w/next(iter(self.states.values()))).parent/'terminal_job.json'),
            self.state['pages'][1]['rows_path'],env['raw_sources'][0]['path'],self.state['rows_path']]
        for name in paths:
            path=self.w/name;original=path.read_bytes();path.write_bytes(original+b' ')
            try:
                with self.subTest(name=name),self.assertRaises(ValueError):self.collect()
            finally:path.write_bytes(original)

    def test_missing_actual_raw_page_is_not_treated_as_historical_annotation(self):
        self.saved();env=h.read(self.w/self.state['pages'][0]['response_receipt']['artifact_path'])
        path=self.w/env['raw_sources'][0]['path'];path.unlink()
        with self.assertRaises((ValueError,FileNotFoundError)):self.collect()

    def test_null_root_preserves_exact_original_rpc_envelopes_and_bodies(self):
        rows=self.f.family();rows[1]['trace_address']=None;self.saved(rows)
        txhash='0x'+'b'*64;blockhash='0x'+'a'*64
        tx={'hash':txhash,'chainId':'0x1','blockNumber':'0x64','blockHash':blockhash,'transactionIndex':'0x0',
            'from':self.f.fixture.a,'to':self.f.fixture.b,'value':'0x3','gas':'0xc350','input':'0x'}
        receipt={'transactionHash':txhash,'blockNumber':'0x64','blockHash':blockhash,'transactionIndex':'0x0',
            'status':'0x1','from':tx['from'],'to':tx['to'],'gasUsed':'0x5208','effectiveGasPrice':'0x2','logs':[]}
        from datetime import datetime
        header={'number':'0x64','hash':blockhash,'timestamp':hex(int(datetime.fromisoformat('2024-08-21T01:30:00+00:00').timestamp())),
                'transactions':[txhash]}
        methods=[('eth_getTransactionByHash',[txhash],tx),('eth_getTransactionReceipt',[txhash],receipt),
                 ('eth_getBlockByNumber',['0x64',False],header)]
        members={}
        for i,(method,params,value) in enumerate(methods):
            folder=self.w/'controlled_rpc'/str(i);folder.mkdir(parents=True)
            plan={'method':method,'params':params};request=dict(plan,id=i,jsonrpc='2.0')
            response={'id':i,'jsonrpc':'2.0','result':value};raw=json.dumps([response]).encode()
            (folder/'response_body.bin').write_bytes(raw)
            env={'provider_alias':'ALCHEMY_ETH_MAINNET_EXISTING','evidence_kind':'REAL_CHAIN','http_status':200,
                 'response_complete':True,'status':'SUCCESS_VALIDATED','request':request,'response':response,
                 'raw_body_sha256':h.sha(folder/'response_body.bin'),
                 'old_note':{'path':self.stale,'sha256':'0'*64}}
            (folder/'envelope.json').write_text(json.dumps(env))
            members[method]={'artifact_path':h.dep(self.w,folder/'envelope.json')['path'],
                'artifact_sha256':h.sha(folder/'envelope.json'),'cache_hit':True,'result':value}
        with patch.object(b.transfers,'cached_member',side_effect=lambda work,plan:members[plan['method']]):
            documents=self.collect()
        for i in range(3):
            self.assertIn('controlled_rpc/'+str(i)+'/response_body.bin',documents)
            self.assertIn('controlled_rpc/'+str(i)+'/envelope.json',documents)
        result=verify_transaction_family_documents('batch/PREPARATION.json',self.states,txhash,documents)
        self.assertEqual(result['root_bindings'][0]['binding']['original_trace_address'],None)
        documents['controlled_rpc/0/response_body.bin']+=b' '
        with self.assertRaises(ValueError):verify_transaction_family_documents('batch/PREPARATION.json',self.states,txhash,documents)


if __name__=='__main__':unittest.main()
