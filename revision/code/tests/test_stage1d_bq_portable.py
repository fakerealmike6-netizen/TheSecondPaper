"""Memory-only synthetic transport-contract regression; no acquired facts."""
import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from stage1d_bq_portable import Documents,digest,verify_export_documents,verify_transaction_family_documents,semantic_materials


def fixture():
    docs={}
    def put(path,value,raw=False):
        data=value if raw else json.dumps(value,sort_keys=True,separators=(',',':')).encode()
        docs[path]=data
        return {'path':path,'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}
    dep=put('controlled/needed.json',{'evidence_kind':'SYNTHETIC_CONTROLLED'})
    sql=put('controlled/query.sql',b'SELECT 1 AS value_raw, TRUE AS success\n',True)
    spec=put('controlled/spec.json',{'sql_path':sql['path'],'sql_sha256':sql['sha256'],
        'scope_dependencies':[dep],'schema_evidence':[],'scope_hash':'controlled-only',
        'canonical_columns':['value_raw','success']})
    dry=put('controlled/dry.json',{'status':'SUCCESS_VALIDATED','identity':{
        'sql_sha256':sql['sha256'],'spec_sha256':spec['sha256'],'scope_hash':'controlled-only','project':'controlled-project'},
        'result':{'kind':'DRY_RUN','actual_query_executed':False,'estimated_processed_bytes':0}})
    plan=put('controlled/plan.json',{'dry_spec_path':spec['path'],'dry_spec_sha256':spec['sha256'],
        'dry_receipt_path':dry['path'],'dry_receipt_sha256':dry['sha256']})
    job='stage1d_recovery_'+digest({'sql_sha256':sql['sha256'],'project':'controlled-project','location':'US'})[:48]
    ref={'jobId':job,'projectId':'controlled-project','location':'US'}
    terminal=put('controlled/job/terminal_job.json',{'jobReference':ref,'status':{'state':'DONE'},
        'configuration':{'query':{'query':docs[sql['path']].decode(),'useLegacySql':False}}})
    schema={'fields':[{'name':'value_raw','type':'STRING'},{'name':'success','type':'BOOLEAN'}]}
    pages=[];combined=b''
    for i,(token,next_token) in enumerate(((None,'second'),('second',None))):
        raw={'jobReference':ref,'jobComplete':True,'totalRows':'2','schema':schema,
             'rows':[{'f':[{'v':str(i+1)},{'v':'true'}]}]}
        if next_token is not None:raw['pageToken']=next_token
        rawdep=put('controlled/job/page'+str(i)+'.bin',raw)
        env=put('controlled/job/env'+str(i)+'.json',{'status':'SUCCESS_VALIDATED','identity':{
            'method':'jobs.getQueryResults','project':'controlled-project','location':'US',
            'selectors':{'job_id':job,'pageToken':token}},'result':raw,
            'raw_sources':[dict(rawdep,complete=True,http_status=200)]})
        line=(json.dumps({'value_raw':str(i+1),'success':True},sort_keys=True)+'\n').encode();combined+=line
        rowdep=put('controlled/job/rows'+str(i)+'.jsonl',line,True)
        pages.append({'page_token':token,'next_page_token':next_token,'row_count':1,
            'rows_path':rowdep['path'],'rows_sha256':rowdep['sha256'],
            'response_receipt':{'artifact_path':env['path'],'artifact_sha256':env['sha256']}})
    rowdep=put('controlled/job/all.jsonl',combined,True)
    state={'state':'COMPLETE_EXPORTED','complete_page_chain':True,'job_id':job,
        'sql_sha256':sql['sha256'],'plan_path':plan['path'],'plan_sha256':plan['sha256'],
        'terminal_job_sha256':terminal['sha256'],'schema':schema,'pages':pages,'total_rows':2,
        'rows_path':rowdep['path'],'rows_sha256':rowdep['sha256']}
    put('controlled/job/state.json',state)
    return docs,spec,state


class Tests(unittest.TestCase):
    def test_two_original_pages_and_precision(self):
        docs,spec,state=fixture();store=Documents(docs)
        out=verify_export_documents(store,'controlled/job/state.json',spec)
        self.assertEqual([r['value_raw'] for r in out['rows']],['1','2'])
        self.assertEqual(out['page_count'],2);self.assertGreater(len(store.used),10)
    def test_path_escape_and_decoded_objects_rejected(self):
        for docs in ({'../escape':b'x'},{'D:/x':b'x'},{'a\\b':b'x'},{'a':{}}):
            with self.subTest(docs=docs),self.assertRaises(ValueError):Documents(docs)
    def test_missing_original_raw_page_rejected(self):
        docs,spec,state=fixture();del docs['controlled/job/page1.bin']
        with self.assertRaisesRegex(ValueError,'missing'):verify_export_documents(docs,'controlled/job/state.json',spec)
    def test_raw_envelope_or_row_changed_rejected(self):
        for name in ('page0.bin','env0.json','rows0.jsonl','all.jsonl','terminal_job.json'):
            docs,spec,state=fixture();docs['controlled/job/'+name]+=b' '
            with self.subTest(name=name),self.assertRaisesRegex(ValueError,'SHA'):verify_export_documents(docs,'controlled/job/state.json',spec)
    def test_state_flags_do_not_replace_actual_complete_chain(self):
        for mutation in ('reorder','truncated','count','same_token','last_token'):
            docs,spec,state=fixture()
            if mutation=='reorder':state['pages'].reverse()
            elif mutation=='truncated':state['pages'].pop()
            elif mutation=='count':state['total_rows']=3
            elif mutation=='same_token':state['pages'][1]['page_token']=None
            else:state['pages'][-1]['next_page_token']='unread'
            docs['controlled/job/state.json']=json.dumps(state).encode()
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):verify_export_documents(docs,'controlled/job/state.json',spec)
    def test_changed_job_or_spec_rejected(self):
        docs,spec,state=fixture();state['job_id']='otherjob';docs['controlled/job/state.json']=json.dumps(state).encode()
        with self.assertRaisesRegex(ValueError,'job identity'):verify_export_documents(docs,'controlled/job/state.json',spec)
        docs,spec,state=fixture();spec['sha256']='0'*64
        with self.assertRaises(ValueError):verify_export_documents(docs,'controlled/job/state.json',spec)


class FamilyTests(unittest.TestCase):
    def setUp(self):
        import test_stage1d_batch_binding_route as batch_tests
        self.f=batch_tests.BatchBindingTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.manifest=self.f.prepare()
        self.docs={p.relative_to(self.f.w).as_posix():p.read_bytes() for p in self.f.w.rglob('*') if p.is_file()}
        self.spec=self.manifest['plans'][0];self.tx='0x'+'b'*64
        self.states={self.spec['path']:'controlled/job/state.json'}

    def put(self,path,obj,raw=False):
        data=obj if raw else json.dumps(obj,sort_keys=True,separators=(',',':')).encode()
        self.docs[path]=data
        return {'path':path,'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}

    def job(self,rows):
        spec=json.loads(self.docs[self.spec['path']]);columns=spec['canonical_columns']
        rows=[{k:r.get(k) for k in columns} for r in rows]
        sql=self.docs[spec['sql_path']].decode();project='controlled-project'
        dry=self.put('controlled/dry.json',{'status':'SUCCESS_VALIDATED','identity':{
            'sql_sha256':spec['sql_sha256'],'spec_sha256':self.spec['sha256'],'scope_hash':spec['scope_hash'],'project':project},
            'result':{'kind':'DRY_RUN','actual_query_executed':False,'estimated_processed_bytes':0}})
        plan=self.put('controlled/plan.json',{'dry_spec_path':self.spec['path'],'dry_spec_sha256':self.spec['sha256'],
            'dry_receipt_path':dry['path'],'dry_receipt_sha256':dry['sha256']})
        job='stage1d_recovery_'+digest({'sql_sha256':spec['sql_sha256'],'project':project,'location':'US'})[:48]
        ref={'jobId':job,'projectId':project,'location':'US'}
        terminal=self.put('controlled/job/terminal_job.json',{'jobReference':ref,'status':{'state':'DONE'},
            'configuration':{'query':{'query':sql,'useLegacySql':False}}})
        schema={'fields':[{'name':k,'type':'BOOLEAN' if k in ('success','tx_success') else 'STRING'} for k in columns]}
        raw={'jobReference':ref,'jobComplete':True,'totalRows':str(len(rows)),'schema':schema,
            'rows':[{'f':[{'v':r[k]} for k in columns]} for r in rows]}
        rawdep=self.put('controlled/job/page.bin',raw)
        env=self.put('controlled/job/env.json',{'status':'SUCCESS_VALIDATED','identity':{
            'method':'jobs.getQueryResults','project':project,'location':'US','selectors':{'job_id':job}},
            'result':raw,'raw_sources':[dict(rawdep,complete=True,http_status=200)]})
        rowsdep=self.put('controlled/job/rows.jsonl',b''.join((json.dumps(r,sort_keys=True)+'\n').encode() for r in rows),True)
        self.put('controlled/job/state.json',{'state':'COMPLETE_EXPORTED','complete_page_chain':True,'job_id':job,
            'sql_sha256':spec['sql_sha256'],'plan_path':plan['path'],'plan_sha256':plan['sha256'],
            'terminal_job_sha256':terminal['sha256'],'schema':schema,'total_rows':len(rows),
            'rows_path':rowsdep['path'],'rows_sha256':rowsdep['sha256'],'pages':[{
                'page_token':None,'next_page_token':None,'row_count':len(rows),'rows_path':rowsdep['path'],
                'rows_sha256':rowsdep['sha256'],'response_receipt':{'artifact_path':env['path'],'artifact_sha256':env['sha256']}}]})

    def verify(self):return verify_transaction_family_documents('batch/PREPARATION.json',self.states,self.tx,self.docs)

    def root_points(self):
        from datetime import datetime
        from stage1d_bq_root_binding import _bind
        result=verify_export_documents(self.docs,'controlled/job/state.json',self.spec)
        rows=[r for r in result['rows'] if r['record_type']=='trace'];row=rows[0]
        block=int(row['block_number']);blockhash=row['block_hash'];sender=row['from_address'];recipient=row['to_address']
        stamp=int(datetime.fromisoformat(row['block_time'].replace('Z','+00:00')).timestamp())
        tx={'hash':self.tx,'chainId':'0x1','blockNumber':hex(block),'blockHash':blockhash,'transactionIndex':'0x0',
            'from':sender,'to':recipient,'value':hex(int(row['value_raw'])),'gas':'0xc350','input':row['input_data']}
        receipt={'transactionHash':self.tx,'blockNumber':hex(block),'blockHash':blockhash,'transactionIndex':'0x0',
            'from':sender,'to':recipient,'status':'0x1','gasUsed':'0x5208','effectiveGasPrice':'0x2','logs':[]}
        header={'number':hex(block),'hash':blockhash,'timestamp':hex(stamp),'transactions':[self.tx]}
        plans=[{'method':'eth_getTransactionByHash','params':[self.tx]},
               {'method':'eth_getTransactionReceipt','params':[self.tx]},
               {'method':'eth_getBlockByNumber','params':[hex(block),False]}]
        requests=[]
        for i,(plan,value) in enumerate(zip(plans,[tx,receipt,header])):
            request=dict(plan,id=i,jsonrpc='2.0');response={'id':i,'jsonrpc':'2.0','result':value}
            raw=self.put('controlled/rpc/'+str(i)+'/response_body.bin',[response])
            env=self.put('controlled/rpc/'+str(i)+'/member.json',{'provider_alias':'ALCHEMY_ETH_MAINNET_EXISTING',
                'evidence_kind':'REAL_CHAIN','http_status':200,'response_complete':True,'status':'SUCCESS_VALIDATED',
                'request':request,'response':response,'raw_body_sha256':raw['sha256']})
            requests.append({'plan':plan,'member':{'artifact_path':env['path'],'artifact_sha256':env['sha256'],
                'cache_hit':True,'result':value}})
        evidence=sorted({result['evidence_id']}|{'sha256:'+r['member']['artifact_sha256'] for r in requests})
        binding=_bind(rows,tx,receipt,header,evidence)['root_binding']
        self.put('_portable/ROOT_RPC_BINDINGS.json',{'schema_version':'stage1d-portable-root-rpc-bindings-v1',
            'tx_hash':self.tx,'root_bindings':[{'tx_hash':self.tx,'binding':binding,'requests':requests}]})

    def test_actual_preparer_full_sql_raw_pages_and_tree(self):
        self.job(self.f.family());result=self.verify()
        self.assertEqual(result['full_tree_proof']['trace_rows'],2)
        self.assertFalse(result['full_context_claimed']);self.assertEqual(result['covered_ranges'],0)
        self.assertEqual(len(result['rows']),3)
        self.assertTrue(all(r['evidence_ids'][0].startswith('BIGQUERY_JOB:') for r in result['rows']))

    def test_success_flags_cannot_hide_missing_actual_sibling(self):
        rows=self.f.family();rows.pop();self.job(rows)
        with self.assertRaisesRegex(ValueError,'gaps'):self.verify()

    def test_unbound_null_root_is_not_empty_path(self):
        rows=self.f.family();rows[1]['trace_address']=None;self.job(rows)
        with self.assertRaisesRegex(ValueError,'missing'):self.verify()

    def test_null_root_recomputed_from_original_three_rpc_points(self):
        rows=self.f.family();rows[1]['trace_address']=None;self.job(rows);self.root_points()
        result=self.verify()
        self.assertEqual(result['root_bindings'][0]['binding']['original_trace_address'],None)
        self.assertEqual(result['full_tree_proof']['trace_rows'],2)
        # Portable verifier retains the accepted old point cache contract.
        del self.docs['controlled/rpc/0/response_body.bin']
        self.assertEqual(self.verify()['rows'],result['rows'])

    def test_saved_root_boolean_or_changed_point_cannot_certify(self):
        rows=self.f.family();rows[1]['trace_address']=None;self.job(rows);self.root_points()
        manifest=json.loads(self.docs['_portable/ROOT_RPC_BINDINGS.json'])
        manifest['root_bindings'][0]['binding']['internal_paths_modified']=True
        self.put('_portable/ROOT_RPC_BINDINGS.json',manifest)
        with self.assertRaisesRegex(ValueError,'recomputed'):self.verify()
        self.root_points();self.docs['controlled/rpc/1/response_body.bin']+=b' '
        with self.assertRaisesRegex(ValueError,'SHA'):self.verify()

    def test_full_tree_does_not_supply_missing_calldata(self):
        rows=self.f.family();rows[1]['input_data']=None;self.job(rows);result=self.verify()
        from datetime import datetime
        policy={'tx_hash':self.tx,'block_number':100,'block_hash':'0x'+'a'*64,'tx_index':0,
            'timestamp':int(datetime.fromisoformat('2024-08-21T01:30:00+00:00').timestamp())}
        with self.assertRaisesRegex(ValueError,'calldata'):semantic_materials(result,policy)

    def test_semantic_tree_retains_bq_provenance_no_frame_logs(self):
        self.job(self.f.family());result=self.verify()
        from datetime import datetime
        policy={'tx_hash':self.tx,'block_number':100,'block_hash':'0x'+'a'*64,'tx_index':0,
            'timestamp':int(datetime.fromisoformat('2024-08-21T01:30:00+00:00').timestamp())}
        material=semantic_materials(result,policy)
        self.assertEqual(material['trace']['evidence_provider'],'CLASSIC_BIGQUERY_FULL_TRANSACTION_FAMILY_V1')
        self.assertEqual(material['trace']['logs'],[]);self.assertEqual(material['trace']['calls'][0]['logs'],[])
        self.assertEqual(material['internal']['result'][0]['trace_address'],[0])
        policy['timestamp']+=1
        with self.assertRaises(ValueError):semantic_materials(result,policy)


if __name__=='__main__':unittest.main()
