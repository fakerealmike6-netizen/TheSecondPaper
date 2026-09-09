"""Entirely synthetic bytes exercising the real portable validation contract.

The local acquisition catalogue below is a test trust root, not acquired data.
No source, address, page or certificate in this module is a research input.
"""
import base64, copy, hashlib, json, unittest
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import stage1d_bq_portable as bq
from stage1d_shared_evidence import (BlobPool, SharedContextBuilder, SharedEvidenceContexts,
    pack_contexts, context_view, serialize_contexts, SCHEMA, REF)
from stage1d_semantic_units import (certify_instance, context_identity, validate_semantic_unit,
    FiniteSemanticResolver, REQUIRED_GATE_SOURCES, REQUIRED_CAPABILITIES, SEMANTIC_VERSION)
from weth_evidence import export_evidence_context, load_portable_evidence_context, payload_hash
from semantic_weth_fixture import materials, controlled_collection_inputs, SOURCE, WETH, HOLDER, SPONSOR, SERVICE

MEASUREMENTS={}


def encoded(raw): return base64.b64encode(raw).decode('ascii')
def sha(raw): return hashlib.sha256(raw).hexdigest()
def rawjson(obj): return json.dumps(obj,sort_keys=True,separators=(',',':')).encode()


class SharedTests(unittest.TestCase):
    def setUp(self):
        from test_stage1d_bq_portable import FamilyTests
        from collector import Scope
        import stage1d_bq_context_prepare as h
        self.f=FamilyTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        fixture=self.f.f
        q=dict(fixture.q,start_block=1,end_block=20,start_time_utc='1970-01-01T00:00:01Z',
               end_time_utc='1970-01-01T00:00:20Z')
        for k in ('scope','scope_id','scope_hash'):q.pop(k,None)
        scope=Scope.from_policy(q);q.update(scope_id=scope.scope_id,scope_hash=scope.scope_hash,scope=asdict(scope))
        fixture.q=q
        oldwork=fixture.w;fixture.w=oldwork/'shared_fixture';(fixture.w/'private').mkdir(parents=True)
        (fixture.w/'CURRENT.json').write_bytes((oldwork/'CURRENT.json').read_bytes())
        for dep in fixture.evidence:
            (fixture.w/dep['path']).write_bytes((oldwork/dep['path']).read_bytes())
        h.save(fixture.w/'private/BATCH_QUERY_FREEZE.json',{'queries':[q]})
        h.FREEZE_SHA=h.sha(fixture.w/'private/BATCH_QUERY_FREEZE.json')
        fixture.need.update(query_id=q['query_id'],scope_id=q['scope_id'],scope_hash=q['scope_hash'],
                            start_block=1,end_block=20,start_time=1,end_time=20)
        h.save(fixture.w/'NEEDS.json',dict(query_id=q['query_id'],scope_hash=q['scope_hash'],
            freeze_sha256=h.sha(fixture.w/'private/BATCH_QUERY_FREEZE.json'),need_rectangles=[fixture.need],
            scope_dependencies=[h.dep(fixture.w,'CURRENT.json')]))
        # The SQL must actually select trace input; NULL calldata is not usable.
        for dep in fixture.evidence:
            envelope=h.read(fixture.w/dep['path'])
            if envelope['result']['table']==h.TR:
                envelope['result']['schema'].append({'name':'input','type':'STRING'})
                (fixture.w/dep['path']).write_bytes(rawjson(envelope));dep.update(h.dep(fixture.w,dep['path']))
        self.f.manifest=fixture.prepare();self.f.spec=self.f.manifest['plans'][0]
        self.f.states={self.f.spec['path']:'controlled/job/state.json'}
        self.f.docs={p.relative_to(fixture.w).as_posix():p.read_bytes() for p in fixture.w.rglob('*') if p.is_file()}
        self.original=[materials(),materials('WITHDRAWAL',block=4,amount=4)]
        rows=[]
        for policy,context in self.original:
            tx=context['payloads']['transaction'];tree=context['payloads']['trace']
            common=dict(block_number=str(policy['block_number']),block_hash=policy['block_hash'],
                block_time=datetime.fromtimestamp(policy['timestamp'],timezone.utc).isoformat().replace('+00:00','Z'),
                tx_hash=policy['tx_hash'],tx_index=str(policy['tx_index']),success=True,
                from_address=tx['from'],to_address=tx['to'],value_raw=str(int(tx['value'],16)),input_data=tx['input'])
            rows.append(dict(common,record_type='transaction',gas_used='1',effective_gas_price='1',
                             gas_limit='50000',transaction_type='2'))
            def frames(frame,path=()):
                rows.append(dict(common,record_type='trace',from_address=frame['from'],to_address=frame['to'],
                    value_raw=str(int(frame['value'],16)),input_data=frame['input'],trace_address=json.dumps(list(path)),
                    trace_type='call',call_type='call',subtraces=str(len(frame.get('calls',[])))))
                for i,child in enumerate(frame.get('calls',[])):frames(child,path+(i,))
            frames(tree)
        self.f.job(rows)
        self.builder=SharedContextBuilder();self.units=[];self.portables=[]
        self.catalogue={'schema_version':'weth-acquisition-catalogue-1','evidence_kind':'REAL_CHAIN',
            'acquisition_run_id':'CONTROLLED_TEST_ONLY','reviewer_record_id':'TEST_TRUST_ROOT',
            'approved_provider_hosts':['rpc.ankr.com','api.etherscan.io'],'records':{}}
        self.bundles=[]
        acq=fixture.w/'acquisition';acq.mkdir()
        def record(role,request,payload,name,source_type=None):
            response=payload if role=='source' else {'jsonrpc':'2.0','id':request['id'],'result':payload}
            content=rawjson({'request':request,'response':response,'http_status':200})
            path=name+'.json';(acq/path).write_bytes(content)
            item={'role':role,'evidence_kind':'REAL_CHAIN','status':'SUCCESS_VALIDATED','chain_id':1,
                'origin_url':'https://api.etherscan.io' if role=='source' else 'https://rpc.ankr.com',
                'request':request,'artifact_path':path,'artifact_sha256':sha(content),'payload_sha256':payload_hash(payload)}
            if source_type:item['source_type']=source_type
            self.catalogue['records'][name]=item
            return name
        code=self.original[0][1]['payloads']['historical_code']
        source_id=record('source',{'params':{'action':'getsourcecode','address':WETH,'chainid':'1'}},
            {'status':'1','result':[{'SourceCode':SOURCE,'CompilerVersion':'v0.4.18','Proxy':'0','ABI':'[]'}]},
            'source','ETHERSCAN_VERIFIED_SOURCE')
        dep_id=record('deployment_runtime',{'id':1,'method':'eth_getCode','params':[WETH,'0x1']},code,'deployment')
        self.catalogue['source_review']={'review_id':'TEST_REVIEW','chain_id':1,'contract':WETH,
            'source_record_id':source_id,'source_text_sha256':sha(SOURCE.encode()),
            'semantics':'CANONICAL_WETH9_DEPOSIT_NO_FEE_NO_REFUND','deployment_runtime_match_evidence_id':dep_id,
            'runtime_code_sha256':sha(bytes.fromhex(code[2:])),'verification_block_number':1}
        for index,(policy,context) in enumerate(self.original):
            data=context['payloads'];records={'source':source_id,'deployment_runtime':dep_id}
            plans={'transaction':('eth_getTransactionByHash',[policy['tx_hash']]),
                'receipt':('eth_getTransactionReceipt',[policy['tx_hash']]),
                'header':('eth_getBlockByNumber',[hex(policy['block_number']),False]),
                'historical_code':('eth_getCode',[WETH,hex(policy['block_number'])])}
            for role,(method,params) in plans.items():
                records[role]=record(role,{'id':1,'method':method,'params':params},data[role],str(index)+'_'+role)
            path=acq/('bundle'+str(index)+'.json');path.write_bytes(rawjson({'records':records}));self.bundles.append(path)
        self.catalogue_path=acq/'catalogue.json';self.catalogue_path.write_bytes(rawjson(self.catalogue))
        self.local_docs=[]
        for index,(policy,_) in enumerate(self.original):
            docs=dict(self.f.docs)
            # Same relative path, different bytes is legitimate across contexts.
            docs[bq.ROOT_BINDINGS_PATH]=rawjson({'schema_version':'stage1d-portable-root-rpc-bindings-v1',
                'tx_hash':policy['tx_hash'],'root_bindings':[]})
            self.local_docs.append(docs)
            family=bq.export_family_documents('batch/PREPARATION.json',self.f.states,policy['tx_hash'],docs,
                blob_pool=self.builder.pool,validation_session=self.builder.validation_session)
            portable=export_evidence_context(self.bundles[index],self.catalogue_path,
                bigquery_family=family,bigquery_policy=policy,blob_pool=self.builder.pool,
                validation_session=self.builder.validation_session)
            self.portables.append(portable)
            key=self.builder.add(portable)
            verified=load_portable_evidence_context(portable,blob_reader=self.builder.pool.read,
                                                   validation_session=self.builder.validation_session)
            unit=certify_instance(policy,verified);self.assertEqual(unit['evidence_context_id'],key)
            self.units.append(unit)
        self.shared=self.builder.serialized()

    def test_two_physical_transactions_one_original_family_and_roundtrip(self):
        view=context_view(json.loads(json.dumps(self.shared)))
        for unit in self.units:self.assertTrue(validate_semantic_unit(unit,evidence_context=view)['passed'])
        self.assertEqual(view.validation_counts,{'job_verifications':1,'job_cache_hits':1,'tx_indexes':1})
        roots=[p['bigquery_extension']['family']['documents'][bq.ROOT_BINDINGS_PATH]['bytes_base64'][REF] for p in self.portables]
        self.assertNotEqual(*roots)
        pages=[p['bigquery_extension']['family']['documents']['controlled/job/page.bin']['bytes_base64'][REF] for p in self.portables]
        self.assertEqual(*pages)
        self.assertEqual(set(view),{u['evidence_context_id'] for u in self.units})
        # A new receiver proves the pages anew; persisted memo claims do not exist.
        other=context_view(json.loads(json.dumps(serialize_contexts(view))))
        other[self.units[0]['evidence_context_id']]
        self.assertEqual(other.validation_counts['job_verifications'],1)

    def test_page_changed_missing_blob_or_local_path_conflict_rejected(self):
        first=self.units[0]['evidence_context_id'];second=self.units[1]['evidence_context_id']
        for mutation in ('bytes','missing','path','identity','page_ref'):
            obj=copy.deepcopy(self.shared)
            doc=obj['contexts'][first]['bigquery_extension']['family']['documents']['controlled/job/page.bin']
            blob=doc['bytes_base64'][REF]
            if mutation=='bytes':obj['blobs'][blob]['bytes_base64']=encoded(b'altered original page')
            elif mutation=='missing':del obj['blobs'][blob]
            elif mutation=='path':
                docs=obj['contexts'][first]['bigquery_extension']['family']['documents']
                docs['../escape']=docs.pop('controlled/job/page.bin')
            elif mutation=='identity':obj['contexts'][first],obj['contexts'][second]=obj['contexts'][second],obj['contexts'][first]
            else:
                other=obj['contexts'][first]['bigquery_extension']['family']['documents']['controlled/job/env.json']
                doc['bytes_base64']=copy.deepcopy(other['bytes_base64'])
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):context_view(obj)[first]

    def test_cached_job_cannot_hide_second_context_changed_original_dependency(self):
        session=bq.ValidationSession()
        a,b=[p['tx_hash'] for p,_ in self.original]
        bq.verify_transaction_family_documents('batch/PREPARATION.json',self.f.states,a,self.local_docs[0],validation_session=session)
        docs=dict(self.local_docs[1]);docs['controlled/job/page.bin']+=b' '
        with self.assertRaisesRegex(ValueError,'SHA'):
            bq.verify_transaction_family_documents('batch/PREPARATION.json',self.f.states,b,docs,validation_session=session)
        self.assertEqual(session.counts['job_cache_hits'],0)

    def test_cached_original_rows_and_spec_cannot_be_mutated_by_a_previous_caller(self):
        store=bq.Documents(self.local_docs[0],bq.ValidationSession())
        original=bq.verify_export_documents(store,'controlled/job/state.json',self.f.spec)
        with self.assertRaises(TypeError):original['rows'][0]['value_raw']='999'
        with self.assertRaises(TypeError):original['spec']['scope_hash']='other'
        with self.assertRaises(TypeError):original['rows'][0]['evidence_ids'][0]='invented'
        self.assertIs(original['rows'][0],original['_tx_index'][original['rows'][0]['tx_hash']][0])

    def test_unpooled_original_unit_identity_is_exactly_preserved(self):
        for index,(policy,_) in enumerate(self.original):
            family=bq.export_family_documents('batch/PREPARATION.json',self.f.states,policy['tx_hash'],self.local_docs[index])
            portable=export_evidence_context(self.bundles[index],self.catalogue_path,
                bigquery_family=family,bigquery_policy=policy)
            self.assertEqual(certify_instance(policy,load_portable_evidence_context(portable)),self.units[index])
        # Schema adapter can also compact an existing portable mapping without
        # trusting any stored unit or changing the original-byte context hash.
        context=context_view(pack_contexts({self.units[index]['evidence_context_id']:portable}))
        self.assertTrue(validate_semantic_unit(self.units[index],evidence_context=context)['passed'])

    def test_current_catalogue_shared_entry_loads_and_binds_exact_instance_set(self):
        from stage1d_semantic_catalogue import load_current_resolver,SCHEMA as CATALOGUE_SCHEMA
        root,gate=self._gate();folder=root/'private/stage1d_semantics';folder.mkdir(parents=True)
        def put(name,value):
            data=rawjson(value);(folder/name).write_bytes(data)
            return {'path':'private/stage1d_semantics/'+name,'sha256':sha(data)}
        shared=put('contexts.json',self.shared);g=put('gate.json',gate)
        refs=[{'unit':put('unit'+str(i)+'.json',u),'context_id':u['evidence_context_id']} for i,u in enumerate(self.units)]
        pointer={'schema_version':CATALOGUE_SCHEMA,'evidence_kind':'REAL_CHAIN','instances':refs,
                 'shared_evidence_context':shared,'capability_gate':g}
        put('CURRENT.json',pointer)
        resolver=load_current_resolver(root)
        self.assertEqual(len(resolver.units),2)
        self.assertEqual(resolver.contexts.validation_counts['job_verifications'],1)
        pointer['instances'].pop();put('CURRENT.json',pointer)
        with self.assertRaisesRegex(ValueError,'membership'):load_current_resolver(root)

    def test_size_keeps_one_copy_of_shared_page_bytes(self):
        refs=[p['bigquery_extension']['family']['documents'] for p in self.portables]
        common=set(refs[0])&set(refs[1]);shared_bytes=0
        for name in common:
            a,b=refs[0][name],refs[1][name]
            if a['sha256']==b['sha256']:
                shared_bytes+=a['bytes']
                self.assertEqual(a['bytes_base64'],b['bytes_base64'])
                self.assertIn(a['sha256'],self.shared['blobs'])
        self.assertGreater(shared_bytes,10000)
        # Only the shared pool stores base64, not each context descriptor.
        def embedded(value):
            if isinstance(value,dict):return sum((len(v) if k.endswith('bytes_base64') and isinstance(v,str) else embedded(v)) for k,v in value.items())
            if isinstance(value,list):return sum(map(embedded,value))
            return 0
        self.assertEqual(embedded(self.shared['contexts']),0)
        baseline=sum(len(encoded(raw)) for docs in self.local_docs for raw in docs.values())
        actual=sum(len(v['bytes_base64']) for v in self.shared['blobs'].values())
        self.assertLess(actual,baseline)
        def unpack(value):
            if isinstance(value,dict):
                if REF in value:return encoded(self.builder.pool.read(value))
                return {k:unpack(v) for k,v in value.items()}
            if isinstance(value,list):return list(map(unpack,value))
            return value
        expanded={u['evidence_context_id']:unpack(p) for u,p in zip(self.units,self.portables)}
        compact_bytes=len(rawjson(self.shared));expanded_bytes=len(rawjson(expanded))
        self.assertLess(compact_bytes,expanded_bytes)
        MEASUREMENTS.update(evidence_kind='SYNTHETIC_CONTROLLED',distinct_physical_transactions=2,
            common_bq_original_bytes=shared_bytes,expanded_context_json_bytes=expanded_bytes,
            pooled_context_json_bytes=compact_bytes,pooled_unique_blobs=len(self.shared['blobs']),
            descriptor_embedded_base64_bytes=0,actual_production_size_measured=False)

    def _gate(self):
        root=self.f.f.w/'gate';(root/'src').mkdir(parents=True);(root/'checks').mkdir()
        sources={}
        for name in REQUIRED_GATE_SOURCES:
            raw=('LOCAL_GATE_VALIDATOR_UNIT_TEST:'+name).encode();(root/'src'/name).write_bytes(raw);sources[name]=sha(raw)
        raw=b'{"status":"PASS","evidence_kind":"SYNTHETIC_CONTROLLED"}';(root/'checks/receipt.json').write_bytes(raw)
        return root,{'status':'PASS','semantic_version':SEMANTIC_VERSION,'capabilities':dict.fromkeys(REQUIRED_CAPABILITIES,True),
            'source_sha256':sources,'test_evidence':[{'path':'checks/receipt.json','sha256':sha(raw),'status':'PASS'}]}

    def test_real_contract_collector_query_isolation_then_context_seven_methods_receiver(self):
        from collector import Collector,Event,FetchResult,Scope,QUERY_WINDOW_MODE
        from test_stage1d_multiasset_pipeline import pipeline_material
        from stage1d_context import build_document
        from stage1c_intervals import run_interval
        from stage1c_baselines import run_baseline
        from stage1c_output_contract import expected_domains,accept_method_results,METHODS
        from run_stage1c import normalized
        root,gate=self._gate()
        resolver=FiniteSemanticResolver(self.units,self.shared,capability_gate=gate,work_root=root)
        self.assertEqual(resolver.contexts.validation_counts['job_verifications'],1)
        seed,events,_,_=controlled_collection_inputs(withdraw=True)
        seed=Event(**{**asdict(seed),'recipient':SPONSOR})
        ordinary=Event('eip155:1:tx:0x'+'e'*64+':top','0x'+'e'*64,SPONSOR,HOLDER,
            'native:eip155:1',10,2,1,2,block_hash='0x'+'f'*64)
        events=[ordinary]+events
        class Provider:
            replay_only=True
            def fetch_interval(self,address,asset,start,end,**kw):
                return FetchResult(events=[e for e in events if e.asset==asset and address in (e.sender,e.recipient)],complete=True,cache_hits=1)
        labels=lambda a:{'kind':'SERVICE' if a==SERVICE else 'UNSUPPORTED_PROTOCOL' if a==WETH else 'UNKNOWN'}
        scope=Scope('controlled-pipeline','controlled-pipeline',1,20,1,20,5,10,QUERY_WINDOW_MODE)
        collected=Collector(Provider(),labels,semantic_resolver=resolver).run(scope,seed)
        other=Collector(Provider(),labels,semantic_resolver=resolver).run(Scope('q-other','q-other',1,20,1,20,5,10,QUERY_WINDOW_MODE),seed)
        self.assertEqual(collected.semantic_units,other.semantic_units)
        self.assertNotEqual(collected.semantic_membership[0]['query_id'],other.semantic_membership[0]['query_id'])
        self.assertNotEqual(collected.metrics['semantic_scope_id'],other.metrics['semantic_scope_id'])
        self.assertEqual(len(collected.semantic_units),2)
        path=self.f.f.w/'collection.json';collected.write(path);collection=json.loads(path.read_text())
        self.assertEqual(collection['semantic_evidence_context']['schema_version'],SCHEMA)
        args=list(pipeline_material(True));args[1]=collection;args[2]['evidence_kind']='REAL_EVIDENCE_BOUND'
        result=build_document(*args);self.assertIsNotNone(result['model_input'],result)
        document=json.loads(json.dumps(result['model_input']))
        self.assertEqual(document['semantic_evidence_context']['schema_version'],SCHEMA)
        self.assertTrue(expected_domains(document)['passed'])
        results={m:normalized(run_interval(document,m) if m in ('FULL_INTERVAL','NO_CROSS_TARGET_COUPLING',
            'NO_PROTOCOL_CONTINUATION','BALANCE_INFORMATION_REMOVED') else run_baseline(document,m)) for m in METHODS}
        identity={'sample_id':document['name'],'query_id':document['query_id'],'input_fact_hash':'controlled-fact',
            'scope_hash':'controlled-scope','label_version':'controlled-label','method_versions':{m:'controlled-v1' for m in METHODS}}
        for method,row in results.items():
            row.update({k:identity[k] for k in ('sample_id','query_id','input_fact_hash','scope_hash','label_version')})
            row.update(method_id=method,method_version='controlled-v1')
        accepted=accept_method_results(document,results,expected_identity=identity)
        self.assertTrue(accepted['passed'],accepted['errors'])
        changed=copy.deepcopy(document);changed['semantic_units'][0]['instance_policy']['amount_raw']='7'
        self.assertFalse(expected_domains(changed)['passed'])
        crossed=copy.deepcopy(document)
        next(flow for tx in crossed['transactions'] for flow in tx['flows'] if flow['role']=='SEED')['amount_raw']='999'
        self.assertFalse(accept_method_results(crossed,results,expected_identity=identity)['passed'])


if __name__=='__main__':unittest.main()
