"""Small synthetic actual SQL + REST pages + exact kind admission tests."""
from collections import defaultdict
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
import json,tempfile,unittest
from unittest.mock import patch

from collector import Scope
import stage1d_current_context_paid_bq as a
import stage1d_bq_context_prepare as h
import stage1d_batch_binding_route as b

S=Path(__file__).resolve().parents[1]
A='0x'+'1'*40;B='0x'+'2'*40;X='0x'+'3'*40


class Fixture:
    def __init__(self,work):
        self.work=work
        self.q={'name':'txphish_src001','query_id':'qry:synthetic:paid-context',
            'start_block':100,'end_block':1000,'start_time_utc':'2024-08-21T00:00:00Z',
            'end_time_utc':'2024-08-21T23:59:59Z','max_acquisition_depth':5,'window_mode':'REFERENCE_FULL'}
        scope=Scope.from_policy(self.q);self.scope=scope
        self.q.update(scope_id=scope.scope_id,scope_hash=scope.scope_hash,scope=asdict(scope),
            start_block_hash='0x'+'c'*64,end_block_hash='0x'+'d'*64)
        h.save(work/'private/BATCH_QUERY_FREEZE.json',{'queries':[self.q]})
        self.freeze=h.sha(work/'private/BATCH_QUERY_FREEZE.json')
        self.ranges=[{'address':A,'start_block':100,'end_block':1000}]
        h.save(work/'current.json',{'synthetic_bound_fact_inputs':True})
        h.save(work/'needed.json',{'query_id':self.q['query_id'],'scope_hash':scope.scope_hash,
            'freeze_sha256':self.freeze,'needed_ranges':self.ranges,'scope_dependencies':[h.dep(work,'current.json')]})
        common={'block_number':'INTEGER','block_hash':'STRING','block_timestamp':'TIMESTAMP',
            'transaction_index':'INTEGER','from_address':'STRING','to_address':'STRING','value':'NUMERIC'}
        fields={h.TR:dict(common,transaction_hash='STRING',status='INTEGER',trace_address='STRING',trace_type='STRING',call_type='STRING',subtraces='INTEGER',error='STRING'),
            h.TX:dict(common,hash='STRING',gas='INTEGER',input='STRING',receipt_status='INTEGER',receipt_gas_used='INTEGER',receipt_effective_gas_price='INTEGER',transaction_type='INTEGER')}
        self.schema=[]
        for n,(table,value) in enumerate(fields.items()):
            file=f'schema{n}.json';h.save(work/file,{'status':'SUCCESS_VALIDATED','result':{'kind':'SCHEMA','table':table,
                'partition':{'field':'block_timestamp','type':'DAY'},'schema':[{'name':k,'type':v} for k,v in value.items()]}})
            self.schema.append(h.dep(work,file))
        self.plan={'rows':[{'account_id':A+'|ETH','address':A,'asset':'ETH','ledger_start_block':100,'ledger_end_block':200}]}

    def family(self,*,root='[]',success=True,amount='9',fee='2',tx_digit='b',sender=A):
        def row(kind,**kw):
            r=dict.fromkeys(h.columns_for('transaction_and_trace'))
            r.update(record_type=kind,block_number='150',block_hash='0x'+'a'*64,
                block_time='2024-08-21T01:30:00Z',tx_hash='0x'+tx_digit*64,tx_index='0',
                from_address=sender,to_address=B,value_raw=amount,success=success,input_data='0x')
            r.update(kw);return r
        return [row('transaction',gas_used='21000',effective_gas_price=fee,gas_limit='50000',transaction_type='2'),
            row('trace',trace_address=root,trace_type='call',call_type='call',subtraces='1'),
            row('trace',trace_address='[0]',trace_type='call',call_type='call',subtraces='0',from_address=B,to_address=X,value_raw='1')]

    def prepare(self,rows,batch=False):
        with patch.object(h,'FREEZE_SHA',self.freeze):
            if batch:
                need={'address':A,'asset':'native:eip155:1','query_id':self.q['query_id'],'query_name':self.q['name'],
                    'scope_id':self.q['scope_id'],'scope_hash':self.q['scope_hash'],'start_block':100,'end_block':1000,
                    'start_time':self.scope.start_time+3600,'end_time':self.scope.start_time+7200,
                    'direction':'OUTGOING','fact_type':'POSITIVE_NATIVE_CANDIDATE_INDEX','gap_reason':'SYNTHETIC'}
                doc={'query_id':self.q['query_id'],'scope_hash':self.q['scope_hash'],'freeze_sha256':self.freeze,
                    'need_rectangles':[need],'scope_dependencies':[h.dep(self.work,'current.json')]}
                h.save(self.work/'batchneeded.json',doc)
                manifest=b.prepare(self.work,'batchneeded.json','prepared',{'tables':[h.TX,h.TR]},self.schema)
            else:manifest=h.prepare(self.work,'needed.json','prepared',{'tables':[h.TX,h.TR]},self.schema,templates=('transaction_and_trace',))
        states={}
        for n,spec in enumerate(manifest['plans']):states[spec['path']]=self.export(spec,rows if n==0 else [],n)
        return {'preparation_ref':h.dep(self.work,'prepared/PREPARATION.json'),'job_states':states}

    def export(self,dep,rows,n):
        folder=f'jobs/{n}';w=self.work;spec=h.read(w/dep['path']);columns=spec['canonical_columns']
        schema={'fields':[{'name':k,'type':'BOOLEAN' if k in ('success','tx_success') else 'STRING'} for k in columns]}
        job='stage1d_recovery_'+h.digest({'sql_sha256':spec['sql_sha256'],'project':'synthetic-project','location':'US'})[:48]
        dry=folder+'/dry.json';h.save(w/dry,{'status':'SUCCESS_VALIDATED','result':{'kind':'DRY_RUN'},
            'identity':{'project':'synthetic-project','sql_sha256':spec['sql_sha256'],'spec_sha256':dep['sha256'],'scope_hash':spec['scope_hash']}})
        plan=folder+'/plan.json';h.save(w/plan,{'dry_spec_path':dep['path'],'dry_spec_sha256':dep['sha256'],
            'dry_receipt_path':dry,'dry_receipt_sha256':h.sha(w/dry)})
        raw=folder+'/raw.json';payload={'jobReference':{'jobId':job},'jobComplete':True,'totalRows':str(len(rows)),
            'schema':schema,'rows':[{'f':[{'v':r.get(k)} for k in columns]} for r in rows]}
        h.save(w/raw,payload);receipt=folder+'/receipt.json'
        h.save(w/receipt,{'status':'SUCCESS_VALIDATED','identity':{'method':'jobs.getQueryResults','selectors':{'job_id':job}},
            'result':payload,'raw_sources':[dict(h.dep(w,raw),complete=True,http_status=200)]})
        data=b''.join(json.dumps({k:r.get(k) for k in columns},sort_keys=True).encode()+b'\n' for r in rows)
        for file in ('page.jsonl','all.jsonl'):(w/folder/file).write_bytes(data)
        h.save(w/folder/'terminal_job.json',{'jobReference':{'jobId':job},'status':{'state':'DONE'},
            'configuration':{'query':{'query':(w/spec['sql_path']).read_text()}}})
        state={'state':'COMPLETE_EXPORTED','complete_page_chain':True,'job_id':job,'plan_path':plan,'plan_sha256':h.sha(w/plan),
            'sql_sha256':spec['sql_sha256'],'terminal_job_sha256':h.sha(w/folder/'terminal_job.json'),
            'schema':schema,'total_rows':len(rows),'rows_path':folder+'/all.jsonl','rows_sha256':h.sha(w/folder/'all.jsonl'),
            'pages':[{'page_token':None,'next_page_token':None,'row_count':len(rows),'rows_path':folder+'/page.jsonl',
                'rows_sha256':h.sha(w/folder/'page.jsonl'),'response_receipt':{'artifact_path':receipt,'artifact_sha256':h.sha(w/receipt)}}]}
        h.save(w/folder/'job.json',state);return h.dep(w,folder+'/job.json')


class PaidContextTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=S);self.addCleanup(self.tmp.cleanup)
        self.w=Path(self.tmp.name);self.f=Fixture(self.w)

    def admit(self,rows,*,batch=False,plan=None):
        source=self.f.prepare(rows,batch=batch)
        loaded=a._load_source(self.w,source)
        return a._admit_plan(self.f.scope,plan or self.f.plan,[loaded],a._Points(self.w,[])),source

    def test_actual_classic_sql_rest_pages_narrow_superset_three_kinds(self):
        result,_=self.admit(self.f.family())
        self.assertEqual({c['data_type'] for c in result['coverage']},set(a.KINDS))
        self.assertEqual({(c['start_block'],c['end_block']) for c in result['coverage']},{(100,200)})
        self.assertEqual(len(result['rows']),3);self.assertEqual(result['missing_paid_source_by_kind'],[])
        self.assertFalse(result['full_context_claimed']);self.assertEqual(len(result['protocol_requirements']),1)

    def test_batch_original_seconds_do_not_truncate_whole_day_ledger(self):
        rows=self.f.family();rows[0]['block_time']='2024-08-21T03:00:00Z'
        for row in rows[1:]:row['block_time']=rows[0]['block_time']
        result,_=self.admit(rows,batch=True)
        self.assertEqual(len(result['coverage']),3);self.assertEqual(len(result['rows']),3)

    def test_null_root_is_internal_pending_not_empty_path_or_new_sql(self):
        result,_=self.admit(self.f.family(root=None))
        self.assertEqual({c['data_type'] for c in result['coverage']},{h.TOP,h.FEES})
        self.assertTrue(any(r['trace_address'] is None for r in result['rows'] if r['record_type']=='trace'))
        self.assertEqual(len(result['point_binding_requests']),3)
        self.assertEqual(result['missing_paid_source_by_kind'],[])
        self.assertTrue(all(not x['new_sql_authorized'] for x in result['paid_pending_binding_by_kind']))

    def test_missing_sibling_and_fee_separate_without_full(self):
        rows=self.f.family(fee=None);rows.pop()
        result,_=self.admit(rows)
        # Existing normalizer treats a contradictory child count as a physical
        # conflict, so the adapter must retain that stricter all-kind refusal.
        self.assertEqual(result['coverage'],[])
        self.assertEqual(result['missing_paid_source_by_kind'],[])
        self.assertEqual({r['data_type'] for r in result['paid_pending_binding_by_kind']},set(a.KINDS))

    def test_missing_fee_alone_keeps_top_and_internal_proof(self):
        result,_=self.admit(self.f.family(fee=None))
        self.assertEqual({c['data_type'] for c in result['coverage']},{h.TOP,h.INTERNAL})
        self.assertEqual({r['data_type'] for r in result['paid_pending_binding_by_kind']},{h.FEES})
        self.assertEqual(result['point_binding_requests'][0]['method'],'eth_getTransactionReceipt')

    def test_failed_zero_gas_and_internal_family_rows_not_filtered(self):
        result,_=self.admit(self.f.family(success=False,amount='0'))
        tops=[r for r in result['rows'] if r['record_type']=='transaction']
        self.assertEqual(tops[0]['value_raw'],'0');self.assertFalse(tops[0]['success'])
        self.assertEqual(tops[0]['gas_used'],'21000');self.assertEqual(len(result['rows']),3)
        self.assertEqual(len(result['coverage']),3)

    def test_unrelated_family_not_in_material_and_asset_not_expanded(self):
        other=self.f.family(tx_digit='c',sender=X)
        for row in other:row['to_address']=X;row['from_address']=X
        plan=deepcopy(self.f.plan);plan['rows'].append(dict(plan['rows'][0],account_id=A+'|WETH',asset='erc20:eip155:1:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))
        result,_=self.admit(self.f.family()+other,plan=plan)
        self.assertEqual(len(result['rows']),3);self.assertEqual(len(result['non_native_plan_rows_preserved']),1)

    def test_source_missing_range_is_exact_and_success_islands_not_filled(self):
        source=self.f.prepare(self.f.family());loaded=a._load_source(self.w,source)
        loaded['manifest']['needed_ranges']=[{'address':A,'start_block':100,'end_block':120},{'address':A,'start_block':180,'end_block':200}]
        result=a._admit_plan(self.f.scope,self.f.plan,[loaded],a._Points(self.w,[]))
        self.assertEqual({(r['start_block'],r['end_block']) for r in result['missing_paid_source_by_kind']},{(121,179)})

    def test_paid_date_gap_blocks_claim_and_not_requery_without_bracket(self):
        source=self.f.prepare([]);loaded=a._load_source(self.w,source)
        loaded['date_intervals']=[[self.f.scope.start_time,self.f.scope.start_time+3600]]
        result=a._admit_plan(self.f.scope,self.f.plan,[loaded],a._Points(self.w,[]))
        self.assertEqual(result['coverage'],[]);self.assertEqual(result['missing_paid_source_by_kind'],[])
        self.assertEqual({p['params'][0] for p in result['point_binding_requests']},{hex(100),hex(200)})

    def test_actual_page_or_sql_mutation_rejected(self):
        source=self.f.prepare(self.f.family())
        raw=self.w/'jobs/0/raw.json';raw.write_text('{}')
        with self.assertRaises(ValueError):a._load_source(self.w,source)

    def test_missing_job_or_metadata_only_rejected(self):
        source=self.f.prepare([]);source['job_states']={}
        with self.assertRaises(ValueError):a._load_source(self.w,source)
        h.save(self.w/'metadata.json',{'values_committed':False,'rows':[]})
        with self.assertRaisesRegex(ValueError,'Bare metadata'):a._load_source(self.w,{'preparation_ref':h.dep(self.w,'metadata.json'),'job_states':{}})

    def test_actual_rpc_root_cross_source_fee_conflict_and_raw_tamper(self):
        rows=self.f.family(root=None);txid=rows[0]['tx_hash'];blockhash=rows[0]['block_hash']
        tx={'hash':txid,'blockNumber':hex(150),'transactionIndex':'0x0','blockHash':blockhash,
            'from':A,'to':B,'value':'0x9','gas':hex(50000),'input':'0x'}
        receipt={'transactionHash':txid,'blockNumber':hex(150),'transactionIndex':'0x0','blockHash':blockhash,
            'from':A,'to':B,'status':'0x1','gasUsed':hex(21000),'effectiveGasPrice':'0x2','logs':[]}
        header={'number':hex(150),'hash':blockhash,'timestamp':hex(self.f.scope.start_time+5400),'transactions':[txid]}
        plans=[{'method':'eth_getTransactionByHash','params':[txid]},
               {'method':'eth_getTransactionReceipt','params':[txid]},a._header_plan(150)]
        bindings=[]
        for i,(plan,value) in enumerate(zip(plans,[tx,receipt,header])):
            folder=self.w/f'rpc/{i}';folder.mkdir(parents=True)
            request=dict(plan,id=i,jsonrpc='2.0');response={'id':i,'jsonrpc':'2.0','result':value}
            (folder/'response_body.bin').write_bytes(json.dumps([response]).encode())
            h.save(folder/'env.json',{'provider_alias':a.transfers.PROVIDER,'evidence_kind':'REAL_CHAIN','http_status':200,
                'response_complete':True,'status':'SUCCESS_VALIDATED','request':request,'response':response,
                'raw_body_sha256':h.sha(folder/'response_body.bin')})
            bindings.append({'plan':plan,'member':{'artifact_path':f'rpc/{i}/env.json','artifact_sha256':h.sha(folder/'env.json'),
                'result':value,'status':'SUCCESS_VALIDATED','cache_hit':False}})
        source=self.f.prepare(rows);loaded=a._load_source(self.w,source)
        result=a._admit_plan(self.f.scope,self.f.plan,[loaded],a._Points(self.w,bindings))
        self.assertEqual(len(result['coverage']),3);self.assertEqual(result['point_binding_requests'],[])
        root=next(r for r in result['rows'] if r['record_type']=='trace' and r['trace_address']=='[]')
        self.assertIsNone(root['root_position_binding']['original_trace_address'])
        self.assertIsNone(rows[1]['trace_address'])
        # A different, fully source-bound receipt must not let the unchanged
        # paid BQ fee silently win merely because the root binding is separate.
        # Equal total fee is insufficient: both actual operands must agree.
        receipt['gasUsed']=hex(42000);receipt['effectiveGasPrice']='0x1'
        rp=self.w/'rpc/1/env.json';env=h.read(rp);env['response']['result']=receipt
        (rp.parent/'response_body.bin').write_text(json.dumps([env['response']]))
        env['raw_body_sha256']=h.sha(rp.parent/'response_body.bin');rp.write_text(json.dumps(env))
        bindings[1]['member'].update(result=receipt,artifact_sha256=h.sha(rp))
        conflict=a._admit_plan(self.f.scope,self.f.plan,[loaded],a._Points(self.w,bindings))
        self.assertEqual(conflict['coverage'],[]);self.assertEqual(conflict['missing_paid_source_by_kind'],[])
        self.assertTrue(any(g.get('provided_binding_evidence_failed') for g in conflict['binding_gaps']))
        partial=a._admit_plan(self.f.scope,self.f.plan,[loaded],a._Points(self.w,[bindings[1]]))
        self.assertEqual(partial['coverage'],[])
        explicit=deepcopy(loaded)
        next(r for r in explicit['families'][txid] if r['record_type']=='trace' and r['trace_address'] is None)['trace_address']='[]'
        explicit_result=a._admit_plan(self.f.scope,self.f.plan,[explicit],a._Points(self.w,[bindings[1]]))
        self.assertEqual(explicit_result['coverage'],[])
        self.assertTrue(any(g.get('reason')=='CURRENT_POINT_VS_BQ_PHYSICAL_CONFLICT' for g in explicit_result['binding_gaps']))
        (self.w/'rpc/0/response_body.bin').write_text('[]')
        with self.assertRaisesRegex(ValueError,'CACHE_RAW_PROOF'):
            a._admit_plan(self.f.scope,self.f.plan,[loaded],a._Points(self.w,bindings))

    def test_incoming_internal_selfdestruct_keeps_full_external_payer_family(self):
        rows=self.f.family(sender=X)
        rows[2].update(to_address=A,trace_type='selfdestruct',call_type=None,created_address=B,refund_address=A)
        result,_=self.admit(rows)
        self.assertEqual(len(result['rows']),3);self.assertEqual(len(result['coverage']),3)
        self.assertTrue(any(r.get('refund_address')==A for r in result['rows']))

    def test_rehashed_sql_still_must_regenerate_original_predicate(self):
        source=self.f.prepare([]);m=h.read(self.w/source['preparation_ref']['path']);dep=m['plans'][0]
        spec=h.read(self.w/dep['path']);sql=self.w/spec['sql_path'];sql.write_text(sql.read_text()+'LIMIT 1\n')
        spec['sql_sha256']=h.sha(sql);(self.w/dep['path']).write_text(json.dumps(spec))
        dep['sha256']=h.sha(self.w/dep['path']);(self.w/source['preparation_ref']['path']).write_text(json.dumps(m))
        source['preparation_ref']=h.dep(self.w,source['preparation_ref']['path'])
        with self.assertRaisesRegex(ValueError,'Original SQL'):
            a._load_source(self.w,source)

    def test_known_contract_creation_null_is_not_missing_endpoint(self):
        family=self.f.family()
        receipt={'transactionHash':family[0]['tx_hash'],'to':None}
        with self.assertRaisesRegex(ValueError,'nullable endpoint'):
            a._compare_available_top(family,None,receipt,None)
        family[0]['created_address']=X
        with self.assertRaisesRegex(ValueError,'nullable endpoint'):
            a._compare_available_top(family,None,{'contractAddress':None},None)

    def test_public_current_entry_verifies_source_once_not_per_window(self):
        source=self.f.prepare(self.f.family())
        qdir=self.w/'derived/stage1d/queries'/self.f.q['name'];h.save(qdir/'collection.json',{'fixture':'pure demand seam'});h.save(qdir/'label_snapshot.json',{})
        demand={'context_plan':deepcopy(self.f.plan),'binding':{'scope_hash':self.f.q['scope_hash']},'point_requests':[]}
        demand['context_plan']['rows'].append(dict(self.f.plan['rows'][0],ledger_start_block=160,ledger_end_block=180))
        with patch.object(a,'requirements',return_value=demand),patch.object(h,'verified_export',wraps=h.verified_export) as export:
            result=a.admit_current(self.w,self.f.q['name'],h.dep(self.w,qdir/'collection.json'),h.dep(self.w,qdir/'label_snapshot.json'),[source,dict(source,comment='same paid identity')])
        self.assertEqual(export.call_count,1);self.assertEqual(result['new_requests_executed'],0)
        self.assertEqual(len(result['sources']),1)


if __name__=='__main__':unittest.main()
