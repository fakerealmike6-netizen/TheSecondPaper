"""Synthetic normal-GET native projection, financial risk and offline replay."""
import copy, hashlib, json, sys, unittest
from dataclasses import asdict, replace
from decimal import Decimal
from pathlib import Path
BASE=Path(__file__).resolve().parents[1];sys.path.insert(0,str(BASE/'src'))
import test_budget_r2
from budget_r1 import consistent_backup
from collector import Scope, Collector, NATIVE
from context_access_r3 import sha
from page_attempts import atomic_json, RequestBlocked
from stage1d_acquisition import interval_sql
from stage1d_export import Stage1DNativeExport, projection_proof, export_bound, native_result_rows, verified_native_pages, COLUMNS, FULL_COLUMNS, FILTER, AUTH
from test_collector import event, SyntheticProvider, labels, A, B, S, X

class RuntimeStub:
    def raw_risk(self,work):return 0

class NativeExportTests(unittest.TestCase):
    def setUp(self):
        self.base=test_budget_r2.BudgetR2Tests();self.base.setUp();self.w=self.base.p
        (self.w/'private').mkdir();consistent_backup(self.base.db.path,self.w/'private/shared_budget_r4.sqlite')
        atomic_json(self.w/'private/dune_user_confirmation.json',test_budget_r2.confirmation())
        atomic_json(self.w/'private/dune_rate_evidence.json',{'account_context_ref':'synthetic','status':'USER_CONFIRMED','plan':'Plus trial','credit_economics_tier':'Free','export_credits_per_decimal_MB_for_budget':'20'})
        self.calls=[];self.responses=[];self.t=0
        self.live=Stage1DNativeExport(self.w,transport=self.transport,runtime=RuntimeStub(),clock=lambda:self.t,sleeper=self.sleep,rng=lambda:0)
        q={'query_id':'synthetic-native-query','name':'synthetic','start_block':1,'end_block':2000,'start_time_utc':1,'end_time_utc':2000,
           'max_acquisition_depth':6,'window_mode':'REFERENCE_FULL','local_window_seconds':None,'seed_asset':NATIVE,'seed_event':asdict(event(1,X,A,1))}
        scope=Scope.from_policy(q);q.update(scope_id=scope.scope_id,scope_hash=scope.scope_hash)
        self.q=q;atomic_json(self.w/'private/BATCH_QUERY_FREEZE.json',{'queries':[q]})
        sql=interval_sql([A],1,2000,1,2000);digest=hashlib.sha256(sql.encode()).hexdigest();self.job='dune_r4:'+digest
        self.folder=self.w/'private/dune_r2_jobs'/digest;self.folder.mkdir(parents=True);(self.folder/'query.sql').write_text(sql,encoding='utf8',newline='\n')
        freeze=self.w/'private/freeze_manifest.json';atomic_json(freeze,{'schema_version':'stage1d-sql-freeze-v1','authorization_id':AUTH,'kind':'candidate',
            'query_ids':[q['query_id']],'scope_id':q['scope_id'],'scope_hash':q['scope_hash'],'sql_sha256':digest,'dependencies':[],
            'addresses':[A],'intervals':[{'address':A,'asset':NATIVE,'start_block':1,'end_block':2000,'start_time':1,'end_time':2000,'kind':'candidate','query_id':q['query_id']}],
            'export_plan':{'all_pages_required':True}})
        self.md={'column_names':FULL_COLUMNS,'column_types':['synthetic']*16,'total_row_count':1260,'row_count':1260,'total_result_set_bytes':314904,'result_set_bytes':314904,'datapoint_count':20160}
        body={'execution_id':'A'*26,'state':'QUERY_STATE_COMPLETED','execution_cost_credits':'1.873088236','result_metadata':self.md}
        raw=self.w/'raw/dune/status.json';raw.parent.mkdir(parents=True);raw.write_text(json.dumps(body),encoding='utf8')
        receipt={'request_id':'synthetic_status','operation':'status','execution_id':'A'*26,'http_status':200,'error_class':None,'raw_path':raw.relative_to(self.w).as_posix(),'raw_bytes':raw.stat().st_size,'sha256':sha(raw),'utc':'2026-09-08T10:00:00Z'}
        atomic_json(self.w/'logs/synthetic_status.json',receipt)
        self.state={'logical_job_id':self.job,'execution_id':'A'*26,'sql_sha256':digest,'state':'QUERY_STATE_COMPLETED','scope_freeze_path':freeze.relative_to(self.w).as_posix(),
            'scope_freeze_sha256':sha(freeze),'status_response':body,'status_receipt':receipt,'r4_page_limit':1000,'r4_page_selection':{'columns':FULL_COLUMNS,'limit':1000},'export_requests':0,'export_offsets':[]}
        atomic_json(self.folder/'job.json',self.state)
        self.live.db.reserve_dune_job(self.job,'synthetic new export');self.live.db.observe_execution(self.job,'1.873088236',receipt,True)

    def tearDown(self):self.base.tearDown()
    def sleep(self,n):self.t+=n
    def transport(self,op,execution,payload,params,cap):
        self.calls.append((op,execution,payload,params))
        value=self.responses.pop(0)
        if isinstance(value,Exception):raise value
        return value
    def page(self,count,total=None,offset=0,extra=None):
        total=count if total is None else total
        rows=[{c:None for c in COLUMNS} for _ in range(count)]
        for n,r in enumerate(rows):r.update(event_kind='top',tx_hash='0x'+format(offset+n+2,'064x'),sender=A,recipient=S,amount_raw='1',block_number=n+2,tx_index=0,block_time='1970-01-01 00:00:02 UTC',success=True,gas_used='21000',gas_price='2')
        body={'execution_id':'A'*26,'state':'QUERY_STATE_COMPLETED','result':{'rows':rows,'metadata':{'column_names':COLUMNS,'column_types':['synthetic']*14,'row_count':count,'total_row_count':total,'result_set_bytes':count*200,'total_result_set_bytes':total*200,'datapoint_count':count*14}}}
        if offset+count<total:body['next_offset']=offset+count
        if extra:body.update(extra)
        return body
    def respond(self,body,status=200):self.responses.append((status,json.dumps(body).encode(),{}))
    def risk(self):
        with self.live.db.connection() as db:return Decimal(db.execute('SELECT export_risk FROM r2_components WHERE job=?',(self.job,)).fetchone()[0])

    def test_full_1260_single_normal_get_restores_only_constants_and_keeps_job(self):
        before=(self.folder/'job.json').read_bytes();proof=projection_proof(self.w,self.folder)
        self.assertEqual(export_bound(proof,self.live.rate_evidence())[0],18)
        self.respond(self.page(1260));result=self.live.export_native_next(self.folder)
        self.assertEqual(result['status'],'COMPLETED_NATIVE_SCOPE_EXPORTED');self.assertFalse(result['all_asset_export_complete'])
        self.assertEqual(before,(self.folder/'job.json').read_bytes());self.assertEqual(self.risk(),18)
        self.assertEqual(self.calls[0][3],{'limit':1260,'offset':0,'columns':','.join(COLUMNS),'filters':FILTER})
        actual=native_result_rows(self.w,self.folder);self.assertEqual(len(actual),1260)
        self.assertTrue(all(r['log_index'] is None and r['contract_address'] is None and set(r)==set(FULL_COLUMNS) for r in actual))
        again=self.live.export_native_next(self.folder);self.assertTrue(again['cache_reused']);self.assertEqual(len(self.calls),1)
        self.assertEqual(self.live.db.snapshot()['dune_credits']['legacy_risk'],'16.433088236')

    def test_smaller_filtered_native_total_is_complete_not_claimed_allasset(self):
        self.respond(self.page(7));result=self.live.export_native_next(self.folder)
        self.assertEqual(result['progress']['total'],7);self.assertEqual(len(native_result_rows(self.w,self.folder)),7)
        self.assertEqual(json.loads((self.folder/'job.json').read_text())['status_response']['result_metadata']['total_row_count'],1260)

    def test_complete_metadata_reconciliation_releases_only_verified_new_request_upper(self):
        self.live.db.reserve_export(self.job,'2',{'rate_evidence':{'old':True},'result_metadata':self.md})
        self.respond(self.page(7));self.live.export_native_next(self.folder);self.assertEqual(self.risk(),20)
        before=self.live.db.snapshot()['dune_credits'];r=self.live.reconcile_native_upper(self.folder)
        self.assertEqual(r['initial_this_request_upper'],'18');self.assertEqual(r['verified_this_request_upper'],'1')
        self.assertEqual(r['prior_export_risk_preserved'],'2');self.assertEqual(r['released_this_request_upper_difference'],'17')
        self.assertIsNone(r['export_actual']);self.assertEqual(self.risk(),3)
        self.assertEqual(before['legacy_risk'],self.live.db.snapshot()['dune_credits']['legacy_risk'])
        self.assertEqual(self.live.reconcile_native_upper(self.folder),r)
        with self.live.db.connection() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM stage1d_export_upper_reconciliation').fetchone()[0],1)

    def test_failed_attempts_cannot_release_envelope_after_success(self):
        self.responses.append(TimeoutError());self.respond(self.page(7));self.live.export_native_next(self.folder)
        with self.assertRaises(ValueError):self.live.reconcile_native_upper(self.folder)
        self.assertEqual(self.risk(),36)

    def test_inconsistent_projected_metering_retains_initial_upper(self):
        b=self.page(7);b['result']['metadata']['total_result_set_bytes']=50000;self.respond(b);self.live.export_native_next(self.folder)
        with self.assertRaises(ValueError):self.live.reconcile_native_upper(self.folder)
        self.assertEqual(self.risk(),18)

    def test_server_lower_limit_retains_partial_and_costs_next_page(self):
        self.respond(self.page(10,total=20));first=self.live.export_native_next(self.folder)
        self.assertEqual(first['status'],'PARTIAL_NATIVE_SCOPE_EXPORT')
        with self.assertRaises(ValueError):native_result_rows(self.w,self.folder)
        self.respond(self.page(10,total=20,offset=10));second=self.live.export_native_next(self.folder)
        self.assertTrue(second['progress']['complete']);self.assertEqual(self.risk(),36)
        self.assertEqual([c[3]['offset'] for c in self.calls],[0,10])

    def test_failed_read_persists_and_retry_get_charges_full_again(self):
        self.responses.append(TimeoutError());self.respond(self.page(7))
        result=self.live.export_native_next(self.folder);self.assertTrue(result['progress']['complete']);self.assertEqual(self.risk(),36)
        self.assertEqual([c[0] for c in self.calls],['results','results'])
        self.assertEqual(len(list((self.folder/'stage1d_native_export/attempts').glob('*/receipt.json'))),2)

    def test_three_total_failures_exhausted_not_reset(self):
        self.responses=[TimeoutError(),TimeoutError(),TimeoutError()]
        result=self.live.export_native_next(self.folder);self.assertEqual(result['status'],'RETRIES_EXHAUSTED');self.assertEqual(self.risk(),54)
        self.assertEqual(self.live.export_native_next(self.folder)['status'],'RETRIES_EXHAUSTED');self.assertEqual(len(self.calls),3)

    def test_unsafe_row_schema_execution_next_uri_fail_closed(self):
        mutations=[lambda b:b['result']['rows'][0].update(event_kind='erc20'),lambda b:b.update(execution_id='B'*26),
                   lambda b:b['result']['rows'][0].update(contract_address='0x'+'9'*40),lambda b:b.update(next_uri='https://evil.example/x')]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                proof=projection_proof(self.w,self.folder);b=self.page(1);mutate(b)
                from stage1d_export import validate_native_page,parameters
                from page_contract import initial_progress,PageContractError
                receipt={'http_status':200,'execution_id':'A'*26,'parameters':parameters(proof,0)}
                with self.assertRaises(PageContractError):validate_native_page(b,receipt,proof,initial_progress())

    def test_token_seed_and_conversion_and_changed_scope_refused(self):
        for key,value in [('seed_asset','erc20:eip155:1:'+X),('certified_conversions',[{'old_weth':True}]),('scope_hash','0'*64)]:
            q=dict(self.q);q[key]=value;atomic_json(self.w/'private/BATCH_QUERY_FREEZE.json',{'queries':[q]})
            with self.assertRaises(ValueError):projection_proof(self.w,self.folder)
        self.assertEqual(self.calls,[])

    def test_original_sql_and_raw_status_identity_bound(self):
        path=self.folder/'query.sql';path.write_text(path.read_text().replace('CAST(NULL AS BIGINT) AS log_index','1 AS log_index'),encoding='utf8')
        with self.assertRaises(ValueError):projection_proof(self.w,self.folder)

    def test_already_dispatched_allasset_get_cannot_reset_attempts(self):
        i=self.live.read_identity('results',self.state,{'limit':1000,'offset':0});claim=self.live.reads.claim(i);self.live.reads.mark_dispatched(claim['attempt_id'])
        self.live.reads.finish(claim['attempt_id'],'RETRYABLE_FAILURE',error_class='TimeoutError')
        with self.assertRaises(RequestBlocked):self.live.prepare(self.folder)

    def test_budget_block_does_not_dispatch_or_forget_previous_selection(self):
        self.live.db.reserve_export(self.job,'67',{'rate_evidence':{'x':1},'result_metadata':self.md})
        before=(self.folder/'job.json').read_bytes()
        with self.assertRaises(RuntimeError):self.live.export_native_next(self.folder)
        self.assertEqual(self.calls,[]);self.assertEqual(before,(self.folder/'job.json').read_bytes());self.assertEqual(self.risk(),67)

    def test_offline_cache_rejects_tampering(self):
        self.respond(self.page(1));self.live.export_native_next(self.folder)
        path=next((self.folder/'stage1d_native_export/verified').glob('*/page.json'));b=json.loads(path.read_text());b['result']['rows'][0]['amount_raw']='999';atomic_json(path,b)
        with self.assertRaises(ValueError):native_result_rows(self.w,self.folder)

    def test_native_candidates_stops_and_lp_native_context_equivalent_with_token_gas(self):
        from stage1d_context import _native_rows
        seed=event(1,X,A,1);gas=replace(event(2,A,X,2,0),gas_raw=42000,gas_used=21000,gas_price=2)
        failed=replace(event(3,A,X,3,5),success=False,gas_raw=84000,gas_used=42000,gas_price=2)
        native=[seed,gas,failed,event(4,A,B,4,7),event(5,B,S,5,7)]
        token=replace(event(6,A,X,6,900,asset='erc20:eip155:1:'+X),kind='erc20',event_id='eip155:1:tx:0x'+format(6,'064x')+':log:1',log_index=1)
        scope=Scope.from_policy(self.q)
        a=Collector(SyntheticProvider(native+[token]),labels).run(scope,seed);b=Collector(SyntheticProvider(native),labels).run(scope,seed)
        for key in ('candidate_events','states','stops','unresolved_frontier'):self.assertEqual(getattr(a,key),getattr(b,key))
        context=[r for r in a.context_events if r['asset']==NATIVE];self.assertEqual(context,b.context_events)
        self.assertEqual(_native_rows(a.candidate_events+a.context_events),_native_rows(b.candidate_events+b.context_events))
        self.assertEqual({r['gas_raw'] for r in context if r.get('gas_raw')},{42000,84000})

if __name__=='__main__':unittest.main(verbosity=2)
