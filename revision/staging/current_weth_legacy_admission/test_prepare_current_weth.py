"""Finite synthetic guards. No acquired index/raw/production DB is opened."""
import copy,json,pathlib,sqlite3,tempfile,unittest
from dataclasses import asdict
from contextlib import closing
from collector import Scope,Event,QUERY_WINDOW_MODE
from prepare_current_weth import derive_query,validate_point,verify_cross_query_facts,importer_type,safe_point,METHODS,WETH,NATIVE

HERE=pathlib.Path(__file__).resolve().parent
A='0x'+'a'*40;B='0x'+'b'*40;TX='0x'+'1'*64;BH='0x'+'2'*64
def fixture(qid='q'):
    s=Scope(qid,qid,1,100,1,100,5,10,QUERY_WINDOW_MODE)
    query={'query_id':qid,'name':qid,'start_block':1,'end_block':100,'start_time_utc':'1970-01-01T00:00:01Z','end_time_utc':'1970-01-01T00:01:40Z',
        'max_acquisition_depth':5,'primary_window_mode':QUERY_WINDOW_MODE,'primary_local_window_seconds':10,'scope_id':s.scope_id,'scope_hash':s.scope_hash}
    seed=Event('seed','0x'+'0'*64,B,A,NATIVE,10,1,0,1,block_hash='0x'+'3'*64)
    entry=Event('entry',TX,A,WETH,NATIVE,6,2,0,2,block_hash=BH)
    state={'query_id':qid,'address':A,'asset':NATIVE,'arrival':asdict(seed),'depth':0,'local_end':11}
    co={'metrics':{'scope_id':s.scope_id,'scope_hash':s.scope_hash},'states':[{'state':state,'identity':{'kind':'UNKNOWN'}}],
        'candidate_events':[asdict(seed),asdict(entry)],'membership':[{'event_id':'entry','arrival_event_id':'seed','depth':1}],'stops':[]}
    ref={'path':'synthetic/current.json','sha256':'a'*64,'bytes':1}
    return query,co,ref

class Tests(unittest.TestCase):
    def derive(self):
        q,c,r=fixture();return derive_query(q,c,r,r)
    def test_current_forward_event_produces_only_four_exact_points(self):
        d,n=self.derive();self.assertEqual({x['method'] for x in n},set(METHODS));self.assertEqual(len(n),4)
        self.assertFalse(d['old_inventory_used_to_authorize_demand']);self.assertEqual(d['events'][0]['event_id'],'entry')
        self.assertTrue(all(x['evidence_refs'] for x in n));self.assertEqual(next(x['params'] for x in n if x['method']=='eth_getCode'),[WETH,'0x2'])
    def test_changed_current_collection_changes_fresh_demand(self):
        q,c,r=fixture();d,_=derive_query(q,c,r,r);c['candidate_events'][1]['amount_raw']=7;new,_=derive_query(q,c,r,r)
        self.assertNotEqual(d['event_facts_sha256'],new['event_facts_sha256']);self.assertEqual(new['events'][0]['amount_raw'],7)
    def test_unrelated_inflow_is_not_authorized(self):
        q,c,r=fixture();c['candidate_events'][1]['sender']=B
        with self.assertRaisesRegex(ValueError,'legal forward'):derive_query(q,c,r,r)
    def test_self_transfer_rejected_and_zero_failed_entries_ignored(self):
        q,c,r=fixture();c['candidate_events'][1]['sender']=WETH
        with self.assertRaisesRegex(ValueError,'self-transfer'):derive_query(q,c,r,r)
        for field,value in [('amount_raw',0),('success',False)]:
            q,c,r=fixture();c['candidate_events'][1][field]=value;d,needs=derive_query(q,c,r,r)
            self.assertEqual(d['events'],[]);self.assertEqual(needs,[])
    def test_membership_missing_cannot_use_candidate_presence_alone(self):
        q,c,r=fixture();c['membership']=[]
        with self.assertRaisesRegex(ValueError,'legal forward'):derive_query(q,c,r,r)
    def test_stopped_source_cannot_generate_demand(self):
        q,c,r=fixture();c['stops']=[copy.deepcopy(c['states'][0])]
        with self.assertRaisesRegex(ValueError,'legal forward'):derive_query(q,c,r,r)
    def test_outside_window_or_unresolved_order_rejected(self):
        for mutation in ('window','order'):
            q,c,r=fixture();e=c['candidate_events'][1]
            if mutation=='window':e['timestamp']=12
            else:e['block']=1;e['tx_hash']=c['candidate_events'][0]['tx_hash'];e['timestamp']=1;e['kind']='top'
            with self.subTest(mutation=mutation),self.assertRaisesRegex(ValueError,'legal forward'):derive_query(q,c,r,r)
    def test_current_scope_must_match(self):
        q,c,r=fixture();c['metrics']['scope_hash']='f'*64
        with self.assertRaisesRegex(ValueError,'exact query scope'):derive_query(q,c,r,r)
    def test_cross_query_physical_sharing_does_not_merge_scopes(self):
        qa,ca,ra=fixture('a');qb,cb,rb=fixture('b');a,an=derive_query(qa,ca,ra,ra);b,bn=derive_query(qb,cb,rb,rb)
        verify_cross_query_facts([a,b]);self.assertNotEqual(an[0]['scope_hash'],bn[0]['scope_hash']);self.assertEqual(an[0]['params'],bn[0]['params'])
        b['events'][0]['tx_index']=1
        with self.assertRaisesRegex(ValueError,'Cross-query'):verify_cross_query_facts([a,b])
    def test_header_exact_time_and_tx_position_required(self):
        d,needs=self.derive();n=next(x for x in needs if x['method']=='eth_getBlockByNumber');p={k:n[k] for k in ('method','params')}
        header={'number':'0x2','timestamp':'0x2','hash':BH,'transactions':[TX]};validate_point(p,header,n['physical_binding'])
        for field,value in [('timestamp','0x3'),('transactions',[]),('hash','0x'+'4'*64)]:
            bad=copy.deepcopy(header);bad[field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):validate_point(p,bad,n['physical_binding'])
    def test_receipt_failure_or_wrong_index_rejected(self):
        d,needs=self.derive();n=next(x for x in needs if x['method']=='eth_getTransactionReceipt');p={k:n[k] for k in ('method','params')}
        rc={'blockNumber':'0x2','blockHash':BH,'transactionHash':TX,'transactionIndex':'0x0','status':'0x1'};validate_point(p,rc,n['physical_binding'])
        for field,value in [('status','0x0'),('transactionIndex','0x1')]:
            bad=dict(rc,**{field:value})
            with self.subTest(field=field),self.assertRaises(ValueError):validate_point(p,bad,n['physical_binding'])
    def test_balances_and_calls_never_admitted(self):
        for method in ('eth_getBalance','eth_call','eth_getLogs','eth_getBlockByHash'):
            with self.subTest(method=method),self.assertRaisesRegex(ValueError,'only tx/receipt'):validate_point({'method':method},None,{})
    def test_cache_binding_checked_before_import(self):
        d,needs=self.derive();need=next(x for x in needs if x['method']=='eth_getCode')
        class Base:
            def prepare_one(self,q,n):return {'status':'CURRENT_SUCCESS_REUSED_LEGACY_NOT_READ','plan':{'method':n['method'],'params':n['params']},'need':n}
            def _current_success(self,p):return {'payload':'0x'}
        importer=importer_type(Base)();importer.current_input_stamps={}
        self.assertEqual(importer.prepare_one({'query_id':'q'},need)['status'],'POINT_ADMISSION_QUARANTINED')
    def test_lock_and_inflight_guard_never_recovers_or_deletes(self):
        with tempfile.TemporaryDirectory(dir=HERE) as tmp:
            root=pathlib.Path(tmp);p=root/'private';p.mkdir();lock=p/'network_worker.lock';lock.write_text('synthetic')
            with self.assertRaisesRegex(ValueError,'Active writer lock'):safe_point(root)
            self.assertEqual(lock.read_text(),'synthetic');lock.unlink()
            db=p/'read_retry_r4.sqlite';con=sqlite3.connect(db);con.executescript("CREATE TABLE read_requests(state TEXT);CREATE TABLE read_attempts(outcome TEXT,dispatched_at REAL);INSERT INTO read_requests VALUES('IN_FLIGHT');");con.close()
            with self.assertRaisesRegex(ValueError,'In-flight'):safe_point(root)
            with closing(sqlite3.connect(db)) as con:self.assertEqual(con.execute('SELECT state FROM read_requests').fetchone()[0],'IN_FLIGHT')
    def test_no_network_or_writer_modules_are_invoked(self):
        text=(HERE/'prepare_current_weth.py').read_text(encoding='utf-8')
        for forbidden in ('RpcAccess(','.call_batch(','.execute_plan(','.request(','.urlopen(','ReadRetryStore('):self.assertNotIn(forbidden,text)

if __name__=='__main__':unittest.main()
