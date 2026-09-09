"""Synthetic inputs only. Real Runtime identity/result validation, RO SQLite."""
from pathlib import Path
from contextlib import closing,redirect_stdout
from copy import deepcopy
from unittest.mock import patch
import csv,gzip,hashlib,io,json,sqlite3,tempfile,unittest
import prepare_historical_code as p

H='0x'+'a'*64
def row(name='txphish_src001',number=1,block=100,depth=1,required=True,hash=H,asset='native:eip155:1'):
    query='query:'+name;addr='0x'+format(number,'040x')
    state={'query_id':query,'address':addr,'asset':asset,'depth':depth,'local_end':200,
      'protocol_context':'ordinary','arrival':{'block':block,'block_hash':hash,'chain_id':'eip155:1',
       'timestamp':block,'tx_index':0,'event_id':name+':'+str(number)+':'+str(block)}}
    return {'query_name':name,'query_id':query,'scope_hash':'scope:'+name,'state':state,
      'new_cost_screen_required':required,'existing_boundary_or_depth':False,
      'collection_ref':{'path':'synthetic/'+name+'.json','sha256':'b'*64}}
def snap(rows):return {'authorization_id':p.AUTH,'preserve_prior_completed_states_without_new_identity_audit':True,'state_screen_rows':rows}
def unknown(s):return {'kind':'UNKNOWN','status':'LOCAL_FROZEN','branch_action':'NORMAL_ACCOUNT_EXPAND'}
def cache(plan):return {'request':plan,'logical_key':p.digest(plan),'state':'NO_CURRENT_EXACT_REQUEST'}

class DerivationTests(unittest.TestCase):
    def test_initial_only_latest_holds_and_supported_roles_filtered(self):
        rows=[row(number=i,required=i!=1) for i in range(1,7)];called=[]
        labels={2:{'kind':'UNKNOWN','branch_action':'USER_REQUESTED_BRANCH_HOLD'},3:{'kind':'SERVICE'},
          4:{'kind':'UNSUPPORTED_PROTOCOL'},5:{'kind':'SUPPORTED_PROTOCOL','branch_action':'SUPPORTED_OPERATION_RESOLVE'},6:unknown(None)}
        def resolve(s):n=int(s['address'],16);called.append(n);return labels[n]
        result=p.derive(snap(rows),resolve,cache,{},5)
        self.assertNotIn(1,called);self.assertEqual(result['initial_screen_states'],5)
        self.assertEqual(result['excluded_by_current_local_role_states'],4)
        self.assertEqual(len(result['code_groups']),1);self.assertFalse(result['old_completed_states_rechecked'])

    def test_priority_original_depth_arrival_and_exact_cross_query_dedup(self):
        rows=[row('xscam_src001',3),row('txphish_src001',1),row('txphish_src002',1),row('txphish_src002',2,depth=0),
              row('txphish_src001',1,block=101),row('txphish_src002',1,asset='erc20:eip155:1:'+'0x'+'c'*40)]
        result=p.derive(snap(rows),unknown,cache,{},5);groups=result['code_groups']
        self.assertEqual([(g['address'],g['arrival_block']) for g in groups],[(rows[3]['state']['address'],100),
            (rows[1]['state']['address'],100),(rows[4]['state']['address'],101),(rows[0]['state']['address'],100)])
        self.assertEqual(len(groups[1]['states']),3)
        self.assertEqual({s['scope_hash'] for s in groups[1]['states']},{'scope:txphish_src001','scope:txphish_src002'})

    def test_trusted_arrival_hash_never_requests_header(self):
        calls=[]
        def lookup(plan):calls.append(plan);return cache(plan)
        result=p.derive(snap([row()]),unknown,lookup,{},5)
        self.assertEqual([x['method'] for x in calls],['eth_getCode'])
        self.assertEqual(result['header_requirements'],[])
        self.assertEqual(result['code_groups'][0]['block_hash'],H)

    def test_missing_hash_exact_header_separate_and_shared(self):
        result=p.derive(snap([row(number=1,hash=None),row(number=2,hash=None)]),unknown,cache,{},5)
        self.assertEqual(len(result['header_requirements']),1)
        self.assertEqual(result['next_batch']['requests'][0]['request'],{'method':'eth_getBlockByNumber','params':['0x64',False]})
        self.assertEqual(len(result['next_batch']['requests']),1)  # Code proposed after the missing hash is resolved.

    def test_existing_exact_header_fills_hash_without_network_need(self):
        def lookup(plan):
            info=cache(plan)
            if plan['method']=='eth_getBlockByNumber':info.update(state='SUCCESS_ENVELOPE_SHA_AND_RUNTIME_VALIDATED',result={'hash':H,'number':'0x64'},artifact_ref={'path':'synthetic/h','sha256':'a'*64})
            return info
        result=p.derive(snap([row(hash=None)]),unknown,lookup,{},5)
        self.assertEqual(result['code_groups'][0]['block_binding_status'],'EXACT_CURRENT_HEADER_CACHE')
        self.assertEqual([r['request']['method'] for r in result['next_batch']['requests']],['eth_getCode'])

    def test_same_block_conflicts_and_malformed_hash_never_replaced(self):
        result=p.derive(snap([row(number=1),row(number=2,hash='0x'+'b'*64)]),unknown,cache,{},5)
        self.assertTrue(all(g['block_binding_status']=='BLOCK_HASH_CONFLICT' for g in result['code_groups']))
        self.assertEqual(result['next_batch']['requests'],[])
        with self.assertRaises(ValueError):p.derive(snap([row(hash='0xBAD')]),unknown,cache,{},5)

    def test_exact_empty_code_vs_zero_byte_and_no_identity_or_stop_claim(self):
        def lookup(plan):return cache(plan)|{'state':'SUCCESS_ENVELOPE_SHA_AND_RUNTIME_VALIDATED','result':'0x' if plan['params'][0].endswith('1') else '0x00'}
        result=p.derive(snap([row(number=1),row(number=2)]),unknown,lookup,{},5)
        self.assertEqual([g['code_status'] for g in result['code_groups']],['NO_CODE_AT_BLOCK','CODE_PRESENT'])
        self.assertEqual(result['next_batch']['requests'],[])
        self.assertTrue(all(g['cost_stop_applied'] is False and g['checked_unknown_claimed'] is False for g in result['code_groups']))
        self.assertEqual(result['web_requests'],0)

    def test_five_or_hundred_and_existing_failure_inflight_not_new_keys(self):
        rows=[row(number=i) for i in range(1,111)]
        def lookup(plan):
            info=cache(plan);n=int(plan['params'][0],16)
            if n in (1,2):info.update(state='PERMANENT_FAILURE' if n==1 else 'IN_FLIGHT',existing_attempt_count=3)
            return info
        for maximum in (5,100):
            result=p.derive(snap(rows),unknown,lookup,{},maximum)
            self.assertEqual(len(result['next_batch']['requests']),maximum)
            self.assertTrue(all(int(r['request']['params'][0],16) not in (1,2) for r in result['next_batch']['requests']))
        with self.assertRaises(ValueError):p.derive(snap(rows),unknown,cache,{},6)

class LocalEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]);self.addCleanup(self.tmp.cleanup)
        self.r=Path(self.tmp.name);self.w=self.r/'code';self.w.mkdir();self.inputs=p.Inputs(self.r)
    def save(self,path,value):
        path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(p.canonical(value));return path

    def test_local_frozen_owner_enrichment_not_four_table_success(self):
        addresses={'0x'+format(i,'040x') for i in range(1,5)};a,b,c,d=sorted(addresses)
        docs={'rows':[{'address':a,'lookup_status':'LOCAL_FROZEN'},
            {'address':b,'lookup_status':'COMPLETED_OWNER_METADATA_ENRICHMENT'},
            {'address':c,'lookup_status':'COMPLETED_FOUR_TABLE_OPPORTUNITY'},{'address':d,'lookup_status':'FAILED_RESOURCE_CAP'}],
            'opportunities':[{'address':c,'source_table':table,'execution_id':'synthetic-exec','status':'SUCCESS_NO_MATCH_IN_THIS_TABLE_EXECUTION_ONLY','match_count':0} for table in sorted(p.TABLES)]}
        self.save(self.w/'derived/stage1d/labels/result.json',docs)
        statuses=p.label_status_index(self.w,addresses,self.inputs)
        self.assertEqual([statuses[x]['online_label_status'] for x in (a,b,c,d)],['UNQUERIED','UNQUERIED','SUCCESS','FAILED'])
        self.assertTrue(all(s['checked_unknown_claimed'] is False for s in statuses.values()))

    def test_incomplete_opportunities_unknown_and_prior_success_record_preserved(self):
        a='0x'+'1'*40
        self.save(self.w/'derived/stage1d/labels/result.json',{'rows':[{'address':a,'lookup_status':'COMPLETED_FOUR_TABLE_OPPORTUNITY'}],'opportunities':[]})
        self.assertEqual(p.label_status_index(self.w,{a},self.inputs)[a]['online_label_status'],'UNKNOWN')
        self.save(self.w/'private/stage1d_inputs/HISTORICAL_LABEL_SUCCESS.json',{'addresses':[a],'source_sha256':'a'*64})
        self.assertEqual(p.label_status_index(self.w,{a},self.inputs)[a]['online_label_status'],'SUCCESS')

    def test_allocated_and_failed_label_job_are_not_empty_success(self):
        a='0x'+'1'*40;doc={'addresses':[a],'query_id':'synthetic'}
        name=hashlib.sha256(json.dumps(doc,sort_keys=True).encode()).hexdigest()+'.json'
        cohort=self.save(self.w/'private/stage1d_label_cohorts'/name,doc)
        self.assertEqual(p.label_status_index(self.w,{a},self.inputs)[a]['online_label_status'],'PENDING')
        self.save(self.w/'private/stage1d_sql/synthetic/freeze_manifest.json',{'kind':'frontier_labels','sql_sha256':'b'*64,
            'dependencies':[{'path':cohort.relative_to(self.w).as_posix(),'sha256':p.sha(cohort)}]})
        self.save(self.w/'private/dune_r2_jobs'/('b'*64)/'job.json',{'state':'QUERY_STATE_FAILED','execution_id':'failed-synthetic'})
        self.assertEqual(p.label_status_index(self.w,{a},self.inputs)[a]['online_label_status'],'FAILED')

    def database(self,plan,value='0x',state='SUCCESS',alter_request=False):
        from stage1d_runtime import Runtime
        from read_retry_r4 import logical_key
        runtime=Runtime();identity=runtime.rpc_identity(p.PROVIDER,plan);key=logical_key(identity)
        req=dict(plan,id=1,jsonrpc='2.0')
        if alter_request:req['params']=[plan['params'][0],'0x65']
        env=self.save(self.w/'raw/synthetic/envelope.json',{'request':req,'response':{'jsonrpc':'2.0','id':1,'result':value}})
        dbpath=self.w/'private/cache.sqlite';dbpath.parent.mkdir(parents=True,exist_ok=True)
        with closing(sqlite3.connect(dbpath)) as db:
            db.executescript('CREATE TABLE read_requests(logical_key TEXT,identity_json TEXT,state TEXT,next_eligible_at REAL,success_payload TEXT,success_receipt TEXT);CREATE TABLE read_attempts(logical_key TEXT,outcome TEXT);')
            db.execute('INSERT INTO read_requests VALUES(?,?,?,?,?,?)',(key,json.dumps(identity),state,1234,json.dumps(value),json.dumps({'artifact_path':env.relative_to(self.w).as_posix(),'artifact_sha256':p.sha(env)})))
            db.executemany('INSERT INTO read_attempts VALUES(?,?)',[(key,'PERMANENT_FAILURE'),(key,'SUCCESS'),(key,'ABANDONED_BEFORE_DISPATCH')])
            db.commit()
        return dbpath,runtime,logical_key,env

    def test_real_runtime_ro_cache_validates_exact_envelope_and_preserves_counts(self):
        plan={'method':'eth_getCode','params':['0x'+'1'*40,'0x64']};path,runtime,key,env=self.database(plan)
        before=p.sha(path)
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
            db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
            result=p.ReadOnlyCache(self.w,db,runtime,self.inputs,key).lookup(plan)
            self.assertEqual(result['state'],'SUCCESS_ENVELOPE_SHA_AND_RUNTIME_VALIDATED');self.assertEqual(result['result'],'0x')
            self.assertEqual(result['existing_attempt_count'],2);self.assertFalse(result['retry_reset'])
            with self.assertRaises(sqlite3.OperationalError):db.execute('DELETE FROM read_requests')
        self.assertEqual(p.sha(path),before)

    def test_wrong_selector_or_mutated_artifact_is_blocked_not_cache_miss(self):
        plan={'method':'eth_getCode','params':['0x'+'1'*40,'0x64']};path,runtime,key,env=self.database(plan,alter_request=True)
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
            result=p.ReadOnlyCache(self.w,db,runtime,self.inputs,key).lookup(plan)
            self.assertEqual(result['state'],'BLOCKED_EXISTING_SUCCESS_EVIDENCE');self.assertFalse(result['new_key_allowed'])
            env.write_bytes(env.read_bytes()+b' ')
            result=p.ReadOnlyCache(self.w,db,runtime,p.Inputs(self.r),key).lookup(plan)
            self.assertEqual(result['state'],'BLOCKED_EXISTING_SUCCESS_EVIDENCE')

    def test_existing_failed_code_key_and_original_next_eligible_preserved(self):
        plan={'method':'eth_getCode','params':['0x'+'1'*40,'0x64']};path,runtime,key,env=self.database(plan,state='PERMANENT_FAILURE')
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
            result=p.ReadOnlyCache(self.w,db,runtime,self.inputs,key).lookup(plan)
        self.assertEqual(result['state'],'PERMANENT_FAILURE');self.assertEqual(result['next_eligible_at'],1234)
        self.assertEqual(result['existing_attempt_count'],2);self.assertNotIn('result',result)

    def test_confined_output_and_default_review_do_not_execute(self):
        for path in ('../escape.json','D:/elsewhere/x.json'):
            with self.assertRaises(ValueError):p.inside(self.r,path)
        with patch.object(p,'prepare',side_effect=AssertionError('No actual preparation')),redirect_stdout(io.StringIO()) as buf:p.main([])
        self.assertEqual(json.loads(buf.getvalue())['status'],'REVIEW_ONLY_NO_IO_OR_NETWORK')

if __name__=='__main__':unittest.main()
