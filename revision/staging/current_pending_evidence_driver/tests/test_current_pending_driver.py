"""Five synthetic entry/transport checks. All files remain in staging mirrors."""
from copy import deepcopy
from dataclasses import asdict
import hashlib,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import execute_unknown_cost_evidence as d
from collector import Event,State,Scope,NATIVE
import stage1d_unknown_cost_boundary as core

STAGE=Path(__file__).resolve().parents[1]
A='0x'+'a'*40;B='0x'+'b'*40

class FakeRuntime:
    def __init__(self):self.gates=0;self.pass_gate=True
    def validate_rpc(self,p):return deepcopy(p)
    def rpc_identity(self,provider,p):return dict(provider=provider,**deepcopy(p))
    def require_gate(self,work):
        self.gates+=1
        if not self.pass_gate:raise RuntimeError('Active gate PENDING')

class Fixture:
    def __init__(self,root,runtime):
        self.root=root;self.runtime=runtime;self.refs={};self.sources={}
        self.q=[];self.collections={};self.members=[]
        self.write('reports/unknown_cost_boundary_v1/inputs/INITIAL_SNAPSHOT.json',{'SYNTHETIC_CONTROLLED':True},bound=False)
        self.initial_sha=d.sha(root/'reports/unknown_cost_boundary_v1/inputs/INITIAL_SNAPSHOT.json')
        for name in d.QUERY_NAMES:
            q=dict(query_id=name,name=name,start_block=1,end_block=100,start_time_utc=0,end_time_utc=1000,
                max_acquisition_depth=9,window_mode='QUERY_ARRIVAL_WINDOW_SECONDS_V1',local_window_seconds=1000)
            scope=Scope.from_policy(q);q.update(scope_id=scope.scope_id,scope_hash=scope.scope_hash);self.q.append(q)
        policy=dict(schema_version=core.POLICY_SCHEMA,authorization_id=core.AUTH,enabled=True,
            threshold_daily_strict_gt=20,authority_source_sha256=core.AUTHORITY_SHA256,
            query_scope_hashes={q['query_id']:q['scope_hash'] for q in self.q})
        pref=self.write('code/private/stage1d_roles/UNKNOWN_COST_POLICY.json',policy)
        current=dict(schema_version='stage1d-unknown-cost-registry-v1',policy_ref={'path':'private/stage1d_roles/UNKNOWN_COST_POLICY.json','sha256':pref['sha256']},
            initial_snapshot_ref={'path':'../reports/unknown_cost_boundary_v1/inputs/INITIAL_SNAPSHOT.json','sha256':self.initial_sha})
        self.write('code/private/stage1d_roles/UNKNOWN_COST_CURRENT.json',current)
        self.write('code/private/BATCH_QUERY_FREEZE.json',{'queries':self.q})
        self.write(d.CURRENT_HELPER,{'synthetic_source':'current'})
        self.write(d.INITIAL_HELPER,{'synthetic_source':'original'})
        for q in self.q:
            scope=Scope.from_policy(q);states=[];decisions=[]
            if q['name'] in d.QUERY_NAMES[:2]:
                ev=Event('event:'+q['name'],'0x'+'1'*64,B,A,NATIVE,100,10,0,10)
                state=asdict(State(scope.query_id,A,NATIVE,ev,1,scope.local_end(ev)))
                decision=core.make_decision(state,scope,policy_sha256=pref['sha256'],action='PENDING',reason='TYPE_UNRESOLVED')
                states=[dict(state=state,identity={'kind':'UNKNOWN'},cost_boundary=decision)];decisions=[decision]
            co=dict(query_id=q['query_id'],states=states,cost_boundary_decisions=decisions,
                cost_boundary_policy=dict(schema_version=core.POLICY_SCHEMA,authorization_id=core.AUTH,enabled=True,policy_sha256=pref['sha256']))
            ref=self.write('code/derived/stage1d/queries/'+q['name']+'/collection.json',co);self.collections[q['name']]=ref
            if states:
                self.members.append(dict(current_row_index=0,state_key=core.state_key(state,scope),query_name=q['name'],query_id=q['query_id'],
                    scope_hash=scope.scope_hash,collection_ref=ref,saved_decision_sha256=decision['decision_sha256'],
                    current_identity={'kind':'UNKNOWN','status':'LOCAL_FROZEN'}))
        self.gkey=hashlib.sha256(json.dumps(['eip155:1',A,10],separators=(',',':')).encode()).hexdigest()
        self.group=dict(group_key=self.gkey,chain_id='eip155:1',address=A,arrival_block=10,states=self.members,
            future_new_states_included=True,input_origin='CURRENT_SAVED_PENDING_STATES',block_binding_status='EXACT_CURRENT_HEADER_CACHE',
            label_status={'online_label_status':'SUCCESS'})
        self.request={'method':'eth_getCode','params':[A,'0xa']}
        self.row=dict(request=self.request,logical_key=d.logical_key(runtime.rpc_identity('ALCHEMY_ETH_MAINNET_EXISTING',self.request)),
            first_group_key=self.gkey,query_names=list(d.QUERY_NAMES[:2]),reason='EXACT_CURRENT_PENDING_HISTORICAL_CODE')
        self.doc=dict(schema_version=d.CURRENT_SCHEMA,authorization_id=core.AUTH,status='PREPARED_OFFLINE_NOT_EXECUTED',
            source_and_current_inputs_stable=True,initial_snapshot_modified=False,
            original_group_key_and_runtime_logical_key_preserved=True,source_and_current_input_sha256=self.sources,
            input_refs=list(self.refs.values()),current_collection_refs=self.collections,code_groups=[self.group],
            current_search_status={},helper_sha256=self.sources[d.CURRENT_HELPER],original_helper_sha256=self.sources[d.INITIAL_HELPER],
            next_batch={'maximum_rpc_members':5,'requests':[self.row]})
        self.save()
    def write(self,rel,value,bound=True):
        p=self.root/rel;p.parent.mkdir(parents=True,exist_ok=True);raw=json.dumps(value,sort_keys=True).encode();p.write_bytes(raw)
        ref=dict(path=rel,sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))
        if bound:self.refs[rel]=ref;self.sources[rel]=ref['sha256']
        return ref
    def save(self):
        self.path='preparation.json';ref=self.write(self.path,self.doc,bound=False);self.sha=ref['sha256']

class CurrentDriverTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=STAGE);self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.runtime=FakeRuntime();self.f=Fixture(self.root,self.runtime)
        self.calls=[];self.constructed=0;self.cache={};self.status='COMPLETE'
        for patcher in (patch.object(d,'R',self.root),patch.object(d,'C',self.root/'code'),patch.object(d,'SNAP_SHA',self.f.initial_sha),
                        patch.object(d,'_current_cache',side_effect=self.lookup)):
            patcher.start();self.addCleanup(patcher.stop)
    def lookup(self,rows,runtime):
        return [dict(logical_key=row['logical_key'],state=self.cache.get(row['logical_key'],'NO_CURRENT_EXACT_REQUEST')) for row in rows]
    def factory(self,work,**kwargs):
        self.constructed+=1
        test=self
        class Access:
            def call_batch(self,plans,owner,job,**opts):
                test.calls.append(dict(plans=deepcopy(plans),owner=owner,job=job,opts=opts))
                for p in plans:test.cache[d.logical_key(test.runtime.rpc_identity('ALCHEMY_ETH_MAINNET_EXISTING',p))]='SUCCESS_ENVELOPE_SHA_AND_RUNTIME_VALIDATED'
                return dict(status=test.status,actual_operations_this_call=len(plans))
        return Access()
    def execute(self):
        record={'results':[]}
        d.execute_code_preparation(self.f.path,self.f.sha,self.runtime,record,self.root/'receipt.json',access_factory=self.factory)
        return record
    def test_stale_graph_source_initial_and_gate_reject_before_access(self):
        for name in ('graph','source','initial','gate','added_label'):
            with self.subTest(name=name):
                f=self.f;modified=None;old=None
                if name=='gate':self.runtime.pass_gate=False
                elif name=='added_label':
                    modified=self.root/'code/derived/stage1d/labels/new.json'
                    modified.parent.mkdir(parents=True,exist_ok=True);modified.write_bytes(b'{}')
                else:
                    rel=f.collections[d.QUERY_NAMES[0]]['path'] if name=='graph' else d.CURRENT_HELPER if name=='source' else 'reports/unknown_cost_boundary_v1/inputs/INITIAL_SNAPSHOT.json'
                    modified=self.root/rel;old=modified.read_bytes();modified.write_bytes(old+b' ')
                try:
                    with self.assertRaises((ValueError,RuntimeError)):self.execute()
                    self.assertEqual(self.constructed,0);self.assertEqual(self.calls,[])
                finally:
                    if modified:
                        if old is None:modified.unlink()
                        else:modified.write_bytes(old)
                    self.runtime.pass_gate=True
    def test_shared_owner_exact_key_preserved_success_not_reexecuted(self):
        first=self.execute();self.assertEqual(len(self.calls),1)
        self.assertEqual(self.calls[0]['owner'],'SHARED');self.assertEqual(self.calls[0]['plans'],[self.f.request])
        self.assertEqual(first['results'][0]['needs'][0]['logical_key'],self.f.row['logical_key'])
        second=self.execute();self.assertEqual(len(self.calls),1);self.assertEqual(self.constructed,1)
        self.assertEqual(second['cache_reuse'][0]['need'],self.f.row)
    def test_finite_failure_and_invalid_owner_key_or_selector_never_dispatch(self):
        original=deepcopy(self.f.doc)
        for mutation in ('failure','owner','key','selector','saved_decision'):
            with self.subTest(mutation=mutation):
                self.f.doc=deepcopy(original)
                if mutation=='failure':self.f.doc['current_search_status'][A]={'status':'ACCESS_BLOCKED'}
                elif mutation=='owner':self.f.doc['next_batch']['requests'][0]['query_names']=['lifi_src001']
                elif mutation=='key':self.f.doc['next_batch']['requests'][0]['logical_key']='0'*64
                elif mutation=='selector':self.f.doc['next_batch']['requests'][0]['request']['params'][1]='0xb'
                else:self.f.doc['code_groups'][0]['states'][0]['saved_decision_sha256']='0'*64
                self.f.save()
                with self.assertRaises(ValueError):self.execute()
                self.assertEqual(self.constructed,0);self.assertEqual(self.calls,[])
        self.f.doc=original;self.f.save();self.cache[self.f.row['logical_key']]='RETRYABLE_FAILED'
        with self.assertRaises(RuntimeError):self.execute()
        self.assertEqual(self.constructed,0)
    def test_initial_mode_keeps_original_grouping_and_partial_stops(self):
        rows=[]
        for i in range(7):
            plan={'method':'eth_getCode','params':[A,hex(i+1)]}
            rows.append(dict(request=plan,logical_key=d.logical_key(self.runtime.rpc_identity('ALCHEMY_ETH_MAINNET_EXISTING',plan)),query_names=['txphish_src002']))
        self.f.doc=dict(schema_version=d.INITIAL_SCHEMA,authorization_id=core.AUTH,
            initial_snapshot_ref={'sha256':self.f.initial_sha},source_and_label_sha256={},next_batch={'requests':rows,'maximum_rpc_members':100})
        self.f.save();self.status='PARTIAL';record=self.execute()
        self.assertEqual(len(self.calls),1);self.assertEqual(len(self.calls[0]['plans']),5)
        self.assertEqual(self.calls[0]['owner'],'txphish_src002');self.assertEqual(record['results'][0]['needs'],rows[:5])
    def test_current_header_schema_uses_same_key_without_code_substitution(self):
        req={'method':'eth_getBlockByNumber','params':['0xa',False]}
        row=self.f.doc['next_batch']['requests'][0];row['request']=req
        row['logical_key']=d.logical_key(self.runtime.rpc_identity('ALCHEMY_ETH_MAINNET_EXISTING',req))
        self.f.save();self.execute()
        self.assertEqual(self.calls[0]['plans'],[req]);self.assertEqual(self.calls[0]['owner'],'SHARED')

if __name__=='__main__':unittest.main()
