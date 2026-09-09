"""Explicit synthetic rule checks. No provider, API, ledger or solver is run."""
import copy,json,tempfile,unittest
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch
from collector import Collector,Event,FetchResult,Scope,State
from stage1d_role_adoption import SCHEMA,STOP_BASIS,STOP_SUPPLEMENT,TechnicalRoles,validate_certificate,sha
from test_stage1d_role_adoption_evidence import fixture,ADDR,PAYER,IMPL

class ProportionateScopeStopTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent);self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
  self.scope=Scope('controlled-query','synthetic',1,20,1,200,3)
  self.q={'query_id':self.scope.query_id,'scope_id':self.scope.scope_id,'scope_hash':self.scope.scope_hash,'start_block':1,'end_block':20}
  self.batch=patch('stage1d_closure_scope.active_batch',return_value={'queries':[self.q]});self.batch.start();self.addCleanup(self.batch.stop)
 def certificate(self,mutation=None):
  c=fixture(self.root,mutation=mutation,public_text=True)
  c.update(decision_basis=STOP_BASIS,supplement_id=STOP_SUPPLEMENT,query_scopes=[self.q.copy()],decision_relevant_conflicts=[],
   evidence_applicability='SYNTHETIC_CONTROLLED official current exact deployment role; no historical operation or upgrade claim')
  for key in ('start_block','end_block','code_sha256'):c.pop(key)
  c['dependencies']=c['dependencies'][:1];dep=c['dependencies'][0];p=self.root/dep['path'];d=json.loads(p.read_text());d['basis']=STOP_BASIS;p.write_text(json.dumps(d));dep['sha256']=sha(p)
  return c
 def resolve(self,c,state=None):
  roles=TechnicalRoles(self.root);roles.records=[validate_certificate(self.root,c)]
  e=Event('enter','0x'+'4'*64,PAYER,ADDR,'native:eip155:1',9,10,0,100)
  return roles.resolve(state or State(self.scope.query_id,ADDR,e.asset,e,1,200),{'kind':'UNKNOWN'})
 def test_single_exact_original_source_stops_without_historical_rpc_or_proxy(self):
  c=self.certificate()
  with patch('stage1d_role_adoption.transaction',side_effect=AssertionError('No historical RPC required for scope stop')),patch('stage1d_role_adoption.code_at',side_effect=AssertionError('No code/proxy required for scope stop')):
   verified=validate_certificate(self.root,c)
  self.assertEqual(verified['verification']['status'],'PASS');self.assertFalse(verified['verification']['historical_operation_certification'])
  out=self.resolve(c);self.assertEqual(out['technical_role_status'],'OFFICIAL_SOURCE_SUPPORTED_SCOPE_BOUNDARY');self.assertNotIn('historical_role_blocks',out);self.assertIsNone(out['actor'])
 def test_actual_collector_stops_before_fetch_retains_entry_and_other_legal_branch(self):
  c=self.certificate();roles=TechnicalRoles(self.root);roles.records=[validate_certificate(self.root,c)];requests=[]
  seed=Event('seed','0x'+'5'*64,IMPL,PAYER,'native:eip155:1',10,1,0,10)
  entering=Event('enter','0x'+'4'*64,PAYER,ADDR,seed.asset,6,10,0,100)
  legal='0x'+'6'*40;other=Event('other','0x'+'6'*64,PAYER,legal,seed.asset,3,11,0,110)
  class Provider:
   replay_only=True
   def fetch_interval(self,address,*args,**kwargs):
    requests.append(address);return FetchResult(events=[entering,other] if address==PAYER else [],complete=True,cache_hits=1)
  class Labels:
   def __call__(self,address):return {'kind':'UNKNOWN','name':'DEX Trader'}
   def resolve_state(self,state):return roles.resolve(state,self(state.address))
  result=asdict(Collector(Provider(),Labels()).run(self.scope,seed))
  self.assertNotIn(ADDR,requests);self.assertIn(legal,requests)
  self.assertEqual({e['event_id'] for e in result['candidate_events']},{'seed','enter','other'})
  self.assertTrue(any(s['entry_event_id']=='enter' for s in result['stops']))
  self.assertFalse(any(s.get('state',{}).get('address')==ADDR for s in result['unresolved_frontier']))
 def test_wrong_chain_address_and_ambiguous_name_rejected(self):
  for mutation in ('other_chain_record','name_only','not_official'):
   with self.subTest(mutation=mutation),tempfile.TemporaryDirectory(dir=self.root) as tmp:
    original=self.root;self.root=Path(tmp)
    try:
     c=self.certificate(mutation)
     with self.assertRaises((ValueError,KeyError)):validate_certificate(self.root,c)
    finally:self.root=original
  c=self.certificate();c['address']=IMPL
  with self.assertRaisesRegex(ValueError,'chain/address'):validate_certificate(self.root,c)
 def test_wrong_certificate_chain_rejected(self):
  c=self.certificate();c['chain_id']='eip155:56'
  with self.assertRaisesRegex(ValueError,'Ethereum'):validate_certificate(self.root,c)
 def test_pure_trader_record_cannot_borrow_pagewide_bridge_context(self):
  c=self.certificate();base=self.root/'private/stage1d_roles';body=base/'body_1.txt';d=json.loads(body.read_text());d['deployments'][0]['role']='DEX Trader';d['unrelated_caption']='Bridge contract';body.write_text(json.dumps(d))
  jp=base/'journal.json';j=json.loads(jp.read_text());j['body_sha256']=sha(body);j['attempts'][0].update(body_sha256=sha(body),bytes=body.stat().st_size);jp.write_text(json.dumps(j))
  dp=self.root/c['dependencies'][0]['path'];d=json.loads(dp.read_text());d['source_refs'][0].update(sha256=sha(body));d['source_refs'][0]['journal']['sha256']=sha(jp);dp.write_text(json.dumps(d));c['dependencies'][0]['sha256']=sha(dp)
  with self.assertRaisesRegex(ValueError,'record.*role'):validate_certificate(self.root,c)
 def test_known_decision_relevant_contradiction_stays_pending(self):
  c=self.certificate();c['decision_relevant_conflicts']=[{'fact':'Address may belong to a different chain deployment','status':'PENDING'}]
  with self.assertRaisesRegex(ValueError,'contradiction.*pending'):validate_certificate(self.root,c)
 def test_supported_operation_cannot_use_scope_stop_route(self):
  c=self.certificate();c.update(branch_action='SUPPORTED_OPERATION_RESOLVE',technical_role_status='VERIFIED_SUPPORTED_COMPONENT_CONTRACT')
  with self.assertRaisesRegex(ValueError,'supported operation proof remains strict'):validate_certificate(self.root,c)
 def test_supported_default_route_still_requires_historical_evidence(self):
  c=self.certificate();c.pop('decision_basis');c.update(branch_action='SUPPORTED_OPERATION_RESOLVE',technical_role_status='VERIFIED_SUPPORTED_COMPONENT_CONTRACT',start_block=10,end_block=10)
  with self.assertRaisesRegex(ValueError,'historical execution'):validate_certificate(self.root,c)
 def test_scope_cannot_expand_to_other_query_or_block(self):
  c=self.certificate();c['query_scopes'][0]['scope_hash']='0'*64
  with self.assertRaisesRegex(ValueError,'another frozen'):validate_certificate(self.root,c)
 def test_no_query_identity_no_scope_stop(self):
  c=self.certificate();e=Event('enter','0x'+'4'*64,PAYER,ADDR,'native:eip155:1',9,10,0,100)
  result=self.resolve(c,State('other-query',ADDR,e.asset,e,1,200));self.assertEqual(result['kind'],'UNKNOWN')
 def test_original_source_tamper_rejected(self):
  c=self.certificate();p=self.root/'private/stage1d_roles/body_1.txt';p.write_text('unbound name')
  with self.assertRaisesRegex(ValueError,'changed'):validate_certificate(self.root,c)
 def test_historical_claims_not_created_from_current_source(self):
  c=self.certificate();c['start_block']=10;c['end_block']=20
  with self.assertRaisesRegex(ValueError,'cannot claim historical'):validate_certificate(self.root,c)
 def test_scope_stop_never_substitutes_for_zero_amount_proof(self):
  c=self.certificate();c['zero_source_upper_bound_certification']=True
  with self.assertRaisesRegex(ValueError,'zero source bounds'):validate_certificate(self.root,c)
 def test_user_boundary_keeps_nonverified_channel(self):
  from test_stage1d_task_boundaries import TaskBoundaryTests
  helper=TaskBoundaryTests();roles,scope=helper.setup_record(self.root)
  e=Event('enter','0x'+'4'*64,'0x'+'1'*40,'0x'+'2'*40,'native:eip155:1',9,2,0,20)
  result=roles.resolve(State('query1',e.recipient,e.asset,e,1,100),{'kind':'UNKNOWN'})
  self.assertEqual(result['technical_role_status'],'USER_DECLARED_TASK_BOUNDARY');self.assertFalse(result['chain_verified_role_claim'])
 def test_context_removes_platform_only_retains_ordinary_normal_return_and_gas(self):
  from test_stage1d_context_boundaries import fixture as context_fixture
  from test_stage1d_context import event,anchors,complete,A,B,X
  from stage1d_context import necessary_context_windows,build_document
  q,col,labels,seed,out,service=context_fixture();returning=event(4,X,A,10,11,index=1);unrelated=event(5,X,B,1234,1000)
  saved=copy.deepcopy(col);plan=necessary_context_windows(q,col,[returning,unrelated],labels)
  self.assertEqual([r['address'] for r in plan['rows']],[A]);self.assertEqual(col,saved)
  b,h=anchors([(A,9,20),(A,12,38),(X,10,1000)])
  result=build_document(q,col,[returning],b,h,label_snapshot=labels,coverage=complete(plan),context_plan=plan)
  doc=result['model_input'];flows={f['event_id']:f for t in doc['transactions'] for f in t['flows']};fees=[f for t in doc['transactions'] for f in t['fees']]
  self.assertEqual(flows[out.event_id]['amount_raw'],'50');self.assertEqual(flows[returning.event_id]['amount_raw'],'10');self.assertNotIn('source_zero_basis',flows[returning.event_id]);self.assertEqual(doc['accounts'][0]['initial_actual_balance_raw'],'20');self.assertTrue(any(f['amount_raw']=='2' for f in fees))
 def test_weth_incomplete_evidence_still_cannot_certify(self):
  from stage1d_semantic_units import certify_instance
  with self.assertRaises((ValueError,KeyError,TypeError)):certify_instance({'kind':'DEPOSIT','contract':ADDR},{})

if __name__=='__main__':unittest.main()
