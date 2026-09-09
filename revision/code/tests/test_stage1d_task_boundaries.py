import copy,hashlib,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from dataclasses import asdict
from collector import Collector,Event,Scope,FetchResult,State
from stage1d_task_boundaries import TaskBoundaries,SCHEMA
A='0x'+'1'*40;B='0x'+'2'*40;D='0x'+'3'*40
class TaskBoundaryTests(unittest.TestCase):
 def setup_record(self,root):
  scope=Scope('query1','scope',1,10,1,100,3)
  q={'name':'q1','query_id':'query1','scope_id':scope.scope_id,'scope_hash':scope.scope_hash,'start_block':1,'end_block':10}
  message='Stop this current protocol branch.'
  record={'boundary_id':'explicit-test-decision','chain_id':'eip155:1','address':B,'branch_action':'UNSUPPORTED_PROTOCOL_STOP',
   'user_message':message,'user_message_sha256':hashlib.sha256(message.encode()).hexdigest(),'queries':[q]}
  doc={'schema_version':SCHEMA,'authorization_id':'STAGE1D_WINDOW_ROLE_SEMANTIC_CLOSURE_V1','chain_verified_role_claim':False,'raw_evidence_deleted':False,'boundaries':[record]}
  p=root/'private/stage1d_roles/USER_TASK_BOUNDARIES.json';p.parent.mkdir(parents=True);p.write_text(json.dumps(doc))
  with patch('stage1d_task_boundaries.active_batch',return_value={'queries':[q]}):roles=TaskBoundaries(root)
  return roles,scope
 def test_actual_collector_stops_before_protocol_request_keeps_entry(self):
  with tempfile.TemporaryDirectory() as d:
   roles,scope=self.setup_record(Path(d));requested=[]
   incoming=Event('enter','0x'+'4'*64,A,B,'native:eip155:1',9,2,0,20)
   class Provider:
    replay_only=True
    def fetch_interval(self,address,*a,**kw):requested.append(address);return FetchResult(events=[incoming],complete=True,cache_hits=1)
   class Labels:
    def __call__(self,address):return {'kind':'UNKNOWN'}
    def resolve_state(self,state):return roles.resolve(state,self(state.address))
   seed=Event('seed','0x'+'5'*64,D,A,'native:eip155:1',10,1,0,10)
   got=asdict(Collector(Provider(),Labels()).run(scope,seed))
   self.assertNotIn(B,requested)
   self.assertTrue(any(e['event_id']=='enter' for e in got['candidate_events']))
   identity=next(x['identity'] for x in got['states'] if x['state']['address']==B)
   self.assertFalse(identity['chain_verified_role_claim']);self.assertIsNone(identity['actor'])
 def test_other_query_not_stopped(self):
  with tempfile.TemporaryDirectory() as d:
   roles,scope=self.setup_record(Path(d));e=Event('enter','0x'+'4'*64,A,B,'native:eip155:1',9,2,0,20)
   state=State('another-query',B,e.asset,e,1,100)
   self.assertEqual(roles.resolve(state,{'kind':'UNKNOWN'}),{'kind':'UNKNOWN'})
 def test_cannot_upgrade_user_decision_to_chain_verified_claim(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);roles,scope=self.setup_record(root);p=root/'private/stage1d_roles/USER_TASK_BOUNDARIES.json'
   doc=json.loads(p.read_text());doc['chain_verified_role_claim']=True;p.write_text(json.dumps(doc))
   with self.assertRaisesRegex(ValueError,'masquerade'):TaskBoundaries(root)

if __name__=='__main__':unittest.main()
