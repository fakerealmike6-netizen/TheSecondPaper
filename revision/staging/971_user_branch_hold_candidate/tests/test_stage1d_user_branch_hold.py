"""Synthetic offline hold semantics; no production data, requests, or solver."""
import copy
import hashlib
import json
import tempfile
import unittest
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from collector import Collector, Event, Scope, FetchResult, State, NATIVE
from stage1d_task_boundaries import TaskBoundaries, SCHEMA, HOLD
from stage1d_context import required_context_windows, build_document
from stage1d_multiasset_context import required_windows
from stage1d_closure_context import ROLE_FIELDS

S,A,H,B,T,N = ['0x'+x*40 for x in '123456']

def event(n, sender, recipient, value, block, gas=0, success=True):
    tx='0x'+format(n,'064x')
    return Event('eip155:1:tx:'+tx+':top',tx,sender,recipient,NATIVE,value,block,0,block*10,
                 gas_raw=gas,success=success,provenance='SYNTHETIC_USER_HOLD')

class UserBranchHoldTests(unittest.TestCase):
    def setup_hold(self, root, mutate=None):
        scope=Scope('synthetic:q1','scope',1,20,1,200,3)
        q={'name':'q1','query_id':scope.query_id,'scope_id':scope.scope_id,'scope_hash':scope.scope_hash,'start_block':1,'end_block':20}
        message='Temporarily hold this current branch until I explicitly resume it.'
        record={'boundary_id':'synthetic-hold','authority_id':'synthetic-user-authority',
                'chain_id':'eip155:1','address':H,'branch_action':HOLD,'reason':'USER_DIRECTION',
                'temporary':True,'resume_condition':'EXPLICIT_USER_RESUME',
                'chain_verified_role_claim':False,'source_zero_claim':False,'identity_change_authorized':False,
                'user_message':message,'user_message_sha256':hashlib.sha256(message.encode()).hexdigest(),'queries':[q]}
        if mutate: mutate(record)
        doc={'schema_version':SCHEMA,'authorization_id':'STAGE1D_WINDOW_ROLE_SEMANTIC_CLOSURE_V1',
             'chain_verified_role_claim':False,'raw_evidence_deleted':False,'boundaries':[record]}
        path=root/'private/stage1d_roles/USER_TASK_BOUNDARIES.json';path.parent.mkdir(parents=True)
        path.write_text(json.dumps(doc),encoding='utf-8')
        with patch('stage1d_task_boundaries.active_batch',return_value={'queries':[q]}):
            roles=TaskBoundaries(root)
        return roles,scope

    def collect(self, roles, scope, partial=False):
        seed=event(1,S,A,80,1)
        facts=[seed,event(2,A,H,30,2),event(3,A,B,40,3),event(4,H,T,30,4),
               event(5,B,T,20,5),event(6,B,H,10,6)]
        requested=[]
        class Provider:
            replay_only=True
            def fetch_interval(self,address,*args,**kwargs):
                requested.append(address)
                complete=not (partial and address==B)
                return FetchResult(events=[e for e in facts if address in (e.sender,e.recipient)],
                                   complete=complete,cache_hits=1,
                                   gaps=[] if complete else [{'reason':'SYNTHETIC_MISSING_PAGE'}])
        class Labels:
            def __call__(self,address):
                return {'kind':'SERVICE' if address==T else 'UNKNOWN','actor':None,'status':'LOCAL_FROZEN'}
            def resolve_state(self,state):return roles.resolve(state,self(state.address))
        return Collector(Provider(),Labels()).run(scope,seed),requested,facts

    def test_stops_request_retains_each_arrival_and_alternative_route(self):
        with tempfile.TemporaryDirectory() as tmp:
            roles,scope=self.setup_hold(Path(tmp));got,requests,facts=self.collect(roles,scope)
            self.assertNotIn(H,requests);self.assertIn(B,requests)
            ids={e['event_id'] for e in got.candidate_events}
            self.assertIn(facts[1].event_id,ids);self.assertIn(facts[5].event_id,ids)
            self.assertIn(facts[4].event_id,ids);self.assertNotIn(facts[3].event_id,ids)
            held=[s for s in got.stops if s['reason']==HOLD]
            self.assertEqual(len(held),2)
            self.assertEqual({s['entry_event_id'] for s in held},{facts[1].event_id,facts[5].event_id})
            self.assertEqual({s['identity']['kind'] for s in held},{'UNKNOWN'})
            self.assertEqual({s['state']['depth'] for s in held},{1,2})
            self.assertEqual({s['state']['local_end'] for s in held},{scope.end_time})
            self.assertEqual(len(got.unresolved_frontier),2)
            self.assertEqual(got.status,'INCOMPLETE_USER_REQUESTED_BRANCH_HOLD')
            self.assertEqual(len([g for g in got.gaps if g['reason']==HOLD]),2)
            self.assertEqual(got.metrics['service_entry_state_count'],1)

    def test_mixed_missing_page_keeps_data_gap_classification(self):
        with tempfile.TemporaryDirectory() as tmp:
            roles,scope=self.setup_hold(Path(tmp));got,_,_=self.collect(roles,scope,True)
            self.assertEqual(got.status,'INCOMPLETE_PROVIDER_OR_DATA_GAP')
            self.assertTrue(any(s['reason']==HOLD for s in got.unresolved_frontier))

    def test_identity_is_preserved_and_hold_is_not_a_chain_or_amount_claim(self):
        with tempfile.TemporaryDirectory() as tmp:
            roles,scope=self.setup_hold(Path(tmp));e=event(2,A,H,30,2)
            original={'kind':'UNKNOWN','actor':'existing-attribution','status':'LOCAL_FROZEN',
                      'technical_role_status':'UNKNOWN','custodial_actor_status':'NOT_ESTABLISHED',
                      'branch_action':'NORMAL_ACCOUNT_EXPAND'}
            saved=copy.deepcopy(original);result=roles.resolve(State(scope.query_id,H,NATIVE,e,1,200),original)
            self.assertEqual(original,saved)
            for k in ('kind','actor','status','technical_role_status','custodial_actor_status'):self.assertEqual(result[k],original[k])
            self.assertEqual(result['branch_action'],HOLD)
            self.assertEqual(result['task_boundary_ids'],['synthetic-hold'])
            self.assertFalse(result['branch_hold_source_zero_claim'])
            self.assertFalse(result['branch_hold_chain_verified_role_claim'])
            self.assertTrue(set(['task_boundary_authority_ids','branch_hold_resume_condition']).issubset(ROLE_FIELDS))

    def test_exact_chain_address_query_and_range_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            roles,scope=self.setup_hold(Path(tmp));e=event(2,A,H,30,2)
            state=State(scope.query_id,H,NATIVE,e,1,200);identity={'kind':'UNKNOWN'}
            for other in [replace(state,address=B),replace(state,query_id='another-source'),
                          replace(state,arrival=SimpleNamespace(chain_id='eip155:137',block=2)),
                          replace(state,arrival=SimpleNamespace(chain_id='eip155:1',block=21))]:
                self.assertIs(roles.resolve(other,identity),identity)
            got,requests,_=self.collect(roles,replace(scope,query_id='another-source'))
            self.assertIn(H,requests);self.assertFalse(any(s['reason']==HOLD for s in got.stops))
            self.assertEqual(got.query_id,'another-source')

    def test_rejects_chain_source_zero_identity_or_automatic_resume_claims(self):
        for key,value in [('chain_verified_role_claim',True),('source_zero_claim',True),
                          ('identity_change_authorized',True),('kind','UNSUPPORTED_PROTOCOL'),
                          ('resume_condition','AUTOMATIC'),('temporary',False)]:
            with self.subTest(key=key), tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(ValueError):self.setup_hold(Path(tmp),lambda r:r.update({key:value}))

    def test_ordinary_context_keeps_normal_funds_returns_gas_and_amount_domain(self):
        with tempfile.TemporaryDirectory() as tmp:
            roles,scope=self.setup_hold(Path(tmp));got,_,facts=self.collect(roles,scope)
            query={'name':'q1','query_id':scope.query_id,'seed_event_id':facts[0].event_id,'seed_amount_raw':'80'}
            collection=got.to_dict()
            background=[event(10,N,H,7,7),event(11,H,A,5,8),event(12,H,N,999,9,gas=3,success=False)]
            labels={s['state']['address']:s['identity'] for s in got.states}
            plan=required_context_windows(query,collection,background,labels)
            self.assertIn(H,{r['address'] for r in plan['rows']})
            self.assertNotIn(H,plan['service_terminals_excluded'])
            multi_plan=required_windows(query,collection,background,labels)
            self.assertIn(H,{r['address'] for r in multi_plan['rows']})
            result=build_document(query,collection,background,label_snapshot=labels)
            self.assertNotEqual(result['completion_status'],'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')
            doc=result['model_input'];accounts={a['account_id']:a for a in doc['accounts']}
            self.assertIn(H+'|ETH',accounts);self.assertIsNone(accounts[H+'|ETH']['initial_actual_balance_raw'])
            flows={f['event_id']:f for tx in doc['transactions'] for f in tx['flows']}
            self.assertIn(facts[1].event_id,flows)
            self.assertNotIn('source_zero_basis',flows[facts[1].event_id])
            self.assertIn(background[0].event_id,flows);self.assertIn(background[1].event_id,flows)
            failed=next(tx for tx in doc['transactions'] if tx['tx_id']==background[2].tx_hash)
            self.assertEqual(failed['flows'],[])
            self.assertEqual(failed['fees'][0]['payer_account'],H+'|ETH')
            self.assertEqual(failed['fees'][0]['amount_raw'],'3')

if __name__=='__main__':unittest.main()
