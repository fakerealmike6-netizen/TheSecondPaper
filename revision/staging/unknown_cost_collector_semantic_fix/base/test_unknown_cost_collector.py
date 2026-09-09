import copy
from dataclasses import asdict
from pathlib import Path
import tempfile
import unittest

from collector import Collector, Event, Scope, FetchResult, NATIVE
from stage1d_unknown_cost_boundary import make_decision, POLICY_SCHEMA, AUTH

A='0x'+'a'*40;B='0x'+'b'*40;C='0x'+'c'*40;P='0x'+'d'*40
POLICY_SHA='f'*64

def ev(n,sender,recipient,time=None):
    return Event('event'+str(n),'0x'+format(n,'064x'),sender,recipient,NATIVE,
                 100,n,0,n if time is None else time)

def scope():return Scope('query','query',1,100,0,1000,4,1000)

class Provider:
    replay_only=True
    def __init__(self,events):self.events=events;self.calls=[]
    def fetch_interval(self,address,asset,start,end,**kwargs):
        self.calls.append((address,start,kwargs['start_time']))
        return FetchResult(events=[e for e in self.events if e.sender==address or e.recipient==address],complete=True,cache_hits=1)

class Resolver:
    def __init__(self,action=lambda s:('CONTINUE','SYNTHETIC_ALLOWED'),enabled=True,identity='e'*64):
        self.action=action;self.enabled=enabled;self.identity=identity
    def policy_for_scope(self,q):
        return {'schema_version':POLICY_SCHEMA,'authorization_id':AUTH,'enabled':True,
                'policy_sha256':POLICY_SHA} if self.enabled else None
    def resolve(self,state,q,identity):
        action,reason=self.action(state)
        if identity.get('kind')=='SERVICE' or identity.get('branch_action')=='USER_REQUESTED_BRANCH_HOLD' or state.depth>=q.max_depth:
            action,reason='BYPASS','EXISTING_BOUNDARY'
        return make_decision(state,q,policy_sha256=POLICY_SHA,action=action,reason=reason,
            evidence_refs=[{'path':'synthetic/evidence.json','sha256':'b'*64}],base_identity=copy.deepcopy(identity))

def label(address):return {'kind':'UNKNOWN','status':'LOCAL_FROZEN'}

class CostCollectorTests(unittest.TestCase):
    def test_stop_happens_before_actual_provider_and_retains_entering_fact(self):
        seed,ab,bc=ev(1,P,A),ev(2,A,B),ev(3,B,C)
        provider=Provider([seed,ab,bc])
        resolver=Resolver(lambda s:('STOP','UNKNOWN_CODE_COST_BOUNDARY') if s.address==B else ('CONTINUE','ALLOWED'))
        result=Collector(provider,label,cost_boundary_resolver=resolver).run(scope(),seed).to_dict()
        self.assertEqual([x[0] for x in provider.calls],[A])
        self.assertEqual({x['event_id'] for x in result['candidate_events']},{seed.event_id,ab.event_id})
        self.assertEqual(result['status'],'COMPLETED_WITH_DECLARED_COST_BOUNDARIES')
        self.assertEqual(result['unresolved_frontier'],[])
        self.assertEqual(result['stops'][0]['entry_event_id'],ab.event_id)
        self.assertEqual(result['stops'][0]['identity']['kind'],'UNKNOWN')
        self.assertFalse(result['metrics']['scope_complete'])

    def test_pending_branch_leaves_other_legitimate_arrival_active(self):
        seed,ab,ac,cb,bc=ev(1,P,A),ev(2,A,B),ev(3,A,C),ev(4,C,B),ev(5,B,C)
        provider=Provider([seed,ab,ac,cb,bc])
        resolver=Resolver(lambda s:('PENDING','TYPE_UNRESOLVED') if s.arrival.event_id==ab.event_id else ('CONTINUE','ALLOWED'))
        result=Collector(provider,label,cost_boundary_resolver=resolver).run(scope(),seed).to_dict()
        self.assertNotIn((B,ab.block,ab.timestamp),provider.calls)
        self.assertIn((B,cb.block,cb.timestamp),provider.calls)
        self.assertIn(bc.event_id,{e['event_id'] for e in result['candidate_events']})
        self.assertEqual(result['unresolved_frontier'][0]['reason'],'TYPE_UNRESOLVED')
        self.assertFalse(result['metrics']['has_cost_boundary'])
        self.assertTrue(result['metrics']['has_pending_identity_or_type'])

    def test_service_and_user_hold_keep_original_semantics(self):
        seed=ev(1,P,A)
        for identity,reason in [({'kind':'SERVICE'},'FIRST_IDENTIFIED_SERVICE'),
                ({'kind':'UNKNOWN','branch_action':'USER_REQUESTED_BRANCH_HOLD'},'USER_REQUESTED_BRANCH_HOLD')]:
            with self.subTest(reason=reason):
                p=Provider([]);r=Collector(p,lambda a:identity,cost_boundary_resolver=Resolver()).run(scope(),seed).to_dict()
                self.assertEqual(p.calls,[]);self.assertEqual(r['stops'][0]['reason'],reason)
                self.assertEqual(r['stops'][0]['cost_boundary']['action'],'BYPASS')
                self.assertEqual(len(r['unresolved_frontier']),int(reason=='USER_REQUESTED_BRANCH_HOLD'))

    def test_automatic_label_resolver_hook_and_disabled_legacy_serialization(self):
        class Labels:
            cost_boundary_resolver=Resolver(lambda s:('PENDING','IDENTITY_CHECK_PENDING'))
            def __call__(self,a):return label(a)
        seed=ev(1,P,A);p=Provider([])
        active=Collector(p,Labels()).run(scope(),seed).to_dict()
        self.assertEqual(p.calls,[]);self.assertEqual(len(active['cost_boundary_decisions']),1)
        old=Collector(Provider([]),label).run(scope(),seed).to_dict()
        disabled=Collector(Provider([]),label,cost_boundary_resolver=Resolver(enabled=False)).run(scope(),seed).to_dict()
        for result in (old,disabled):
            self.assertNotIn('cost_boundary_policy',result)
            self.assertNotIn('cost_boundary_decisions',result)
        old['metrics'].pop('provider_total_wall_seconds');disabled['metrics'].pop('provider_total_wall_seconds')
        self.assertEqual(old,disabled)

    def test_changed_catalogue_cannot_reuse_checkpoint_or_hash(self):
        seed=ev(1,P,A)
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'checkpoint.json'
            one=Collector(Provider([]),label,checkpoint_path=path,cost_boundary_resolver=Resolver()).run(scope(),seed)
            with self.assertRaisesRegex(ValueError,'cost policy/evidence'):
                Collector(Provider([]),label,checkpoint_path=path,cost_boundary_resolver=Resolver(identity='d'*64)).run(scope(),seed)
            two=Collector(Provider([]),label,cost_boundary_resolver=Resolver(identity='d'*64)).run(scope(),seed)
            self.assertNotEqual(one.metrics['candidate_stop_coverage_sha256'],two.metrics['candidate_stop_coverage_sha256'])

    def test_malformed_overlay_fails_before_any_request(self):
        class Bad(Resolver):
            def resolve(self,s,q,i):
                d=super().resolve(s,q,i);d['action']='STOP';return d
        p=Provider([])
        with self.assertRaises(ValueError):Collector(p,label,cost_boundary_resolver=Bad()).run(scope(),ev(1,P,A))
        self.assertEqual(p.calls,[])

if __name__=='__main__':unittest.main()
