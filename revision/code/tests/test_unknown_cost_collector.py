import copy
from dataclasses import asdict, replace
import hashlib
import json
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
        if identity.get('kind') in ('SERVICE','BRIDGE','MIXER','UNSUPPORTED_PROTOCOL') or identity.get('branch_action') in ('USER_REQUESTED_BRANCH_HOLD','SUPPORTED_OPERATION_RESOLVE') or state.depth>=q.max_depth:
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

    def semantic_case(self, action='STOP', *, withdraw=True, cap=10, depth=4, label_override=None):
        from semantic_weth_fixture import controlled_collection_inputs, SERVICE
        from stage1d_semantic_units import FiniteSemanticResolver
        seed, events, units, contexts = controlled_collection_inputs(withdraw=withdraw)
        q = {'query_id':'synthetic:finite-cost', 'name':'synthetic_finite_cost',
             'start_block':1, 'end_block':20, 'start_time_utc':1, 'end_time_utc':20,
             'max_acquisition_depth':depth, 'window_mode':'QUERY_ARRIVAL_WINDOW_SECONDS_V1',
             'local_window_seconds':cap, 'seed_event_id':seed.event_id}
        scoped = Scope.from_policy(q); q.update(scope_hash=scoped.scope_hash, scope_id=scoped.scope_id)
        resolve_label = label_override or (lambda a: {'kind':'SERVICE' if a==SERVICE else 'UNKNOWN'})
        resolver = Resolver(lambda s:(action,'UNKNOWN_CODE_COST_BOUNDARY' if action=='STOP' else 'TYPE_UNRESOLVED'))
        provider = Provider(events)
        finite = FiniteSemanticResolver(units, contexts, controlled=True)
        result = Collector(provider, resolve_label, semantic_resolver=finite,
                           cost_boundary_resolver=resolver).run(scoped, seed).to_dict()
        return q, result, provider, units

    def test_exact_deposit_withdraw_continue_without_ordinary_holder_fetch(self):
        for action in ('STOP','PENDING'):
            with self.subTest(action=action):
                q,c,p,units = self.semantic_case(action)
                self.assertEqual(p.calls, [])
                self.assertEqual({u['unit_id'] for u in c['semantic_units']},{u['unit_id'] for u in units})
                self.assertEqual([m['output_depth'] for m in c['semantic_membership']], [1,2])
                self.assertEqual(len(c['candidate_events']),3)  # seed and two exact native legs
                self.assertTrue(all(d['action']==action for d in c['cost_boundary_decisions']))
                continuations = [r['finite_semantic_continuation'] for r in c['states'] if 'finite_semantic_continuation' in r]
                self.assertEqual(len(continuations),2)
                self.assertTrue(all(r['ordinary_discovery_allowed'] is False for r in continuations))
                self.assertFalse(c['metrics']['scope_complete'])
                self.assertEqual(c['metrics']['real_requests'],0)
                self.assertEqual(c['metrics']['candidate_membership'][0]['event_id'],units[0]['native_event']['event_id'])

    def test_protocol_user_service_component_and_depth_precede_finite_semantics(self):
        for identity in ({'kind':'UNSUPPORTED_PROTOCOL','branch_action':'UNSUPPORTED_PROTOCOL_STOP'},
                         {'kind':'BRIDGE'}, {'kind':'SERVICE'},
                         {'kind':'UNKNOWN','branch_action':'USER_REQUESTED_BRANCH_HOLD'},
                         {'kind':'SUPPORTED_PROTOCOL','branch_action':'SUPPORTED_OPERATION_RESOLVE'}):
            with self.subTest(identity=identity):
                q,c,p,u = self.semantic_case(label_override=lambda a:identity)
                self.assertEqual(p.calls,[]); self.assertEqual(c['semantic_units'],[])
                self.assertNotIn('finite_semantic_continuation',c['states'][0])
        q,c,p,u = self.semantic_case(depth=0)
        self.assertEqual(c['semantic_units'],[]); self.assertEqual(p.calls,[])

    def test_window_excludes_unit_and_unknown_order_never_renews(self):
        q,c,p,u = self.semantic_case(cap=1)
        self.assertEqual(c['semantic_units'],[]); self.assertEqual(p.calls,[])
        from semantic_weth_fixture import materials, HOLDER, SPONSOR
        from stage1d_semantic_units import certify_instance, context_identity, FiniteSemanticResolver
        policy, context = materials(internal=True); unit=certify_instance(policy,context)
        native=Event(**unit['native_event'])
        seed=replace(native,event_id='synthetic:unknown-order-seed',sender=SPONSOR,
                     recipient=HOLDER,trace_address='[9]')
        p=Provider([]); resolver=FiniteSemanticResolver([unit],{context_identity(context):context},controlled=True)
        r=Collector(p,label,semantic_resolver=resolver,
                    cost_boundary_resolver=Resolver(lambda s:('STOP','UNKNOWN_CODE_COST_BOUNDARY'))).run(Scope.from_policy(q),seed).to_dict()
        self.assertEqual(r['semantic_units'],[]); self.assertEqual(p.calls,[])
        self.assertTrue(any(g.get('reason')=='SEMANTIC_OPERATION_ORDER_UNRESOLVED' for g in r['gaps']))

    def test_output_arrival_remains_independently_eligible(self):
        from semantic_weth_fixture import controlled_collection_inputs, SERVICE
        from stage1d_semantic_units import FiniteSemanticResolver, TOKEN
        seed,events,units,contexts=controlled_collection_inputs()
        scoped=Scope('q','q',1,20,1,20,4,10,'QUERY_ARRIVAL_WINDOW_SECONDS_V1')
        p=Provider(events); resolver=Resolver(lambda s:('CONTINUE','SYNTHETIC_ALLOWED') if s.asset==TOKEN else ('STOP','UNKNOWN_CODE_COST_BOUNDARY'))
        c=Collector(p,lambda a:{'kind':'SERVICE' if a==SERVICE else 'UNKNOWN'},
                    semantic_resolver=FiniteSemanticResolver(units,contexts,controlled=True),
                    cost_boundary_resolver=resolver).run(scoped,seed).to_dict()
        self.assertEqual(len(p.calls),1)
        self.assertEqual(len(c['candidate_events']),3)
        self.assertEqual(c['stops'][-1]['reason'],'FIRST_IDENTIFIED_SERVICE')

    def test_finite_context_keeps_exact_holder_gas_and_both_asset_ledgers(self):
        from stage1d_cost_boundary_context import project_plan, account_key
        from stage1d_multiasset_context import ETH,WETH,required_windows
        from semantic_weth_fixture import HOLDER
        q,c,p,u = self.semantic_case()
        plan={'query_id':q['query_id'],'rows':[{'account_id':account_key(HOLDER,a),'original_ledger_ref':'synthetic:'+a} for a in (ETH,WETH)]}
        projected=project_plan(q,c,plan)
        self.assertEqual(projected['rows'],plan['rows'])
        self.assertEqual(projected['cost_boundary_context']['deferred_context_rows'],[])
        self.assertFalse(projected['cost_boundary_context']['finite_semantic_continuation_claims_complete_ledger'])
        self.assertTrue(projected['cost_boundary_context']['has_cost_boundary'])
        actual = required_windows(q,c)
        self.assertEqual({r['account_id'] for r in actual['rows']}, {account_key(HOLDER,a) for a in (ETH,WETH)})
        self.assertEqual(max(r['ledger_end_block'] for r in actual['rows']),4)
        self.assertLess(max(r['ledger_end_block'] for r in actual['rows']),q['end_block'])
        for mutation in ('missing_record','wrong_membership','wrong_catalogue','wrong_native','wrong_scope'):
            changed=copy.deepcopy(c)
            if mutation=='missing_record':
                for row in changed['states']:row.pop('finite_semantic_continuation',None)
            elif mutation=='wrong_membership':changed['semantic_membership'][0]['input_depth']+=1
            elif mutation=='wrong_catalogue':changed['states'][0]['finite_semantic_continuation']['semantic_resolver_sha256']='0'*64
            elif mutation=='wrong_native':changed['candidate_events'][1]['amount_raw']+=1
            else:changed['semantic_membership'][0]['scope_hash']='0'*64
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):project_plan(q,changed,plan)

    def test_real_disabled_registry_collector_context_and_request_guard_agree(self):
        from stage1d_unknown_cost_boundary import AUTHORITY_SHA256
        from stage1d_unknown_cost_registry import Registry, SCHEMA, POLICY_PATH, CURRENT_PATH
        from stage1d_cost_request_guard import validate_candidate_entry
        from stage1d_cost_boundary_context import validate_overlay
        q={'query_id':'synthetic:disabled','name':'synthetic_disabled','start_block':1,'end_block':100,
           'start_time_utc':0,'end_time_utc':1000,'max_acquisition_depth':4,
           'window_mode':'QUERY_ARRIVAL_WINDOW_SECONDS_V1','local_window_seconds':1000}
        scoped=Scope.from_policy(q);q.update(scope_hash=scoped.scope_hash,scope_id=scoped.scope_id)
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]) as tmp:
            work=Path(tmp)
            def write(path,value):
                p=work/path;p.parent.mkdir(parents=True,exist_ok=True)
                raw=json.dumps(value,sort_keys=True).encode();p.write_bytes(raw)
                return {'path':path,'sha256':hashlib.sha256(raw).hexdigest()}
            pref=write(POLICY_PATH,{'schema_version':POLICY_SCHEMA,'authorization_id':AUTH,'enabled':False,
                 'threshold_daily_strict_gt':20,'authority_source_sha256':AUTHORITY_SHA256,
                 'query_scope_hashes':{scoped.query_id:scoped.scope_hash,'q2':'2'*64,'q3':'3'*64,'q4':'4'*64}})
            initial=write('private/initial.json',{'authorization_id':AUTH,'authority_sha256':AUTHORITY_SHA256,
                 'query_snapshots':[{'query':q}],'state_screen_rows':[]})
            write(CURRENT_PATH,{'schema_version':SCHEMA,'policy_ref':pref,'initial_snapshot_ref':initial,
                 'identity_checks':[],'historical_codes':[],'activity_observations':[]})
            registry=Registry(work);seed=ev(1,P,A);p=Provider([])
            c=Collector(p,label,cost_boundary_resolver=registry).run(scoped,seed).to_dict()
            self.assertFalse(c['cost_boundary_policy']['enabled'])
            self.assertEqual(c['cost_boundary_decisions'][0]['action'],'BYPASS')
            self.assertIsNotNone(validate_overlay(q,c))
            ref=write('derived/stage1d/queries/'+q['name']+'/collection.json',c)
            entry={'query_name':q['name'],'query_id':q['query_id'],'scope_hash':scoped.scope_hash,
                'scope_id':scoped.scope_id,'collection_path':ref['path'],'collection_sha256':ref['sha256'],
                'needed_ranges':[dict(address=A,asset=NATIVE,start_block=1,end_block=100,start_time=1,end_time=1000)]}
            self.assertEqual(validate_candidate_entry(work,q,entry)['status'],'CURRENT_COST_STATE_REQUEST_DOMAIN_VERIFIED')
            self.assertEqual(len(p.calls),1)
        class InvalidDisabled(Resolver):
            def policy_for_scope(self,q):return dict(super().policy_for_scope(q),enabled=False)
        p=Provider([])
        with self.assertRaisesRegex(ValueError,'Disabled cost policy'):
            Collector(p,label,cost_boundary_resolver=InvalidDisabled()).run(scoped,seed)
        self.assertEqual(p.calls,[])

if __name__=='__main__':unittest.main()
