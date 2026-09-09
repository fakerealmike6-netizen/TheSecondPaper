"""Synthetic report contracts only. No real graph, Registry, requests or LP."""
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import build_unknown_cost_adoption_report as r
from collector import Event, Scope, State
from physical_facts import canonical_event, FACT_FIELDS
from stage1d_unknown_cost_boundary import make_decision, state_key
from stage1d_cost_boundary_context import validate_overlay

P='a'*64
RESOLVER='b'*64
A='0x'+'1'*40
B='0x'+'2'*40


def fixture(action='STOP'):
    e=Event('eip155:1:tx:0x'+'1'*64+':top','0x'+'1'*64,A,B,'native:eip155:1',2**100,10,0,1010)
    q={'query_id':'synthetic:report','name':'synthetic','seed_event_id':e.event_id,
       'seed_amount_raw':str(e.amount_raw),'start_block':1,'end_block':100,
       'start_time_utc':1000,'end_time_utc':2000,'max_acquisition_depth':5,
       'window_mode':'QUERY_ARRIVAL_WINDOW_SECONDS_V1','local_window_seconds':100}
    scope=Scope.from_policy(q);q.update(scope_hash=scope.scope_hash,scope_id=scope.scope_id)
    s=asdict(State(q['query_id'],B,e.asset,e,1,scope.local_end(e)))
    reason={'STOP':r.STOP_REASONS[0],'PENDING':'TYPE_UNRESOLVED'}[action]
    d=make_decision(s,scope,policy_sha256=P,action=action,reason=reason,evidence_refs=[{'path':'synthetic.json','sha256':'c'*64}])
    row={'state':s,'identity':{'kind':'UNKNOWN'},'cost_boundary':d}
    cross=dict(deepcopy(row),reason=reason)
    if action=='STOP':cross['entry_event_id']=e.event_id
    co={'query_id':q['query_id'],'candidate_events':[asdict(e)],'states':[row],
        'stops':[cross] if action=='STOP' else [],'unresolved_frontier':[cross] if action=='PENDING' else [],
        'cost_boundary_policy':{'schema_version':'stage1d-unknown-cost-policy-v1','authorization_id':r.AUTH,'enabled':True,'policy_sha256':P},
        'cost_boundary_decisions':[d],'metrics':{'cost_boundary_resolver_sha256':RESOLVER}}
    return q,co


def summarize(q,co):
    return r.summarize_query(q,co,{'path':'synthetic/collection.json','sha256':'d'*64},P,RESOLVER,
        validate_overlay,state_key,Scope,canonical_event,FACT_FIELDS)


class ReportTests(unittest.TestCase):
    def test_actual_stop_retains_large_integer_unknown_and_exact_state(self):
        q,co=fixture(); summary,stops,pending=summarize(q,co)
        self.assertEqual(summary['status'],'ACTUAL_COST_REPLAY_SYNCHRONIZED')
        self.assertEqual(len(stops),1); self.assertEqual(pending,[])
        self.assertEqual(stops[0]['state']['arrival']['amount_raw'],2**100)
        self.assertEqual(stops[0]['state_key'],state_key(co['states'][0]['state'],Scope.from_policy(q)))
        self.assertFalse(stops[0]['entry_amounts_addable'])

    def test_removed_or_modified_entry_rejects(self):
        for mutation in ('removed','amount'):
            q,co=fixture()
            if mutation=='removed':co['candidate_events']=[]
            else:co['candidate_events'][0]['amount_raw']+=1
            with self.assertRaises(ValueError):summarize(q,co)

    def test_pending_never_counted_as_stop(self):
        q,co=fixture('PENDING');summary,stops,waiting=summarize(q,co)
        self.assertEqual(stops,[]);self.assertEqual(summary['pending_by_reason'],{'TYPE_UNRESOLVED':1})
        self.assertEqual(len(waiting),1)

    def test_code_present_does_not_invent_activity_when_not_needed(self):
        q,co=fixture();state=co['states'][0]['state']
        d=make_decision(state,Scope.from_policy(q),policy_sha256=P,action='STOP',reason=r.STOP_REASONS[0],
            evidence_refs=[{'path':'synthetic.json','sha256':'c'*64}],code_status='CODE_PRESENT',activity=None)
        co['states'][0]['cost_boundary']=d;co['cost_boundary_decisions']=[d];co['stops'][0]['cost_boundary']=d
        _,stops,_=summarize(q,co)
        self.assertIsNone(stops[0]['N_obs']);self.assertIsNone(stops[0]['observed_count_is_lower_bound'])

    def test_old_policy_or_registry_is_replay_pending(self):
        for mutation in ('policy','resolver'):
            q,co=fixture()
            if mutation=='policy':co.pop('cost_boundary_policy')
            else:co['metrics']['cost_boundary_resolver_sha256']='e'*64
            summary,stops,waiting=summarize(q,co)
            self.assertEqual(summary['status'],'REPLAY_PENDING');self.assertEqual(stops,[])

    def test_forged_state_cross_binding_is_rejected(self):
        q,co=fixture();co['stops'][0]['state']['depth']+=1
        with self.assertRaises(ValueError):summarize(q,co)

    def test_existing_hold_not_reclassified_or_silently_resumed(self):
        q,co=fixture()
        with patch.object(r,'HOLD',B):
            with self.assertRaisesRegex(ValueError,'971 hold'):summarize(q,co)
            state=co['states'][0]['state'];scope=Scope.from_policy(q)
            d=make_decision(state,scope,policy_sha256=P,action='BYPASS',reason='EXISTING_TASK_BOUNDARY',evidence_refs=[])
            identity={'kind':'UNKNOWN','branch_action':'USER_REQUESTED_BRANCH_HOLD','task_boundary_ids':[r.HOLD_ID]}
            co['states'][0].update(identity=identity,cost_boundary=d);co['cost_boundary_decisions']=[d]
            co['stops']=[dict(deepcopy(co['states'][0]),reason='USER_REQUESTED_BRANCH_HOLD',entry_event_id=state['arrival']['event_id'])]
            summary,stops,_=summarize(q,co)
            self.assertEqual(len(summary['current_971_hold_state_keys']),1);self.assertEqual(stops,[])

    def test_queue_exact_duplicate_and_overlap_not_savings(self):
        x={'address':A,'asset':'native:eip155:1','start_block':1,'end_block':10,'start_time':1,'end_time':10}
        y=dict(x,start_block=2)
        result=r.queue_summary([x,x],[y])
        self.assertEqual(result['before_exact_rectangles'],1);self.assertEqual(result['exact_old_keys_absent_now'],1)
        self.assertIsNone(result['unsubmitted_request_reduction'])
        with self.assertRaises(ValueError):r.queue_summary([dict(x,start_time=None)],[])

    def test_input_mutation_rejected(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as folder:
            root=Path(folder);p=root/'f.json';p.write_text('{}')
            i=r.Inputs(root);i.read('f.json');p.write_text('{"x":1}')
            with self.assertRaises(ValueError):i.stable()
            with self.assertRaises(ValueError):i.bind('f.json')

    def test_safe_output_refuses_overwrite_and_escape(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as folder:
            root=Path(folder)
            out,paths=r.output_paths(root,'reports/new',['r.json'])
            out.mkdir(parents=True);paths['r.json'].write_text('{}')
            with self.assertRaises(FileExistsError):r.output_paths(root,'reports/new',['r.json'])
            for path in ('../outside','code/new','01_inputs/new'):
                with self.assertRaises(ValueError):r.output_paths(root,path,['r.json'])


if __name__=='__main__':unittest.main()
