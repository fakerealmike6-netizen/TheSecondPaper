"""Finite explicit-window/state-role regression; no provider I/O."""
import dataclasses, json, sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from collector import Collector, Event, FetchResult, Scope, QUERY_WINDOW_MODE

def event(name,sender,recipient,t,**kw):
    return Event(name,name,sender,recipient,'native:eip155:1',1,t,1,t,**kw)

def policy(w=4):
    return dict(query_id='q',name='q',start_block=0,end_block=20,start_time_utc=0,end_time_utc=20,
        max_acquisition_depth=3,primary_window_mode=QUERY_WINDOW_MODE,primary_local_window_seconds=w)

class Provider:
    replay_only=True
    def __init__(self,events):self.events=events;self.calls=[]
    def fetch_interval(self,address,asset,start,end,**kw):
        self.calls.append((address,asset,start,end,kw))
        return FetchResult(events=[e for e in self.events if address in (e.sender,e.recipient)],complete=True,cache_hits=1)

class Tests(unittest.TestCase):
    def test_exact_positive_window_only(self):
        for bad in (None,False,True,0,-1,1.0,'1'):
            with self.subTest(bad=bad),self.assertRaises(ValueError):Scope.from_policy(policy(bad))
    def test_missing_custom_window_rejected(self):
        p=policy();del p['primary_local_window_seconds']
        with self.assertRaises(ValueError):Scope.from_policy(p)
    def test_conflicting_custom_window_rejected(self):
        with self.assertRaises(ValueError):Scope.from_policy(dict(policy(),local_window_seconds=5))
        for bad in (True,1.0,'1'):
            with self.subTest(bad=bad),self.assertRaises(ValueError):Scope.from_policy(dict(policy(bad),local_window_seconds=1))
    def test_full_rejects_short_cap(self):
        with self.assertRaises(ValueError):Scope.from_policy(dict(policy(),primary_window_mode='REFERENCE_FULL'))
        with self.assertRaises(ValueError):Scope.from_policy(dict(policy(),primary_window_mode='REFERENCE_FULL',local_window_seconds=None))
    def test_full_null_and_zero_depth_supported(self):
        p=dict(policy(None),primary_window_mode='REFERENCE_FULL',max_acquisition_depth=0)
        s=Scope.from_policy(p);self.assertIsNone(s.local_window_seconds);self.assertEqual(s.local_end(1),20)
    def test_old_mode_missing_stays_90d(self):
        p=policy(12);del p['primary_window_mode']
        self.assertEqual(Scope.from_policy(p).local_window_seconds,90*86400)
    def test_scope_roundtrip_exact_hash(self):
        s=Scope.from_policy(policy());self.assertEqual(s,Scope(**s.freeze_dict()))
        self.assertEqual(s.scope_hash,Scope(**s.freeze_dict()).scope_hash)
    def test_changed_window_rejects_stale_id(self):
        s=Scope.from_policy(policy())
        with self.assertRaises(ValueError):dataclasses.replace(s,local_window_seconds=5)
    def test_true_arrival_and_inclusive_window(self):
        s=Scope.from_policy(policy());seed=event('seed','outside','a',0)
        es=[event('first','a','b',4),event('return','b','a',5),event('last','a','c',9),event('late','a','d',10)]
        result=Collector(Provider(es),lambda _: {'kind':'UNKNOWN'}).run(s,seed)
        self.assertEqual({e['event_id'] for e in result.candidate_events},{'seed','first','return','last'})
        self.assertEqual([x['state']['local_end'] for x in result.states if x['state']['arrival']['event_id']=='return'],[9])
    def test_self_and_unrelated_inflow_do_not_refresh(self):
        seed=event('seed','outside','a',0)
        result=Collector(Provider([event('self','a','a',3),event('normal','z','a',4),event('late','a','b',5)]),lambda _:{'kind':'UNKNOWN'}).run(Scope.from_policy(policy()),seed)
        self.assertEqual([e['event_id'] for e in result.candidate_events],['seed'])
    def test_state_role_resolver_receives_exact_arrival(self):
        calls=[]
        class Labels:
            def resolve_state(self,state):
                calls.append((state.query_id,state.address,state.arrival.block));return {'kind':'SERVICE' if state.arrival.block==4 else 'UNKNOWN'}
        result=Collector(Provider([event('entry','a','b',4)]),Labels()).run(Scope.from_policy(policy()),event('seed','outside','a',0))
        self.assertEqual(calls,[('q','a',0),('q','b',4)]);self.assertEqual(result.stops[0]['reason'],'FIRST_IDENTIFIED_SERVICE')
    def test_resume_changed_window_rejected(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as t:
            p=Path(t)/'cp.json';seed=event('seed','outside','a',0)
            Collector(Provider([]),lambda _:{'kind':'UNKNOWN'},checkpoint_path=p).run(Scope.from_policy(policy()),seed)
            saved=json.loads(p.read_bytes());self.assertEqual(saved['scope']['local_window_seconds'],4)
            with self.assertRaises(ValueError):Collector(Provider([]),lambda _:{'kind':'UNKNOWN'},checkpoint_path=p).run(Scope.from_policy(policy(5)),seed)

if __name__=='__main__':unittest.main()
