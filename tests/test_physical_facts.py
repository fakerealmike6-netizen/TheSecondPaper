"""Synthetic physical-fact counterexamples; no credentials, network or real data."""
from dataclasses import asdict,replace
import hashlib,itertools,json,sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from collector import Event,FetchResult,Collector,Scope,NATIVE
from physical_facts import PhysicalFactRegistry,CONFLICT_STATUS
from provider_dune import normalize_rows,build_interval_sql,DuneProvider
from dune_observed_replay import SavedDuneProvider,live_fixed_graph
from cache_probe import fixed_graph
from lp_model import build_model
from lp_run import fixed_graph_run

A,B,S,T,X=['0x'+str(i)*40 for i in range(1,6)]
def event(i,sender=A,recipient=S,amount=7,**kw):
    tx='0x'+format(i,'064x')
    return Event('eip155:1:tx:'+tx+':top',tx,sender,recipient,NATIVE,amount,i,0,i,**kw)
def row(amount='7',recipient=S):
    return dict(event_kind='top',tx_hash='0x'+format(5,'064x'),sender=A,recipient=recipient,amount_raw=amount,
                block_number=5,tx_index=0,block_time=5,success=True)
def write_job(work,tag,rows,address=A,start=1):
    sql=build_interval_sql(address,NATIVE,start,10,start_time=start,end_time=10)
    digest=hashlib.sha256(sql.encode()).hexdigest();folder=work/'jobs'/tag;folder.mkdir(parents=True)
    (folder/'query.sql').write_text(sql,encoding='utf-8')
    execution='EXECUTION_'+tag
    (folder/'job.json').write_text(json.dumps(dict(state='QUERY_STATE_COMPLETED',execution_id=execution,logical_job_id=tag,sql_sha256=digest)))
    page=dict(execution_id=execution,state='QUERY_STATE_COMPLETED',result=dict(rows=rows,metadata=dict(row_count=len(rows),total_row_count=len(rows))))
    raw=json.dumps(page).encode();path=work/(tag+'_raw.json');path.write_bytes(raw)
    (folder/'page_0.json').write_text(json.dumps(page))
    (folder/'page_0_receipt.json').write_text(json.dumps(dict(execution_id=execution,http_status=200,parameters={'offset':0},raw_path=path.name,raw_bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())))
def fetch(provider,address=A,start=3):
    return provider.fetch_interval(address,NATIVE,start,10,start_time=start,end_time=10,global_end_time=10)

class PhysicalFactTests(unittest.TestCase):
    def test_case_integer_equivalence_and_provenance_merge(self):
        a=event(5,provenance='source_b');b=asdict(a)
        b.update(tx_hash=a.tx_hash[:2]+a.tx_hash[2:].upper(),sender=A.upper().replace('0X','0x'),amount_raw='0007',block='005',tx_index='00',timestamp='0005',provenance='source_a')
        reg=PhysicalFactRegistry();reg.add(a);reg.add(b);s=reg.snapshot()
        self.assertFalse(s['conflicts']);self.assertEqual(len(s['events']),1)
        self.assertEqual(s['events'][0]['amount_raw'],7)
        self.assertEqual(json.loads(s['events'][0]['provenance']),['source_a','source_b'])

    def test_compatible_missing_fields_enriched_with_sources(self):
        a=replace(event(5,provenance='incomplete'),tx_index=None)
        b=replace(a,tx_index=2,block_hash='0x'+'a'*64,gas_raw=6,gas_used=2,gas_price=3,provenance='complete')
        reg=PhysicalFactRegistry();reg.add(a);reg.add(b);s=reg.snapshot()
        self.assertFalse(s['conflicts']);self.assertEqual(s['events'][0]['tx_index'],2)
        self.assertEqual(s['events'][0]['gas_raw'],6)
        self.assertIn('incomplete',s['events'][0]['provenance'])

    def test_legacy_json_trace_path_and_index_string_are_equivalent(self):
        a=replace(event(5),event_id=event(5).event_id.removesuffix('top')+'trace:1_0',kind='internal',trace_address='[1, 0]')
        reg=PhysicalFactRegistry();reg.add(a);reg.add(asdict(a)|{'trace_address':'01_00'})
        self.assertFalse(reg.snapshot()['conflicts']);self.assertEqual(len(reg.snapshot()['events']),1)

    def test_all_explicit_physical_fields_contradict_not_last_win(self):
        a=event(5,block_hash='0x'+'a'*64,gas_raw=6,gas_used=2,gas_price=3)
        changes={'amount_raw':9,'sender':B,'recipient':T,'asset':'erc20:eip155:1:'+X,
                 'block':6,'block_hash':'0x'+'b'*64,'tx_index':1,'timestamp':6,'success':False,
                 'gas_raw':7,'gas_used':3,'gas_price':4,'chain_id':'eip155:2','tx_hash':'0x'+'f'*64,'kind':'internal'}
        for field,value in changes.items():
            with self.subTest(field=field):
                reg=PhysicalFactRegistry();reg.add(a);reg.add(asdict(a)|{field:value})
                s=reg.snapshot();self.assertFalse(s['events']);self.assertEqual(len(s['conflicts']),1)
                self.assertEqual(len(s['quarantined_versions']),2)

    def test_position_and_execution_order_conflicts(self):
        tx='0x'+'a'*64
        a=Event('eip155:1:tx:'+tx+':log:7',tx,A,S,'erc20:eip155:1:'+X,1,5,0,5,'erc20',7,execution_index=2)
        for change in ({'log_index':8},{'execution_index':3},{'kind':'internal','trace_address':'0'}):
            reg=PhysicalFactRegistry();reg.add(a);reg.add(asdict(a)|change)
            self.assertTrue(reg.snapshot()['conflicts'])

    def test_alias_cannot_duplicate_physical_capacity(self):
        a=event(5);reg=PhysicalFactRegistry();reg.add(a);reg.add(asdict(a)|{'event_id':'alternate_locator','provenance':'other'})
        self.assertEqual(len(reg.snapshot()['events']),1)
        reg.add(asdict(a)|{'event_id':'alternate_locator','amount_raw':9})
        self.assertFalse(reg.snapshot()['events'])

    def test_conflicts_and_enrichment_order_invariant(self):
        a=event(5,provenance='a');b=replace(a,amount_raw=9,provenance='b');c=replace(a,gas_raw=4,provenance='c')
        signatures=[]
        for order in itertools.permutations([a,b,c]):
            reg=PhysicalFactRegistry()
            for e in order:reg.add(e)
            signatures.append(json.dumps(reg.snapshot(),sort_keys=True))
        self.assertEqual(len(set(signatures)),1)

    def test_normalization_quarantines_raw_versions(self):
        for rows in ([row(),row('9',T)],[row('9',T),row()]):
            events,gaps=normalize_rows(rows)
            self.assertEqual(events,[]);self.assertEqual(gaps[0]['reason'],'PHYSICAL_FACT_CONFLICT')
            self.assertEqual({v['raw']['amount_raw'] for v in gaps[0]['versions']},{'7','9'})

    def test_equal_gas_product_does_not_hide_different_constraint_facts(self):
        a=row()|{'gas_used':'2','gas_price':'3'};b=row()|{'gas_used':'3','gas_price':'2'}
        events,gaps=normalize_rows([a,b]);self.assertFalse(events)
        self.assertIn('gas_used',gaps[0]['fields']);self.assertNotIn('gas_raw',gaps[0]['fields'])

    def test_chain_id_and_locator_binding_conflict_even_single_record(self):
        events,gaps=normalize_rows([row()|{'chain_id':2}]);self.assertFalse(events)
        self.assertIn('identity_binding',gaps[0]['fields'])

    def test_late_conflict_revokes_all_prior_candidate_stop_and_coverage(self):
        seed=event(1,X,A,20);ab=event(2,A,B);ba=event(3,B,A);old=event(5);new=replace(old,recipient=T,amount_raw=9)
        class Provider:
            replay_only=True
            def fetch_interval(self,address,asset,start_block,end_block,**kw):
                values=[ab,old] if address==A and start_block==1 else [ba] if address==B else [new] if address==A else []
                return FetchResult(values,[{'complete':True,'address':address}],True,cache_hits=1)
        r=Collector(Provider(),lambda a:{'kind':'SERVICE' if a in(S,T) else 'UNKNOWN'}).run(Scope('q','q',1,10,1,10,5),seed)
        self.assertEqual(r.status,CONFLICT_STATUS);self.assertFalse(r.candidate_events);self.assertFalse(r.stops)
        self.assertTrue(r.invalidated_evidence['stops']);self.assertFalse(any(c['complete'] for c in r.coverage))
        self.assertEqual(r.metrics['service_address_count'],0)
        self.assertEqual(len(r.quarantined_facts),2)
        graph,_=live_fixed_graph(r,seed);self.assertFalse(graph['objective_groups'])
        with self.assertRaisesRegex(ValueError,'physical fact conflict'):build_model(graph)

    def test_fixed_graph_rechecks_even_unflagged_injected_collection(self):
        seed=event(1,X,A,20)
        class P:
            def fetch_interval(self,*args,**kw):return FetchResult([],complete=True)
        r=Collector(P(),lambda a:{'kind':'UNKNOWN'}).run(Scope('q','q',1,10,1,10,1),seed)
        r.candidate_events += [asdict(event(5)),asdict(event(5,amount=9))]
        graph,_=fixed_graph(r,seed);self.assertEqual(graph['scope'],CONFLICT_STATUS)
        with self.assertRaises(ValueError):build_model(graph)

    def test_lp_rechecks_physical_manifest_binding(self):
        seed=event(1,X,A,20)
        class P:
            def fetch_interval(self,*args,**kw):return FetchResult([],complete=True)
        r=Collector(P(),lambda a:{'kind':'UNKNOWN'}).run(Scope('q','q',1,10,1,10,1),seed)
        graph,_=fixed_graph(r,seed);build_model(graph)
        graph['events'][0]['amount_raw']='999'
        with self.assertRaisesRegex(ValueError,'differs'):build_model(graph)

    def test_lp_cli_conflict_replaces_stale_amount_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            w=Path(tmp);graph=w/'graph.json';out=w/'result';out.mkdir()
            (out/'lp_fixed_graph_result.json').write_text(json.dumps({'results':{'stale':{'upper_raw':'999'}}}))
            graph.write_text(json.dumps({'scope':CONFLICT_STATUS,'events':[],'fact_conflicts':[{'reason':'synthetic'}]}))
            fixed_graph_run(graph,out)
            got=json.loads((out/'lp_fixed_graph_result.json').read_text())
            self.assertFalse(got['amounts_allowed']);self.assertEqual(got['results'],{});self.assertEqual(got['status'],CONFLICT_STATUS)

    def test_saved_jobs_conflict_cannot_be_ranked_away(self):
        with tempfile.TemporaryDirectory() as tmp:
            w=Path(tmp);write_job(w,'a',[row()]);write_job(w,'b',[row('9',T)],start=3)
            p=SavedDuneProvider(w/'jobs',w);a=fetch(p);p.jobs.reverse();b=fetch(p)
            self.assertFalse(a.complete);self.assertFalse(a.events);self.assertTrue(a.fact_conflicts)
            self.assertEqual(a.fact_conflicts,b.fact_conflicts);self.assertEqual(a.quarantined_facts,b.quarantined_facts)

    def test_saved_conflict_other_query_does_not_block_unrelated_interval(self):
        with tempfile.TemporaryDirectory() as tmp:
            w=Path(tmp);write_job(w,'a',[row()]);write_job(w,'b',[row('9',T)],start=3)
            other=row()|{'tx_hash':'0x'+'f'*64,'sender':B,'recipient':X}
            write_job(w,'c',[other],address=B)
            p=SavedDuneProvider(w/'jobs',w);self.assertFalse(fetch(p).complete)
            clean=fetch(p,B);self.assertTrue(clean.complete);self.assertEqual(len(clean.events),1)

    def test_saved_consistent_duplicate_enrichment_merges_all_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            w=Path(tmp);write_job(w,'a',[row()|{'tx_index':None}]);write_job(w,'b',[row('0007')],start=3)
            result=fetch(SavedDuneProvider(w/'jobs',w))
            self.assertTrue(result.complete);self.assertEqual(len(result.events),1);self.assertEqual(result.events[0].tx_index,0)
            self.assertIn('EXECUTION_a',result.events[0].provenance);self.assertIn('EXECUTION_b',result.events[0].provenance)

    def test_generic_cross_fetch_conflict_persists_restart_and_revokes_prior(self):
        calls=[]
        def execute(sql,job):
            calls.append(('execute',job));return {'state':'QUERY_STATE_COMPLETED','execution_id':job[-20:]}
        def export(execution,params,job):
            n=len([c for c in calls if c[0]=='execute']);calls.append(('export',job))
            values=[row('7' if n==1 else '9',S if n==1 else T)]
            return {'state':'QUERY_STATE_COMPLETED','execution_id':execution,'result':{'rows':values,'metadata':{'row_count':1,'total_row_count':1}}}
        with tempfile.TemporaryDirectory() as tmp:
            p=DuneProvider(execute,export,tmp)
            first=fetch(p,start=1);self.assertTrue(first.complete)
            second=fetch(p,start=3);self.assertFalse(second.complete);self.assertFalse(second.events)
            self.assertFalse(first.complete);self.assertFalse(first.events)
            before=len(calls)
            restored=DuneProvider(execute,export,tmp)
            blocked=fetch(restored,start=4)
            self.assertFalse(blocked.complete);self.assertTrue(blocked.fact_conflicts);self.assertEqual(len(calls),before)

if __name__=='__main__':unittest.main()
