"""Pure synthetic gap storage; no acquired data, network or source results."""
import copy,hashlib,json,tempfile,unittest
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch
import stage1d_gap_sequence as g
from collector import Collector,CollectionResult,Event,FetchResult,Scope,NATIVE

FIXTURE=Path(__file__).resolve().parent/'fixtures/collector_gap_reference.json'


def flat(value):return list(g.iter_gaps(value))
def wire(value):return g.serialize_gaps(value,force_compact=True)


class GapSequenceTests(unittest.TestCase):
    def sample(self):
        return [{'reason':'R','value':[True,1,1.0,None,{'x':0}], 'address':'old','arrival_event_id':'old'},
                {'reason':'R','value':[True,1,1.0,None,{'x':0}], 'address':'old','arrival_event_id':'old'},
                {'reason':'OTHER','nested':{'requests':[{'result':None}]}}]
    def test_flat_and_compact_round_trip_exact_order_duplicates(self):
        source=self.sample();seq=g.as_gap_sequence(source)
        self.assertEqual(flat(seq),source);self.assertEqual(flat(wire(seq)),source)
        self.assertEqual(g.gap_count(seq),3);self.assertEqual(seq,source)
        self.assertEqual(g.serialize_gaps(seq),source)
    def test_overlay_overwrites_prior_values_preserves_nested(self):
        source=self.sample();seq=g.overlay_gaps(source,{'address':'new','arrival_event_id':'arr2'})
        expected=[{**x,'address':'new','arrival_event_id':'arr2'} for x in source]
        self.assertEqual(flat(seq),expected);self.assertEqual(flat(wire(seq)),expected)
    def test_freeze_and_yield_and_serialization_mutation_isolation(self):
        source=self.sample();expected=copy.deepcopy(source);seq=g.as_gap_sequence(source)
        source[0]['value'][4]['x']=999;one=seq[0];one['value'][4]['x']=888
        exported=wire(seq);next(iter(exported['templates'].values()))['items'][0]['value'][4]['x']=777
        self.assertEqual(flat(seq),expected)
    def test_append_extend_and_cross_query_bindings_do_not_merge(self):
        base=g.as_gap_sequence(self.sample());acc=g.GapAccumulator()
        acc.extend(g.overlay_gaps(base,{'query_id':'q1','arrival_event_id':'a1'}))
        acc.extend(g.overlay_gaps(base,{'query_id':'q2','arrival_event_id':'a2'}));acc.append({'reason':'LABEL'})
        self.assertEqual(len(acc),7);self.assertEqual(acc[0]['query_id'],'q1');self.assertEqual(acc[3]['query_id'],'q2')
        self.assertEqual(g.storage_counts(acc)['template_items'],4)
    def test_original_prefix_suppression_and_suffix_duplicates(self):
        source=self.sample();base=[{'reason':'R','value':[1.0,True,1,None,{'x':False}],'address':'old','arrival_event_id':'old'}]
        expected=[x for x in source+source if x not in base]
        self.assertEqual(flat(g.exclude_gaps(g.concat_gaps(source,source),base)),expected)
        self.assertEqual(len(flat(g.exclude_gaps(g.concat_gaps(source,source),[]))),6)
    def test_non_string_reasons_keep_python_equality(self):
        source=[{'reason':True,'value':[False]}, {'reason':['x'],'value':1}, {'reason':None}]
        prefix=[{'reason':1,'value':[0]},{'reason':['x'],'value':True}]
        self.assertEqual(flat(g.exclude_gaps(source,prefix)),[source[-1]])
    def test_repeated_and_reordered_runs_are_lossless(self):
        payload=wire(self.sample());payload['segments'][0]['runs']=[[2,3],[0,2],[0,1]];payload['expanded_count']=4
        self.assertEqual(flat(payload),[self.sample()[i] for i in (2,0,1,0)])
    def test_wrap_account_exact_native_and_none(self):
        source=[{'reason':'R','address':'0xAbC'},{'reason':'R','address':None}]
        seq=g.wrap_gaps(source,{'type':'CANDIDATE_ACQUISITION_GAP'},account_from='address')
        self.assertEqual(flat(seq),[{'type':'CANDIDATE_ACQUISITION_GAP','account_id':'0xabc|ETH','original_gap':source[0]},
                                    {'type':'CANDIDATE_ACQUISITION_GAP','account_id':None,'original_gap':source[1]}])
        self.assertEqual(flat(g.filter_gaps_by_account(seq,{'0xabc|ETH'},False)),flat(seq)[:1])
        self.assertEqual(len(g.filter_gaps_by_account(seq,set(),True)),1)
    def test_boolean_and_integer_overlay_identity_never_aliases(self):
        base=g.as_gap_sequence([{'reason':'R'}])
        seq=g.concat_gaps(g.overlay_gaps(base,{'address':True}),g.overlay_gaps(base,{'address':1}))
        wrapped=g.wrap_gaps(seq,{},account_from='address')
        self.assertEqual([x['account_id'] for x in flat(wrapped)],['true|ETH','1|ETH'])
        self.assertEqual(len(g.filter_gaps_by_account(wrapped,{'true|ETH'},False)),1)
        self.assertEqual(len(g.filter_gaps_by_account(wrapped,{'1|ETH'},False)),1)
    def test_ordered_wrap_then_overlay_then_wrap(self):
        seq=g.wrap_gaps(g.overlay_gaps(g.wrap_gaps(self.sample(),{'kind':'inner'}),{'tag':1}),{'kind':'outer'})
        expected=[{'kind':'outer','original_gap':{'kind':'inner','original_gap':x,'tag':1}} for x in self.sample()]
        self.assertEqual(flat(wire(seq)),expected)
    def test_missing_template_hash_count_version_and_run_mutations_reject(self):
        payload=wire(self.sample());key=next(iter(payload['templates']))
        mutations=[lambda x:x['templates'].clear(),lambda x:x['templates'][key]['items'][0].update(reason='CHANGED'),
                   lambda x:x.update(expanded_count=4),lambda x:x.update(expanded_count=True),
                   lambda x:x.update(schema_version='unknown'),lambda x:x['segments'][0].update(runs=[[0,4]]),
                   lambda x:x['segments'][0].update(runs=[[True,2]]),lambda x:x['segments'][0].update(runs=[[1,1]]),
                   lambda x:x['segments'][0].update(operations=[{'op':'unknown','value':{}}])]
        for mutate in mutations:
            changed=copy.deepcopy(payload);mutate(changed)
            with self.assertRaises(ValueError):g.as_gap_sequence(changed)
    def test_unknown_keys_unused_templates_and_invalid_json_reject(self):
        payload=wire(self.sample());payload['extra']=1
        with self.assertRaises(ValueError):g.as_gap_sequence(payload)
        payload=wire(self.sample());extra=wire([{'reason':'UNUSED'}]);payload['templates'].update(extra['templates'])
        with self.assertRaises(ValueError):g.as_gap_sequence(payload)
        for source in ([{'value':float('nan')}],[{1:'value'}],[{'value':{1,2}}]):
            with self.assertRaises(ValueError):g.as_gap_sequence(source)
    def test_selected_object_shape_ignores_unselected_scalar(self):
        payload=wire([0,{'reason':'R'}]);payload['segments'][0]['runs']=[[1,2]];payload['expanded_count']=1
        self.assertEqual(flat(g.overlay_gaps(payload,{'address':'a'})),[{'reason':'R','address':'a'}])
        with self.assertRaises(ValueError):g.overlay_gaps([0,{'reason':'R'}],{'address':'a'})
    def test_shape_memo_checks_each_selected_run_and_preserves_null_storage(self):
        for scalar in (None,0,False,'scalar',[]):
            with self.subTest(scalar=scalar):
                source=[{'reason':'BUDGET_SYNTHETIC','nested':None},scalar]
                base=g.as_gap_sequence(source)
                split=g.concat_gaps(base[:1],base[1:])
                self.assertEqual(flat(wire(split)),source)
                # Earlier object-only selected runs cannot validate later scalars.
                # Reject at the operation boundary, without iteration/reloading.
                with self.assertRaises(ValueError):g.overlay_gaps(split,{'address':'a'})
                with self.assertRaises(ValueError):g.wrap_gaps(split,{},account_from='address')
                allowed=g.wrap_gaps(split,{'kind':'stored'})
                self.assertEqual(flat(wire(allowed)),[{'kind':'stored','original_gap':x} for x in source])
    def test_empty_and_asdict_compatibility(self):
        self.assertEqual(g.serialize_gaps(g.GapAccumulator()),[])
        self.assertEqual(flat(wire([])),[])
        self.assertEqual(asdict(FetchResult(gaps=g.as_gap_sequence(self.sample())))['gaps'],self.sample())
        self.assertEqual(FetchResult(gaps=g.as_gap_sequence(self.sample())).to_dict()['gaps'],self.sample())
    def test_reliable_negative_reason_checks_do_not_thaw_global_templates(self):
        base=g.as_gap_sequence([{'reason':'ROOT_GAP','nested':[{'x':i}]} for i in range(256)])
        seq=g.concat_gaps(*(g.overlay_gaps(base,{'address':str(i)}) for i in range(30)))
        with patch.object(g,'_apply',side_effect=AssertionError('negative reason scan expanded')):
            self.assertFalse(g.any_gap_reason(seq,('PHYSICAL_FACT_CONFLICT',)))
            self.assertFalse(g.any_gap_reason(seq,substring='BUDGET'))
        positive=g.concat_gaps(seq,[{'reason':'PHYSICAL_FACT_CONFLICT','detail':'keep'}])
        self.assertEqual(list(g.iter_gaps_with_reasons(positive,('PHYSICAL_FACT_CONFLICT',))),[{'reason':'PHYSICAL_FACT_CONFLICT','detail':'keep'}])
    def test_positive_reason_only_unselected_template_entry_does_not_hit(self):
        payload=wire([{'reason':'BUDGET_X'},{'reason':'ROOT_GAP'}]);payload['segments'][0]['runs']=[[1,2]];payload['expanded_count']=1
        self.assertFalse(g.any_gap_reason(payload,substring='BUDGET'))
    def test_unhashable_reason_preserves_old_tuple_membership(self):
        source=[{'reason':['x']},{'reason':{'x':1}},{'reason':'PHYSICAL_FACT_CONFLICT'}]
        self.assertEqual(list(g.iter_gaps_with_reasons(source,('PHYSICAL_FACT_CONFLICT',))),[source[-1]])
    def test_million_logical_shape_serialization_and_constant_account_filter(self):
        base=g.as_gap_sequence([{'reason':'ROOT_GAP','i':i,'nested':{'requests':[{'status':None}]}} for i in range(1000)])
        with patch.object(g,'_apply',side_effect=AssertionError('metadata operation expanded logical rows')):
            seq=g.concat_gaps(*(g.overlay_gaps(base,{'address':'0xAbC','arrival_event_id':str(i)}) for i in range(1000)))
            wrapped=g.wrap_gaps(seq,{'type':'CANDIDATE_ACQUISITION_GAP'},account_from='address')
            selected=g.filter_gaps_by_account(wrapped,{'0xabc|ETH'},False)
            payload=wire(selected);loaded=g.as_gap_sequence(payload)
            copied=asdict(FetchResult(gaps=loaded))
        self.assertEqual(g.storage_counts(loaded),{'logical_gaps':1000000,'templates':1,'template_items':1000,'segments':1000,'selected_runs':1000})
        self.assertLess(len(json.dumps(payload)),600000)
        self.assertEqual(g.gap_count(copied['gaps']),1000000)
        self.assertEqual(loaded[0]['original_gap']['i'],0);self.assertEqual(loaded[-1]['original_gap']['i'],999)


class CollectorGapIntegrationTests(unittest.TestCase):
    def fixture(self):
        a,b,t,source=['0x'+str(i)*40 for i in range(1,5)]
        def event(n,frm,to):return Event('synthetic:e'+str(n),'0x'+format(n+1,'064x'),frm,to,NATIVE,5,100+n,n,1000+n)
        seed=event(0,source,a);events=[event(1,a,b),event(2,b,a),event(3,a,t)]
        gaps=[{'reason':'ROOT_EQUIVALENCE_UNRESOLVED_GAP','tx_hash':'synthetic','nested':{'x':[1,None]}}]*3
        class Provider:
            replay_only=True
            def fetch_interval(self,address,asset,start,end,**kw):
                return FetchResult(events=[e for e in events if address in (e.sender,e.recipient)],
                    coverage=[{'complete':False,'evidence':'SYNTHETIC'}],gaps=gaps,complete=False,cache_hits=1)
        return seed,Provider,lambda addr:{'kind':'SERVICE' if addr==t else 'UNKNOWN'},Scope('synthetic:q','synthetic',100,105,1000,1010,3,None,'REFERENCE_FULL')
    def test_complete_legacy_collector_value_order_and_status_equal(self):
        seed,provider,labels,scope=self.fixture()
        # Frozen synthetic output from the preserved pre-storage-change source.
        payload=FIXTURE.read_bytes()
        self.assertEqual(hashlib.sha256(payload).hexdigest(),'3127c1667cd1851f291e711392ba66521c67d7029f34cf89ece3da4a64a32e49')
        reference=json.loads(payload)
        self.assertEqual(reference['source_sha256'],'2255317fb8d1f40f5b3cab028202c850b5645993f42b19106836af74d7ff8608')
        before=reference['collection']
        after=Collector(provider(),labels).run(scope,seed).to_dict()
        for value in (before,after):value['metrics'].pop('provider_total_wall_seconds',None)
        self.assertEqual(before,after)
    def test_write_hash_and_restart_preserve_compact_gaps(self):
        seed,provider,labels,scope=self.fixture();result=Collector(provider(),labels).run(scope,seed)
        result.gaps=g.GapAccumulator(g.concat_gaps(*(g.as_gap_sequence([{'reason':'R','n':i} for i in range(32)]) for _ in range(10))))
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'collection.json';digest=result.write(path);loaded=json.loads(path.read_bytes())
            self.assertEqual(digest,hashlib.sha256(path.read_bytes()).hexdigest());self.assertEqual(flat(loaded['gaps']),flat(result.gaps))
            self.assertFalse(path.with_name('collection.json.gap-write.tmp').exists())
    def test_failed_stream_keeps_prior_collection(self):
        seed,provider,labels,scope=self.fixture();result=Collector(provider(),labels).run(scope,seed)
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'collection.json';path.write_bytes(b'PRIOR')
            with patch('collector.json.dump',side_effect=OSError('synthetic disk failure')):
                with self.assertRaises(OSError):result.write(path)
            self.assertEqual(path.read_bytes(),b'PRIOR');self.assertEqual(list(Path(tmp).iterdir()),[path])


if __name__=='__main__':unittest.main()
