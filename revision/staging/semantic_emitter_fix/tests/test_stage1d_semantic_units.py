import copy, hashlib, json, sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from stage1d_semantic_units import *
from collector import Collector,Event,FetchResult,Scope,QUERY_WINDOW_MODE
from semantic_weth_fixture import materials,controlled_collection_inputs,HOLDER,SPONSOR,SERVICE

class Provider:
    replay_only=True
    def __init__(self,events):self.events=events;self.calls=[]
    def fetch_interval(self,address,asset,start,end,**kw):
        self.calls.append((address,asset))
        return FetchResult(events=[e for e in self.events if address in (e.sender,e.recipient)],complete=True,cache_hits=1)

def scope(q='q',depth=4,w=10):return Scope(q,q,1,20,1,20,depth,w,QUERY_WINDOW_MODE)
def labels(addr):return {'kind':'SERVICE' if addr==SERVICE else 'UNSUPPORTED_PROTOCOL' if addr==WETH else 'UNKNOWN'}

class Tests(unittest.TestCase):
    def test_deposit_no_mint_transfer_required(self):
        p,c=materials();u=certify_instance(p,c)
        self.assertTrue(validate_semantic_unit(u,evidence_context=c)['passed']);self.assertFalse(u['real_component_certified'])
        self.assertEqual(u['input']['asset'],NATIVE);self.assertEqual(u['output']['asset'],TOKEN)
        self.assertEqual(u['raw_consumed_event_ids'],[u['native_event']['event_id']]);self.assertFalse(u['certificate']['log_is_transfer'])
    def test_actual_caller_not_tx_sender(self):
        p,c=materials(internal=True);u=certify_instance(p,c)
        self.assertNotEqual(c['payloads']['transaction']['from'],u['holder']);self.assertEqual(u['holder'],HOLDER)
        self.assertEqual(u['transaction_gas_context']['payer'],SPONSOR)
    def test_withdraw_is_separately_certified(self):
        p,c=materials('WITHDRAWAL');u=certify_instance(p,c)
        self.assertEqual((u['input']['asset'],u['output']['asset']),(TOKEN,NATIVE))
        self.assertEqual(u['native_event']['trace_address'],'0')
        c['payloads']['source_text']=c['payloads']['source_text'].split('function withdraw')[0]
        with self.assertRaisesRegex(ValueError,'source body'):certify_instance(p,c)
    def test_wrong_identity_rejected(self):
        for key,value in [('chain_id',2),('contract','0x'+'9'*40),('block_number',4),('block_hash','0x'+'9'*64)]:
            p,c=materials();p[key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):certify_instance(p,c)
    def test_duplicate_log_locator_rejected(self):
        p,c=materials();c['payloads']['receipt']['logs'].append(copy.deepcopy(c['payloads']['receipt']['logs'][0]))
        with self.assertRaisesRegex(ValueError,'Duplicate'):certify_instance(p,c)
    def test_ancestor_reverted_rejected(self):
        p,c=materials(internal=True);c['payloads']['trace']['error']='execution reverted'
        with self.assertRaisesRegex(ValueError,'reverted'):certify_instance(p,c)
    def test_omitted_refund_rejected(self):
        p,c=materials();c['payloads']['trace']['calls']=[{'from':WETH,'to':HOLDER,'type':'CALL','input':'0x','value':'0x1'}]
        with self.assertRaisesRegex(ValueError,'refund'):certify_instance(p,c)
    def test_tampered_serialized_unit_rejected(self):
        p,c=materials();u=certify_instance(p,c);u['output']['amount_raw']='7'
        self.assertFalse(validate_semantic_unit(u,evidence_context=c)['passed'])
    def test_fake_real_dictionary_and_closed_gate_rejected(self):
        p,c=materials();u=certify_instance(p,c)
        with self.assertRaisesRegex(ValueError,'gate'):FiniteSemanticResolver([u],{context_identity(c):c})
        fake=copy.deepcopy(c);fake['evidence_kind']='REAL_CHAIN'
        with self.assertRaises(ValueError):certify_instance(p,fake)
    def test_actual_collector_deposit_to_weth_service(self):
        seed,es,us,cs=controlled_collection_inputs();provider=Provider(es)
        result=Collector(provider,labels,semantic_resolver=FiniteSemanticResolver(us,cs,controlled=True)).run(scope(),seed)
        self.assertEqual(len(result.semantic_units),1);self.assertEqual(result.semantic_membership[0]['output_depth'],1)
        self.assertEqual(result.stops[-1]['identity']['kind'],'SERVICE');self.assertEqual(result.stops[-1]['state']['asset'],TOKEN)
        self.assertNotIn((WETH,NATIVE),provider.calls)
        self.assertFalse(any(e['event_id'].startswith('semport:') for e in result.candidate_events))
        self.assertEqual(len(result.candidate_events),3)
    def test_seed_ordinary_eth_deposit_weth_service_real_resolver_chain(self):
        seed,es,us,cs=controlled_collection_inputs(ordinary_prefix=True)
        result=Collector(Provider(es),labels,semantic_resolver=FiniteSemanticResolver(us,cs,controlled=True)).run(scope(),seed)
        self.assertEqual(result.semantic_membership[0]['input_depth'],1)
        self.assertEqual(result.semantic_membership[0]['output_depth'],2)
        self.assertEqual(result.stops[-1]['state']['depth'],3)
        self.assertEqual(len(result.candidate_events),4)
    def test_no_withlog_unique_complete_call_tree_is_recomputed(self):
        for kind in ('DEPOSIT','WITHDRAWAL'):
            p,c=materials(kind,internal=True)
            c['payloads']['trace']['calls'][0].pop('logs')
            u=certify_instance(p,c)
            self.assertEqual(u['certificate']['log_binding_proof']['basis'],'COMPLETE_BOUND_TREE_UNIQUE_CANONICAL_STORAGE_LOG_EMITTER')
            self.assertFalse(u['certificate']['log_binding_proof']['frame_logs_fabricated'])
    def test_no_withlog_two_canonical_calls_rejected(self):
        p,c=materials(internal=True);frame=c['payloads']['trace']['calls'][0];frame.pop('logs')
        c['payloads']['trace']['calls'].append(copy.deepcopy(frame))
        with self.assertRaisesRegex(ValueError,'ambiguous'):certify_instance(p,c)
    def test_no_withlog_delegate_storage_context_rejected(self):
        p,c=materials(internal=True);frame=c['payloads']['trace']['calls'][0];frame.pop('logs')
        c['payloads']['trace']['calls'].append({'type':'DELEGATECALL','from':HOLDER,'to':WETH,'value':'0x0','input':'0x','calls':[]})
        with self.assertRaisesRegex(ValueError,'ambiguous'):certify_instance(p,c)
    def test_withdraw_extra_or_wrong_native_output_rejected(self):
        for mutation in ('recipient','additional'):
            p,c=materials('WITHDRAWAL');out=c['payloads']['trace']['calls'][0]
            if mutation=='recipient':out['to']=SERVICE
            else:c['payloads']['trace']['calls'].append(copy.deepcopy(out))
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):certify_instance(p,c)
    def test_input_output_positions_do_not_invent_cross_event_order(self):
        p,c=materials();u=certify_instance(p,c)
        self.assertIsNone(u['input']['execution_position']['log_index'])
        self.assertEqual(u['output']['execution_position']['log_index'],7)
        p['execution_index']=123
        with self.assertRaisesRegex(ValueError,'execution_index'):certify_instance(p,c)
    def test_withdraw_returns_native_and_uses_one_hop(self):
        seed,es,us,cs=controlled_collection_inputs(withdraw=True)
        result=Collector(Provider(es),labels,semantic_resolver=FiniteSemanticResolver(us,cs,controlled=True)).run(scope(),seed)
        self.assertEqual({x['kind'] for x in result.semantic_units},{'DEPOSIT','WITHDRAWAL'})
        self.assertEqual(sorted(m['output_depth'] for m in result.semantic_membership),[1,2])
        self.assertTrue(any(x['state']['asset']==NATIVE for x in result.stops))
    def test_duplicate_unit_no_extra_refresh(self):
        seed,es,us,cs=controlled_collection_inputs()
        r=Collector(Provider(es),labels,semantic_resolver=FiniteSemanticResolver(us+us,cs,controlled=True)).run(scope(),seed)
        self.assertEqual(len(r.semantic_units),1);self.assertEqual(len(r.semantic_membership),1)
    def test_unsupported_caller_not_bypassed(self):
        seed,es,us,cs=controlled_collection_inputs()
        r=Collector(Provider(es),lambda _:{'kind':'UNSUPPORTED_PROTOCOL'},semantic_resolver=FiniteSemanticResolver(us,cs,controlled=True)).run(scope(),seed)
        self.assertEqual(r.semantic_units,[]);self.assertEqual(len(r.candidate_events),1)
    def test_window_and_depth_gate_before_conversion(self):
        seed,es,us,cs=controlled_collection_inputs()
        for s in [scope(w=1),scope(depth=0)]:
            r=Collector(Provider(es),labels,semantic_resolver=FiniteSemanticResolver(us,cs,controlled=True)).run(s,seed)
            self.assertEqual(r.semantic_units,[])
    def test_query_sources_do_not_merge(self):
        seed,es,us,cs=controlled_collection_inputs();resolver=FiniteSemanticResolver(us,cs,controlled=True)
        a=Collector(Provider(es),labels,semantic_resolver=resolver).run(scope('a'),seed)
        b=Collector(Provider(es),labels,semantic_resolver=resolver).run(scope('b'),seed)
        self.assertEqual(a.semantic_units,b.semantic_units)
        self.assertEqual(a.semantic_membership[0]['query_id'],'a');self.assertEqual(b.semantic_membership[0]['query_id'],'b')
        self.assertNotEqual(a.metrics['semantic_scope_id'],b.metrics['semantic_scope_id'])
    def test_json_context_revalidation_no_process_seal_needed(self):
        p,c=materials();u=certify_instance(p,c)
        self.assertTrue(validate_semantic_unit(json.loads(json.dumps(u)),evidence_context=json.loads(json.dumps(c)))['passed'])
    def test_live_gate_rechecks_every_source_and_receipt_byte(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as temp:
            root=Path(temp);(root/'src').mkdir();(root/'checks').mkdir()
            source={}
            for name in REQUIRED_GATE_SOURCES:
                raw=('LOCAL_GATE_VALIDATOR_UNIT_TEST:'+name).encode();(root/'src'/name).write_bytes(raw)
                source[name]=hashlib.sha256(raw).hexdigest()
            evidence=b'{"status":"PASS","evidence_kind":"SYNTHETIC_CONTROLLED"}'
            (root/'checks'/'receipt.json').write_bytes(evidence)
            gate={'status':'PASS','semantic_version':SEMANTIC_VERSION,'capabilities':dict.fromkeys(REQUIRED_CAPABILITIES,True),
                'source_sha256':source,'test_evidence':[{'path':'checks/receipt.json','sha256':hashlib.sha256(evidence).hexdigest(),'status':'PASS'}]}
            self.assertEqual(validate_live_capability_gate(gate,root)['status'],'PASS')
            for mutation in ('source','missing','receipt','failed'):
                g=copy.deepcopy(gate)
                if mutation=='source':g['source_sha256']['collector.py']='0'*64
                elif mutation=='missing':del g['source_sha256']['collector.py']
                elif mutation=='receipt':g['test_evidence'][0]['sha256']='0'*64
                else:g['test_evidence'][0]['status']='FAIL'
                with self.subTest(mutation=mutation),self.assertRaises(ValueError):validate_live_capability_gate(g,root)
            (root/'checks'/'receipt.json').write_bytes(b'{"status":"FAIL"}')
            gate['test_evidence'][0]['sha256']=hashlib.sha256((root/'checks'/'receipt.json').read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError,'actually pass'):validate_live_capability_gate(gate,root)

if __name__=='__main__':unittest.main()
