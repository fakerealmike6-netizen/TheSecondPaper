"""Actual collector → evidence adapter → context LP → seven-method receiver."""
import copy
import json
import unittest
from dataclasses import asdict
from fractions import Fraction as F
from collector import Collector, Event, FetchResult, Scope, QUERY_WINDOW_MODE
from stage1d_semantic_units import FiniteSemanticResolver, NATIVE, TOKEN, TRANSFER_TOPIC
from semantic_weth_fixture import controlled_collection_inputs, HOLDER, SPONSOR, SERVICE, WETH
from stage1d_context import build_document, necessary_context_windows
from stage1d_multiasset_context import SCHEMA
from stage1c_intervals import run_interval
from stage1c_baselines import run_baseline
from stage1c_output_contract import expected_domains, accept_method_results, METHODS
from run_stage1c import observed_port_facts, normalized


def pipeline_material(withdraw=False):
    seed, events, units, contexts = controlled_collection_inputs(withdraw=withdraw)
    seed = Event(**{**asdict(seed), 'recipient': SPONSOR})
    tx2='0x'+'e'*64; block2='0x'+'f'*64
    ordinary=Event('eip155:1:tx:'+tx2+':top',tx2,SPONSOR,HOLDER,NATIVE,10,2,1,2,block_hash=block2)
    events=[ordinary]+events
    class Provider:
        replay_only=True
        def fetch_interval(self,address,asset,start,end,**kw):
            return FetchResult(events=[e for e in events if e.asset==asset and address in (e.sender,e.recipient)],complete=True,cache_hits=1)
    labels=lambda a:{'kind':'SERVICE' if a==SERVICE else 'UNSUPPORTED_PROTOCOL' if a==WETH else 'UNKNOWN'}
    scope=Scope('controlled-pipeline','controlled-pipeline',1,20,1,20,5,10,QUERY_WINDOW_MODE)
    collection=asdict(Collector(Provider(),labels,semantic_resolver=FiniteSemanticResolver(units,contexts,controlled=True)).run(scope,seed))
    query={'name':'controlled-pipeline','query_id':scope.query_id,'seed_event_id':seed.event_id,
           'seed_tx_hash':seed.tx_hash,'seed_from':seed.sender,'seed_to':seed.recipient,'seed_amount_raw':'10','scope_hash':scope.scope_hash}
    headers={0:{'number':'0x0','hash':'0x'+'0'*64,'timestamp':'0x0'}}
    receipts={};raw=[asdict(seed),asdict(ordinary)]
    def add_event(e,gas=1):
        headers[e.block]={'number':hex(e.block),'hash':e.block_hash,'timestamp':hex(e.timestamp)}
        receipts[e.tx_hash]={'transactionHash':e.tx_hash,'blockNumber':hex(e.block),'blockHash':e.block_hash,
            'transactionIndex':hex(e.tx_index),'status':'0x1','from':e.sender,'to':e.recipient,
            'logs':[],'gasUsed':hex(gas),'effectiveGasPrice':'0x1'}
    add_event(seed);add_event(ordinary)
    for unit in units:
        context=contexts[unit['evidence_context_id']]['payloads']
        headers[unit['block_number']]=copy.deepcopy(context['header'])
        receipts[unit['tx_hash']]=copy.deepcopy(context['receipt'])
        transaction=copy.deepcopy(context['transaction']);transaction.update(record_type='transaction',success=True)
        raw.append(transaction)
    entry=events[-1]
    add_event(entry)
    if entry.asset==TOKEN:
        receipts[entry.tx_hash]['to']=WETH
        receipts[entry.tx_hash]['logs']=[{'address':WETH,'logIndex':hex(entry.log_index),
            'transactionHash':entry.tx_hash,'blockNumber':hex(entry.block),'blockHash':entry.block_hash,
            'transactionIndex':hex(entry.tx_index),'removed':False,
            'topics':[TRANSFER_TOPIC,'0x'+'0'*24+entry.sender[2:],'0x'+'0'*24+entry.recipient[2:]],
            'data':'0x'+format(entry.amount_raw,'064x')}]
        raw.append({'record_type':'transaction','hash':entry.tx_hash,'from':entry.sender,'to':WETH,'value':'0x0',
            'blockNumber':hex(entry.block),'blockHash':entry.block_hash,'transactionIndex':hex(entry.tx_index),'success':True})
    else:raw.append(asdict(entry))
    # Reuse actual current raw event fields; native rows carry every zero/failed
    # transaction and its one fee, independently of candidate membership.
    labels_snapshot={a:labels(a) for a in [SPONSOR,HOLDER,SERVICE,WETH]}
    plan=necessary_context_windows(query,collection,raw,labels_snapshot)
    balances={};coverage=[]
    final_eth={SPONSOR:1,HOLDER:4}
    final_token=6
    for row in plan['rows']:
        a=row['address'];token=row['asset']==TOKEN
        before=4 if token else 2
        after=final_token if token else final_eth[a]
        for block,value in [(row['before_anchor_block'],before),(row['after_anchor_block'],after)]:
            headers.setdefault(block,{'number':hex(block),'hash':'0x'+format(1000+block,'064x'),'timestamp':hex(block)})
            req={'method':'eth_call','params':[{'to':WETH,'data':'0x70a08231'+'0'*24+a[2:]},hex(block)]} if token else {'method':'eth_getBalance','params':[a,hex(block)]}
            balances[a+':'+row['asset']+':'+str(block)]={'request':req,'response':{'result':'0x'+format(value,'064x')},'evidence_ids':['controlled-anchor:'+str(block)]}
        for kind in row['required_coverage']:
            coverage.append({'address':a,'asset':row['asset'],'start_block':row['ledger_start_block'],'end_block':row['ledger_end_block'],
                'data_type':kind,'status':'COMPLETE','complete':True,'pagination_complete':True,'evidence_ids':['controlled-full-ledger']})
    # Existing native balance adapter uses address:block exact-selector keys.
    native_balances={}
    for value in balances.values():
        if value['request']['method']=='eth_getBalance':
            address,block=value['request']['params'];native_balances[address+':'+str(int(block,16))]=value
        else:native_balances['token:'+str(len(native_balances))]=value
    return query,collection,{'events':raw,'coverage':coverage,'evidence_kind':'SYNTHETIC_CONTROLLED'},native_balances,headers,receipts,labels_snapshot


class PipelineTests(unittest.TestCase):
    def test_actual_withdraw_collector_adapter(self):
        result=build_document(*pipeline_material(True))
        self.assertIsNotNone(result['model_input'],result)
        doc=result['model_input']
        self.assertEqual({u['kind'] for u in doc['semantic_units']},{'DEPOSIT','WITHDRAWAL'})
        self.assertEqual(sum(len(t['fees']) for t in doc['transactions']),4)
        self.assertEqual(run_interval(doc)['status'],'COMPLETED')
        self.assertEqual(run_baseline(doc,'HAIRCUT')['status'],'OK')

    def test_actual_collector_adapter_and_all_methods(self):
        args=pipeline_material()
        result=build_document(*args)
        self.assertIsNotNone(result['model_input'],result)
        doc=result['model_input'];self.assertEqual(doc['schema_version'],SCHEMA)
        self.assertTrue(expected_domains(doc)['passed'],expected_domains(doc))
        self.assertEqual(len(doc['semantic_units']),1)
        # The pre-injection seed sender is outside the modeled source ledger.
        self.assertEqual(sum(len(t['fees']) for t in doc['transactions']),3)
        self.assertEqual(len(doc['accounts']),3)
        self.assertNotIn(WETH+'|ETH',[a['account_id'] for a in doc['accounts']])
        facts=observed_port_facts(doc)
        self.assertEqual({x['asset'] for x in facts.values()},{'ETH',TOKEN})
        results={m:normalized(run_interval(doc,m) if m in ('FULL_INTERVAL','NO_CROSS_TARGET_COUPLING','NO_PROTOCOL_CONTINUATION','BALANCE_INFORMATION_REMOVED') else run_baseline(doc,m)) for m in METHODS}
        identity={'sample_id':doc['name'],'query_id':doc['query_id'],'input_fact_hash':'controlled-fact',
                  'scope_hash':'controlled-scope','label_version':'controlled-label','method_versions':{m:'controlled-v1' for m in METHODS}}
        for method,row in results.items():
            row.update({k:identity[k] for k in ('sample_id','query_id','input_fact_hash','scope_hash','label_version')})
            row.update(method_id=method,method_version='controlled-v1')
        receipt=accept_method_results(doc,results,expected_identity=identity)
        self.assertTrue(receipt['passed'],receipt['errors'])
        self.assertEqual(results['NO_PROTOCOL_CONTINUATION']['joint_by_asset'][TOKEN]['upper_raw'],'0')
        # Offline serialization preserves the independently verified instances.
        self.assertTrue(expected_domains(json.loads(json.dumps(doc)))['passed'])

    def test_fake_real_material_rejected(self):
        args=list(pipeline_material());args[2]['evidence_kind']='REAL_EVIDENCE_BOUND'
        with self.assertRaisesRegex(ValueError,'Synthetic instance'):build_document(*args)

    def test_wrong_balanceof_selector_rejected(self):
        args=list(pipeline_material())
        for v in args[3].values():
            if v['request']['method']=='eth_call':v['request']['params'][0]['to']=HOLDER
        with self.assertRaisesRegex(ValueError,'balanceOf'):build_document(*args)

    def test_observed_uncertified_withdrawal_keeps_explicit_blocked_material(self):
        args=list(pipeline_material(True))
        args[1]['semantic_units']=[u for u in args[1]['semantic_units'] if u['kind']=='DEPOSIT']
        result=build_document(*args)
        self.assertIsNone(result['model_input'])
        self.assertTrue(any(g['type']=='OBSERVED_WETH_OPERATION_LINKAGE_UNRESOLVED' for g in result['evidence_gaps']))
        self.assertTrue(result['token_operation_observations'])

    def test_missing_blob_field_does_not_succeed_as_full_fee(self):
        args=list(pipeline_material())
        txid=next(tx for tx in args[5] if tx=='0x'+'c'*64)
        args[5][txid]['blobGasUsed']='0x1'
        result=build_document(*args)
        self.assertIsNone(result['model_input'])
        self.assertTrue(any(g['type']=='BLOB_FEE_FIELDS_INCOMPLETE' for g in result['evidence_gaps']))

    def test_wrong_query_context_plan_rejected(self):
        args=pipeline_material()
        with self.assertRaisesRegex(ValueError,'another query'):
            build_document(*args,context_plan={'query_id':'other','seed_event_id':args[0]['seed_event_id']})


if __name__=='__main__':unittest.main()
