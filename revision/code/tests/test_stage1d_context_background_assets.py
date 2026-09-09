"""Synthetic asset-domain projections: preserve raw/native fees; reject source escape."""
import copy
import unittest
from unittest.mock import patch
from test_stage1d_multiasset_pipeline import pipeline_material
from test_stage1d_closure_context_family import native_fixture, HOLDER, SPONSOR, BLOCK, SEED
import stage1d_closure_context as closure
import stage1d_multiasset_context as multi
from stage1d_context import _hash, necessary_context_windows
from context_lp_r3 import validate_document

FOREIGN='erc20:eip155:1:0x'+'6'*40
FOREIGN_CONTRACT='0x'+'6'*40
OTHER='0x'+'7'*40
TX='0x'+'8'*64


def background(seed, *, tx=None, block=None):
    event=copy.deepcopy(seed)
    event.update(asset=FOREIGN,kind='erc20',log_index=99,amount_raw=987654321,
                 event_id='synthetic-foreign-log',tx_hash=tx or seed['tx_hash'])
    if block is not None:event['block']=block
    return event


class ContextBackgroundAssetTests(unittest.TestCase):
    def test_native_background_projection_preserves_original_and_native_facts(self):
        q,c,labels,m,plan=native_fixture()
        original=background(c['candidate_events'][0])
        c['context_events'].append(original)
        before=copy.deepcopy(c);needed=closure.requirements(q,c,labels,m)
        self.assertEqual(c,before)
        self.assertEqual(needed['selected_events'],m['events'])
        self.assertEqual(needed['context_plan']['rows'],plan['rows'])
        self.assertEqual(needed['context_plan']['objective_groups'],{})
        audit=needed['background_asset_projection'][0]
        self.assertEqual(audit['asset'],FOREIGN)
        self.assertEqual(audit['raw_row_canonical_sha256'],_hash(original))
        self.assertFalse(audit['source_value_assigned']);self.assertFalse(audit['coverage_certified'])

    def test_foreign_token_only_tx_keeps_native_zero_top_fee_and_full_receipt(self):
        q,c,labels,m,plan=native_fixture()
        c['context_events'].append(background(c['candidate_events'][0],tx=TX))
        top={'record_type':'transaction','hash':TX,'from':HOLDER,'to':FOREIGN_CONTRACT,'value':'0x0',
             'blockNumber':'0x1','blockHash':BLOCK,'transactionIndex':'0x3','success':True,
             'gasUsed':'0x1','effectiveGasPrice':'0x1','type':'0x2','input':'0x'}
        log={'address':FOREIGN_CONTRACT,'transactionHash':TX,'logIndex':'0x63','data':'0x1234','topics':[]}
        rec={'transactionHash':TX,'from':HOLDER,'to':FOREIGN_CONTRACT,'blockNumber':'0x1','blockHash':BLOCK,
             'transactionIndex':'0x3','status':'0x1','gasUsed':'0x1','effectiveGasPrice':'0x1','logs':[log]}
        m['events'].append(top);m['receipts'][TX]=rec
        m['balances'][HOLDER+':1']['response']['result']=hex(14)
        before=copy.deepcopy(m)
        with patch.object(closure,'build_document',wraps=closure.build_document) as build:
            result=closure.assemble(q,c,labels,m)
        doc=result['context_result']['model_input']
        self.assertEqual(m,before)
        self.assertEqual(build.call_args.args[5][TX]['logs'],[log])
        fees=[f for tx in doc['transactions'] for f in tx['fees']]
        self.assertEqual(len(fees),1);self.assertEqual(fees[0]['amount_raw'],'1')
        self.assertEqual(fees[0]['payer_account'],HOLDER+'|ETH')
        self.assertNotIn(FOREIGN,{f.get('asset','ETH') for tx in doc['transactions'] for f in tx['flows']})
        self.assertEqual(doc['objective_groups'],{})
        self.assertEqual(result['context_result']['ledger_reconciliation'][0]['difference_raw'],'0')
        self.assertEqual(m['coverage'],before['coverage'])

    def test_missing_foreign_token_physical_tx_remains_exact_point_need(self):
        q,c,labels,m,plan=native_fixture()
        c['context_events'].append(background(c['candidate_events'][0],tx=TX))
        needed=closure.requirements(q,c,labels,m)
        for method in ('eth_getTransactionByHash','eth_getTransactionReceipt'):
            self.assertIn({'method':method,'params':[TX]},needed['point_requests'])
        assembled=closure.assemble(q,c,labels,m)
        self.assertTrue(assembled['missing_points'])
        self.assertNotEqual(assembled['context_result']['completion_status'],'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')

    def test_unrelated_foreign_background_does_not_extend_range_or_add_account(self):
        q,c,labels,m,plan=native_fixture()
        event=background(c['candidate_events'][0],tx=TX,block=999)
        event.update(sender=OTHER,recipient=FOREIGN_CONTRACT)
        c['context_events'].append(event)
        needed=closure.requirements(q,c,labels,m)
        self.assertEqual(needed['context_plan']['rows'],plan['rows'])
        self.assertNotIn(TX,needed['relevant_transaction_ids'])
        self.assertEqual(len(needed['background_asset_projection']),1)

    def test_unknown_candidate_state_arrival_seed_and_semantic_port_rejected(self):
        for where in ('candidate','state','arrival','frontier','seed','semantic_port'):
            q,c,labels,m,plan=native_fixture()
            if where=='candidate':c['candidate_events'][0]['asset']=FOREIGN
            if where=='state':c['states'][0]['state']['asset']=FOREIGN
            if where=='arrival':c['states'][0]['state']['arrival']['asset']=FOREIGN
            if where=='frontier':
                frontier=copy.deepcopy(c['states'][0]);frontier['state']['asset']=FOREIGN;c['unresolved_frontier'].append(frontier)
            if where=='seed':q['seed_asset']=FOREIGN
            if where=='semantic_port':c['semantic_units']=[{'input':{'asset':FOREIGN},'output':{'asset':multi.WETH}}]
            with self.subTest(where=where),self.assertRaises(ValueError):closure.requirements(q,c,labels,m)

    def test_non_ethereum_malformed_and_native_frame_masquerade_rejected(self):
        for value in ('erc20:eip155:56:0x'+'6'*40,'DOGE','erc20:eip155:1:0x1234'):
            q,c,labels,m,plan=native_fixture();e=background(c['candidate_events'][0]);e['asset']=value;c['context_events'].append(e)
            with self.subTest(value=value),self.assertRaises(ValueError):closure.requirements(q,c,labels,m)
        q,c,labels,m,plan=native_fixture();e=background(c['candidate_events'][0]);e['kind']='top';c['context_events'].append(e)
        with self.assertRaisesRegex(ValueError,'never a native frame'):closure.requirements(q,c,labels,m)
        e['kind']='erc20';e['chain_id']='eip155:56'
        with self.assertRaisesRegex(ValueError,'chain disagrees'):closure.requirements(q,c,labels,m)

    def test_strict_asset_and_lp_gate_are_unchanged(self):
        with self.assertRaises(ValueError):multi.asset(FOREIGN)
        q,c,labels,m,plan=native_fixture();doc=closure.assemble(q,c,labels,m)['context_result']['model_input']
        doc['transactions'][0]['flows'][0]['asset']=FOREIGN
        with self.assertRaisesRegex(ValueError,'Unregistered flow asset'):validate_document(doc)

    def test_future_weth_path_projects_background_without_losing_native_or_token_fees(self):
        q,c,ledger,balances,headers,receipts,labels=pipeline_material()
        q['scope_id']=c['metrics']['scope_id']
        before=multi.build_document(q,c,ledger,balances,headers,receipts,labels)
        ordinary=next(e for e in c['candidate_events'] if e['block']==2)
        foreign=background(ordinary)
        c['context_events'].append(foreign);ledger['events'].append(copy.deepcopy(foreign))
        original=copy.deepcopy((c,ledger,receipts))
        after=multi.build_document(q,c,ledger,balances,headers,receipts,labels)
        a,b=before['model_input'],after['model_input']
        self.assertEqual((c,ledger,receipts),original)
        self.assertEqual(a['accounts'],b['accounts']);self.assertEqual(a['objective_groups'],b['objective_groups'])
        self.assertEqual(a['transactions'],b['transactions'])
        self.assertEqual(sum(len(tx['fees']) for tx in b['transactions']),3)
        self.assertEqual(sum(len(tx.get('conversions',[])) for tx in b['transactions']),1)
        self.assertEqual(len(b['background_asset_projection']),2)
        self.assertEqual(ledger['coverage'],pipeline_material()[2]['coverage'])
        needed=closure.requirements(q,c,labels,{**ledger,'balances':balances,'headers':headers,'receipts':receipts})
        self.assertTrue(needed['weth_ledger_requirements'])
        for unit in c['semantic_units']:
            self.assertIn({'method':'eth_getTransactionReceipt','params':[unit['tx_hash']]},needed['point_requests'])

    def test_background_cannot_shadow_a_reachable_event_id_or_token_log(self):
        q,c,labels,m,plan=native_fixture()
        event=background(c['candidate_events'][0]);event['event_id']=c['candidate_events'][0]['event_id']
        c['context_events'].append(event)
        with self.assertRaisesRegex(ValueError,'reachable physical event'):closure.requirements(q,c,labels,m)
        q,c,ledger,balances,headers,receipts,labels=pipeline_material()
        token=next(e for e in c['candidate_events'] if e.get('kind')=='erc20')
        event=background(token);event['log_index']=token['log_index']
        c['context_events'].append(event)
        with self.assertRaisesRegex(ValueError,'reachable physical event'):multi.required_windows(q,c,ledger['events'],labels)

    def test_unknown_prior_model_account_is_not_context_background(self):
        q,c,ledger,balances,headers,receipts,labels=pipeline_material()
        with self.assertRaises(ValueError):multi.required_windows(q,c,ledger['events'],labels,prior_rows=[
            {'asset':FOREIGN,'account_id':OTHER+'|'+FOREIGN,'address':OTHER}])


if __name__=='__main__':unittest.main()
