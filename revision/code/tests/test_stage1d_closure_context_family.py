"""Synthetic native-family and receipt reuse regression; no files/network/solver."""
import copy
from dataclasses import asdict
import unittest
from unittest.mock import patch

from collector import Collector, Event, Scope, NATIVE
import stage1d_closure_context as bridge
from stage1d_context import _native_rows, necessary_context_windows
from context_ledger_r3 import normalize_rows

HOLDER = '0x'+'1'*40
SPONSOR = '0x'+'2'*40
OTHER = '0x'+'3'*40
MID = '0x'+'4'*40
DEAD = '0x'+'5'*40
TX = '0x'+'a'*64
CREATE = '0x'+'b'*64
SEED = '0x'+'c'*64
BLOCK = '0x'+'d'*64
FOREIGN = '0x'+'e'*64


def native_fixture():
    seed = Event('eip155:1:tx:'+SEED+':top', SEED, SPONSOR, HOLDER, NATIVE,
                 10, 1, 0, 1, block_hash=BLOCK)
    scope = Scope('synthetic-native', 'synthetic-native', 1, 1, 1, 1, 0, None, 'REFERENCE_FULL')
    class NoFetch:
        replay_only = True
        def fetch_interval(self, *args, **kwargs):
            raise AssertionError('Depth-zero synthetic collection must not fetch')
    labels = {HOLDER: {'kind': 'UNKNOWN'}}
    collection = asdict(Collector(NoFetch(), lambda a: labels.get(a, {'kind':'UNKNOWN'})).run(scope, seed))
    query = {'name':scope.name, 'query_id':scope.query_id, 'seed_event_id':seed.event_id,
             'seed_tx_hash':SEED, 'seed_from':SPONSOR, 'seed_to':HOLDER, 'seed_amount_raw':'10',
             'scope_hash':scope.scope_hash, 'scope_id':scope.scope_id}
    def row(tx, index, kind, sender, recipient, amount, path=None, children=0, **extra):
        value = {'record_type':kind, 'tx_hash':tx, 'block_number':1, 'block_hash':BLOCK,
                 'tx_index':index, 'block_timestamp':1, 'from_address':sender,
                 'to_address':recipient, 'value_raw':str(amount), 'success':True,
                 'evidence_ids':['SYNTHETIC_CONTROLLED:'+tx]}
        if kind == 'transaction':
            value.update(gas_used=3, effective_gas_price=1, gas_raw='3', input_data='0x')
        else:
            value.update(trace_address=path, subtraces=children, trace_type='call', call_type='call')
        value.update(extra)
        return value
    rows = [row(SEED,0,'transaction',SPONSOR,HOLDER,10),
            row(SEED,0,'trace',SPONSOR,HOLDER,10,[]),
            row(TX,1,'transaction',SPONSOR,OTHER,0),
            row(TX,1,'trace',SPONSOR,OTHER,0,[],2),
            row(TX,1,'trace',OTHER,MID,0,[0],1),
            row(TX,1,'trace',MID,HOLDER,4,[0,0]),
            row(TX,1,'trace',DEAD,None,1,[1],trace_type='suicide',
                created_address=DEAD,refund_address=HOLDER),
            row(CREATE,2,'transaction',SPONSOR,None,0),
            row(CREATE,2,'trace',SPONSOR,None,0,[],trace_type='create',created_address=HOLDER)]
    plan = necessary_context_windows(query, collection, (), labels)
    headers = {0:{'number':'0x0','hash':'0x'+'0'*64,'timestamp':'0x0'},
               1:{'number':'0x1','hash':BLOCK,'timestamp':'0x1'}}
    balances = {HOLDER+':'+str(b):{'request':{'method':'eth_getBalance','params':[HOLDER,hex(b)]},
                'response':{'result':hex(v)},'evidence_ids':['SYNTHETIC_CONTROLLED:anchor']}
                for b,v in ((0,0),(1,15))}
    coverage = [{'address':HOLDER,'asset':'ETH','start_block':1,'end_block':1,
                 'data_type':kind,'status':'COMPLETE','complete':True,'pagination_complete':True,
                 'evidence_ids':['SYNTHETIC_CONTROLLED:complete-ledger']}
                for kind in plan['rows'][0]['required_coverage']]
    material = {'events':rows,'headers':headers,'balances':balances,'receipts':{},
                'coverage':coverage,'evidence_kind':'SYNTHETIC_CONTROLLED'}
    return query, collection, labels, material, plan


def receipt(tx=TX, gas=3):
    return {'transactionHash':tx,'blockNumber':'0x1','blockHash':BLOCK,'transactionIndex':'0x1',
            'from':SPONSOR,'to':OTHER,'status':'0x1','gasUsed':hex(gas),'effectiveGasPrice':'0x1','logs':[]}


class OriginalFamilyTests(unittest.TestCase):
    def test_projection_preserves_original_input_without_mutating_it(self):
        _,_,_,m,plan = native_fixture()
        original = copy.deepcopy(m['events'])
        selected = bridge._project(m['events'],plan)
        selected[0]['evidence_ids'].append('caller-change')
        self.assertEqual(m['events'],original)

    def test_entire_original_family_zero_creation_refund_and_fees_are_preserved(self):
        _,_,_,m,plan = native_fixture()
        selected = bridge._project(m['events'],plan)
        self.assertEqual(selected,m['events'])
        self.assertEqual(len(selected),9)
        self.assertEqual(len([r for r in selected if r['tx_hash']==TX]),5)
        self.assertEqual(len([r for r in selected if r['tx_hash']==CREATE]),2)
        normalized = normalize_rows(_native_rows(selected)[0])
        self.assertEqual(normalized['conflicts'],[])
        self.assertEqual(sum(int(t['fee_raw']) for t in normalized['transactions']),9)
        self.assertEqual(sum(int(f['amount_raw']) for f in normalized['flows']),15)

    def test_unrelated_shared_transaction_family_never_enters(self):
        _,_,_,m,plan = native_fixture()
        foreign = copy.deepcopy(m['events'])
        for row in foreign:
            row['tx_hash'] = FOREIGN
            for key in ('from_address','to_address','created_address','refund_address','fee_recipient'):
                if row.get(key): row[key] = OTHER
        self.assertEqual(bridge._project(m['events']+foreign,plan),m['events'])

    def test_retained_family_accounts_do_not_expand_modeled_membership(self):
        q,c,labels,m,plan = native_fixture()
        actual = necessary_context_windows(q,c,bridge._project(m['events'],plan),labels)
        self.assertEqual([r['account_id'] for r in actual['rows']],[HOLDER+'|ETH'])
        self.assertEqual(actual['objective_groups'],{})
        self.assertEqual(actual['new_candidate_or_reference_neighbors_added'],0)

    def test_same_address_outside_needed_block_does_not_select_family(self):
        _,_,_,m,plan = native_fixture()
        foreign = copy.deepcopy(m['events'])
        for row in foreign:
            row['tx_hash']=FOREIGN;row['block_number']=2
        self.assertEqual(bridge._project(m['events']+foreign,plan),m['events'])

    def test_raw_child_count_failure_is_retained_not_repaired(self):
        _,_,_,m,plan = native_fixture()
        incomplete = [r for r in m['events'] if not (r['tx_hash']==TX and r.get('trace_address')==[0])]
        self.assertEqual(bridge._project(incomplete,plan),incomplete)
        self.assertTrue(any(r['reason']=='TRACE_TREE_CHILD_COUNT_CONFLICT' for r in _native_rows(incomplete)[4]))


class NativeReceiptReuseTests(unittest.TestCase):
    def test_complete_native_families_need_no_redundant_receipts_and_assemble(self):
        q,c,labels,m,plan = native_fixture()
        needed = bridge.requirements(q,c,labels,m)
        self.assertEqual(set(needed['native_receipt_point_requests_satisfied_by_ledger']),{SEED,TX,CREATE})
        self.assertFalse(any(r['method']=='eth_getTransactionReceipt' for r in needed['point_requests']))
        result=bridge.assemble(q,c,labels,m)
        self.assertEqual(result['missing_points'],[])
        self.assertEqual(result['context_result']['completion_status'],'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')
        self.assertEqual(result['context_result']['evidence_gaps'],[])
        self.assertEqual(result['methods_executed'],0)

    def test_partial_coverage_missing_header_and_broken_tree_keep_receipt_demand(self):
        for defect in ('coverage','header','tree'):
            with self.subTest(defect=defect):
                q,c,labels,m,plan=native_fixture()
                if defect=='coverage':m['coverage'].pop()
                if defect=='header':m['headers'].pop(1)
                if defect=='tree':m['events']=[r for r in m['events'] if not (r['tx_hash']==TX and r.get('trace_address')==[0])]
                n=bridge.requirements(q,c,labels,m)
                self.assertIn({'method':'eth_getTransactionReceipt','params':[TX]},n['point_requests'])

    def test_gas_arithmetic_and_alias_conflicts_never_gain_reuse(self):
        for fields in ({'gas_raw':'4'},{'fee_raw':'4'},{'gas_used':3.5}):
            with self.subTest(fields=fields):
                q,c,labels,m,plan=native_fixture()
                next(r for r in m['events'] if r['tx_hash']==TX and r['record_type']=='transaction').update(fields)
                try:n=bridge.requirements(q,c,labels,m)
                except ValueError:continue  # Strict malformed integer rejection is acceptable.
                self.assertNotIn(TX,n['native_receipt_point_requests_satisfied_by_ledger'])
                self.assertIn({'method':'eth_getTransactionReceipt','params':[TX]},n['point_requests'])

    def test_modeled_payer_unknown_or_blob_type_needs_receipt(self):
        _,_,_,m,plan=native_fixture()
        family=[copy.deepcopy(r) for r in m['events'] if r['tx_hash']==SEED]
        for r in family:r['from_address']=HOLDER;r['to_address']=OTHER
        top=family[0]
        self.assertEqual(bridge._native_receipt_reuse(family,plan,m,set()),{})
        top['type']='0x2'
        self.assertIn(SEED,bridge._native_receipt_reuse(family,plan,m,set()))
        top['type']='0x3'
        self.assertEqual(bridge._native_receipt_reuse(family,plan,m,set()),{})
        top['type']='0x2';top['blobGasUsed']='0x1'
        self.assertEqual(bridge._native_receipt_reuse(family,plan,m,set()),{})

    def test_failed_top_retains_actual_fee_and_zero_value(self):
        _,_,_,m,plan=native_fixture()
        family=[copy.deepcopy(r) for r in m['events'] if r['tx_hash']==SEED]
        for r in family:r.update(success=False,value_raw='0')
        reuse=bridge._native_receipt_reuse(family,plan,m,set())
        self.assertEqual(reuse[SEED]['execution_fee_raw'],'3')
        normalized=normalize_rows(_native_rows(family)[0])
        self.assertEqual(normalized['flows'],[])
        self.assertEqual(normalized['transactions'][0]['fee_raw'],'3')

    def test_relevant_existing_receipt_still_reaches_adapter_unrelated_is_excluded(self):
        q,c,labels,m,plan=native_fixture()
        m['receipts']={TX:receipt(),FOREIGN:receipt(FOREIGN)}
        with patch.object(bridge,'build_document',wraps=bridge.build_document) as build:
            result=bridge.assemble(q,c,labels,m)
        self.assertEqual(set(build.call_args.args[5]),{TX})
        self.assertEqual(result['missing_points'],[])
        m['receipts'][TX]=receipt(gas=4)
        try: result=bridge.assemble(q,c,labels,m)
        except ValueError: return  # Conflicting real point material must fail closed.
        self.assertNotEqual(result['context_result']['completion_status'],'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')

    def test_observed_canonical_token_log_never_satisfies_full_receipt(self):
        q,c,labels,m,plan=native_fixture()
        token_receipt=receipt()
        token_receipt['logs']=[{'address':bridge.CONTRACT}]
        m['receipts'][TX]=token_receipt
        n=bridge.requirements(q,c,labels,m)
        self.assertIn({'method':'eth_getTransactionReceipt','params':[TX]},n['point_requests'])
        self.assertNotIn(TX,n['native_receipt_point_requests_satisfied_by_ledger'])
        self.assertEqual(bridge._native_receipt_reuse(m['events'],plan,m,{TX}).get(TX),None)


if __name__=='__main__':unittest.main()
