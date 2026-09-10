"""Full-tree identities and the actual closure caller; synthetic funds retained."""
import copy,unittest
from stage1d_trace_provider_identity import reconcile
from test_stage1d_closure_context_family import native_fixture,TX,MID,HOLDER,BLOCK
from stage1d_closure_context import assemble

def fixture():
    q,c,l,m,plan=native_fixture()
    for row in m['events']:
        if row.get('record_type')=='trace':row['tx_success']=True
    rows=copy.deepcopy([r for r in m['events'] if r['tx_hash']==TX and r['record_type']=='trace'])
    for row in rows:
        if row['trace_address']:row['trace_address'][0]+=1
        else:row['subtraces']+=1
    leaf=copy.deepcopy(rows[0]);leaf.update(trace_address=[0],from_address=rows[0]['to_address'],to_address='0x'+'0'*39+'1',
        value_raw=None,call_type='staticcall',subtraces=0)
    rows.append(leaf)
    s={'provider':'DUNE','events':rows,'coverage':[],'complete_transaction_trees':[TX]}
    return q,c,l,m,s

class ProviderTreeTests(unittest.TestCase):
    def test_complete_ordered_tree_mapping_preserves_all_rows_and_sources(self):
        q,c,l,m,s=fixture();before=copy.deepcopy((m,s))
        result,proofs=reconcile(m,s)
        self.assertEqual((m,s),before)
        self.assertEqual(len(result['events']),len(m['events'])+1)
        self.assertEqual(proofs[0]['right_nonvalue_leaves'],[[0]])
        self.assertIn({'left':[0,0],'right':[1,0]},proofs[0]['left_to_right_paths'])
        self.assertTrue(all('SYNTHETIC_CONTROLLED:'+TX in r['evidence_ids'] for r in result['events'] if r['tx_hash']==TX))

    def test_actual_caller_removes_double_count_with_real_ledger_capacity(self):
        q,c,l,m,s=fixture()
        c['context_events']=[dict(event_id='eip155:1:tx:'+TX+':trace:1_0',tx_hash=TX,sender=MID,recipient=HOLDER,
            asset='native:eip155:1',amount_raw=4,block=1,tx_index=1,timestamp=1,block_hash=BLOCK,
            kind='internal',trace_address='1_0',success=True,ancestor_success_verified=True,
            provenance='DUNE_INDEX_stage1b-dune-index-adapter-1.0',context_only=True)]
        failed=assemble(q,c,l,m)
        self.assertEqual(failed['status'],'MODEL_BLOCKED')
        m['provider_trace_supplements']=[s]
        got=assemble(q,c,l,m)
        self.assertEqual(got['missing_points'],[])
        self.assertEqual(got['context_result']['fact_conflicts'],[])
        self.assertEqual(got['context_result']['completion_status'],'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')
        self.assertTrue(got['context_evidence']['trace_provider_identity_proofs'])
        self.assertTrue(got['context_result']['model_input']['transactions'])
        self.assertEqual(got['context_result']['model_input']['accounts'],failed['context_result']['model_input']['accounts'])

    def test_amount_counterparty_order_and_block_changes_are_conflicts(self):
        for key,value in [('value_raw','5'),('to_address',MID),('block_hash','0x'+'a'*64),('tx_index',2)]:
            q,c,l,m,s=fixture();s['events'][2][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):reconcile(m,s)
        q,c,l,m,s=fixture();s['events'][1]['trace_address'],s['events'][3]['trace_address']=[2],[1]
        with self.assertRaises(ValueError):reconcile(m,s)

    def test_nonzero_failed_or_nonprecompile_leaf_cannot_be_ignored(self):
        for change in ({'value_raw':'1'},{'success':False},{'to_address':MID},{'error':'reverted'},{'call_type':'delegatecall'}):
            q,c,l,m,s=fixture();s['events'][-1].update(change)
            with self.subTest(change=change),self.assertRaises(ValueError):reconcile(m,s)

    def test_missing_node_duplicate_unknown_success_and_bad_numeric_reject(self):
        for mode in ('missing','duplicate','success','numeric','alias'):
            q,c,l,m,s=fixture()
            if mode=='missing':s['events'].pop(2)
            if mode=='duplicate':s['events'].append(copy.deepcopy(s['events'][0]))
            if mode=='success':s['events'][0]['success']=1
            if mode=='numeric':s['events'][0]['tx_index']=1.5
            if mode=='alias':s['events'][0]['value']=3
            with self.subTest(mode=mode),self.assertRaises(ValueError):reconcile(m,s)

    def test_zero_value_call_precompile_can_match_but_is_retained(self):
        q,c,l,m,s=fixture();s['events'][-1].update(value_raw='0',call_type='call',to_address='0x'+'0'*39+'4')
        result,proof=reconcile(m,s)
        self.assertEqual(len(result['events']),len(m['events'])+1)
        self.assertEqual(proof[0]['right_nonvalue_leaves'],[[0]])

    def test_no_unrelated_transactions_or_interval_coverage_accepted(self):
        for mode in ('tx','coverage'):
            q,c,l,m,s=fixture()
            if mode=='tx':s['events'][0]['tx_hash']='0x'+'1'*64
            else:s['coverage']=[{'complete':True}]
            with self.assertRaises(ValueError):reconcile(m,s)

if __name__=='__main__':unittest.main()
