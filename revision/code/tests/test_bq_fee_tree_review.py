"""All fixtures synthetic, all files confined to this review staging directory."""
from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
import sys,unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
sys.path[:0]=[str(HERE),str(HERE.parent/'src')]
import stage1d_bq_context_prepare as p
from stage1d_bq_fee_tree_guard import inspect_rows,fee_gaps_for_range
from stage1d_bq_root_binding import _bind
from context_ledger_r3 import normalize_rows

A='0x'+'1'*40;B='0x'+'2'*40;C='0x'+'3'*40
TX='0x'+'a'*64;BH='0x'+'b'*64;T=1700000000

def row(kind,**extra):
    value=dict.fromkeys(p.COLUMNS+p.FEE_COLUMNS)
    value.update(record_type=kind,block_number='100',block_hash=BH,
        block_time=datetime.fromtimestamp(T,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        tx_hash=TX,tx_index='0',from_address=A,to_address=B,value_raw='5',success=True)
    if kind=='transaction':value.update(gas_used='21000',effective_gas_price='2',transaction_type='2')
    if kind=='trace':value.update(trace_address='[]',trace_type='call',call_type='call',subtraces='0')
    value.update(extra);return value

class FeeTreeTests(unittest.TestCase):
    def inspect(self,*rows):return inspect_rows(list(rows),normalize_rows(list(rows)))

    def test_nonblob_failed_zero_preserves_execution_fee(self):
        top=row('transaction',success=False,value_raw='0');root=row('trace',success=False,value_raw='0')
        review=self.inspect(top,root)
        self.assertFalse(review['fee_gaps']);self.assertFalse(review['conflicts'])
        self.assertEqual(review['fee_components'][0]['total_fee_raw'],'42000')

    def test_positive_blob_fee_is_exact_gap_not_complete(self):
        review=self.inspect(row('transaction',transaction_type='3',blob_gas_used='131072',blob_gas_price=str(2**127+1)))
        self.assertEqual(review['fee_gaps'][0]['type'],'POSITIVE_BLOB_FEE_REQUIRES_CONTEXT_COMPONENT')
        self.assertEqual(review['fee_components'][0]['total_fee_raw'],str(42000+131072*(2**127+1)))
        self.assertFalse(review['fee_components'][0]['total_fee_installed_in_current_ledger'])

    def test_missing_type_and_blob_operand_are_gaps(self):
        self.assertEqual(self.inspect(row('transaction',transaction_type=None))['fee_gaps'][0]['type'],
                         'TRANSACTION_TYPE_MISSING_BLOB_FEE_NOT_EXCLUDED')
        self.assertEqual(self.inspect(row('transaction',transaction_type='3'))['fee_gaps'][0]['type'],
                         'BLOB_FEE_OPERAND_MISSING')

    def test_fee_operand_missing_is_explicit(self):
        self.assertEqual(self.inspect(row('transaction',effective_gas_price=None))['fee_gaps'][0]['type'],
                         'EXECUTION_FEE_OPERAND_MISSING')

    def test_nonblob_fields_and_float_fee_conflict(self):
        self.assertTrue(self.inspect(row('transaction',blob_gas_used='1',blob_gas_price='1'))['conflicts'])
        self.assertTrue(self.inspect(row('transaction',transaction_type='3',blob_gas_used=131072.0,blob_gas_price='1'))['conflicts'])

    def test_positive_create_missing_recipient_blocks_top(self):
        self.assertEqual(self.inspect(row('transaction',to_address=None,created_address=B))['top_gaps'][0]['type'],
                         'POSITIVE_CREATE_RECIPIENT_NOT_ESTABLISHED')

    def test_success_error_conflict_and_unknown_call_type(self):
        self.assertEqual(self.inspect(row('trace',trace_address='[0]',error='reverted'))['conflicts'][0]['type'],
                         'TRACE_SUCCESS_ERROR_CONFLICT')
        self.assertEqual(self.inspect(row('trace',call_type=None))['internal_gaps'][0]['type'],
                         'CALL_TYPE_PHYSICAL_SEMANTICS_UNSUPPORTED')

    def test_blob_gap_scoped_to_actual_payer_and_block(self):
        review=self.inspect(row('transaction',transaction_type='3'))
        self.assertFalse(fee_gaps_for_range(review,{'address':B,'start_block':100,'end_block':100}))
        self.assertFalse(fee_gaps_for_range(review,{'address':A,'start_block':101,'end_block':101}))
        self.assertTrue(fee_gaps_for_range(review,{'address':A,'start_block':100,'end_block':100}))

    def test_combined_sql_fee_columns_and_creation_touch_preserve_trace_sql(self):
        fields={p.TX:{k:'INTEGER' for k in ('receipt_status','receipt_gas_used','receipt_effective_gas_price',
                'transaction_type','receipt_blob_gas_used','receipt_blob_gas_price','receipt_contract_address')},p.TR:{}}
        ranges=[{'address':A,'start_block':100,'end_block':102}]
        combined=p.build_sql(ranges,fields,'transaction_and_trace','2024-08-21','2024-08-28')
        for name in ('receipt_blob_gas_used','receipt_blob_gas_price','transaction_type'):
            self.assertIn('t.'+name,combined)
        self.assertIn('OR t.receipt_contract_address=r.address)',combined)
        self.assertEqual(p.columns_for('trace_only'),p.COLUMNS)
        self.assertEqual(p.columns_for('transaction_and_trace'),p.COLUMNS+p.FEE_COLUMNS)
        trace=p.build_sql(ranges,fields,'trace_only','2024-08-21','2024-08-28')
        self.assertNotIn('blob_gas_used',trace);self.assertNotIn('value >',combined)

    def test_pinned_source_not_modified(self):
        self.assertEqual(p.sha(HERE/'fixtures/bq_context_prepare_initial.py.txt'),
                         '891dee851f683c46433e772d907faf8b1e0867eafd7344c78f041be95a2856c1')

    def test_full_helper_with_blob_transaction_emits_fee_gap(self):
        q={'query_id':'synthetic','scope_hash':'synthetic','start_block':100,'end_block':102,
           'start_block_hash':BH,'end_block_hash':BH,'scope':{'start_block':100,'end_block':102,'start_time':T,'end_time':T+24}}
        start,end=p.date_chunks(q)[0]
        spec={'path':'spec.json','template':'transaction_and_trace','date_start_inclusive':start,'date_end_exclusive':end}
        manifest={'scope_dependencies':[],'freeze_sha256':p.FREEZE_SHA,'query_id':q['query_id'],
                  'scope_hash':q['scope_hash'],'needed_ranges':[{'address':A,'start_block':100,'end_block':102}],
                  'plans':[spec],'chunk_days':7}
        rows=[row('transaction',transaction_type='3',blob_gas_used='131072',blob_gas_price='1'),row('trace')]
        def saved(path):return {'queries':[q]} if str(path).endswith('BATCH_QUERY_FREEZE.json') else manifest
        with patch.object(p,'read',side_effect=saved),patch.object(p,'sha',return_value=p.FREEZE_SHA),\
                patch.object(p,'batch_path_for_sha',return_value=HERE/'private/BATCH_QUERY_FREEZE.json'),\
                patch.object(p,'verified_export',return_value=(rows,'SYNTHETIC_ONLY')):
            result=p.normalize_complete_family(HERE,'manifest.json','transaction_and_trace',{'spec.json':'state.json'})
        self.assertNotIn(p.FEES,{r['data_type'] for r in result['coverage']})
        self.assertIn('POSITIVE_BLOB_FEE_REQUIRES_CONTEXT_COMPONENT',{r['type'] for r in result['gaps']})
        self.assertFalse(result['full_context_claimed'])

class RootBindingTests(unittest.TestCase):
    def setUp(self):
        self.family=[row('trace',trace_address=None,subtraces='1'),
                     row('trace',trace_address='[0]',from_address=B,to_address=C)]
        self.tx={'hash':TX,'blockHash':BH,'blockNumber':'0x64','transactionIndex':'0x0',
                 'from':A,'to':B,'value':'0x5','gas':'0x5208','gasPrice':'0x2','input':'0x'}
        self.receipt={'transactionHash':TX,'blockHash':BH,'blockNumber':'0x64','transactionIndex':'0x0',
                 'from':A,'to':B,'status':'0x1','gasUsed':'0x5208','effectiveGasPrice':'0x2'}
        self.header={'number':'0x64','hash':BH,'timestamp':hex(T),'transactions':[TX]}

    def bind(self):return _bind(self.family,self.tx,self.receipt,self.header,['SYNTHETIC_EXPORTED_FAMILY','SYNTHETIC_RPC'])

    def test_exact_unique_null_root_changes_only_root_path(self):
        original=deepcopy(self.family);result=self.bind()
        self.assertEqual(self.family,original);self.assertEqual(result['rows'][0]['trace_address'],'[]')
        self.assertEqual(result['rows'][1]['trace_address'],'[0]')
        self.assertEqual(result['root_binding']['original_trace_address'],None)
        self.assertFalse(result['root_binding']['internal_paths_modified']);self.assertEqual(result['covered_ranges'],0)

    def test_two_nulls_or_existing_root_fail(self):
        self.family[1]['trace_address']=None
        with self.assertRaisesRegex(ValueError,'Exactly one'):self.bind()
        self.family[1]['trace_address']='[]'
        with self.assertRaisesRegex(ValueError,'Missing/duplicate'):self.bind()

    def test_incomplete_children_or_missing_internal_ancestor_fail(self):
        self.family[0]['subtraces']='2'
        with self.assertRaisesRegex(ValueError,'child indices'):self.bind()
        self.family[0]['subtraces']='1';self.family[1]['trace_address']='[0,0]'
        with self.assertRaises(ValueError):self.bind()

    def test_value_status_position_header_conflicts_fail(self):
        for obj,key,bad in ((self.tx,'value','0x6'),(self.receipt,'status','0x0'),
                (self.header,'transactions',[]),(self.receipt,'transactionIndex','0x1')):
            old=obj[key];obj[key]=bad
            with self.assertRaises(ValueError):self.bind()
            obj[key]=old

    def test_delegatecall_and_zero_preserved(self):
        self.family[1]['call_type']='delegatecall';self.family[1]['value_raw']='0'
        result=self.bind();self.assertEqual(result['rows'][1]['call_type'],'delegatecall')
        self.assertEqual(result['rows'][1]['value_raw'],'0')

    def test_nonroot_success_error_conflict_fails(self):
        self.family[1]['error']='reverted'
        with self.assertRaisesRegex(ValueError,'status conflict'):self.bind()

if __name__=='__main__':unittest.main(verbosity=2)
