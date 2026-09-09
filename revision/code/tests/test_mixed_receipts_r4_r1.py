"""R4-F05-R1: required ERC20 with unrelated ERC721, synthetic I/O only."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from provider_receipts import ReceiptEnricher,TRANSFER_TOPIC
from provider_etherscan import EtherscanProvider,normalize_rows

FIXTURE=Path(__file__).resolve().parents[1]/'fixtures/collector/mixed_erc20_erc721_receipt_r4_r1.json'

class MixedReceiptR4R1Tests(unittest.TestCase):
    def setUp(self):
        self.original=FIXTURE.read_bytes();fixture=json.loads(self.original)
        self.receipt,self.row=fixture['receipt'],fixture['row']
    def tearDown(self):self.assertEqual(self.original,FIXTURE.read_bytes())
    def enrich(self,receipt=None,rows=None):
        receipt=copy.deepcopy(self.receipt if receipt is None else receipt)
        rows=copy.deepcopy([self.row] if rows is None else rows)
        return ReceiptEnricher(lambda _:(receipt,{'real_requests':1}))('tokentx',rows)
    def fetch(self,receipt=None,rows=None):
        receipt=copy.deepcopy(self.receipt if receipt is None else receipt)
        rows=copy.deepcopy([self.row] if rows is None else rows)
        with tempfile.TemporaryDirectory() as cache:
            def transport(params):return {'status':'1','result':rows if params['action']=='tokentx' else []}
            provider=EtherscanProvider(transport,cache,detail_enricher=ReceiptEnricher(lambda _:(receipt,{'real_requests':1})))
            return provider.fetch_interval(self.row['from'],'erc20:eip155:1:'+self.row['contractAddress'],99,101,start_time=1704153600,end_time=1704153624,global_end_time=1704153624)
    def accept(self,receipt=None,rows=None,amounts=(17,),positions=(7,)):
        enriched,gaps,counters=self.enrich(receipt,rows)
        self.assertEqual([],gaps);self.assertEqual(len(amounts),len(enriched));self.assertEqual(1,counters['real_requests'])
        events,gaps=normalize_rows('tokentx',enriched);self.assertEqual([],gaps)
        self.assertEqual(sorted(zip(amounts,positions)),sorted((e.amount_raw,e.log_index) for e in events))
        complete=self.fetch(receipt,rows);self.assertTrue(complete.complete,complete.gaps);self.assertEqual([],complete.gaps)
        self.assertEqual(sorted(zip(amounts,positions)),sorted((e.amount_raw,e.log_index) for e in complete.events))
        self.assertEqual(4,complete.real_requests) # three fake pages plus fake receipt
        return complete.events
    def reject(self,receipt=None,rows=None):
        enriched,gaps,_=self.enrich(receipt,rows);self.assertEqual([],enriched);self.assertTrue(gaps)
        complete=self.fetch(receipt,rows);self.assertFalse(complete.complete);self.assertEqual([],complete.events);self.assertTrue(complete.gaps)
        return gaps

    def test_exact_external_mixed_fixture_returns17_log7_and_complete(self):
        events=self.accept();self.assertEqual('erc20:eip155:1:'+self.row['contractAddress'],events[0].asset)
    def test_reversed_mixed_logs_have_identical_event_and_complete_result(self):
        expected=self.accept();self.receipt['logs'].reverse();self.assertEqual(expected,self.accept())
    def test_pure_required_erc20_remains_valid(self):
        self.receipt['logs']=self.receipt['logs'][:1];self.accept()
    def test_legitimate_zero_amount_is_not_treated_as_missing(self):
        self.row['value']='0';self.receipt['logs'][0]['data']='0x'+'0'*64;self.accept(amounts=(0,))
    def test_multiple_unrelated_logs_do_not_expand_required_event_set(self):
        nft=self.receipt['logs'][1]
        self.receipt['logs'] += [copy.deepcopy(nft)|{'logIndex':'0x9'},copy.deepcopy(nft)|{'address':'0x'+'a'*40,'logIndex':'0xa'},copy.deepcopy(nft)|{'topics':['0x'+'b'*64],'data':'0x1234','logIndex':'0xb'}]
        self.receipt['logs'].reverse();self.accept()
    def test_required_contract_nontransfer_event_does_not_expand_results(self):
        self.receipt['logs'].append(copy.deepcopy(self.receipt['logs'][0])|{'topics':['0x'+'a'*64],'data':'0x','logIndex':'0x9'})
        self.accept()
    def test_equal_amount_distinct_required_positions_both_retained(self):
        self.receipt['logs'].append(copy.deepcopy(self.receipt['logs'][0])|{'logIndex':'0x9'})
        self.receipt['logs'].reverse();self.accept(rows=[self.row,self.row],amounts=(17,17),positions=(7,9))
    def test_known_position_bound_before_matching_missing_equal_row(self):
        self.receipt['logs'].append(copy.deepcopy(self.receipt['logs'][0])|{'logIndex':'0x9'})
        self.accept(rows=[self.row,self.row|{'logIndex':'9'}],amounts=(17,17),positions=(7,9))
    def test_two_required_contracts_parsed_with_unrelated_nft_ignored(self):
        other='0x'+'a'*40
        self.receipt['logs'].append(copy.deepcopy(self.receipt['logs'][0])|{'address':other,'logIndex':'0x9'})
        events=self.accept(rows=[self.row,self.row|{'contractAddress':other}],amounts=(17,17),positions=(7,9))
        self.assertEqual(2,len({e.asset for e in events}))
    def test_same_emitter_nft_shape_is_not_implicitly_accepted_as_erc20(self):
        self.receipt['logs'][1]['address']=self.row['contractAddress'];self.reject()
    def test_required_transfer_wrong_topic_count_is_rejected(self):
        for topics in (self.receipt['logs'][0]['topics'][:2],self.receipt['logs'][1]['topics']):
            with self.subTest(topics=len(topics)):
                receipt=copy.deepcopy(self.receipt);receipt['logs'][0]['topics']=topics;self.reject(receipt)
    def test_required_value_wrong_encoding_is_rejected(self):
        for data in ('0x','0x1','0x'+'g'*64,'0x'+'0'*63,'0x'+'0'*66,None,17,True):
            with self.subTest(data=data):
                receipt=copy.deepcopy(self.receipt);receipt['logs'][0]['data']=data;self.reject(receipt)
    def test_required_nonzero_address_padding_is_rejected_before_matching(self):
        receipt=copy.deepcopy(self.receipt);receipt['logs'][0]['topics'][1]='0x1'+receipt['logs'][0]['topics'][1][3:]
        self.assertEqual('receipt.Transfer.addressPadding',self.reject(receipt)[0]['field'])
    def test_required_corrupt_address_topic_cannot_be_skipped_as_unmatched(self):
        for topic in ('0x'+'z'*64,'0x1234',None):
            with self.subTest(topic=topic):
                receipt=copy.deepcopy(self.receipt);receipt['logs'][0]['topics'][2]=topic;self.reject(receipt)
    def test_required_wrong_amount_is_not_successful_empty(self):
        self.receipt['logs'][0]['data']='0x'+format(18,'064x');self.reject()
    def test_required_wrong_endpoint_is_not_silently_matched(self):
        self.receipt['logs'][0]['topics'][1]='0x'+'0'*24+'f'*40;self.reject()
    def test_receipt_known_identity_conflicts_still_reject(self):
        for patch in ({'transactionHash':'0x'+'f'*64},{'blockNumber':'0x65'},{'transactionIndex':'0x3'},{'blockHash':'0x'+'f'*64},{'chainId':'2'}):
            with self.subTest(patch=patch):self.reject(self.receipt|patch)
    def test_required_log_known_identity_conflicts_still_reject(self):
        for patch in ({'transactionHash':'0x'+'f'*64},{'blockNumber':'0x65'},{'transactionIndex':'0x3'},{'blockHash':'0x'+'f'*64},{'chainId':'2'}):
            with self.subTest(patch=patch):
                receipt=copy.deepcopy(self.receipt);receipt['logs'][0].update(patch);self.reject(receipt)
    def test_unrelated_log_cannot_bypass_common_identity_checks(self):
        for patch in ({'transactionHash':'0x'+'f'*64},{'blockNumber':'0x65'},{'transactionIndex':'0x3'},{'blockHash':'0x'+'f'*64},{'chainId':'2'}):
            with self.subTest(patch=patch):
                receipt=copy.deepcopy(self.receipt);receipt['logs'][1].update(patch);self.reject(receipt)
    def test_unrelated_log_cannot_bypass_global_index_uniqueness(self):
        self.receipt['logs'][1]['logIndex']='0x7';self.reject()
    def test_two_unrelated_logs_with_same_global_index_still_reject(self):
        self.receipt['logs'].append(copy.deepcopy(self.receipt['logs'][1]));self.reject()
    def test_unrelated_removed_or_invalid_removal_marker_still_rejects(self):
        for removed in (True,'true',None,0,1,'unknown'):
            with self.subTest(removed=removed):
                receipt=copy.deepcopy(self.receipt);receipt['logs'][1]['removed']=removed;self.reject(receipt)
    def test_common_shell_of_unrelated_log_must_still_be_valid(self):
        for patch in ({'address':None},{'address':'0xbad'},{'topics':{}},{'logIndex':None},{'logIndex':1.5}):
            with self.subTest(patch=patch):
                receipt=copy.deepcopy(self.receipt);receipt['logs'][1].update(patch);self.reject(receipt)
    def test_missing_required_transfer_is_rejected_not_complete_empty(self):
        self.receipt['logs']=self.receipt['logs'][1:];self.reject()
    def test_extra_matching_required_transfer_violates_multiplicity(self):
        self.receipt['logs'].append(copy.deepcopy(self.receipt['logs'][0])|{'logIndex':'0x9'});self.reject()
    def test_two_required_index_rows_cannot_share_one_receipt_transfer(self):
        self.reject(rows=[self.row,self.row])
    def test_known_index_conflict_is_not_overwritten(self):
        self.reject(rows=[self.row|{'transactionIndex':'1'}])
    def test_wrong_contract_association_cannot_supply_required_transfer(self):
        self.receipt['logs'][0]['address']=self.receipt['logs'][1]['address'];self.reject()

if __name__=='__main__':unittest.main()
