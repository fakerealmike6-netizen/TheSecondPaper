"""F04/F05/F08 full Etherscan fetch contracts, entirely synthetic I/O."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from collector import NATIVE
from provider_etherscan import EtherscanProvider, ProviderReadFailure, normalize_rows
from provider_receipts import ReceiptEnricher, TRANSFER_TOPIC
from read_retry_r4 import ReadRetryStore

A='0x'+'1'*40; B='0x'+'2'*40; TOKEN='0x'+'3'*40; TX='0x'+'a'*64; BH='0x'+'b'*64

def row():
    return {'hash':TX,'from':A,'to':B,'value':'1','blockNumber':'2','timeStamp':'3','transactionIndex':'4','isError':'0','gasUsed':'21000','gasPrice':'2'}
def token(): return row()|{'contractAddress':TOKEN}
def receipt():
    topics=[TRANSFER_TOPIC,'0x'+'0'*24+A[2:],'0x'+'0'*24+B[2:]]
    log={'address':TOKEN,'topics':topics,'data':'0x'+format(1,'064x'),'logIndex':'0x7'}
    return {'transactionHash':TX,'transactionIndex':'0x4','blockNumber':'0x2','blockHash':BH,'status':'0x1','logs':[log]}
def fetch(provider): return provider.fetch_interval(A,NATIVE,1,10,start_time=1,end_time=10,global_end_time=10)
class Clock:
    def __init__(self): self.now=1000.
    def __call__(self): return self.now
    def sleep(self,value): self.now+=value

class EtherscanContractsR4Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
    def provider(self, rows, action='txlist', **kwargs):
        return EtherscanProvider(lambda p:{'status':'1','result':rows if p['action']==action else []},self.tmp.name,**kwargs)
    def test_missing_value_full_fetch_is_incomplete(self):
        item=row(); item.pop('value')
        result=fetch(self.provider([item])); self.assertFalse(result.complete); self.assertEqual([],result.events)
        self.assertEqual('MISSING',result.gaps[0]['field_reason'])
    def test_invalid_value_float_bool_negative_null_never_normalizes(self):
        for value in (7.9,True,False,None,-1,'1.5','-2','NaN',''):
            with self.subTest(value=value):
                events,gaps=normalize_rows('txlist',[row()|{'value':value}])
                self.assertEqual([],events); self.assertTrue(gaps)
    def test_exact_large_raw_integer_preserves_precision(self):
        value=2**200+1
        events,gaps=normalize_rows('txlist',[row()|{'value':str(value)}])
        self.assertFalse(gaps); self.assertEqual(value,events[0].amount_raw)
        events,gaps=normalize_rows('txlist',[row()|{'value':hex(value)}])
        self.assertFalse(gaps); self.assertEqual(value,events[0].amount_raw)
    def test_missing_status_all_actions_never_defaults_to_success(self):
        for action in ('txlist','txlistinternal','tokentx'):
            item=token()|{'traceId':'0_1','logIndex':'7'}; item.pop('isError')
            with self.subTest(action=action):
                result=fetch(self.provider([item],action)); self.assertFalse(result.complete); self.assertEqual([],result.events)
    def test_conflicting_and_invalid_status_are_gaps(self):
        for patch in ({'isError':'0','txreceipt_status':'0'},{'isError':None},{'isError':True},{'isError':0.0},{'isError':'7'},{'isError':'unknown'}):
            with self.subTest(patch=patch):
                events,gaps=normalize_rows('txlist',[row()|patch]); self.assertEqual([],events); self.assertTrue(gaps)
    def test_failed_top_level_value_is_not_success_and_fee_retained(self):
        events,gaps=normalize_rows('txlist',[row()|{'isError':'1','txreceipt_status':'0'}])
        self.assertFalse(gaps); self.assertFalse(events[0].success); self.assertEqual(42000,events[0].gas_raw)
    def test_parent_success_does_not_prove_internal_success(self):
        item=row()|{'traceId':'0_1','txreceipt_status':'1'}; item.pop('isError')
        events,gaps=normalize_rows('txlistinternal',[item]); self.assertEqual([],events); self.assertTrue(gaps)
        events,gaps=normalize_rows('txlistinternal',[item|{'isError':'1'}]); self.assertFalse(gaps); self.assertFalse(events[0].success)
    def test_invalid_tx_position_does_not_raise_out_of_full_fetch(self):
        result=fetch(self.provider([row()|{'transactionIndex':2.2}]))
        self.assertFalse(result.complete); self.assertEqual([],result.events)
    def test_position_alias_conflict_rejected(self):
        events,gaps=normalize_rows('txlist',[row()|{'tx_index':'5'}]); self.assertEqual([],events); self.assertEqual('CONFLICT',gaps[0]['field_reason'])
    def test_receipt_does_not_overwrite_known_position_or_block(self):
        for patch in ({'transactionIndex':'0'},{'blockNumber':'9'},{'blockHash':'0x'+'c'*64},{'chainId':'2'}):
            with self.subTest(patch=patch):
                enriched,gaps,_=ReceiptEnricher(lambda _:(receipt(),{}))('tokentx',[token()|patch])
                self.assertEqual([],enriched); self.assertEqual('CONFLICT',gaps[0]['field_reason'])
    def test_log_known_identity_conflicts_are_rejected(self):
        for patch in ({'transactionHash':'0x'+'c'*64},{'transactionIndex':'0x0'},{'blockNumber':'0x9'},{'blockHash':'0x'+'c'*64},{'chainId':'0x2'}):
            rec=receipt(); rec['logs'][0].update(patch)
            with self.subTest(patch=patch):
                enriched,gaps,_=ReceiptEnricher(lambda _:(rec,{}))('tokentx',[token()])
                self.assertEqual([],enriched); self.assertEqual('CONFLICT',gaps[0]['field_reason'])
    def test_missing_redundant_log_fields_inherit_parent_and_request(self):
        enriched,gaps,counts=ReceiptEnricher(lambda _:(receipt(),{'real_requests':1}))('tokentx',[token()])
        self.assertFalse(gaps); self.assertEqual('7',enriched[0]['logIndex']); self.assertEqual('4',enriched[0]['transactionIndex'])
        self.assertEqual(1,counts['real_requests'])
    def test_missing_locators_are_filled_without_changing_known_raw_fields(self):
        item=token(); item.pop('transactionIndex'); item['blockNumber']='0x2'
        enriched,gaps,_=ReceiptEnricher(lambda _:(receipt(),{}))('tokentx',[item])
        self.assertFalse(gaps); self.assertEqual('0x2',enriched[0]['blockNumber']); self.assertEqual('4',enriched[0]['transactionIndex'])
    def test_duplicate_transfer_values_preserve_two_physical_log_ids(self):
        rec=receipt(); rec['logs'].append(rec['logs'][0]|{'logIndex':'0x8'})
        enriched,gaps,_=ReceiptEnricher(lambda _:(rec,{}))('tokentx',[token(),token()])
        self.assertFalse(gaps); self.assertEqual(['7','8'],[r['logIndex'] for r in enriched])
    def test_known_log_id_is_bound_before_missing_indistinguishable_row(self):
        rec=receipt(); rec['logs'].append(rec['logs'][0]|{'logIndex':'0x8'})
        enriched,gaps,_=ReceiptEnricher(lambda _:(rec,{}))('tokentx',[token(),token()|{'logIndex':'8'}])
        self.assertFalse(gaps); self.assertEqual({'7','8'},{r['logIndex'] for r in enriched})
    def test_duplicate_receipt_id_and_multiplicity_mismatch_fail_atomically(self):
        for mode in ('duplicate','mismatch'):
            rec=receipt()
            if mode=='duplicate': rec['logs'].append(dict(rec['logs'][0]))
            enriched,gaps,_=ReceiptEnricher(lambda _:(rec,{}))('tokentx',[token(),token()])
            self.assertEqual([],enriched); self.assertTrue(gaps)
    def test_invalid_receipt_values_and_log_indexes_are_gaps(self):
        for patch in ({'transactionIndex':4.0},{'blockNumber':True},{'status':True},{'status':'true'}):
            enriched,gaps,_=ReceiptEnricher(lambda _:(receipt()|patch,{}))('tokentx',[token()])
            self.assertEqual([],enriched); self.assertTrue(gaps)
        rec=receipt(); rec['logs'][0]['logIndex']=7.5
        enriched,gaps,_=ReceiptEnricher(lambda _:(rec,{}))('tokentx',[token()])
        self.assertEqual([],enriched); self.assertTrue(gaps)
    def test_receipt_conflict_propagates_full_fetch_and_operation_counts(self):
        rec=receipt(); rec['logs'][0]['transactionHash']='0x'+'c'*64
        enricher=ReceiptEnricher(lambda _:(rec,{'real_requests':1,'new_raw_bytes':13}))
        result=fetch(self.provider([token()],'tokentx',detail_enricher=enricher))
        self.assertFalse(result.complete); self.assertEqual([],result.events); self.assertEqual(4,result.real_requests)
        self.assertIn('RECEIPT_IDENTITY_OR_SCHEMA_CONFLICT',{g['reason'] for g in result.gaps})
    def test_receipt_unexpected_exception_becomes_incomplete_with_counts(self):
        def detail(*_): raise ProviderReadFailure('receipt-failed',{'real_requests':2})
        result=fetch(self.provider([token()],'tokentx',detail_enricher=detail))
        self.assertFalse(result.complete); self.assertEqual(5,result.real_requests)
    def test_temporary_error_retry_then_success_cache(self):
        clock=Clock(); called=[]
        def transport(params):
            called.append(params)
            if len(called)==1: return {'status':'0','message':'NOTOK','result':'Max rate limit reached'}
            return {'status':'1','result':[]}
        provider=EtherscanProvider(transport,self.tmp.name,clock=clock,sleep=clock.sleep,rng=lambda:0)
        result=fetch(provider); self.assertTrue(result.complete); self.assertEqual(4,result.real_requests)
        self.assertEqual(1,len(list((Path(self.tmp.name)/'attempts').glob('*.json')))-3)
        replay=fetch(EtherscanProvider(lambda _:self.fail('network'),self.tmp.name,replay_only=True))
        self.assertTrue(replay.complete); self.assertEqual(0,replay.real_requests)
    def test_permanent_auth_error_is_not_retried_and_restart_reuses_failure(self):
        called=[]
        def transport(p): called.append(p); return {'status':'0','result':'Invalid API Key'}
        first=fetch(EtherscanProvider(transport,self.tmp.name))
        second=fetch(EtherscanProvider(transport,self.tmp.name))
        self.assertFalse(first.complete); self.assertEqual(3,first.real_requests); self.assertEqual(0,second.real_requests); self.assertEqual(3,len(called))
    def test_timeout_exhaustion_counts_every_attempt_across_restart(self):
        clock=Clock()
        def transport(_): raise TimeoutError()
        first=fetch(EtherscanProvider(transport,self.tmp.name,clock=clock,sleep=clock.sleep,rng=lambda:0))
        second=fetch(EtherscanProvider(transport,self.tmp.name,clock=clock,sleep=clock.sleep,rng=lambda:0))
        self.assertEqual(9,first.real_requests); self.assertEqual(0,second.real_requests); self.assertFalse(first.complete)
    def test_historical_error_cache_migration_preserves_bytes_and_recovers(self):
        params={'chainid':'1','module':'account','action':'txlist','address':A,'startblock':1,'endblock':10,'page':1,'offset':1000,'sort':'asc'}
        key=hashlib.sha256(json.dumps(params,sort_keys=True).encode()).hexdigest()
        old=Path(self.tmp.name)/(key+'.json'); old.write_text('{"status":"0","message":"NOTOK","result":"Max rate limit reached"}')
        original=old.read_bytes(); clock=Clock(); hooks=[]
        history={key:[{'legacy_id':'synthetic:original-dispatch','outcome':'RETRYABLE_FAILURE','accounting':{'risk':'original-unknown-kept'}}]}
        provider=EtherscanProvider(lambda _:{'status':'1','result':[]},self.tmp.name,clock=clock,sleep=clock.sleep,rng=lambda:0,attempt_hook=lambda claim,_:hooks.append(claim) or {'risk':'kept'},legacy_failure_history=history)
        result=fetch(provider); self.assertTrue(result.complete); self.assertEqual(original,old.read_bytes())
        self.assertEqual(2,hooks[0]['attempt_no']); self.assertEqual(3,len(hooks)); self.assertEqual(3,result.real_requests)
    def test_retry_after_deadline_does_not_call_again_or_mark_empty(self):
        import urllib.error
        clock=Clock(); called=[]
        def transport(p):
            called.append(p)
            error=urllib.error.HTTPError('synthetic',429,'rate',{'Retry-After':'120'},None)
            error.close()
            raise error
        result=fetch(EtherscanProvider(transport,self.tmp.name,clock=clock,sleep=clock.sleep,deadline=1010))
        self.assertFalse(result.complete); self.assertEqual(3,result.real_requests)
        self.assertEqual({'DEFERRED'},{g['reason'] for g in result.gaps})
    def test_attempt_budget_hook_runs_once_per_actual_dispatch(self):
        calls=[]
        def hook(claim,params): calls.append(claim); raise RuntimeError('budget')
        result=fetch(EtherscanProvider(lambda _:self.fail('unbudgeted dispatch'),self.tmp.name,attempt_hook=hook))
        self.assertEqual(0,result.real_requests); self.assertEqual(3,len(calls)); self.assertFalse(result.complete)

if __name__=='__main__': unittest.main()
