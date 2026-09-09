"""Synthetic, offline label/finite reference regressions; no provider calls."""
import unittest,sys,tempfile
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from labels_policy import resolve_address,historical_successes,full_address,lookup_outcome
from reference_core import reference_certificates
from label_queue import freeze_queue
from reference_recompute import rows

A='0x'+'1'*40
def obs(actor='Service A',platform='Dune',role='SERVICE',semantic='ATTRIBUTION',raw='',**kw):
    return dict(address=A,actor=actor,platform=platform,role=role,semantic_kind=semantic,raw_label=raw or actor,observation_id=platform+actor,source_version='synthetic-v1',generation_mechanism=None,record_conflict=False,**kw)
def event(n,from_,to_,amount='1',time=None):
    return dict(event_id=str(n),block_number=str(n),transaction_index='0',tx_hash=str(n),event_type='ETH_TOP_LEVEL',log_index='',from_address=from_,to_address=to_,asset_key='ETH',amount_raw=amount,transaction_status='SUCCESS',block_timestamp=time or f'2023-01-0{n}T00:00:00Z')

class LabelsTests(unittest.TestCase):
    def test_dune_priority_preserves_raw_conflict(self):
        raw=[obs(),obs('Other','Professional')];r=resolve_address(raw)
        self.assertEqual(r['actor'],'Service A');self.assertTrue(r['preserved_conflict']);self.assertEqual(len(raw),2)
    def test_dune_internal_conflict(self):
        r=resolve_address([obs(),obs('Different')]);self.assertEqual(r['identity_class'],'CONFLICTED_IDENTITY');self.assertIsNone(r['actor'])
    def test_named_raw_contract_conflict_not_erased(self):
        r=resolve_address([obs('SushiSwap',role='DEX_OR_PROTOCOL'),obs('',role='UNKNOWN',semantic='CONTEXT',raw='Chainflip: Vault')]);self.assertEqual(r['resolution_rule'],'DUNE_INTERNAL_CONFLICT_PRESERVED')
    def test_unknown_method_does_not_block(self):
        r=resolve_address([obs()]);self.assertEqual(r['identity_class'],'SERVICE');self.assertEqual(r['generation_method_undisclosed_observations'],1)
    def test_behavior_and_deployer_not_ownership(self):
        for o in (obs(semantic='BEHAVIOR'),obs(raw='Exchange User'),obs(raw='contract deployer')):
            self.assertEqual(resolve_address([o])['identity_class'],'UNKNOWN')
    def test_previous_empty_success_excluded(self):
        self.assertIn(A,historical_successes([{'address':A,'api_response_status':'SUCCESS','label_result_status':'EMPTY_LABEL_RESULT'}]))
    def test_failed_lookup_not_empty(self):
        self.assertEqual(lookup_outcome({'code':403},A),'FAILED_OR_UNRESOLVED')
        self.assertEqual(lookup_outcome({'code':200000,'data':[]},A),'MISSING_OR_DUPLICATE_ADDRESS_UNRESOLVED')
    def test_exact_chain_and_full_address(self):
        with self.assertRaises(ValueError):full_address('0x123...456')
        self.assertEqual(lookup_outcome({'code':200000,'data':[{'chain_id':56,'address':A}]},A),'MISSING_OR_DUPLICATE_ADDRESS_UNRESOLVED')
    def test_unknown_middle_passes_and_service_stops(self):
        es=[event(1,'seed','mid'),event(2,'mid','service'),event(3,'service','later')]
        cert=reference_certificates(es,['1'],{'service':{'identity_class':'SERVICE'}},policy=True)
        self.assertIn('2',cert);self.assertNotIn('3',cert);self.assertEqual(cert['2']['unknown_intermediates'],['mid'])
    def test_first_service_can_reduce_downstream(self):
        es=[event(1,'seed','mid'),event(2,'mid','target')]
        self.assertIn('2',reference_certificates(es,['1'],{},policy=True))
        self.assertNotIn('2',reference_certificates(es,['1'],{'mid':{'identity_class':'SERVICE'}},policy=True))
    def test_no_early_or_zero_capacity_certificate(self):
        es=[event(1,'middle','target'),event(2,'seed','middle'),event(3,'middle','later','0')]
        cert=reference_certificates(es,['2'],{},policy=True)
        self.assertNotIn('1',cert);self.assertNotIn('3',cert)
    def test_lexical_bitpay_alias_only(self):
        self.assertEqual(resolve_address([obs('BitPay'),obs('BitPay.com')])['actor'],'BitPay')
        self.assertEqual(resolve_address([obs('eXch.sc'),obs('eXch.cx')])['identity_class'],'CONFLICTED_IDENTITY')
    def test_queue_unique_and_frozen_before_results(self):
        records=[]
        for n in range(12):
            address='0x'+format(n+1,'040x')
            records.append(dict(target_address=address,target_identity_class='UNKNOWN',change_reason='TARGET_SERVICE_IDENTITY_UNRESOLVED',policy_chain_with_unknowns_observed=True,task_reference_status='UNRESOLVED',query_id='q1',incident_id='caseA' if n<6 else 'caseB'))
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            w=Path(tmp);history=[dict(address=records[0]['target_address'],api_response_status='SUCCESS',label_result_status='EMPTY_LABEL_RESULT')]
            a=freeze_queue(w,records,[],{'query_pilots':[]},history)
            q=rows(w/'derived/label_lookup_queue.csv')
            self.assertEqual(len(q),10);self.assertEqual(len({r['address'] for r in q}),10)
            self.assertNotIn(records[0]['target_address'],{r['address'] for r in q})
            b=freeze_queue(w,records[::-1],[],{'query_pilots':[]},history)
            self.assertEqual(a['queue_sha256'],b['queue_sha256'])

if __name__=='__main__':unittest.main()
