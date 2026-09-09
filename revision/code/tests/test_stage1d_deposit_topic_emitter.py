"""Controlled fixtures; no external reads, acquisition, catalogue adoption or LP."""
import copy, hashlib, json, unittest
from stage1d_semantic_units import (WETH, DEPOSIT_TOPIC, DEPOSIT_TOPIC_PROOF_VERSION,
    certify_instance, validate_semantic_unit)
from semantic_weth_fixture import materials, HOLDER, SERVICE

TRANSFER_TOPIC='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
TRANSFER_BODY='''function transferFrom(address src, address dst, uint wad) public returns (bool) {
require(balanceOf[src] >= wad);
if (src != msg.sender && allowance[src][msg.sender] != uint(-1)) {
require(allowance[src][msg.sender] >= wad); allowance[src][msg.sender] -= wad;
} balanceOf[src] -= wad; balanceOf[dst] += wad; Transfer(src, dst, wad); return true; }'''

def source(c,text):
    c['payloads']['source_text']=text
    c['payloads']['source_attestation']['source_sha256']=hashlib.sha256(text.encode()).hexdigest()

def varied_tree():
    p,c=materials(internal=True);d=c['payloads'];root=d['trace'];deposit=root['calls'][0];deposit.pop('logs')
    source(c,d['source_text'].rsplit('}',1)[0]+TRANSFER_BODY+'\n}')
    def call(typ,frm,to,data,value=0,children=None):
        return {'type':typ,'from':frm,'to':to,'input':data,'value':hex(value),'calls':children or []}
    data='0x23b872dd'+'0'*24+HOLDER[2:]+'0'*24+SERVICE[2:]+format(6,'064x')
    transfer=call('CALL',SERVICE,WETH,data)
    delegate=call('DELEGATECALL',SERVICE,'0x'+'5'*40,'0xce654c17',children=[transfer])
    root['calls'] += [call('STATICCALL',HOLDER,WETH,'0x70a08231'+'0'*24+HOLDER[2:]),
        call('STATICCALL',HOLDER,WETH,'0xdd62ed3e'+'0'*24+HOLDER[2:]+'0'*24+SERVICE[2:]),
        call('CALL',HOLDER,SERVICE,'0xce654c17',children=[delegate])]
    ordinary=copy.deepcopy(d['receipt']['logs'][0]);ordinary.update(logIndex='0x8',topics=[TRANSFER_TOPIC,'0x'+'0'*24+HOLDER[2:],'0x'+'0'*24+SERVICE[2:]])
    d['receipt']['logs'].append(ordinary)
    return p,c

class Tests(unittest.TestCase):
    def test_real_shape_controlled_unique_topic_with_getters_and_transfer(self):
        p,c=varied_tree();original=copy.deepcopy(c);u=certify_instance(p,c);proof=u['certificate']['log_binding_proof']
        self.assertEqual(proof['proof_version'],DEPOSIT_TOPIC_PROOF_VERSION)
        self.assertEqual(proof['deposit_topic_log_count'],1);self.assertEqual(proof['canonical_log_count'],2)
        self.assertEqual(len(proof['candidates']),1);self.assertEqual(c,original)
        self.assertEqual(u['input']['amount_raw'],u['output']['amount_raw']);self.assertEqual(u['refund_raw'],'0')
        self.assertEqual(u['transaction_gas_context']['amount_raw'],'1');self.assertFalse(u['surrounding_protocol_certified'])
        self.assertEqual(c['payloads']['receipt']['logs'][1]['topics'][0],TRANSFER_TOPIC)
        self.assertFalse(proof['frame_logs_fabricated']);self.assertTrue(validate_semantic_unit(json.loads(json.dumps(u)),evidence_context=json.loads(json.dumps(c)))['passed'])
    def test_simple_v1_proof_unchanged(self):
        p,c=materials(internal=True);c['payloads']['trace']['calls'][0].pop('logs');u=certify_instance(p,c)
        self.assertEqual(u['certificate']['log_binding_proof']['basis'],'COMPLETE_BOUND_TREE_UNIQUE_CANONICAL_STORAGE_LOG_EMITTER')
        self.assertNotIn('proof_version',u['certificate']['log_binding_proof'])
    def test_static_subtree_cannot_emit_even_call_to_weth(self):
        p,c=varied_tree();c['payloads']['trace']['calls'][1]={'type':'STATICCALL','from':HOLDER,'to':SERVICE,'input':'0x1234','value':'0x0','calls':[
            {'type':'CALL','from':SERVICE,'to':WETH,'input':'0x70a08231'+'0'*24+HOLDER[2:],'value':'0x0','calls':[]}]}
        proof=certify_instance(p,c)['certificate']['log_binding_proof']
        self.assertTrue(any(x.get('inherited_static') for x in proof['excluded_execution_contexts']))
    def test_source_transfer_body_cannot_be_inferred_from_abi(self):
        p,c=varied_tree();source(c,c['payloads']['source_text'].replace('Transfer(src, dst, wad);','Deposit(src, wad);'))
        with self.assertRaisesRegex(ValueError,'full canonical transferFrom source'):certify_instance(p,c)
    def test_unknown_selector_is_not_excluded(self):
        p,c=varied_tree();c['payloads']['trace']['calls'][3]['calls'][0]['calls'][0]['input']='0x12345678'
        with self.assertRaisesRegex(ValueError,'Unknown canonical selector'):certify_instance(p,c)
    def test_two_identical_deposit_calls_ambiguous(self):
        p,c=varied_tree();c['payloads']['trace']['calls'].append(copy.deepcopy(c['payloads']['trace']['calls'][0]))
        with self.assertRaisesRegex(ValueError,'nonunique'):certify_instance(p,c)
    def test_duplicate_deposit_topic_with_new_locator_rejected(self):
        p,c=varied_tree();log=copy.deepcopy(c['payloads']['receipt']['logs'][0]);log['logIndex']='0x9';c['payloads']['receipt']['logs'].append(log)
        with self.assertRaisesRegex(ValueError,'Deposit topic/holder/value log is not unique'):certify_instance(p,c)
    def test_delegated_weth_code_rejected(self):
        p,c=varied_tree();c['payloads']['trace']['calls'][3]['calls'][0]['to']=WETH
        with self.assertRaisesRegex(ValueError,'Delegated WETH'):certify_instance(p,c)
    def test_callcode_rejected(self):
        p,c=varied_tree();c['payloads']['trace']['calls'][3]['calls'][0]['type']='CALLCODE'
        with self.assertRaisesRegex(ValueError,'unambiguous'):certify_instance(p,c)
    def test_caller_storage_mismatch_rejected(self):
        p,c=varied_tree();c['payloads']['trace']['calls'][3]['calls'][0]['calls'][0]['from']=HOLDER
        with self.assertRaisesRegex(ValueError,'parent storage'):certify_instance(p,c)
    def test_static_value_transfer_rejected(self):
        p,c=varied_tree();c['payloads']['trace']['calls'][1]['value']='0x1'
        with self.assertRaisesRegex(ValueError,'static context'):certify_instance(p,c)
    def test_reverted_other_branch_cannot_prove_complete_successful_tree(self):
        p,c=varied_tree();c['payloads']['trace']['calls'][3]['error']='execution reverted'
        with self.assertRaises(ValueError):certify_instance(p,c)
    def test_dirty_truncated_or_payable_transfer_calldata_rejected(self):
        for which in ('dirty','short','value'):
            p,c=varied_tree();f=c['payloads']['trace']['calls'][3]['calls'][0]['calls'][0]
            if which=='dirty':f['input']=f['input'][:10]+'1'+f['input'][11:]
            elif which=='short':f['input']=f['input'][:-2]
            else:f['value']='0x1'
            with self.subTest(which=which),self.assertRaisesRegex(ValueError,'full canonical transferFrom'):certify_instance(p,c)
    def test_withdrawal_does_not_use_deposit_topic_proof(self):
        p,c=materials('WITHDRAWAL',internal=True);c['payloads']['trace']['calls'][0].pop('logs')
        c['payloads']['trace']['calls'].append({'type':'STATICCALL','from':HOLDER,'to':WETH,'value':'0x0','input':'0x70a08231','calls':[]})
        with self.assertRaisesRegex(ValueError,'Complete successful supported'):certify_instance(p,c)
    def test_tampered_exclusion_proof_rejected_by_receiver(self):
        p,c=varied_tree();u=certify_instance(p,c);u['certificate']['log_binding_proof']['excluded_execution_contexts']=[]
        self.assertFalse(validate_semantic_unit(u,evidence_context=c)['passed'])

if __name__=='__main__':unittest.main()
