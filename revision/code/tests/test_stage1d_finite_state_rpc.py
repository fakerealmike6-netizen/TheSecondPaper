import unittest,sys
from pathlib import Path
from copy import deepcopy
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
import stage1d_finite_state_rpc as f

A='0x'+'1'*40;B='0x'+'2'*40
class FiniteStateTests(unittest.TestCase):
 def test_holder_balance_is_exact_read_only(self):
  p=f.balance_plan(A,123);rt=f.FiniteStateRuntime([p])
  self.assertEqual(p,rt.validate_rpc(p))
  for q in [f.balance_plan(B,123),f.balance_plan(A,124),{'method':'eth_call','params':[p['params'][0],'latest']}, {'method':'eth_call','params':[{'to':A,'data':p['params'][0]['data']},'0x7b']}]:
   with self.assertRaises(ValueError):rt.validate_rpc(q)
 def test_exact_balance_zero_valid_not_missing(self):
  r={'id':1,**f.balance_plan(A,123)}
  self.assertEqual('SUCCESS_VALIDATED',f.response_status(r,{'id':1,'jsonrpc':'2.0','result':'0x'+'0'*64}))
  for v in (None,'0x','0x0','0x'+'f'*65):self.assertNotEqual('SUCCESS_VALIDATED',f.response_status(r,{'id':1,'jsonrpc':'2.0','result':v}))
 def test_slot_only_freezes_proxy_at_exact_block(self):
  p=f.implementation_plan(A,123);self.assertEqual(p,f.validate(p))
  p['params'][1]='0x0'
  with self.assertRaises(ValueError):f.validate(p)
 def log(self):return {'address':f.WETH,'removed':False,'blockNumber':'0x7b','blockHash':'0x'+'a'*64,'transactionHash':'0x'+'b'*64,'transactionIndex':'0x0','logIndex':'0x1','topics':[f.TRANSFER,f.topic_address(A),f.topic_address(B)],'data':'0x'+'0'*63+'1'}
 def test_outgoing_incoming_and_duplicate_physical_identity(self):
  r={'id':1,**f.logs_plan(A,123,124,'TRANSFER_OUT')};l=self.log()
  self.assertEqual('SUCCESS_VALIDATED',f.response_status(r,{'id':1,'jsonrpc':'2.0','result':[l]}))
  self.assertNotEqual('SUCCESS_VALIDATED',f.response_status(r,{'id':1,'jsonrpc':'2.0','result':[l,l]}))
  r2={'id':1,**f.logs_plan(B,123,124,'TRANSFER_IN')}
  self.assertEqual('SUCCESS_VALIDATED',f.response_status(r2,{'id':1,'jsonrpc':'2.0','result':[l]}))
 def test_wrong_contract_holder_or_removed_never_completes(self):
  r={'id':1,**f.logs_plan(A,123,124,'TRANSFER_OUT')}
  for k,v in [('address',A),('removed',True),('blockNumber','0x7d'),('topics',[f.TRANSFER,f.topic_address(B),f.topic_address(A)])]:
   l=self.log();l[k]=v
   self.assertNotEqual('SUCCESS_VALIDATED',f.response_status(r,{'id':1,'jsonrpc':'2.0','result':[l]}))
 def test_no_trace_entitlement_or_grant_added(self):
  rt=f.FiniteStateRuntime([]);p={'method_cu_upper_bounds':{'eth_getBalance':20},'paid_overage_authorized':False,'included_compute_units_remaining':777}
  q=rt.rpc_permission(p);self.assertFalse(q['paid_overage_authorized']);self.assertEqual(777,q['included_compute_units_remaining']);self.assertNotIn('eth_call',p['method_cu_upper_bounds'])
  with self.assertRaises(ValueError):rt.validate_rpc({'method':'debug_traceTransaction','params':['0x'+'1'*64,{}]})
if __name__=='__main__':unittest.main()
