"""Saved paid ordinary operands reach the real source consumer unchanged."""
import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from test_current_context_paid_bq import Fixture,A,B,S
import stage1d_current_context_paid_bq as a
import stage1d_bq_context_prepare as h
from stage1d_bq_root_binding import _bind,paid_top_fields,bind_paid_top_header
from context_ledger_r3 import normalize_rows


class PaidTopBindingTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(dir=S);self.addCleanup(self.temp.cleanup)
  self.w=Path(self.temp.name);self.f=Fixture(self.w)
  self.rows=self.f.family(root=None);top=self.rows[0]
  self.header=dict(number=hex(150),hash=top['block_hash'],timestamp=hex(self.f.scope.start_time+5400),transactions=[top['tx_hash']])

 def member(self,plan,value,name):
  folder=self.w/name;folder.mkdir(parents=True,exist_ok=True)
  req=dict(plan,id=name,jsonrpc='2.0');resp=dict(id=name,jsonrpc='2.0',result=value)
  raw=folder/'response_body.bin';raw.write_bytes(json.dumps([resp]).encode())
  h.save(folder/'env.json',dict(provider_alias=a.transfers.PROVIDER,evidence_kind='REAL_CHAIN',http_status=200,
   response_complete=True,status='SUCCESS_VALIDATED',request=req,response=resp,raw_body_sha256=h.sha(raw)))
  return dict(plan=plan,member=dict(artifact_path=(folder/'env.json').relative_to(self.w).as_posix(),
   artifact_sha256=h.sha(folder/'env.json'),result=value,status='SUCCESS_VALIDATED',cache_hit=False))

 def load(self,rows=None):
  source=self.f.prepare(rows or self.rows)
  return source,a._load_source(self.w,source)

 def admit(self,loaded,bindings=()):
  return a._admit_plan(self.f.scope,self.f.plan,[loaded],a._Points(self.w,bindings,allow_paid_top_fields=True))

 def test_actual_public_paid_entry_uses_header_only_and_complete_original_pages(self):
  source,_=self.load();qdir=self.w/'derived/stage1d/queries'/self.f.q['name']
  h.save(qdir/'collection.json',{'controlled_current_input':True});h.save(qdir/'label_snapshot.json',{})
  demand=dict(context_plan=self.f.plan,binding={'scope_hash':self.f.q['scope_hash']},point_requests=[])
  hb=self.member(a._header_plan(150),self.header,'header')
  with patch.object(a,'requirements',return_value=demand):
   got=a.admit_current(self.w,self.f.q['name'],h.dep(self.w,qdir/'collection.json'),h.dep(self.w,qdir/'label_snapshot.json'),[source],point_bindings=[hb])
  self.assertEqual({r['data_type'] for r in got['coverage']},set(a.KINDS))
  self.assertEqual(got['point_binding_requests'],[]);self.assertEqual(len(got['used_point_bindings']),1)
  self.assertTrue(got['ordinary_paid_top_field_binding_enabled']);self.assertFalse(got['full_context_claimed'])
  proof=got['sources'][0]['root_bindings'][0]['root_binding']
  self.assertIn('VERIFIED_PAID_ORDINARY_TOP',proof['basis'])
  self.assertFalse(proof['paid_top_field_proof']['rpc_transaction_or_receipt_manufactured'])
  self.assertFalse(proof['paid_top_field_proof']['full_receipt_or_logs_materialized'])
  self.assertFalse(proof['paid_top_field_proof']['gas_prepayment_timing_certified'])
  self.assertIsNone(self.rows[1]['trace_address'])

 def test_no_header_requests_only_exact_missing_header_not_tx_and_receipt(self):
  _,loaded=self.load();got=self.admit(loaded)
  self.assertEqual(got['point_binding_requests'],[a._header_plan(150)])
  self.assertNotIn(h.INTERNAL,{c['data_type'] for c in got['coverage']})
  self.assertTrue(any(r.get('trace_address') is None for r in got['rows'] if r['record_type']=='trace'))

 def test_missing_or_unsupported_type_or_fee_retains_old_point_requirements(self):
  for field,value in [('transaction_type',None),('transaction_type','3'),('transaction_type','4'),
                      ('gas_used',None),('effective_gas_price',None),('input_data',None),('to_address',None),('blob_gas_used','0')]:
   with self.subTest(field=field,value=value):
    rows=copy.deepcopy(self.rows);rows[0][field]=value
    self.assertIsNone(paid_top_fields(rows))

 def test_exact_aliases_zero_failed_and_large_values_preserve_original_normalized_money(self):
  for typ in (0,1,2):
   for success in (False,True):
    with self.subTest(type=typ,success=success):
     rows=self.f.family(root=None,success=success,amount=str(2**200+1 if success else 0))
     rows[0].update(type=hex(typ),transaction_type=str(typ))
     tx,fee=paid_top_fields(rows)
     old=_bind(rows[1:],tx,fee,self.header,['controlled:paid','controlled:header'])
     new=bind_paid_top_header(rows,self.header,['controlled:paid','controlled:header'])
     before=normalize_rows([old['current_top_row']]+old['rows'])
     after=normalize_rows([new['current_top_row']]+new['rows'])
     self.assertEqual(before,after);self.assertFalse(after['conflicts'])
     self.assertNotIn('response',new['current_top_row']);self.assertNotIn('logs',new['current_top_row'])

 def test_invalid_aliases_numbers_and_gas_operands_cannot_fallback_silently(self):
  for changes in ({'type':'0x1'},{'gas_used':21000.0},{'value_raw':True},{'tx_index':float('nan')},
                  {'gas_used':'50001'},{'input_data':'0x1'},{'success':'true'}):
   with self.subTest(changes=changes),self.assertRaises(ValueError):
    rows=copy.deepcopy(self.rows);rows[0].update(changes);paid_top_fields(rows)

 def test_separate_complete_header_and_full_tree_still_mandatory(self):
  for mutation in ('hash','time','order','sibling','ancestor','value','status'):
   with self.subTest(mutation=mutation),self.assertRaises(ValueError):
    rows=copy.deepcopy(self.rows);head=copy.deepcopy(self.header)
    if mutation=='hash':head['hash']='0x'+'c'*64
    elif mutation=='time':head['timestamp']=hex(int(head['timestamp'],16)+1)
    elif mutation=='order':head['transactions']=['0x'+'d'*64]
    elif mutation=='sibling':rows.pop()
    elif mutation=='ancestor':rows[2]['trace_address']='[0,0]'
    elif mutation=='value':rows[1]['value_raw']='8'
    elif mutation=='status':rows[1]['success']=False
    bind_paid_top_header(rows,head,['controlled:paid','controlled:header'])

 def test_supplied_conflicting_receipt_prevents_paid_fields_from_winning(self):
  _,loaded=self.load();tx,fee=paid_top_fields(self.rows)
  fee.update(gasUsed=hex(42000),effectiveGasPrice='0x1') # same product, different operands
  bad=self.member(dict(method='eth_getTransactionReceipt',params=[tx['hash']]),fee,'receipt')
  hb=self.member(a._header_plan(150),self.header,'header')
  got=self.admit(loaded,[bad,hb]);self.assertEqual(got['coverage'],[])
  self.assertTrue(any(g.get('provided_binding_evidence_failed') for g in got['binding_gaps']))

 def test_original_page_and_header_raw_tampering_is_not_a_reuse_exception(self):
  source,loaded=self.load();hb=self.member(a._header_plan(150),self.header,'header')
  (self.w/'header/response_body.bin').write_bytes(b'[]')
  with self.assertRaises(ValueError):self.admit(loaded,[hb])
  (self.w/'jobs/0/raw.json').write_bytes(b'{}')
  with self.assertRaises(ValueError):a._load_source(self.w,source)

if __name__=='__main__':unittest.main()
