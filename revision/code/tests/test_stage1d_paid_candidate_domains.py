"""Exact discovery tails, source holes, current roles and real cache consumer."""
from pathlib import Path
from copy import deepcopy
import tempfile,unittest,json
from unittest.mock import patch
from test_current_context_paid_bq import Fixture,A,B,X
from test_cost_request_guard import Fixture as CostFixture
import stage1d_bq_context_prepare as h
import stage1d_current_context_paid_bq as paid
import stage1d_paid_candidate_domains as p
from stage1d_acquisition import CachedIntervals

HERE=Path(__file__).resolve().parent

class PaidCandidateDomainTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(dir=HERE);self.addCleanup(self.tmp.cleanup)
  self.w=Path(self.tmp.name);self.f=Fixture(self.w)

 def fixture(self,rows=None,**bounds):
  self.original_rows=self.f.family() if rows is None else rows
  self.source=self.f.prepare(self.original_rows)
  self.collection={'query_id':self.f.q['query_id']}
  qdir=self.w/'derived/stage1d/queries'/self.f.q['name']
  h.save(qdir/'collection.json',self.collection);h.save(qdir/'label_snapshot.json',{})
  h.save(self.w/'INVENTORY.json',{'sources':[self.source]})
  self.need=dict(address=A,asset=p.NATIVE,start_block=100,end_block=1000,
   start_time=self.f.scope.start_time,end_time=self.f.scope.end_time,**{})|bounds
  self.entry=dict(query_name=self.f.q['name'],query_id=self.f.q['query_id'],scope_id=self.f.q['scope_id'],scope_hash=self.f.q['scope_hash'],
   collection_path=(qdir/'collection.json').relative_to(self.w).as_posix(),collection_sha256=h.sha(qdir/'collection.json'),
   needed_ranges=[self.need])

 def prepare(self):return p.prepare(self.w,self.f.q,self.entry,'INVENTORY.json','admitted')
 def install(self):
  result=self.prepare();p.install(self.w,result['proof']['path'],self.entry)
  return h.read(self.w/'admitted/ADMISSION.json')['result']
 def fetch(self,cache=None,need=None):
  cache=cache or CachedIntervals(self.w);cache.bind_scope(self.f.scope);n=need or self.need
  return cache.fetch_interval(n['address'],n['asset'],n['start_block'],n['end_block'],
   start_time=n['start_time'],end_time=n['end_time'],global_end_time=self.f.scope.end_time)

 def test_real_entry_closes_tail_beyond_last_context_event_without_model_promotion(self):
  self.fixture();source=paid._load_source(self.w,self.source)
  old=paid._admit_plan(self.f.scope,self.f.plan,[source],paid._Points(self.w,[]))
  self.assertEqual({r['end_block'] for r in old['coverage']},{200})
  actual=self.install();got=self.fetch()
  self.assertTrue(got.complete);self.assertEqual(actual['segments'][0]['end_block'],1000)
  self.assertFalse(actual['full_context_claimed']);self.assertFalse(actual['model_or_protocol_credit_coverage_claimed'])
  self.assertFalse(actual['empty_index_sets_source_balance_to_zero'])
  self.assertEqual(len(got.events),1);self.assertEqual(got.events[0].amount_raw,9)
  self.assertEqual(got.coverage[0]['coverage_capability'],'NATIVE_INDEX_ONLY')

 def test_outside_seconds_does_not_demand_root_binding_and_original_rows_stay(self):
  self.fixture(self.f.family(root=None),end_time=self.f.scope.start_time+3600)
  actual=self.install();got=self.fetch()
  self.assertTrue(got.complete);self.assertEqual(got.events,[])
  self.assertEqual(actual['point_binding_requests'],[]);self.assertEqual(actual['rows'],[])
  self.assertEqual(len(h.read(self.w/'jobs/0/raw.json')['rows']),3)
  self.assertFalse(actual['empty_index_sets_source_balance_to_zero'])

 def test_null_root_within_seconds_remains_partial_and_only_exact_header_is_requested(self):
  self.fixture(self.f.family(root=None));self.prepare()
  result=h.read(self.w/'admitted/ADMISSION.json')['result']
  self.assertEqual(result['segments'],[]);self.assertEqual(result['point_binding_requests'],[paid._header_plan(150)])
  self.assertEqual(result['missing_source_rectangles'],[])
  self.assertTrue(result['incomplete_candidate_rectangles'])

 def test_whole_family_refund_failed_zero_gas_stay_in_original_admission(self):
  rows=self.f.family(success=False,amount='0')
  self.fixture(rows);result=self.install();got=self.fetch()
  self.assertEqual(len(result['rows']),3);self.assertEqual(len(got.events),1)
  self.assertFalse(got.events[0].success);self.assertEqual(got.events[0].gas_raw,42000)
  self.assertEqual(got.events[0].amount_raw,0)

 def test_block_AND_time_intersection_leaves_source_holes_not_a_bounding_envelope(self):
  self.fixture([]);source=paid._load_source(self.w,self.source)
  source['manifest']['needed_ranges']=[dict(address=A,start_block=100,end_block=200)]
  source['date_intervals']=[[self.f.scope.start_time,self.f.scope.start_time+3600]]
  result=p.compute(self.f.q,self.collection,[self.need],[source],paid._Points(self.w,[]))
  self.assertEqual(len(result['segments']),1)
  self.assertEqual(result['segments'][0]['end_block'],200)
  self.assertEqual(result['segments'][0]['end_time'],self.f.scope.start_time+3600)
  self.assertTrue(result['missing_source_rectangles'])

 def test_different_family_timestamps_are_conflicts_not_favorable_window_selection(self):
  rows=self.f.family();rows[1]['block_time']='2024-08-21T03:00:00Z'
  self.fixture(rows)
  with self.assertRaisesRegex(ValueError,'conflicting block/time'):self.prepare()

 def test_other_address_bad_root_does_not_poison_exact_current_query(self):
  rows=self.f.family();other=self.f.family(root=None,tx_digit='c',sender=X)
  for row in other:row.update(from_address=X,to_address=X)
  self.fixture(rows+other);result=self.install()
  self.assertEqual(len(result['rows']),3);self.assertEqual(result['point_binding_requests'],[])
  self.assertTrue(self.fetch().complete)

 def test_all_pages_schema_sql_and_event_integrity_checked_at_actual_cache_entry(self):
  self.fixture();self.install()
  for file in ('jobs/0/raw.json','prepared/PREPARATION.json','private/BATCH_QUERY_FREEZE.json','admitted/EVENTS.json'):
   path=self.w/file;original=path.read_bytes();path.write_bytes(original+b' ')
   with self.assertRaises(ValueError):CachedIntervals(self.w)
   path.write_bytes(original)

 def test_record_cannot_expand_domain_change_asset_or_claim_full_context(self):
  self.fixture();self.install();record=next((self.w/'derived/stage1d/intervals').glob('*.json'))
  original=record.read_bytes();doc=json.loads(original)
  for change in ({'start_block':99},{'end_time':self.f.scope.end_time+1},{'asset':'ETH'},
                 {'provider':'DUNE'},{'context_complete':True},{'all_asset_export_complete':True}):
   record.write_text(json.dumps(dict(doc,**change)),encoding='utf8')
   with self.assertRaises(ValueError):CachedIntervals(self.w)
  record.write_bytes(original)
  token=self.fetch(need=dict(self.need,asset='erc20:eip155:1:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))
  self.assertFalse(token.complete);self.assertEqual(token.events,[])

 def test_one_source_replay_per_cache_operation_and_physical_cross_query_reuse(self):
  self.fixture();self.entry['needed_ranges'].append(dict(self.need,start_block=160))
  self.install()
  with patch.object(paid,'_load_source',wraps=paid._load_source) as load:
   cache=CachedIntervals(self.w);self.assertEqual(len(cache.records),2);self.assertEqual(load.call_count,1)
  from dataclasses import replace
  other=replace(self.f.scope,query_id='another-controlled-query')
  cache.bind_scope(other);n=self.need
  got=cache.fetch_interval(n['address'],n['asset'],n['start_block'],n['end_block'],start_time=n['start_time'],
   end_time=n['end_time'],global_end_time=other.end_time)
  self.assertTrue(got.complete);self.assertEqual(got.coverage[0]['query_id'],other.query_id)
  self.assertNotEqual(got.coverage[0]['query_id'],self.f.q['query_id'])

 def test_stopped_or_pending_arrival_does_not_authorize_discovery(self):
  w=self.w/'cost';w.mkdir();f=CostFixture(w)
  self.assertEqual(len(p.lawful_needs(f.q,f.c,[f.rectangle(f.a)])),1)
  with self.assertRaises(ValueError):p.lawful_needs(f.q,f.c,[f.rectangle(f.b)])
  with self.assertRaises(ValueError):p.lawful_needs(f.q,f.c,[dict(f.rectangle(f.a),end_time=f.a['local_end']+1)])

if __name__=='__main__':unittest.main()
