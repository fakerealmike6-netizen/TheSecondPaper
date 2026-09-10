"""Full saved source -> exact native cache -> real consumer, offline only."""
from copy import deepcopy
from pathlib import Path
import json,tempfile,unittest
from unittest.mock import patch
from test_current_context_paid_bq import Fixture,A,B,X
from test_cost_request_guard import Fixture as GuardFixture
import stage1d_current_context_paid_bq as paid
import stage1d_bq_context_prepare as h
import stage1d_paid_candidate_projection as p
from stage1d_acquisition import CachedIntervals

HERE=Path(__file__).resolve().parent

class PaidCandidateProjectionTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(dir=HERE);self.addCleanup(self.temp.cleanup)
  self.w=Path(self.temp.name);self.f=Fixture(self.w)

 def setup_source(self,rows=None,*,plan=None):
  self.rows=self.f.family() if rows is None else rows
  source=self.f.prepare(self.rows)
  self.qdir=self.w/'derived/stage1d/queries'/self.f.q['name']
  h.save(self.qdir/'collection.json',{'controlled_current_input':True})
  h.save(self.qdir/'label_snapshot.json',{})
  self.demand=dict(context_plan=plan or self.f.plan,binding={'scope_hash':self.f.q['scope_hash']},point_requests=[])
  self.need=dict(address=A,asset=p.NATIVE,start_block=100,end_block=200,
   start_time=self.f.scope.start_time,end_time=self.f.scope.end_time)
  self.entry=dict(query_name=self.f.q['name'],query_id=self.f.q['query_id'],scope_hash=self.f.q['scope_hash'],
   scope_id=self.f.q['scope_id'],collection_path=(self.qdir/'collection.json').relative_to(self.w).as_posix(),
   collection_sha256=h.sha(self.qdir/'collection.json'),needed_ranges=[self.need])
  with patch.object(paid,'requirements',return_value=self.demand):
   self.admission=paid.admit_current(self.w,self.f.q['name'],h.dep(self.w,self.qdir/'collection.json'),
    h.dep(self.w,self.qdir/'label_snapshot.json'),[source])
  h.save(self.w/'ADMISSION.json',self.admission);h.save(self.w/'SOURCES.json',{'sources':[source]})

 def install(self):
  with patch.object(p,'requirements',return_value=self.demand):
   return p.install(self.w,self.f.q,self.entry,'ADMISSION.json','SOURCES.json','projected')

 def fetch(self,cache=None,need=None):
  n=need or self.need;cache=cache or CachedIntervals(self.w);cache.bind_scope(self.f.scope)
  return cache.fetch_interval(n['address'],n['asset'],n['start_block'],n['end_block'],
   start_time=n['start_time'],end_time=n['end_time'],global_end_time=self.f.scope.end_time)

 def test_actual_source_install_and_cache_consumer_keep_native_only_claim(self):
  self.setup_source();receipt=self.install()
  self.assertEqual(receipt['new_external_requests'],0)
  with patch.object(p,'requirements',return_value=self.demand):got=self.fetch()
  self.assertTrue(got.complete);self.assertEqual(len(got.events),1)
  self.assertEqual(got.events[0].amount_raw,9)
  self.assertEqual(got.coverage[0]['coverage_capability'],'NATIVE_INDEX_ONLY')
  self.assertFalse(got.coverage[0]['all_asset_export_complete'])
  self.assertFalse(receipt['full_context_claimed']);self.assertFalse(receipt['source_zero_claimed'])
  self.assertEqual(len(h.read(self.w/'projected/PROJECTED_ROWS.json')),3)

 def test_range_difference_stays_missing_and_does_not_refresh_time_or_token(self):
  self.setup_source();self.install()
  with patch.object(p,'requirements',return_value=self.demand):
   cache=CachedIntervals(self.w)
   wide=self.fetch(cache,dict(self.need,end_block=1000))
   self.assertFalse(wide.complete)
   self.assertEqual(wide.coverage[0]['uncovered_intervals'][0]['start_block'],201)
   token=self.fetch(cache,dict(self.need,asset='erc20:eip155:1:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'))
   self.assertFalse(token.complete);self.assertEqual(token.events,[])

 def test_source_islands_do_not_fill_hole_and_each_kind_required(self):
  plan={'rows':[dict(self.f.plan['rows'][0],ledger_start_block=100,ledger_end_block=120),
                dict(self.f.plan['rows'][0],ledger_start_block=180,ledger_end_block=200)]}
  self.setup_source(plan=plan);self.install()
  with patch.object(p,'requirements',return_value=self.demand):got=self.fetch()
  self.assertFalse(got.complete)
  self.assertEqual([(r['start_block'],r['end_block']) for r in got.coverage[0]['uncovered_intervals']],[(121,179)])
  for kind in paid.KINDS:
   admitted=deepcopy(self.admission);admitted['coverage']=[r for r in admitted['coverage'] if r['data_type']!=kind]
   self.assertEqual(p.project(self.f.q,{},admitted,[self.need])['records'],[])

 def test_unrelated_unbound_original_family_does_not_poison_proven_account(self):
  other=self.f.family(root=None,sender=X,tx_digit='c')
  for row in other:row.update(from_address=X,to_address=X)
  self.setup_source(self.f.family()+other);self.install()
  with patch.object(p,'requirements',return_value=self.demand):self.assertTrue(self.fetch().complete)
  self.assertEqual(len(h.read(self.w/'projected/PROJECTED_ROWS.json')),3)
  self.assertEqual(len(h.read(self.w/'jobs/0/raw.json')['rows']),6)

 def test_failed_zero_and_gas_retained_without_positive_event_or_zero_upper_claim(self):
  self.setup_source(self.f.family(success=False,amount='0'));receipt=self.install()
  with patch.object(p,'requirements',return_value=self.demand):got=self.fetch()
  self.assertTrue(got.complete)
  self.assertEqual(len(got.events),1);self.assertFalse(got.events[0].success)
  self.assertEqual(got.events[0].amount_raw,0);self.assertEqual(got.events[0].gas_raw,42000)
  rows=h.read(self.w/'projected/PROJECTED_ROWS.json')
  top=next(r for r in rows if r['record_type']=='transaction')
  self.assertFalse(top['success']);self.assertEqual(top['gas_used'],'21000')
  self.assertFalse(receipt['source_zero_claimed'])

 def test_incoming_refund_keeps_external_payer_and_full_family(self):
  rows=self.f.family(sender=X)
  rows[2].update(to_address=A,trace_type='selfdestruct',call_type=None,created_address=B,refund_address=A)
  self.setup_source(rows);self.install()
  with patch.object(p,'requirements',return_value=self.demand):got=self.fetch()
  self.assertTrue(got.complete);self.assertTrue(any(e.recipient==A for e in got.events))
  self.assertEqual(len(h.read(self.w/'projected/PROJECTED_ROWS.json')),3)

 def test_cache_replays_original_source_once_per_operation_even_many_rectangles(self):
  self.setup_source();self.entry['needed_ranges'] += [dict(self.need,start_block=160),dict(self.need,end_block=140)]
  self.install()
  with patch.object(p,'requirements',return_value=self.demand),patch.object(paid,'_load_source',wraps=paid._load_source) as load:
   cache=CachedIntervals(self.w)
   self.assertEqual(len(cache.records),3);self.assertEqual(load.call_count,1)
   self.assertTrue(self.fetch(cache).complete)

 def test_frozen_query_original_page_and_export_sql_tamper_are_rejected(self):
  self.setup_source();self.install()
  for name in ('private/BATCH_QUERY_FREEZE.json','jobs/0/raw.json','prepared/PREPARATION.json'):
   path=self.w/name;raw=path.read_bytes();path.write_bytes(raw+b' ')
   with patch.object(p,'requirements',return_value=self.demand),self.assertRaises(ValueError):CachedIntervals(self.w)
   path.write_bytes(raw)

 def test_forged_expanded_cache_scope_asset_and_event_bytes_rejected(self):
  self.setup_source();receipt=self.install();path=self.w/receipt['coverage_records'][0]['path']
  original=h.read(path)
  for change in ({'end_block':201},{'context_complete':True},{'all_asset_export_complete':True},
                 {'asset':'ETH'},{'provider':'ALCHEMY'},{'coverage_basis':'UNVERIFIED'},
                 {'scope_hash_at_acquisition':'0'*64},{'evidence_id':'0'*64}):
   path.write_text(json.dumps(dict(original,**change)),encoding='utf8')
   with patch.object(p,'requirements',return_value=self.demand),self.assertRaises(ValueError):CachedIntervals(self.w)
  path.write_text(json.dumps(original),encoding='utf8');(self.w/'projected/EVENTS.json').write_bytes(b'[]')
  with patch.object(p,'requirements',return_value=self.demand),self.assertRaises(ValueError):CachedIntervals(self.w)

 def test_real_cost_overlay_stops_and_out_of_window_needs_do_not_project(self):
  work=self.w/'guard';work.mkdir();f=GuardFixture(work)
  def admission(address):
   return {'rows':[],'coverage':[dict(address=address,data_type=kind,start_block=1,end_block=1000,
    status='COMPLETE',pagination_complete=True,evidence_ids=['SYNTHETIC'],date_domain_verified=True,
    block_domain_verified=True,source_bound_kind_validation=True) for kind in paid.KINDS]}
  legal=f.rectangle(f.a)
  self.assertEqual(len(p.project(f.q,f.c,admission(f.a['address']),[legal])['records']),1)
  for need in (f.rectangle(f.b),dict(legal,end_time=legal['end_time']+1),dict(legal,start_block=True)):
   with self.assertRaises(ValueError):p.project(f.q,f.c,admission(need['address']),[need])

if __name__=='__main__':unittest.main()
