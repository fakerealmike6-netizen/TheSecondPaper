"""Synthetic transport/coverage controls; no actual provider or production run."""
import copy,hashlib,json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from dataclasses import asdict
import stage1d_weth_log_index as adapter
from stage1d_weth_log_index import normalize,_actual_need,TOKEN
from stage1d_finite_state_rpc import WETH,TRANSFER,DEPOSIT,WITHDRAWAL,logs_plan,topic_address
from collector import Collector,Event,FetchResult,Scope,QUERY_WINDOW_MODE

A='0x'+'1'*40;B='0x'+'2'*40;TX='0x'+'3'*64;BH='0x'+'4'*64
def material():
 need={'address':A,'asset':TOKEN,'start_block':10,'end_block':20,'start_time':100,'end_time':200}
 log={'address':WETH,'transactionHash':TX,'blockHash':BH,'blockNumber':'0xf','transactionIndex':'0x1','logIndex':'0x2','removed':False,
      'topics':[TRANSFER,topic_address(A),topic_address(B)],'data':'0x'+format(10,'064x')}
 plan=logs_plan(A,10,20,'TRANSFER_OUT')
 env={'request':{'id':'test','jsonrpc':'2.0',**plan},'response':{'id':'test','jsonrpc':'2.0','result':[log]}}
 headers={15:{'number':'0xf','hash':BH,'timestamp':'0x96','transactions':['0x'+'9'*64,TX]}}
 return env,headers,need
def current(lo=10,hi=20,start=100,end=220):
 query={'query_id':'controlled-weth-logs','name':'txphish_src001','start_block':lo,'end_block':hi,
   'start_time_utc':start,'end_time_utc':end,'max_acquisition_depth':3,
   'primary_window_mode':QUERY_WINDOW_MODE,'primary_local_window_seconds':100}
 scope=Scope.from_policy(query);query.update(scope_id=scope.scope_id,scope_hash=scope.scope_hash,scope=asdict(scope))
 seed=Event('seed','0x'+'5'*64,'0x'+'6'*40,A,TOKEN,10,lo,0,start,kind='erc20',log_index=0)
 state={'query_id':query['query_id'],'address':A,'asset':TOKEN,'arrival':asdict(seed),'depth':0,'local_end':min(end,start+100),'protocol_context':'ordinary'}
 collection={'query_id':query['query_id'],'metrics':{'scope_hash':scope.scope_hash},'states':[{'state':state,'identity':{'kind':'UNKNOWN'}}],'stops':[],
   'evidence_kind':'SYNTHETIC_CONTROLLED'}
 return query,collection
class WethLogIndexTests(unittest.TestCase):
 def test_real_normalized_transfer_enters_collector(self):
  env,headers,need=material();events,logs=normalize([env],headers,need)
  class Provider:
   replay_only=True
   def fetch_interval(self,*a,**kw):return FetchResult(events=events,complete=True,cache_hits=1)
  seed=Event('seed','0x'+'5'*64,'0x'+'6'*40,A,TOKEN,10,10,0,100,kind='erc20',log_index=0)
  scope=Scope('q','s',10,20,100,200,3,100,QUERY_WINDOW_MODE)
  got=asdict(Collector(Provider(),lambda a:{'kind':'SERVICE' if a==B else 'UNKNOWN'}).run(scope,seed))
  self.assertTrue(any(s['state']['address']==B for s in got['states']))
  self.assertEqual(events[0].amount_raw,10);self.assertEqual(events[0].log_index,2)
 def test_deposit_is_observed_and_does_not_create_transfer(self):
  env,headers,need=material();env['request'].update(logs_plan(A,10,20,'DEPOSIT'))
  env['response']['result'][0]['topics']=[DEPOSIT,topic_address(A)]
  events,logs=normalize([env],headers,need);self.assertEqual(events,[]);self.assertEqual(len(logs),1)
 def test_physical_duplicate_is_one_capacity_conflict_rejected(self):
  env,h,n=material();e,_=normalize([env,copy.deepcopy(env)],h,n);self.assertEqual(len(e),1)
  bad=copy.deepcopy(env);bad['response']['result'][0]['data']='0x'+format(11,'064x')
  with self.assertRaisesRegex(ValueError,'conflict'):normalize([env,bad],h,n)
 def test_no_header_no_timestamp_and_wrong_hash_fail(self):
  env,h,n=material()
  with self.assertRaisesRegex(ValueError,'gap'):normalize([env],{},n)
  h[15]['hash']='0x'+'9'*64
  with self.assertRaisesRegex(ValueError,'differs'):normalize([env],h,n)
 def test_exact_seconds_filter(self):
  env,h,n=material();n['end_time']=149
  e,logs=normalize([env],h,n);self.assertEqual(e,[]);self.assertEqual(len(logs),1)
 def test_reference_or_stopped_arrival_cannot_authorize_logs(self):
  query,collection=current();_,_,need=material()
  self.assertEqual(_actual_need(query,collection,need).scope_hash,query['scope_hash'])
  for reason in ('service','supported','user_stop','missing','past_depth'):
   c=copy.deepcopy(collection)
   if reason=='service':c['states'][0]['identity']['kind']='SERVICE'
   elif reason=='supported':c['states'][0]['identity']['branch_action']='SUPPORTED_OPERATION_RESOLVE'
   elif reason=='user_stop':c['stops']=[dict(c['states'][0],reason='USER_DECLARED_MAGPIE_BRANCH_STOP')]
   elif reason=='past_depth':c['states'][0]['state']['depth']=3
   else:c['states']=[]
   with self.subTest(reason=reason),self.assertRaisesRegex(ValueError,'expandable'):_actual_need(query,c,need)
 def test_header_transaction_index_must_bind_exact_log_hash(self):
  env,h,n=material();h[15]['transactions'][1]='0x'+'8'*64
  with self.assertRaisesRegex(ValueError,'ordering'):normalize([env],h,n)

class CoverageTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]);self.addCleanup(self.temp.cleanup)
  self.work=Path(self.temp.name);self.query,self.collection=current();self.need={'address':A,'asset':TOKEN,
   'start_block':10,'end_block':20,'start_time':150,'end_time':160}
  self.times={b:100+(b-10)*12 for b in range(10,21)};self.executed=[];self.empty_logs=False
  self.collection_path=self.work/'derived/query/collection.json';self.collection_path.parent.mkdir(parents=True)
  self.save(self.collection_path,self.collection);self.save(self.collection_path.parent/'label_snapshot.json',{'evidence_kind':'SYNTHETIC_CONTROLLED'})
  for target in ('stage1d_weth_log_index.active_batch','stage1d_finite_state_route.active_batch'):
   patcher=patch(target,side_effect=lambda work:{'queries':[self.query]});patcher.start();self.addCleanup(patcher.stop)
  patcher=patch('stage1d_weth_log_index.execute',side_effect=self.execute)
  self.execute_mock=patcher.start();self.addCleanup(patcher.stop)
 def save(self,path,obj):
  path.parent.mkdir(parents=True,exist_ok=True);raw=(json.dumps(obj,sort_keys=True)+'\n').encode();path.write_bytes(raw)
  return {'artifact_path':path.relative_to(self.work).as_posix(),'artifact_sha256':hashlib.sha256(raw).hexdigest()}
 def execute(self,work,path):
  requirement=json.loads(path.read_bytes());members=[]
  for plan in requirement['plans']:
   self.executed.append(copy.deepcopy(plan));ident=adapter.digest(plan);request={'jsonrpc':'2.0','id':ident,**plan}
   if plan['method']=='eth_getBlockByNumber':
    number=int(plan['params'][0],16);value={'number':hex(number),'timestamp':hex(self.times[number]),
     'hash':BH if number==15 else '0x'+format(number,'064x'),'transactions':['0x'+'9'*64,TX]}
   else:
    f=plan['params'][0];value=[]
    if not self.empty_logs and int(f['fromBlock'],16)<=15<=int(f['toBlock'],16):
     base=material()[0]['response']['result'][0]
     all_logs=[]
     for index,topic,sender,recipient,amount in ((2,TRANSFER,A,B,10),(3,TRANSFER,B,A,5),(4,DEPOSIT,A,None,8),
         (5,WITHDRAWAL,A,None,4),(6,TRANSFER,A,B,0),(7,TRANSFER,A,A,2)):
      log=dict(base,logIndex=hex(index),data='0x'+format(amount,'064x'),topics=[topic,topic_address(sender)]+([topic_address(recipient)] if recipient else []))
      all_logs.append(log)
     value=[log for log in all_logs if all(topic is None or log['topics'][i]==topic for i,topic in enumerate(f['topics']))]
   response={'jsonrpc':'2.0','id':ident,'result':value};folder=self.work/'raw/controlled'/ident
   raw_ref=self.save(folder/'response_body.bin',[response])
   env={'request':request,'response':response,'provider_alias':'ALCHEMY_ETH_MAINNET_EXISTING','evidence_kind':'REAL_CHAIN',
    'status':'SUCCESS_VALIDATED','http_status':200,'response_complete':True,'raw_body_sha256':raw_ref['artifact_sha256']}
   member=self.save(folder/'member.json',env);members.append(dict(member,plan=plan,result=value,cache_hit=True))
  return {'status':'COMPLETE','actual_operations_this_call':0,'results':list(reversed(members))}
 def run_acquire(self):return adapter.acquire(self.work,self.query,self.need,self.collection_path)
 def coverage(self):
  path=next((self.work/'derived/stage1d/intervals').glob('*.coverage.json'))
  return json.loads(path.read_bytes())
 def test_actual_exact_bracket_raw_four_kinds_and_cache_consumption(self):
  state=self.run_acquire();self.assertEqual(state['status'],'COMPLETE',state)
  self.assertEqual(state['resolved_blocks'],[15,15]);self.assertEqual(state['observed_log_count'],6)
  self.assertEqual(state['ordinary_transfer_events'],3)
  logs=[p for p in self.executed if p['method']=='eth_getLogs'];self.assertEqual(len(logs),4)
  self.assertTrue(all(p['params'][0]['fromBlock']=='0xf' and p['params'][0]['toBlock']=='0xf' for p in logs))
  record=self.coverage();self.assertTrue(adapter.verify_interval_record(self.work,record))
  from stage1d_acquisition import CachedIntervals
  result=CachedIntervals(self.work).fetch_interval(A,TOKEN,10,20,start_time=150,end_time=160,global_end_time=220)
  self.assertTrue(result.complete);self.assertEqual(len(result.events),3)
 def test_empty_between_adjacent_blocks_proves_coverage_without_logs(self):
  self.query,self.collection=current(10,11,100,200);self.need.update(end_block=11)
  self.times={10:100,11:200};self.save(self.collection_path,self.collection)
  state=self.run_acquire();self.assertEqual(state['status'],'COMPLETE',state)
  self.assertEqual(state['time_intersection'],'EMPTY');self.assertFalse(any(p['method']=='eth_getLogs' for p in self.executed))
  self.assertTrue(adapter.verify_interval_record(self.work,self.coverage()))
 def test_empty_before_and_after_physical_block_domain(self):
  self.need.update(start_block=15,start_time=100,end_time=120)
  state=self.run_acquire();self.assertEqual(state['status'],'COMPLETE',state)
  self.assertTrue(self.coverage()['empty_time_intersection']);self.assertFalse(any(p['method']=='eth_getLogs' for p in self.executed))
 def test_no_logs_in_nonempty_block_interval_still_requires_four_raw_responses(self):
  self.empty_logs=True;state=self.run_acquire();self.assertEqual(state['status'],'COMPLETE',state)
  self.assertEqual(len(state['log_members']),4);self.assertEqual(state['ordinary_transfer_events'],0)
  self.assertTrue(adapter.verify_interval_record(self.work,self.coverage()))
 def test_new_log_body_missing_or_changed_rejected(self):
  self.run_acquire();record=self.coverage();state=json.loads((self.work/record['state_path']).read_bytes())
  raw=(self.work/state['log_members'][0]['artifact_path']).parent/'response_body.bin';raw.write_bytes(b'[]')
  with self.assertRaisesRegex(ValueError,'original response body'):adapter.verify_interval_record(self.work,record)
  raw.unlink()
  with self.assertRaisesRegex(ValueError,'original response body'):adapter.verify_interval_record(self.work,record)
 def test_inherited_header_envelope_remains_a_point_proof(self):
  self.run_acquire();record=self.coverage();state=json.loads((self.work/record['state_path']).read_bytes())
  for member in state['headers'].values():
   raw=(self.work/member['artifact_path']).parent/'response_body.bin';raw.unlink()
  self.assertTrue(adapter.verify_interval_record(self.work,record))
 def test_partial_requirement_keeps_state_and_never_emits_coverage(self):
  self.execute_mock.side_effect=lambda work,path:{'status':'PARTIAL','actual_operations_this_call':0,'results':[]}
  state=self.run_acquire();self.assertEqual(state['status'],'PARTIAL')
  self.assertEqual(len(state['requirements']),1);self.assertEqual(state['new_rpc_operations'],0)
  self.assertFalse(list((self.work/'derived/stage1d/intervals').glob('*.coverage.json')))
 def test_completed_coverage_uses_immutable_snapshot_and_never_redispatches(self):
  self.run_acquire();record=self.coverage();proof=self.work/record['state_path']
  self.save(proof.parent/'STATE.json',{'status':'PARTIAL','only_mutable_retry_state':True})
  self.assertTrue(adapter.verify_interval_record(self.work,record));before=len(self.executed)
  self.assertEqual(self.run_acquire()['status'],'COMPLETE');self.assertEqual(len(self.executed),before)
 def test_user_stopped_state_cannot_dispatch(self):
  self.collection['stops']=[dict(self.collection['states'][0],reason='USER_DECLARED_MAGPIE_BRANCH_STOP')]
  self.save(self.collection_path,self.collection)
  with self.assertRaisesRegex(ValueError,'expandable'):self.run_acquire()
  self.execute_mock.assert_not_called()

if __name__=='__main__':unittest.main()
