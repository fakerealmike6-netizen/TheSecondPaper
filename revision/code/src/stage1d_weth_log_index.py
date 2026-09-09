"""Canonical holder logs with original-byte and exact timestamp coverage proof."""
from pathlib import Path
from dataclasses import asdict
import hashlib,json,re
from stage1d_closure_scope import active_batch
from stage1d_finite_state_rpc import WETH,TRANSFER,logs_plan,response_status,address,quantity
from stage1d_finite_state_route import prepare,execute
from stage1d_rpc import Stage1DRpcValidation
from collector import Event,Scope
from context_access_r3 import read,sha
from page_attempts import atomic_json

TOKEN='erc20:eip155:1:'+WETH
ADAPTER='stage1d-canonical-weth-log-index-v1'
KINDS=('TRANSFER_OUT','TRANSFER_IN','DEPOSIT','WITHDRAWAL')
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def _inside(work,value):
 path=(work/value).resolve()
 if not path.is_relative_to(work):raise ValueError('WETH evidence outside current work')
 return path
def _dependency(work,path):
 path=_inside(work,path)
 return {'path':path.relative_to(work).as_posix(),'sha256':sha(path)}
def _checked(work,ref):
 path=_inside(work,ref['path'])
 if sha(path)!=ref['sha256']:raise ValueError('WETH original dependency changed')
 return path
def _header(value,number):
 if (not isinstance(value,dict) or not quantity(value.get('number')) or int(value['number'],16)!=number
     or not quantity(value.get('timestamp')) or not re.fullmatch('0x[0-9a-fA-F]{64}',str(value.get('hash')))):
  raise ValueError('Exact canonical historical header required')
 hashes=value.get('transactions')
 if not isinstance(hashes,list) or any(not re.fullmatch('0x[0-9a-fA-F]{64}',str(v)) for v in hashes) or len(set(v.lower() for v in hashes))!=len(hashes):
  raise ValueError('Complete unique historical transaction order required')
 return value
def _env(work,member,expected_plan=None):
 """New logs need original body; inherited ordinary header points do not."""
 path=_checked(work,{'path':member['artifact_path'],'sha256':member['artifact_sha256']})
 env=read(path);req=env.get('request',{});response=env.get('response',{});plan={k:req.get(k) for k in ('method','params')}
 if expected_plan is not None and plan!=expected_plan:raise ValueError('Original WETH request selector differs')
 if 'result' in member and member['result']!=response.get('result'):raise ValueError('Saved member payload differs')
 if (not isinstance(req.get('id'),(int,str)) or isinstance(req.get('id'),bool)
     or type(req.get('id')) is not type(response.get('id'))):raise ValueError('Exact RPC member ID required')
 if plan['method']=='eth_getBlockByNumber':
  if Stage1DRpcValidation({}).rpc_result_status(req,response)!='SUCCESS_VALIDATED':raise ValueError('Historical header response binding failed')
  _header(response['result'],int(plan['params'][0],16))
  return env
 if plan['method']!='eth_getLogs' or response_status(req,response)!='SUCCESS_VALIDATED':raise ValueError('Full canonical holder log response required')
 if (env.get('provider_alias')!='ALCHEMY_ETH_MAINNET_EXISTING' or env.get('evidence_kind')!='REAL_CHAIN'
     or env.get('http_status')!=200 or env.get('response_complete') is not True or env.get('status')!='SUCCESS_VALIDATED'):
  raise ValueError('Successful original real WETH log envelope required')
 original_path=path;compressed=env.get('decoding')=='BOUNDED_ZSTD_FROM_SAVED_HTTP200_BYTES'
 if 'original_artifact' in env:
  if not compressed or env.get('new_http_requests')!=0:raise ValueError('Unknown WETH response recovery form')
  original_path=_checked(work,env['original_artifact']);original=read(original_path)
  if original.get('request')!=req or original.get('raw_body_sha256')!=env.get('raw_body_sha256'):raise ValueError('Recovered WETH log original request differs')
 raw=_inside(work,original_path.parent/'response_body.bin')
 if not raw.is_file() or sha(raw)!=env.get('raw_body_sha256'):raise ValueError('New WETH logs require unchanged original response body')
 body=raw.read_bytes()
 if compressed:
  import io
  from compression import zstd
  with zstd.ZstdFile(io.BytesIO(body)) as stream:body=stream.read(8388609)
 if len(body)>8388608:raise ValueError('Original WETH response exceeds bounded transport')
 values=json.loads(body)
 matches=[v for v in values if isinstance(v,dict) and type(v.get('id')) is type(req['id']) and v.get('id')==req['id']] if isinstance(values,list) else []
 if len(matches)!=1 or matches[0]!=response:raise ValueError('Original raw batch does not bind exactly this WETH log member')
 return env
def normalize(log_envelopes,headers,need):
 """Keep every raw observation, emitting only positive ordinary Transfers."""
 rows={};events=[]
 for env in log_envelopes:
  req=env.get('request',{});response=env.get('response',{})
  if req.get('method')!='eth_getLogs' or response_status(req,response)!='SUCCESS_VALIDATED':raise ValueError('Full canonical holder log response required')
  for log in response['result']:
   physical=(log['transactionHash'].lower(),int(log['logIndex'],16))
   if physical in rows and rows[physical]!=log:raise ValueError('Physical WETH log conflict')
   rows[physical]=log
 for ident,log in sorted(rows.items()):
  number=int(log['blockNumber'],16);header=headers.get(number)
  if header is None:raise ValueError('WETH log historical timestamp/header gap')
  _header(header,number)
  if header['hash'].lower()!=log['blockHash'].lower():raise ValueError('WETH log/header canonical block differs')
  index=int(log['transactionIndex'],16)
  if index>=len(header['transactions']) or header['transactions'][index].lower()!=ident[0]:raise ValueError('WETH log transaction ordering differs from full header')
  timestamp=int(header['timestamp'],16)
  if not need['start_block']<=number<=need['end_block'] or not need['start_time']<=timestamp<=need['end_time']:continue
  if log['topics'][0].lower()!=TRANSFER:continue
  sender='0x'+log['topics'][1][-40:].lower();recipient='0x'+log['topics'][2][-40:].lower();amount=int(log['data'],16)
  if amount==0:continue
  events.append(Event('eip155:1:tx:'+ident[0]+':log:'+str(ident[1]),ident[0],sender,recipient,TOKEN,amount,number,
   index,timestamp,kind='erc20',log_index=ident[1],block_hash=log['blockHash'].lower(),provenance='CANONICAL_WETH_TRANSFER_LOG'))
 return events,list(rows.values())
def _state_key(state):
 return (state.get('query_id'),state.get('address'),state.get('asset'),state.get('arrival',{}).get('event_id'),
         state.get('depth'),state.get('local_end'),state.get('protocol_context','ordinary'))
def _actual_need(query,collection,need):
 scope=Scope.from_policy(query);address(need['address'])
 if any(type(need.get(k)) is not int for k in ('start_block','end_block','start_time','end_time')):raise ValueError('Exact integer WETH need bounds required')
 if need.get('asset')!=TOKEN or collection.get('query_id')!=query['query_id'] or collection.get('metrics',{}).get('scope_hash')!=scope.scope_hash:raise ValueError('Current WETH query/graph required')
 if not scope.start_block<=need['start_block']<=need['end_block']<=scope.end_block or not scope.start_time<=need['start_time']<=need['end_time']<=scope.end_time:raise ValueError('WETH interval outside query')
 stopped={_state_key(v['state']) for v in collection.get('stops',[])}
 states=[]
 for value in collection['states']:
  state=value['state'];identity=value.get('identity',{})
  if (_state_key(state) in stopped or identity.get('branch_action')=='SUPPORTED_OPERATION_RESOLVE'
      or identity.get('kind') in ('SERVICE','BRIDGE','MIXER','UNSUPPORTED_PROTOCOL')):continue
  if (state.get('query_id')==query['query_id'] and state['address']==need['address'] and state['asset']==TOKEN
      and state.get('protocol_context','ordinary')=='ordinary' and state['depth']<scope.max_depth
      and state['arrival']['block']<=need['start_block'] and state['arrival']['timestamp']<=need['start_time']
      and need['end_time']<=state['local_end']):states.append(value)
 if not states:raise ValueError('WETH log interval has no actual expandable source arrival')
 return scope
def _ordered_results(plans,members):
 indexed={}
 for member in members:
  plan=member.get('plan');key=digest(plan)
  if plan not in plans or key in indexed:raise ValueError('Unexpected/duplicate finite member plan')
  indexed[key]=member
 if set(indexed)!={digest(p) for p in plans}:raise ValueError('Finite member plan missing')
 return [indexed[digest(p)] for p in plans]
def acquire(work,query,need,collection_path):
 from stage1d_timestamp_bracket import resolve_timestamp_bracket
 work=Path(work).resolve();collection_path=_inside(work,collection_path);collection=read(collection_path)
 if query not in active_batch(work)['queries']:raise ValueError('Active exact query required')
 _actual_need(query,collection,need)
 label=collection_path.parent/'label_snapshot.json';deps=[_dependency(work,collection_path)];role=_dependency(work,label)
 ident=digest({'scope':query['scope_hash'],'need':need,'graph':deps,'roles':role})
 folder=work/'private/stage1d_weth_log_index'/ident;folder.mkdir(parents=True,exist_ok=True)
 state_path=folder/'STATE.json';ep=work/'derived/stage1d/intervals'/(ident+'.events.json');coverage_path=ep.with_name(ident+'.coverage.json')
 if coverage_path.exists():
  record=read(coverage_path);verify_interval_record(work,record)
  return read(_checked(work,{'path':record['state_path'],'sha256':record['state_sha256']}))
 state=read(state_path) if state_path.exists() else {'schema_version':ADAPTER,'query':query,'query_name':query['name'],
  'scope_hash':query['scope_hash'],'need':need,'requirements':[],'headers':{},'new_rpc_operations':0}
 if state.get('need')!=need or state.get('query')!=query:raise ValueError('Preserved WETH request identity changed')
 def call(plans,label_text):
  path=prepare(work,query,plans,purpose='CANONICAL_WETH_DISCOVERY',requirement_evidence=deps,role_snapshot=role,missing_fields=[label_text])
  requirement=read(path);state.setdefault('input_snapshots',requirement['dependencies'])
  outcome=execute(work,path)
  state['requirements'].append({'path':path.relative_to(work).as_posix(),'sha256':sha(path),'outcome_status':outcome['status']})
  state['new_rpc_operations']+=outcome['actual_operations_this_call'];atomic_json(state_path,state)
  if outcome['status']!='COMPLETE':raise RuntimeError('Finite WETH requirement remains '+outcome['status'])
  return _ordered_results(plans,outcome['results'])
 def header(number):
  key=str(number);plan={'method':'eth_getBlockByNumber','params':[hex(number),False]}
  if key not in state['headers']:
   member=call([plan],'CANONICAL_TIMESTAMP_BRACKET')[0]
   state['headers'][key]={k:member[k] for k in ('result','artifact_path','artifact_sha256')};atomic_json(state_path,state)
  return _env(work,state['headers'][key],plan)['response']['result']
 try:
  proof=resolve_timestamp_bracket(need,header);state['timestamp_bracket']=proof
  state['resolved_blocks']=None if proof['empty'] else [proof['first_block'],proof['last_block']]
  members=[];envelopes=[]
  if not proof['empty']:
   plans=[logs_plan(need['address'],proof['first_block'],proof['last_block'],kind) for kind in KINDS]
   members=call(plans,'ALL_CANONICAL_WETH_HOLDER_LOGS')
   envelopes=[_env(work,member,plan) for plan,member in zip(plans,members)]
   state['log_members']=[{k:member[k] for k in ('plan','artifact_path','artifact_sha256')} for member in members];atomic_json(state_path,state)
   blocks=sorted({int(log['blockNumber'],16) for env in envelopes for log in env['response']['result']})
   plans=[{'method':'eth_getBlockByNumber','params':[hex(b),False]} for b in blocks if str(b) not in state['headers']]
   if plans:
    for member in call(plans,'MISSING_UNIQUE_WETH_LOG_BLOCK_HEADERS'):
     number=int(member['result']['number'],16);_env(work,member,member['plan'])
     state['headers'][str(number)]={k:member[k] for k in ('result','artifact_path','artifact_sha256')}
    atomic_json(state_path,state)
  else:state['log_members']=[]
  events,logs=normalize(envelopes,{int(k):v['result'] for k,v in state['headers'].items()},need)
  atomic_json(ep,[asdict(event) for event in events])
  state.update(status='COMPLETE',time_intersection='EMPTY' if proof['empty'] else 'NONEMPTY',
   observed_log_count=len(logs),ordinary_transfer_events=len(events))
  atomic_json(state_path,state)
  # Published coverage never points to a mutable retry state.
  proof_path=folder/('PROOF_'+digest(state)+'.json')
  if proof_path.exists() and read(proof_path)!=state:raise ValueError('Immutable WETH proof changed')
  if not proof_path.exists():atomic_json(proof_path,state)
  record={'evidence_id':ident,'addresses':[need['address']],'asset':TOKEN,**{k:need[k] for k in ('start_block','end_block','start_time','end_time')},
   'complete':True,'normalization_gaps':[],'provider':'Alchemy','acquisition_adapter':ADAPTER,
   'events_path':ep.relative_to(work).as_posix(),'events_sha256':sha(ep),'state_path':proof_path.relative_to(work).as_posix(),'state_sha256':sha(proof_path),
   'query_id_at_acquisition':query['query_id'],'scope_id_at_acquisition':query['scope_id'],'scope_hash_at_acquisition':query['scope_hash'],
   'log_scope':'ALL_CANONICAL_HOLDER_TRANSFER_DEPOSIT_WITHDRAWAL','native_gas_ledger_complete':False,'empty_time_intersection':proof['empty']}
  verify_interval_record(work,record);atomic_json(coverage_path,record)
  return state
 except (RuntimeError,ValueError,KeyError,TypeError) as exc:
  state.update(status='PARTIAL',reason=str(exc));atomic_json(state_path,state);return state
def verify_interval_record(work,record):
 from stage1d_timestamp_bracket import verify_timestamp_bracket
 work=Path(work).resolve();state=read(_checked(work,{'path':record['state_path'],'sha256':record['state_sha256']}))
 if state.get('status')!='COMPLETE' or state.get('schema_version')!=ADAPTER:raise ValueError('Only completed finite holder logs prove coverage')
 need=state['need'];query=state['query']
 if (record.get('complete') is not True or record.get('normalization_gaps')!=[]
     or record.get('acquisition_adapter')!=ADAPTER or record.get('native_gas_ledger_complete') is not False):
  raise ValueError('Only exact token discovery coverage is claimed')
 if (record['asset']!=TOKEN or record['addresses']!=[need['address']] or any(record[k]!=need[k] for k in ('start_block','end_block','start_time','end_time'))
     or any(record[k+'_at_acquisition']!=query[k] for k in ('query_id','scope_id','scope_hash'))):raise ValueError('WETH coverage need/scope differs')
 snapshots=state.get('input_snapshots',[])
 if len(snapshots)!=2:raise ValueError('Immutable collection and role snapshots required')
 inputs=[read(_checked(work,ref)) for ref in snapshots];_actual_need(query,inputs[0],need)
 source_refs=[{'path':ref['original_path'],'sha256':ref['sha256']} for ref in snapshots]
 if record['evidence_id']!=digest({'scope':query['scope_hash'],'need':need,'graph':source_refs[:1],'roles':source_refs[1]}):raise ValueError('Original WETH claim identity changed')
 if Path(record['state_path']).name!='PROOF_'+digest(state)+'.json':raise ValueError('Immutable completed WETH state required')
 if not state.get('requirements'):raise ValueError('Actual current requirement evidence required')
 authorized=set()
 for ref in state['requirements']:
  req=read(_checked(work,ref))
  if (Path(ref['path']).stem!=digest(req) or req.get('schema_version')!='stage1d-finite-state-requirement-v1'
      or req.get('purpose')!='CANONICAL_WETH_DISCOVERY' or req.get('additional_budget') is not False
      or req.get('authorization_id')!='STAGE1D_WINDOW_ROLE_SEMANTIC_CLOSURE_V1'
      or req.get('role_snapshot_sha256')!=snapshots[1]['sha256']
      or any(req.get(k)!=query[k] for k in ('query_id','scope_id','scope_hash')) or req.get('dependencies')!=snapshots):
   raise ValueError('Actual WETH requirement scope or original inputs differ')
  authorized.update(digest(plan) for plan in req['plans'])
 headers={}
 for block,member in state['headers'].items():
  plan={'method':'eth_getBlockByNumber','params':[hex(int(block)),False]}
  if digest(plan) not in authorized:raise ValueError('Header selector absent from actual finite requirements')
  headers[int(block)]=_env(work,member,plan)['response']['result']
 proof=verify_timestamp_bracket(need,state['timestamp_bracket'],headers)
 if proof['empty']:
  if state['log_members'] or state['resolved_blocks'] is not None or record.get('empty_time_intersection') is not True:raise ValueError('Empty interval cannot invent a log request or block range')
  envelopes=[]
 else:
  first,last=proof['first_block'],proof['last_block']
  if state['resolved_blocks']!=[first,last] or record.get('empty_time_intersection') is not False:raise ValueError('Exact WETH physical bracket differs')
  plans=[logs_plan(need['address'],first,last,kind) for kind in KINDS];members=state['log_members']
  if any(digest(plan) not in authorized for plan in plans):raise ValueError('Log selector absent from actual finite requirements')
  if len(members)!=len(plans) or [m.get('plan') for m in members]!=plans:raise ValueError('All four exact canonical holder log kinds required')
  envelopes=[_env(work,member,plan) for plan,member in zip(plans,members)]
 events,logs=normalize(envelopes,headers,need)
 if state.get('observed_log_count')!=len(logs) or state.get('ordinary_transfer_events')!=len(events):raise ValueError('Actual WETH observed/event counts differ')
 ep=_checked(work,{'path':record['events_path'],'sha256':record['events_sha256']})
 if read(ep)!=[asdict(e) for e in events]:raise ValueError('Canonical WETH normalization differs')
 return True
