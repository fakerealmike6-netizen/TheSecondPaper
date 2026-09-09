"""Revalidate historical technical-role sources, independent of custody labels.
Only exact arrival blocks are certified. Official deployment and verified
creation are alternative routes; names or derived booleans alone never suffice.
"""
from pathlib import Path
from copy import deepcopy
import hashlib,json,re
from urllib.parse import urlsplit
SCHEMA='stage1d-historical-technical-role-v1'
STOP_BASIS='OFFICIAL_DIRECT_ROLE_SCOPE_STOP_V1'
STOP_SUPPLEMENT='STAGE1D_STOP_BOUNDARY_PROPORTIONALITY_V1'
IMPLEMENTATION_SLOT='0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def ordinary(identity):
 r=deepcopy(identity);kind=r.get('kind','UNKNOWN')
 r.update(custodial_actor_status='EXISTING_QUALIFIED_CUSTODIAL_ACTOR' if kind=='SERVICE' else 'NOT_ESTABLISHED',
  technical_role_status='EXISTING_FROZEN_PROTOCOL_BOUNDARY' if kind in ('BRIDGE','MIXER','UNSUPPORTED_PROTOCOL') else 'UNKNOWN',
  branch_action='FIRST_SERVICE_STOP' if kind=='SERVICE' else 'UNSUPPORTED_PROTOCOL_STOP' if kind in ('BRIDGE','MIXER','UNSUPPORTED_PROTOCOL') else 'NORMAL_ACCOUNT_EXPAND')
 return r
def uint(v):
 if isinstance(v,bool):raise ValueError('Boolean is not a chain integer')
 if isinstance(v,int):n=v
 elif isinstance(v,str) and re.fullmatch(r'0x[0-9a-fA-F]+|[0-9]+',v):n=int(v,16 if v.startswith('0x') else 10)
 else:raise ValueError('Exact chain integer required')
 if n<0:raise ValueError('Negative chain integer')
 return n
def address(v):
 if not isinstance(v,str) or not re.fullmatch(r'0x[0-9a-fA-F]{40}',v):raise ValueError('Exact chain address required')
 return v.lower()
def binary(v):
 if not isinstance(v,str) or not re.fullmatch(r'0x(?:[0-9a-fA-F]{2})*',v):raise ValueError('Exact hex bytes required')
 return bytes.fromhex(v[2:])
def dependency(root,ref):
 if not isinstance(ref,dict) or not isinstance(ref.get('path'),str) or not re.fullmatch(r'[0-9a-f]{64}',ref.get('sha256','')):raise ValueError('Every raw source reference needs path and SHA-256')
 original=root/ref['path'];p=original.resolve()
 if not p.is_relative_to(root) or original.is_symlink() or not p.is_file() or sha(p)!=ref['sha256']:raise ValueError('Role evidence changed or escaped work root')
 if ref.get('format')=='public_text':
  journal=dependency(root,ref.get('journal',{}))
  url=journal.get('source_url',journal.get('url'))
  attempts=[a for a in journal.get('attempts',[]) if a.get('status')=='SUCCESS' and a.get('body_sha256')==ref['sha256']]
  if (journal.get('status')!='SUCCESS' or journal.get('body_sha256')!=ref['sha256'] or journal.get('body_path')!=p.name
      or len(attempts)!=1 or attempts[0].get('http_status')!=200 or attempts[0].get('final_url')!=url
      or attempts[0].get('request_url')!=url or attempts[0].get('transport_called') is not True
      or attempts[0].get('bytes')!=p.stat().st_size):raise ValueError('Public source journal does not bind original successful response')
  return {'source_url':url,'body':p.read_text(encoding='utf-8-sig'),'body_sha256':ref['sha256'],'journal_sha256':ref['journal']['sha256']}
 return read(p)
def rpc(raw,method,params):
 req,res=raw.get('request'),raw.get('response')
 if not isinstance(req,dict) or not isinstance(res,dict) or req.get('method')!=method or req.get('params')!=params:raise ValueError('Raw RPC method/selector mismatch: '+method)
 if res.get('error') or res.get('result') is None:raise ValueError('Raw RPC did not succeed')
 if req.get('id') is not None and res.get('id') is not None and req['id']!=res['id']:raise ValueError('RPC response id mismatch')
 return res['result']
def code_at(raw,addr,block):
 data=binary(rpc(raw,'eth_getCode',[addr,hex(block)]))
 if not data:raise ValueError('No deployed code at exact historical block')
 ops=[];i=0
 while i<len(data):
  op=data[i];ops.append(op);i+=1+(op-0x5f if 0x60<=op<=0x7f else 0)
 return hashlib.sha256(data).hexdigest(),ops
def transaction(raws,txhash,block,prefix='historical'):
 if not re.fullmatch(r'0x[0-9a-f]{64}',txhash or ''):raise ValueError('Exact transaction hash required')
 tx=rpc(raws[prefix+'_transaction'],'eth_getTransactionByHash',[txhash]);rc=rpc(raws[prefix+'_receipt'],'eth_getTransactionReceipt',[txhash])
 if tx.get('hash')!=txhash or rc.get('transactionHash')!=txhash or uint(tx.get('blockNumber'))!=block or uint(rc.get('blockNumber'))!=block:raise ValueError('Transaction/receipt identity or block mismatch')
 if tx.get('blockHash')!=rc.get('blockHash') or uint(tx.get('transactionIndex'))!=uint(rc.get('transactionIndex')):raise ValueError('Transaction canonical position mismatch')
 if uint(rc.get('status'))!=1:raise ValueError('Historical operation reverted or unknown')
 head=rpc(raws[prefix+'_header'],'eth_getBlockByNumber',[hex(block),False])
 if uint(head.get('number'))!=block or head.get('hash')!=tx['blockHash']:raise ValueError('Historical header identity mismatch')
 if tx.get('chainId') is not None and uint(tx['chainId'])!=1:raise ValueError('Historical transaction chain mismatch')
 return tx,rc
def effective_call(raws,operation,tx,addr,*,require_abi=True):
 loc=operation.get('exact_trace_locator')
 if loc in ('top',[]):frame=tx
 else:
  if not isinstance(loc,list) or not loc or any(type(i)is not int or i<0 for i in loc):raise ValueError('Exact call path or top locator required')
  raw=raws.get('historical_trace',{});params=raw.get('request',{}).get('params',[])
  if raw.get('request',{}).get('method')=='trace_transaction':
   rows=rpc(raw,'trace_transaction',[tx['hash']]);indexed={tuple(r['traceAddress']):r for r in rows}
   if len(indexed)!=len(rows):raise ValueError('Duplicate raw trace locator')
   for depth in range(len(loc)+1):
    row=indexed.get(tuple(loc[:depth]))
    if (not row or row.get('error') or not isinstance(row.get('result'),dict)
        or row.get('transactionHash',tx['hash'])!=tx['hash']
        or uint(row.get('blockNumber',tx['blockNumber']))!=uint(tx['blockNumber'])):
     raise ValueError('Successful exact entering call and its ancestors are required')
   root=indexed[()].get('action',{})
   if any(root.get(k)!=tx.get(k) for k in ('from','to','input')) or uint(root.get('value','0x0'))!=uint(tx.get('value','0x0')):raise ValueError('Trace root differs from transaction')
   selected=indexed[tuple(loc)];frame=dict(selected['action'],type=selected['action'].get('callType','call'))
  else:
   if len(params)!=2 or params[0]!=tx['hash'] or not isinstance(params[1],dict) or params[1].get('tracer')!='callTracer':raise ValueError('Exact historical call trace request required')
   tree=rpc(raw,'debug_traceTransaction',params)
   if any(tree.get(k)!=tx.get(k) for k in ('from','to','input')) or uint(tree.get('value','0x0'))!=uint(tx.get('value','0x0')):raise ValueError('Trace root differs from transaction')
   frame=tree
   for i in loc:
    if frame.get('error') or frame.get('revertReason'):raise ValueError('Historical ancestor reverted')
    children=frame.get('calls',[])
    if i>=len(children):raise ValueError('Exact historical call path absent')
    frame=children[i]
  if frame.get('error') or frame.get('revertReason') or frame.get('type','CALL').upper()!='CALL':raise ValueError('Historical call is not an effective CALL')
 if address(frame.get('to'))!=addr:raise ValueError('Historical call did not execute certified address')
 data=binary(frame.get('input','0x'))
 if require_abi and len(data)<4:raise ValueError('Documented ABI invocation required for operation resolution')
 if not require_abi and uint(frame.get('value','0x0'))<=0:raise ValueError('Current native entering event must have positive observed value')
 if operation.get('caller') is not None and address(frame.get('from'))!=address(operation['caller']):raise ValueError('Actual caller mismatch')
 if operation.get('input') is not None and binary(operation['input'])!=data:raise ValueError('Actual call input mismatch')
 return '0x'+data[:4].hex() if len(data)>=4 else None
def abi_signatures(body):
 try:
  values=json.loads(body)
  if isinstance(values,dict):values=values.get('abi',values.get('result',[]))
  if isinstance(values,str):values=json.loads(values)
  def typ(p):return '('+','.join(typ(c) for c in p['components'])+')'+p['type'][5:] if p['type'].startswith('tuple') else p['type']
  return {e['name']+'('+','.join(typ(p) for p in e['inputs'])+')' for e in values if isinstance(e,dict) and e.get('type')=='function'}
 except (ValueError,KeyError,TypeError):return set()
def official_authority(root,c,raws,selector,*,require_abi,creation_runtime_hash=None,require_direct_role_record=False):
 p=root/'private/stage1d_roles/AUTHORITIES.json'
 if not p.is_file():raise ValueError('Explicit official-source authority registry absent')
 registry=read(p)
 if registry.get('schema_version')!='stage1d-role-authorities-v1':raise ValueError('Unknown role authority registry')
 matches=[x for x in registry.get('authorities',[]) if x.get('protocol_id')==c.get('protocol_id')]
 if len(matches)!=1:raise ValueError('No unique approved official-source authority')
 a=matches[0];official=raws.get('verified_implementation' if creation_runtime_hash else 'official_deployment',{});abi=raws.get('documented_abi',{})
 if require_direct_role_record and a.get('decision_relevant_conflicts'):
  raise ValueError('Known decision-relevant official-role contradiction remains pending')
 sources=[(official,a.get('allowed_origins',[]))]
 if require_abi:sources.append((abi,a.get('allowed_abi_origins',a.get('allowed_origins',[]))))
 for item,origins in sources:
  u=urlsplit(item.get('source_url',''));origin=u.scheme+'://'+u.netloc.lower()
  if u.scheme!='https' or u.username or u.password or origin not in origins or not isinstance(item.get('body'),str):raise ValueError('Original source body and approved exact official HTTPS origin required')
  if u.hostname in ('github.com','raw.githubusercontent.com') and not any(item['source_url'].startswith(prefix) for prefix in a.get('allowed_source_url_prefixes',[])):
   raise ValueError('Shared source host needs exact official repository URL prefix')
 body=official['body'].lower()
 if creation_runtime_hash:
  artifact=json.loads(official['body']);runtime=artifact.get('deployedBytecode',artifact.get('runtimeBytecode'))
  if isinstance(runtime,dict):runtime=runtime.get('object')
  if not isinstance(runtime,str) or hashlib.sha256(binary(runtime)).hexdigest()!=creation_runtime_hash or artifact.get('contractName') not in a.get('contract_names',[]):
   raise ValueError('Verified creation route requires exact official implementation runtime artifact and registered role')
 elif a.get('deployment_json_pointer'):
  if c['address'] not in body or not a.get('ethereum_markers') or not any(s.lower() in body for s in a['ethereum_markers']):raise ValueError('Official body lacks exact Ethereum deployment')
  if not a.get('deployment_context_markers') or not all(s.lower() in body for s in a['deployment_context_markers']):raise ValueError('Official deployment role context missing')
  value=json.loads(official['body'])
  for token in a['deployment_json_pointer'].split('/')[1:]:
   token=token.replace('~1','/').replace('~0','~');value=value[int(token)] if isinstance(value,list) else value[token]
  if not isinstance(value,dict) or value.get('chain_id') not in (1,'1','eip155:1') or address(value.get('address'))!=c['address']:
   raise ValueError('Exact official deployment record does not bind Ethereum and address together')
  if require_direct_role_record and not all(s.lower() in json.dumps(value).lower() for s in a['deployment_context_markers']):
   raise ValueError('Exact deployment record must itself establish the technical role, not a trader label')
  if require_direct_role_record and type(value.get('chain_id')) not in (str,int):
   raise ValueError('Exact official chain integer/string required; Boolean is not Ethereum')
 else:
  if not a.get('deployment_context_markers') or not all(s.lower() in body for s in a['deployment_context_markers']):raise ValueError('Official deployment role context missing')
  patterns=a.get('deployment_record_patterns',[])
  records=[(m.groupdict(),m.group(0)) for pattern in patterns for m in re.finditer(pattern,official['body'],re.IGNORECASE)]
  matching=[text for r,text in records if r.get('address','').lower()==c['address'] and r.get('chain','').lower() in ('ethereum','eip155:1','1')]
  if not matching:
   raise ValueError('Official chain/address relationship needs an exact record selector, not page-wide co-occurrence')
  if require_direct_role_record and not any(all(s.lower() in text.lower() for s in a['deployment_context_markers']) for text in matching):
   raise ValueError('Exact deployment record must itself establish the technical role, not a trader label')
 if c['technical_role_status'] not in a.get('technical_role_statuses',[]):raise ValueError('Authority does not establish requested role')
 if not require_abi:
  return {'registry_sha256':sha(p),'official_source_url':official['source_url'],
          'identity_scope':'UNSUPPORTED_ADDRESS_ROLE_NOT_OPERATION_COMPONENT','abi_required':False}
 methods=[m for m in a.get('documented_methods',[]) if m.get('selector')==selector]
 if len(methods)!=1 or selector!=c.get('documented_selector'):raise ValueError('Actual selector is not documented ABI method')
 signature=methods[0].get('signature')
 if not signature or signature not in abi_signatures(abi['body']) and signature not in abi['body']:raise ValueError('Actual documented ABI signature absent from original source')
 return {'registry_sha256':sha(p),'official_source_url':official['source_url'],'actual_selector':selector,'documented_signature':signature}
def _official_scope_stop(root,c):
 """One exact official source establishes scope only, never historical execution."""
 if c.get('branch_action')!='UNSUPPORTED_PROTOCOL_STOP' or c.get('technical_role_status')!='VERIFIED_PROTOCOL_ROLE':
  raise ValueError('Direct official source route is only an unsupported scope stop; supported operation proof remains strict')
 if c.get('supplement_id')!=STOP_SUPPLEMENT:
  raise ValueError('Explicit stop-proportionality supplement required')
 if not isinstance(c.get('decision_relevant_conflicts'),list) or c['decision_relevant_conflicts']:
  raise ValueError('Known decision-relevant role contradiction remains pending')
 if not isinstance(c.get('evidence_applicability'),str) or not c['evidence_applicability'].strip():
  raise ValueError('Actual official-source applicability must be stated without historical operation claims')
 if any(c.get(k) is not None for k in ('code_sha256','documented_selector','start_block','end_block')):
  raise ValueError('Scope stop cannot claim historical code, ABI execution, or historical role block certification')
 if any(c.get(k) not in (None,False) for k in ('historical_operation_certification','historical_code_certification','operation_component_certification','zero_source_upper_bound_certification')):
  raise ValueError('Scope stop cannot certify historical operations, components or zero source bounds')
 from stage1d_closure_scope import active_batch
 current={q['query_id']:q for q in active_batch(root)['queries']}
 scopes=c.get('query_scopes')
 if not isinstance(scopes,list) or not scopes or len({s.get('query_id') for s in scopes})!=len(scopes):
  raise ValueError('Unique explicit current query scopes required')
 for scope in scopes:
  actual=current.get(scope.get('query_id'))
  if (not actual or set(scope)!={'query_id','scope_id','scope_hash','start_block','end_block'}
      or any(scope[k]!=actual[k] for k in scope) or type(scope['start_block'])is not int or type(scope['end_block'])is not int):
   raise ValueError('Official scope stop belongs to another frozen query scope')
 dependencies=c.get('dependencies')
 if not isinstance(dependencies,list) or len(dependencies)!=1 or dependencies[0].get('kind')!='deployment_binding':
  raise ValueError('Direct scope stop requires one deployment binding and its one sufficient original source')
 d=dependency(root,dependencies[0])
 if d.get('basis')!=STOP_BASIS or d.get('chain_id')!=c['chain_id'] or d.get('address')!=c['address']:
  raise ValueError('Official scope deployment chain/address/basis mismatch')
 if d.get('decision_relevant_conflicts'):
  raise ValueError('Known decision-relevant deployment contradiction remains pending')
 refs=d.get('source_refs')
 if not isinstance(refs,list) or len(refs)!=1 or refs[0].get('kind')!='official_deployment':
  raise ValueError('One sufficient original official deployment source required')
 raw=dependency(root,refs[0]);authority=official_authority(root,c,{'official_deployment':raw},None,
  require_abi=False,require_direct_role_record=True)
 c['verification']={'status':'PASS','method':STOP_BASIS,'scope':'CURRENT_QUERY_UNSUPPORTED_ADDRESS_SCOPE_BOUNDARY_ONLY',
  'historical_operation_certification':False,'historical_code_certification':False,'operation_component_certification':False,
  'custodial_service_certification':False,'zero_source_upper_bound_certification':False,
  'historical_each_entry_behavior':'UNKNOWN_NOT_CLAIMED','evidence_applicability':c['evidence_applicability'],
  'authority':authority,'raw_sources':{'official_deployment':{'path':refs[0]['path'],'sha256':refs[0]['sha256']}}}
 return c
def validate_certificate(work,certificate):
 root=Path(work).resolve();c=deepcopy(certificate)
 if c.get('schema_version')!=SCHEMA or c.get('chain_id')!='eip155:1':raise ValueError('Historical Ethereum role schema required')
 if c.get('address')!=address(c.get('address')):raise ValueError('Lowercase chain address required')
 if not isinstance(c.get('certificate_id'),str) or not c['certificate_id']:raise ValueError('Certificate identity required')
 if c.get('decision_basis')==STOP_BASIS:return _official_scope_stop(root,c)
 if c.get('technical_role_status') not in ('VERIFIED_PROTOCOL_ROLE','VERIFIED_SUPPORTED_COMPONENT_CONTRACT'):raise ValueError('Name alone is not a technical role')
 if type(c.get('start_block'))is not int or type(c.get('end_block'))is not int or c['start_block']<0 or c['start_block']!=c['end_block']:raise ValueError('Only exact single-block certificate supported; endpoints do not prove interval')
 if c.get('branch_action') not in ('UNSUPPORTED_PROTOCOL_STOP','SUPPORTED_OPERATION_RESOLVE'):raise ValueError('Technical role is separate from custodial service')
 evidence={};raws={};refs={}
 for d in c.get('dependencies',[]):
  if d.get('kind') in evidence:raise ValueError('Duplicate role evidence kind')
  row=dependency(root,d);evidence[d['kind']]=row
  if row.get('chain_id')!=c['chain_id'] or row.get('address')!=c['address']:raise ValueError('Role evidence chain/address mismatch')
  if not row.get('source_refs'):raise ValueError('Original raw source references required')
  for ref in row['source_refs']:
   value=dependency(root,ref);kind=ref.get('kind')
   if not isinstance(kind,str) or not kind:raise ValueError('Raw source kind required')
   if kind in refs and refs[kind]['sha256']!=ref['sha256']:raise ValueError('Conflicting raw source identity')
   raws[kind]=value;refs[kind]=ref
 if set(evidence)!={'deployment_binding','historical_execution_binding','code_identity_binding'}:raise ValueError('Deployment, code and historical execution bindings required')
 deployment=evidence['deployment_binding'];operation=evidence['historical_execution_binding'];code=evidence['code_identity_binding'];block=c['start_block']
 if not {'historical_transaction','historical_receipt','historical_header','historical_code'}<=raws.keys():raise ValueError('Original historical RPC transaction/receipt/header/code sources required')
 if operation.get('block')!=block:raise ValueError('Operation must bind exact arrival block')
 tx,rc=transaction(raws,operation.get('tx_hash'),block)
 require_abi=c['branch_action']=='SUPPORTED_OPERATION_RESOLVE'
 selector=effective_call(raws,operation,tx,c['address'],require_abi=require_abi)
 digest,ops=code_at(raws['historical_code'],c['address'],block)
 if digest!=code.get('code_sha256') or digest!=c.get('code_sha256'):raise ValueError('Original historical bytecode hash mismatch')
 proxy=0xf4 in ops or 0xf2 in ops or code.get('is_proxy') is True
 if proxy:
  impl=address(code.get('implementation_address'));slot=binary(rpc(raws['proxy_storage'],'eth_getStorageAt',[c['address'],IMPLEMENTATION_SLOT,hex(block)]))
  if len(slot)!=32 or any(slot[:12]) or '0x'+slot[-20:].hex()!=impl:raise ValueError('Exact historical proxy implementation storage mismatch')
  impl_digest,_=code_at(raws['implementation_code'],impl,block)
  if impl_digest!=code.get('implementation_code_sha256'):raise ValueError('Historical implementation code hash mismatch')
 elif code.get('implementation_address') or code.get('implementation_code_sha256'):raise ValueError('Unbound nonproxy implementation fields')
 basis=deployment.get('basis')
 if basis=='VERIFIED_CONTRACT_CREATION_AND_IMPLEMENTATION':
  cb=deployment.get('creation_block')
  if type(cb)is not int or cb>block:raise ValueError('Role predates verified creation')
  ct,cr=transaction(raws,deployment.get('creation_tx_hash'),cb,'creation')
  if ct.get('to') is not None or address(cr.get('contractAddress'))!=c['address'] or not binary(ct.get('input','0x')):raise ValueError('Direct creation does not bind deployment; factory CREATE requires separate proof')
 elif basis not in ('OFFICIAL_CHAIN_DEPLOYMENT_AND_HISTORICAL_BINDING','OFFICIAL_DEPLOYMENT_AND_VERIFIED_CREATION'):raise ValueError('Unsupported deployment evidence route')
 auth=official_authority(root,c,raws,selector,require_abi=require_abi,
    creation_runtime_hash=(code['implementation_code_sha256'] if proxy else digest) if basis=='VERIFIED_CONTRACT_CREATION_AND_IMPLEMENTATION' else None)
 c['verification']={'status':'PASS','method':'RAW_DEPLOYMENT_RPC_SINGLE_BLOCK_ROLE_REVALIDATION_V1','scope':'EXACT_ARRIVAL_BLOCK_ONLY','operation_component_certification':False,
  'block':block,'block_hash':tx['blockHash'],'tx_hash':tx['hash'],'code_sha256':digest,'proxy_implementation_required':proxy,'authority':auth,
  'raw_sources':{k:{'path':v['path'],'sha256':v['sha256']} for k,v in sorted(refs.items())}}
 return c
class TechnicalRoles:
 def __init__(self,work):
  self.work=Path(work);self.records=[];p=self.work/'private/stage1d_roles/CURRENT.json'
  if p.exists():
   for ref in read(p).get('certificates',[]):self.records.append(validate_certificate(self.work,dependency(self.work.resolve(),ref)))
 def resolve(self,state,identity):
  result=ordinary(identity)
  def applies(c):
   if c['address']!=state.address or c['chain_id']!=state.arrival.chain_id:return False
   if c.get('decision_basis')==STOP_BASIS:
    return any(s['query_id']==getattr(state,'query_id',None) and s['start_block']<=state.arrival.block<=s['end_block'] for s in c['query_scopes'])
   return c['start_block']<=state.arrival.block<=c['end_block']
  matches=[c for c in self.records if applies(c)]
  if not matches or result['kind']=='SERVICE':return result
  if len({(r['technical_role_status'],r['branch_action'],r.get('protocol_id')) for r in matches})!=1 or len({r['code_sha256'] for r in matches if r.get('code_sha256')})>1:
   return {**result,'technical_role_status':'ROLE_CONFLICT_NEEDS_REVIEW','branch_action':'ROLE_CONFLICT_NEEDS_REVIEW'}
  direct=[c for c in matches if c.get('decision_basis')==STOP_BASIS]
  if direct:
   return {**result,'kind':'UNSUPPORTED_PROTOCOL','actor':None,'custodial_actor_status':'NOT_ESTABLISHED',
    'technical_role_status':'OFFICIAL_SOURCE_SUPPORTED_SCOPE_BOUNDARY','branch_action':'UNSUPPORTED_PROTOCOL_STOP',
    'role_certificate_ids':[m['certificate_id'] for m in matches],'stop_decision_basis':STOP_BASIS,
    'historical_operation_certification':False,'historical_code_certification':False,
    'scope_boundary_evidence_applicability':[c['evidence_applicability'] for c in direct]}
  c=matches[0]
  return {**result,'kind':'SUPPORTED_PROTOCOL' if c['branch_action']=='SUPPORTED_OPERATION_RESOLVE' else 'UNSUPPORTED_PROTOCOL',
   'technical_role_status':c['technical_role_status'],'branch_action':c['branch_action'],'role_certificate_ids':[m['certificate_id'] for m in matches],
   'historical_role_blocks':[c['start_block'],c['end_block']],'actor':None}
