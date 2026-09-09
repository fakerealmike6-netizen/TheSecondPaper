"""Exact historical WETH balance/logs and proxy-slot reads on the existing pool.

No arbitrary contract call, latest state, node mutation, or trace entitlement.
The root supplies the finite frozen selector list derived from current needs.
"""
from copy import deepcopy
import hashlib,json,re
from pathlib import Path
from stage1d_runtime import Runtime

WETH='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
TRANSFER='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
DEPOSIT='0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
WITHDRAWAL='0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65'
IMPLEMENTATION_SLOT='0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc'
METHOD_RATES={'eth_call':26,'eth_getLogs':60,'eth_getStorageAt':20}
def key(x):return json.dumps(x,sort_keys=True,separators=(',',':'))
def address(a):
 if not isinstance(a,str) or not re.fullmatch('0x[0-9a-f]{40}',a):raise ValueError('Exact lowercase address required')
 return a
def block(b):
 if type(b) is not int or b<0:raise ValueError('Exact historical block required')
 return hex(b)
def quantity(v):return isinstance(v,str) and re.fullmatch('0x(?:0|[1-9a-f][0-9a-f]*)',v) is not None
def topic_address(a):return '0x'+'0'*24+address(a)[2:]
def balance_plan(holder,at_block):
 return {'method':'eth_call','params':[{'to':WETH,'data':'0x70a08231'+'0'*24+address(holder)[2:]},block(at_block)]}
def implementation_plan(contract,at_block):
 return {'method':'eth_getStorageAt','params':[address(contract),IMPLEMENTATION_SLOT,block(at_block)]}
def logs_plan(holder,start_block,end_block,kind):
 topics={'TRANSFER_OUT':[TRANSFER,topic_address(holder)],'TRANSFER_IN':[TRANSFER,None,topic_address(holder)],
         'DEPOSIT':[DEPOSIT,topic_address(holder)],'WITHDRAWAL':[WITHDRAWAL,topic_address(holder)]}
 if kind not in topics or start_block>end_block:raise ValueError('Known holder log kind and ordered bounds required')
 return {'method':'eth_getLogs','params':[{'address':WETH,'fromBlock':block(start_block),'toBlock':block(end_block),'topics':topics[kind]}]}
def validate(plan):
 if not isinstance(plan,dict) or set(plan)!={'method','params'}:raise ValueError('Exact method/params')
 m,p=plan['method'],plan['params']
 if m=='eth_call':
  if not isinstance(p,list) or len(p)!=2 or not isinstance(p[0],dict) or set(p[0])!={'to','data'} or p[0]['to']!=WETH or not re.fullmatch('0x70a08231'+'0'*24+'[0-9a-f]{40}',p[0].get('data','')) or not quantity(p[1]):raise ValueError('Only historical canonical WETH balanceOf permitted')
 elif m=='eth_getStorageAt':
  if not isinstance(p,list) or len(p)!=3 or p[1]!=IMPLEMENTATION_SLOT or not quantity(p[2]):raise ValueError('Only exact historical ERC1967 implementation slot permitted')
  address(p[0])
 elif m=='eth_getLogs':
  if not isinstance(p,list) or len(p)!=1 or set(p[0])!={'address','fromBlock','toBlock','topics'}:raise ValueError('Finite holder logs only')
  f=p[0];t=f['topics']
  if f['address']!=WETH or not quantity(f['fromBlock']) or not quantity(f['toBlock']) or int(f['fromBlock'],16)>int(f['toBlock'],16):raise ValueError('Exact canonical WETH historical log bounds')
  if not isinstance(t,list) or len(t) not in (2,3) or t[0] not in (TRANSFER,DEPOSIT,WITHDRAWAL):raise ValueError('Known WETH event topic required')
  if len(t)==3 and (t[0]!=TRANSFER or t[1] is not None):raise ValueError('Incoming transfer holder topic required')
  if not isinstance(t[-1],str) or not re.fullmatch('0x'+'0'*24+'[0-9a-f]{40}',t[-1]):raise ValueError('Exact holder filter required')
 else:raise ValueError('Not a finite-state method')
 return deepcopy(plan)
def response_status(request,response):
 try:
  plan=validate({k:request[k] for k in ('method','params')})
  if response.get('error') or response.get('jsonrpc')!='2.0' or response.get('id')!=request.get('id'):return 'INVALID_RPC_BINDING'
  r=response['result']
  if plan['method'] in ('eth_call','eth_getStorageAt'):
   return 'SUCCESS_VALIDATED' if isinstance(r,str) and re.fullmatch('0x[0-9a-fA-F]{64}',r) else 'INVALID_RPC_BINDING'
  if not isinstance(r,list):return 'INVALID_RPC_BINDING'
  f=plan['params'][0];seen=set()
  for log in r:
   if log.get('address','').lower()!=WETH or log.get('removed') is not False or not quantity(log.get('blockNumber')) or not int(f['fromBlock'],16)<=int(log['blockNumber'],16)<=int(f['toBlock'],16):return 'INVALID_RPC_BINDING'
   for name in ('blockHash','transactionHash'):
    if not isinstance(log.get(name),str) or not re.fullmatch('0x[0-9a-fA-F]{64}',log[name]):return 'INVALID_RPC_BINDING'
   if not quantity(log.get('transactionIndex')) or not quantity(log.get('logIndex')) or not isinstance(log.get('data'),str) or not re.fullmatch('0x[0-9a-fA-F]{64}',log['data']):return 'INVALID_RPC_BINDING'
   topics=log.get('topics',[])
   if any(t is not None and (i>=len(topics) or topics[i].lower()!=t) for i,t in enumerate(f['topics'])):return 'INVALID_RPC_BINDING'
   expected=3 if topics[0].lower()==TRANSFER else 2
   if len(topics)!=expected or any(not re.fullmatch('0x'+'0'*24+'[0-9a-fA-F]{40}',t) for t in topics[1:]):return 'INVALID_RPC_BINDING'
   ident=(log['blockHash'].lower(),log['transactionHash'].lower(),int(log['logIndex'],16))
   if ident in seen:return 'INVALID_RPC_BINDING'
   seen.add(ident)
  return 'SUCCESS_VALIDATED'
 except (ValueError,TypeError,KeyError,IndexError):return 'INVALID_RPC_BINDING'
class FiniteStateRuntime(Runtime):
 def __init__(self,allowed_plans,**kwargs):
  super().__init__(**kwargs);self.allowed={key(validate(p)) for p in allowed_plans}
 def validate_rpc(self,plan):
  if plan.get('method') not in METHOD_RATES:return super().validate_rpc(plan)
  p=validate(plan)
  if key(p) not in self.allowed:raise ValueError('State selector absent from frozen current requirements')
  return p
 def rpc_identity(self,provider,plan):
  if plan.get('method') in METHOD_RATES:return {'provider':provider,'chain':1,**self.validate_rpc(plan)}
  return super().rpc_identity(provider,plan)
 def rpc_result_status(self,request,response):
  if request.get('method') in METHOD_RATES:return response_status(request,response)
  return super().rpc_result_status(request,response)
 def rpc_permission(self,permission):
  result=deepcopy(permission);result['method_cu_upper_bounds'].update(METHOD_RATES)
  result['finite_state_rate_source']='https://www.alchemy.com/docs/reference/compute-unit-costs'
  result['budget_grant_changed']=False
  return result
