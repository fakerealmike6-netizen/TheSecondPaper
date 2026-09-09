"""Explicit user scope decisions, separate from chain-verified role certificates."""
from pathlib import Path
import json,hashlib,re
from stage1d_closure_scope import active_batch
SCHEMA='stage1d-user-task-boundary-v1'
HOLD='USER_REQUESTED_BRANCH_HOLD'
class TaskBoundaries:
 def __init__(self,work):
  self.work=Path(work).resolve();self.records=[];self.identity=None
  path=self.work/'private/stage1d_roles/USER_TASK_BOUNDARIES.json'
  if not path.exists():return
  data=path.read_bytes();document=json.loads(data)
  if document.get('schema_version')!=SCHEMA or document.get('authorization_id')!='STAGE1D_WINDOW_ROLE_SEMANTIC_CLOSURE_V1':raise ValueError('Current explicit task authority required')
  if document.get('chain_verified_role_claim') is not False or document.get('raw_evidence_deleted') is not False:raise ValueError('Task boundary cannot masquerade as a chain certificate')
  queries={q['name']:q for q in active_batch(self.work)['queries']}
  for record in document['boundaries']:
   if record.get('chain_id')!='eip155:1' or not re.fullmatch('0x[0-9a-f]{40}',record.get('address','')) or record.get('branch_action') not in ('UNSUPPORTED_PROTOCOL_STOP',HOLD):raise ValueError('Explicit chain/address task boundary required')
   if not record.get('user_message') or record.get('user_message_sha256')!=hashlib.sha256(record['user_message'].encode()).hexdigest():raise ValueError('Task decision source changed')
   if record['branch_action']==HOLD:
    if (record.get('reason')!='USER_DIRECTION' or record.get('temporary') is not True
        or record.get('resume_condition')!='EXPLICIT_USER_RESUME' or not record.get('authority_id')
        or record.get('chain_verified_role_claim') is not False or record.get('source_zero_claim') is not False
        or record.get('identity_change_authorized') is not False):
     raise ValueError('Temporary user hold needs explicit authority and cannot claim chain identity or source zero')
    if any(k in record for k in ('kind','actor','technical_role_status','identity_override','source_zero')):
     raise ValueError('User hold cannot reclassify an identity or source amount')
   if not record.get('queries'):raise ValueError('Current query applicability required')
   for q in record['queries']:
    if q['name'] not in queries or any(q[k]!=queries[q['name']][k] for k in ('query_id','scope_id','scope_hash','start_block','end_block')):raise ValueError('Task boundary scope changed')
   self.records.append(record)
  self.identity=hashlib.sha256(data).hexdigest()
 def resolve(self,state,identity,query_id=None):
  query_id=state.query_id if query_id is None else query_id
  matches=[r for r in self.records if r['address']==state.address and r['chain_id']==state.arrival.chain_id and
    any((query_id is None or query_id==q['query_id']) and q['start_block']<=state.arrival.block<=q['end_block'] for q in r['queries'])]
  if not matches:return identity
  protocols=[r for r in matches if r['branch_action']=='UNSUPPORTED_PROTOCOL_STOP']
  holds=[r for r in matches if r['branch_action']==HOLD]
  resolved=identity
  if protocols:
   resolved={**identity,'kind':'UNSUPPORTED_PROTOCOL','actor':None,'custodial_actor_status':'NOT_ESTABLISHED',
    'technical_role_status':'USER_DECLARED_TASK_BOUNDARY','branch_action':'UNSUPPORTED_PROTOCOL_STOP',
    'task_boundary_ids':sorted(r['boundary_id'] for r in protocols),'task_boundary_source_sha256':self.identity,
    'chain_verified_role_claim':False,'reference_version_effect':'NEW_TASK_REFERENCE_B_WITH_FIXED_PRIOR_DENOMINATORS'}
  if holds:
   # A collection action is not a technical/custodial identity or an amount rule.
   resolved={**resolved,'branch_action':HOLD,
    'task_boundary_ids':sorted(set(resolved.get('task_boundary_ids',[]))|{r['boundary_id'] for r in holds}),
    'task_boundary_authority_ids':sorted({r['authority_id'] for r in holds}),
    'task_boundary_source_sha256':self.identity,'branch_hold_reason':'USER_DIRECTION',
    'branch_hold_temporary':True,'branch_hold_resume_condition':'EXPLICIT_USER_RESUME',
    'branch_hold_chain_verified_role_claim':False,'branch_hold_source_zero_claim':False,
    'branch_hold_identity_reclassification':False}
  return resolved
