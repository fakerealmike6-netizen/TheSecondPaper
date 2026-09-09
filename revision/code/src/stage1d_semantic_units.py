"""Finite canonical WETH evidence -> isolated ports -> bounded collector arrivals.

No network, solver, role inference or generic protocol parser. Original value
events stay physical facts. A Deposit credit is an explicit semantic port, never
a fabricated ERC20 Transfer. Every validation recomputes from bound materials.
"""
from dataclasses import asdict, is_dataclass
import copy, hashlib, json, re
from pathlib import Path

from weth_component import WETH, DEPOSIT_TOPIC, code_hash, frames
from weth_evidence import (audit_bindings, is_verified_context, payload_hash,
    load_portable_evidence_context, PORTABLE_SCHEMA)

SCHEMA = 'stage1d-weth-instance-v1'
SEMANTIC_VERSION = 'STAGE1D_CANONICAL_WETH_1TO1_V1'
DEPOSIT_TOPIC_PROOF_VERSION = 'STAGE1D_CANONICAL_DEPOSIT_TOPIC_EMITTER_V1'
NATIVE = 'native:eip155:1'
TOKEN = 'erc20:eip155:1:' + WETH
WITHDRAWAL_TOPIC = '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65'
TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
SYNTHETIC_SCHEMA = 'stage1d-controlled-weth-evidence-v1'
REQUIRED_CAPABILITIES = ('instance_evidence','collector_assets','multiasset_context',
    'shared_source_constraints','seven_methods','output_acceptance')
REQUIRED_GATE_SOURCES = (
    'collector.py','stage1d_semantic_units.py','weth_evidence.py','weth_component.py',
    'weth_trace_adapter_r4.py','weth_source_adapter_r3.py','stage1d_context.py',
    'stage1d_multiasset_context.py','context_lp_r3.py','lp_model.py',
    'stage1c_baselines.py','stage1c_intervals.py','stage1c_output_contract.py','run_stage1c.py',
    'stage1c_zero_derivation.py','stage1d_acquisition.py','stage1d_semantic_catalogue.py',
    'stage1d_bq_portable.py','stage1d_batch_binding_route.py','stage1d_bq_root_binding.py',
    'stage1d_rpc.py','stage1d_finite_state_route.py','stage1d_finite_state_rpc.py',
    'stage1d_weth_log_index.py','stage1d_timestamp_bracket.py','stage1d_closure_context.py',
    'stage1d_shared_evidence.py')

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()

def integer(value):
    if isinstance(value,bool) or not isinstance(value,(int,str)) or (isinstance(value,str) and not re.fullmatch(r'(?:0x[0-9a-fA-F]+|[0-9]+)',value)):
        raise ValueError('Exact nonnegative integer identity or amount required')
    value=int(value,16) if isinstance(value,str) and value.startswith('0x') else int(value)
    if value<0:raise ValueError('Negative integer evidence')
    return value

def address(value):
    if not isinstance(value,str) or not re.fullmatch(r'0x[0-9a-f]{40}',value):raise ValueError('Canonical lower-case address required')
    return value

def txhash(value):
    if not isinstance(value,str) or not re.fullmatch(r'0x[0-9a-f]{64}',value):raise ValueError('Exact lower-case transaction/block hash required')
    return value

def event_id(tx,path):
    return 'eip155:1:tx:'+tx+(':top' if not path else ':trace:'+'_'.join(map(str,path)))

def context_identity(context):
    if is_verified_context(context):return 'wethctx:'+digest({'manifest':context.manifest_sha256,'catalogue':context.catalogue_sha256,'records':context.records,'review':context.source_review})
    if isinstance(context,dict) and context.get('schema_version')==PORTABLE_SCHEMA:
        return context_identity(load_portable_evidence_context(context))
    if isinstance(context,dict) and context.get('schema_version')==SYNTHETIC_SCHEMA and context.get('evidence_kind')=='SYNTHETIC_CONTROLLED':
        return 'wethctx:'+digest(context)
    raise ValueError('Unrecognized semantic evidence context')

def _materials(context):
    if isinstance(context,dict) and context.get('schema_version')==PORTABLE_SCHEMA:context=load_portable_evidence_context(context)
    if is_verified_context(context):
        return {k:v.get('payload') for k,v in context.records.items()},context,'REAL_CHAIN'
    if not isinstance(context,dict) or context.get('schema_version')!=SYNTHETIC_SCHEMA or context.get('evidence_kind')!='SYNTHETIC_CONTROLLED':
        raise ValueError('A caller assertion cannot authenticate real semantic evidence')
    data=copy.deepcopy(context.get('payloads',{})); records={}
    policy=context.get('binding_policy',{})
    for role,method,params in (
        ('transaction','eth_getTransactionByHash',[policy.get('tx_hash')]),
        ('receipt','eth_getTransactionReceipt',[policy.get('tx_hash')]),
        ('trace','debug_traceTransaction',[policy.get('tx_hash'),{'tracer':'callTracer','tracerConfig':{'withLog':True}}]),
        ('historical_code','eth_getCode',[WETH,hex(integer(policy.get('block_number')))])):
        if role in data:records[role]={'chain_id':1,'payload_sha256':payload_hash(data[role]),'request':{'method':method,'params':params}}
    return data,{'kind':'SYNTHETIC','records':records},'SYNTHETIC_CONTROLLED'

def _source_text(context,data):
    if is_verified_context(context):
        source=context.records.get('source',{}).get('payload',{})
        if 'sources' in source:return source.get('sources',{}).get('WETH9.sol',{}).get('content','')
        rows=source.get('result',[]);return rows[0].get('SourceCode','') if len(rows)==1 else ''
    return data.get('source_text','')

def _body_support(text,kind):
    stripped=re.sub(r'/\*.*?\*/|//[^\n]*|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',lambda m:'' if m.group(0).startswith(('/*','//')) else '__STRING_LITERAL__',text,flags=re.S)
    compact=re.sub(r'\s+','',stripped)
    body='functiondeposit()publicpayable{balanceOf[msg.sender]+=msg.value;Deposit(msg.sender,msg.value);}' if kind=='DEPOSIT' else 'functionwithdraw(uintwad)public{require(balanceOf[msg.sender]>=wad);balanceOf[msg.sender]-=wad;msg.sender.transfer(wad);Withdrawal(msg.sender,wad);}'
    return compact.count(body)==1

def _check_identity(policy,data):
    required=('chain_id','tx_hash','block_number','block_hash','tx_index','timestamp','call_path','kind','log_index','holder','amount_raw')
    if any(k not in policy for k in required):raise ValueError('Incomplete exact component policy')
    if policy['chain_id']!=1 or type(policy['chain_id']) is not int or policy['kind'] not in ('DEPOSIT','WITHDRAWAL'):
        raise ValueError('Only exact mainnet canonical deposit/withdraw supported')
    txhash(policy['tx_hash']);txhash(policy['block_hash']);address(policy['holder'])
    if policy.get('contract',WETH)!=WETH:raise ValueError('Noncanonical contract')
    for k in ('block_number','tx_index','timestamp','log_index'):
        if type(policy[k]) is not int or policy[k]<0:raise ValueError('Exact integer position required')
    path=policy['call_path']
    if not isinstance(path,list) or any(type(x) is not int or x<0 for x in path):raise ValueError('Exact call path required')
    if integer(policy['amount_raw'])<=0:raise ValueError('Positive component amount required')
    tx,rc,header=data['transaction'],data['receipt'],data['header']
    if tx.get('hash')!=policy['tx_hash'] or rc.get('transactionHash')!=policy['tx_hash'] or integer(tx.get('chainId'))!=1 or integer(rc.get('status'))!=1:
        raise ValueError('Successful mainnet transaction/receipt required')
    for obj in (tx,rc):
        if obj.get('blockHash')!=policy['block_hash'] or integer(obj.get('blockNumber'))!=policy['block_number'] or integer(obj.get('transactionIndex'))!=policy['tx_index']:
            raise ValueError('Transaction/receipt fixed block/position differs')
    if header.get('hash')!=policy['block_hash'] or integer(header.get('number'))!=policy['block_number'] or integer(header.get('timestamp'))!=policy['timestamp']:
        raise ValueError('Exact historical header/time binding differs')
    logs=rc.get('logs')
    if not isinstance(logs,list):raise ValueError('Complete receipt logs required')
    indices=[]
    for log in logs:
        idx=integer(log.get('logIndex'));indices.append(idx)
        if log.get('transactionHash')!=policy['tx_hash'] or log.get('blockHash')!=policy['block_hash'] or integer(log.get('blockNumber'))!=policy['block_number'] or integer(log.get('transactionIndex'))!=policy['tx_index'] or log.get('removed') is not False:
            raise ValueError('Receipt log identity/removal conflict')
    if indices!=sorted(set(indices)):raise ValueError('Duplicate/unordered exact log positions')

def _deposit_topic_emitter_proof(policy,data,context):
    """Narrow topic-specific proof; never fabricate frame logs or certify a router.

    Existing source/runtime and full-tree verification precedes this helper.
    Static execution cannot LOG. The only additional nonstatic WETH operation
    accepted here is exact canonical transferFrom, whose complete body emits
    Transfer alone. Unknown selectors may reach payable fallback, so stay open.
    """
    if policy['kind']!='DEPOSIT':raise ValueError('Deposit topic proof cannot certify withdrawal')
    text=_source_text(context,data)
    stripped=re.sub(r'/\*.*?\*/|//[^\n]*|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',lambda m:'' if m.group(0).startswith(('/*','//')) else '__STRING_LITERAL__',text,flags=re.S)
    compact=re.sub(r'\s+','',stripped)
    deposit='functiondeposit()publicpayable{balanceOf[msg.sender]+=msg.value;Deposit(msg.sender,msg.value);}'
    fallback='function()publicpayable{deposit();}'
    transfer='functiontransferFrom(addresssrc,addressdst,uintwad)publicreturns(bool){require(balanceOf[src]>=wad);if(src!=msg.sender&&allowance[src][msg.sender]!=uint(-1)){require(allowance[src][msg.sender]>=wad);allowance[src][msg.sender]-=wad;}balanceOf[src]-=wad;balanceOf[dst]+=wad;Transfer(src,dst,wad);returntrue;}'
    if (compact.count('contractWETH9{')!=1 or len(re.findall(r'\bcontract\b',stripped))!=1
            or compact.count(deposit)!=1 or compact.count(fallback)!=1):
        raise ValueError('Exact canonical deposit/fallback source body required for topic proof')
    indexed=dict(frames(data['trace']));contexts={};statics={};candidates=[];excluded=[]
    for path,frame in sorted(indexed.items(),key=lambda item:(len(item[0]),item[0])):
        typ=frame.get('type','').upper();parent=path[:-1]
        if typ not in ('CALL','DELEGATECALL','STATICCALL') or frame.get('error') or frame.get('revertReason'):
            raise ValueError('Topic proof needs successful unambiguous CALL/DELEGATECALL/STATICCALL contexts')
        if path and frame.get('from')!=contexts.get(parent):raise ValueError('Topic proof caller differs from parent storage context')
        if not path and typ!='CALL':raise ValueError('Topic proof requires canonical CALL root')
        if frame.get('to')==WETH and typ=='DELEGATECALL':raise ValueError('Delegated WETH code cannot establish canonical storage emitter')
        ctx=frame.get('to') if typ in ('CALL','STATICCALL') else contexts.get(parent)
        address(ctx);contexts[path]=ctx;static=(typ=='STATICCALL' or statics.get(parent,False)) if path else False;statics[path]=static
        if static:
            if integer(frame.get('value','0x0'))!=0:raise ValueError('Successful static context cannot transfer native value')
            excluded.append({'path':list(path),'reason':'STATIC_EXECUTION_FORBIDS_LOG_EIP214','inherited_static':typ!='STATICCALL'})
            continue
        if ctx!=WETH:
            excluded.append({'path':list(path),'reason':'NON_CANONICAL_STORAGE_ADDRESS_CANNOT_EMIT_WETH_LOG','storage_address':ctx})
            continue
        if frame.get('to')!=WETH or typ!='CALL' or frame.get('calls'):
            raise ValueError('Canonical storage requires direct leaf operation; code/storage ambiguity remains open')
        calldata=frame.get('input','').lower()
        if calldata in ('0x','0xd0e30db0'):
            candidates.append({'path':list(path),'holder':frame.get('from'),'amount_raw':str(integer(frame.get('value','0x0'))),
                               'reason':'EXACT_CANONICAL_DEPOSIT_OR_FALLBACK_BODY'})
        elif calldata.startswith('0x23b872dd'):
            if (not re.fullmatch('0x23b872dd[0-9a-f]{192}',calldata) or calldata[10:34]!='0'*24
                    or calldata[74:98]!='0'*24 or integer(frame.get('value','0x0'))!=0 or compact.count(transfer)!=1):
                raise ValueError('Exact full canonical transferFrom source/calldata required to exclude Deposit')
            excluded.append({'path':list(path),'reason':'EXACT_CANONICAL_TRANSFER_FROM_BODY_EMITS_TRANSFER_ONLY',
                'selector':'0x23b872dd','source_body_sha256':hashlib.sha256(transfer.encode()).hexdigest(),
                'src':'0x'+calldata[34:74],'dst':'0x'+calldata[98:138],'amount_raw':str(int(calldata[138:202],16))})
        else:raise ValueError('Unknown canonical selector may reach fallback; Deposit emitter remains open')
    expected={'path':policy['call_path'],'holder':policy['holder'],'amount_raw':str(integer(policy['amount_raw']))}
    if len(candidates)!=1 or any(candidates[0][k]!=v for k,v in expected.items()):
        raise ValueError('Deposit emitting successful CALL is absent, nonunique or differs from exact holder/value')
    logs=[x for x in data['receipt']['logs'] if x.get('address')==WETH and isinstance(x.get('topics'),list) and x['topics'] and x['topics'][0]==DEPOSIT_TOPIC]
    if len(logs)!=1 or integer(logs[0]['logIndex'])!=policy['log_index'] or logs[0].get('topics')!=[DEPOSIT_TOPIC,'0x'+'0'*24+policy['holder'][2:]] or integer(logs[0]['data'])!=integer(policy['amount_raw']):
        raise ValueError('Canonical Deposit topic/holder/value log is not unique')
    return {'basis':'COMPLETE_BOUND_TREE_UNIQUE_CANONICAL_DEPOSIT_TOPIC_EMITTER_V1','proof_version':DEPOSIT_TOPIC_PROOF_VERSION,'actual_call_tree_path':policy['call_path'],
        'log_index':policy['log_index'],'frame_count':len(indexed),'receipt_log_count':len(data['receipt']['logs']),
        'canonical_log_count':sum(x.get('address')==WETH for x in data['receipt']['logs']),'deposit_topic_log_count':1,
        'kind':'DEPOSIT','source_sha256':hashlib.sha256(text.encode()).hexdigest(),'candidates':candidates,'excluded_execution_contexts':excluded,
        'frame_logs_fabricated':False,'normal_receipt_logs_retained':True,'withdrawal_proof_reused':False,
        'scope':'ONE_CANONICAL_DEPOSIT_ONLY_NOT_SURROUNDING_PROTOCOL'}

def _frame_and_log(policy,data,binding,context):
    tree=data['trace'];tx=data['transaction'];rc=data['receipt'];path=tuple(policy['call_path']);amount=integer(policy['amount_raw'])
    indexed=dict(frames(tree));frame=indexed.get(path)
    if not frame:raise ValueError('Exact component call frame absent')
    if tree.get('from')!=tx.get('from') or tree.get('to')!=tx.get('to') or integer(tree.get('value','0x0'))!=integer(tx.get('value','0x0')) or tree.get('input','0x').lower()!=tx.get('input','0x').lower():
        raise ValueError('Full call tree root differs from transaction')
    for p in (path[:i] for i in range(len(path)+1)):
        if p not in indexed or indexed[p].get('error') or indexed[p].get('revertReason'):raise ValueError('Missing/reverted component ancestor')
    if frame.get('type','').upper()!='CALL' or frame.get('from')!=policy['holder'] or frame.get('to')!=WETH:
        raise ValueError('Exact CALL caller/contract differs')
    topic=DEPOSIT_TOPIC if policy['kind']=='DEPOSIT' else WITHDRAWAL_TOPIC
    matching=[x for x in rc['logs'] if integer(x['logIndex'])==policy['log_index']]
    if len(matching)!=1:raise ValueError('Exact component log absent')
    log=matching[0]
    if log.get('address')!=WETH or log.get('topics')!=[topic,'0x'+'0'*24+policy['holder'][2:]] or integer(log['data'])!=amount:
        raise ValueError('Exact component topic/holder/amount differs')
    flogs=[x for x in frame.get('logs',[]) if x.get('address')==WETH and x.get('topics')==log['topics'] and x.get('data','').lower()==log['data'].lower()]
    direct=len(flogs)==1
    if direct:
        loc=[flogs[0][k] for k in ('logIndex','log_index') if flogs[0].get(k) is not None]
        direct=all(integer(x)==policy['log_index'] for x in loc) if loc else len([x for x in rc['logs'] if x.get('address')==WETH and x.get('topics')==log['topics'] and x.get('data','').lower()==log['data'].lower()])==1
    # Indexed complete-tree proof is recomputed by its sealed adapter; an
    # arbitrary serialized proof_valid field is never sufficient.
    if not binding.get('trace_bound'):raise ValueError('Call trace has no request binding')
    equivalent=binding.get('equivalent_deposit_binding') if policy['kind']=='DEPOSIT' else None
    proof={'basis':'EXACT_REQUEST_BOUND_FRAME_LOG','actual_call_tree_path':policy['call_path'],'log_index':policy['log_index']}
    if not direct:
        # A separately validated complete tree plus unique canonical execution
        # context proves the one canonical receipt log came from this operation.
        # Recompute this for withdrawal too; never inherit Deposit semantics.
        try:
            contexts={};code_paths=[];storage_paths=[]
            for p,f in sorted(indexed.items(),key=lambda item:(len(item[0]),item[0])):
                typ=f.get('type','').upper()
                if typ not in ('CALL','DELEGATECALL') or f.get('error') or f.get('revertReason'):
                    raise ValueError('Complete successful supported execution contexts required for unique emitter')
                if p and f.get('from')!=contexts.get(p[:-1]):raise ValueError('Indexed caller differs from parent execution context')
                ctx=f.get('to') if typ=='CALL' else contexts.get(p[:-1]) if p else None
                if ctx is None:raise ValueError('Unique emitter proof needs CALL root')
                contexts[p]=ctx
                if f.get('to')==WETH:code_paths.append(p)
                if ctx==WETH:storage_paths.append(p)
            emitted=[x for x in rc['logs'] if x.get('address')==WETH]
            if code_paths!=[path] or storage_paths!=[path] or len(emitted)!=1 or emitted[0]!=log:
                raise ValueError('Log-to-call binding absent or ambiguous')
            proof={'basis':'COMPLETE_BOUND_TREE_UNIQUE_CANONICAL_STORAGE_LOG_EMITTER',
                'actual_call_tree_path':list(path),'log_index':policy['log_index'],'frame_count':len(indexed),
                'receipt_log_count':len(rc['logs']),'canonical_log_count':1,'kind':policy['kind'],
                'frame_logs_fabricated':False,'deposit_equivalent_proof_also_present':equivalent is not None}
        except ValueError as original:
            if policy['kind']!='DEPOSIT':raise
            try:proof=_deposit_topic_emitter_proof(policy,data,context)
            except ValueError as topic_error:
                raise ValueError(str(original)+'; '+str(topic_error)) from topic_error
    if policy['kind']=='DEPOSIT':
        if integer(frame.get('value','0x0'))!=amount or frame.get('input','0x').lower() not in ('0x','0xd0e30db0') or frame.get('calls'):
            raise ValueError('Pure deposit has unsupported input, refund or child operation')
        native_path=list(path)
    else:
        expected='0x2e1a7d4d'+format(amount,'064x')
        if integer(frame.get('value','0x0'))!=0 or frame.get('input','').lower()!=expected:
            raise ValueError('Independent withdraw input/amount evidence failed')
        children=frame.get('calls',[])
        if len(children)!=1:raise ValueError('Withdraw ETH output is absent or nonunique')
        out=children[0]
        if out.get('type','').upper()!='CALL' or out.get('from')!=WETH or out.get('to')!=policy['holder'] or integer(out.get('value','0x0'))!=amount or out.get('input','0x')!='0x' or out.get('calls') or out.get('error') or out.get('revertReason'):
            raise ValueError('Withdraw exact ETH output/receiver/no-refund contract failed')
        native_path=list(path)+( [0] )
    return frame,log,native_path,proof

def certify_instance(policy, evidence_context):
    """Recompute one instance. Returned ports are facts, not source allocations."""
    policy=copy.deepcopy(policy);data,verified,kind=_materials(evidence_context)
    _check_identity(policy,data)
    legacy_policy={'tx_hash':policy['tx_hash'],'block_number':policy['block_number'],'contract':WETH,
        'credited_address':policy['holder'],'amount_raw':str(integer(policy['amount_raw'])),
        'input_trace_address':policy['call_path'],'deposit_log_index':policy['log_index'],'block_hash':policy['block_hash']}
    binding=audit_bindings(legacy_policy,data['transaction'],data['receipt'],data.get('internal',{'result':[]}),data['trace'],data['historical_code'],data.get('source_attestation'),verified)
    if binding['conflicts']:raise ValueError('Semantic evidence contradiction: '+','.join(binding['conflicts']))
    if kind=='REAL_CHAIN':
        if not is_verified_context(verified) or not binding['source_verified'] or not all(binding['request_bound'].get(k) for k in ('transaction','receipt','trace','historical_code')):
            raise ValueError('Real component lacks bound acquisition/source/runtime')
        header=verified.records.get('header',{});req=header.get('request',{})
        if header.get('payload_sha256')!=payload_hash(data['header']) or req.get('method')!='eth_getBlockByNumber' or req.get('params')!=[hex(policy['block_number']),False]:
            raise ValueError('Historical header lacks exact request binding')
        tr=verified.records.get('trace',{}).get('request',{})
        if tr.get('method')=='debug_traceTransaction':
            params=tr.get('params',[])
            if len(params)!=2 or not isinstance(params[1],dict) or params[1].get('tracer')!='callTracer' or params[1].get('tracerConfig',{}).get('onlyTopCall') is True:
                raise ValueError('Complete callTracer request required; top-only tree cannot certify children')
    else:
        att=data.get('source_attestation',{})
        if code_hash(data['historical_code'])!=code_hash(att.get('reference_runtime_code')) or not att.get('source_sha256'):
            raise ValueError('Controlled runtime/source consistency failed')
    text=_source_text(verified,data)
    if not _body_support(text,policy['kind']):raise ValueError('Exact independently supported source body absent')
    frame,log,native_path,logproof=_frame_and_log(policy,data,binding,verified)
    unit_key={'chain_id':1,'tx_hash':policy['tx_hash'],'call_path':policy['call_path'],'kind':policy['kind'],'exact_log_locator':policy['log_index']}
    uid='wethunit:'+digest(unit_key);prefix='semport:'+uid;amount=str(integer(policy['amount_raw']))
    logid='eip155:1:tx:'+policy['tx_hash']+':log:'+str(policy['log_index'])
    native_id=event_id(policy['tx_hash'],native_path)
    inp_asset,out_asset=(NATIVE,TOKEN) if policy['kind']=='DEPOSIT' else (TOKEN,NATIVE)
    position={'block':policy['block_number'],'tx_index':policy['tx_index'],'timestamp':policy['timestamp'],
        'log_index':policy['log_index'],'exact_call_path':policy['call_path'],
        'phase':'ATOMIC_CANONICAL_OPERATION','order_basis':'EXACT_REQUEST_BOUND_CALL_AND_RECEIPT_LOG_POSITION',
        'execution_index':policy.get('execution_index')}
    input_position=dict(position,log_index=None,phase='CALL_VALUE_TRANSFER' if policy['kind']=='DEPOSIT' else 'CALL_ENTRY_TOKEN_DEBIT')
    output_position=dict(position,phase='CANONICAL_DEPOSIT_LOG') if policy['kind']=='DEPOSIT' else dict(position,
        log_index=None,exact_call_path=native_path,phase='CALL_VALUE_TRANSFER')
    # Unverified caller-supplied execution_index must not invent trace/log order.
    if policy.get('execution_index') is not None:raise ValueError('Generic caller execution_index not an execution proof')
    ports={name:prefix+':'+name for name in ('input','refund','net','output')}
    native_source=policy['holder'] if policy['kind']=='DEPOSIT' else WETH
    native_target=WETH if policy['kind']=='DEPOSIT' else policy['holder']
    native_event={'event_id':native_id,'tx_hash':policy['tx_hash'],'sender':native_source,'recipient':native_target,
        'asset':NATIVE,'amount_raw':int(amount),'block':policy['block_number'],'block_hash':policy['block_hash'],
        'tx_index':policy['tx_index'],'timestamp':policy['timestamp'],'kind':'top' if not native_path else 'internal',
        'trace_address':None if not native_path else '_'.join(map(str,native_path)),'log_index':None,
        'execution_index':None,'success':True,'provenance':'CERTIFIED_WETH_NATIVE_LEG:'+uid,'chain_id':'eip155:1'}
    return {'schema_version':SCHEMA,'semantic_version':SEMANTIC_VERSION,'unit_id':uid,'kind':policy['kind'],
        'chain_id':1,'contract':WETH,'tx_hash':policy['tx_hash'],'holder':policy['holder'],
        'block_number':policy['block_number'],'block_hash':policy['block_hash'],'tx_index':policy['tx_index'],'timestamp':policy['timestamp'],
        'call_path':policy['call_path'],'exact_log_locator':policy['log_index'],'instance_policy':policy,
        'input':{'asset':inp_asset,'address':policy['holder'],'amount_raw':amount,'port_id':ports['input'],
            'physical_event_id':native_id if policy['kind']=='DEPOSIT' else logid,'execution_position':input_position},
        'output':{'asset':out_asset,'address':policy['holder'],'amount_raw':amount,'port_id':ports['output'],
            'physical_event_id':logid if policy['kind']=='DEPOSIT' else native_id,'execution_position':output_position},
        'operation_position':position,
        'virtual_ports':ports,'raw_consumed_event_ids':[native_id], 'raw_log_event_id':logid,'native_event':native_event,
        'refund_raw':'0','component_fee_raw':'0','conversion_ratio_raw':'1',
        'transaction_gas_context':{'payer':data['transaction']['from'],'asset':NATIVE,
            'amount_raw':str(integer(data['receipt']['gasUsed'])*integer(data['receipt']['effectiveGasPrice'])),
            'physical_fee_id':'eip155:1:tx:'+policy['tx_hash']+':fee','count_once_per_physical_tx':True},
        'evidence_context_id':context_identity(evidence_context),'evidence_kind':kind,
        'certificate':{'validator_version':SEMANTIC_VERSION,'checks_recomputed':True,'evidence_bindings':binding,
            'source_sha256':hashlib.sha256(text.encode()).hexdigest(),'runtime_code_sha256':code_hash(data['historical_code']),
            'material_payload_sha256':{k:payload_hash(v) for k,v in data.items()},
            'native_input_or_output_exact_call_path':native_path,'log_is_transfer':False,'log_binding_proof':logproof},
        'certification_status':'CERTIFIED_REAL_INSTANCE' if kind=='REAL_CHAIN' else 'SYNTHETIC_CONTROLLED_INSTANCE',
        'real_component_certified':kind=='REAL_CHAIN','surrounding_protocol_certified':False}

def validate_semantic_unit(unit, *, evidence_context=None):
    try:
        if not isinstance(unit,dict) or unit.get('schema_version')!=SCHEMA:raise ValueError('Unknown semantic unit schema')
        from stage1d_shared_evidence import context_view
        from collections.abc import Mapping
        evidence_context=context_view(evidence_context)
        if isinstance(evidence_context,Mapping) and unit.get('evidence_context_id') in evidence_context:
            evidence_context=evidence_context[unit['evidence_context_id']]
        expected=certify_instance(unit['instance_policy'],evidence_context)
        if unit!=expected:raise ValueError('Stored semantic unit differs from recomputed original evidence')
        return {'passed':True,'status':'PASS','unit_id':unit['unit_id'],'unit_sha256':digest(unit),
            'evidence_kind':expected['evidence_kind'],'real_component_certified':expected['real_component_certified']}
    except (ValueError,KeyError,TypeError) as exc:
        return {'passed':False,'status':'FAIL','reason':str(exc),'real_component_certified':False}

def validate_live_capability_gate(gate,work_root):
    """Live activation only: bind current source and actual PASS evidence files.

    This is deliberately separate from portable instance/model revalidation,
    which needs neither a live runtime gate nor production credentials.
    """
    if work_root is None:raise ValueError('Live semantic gate requires explicit work_root')
    root=Path(work_root).resolve()
    if not root.is_dir() or not isinstance(gate,dict) or gate.get('status')!='PASS' or gate.get('semantic_version')!=SEMANTIC_VERSION or not all(gate.get('capabilities',{}).get(k) is True for k in REQUIRED_CAPABILITIES):
        raise ValueError('Semantic pipeline integration gate is closed')
    sources=gate.get('source_sha256')
    if not isinstance(sources,dict):raise ValueError('Source-bound semantic gate required')
    normalized={}
    def bound_file(relative):
        if not isinstance(relative,str) or not relative or '\\' in relative or ':' in relative or relative.startswith('/') or any(p in ('','..','.') for p in relative.split('/')):
            raise ValueError('Gate path must be a safe work-relative filename')
        path=(root/relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():raise ValueError('Semantic gate dependency missing/outside work')
        return path
    for name,expected in sources.items():
        relative=name if '/' in name else 'src/'+name
        if relative in normalized:raise ValueError('Duplicate gate source identity')
        if not isinstance(expected,str) or not re.fullmatch(r'[0-9a-f]{64}',expected):raise ValueError('Gate source SHA missing')
        path=bound_file(relative)
        if hashlib.sha256(path.read_bytes()).hexdigest()!=expected:raise ValueError('Semantic gate source changed: '+relative)
        normalized[relative]=expected
    if not {'src/'+name for name in REQUIRED_GATE_SOURCES}<=set(normalized):raise ValueError('Semantic gate omits an actual pipeline source')
    evidence=gate.get('test_evidence')
    if not isinstance(evidence,list) or not evidence:raise ValueError('Actual controlled-chain/test receipts required')
    seen=set()
    for item in evidence:
        if not isinstance(item,dict) or item.get('status')!='PASS' or item.get('path') in seen:raise ValueError('Invalid/duplicate semantic test receipt binding')
        seen.add(item['path']);data=bound_file(item['path']).read_bytes()
        if hashlib.sha256(data).hexdigest()!=item.get('sha256'):raise ValueError('Semantic test receipt bytes differ')
        receipt=json.loads(data)
        if not isinstance(receipt,dict) or receipt.get('status')!='PASS' or receipt.get('passed') is False:
            raise ValueError('Semantic test receipt did not actually pass')
    return {'status':'PASS','source_sha256':normalized,'test_evidence':copy.deepcopy(evidence)}

class FiniteSemanticResolver:
    """A finite validated catalogue; the collector supplies actual reachable state.

    Construction never executes external work. The explicit controlled flag is
    accepted only with synthetic contexts; real collection additionally requires
    the root's complete pipeline gate. Query membership is created at resolve.
    """
    def __init__(self,units,evidence_contexts,*,controlled=False,capability_gate=None,work_root=None):
        from stage1d_shared_evidence import context_view,serialize_contexts
        # Each resolver owns a fresh proof-reading session. Copy descriptors,
        # never a prior resolver's in-process verified contexts or seals.
        self.contexts=context_view(serialize_contexts(evidence_contexts));self.units={};self.by_holder={};self.controlled=controlled
        if not controlled:
            self.live_gate=validate_live_capability_gate(capability_gate,work_root)
        for unit in units:
            check=validate_semantic_unit(unit,evidence_context=self.contexts)
            if not check['passed']:raise ValueError(check['reason'])
            if controlled != (unit['evidence_kind']=='SYNTHETIC_CONTROLLED'):raise ValueError('Controlled/real semantic contexts cannot be mixed')
            old=self.units.get(unit['unit_id'])
            if old is not None and old!=unit:raise ValueError('Conflicting duplicate semantic instance')
            if old is not None:continue
            self.units[unit['unit_id']]=copy.deepcopy(unit)
            self.by_holder.setdefault((unit['holder'],unit['input']['asset']),[]).append(unit)
        self.identity=digest({'units':self.units,'contexts':{k:context_identity(v) for k,v in self.contexts.items()},'controlled':controlled})

    def resolve(self,state,events,scope,identity):
        """Eligible operations before protocol stop, using current actual caller.

        Returns original native legs plus separate semantic arrivals. For
        withdrawal the log is a debit fact, not a fabricated token Transfer.
        """
        from collector import Event, strictly_after
        if identity.get('kind') in ('SERVICE','BRIDGE','MIXER','UNSUPPORTED_PROTOCOL') or identity.get('branch_action') in ('FIRST_SERVICE_STOP','UNSUPPORTED_PROTOCOL_STOP','ROLE_CONFLICT_NEEDS_REVIEW'):
            return []
        out=[];observed={e.event_id:e for e in events}
        for unit in sorted(self.by_holder.get((state.address,state.asset),[]),key=lambda u:(u['timestamp'],u['tx_hash'],u['call_path'],u['exact_log_locator'])):
            if not (scope.start_block<=unit['block_number']<=scope.end_block and state.arrival.timestamp<=unit['timestamp']<=state.local_end):continue
            # Deposit spends the already observed physical native input. Its
            # exact fields must agree; a certificate cannot select a new neighbor.
            if unit['kind']=='DEPOSIT':
                physical=observed.get(unit['native_event']['event_id'])
                if physical is None:continue
                fields=('sender','recipient','asset','amount_raw','block','block_hash','tx_hash','tx_index','timestamp','trace_address','kind')
                if any(getattr(physical,k)!=unit['native_event'].get(k) for k in fields):raise ValueError('Current native candidate input differs from exact certificate')
                ordering=physical
            else:
                # Withdrawal activation is selected only from this finite
                # catalogue for the reachable holder; actual log remains in unit.
                ordering=Event(unit['raw_log_event_id'],unit['tx_hash'],state.address,WETH,TOKEN,int(unit['input']['amount_raw']),
                    unit['block_number'],unit['tx_index'],unit['timestamp'],kind='semantic_log',log_index=unit['exact_log_locator'],block_hash=unit['block_hash'])
            relation=strictly_after(ordering,state.arrival)
            if relation is not True:
                if relation is None:out.append({'gap':{'reason':'SEMANTIC_OPERATION_ORDER_UNRESOLVED','unit_id':unit['unit_id'],'arrival_event_id':state.arrival.event_id}})
                continue
            if state.depth>=scope.max_depth:continue
            if unit['kind']=='DEPOSIT':
                arrival=Event(unit['virtual_ports']['output'],unit['tx_hash'],WETH,unit['holder'],TOKEN,int(unit['output']['amount_raw']),
                    unit['block_number'],unit['tx_index'],unit['timestamp'],kind='semantic_log',log_index=unit['exact_log_locator'],
                    provenance='CERTIFIED_DEPOSIT_CREDIT_NOT_TRANSFER:'+unit['unit_id'],block_hash=unit['block_hash'])
            else:
                arrival=Event(**unit['native_event'])
            out.append({'unit':copy.deepcopy(unit),'arrival':arrival,'native_event':Event(**unit['native_event']),
                'replace_native_recipient_push':unit['kind']=='DEPOSIT','membership':{
                    'query_id':scope.query_id,'scope_id':scope.scope_id,'scope_hash':scope.scope_hash,'unit_id':unit['unit_id'],
                    'input_arrival_event_id':state.arrival.event_id,'input_depth':state.depth,
                    'output_arrival_event_id':arrival.event_id,'output_depth':state.depth+1,
                    'window_end':scope.local_end(arrival),'physical_hops':1,'extra_virtual_port_hops':0}})
        return out
