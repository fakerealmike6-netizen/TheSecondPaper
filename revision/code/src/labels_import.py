"""Offline parser for an authorized, already saved one-off MetaSleuth response."""
import argparse,csv,gzip,json
from pathlib import Path
from labels_policy import full_address,lookup_outcome
from reference_core import canonical_actor,digest,sid,stable,dump,write_csv

VERSION='stage1b-r4-metasleuth-import-2.0'

def frozen_addresses(metadata, requested_addresses=None):
    """Only an explicit frozen request list establishes expected addresses."""
    values=requested_addresses
    if values is None:
        values=metadata.get('requested_addresses', metadata.get('addresses'))
    if values is None:return None
    if not isinstance(values,list):raise ValueError('Frozen request addresses must be a list')
    addresses=[full_address(x) for x in values]
    if len(set(addresses))!=len(addresses):raise ValueError('Duplicate frozen request address')
    return addresses

def batch_failure(payload,metadata):
    if metadata.get('transport_error') or metadata.get('error_class') or metadata.get('error') or str(metadata.get('status','')).upper() in {
        'FAILED','TRANSPORT_FAILED','TRANSPORT_ERROR','NETWORK_ERROR','HTTP_ERROR'}:
        return 'TRANSPORT_FAILED'
    http=metadata.get('http_status')
    if http is not None and (type(http) is not int or not 200<=http<300):return 'HTTP_FAILED'
    if not isinstance(payload,dict):return 'MALFORMED_RESPONSE'
    if type(payload.get('code')) is not int or payload['code']!=200000:return 'PROVIDER_FAILED'
    if not isinstance(payload.get('data'),list):return 'MALFORMED_SUCCESS_RESPONSE'
    return None

def outcome(status,metadata,raw_hash,address=None,**extra):
    return dict(schema_version=VERSION,scope='ADDRESS' if address is not None else 'BATCH',address=address,
        request_status='FAILED' if status.startswith(('FAILED','MISSING','DUPLICATE','UNEXPECTED','INVALID')) else 'SUCCESS',
        outcome_status=status,label_result_status=None,actor=None,role=None,
        request_id=str(metadata.get('request_id') or ''),raw_response_sha256=raw_hash,**extra)

def parse(payload, metadata, raw_ref, raw_hash, *, requested_addresses=None):
    """Return observations and explicit attempt outcomes, including batch failures.

    Existing observations are never input to or overwritten by a failed parse.
    A successful empty address record differs from an omitted requested member.
    """
    if not isinstance(metadata,dict):raise ValueError('Request metadata must be an object')
    expected=frozen_addresses(metadata,requested_addresses)
    metadata=dict(metadata)
    if isinstance(payload,dict):metadata['request_id']=payload.get('request_id') or metadata.get('request_id') or ''
    failure=batch_failure(payload,metadata)
    if failure:
        status='FAILED_'+failure
        return [],[outcome(status,metadata,raw_hash,provider_code=payload.get('code') if isinstance(payload,dict) else None)]+[
            outcome(status,metadata,raw_hash,a) for a in (expected or [])]
    observations=[];outcomes=[];counts={};invalid=[]
    for index,r in enumerate(payload['data']):
        if not isinstance(r,dict):invalid.append(index);continue
        try:address=full_address(r.get('address'))
        except ValueError:invalid.append(index);continue
        if str(r.get('chain_id'))!='1':invalid.append(index);continue
        counts[address]=counts.get(address,0)+1
    if invalid:outcomes.append(outcome('FAILED_INVALID_RESPONSE_MEMBERS',metadata,raw_hash,invalid_member_indexes=invalid))
    for address,count in sorted(counts.items()):
        if count>1:outcomes.append(outcome('DUPLICATE_ADDRESS_UNRESOLVED',metadata,raw_hash,address))
        elif expected is not None and address not in expected:outcomes.append(outcome('UNEXPECTED_ADDRESS_UNRESOLVED',metadata,raw_hash,address))
    if expected is not None:
        for address in expected:
            if address not in counts:outcomes.append(outcome('MISSING_ADDRESS_UNRESOLVED',metadata,raw_hash,address))
    for index,r in enumerate(payload['data']):
        if index in invalid:continue
        address=full_address(r['address']);status=lookup_outcome(payload,address)
        if counts[address]!=1 or (expected is not None and address not in expected):continue
        if not isinstance(r.get('main_entity_info') or {},dict) or not isinstance(r.get('attributes') or [],list):
            outcomes.append(outcome('FAILED_INVALID_LABEL_STRUCTURE',metadata,raw_hash,address));continue
        info=r.get('main_entity_info') or {};actor,key=canonical_actor(r.get('main_entity'))
        if not isinstance(info.get('categories') or [],list) or any(not isinstance(x,dict) for x in (info.get('categories') or [])+(r.get('attributes') or [])):
            outcomes.append(outcome('FAILED_INVALID_LABEL_STRUCTURE',metadata,raw_hash,address));continue
        cats=[x.get('name','') for x in info.get('categories') or []];attrs=[x.get('name','') for x in r.get('attributes') or []]
        text=' '.join(cats).upper();role='UNKNOWN';kind='ATTRIBUTION' if actor else 'CONTEXT'
        if any(s in text for s in ('BRIDGE','CROSS CHAIN')):role='BRIDGE_BOUNDARY'
        elif any(s in text for s in ('MIXER','MIXING')):role='MIXER_BOUNDARY'
        elif any(s in text for s in ('DEX','DECENTRALIZED','NFT MARKETPLACE','LENDING','PROTOCOL')):role='DEX_OR_PROTOCOL'
        elif actor and any(s in text for s in ('EXCHANGE','CUSTOD','ECOMMERCE','E-COMMERCE','PAYMENT','GAMBL','CASINO','SPORTS BET','OTC DESK')):role='SERVICE'
        if any(s in text for s in ('SCAM','PHISH','HACK','EXPLOIT','RISK')):role='UNKNOWN';kind='RISK'
        if any(s in ' '.join(attrs).upper() for s in ('DEFI USER','DEX USER','CEX USER')):role='UNKNOWN';kind='BEHAVIOR'
        request_id=str(payload.get('request_id') or metadata.get('request_id') or '')
        o={'chain_id':'1','address':address,'actor':actor,'actor_key':key,'role':role,'raw_label':stable(r),'platform':'MetaSleuth','source_version':request_id,'acquired_at':metadata.get('queried_at') or metadata.get('submitted_at') or metadata.get('utc'),'generation_mechanism':None,'generation_status':'PROVIDER_METHOD_UNDISCLOSED','semantic_kind':kind,'collection_scope':'REFERENCE_TARGETED','request_or_execution_id':request_id,'raw_evidence_ref':raw_ref,'raw_evidence_sha256':raw_hash,'source_locator':f'data[{index}]','record_conflict':False,'source_metadata':stable({'categories':cats,'attributes':attrs,'name_tag':r.get('name_tag'),'label_result_status':status,'main_entity_info':info,'api_transport_is_not_label_generation_method':True})}
        o['semantic_label_key']=sid([address,actor,role,r]);o['observation_id']='obs:'+sid(o);observations.append(o)
        result=outcome('SUCCESS_EMPTY' if status=='EMPTY_LABEL_RESULT' else 'SUCCESS_WITH_LABEL',metadata,raw_hash,address)
        result.update(label_result_status=status,actor=actor,role=role);outcomes.append(result)
    if not outcomes:outcomes.append(outcome('SUCCESS_EMPTY',metadata,raw_hash))
    elif any(r['request_status']=='FAILED' for r in outcomes) and not any(r['scope']=='BATCH' for r in outcomes):
        outcomes.insert(0,outcome('FAILED_INCOMPLETE_ADDRESS_COVERAGE',metadata,raw_hash))
    return observations,outcomes

def merge_observations(existing,added):
    by={}
    for row in existing+added:
        oid=row['observation_id']
        if oid in by and by[oid]!=row:raise ValueError('Existing successful observation cannot be overwritten')
        by[oid]=row
    return list(by.values())

def read_outcomes(path):
    if not path.exists():return []
    op=gzip.open if path.suffix=='.gz' else open
    with op(path,'rt',encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f))

def main():
    p=argparse.ArgumentParser();p.add_argument('--payload',type=Path,required=True);p.add_argument('--metadata',type=Path,required=True);p.add_argument('--raw-ref',required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--outcomes',type=Path,required=True);p.add_argument('--requested-addresses',type=Path);a=p.parse_args()
    paths=[a.payload,a.metadata,a.output,a.outcomes]+([a.requested_addresses] if a.requested_addresses else [])
    if len({path.resolve() for path in paths})!=len(paths):p.error('Input and output paths must be distinct')
    try:payload=json.loads(a.payload.read_text(encoding='utf-8-sig'))
    except (UnicodeError,json.JSONDecodeError):payload=None
    metadata=json.loads(a.metadata.read_text(encoding='utf-8-sig'))
    expected=json.loads(a.requested_addresses.read_text(encoding='utf-8-sig')) if a.requested_addresses else None
    obs,outcomes=parse(payload,metadata,a.raw_ref,digest(a.payload),requested_addresses=expected)
    added_count=len(obs);existing=json.loads(a.output.read_text(encoding='utf-8-sig')) if a.output.exists() else []
    merged=merge_observations(existing,obs)
    if obs or not a.output.exists():dump(a.output,merged)
    prior=read_outcomes(a.outcomes);combined=prior+outcomes;columns=[]
    for row in combined:
        for key in row:
            if key not in columns:columns.append(key)
    write_csv(a.outcomes,combined,columns)
    failed=any(row['request_status']=='FAILED' for row in outcomes)
    print(json.dumps({'schema_version':VERSION,'batch_status':'FAILED' if failed else 'SUCCESS','new_response_observations':added_count,'total_supplement_observations':len(merged),'outcomes':len(outcomes)}))
    return 2 if failed else 0

if __name__=='__main__':raise SystemExit(main())
