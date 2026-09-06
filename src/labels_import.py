"""Offline parser for an authorized, already saved one-off MetaSleuth response."""
import argparse,json
from pathlib import Path
from labels_policy import full_address,lookup_outcome
from reference_core import canonical_actor,digest,sid,stable,dump,write_csv

def parse(payload, metadata, raw_ref, raw_hash):
    observations=[];outcomes=[]
    if payload.get('code')!=200000 or not isinstance(payload.get('data'),list):return [],[]
    for index,r in enumerate(payload['data']):
        if str(r.get('chain_id'))!='1':continue
        address=full_address(r['address']);status=lookup_outcome(payload,address)
        if status=='MISSING_OR_DUPLICATE_ADDRESS_UNRESOLVED':raise ValueError('Duplicate provider address')
        info=r.get('main_entity_info') or {};actor,key=canonical_actor(r.get('main_entity'))
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
        outcomes.append({'address':address,'request_status':'SUCCESS','label_result_status':status,'actor':actor,'role':role,'request_id':request_id,'raw_response_sha256':raw_hash})
    return observations,outcomes

def main():
    p=argparse.ArgumentParser();p.add_argument('--payload',type=Path,required=True);p.add_argument('--metadata',type=Path,required=True);p.add_argument('--raw-ref',required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--outcomes',type=Path,required=True);a=p.parse_args()
    obs,outcomes=parse(json.loads(a.payload.read_text(encoding='utf-8-sig')),json.loads(a.metadata.read_text(encoding='utf-8-sig')),a.raw_ref,digest(a.payload))
    if a.output.exists():
        existing=json.loads(a.output.read_text());by={o['observation_id']:o for o in existing+obs};obs=list(by.values())
    dump(a.output,obs);write_csv(a.outcomes,outcomes)
    print(json.dumps({'new_response_observations':len(outcomes),'total_supplement_observations':len(obs)}))

if __name__=='__main__':main()
