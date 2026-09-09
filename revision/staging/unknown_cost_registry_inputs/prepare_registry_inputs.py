"""Fixed-source UNKNOWN cost descriptors and lower-bound diagnostics; no writes to code."""
from collections import Counter,defaultdict
from contextlib import closing
from pathlib import Path
import argparse,hashlib,json,os,sqlite3,sys,time
import re

HERE=Path(__file__).resolve().parent
R=HERE.parents[1];C=R/'code'
CORE=R/'staging/unknown_cost_boundary_core_candidate/src'
REGISTRY=R/'staging/unknown_cost_boundary_registry_candidate/src'
sys.dont_write_bytecode=True
sys.path[:0]=[str(CORE),str(REGISTRY),str(C/'src'),str(C)]
from collector import Scope
from physical_facts import canonical_event,PhysicalFactRegistry
from stage1d_unknown_cost_boundary import activity,state_key,state_binding,digest,verify_source_bytes
from stage1d_runtime import Runtime
from read_retry_r4 import logical_key

FIXED={
 'reports/unknown_cost_boundary_v1/inputs/INITIAL_SNAPSHOT.json':'d577d3cf9bce7fc5e780a31c12de2de60c6eb6010a767e0440d3e41c004feea1',
 'reports/unknown_cost_boundary_v1/preparations/historical_code_100_v1.json':'468be2202a560ecbf661b10cd9d3948b0f2187b859ce890709028441bc446f51',
 'operations/unknown_cost_code_bc602436eb9f49e995f551bd18ae08c7.json':'a17c6e03936152fcb67bd731302e79b64e16d06d524eef668557dbb78490efe3'}
AUTH='STAGE1D_UNKNOWN_CONTRACT_OR_RATE20_BOUNDARY_V1'
PROVIDER='ALCHEMY_ETH_MAINNET_EXISTING'

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def relative(p):return Path(os.path.relpath(Path(p).resolve(),C)).as_posix()
def save(p,value):
    p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
    with p.open('x',encoding='utf8') as f:json.dump(value,f,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    return {'path':relative(p),'sha256':sha(p),'bytes':p.stat().st_size}

class Reader:
    def __init__(self):self.blobs={};self.refs={}
    def raw(self,ref):
        p=(C/ref['path']).resolve()
        if not p.is_relative_to(R) or p.is_symlink():raise ValueError('Evidence escaped current revision')
        key=(str(p),ref['sha256'])
        if key not in self.blobs:
            b=p.read_bytes()
            if hashlib.sha256(b).hexdigest()!=ref['sha256']:raise ValueError('Original SHA differs: '+ref['path'])
            if ref.get('bytes') not in (None,len(b)):raise ValueError('Original length differs')
            self.blobs[key]=b;self.refs[relative(p)]={'path':relative(p),'sha256':ref['sha256'],'bytes':len(b)}
        return self.blobs[key]
    def read(self,ref):return json.loads(self.raw(ref))
    def fixed(self,rel):return self.read({'path':relative(R/rel),'sha256':FIXED[rel]})

def checked_rpc(reader,db,runtime,ref,expected=None):
    env=reader.read(ref);req=env.get('request',{});response=env.get('response',{})
    if (env.get('evidence_kind')!='REAL_CHAIN' or env.get('provider_alias')!=PROVIDER or env.get('status')!='SUCCESS_VALIDATED'
            or runtime.rpc_result_status(req,response)!='SUCCESS_VALIDATED'):raise ValueError('RPC original did not validate')
    plan={k:req.get(k) for k in ('method','params')}
    if expected is not None and plan!=expected:raise ValueError('RPC selectors differ')
    ident=runtime.rpc_identity(PROVIDER,plan);key=logical_key(ident)
    row=db.execute('SELECT identity_json,state,success_payload,success_receipt FROM read_requests WHERE logical_key=?',(key,)).fetchone()
    if row is None or json.loads(row[0])!=ident or row[1]!='SUCCESS' or json.loads(row[2])!=response['result']:raise ValueError('Exact cache success differs')
    receipt=json.loads(row[3])
    if (C/receipt['artifact_path']).resolve()!=(C/ref['path']).resolve() or receipt['artifact_sha256']!=ref['sha256']:raise ValueError('Cache envelope binding differs')
    body=(C/ref['path']).parent/'response_body.bin'
    bodyref={'path':relative(body),'sha256':env['raw_body_sha256']}
    wire=json.loads(reader.raw(bodyref));wire=wire if isinstance(wire,list) else [wire]
    found=[x for x in wire if x.get('id')==req.get('id')]
    if found!=[response]:raise ValueError('Envelope response differs from its original batch body')
    return env,bodyref,key

def prepare_codes(reader,snapshot,prepared,operation):
    runtime=Runtime();groups={g['group_key']:g for g in prepared['code_groups']};snaprows=snapshot['state_screen_rows']
    scopes={q['query']['name']:Scope.from_policy(q['query']) for q in snapshot['query_snapshots']}
    descriptors=[];mapping=[];gaps=[];seen=set();classifications=Counter()
    dbpath=(C/'private/read_retry_r4.sqlite').resolve()
    with closing(sqlite3.connect(dbpath.as_uri()+'?mode=ro',uri=True)) as db:
        db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        for batch in operation['results']:
            if batch['result']['status']!='COMPLETE':raise ValueError('Only completed fixed batch is admitted')
            members={m['identity']:m for m in batch['result']['members']}
            if len(members)!=len(batch['needs']):raise ValueError('Operation members differ')
            for need in batch['needs']:
                member=members[need['logical_key']];group=groups[need['first_group_key']]
                if need['request']!=group['request'] or member['identity'] in seen:raise ValueError('Exact code group differs or repeats')
                seen.add(member['identity'])
                ref={'path':member['artifact_path'],'sha256':member['artifact_sha256']}
                env,bodyref,key=checked_rpc(reader,db,runtime,ref,need['request'])
                if env['response']['result']!=member['result'] or key!=member['identity']:raise ValueError('Execution receipt response differs')
                code='NO_RUNTIME_CODE_AT_BLOCK' if member['result']=='0x' else 'CODE_PRESENT';classifications[code]+=1
                descriptor={'schema_version':'stage1d-unknown-cost-code-evidence-v1','chain_id':'eip155:1',
                    'address':group['address'],'request_ref':ref,'response_ref':ref,
                    'arrival_block_hash':group['block_hash'],'same_block_conflict':False,
                    'original_body_ref':bodyref}
                needs_header=[m for m in group['states'] if not snaprows[m['initial_row_index']]['state']['arrival'].get('block_hash')]
                header_state='NOT_REQUIRED_OWN_ARRIVAL_HASH'
                if needs_header:
                    plan={'method':'eth_getBlockByNumber','params':[hex(group['arrival_block']),False]}
                    ident=runtime.rpc_identity(PROVIDER,plan);hk=logical_key(ident)
                    row=db.execute('SELECT state,success_receipt FROM read_requests WHERE logical_key=?',(hk,)).fetchone()
                    header_state=row[0] if row else 'NO_CURRENT_EXACT_REQUEST'
                    if row and row[0]=='SUCCESS':
                        rr=json.loads(row[1]);h={'path':rr['artifact_path'],'sha256':rr['artifact_sha256']}
                        he,_,_=checked_rpc(reader,db,runtime,h,plan)
                        if he['response']['result']['hash']!=group['block_hash']:raise ValueError('Header hash differs from initial same-block evidence')
                        for m in needs_header:
                            if int(he['response']['result']['timestamp'],16)!=snaprows[m['initial_row_index']]['state']['arrival']['timestamp']:raise ValueError('Header timestamp differs')
                        descriptor['header_ref']=h;header_state='SUCCESS_HEADER_BOUND'
                    else:gaps.append({'address':group['address'],'block':group['arrival_block'],'logical_key':hk,'request':plan,
                        'current_request_state':header_state,'affected_initial_row_indices':[m['initial_row_index'] for m in needs_header],
                        'reason':'REGISTRY_REQUIRES_OWN_ARRIVAL_HASH_OR_EXACT_HEADER','new_request_performed':False})
                dref=save(HERE/'historical_codes'/(key+'.json'),descriptor);descriptors.append(dref)
                for m in group['states']:
                    s=snaprows[m['initial_row_index']]['state'];q=scopes[m['query_name']]
                    own=s['arrival'].get('block_hash')
                    if own and own!=group['block_hash']:raise ValueError('Own arrival hash differs')
                    mapping.append({'initial_row_index':m['initial_row_index'],'query_name':m['query_name'],
                        'registry_state_key':state_key(s,q),'preparation_state_key':m['state_key'],
                        'address':group['address'],'arrival_block':group['arrival_block'],'classification':code,
                        'descriptor_ref':dref,'registry_block_binding_ready':bool(own or 'header_ref' in descriptor),
                        'header_state':header_state,'group_key':group['group_key']})
    if len(descriptors)!=100:raise ValueError('Fixed batch must have 100 exact code descriptors')
    summary={'schema_version':'stage1d-fixed-code100-descriptor-summary-v1','descriptors':descriptors,
        'classification_by_exact_address_block':dict(classifications),'mapped_screen_states':len(mapping),
        'nonempty_distinct_addresses':len({x['address'] for x in mapping if x['classification']=='CODE_PRESENT'}),
        'nonempty_screen_states':sum(x['classification']=='CODE_PRESENT' for x in mapping),
        'exact_state_mapping':mapping,'unresolved_header_bindings':gaps,'new_external_requests':0,'production_writes':False}
    save(HERE/'CODE_DESCRIPTOR_SUMMARY.json',summary)
    print(json.dumps({'phase':'CODE_DESCRIPTORS_READY','count':100,'classifications':dict(classifications),'header_binding_gaps':len(gaps)}),flush=True)
    return descriptors,summary

def build_index(events):
    """Retain contrary physical variants together, including across sender/asset keys."""
    registry=PhysicalFactRegistry();excluded=Counter()
    for raw in events:
        try:
            if (raw.get('semantic_virtual') or raw.get('is_virtual') or str(raw.get('event_id','')).startswith('semport:')
                or str(raw.get('kind','')).lower() not in ('top','internal','erc20')):
                excluded['NOT_PHYSICAL_VALUE_TRANSFER']+=1;continue
            fixed=dict(raw)
            if any(raw.get(k) is False for k in ('tx_success','ancestor_success','ancestors_success','effective_success')) or raw.get('reverted') is True:fixed['success']=False
            row=canonical_event(fixed)
            if row['kind']=='internal' and (raw.get('trace_address') is None or raw.get('trace_address')==''):
                excluded['PHYSICAL_POSITION_UNRESOLVED']+=1;continue
            if row['kind']=='erc20' and row.get('log_index') is None:
                excluded['PHYSICAL_POSITION_UNRESOLVED']+=1;continue
            registry.add(row,raw=raw)
        except (ValueError,TypeError,KeyError):excluded['MALFORMED_FACT']+=1
    index=defaultdict(list);arrivals={}
    for group in registry.groups.values():
        material={digest(v['raw']):v['raw'] for v in group['versions'].values()}
        keys={(v['facts']['chain_id'],v['facts']['sender'],v['facts']['asset']) for v in group['versions'].values()}
        for key in keys:index[key].extend(material.values())
        for alias in group['aliases']:
            if alias.startswith('id:'):arrivals[alias[3:]]=list(material.values())
    conflicts=registry.snapshot(include_events=False)['conflicts']
    return index,arrivals,{'excluded_unusable_before_index':dict(excluded),'physical_groups':len(registry.groups),
                          'global_conflicts':conflicts,'sender_asset_keys':len(index)}

def prepare_activity(reader,snapshot):
    events=[];sources=[];descriptors=[];counts=[];scopes={}
    for qs in snapshot['query_snapshots']:
        name=qs['query']['name'];scopes[name]=Scope.from_policy(qs['query']);original=qs['refs']['collection']
        ref={'path':relative(R/original['path']),'sha256':original['sha256'],'bytes':original['bytes']}
        collection=reader.read(ref)
        if collection.get('query_id') not in (None,qs['query']['query_id']):raise ValueError('Snapshot query identity differs')
        material=collection.get('candidate_events',[])+collection.get('context_events',[])
        events.extend(material);sources.append(ref)
        doc={'schema_version':'stage1d-unknown-cost-observations-v1','chain_id':'eip155:1','source_refs':[ref],
            'basis':'FIXED_INITIAL_SNAPSHOT_ALL_CANDIDATE_AND_CONTEXT_NORMALIZED_PHYSICAL_FACTS',
            'query_source_name':name,'full_coverage':False,'membership_after_cost_replay_is_not_an_input':True}
        descriptors.append(save(HERE/'activity_observations'/(name+'.json'),doc))
        counts.append({'query_name':name,'candidate_events':len(collection.get('candidate_events',[])),
            'context_events':len(collection.get('context_events',[])),'original_ref':ref})
        del collection
    index,arrivals,index_report=build_index(events);del events
    verified=verify_source_bytes((ref,reader.raw(ref)) for ref in sources)
    memo={};results=[];memo_results=[];unavailable=[];started=time.perf_counter()
    screen=[(i,row) for i,row in enumerate(snapshot['state_screen_rows']) if row['new_cost_screen_required']]
    for n,(i,row) in enumerate(screen):
        s=row['state'];q=scopes[row['query_name']]
        try:
            binding=state_binding(s,q);identity=state_key(s,q)
            signature=digest({'scope_hash':q.scope_hash,'address':s['address'],'asset':s['asset'],
                              'arrival':canonical_event(s['arrival']),'local_end':s['local_end']})
            if signature not in memo:
                key=(binding['chain_id'],binding['address'],binding['asset'])
                selected=index.get(key,[])+arrivals.get(s['arrival']['event_id'].lower(),[])
                evidence=activity(s,q,selected,sources=verified,evidence_refs=sources).to_dict()
                memo[signature]=evidence
                memo_results.append({'activity_group_id':signature,'representative_initial_row_index':i,
                    'diagnostic_only_not_a_deserializable_certificate':True,'result':evidence})
            value=memo[signature]
            result={k:value[k] for k in ('N_obs','D_seconds','strict_gt20_observed','rate_truth','coverage',
                'full_coverage','short_window_rate_unstable','physical_event_count','unique_counterpart_count')}
            results.append(dict(result,initial_row_index=i,query_name=row['query_name'],registry_state_key=identity,
                address=s['address'],asset=s['asset'],arrival_event_id=s['arrival']['event_id'],arrival_block=s['arrival']['block'],
                depth=s['depth'],local_end=s['local_end'],activity_group_id=signature,
                representative_evidence_state_key=value['state_key'],cost_stop_applied=False))
        except (ValueError,KeyError,TypeError) as exc:
            unavailable.append({'initial_row_index':i,'query_name':row['query_name'],'reason':str(exc),'rate_truth':'UNKNOWN','full_coverage':False})
        if (n+1)%250==0:print(json.dumps({'phase':'ACTIVITY_INDEXED_DIAGNOSTIC','states_processed':n+1,'screen_states':len(screen),'unique_arrival_calculations':len(memo),'seconds':round(time.perf_counter()-started,3)}),flush=True)
    report={'schema_version':'stage1d-initial-screen-rate20-lower-bound-diagnostic-v1','authorization_id':AUTH,
        'initial_snapshot_sha256':FIXED['reports/unknown_cost_boundary_v1/inputs/INITIAL_SNAPSHOT.json'],
        'activity_descriptors':descriptors,'source_event_counts':counts,'index_report':index_report,
        'states':results,'unavailable_states':unavailable,'activity_groups':memo_results,
        'summary':{'screen_states':len(screen),'diagnosed_states':len(results),'unavailable_states':len(unavailable),
            'unique_arrival_calculations':len(memo),'strict_gt20_states':sum(v['strict_gt20_observed'] for v in results),
            'strict_gt20_by_query':dict(Counter(v['query_name'] for v in results if v['strict_gt20_observed'])),
            'strict_gt20_chain_addresses':len({v['address'] for v in results if v['strict_gt20_observed']})},
        'new_external_requests':0,'production_writes':False,'current_candidate_projection_used':False,
        'threshold_evidence_is_not_identity_or_code_evidence':True,'credits_saving':None,'CU_saving':None,
        'seconds':time.perf_counter()-started}
    save(HERE/'RATE20_OBSERVED_LOWER_BOUND.json',report)
    return descriptors,report

def main():
    global HERE
    p=argparse.ArgumentParser();p.add_argument('--prepare',action='store_true');p.add_argument('--output-subdir');args=p.parse_args()
    if not args.prepare:print(json.dumps({'read_only_preparation':True,'execution':False}));return
    if args.output_subdir:
        if not re.fullmatch('[A-Za-z0-9][A-Za-z0-9_-]{0,47}',args.output_subdir):raise ValueError('New bounded staging output name required')
        HERE=HERE/'outputs'/args.output_subdir
        HERE.mkdir(parents=True,exist_ok=False)
    # The registry is a descriptor consumer, not an imported computation input.
    watched=[Path(__file__),CORE/'stage1d_unknown_cost_boundary.py',C/'src/physical_facts.py',C/'src/collector.py',C/'src/stage1d_runtime.py',C/'src/stage1d_rpc.py',C/'src/context_access_r3.py',C/'src/read_retry_r4.py']
    before={relative(x):sha(x) for x in watched};reader=Reader();started=time.perf_counter()
    save(HERE/'SOURCE_READ_BINDING.json',{'source_sha256':before,'new_external_requests':0})
    snapshot=reader.fixed(list(FIXED)[0]);prepared=reader.fixed(list(FIXED)[1]);operation=reader.fixed(list(FIXED)[2])
    codes,csummary=prepare_codes(reader,snapshot,prepared,operation)
    observations,activity_summary=prepare_activity(reader,snapshot)
    after={relative(x):sha(x) for x in watched}
    if before!=after:
        save(HERE/'SOURCE_CHANGED_DO_NOT_ADOPT.json',{'before':before,'after':after,'status':'DO_NOT_ADOPT_THIS_PASS'})
        raise ValueError('Source changed during fixed-input preparation; do not adopt outputs')
    summary={'status':'READY_READ_ONLY_DESCRIPTORS','historical_codes':codes,'activity_observations':observations,
        'historical_code_summary_ref':{'path':relative(HERE/'CODE_DESCRIPTOR_SUMMARY.json'),'sha256':sha(HERE/'CODE_DESCRIPTOR_SUMMARY.json')},
        'activity_diagnostic_ref':{'path':relative(HERE/'RATE20_OBSERVED_LOWER_BOUND.json'),'sha256':sha(HERE/'RATE20_OBSERVED_LOWER_BOUND.json')},
        'source_sha256':before,'verified_input_refs':list(reader.refs.values()),'new_external_requests':0,'production_writes':False,
        'activity_summary':activity_summary['summary'],'seconds':time.perf_counter()-started}
    save(HERE/'READY_MANIFEST.json',summary);print(json.dumps({'status':summary['status'],'activity':summary['activity_summary'],'seconds':summary['seconds']}),flush=True)

if __name__=='__main__':main()
