"""Refine saved compact rows; never reread the two large BQ artifacts."""
from pathlib import Path
import collections,json,gzip,hashlib,datetime,time
S=Path(__file__).resolve().parent;start=time.perf_counter();cpu=time.process_time()
boundary_raw=(S/'BOUNDARY_SNAPSHOT.json').read_bytes();boundary=json.loads(boundary_raw)
summary=json.loads((S/'TRIAGE.json').read_bytes());facts=[json.loads(line) for line in gzip.open(S/'TRANSACTION_TRIAGE.jsonl.gz','rt',encoding='utf-8')]
report={'status':'READ_ONLY_TOP_ROW_LOWER_BOUND_NOT_COMPLETE_INTERNAL_TRIAGE','boundary_sha256':hashlib.sha256(boundary_raw).hexdigest(),'queries':{},'large_files_reread':0,'original_evidence_revalidated':False}
def in_need(f,need):
    t=int(datetime.datetime.fromisoformat(f['block_time'].replace('Z','+00:00')).timestamp())
    return need['start_block']<=f['block']<=need['end_block'] and need['start_time']<=t<=need['end_time']
for q in boundary['queries']:
    name=q['query_name']
    if name not in summary['queries']:continue
    checked=summary['queries'][name]['collection']['sha256']==q['collection_sha256']
    if not checked:raise ValueError('Boundary does not refer to captured current collection: '+name)
    outgoing=[];top_touched=[];normal_or_fee=[]
    needs=q['needed_ranges'];frontiers=collections.defaultdict(list)
    for frontier in q['frontier']:frontiers[frontier['state']['address']].append(frontier['state'])
    for f in facts:
        top=f['tops'][0] if len(f['tops'])==1 else None
        if top is None:continue
        touched=[n for n in needs if n['asset']=='native:eip155:1' and n['address'] in (top['from_address'],top['to_address']) and in_need(f,n)]
        if not touched:continue
        top_touched.append(f);legal=[]
        for need in touched:
            if top['from_address']!=need['address'] or top['to_address']==need['address'] or top['success'] is not True or int(top['value_raw'])<=0:continue
            for state in frontiers[need['address']]:
                arrival=state['arrival'];when=int(datetime.datetime.fromisoformat(f['block_time'].replace('Z','+00:00')).timestamp())
                order=f['block']>arrival['block'] or f['block']==arrival['block'] and f['tx_hash']!=arrival['tx_hash'] and arrival['tx_index'] is not None and int(f['tx_index'])>arrival['tx_index']
                if order and arrival['timestamp']<=when<=state['local_end']:legal.append(need)
        if legal:outgoing.append({'tx_hash':f['tx_hash'],'block':f['block'],'top':top,'matched_need_count':len(legal),'needs':legal[:1],'cache':f['cache']})
        else:normal_or_fee.append(f)
    context=[f for f in facts if f['queries'][name]['context_touched']]
    def counts(rows):
        points={json.dumps({'method':method,'params':v['params']},sort_keys=True):v for f in rows for method,v in f['cache'].items()}
        return dict(collections.Counter(v['method']+'|'+v['state'] for v in points.values()))
    report['queries'][name]={'collection_sha256_match':checked,'frontier_count':q['frontier_count'],'actual_missing_rectangles':len(needs),
        'top_rows_touching_exact_current_needs':len(top_touched),'positive_outgoing_top_rows_with_legal_current_arrival_lower_bound':len(outgoing),
        'top_rows_normal_incoming_zero_failed_or_nonrenewing':len(normal_or_fee),'additional_internal_family_need_count':None,
        'internal_need_count_status':'NOT_COUNTED_AFTER_COMPACT_EXTRACTION; NO_SECOND_LARGE_FILE_PASS',
        'top_outgoing_cache_points':counts(outgoing),'background_current_context_touched':len(context) if summary['queries'][name]['context_status']=='CURRENT_PLAN_ROWS_AVAILABLE' else None,
        'background_context_cache_points':counts(context),'samples':outgoing[:10]}
    # The initial pending alias was wrong, so its zeros are explicitly replaced.
    sq=summary['queries'][name]
    for k in ('pending_outgoing','pending_touched','strict_order_unknown','pending_outgoing_all_three_success_index','pending_outgoing_point_states'):sq[k]=None
    sq['pending_count']=q['frontier_count'];sq['pending_alias_correction']='Actual field is unresolved_frontier; original pending-only zeros invalidated. Use REFINED_TRIAGE.json top-row lower bound.'
report['cpu_seconds']=time.process_time()-cpu;report['wall_seconds']=time.perf_counter()-start
(S/'REFINED_TRIAGE.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
(S/'TRIAGE.json').write_text(json.dumps(summary,indent=2)+'\n',encoding='utf-8')
print(json.dumps({q:{k:v for k,v in r.items() if k!='samples'} for q,r in report['queries'].items()},indent=2))
