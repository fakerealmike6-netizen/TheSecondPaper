"""Finite Dune supplement: only the missing existing protocol ledger kind."""
from pathlib import Path
import sys,argparse,json,os,traceback
R=Path(__file__).resolve().parents[1];C=R/'code';sys.path[:0]=[str(C/'src'),str(C)]
from context_access_r3 import read,sha
from page_attempts import atomic_json
import stage1d_bq_context_prepare as b
from stage1d_closure_scope import active_batch
from stage1d_context_online import _dates
from stage1d_protocol_supplement import build_protocol_sql
from stage1d_acquisition import save_sql
from stage1d_runtime import Runtime,execute_sql,result_rows
from stage1d_context_coverage import merge_coverage

p=argparse.ArgumentParser();p.add_argument('--bundle',required=True);p.add_argument('--query',required=True);p.add_argument('--revision',required=True);a=p.parse_args()
Runtime().require_gate(C)
if (C/'private/network_worker.lock').exists():raise ValueError('Existing single writer must finish first')
q=next(q for q in active_batch(C)['queries'] if q['name']==a.query)
bp=b.inside(C,a.bundle);bundle=read(bp);consumer=bundle['consumers'][a.query]
for key,file in (('collection','collection.json'),('labels','label_snapshot.json')):
    if sha(C/'derived/stage1d/queries'/a.query/file)!=consumer['inputs'][key]['sha256']:raise ValueError('Current graph/role binding changed')
out=C/'private/integration_protocol_supplement'/a.query/a.revision
out.mkdir(parents=True,exist_ok=True)
binding={'bundle':b.dep(C,bp),'source':b.dep(C,C/'src/stage1d_protocol_supplement.py')}
if (out/'BINDING.json').exists() and read(out/'BINDING.json')!=binding:raise ValueError('Immutable supplement binding changed')
atomic_json(out/'BINDING.json',binding)
headers=read(b.checked(C,consumer['prepared_context']['headers']));headers={int(k):v for k,v in headers.items()}
pending={}
for gap in consumer['protocol_gaps']:
    pending.setdefault(gap['address'],[]).append(dict(address=gap['address'],role='NON_TERMINAL_MODEL_ACCOUNT',ledger_start_block=gap['start_block'],ledger_end_block=gap['end_block']))
results=[];coverage=[];events=[];i=0
os.environ['HTTP_PROXY']='http://127.0.0.1:7890';os.environ['HTTPS_PROXY']='http://127.0.0.1:7890'
while any(pending.values()):
    rows=[pending[address].pop(0) for address in sorted(pending) if pending[address]]
    ready,domain=_dates(C,q,rows,headers)
    if domain['unready']:raise ValueError('Existing scope cannot prove exact protocol query dates')
    folder=out/str(i);folder.mkdir(exist_ok=True)
    atomic_json(folder/'SCOPE_AND_DATES.json',domain)
    sql=build_protocol_sql({'rows':ready},domain['start_date_utc'],domain['end_date_utc'])
    intervals=[dict(query_id=q['query_id'],kind='context',address=r['address'],start_block=r['ledger_start_block'],end_block=r['ledger_end_block']) for r in ready]
    freeze=save_sql(C,sql,'context',q,intervals,[b.dep(C,bp),b.dep(C,folder/'SCOPE_AND_DATES.json'),binding['source'],b.dep(C,C/'src/context_queries_r3.py')])
    print(json.dumps({'phase':'EXECUTE_COMPACT_PROTOCOL_ONLY','index':i,'accounts':len(rows),'freeze':b.dep(C,freeze)}),flush=True)
    try:result=execute_sql(C,freeze,a.query,'repair_protocol_'+a.query)
    except Exception as exc:
        result={'status':'PROTOCOL_SUPPLEMENT_BLOCKED','error_class':type(exc).__name__,'reason':str(exc)}
    atomic_json(folder/'EXECUTION.json',result);results.append(result)
    if result.get('status')=='COMPLETED_EXPORTED':
        added=result_rows(C,result['job_folder'])
        if any(r.get('record_type') not in {'withdrawal','fee_recipient','protocol_credit'} for r in added):raise ValueError('Protocol-only source returned different kind')
        ids=['sha256:'+sha(freeze),'sha256:'+sha(Path(result['job_folder'])/'job.json')]
        events.extend(dict(r,evidence_ids=sorted(set(r.get('evidence_ids',[])+ids))) for r in added)
        coverage.extend(dict(address=r['address'],asset='ETH',data_type=b.PROTOCOL,start_block=r['ledger_start_block'],end_block=r['ledger_end_block'],status='COMPLETE',pagination_complete=True,provider_frozen_scope=True,date_domain_verified=True,block_domain_verified=True,evidence_ids=ids) for r in ready)
    atomic_json(out/'MATERIAL.json',{'events':events,'coverage':merge_coverage(coverage),'source_refs':[binding['bundle'],b.dep(C,folder/'EXECUTION.json')],'no_ordinary_ledger_coverage_promoted':True})
    i+=1
summary={'status':'PROTOCOL_SUPPLEMENT_FINISHED','query':a.query,'results':results,'protocol_rows':len(events),'covered_intervals':len(coverage),'material':b.dep(C,out/'MATERIAL.json')}
atomic_json(out/'RECEIPT.json',summary)
print(json.dumps({k:v for k,v in summary.items() if k!='results'}),flush=True)
