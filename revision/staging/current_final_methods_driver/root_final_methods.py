"""Root-only offline final dispatch. Default prepare never registers or solves.

The existing Stage1D experiment API owns methods, timing, outputs and acceptance.
Final requires an explicit four-query terminal disposition, source SHA and claim.
"""
from __future__ import annotations
import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys
import traceback
from unittest.mock import patch

SCHEMA = 'stage1d-final-methods-preparation-v1'
DISPOSITIONS = 'stage1d-final-query-dispositions-v1'
METHODS = ['FULL_INTERVAL','BOUNDED_REACHABILITY','POISON','HAIRCUT',
           'NO_CROSS_TARGET_COUPLING','NO_PROTOCOL_CONTINUATION','BALANCE_INFORMATION_REMOVED']
CLAIM = 'private/STAGE1D_FINAL_METHODS_ONCE.json'


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def digest(value): return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
def read(path): return json.loads(Path(path).read_bytes())
def inside(work, path):
    work=Path(work).resolve(); path=(work/path).resolve()
    if not path.is_relative_to(work) or path.is_symlink(): raise ValueError('Path escaped current work')
    return path

def ref(work, path):
    path=inside(work,path)
    return {'path':path.relative_to(Path(work).resolve()).as_posix(),'sha256':sha(path)}

def checked(work, value):
    if not isinstance(value,dict) or not isinstance(value.get('path'),str) or not isinstance(value.get('sha256'),str):
        raise ValueError('Explicit file path and SHA required')
    path=inside(work,value['path'])
    if sha(path)!=value['sha256']:raise ValueError('Changed input: '+value['path'])
    return read(path)

def unique_refs(values):
    found={}
    for value in values:
        if value['path'] in found and found[value['path']]!=value['sha256']:raise ValueError('Conflicting dependency SHA')
        found[value['path']]=value['sha256']
    return [{'path':k,'sha256':v} for k,v in sorted(found.items())]

def final_mapping(value,names):
    if value.get('schema_version')!=DISPOSITIONS or value.get('final_dispositions') is not True:
        raise ValueError('Explicit terminal four-query dispositions required')
    if set(value.get('queries',{}))!=set(names):raise ValueError('All four final dispositions required')
    for name,item in value['queries'].items():
        if item.get('disposition') not in ('RUN','NOT_RUN') or not item.get('reason','').strip():
            raise ValueError('Explicit final disposition and reason required: '+name)
        if not isinstance(item.get('evidence'),list) or not item['evidence']:
            raise ValueError('Final acquisition/disposition evidence refs required: '+name)
        if item['disposition']=='RUN' and not item.get('context_pointer'):raise ValueError('Current context pointer required')
    return value['queries']

def apis(work):
    path=str(Path(work).resolve()/'src')
    if path not in sys.path:sys.path.insert(0,path)
    import stage1d_experiments as experiments
    import stage1d_closure_context as context
    import stage1d_closure_scope as scopes
    if list(experiments.METHODS)!=METHODS:raise ValueError('Frozen seven-method contract changed')
    return experiments,context,scopes

def inspect_context(work,name,pointer_ref,context,scopes):
    pointer=checked(work,pointer_ref); receipt=checked(work,pointer)
    assembly=checked(work,receipt['assembly'])
    # Check the fixed material bytes without rebuilding a model or asking a provider.
    material=checked(work,receipt['material'])
    query,collection,labels,binding=context.load_frozen_inputs(work,receipt['frozen_inputs'],name)
    current_collection=ref(work,'derived/stage1d/queries/'+name+'/collection.json')
    current_labels=ref(work,'derived/stage1d/queries/'+name+'/label_snapshot.json')
    if context._hash(checked(work,current_collection))!=binding['collection_canonical_sha256'] or context._hash(checked(work,current_labels))!=binding['label_canonical_sha256']:
        raise ValueError('Frozen collection/roles are no longer current')
    if receipt.get('source_sha256')!={p.name:sha(p) for p in sorted((Path(work)/'src').glob('*.py'))}:
        raise ValueError('Assembly source differs; root must refresh current assembly before final')
    args=assembly.get('registration_arguments')
    if not isinstance(args,dict) or not args.get('document') or assembly.get('status')=='MODEL_BLOCKED':
        raise ValueError('No runnable current context; never register a None/empty model')
    if (assembly.get('query_id')!=query['query_id'] or assembly.get('scope_hash')!=query['scope_hash']
        or args.get('query_id')!=query['query_id'] or args.get('scope')!=binding['scope']
        or args.get('label_snapshot')!=labels or args.get('document')!=assembly['context_result']['model_input']
        or args.get('context_evidence')!=assembly['context_evidence']):raise ValueError('Current context registration binding differs')
    evidence=args['context_evidence']
    if any(evidence['binding'].get(k)!=binding[k] for k in binding if k!='dependencies'):
        raise ValueError('Context candidate/label/scope binding differs')
    if evidence.get('material_canonical_sha256')!=context._hash(material):raise ValueError('Context material differs')
    status=assembly['context_result']['completion_status']
    mapped='PARTIAL_CONTEXT_WITH_EXPLICIT_ASSUMPTIONS' if status=='PARTIAL_CONTEXT_WITH_EXPLICIT_GAPS' else status
    if args['context_status']!=mapped or evidence.get('adapter_status')!=status:raise ValueError('Context completeness was changed')
    if status=='FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE' and (assembly.get('missing_points') or evidence.get('gaps') or args['document'].get('gaps')):
        raise ValueError('FULL context carries unpropagated gaps')
    from stage1c_output_contract import expected_domains
    domains=expected_domains(args['document'])  # Structural/constraint-domain checks only; no optimizer.
    if not domains['passed']:raise ValueError('Current method domain rejected: '+str(domains['errors']))
    deps=unique_refs([pointer_ref,pointer,receipt['assembly'],receipt['material'],receipt['frozen_inputs'],
        current_collection,current_labels]+args.get('dependencies',[]))
    for dependency in deps:checked(work,dependency)
    summary={'query_id':query['query_id'],'scope_hash':query['scope_hash'],'scope_id':query['scope_id'],
        'window_mode':query['scope']['window_mode'],'context_status':args['context_status'],
        'candidate_sha256':binding['collection_canonical_sha256'],'label_sha256':binding['label_canonical_sha256'],
        'context_evidence_sha256':context._hash(evidence),'document_sha256':context._hash(args['document']),
        'target_addresses':domains['addresses'],'interval_events':domains['interval_events'],
        'baseline_events_count':len(domains['baseline_events']),
        'candidate_completion_status':collection.get('status'),
        'candidate_has_gaps_or_frontier':bool(collection.get('gaps') or collection.get('unresolved_frontier')),
        'gaps':evidence.get('gaps',[]),
        'missing_points':assembly.get('missing_points',[]),'dependencies':deps,
        'empty_target_is_not_missing_data_or_service_label':not bool(domains['addresses'])}
    return summary,args

def prepare(work,dispositions_ref=None):
    work=Path(work).resolve();experiments,context,scopes=apis(work)
    batch_path=scopes.active_batch_path(work);batch=read(batch_path)
    names=[q['name'] for q in batch['queries']]
    if len(names)!=4 or len(set(names))!=4:raise ValueError('Four active frozen queries required')
    source=experiments.inherited.source_inventory(work)
    proof=read(work/scopes.PROOF); policy=checked(work,proof['policy'])
    method=policy['method_contract']
    if method['methods']!=METHODS or method['warmup']!=1 or method['timed_repetitions']!=5:raise ValueError('Authorized method/timing policy differs')
    terminal=dispositions_ref is not None
    mapping=final_mapping(checked(work,dispositions_ref),names) if terminal else {
        'lifi_src001':{'disposition':'REVIEW_ONLY','context_pointer':ref(work,'derived/stage1d/queries/lifi_src001/CURRENT_CLOSURE_CONTEXT.json')}}
    inspections={};deps=[ref(work,batch_path),proof['policy'],ref(work,work/scopes.PROOF)]
    if dispositions_ref:deps.append(dispositions_ref)
    for name,item in mapping.items():
        query=next(q for q in batch['queries'] if q['name']==name)
        evidence=item.get('evidence',[])
        for value in evidence:checked(work,value)
        deps+=evidence
        if item['disposition'] in ('RUN','REVIEW_ONLY'):
            summary,_=inspect_context(work,name,item['context_pointer'],context,scopes)
            inspections[name]=summary;deps+=summary['dependencies']
        if terminal:
            status=item.get('acquisition_status')
            if status not in experiments.ACQUISITION|{'EVIDENCE_CONFLICT_MODEL_BLOCKED'}:raise ValueError('Explicit supported acquisition status required')
            if item['disposition']=='RUN':
                if status in {'NOT_STARTED','ERROR','IDENTITY_CONFLICT','EVIDENCE_CONFLICT_MODEL_BLOCKED'}:raise ValueError('Unavailable acquisition cannot run')
                if status=='COMPLETE_FULL_SCOPE' and query['scope']['window_mode']!='REFERENCE_FULL':raise ValueError('Reduced W cannot become FULL')
                if status=='COMPLETE_REDUCED_SCOPE' and query['scope']['window_mode']=='REFERENCE_FULL':raise ValueError('FULL scope is not reduced')
                if status in {'COMPLETE','COMPLETE_FULL_SCOPE','COMPLETE_REDUCED_SCOPE'} and (
                    inspections[name]['candidate_completion_status']!='COMPLETED_WITHIN_DECLARED_SCOPE'
                    or inspections[name]['candidate_has_gaps_or_frontier']):
                    raise ValueError('Incomplete current candidates cannot be promoted to complete acquisition')
    return {'schema_version':SCHEMA,'status':'PREPARED_FOR_EXPLICIT_ROOT_FINAL' if terminal else 'PREPARED_LIFI_REVIEW_ONLY',
        'final_dispositions_supplied':terminal,'final_dispositions_ref':dispositions_ref,'query_names':names,
        'inspections':inspections,'dependencies':unique_refs(deps),'active_batch_ref':ref(work,batch_path),
        'source_inventory':source,'source_inventory_sha256':digest(source),'driver_sha256':sha(__file__),
        'method_contract':method,'solver_execution_policy':experiments.SOLVER_EXECUTION_POLICY,
        'methods':METHODS,'warmups':1,'timed_repetitions':5,'registration_calls':0,'solver_calls':0,
        'qualification':'Preparation only. No query terminal state, method amount, target label, final freeze or external acceptance is inferred.'}

def claim_once(work,plan_hash,batch,output):
    work=Path(work).resolve();path=inside(work,CLAIM)
    if path.exists() or inside(work,batch).exists() or inside(work,output).exists():
        raise ValueError('Final claim/batch/results already exist; preserve them, do not rerun or initialize')
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x',encoding='utf-8') as stream:json.dump({'schema_version':'stage1d-final-methods-once-v1',
        'plan_sha256':plan_hash,'batch':str(batch),'output':str(output),'driver_sha256':sha(__file__),
        'status':'CLAIMED_NO_AUTOMATIC_RETRY'},stream,indent=2)
    return path

def execute_once(work,plan,plan_hash,source_hash,root_final_once=False):
    if not root_final_once or plan.get('final_dispositions_supplied') is not True:
        raise ValueError('Final requires root explicit once flag and four terminal dispositions')
    if plan.get('driver_sha256')!=sha(__file__):raise ValueError('Driver changed after preparation')
    current=prepare(work,plan['final_dispositions_ref'])
    if current!=plan or source_hash!=current['source_inventory_sha256']:
        raise ValueError('Final source or inputs differ from reviewed preparation')
    experiments,context,scopes=apis(work);work=Path(work).resolve()
    base='derived/stage1d/final_methods/'+plan_hash
    batch,output=inside(work,base+'/batch'),inside(work,base+'/results')
    claim=claim_once(work,plan_hash,batch,output)
    status={'schema_version':'stage1d-final-methods-driver-receipt-v1','plan_sha256':plan_hash,
        'source_inventory_sha256':source_hash,'source_inventory':current['source_inventory'],
        'claim':ref(work,claim),'batch':batch.relative_to(work).as_posix(),'output':output.relative_to(work).as_posix(),
        'external_acceptance':'PENDING_REVIEW','automatic_rerun':False}
    try:
        active=scopes.active_batch(work)
        experiments.initialize_batch(work,batch,active['queries'],policy={'final_dispositions':True,
            'active_batch_ref':current['active_batch_ref'],'driver_sha256':sha(__file__)})
        experiments.immutable(batch/'FINAL_PREPARATION.json',current)
        terminal_map=checked(work,current['final_dispositions_ref'])
        experiments.immutable(batch/'FINAL_DISPOSITIONS.json',terminal_map)
        mapped=terminal_map['queries']
        for query in active['queries']:
            name=query['name'];item=mapped[name]
            dependencies=unique_refs(current['dependencies'])
            if item['disposition']=='RUN':
                _,args=inspect_context(work,name,item['context_pointer'],context,scopes)
                args=copy.deepcopy(args);args['dependencies']=unique_refs(args.get('dependencies',[])+dependencies)
                experiments.register_document(work,batch,acquisition_status=item['acquisition_status'],**args)
            else:
                experiments.record_unavailable(work,batch,query['query_id'],item['acquisition_status'],item['reason'],
                    evidence={'schema_version':'stage1d-final-explicit-not-run-v1','final_disposition':copy.deepcopy(item)},
                    scope=query['scope'],dependencies=dependencies)
        frozen=experiments.freeze_batch(work,batch,version='window-semantic-final-'+source_hash[:16])
        status['execution_freeze_sha256']=sha(batch/'EXECUTION_FREEZE.json')
        if frozen['source_inventory']!=current['source_inventory']:raise ValueError('Source changed while freezing')
        status['results_summary']=experiments.run_batch(work,batch,output)
    except BaseException as exc:
        status.update(status='FAILED_PRESERVED_NO_RETRY',exception=type(exc).__name__+': '+str(exc),traceback=traceback.format_exc())
    # Always let the existing final saved-result validator record normal failures.
    status['final_validator']=experiments.validate_saved_batch(work,batch,output)
    status['passed']=status['final_validator']['passed'] and 'exception' not in status
    status.setdefault('status','PASS' if status['passed'] else 'FAIL')
    experiments.write(work/(base+'/FINAL_DRIVER_RECEIPT.json'),status)
    return status

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work',type=Path);parser.add_argument('--action',choices=['prepare','final'],default='prepare')
    parser.add_argument('--plan',type=Path,default=Path(__file__).parent/'LIFI_PREPARATION.json')
    parser.add_argument('--dispositions-ref',type=Path,help='JSON object with current-work path and SHA of explicit four-query terminal map')
    parser.add_argument('--plan-sha256');parser.add_argument('--source-inventory-sha256')
    parser.add_argument('--root-final-once',action='store_true')
    args=parser.parse_args()
    with patch('socket.create_connection',side_effect=RuntimeError('Final methods are offline')),patch('socket.socket.connect',side_effect=RuntimeError('Final methods are offline')):
        if args.action=='prepare':
            plan=prepare(args.work,read(args.dispositions_ref) if args.dispositions_ref else None)
            # Default and supported preparation outputs remain in this staging directory.
            if not args.plan.resolve().is_relative_to(Path(__file__).resolve().parent):raise ValueError('Prepare writes staging only')
            with args.plan.open('x',encoding='utf-8') as stream:json.dump(plan,stream,indent=2,ensure_ascii=False)
            print(json.dumps({'status':plan['status'],'plan_sha256':sha(args.plan),'source_inventory_sha256':plan['source_inventory_sha256'],'solver_calls':0}))
        else:
            if not args.plan_sha256 or sha(args.plan)!=args.plan_sha256:raise ValueError('Explicit reviewed plan SHA required')
            result=execute_once(args.work,read(args.plan),args.plan_sha256,args.source_inventory_sha256,args.root_final_once)
            print(json.dumps({'status':result['status'],'passed':result['passed']}))
            if not result['passed']:raise SystemExit(1)

if __name__=='__main__':main()
