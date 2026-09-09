"""Append-only Stage1D input registration and the inherited seven-method runner.

Acquisition completion, context sufficiency and software acceptance are separate.
The four predeclared sources remain in the outer domain even without a model.
No reference answers enter dispatch; all methods receive the same observed JSON.
"""
from __future__ import annotations
import argparse, copy, csv, hashlib, json, platform, time
from pathlib import Path
import run_stage1c as inherited
from stage1c_result_gate import scientific_receipt_projection

METHODS=inherited.METHODS
SOLVER_EXECUTION_POLICY={
    'schema_version':'stage1d-solver-execution-policy-v1','stage1d_empty_target_recovery':True,
    'policy':'STAGE1D_EMPTY_FIXED_TARGET_CERTIFICATE_RETRY_V1',
    'eligibility':'EMPTY_FIXED_TARGET_UNION_AND_EXACTLY_FIXED_FEASIBILITY_OBJECTIVE',
    'trigger':'ORIGINAL_EXACT_CERTIFICATE_FAILURE','max_additional_attempts_per_endpoint':1,
    'initial_method':'highs','initial_presolve':True,'retry_method':'highs-ds','retry_presolve':False,
    'primal_feasibility_tolerance':'1e-9','dual_feasibility_tolerance':'1e-9',
    'time_budget_rule':'ORIGINAL_ENDPOINT_BUDGET_SHARED_WITH_RETRY',
    'constraints_objective_and_exact_audits_unchanged':True}
CONTEXT_RUNNABLE={'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE','PARTIAL_CONTEXT_WITH_EXPLICIT_ASSUMPTIONS','NO_OBSERVED_TARGET'}
ACQUISITION={'COMPLETE','COMPLETE_FULL_SCOPE','COMPLETE_REDUCED_SCOPE','ACQUISITION_PARTIAL','PARTIAL_FULL_SCOPE','NOT_STARTED','ERROR','IDENTITY_CONFLICT'}
SCHEMA='stage1d-experiment-batch-v1'
read=inherited.read
write=inherited.write
file_hash=inherited.file_hash
canonical=inherited.canonical
digest=inherited.digest

def safe(tree,path,exists=True):
    from validate_review_bundle_r1 import input_path
    return input_path(Path(tree).resolve(),path,exists=exists)

def relative(tree,path):
    tree=Path(tree).resolve();path=Path(path).resolve()
    if not path.is_relative_to(tree):raise ValueError('Batch inputs must be portable within tree')
    return path.relative_to(tree).as_posix()

def immutable(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    payload=canonical(value)
    if path.exists():
        if path.read_bytes()!=payload:raise ValueError('Immutable input already exists: '+path.name)
    else:
        with path.open('xb') as stream:stream.write(payload)
    return file_hash(path)

def _open_batch(tree,batch_dir,mutable=False):
    tree=Path(tree).resolve();batch=Path(batch_dir).resolve();relative(tree,batch)
    domain=read(batch/'QUERY_DOMAIN.json')
    if domain.get('schema_version')!=SCHEMA or len(domain.get('queries',[]))!=4:raise ValueError('Exactly four predeclared queries required')
    if mutable and (batch/'EXECUTION_FREEZE.json').exists():raise ValueError('Execution freeze is immutable; register before freezing')
    return tree,batch,domain

def initialize_batch(tree,batch_dir,declared_queries,policy=None):
    tree=Path(tree).resolve();batch=Path(batch_dir).resolve();relative(tree,batch)
    ids=[q['query_id'] for q in declared_queries]
    if len(ids)!=4 or len(set(ids))!=4:raise ValueError('Stage1D requires four distinct declared source queries')
    for q in declared_queries:
        if not q.get('seed_event_id') or not q.get('name'):raise ValueError('Declared source identity incomplete')
        from validate_review_bundle_r1 import safe_name
        safe_name(q['name'])
    value={'schema_version':SCHEMA,'queries':copy.deepcopy(declared_queries),'policy':copy.deepcopy(policy or {}),
           'outcomes_used_for_selection':False,'external_acceptance':'PENDING_REVIEW'}
    immutable(batch/'QUERY_DOMAIN.json',value)
    return value

def _query(domain,query_id):
    matches=[q for q in domain['queries'] if q['query_id']==query_id]
    if len(matches)!=1:raise ValueError('Query is outside the predeclared four-source domain')
    return matches[0]

def _scope(scope,query_id):
    """Recompute collector identity, also binding all serialized scope evidence."""
    from collector import Scope
    from dataclasses import asdict
    if not isinstance(scope,dict) or scope.get('query_id')!=query_id:raise ValueError('Scope query identity mismatch')
    if 'window_mode' not in scope or not scope.get('scope_id'):raise ValueError('New scope must explicitly name its mode and identity')
    if 'start_time_utc' in scope:obj=Scope.from_policy(scope)
    else:
        fields=Scope.__dataclass_fields__
        obj=Scope(**{k:v for k,v in scope.items() if k in fields})
    actual=obj.scope_hash if not callable(getattr(obj,'scope_hash',None)) else obj.scope_hash()
    if not isinstance(actual,str) or len(actual)!=64:raise ValueError('Collector must expose a verified SHA-256 scope_hash')
    if scope.get('scope_hash') not in (None,actual):raise ValueError('Supplied scope_hash disagrees with collector')
    normalized=asdict(obj)
    if normalized.get('scope_id')!=scope['scope_id']:raise ValueError('Collector scope_id mismatch')
    return normalized,actual

def _dependencies(tree,dependencies):
    found=[]
    for item in dependencies or []:
        path=safe(tree,item['path']);actual=file_hash(path)
        if item.get('sha256') not in (None,actual):raise ValueError('Replay dependency hash mismatch')
        found.append({'path':relative(tree,path),'sha256':actual})
    if len({x['path'] for x in found})!=len(found):raise ValueError('Duplicate dependency path')
    return found

def register_document(tree,batch_dir,query_id,document,scope,label_snapshot,context_evidence,
                      acquisition_status,context_status,dependencies=None):
    tree,batch,domain=_open_batch(tree,batch_dir,True);query=_query(domain,query_id)
    if acquisition_status not in ACQUISITION or acquisition_status in {'NOT_STARTED','ERROR','IDENTITY_CONFLICT'}:raise ValueError('A model cannot replace an unstarted or failed acquisition')
    if context_status not in CONTEXT_RUNNABLE:raise ValueError('Context does not declare a runnable conditional model')
    normalized,scope_hash=_scope(scope,query_id)
    if acquisition_status=='COMPLETE_FULL_SCOPE' and normalized['window_mode']!='REFERENCE_FULL':raise ValueError('A reduced scope cannot complete the full reference scope')
    if acquisition_status=='COMPLETE_REDUCED_SCOPE' and normalized['window_mode'] not in {'ARRIVAL_90D','QUERY_ARRIVAL_WINDOW_SECONDS_V1'}:raise ValueError('Reduced completion requires its own reduced scope')
    doc=copy.deepcopy(document)
    if doc.get('query_id')!=query_id:raise ValueError('Observed document query identity mismatch')
    if 'transactions' in doc:
        seeds=[f for tx in doc['transactions'] for f in tx.get('flows',[]) if f.get('role')=='SEED']
        seed_key='event_id'
    else:seeds=[e for e in doc['events'] if e.get('kind')=='seed'];seed_key='id'
    if len(seeds)!=1 or seeds[0][seed_key]!=query['seed_event_id']:raise ValueError('Exact registered seed event identity changed')
    if str(seeds[0]['amount_raw'])!=str(query['seed_amount_raw']):raise ValueError('Exact registered seed amount changed')
    label_hash=digest(canonical(label_snapshot));context_hash=digest(canonical(context_evidence))
    sid=query['name']+'__'+digest(canonical(normalized['scope_id']))[:16]
    folder=batch/'registrations'/sid
    # The inherited contract binds document sample identity as well as query
    # identity. Scope versions are separate samples; retain the supplied display
    # identity as provenance while assigning this registration's sample name.
    doc['registration_original_identity']={k:doc[k] for k in ('scenario_id','name','sample_id') if k in doc}
    for key in ('scenario_id','name','sample_id'):
        if key in doc:doc[key]=sid
    doc['sample_id']=sid
    for key,value in {'scope_id':normalized['scope_id'],'scope_hash':scope_hash,'label_version':label_hash,'context_evidence_sha256':context_hash}.items():
        if key in doc and doc[key]!=value:raise ValueError('Observed '+key+' differs from frozen evidence')
        doc[key]=value
    if acquisition_status in {'ACQUISITION_PARTIAL','PARTIAL_FULL_SCOPE'}:
        if context_status=='FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE':
            # Full accounting can exist inside an explicitly partial observation
            # scope, but the missing frontier must still accompany that scope.
            if not context_evidence.get('unresolved_frontier') and not context_evidence.get('gaps'):raise ValueError('Partial acquisition must retain its missing frontier or gaps')
        if not doc.get('assumptions') and not doc.get('gaps'):raise ValueError('Partial model must state its limitations')
    from stage1c_output_contract import expected_domains
    domains=expected_domains(doc)
    if not domains['passed']:raise ValueError('Invalid common observed input: '+json.dumps(domains['errors']))
    if context_status=='NO_OBSERVED_TARGET' and domains['addresses']:raise ValueError('No-target status contradicts observed service targets')
    # Freeze facts and evidence before method invocation. No reference input is
    # part of these APIs, and no dispatch occurs during registration.
    bindings={}
    for filename,value in [('MODEL_INPUT.json',doc),('SCOPE.json',normalized),('LABEL_SNAPSHOT.json',label_snapshot),('CONTEXT_EVIDENCE.json',context_evidence)]:
        bindings[filename]=immutable(folder/filename,value)
    row={'sample_id':sid,'query_id':query_id,'incident_id':query.get('incident_id'),'kind':'stage1d_real','source_layer':query.get('seed_rule','S2'),
         'scope_id':normalized['scope_id'],'scope_hash':scope_hash,'window_mode':normalized['window_mode'],
         'input_fact_hash':bindings['MODEL_INPUT.json'],'observed_sha256':bindings['MODEL_INPUT.json'],
         'observed_path':relative(tree,folder/'MODEL_INPUT.json'),'scope_path':relative(tree,folder/'SCOPE.json'),
         'label_version':label_hash,'context_evidence_sha256':context_hash,'label_path':relative(tree,folder/'LABEL_SNAPSHOT.json'),
         'context_path':relative(tree,folder/'CONTEXT_EVIDENCE.json'),'bindings':bindings,
         'acquisition_status':acquisition_status,'context_status':context_status,'method_execution':'REGISTERED',
         'zero_hop':query['seed_event_id'] in domains['interval_events'],
         'dependencies':_dependencies(tree,dependencies),'assumptions':doc.get('assumptions',[]),'gaps':doc.get('gaps',[])}
    immutable(folder/'REGISTRATION.json',row)
    return row

def _verify_unavailable(tree,batch,domain,value,models):
    _query(domain,value['query_id'])
    if value.get('schema_version')!='stage1d-unavailable-binding-v1' or value.get('acquisition_status') not in ACQUISITION|{'EVIDENCE_CONFLICT_MODEL_BLOCKED'} or value.get('method_execution')!='NOT_RUN':
        raise ValueError('Unavailable query requires its explicit frozen evidence contract')
    if not isinstance(value.get('reason'),str) or not value['reason'].strip():raise ValueError('Unavailable query reason missing')
    evidence=value.get('evidence');dependencies=value.get('dependencies')
    if not isinstance(evidence,dict) or not isinstance(evidence.get('schema_version'),str) or not evidence['schema_version'] or not isinstance(dependencies,list) or not dependencies:
        raise ValueError('Unavailable external evidence needs a schema and bound replay dependencies')
    actual=_dependencies(tree,dependencies)
    if actual!=dependencies or evidence.get('replay_dependencies')!=dependencies or value.get('evidence_sha256')!=digest(canonical(evidence)):
        raise ValueError('Unavailable replay dependency/evidence identity differs')
    if value.get('evidence_basis')=='CURRENT_BATCH_REGISTRATION_ABSENCE':
        expected={'schema_version':'stage1d-registration-absence-v1','query_id':value['query_id'],
                  'observation':'NO_REGISTERED_MODEL_IN_THIS_BATCH','replay_dependencies':[
                      {'path':relative(tree,batch/'QUERY_DOMAIN.json'),'sha256':file_hash(batch/'QUERY_DOMAIN.json')}]}
        if value['acquisition_status']!='NOT_STARTED' or evidence!=expected or any(r['query_id']==value['query_id'] for r in models):
            raise ValueError('Structural NOT_STARTED evidence contradicts registered inputs')
    elif value.get('evidence_basis')!='BOUND_EXTERNAL_REPLAY_EVIDENCE':raise ValueError('Unknown unavailable evidence basis')
    if 'scope' in value:
        normalized,scope_hash=_scope(value['scope'],value['query_id'])
        if normalized!=value['scope'] or scope_hash!=value.get('scope_hash') or normalized['scope_id']!=value.get('scope_id'):raise ValueError('Unavailable scope identity differs')
    elif 'scope_id' in value or 'scope_hash' in value:raise ValueError('Unavailable scope claim has no full scope evidence')
    path=batch/'unavailable'/(digest(canonical(value))+'.json')
    if file_hash(safe(tree,relative(tree,path)))!=digest(canonical(value)):raise ValueError('Unavailable record is not its immutable original')
    return value


def record_unavailable(tree,batch_dir,query_id,status,reason,evidence=None,scope=None,*,dependencies=None):
    tree,batch,domain=_open_batch(tree,batch_dir,True);_query(domain,query_id)
    if status not in ACQUISITION|{'EVIDENCE_CONFLICT_MODEL_BLOCKED'}:raise ValueError('Unknown acquisition/context status')
    if not isinstance(reason,str) or not reason.strip():raise ValueError('Unavailable model requires an explicit reason')
    if evidence is not None and not isinstance(evidence,dict):raise ValueError('Unavailable evidence cannot be a saved PASS string')
    evidence=copy.deepcopy(evidence or {})
    if not evidence and dependencies is None and status=='NOT_STARTED':
        # This is only the locally re-computable absence of a registered model,
        # never a claim that an external provider succeeded or found no target.
        basis='CURRENT_BATCH_REGISTRATION_ABSENCE'
        bound=_dependencies(tree,[{'path':relative(tree,batch/'QUERY_DOMAIN.json')}])
        evidence={'schema_version':'stage1d-registration-absence-v1','query_id':query_id,
                  'observation':'NO_REGISTERED_MODEL_IN_THIS_BATCH','replay_dependencies':bound}
    else:
        basis='BOUND_EXTERNAL_REPLAY_EVIDENCE'
        supplied=evidence.get('replay_dependencies') if dependencies is None else dependencies
        if not isinstance(supplied,list) or not supplied or not isinstance(evidence.get('schema_version'),str) or not evidence['schema_version']:
            raise ValueError('Unavailable external evidence requires a schema and nonempty replay dependencies')
        bound=_dependencies(tree,supplied)
        if dependencies is not None and 'replay_dependencies' in evidence and _dependencies(tree,evidence['replay_dependencies'])!=bound:
            raise ValueError('Explicit and embedded unavailable dependencies disagree')
        evidence['replay_dependencies']=bound
    value={'schema_version':'stage1d-unavailable-binding-v1','query_id':query_id,'acquisition_status':status,'reason':reason,
           'evidence':evidence,'evidence_basis':basis,'evidence_sha256':digest(canonical(evidence)),
           'dependencies':bound,'method_execution':'NOT_RUN'}
    if scope is not None:
        normalized,scope_hash=_scope(scope,query_id);value.update(scope=normalized,scope_id=normalized['scope_id'],scope_hash=scope_hash)
    name=digest(canonical(value))+'.json';immutable(batch/'unavailable'/name,value)
    return value

def freeze_batch(tree,batch_dir,version='stage1d-v1'):
    tree,batch,domain=_open_batch(tree,batch_dir,True)
    rows=[read(p) for p in sorted((batch/'registrations').glob('*/REGISTRATION.json'))]
    unavailable=[read(p) for p in sorted((batch/'unavailable').glob('*.json'))]
    # Missing acquisitions remain explicit NOT_STARTED tasks; they are never
    # promoted to empty target documents or omitted from denominators.
    for q in domain['queries']:
        if not any(r['query_id']==q['query_id'] for r in rows+unavailable):
            unavailable.append(record_unavailable(tree,batch,q['query_id'],'NOT_STARTED','No runnable observation registered'))
    # Validate external failure/not-started evidence before creating an immutable
    # execution freeze, so missing dependencies cannot become a trusted PASS.
    for value in unavailable:_verify_unavailable(tree,batch,domain,value,rows)
    manifest={'schema_version':SCHEMA,'query_count':4,'query_ids':[q['query_id'] for q in domain['queries']],
              'models':rows,'unavailable':unavailable,'method_versions':{m:'stage1d-'+version for m in METHODS}}
    inherited_freeze=tree/'EXPERIMENT_FREEZE.json'
    if inherited_freeze.exists():manifest['method_versions']=read(inherited_freeze)['method_versions']
    immutable(batch/'EXPERIMENT_INPUTS.json',manifest)
    bindings=[{'path':relative(tree,p),'sha256':file_hash(p)} for p in sorted(batch.rglob('*.json'))]
    source_inventory=inherited.source_inventory(tree)
    frozen={'schema_version':'stage1d-execution-freeze-v1','version':version,'stage':'Stage1D',
            'query_count':4,'method_versions':manifest['method_versions'],'methods':list(METHODS),'warmups':1,'timed_repetitions':5,
            'input_bindings':bindings,'source_inventory':source_inventory,'reference_not_used_for_execution':True,
            'solver_execution_policy':copy.deepcopy(SOLVER_EXECUTION_POLICY),
            'parent_scientific_freeze_sha256':file_hash(inherited_freeze) if inherited_freeze.exists() else None}
    immutable(batch/'EXECUTION_FREEZE.json',frozen)
    verify_freeze(tree,batch)
    return frozen

def verify_freeze(tree,batch_dir):
    tree,batch,domain=_open_batch(tree,batch_dir)
    frozen=read(batch/'EXECUTION_FREEZE.json')
    if frozen.get('schema_version')!='stage1d-execution-freeze-v1':raise ValueError('Wrong execution freeze')
    if frozen.get('solver_execution_policy')!=SOLVER_EXECUTION_POLICY:raise ValueError('Stage1D solver execution policy changed')
    if frozen.get('source_inventory')!=inherited.source_inventory(tree):raise ValueError('Stage1D execution source/test identity changed')
    if frozen.get('parent_scientific_freeze_sha256') is not None and file_hash(tree/'EXPERIMENT_FREEZE.json')!=frozen['parent_scientific_freeze_sha256']:raise ValueError('Parent scientific freeze changed')
    if frozen.get('methods')!=list(METHODS) or frozen.get('warmups')!=1 or frozen.get('timed_repetitions')!=5:raise ValueError('Method or timing contract changed')
    for item in frozen['input_bindings']:
        if file_hash(safe(tree,item['path']))!=item['sha256']:raise ValueError('Frozen input changed: '+item['path'])
    manifest=read(batch/'EXPERIMENT_INPUTS.json')
    ids=[q['query_id'] for q in domain['queries']]
    if manifest.get('query_ids')!=ids or manifest.get('query_count')!=4:raise ValueError('Predeclared domain changed')
    if frozen.get('method_versions')!=manifest.get('method_versions'):raise ValueError('Method version binding changed')
    if len({r['sample_id'] for r in manifest['models']})!=len(manifest['models']):raise ValueError('Duplicate query/scope registration')
    for value in manifest['unavailable']:_verify_unavailable(tree,batch,domain,value,manifest['models'])
    if any(not any(r['query_id']==qid for r in manifest['models']+manifest['unavailable']) for qid in ids):raise ValueError('A predeclared query has no registered disposition')
    for row in manifest['models']:
        _query(domain,row['query_id'])
        doc=read(safe(tree,row['observed_path']));scope,scope_hash=_scope(read(safe(tree,row['scope_path'])),row['query_id'])
        if scope_hash!=row['scope_hash'] or doc['scope_hash']!=scope_hash or doc['scope_id']!=row['scope_id'] or scope['scope_id']!=row['scope_id']:raise ValueError('Scope binding mismatch')
        for field,path in [('input_fact_hash','observed_path'),('label_version','label_path'),('context_evidence_sha256','context_path')]:
            if file_hash(safe(tree,row[path]))!=row[field]:raise ValueError('Fact/evidence binding mismatch')
        if doc['query_id']!=row['query_id'] or doc['label_version']!=row['label_version']:raise ValueError('Document identity mismatch')
        _dependencies(tree,row['dependencies'])
    return frozen,manifest

def binding(tree,batch,folder,row,frozen):
    return {'expected_identity':inherited.expected_identity(row,frozen),'scope_id':row['scope_id'],
            'execution_freeze_sha256':file_hash(Path(batch)/'EXECUTION_FREEZE.json'),
            'observed_sha256':file_hash(safe(tree,row['observed_path'])),
            'files':{n:file_hash(folder/n) for n in ('METHOD_RESULTS.json','OUTPUT_CONTRACT.json','EVALUATION.json','ABLATIONS.json','RAW_METHOD_RETURNS.json')}}

def _progress(domain,manifest,index):
    result=[]
    for q in domain['queries']:
        records=[r for r in manifest['models'] if r['query_id']==q['query_id']]
        entries=[r for r in index if r['query_id']==q['query_id']]
        result.append({'query_id':q['query_id'],'name':q['name'],'scope_results':entries,
                       'unavailable':[r for r in manifest['unavailable'] if r['query_id']==q['query_id']],
                       'has_runnable_model':bool(records),'all_registered_methods_accepted':bool(entries) and all(r['passed'] for r in entries),
                       'full_reference_scope_complete':any(r['window_mode']=='REFERENCE_FULL' and r['acquisition_status'] in {'COMPLETE','COMPLETE_FULL_SCOPE'} for r in records)})
    return result

def run_batch(tree,batch_dir,output):
    tree,batch,domain=_open_batch(tree,batch_dir);frozen,manifest=verify_freeze(tree,batch)
    out=Path(output).resolve()
    if out.exists():raise ValueError('Run output must be fresh; failures are retained')
    out.mkdir(parents=True);index=[];flat=[]
    for row in manifest['models']:
        doc=read(safe(tree,row['observed_path']));sid=row['sample_id'];folder=out/'samples'/sid
        offset=hashlib.sha256(sid.encode()).digest()[0]%len(METHODS);order=METHODS[offset:]+METHODS[:offset]
        results={};profiles={};native={}
        begin=time.perf_counter();targets=inherited.observed_targets(doc);physical=inherited.observed_port_facts(doc)
        if targets!=inherited.groups(doc):raise ValueError('Common target domains differ')
        preprocessing=time.perf_counter()-begin
        for method in order:
            value,profile=inherited.measure(doc,method,stage1d_empty_target_recovery=frozen['solver_execution_policy']['stage1d_empty_target_recovery'])
            native[method]={'first_return':profile.pop('raw_method_return'),'failed_attempts':profile.pop('raw_failed_attempts')}
            results[method]=inherited.attach_identity(doc,method,value,row,frozen);profiles[method]=profile
        write(folder/'RAW_METHOD_RETURNS.json',native);write(folder/'METHOD_RESULTS.json',results)
        contract,evaluation,ablations,timing=inherited.evaluate_query(tree,row,frozen,doc,results)
        write(folder/'OUTPUT_CONTRACT.json',contract);write(folder/'EVALUATION.json',evaluation);write(folder/'ABLATIONS.json',ablations)
        write(folder/'EFFICIENCY.json',{'method_order':order,'warmups':1,'timed_repetitions':5,'profiles':profiles,
             'common_preprocessing_seconds':preprocessing,'validation':timing,'online_time_included':False})
        receipt={'passed':evaluation['passed'],'contract_passed':contract['passed'],'binding':binding(tree,batch,folder,row,frozen),'errors':evaluation.get('errors',[])}
        write(folder/'QUERY_ACCEPTANCE.json',receipt)
        entry={k:row[k] for k in ('sample_id','query_id','scope_id','scope_hash','input_fact_hash','window_mode','acquisition_status','context_status')}
        entry.update(path=folder.relative_to(out).as_posix(),passed=evaluation['passed'],contract_passed=contract['passed'],
                     method_statuses={m:v['status'] for m,v in results.items()},query_acceptance_sha256=file_hash(folder/'QUERY_ACCEPTANCE.json'))
        index.append(entry)
        if evaluation['passed']:
            flat.extend(dict(r,scope_id=row['scope_id'],scope_hash=row['scope_hash'],acquisition_status=row['acquisition_status'],context_status=row['context_status']) for r in inherited.flat_results(row,results,targets,physical))
    progress=_progress(domain,manifest,index);passed=all(x['passed'] for x in index)
    summary={'schema_version':'stage1d-results-v1','status':'ACCEPTED' if passed else 'FAIL','passed':passed,
             'query_count':4,'model_count':len(index),'query_progress':progress,'method_results_index':index,
             'all_full_scopes_complete':all(q['full_reference_scope_complete'] for q in progress),
             'execution_freeze_sha256':file_hash(batch/'EXECUTION_FREEZE.json'),'new_research_requests':0,
             'real_amount_accuracy':None,'real_source_amount_ground_truth':None,'external_acceptance':'PENDING_REVIEW',
             'runtime':{'platform':platform.platform(),'python':platform.python_version()}}
    write(out/'RESULTS_INDEX.json',summary);inherited.write_csv(out/'PAIRED_RESULTS.csv',flat)
    verify_freeze(tree,batch)
    return summary

def validate_saved_batch(tree,batch_dir,results):
    errors=[];checks=[];expected_flat=[]
    def fail(code,detail=None):errors.append({'code':code,'detail':detail})
    try:
        tree,batch,domain=_open_batch(tree,batch_dir);frozen,manifest=verify_freeze(tree,batch);out=Path(results).resolve()
        saved=read(out/'RESULTS_INDEX.json');entries=saved['method_results_index'];expected={r['sample_id']:r for r in manifest['models']}
        if len(entries)!=len(expected) or len({r['sample_id'] for r in entries})!=len(entries) or {r['sample_id'] for r in entries}!=set(expected):raise ValueError('Frozen runnable query/scope domain mismatch')
        if saved.get('query_count')!=4 or saved.get('model_count')!=len(expected):fail('BATCH_COUNTS_MISMATCH')
        if saved.get('execution_freeze_sha256')!=file_hash(batch/'EXECUTION_FREEZE.json'):fail('EXECUTION_FREEZE_MISMATCH')
        for entry in entries:
            row=expected[entry['sample_id']];qerrors=[]
            try:
                if any(entry.get(k)!=row[k] for k in ('query_id','scope_id','scope_hash','input_fact_hash','window_mode','acquisition_status','context_status')):raise ValueError('Query index identity differs from trusted input')
                if entry['path']!='samples/'+row['sample_id']:raise ValueError('Unexpected result path')
                folder=safe(out,entry['path']);doc=read(safe(tree,row['observed_path']));methods=read(folder/'METHOD_RESULTS.json')
                native=read(folder/'RAW_METHOD_RETURNS.json')
                if set(native)!=set(METHODS):qerrors.append('NATIVE_METHOD_DOMAIN_MISMATCH')
                for method in METHODS:
                    if methods.get(method,{}).get('status')=='ERROR':continue
                    rebuilt=inherited.attach_identity(doc,method,inherited.normalized(native[method]['first_return']),row,frozen)
                    if rebuilt.get('identity_errors'):qerrors.append('RECOMPUTED_NATIVE_IDENTITY_FAILED:'+method)
                    if inherited.semantic_result(rebuilt)!=inherited.semantic_result(methods[method]):qerrors.append('NATIVE_ENVELOPE_RECOMPUTATION_MISMATCH:'+method)
                # Existing C1-R1 evaluate_query executes the common contract and
                # hard invariants using trusted registered input, never saved PASS.
                contract,evaluation,ablations,_=inherited.evaluate_query(tree,row,frozen,doc,methods)
                for filename,value in [('OUTPUT_CONTRACT.json',contract),('EVALUATION.json',evaluation),('ABLATIONS.json',ablations)]:
                    if scientific_receipt_projection(read(folder/filename))!=scientific_receipt_projection(value):qerrors.append('RECOMPUTATION_MISMATCH:'+filename)
                if not contract['passed'] or not evaluation['passed']:qerrors.append('RECOMPUTED_METHOD_ACCEPTANCE_FAILED')
                if evaluation['passed']:
                    expected_flat.extend(dict(r,scope_id=row['scope_id'],scope_hash=row['scope_hash'],acquisition_status=row['acquisition_status'],context_status=row['context_status']) for r in inherited.flat_results(row,methods,inherited.groups(doc),inherited.observed_port_facts(doc)))
                acceptance=read(folder/'QUERY_ACCEPTANCE.json')
                if acceptance.get('binding')!=binding(tree,batch,folder,row,frozen):qerrors.append('ACCEPTANCE_BINDING_MISMATCH')
                if entry.get('query_acceptance_sha256')!=file_hash(folder/'QUERY_ACCEPTANCE.json'):qerrors.append('ACCEPTANCE_HASH_MISMATCH')
                if acceptance.get('passed') is not True or entry.get('passed') is not True or entry.get('contract_passed') is not True:qerrors.append('SAVED_ACCEPTANCE_FAILED')
                if entry.get('method_statuses')!={m:v['status'] for m,v in methods.items()}:qerrors.append('METHOD_STATUS_MISMATCH')
                efficiency=read(folder/'EFFICIENCY.json')
                if efficiency.get('warmups')!=1 or efficiency.get('timed_repetitions')!=5: qerrors.append('TIMING_CONTRACT_MISMATCH')
                for method in METHODS:
                    reps=efficiency['profiles'][method]['repetitions']
                    if len(reps)!=6 or [r['warmup'] for r in reps]!=[True,False,False,False,False,False]:qerrors.append('TIMING_REPETITIONS_MISSING:'+method)
            except Exception as exc:qerrors.append(type(exc).__name__+': '+str(exc))
            checks.append({'sample_id':row['sample_id'],'passed':not qerrors,'errors':qerrors})
        progress=_progress(domain,manifest,entries)
        if saved.get('query_progress')!=progress:fail('PREDECLARED_PROGRESS_MISMATCH')
        if saved.get('all_full_scopes_complete')!=all(q['full_reference_scope_complete'] for q in progress):fail('FALSE_COMPLETION_CLAIM')
        if saved.get('passed') is not True or saved.get('status')!='ACCEPTED':fail('SAVED_BATCH_FAILED')
        if saved.get('real_amount_accuracy') is not None or saved.get('real_source_amount_ground_truth') is not None:fail('UNAVAILABLE_REAL_TRUTH_CLAIM')
        with (out/'PAIRED_RESULTS.csv').open(encoding='utf-8',newline='') as stream:saved_flat=list(csv.DictReader(stream))
        expected_flat=[{k:'' if v is None else str(v) for k,v in r.items()} for r in expected_flat]
        if sorted(saved_flat,key=canonical)!=sorted(expected_flat,key=canonical):fail('PAIRED_RESULT_RECOMPUTATION_MISMATCH')
    except Exception as exc:fail('BATCH_REVALIDATION_EXCEPTION',type(exc).__name__+': '+str(exc))
    passed=not errors and all(q['passed'] for q in checks)
    return {'schema_version':'stage1d-saved-gate-v1','status':'PASS' if passed else 'FAIL','passed':passed,
            'queries':checks,'errors':errors,'recomputed_common_contract_and_scientific_checks':True,
            'acceptance_means_valid_declared_conditional_models_not_full_acquisition':True}

def write_report(tree,batch_dir,results,output):
    gate=validate_saved_batch(tree,batch_dir,results);output=Path(output)
    write(output.with_suffix('.gate.json'),gate)
    if not gate['passed']:
        output.write_text('Stage1D report rejected: saved outputs failed the common acceptance chain.\n',encoding='utf-8')
        return gate
    summary=read(Path(results)/'RESULTS_INDEX.json')
    lines=['Stage1D paired results','', 'External acceptance: PENDING_REVIEW. Real source amount truth and amount accuracy are unavailable.','']
    for query in summary['query_progress']:
        lines.append(query['name']+': full reference scope complete = '+str(query['full_reference_scope_complete']))
        for row in query['scope_results']:
            lines.append('- '+row['scope_id']+' | '+row['window_mode']+' | '+row['acquisition_status']+' | '+row['context_status']+' | methods accepted = '+str(row['passed']))
        for row in query['unavailable']:lines.append('- '+row['acquisition_status']+': '+row['reason'])
    lines+=['','Independent target upper bounds are not a joint source amount. TxPhish queries retain distinct source identities.','']
    output.write_text('\n'.join(lines),encoding='utf-8');return gate

def evaluate_reference(tree,batch_dir,results,reference_rows,output):
    """Evaluate independently supplied positives only after all results are fixed.

    Each row names query_id, scope_id, label_version and reference_version, plus
    positive_address_assets / positive_event_ids and their full_reference_*
    counterparts. All four lists are explicit, including empty lists. References
    outside this observed scope remain in the full-window denominator. No result
    outside these positive sets becomes a negative label or amount truth.
    """
    from fractions import Fraction
    from stage1c_reports import output_sets
    gate=validate_saved_batch(tree,batch_dir,results)
    if not gate['passed']:raise ValueError('Reference evaluation blocked by common output acceptance failure')
    frozen,manifest=verify_freeze(tree,batch_dir);summary=read(Path(results)/'RESULTS_INDEX.json')
    refs={}
    for ref in reference_rows:
        key=(ref['query_id'],ref['scope_id'])
        if key in refs:raise ValueError('Duplicate query/scope reference domain')
        if not ref.get('reference_version'):raise ValueError('Independent reference version is required')
        for name in ('positive_address_assets','positive_event_ids','full_reference_positive_address_assets','full_reference_positive_event_ids'):
            values=ref.get(name)
            if not isinstance(values,list) or len(values)!=len(set(values)):raise ValueError('Reference domain must be an explicit unique list: '+name)
        for name in ('positive_address_assets','positive_event_ids'):
            if not set(ref[name])<=set(ref['full_reference_'+name]):raise ValueError('Scope positives must belong to the declared full-reference positives')
        refs[key]=ref
    expected={(r['query_id'],r['scope_id']) for r in manifest['models']}
    if set(refs)!=expected:raise ValueError('Reference evaluation must retain every registered query/scope, with explicit empty domains')
    indexed={r['sample_id']:r for r in summary['method_results_index']};rows=[]
    for row in manifest['models']:
        ref=refs[(row['query_id'],row['scope_id'])]
        if ref.get('label_version')!=row['label_version']:raise ValueError('Reference positives were not frozen under the compared label version')
        methods=read(safe(Path(results).resolve(),indexed[row['sample_id']]['path'])/'METHOD_RESULTS.json');records={}
        for method,value in methods.items():
            addresses,events=output_sets(value)
            supported=value['status']=='COMPLETED'
            def metric(predicted,positive):
                target=set(positive)
                hits=len(predicted&target) if supported else None
                return {'positive_denominator':len(target),'true_positive_count':hits,
                        'recall':str(Fraction(hits,len(target))) if supported and target else None,
                        'no_positive_denominator':not target,'false_positive_count':None,'precision':None}
            records[method]={'status':value['status'],'output_kind':value['output_kind'],
                  'scope_address_positive_recall':metric(addresses,ref['positive_address_assets']),
                  'scope_event_positive_recall':metric(events,ref['positive_event_ids']),
                  'full_reference_address_positive_recall':metric(addresses,ref['full_reference_positive_address_assets']),
                  'full_reference_event_positive_recall':metric(events,ref['full_reference_positive_event_ids']),
                  'real_amount_accuracy':None,'real_amount_ground_truth':None}
        rows.append({'query_id':row['query_id'],'scope_id':row['scope_id'],'scope_hash':row['scope_hash'],
                     'label_version':row['label_version'],'reference_version':ref['reference_version'],
                     'acquisition_status':row['acquisition_status'],'methods':records})
    out=Path(output);out.mkdir(parents=True,exist_ok=True)
    ref_hash=immutable(out/'REFERENCE_EVALUATION_INPUTS.json',reference_rows)
    value={'schema_version':'stage1d-reference-positive-evaluation-v1','passed':True,'status':'PASS','reference_input_sha256':ref_hash,
           'method_results_index_sha256':file_hash(Path(results)/'RESULTS_INDEX.json'),'rows':rows,
           'reference_opened_after_fixed_method_results':True,'reference_outside_prediction_is_not_negative':True,
           'txphish_source_allocations_are_not_combined':True,'no_independent_query_upper_bounds_are_added':True,
           'external_acceptance':'PENDING_REVIEW'}
    immutable(out/'REFERENCE_EVALUATION.json',value)
    return value

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--tree',type=Path,required=True);p.add_argument('--batch',required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--results',type=Path);p.add_argument('--action',choices=('run','gate','report'),default='run');args=p.parse_args()
    try:
        batch=safe(args.tree,args.batch)
        if args.action=='run':value=run_batch(args.tree,batch,args.output)
        elif args.action=='report':value=write_report(args.tree,batch,args.results,args.output)
        else:value=validate_saved_batch(args.tree,batch,args.results);write(args.output,value)
        print(json.dumps({'status':value.get('status'),'passed':value['passed']}));return 0 if value['passed'] else 1
    except Exception as exc:
        print(json.dumps({'status':'ERROR','error':type(exc).__name__+': '+str(exc)}));return 1

if __name__=='__main__':raise SystemExit(main())
