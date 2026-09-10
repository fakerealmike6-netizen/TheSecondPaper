"""Frozen offline paired experiments. Observed input only crosses method API.

Use --freeze once after local tests, then --kind min/public --output NEW_DIR.
Evaluation opens hidden and Oracle only after every method result is frozen.
"""
from __future__ import annotations
import argparse, copy, csv, hashlib, json, platform, statistics, sys, time, tracemalloc
from datetime import datetime, timezone
from fractions import Fraction as F
from pathlib import Path
from stage1c_intervals import run_interval, audit_allocation, groups, verify_product, METHODS as INTERVALS
from stage1c_baselines import run_baseline, observed_targets
from stage1c_output_contract import accept_method_results

METHODS=('FULL_INTERVAL','BOUNDED_REACHABILITY','POISON','HAIRCUT','NO_CROSS_TARGET_COUPLING','NO_PROTOCOL_CONTINUATION','BALANCE_INFORMATION_REMOVED')
def canonical(value): return (json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
def digest(value): return hashlib.sha256(value).hexdigest()
def file_hash(path): return digest(Path(path).read_bytes())
def read(path): return json.loads(Path(path).read_text(encoding='utf-8'))
def write(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(diagnostic_json(value,indent=2)+'\n',encoding='utf-8')

def diagnostic_value(value):
    if isinstance(value,F):return str(value)
    return {'unserializable_type':type(value).__name__,'representation':repr(value)}

def diagnostic_json(value,indent=None):
    try:return json.dumps(value,ensure_ascii=False,sort_keys=True,indent=indent,default=diagnostic_value)
    except (TypeError,ValueError,RecursionError) as exc:
        return json.dumps({'malformed_serialization':type(exc).__name__,'original_type':type(value).__name__,'original_representation':repr(value)},ensure_ascii=False,indent=indent)
def safe_path(tree,name):
    from validate_review_bundle_r1 import input_path
    return input_path(Path(tree).resolve(),name)
def write_csv(path,rows):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    fields=list(dict.fromkeys(k for r in rows for k in r)) or ['status']
    with path.open('w',encoding='utf-8',newline='') as out:
        writer=csv.DictWriter(out,fields);writer.writeheader();writer.writerows(rows)

def source_inventory(tree):
    return [{'path':p.relative_to(tree).as_posix(),'sha256':file_hash(p)}
        for folder in ('src','tests') for p in sorted((tree/folder).glob('*.py'))]

def freeze(tree,version):
    if (tree/'EXPERIMENT_FREEZE.json').exists():
        raise ValueError('Existing scientific freeze is immutable; use --freeze-revision for Stage1C-R1 execution changes')
    from stage1c_controlled import generate_suite
    manifest=generate_suite(tree)
    original=read(tree/'configs/stage1c/config/STAGE1C_POLICY.json')
    public_policy={k:original[k] for k in ('schema_version','stage','checkpoint','authorization_id','research_status','sampling_rules','methods','paired_fairness','controlled','evaluation','next_batch')}
    write(tree/'configs/STAGE1C_EFFECTIVE_POLICY.json',public_policy)
    spec=(tree/'configs/stage1c/notes/METHOD_SPEC_V1.md').read_bytes()
    (tree/'METHOD_SPEC_EFFECTIVE.md').write_bytes(spec)
    controls=[]
    for row in manifest['samples']:
        doc=read(tree/row['observed_path'])
        controls.append({**row,'kind':'controlled','query_id':row['sample_id'],
            'incident_id':'SYNTHETIC_CONTROLLED_V1','source_layer':'SYNTHETIC',
            'input_fact_hash':row['observed_sha256'],'label_version':doc['label_version'],
            'scope_hash':digest(canonical({'sampling':doc['sampling'],'targets':doc['objective_groups'],'source_layer':'SYNTHETIC'}))})
    pilots=[]
    for p in original['real_pilots']:
        path=f'derived/context_pipeline/{p["name"]}/model_input.json'; doc=read(tree/path)
        if len(doc['accounts'])+len(doc.get('anchors',[]))!=p['accepted_counts']['anchors']:
            raise ValueError('Accepted anchor count changed')
        if sum(len(t.get('flows',[])) for t in doc['transactions'])!=p['accepted_counts']['value_events']:
            raise ValueError('Accepted flow count changed')
        if sum(len(t.get('fees',[])) for t in doc['transactions'])!=p['accepted_counts']['fees']:
            raise ValueError('Accepted fee count changed')
        pilots.append({**p,'sample_id':p['name'],'kind':'real','source_layer':'S2','zero_hop':False,
            'observed_path':path,'observed_sha256':file_hash(tree/path),'input_fact_hash':file_hash(tree/path),
            'scope_hash':digest(canonical({'sampling':original['sampling_rules'],'pilot':p,'targets':doc['objective_groups']})),
            'label_version':file_hash(tree/'derived/CONTEXT_TARGETS_ENHANCED.json'),
            'seed_raw':next(f['amount_raw'] for t in doc['transactions'] for f in t['flows'] if f['role']=='SEED'),
            'objective_groups':doc['objective_groups'],'assumptions':doc.get('assumptions',[]),'gaps':doc.get('gaps',[])})
    inputs={'schema_version':'stage1c-experiment-inputs-1.0','controlled':controls,
        'real_private_manifest':'private/REAL_EXPERIMENT_INPUTS.json',
        'real_identity_summary':[{k:p[k] for k in ('sample_id','query_id','incident_id','input_fact_hash','scope_hash','source_layer','label_version','accepted_counts')} for p in pilots],
        'scope':'DEVELOPMENT_FIRST_VALIDATION_NOT_HOLDOUT','reference_never_used_for_method_execution':True,
        'sampling_rules':original['sampling_rules']}
    write(tree/'EXPERIMENT_INPUTS.json',inputs)
    write(tree/'private/REAL_EXPERIMENT_INPUTS.json',pilots)
    frozen={'schema_version':'stage1c-experiment-freeze-1.0','version':version,
        'frozen_at_utc':datetime.now(timezone.utc).isoformat(),'stage':'Stage1C',
        'source_inventory':source_inventory(tree),'inputs_manifest_sha256':file_hash(tree/'EXPERIMENT_INPUTS.json'),
        'effective_policy_sha256':file_hash(tree/'configs/STAGE1C_EFFECTIVE_POLICY.json'),
        'method_spec_sha256':file_hash(tree/'METHOD_SPEC_EFFECTIVE.md'),
        'controlled_manifest_sha256':file_hash(tree/'controlled_v1/MANIFEST.json'),
        'private_inputs_manifest_sha256':file_hash(tree/'private/REAL_EXPERIMENT_INPUTS.json'),
        'warmups':1,'timed_repetitions':5,'methods':list(METHODS),
        'order_rule':'Per-sample SHA256(sample_id)[0] mod 7 rotation; fixed 1 warm-up then 5 timed executions of each method; no cross-method result reuse.',
        'method_versions':{m:version+':'+digest(canonical(source_inventory(tree)))[:16] for m in METHODS},
        'outcomes_read_to_choose_samples':False}
    current=tree/'EXPERIMENT_FREEZE.json'
    if current.exists():
        prior=read(current)
        if prior['version']==version: raise ValueError('Freeze version already exists; corrections need explicit new version')
        write(tree/'freeze_history'/(prior['version']+'.json'),prior)
    write(current,frozen)
    return frozen

def verify_freeze(tree,kind):
    frozen=read(tree/'EXPERIMENT_FREEZE.json')
    if (tree/'REVISION_FREEZE.json').exists():
        from stage1c_revision import verify_revision
        frozen={**frozen,'execution_revision':verify_revision(tree,frozen)}
    elif source_inventory(tree)!=frozen['source_inventory']: raise ValueError('Frozen source/test version changed')
    for name,key in [('EXPERIMENT_INPUTS.json','inputs_manifest_sha256'),('configs/STAGE1C_EFFECTIVE_POLICY.json','effective_policy_sha256'),('METHOD_SPEC_EFFECTIVE.md','method_spec_sha256'),('controlled_v1/MANIFEST.json','controlled_manifest_sha256')]:
        if file_hash(tree/name)!=frozen[key]: raise ValueError('Freeze mismatch '+name)
    inp=read(tree/'EXPERIMENT_INPUTS.json');rows=inp['controlled']
    if kind=='min':
        p=safe_path(tree,inp['real_private_manifest'])
        if file_hash(p)!=frozen['private_inputs_manifest_sha256']:raise ValueError('Private experiment manifest changed')
        rows=rows+read(p)
    for r in rows:
        if file_hash(safe_path(tree,r['observed_path']))!=r['observed_sha256']: raise ValueError('Observed facts changed '+r['sample_id'])
        if r.get('hidden_path') and file_hash(safe_path(tree,r['hidden_path']))!=r['hidden_sha256']: raise ValueError('Hidden evaluation input changed')
    return frozen,rows

def semantic_result(result):
    if not isinstance(result,dict):return result
    return {k:v for k,v in result.items() if k not in ('timing_parts','elapsed_seconds','profile')}

def dispatch(doc,method, *, stage1d_empty_target_recovery=False, stage1d_coordinate_recovery=False):
    if stage1d_coordinate_recovery and method in INTERVALS:
        return run_interval(doc,method,stage1d_empty_target_recovery=stage1d_empty_target_recovery,
                            stage1d_coordinate_recovery=True)
    if stage1d_empty_target_recovery and method in INTERVALS:
        return run_interval(doc,method,stage1d_empty_target_recovery=True)
    return run_interval(doc,method) if method in INTERVALS else run_baseline(doc,method)

def normalized(result):
    if not isinstance(result,dict):raise TypeError('Method return must be an object')
    result=copy.deepcopy(result)
    if result.get('status')=='OK': result['status']='COMPLETED'
    if 'positive_addresses' not in result: result['positive_addresses']=result.get('output_address_ids')
    if result.get('events') is not None and not result.get('task_counts'):
        result['task_counts']={'address_outputs':len(result['addresses']), 'physical_ports_visited':len(result['events']),
                              'joint_outputs':len(result['joint_by_asset']),'endpoint_optimizations':0}
    return result

def observed_port_facts(doc):
    """Physical capacities and evidence only; no source attribution state."""
    facts={}
    if 'transactions' in doc:
        for tx in doc['transactions']:
            for f in tx.get('flows',[]):
                facts[f['event_id']]={'amount_raw':f['amount_raw'],'asset':f.get('asset','ETH'),'evidence_ids':f.get('evidence_ids',[])}
            for f in tx.get('fees',[]):
                facts[f['fee_id']]={'amount_raw':f['amount_raw'],'asset':'ETH','evidence_ids':f.get('evidence_ids',[])}
            for operation in tx.get('conversions', []):
                from stage1d_multiasset_context import conversion_ports
                for name, asset, amount in conversion_ports(operation):
                    if name in facts:
                        raise ValueError('Duplicate physical/semantic observation port')
                    facts[name] = {'amount_raw': amount, 'asset': asset,
                                  'evidence_ids': operation.get('evidence_ids', []),
                                  'unit_id': operation['unit_id'],
                                  'raw_consumed_event_ids': operation['raw_consumed_event_ids']}
    else:
        for e in doc['events']:
            if e['kind']=='conversion':
                for suffix,key,asset in [('input','gross_raw',e['asset']),('refund','refund_raw',e['asset']),('output','output_raw',e['output_asset'])]:
                    facts[e['id']+':'+suffix]={'amount_raw':e.get(key,'0'),'asset':asset,'evidence_ids':[]}
                facts[e['id']+':net']={'amount_raw':str(F(e['gross_raw'])-F(e.get('refund_raw','0'))),'asset':e['asset'],'evidence_ids':[]}
            elif e['kind']=='multioutput':
                facts[e['id']+':input']={'amount_raw':e['gross_raw'],'asset':e['asset'],'evidence_ids':[]}
                for i,o in enumerate(e['outputs']):facts[e['id']+f':output{i}']={'amount_raw':o['amount_raw'],'asset':e['asset'],'evidence_ids':[]}
            else:facts[e['id']]={'amount_raw':e['amount_raw'],'asset':e['asset'],'evidence_ids':[]}
    return facts

def measure(doc,method, *, stage1d_empty_target_recovery=False, stage1d_coordinate_recovery=False):
    before=digest(canonical(doc));rows=[];errors=[];chosen=None;signature=None;raw_first=None;raw_failures=[]
    for repetition in range(6):
        # Separate warm-up Python-allocation high-water measurement; timings
        # exclude tracemalloc, imported-library setup and input I/O.
        if repetition==0: tracemalloc.start()
        start=time.perf_counter()
        raw=None
        try:
            if stage1d_coordinate_recovery:
                raw=dispatch(copy.deepcopy(doc),method,stage1d_empty_target_recovery=stage1d_empty_target_recovery,
                             stage1d_coordinate_recovery=True)
            else:
                raw=dispatch(copy.deepcopy(doc),method,stage1d_empty_target_recovery=True) if stage1d_empty_target_recovery else dispatch(copy.deepcopy(doc),method)
            result=normalized(raw)
        except Exception as exc:
            result={'status':'ERROR','applicability':'ERROR','output_kind':None,
                'addresses':None,'events':None,'joint_by_asset':None,'positive_addresses':None,
                'failure_reason':type(exc).__name__+': '+str(exc)}
        elapsed=time.perf_counter()-start
        if repetition==0:raw_first=copy.deepcopy(raw)
        if result.get('status')=='ERROR':raw_failures.append({'repetition':repetition,'raw_return':copy.deepcopy(raw),'failure':result.get('failure_reason')})
        memory=None
        if repetition==0:
            _,memory=tracemalloc.get_traced_memory();tracemalloc.stop()
        current=digest(diagnostic_json(semantic_result(result)).encode())
        if signature is None: signature=current;chosen=result
        if current!=signature: errors.append('Non-deterministic method output at repetition '+str(repetition))
        rows.append({'repetition':repetition,'warmup':repetition==0,'elapsed_seconds':elapsed,
                     'python_peak_allocated_bytes_warmup_only':memory,
                     'result_semantic_sha256':current,'status':result.get('status','MISSING'),
                     'parts':result.get('timing_parts'),'task_counts':result.get('task_counts')})
    if digest(canonical(doc))!=before:errors.append('Method mutated shared observed input')
    if errors:chosen['status']='ERROR';chosen['execution_errors']=errors
    times=[r['elapsed_seconds'] for r in rows[1:]]
    return chosen,{'repetitions':rows,'median_seconds':statistics.median(times),'min_seconds':min(times),'max_seconds':max(times),
        'warmup_python_peak_bytes':rows[0]['python_peak_allocated_bytes_warmup_only'],
        'memory_scope':'Python allocations during warmup; excludes native SciPy/HiGHS memory. No whole-process memory claim.',
        'different_output_workloads_not_speed_equivalent':True,'deterministic':not errors,
        'raw_method_return':raw_first,'raw_failed_attempts':raw_failures}

def evaluate_controlled(doc,hidden,results):
    from stage1c_oracle import oracle_intervals, tiny_enumeration_check
    oracle=oracle_intervals(doc)
    values=hidden['event_source_amounts_raw']
    hidden_audit=audit_allocation(doc,values)
    full=results['FULL_INTERVAL'];errors=[];comparisons=[]
    target_groups=groups(doc)
    seed=F(next(e['amount_raw'] for e in doc['events'] if e['kind']=='seed'))
    for category in ('addresses','events','joint_by_asset'):
        for key,v in (full.get(category) or {}).items():
            expected=oracle[category].get(key)
            names=target_groups[key] if category=='addresses' else [key] if category=='events' else [e for g,es in target_groups.items() if g.rsplit('|',1)[1]==key for e in es]
            if expected is None and category=='joint_by_asset' and not names:
                expected={'lower_raw':'0','upper_raw':'0'}
            if expected is None: errors.append('Oracle objective missing '+category+':'+key);continue
            h=sum((F(values[e]) for e in names),F(0))
            good=v.get('lower_raw') is not None and v.get('upper_raw') is not None
            endpoint_error=max(abs(F(v[k])-F(expected[k])) for k in ('lower_raw','upper_raw')) if good else None
            covered=good and F(v['lower_raw'])<=h<=F(v['upper_raw'])
            comparisons.append({'category':category,'objective':key,'endpoint_error_raw':str(endpoint_error) if endpoint_error is not None else None,
                'hidden_raw':str(h),'hidden_covered':covered,'width_raw':str(F(v['upper_raw'])-F(v['lower_raw'])) if good else None,
                'width_over_seed':str((F(v['upper_raw'])-F(v['lower_raw']))/seed) if good else None,
                'positive_lower':F(v['lower_raw'])>0 if good else None})
            if endpoint_error!=0 or not covered:errors.append('Full endpoint/coverage mismatch '+category+':'+key)
    positives={g for g,v in oracle['addresses'].items() if F(v['upper_raw'])>0}
    addresses={}
    for method,result in results.items():
        pred=result.get('positive_addresses')
        if pred is None: addresses[method]={'status':result['status'],'recall':None,'false_positive_count':None};continue
        predicted=set(pred);addresses[method]={'status':result['status'],'oracle_positive_count':len(positives),
            'true_positive_count':len(predicted&positives),'false_positive_count':len(predicted-positives),
            'recall':str(F(len(predicted&positives),len(positives))) if positives else None,
            'no_positive_control':not positives}
    haircut=results['HAIRCUT'];haircut_audit=None;haircut_points=[]
    if haircut.get('allocation_raw') is not None:
        haircut_audit=audit_allocation(doc,haircut['allocation_raw'])
        if not haircut_audit['exact_feasible']:errors.append('Haircut full assignment infeasible')
        for group,v in haircut['addresses'].items():
            point=F(v['point_raw']); h=sum((F(values[e]) for e in target_groups[group]),F(0))
            haircut_points.append({'address_asset':group,'absolute_point_error_to_one_hidden_assignment_raw':str(abs(point-h)),
                'normalized_absolute_error':str(abs(point-h)/seed),'point_raw':str(point),'hidden_raw':str(h)})
    tiny=tiny_enumeration_check(doc) if doc['index']<2 else {'status':'NOT_REQUIRED','reason':'Only prespecified two per family require third enumeration'}
    if tiny.get('passed') is False or tiny.get('status')=='FAIL':errors.append('Third enumeration mismatch')
    return {'oracle':oracle,'hidden_exact_feasibility':hidden_audit,'comparisons':comparisons,
        'address_metrics':addresses,'haircut_full_assignment_audit':haircut_audit,'haircut_point_errors':haircut_points,
        'tiny_crosscheck':tiny,'errors':errors,'passed':not errors and hidden_audit['exact_feasible']}

def ablation_comparison(results):
    full=results['FULL_INTERVAL'].get('joint_by_asset') or {};out=[]
    for a,v in full.items():
        if v.get('lower_raw') is None:continue
        independent=(results['NO_CROSS_TARGET_COUPLING'].get('joint_by_asset') or {}).get(a,{})
        relaxed=(results['BALANCE_INFORMATION_REMOVED'].get('joint_by_asset') or {}).get(a,{})
        no_protocol=(results['NO_PROTOCOL_CONTINUATION'].get('joint_by_asset') or {}).get(a,{})
        if any(x.get('lower_raw') is None for x in (independent,relaxed,no_protocol)):
            out.append({'asset':a,'status':'UNRESOLVED','reason':'Ablation error or nonapplicability retained',
                'method_statuses':{m:results[m].get('status') for m in ('NO_CROSS_TARGET_COUPLING','BALANCE_INFORMATION_REMOVED','NO_PROTOCOL_CONTINUATION')}})
            continue
        out.append({'asset':a,'full_lower_raw':v['lower_raw'],'full_upper_raw':v['upper_raw'],
            'independent_lower_raw':independent['lower_raw'],'independent_upper_raw':independent['upper_raw'],
            'independent_upper_excess_raw':str(F(independent['upper_raw'])-F(v['upper_raw'])),
            'independent_upper_vector_jointly_realizable_in_full':F(independent['upper_raw'])==F(v['upper_raw']),
            'balance_relaxed_lower_raw':relaxed['lower_raw'],'balance_relaxed_upper_raw':relaxed['upper_raw'],
            'balance_nested_endpoints':F(relaxed['lower_raw'])<=F(v['lower_raw'])<=F(v['upper_raw'])<=F(relaxed['upper_raw']),
            'no_protocol_lower_raw':no_protocol['lower_raw'],'no_protocol_upper_raw':no_protocol['upper_raw'],
            'protocol_result_changed':any(F(v[k])!=F(no_protocol[k]) for k in ('lower_raw','upper_raw'))})
    return out

def expected_identity(row,frozen):
    return {k:row[k] for k in ('sample_id','query_id','input_fact_hash','scope_hash','label_version')} | {'method_versions':frozen['method_versions']}

def attach_identity(doc,method,value,row,frozen):
    """Validate native algorithm metadata before adding the execution envelope."""
    from stage1c_baselines import VERSION as BASELINE_VERSION, _hash
    value=copy.deepcopy(value);errors=[];native={}
    expected=expected_identity(row,frozen)
    native_expected={'method_id':method,'method_version':BASELINE_VERSION,'query_or_sample_id':doc.get('query_id',doc.get('scenario_id')),'input_fact_hash':_hash(doc)} if method not in INTERVALS else {}
    for key,want in native_expected.items():
        native[key]=value.get(key)
        if value.get('status')!='ERROR' and (key not in value or value[key]!=want):
            errors.append({'code':'NATIVE_IDENTITY_MISMATCH','method':method,'path':key,'expected':want,'actual':value.get(key)})
    envelope={**{k:v for k,v in expected.items() if k!='method_versions'},'method_id':method,'method_version':expected['method_versions'][method]}
    for key,want in envelope.items():
        if key in value and key not in native_expected and value[key]!=want:
            errors.append({'code':'EXECUTION_IDENTITY_MISMATCH','method':method,'path':key,'expected':want,'actual':value[key]})
        value[key]=want
    value['native_identity']=native
    value['identity_errors']=errors
    return value

def evaluate_real(doc,row,results):
    point=results['HAIRCUT']
    audit=audit_allocation(doc,point['allocation_raw']) if point.get('allocation_raw') is not None else None
    if row.get('kind')=='stage1d_real':
        # New observations have no accepted historical endpoints or source-amount
        # truth. Keep the common acceptance contract and allocation audit without
        # inventing a same-input pilot regression target from this run's answer.
        errors=[]
        if audit is not None and not audit['exact_feasible']:errors.append('HAIRCUT_ASSIGNMENT_INFEASIBLE')
        return {'same_input_full_regression':None,'haircut_full_assignment_audit':audit,
            'real_source_amount_ground_truth':None,'real_amount_accuracy':None,
            'reference_evaluation':'POST_RESULT_ONLY_SEPARATE_REVIEW_REPORT',
            'context_status':row['context_status'],'acquisition_status':row['acquisition_status'],
            'scope_id':row['scope_id'],'errors':errors,'passed':not errors}
    v=(results['FULL_INTERVAL'].get('joint_by_asset') or {}).get('ETH',{'lower_raw':None,'upper_raw':None})
    accepted=row['accepted_same_input_joint_eth']
    regression=all(v[k] is not None and F(v[k])/10**18==F(accepted[i]) for i,k in enumerate(('lower_raw','upper_raw')))
    errors=[]
    if not regression:errors.append('SAME_INPUT_FULL_REGRESSION_MISMATCH')
    if audit is not None and not audit['exact_feasible']:errors.append('HAIRCUT_ASSIGNMENT_INFEASIBLE')
    return {'same_input_full_regression':regression,'haircut_full_assignment_audit':audit,
        'real_source_amount_ground_truth':None,'real_amount_accuracy':None,
        'reference_evaluation':'POST_RESULT_ONLY_SEPARATE_REVIEW_REPORT','errors':errors,'passed':not errors}

def evaluate_query(tree,row,frozen,doc,results):
    """One acceptance path for controlled and real; science runs only after it."""
    started=time.perf_counter()
    contract=accept_method_results(doc,results,expected_identity=expected_identity(row,frozen))
    identity_errors=[e for value in results.values() if isinstance(value,dict) for e in value.get('identity_errors',[])]
    if identity_errors:
        contract['errors'].extend(identity_errors);contract['passed']=False;contract['status']='FAIL'
        for error in identity_errors:
            if error['method'] in contract['method_checks']:
                record=contract['method_checks'][error['method']]
                record['passed']=False;record['errors'].append(error)
    contract_seconds=time.perf_counter()-started
    science_started=time.perf_counter()
    if not contract['passed']:
        evaluation={'passed':False,'status':'NOT_CERTIFIED_CONTRACT_FAILED','errors':contract['errors'],
                    'comparisons':[],'address_metrics':{},'haircut_full_assignment_audit':None}
        ablations=[]
    else:
        try:
            evaluation=evaluate_controlled(doc,read(safe_path(tree,row['hidden_path'])),results) if row['kind']=='controlled' else evaluate_real(doc,row,results)
            ablations=ablation_comparison(results)
            hard=[{'code':'HARD_ABLATION_FAILURE','asset':a.get('asset'),'details':a} for a in ablations
                  if a.get('status') in ('ERROR','UNRESOLVED','NOT_COMPARABLE') or a.get('balance_nested_endpoints') is False]
            if hard:evaluation.setdefault('errors',[]).extend(hard);evaluation['passed']=False
        except Exception as exc:
            evaluation={'passed':False,'status':'SCIENTIFIC_CHECK_ERROR','errors':[type(exc).__name__+': '+str(exc)],'comparisons':[]};ablations=[]
    evaluation['contract_passed']=contract['passed']
    evaluation['passed']=evaluation.get('passed') is True and contract['passed'] is True
    return contract,evaluation,ablations,{'common_output_contract_seconds':contract_seconds,'scientific_evaluation_seconds':time.perf_counter()-science_started,
        'scope':'Post-method validation only; excluded from algorithm warmup and five timed executions'}

def query_binding(tree,folder,row,frozen):
    return {'expected_identity':expected_identity(row,frozen),'parent_freeze_sha256':file_hash(tree/'EXPERIMENT_FREEZE.json'),
        'execution_revision':frozen.get('execution_revision'),
        'files':{name:file_hash(folder/name) for name in ('METHOD_RESULTS.json','EVALUATION.json','ABLATIONS.json','OUTPUT_CONTRACT.json')},
        'observed_sha256':file_hash(safe_path(tree,row['observed_path']))}

def flat_results(row,results,tg,physical):
    records=[]
    for method,value in results.items():
        for category in ('addresses','events','joint_by_asset'):
            for target,rec in (value.get(category) or {}).items():
                names=[target] if category=='events' else tg[target] if category=='addresses' else [e for g,es in tg.items() if g.rsplit('|',1)[1]==target for e in es]
                asset=physical[target]['asset'] if category=='events' else target.rsplit('|',1)[1] if category=='addresses' else target
                records.append({'sample_id':row['sample_id'],'query_id':row['query_id'],'incident_id':row['incident_id'],'kind':row['kind'],
                    'source_layer':row['source_layer'],'zero_hop':row.get('zero_hop',False),'method':method,'category':category,'target':target,
                    'status':value['status'],'output_kind':value['output_kind'],'lower_raw':rec.get('lower_raw'),'upper_raw':rec.get('upper_raw'),
                    'point_raw':rec.get('point_raw'),'POISON_NOMINAL_RAW':rec.get('nominal_raw'),
                    'physical_capacity_raw':str(sum((F(physical[e]['amount_raw']) for e in names),F(0))),'asset':asset,
                    'evidence_ids':json.dumps(sorted({x for e in names for x in physical[e]['evidence_ids']})),
                    'unit':'raw_by_asset_no_cross_asset_sum','input_fact_hash':row['input_fact_hash'],'certification':'QUERY_ACCEPTED'})
    return records

def select_rows(rows,selection):
    if selection=='all':return list(rows)
    if isinstance(selection,str) and selection.startswith('ids:'):
        ids=selection[4:].split(',');known={r['sample_id']:r for r in rows}
        if not ids or len(set(ids))!=len(ids) or any(s not in known for s in ids):raise ValueError('Empty, duplicate or unknown selection')
        return [known[s] for s in ids]
    return [r for r in rows if r['kind']==selection or r['sample_id']==selection]

def run(tree,out,kind='min',selection='all'):
    import numpy,scipy
    frozen,rows=verify_freeze(tree,kind)
    rows=select_rows(rows,selection)
    if not rows:raise ValueError('Empty or unknown selection')
    if out.exists():raise ValueError('Run output must be fresh; prior failures are retained')
    out.mkdir(parents=True)
    index=[];support=[];flat=[];timing_rows=[];validation_rows=[];all_passed=True
    for number,row in enumerate(rows):
        sid=row['sample_id']; data_bytes=safe_path(tree,row['observed_path']).read_bytes()
        # Timed common preprocessing = deserialize and canonical target extraction.
        preprocess=[]
        for repeat in range(6):
            begin=time.perf_counter();doc=json.loads(data_bytes);tg=observed_targets(doc)
            preprocess.append(time.perf_counter()-begin)
        if tg!=groups(doc):raise ValueError('Baseline/main target domains differ before execution')
        physical=observed_port_facts(doc)
        offset=hashlib.sha256(sid.encode()).digest()[0]%len(METHODS)
        order=METHODS[offset:]+METHODS[:offset]
        results={};profiles={};raw_returns={}
        for method in order:
            value,profile=measure(doc,method)
            raw_returns[method]={'first_return':profile.pop('raw_method_return'),'failed_attempts':profile.pop('raw_failed_attempts')}
            value=attach_identity(doc,method,value,row,frozen)
            results[method]=value;profiles[method]=profile
            timing_rows.append({'sample_id':sid,'method':method,'median_seconds':profile['median_seconds'],
                'min_seconds':profile['min_seconds'],'max_seconds':profile['max_seconds'],
                'common_preprocess_median_seconds':statistics.median(preprocess[1:]),
                'fixed_graph_end_to_end_median_seconds':profile['median_seconds']+statistics.median(preprocess[1:]),
                'warmup_python_peak_bytes':profile['warmup_python_peak_bytes'],
                'endpoint_optimizations':(value.get('task_counts') or {}).get('endpoint_optimizations',0)})
        # Persist methods before opening hidden/reference or running Oracle.
        dest=out/'samples'/sid.replace(':','_').replace('/','_')
        write(dest/'RAW_METHOD_RETURNS.json',raw_returns)
        write(dest/'METHOD_RESULTS.json',results)
        contract,evaluation,ablations,validation_time=evaluate_query(tree,row,frozen,doc,results)
        write(dest/'OUTPUT_CONTRACT.json',contract)
        write(dest/'EVALUATION.json',evaluation)
        write(dest/'ABLATIONS.json',ablations)
        write(dest/'QUERY_ACCEPTANCE.json',{'schema_version':'stage1c-r1-query-acceptance-1.0','passed':evaluation['passed'],
            'binding':query_binding(tree,dest,row,frozen),'contract_passed':contract['passed'],'scientific_checks_passed':evaluation['passed'],
            'errors':evaluation.get('errors',[]),'validation_timing':validation_time})
        validation_rows.append({'sample_id':sid,**validation_time})
        if evaluation['passed']:flat.extend(flat_results(row,results,tg,physical))
        else:all_passed=False
        for method,value in results.items():
            support.append({'sample_id':sid,'kind':row['kind'],'family':row.get('family'),'method':method,
                'status':value.get('status','MISSING'),'applicability':value.get('applicability'),'output_kind':value.get('output_kind'),
                'reason':value.get('failure_reason'),'contract_passed':contract['passed'],'query_passed':evaluation['passed']})
        write(dest/'EFFICIENCY.json',{'method_order':order,'shared_preprocessing_raw_seconds':preprocess,'profiles':profiles,
            'common_scope':'Fixed local input JSON read once; no online acquisition was timed','warmups':1,'timed_repetitions':5,
            'post_method_validation':validation_time})
        index.append({'sample_id':sid,'kind':row['kind'],'family':row.get('family'),'path':dest.relative_to(out).as_posix(),
            'input_fact_hash':row['input_fact_hash'],'method_statuses':{m:r.get('status','MISSING') for m,r in results.items()},
            'evaluation_passed':evaluation['passed'],'contract_passed':contract['passed'],'passed':evaluation['passed'],
            'query_acceptance_sha256':file_hash(dest/'QUERY_ACCEPTANCE.json')})
        print(json.dumps({'sample':sid,'n':number+1,'total':len(rows),'passed':evaluation['passed'],'statuses':index[-1]['method_statuses']}),flush=True)
    write_csv(out/'METHOD_SUPPORT_MATRIX.csv',support);write_csv(out/'PAIRED_RESULTS.csv',flat);write_csv(out/'TIMINGS.csv',timing_rows)
    write_csv(out/'VALIDATION_TIMINGS.csv',validation_rows)
    summary={'schema_version':'stage1c-r1-run-results-1.0','status':'COMPLETED' if all_passed else 'PARTIAL',
        'passed':all_passed,'scope':kind,'selection':selection,'sample_count':len(rows),
        'controlled_count':sum(r['kind']=='controlled' for r in rows),'real_count':sum(r['kind']=='real' for r in rows),
        'freeze_sha256':file_hash(tree/'EXPERIMENT_FREEZE.json'),'execution_revision':frozen.get('execution_revision'),'method_results_index':index,
        'runtime':{'system':platform.system(),'platform':platform.platform(),'python':platform.python_version(),'numpy':numpy.__version__,'scipy':scipy.__version__},
        'research_platform_requests':0,'new_research_cost':'0','external_acceptance':'PENDING_REVIEW',
        'input_unchanged':verify_freeze(tree,kind)[0]==frozen}
    write(out/'RESULTS_INDEX.json',summary)
    return summary

def main():
    p=argparse.ArgumentParser();p.add_argument('--tree',type=Path,default=Path(__file__).resolve().parents[1])
    p.add_argument('--freeze',action='store_true');p.add_argument('--version',default='stage1c-v1')
    p.add_argument('--freeze-revision',action='store_true')
    p.add_argument('--kind',choices=('min','public'),default='public');p.add_argument('--selection',default='all')
    p.add_argument('--output',type=Path);a=p.parse_args();tree=a.tree.resolve()
    try:
        if a.freeze_revision:
            from stage1c_revision import freeze_revision
            result=freeze_revision(tree,a.version);print(json.dumps({'revision_frozen':result['version']}));return 0
        if a.freeze: result=freeze(tree,a.version);print(json.dumps({'frozen':result['version']}));return 0
        if a.output is None:raise ValueError('--output required')
        result=run(tree,a.output.resolve(),a.kind,a.selection)
        return 0 if result['passed'] else 1
    except Exception as exc:
        print(json.dumps({'status':'ERROR','error':type(exc).__name__+': '+str(exc)}),file=sys.stderr)
        return 1
if __name__=='__main__':raise SystemExit(main())
