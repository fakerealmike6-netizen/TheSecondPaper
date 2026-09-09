"""Root-only bounded Tx2 legacy point audit/apply, from one reviewed metadata manifest.

Audit reads original candidates only through LegacyPointImporter.prepare_one.
Apply requires an exact prior audit SHA and reuses its unchanged explicit needs.
No provider dispatch, BQ, LP, query replay, attempt reset or new cache DB exists here.
"""
from __future__ import annotations
import argparse
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
import socket
import sys
from collections import Counter

QUERY='txphish_src002'
PREPARATION_SHA='7f942cd27107528dc289d4a76a9306cd4e011a0b1263493ff20d1cabc2c2b925'
PREPARATION_PATH='private/current_context_points/20260909_post_hold_pair_context_v1/PREPARATION_RESULT.json'
ESTIMATE_SHA='151201291e9c84f81f131301160dcf6f1e8f486ba008ef22f6e1b1b8cf536000'
INDEX_SHA='041ad4c4fb9bd9f2a65e84872ae25bd2d1ea8785075b93d6ecf8f8e4853238b1'
WRAPPER_SHA='b68fb6f3e3740c90365ada19cd4457c189b13458b9386fe33726323727ff087d'
SINGLE='SINGLE_RESPONSE_METADATA_CANDIDATE_PENDING_RAW_VALIDATION'

def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
def digest(v):return hashlib.sha256(canonical(v)).hexdigest()
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def checked(p,wanted):
    data=p.read_bytes()
    if hashlib.sha256(data).hexdigest()!=wanted:raise ValueError('Explicit file SHA differs: '+p.name)
    return json.loads(data)
def module(path,name,expected):
    if sha(path)!=expected:raise ValueError('Reviewed helper source changed: '+path.name)
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

def selected_needs(preparation,estimate,preparation_ref):
    from stage1d_legacy_rpc_import import exact_request_sha
    query=preparation['queries'][QUERY]
    lookup={x['request_sha256']:x for x in query['point_requests']}
    selected=[];seen=set()
    for record in estimate['rows']:
        if record['status']!=SINGLE:continue
        row=lookup[record['request_sha256']]
        if (row['current_cache']['status']=='SUCCESS_ENVELOPE_SHA_AND_RUNTIME_VALIDATED'
            or row['request']!=record['request'] or exact_request_sha(row['request'])!=record['legacy_exact_request_sha256']
            or len(record['response_sha256_variants'])!=1):
            raise ValueError('Metadata candidate no longer matches the exact fixed missing point')
        original=row['legacy_need_for_root_only']
        if original['expected_block']!=record['expected_block']:raise ValueError('Original expected block differs')
        need=deepcopy(original)
        if {k:need[k] for k in ('method','params')}!=row['request']:raise ValueError('Current independent need differs')
        # PREP is 23.7MB, under the importer's 64MB evidence-ref limit. It already
        # contains this exact request, expected block, input bindings and the
        # 89MB MATERIAL ref. Preserve the latter as transitive provenance rather
        # than repeatedly submitting that oversized file as a direct point ref.
        need['original_context_evidence_refs']=deepcopy(original['evidence_refs'])
        need['evidence_refs']=[query['inputs']['collection'],preparation['active_batch'],preparation_ref]
        key=exact_request_sha(row['request'])
        if key in seen:raise ValueError('Duplicate candidate request')
        seen.add(key)
        selected.append({'need':need,'legacy_exact_request_sha256':key,'expected_legacy_response_sha256':record['response_sha256_variants'][0]})
    # Headers first supports existing numbered/hash anchor normalization. This
    # does not modify selector order, expected block, query source or request key.
    return sorted(selected,key=lambda x:(x['need']['method']!='eth_getBlockByNumber',x['need']['expected_block'],x['legacy_exact_request_sha256']))

def audit_points(importer,query,selected):
    rows=[]
    for candidate in selected:
        prepared=importer.prepare_one(query,candidate['need'])
        row={**candidate,'need_sha256':digest(candidate['need']),'status':prepared['status']}
        if prepared['status']=='ADMISSIBLE_POINT_PENDING_ROOT_APPLY':
            if prepared['legacy_source']['raw_sha256']!=candidate['expected_legacy_response_sha256']:
                raise ValueError('Prepared raw response differs from fixed single-variant candidate')
            row.update(legacy_source=prepared['legacy_source'],normalization_sha256=digest(prepared['normalization']),
                       logical_key=prepared['logical_key'],coverage_complete=False)
        else:
            row.update({k:prepared[k] for k in ('reason','error_class','current_receipt','response_sha256_variants') if k in prepared})
        rows.append(row)
    return rows

def exact_apply_needs(audit,selected):
    if audit.get('mode')!='AUDIT_ONLY' or audit.get('status')!='LEGACY_POINT_AUDIT_COMPLETE':raise ValueError('Completed exact audit required')
    original={x['legacy_exact_request_sha256']:x for x in selected};needs=[]
    if len(audit['rows'])!=len(original):raise ValueError('Audit candidate set changed')
    seen=set()
    for row in audit['rows']:
        key=row['legacy_exact_request_sha256'];candidate=original[key]
        if key in seen or row['need']!=candidate['need'] or row['need_sha256']!=digest(candidate['need']):raise ValueError('Audit need changed')
        seen.add(key)
        if row['status']=='ADMISSIBLE_POINT_PENDING_ROOT_APPLY':
            if row['legacy_source']['raw_sha256']!=candidate['expected_legacy_response_sha256']:raise ValueError('Audit source response changed')
            needs.append(deepcopy(row['need']))
    return needs

def run(args):
    work=Path(args.work).resolve();revision=work.parent;folder=Path(__file__).resolve().parent
    sys.dont_write_bytecode=True;sys.path.insert(0,str(work/'src'))
    def no_network(*a,**k):raise RuntimeError('Legacy point audit/apply has no online route')
    socket.socket.connect=no_network;socket.create_connection=no_network
    from stage1d_legacy_rpc_import import LegacyPointImporter
    from stage1d_closure_scope import active_batch
    wrapper=module(revision/'staging/post_hold_context_spec/prepare_post_hold_context_spec.py','tx2_point_spec_guard',WRAPPER_SHA)
    output=wrapper.load_driver(revision).inside(folder,args.output)
    if output.exists():raise FileExistsError('Output already exists; no duplicate operation')
    before=wrapper.safe_point(work)
    prep_ref={'path':PREPARATION_PATH,'sha256':PREPARATION_SHA};prep=checked(work/PREPARATION_PATH,PREPARATION_SHA)
    estimate=checked(folder/'TX2_LEGACY_METADATA_ESTIMATE.json',ESTIMATE_SHA)
    helper=wrapper.load_driver(revision)
    if helper.source_inventory(work)!=prep['source_sha256']:raise ValueError('Bound preparation source has changed')
    role_ref={'path':args.role_binding_receipt,'sha256':args.role_binding_sha256}
    role=helper.checked(work,role_ref)
    guard=module(revision/'staging/post_hold_role_binding_supplement/verify_role_binding.py','tx2_current_role_guard',role['validator_sha256'])
    guard.verify_guard(work,role_ref,prep_ref,QUERY)
    query=next(q for q in active_batch(work)['queries'] if q['name']==QUERY)
    wrapper.current_entry(work,query,helper)
    selected=selected_needs(prep,estimate,prep_ref)
    if len(selected)!=137:raise ValueError('This reviewed bounded manifest contains exactly 137 single-response candidates')
    importer=LegacyPointImporter(work,revision/'recovery/legacy_metadata_index/legacy_metadata.sqlite')
    if importer.index_sha!=INDEX_SHA:raise ValueError('Existing sealed index changed')
    importer.runtime.require_gate(work)
    common={'schema_version':'stage1d-tx2-post-hold-legacy-point-audit-v1','query_name':QUERY,'query_id':query['query_id'],
            'preparation_ref':prep_ref,'metadata_estimate_sha256':ESTIMATE_SHA,'index_sha256':INDEX_SHA,
            'role_binding_ref':role_ref,'source_sha256':prep['source_sha256'],'driver_sha256':sha(Path(__file__)),
            'single_response_candidates':137,'fixed_missing_points':650,'response_conflict_points_preserved':60,
            'no_exact_metadata_points_preserved':453,'new_external_requests':0,'context_complete':False,'covered_ranges':0,
            'fixed_point_selectors_or_scope_changed':False}
    if args.mode=='audit':
        rows=audit_points(importer,query,selected)
        result={**common,'mode':'AUDIT_ONLY','status':'LEGACY_POINT_AUDIT_COMPLETE','rows':rows,
                'status_counts':dict(Counter(x['status'] for x in rows)),'production_writes':0,'apply_calls':0}
    else:
        if not args.audit_manifest or not args.audit_sha256:raise ValueError('Apply requires exact prior audit manifest and SHA')
        ap=helper.inside(folder,args.audit_manifest);audit=checked(ap,args.audit_sha256)
        if any(audit.get(k)!=common[k] for k in ('preparation_ref','metadata_estimate_sha256','index_sha256','role_binding_ref','source_sha256','driver_sha256')):
            raise ValueError('Apply inputs differ from reviewed exact audit')
        needs=exact_apply_needs(audit,selected)
        if not (work/'private/read_retry_r4.sqlite').is_file():raise ValueError('Existing current cache DB is required; do not create a new one')
        # Existing importer acquires the one-writer lock and repeats all current
        # success/raw/SHA/scope/strict-normalizer checks before each local import.
        claim=folder/('APPLY_CLAIM_'+args.audit_sha256+'.json')
        with claim.open('xb') as handle:handle.write(canonical({'audit_sha256':args.audit_sha256,'output':args.output,'status':'ROOT_EXPLICIT_APPLY_CLAIMED_NO_AUTOMATIC_RETRY'})+b'\n')
        applied=importer.apply_many(query,needs)
        result={**common,'mode':'ROOT_EXPLICIT_APPLY','status':'LEGACY_POINT_APPLY_COMPLETE','audit_manifest_sha256':args.audit_sha256,
                'apply_result':applied,'status_counts':dict(Counter(x['status'] for x in applied['results']))}
    after=wrapper.safe_point(work)
    if args.mode=='audit' and after!=before:raise ValueError('Current cache changed during read-only audit; preserve source and re-audit at safe point')
    guard.verify_guard(work,role_ref,prep_ref,QUERY)
    if helper.source_inventory(work)!=prep['source_sha256']:raise ValueError('Source changed during operation')
    output=helper.inside(folder,args.output)
    with output.open('xb') as handle:handle.write(canonical(result)+b'\n')
    print(json.dumps({'status':result['status'],'output':str(output),'sha256':sha(output),'status_counts':result['status_counts']},ensure_ascii=False))

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work',required=True);parser.add_argument('--mode',choices=('audit','apply'),required=True)
    parser.add_argument('--role-binding-receipt',required=True,help='Exact C-relative validated role supplement')
    parser.add_argument('--role-binding-sha256',required=True)
    parser.add_argument('--output',required=True,help='New filename within this staging directory')
    parser.add_argument('--audit-manifest');parser.add_argument('--audit-sha256')
    args=parser.parse_args();run(args)

if __name__=='__main__':main()
