"""Inventory every current executable/configuration surface without importing it.

Static imports are a conservative module reachability bound, not proof a function
ran. Review annotations are valid only at their exact current file SHA-256.
"""
from __future__ import annotations
import argparse, ast, csv, hashlib, json, re
from collections import Counter, defaultdict, deque
from pathlib import Path

REV=Path(__file__).resolve().parent
EXTERNAL_NEW={'run_stage1c.py','stage1c_baselines.py','stage1c_catalog.py','stage1c_controlled.py','stage1c_intervals.py','stage1c_oracle.py','stage1c_real_replay.py','stage1c_reports.py','validate_stage1c.py'}
ENTRY_ROOTS={'experiment_cli':'src/run_stage1c.py','reports_cli':'src/stage1c_reports.py','package_validation_cli':'src/validate_stage1c.py','final_real_replay_cli':'src/stage1c_real_replay.py','offline_test_cli':'src/run_tests.py','controlled_regression_cli':'src/lp_run.py','release_packaging_cli':'tools/stage1c_release.py'}
FIELDS=['path','sha256','bytes','kind','change_status','baseline_sha256','purpose','has_direct_entry','default_entry_reachability','reachability_basis','upstream','downstream','literal_file_references','check_depth','substantive_review_scope','associated_tests_static','executed_test_evidence','inherited_review_source','inherited_review_scope','status','limitations']


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def read(path):return json.loads(path.read_text(encoding='utf-8'))
def write(path,data):path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')


def independent_evidence():
    """Allowlisted receipt metadata; private commands, paths and ledger data stay out."""
    rows=[]
    for directory in ('independent_results','independent_results_packaged_preflight','independent_results_v5','independent_results_packaged_v5'):
        path=REV/directory/'INDEPENDENT_CHECK_EXECUTION_RECEIPT.json'
        if not path.is_file():continue
        receipt=read(path)
        rows.append({'receipt_path':path.relative_to(REV).as_posix(),'receipt_sha256':sha(path),
                     'passed':receipt['passed'],'checked_inputs_unchanged':receipt['checked_inputs_unchanged'],
                     'windows_executed':receipt['runtime']['windows_executed'],
                     'linux_executed':receipt['runtime']['linux_executed'],
                     'source_adaptation_manifest_sha256':receipt['source_adaptation_manifest_sha256'],
                     'runs':[{'script':r['script'],'script_sha256':r['script_sha256'],
                              'return_code':r['return_code'],'passed':r['passed'],
                              'network_attempt_count':len(r['guard']['network_attempts'])}
                             for r in receipt['runs']],
                     'output_hashes':receipt['output_hashes'],
                     'scope':'Actual internal rerun of supplied external mathematical checks after official timing. Full execution records and external material remain MIN-only; external acceptance is still PENDING_REVIEW.'})
    return rows


def current_execution_evidence(tree):
    """Bind current actual records using a public-safe field allowlist."""
    frozen=read(tree/'REVISION_FREEZE.json')
    result={'version':frozen['version'],'revision_freeze_sha256':sha(tree/'REVISION_FREEZE.json'),
            'scientific_methods_and_inputs_changed':frozen['scientific_methods_and_inputs_changed'],
            'records':[]}
    for name,keys in (
        ('checks/ALL_TESTS_FINAL_V5.json',('tests_run','passed','failed','errors','skipped','success')),
        ('batch_final_v5/RESULTS_INDEX.json',('status','passed','sample_count','controlled_count','real_count','input_unchanged','research_platform_requests')),
        ('reports_final_v5/REPORT_ACCEPTANCE.json',('status','passed','sample_count','failed_queries','recomputed_common_contract_and_scientific_checks')),
        ('checks/OUTPUT_CONTRACT_MINIMAL_MIRROR_TESTS.json',('status','exit_code','tests_run','failure_or_error_or_skip')),
        ('checks/PREPARED_V5_SELF_CONTAINED_COMPARISON.json',('status','passed','sample_count','method_pairs_checked','auxiliary_pairs_checked','pairs_equal','field_differences_count','current_contracts_passed_and_bound','frozen_input_files_checked')),
        ('full_public_validator_fault_v5/FULL_VALIDATOR_FAULT_CHECK.json',('fault_chain_verified','validator_cli_exit_code','validator_passed','batch_query_count','bad_query_count','good_query_count','no_missing_required_check_shortcut','no_payload_hash_corruption'))):
        path=REV/name
        if path.is_file():
            receipt=read(path)
            result['records'].append({'path':name,'sha256':sha(path),**{k:receipt[k] for k in keys}})
    result['limits']='Windows actual execution only. Inherited fixture description is clarified by docs/TEST_SCOPE_CLARIFICATION.md; public packaged validator and external acceptance require their separate receipts.'
    return result


def public_projection(value):
    """Rebuild a disclosure-minimized view; never recursively copy private metadata."""
    if not isinstance(value,dict) or 'files' not in value:
        raise ValueError('Public projection requires the complete inventory object')
    # This immutable already reviewed payload defines which code identities are
    # public. A private dependency does not become public merely because a
    # source annotation, import, helper, or MIN inventory refers to it.
    candidates=(REV/'prepared_final/public',REV/'prepared_final_rejected_public_metadata_v5/public')
    public_tree=next((p for p in candidates if p.is_dir()),None)
    if public_tree is None:raise ValueError('Reviewed public code identity tree is required for projection')
    public_parent=REV/'baseline/public'
    safe_prefixes=('src/','tests/','configs/','config/','tools/','.github/')
    safe_roots={'.gitattributes','.gitignore','requirements.txt'}
    disclosed={r['path'] for r in value['files']
               if (r['path'].startswith(safe_prefixes) or r['path'] in safe_roots)
               and (public_tree/r['path']).is_file()
               and sha(public_tree/r['path'])==r['sha256']}
    rows=[];withheld=[]
    for original in value['files']:
        if original['path'] not in disclosed:
            withheld.append(original)
            continue
        name=original['path'];prior=public_parent/name
        parent_sha=original['baseline_sha256']
        if not prior.is_file() or sha(prior)!=parent_sha:parent_sha=None
        row={k:original[k] for k in ('path','sha256','bytes','kind','change_status','has_direct_entry','check_depth','status')}
        row.update({'baseline_sha256':parent_sha,
                    'purpose':'Publicly shipped file; exact byte identity inventoried.',
                    'default_entry_reachability':original['default_entry_reachability'],
                    'reachability_basis':'Static AST imports only; module reachability is not function execution evidence.',
                    'upstream':[x for x in original['upstream'] if x in disclosed],
                    'downstream':[x for x in original['downstream'] if x in disclosed],
                    'literal_file_references':[x for x in original['literal_file_references'] if x in disclosed],
                    'substantive_review_scope':(['Current exact-byte scoped substantive review documented; complete private review notes are withheld. This does not mean every function was newly audited.'] if original['check_depth']=='TARGETED_SUBSTANTIVE_REVIEW' else []),
                    'associated_tests_static':[x for x in original['associated_tests_static'] if x in disclosed],
                    'executed_test_evidence':[],
                    'inherited_review_source':None,
                    'inherited_review_scope':'Prior accepted public code identity when the public parent hash is shown; internal acceptance provenance is withheld.',
                    'limitations':'Only public dependency edges are displayed. Private paths, hashes, review annotations and internal evidence identities are withheld; aggregate executed checks appear separately.'})
        rows.append(row)
    # Grouping removes the original ordering-to-private-file association. Each
    # placeholder retains only the requested file category and review depth.
    groups=Counter((r['kind'],r['check_depth'],r['status']) for r in withheld)
    for (kind,depth,status),count in sorted(groups.items()):
        for _ in range(count):
            ordinal=sum(r['path'].startswith('WITHHELD_DEPENDENCY_') for r in rows)+1
            rows.append({'path':f'WITHHELD_DEPENDENCY_{ordinal:03d}','sha256':None,'bytes':None,'kind':kind,
                         'change_status':'WITHHELD','baseline_sha256':None,'purpose':'Private dependency identity withheld.',
                         'has_direct_entry':None,'default_entry_reachability':[],'reachability_basis':'Private dependency relationships withheld.',
                         'upstream':[],'downstream':[],'literal_file_references':[],'check_depth':depth,
                         'substantive_review_scope':[],'associated_tests_static':[],'executed_test_evidence':[],
                         'inherited_review_source':None,'inherited_review_scope':'Withheld.','status':status,
                         'limitations':'Placeholder does not disclose an original path, hash, size, source, annotation, evidence identity or dependency edge.'})
    identity={r['path']:r['sha256'] for r in rows if r['sha256']}
    safe={'schema_version':'stage1c-r1-public-integration-matrix-v2',
          'view':'PUBLIC_CODE_IDENTITIES_AND_ANONYMOUS_PRIVATE_REVIEW_DEPTH_COUNTS',
          'inventory_sha256':hashlib.sha256(json.dumps(identity,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
          'inventory_hash_scope':'Public disclosed code identities only; no private inventory hash.',
          'file_count':len(rows),'disclosed_file_count':len(disclosed),'withheld_file_count':len(withheld),
          'counts':{field:dict(Counter(str(r[field]) for r in rows)) for field in ('kind','change_status','check_depth','status')},
          'entry_roots':{k:p for k,p in value['entry_roots'].items() if p in disclosed},
          'source_py_count':sum(r['kind']=='source' and r['path'].endswith('.py') for r in rows),
          'full_version_meaning':'Public code paths and hashes plus anonymous withheld-dependency review-depth counts. The complete exact-path and exact-hash matrix is available only in the private review bundle.',
          'runtime_trace_collected':False,'syntax_error_count':len(value['syntax_errors']),
          'private_content_policy':'Private file identities, paths, hashes, dependency edges, acceptance provenance, internal annotations and evidence references are omitted.',
          'files':rows}
    independent=value.get('independent_verification_evidence',[])
    safe['independent_verification_summary']={'internal_verification_rounds':len(independent),
        'all_recorded_rounds_passed':all(e['passed'] for e in independent),
        'all_checked_inputs_unchanged':all(e['checked_inputs_unchanged'] for e in independent),
        'total_script_runs':sum(len(e['runs']) for e in independent),
        'network_attempt_count':sum(r['network_attempt_count'] for e in independent for r in e['runs']),
        'windows_executed':bool(independent) and all(e['windows_executed'] for e in independent),
        'linux_executed':any(e['linux_executed'] for e in independent),
        'external_acceptance':'PENDING_REVIEW'}
    execution=value.get('current_execution_evidence',{})
    keys={'tests_run','passed','failed','errors','skipped','success','status','sample_count','controlled_count','real_count',
          'input_unchanged','research_platform_requests','failed_queries','recomputed_common_contract_and_scientific_checks',
          'exit_code','failure_or_error_or_skip','method_pairs_checked','auxiliary_pairs_checked','pairs_equal',
          'field_differences_count','current_contracts_passed_and_bound','frozen_input_files_checked','fault_chain_verified',
          'validator_cli_exit_code','validator_passed','batch_query_count','bad_query_count','good_query_count',
          'no_missing_required_check_shortcut','no_payload_hash_corruption'}
    labels=('unit_tests','same_input_batch','saved_result_acceptance','minimal_unit_mirror','compact_same_input_comparison','mixed_controlled_fault_validation')
    safe['executed_check_summary']=[{'check':label,**{k:v for k,v in record.items() if k in keys}}
                                     for label,record in zip(labels,execution.get('records',[]))]
    return safe


def write_public_matrix(output,result):
    public=public_projection(result)
    serialized=json.dumps(public,ensure_ascii=False)
    disclosed={r['path'] for r in public['files'] if r['sha256']}
    private=[r for r in result['files'] if r['path'] not in disclosed]
    forbidden_paths={r['path'] for r in private}|{r['path'].removeprefix('@revision/') for r in private}
    allowed_hashes={public['inventory_sha256']}|{r[k] for r in public['files'] for k in ('sha256','baseline_sha256') if r[k]}
    scans={'evm_address_or_transaction_literals':len(re.findall(r'0x[0-9a-fA-F]{40,64}',serialized)),
           'windows_absolute_host_paths':len(re.findall(r'(?i)(?<![a-z0-9])[a-z]:[\\/]',serialized)),
           'private_inherited_risk_literal':serialized.count('47.167873093'),
           'private_remaining_risk_literal':serialized.count('52.832126907'),
           'private_exact_path_hits':sum(path in serialized for path in forbidden_paths),
           'non_public_sha256_literals':len(set(re.findall(r'(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])',serialized))-allowed_hashes),
           'private_internal_path_prefixes':len(re.findall(r'(?:private|derived|raw|governance|bootstrap|execution_tools|independent_results(?:_\w+)?)/|@revision/',serialized))}
    if any(scans.values()):raise ValueError('Public integration metadata contains unreviewed private content: '+repr(scans))
    write(output/'FULL_VERSION_INTEGRATION_MATRIX_PUBLIC.json',public)
    with (output/'FULL_VERSION_INTEGRATION_MATRIX_PUBLIC.csv').open('w',encoding='utf-8',newline='') as handle:
        writer=csv.DictWriter(handle,fieldnames=FIELDS);writer.writeheader()
        for row in public['files']:writer.writerow({k:json.dumps(row[k],ensure_ascii=False,separators=(',',':')) if isinstance(row[k],(dict,list)) else row[k] for k in FIELDS})
    scan={'schema_version':'stage1c-r1-public-integration-metadata-check-v1','passed':not any(scans.values()),'scan_counts':scans,
          'same_all_paths_hashes_and_depth':False,
          'same_total_file_and_check_depth_counts':len(public['files'])==len(result['files']) and Counter(r['check_depth'] for r in public['files'])==Counter(r['check_depth'] for r in result['files']),
          'disclosed_file_count':public['disclosed_file_count'],'withheld_file_count':public['withheld_file_count'],
          'public_code_inventory_sha256':public['inventory_sha256'],'public_json_sha256':sha(output/'FULL_VERSION_INTEGRATION_MATRIX_PUBLIC.json'),
          'public_csv_sha256':sha(output/'FULL_VERSION_INTEGRATION_MATRIX_PUBLIC.csv'),
          'scope':'Public code allowlist; private identities and internal evidence omitted. Exact-path and SHA allowlist checks supplement field construction. Full MIN identities remain private.'}
    write(output/'PUBLIC_MATRIX_PROJECTION_RECEIPT.json',scan)
    destination=output/'public';destination.mkdir(exist_ok=True)
    for source_name,target_name in (('FULL_VERSION_INTEGRATION_MATRIX_PUBLIC.json','FULL_VERSION_INTEGRATION_MATRIX.json'),
                                  ('FULL_VERSION_INTEGRATION_MATRIX_PUBLIC.csv','FULL_VERSION_INTEGRATION_MATRIX.csv'),
                                  ('PUBLIC_MATRIX_PROJECTION_RECEIPT.json','PUBLIC_MATRIX_PROJECTION_RECEIPT.json')):
        (destination/target_name).write_bytes((output/source_name).read_bytes())
    lines=['# Public integration review','',public['full_version_meaning'],'',
           f"{public['file_count']} total entries: {public['disclosed_file_count']} public code identities and {public['withheld_file_count']} anonymous private dependency placeholders.",'',
           'Static import edges, scoped substantive source review and actual execution are separate evidence. Hash identity does not establish a new line-by-line historical audit. Private dependency relationships and internal acceptance metadata are withheld.','',
           '| Review depth | Count |','|---|---:|']
    lines += [f'| {k} | {v} |' for k,v in sorted(public['counts']['check_depth'].items())]
    lines += ['','Internal independent checks: '+json.dumps(public['independent_verification_summary'],ensure_ascii=False)+'.','',
              'Executed check summaries (no internal paths or receipt hashes):','']
    lines += ['- '+json.dumps(row,ensure_ascii=False) for row in public['executed_check_summary']]
    summary='\n'.join(lines)+'\n'
    (output/'INTEGRATION_REVIEW_SUMMARY_PUBLIC.md').write_text(summary,encoding='utf-8')
    (destination/'INTEGRATION_REVIEW_SUMMARY.md').write_text(summary,encoding='utf-8')


def category(p):
    rel=p.as_posix()
    if p.parts[0]=='@revision':return 'external_revision_auxiliary_entry'
    if p.parts[0]=='src':return 'source'
    if p.parts[0]=='tests':return 'test'
    if p.suffix.lower()=='.sql':return 'sql_snapshot'
    if p.parts[0] in ('configs','config'):return 'configuration_or_method_rule'
    if p.parts[0]=='.github':return 'workflow'
    if p.parts[0]=='tools' or p.suffix.lower() in ('.sh','.ps1','.cmd','.bat'):return 'auxiliary_entry'
    return 'other_entry_or_runtime_dependency'


def inventory(tree):
    for p in sorted(tree.rglob('*')):
        if not p.is_file():continue
        rel=p.relative_to(tree)
        if any(part in {'__pycache__','.git','.test_tmp','node_modules'} for part in rel.parts):continue
        if rel.parts[0] in {'baseline','results','raw','controlled_v1','fixtures','validation_evidence'}:continue
        if rel.parts[0] in {'src','tests','configs','config','.github','tools'} or p.suffix.lower() in {'.sql','.ps1','.sh','.cmd','.bat'} or (p.suffix.lower()=='.py' and rel.parts[0]=='governance') or (len(rel.parts)==1 and (p.suffix.lower() in {'.py','.toml','.ini'} or p.name in {'requirements.txt','.gitattributes','.gitignore'})):
            yield p,rel.as_posix()


def closure(start,edges):
    visited=set();pending=deque([start])
    while pending:
        node=pending.popleft()
        if node in visited:continue
        visited.add(node);pending.extend(edges.get(node,set())-visited)
    return visited


def build(tree,baseline,output,annotations,external,auxiliary_entries=()):
    output.mkdir(parents=True,exist_ok=True)
    files=dict((rel,p) for p,rel in inventory(tree));module_map={}
    for entry in auxiliary_entries:
        entry=entry.resolve()
        if not entry.is_relative_to(REV):raise ValueError('Auxiliary entry must remain within this revision')
        if not entry.is_file() or entry.is_symlink():raise ValueError('Auxiliary entry must be a regular file')
        if entry.is_relative_to(tree):continue
        files['@revision/'+entry.relative_to(REV).as_posix()]=entry
    for rel,p in files.items():
        if p.suffix=='.py':
            module_map[p.stem]=rel
            if rel.startswith('src/'):module_map['src.'+p.stem]=rel
    edges=defaultdict(set);upstream=defaultdict(set);refs=defaultdict(set);parsed={};errors={};functions={}
    for rel,p in files.items():
        try:source=p.read_text(encoding='utf-8-sig')
        except UnicodeError:source=''
        if p.suffix=='.py':
            try:
                node=ast.parse(source);parsed[rel]=node
                functions[rel]=[n.name for n in node.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef))]
                for n in ast.walk(node):
                    names=[]
                    if isinstance(n,ast.Import):names=[x.name for x in n.names]
                    elif isinstance(n,ast.ImportFrom):names=[n.module] if n.module else []
                    for name in names:
                        target=module_map.get(name) or module_map.get(name.split('.')[0])
                        if target and target!=rel:edges[rel].add(target);upstream[target].add(rel)
            except SyntaxError as exc:errors[rel]=str(exc)
        # Literal file references are recorded separately and never treated as
        # evidence of execution or a resolved dynamic invocation.
        for target in files:
            if target!=rel and (target in source or (Path(target).suffix in {'.py','.json','.yml','.yaml','.sql'} and Path(target).name in source)):
                refs[rel].add(target)
    roots={name:path for name,path in ENTRY_ROOTS.items() if path in files}
    reached={name:closure(path,edges) for name,path in roots.items()}
    test_files=[p for p in files if p.startswith('tests/') and p.endswith('.py')]
    test_reach={p:closure(p,edges) for p in test_files}
    annotation_rows={}
    for source in annotations:
        data=read(source)
        for row in data.get('files',[]):
            key=row['path']
            if key.startswith('code/'):key=key.removeprefix('code/')
            if key not in files and '@revision/'+key in files:key='@revision/'+key
            annotation_rows.setdefault(key,[]).append({**row,'normalized_inventory_path':key,'annotation_source':str(source)})
    external_hash=sha(external) if external.is_file() else None
    rows=[]
    for rel,p in files.items():
        current=sha(p);old=baseline/rel;prior=sha(old) if old.is_file() else None
        changed='ADDED' if prior is None else 'UNCHANGED' if prior==current else 'MODIFIED'
        node=parsed.get(rel);doc=(ast.get_docstring(node) or '').splitlines()[0] if node and ast.get_docstring(node) else ''
        has_entry=bool(node and any(isinstance(n,ast.If) and '__name__' in ast.unparse(n.test) and '__main__' in ast.unparse(n.test) for n in node.body))
        valid=[x for x in annotation_rows.get(rel,[]) if x.get('sha256')==current]
        stale=[x for x in annotation_rows.get(rel,[]) if x.get('sha256')!=current]
        relevant_roots=[name for name,nodes in reached.items() if rel in nodes]
        deep=[x for x in valid if x.get('check_depth')=='TARGETED_SUBSTANTIVE_REVIEW']
        inherited='NONE'
        if prior is not None:
            inherited=('External Stage1C substantive new-module review plus supplied independent saved-result checks' if Path(rel).name in EXTERNAL_NEW and rel.startswith('src/') else 'Inherited accepted parent identity/regression; external Stage1C did not newly review every old function or line')
            if changed!='UNCHANGED':inherited+='; inheritance is context only because current bytes changed'
        depth='TARGETED_SUBSTANTIVE_REVIEW' if deep else 'STATIC_AST_AND_LITERAL_REFERENCE_INVENTORY' if node else 'FILE_IDENTITY_AND_REFERENCE_INVENTORY'
        if rel in errors:status='SYNTAX_ERROR_REQUIRES_FIX'
        elif stale and not valid:status='REVIEW_ANNOTATION_STALE_REVIEW_REQUIRED'
        elif changed!='UNCHANGED' and not deep:status='CHANGED_SURFACE_REVIEW_REQUIRED'
        elif deep:status='CURRENT_SCOPED_REVIEW_DOCUMENTED'
        elif relevant_roots:status='STATICALLY_REACHABLE_INHERITED_REVIEW_LIMITED'
        else:status='NOT_ON_STATIC_DEFAULT_IMPORT_PATH_RETAINED_HISTORICAL_SURFACE'
        row={'path':rel,'sha256':current,'bytes':p.stat().st_size,'kind':category(Path(rel)),'change_status':changed,'baseline_sha256':prior,
             'purpose':doc or ('Frozen SQL evidence; no query execution authorized in R1' if p.suffix=='.sql' else 'Configuration/workflow/test/entry surface; see source and explicit scoped annotations'),
             'has_direct_entry':has_entry,'default_entry_reachability':relevant_roots,
             'reachability_basis':'Static AST imports including conditional/function-local imports; conservative module bound, not dynamic call proof. Literal references are separate.',
             'upstream':sorted(upstream[rel]),'downstream':sorted(edges[rel]),'literal_file_references':sorted(refs[rel]),'check_depth':depth,
             'substantive_review_scope':[x.get('scope') for x in deep],'associated_tests_static':[t for t,reachable in test_reach.items() if rel in reachable],
             'executed_test_evidence':[e for x in valid for e in x.get('executed_test_evidence',[])],
             'inherited_review_source':({'path':'bootstrap/acceptance/SOURCE_REVIEW_MATRIX.md','sha256':external_hash,'baseline_file_sha256':prior} if prior else None),
             'inherited_review_scope':inherited,'status':status,
             'limitations':('No new per-line/per-function review is claimed outside substantive_review_scope. No test execution is inferred from a test import or hash. Historical direct execution remains a separate entry; offline guarded launcher is mandatory for this revision.'),
             'definitions':functions.get(rel,[]),'review_annotations':valid,'stale_annotations':stale}
        rows.append(row)
    source_identity={r['path']:r['sha256'] for r in rows}
    identity_hash=hashlib.sha256(json.dumps(source_identity,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    counts={field:dict(Counter(str(r[field]) for r in rows)) for field in ('kind','change_status','check_depth','status')}
    result={'schema_version':'stage1c-r1-full-version-integration-matrix-v1','tree':str(tree),'baseline':str(baseline),'inventory_sha256':identity_hash,'file_count':len(rows),'counts':counts,'entry_roots':roots,
            'source_py_count':sum(r['kind']=='source' and r['path'].endswith('.py') for r in rows),
            'full_version_meaning':'Every current source, test, SQL snapshot, config/rule, workflow and executable entry is inventoried. This is not a claim every historical function/line was newly semantically reviewed.',
            'removed_vs_baseline':[rel for _,rel in inventory(baseline) if rel not in files],
            'runtime_trace_collected':False,'syntax_errors':errors,'files':rows}
    result['independent_verification_evidence']=independent_evidence()
    result['current_execution_evidence']=current_execution_evidence(tree)
    result['annotation_paths_outside_inventory']=sorted(set(annotation_rows)-set(files))
    write(output/'FULL_VERSION_INTEGRATION_MATRIX.json',result)
    with (output/'FULL_VERSION_INTEGRATION_MATRIX.csv').open('w',encoding='utf-8',newline='') as handle:
        writer=csv.DictWriter(handle,fieldnames=FIELDS);writer.writeheader()
        for r in rows:writer.writerow({k:json.dumps(r[k],ensure_ascii=False,separators=(',',':')) if isinstance(r[k],(dict,list)) else r[k] for k in FIELDS})
    lines=['# Current full-version integration inventory','',f"Inventory: {len(rows)} files; source Python modules: {result['source_py_count']}; identity: `{identity_hash}`.",'',result['full_version_meaning'],'',
           'Reachability is static AST import reachability, including optional/conditional branches. It does not establish that every reached historical function runs in the default experiment. File literals, test import associations, substantive review and actual execution receipts occupy separate fields. Hash equality is identity evidence only.','',
           'Changed/new files without an exact-hash scoped review remain marked REVIEW_REQUIRED. Hash-stale review annotations are not reused. The supplied external matrix deeply reviewed the original nine Stage1C modules, while its 84 inherited old sources received identity/call/regression checks rather than a new line-by-line review.','',
           'Historical SQL is retained as frozen evidence and is not executed. Old collectors/live adapters do not gain authorization from being shipped. Current experiment and package commands must use the established offline guarded launcher; run_tests also blocks sockets and removes credential environment variables. The R4 legacy workspace marker blocks superseded live entry paths, but this inventory is not a proof that arbitrary direct script execution is sandboxed.','',
           '| Category | Count |','|---|---:|']
    lines += [f'| {k} | {v} |' for k,v in sorted(counts['kind'].items())]
    lines += ['','## Exact-hash substantive review scopes','']
    for r in rows:
        if r['substantive_review_scope']:lines += [f"- `{r['path']}` ({r['sha256']}): "+'; '.join(str(x) for x in r['substantive_review_scope'])]
    lines += ['','## Outstanding review states','']
    lines += [f"- `{r['path']}`: {r['status']}" for r in rows if 'REQUIRED' in r['status'] or 'ERROR' in r['status']]
    lines += ['','## Actual supplied independent check reruns','']
    for e in result['independent_verification_evidence']:
        lines += [f"- `{e['receipt_path']}` ({e['receipt_sha256']}): passed={e['passed']}; {len(e['runs'])} scripts; checked inputs unchanged={e['checked_inputs_unchanged']}; Windows executed={e['windows_executed']}; Linux executed={e['linux_executed']}. Internal rerun, external acceptance PENDING_REVIEW."]
    (output/'INTEGRATION_REVIEW_SUMMARY.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    write_public_matrix(output,result)
    print(json.dumps({'file_count':len(rows),'source_py_count':result['source_py_count'],'inventory_sha256':identity_hash,'counts':counts,'output':str(output)},ensure_ascii=False))
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--tree',type=Path,default=REV/'code');p.add_argument('--baseline',type=Path,default=REV/'baseline/min');p.add_argument('--output',type=Path,default=REV/'integration_review');p.add_argument('--annotations',type=Path,action='append',default=[]);p.add_argument('--external-matrix',type=Path,default=REV/'bootstrap/acceptance/SOURCE_REVIEW_MATRIX.md');p.add_argument('--auxiliary-entry',type=Path,action='append',default=[]);a=p.parse_args()
    result=build(a.tree.resolve(),a.baseline.resolve(),a.output.resolve(),a.annotations,a.external_matrix,a.auxiliary_entry)
    return 1 if result['syntax_errors'] else 0


if __name__=='__main__':raise SystemExit(main())
