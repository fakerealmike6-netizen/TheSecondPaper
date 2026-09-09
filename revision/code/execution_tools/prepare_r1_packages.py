"""Append-only R1 package assembly; private originals are never modified."""
from pathlib import Path
import argparse,csv,difflib,hashlib,json,shutil,sys
R=Path(__file__).resolve().parent;ROOT=R.parents[3];CODE=R/'code'
sys.path.insert(0,str(CODE/'src'))
from run_stage1c import read,write,file_hash,verify_freeze

EXCLUDED_DIRS={'__pycache__','.test_tmp','.testtmp','test_scratch','.git','node_modules'}
def files(root):
    return {p.relative_to(root).as_posix():p for p in sorted(root.rglob('*')) if p.is_file() and not any(x in EXCLUDED_DIRS for x in p.relative_to(root).parts) and not p.name.endswith('.pyc')}
def cp(source,dest):
    dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source,dest)
def cpdir(source,dest):
    for name,p in files(source).items():cp(p,dest/name)
def manifest(root):
    (root/'12_FILE_HASHES.txt').write_text(''.join(file_hash(p)+'  '+n+'\n' for n,p in files(root).items() if n!='12_FILE_HASHES.txt'),encoding='utf-8')
def text(path,value):path.parent.mkdir(parents=True,exist_ok=True);path.write_text(value,encoding='utf-8')

def prepare(name,final):
    target=R/name;MIN=target/'min';PUBLIC=target/'public'
    if target.exists():raise ValueError('Fresh package preparation directory required')
    frozen,_=verify_freeze(CODE,'min');batch=read(R/'batch_final_v5/RESULTS_INDEX.json')
    tests=read(R/'checks/ALL_TESTS_FINAL_V5.json');faults=read(R/'faults_after_v5/FAULT_PROPAGATION_RESULTS.json')
    comparison=read(R/'comparison_final/SAME_INPUT_COMPARISON.json')
    assert batch['passed'] and batch['sample_count']==62 and tests['success'] and tests['tests_run']==950
    assert faults['passed'] and comparison['passed']
    fullfault=read(R/'full_public_validator_fault_v5/FULL_VALIDATOR_FAULT_CHECK.json') if (R/'full_public_validator_fault_v5/FULL_VALIDATOR_FAULT_CHECK.json').exists() else None
    if final:assert fullfault and fullfault['fault_chain_verified']
    MIN.mkdir(parents=True);PUBLIC.mkdir()
    inherited_roots={'EXPERIMENT_FREEZE.json','EXPERIMENT_INPUTS.json','REVISION_FREEZE.json','METHOD_SPEC_EFFECTIVE.md',
        'requirements.txt','.gitattributes','.gitignore','NOTICE.md','NEXT_BATCH_PROPOSAL.json','QUERY_CATALOG_CURRENT.csv','08_NEXT_BATCH_PROPOSAL.md'}
    prefixes=('src/','tests/','fixtures/','controlled_v1/','configs/','raw/','derived/','private/','baseline/','notes/','docs/','tools/','.github/','revision_freeze_history/')
    for n,p in files(CODE).items():
        if n in inherited_roots or n.startswith(prefixes):cp(p,MIN/n)
    # An exact-byte accepted scientific parent is provenance, not a second embedded ZIP/run tree.
    cp(CODE/'EXPERIMENT_FREEZE.json',MIN/'governance/PARENT_EXPERIMENT_FREEZE.json')
    for n in ('BASELINE_VERIFICATION.json','bootstrap/BOOTSTRAP_VERIFICATION.json'):
        cp(R/n,MIN/'governance'/Path(n).name)
    cp(ROOT/'AGENTS.md',MIN/'governance/ORIGINAL_AGENTS.md')
    cpdir(R/'bootstrap/acceptance',MIN/'governance/current_external_acceptance')
    for n,p in files(R/'bootstrap/package').items():
        if not n.endswith('.zip') and n!='PACKAGE_FILE_HASHES.txt':cp(p,MIN/'governance/current_authority'/n)
    cpdir(R/'reports_final_v5',MIN)
    cpdir(R/'reports_draft',MIN)
    cpdir(R/'independent_checks',MIN/'independent_checks')
    cpdir(R/'independent_results_v5',MIN/'independent_evidence')
    cpdir(R/'comparison_inputs',MIN/'comparison_inputs')
    comparison_readme=MIN/'comparison_inputs/README.md'
    text(comparison_readme,comparison_readme.read_text(encoding='utf-8').replace('tools/verify_packaged_same_inputs.py','review_tools/verify_packaged_same_inputs.py').replace('tools/compare_same_inputs.py','review_tools/compare_same_inputs.py'))
    for n in ('private/ledger/shared_budget_r4.sqlite','private/FINAL_BUDGET_SNAPSHOT_R4.json','private/CUMULATIVE_REQUEST_COST_ROWS_R4.json'):
        cp(R/'baseline/min'/n,MIN/'comparison_inputs/accepted_parent'/n)
    cpdir(R/'checks/old12_regression',MIN/'validation_evidence/old12_regression')
    for n in ('ALL_TESTS_FINAL_V5.json','ALL_TESTS_FINAL_V5.txt','OUTPUT_CONTRACT_TESTS.json','OUTPUT_CONTRACT_TESTS.txt'):
        cp(R/'checks'/n,MIN/'validation_evidence'/n)
    for n in ('OUTPUT_CONTRACT_MINIMAL_MIRROR_TESTS.json','OUTPUT_CONTRACT_MINIMAL_MIRROR_TESTS.txt'):
        cp(R/'checks'/n,MIN/'validation_evidence'/n)
    test_index=read(R/'checks/ALL_TESTS_FINAL_V5.json')
    test_index['test_log']='validation_evidence/ALL_TESTS_FINAL_V5.txt'
    write(MIN/'09_TEST_RESULTS.json',test_index)
    cp(R/'publication/REJECTED_PUBLICATION_ATTEMPT.json',MIN/'publication_review/REJECTED_PUBLICATION_ATTEMPT.json')
    cp(R/'publication/REJECTED_CANDIDATE_REMOTE_OBJECT_CHECK.json',MIN/'publication_review/REJECTED_CANDIDATE_REMOTE_OBJECT_CHECK.json')
    cp(R/'checks/PUBLIC_COMPARISON_PROJECTION_REPAIR_CHECK.json',MIN/'publication_review/PUBLIC_COMPARISON_PROJECTION_REPAIR_CHECK.json')
    cp(R/'checks/PUBLIC_MATRIX_PROJECTION_REPAIR_CHECK.json',MIN/'publication_review/PUBLIC_MATRIX_PROJECTION_REPAIR_CHECK.json')
    cp(R/'checks/REJECTED_PUBLIC_METADATA_ARCHIVE_IDENTITIES.json',MIN/'publication_review/REJECTED_CANDIDATE_IDENTITIES.json')
    cpdir(R/'checks/real_evidence_replay_final',MIN/'validation_evidence/real_evidence_replay')
    cp(R/'checks/packaged_same_input_controls/CONTROLS_RECEIPT.json',MIN/'validation_evidence/COMPACT_COMPARISON_CONTROLS.json')
    cpdir(R/'faults_before',MIN/'fault_evidence/before')
    cpdir(R/'faults_after_v5',MIN/'fault_evidence/after')
    cp(R/'checks/REPORT_RECOMPUTATION_FAILURE_V3.json',MIN/'execution_history/V3_REPORT_COMPARISON_ORDER.json')
    cp(R/'reports_r1/REPORT_ACCEPTANCE.json',MIN/'execution_history/V3_REPORT_ACCEPTANCE.json')
    cp(R/'reports_r1/STATISTICS.json',MIN/'execution_history/V3_FAILED_REPORT_STATISTICS.json')
    for n in ('FULL_VALIDATOR_FAULT_CHECK.json','PROCESS_RECEIPT.json','VALIDATION_RECEIPT.json','tests.json','tests.txt'):
        cp(R/'full_public_validator_fault'/n,MIN/'execution_history/v4_unit_mirror_failure'/n)
    for index in range(10):
        name='controlled-v1_gas_background_and_anchors_'+f'{index:02d}'
        cpdir(R/'batch_r1/samples'/name,MIN/'execution_history/v3_affected_queries'/name)
    if fullfault:
        # Keep actual invalid/valid mixed batch and receipts; omit executable mirrors
        # and duplicated temporary test fixtures generated by the validator itself.
        for n,p in files(R/'full_public_validator_fault_v5').items():
            if n.startswith(('stage1c_controlled/','logs/')) or '/' not in n:
                cp(p,MIN/'fault_evidence/full_package'/n)
    for n in ('run_fault_suite.py','validator_fault_hook.py','run_full_public_validator_fault.py','compare_same_inputs.py','verify_packaged_same_inputs.py'):
        cp(R/n,MIN/'review_tools'/n)
    for n in ('SAME_INPUT_COMPARISON.json','SAME_INPUT_COMPARISON.csv','SAME_INPUT_COMPARISON.md'):
        cp(R/'comparison_final'/n,MIN/n)
    for n in ('FULL_VERSION_INTEGRATION_MATRIX.json','FULL_VERSION_INTEGRATION_MATRIX.csv','INTEGRATION_REVIEW_SUMMARY.md'):
        cp(R/'integration_review'/n,MIN/n)
    auxiliary=[]
    for row in read(MIN/'FULL_VERSION_INTEGRATION_MATRIX.json')['files']:
        if not row['path'].startswith('@revision/'):continue
        relative=row['path'][len('@revision/'):]
        assert file_hash(R/relative)==row['sha256'],'Stale auxiliary inventory: '+relative
        cp(R/relative,MIN/'execution_tools'/relative)
        auxiliary.append({'matrix_path':row['path'],'package_path':'execution_tools/'+relative,'sha256':row['sha256']})
    write(MIN/'EXECUTION_TOOL_MAP.json',{'scope':'Exact auxiliary source for audit; original workspace-relative assembly scripts are not package replay commands. Use src/validate_stage1c.py for offline package replay.','files':auxiliary})
    # Split by declared provenance only; no result-dependent selection.
    for kind in ('controlled','real'):
        rows=[r for r in batch['method_results_index'] if r['kind']==kind];dest=MIN/'results'/kind
        for row in rows:cpdir(R/'batch_final_v5'/row['path'],dest/row['path'])
        idx={**batch,'selection':kind,'sample_count':len(rows),'controlled_count':len(rows) if kind=='controlled' else 0,
            'real_count':len(rows) if kind=='real' else 0,'method_results_index':rows}
        write(dest/'RESULTS_INDEX.json',idx)
        for tab in ('METHOD_SUPPORT_MATRIX.csv','PAIRED_RESULTS.csv','TIMINGS.csv','VALIDATION_TIMINGS.csv'):
            ids={r['sample_id'] for r in rows}
            with (R/'batch_final_v5'/tab).open(encoding='utf-8',newline='') as f:
                reader=csv.DictReader(f);fields=reader.fieldnames;data=[r for r in reader if r['sample_id'] in ids]
            with (dest/tab).open('w',encoding='utf-8',newline='') as f:
                w=csv.DictWriter(f,fields);w.writeheader();w.writerows(data)
    changes=[];diff=[]
    for n,p in files(CODE).items():
        if not n.startswith(('src/','tests/','.github/')):continue
        old=R/'baseline/min'/n
        if not old.exists() or old.read_bytes()!=p.read_bytes():
            changes.append({'path':n,'parent_sha256':file_hash(old) if old.exists() else None,'revision_sha256':file_hash(p)})
            diff.extend(difflib.unified_diff(old.read_text(encoding='utf-8').splitlines(True) if old.exists() else [],p.read_text(encoding='utf-8').splitlines(True),fromfile='accepted-stage1c/'+n,tofile='stage1c-r1/'+n))
    text(MIN/'docs/STAGE1C_R1_SOURCE_DIFF.patch',''.join(diff))
    write(MIN/'SOURCE_IDENTITY_AND_DIFF.json',{'parent_commit':'d88b839361b8fa93308430641baf7af423d76ae8','parent_tree':'cef2f09aaa6033e9cb7393d6dc5cad6119a4f450',
        'changes':changes,'original_tests_retained':865,'new_tests':85,'method_semantics_changed':False,'execution_revision':frozen['execution_revision']})
    faultbrief={'passed':faults['passed'],'cases_executed':faults['cases_executed'],'negative_cases':faults['fault_cases'],'positive_cases':faults['positive_cases'],
        'source_and_scientific_inputs_unchanged':faults['source_and_scientific_inputs_unchanged'],
        'cases':[{k:r[k] for k in ('case','expected_cli_exit_code','process_exit_code','report_process_exit_code','validator_process_exit_code','fault_chain_verified')} for r in faults['cases']],
        'full_package_fault':{k:fullfault[k] for k in ('fault_chain_verified','validator_cli_exit_code','validator_passed','batch_query_count','bad_query_count','good_query_count','no_payload_hash_corruption','no_missing_required_check_shortcut')} if fullfault else {'status':'PREFLIGHT_PENDING'},
        'full_diagnostics':'MIN fault_evidence; public synthetic originals and return-boundary tests'}
    write(MIN/'05_FAULT_PROPAGATION_RESULTS.json',faultbrief)
    closure={'schema_version':'stage1c-r1-repair-closure-v1','finding':'C1-V01','internal_status':'CLOSED_WITH_EXECUTED_EVIDENCE' if final else 'DRAFT_AWAITING_FULL_PACKAGE_FAULT',
        'symptoms':{'A_expected_output_domains':{'status':'PASS','evidence':['OUTPUT_CONTRACT_SPEC.md','fault_evidence/after/full_missing_output/']},
            'B_complete_haircut_assignment_and_report_equality':{'status':'PASS','evidence':['fault_evidence/after/haircut_missing_witness_bad_point/','fault_evidence/after/haircut_bad_point_with_valid_witness/']},
            'C_hard_invariant_and_failure_propagation':{'status':'PASS' if fullfault else 'PACKAGE_FAULT_PENDING','evidence':['05_FAULT_PROPAGATION_RESULTS.json','fault_evidence/full_package/FULL_VALIDATOR_FAULT_CHECK.json']}},
        'tests_passed':950,'retained_tests':865,'new_tests':85,'same_input_method_pairs':comparison['method_pairs_equal'],
        'scientific_differences':len(comparison['field_differences']),'old12_32':'PASS','research_requests':0,'new_research_cost':'0',
        'external_acceptance':'PENDING_REVIEW','checkpoint':'CHECKPOINT_1C_R1_REACHED',
        'final_zip_execution_receipts':'External EXTRACTED_VALIDATION_RECEIPTS.json binds final archive hashes; no self-referential receipt inside archive.'}
    write(MIN/'REPAIR_CLOSURE.json',closure)
    write(MIN/'02_FINAL_STATUS.json',{'stage':'Stage1C-R1','checkpoint':'CHECKPOINT_1C_R1_REACHED','internal_repair':closure['internal_status'],
        'controlled_samples':60,'real_queries':2,'same_scientific_outputs':True,'tests_passed':950,'external_acceptance':'PENDING_REVIEW',
        'new_research_requests':0,'new_research_cost':'0','Stage1D_started':False,'next_query_collection_started':False,'publication_and_final_zip_validation':'SEE_EXTERNAL_RECEIPTS'})
    write(MIN/'RESULTS_INDEX.json',{'schema_version':'stage1c-r1-package-navigation-index-v1','not_a_runner_batch':True,'queries':62,
        'batches':[{'path':'results/'+k,'queries':60 if k=='controlled' else 2,'index_sha256':file_hash(MIN/'results'/k/'RESULTS_INDEX.json')} for k in ('controlled','real')],
        'canonical_batch_index_sha256':file_hash(R/'batch_final_v5/RESULTS_INDEX.json'),'execution_revision':frozen['execution_revision']})
    make_docs(MIN,final,comparison,tests)
    # Start from inherited public-safe paths and explicit current-public additions.
    inherited_public=set(files(R/'baseline/public'))
    private_prefix=('private/','raw/','derived/','baseline/','governance/','results/real/','independent_checks/','independent_evidence/','fault_evidence/','review_tools/','validation_evidence/real_evidence_replay/')
    new_safe={'REVISION_FREEZE.json','configs/STAGE1C_R1_EXECUTION_POLICY.json','REPAIR_CLOSURE.json','05_FAULT_PROPAGATION_RESULTS.json',
        'OUTPUT_CONTRACT_SPEC.md','01_CHECKPOINT_1C_R1_REPORT.md','03_C1_V01_REPAIR_AND_CONTRACT.md','04_FULL_VERSION_INTEGRATION_REVIEW.md',
        '06_SAME_INPUT_60_PLUS_2_COMPARISON.md','07_REPRODUCE.md','11_OPEN_ITEMS.md','REVIEW_HANDOFF.md'}
    for n,p in files(MIN).items():
        if n.startswith(private_prefix) or n in {'configs/STAGE1C_R1_POLICY.json','QUERY_CATALOG_CURRENT.csv','08_USAGE_AND_SCOPE.md','validation_evidence/COMPACT_COMPARISON_CONTROLS.json'}:continue
        allow=n in inherited_public or n in new_safe or n.startswith(('src/','tests/','results/controlled/','validation_evidence/','docs/','tools/','.github/'))
        if allow:cp(p,PUBLIC/n)
    for n in ('run_fault_suite.py','validator_fault_hook.py','run_full_public_validator_fault.py','compare_same_inputs.py','verify_packaged_same_inputs.py'):
        cp(MIN/'review_tools'/n,PUBLIC/'review_tools'/n)
    # Six current method-level positive/negative semantics stay fully reproducible
    # via public source/tests. Publish exact observed-output evidence for the old four.
    for case in ('normal_positive_control','full_missing_output','haircut_missing_witness_bad_point','haircut_bad_point_with_valid_witness','balance_ablation_non_nested'):
        cpdir(R/'faults_after_v5'/case,PUBLIC/'fault_evidence/after'/case)
    for n in ('FULL_VERSION_INTEGRATION_MATRIX.json','FULL_VERSION_INTEGRATION_MATRIX.csv','INTEGRATION_REVIEW_SUMMARY.md'):
        cp(R/'integration_review/public'/n,PUBLIC/n)
    cp(R/'integration_review/public/INTEGRATION_REVIEW_SUMMARY.md',PUBLIC/'04_FULL_VERSION_INTEGRATION_REVIEW.md')
    # Zero scientific differences do not make diagnostic field paths public:
    # real address/event identities also occur inside excluded-field pointers.
    def public_comparison_view(value):
        result=json.loads(json.dumps({k:v for k,v in value.items()
            if k not in ('identity_evidence','baseline_root','revision_code_root','new_batch_root')}))
        assert not result['field_differences']
        for row in result['method_checks']:
            if row['kind']=='real':
                for field in ('excluded_baseline_fields','excluded_revision_fields'):
                    paths=row.pop(field)
                    row[field+'_count']=len(paths)
        result['public_detail_policy']='Real-query excluded-field pointers are MIN-only; counts and same-input scientific comparison conclusions are retained. Private input/file identities and host paths are omitted.'
        return result
    public_comparison=public_comparison_view(comparison)
    write(PUBLIC/'SAME_INPUT_COMPARISON.json',public_comparison)
    cp(R/'comparison_final/SAME_INPUT_COMPARISON.csv',PUBLIC/'SAME_INPUT_COMPARISON.csv')
    cp(R/'comparison_final/SAME_INPUT_COMPARISON.md',PUBLIC/'SAME_INPUT_COMPARISON.md')
    # Reuse exact already public next-batch proposal; no queue reselection.
    cp(R/'baseline/public/NEXT_BATCH_PROPOSAL.json',PUBLIC/'NEXT_BATCH_PROPOSAL.json')
    idx=read(PUBLIC/'results/controlled/RESULTS_INDEX.json');idx['scope']='public';write(PUBLIC/'results/controlled/RESULTS_INDEX.json',idx)
    write(PUBLIC/'RESULTS_INDEX.json',{'schema_version':'stage1c-r1-package-navigation-index-v1','not_a_runner_batch':True,'queries':60,
        'batches':[{'path':'results/controlled','queries':60,'index_sha256':file_hash(PUBLIC/'results/controlled/RESULTS_INDEX.json')}],
        'private_real_replay':'MIN only; authorized query-level real aggregates in STATISTICS.json','execution_revision':frozen['execution_revision']})
    # Public fault controls must revalidate without private manifests.
    for p in (PUBLIC/'fault_evidence').rglob('RESULTS_INDEX.json'):
        value=read(p);value['scope']='public';write(p,value)
    for n in ('METHOD_SUPPORT_MATRIX.csv','PAIRED_RESULTS.csv','TIMINGS.csv','VALIDATION_TIMINGS.csv'):
        cp(PUBLIC/'results/controlled'/n,PUBLIC/n)
    targeted=read(MIN/'validation_evidence/OUTPUT_CONTRACT_TESTS.json')
    targeted={k:v for k,v in targeted.items() if k!='working_directory'}
    targeted['working_directory']='Package code root; local host path omitted from public metadata'
    write(PUBLIC/'validation_evidence/OUTPUT_CONTRACT_TESTS.json',targeted)
    mirror=read(MIN/'validation_evidence/OUTPUT_CONTRACT_MINIMAL_MIRROR_TESTS.json')
    mirror={k:v for k,v in mirror.items() if k not in ('command','working_directory','source_directory')}
    mirror['command']=['python','-B','-m','unittest','discover','-s','tests','-p','test_stage1c_output_contract.py','-v']
    mirror['working_directory']='Minimal src/tests/fixtures/configs/controlled_v1 mirror; local host paths omitted'
    write(PUBLIC/'validation_evidence/OUTPUT_CONTRACT_MINIMAL_MIRROR_TESTS.json',mirror)
    # Scan every proposed public payload byte, including src/tests, against the
    # identifiers in the two frozen real inputs. This is input-specific, not a
    # ban on generic Ethereum/WETH constants or synthetic address fixtures.
    def scan_known_real_identifiers(selected,input_documents):
        import re
        token=re.compile(rb'(?<![0-9a-fA-F])0x(?:[0-9a-fA-F]{64}|[0-9a-fA-F]{40})(?![0-9a-fA-F])',re.I)
        assert len(input_documents)==2,'Exactly two frozen real context inputs required for publication scan'
        known={item.lower() for data in input_documents for item in token.findall(data)}
        assert known,'Real-input identifier scan must not silently use an empty reference set'
        findings=[]
        for name,data in sorted(selected.items()):
            matched=known & {item.lower() for item in token.findall(data)}
            if matched:
                findings.append({'path':name,'rule':'KNOWN_REAL_INPUT_IDENTIFIER',
                    'distinct_identifier_count':len(matched)})
        return {'schema_version':'stage1c-r1-public-real-identifier-scan-v1',
            'passed':not findings,'real_inputs_scanned':len(input_documents),
            'specific_identifiers_checked':len(known),'public_files_scanned':len(selected),
            'includes_source_and_tests':True,'findings':findings,
            'scope':'Exact case-insensitive complete 0x40/0x64 tokens from the two frozen real model inputs; no raw identifier or private-input hash is exposed in this receipt.'}
    input_manifest=read(MIN/'EXPERIMENT_INPUTS.json')
    real_manifest=(MIN/input_manifest['real_private_manifest']).resolve()
    assert real_manifest.is_relative_to(MIN.resolve()),'Real manifest must remain within MIN'
    real_rows=read(real_manifest)
    real_documents=[]
    for row in real_rows:
        observed=(MIN/row['observed_path']).resolve()
        assert observed.is_relative_to(MIN.resolve()),'Real observed input must remain within MIN'
        assert file_hash(observed)==row['observed_sha256'],'Real observed identity changed before publication scan'
        real_documents.append(observed.read_bytes())
    selected={n:p.read_bytes() for n,p in files(PUBLIC).items()}
    exact_scan=scan_known_real_identifiers(selected,real_documents)
    write(target/'PUBLIC_REAL_IDENTIFIER_SCAN.json',exact_scan)
    from public_export_r1 import scan_selected
    scan=scan_selected(selected)+exact_scan['findings']
    write(target/'PUBLIC_CONTENT_SCAN.json',scan)
    if scan:raise ValueError('Public scan requires explicit review: '+repr(scan))
    assert not any(n.startswith(('private/','raw/','derived/','baseline/','governance/','results/real/')) or n.endswith(('.sqlite','.zip')) for n in files(PUBLIC))
    for n,p in files(PUBLIC).items():
        if '47.167873093' in p.read_text(encoding='utf-8',errors='ignore') or '52.832126907' in p.read_text(encoding='utf-8',errors='ignore'):
            raise ValueError('Historical billing summary in public file: '+n)
    rows=[]
    for n,p in files(PUBLIC).items():
        counterpart=n if (MIN/n).exists() and (MIN/n).read_bytes()==p.read_bytes() else 'public_views/'+n
        if counterpart!=n:cp(p,MIN/counterpart)
        rows.append({'public_path':n,'local_path':counterpart,'bytes':p.stat().st_size,'sha256':file_hash(p),'byte_identical':True})
    local=[{'path':n,'sha256':file_hash(p)} for n,p in files(MIN).items()]
    equivalence={'schema_version':'stage1c-r1-public-local-equivalence-dag-v1','public_rows':rows,'all_public_files_have_byte_identical_private_counterpart':True,
        'same_source_and_test_bytes':all((MIN/n).read_bytes()==p.read_bytes() for n,p in files(PUBLIC).items() if n.startswith(('src/','tests/'))),
        'private_payload_inventory_sha256':hashlib.sha256(json.dumps(local,sort_keys=True).encode()).hexdigest(),
        'excluded_generated':['PUBLIC_LOCAL_EQUIVALENCE.json','12_FILE_HASHES.txt'],'publication_receipt_is_external':True,'private_data_published':False}
    write(MIN/'PUBLIC_LOCAL_EQUIVALENCE.json',equivalence)
    public_equivalence={k:v for k,v in equivalence.items() if k!='private_payload_inventory_sha256'}
    public_equivalence['private_inventory_binding']='Retained only in the private equivalence receipt'
    write(PUBLIC/'PUBLIC_LOCAL_EQUIVALENCE.json',public_equivalence)
    for tree in (MIN,PUBLIC):manifest(tree)
    verify_freeze(MIN,'min');verify_freeze(PUBLIC,'public')
    write(target/'PREPARED_TREES.json',{'min_files':len(files(MIN)),'public_files':len(files(PUBLIC)),'final':final,'same_source_and_test_bytes':True,
        'source_freeze':frozen['execution_revision'],'all_scientific_input_hashes_unchanged':True,'changes':changes})
    print(json.dumps({'min_files':len(files(MIN)),'public_files':len(files(PUBLIC)),'final':final}))

def make_docs(tree,final,comparison,tests):
    status='Internal repair complete; final ZIP execution/publication recorded externally.' if final else 'Draft preflight; full package fault control pending.'
    readme=f'''# Stage1C-R1 review

{status} External acceptance: **PENDING_REVIEW**. Stop: **CHECKPOINT_1C_R1_REACHED**.

Start at 00_REVIEW_INDEX.md. This revision repairs C1-V01 acceptance, status propagation, reports and extracted-package validation. Scientific methods, the original60 controlled observations/hidden assignments, two developed real ETH contexts, labels and reference denominators are unchanged.

Windows Python3.14.3 / NumPy2.4.2 / SciPy1.17.1 actually tested. Use an existing environment; no real credentials or dependency downloads are required on that host. Choose the command matching the package and a fresh output directory:

```text
python -B src/validate_stage1c.py --tree . --kind public --output ../r1_public_check
python -B src/validate_stage1c.py --tree . --kind min --output ../r1_min_check
```

The validator uses the inherited strict offline guard, removes credential environment variables, blocks sockets/DNS and unguarded children, then runs950 tests, original12 scenarios/32 comparisons,60 controlled experiments and MIN-only two real experiments/evidence replay. Saved results and regenerated results each pass the common contract and scientific checks before equality comparison. The root RESULTS_INDEX is navigation; executable batch indices are in results/controlled and results/real.

```text
python -B src/run_stage1c.py --tree . --kind public --selection controlled --output ../r1_controlled
python -B src/stage1c_reports.py --tree . --results results/controlled --output ../r1_controlled_report
python -B src/stage1c_result_gate.py --tree . --results results/controlled --output ../r1_saved_gate.json
python -B tests/test_stage1c_r1_faults.py --fault-child --tree . --kind public --case full_missing_output --output ../r1_expected_failure
```

The last command intentionally exits1 and saves the missing-output failure. Use normal_positive_control for exit0. Full38-case diagnostics and3 independently implemented external checks are in MIN. Direct commands require the same offline conditions; final recorded validation used the guarded package entry. No --freeze or --freeze-revision is needed for review. The original EXPERIMENT_FREEZE is immutable; REVISION_FREEZE separately binds current execution code and the public execution policy. The complete authorizing policy and account records remain private.

Neither package authorizes new data collection, queue execution, Stage1D, or full-cohort evaluation. Linux GitHub archive construction is not Linux experiment testing.
'''
    for n in ('README.md','07_REPRODUCE.md','11_REPRODUCE.md'):text(tree/n,readme)
    text(tree/'docs/TEST_SCOPE_CLARIFICATION.md','''# Test scope clarification

The inherited run_tests receipt keeps its legacy fixture-description string. In a local MIN work copy, the new R1 unit tests may read already accepted saved real-format outputs when present; in the extracted validator's source/test mirror, they use explicitly synthetic context-format fixtures. The full MIN validator separately executes both actual real queries and evidence replay. The three real return-boundary fault CLI cases used the actual immutable Atomic input. These different paths are identified in their receipts; the legacy fixture label is not a claim that every local test read only synthetic bytes.

The v4 full-package fault run correctly failed, but its prerequisite unit check also exposed one new test's dependency on root manifests/saved results absent from the validator mirror. That run is retained as failed evidence, not counted as a completed fault proof. The v5 test reads the same six frozen observed scenarios from controlled_v1/MANIFEST.json, verifies their identities and dispatches the real method implementations. It preserves all six NA assertions and adds checks for every null output field. The minimal mirror test passed36/36 without root manifests/results; the final v5 suite passed950/950 with no skips. OUTPUT_CONTRACT_TESTS is the earlier development receipt; OUTPUT_CONTRACT_MINIMAL_MIRROR_TESTS and ALL_TESTS_FINAL_V5 bind the final repaired test version.
''')
    text(tree/'00_REVIEW_INDEX.md','''# Stage1C-R1 review index

1. 01_CHECKPOINT_1C_R1_REPORT.md,02_FINAL_STATUS.json,REPAIR_CLOSURE.json: actual repair and scope.
2. OUTPUT_CONTRACT_SPEC.md,03_C1_V01_REPAIR_AND_CONTRACT.md and docs/STAGE1C_R1_SOURCE_DIFF.patch: complete output domains, exact Haircut assignment equality, hard invariants and failure chain.
3. FULL_VERSION_INTEGRATION_MATRIX.csv/.json and04_FULL_VERSION_INTEGRATION_REVIEW.md: all current source/tests/SQL/config/entry inventory and honest review depth.
4. 05_FAULT_PROPAGATION_RESULTS.json and fault_evidence/: original/final invalid returns and positive controls. MIN retains all38 cases and full package mixed-fault replay; public retains synthetic examples and executable tests.
5. SAME_INPUT_COMPARISON.json/.csv,06_SAME_INPUT_60_PLUS_2_COMPARISON.md,results/: all434 paired methods,124 common scientific artifacts and441 original file identities.
6. STATISTICS.json and04_CONTROLLED_EXPERIMENT_RESULTS.md/05_REAL_PILOT_COMPARISONS.md: unchanged scientific findings and separate validation timing. MIN independent_evidence/ has actual external DP/Fraction/statistics replay.
7. 09_TEST_RESULTS.json,validation_evidence/:950 valid tests (865 inherited+85 new) and old12/32.
8. 07_REPRODUCE.md,12_FILE_HASHES.txt,PUBLIC_LOCAL_EQUIVALENCE.json: offline reproduction and byte correspondence.
9. 10_GITHUB_PUBLICATION_REPORT.md and external PUBLICATION_RECEIPT.json,EXTRACTED_VALIDATION_RECEIPTS.json,REVIEW_HANDOFF.md: fixed public identity and final extracted ZIP execution. Receipts remain outside payloads to avoid cycles.

External acceptance PENDING_REVIEW. CHECKPOINT_1C_R1_REACHED. The4+2 queue remains proposal-only and unchanged.
''')
    text(tree/'01_CHECKPOINT_1C_R1_REPORT.md',f'''# Stage1C-R1: acceptance repair and same-input regression

{status} C1-V01 is repaired through one input-derived common acceptance contract for controlled and real queries. Missing results, unsupported NA, wrong identities, infeasible/inconsistent Haircut points and applicable hard-invariant failures cannot be certified. Original bad returns are saved before evaluation. A bad query remains in the denominator, fails the batch/CLI/report/package, and does not suppress later good queries.

950/950 Windows tests passed with zero failures/skips/network attempts; all865 original tests are unchanged. Original12 scenarios/32 comparisons pass. Final38-case suite:32 negative cases exit1 and6 positives exit0, with actual report and saved-result-gate agreement. Same60+2 input batch completed all434 method runs; six controlled Haircut NA remain included. All{comparison['method_pairs_equal']} paired scientific method outputs and124 shared evaluation/ablation artifacts match; there are no deterministic scientific differences.441 original input/evidence identities remain unchanged. Validation overhead is separate from1 warm-up+5 method timings.

Controlled FULL endpoints have zero exact Oracle error and all hidden assignments are covered. Two real source-amount truths remain unknown: Atomic FULL359.495 ETH; Harmony FULL[504.907298683760128997,1554.999042315467482] ETH. Haircut remains a point under declared B_min completion, not true opening balance. Reachability has no amount, Poison is nominal, and independent target-copy sums are not a common-source realizable total. Both real ETH queries lack a connected supported WETH conversion and preserve protocol-ablation equality.

This is regression on developed first-batch inputs, not holdout or external blind truth. Current contract audits complete primal allocations/aggregates and structural invariants; it does not claim a new general independent dual proof. The separate supplied DP/Fraction/statistics checks provide additional same-batch evidence. Research requests/cost are0; next4+2 queue is unchanged and unexecuted. External acceptance remains PENDING_REVIEW. CHECKPOINT_1C_R1_REACHED.
''')
    text(tree/'03_C1_V01_REPAIR_AND_CONTRACT.md','''# C1-V01 repair

Expected method-specific output domains are derived from frozen physical ports and independently checked against declared target groups. Interval methods require every target address/entry and asset union, including explicit zero. Baselines retain their full visited-port domain. The genuine empty-target interval source-asset[0,0] and baseline empty joint forms remain valid. Only input-justified Haircut unknown-balance NA is accepted.

Successful Haircut must supply every allocation port. Exact rational checks bind event points and aliases to allocations, address points to frozen service-entry sums and per-asset joint points to their unique union. A full allocation audit checks timing, capacity, balances, source, fees and boundary constraints after generation. It never clips points, substitutes hidden truth or uses FULL endpoints to generate a baseline.

Applicable balance nesting, independent-copy decomposition/singleton equality, protocol boundary preservation and absent-feature equality are hard requirements. Query receipts bind method outputs, inputs, parent and revision freeze, contract and scientific artifacts. Offline reports and package validation independently recompute this path, so matching two malformed projections or forged passed flags cannot certify results. Native method identity failures are preserved before the runner adds its envelope, and update method/contract/query failure states.

The four original CLI counterexamples are preserved before repair, then rejected after repair.32 negative and6 positive CLI/report/saved-gate controls plus the complete public validator mixed batch exercise the chain. Full file/domain and exception details are retained in fault_evidence. See OUTPUT_CONTRACT_SPEC.md for exact type and domain rules and05_FAULT_PROPAGATION_RESULTS.json for executed outcomes.
''')
    cp(R/'integration_review/INTEGRATION_REVIEW_SUMMARY.md',tree/'04_FULL_VERSION_INTEGRATION_REVIEW.md')
    cp(R/'comparison_final/SAME_INPUT_COMPARISON.md',tree/'06_SAME_INPUT_60_PLUS_2_COMPARISON.md')
    text(tree/'08_USAGE_AND_SCOPE.md','''# Usage and unchanged boundaries

New research platform requests0; new research charges/risk0. GitHub reads/publication are separate from research requests. No Dune/RPC/Alchemy/BigQuery/Meta endpoint was used. No usage balance refresh or release of unknown charges occurred. Inherited risk47.167873093 and remaining52.832126907 are reconciled against unchanged private ledger bytes, not a fresh account grant. Original Dune account20/shared cumulative100 and80 reminder remain; RPC500,Alchemy50000 CU,BigQuery5GiB caps and original recovery protections are preserved but unused.

No new fact gap requiring collection was found for this same-input contract repair. No582-label backfill, one-time recovery-package update, new query collection, MFTracer/AMLGuard execution, Stage1D or new full-cohort denominator was started. Existing4+2 proposal is byte-preserved.
''')
    text(tree/'10_GITHUB_PUBLICATION_REPORT.md','''# Publication identity and DAG

The public revision appends to d88b839361b8fa93308430641baf7af423d76ae8. A unique stage1c-r1-20260908T104446-0800 branch/tag/Release is used; main and old tags/branches remain unchanged. Repository access and parent/tree were checked through the existing authorized connector before writes. User authorization for Atomic/Harmony query-level aggregates persists; real identities, per-event allocations, original references/labels, account ledger rows, MIN and private handoff are excluded.

The narrow workflow checks out exactly its triggering public commit and builds a deterministic public ZIP with existing free Ubuntu CI. It creates the unique Release without overwrite. This is archive construction, not Linux scientific testing. The external PUBLICATION_RECEIPT records actual success, exact remote tag/commit/tree, asset size/SHA and downloaded-byte verification.

Payload -> PUBLIC_LOCAL_EQUIVALENCE ->12_FILE_HASHES ->public/MIN ZIPs+sidecars ->external final validation/publication receipts ->private handoff+sidecar. No artifact hashes itself or embeds a receipt binding its enclosing ZIP. Final ZIPs are actually extracted and executed with the inherited strict offline guard; their receipts are external and bind archive hashes.
''')
    text(tree/'11_OPEN_ITEMS.md','''# Open items and limits

External acceptance is PENDING_REVIEW; internal repair evidence does not decide research Go/Revise/Stop. Real amount truth remains unavailable, Haircut B_min is an assumption, and both real protocol features are absent. Six legitimate Haircut NA remain. This same developed batch is not a holdout and does not freeze a future evaluation population. Static import reachability and inherited review are explicitly distinguished from fresh semantic review; no claim of whole-history line-by-line audit or absence of all defects is made.

No new material fact gap or requested query collection remains within this repair scope. Final publication/download and extracted-package outcomes are authoritative in the external receipts. CHECKPOINT_1C_R1_REACHED; do not proceed to Stage1D or new data acquisition.
''')
    text(tree/'REVIEW_HANDOFF.md','''# Stage1C-R1 handoff

Upload the private Stage1C_R1_Review_Handoff.zip and its sidecar once for review. It contains both final bundles, their sidecars and external publication/validation/closure receipts. The public bundle may be shared; MIN and this handoff must remain private. Start with00_REVIEW_INDEX.md inside the selected bundle. External receipts bind exact final ZIP sizes/SHA and the fixed public commit/tag/Release; public/local equivalence maps every public payload file to identical private bytes.

Expected behavior:950 tests and old12/32 pass; same60+2 scientific outputs unchanged; four old negatives and same-class failures are rejected, legal controls remain accepted. See full integration matrix for review depth,05_FAULT_PROPAGATION_RESULTS for failures, andSAME_INPUT_COMPARISON for exact regression. Windows scientific replay only; any Linux workflow is packaging only. External acceptance PENDING_REVIEW. CHECKPOINT_1C_R1_REACHED.
''')
    text(tree/'docs/RELEASE_NOTES.md','''Stage1C-R1 repairs input-derived complete output acceptance, exact Haircut allocation/report consistency and hard-invariant propagation through query/batch/CLI/report/extracted-package validation. Scientific methods, original60 controlled+2 real inputs and references are unchanged. Windows950 tests (865 retained+85 new), original12/32,38 fault/positive CLI chains and same434 paired method results pass their respective criteria. The public bundle contains source/tests/synthetic evidence and authorized query-level aggregates only. MIN/account ledgers/private handoff are excluded. External acceptance PENDING_REVIEW; CHECKPOINT_1C_R1_REACHED. Start at00_REVIEW_INDEX.md.
''')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--name',required=True);p.add_argument('--final',action='store_true');a=p.parse_args();prepare(a.name,a.final)
