"""Revalidate saved outputs against frozen inputs before reports or package acceptance."""
from pathlib import Path
import argparse,copy,json,time

def scientific_receipt_projection(value):
    """Objective-labeled comparison rows are unordered; preserve every row/value.

    JSON sorts method dictionaries during persistence. Iterating those dictionaries
    may change only this diagnostic row order after reload. Sorting full rows keeps
    duplicates and every scientific field, unlike a set or key-to-row overwrite.
    """
    value=copy.deepcopy(value)
    if isinstance(value,dict) and isinstance(value.get('comparisons'),list):
        value['comparisons']=sorted(value['comparisons'],key=lambda row:json.dumps(row,sort_keys=True,separators=(',',':')))
    return value

def validate_saved_batch(tree,results,kind=None,selection=None):
    import run_stage1c as runner
    tree=Path(tree).resolve();results=Path(results).resolve()
    errors=[];queries=[];start=time.perf_counter()
    def fail(code,**detail):errors.append({'code':code,**detail})
    try:
        index=runner.read(results/'RESULTS_INDEX.json')
        actual_kind=index.get('scope');actual_selection=index.get('selection')
        if actual_kind not in ('public','min'):raise ValueError('Invalid batch scope')
        if kind is not None and actual_kind!=kind:fail('BATCH_SCOPE_MISMATCH',expected=kind,actual=actual_kind)
        if selection is not None and actual_selection!=selection:fail('BATCH_SELECTION_MISMATCH',expected=selection,actual=actual_selection)
        frozen,rows=runner.verify_freeze(tree,actual_kind)
        rows=runner.select_rows(rows,actual_selection)
        if not rows:raise ValueError('Empty or unknown frozen selection')
        if index.get('freeze_sha256')!=runner.file_hash(tree/'EXPERIMENT_FREEZE.json'):fail('PARENT_FREEZE_BINDING_MISMATCH')
        if index.get('execution_revision')!=frozen.get('execution_revision'):fail('EXECUTION_REVISION_BINDING_MISMATCH')
        selected={r['sample_id']:r for r in rows}
        listed=index.get('method_results_index')
        if not isinstance(listed,list):raise ValueError('Invalid sample index')
        ids=[r.get('sample_id') if isinstance(r,dict) else None for r in listed]
        if len(ids)!=len(set(ids)) or set(ids)!=set(selected):fail('FROZEN_QUERY_DOMAIN_MISMATCH',expected_count=len(selected),actual_count=len(ids))
        if index.get('sample_count')!=len(rows) or index.get('controlled_count')!=sum(r['kind']=='controlled' for r in rows) or index.get('real_count')!=sum(r['kind']=='real' for r in rows):fail('BATCH_COUNTS_MISMATCH')
        for entry in listed:
            query_errors=[]
            if not isinstance(entry,dict) or entry.get('sample_id') not in selected:
                fail('UNKNOWN_QUERY_ENTRY');continue
            sid=entry['sample_id'];row=selected[sid]
            def qfail(code,**detail):query_errors.append({'code':code,**detail})
            try:
                if entry.get('kind')!=row['kind'] or entry.get('input_fact_hash')!=row['input_fact_hash']:qfail('QUERY_INDEX_IDENTITY_MISMATCH')
                expected_path='samples/'+sid.replace(':','_').replace('/','_')
                if entry.get('path')!=expected_path:qfail('QUERY_RESULT_PATH_MISMATCH')
                folder=runner.safe_path(results,expected_path)
                methods=runner.read(folder/'METHOD_RESULTS.json')
                if entry.get('method_statuses')!={m:v.get('status','MISSING') for m,v in methods.items()}:qfail('QUERY_INDEX_METHOD_STATUS_MISMATCH')
                doc=runner.read(runner.safe_path(tree,row['observed_path']))
                contract,evaluation,ablations,_=runner.evaluate_query(tree,row,frozen,doc,methods)
                if contract.get('passed') is not True:qfail('RECOMPUTED_OUTPUT_CONTRACT_FAILED',errors=contract.get('errors'))
                if evaluation.get('passed') is not True:qfail('RECOMPUTED_SCIENTIFIC_CHECK_FAILED',errors=evaluation.get('errors'))
                saved_contract=runner.read(folder/'OUTPUT_CONTRACT.json')
                if saved_contract!=contract:qfail('CONTRACT_RECEIPT_RECOMPUTATION_MISMATCH')
                if scientific_receipt_projection(runner.read(folder/'EVALUATION.json'))!=scientific_receipt_projection(evaluation):qfail('SCIENTIFIC_RECEIPT_RECOMPUTATION_MISMATCH')
                if runner.read(folder/'ABLATIONS.json')!=ablations:qfail('ABLATION_RECEIPT_RECOMPUTATION_MISMATCH')
                receipt=runner.read(folder/'QUERY_ACCEPTANCE.json')
                if receipt.get('binding')!=runner.query_binding(tree,folder,row,frozen):qfail('QUERY_RECEIPT_BINDING_MISMATCH')
                if entry.get('query_acceptance_sha256')!=runner.file_hash(folder/'QUERY_ACCEPTANCE.json'):qfail('QUERY_RECEIPT_HASH_MISMATCH')
                if receipt.get('passed') is not True or receipt.get('contract_passed') is not True or receipt.get('scientific_checks_passed') is not True:qfail('QUERY_RECEIPT_FAILED')
                if entry.get('passed') is not True or entry.get('contract_passed') is not True or entry.get('evaluation_passed') is not True:qfail('QUERY_INDEX_FAILED')
            except Exception as exc:qfail('QUERY_REVALIDATION_EXCEPTION',detail=type(exc).__name__+': '+str(exc))
            queries.append({'sample_id':sid,'passed':not query_errors,'errors':query_errors})
        if index.get('passed') is not True or index.get('status')!='COMPLETED' or index.get('input_unchanged') is not True:fail('BATCH_RECEIPT_FAILED')
        if len(queries)!=len(rows):fail('REVALIDATION_COVERAGE_MISMATCH')
    except Exception as exc:fail('BATCH_REVALIDATION_EXCEPTION',detail=type(exc).__name__+': '+str(exc))
    passed=not errors and bool(queries) and all(q['passed'] for q in queries)
    return {'schema_version':'stage1c-r1-saved-result-gate-1.0','passed':passed,'status':'PASS' if passed else 'FAIL',
        'sample_count':len(queries),'failed_queries':sum(not q['passed'] for q in queries),'queries':queries,'errors':errors,
        'recomputed_common_contract_and_scientific_checks':True,'elapsed_validation_seconds':time.perf_counter()-start,
        'timing_scope':'Result acceptance verification; not algorithm execution timing'}

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--tree',type=Path,required=True);parser.add_argument('--results',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--kind',choices=('min','public'));parser.add_argument('--selection')
    args=parser.parse_args();receipt=validate_saved_batch(args.tree,args.results,args.kind,args.selection)
    from run_stage1c import write
    write(args.output,receipt);print(json.dumps({'status':receipt['status'],'samples':receipt['sample_count'],'failed_queries':receipt['failed_queries']}))
    return 0 if receipt['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
