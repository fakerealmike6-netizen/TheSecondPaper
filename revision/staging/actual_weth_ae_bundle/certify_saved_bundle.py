"""Revalidate one saved original-byte bundle; no disk batch export or acquisition."""
import argparse,hashlib,json,pathlib,sys,datetime
def sha(raw):return hashlib.sha256(raw).hexdigest()
def write(out,name,obj):
    p=out/name;p.write_text(json.dumps(obj,sort_keys=True,separators=(',',':'))+'\n',encoding='utf-8')
    return {'path':name,'sha256':sha(p.read_bytes()),'bytes':p.stat().st_size}
def certify(work,saved,out):
    work=pathlib.Path(work).resolve();saved=pathlib.Path(saved).resolve();out=pathlib.Path(out).resolve()
    if out==work or out in work.parents or out==saved or out in saved.parents or (out.exists() and any(out.iterdir())):raise ValueError('Fresh output directory required')
    sys.path.insert(0,str(work/'src'))
    from stage1d_shared_evidence import context_view
    from stage1d_semantic_units import certify_instance,validate_semantic_unit
    original=(saved/'RESULT.json').read_bytes();before=json.loads(original)
    ref=before['shared_evidence_context'];path=(saved/ref['path']).resolve()
    if not path.is_relative_to(saved):raise ValueError('Saved evidence path escaped bundle')
    raw=path.read_bytes()
    if sha(raw)!=ref['sha256'] or len(raw)!=ref['bytes']:raise ValueError('Saved shared originals changed')
    shared=json.loads(raw);view=context_view(shared);context=view[before['context_id']]
    out.mkdir(parents=True,exist_ok=True)
    result={'schema_version':'stage1d-actual-ae-saved-certification-v1','created_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'before':{'path':str(saved/'RESULT.json'),'sha256':sha(original),'status':before['status']},'context_id':before['context_id'],
        'external_requests':0,'producer_database_writes':0,'physical_batch_exports':0,'solver_runs':0,'live_catalogue_adopted':False,
        'source_sha256':{name:sha(pathlib.Path(sys.modules[name[:-3]].__file__).read_bytes()) for name in ['stage1d_semantic_units.py','stage1d_shared_evidence.py','stage1d_bq_portable.py','stage1d_batch_binding_route.py','weth_evidence.py']}}
    try:
        unit=certify_instance(before['policy'],context)
        receiver=context_view(json.loads(raw));validation=validate_semantic_unit(unit,evidence_context=receiver)
        if not validation['passed']:raise ValueError('Independent fresh receiver failed: '+str(validation))
    except (ValueError,KeyError,TypeError) as ex:
        result.update(status='SEMANTIC_CERTIFICATION_FAILED_OPEN',certified=False,not_a_data_gap=True,reason=type(ex).__name__+': '+str(ex))
        write(out,'RESULT.json',result);return result
    (out/'SHARED_EVIDENCE_CONTEXTS.json').write_bytes(raw)
    result.update(status='CERTIFIED_REAL_INSTANCE_PENDING_ROOT_ADOPTION',certified=True,unit=write(out,'UNIT.json',unit),
        shared_evidence_context={'path':'SHARED_EVIDENCE_CONTEXTS.json','sha256':sha(raw),'bytes':len(raw)},receiver=write(out,'RECEIVER_VALIDATION.json',validation),
        semantic_amount_raw=unit['input']['amount_raw'],target_source_amount_result=None,
        semantic_amount_is_target_source_result=False,query_source_variables_remain_isolated=True,
        shared_loader_validation_counts=view.validation_counts,independent_receiver_validation_counts=receiver.validation_counts)
    write(out,'RESULT.json',result);return result
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--work',required=True);ap.add_argument('--saved',required=True);ap.add_argument('--output',required=True);a=ap.parse_args()
    result=certify(a.work,a.saved,a.output);print(json.dumps({k:result[k] for k in ('status','certified','reason','semantic_amount_raw') if k in result},indent=2));return 0 if result['certified'] else 2
if __name__=='__main__':raise SystemExit(main())
