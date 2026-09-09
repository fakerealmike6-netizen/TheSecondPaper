"""Root-only offline runner; produces a new report/material copy, no install."""
from pathlib import Path
import argparse,hashlib,json,socket,sys,time

S=Path(__file__).resolve().parent;R=S.parents[1];C=R/'code'


def checked_path(value):
    p=(R/value).resolve()
    if not p.is_relative_to(R) or p.is_symlink():raise ValueError('Explicit input/output must remain within revision')
    return p


def output_directory(value):
    p=checked_path(value)
    if p.relative_to(R).parts[0] not in ('reports','operations') or p.exists():
        raise ValueError('Output must be a new report/operation directory, never code or an existing report')
    return p


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--query',required=True,choices=('txphish_src001','txphish_src002','xscam_src001','lifi_src001'))
    parser.add_argument('--sources',default='staging/current_context_paid_bq_admission/EXISTING_PAID_3JOB_INPUT.json')
    parser.add_argument('--point-bindings',help='Explicit revision-relative JSON list or {point_bindings:[{plan,member}]}')
    parser.add_argument('--output',required=True,help='New revision-relative operations/ or reports/ directory')
    args=parser.parse_args(argv);out=output_directory(args.output)
    if (C/'private/network_worker.lock').exists():raise RuntimeError('Wait for existing writer safe point')
    def blocked(*a,**k):raise RuntimeError('Paid BQ admission is offline only')
    socket.socket.connect=blocked;socket.create_connection=blocked
    sys.path[:0]=[str(S/'src'),str(C/'src')]
    import stage1d_current_context_paid_bq as admission
    import stage1d_bq_context_prepare as h
    input_path=checked_path(args.sources);inventory=json.loads(input_path.read_bytes())
    inputs=[{'path':input_path.relative_to(R).as_posix(),'sha256':h.sha(input_path)}]
    points=[]
    if args.point_bindings:
        p=checked_path(args.point_bindings);points=json.loads(p.read_bytes())
        inputs.append({'path':p.relative_to(R).as_posix(),'sha256':h.sha(p)})
        if isinstance(points,dict):points=points['point_bindings']
    source_hashes={}
    for module in tuple(sys.modules.values()):
        p=getattr(module,'__file__',None)
        if p and Path(p).resolve().is_relative_to(R):source_hashes[str(Path(p).resolve().relative_to(R))]=h.sha(p)
    qdir=C/'derived/stage1d/queries'/args.query
    started=time.perf_counter();cpu=time.process_time()
    result=admission.admit_current(C,args.query,h.dep(C,qdir/'collection.json'),h.dep(C,qdir/'label_snapshot.json'),
        inventory['sources'],point_bindings=points)
    for rel,digest in source_hashes.items():
        if h.sha(R/rel)!=digest:raise ValueError('Loaded source changed during admission')
    for ref in inputs:
        if h.sha(R/ref['path'])!=ref['sha256']:raise ValueError('Explicit input list changed during admission')
    if (C/'private/network_worker.lock').exists():raise RuntimeError('Writer appeared during admission')
    # Evidence remains original. This material is a derived copy for root to
    # review/register; output completion is not a FULL context assertion.
    summary={'schema_version':'stage1d-paid-bq-admission-operation-v1','status':'OFFLINE_PAID_SOURCE_ADMISSION_SAVED',
        'query_name':args.query,'query_id':result['query_id'],'scope_hash':result['scope_hash'],
        'input_refs':inputs,'sources':result['sources'],'source_sha256':source_hashes,
        'current_input_refs':result['current_input_refs'],'rows':len(result['rows']),'coverage_rows':len(result['coverage']),
        'paid_pending_binding_rows':len(result['paid_pending_binding_by_kind']),
        'missing_paid_source_rows':len(result['missing_paid_source_by_kind']),
        'point_binding_request_keys':len(result['point_binding_requests']),
        'new_requests_executed':0,'coverage_installed':False,'full_context_claimed':False,
        'wall_seconds':time.perf_counter()-started,'cpu_seconds':time.process_time()-cpu}
    out.mkdir(parents=True)
    for name,value in [('MATERIAL_ADMISSION.json',result),('SUMMARY.json',summary)]:
        with (out/name).open('x',encoding='utf-8') as f:json.dump(value,f,sort_keys=True,indent=2)
    manifest={'files':[{'path':(out/n).relative_to(R).as_posix(),'sha256':h.sha(out/n),'bytes':(out/n).stat().st_size}
        for n in ('MATERIAL_ADMISSION.json','SUMMARY.json')]}
    with (out/'MANIFEST.json').open('x',encoding='utf-8') as f:json.dump(manifest,f,sort_keys=True,indent=2)
    print(json.dumps({k:v for k,v in summary.items() if k not in ('input_refs','sources','source_sha256','current_input_refs')},ensure_ascii=False))


if __name__=='__main__':main()
