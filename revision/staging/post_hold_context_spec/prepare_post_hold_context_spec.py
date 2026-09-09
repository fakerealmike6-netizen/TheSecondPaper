"""Root-only offline preparation: reviewed raw material -> current point spec/cache inventory.

No network, legacy import, assembler, provider, budget writer, or solver is called.
The explicit --prepare flag creates only a new private/current_context_points run.
"""
from __future__ import annotations
import argparse
from contextlib import closing
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import socket
import sqlite3
import sys
import time
import traceback

DRIVER_SHA='865d56f9fcae38de09c66eb799f15a4ffe35e783af0dfa099790afec9c2a44a6'
NAMES=('txphish_src001','txphish_src002')

def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
def digest(v):return hashlib.sha256(encoded(v)).hexdigest()
def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()
def write_new(path,value):
    with path.open('xb') as f:f.write(encoded(value)+b'\n')
def blocked_network(*a,**kw):raise RuntimeError('Context point preparation is strictly offline')

def load_driver(revision):
    path=revision/'staging/current_context_route_notes/prepare_context_points.py'
    if sha(path)!=DRIVER_SHA:raise ValueError('Reviewed existing prepare_context_points driver changed')
    spec=importlib.util.spec_from_file_location('post_hold_existing_point_driver',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module

def safe_point(work):
    if (work/'private/network_worker.lock').exists():raise ValueError('Existing writer/replay lock; wait for its normal completion')
    for folder in ('private/stage1d_sessions','private/context_sessions_r4'):
        for path in (work/folder).glob('*.json'):
            if json.loads(path.read_bytes()).get('closed') is not True:
                raise ValueError('Unclosed existing measured session: '+path.relative_to(work).as_posix())
    path=work/'private/read_retry_r4.sqlite'
    if not path.exists():return {'cache_database':'ABSENT'}
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        requests=db.execute("SELECT count(*) FROM read_requests WHERE state IN ('IN_FLIGHT','INFLIGHT')").fetchone()[0]
        attempts=db.execute("SELECT count(*) FROM read_attempts WHERE outcome IN ('IN_FLIGHT','INFLIGHT')").fetchone()[0]
        if requests or attempts:raise ValueError('In-flight persistent intent exists; preserve it for root')
        return {'request_rows':db.execute('SELECT count(*) FROM read_requests').fetchone()[0],
                'attempt_rows':db.execute('SELECT count(*) FROM read_attempts').fetchone()[0]}

def current_entry(work,query,driver):
    folder='derived/stage1d/queries/'+query['name']+'/'
    refs={key:{'path':folder+name,'sha256':sha(driver.inside(work,folder+name))}
          for key,name in [('collection','collection.json'),('labels','label_snapshot.json'),('role_replay','ROLE_ADOPTION_AND_REPLAY.json')]}
    c=driver.checked(work,refs['collection']);labels=driver.checked(work,refs['labels']);roles=driver.checked(work,refs['role_replay'])
    bindings=[{'address':s['state']['address'],'arrival_id':s['state']['arrival']['event_id'],
               'depth':s['state']['depth'],'asset':s['state']['asset'],'identity':s['identity']} for s in c['states']]
    expected={'query_id':query['query_id'],'scope_hash':query['scope_hash'],
              'collection_sha256':refs['collection']['sha256'],'label_snapshot_sha256':refs['labels']['sha256'],
              'label_snapshot_canonical_hash':digest(labels),'actual_state_roles_sha256':digest(bindings),
              'rule_source_sha256':sha(work/'src/stage1d_role_adoption.py'),
              'task_boundary_rule_source_sha256':sha(work/'src/stage1d_task_boundaries.py')}
    boundary=work/'private/stage1d_roles/USER_TASK_BOUNDARIES.json'
    expected['task_boundary_source_sha256']=sha(boundary) if boundary.exists() else None
    if any(roles.get(k)!=v for k,v in expected.items()):
        raise ValueError('Current ROLE replay/graph/labels/rule binding differs: '+query['name'])
    if (c.get('metrics',{}).get('actual_state_roles_sha256')!=digest(bindings)
        or c['metrics'].get('label_snapshot_hash')!=digest(labels)):
        raise ValueError('Collection adopted-role metrics differ: '+query['name'])
    return refs

def merge_material(work,reviewed,driver):
    # Raw events are preserved in original order, including duplicate physical
    # records, NULL paths and nonmatching token fields. The existing strict
    # requirement/normalization boundary decides admission later.
    from stage1d_context_online import _merge
    from stage1d_context_coverage import merge_coverage
    material={'events':[],'headers':{},'balances':{},'receipts':{},'coverage':[],
              'source_refs':[],'full_context_claimed':False,'old_model_input_imported':False,
              'raw_event_preservation':'CONCATENATE_UNMODIFIED_ROWS_WITHOUT_ASSET_OR_IDENTITY_PROJECTION'}
    entries=list(reviewed['old_materials'])+[dict(reviewed['shared_bq_ledger'],field='events',query_name='SHARED_FACTS')]
    for item in entries:
        field=item['field']
        if field not in ('events','headers','balances','receipts','coverage'):
            raise ValueError('Unreviewed material field; no old model input allowed')
        ref={k:item[k] for k in ('path','sha256')}
        path=driver.inside(work,ref['path'])
        if path.stat().st_size!=item['bytes']:raise ValueError('Reviewed material size changed')
        value=driver.checked(work,ref)
        material['source_refs'].append(dict(ref,field=field,query_origin=item.get('query_name')))
        if field=='events':
            if not isinstance(value,list) or any(not isinstance(x,dict) for x in value):raise ValueError('Original raw event list required')
            material['events'].extend(value)
        elif field=='coverage':material[field]=merge_coverage(material[field],value)
        else:
            for key,entry in value.items():
                material[field][key]=_merge(material[field][key],entry) if key in material[field] else entry
    return material

def prepare(work,reviewed,run_id,driver,*,check_cache=True):
    from stage1d_closure_scope import active_batch_path
    from stage1d_runtime import Runtime
    if not re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.+-]{0,95}',run_id):raise ValueError('Finite new run ID required')
    before=safe_point(work);Runtime().require_gate(work)
    source=driver.source_inventory(work)
    batch_path=active_batch_path(work);batch_ref={'path':batch_path.relative_to(work).as_posix(),'sha256':sha(batch_path)}
    batch=driver.checked(work,batch_ref);queries={q['name']:q for q in batch['queries']}
    entries={name:current_entry(work,queries[name],driver) for name in NAMES}
    spec={'schema_version':driver.SCHEMA,'final_candidate_freeze':False,'source_sha256':source,
          'active_batch':batch_ref,'queries':entries,'preparation_only':True,
          'reviewed_material_refs_sha256':digest(reviewed),'builder_sha256':sha(Path(__file__)),
          'existing_driver_sha256':DRIVER_SHA,'production_graph_or_label_modified':False}
    # Exclusive new directory prevents accidental repeat/overwrite. No attempt,
    # budget, session or network lock is created, reset, recovered or deleted.
    out=driver.inside(work,'private/current_context_points/'+run_id);out.mkdir(parents=True,exist_ok=False)
    phase='MATERIAL_MERGE';started=time.perf_counter()
    try:
        material=merge_material(work,reviewed,driver)
        write_new(out/'MATERIAL.json',material)
        material_ref={'path':(out/'MATERIAL.json').relative_to(work).as_posix(),'sha256':sha(out/'MATERIAL.json')}
        for entry in entries.values():entry['material']=material_ref
        write_new(out/'SPEC.json',spec)
        del material
        phase='CURRENT_REQUIREMENTS_AND_EXACT_CACHE'
        result=driver.derive(work,spec,check_cache=check_cache)
        phase='FINAL_IDENTITY_RECHECK'
        if safe_point(work)!=before:raise ValueError('Persistent requests/attempts changed during offline preparation')
        if driver.source_inventory(work)!=source:raise ValueError('Source changed during offline preparation')
        driver.checked(work,batch_ref)
        for name in NAMES:
            current=current_entry(work,queries[name],driver)
            if any(current[k]!=entries[name][k] for k in current):raise ValueError('Current graph/labels/ROLE changed during preparation')
        result.update(builder_sha256=sha(Path(__file__)),spec_ref={'path':(out/'SPEC.json').relative_to(work).as_posix(),'sha256':sha(out/'SPEC.json')},
                      production_writes='NEW_PREPARATION_FILES_ONLY',production_graph_or_label_modified=False,
                      input_reviewed_refs=reviewed,safe_point=before,seconds=time.perf_counter()-started)
    except Exception as exc:
        if not (out/'SPEC.json').exists():
            # A failed merge still preserves every original proposed input ref.
            for name,entry in entries.items():
                entry['material_parts']=[{m['field']:{k:m[k] for k in ('path','sha256')}} for m in reviewed['old_materials'] if m['query_name']==name]
                entry['material_parts'].append({'events':{k:reviewed['shared_bq_ledger'][k] for k in ('path','sha256')}})
            spec['material_merge_blocked']=True;write_new(out/'SPEC.json',spec)
        result={'status':'REQUIREMENTS_BLOCKED_INPUTS_PRESERVED','blocked_phase':phase,'exception_class':type(exc).__name__,
                'reason':str(exc),'traceback':traceback.format_exc(limit=8),'spec_ref':{'path':(out/'SPEC.json').relative_to(work).as_posix(),'sha256':sha(out/'SPEC.json')},
                'source_sha256':source,'input_reviewed_refs':reviewed,'new_external_requests':0,'legacy_imports':0,
                'formal_model_built':False,'full_context_claimed':False,'candidate_freeze_created':False,
                'automatic_alternate_input_or_retry':False,'seconds':time.perf_counter()-started}
    write_new(out/'PREPARATION_RESULT.json',result)
    return {'status':result['status'],'path':(out/'PREPARATION_RESULT.json').relative_to(work).as_posix(),
            'sha256':sha(out/'PREPARATION_RESULT.json'),'spec':result.get('spec_ref'),
            'summaries':{k:v['summary'] for k,v in result.get('queries',{}).items()},
            'blocked_phase':result.get('blocked_phase'),'reason':result.get('reason')}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work',required=True);parser.add_argument('--run-id',required=True)
    parser.add_argument('--reviewed-materials',default=str(Path(__file__).with_name('REVIEWED_MATERIAL_REFS.json')))
    parser.add_argument('--reviewed-materials-sha256',required=True)
    parser.add_argument('--prepare',action='store_true');parser.add_argument('--no-current-cache',action='store_true')
    args=parser.parse_args()
    if not args.prepare:
        print(json.dumps({'status':'NOT_EXECUTED_EXPLICIT_PREPARE_REQUIRED','new_external_requests':0}));return
    sys.dont_write_bytecode=True;work=Path(args.work).resolve();sys.path.insert(0,str(work/'src'))
    socket.socket.connect=blocked_network;socket.create_connection=blocked_network
    path=Path(args.reviewed_materials).resolve()
    if not path.is_relative_to(work.parent) or sha(path)!=args.reviewed_materials_sha256:
        raise ValueError('Explicit reviewed material index path/SHA differs')
    reviewed=json.loads(path.read_bytes())
    if reviewed.get('schema_version')!='stage1d-reviewed-post-hold-materials-v1' or len(reviewed.get('old_materials',[]))!=18:
        raise ValueError('The 18 reviewed old material references are required')
    driver=load_driver(work.parent)
    print(json.dumps(prepare(work,reviewed,args.run_id,driver,check_cache=not args.no_current_cache),ensure_ascii=False))

if __name__=='__main__':main()
