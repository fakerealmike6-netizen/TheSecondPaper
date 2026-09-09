"""Portable offline validation of an extracted Stage1B-R1 MIN or public tree.

The supplied tree is never a working directory: code/tests/fixtures are copied
into a fresh output mirror so even test temporary files cannot modify frozen
inputs. Optional manifests choose only declared replay kinds and in-tree paths.
No network, account login, ledger reconciliation or live provider is invoked.
"""
from __future__ import annotations
import argparse,hashlib,json,os,shutil,subprocess,sys
from pathlib import Path,PureWindowsPath
from datetime import datetime,timezone

SECRET_PARTS=('API_KEY','TOKEN','CREDENTIAL','PASSWORD','SECRET','AUTHORIZATION','ACCESS_KEY','PRIVATE_KEY')

def gap_projection_multiset(value, fields):
    """The historical gap comparison retains multiplicity, including shared views."""
    from collections import Counter
    from stage1d_gap_sequence import iter_gaps
    return Counter(json.dumps({key:gap[key] for key in fields if key in gap}, sort_keys=True)
                   for gap in iter_gaps(value.get('gaps', [])))

def clean_environment(env):
    return {k:v for k,v in env.items() if not any(part in k.upper() for part in SECRET_PARTS)}

def input_path(tree,value,*,exists=True):
    """Accept a relative portable input path; reject traversal/drive/links."""
    if not isinstance(value,str) or not value:raise ValueError('Input path must be a nonempty relative string')
    path=Path(value);win=PureWindowsPath(value)
    if path.is_absolute() or win.is_absolute() or win.drive or '..' in path.parts or '..' in win.parts:
        raise ValueError('Input path escapes the supplied review tree')
    result=(tree/path).resolve()
    if not result.is_relative_to(tree):raise ValueError('Resolved input path escapes the supplied review tree')
    current=tree
    for part in path.parts:
        current=current/part
        if current.is_symlink() or (hasattr(current,'is_junction') and current.is_junction()):raise ValueError('Linked input is not accepted')
    if exists and not result.exists():raise FileNotFoundError(value)
    return result

def tree_hashes(tree):
    result={}
    for path in sorted(tree.rglob('*')):
        if path.is_symlink() or (hasattr(path,'is_junction') and path.is_junction()):raise ValueError('Review tree contains a link')
        if path.is_file():result[path.relative_to(tree).as_posix()]=hashlib.sha256(path.read_bytes()).hexdigest()
    return result

def fresh_output(tree,path):
    output=Path(path).resolve()
    if output==tree or output.is_relative_to(tree) or tree.is_relative_to(output):
        raise ValueError('Output must be a fresh directory separate from the frozen input tree')
    output.mkdir(parents=True,exist_ok=False)
    return output

def safe_name(value):
    if not isinstance(value,str) or not value or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in value):
        raise ValueError('Replay name must be a simple portable identifier')
    return value

def read(path):return json.loads(Path(path).read_text(encoding='utf-8'))
def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')

OFFLINE_SITE = r'''import os,socket,sys,subprocess,shlex,inspect,hashlib
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import url2pathname
secret_parts=('API_KEY','TOKEN','CREDENTIAL','PASSWORD','SECRET','AUTHORIZATION','ACCESS_KEY','PRIVATE_KEY')
for key in list(os.environ):
    if any(x in key.upper() for x in secret_parts):os.environ.pop(key,None)
def deny(*a,**k):raise RuntimeError('OFFLINE_VALIDATION_NETWORK_FORBIDDEN')
socket.create_connection=deny;socket.socket.connect=deny;socket.socket.connect_ex=deny;socket.getaddrinfo=deny;socket.gethostbyname=deny;socket.gethostbyname_ex=deny
output=Path(os.environ['REVIEW_VALIDATION_OUTPUT']).resolve()
tree=Path(os.environ['REVIEW_VALIDATION_TREE']).resolve()
runtime=Path(__file__).resolve()
runtime_hash=hashlib.sha256(runtime.read_bytes()).hexdigest()
inherited={key:os.environ[key] for key in ('REVIEW_VALIDATION_OUTPUT','REVIEW_VALIDATION_TREE','PYTHONPATH','TMP','TEMP','TMPDIR')}
def child_environment(value):
    # Sanitize immediately before every dispatch, including credentials injected
    # after startup by a synthetic test. Never mutate the caller's environment.
    env={k:v for k,v in (os.environ if value is None else value).items() if not any(x in k.upper() for x in secret_parts)}
    for key,val in inherited.items():env.setdefault(key,val)
    child_output=Path(env['REVIEW_VALIDATION_OUTPUT']).resolve()
    if not child_output.is_relative_to(output):raise PermissionError('Child output widens offline write boundary')
    bootstrap=Path(env['PYTHONPATH']).resolve()/'sitecustomize.py'
    if not bootstrap.is_relative_to(output) or hashlib.sha256(bootstrap.read_bytes()).hexdigest()!=runtime_hash:
        raise PermissionError('Child must inherit the verified offline bootstrap')
    for key in ('TMP','TEMP','TMPDIR'):
        if not Path(env[key]).resolve().is_relative_to(child_output):raise PermissionError('Child temporary directory escapes its output')
    env.update(PYTHONDONTWRITEBYTECODE='1',PYTHONUTF8='1',PYTHONIOENCODING='utf-8')
    return env
def python_dispatch(executable,command,env):
    if isinstance(command,str):raise PermissionError('Offline Python dispatch requires an argument list')
    if not command:raise PermissionError('Empty Python command')
    executable=executable or command[0]
    if Path(executable).resolve()!=Path(sys.executable).resolve():raise PermissionError('Only inherited offline Python subprocesses allowed')
    # These startup flags suppress PYTHONPATH/sitecustomize. Their combined
    # forms must be rejected too; arguments after -c/-m/script are ordinary data.
    options=iter(command[1:])
    for arg in options:
        arg=os.fsdecode(arg)
        if arg in ('-c','-m','--','-') or not arg.startswith('-'):break
        if arg in ('-W','-X'):
            next(options,None);continue
        if arg.startswith(('-W','-X')):continue
        if any(flag in arg[1:] for flag in ('S','I','E')):raise PermissionError('Python startup cannot suppress the offline guard')
    checked=child_environment(env)
    if env is not None and checked!=dict(env):raise PermissionError('Unsanitized Python environment at audit boundary')

# Popen and direct posix_spawn use the same executable/argv/environment policy.
# Retain Python's platform-selected implementation instead of disabling spawn.
original_popen_init=subprocess.Popen.__init__
popen_signature=inspect.signature(original_popen_init)
def guarded_popen_init(self,*args,**kwargs):
    bound=popen_signature.bind(self,*args,**kwargs)
    if bound.arguments.get('shell',False):raise PermissionError('External shell dispatch is not permitted by offline validation')
    bound.arguments['env']=child_environment(bound.arguments.get('env'))
    python_dispatch(bound.arguments.get('executable'),bound.arguments['args'],bound.arguments['env'])
    return original_popen_init(*bound.args,**bound.kwargs)
guarded_popen_init.__signature__=popen_signature
subprocess.Popen.__init__=guarded_popen_init
def guarded_spawn(original):
    def dispatch(path,argv,env,**kwargs):
        env=child_environment(env);python_dispatch(path,argv,env)
        return original(path,argv,env,**kwargs)
    return dispatch
for spawn_name in ('posix_spawn','posix_spawnp'):
    if hasattr(os,spawn_name):setattr(os,spawn_name,guarded_spawn(getattr(os,spawn_name)))
def write_target(value):
    if isinstance(value,int):return
    value=os.fsdecode(value)
    if value.startswith('file:'):
        uri=urlsplit(value)
        if uri.netloc not in ('','localhost'):raise PermissionError('Remote file URI is not a validation output')
        value=url2pathname(uri.path)
    path=Path(value).resolve()
    if not path.is_relative_to(output):raise PermissionError('Offline validator forbids writes outside the new output directory')
def audit(name,args):
    if name=='open':
        path,mode,flags=args
        writing=(isinstance(mode,str) and any(x in mode for x in 'wax+')) or (isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))
        if writing:write_target(path)
    elif name=='sqlite3.connect' and args and args[0]!=':memory:':write_target(args[0])
    elif name in ('os.remove','os.rmdir','os.mkdir','os.chmod','os.utime') and args:write_target(args[0])
    elif name in ('os.rename','os.replace'):
        write_target(args[0]);write_target(args[1])
    elif name in ('socket.connect','socket.getaddrinfo','socket.gethostbyname','socket.gethostbyaddr'):deny()
    elif name=='subprocess.Popen':
        executable,command,cwd,env=args
        # Windows emits its already-quoted command line at this audit event;
        # guarded_popen_init checked the original argument list beforehand.
        if isinstance(command,str):
            if Path(executable or shlex.split(command,posix=False)[0].strip('"')).resolve()!=Path(sys.executable).resolve():raise PermissionError('Only inherited offline Python subprocesses allowed')
            if child_environment(env)!=dict(env):raise PermissionError('Unsanitized Python environment at audit boundary')
        else:python_dispatch(executable,command,env)
    elif name in ('os.posix_spawn','os.posix_spawnp'):python_dispatch(args[0],args[1],args[2])
    elif name=='os.system':raise PermissionError('External shell dispatch is not permitted by offline validation')
sys.addaudithook(audit)
'''

# Load the exact bundled guard even if a host startup hook masks sitecustomize.
# Register its canonical module name so nested probes can verify inheritance.
GUARDED_LAUNCH = """import importlib.util,runpy,sys
from pathlib import Path
guard,script,*arguments=sys.argv[1:]
prior=sys.modules.get('sitecustomize')
if not prior or Path(getattr(prior,'__file__','')).resolve()!=Path(guard).resolve():
    spec=importlib.util.spec_from_file_location('sitecustomize',guard)
    module=importlib.util.module_from_spec(spec);sys.modules['sitecustomize']=module
    spec.loader.exec_module(module)
sys.path.insert(0,str(Path(script).parent))
sys.argv=[script,*arguments];runpy.run_path(script,run_name='__main__')
"""

DEFAULTS={
 'reference':{'data':'private/reference_replay','identity':'baseline_derived/reference_identity_stage1b_slice.csv.gz','expected':'baseline_derived/reference_summary_by_scenario.json'},
 'policy':'configs/STAGE1B_POLICY.json',
 'events':'private/cache_replay/value_events_normalized.csv.gz',
 'members':'private/reference_replay/reference_query_members.csv',
 'cache_registry':'private/cache_replay/cache_address_registry.csv.gz',
 'dune_registry':'private/dune_replay_inputs/address_registry.csv.gz',
 'jobs':'private/dune_live_jobs','weth_root':'private/weth_replay','weth_evidence':'private/weth_replay/evidence_samples',
 'expected_root':'derived/bugfix_only_same_input'}

def semantic_collection(value):
    fields=('event_id','tx_hash','sender','recipient','asset','amount_raw','block','tx_index','timestamp','kind','log_index','trace_address','execution_index','success')
    events=sorted([{k:e.get(k) for k in fields} for e in value['candidate_events']],key=lambda e:e['event_id'])
    states=lambda rows:sorted((r['reason'],r['state']['address'],r['state']['arrival']['event_id'],r['state']['depth'],r['state']['local_end'],r.get('identity',{}).get('actor') or '') for r in rows)
    coverage_keys=('address','asset','start_block','end_block','start_time','end_time','complete','execution_id','logical_job_id')
    coverage=sorted(json.dumps({k:c.get(k) for k in coverage_keys},sort_keys=True) for c in value['coverage'])
    return {'status':value['status'],'events':events,'stops':states(value['stops']),
            'unfinished_frontier':states(value['unresolved_frontier']),'coverage':coverage,
            'conflict_event_ids':sorted(eid for c in value.get('fact_conflicts',[]) for eid in c.get('event_ids',[]))}

def semantic_intervals(value):
    return {'scope':value.get('scope'),'status':value.get('status'),
            'results':{group:{'joint':{k:d['joint'].get(k) for k in ('status','lower_raw','upper_raw')},
                              'entries':{entry:{k:e.get(k) for k in ('status','lower_raw','upper_raw')} for entry,e in d['entry_intervals'].items()}}
                       for group,d in value.get('results',{}).items()}}

def evidence_enrichment(old,new):
    """Record harmless provenance changes; reject changed explicit gas facts."""
    before={e['event_id']:e for e in old['candidate_events']}
    changes=[];compatible=True
    for event in new['candidate_events']:
        prior=before.get(event['event_id'],{})
        for field in ('provenance','chain_id','block_hash','gas_raw','gas_used','gas_price'):
            a,b=prior.get(field),event.get(field)
            if a==b or (field in ('chain_id','block_hash') and isinstance(a,str) and isinstance(b,str) and a.lower()==b.lower()):continue
            allowed=field=='provenance' or a in (None,'')
            compatible &= allowed
            changes.append({'event_id':event['event_id'],'field':field,'old':a,'new':b,
                            'classification':'PROVENANCE_CHANGE' if field=='provenance' else 'COMPATIBLE_MISSING_FIELD_ENRICHMENT' if allowed else 'EXPLICIT_PHYSICAL_FACT_CHANGED'})
    return {'compatible':compatible,'changes':changes}

def label_slice_identity(tree,spec,paths,label):
    """Bind slice identity to a byte-verified original apply manifest.

    This validates the archived provenance chain, not membership against a
    withheld full registry. Collector outputs and label gaps still compare.
    """
    mapping=spec.get('label_slice_mapping')
    is_slice=label.get('type')=='PRIVATE_PORTABLE_LABEL_REFERENCE_SLICE'
    if not mapping:
        if is_slice:raise ValueError('A portable label slice needs an explicit source manifest mapping')
        return None
    if not is_slice or not isinstance(mapping,dict) or set(mapping)!={'source_apply_manifest'}:
        raise ValueError('Invalid explicit label slice mapping')
    source=input_path(tree,mapping['source_apply_manifest'])
    source_bytes=source.read_bytes();source_sha=hashlib.sha256(source_bytes).hexdigest()
    original=read(source)
    if source_sha!=label.get('source_apply_manifest_sha256'):
        raise ValueError('Portable slice source apply manifest bytes do not match its bound hash')
    full_sha=label.get('source_registry_sha256')
    if not isinstance(full_sha,str) or len(full_sha)!=64 or original.get('registry_sha256')!=full_sha:
        raise ValueError('Portable slice full-source registry identity differs from original apply manifest')
    if input_path(paths['label_manifest'].parent,label['registry_path'])!=paths['registry']:
        raise ValueError('Portable slice registry path mapping differs')
    records=[]
    for record in label.get('byte_identical_file_mappings',[]):
        mapped=input_path(paths['label_manifest'].parent,record['portable_path'])
        if mapped==source:records.append(record)
    if len(records)!=1:raise ValueError('Exactly one original apply manifest byte mapping is required')
    record=records[0]
    if (record.get('byte_identical') is not True or record.get('bytes')!=len(source_bytes)
            or record.get('portable_sha256')!=source_sha or record.get('source_sha256')!=source_sha
            or record.get('source')!=label.get('source_apply_manifest_path')):
        raise ValueError('Original apply manifest byte mapping is inconsistent')
    return {'mapping_type':'VERIFIED_ORIGINAL_APPLY_MANIFEST_TO_PORTABLE_LABEL_SLICE',
            'source_apply_manifest':mapping['source_apply_manifest'],'source_apply_manifest_sha256':source_sha,
            'source_registry_sha256':full_sha,'slice_registry_sha256':label['registry_sha256'],
            'slice_manifest_sha256':hashlib.sha256(paths['label_manifest'].read_bytes()).hexdigest(),
            'full_registry_membership_independently_rechecked':False,
            'requires_equal_collector_facts_stops_coverage_label_gaps':True}

def batch_status_identity(prior,current,mapping):
    """Compare separate source/slice identities; never blanket-ignore hashes."""
    if mapping:
        return (prior.get('registry_sha256')==mapping['source_registry_sha256']
                and prior.get('label_manifest_sha256')==mapping['source_apply_manifest_sha256']
                and current.get('registry_sha256')==mapping['slice_registry_sha256']
                and current.get('label_manifest_sha256')==mapping['slice_manifest_sha256'])
    return all(prior.get(k)==current.get(k) for k in ('registry_sha256','label_manifest_sha256'))

def batch_inputs(tree,spec):
    """Resolve only explicit portable mappings; historical paths are metadata."""
    paths={k:input_path(tree,spec[k]) for k in ('policy','events','members','registry','label_manifest','jobs','work')}
    label=read(paths['label_manifest'])
    if not (label.get('label_snapshot_path') or label.get('registry_path')):
        raise ValueError('Applied label manifest must identify its original registry')
    registry_hash=hashlib.sha256(paths['registry'].read_bytes()).hexdigest()
    if registry_hash!=label.get('registry_sha256'):raise ValueError('Portable registry differs from original label manifest hash')
    label_slice_identity(tree,spec,paths,label)
    if not isinstance(spec['batch_specs'],list):raise ValueError('batch_specs must be an explicit list, including [] for same raw')
    if 'required_no_new_candidate_jobs' in spec and type(spec['required_no_new_candidate_jobs']) is not bool:
        raise ValueError('required_no_new_candidate_jobs must be boolean')
    if spec.get('required_no_new_candidate_jobs') and spec['batch_specs']:
        raise ValueError('Declared same-raw view forbids new candidate jobs')
    batches=[]
    for item in spec['batch_specs']:
        if set(item)!={'folder','freeze'}:raise ValueError('Each required batch mapping needs exactly folder and freeze')
        folder=input_path(tree,item['folder']);freeze=input_path(tree,item['freeze'])
        if not folder.is_dir() or not freeze.is_file():raise ValueError('Required batch folder/freeze input has wrong type')
        if not folder.is_relative_to(paths['work']) or not freeze.is_relative_to(paths['work']):
            raise ValueError('Required batch folder/freeze must stay within portable work root')
        input_path(tree,(folder/'job.json').relative_to(tree).as_posix())
        batches.append((folder,freeze))
    if len({str(folder) for folder,_ in batches})!=len(batches):raise ValueError('Duplicate required batch mapping')
    if not paths['jobs'].is_dir() or not paths['work'].is_dir():raise ValueError('jobs/work must be directories')
    return paths,label,batches

def batch_worker(tree,spec,output):
    """Offline adapter: preserve original bytes, change only declared resolvers."""
    import dune_batch_r1 as batch
    tree=Path(tree).resolve();output=Path(output).resolve()
    paths,label,batches=batch_inputs(tree,spec)
    identity_mapping=label_slice_identity(tree,spec,paths,label)
    original_provider=batch.BatchSavedDuneProvider
    original_resolver=batch.registry_from_manifest
    loaded=[]
    class PortableProvider(original_provider):
        def __init__(self,jobs_root,work,batch_jobs_root=None,**kwargs):
            if batch_jobs_root is not None or kwargs:raise ValueError('Undeclared automatic batch discovery is forbidden')
            super().__init__(jobs_root,work,batch_specs=batches)
            if self.rejected_jobs:raise ValueError('Required saved input rejected: '+json.dumps(self.rejected_jobs,sort_keys=True))
            loaded.append([{'logical_job_id':j['logical_job_id'],'execution_id':j['execution_id'],
                            'rows':j['exported_rows'],'job_file_sha256':j['job_file_sha256'],
                            'scope_freeze_sha256':j.get('scope_freeze_sha256')} for j in self.jobs])
    def portable_registry(path,root):
        if Path(path).resolve()!=paths['label_manifest'] or Path(root).resolve()!=tree:
            raise ValueError('Unexpected label resolver request')
        return paths['registry'],label
    batch.BatchSavedDuneProvider=PortableProvider;batch.registry_from_manifest=portable_registry
    try:
        summaries=batch.replay(paths['policy'],paths['events'],paths['members'],paths['label_manifest'],tree,
                               paths['jobs'],None,paths['work'],output)
    finally:
        batch.BatchSavedDuneProvider=original_provider;batch.registry_from_manifest=original_resolver
    original_registry=(label['label_snapshot_path'].rstrip('/')+'/address_registry.csv.gz'
                       if label.get('label_snapshot_path') else label['registry_path'])
    mapping={'schema_version':'stage1b-r1-portable-batch-mapping-1','status':'PASS',
             'original_files_rewritten':False,'replay_network_requests':0,'live_provider_verified_by_this_run':False,
             'label_manifest':{'portable_path':spec['label_manifest'],'sha256':hashlib.sha256(paths['label_manifest'].read_bytes()).hexdigest()},
             'registry':{'original_declared_path':original_registry,'portable_path':spec['registry'],'sha256':label['registry_sha256']},
             'required_batch_count':len(batches),'loaded_jobs_by_pilot':loaded,
             'required_no_new_candidate_jobs':spec.get('required_no_new_candidate_jobs',False),
             'label_slice_identity':identity_mapping,
             'batch_mappings':[{'folder':item['folder'],'freeze':item['freeze'],
                                'job_sha256':hashlib.sha256((folder/'job.json').read_bytes()).hexdigest(),
                                'freeze_sha256':hashlib.sha256(freeze.read_bytes()).hexdigest(),
                                'original_declared_freeze_path':read(folder/'job.json').get('scope_freeze_path')}
                               for item,(folder,freeze) in zip(spec['batch_specs'],batches)],
             'summary':summaries}
    write(output/'portable_mapping_receipt.json',mapping)
    return mapping

class Validator:
    def __init__(self,tree,output,kind,manifest=None):
        self.tree=Path(tree).resolve();self.before=tree_hashes(self.tree)
        self.out=fresh_output(self.tree,output);self.kind=kind;self.commands=[]
        self.config=json.loads(json.dumps(DEFAULTS));self.config.update(manifest or {})
        if set(self.config)-set(DEFAULTS)-{'additional_replays','additional_fixed_graphs','weth_optional'}:raise ValueError('Unsupported validation manifest key')
        names=['old_cache','old_dune']+[safe_name(s['name']) for key in ('additional_replays','additional_fixed_graphs') for s in self.config.get(key,[])]
        if len(names)!=len(set(names)):raise ValueError('Validation replay output names must be distinct')
        self.mirror=self.out/'execution_tree';self.mirror.mkdir()
        for folder in ('src','tests','fixtures','configs'):
            source=input_path(self.tree,folder,exists=False)
            if source.exists():shutil.copytree(source,self.mirror/folder)
        self.bootstrap=self.out/'offline_runtime';self.bootstrap.mkdir()
        (self.bootstrap/'sitecustomize.py').write_text(OFFLINE_SITE,encoding='utf-8')
        temp=self.out/'temp';temp.mkdir()
        self.env=clean_environment(os.environ)
        self.env.update(PYTHONPATH=str(self.bootstrap),PYTHONDONTWRITEBYTECODE='1',PYTHONUTF8='1',PYTHONIOENCODING='utf-8',
                        REVIEW_VALIDATION_OUTPUT=str(self.out),REVIEW_VALIDATION_TREE=str(self.tree),
                        TMP=str(temp),TEMP=str(temp),TMPDIR=str(temp))

    def path(self,key):return input_path(self.tree,self.config[key])
    def check(self,name,passed,/,**detail):
        from validation_result_r4 import check_row
        row=check_row(name,passed,**detail)
        self.commands.append(row)
        return row['passed']
    def skipped(self,name,reason,required=False):
        from validation_result_r4 import skip_row
        self.commands.append(skip_row(name,reason,required))
    def validation_failures(self,required_names=()):
        from validation_result_r4 import failures
        names={row.get('name') for row in self.commands if isinstance(row,dict)}
        for name in required_names:
            if name not in names:self.check('required_check_not_run:'+name,False,missing_check=name)
        return failures(self.commands)
    def command(self,name,script,args):
        source=input_path(self.mirror,script)
        try:
            result=subprocess.run([sys.executable,'-B','-c',GUARDED_LAUNCH,str(self.bootstrap/'sitecustomize.py'),str(source),*map(str,args)],cwd=self.mirror,
                                  env=clean_environment(self.env),capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=600)
            stdout,stderr,code=result.stdout or '',result.stderr or '',result.returncode
        except subprocess.TimeoutExpired as exc:
            stdout=str(exc.stdout or '');stderr=str(exc.stderr or '')+'\nVALIDATION_COMMAND_TIMEOUT';code=None
        i=len(self.commands)+1
        (self.out/f'{i:02d}_{name}_stdout.txt').write_text(stdout,encoding='utf-8')
        (self.out/f'{i:02d}_{name}_stderr.txt').write_text(stderr,encoding='utf-8')
        return self.check(name,code==0,script=script,args=[str(a) for a in args],exit_code=code,
                          stdout=f'{i:02d}_{name}_stdout.txt',stderr=f'{i:02d}_{name}_stderr.txt')
    def bounded(self,name,fn):
        try:fn()
        except Exception as exc:self.check(name,False,error_type=type(exc).__name__,reason=str(exc))

    def public_checks(self):
        if self.command('unit_tests','src/run_tests.py',['--output',self.out/'tests.json']):
            tests=read(self.out/'tests.json')
            self.check('unit_test_receipt',tests.get('success') and tests.get('tests_run',0)>0 and tests.get('failed')==0 and tests.get('errors')==0 and tests.get('skipped')==0 and not tests.get('network_attempts'),tests_run=tests.get('tests_run'),passed_count=tests.get('passed'))
        if self.command('controlled_lp','src/lp_run.py',['--fixtures',input_path(self.mirror,'fixtures/controlled'),'--output',self.out/'controlled']):
            receipt=read(self.out/'controlled/lp_verification_results.json')
            self.check('controlled_oracle_receipt',receipt['scenario_count']==12 and receipt['objective_comparisons']==32 and receipt['all_passed'],
                       scenarios=receipt['scenario_count'],objective_comparisons=receipt['objective_comparisons'],real_provider_claimed=False)

    def reference(self):
        cfg=self.config['reference'];paths={k:input_path(self.tree,cfg[k]) for k in ('data','identity','expected')}
        if self.command('reference_replay','src/reference_replay.py',['--data-dir',paths['data'],'--identity-file',paths['identity'],'--expected',paths['expected'],'--output',self.out/'reference.json']):
            receipt=read(self.out/'reference.json')
            self.check('reference_receipt',receipt.get('passed') and receipt.get('relation_rows')==10430,relation_rows=receipt.get('relation_rows'),network_calls=receipt.get('network_calls'))

    def replay(self,name,kind,config,expected):
        safe_name(name)
        if kind not in ('cache','dune','batch'):raise ValueError('Only cache, dune or batch replay is permitted')
        folder=self.out/'replays'/name
        if kind=='batch':
            paths,_,_=batch_inputs(self.tree,config)
            descriptor=self.out/'batch_specs'/(name+'.json');write(descriptor,config)
            if not self.command(name,'src/validate_review_bundle_r1.py',
                                ['--batch-worker-spec',descriptor,'--tree',self.tree,'--output',folder]):return
            mapping=read(folder/'portable_mapping_receipt.json')
            self.check(name+'_portable_mapping',mapping['status']=='PASS' and not mapping['original_files_rewritten'],
                       required_batch_count=mapping['required_batch_count'],mapping_receipt=str((folder/'portable_mapping_receipt.json').relative_to(self.out)),
                       replay_network_requests=mapping['replay_network_requests'],live_provider_verified_by_this_run=False)
        else:
            keys=('policy','events','members','registry')
            paths={k:input_path(self.tree,config[k]) for k in keys}
            args=[arg for k in keys for arg in ('--'+k,paths[k])]
            if kind=='dune':args += ['--jobs',input_path(self.tree,config['jobs']),'--work',input_path(self.tree,config.get('work','.'))]
            args+=['--output',folder]
            if not self.command(name,'src/cache_probe.py' if kind=='cache' else 'src/dune_observed_replay.py',args):return
        expected=input_path(self.tree,expected)
        pilots=read(paths['policy'])['query_pilots']
        for pilot in pilots:
            pname=safe_name(pilot['name']);new=folder/pname;old=input_path(expected,pname)
            collection='cached_collection.json' if kind=='cache' else 'collection.json'
            old_collection,new_collection=read(input_path(old,collection)),read(new/collection)
            enrichment=evidence_enrichment(old_collection,new_collection)
            same=semantic_collection(new_collection)==semantic_collection(old_collection) and enrichment['compatible']
            self.check(name+'_'+pname+'_collection',same,
                       comparison='event facts, stop identities, coverage identity/completion, unresolved frontier; provenance/path and optional evidence enrichment excluded',
                       evidence_changes=enrichment['changes'],optional_physical_enrichment_only=enrichment['compatible'],
                       volatile_evidence_fields_excluded=['raw_path','wall_time'])
            if kind=='batch':
                gap_fields=('reason','address','asset','start_block','end_block','start_time','end_time','lookup_status','identity_class','query_id')
                # Preserve the prior multiset comparison, including duplicates,
                # while accepting ordered shared gap encodings without a flat copy.
                gaps=lambda value:gap_projection_multiset(value,gap_fields)
                self.check(name+'_'+pname+'_label_and_acquisition_gaps',gaps(old_collection)==gaps(new_collection))
                status_keys=('status','collector_status','live_query_interval_count','live_logical_job_count','live_exported_rows',
                             'live_interval_rows','live_normalized_distinct_events','live_queried_address_count','unresolved_frontier_count',
                             'unqueried_label_addresses','failed_label_addresses','conflicted_label_addresses')
                prior_status=read(input_path(old,'status.json'));next_status=read(new/'status.json')
                self.check(name+'_'+pname+'_batch_status',{k:prior_status.get(k) for k in status_keys}=={k:next_status.get(k) for k in status_keys})
                self.check(name+'_'+pname+'_batch_source_and_slice_identity',
                           batch_status_identity(prior_status,next_status,mapping.get('label_slice_identity')),
                           source_and_slice_mapping=mapping.get('label_slice_identity'),
                           expected_identity={k:prior_status.get(k) for k in ('registry_sha256','label_manifest_sha256')},
                           replay_identity={k:next_status.get(k) for k in ('registry_sha256','label_manifest_sha256')})
            if self.command(name+'_'+pname+'_lp','src/lp_run.py',['--graph',new/'fixed_graph.json','--output',new/'lp']):
                old_lp=input_path(self.tree,config['expected_lp'])/pname if config.get('expected_lp') else old
                previous=input_path(old_lp,'lp/lp_fixed_graph_result.json')
                self.check(name+'_'+pname+'_intervals',semantic_intervals(read(previous))==semantic_intervals(read(new/'lp/lp_fixed_graph_result.json')))

    def private_checks(self):
        self.bounded('reference_inputs',self.reference)
        base={k:self.config[k] for k in ('policy','events','members')}
        self.bounded('cache_inputs',lambda:self.replay('old_cache','cache',base|{'registry':self.config['cache_registry']},self.config['expected_root']+'/cache_probe'))
        self.bounded('dune_inputs',lambda:self.replay('old_dune','dune',base|{'registry':self.config['dune_registry'],'jobs':self.config['jobs']},self.config['expected_root']+'/dune_live_replay'))
        def independent():
            input_path(self.tree,self.config['expected_root'])
            if self.command('independent_fixed_graphs','src/verify_fixed_graph_independent.py',
                            ['--root',self.tree,'--derived-subdir',self.config['expected_root'],'--output',self.out/'independent_fixed_graphs.json']):
                data=read(self.out/'independent_fixed_graphs.json')
                self.check('independent_fixed_graph_receipt',data.get('passed') and not data.get('failures'),
                           intervals_compared=data.get('intervals_compared'),primal_witnesses_checked=data.get('primal_witnesses_checked'))
        self.bounded('independent_graph_inputs',independent)
        self.bounded('weth_inputs',self.weth)
        for spec in self.config.get('additional_replays',[]):
            self.bounded('additional_replay_'+safe_name(spec['name']),lambda s=spec:self.replay(s['name'],s['kind'],s,s['expected']))
        for spec in self.config.get('additional_fixed_graphs',[]):
            def fixed(s=spec):
                name=safe_name(s['name']);graph=input_path(self.tree,s['graph']);expected=input_path(self.tree,s['expected_result'])
                dest=self.out/'additional_fixed_graphs'/name
                if self.command(name,'src/lp_run.py',['--graph',graph,'--output',dest]):self.check(name+'_intervals',semantic_intervals(read(expected))==semantic_intervals(read(dest/'lp_fixed_graph_result.json')))
            self.bounded('fixed_graph_'+safe_name(spec['name']),fixed)

    def weth(self):
        args=['--project-root',self.path('weth_root'),'--evidence-dir',self.path('weth_evidence'),
              '--policy',self.path('policy'),'--output',self.out/'weth.json']
        optional=self.config.get('weth_optional',{})
        allowed={'call_trace','historical_code','source_attestation','evidence_bundle','acquisition_catalogue','expected'}
        if set(optional)-allowed:raise ValueError('Unknown WETH validation option')
        for k,v in optional.items():
            if k!='expected':args+=['--'+k.replace('_','-'),input_path(self.tree,v)]
        if self.command('weth_replay','src/weth_component.py',args):
            data=read(self.out/'weth.json')
            self.check('weth_receipt',all(x.get('payload_identical') for x in data.get('source_manifest',[])) and bool(data.get('source_manifest')),
                       component_status=data['status'],real_component_certified=data.get('real_component_certified',False),
                       replay_pass_does_not_mean_component_certified=True)
            if optional.get('expected'):
                old=read(input_path(self.tree,optional['expected']))
                keys=('status','checks','gaps','real_component_certified','synthetic_component_verified')
                self.check('weth_expected_semantics',{k:old.get(k) for k in keys}=={k:data.get(k) for k in keys})

    def run(self):
        self.bounded('public_checks',self.public_checks)
        if self.kind=='min':self.private_checks()
        else:
            for name in ('reference_replay','old_cache_four_graph_replay','old_dune_replay','real_weth_replay','additional_real_graphs'):
                self.skipped(name,'PUBLIC_BUNDLE_INTENTIONALLY_EXCLUDES_PRIVATE_DATA; no real replay is claimed')
        after=tree_hashes(self.tree)
        self.check('frozen_tree_unchanged',self.before==after,files=len(self.before),
                   changed=[p for p in set(self.before)|set(after) if self.before.get(p)!=after.get(p)])
        failed=self.validation_failures(('unit_tests','unit_test_receipt','controlled_lp','controlled_oracle_receipt','frozen_tree_unchanged'))
        receipt={'schema_version':'stage1b-r1-portable-validation-1','created_at_utc':datetime.now(timezone.utc).isoformat(),
                 'status':'PASS' if not failed else 'FAIL','tree_kind':self.kind,'network_disabled':True,
                 'socket_dns_and_python_children_blocked':True,'credentials_removed_from_child_environment':True,
                 'input_tree_unchanged':self.before==after,'private_real_data_replay_requested':self.kind=='min',
                 'external_acceptance_status':'PENDING_REVIEW','commands':self.commands,
                 'input_files':self.before,'validation_output':str(self.out)}
        write(self.out/'validation_receipt.json',receipt)
        print(json.dumps({k:receipt[k] for k in ('status','tree_kind','input_tree_unchanged','private_real_data_replay_requested')},indent=2))
        return 0 if not failed else 1

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tree',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--kind',choices=('auto','min','public'),default='auto')
    parser.add_argument('--manifest',help='Optional JSON path relative to tree; inputs must stay inside tree')
    parser.add_argument('--batch-worker-spec',type=Path,help=argparse.SUPPRESS)
    args=parser.parse_args();tree=args.tree.resolve()
    if args.batch_worker_spec:
        # This internal command must inherit the parent's offline guard.
        if 'sitecustomize' not in sys.modules or 'REVIEW_VALIDATION_OUTPUT' not in os.environ:
            raise RuntimeError('Batch worker requires inherited offline validation guard')
        descriptor=args.batch_worker_spec.resolve();guard_output=Path(os.environ['REVIEW_VALIDATION_OUTPUT']).resolve()
        if not descriptor.is_relative_to(guard_output) or not args.output.resolve().is_relative_to(guard_output):
            raise ValueError('Batch worker descriptor/output must stay in validation output')
        batch_worker(tree,read(descriptor),args.output)
        return 0
    manifest=read(input_path(tree,args.manifest)) if args.manifest else None
    kind=args.kind if args.kind!='auto' else 'min' if input_path(tree,'private/reference_replay',exists=False).exists() else 'public'
    return Validator(tree,args.output,kind,manifest).run()

if __name__=='__main__':raise SystemExit(main())
