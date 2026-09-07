"""Synthetic orchestration tests; stub receipts do not validate scientific code."""
import contextlib,copy,csv,hashlib,io,json,os,shutil,sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from validate_review_bundle_r1 import Validator,input_path,fresh_output,clean_environment,semantic_collection,evidence_enrichment,batch_inputs,batch_worker,tree_hashes,read,write,label_slice_identity,batch_status_identity

BASE=Path(__file__).resolve().parents[1]

class PortableValidationTests(unittest.TestCase):
    def temp(self):
        scratch=BASE/'.testtmp';scratch.mkdir(exist_ok=True)
        return tempfile.TemporaryDirectory(dir=scratch)
    def minimal_tree(self,base):
        tree=base/'tree';(tree/'src').mkdir(parents=True);(tree/'fixtures/controlled').mkdir(parents=True)
        (tree/'src/run_tests.py').write_text("import argparse,json,pathlib\np=argparse.ArgumentParser();p.add_argument('--output');a=p.parse_args();pathlib.Path(a.output).write_text(json.dumps({'success':True,'tests_run':237,'passed':237,'failed':0,'errors':0,'skipped':0,'network_attempts':[]}))\n")
        (tree/'src/lp_run.py').write_text("import argparse,json,pathlib\np=argparse.ArgumentParser();p.add_argument('--fixtures');p.add_argument('--output');a=p.parse_args();o=pathlib.Path(a.output);o.mkdir(parents=True);(o/'lp_verification_results.json').write_text(json.dumps({'scenario_count':12,'objective_comparisons':32,'all_passed':True}))\n")
        return tree

    def test_input_paths_reject_traversal_drives_unc_and_absolute(self):
        with self.temp() as tmp:
            tree=Path(tmp).resolve()
            for value in ('../escape','a/../../escape',r'..\escape',r'D:\outside',r'\\server\share\x','/outside'):
                with self.subTest(value=value),self.assertRaises(ValueError):input_path(tree,value,exists=False)
            self.assertEqual(input_path(tree,'inside/file',exists=False),tree/'inside/file')

    def test_environment_removes_credential_names_without_echo(self):
        env={'DUNE_API_KEY':'fake','GH_TOKEN':'fake','some_password':'fake','AWS_ACCESS_KEY_ID':'fake','PATH':'keep','SAFE':'keep'}
        clean=clean_environment(env)
        self.assertEqual(clean,{'PATH':'keep','SAFE':'keep'})

    def test_output_must_be_new_and_separate(self):
        with self.temp() as tmp:
            root=Path(tmp).resolve();tree=root/'tree';tree.mkdir()
            with self.assertRaises(ValueError):fresh_output(tree,tree/'out')
            existing=root/'existing';existing.mkdir()
            with self.assertRaises(FileExistsError):fresh_output(tree,existing)

    def test_public_dynamic_test_count_and_explicit_real_skips(self):
        with self.temp() as tmp:
            base=Path(tmp);tree=self.minimal_tree(base);before={p.relative_to(tree).as_posix():p.read_bytes() for p in tree.rglob('*') if p.is_file()}
            validator=Validator(tree,base/'out','public')
            with contextlib.redirect_stdout(io.StringIO()):code=validator.run()
            receipt=json.loads((base/'out/validation_receipt.json').read_text(encoding='utf-8'))
            self.assertEqual(code,0,receipt['commands']);self.assertEqual(receipt['status'],'PASS');self.assertFalse(receipt['private_real_data_replay_requested'])
            self.assertEqual(next(c for c in receipt['commands'] if c['name']=='unit_test_receipt')['tests_run'],237)
            self.assertEqual(len([c for c in receipt['commands'] if c['status']=='SKIP']),5)
            after={p.relative_to(tree).as_posix():p.read_bytes() for p in tree.rglob('*') if p.is_file()}
            self.assertEqual(before,after)

    def test_min_missing_private_inputs_fail_not_pass_or_real_claim(self):
        with self.temp() as tmp:
            base=Path(tmp);tree=self.minimal_tree(base);validator=Validator(tree,base/'out','min')
            with contextlib.redirect_stdout(io.StringIO()):code=validator.run()
            receipt=json.loads((base/'out/validation_receipt.json').read_text(encoding='utf-8'))
            self.assertEqual(code,1);self.assertEqual(receipt['status'],'FAIL')
            self.assertTrue(any(c['status']=='FAIL' and c['error_type']=='FileNotFoundError' for c in receipt['commands']))

    def test_subprocess_socket_dns_credentials_and_write_guards_inherited(self):
        with self.temp() as tmp:
            base=Path(tmp);tree=self.minimal_tree(base);protected=tree/'protected.txt';protected.write_text('frozen')
            probe='''import os,socket,subprocess,sys,json,sqlite3
from pathlib import Path
assert 'DUNE_API_KEY' not in os.environ
assert socket.getaddrinfo.__module__=='sitecustomize'
database=Path(os.environ['REVIEW_VALIDATION_OUTPUT'])/'synthetic.sqlite'
with sqlite3.connect(database) as db:db.execute('CREATE TABLE test(x)')
with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True) as db:assert db.execute('SELECT count(*) FROM test').fetchone()[0]==0
for fn in (lambda:socket.getaddrinfo('127.0.0.1',9),lambda:socket.create_connection(('127.0.0.1',9))):
    try:fn();raise AssertionError('network guard missing')
    except RuntimeError as e:assert 'OFFLINE_VALIDATION' in str(e)
try:Path(sys.argv[1]).write_text('changed');raise AssertionError('write guard missing')
except PermissionError:pass
child=subprocess.run([sys.executable,'-c',"import socket,os;assert socket.getaddrinfo.__module__=='sitecustomize';assert 'DUNE_API_KEY' not in os.environ"],capture_output=True)
assert child.returncode==0,child.stderr
'''
            (tree/'src/probe.py').write_text(probe)
            validator=Validator(tree,base/'out','public');validator.env['DUNE_API_KEY']='SYNTHETIC_NOT_REAL'
            passed=validator.command('guard_probe','src/probe.py',[protected])
            self.assertTrue(passed,(base/'out/01_guard_probe_stderr.txt').read_text(encoding='utf-8'));self.assertEqual(protected.read_text(),'frozen')

    def test_manifest_does_not_allow_arbitrary_command(self):
        with self.temp() as tmp:
            base=Path(tmp);tree=self.minimal_tree(base)
            with self.assertRaisesRegex(ValueError,'Unsupported'):Validator(tree,base/'out','public',{'commands':['arbitrary']})

    def test_r2_nested_explicit_environment_strips_all_provider_credentials(self):
        with self.temp() as tmp:
            base=Path(tmp);tree=self.minimal_tree(base)
            probe=r'''import json,os,subprocess,sys
names=('DUNE_API_KEY','ETHERSCAN_API_KEY','METASLEUTH_API_KEY','ALCHEMY_API_KEY','GH_TOKEN','GOOGLE_APPLICATION_CREDENTIALS','AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','TEST_PASSWORD','TEST_AUTHORIZATION','TEST_PRIVATE_KEY')
child_env={'SAFE_SENTINEL':'preserved',**{name:'SYNTHETIC_NOT_REAL' for name in names}}
code="import json,os,socket,subprocess,sys; assert not any(x in k.upper() for k in os.environ for x in ('API_KEY','TOKEN','CREDENTIAL','PASSWORD','SECRET','AUTHORIZATION','ACCESS_KEY','PRIVATE_KEY')); assert os.environ['SAFE_SENTINEL']=='preserved'; assert socket.getaddrinfo.__module__=='sitecustomize'; os.environ['DUNE_API_KEY']='SYNTHETIC_REINSERTED'; r=subprocess.run([sys.executable,'-c',\"import os,socket; assert 'DUNE_API_KEY' not in os.environ; assert socket.getaddrinfo.__module__=='sitecustomize'\"],capture_output=True); assert r.returncode==0,r.stderr"
# Exercise inherited and explicit environments, and the normal fast-path request.
for close_fds in (True,False):
    child=subprocess.run([sys.executable,'-c',code],env=child_env,close_fds=close_fds,capture_output=True)
    assert child.returncode==0,child.stderr
assert all(child_env[name]=='SYNTHETIC_NOT_REAL' for name in names)
'''
            (tree/'src/probe.py').write_text(probe,encoding='utf-8')
            validator=Validator(tree,base/'out','public')
            self.assertTrue(validator.command('nested_env','src/probe.py',[]),(validator.out/'01_nested_env_stderr.txt').read_text(encoding='utf-8'))

    def test_r2_posix_spawn_uses_same_python_and_environment_policy(self):
        with self.temp() as tmp:
            base=Path(tmp);tree=self.minimal_tree(base)
            probe=r'''import os,sys,sitecustomize
from pathlib import Path
env=sitecustomize.child_environment(None)
argv=[sys.executable,'-c','pass']
# The actual audit callback and wrapped dispatch are exercised on every host;
# only POSIX can execute the OS spawn itself.
sitecustomize.audit('os.posix_spawn',(sys.executable,argv,env))
called=[]
wrapped=sitecustomize.guarded_spawn(lambda path,args,env,**kwargs:called.append(env))
wrapped(sys.executable,argv,dict(env,DUNE_API_KEY='SYNTHETIC_NOT_REAL'))
assert len(called)==1 and 'DUNE_API_KEY' not in called[0]
for path,args,values in [('forbidden_shell',argv,env),(sys.executable,[sys.executable,'-S','-c','pass'],env),(sys.executable,argv,dict(env,DUNE_API_KEY='SYNTHETIC_NOT_REAL'))]:
    try:sitecustomize.audit('os.posix_spawn',(path,args,values));raise AssertionError('spawn policy missing')
    except PermissionError:pass
if hasattr(os,'posix_spawn'):
    result=Path(os.environ['REVIEW_VALIDATION_OUTPUT'])/'actual_posix_spawn.txt'
    code="import os,socket; from pathlib import Path; assert 'DUNE_API_KEY' not in os.environ; assert socket.getaddrinfo.__module__=='sitecustomize'; Path("+repr(str(result))+").write_text('guarded')"
    pid=os.posix_spawn(sys.executable,[sys.executable,'-c',code],dict(env,DUNE_API_KEY='SYNTHETIC_NOT_REAL'))
    _,status=os.waitpid(pid,0)
    assert os.waitstatus_to_exitcode(status)==0 and result.read_text()=='guarded'
'''
            (tree/'src/probe.py').write_text(probe,encoding='utf-8')
            validator=Validator(tree,base/'out','public')
            self.assertTrue(validator.command('spawn_policy','src/probe.py',[]),(validator.out/'01_spawn_policy_stderr.txt').read_text(encoding='utf-8'))

    def test_r2_nested_network_write_shell_and_bootstrap_bypass_stay_blocked(self):
        with self.temp() as tmp:
            base=Path(tmp);tree=self.minimal_tree(base);protected=tree/'protected.txt';protected.write_text('frozen')
            probe=r'''import os,subprocess,sys
code=r"""import os,socket,subprocess,sys
from pathlib import Path
for operation in (lambda:socket.getaddrinfo('127.0.0.1',9),lambda:socket.socket().connect(('127.0.0.1',9))):
    try:operation();raise AssertionError('network allowed')
    except RuntimeError:pass
try:Path(sys.argv[1]).write_text('changed');raise AssertionError('write allowed')
except PermissionError:pass
for command,kwargs in [([sys.executable,'-S','-c','pass'],{}),([sys.executable,'-IE','-c','pass'],{}),([sys.executable,'-X','utf8','-S','-c','pass'],{}),([sys.executable,'-c','pass'],{'shell':True}),(['forbidden_shell'],{})]:
    try:subprocess.run(command,**kwargs);raise AssertionError('unsafe command allowed')
    except PermissionError:pass
try:os.system('forbidden_shell');raise AssertionError('shell allowed')
except PermissionError:pass
"""
child=subprocess.run([sys.executable,'-c',code,sys.argv[1]],capture_output=True)
assert child.returncode==0,child.stderr
'''
            (tree/'src/probe.py').write_text(probe,encoding='utf-8')
            validator=Validator(tree,base/'out','public')
            self.assertTrue(validator.command('nested_guards','src/probe.py',[protected]),(validator.out/'01_nested_guards_stderr.txt').read_text(encoding='utf-8'))
            self.assertEqual(protected.read_text(),'frozen')

    def test_r2_explicit_bootstrap_recovers_masked_sitecustomize_module(self):
        with self.temp() as tmp:
            base=Path(tmp);tree=self.minimal_tree(base)
            shutil.copyfile(BASE/'src/validate_review_bundle_r1.py',tree/'src/validate_review_bundle_r1.py')
            (tree/'src/inner.py').write_text("import os,socket; assert 'DUNE_API_KEY' not in os.environ; assert socket.getaddrinfo.__module__=='sitecustomize'",encoding='utf-8')
            probe=r'''import os,sys,types
from pathlib import Path
from validate_review_bundle_r1 import GUARDED_LAUNCH
guard=Path(os.environ['PYTHONPATH'])/'sitecustomize.py'
sys.modules['sitecustomize']=types.ModuleType('sitecustomize')
os.environ['DUNE_API_KEY']='SYNTHETIC_NOT_REAL'
sys.argv=['wrapper',str(guard),str(Path(__file__).with_name('inner.py'))]
exec(GUARDED_LAUNCH,{'__name__':'__main__'})
'''
            (tree/'src/probe.py').write_text(probe,encoding='utf-8')
            validator=Validator(tree,base/'out','public')
            self.assertTrue(validator.command('masked_startup','src/probe.py',[]),(validator.out/'01_masked_startup_stderr.txt').read_text(encoding='utf-8'))

    def test_manifest_output_names_cannot_overwrite_an_earlier_replay(self):
        with self.temp() as tmp:
            base=Path(tmp);tree=self.minimal_tree(base)
            with self.assertRaisesRegex(ValueError,'distinct'):
                Validator(tree,base/'out','min',{'additional_fixed_graphs':[{'name':'old_cache','graph':'unused','expected_result':'unused'}]})

    def test_semantic_projection_ignores_only_documented_evidence_noise(self):
        event=dict(event_id='x',tx_hash='tx',sender='a',recipient='b',asset='ETH',amount_raw=1,block=1,tx_index=0,timestamp=1,kind='top',success=True,provenance='one')
        base=dict(status='PARTIAL',candidate_events=[event],stops=[],unresolved_frontier=[],coverage=[{'address':'a','complete':False,'raw_path':'one'}])
        changed=json.loads(json.dumps(base));changed['candidate_events'][0]['provenance']='two';changed['coverage'][0]['raw_path']='two'
        self.assertEqual(semantic_collection(base),semantic_collection(changed))
        changed['candidate_events'][0]['amount_raw']=2
        self.assertNotEqual(semantic_collection(base),semantic_collection(changed))

    def test_optional_gas_enrichment_does_not_hide_explicit_change(self):
        old={'candidate_events':[{'event_id':'x','gas_raw':None,'provenance':'one'}]}
        enriched={'candidate_events':[{'event_id':'x','gas_raw':6,'provenance':'two'}]}
        check=evidence_enrichment(old,enriched);self.assertTrue(check['compatible']);self.assertEqual(len(check['changes']),2)
        changed={'candidate_events':[{'event_id':'x','gas_raw':7,'provenance':'three'}]}
        self.assertFalse(evidence_enrichment(enriched,changed)['compatible'])
        self.assertFalse(evidence_enrichment(enriched,old)['compatible'])

    def batch_tree(self,base):
        """Actual replay fixture, with deliberately obsolete historical paths."""
        from dune_batch_r1 import prepare
        tree=base/'tree';tree.mkdir();data=read(BASE/'fixtures/fault/dune_batch/synthetic_plan.json')
        sha=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
        def table(name,rows):
            with (tree/name).open('w',encoding='utf-8',newline='') as handle:
                writer=csv.DictWriter(handle,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
        registry=copy.deepcopy(data['registry'])
        for row in registry:row['lookup_status']='LOOKUP_FAILED_RESOURCE_CAP'
        table('registry.csv',registry)
        write(tree/'source_labels.json',{'registry_path':'registry.csv','registry_sha256':sha(tree/'registry.csv')})
        write(tree/'labels.json',{'label_snapshot_path':'obsolete_project/old_stage/labels','registry_sha256':sha(tree/'registry.csv')})
        write(tree/'frontier.json',data['frontier']);policy=copy.deepcopy(data['policy'])
        tx='0x'+'f'*64;eid='eip155:1:tx:'+tx+':top';policy['query_pilots'][0]['seed_event_id']=eid
        write(tree/'policy.json',policy)
        event={'event_id':eid,'tx_hash':tx,'from_address':'0x'+'c'*40,'to_address':registry[0]['address'],
               'asset_key':'native:eip155:1','amount_raw':'10','block_number':'100','block_timestamp':'1970-01-01T00:16:40Z',
               'transaction_index':'0','transaction_status':'SUCCESS','chain_id':'1','event_type':'ETH_TOP_LEVEL',
               'log_index':'','trace_address':'','raw_evidence_sha256':'SYNTHETIC'}
        member={'query_id':'synthetic-query','seed_rule':'S2','seed_event_id':eid,'seed_match_status':'EXACT_EVENT_MATCH',
                'seed_amount_raw':'10','seed_asset':event['asset_key'],'seed_from':event['from_address'],
                'seed_to':event['to_address'],'seed_time':event['block_timestamp'],'seed_tx_hash':tx}
        table('events.csv',[event]);table('members.csv',[member]);(tree/'old_jobs').mkdir()
        frozen=prepare(tree/'frontier.json','synthetic_probe',tree/'source_labels.json',tree/'policy.json',tree,tree/'frozen')
        jobfolder=tree/'jobs/batch';jobfolder.mkdir(parents=True);execution='SYNTHETIC_PORTABLE_BATCH'
        def raw(name,value,**fields):
            path=tree/'raw'/(name+'.json');write(path,value)
            return {'http_status':200,'error_class':None,'raw_path':path.relative_to(tree).as_posix(),
                    'raw_bytes':path.stat().st_size,'sha256':sha(path),**fields}
        submit={'execution_id':execution,'state':'QUERY_STATE_PENDING'}
        status={'execution_id':execution,'state':'QUERY_STATE_COMPLETED','result_metadata':{'total_row_count':2}}
        job={'kind':'candidate','state':'QUERY_STATE_COMPLETED','execution_id':execution,'logical_job_id':'synthetic-required-batch',
             'sql_sha256':frozen['full_sql_sha256'],'scope_freeze_path':'Z:/inaccessible_old_run/freeze_manifest.json',
             'scope_freeze_sha256':sha(tree/'frozen/freeze_manifest.json'),'submit_response':submit,'status_response':status,
             'submit_receipt':raw('submit',submit,operation='execute'),
             'status_receipt':raw('status',status,operation='status',execution_id=execution)}
        write(jobfolder/'job.json',job);shutil.copyfile(tree/'frozen/query.sql',jobfolder/'query.sql')
        rows=[data['row']|{'interval_id':i['interval_id']} for i in frozen['intervals']]
        page={'execution_id':execution,'state':'QUERY_STATE_COMPLETED','result':{'rows':rows,'metadata':{'total_row_count':2,'row_count':2}}}
        receipt=raw('page',page,operation='results',execution_id=execution,parameters={'offset':0,'limit':50})
        write(jobfolder/'page_0.json',page);write(jobfolder/'page_0_receipt.json',receipt)
        spec={'name':'batch_synthetic','kind':'batch','policy':'policy.json','events':'events.csv','members':'members.csv',
              'registry':'registry.csv','label_manifest':'labels.json','jobs':'old_jobs','work':'.',
              'batch_specs':[{'folder':'jobs/batch','freeze':'frozen/freeze_manifest.json'}],'expected':'unused'}
        return tree,spec

    def test_batch_remap_preserves_bytes_and_verifies_required_real_format_pages(self):
        with self.temp() as tmp:
            base=Path(tmp);tree,spec=self.batch_tree(base);before=tree_hashes(tree)
            result=batch_worker(tree,spec,base/'replayed')
            self.assertEqual(result['required_batch_count'],1);self.assertEqual(len(result['loaded_jobs_by_pilot'][0]),2)
            self.assertEqual(result['summary'][0]['live_logical_job_count'],1)
            self.assertEqual(result['summary'][0]['live_exported_rows'],2)
            self.assertEqual(result['summary'][0]['candidate_event_count'],2)
            self.assertEqual(len(result['summary'][0]['failed_label_addresses']),2)
            self.assertFalse(result['live_provider_verified_by_this_run']);self.assertEqual(before,tree_hashes(tree))
            self.assertEqual(result['registry']['original_declared_path'],'obsolete_project/old_stage/labels/address_registry.csv.gz')

    def test_batch_explicit_empty_list_replays_failed_labels_without_new_batch(self):
        with self.temp() as tmp:
            base=Path(tmp);tree,spec=self.batch_tree(base);spec['batch_specs']=[]
            result=batch_worker(tree,spec,base/'replayed')
            self.assertEqual(result['required_batch_count'],0);self.assertEqual(result['summary'][0]['live_exported_rows'],0)
            self.assertEqual(result['summary'][0]['candidate_event_count'],1)
            self.assertTrue(result['summary'][0]['failed_label_addresses'])

    def test_batch_required_missing_inputs_and_corrupt_page_fail(self):
        with self.temp() as tmp:
            base=Path(tmp);tree,spec=self.batch_tree(base)
            for key in ('label_manifest','registry','policy','events','members','jobs','work','batch_specs'):
                absent=copy.deepcopy(spec);absent.pop(key)
                with self.subTest(key=key),self.assertRaises(KeyError):batch_inputs(tree,absent)
            missing=copy.deepcopy(spec);missing['batch_specs'][0]['freeze']='missing.json'
            with self.assertRaises(FileNotFoundError):batch_inputs(tree,missing)
            (tree/'jobs/batch/page_0_receipt.json').unlink()
            with self.assertRaisesRegex(ValueError,'Required saved input rejected'):batch_worker(tree,spec,base/'replayed')

    def test_batch_mapping_escape_and_wrong_registry_hash_rejected(self):
        with self.temp() as tmp:
            base=Path(tmp);tree,spec=self.batch_tree(base)
            for key in ('label_manifest','registry','jobs','work','policy','events','members'):
                bad=copy.deepcopy(spec);bad[key]='../escape'
                with self.subTest(key=key),self.assertRaises(ValueError):batch_inputs(tree,bad)
            for key in ('folder','freeze'):
                bad=copy.deepcopy(spec);bad['batch_specs'][0][key]='../escape'
                with self.subTest(key=key),self.assertRaises(ValueError):batch_inputs(tree,bad)
            label=read(tree/'labels.json');label['registry_sha256']='0'*64;write(tree/'labels.json',label)
            with self.assertRaisesRegex(ValueError,'registry differs'):batch_inputs(tree,spec)

    def test_batch_worker_runs_with_parent_offline_guard(self):
        with self.temp() as tmp:
            base=Path(tmp);tree,spec=self.batch_tree(base)
            shutil.copytree(BASE/'src',tree/'src')
            validator=Validator(tree,base/'out','public');descriptor=validator.out/'batch.json';write(descriptor,spec)
            self.assertTrue(validator.command('batch_worker','src/validate_review_bundle_r1.py',
                            ['--batch-worker-spec',descriptor,'--tree',tree,'--output',validator.out/'replay']),
                            (validator.out/'01_batch_worker_stderr.txt').read_text(encoding='utf-8'))
            self.assertEqual(read(validator.out/'replay/portable_mapping_receipt.json')['status'],'PASS')

    def test_batch_missing_required_expected_lp_records_fail(self):
        with self.temp() as tmp:
            base=Path(tmp);tree,spec=self.batch_tree(base)
            batch_worker(tree,spec,tree/'expected');spec['expected']='expected'
            shutil.copytree(BASE/'src',tree/'src')
            validator=Validator(tree,base/'out','min')
            validator.bounded('required_batch',lambda:validator.replay(spec['name'],'batch',spec,spec['expected']))
            self.assertEqual(validator.commands[-1]['name'],'required_batch')
            self.assertEqual(validator.commands[-1]['status'],'FAIL')
            self.assertEqual(validator.commands[-1]['error_type'],'FileNotFoundError')
            self.assertTrue(any(c['name'].endswith('_batch_status') and c['status']=='PASS' for c in validator.commands))
            self.assertFalse(any(c['status']=='SKIP' for c in validator.commands))

    def slice_spec(self,tree,spec):
        full_sha='a'*64;source={'registry_sha256':full_sha,'label_snapshot_path':'original/full_labels'}
        write(tree/'source_apply.json',source);source_bytes=(tree/'source_apply.json').read_bytes()
        source_sha=hashlib.sha256(source_bytes).hexdigest();label=read(tree/'labels.json')
        label.pop('label_snapshot_path');label.update(type='PRIVATE_PORTABLE_LABEL_REFERENCE_SLICE',registry_path='registry.csv',
            source_registry_sha256=full_sha,source_apply_manifest_sha256=source_sha,
            source_apply_manifest_path='historical/source_apply.json',byte_identical_file_mappings=[{
                'portable_path':'source_apply.json','source':'historical/source_apply.json','source_sha256':source_sha,
                'portable_sha256':source_sha,'bytes':len(source_bytes),'byte_identical':True}])
        write(tree/'labels.json',label);spec['label_slice_mapping']={'source_apply_manifest':'source_apply.json'}
        return label

    def test_slice_mapping_binds_full_source_and_slice_hashes_separately(self):
        with self.temp() as tmp:
            base=Path(tmp);tree,spec=self.batch_tree(base);label=self.slice_spec(tree,spec)
            paths,label,_=batch_inputs(tree,spec);mapping=label_slice_identity(tree,spec,paths,label)
            old={'registry_sha256':mapping['source_registry_sha256'],'label_manifest_sha256':mapping['source_apply_manifest_sha256']}
            new={'registry_sha256':mapping['slice_registry_sha256'],'label_manifest_sha256':mapping['slice_manifest_sha256']}
            self.assertTrue(batch_status_identity(old,new,mapping));self.assertFalse(batch_status_identity(old,new,None))
            for record in (old,new):
                for key in ('registry_sha256','label_manifest_sha256'):
                    changed=record.copy();changed[key]='0'*64
                    self.assertFalse(batch_status_identity(changed,new,mapping) if record is old else batch_status_identity(old,changed,mapping))
            result=batch_worker(tree,spec,base/'replayed')
            self.assertEqual(result['label_slice_identity'],mapping)

    def test_forged_slice_mapping_original_bytes_and_source_hash_rejected(self):
        with self.temp() as tmp:
            base=Path(tmp);tree,spec=self.batch_tree(base);label=self.slice_spec(tree,spec)
            for key in ('source_apply_manifest_sha256','source_registry_sha256'):
                forged=copy.deepcopy(label);forged[key]='0'*64;write(tree/'labels.json',forged)
                with self.subTest(key=key),self.assertRaises(ValueError):batch_inputs(tree,spec)
            write(tree/'labels.json',label)
            (tree/'source_apply.json').write_text('{}',encoding='utf-8')
            with self.assertRaisesRegex(ValueError,'source apply manifest bytes'):batch_inputs(tree,spec)

    def test_slice_mapping_requires_explicit_path_and_consistent_byte_mapping(self):
        with self.temp() as tmp:
            base=Path(tmp);tree,spec=self.batch_tree(base);label=self.slice_spec(tree,spec)
            missing=copy.deepcopy(spec);missing.pop('label_slice_mapping')
            with self.assertRaisesRegex(ValueError,'explicit'):batch_inputs(tree,missing)
            escaping=copy.deepcopy(spec);escaping['label_slice_mapping']['source_apply_manifest']='../escape'
            with self.assertRaises(ValueError):batch_inputs(tree,escaping)
            for field,value in [('byte_identical',False),('source_sha256','0'*64),('portable_sha256','0'*64),('bytes',1),('source','wrong')]:
                forged=copy.deepcopy(label);forged['byte_identical_file_mappings'][0][field]=value;write(tree/'labels.json',forged)
                with self.subTest(field=field),self.assertRaisesRegex(ValueError,'byte mapping'):batch_inputs(tree,spec)

    def test_required_no_new_candidate_jobs_is_enforced(self):
        with self.temp() as tmp:
            base=Path(tmp);tree,spec=self.batch_tree(base);spec['required_no_new_candidate_jobs']=True
            with self.assertRaisesRegex(ValueError,'forbids new candidate'):batch_inputs(tree,spec)
            spec['batch_specs']=[]
            result=batch_worker(tree,spec,base/'replayed')
            self.assertTrue(result['required_no_new_candidate_jobs']);self.assertEqual(result['required_batch_count'],0)

if __name__=='__main__':unittest.main()
