import hashlib
import json
import sqlite3
import tempfile
import shutil
import unittest
from copy import deepcopy
from dataclasses import asdict, replace
from pathlib import Path
from contextlib import closing

from collector import Event, State, Scope
from stage1d_acquisition import Labels
from stage1d_runtime import Runtime
from read_retry_r4 import logical_key
from stage1d_unknown_cost_registry import Registry, SCHEMA, CODE_SCHEMA, OBSERVATION_SCHEMA, CHECK_SCHEMA, REVIEW_SCHEMA, PROVIDER
import stage1d_unknown_cost_boundary as core

A='0x'+'a'*40; B='0x'+'b'*40; P='0x'+'c'*40; BH='0x'+'d'*64


class Fixture:
    def __init__(self, root, completed=False, code='0x', n=1):
        self.work=Path(root)/'code';self.work.mkdir(parents=True)
        self.query=dict(query_id='q1',name='q1',start_block=1,end_block=100,
                        start_time_utc='1970-01-01T00:00:00Z',end_time_utc='1970-01-01T01:00:00Z',
                        max_acquisition_depth=3,window_mode='QUERY_ARRIVAL_WINDOW_SECONDS_V1',
                        local_window_seconds=3600,primary_local_window_seconds=3600)
        self.scope=Scope.from_policy(self.query)
        arrival=Event('arrival','0x'+'0'*64,P,A,core.NATIVE,100,1,0,0,block_hash=BH)
        self.state=State('q1',A,core.NATIVE,arrival,0,3600)
        policy=dict(schema_version=core.POLICY_SCHEMA,authorization_id=core.AUTH,enabled=True,
                    threshold_daily_strict_gt=20,authority_source_sha256=core.AUTHORITY_SHA256,
                    query_scope_hashes={'q1':self.scope.scope_hash,'q2':'a'*64,'q3':'b'*64,'q4':'c'*64})
        self.policy_ref=self.write('private/stage1d_roles/UNKNOWN_COST_POLICY.json',policy)
        snap=dict(authorization_id=core.AUTH,authority_sha256=core.AUTHORITY_SHA256,
                  query_snapshots=[{'query':self.query}],
                  state_screen_rows=[dict(state=asdict(self.state),scope_hash=self.scope.scope_hash,
                                          new_cost_screen_required=not completed,
                                          initial_unfinished_arrival=not completed,
                                          existing_boundary_or_depth=False)])
        self.snapshot_ref=self.write('private/test_initial.json',snap)
        self.write('derived/stage1d/labels/a.json',{'rows':[{'address':A,'identity_class':'UNKNOWN','lookup_status':'COMPLETED_FOUR_TABLE_OPPORTUNITY'}]})
        self.current=dict(schema_version=SCHEMA,policy_ref=self.policy_ref,initial_snapshot_ref=self.snapshot_ref,
                          identity_checks=[],historical_codes=[],activity_observations=[])
        self.add_code(code)
        rows=[asdict(Event('out'+str(i),'0x'+format(i+1,'064x'),A,B,core.NATIVE,1,i+2,0,i+1)) for i in range(n)]
        self.events_ref=self.write('private/observed.json',{'candidate_events':rows,'context_events':[]})
        self.observation_ref=self.write('private/activity.json',dict(schema_version=OBSERVATION_SCHEMA,chain_id='eip155:1',source_refs=[self.events_ref]))
        self.current['activity_observations']=[self.observation_ref]
        self.add_online();self.add_search();self.commit()

    def write(self, path, value):
        p=self.work/path;p.parent.mkdir(parents=True,exist_ok=True)
        raw=(json.dumps(value,sort_keys=True)+'\n').encode();p.write_bytes(raw)
        return dict(path=path,sha256=hashlib.sha256(raw).hexdigest())

    def add_code(self, code):
        request={'jsonrpc':'2.0','id':'code1','method':'eth_getCode','params':[A,'0x1']}
        response={'jsonrpc':'2.0','id':'code1','result':code}
        body_ref=self.write('raw/test/response_body.bin',[response])
        env=dict(provider_alias=PROVIDER,evidence_kind='REAL_CHAIN',status='SUCCESS_VALIDATED',
                 raw_body_sha256=body_ref['sha256'],http_status=200,response_complete=True,request=request,response=response)
        self.code_ref=self.write('raw/test/envelope.json',env)
        plan={k:request[k] for k in ('method','params')};identity=Runtime().rpc_identity(PROVIDER,plan)
        p=self.work/'private/read_retry_r4.sqlite';p.parent.mkdir(parents=True,exist_ok=True)
        with closing(sqlite3.connect(p)) as db:
            db.execute('CREATE TABLE IF NOT EXISTS read_requests(logical_key TEXT PRIMARY KEY,identity_json TEXT,state TEXT,success_payload TEXT,success_receipt TEXT)')
            db.execute('INSERT OR REPLACE INTO read_requests VALUES(?,?,?,?,?)',(logical_key(identity),json.dumps(identity),'SUCCESS',json.dumps(code),json.dumps(dict(artifact_path=self.code_ref['path'],artifact_sha256=self.code_ref['sha256']))))
            db.commit()
        descriptor=dict(schema_version=CODE_SCHEMA,chain_id='eip155:1',address=A,
                        request_ref=self.code_ref,response_ref=self.code_ref,arrival_block_hash=BH)
        self.current['historical_codes']=[self.write('private/code_descriptor.json',descriptor)]

    def add_online(self):
        aliases=['cex_addresses','labels_addresses','owner_addresses','deposit_addresses']
        row={'address':A}
        for k in aliases:row[k+'_json']='[]';row[k+'_count']=0
        md={'total_row_count':1,'row_count':1,'column_names':list(row)}
        page={'execution_id':'execution1','state':'QUERY_STATE_COMPLETED','result':{'rows':[row],'metadata':md}}
        raw_ref=self.write('raw/test/labels.json',page)
        page_ref=self.write('private/label_page.json',page)
        receipt=dict(evidence_kind='REAL_PROVIDER',http_status=200,error_class=None,execution_id='execution1',
                     parameters={'offset':0,'limit':1000},raw_path=raw_ref['path'],sha256=raw_ref['sha256'])
        receipt_ref=self.write('private/label_receipt.json',receipt)
        freeze_ref=self.write('private/label_freeze.json',{'kind':'frontier_labels','sql_sha256':'f'*64,'addresses':[A]})
        job=dict(state='QUERY_STATE_COMPLETED',execution_id='execution1',sql_sha256='f'*64,
                 status_response={'state':'QUERY_STATE_COMPLETED','execution_id':'execution1','result_metadata':md},
                 r4_verified_pages={'0':dict(page_path=page_ref['path'],page_sha256=page_ref['sha256'],receipt_path=receipt_ref['path'],receipt_sha256=receipt_ref['sha256'])})
        self.job_ref=self.write('private/label_job.json',job)
        check=dict(schema_version=CHECK_SCHEMA,chain_id='eip155:1',address=A,channel='online',adapter='DUNE_FOUR_TABLE_R4',freeze_ref=freeze_ref,job_ref=self.job_ref)
        self.current['identity_checks'].append(self.write('private/online_check.json',check))

    def add_search(self):
        env=dict(provider='web.run',request={'search_query':[{'q':A+' Ethereum'}]},tool_result={'content':[{'type':'text','text':'Synthetic finite search output.'}]},completed=True)
        self.search_ref=self.write('raw/test/search.json',env)
        review=dict(schema_version=REVIEW_SCHEMA,chain_id='eip155:1',address=A,search_ref=self.search_ref,
                    conclusion='NO_ADOPTABLE_ROLE_AFTER_FINITE_REVIEW',explanation='Synthetic no adoptable role fixture.')
        self.review_ref=self.write('private/search_review.json',review)
        check=dict(schema_version=CHECK_SCHEMA,chain_id='eip155:1',address=A,channel='exact_search',adapter='EXACT_ADDRESS_SEARCH_V1',
                   request_ref=self.search_ref,response_ref=self.search_ref,review_ref=self.review_ref,outcome='EXACT_SEARCH_CHECKED')
        self.current['identity_checks'].append(self.write('private/search_check.json',check))

    def commit(self):
        self.write('private/stage1d_roles/UNKNOWN_COST_CURRENT.json',self.current)

    def resolver(self):
        labels=Labels(self.work)
        return labels.cost_boundary_resolver,labels


class RegistryTests(unittest.TestCase):
    def setUp(self):
        tmp=Path(__file__).resolve().parents[1]/'tmp';tmp.mkdir(exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=tmp)
    def tearDown(self):self.temp.cleanup()

    def test_actual_registry_labels_binding_and_high_rate_stop(self):
        f=Fixture(self.temp.name);r,labels=f.resolver();d=r.resolve(f.state,f.scope,labels.resolve_state(f.state))
        self.assertEqual(d['action'],'STOP',d)
        self.assertEqual(d['reason'],'UNKNOWN_NO_CODE_HIGH_OUTFLOW_COST_BOUNDARY')
        self.assertEqual(d['activity']['N_obs'],1)
        self.assertEqual(d['base_identity']['kind'],'UNKNOWN')
        self.assertEqual(len(r.identity),64)
        self.assertEqual(r.identity,Registry(f.work).identity)
        self.assertTrue(core.validate_decision(f.state,f.scope,d,f.policy_ref['sha256']))

    def test_code_present_low_activity_stops_and_previous_complete_bypasses(self):
        f=Fixture(self.temp.name,code='0x00',n=0);r,l=f.resolver()
        self.assertEqual(r.resolve(f.state,f.scope,l.resolve_state(f.state))['reason'],'UNKNOWN_CODE_COST_BOUNDARY')
        snap=json.loads((f.work/f.snapshot_ref['path']).read_text());snap['state_screen_rows'][0].update(new_cost_screen_required=False,initial_unfinished_arrival=False)
        f.current['initial_snapshot_ref']=f.write(f.snapshot_ref['path'],snap);f.commit();r,l=f.resolver()
        self.assertEqual(r.resolve(f.state,f.scope,l.resolve_state(f.state))['reason'],'PREVIOUSLY_COMPLETED_EXACT_STATE_PRESERVED')

    def test_code_wrong_block_and_failed_code_are_pending(self):
        f=Fixture(self.temp.name);r,l=f.resolver()
        future=replace(f.state,arrival=replace(f.state.arrival,event_id='future',block=2,timestamp=1),local_end=3600)
        self.assertEqual(r.resolve(future,f.scope,l.resolve_state(future))['reason'],'TYPE_UNRESOLVED')
        f.add_code('0x0');f.commit();r,l=f.resolver()
        self.assertEqual(r.resolve(f.state,f.scope,l.resolve_state(f.state))['reason'],'TYPE_UNRESOLVED')

    def test_current_success_payload_provenance_is_required(self):
        f=Fixture(self.temp.name)
        with closing(sqlite3.connect(f.work/'private/read_retry_r4.sqlite')) as db:
            db.execute("UPDATE read_requests SET success_payload='\"0x00\"'");db.commit()
        r,l=f.resolver();self.assertEqual(r.resolve(f.state,f.scope,l.resolve_state(f.state))['reason'],'TYPE_UNRESOLVED')

    def test_original_transport_body_is_reverified(self):
        f=Fixture(self.temp.name,code='0x00')
        (f.work/'raw/test/response_body.bin').write_text('[]')
        r,l=f.resolver();d=r.resolve(f.state,f.scope,l.resolve_state(f.state))
        self.assertEqual(d['reason'],'TYPE_UNRESOLVED')
        self.assertIn('bytes changed',d['evidence_validation_error'])

    def test_naked_checked_or_unreviewed_search_cannot_stop(self):
        f=Fixture(self.temp.name)
        f.current['identity_checks']=[];f.commit();r,l=f.resolver()
        self.assertEqual(r.resolve(f.state,f.scope,l.resolve_state(f.state))['reason'],'IDENTITY_CHECK_PENDING')
        f.add_online();f.add_search()
        check=json.loads((f.work/'private/search_check.json').read_text());check.pop('review_ref')
        f.current['identity_checks'][-1]=f.write('private/search_check.json',check);f.commit();r,l=f.resolver()
        self.assertEqual(r.resolve(f.state,f.scope,l.resolve_state(f.state))['reason'],'IDENTITY_CHECK_PENDING')

    def test_partial_low_activity_needs_no_exact_search(self):
        f=Fixture(self.temp.name,n=0);f.current['identity_checks']=f.current['identity_checks'][:1];f.commit()
        r,l=f.resolver();d=r.resolve(f.state,f.scope,l.resolve_state(f.state))
        self.assertEqual(d['action'],'CONTINUE');self.assertEqual(d['activity']['coverage'],'OBSERVED_LOWER_BOUND')

    def test_successful_search_failed_single_page_stays_access_blocked(self):
        f=Fixture(self.temp.name,code='0x00')
        page_ref=f.write('raw/test/page_failed.json',dict(provider='web.run',completed=False,
                         request={'open':[{'ref_id':'https://example.invalid/exact'}]},
                         tool_result={'content':[{'type':'text','text':'Original nonretryable failure'}]}))
        check=json.loads((f.work/'private/search_check.json').read_text())
        check.update(page_refs=[page_ref],outcome='ACCESS_BLOCKED');check.pop('review_ref')
        f.current['identity_checks'][-1]=f.write('private/search_check.json',check);f.commit()
        r,l=f.resolver();d=r.resolve(f.state,f.scope,l.resolve_state(f.state))
        self.assertEqual(d['reason'],'IDENTITY_CHECK_PENDING')
        self.assertEqual(d['identity_checks']['exact_search']['status'],'ACCESS_BLOCKED')
        self.assertIn(page_ref,d['evidence_refs'])

    def test_activity_original_mismatch_is_rejected(self):
        f=Fixture(self.temp.name)
        original=json.loads((f.work/f.events_ref['path']).read_text())['candidate_events'][0]
        bad=deepcopy(original);bad['amount_raw']=999
        d=dict(schema_version=OBSERVATION_SCHEMA,chain_id='eip155:1',source_refs=[f.events_ref],events=[bad])
        f.current['activity_observations']=[f.write('private/activity.json',d)];f.commit()
        with self.assertRaises(ValueError):Registry(f.work)

    def test_changed_source_bytes_are_rejected(self):
        f=Fixture(self.temp.name);(f.work/f.events_ref['path']).write_text('{}')
        with self.assertRaises(ValueError):Registry(f.work)

    def test_old_scope_has_no_policy_and_hold_priority(self):
        f=Fixture(self.temp.name);r,l=f.resolver()
        old=replace(f.scope,end_time=7200,scope_id=None)
        self.assertIsNone(r.policy_for_scope(old))
        held=dict(kind='UNKNOWN',branch_action='USER_REQUESTED_BRANCH_HOLD')
        self.assertEqual(r.resolve(f.state,f.scope,held)['reason'],'EXISTING_USER_REQUESTED_BRANCH_HOLD')

    def test_cross_sender_conflict_not_hidden_by_address_index(self):
        f=Fixture(self.temp.name)
        source=json.loads((f.work/f.events_ref['path']).read_text());bad=deepcopy(source['candidate_events'][0]);bad['sender']=P
        source['context_events']=[bad];f.events_ref=f.write('private/observed.json',source)
        f.current['activity_observations']=[f.write('private/activity.json',dict(schema_version=OBSERVATION_SCHEMA,chain_id='eip155:1',source_refs=[f.events_ref]))]
        f.commit();r,l=f.resolver();d=r.resolve(f.state,f.scope,l.resolve_state(f.state))
        self.assertEqual(d['activity']['N_obs'],0)
        self.assertEqual(d['action'],'CONTINUE')

    def test_role_current_and_authority_versions_change_registry_identity(self):
        f=Fixture(self.temp.name)
        f.write('private/stage1d_roles/CURRENT.json',{'certificates':[],'version':'one'})
        f.write('private/stage1d_roles/AUTHORITIES.json',{'schema_version':'stage1d-role-authorities-v1','authorities':[]})
        first=Registry(f.work).identity
        f.write('private/stage1d_roles/CURRENT.json',{'certificates':[],'version':'two'})
        second=Registry(f.work).identity;self.assertNotEqual(first,second)
        f.write('private/stage1d_roles/AUTHORITIES.json',{'schema_version':'stage1d-role-authorities-v1','authorities':[],'version':'two'})
        third=Registry(f.work).identity;self.assertNotEqual(second,third)
        f.write('private/stage1d_roles/USER_TASK_BOUNDARIES.json',{'boundaries':[],'fixture_version':'one'})
        self.assertNotEqual(third,Registry(f.work).identity)

    def test_existing_technical_role_verifier_rechecks_nested_raw_sources(self):
        from test_stage1d_role_adoption_evidence import fixture as role_fixture
        f=Fixture(self.temp.name)
        role_root=Path(self.temp.name)/'controlled_role'
        certificate=role_fixture(role_root,public_text=True)
        for path in (role_root/'private/stage1d_roles').iterdir():
            shutil.copyfile(path,f.work/'private/stage1d_roles'/path.name)
        cert_ref=f.write('private/stage1d_roles/certificate.json',certificate)
        f.write('private/stage1d_roles/CURRENT.json',{'certificates':[cert_ref]})
        registry=Registry(f.work)
        refs=registry.identity_document['role_source_refs']
        self.assertTrue(any(r['path'].endswith('historical_code.json') for r in refs))
        self.assertTrue(any(r['path'].endswith('journal.json') for r in refs))
        self.assertEqual(registry.identity_document['role_validation']['certificate_ids'],['controlled-cert'])
        (f.work/'private/stage1d_roles/historical_code.json').write_text('{}')
        with self.assertRaises(ValueError):Registry(f.work)


if __name__=='__main__':unittest.main()
