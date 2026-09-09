import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import zipfile
from build_delivery import (TARGETS, NAME, IMPACT_SCHEMA, build, checked, encoded, sha,
                            project_collection_document, snapshot_projection, validate_impact, verify_archive)

S = Path(__file__).resolve().parent
FIXED_HOLD_REQUEST = (S.parent.parent / 'operations/USER_971_BRANCH_HOLD_REQUEST.json').read_bytes()
def save(p, obj):
    p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(encoded(obj)); return p
def reference(p, root): return {'path': p.relative_to(root).as_posix(), 'sha256': sha(p), 'bytes': p.stat().st_size}

class DeliveryChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='test_', dir=S); self.r = Path(self.temp.name)
        self.t = self.r/'reports/fixture'; self.c = self.r/'code'
    def tearDown(self): self.temp.cleanup()
    def fixture(self):
        addr = TARGETS[1]
        event = {'event_id':'event:one','chain_id':'eip155:1','sender':addr,'recipient':TARGETS[0], 'amount_raw':1,'success':True}
        snap = {'snapshot_id':'synthetic','started_at':'synthetic','ended_at':'synthetic','queries':[]}
        scopes = {'queries':[]}
        for index,name in enumerate(('txphish_src001','txphish_src002')):
            target = {a:{'events':[event], 'states':[], 'frontier':[]} for a in TARGETS}
            snap['queries'].append({'query_name':name,'metrics':{},'targets':target,'semantic_units':[]})
            scopes['queries'].append({'name':name,'query_id':'qry:'+str(index),'scope_hash':str(index)*64})
        sp = save(self.t/'inputs/SAFE_SNAPSHOT.json',snap)
        lp = save(self.t/'inputs/LOCAL_LABEL_EVIDENCE.json',[])
        ip = self.t/'inputs/STAGE1D_TWO_ADDRESS_IDENTITY_CHECK_V1.txt'; ip.write_text('SYNTHETIC_CONTROLLED',encoding='utf-8')
        save(self.t/'SOURCE_MANIFEST.json',{'snapshot':reference(sp,self.t),'local_label_evidence':reference(lp,self.t),'instruction_sha256':sha(ip)})
        save(self.t/'inputs/STAGE1D_CLOSURE_BATCH.json',scopes)
        save(self.t/'inputs/CURRENT_SEMANTIC_CATALOGUE.json',{'instances':[]})
        adoption={'status':'OFFICIAL_SCOPE_STOP_ADOPTED_REPLAY_PENDING','verification':{'status':'PASS'},'address':addr,
                  'decision_basis':'OFFICIAL_DIRECT_ROLE_SCOPE_STOP_V1','query_scopes':[]}
        save(self.r/'operations/F621_OFFICIAL_ROLE_ADOPTION.json',adoption)
        hold_request=self.r/'operations/USER_971_BRANCH_HOLD_REQUEST.json';hold_request.write_bytes(FIXED_HOLD_REQUEST)
        hold=save(self.r/'operations/USER_971_BRANCH_HOLD_ADOPTION.json',{
            'status':'USER_BRANCH_HOLD_ADOPTED_REPLAY_PENDING','address':TARGETS[0],
            'action':'USER_REQUESTED_BRANCH_HOLD','identity':'UNKNOWN','synthetic_fixture':True})
        save(self.t/'inputs/LATEST_GRAPH_IMPACT.json',{
            'schema_version':IMPACT_SCHEMA,'status':'REPLAY_PENDING','query_results':[],
            'prior_snapshot_sha256':sha(sp),'adoption_receipt_sha256':sha(self.r/'operations/F621_OFFICIAL_ROLE_ADOPTION.json'),
            'user_971_hold_adoption_sha256':sha(hold),
            'user_requested_971_hold':{'synchronization_status':'REPLAY_PENDING'},
            'actual_changes':None,'physical_dedup':None,'reference_impact':{'status':'UNKNOWN'},
            'credits_saving':None,'CU_saving':None,'bytes_saving':None,'seconds_saving':None})
        roles=self.c/'private/stage1d_roles'
        save(roles/'AUTHORITIES.json',{});save(roles/'CURRENT.json',{})
        for name in ('certificate','deployment_binding','official_visible_excerpt'):
            save(roles/'adoptions/f621_official_scope_stop_v1'/(name+'.json'),{'synthetic':True})
        raw=self.c/'raw/stage1d_two_address_identity/fixture'
        finish={'web_tool_calls':5,'logical_search_requests':3,'logical_page_open_requests':2,'research_rpc_sql_requests':0,
                'underlying_http_requests':None,'web_platform_billing':None}
        members=[]
        for i in range(5):
            request={'address':TARGETS[i%2],'chain_id':'eip155:1','kind':'SEARCH' if i<3 else 'PAGE_OPEN'}
            request_path=save(raw/f'call_{i}.json',request);members.append(reference(request_path,self.c))
            p=save(raw/f'call_{i}_response.json',{'request':request,'http_status':None,'tool_call_count':1})
            members.append(reference(p,self.c))
        fp=save(raw/'FINISH.json',finish); members.append(reference(fp,self.c))
        save(raw/'SESSION_RECEIPT.json',{'status':'FINITE_PUBLIC_TOOL_SESSION_CLOSED','finish':finish,'members':members})
        return snap,scopes
    def test_complete_archive_readback_and_unknowns(self):
        self.fixture(); out=self.t/'delivery/files'
        report=build(self.r,self.t,out)
        self.assertEqual(report['status'],'TWO_ADDRESS_IDENTITY_CHECK_READY')
        self.assertEqual(verify_archive(out.parent/NAME)['status'],'PASS')
        data=json.loads((out/'TWO_ADDRESS_IDENTITY_RESULTS.json').read_text(encoding='utf-8'))
        self.assertEqual(data['graph_role_semantic_sync_status'],'SEMANTIC_REPLAY_PENDING')
        self.assertIsNone(data['actual_graph_impact']['actual_changes'])
        self.assertIsNone(data['request_accounting']['actual_fee'])
        self.assertEqual(data['request_accounting']['logical_requests'],5)
        unknown=data['addresses'][0]
        self.assertTrue(unknown['still_unknown']);self.assertTrue(unknown['adopted_boundary'])
        self.assertEqual(unknown['boundary_kind'],'USER_REQUESTED_BRANCH_HOLD')
        self.assertEqual(unknown['adoption_status'],'USER_BRANCH_HOLD_ADOPTED_REPLAY_PENDING')
        self.assertEqual(unknown['graph_synchronization_status'],'REPLAY_PENDING')
        self.assertFalse(data['generic_activity_threshold_cutoff']['applied'])
        with zipfile.ZipFile(out.parent/NAME) as z:
            self.assertNotIn('inputs/SAFE_SNAPSHOT.json',z.namelist())
            self.assertTrue(all('collection.json' not in n for n in z.namelist()))
    def test_existing_output_preserved(self):
        self.fixture(); out=self.t/'delivery/files'; build(self.r,self.t,out)
        before=sha(out/'TWO_ADDRESS_IDENTITY_RESULTS.json')
        with self.assertRaises(ValueError):build(self.r,self.t,out)
        self.assertEqual(before,sha(out/'TWO_ADDRESS_IDENTITY_RESULTS.json'))
    def test_mutated_source_rejected(self):
        self.fixture(); p=self.t/'inputs/SAFE_SNAPSHOT.json';p.write_bytes(p.read_bytes()+b' ')
        with self.assertRaisesRegex(ValueError,'hash'):build(self.r,self.t,self.t/'delivery/files')
    def test_projection_wire_is_not_iterated_or_treated_as_four_gaps(self):
        doc={'query_id':'q','status':'PARTIAL','gaps':{'schema_version':'stage1d-ordered-gap-sequence-v1','expanded_count':1000000,'templates':{},'segments':[]}}
        self.assertEqual(project_collection_document(doc)['gap_metadata']['declared_count'],1000000)
        self.assertNotIn('gaps',project_collection_document(doc))
        doc['gaps']['expanded_count']=True
        with self.assertRaises(ValueError):project_collection_document(doc)
    def test_physical_projection_dedup_keeps_query_rows(self):
        snap,_=self.fixture(); p=snapshot_projection(snap)
        self.assertEqual(len(p['queries']),2)
        self.assertEqual(p['physical_event_dedup'][TARGETS[1]]['distinct_chain_event_ids'],1)
        self.assertEqual([q['addresses'][TARGETS[1]]['direct_candidate_event_count'] for q in p['queries']],[1,1])
    def test_actual_completion_requires_both_matching_current_graph_refs(self):
        _,scopes=self.fixture()
        val={'schema_version':IMPACT_SCHEMA,'status':'ACTUAL_REPLAY_COMPLETE','prior_snapshot_sha256':'a','adoption_receipt_sha256':'b','query_results':[]}
        with self.assertRaises(ValueError):validate_impact(val,self.r,'a','b',scopes)
        for q in scopes['queries']:
            p=save(self.c/(q['name']+'.json'),{'synthetic':True})
            val['query_results'].append({'query_name':q['name'],'query_id':q['query_id'],'scope_hash':q['scope_hash'],'after_collection':reference(p,self.r),'addresses':{}})
        self.assertEqual(validate_impact(val,self.r,'a','b',scopes)['status'],'ACTUAL_REPLAY_COMPLETE')
        val['query_results'][0]['scope_hash']='wrong'
        with self.assertRaises(ValueError):validate_impact(val,self.r,'a','b',scopes)
    def test_nonnull_savings_and_wrong_prior_rejected(self):
        _,scopes=self.fixture();v={'schema_version':IMPACT_SCHEMA,'status':'REPLAY_PENDING','prior_snapshot_sha256':'a','adoption_receipt_sha256':'b','credits_saving':3}
        with self.assertRaises(ValueError):validate_impact(v,self.r,'a','b',scopes)
        v['credits_saving']=None
        with self.assertRaises(ValueError):validate_impact(v,self.r,'wrong','b',scopes)
    def test_crc_pass_does_not_hide_sha_tampering(self):
        archive=self.r/'bad.zip'
        with zipfile.ZipFile(archive,'w') as z:
            z.writestr('REPORT.md','changed');z.writestr('FILE_HASHES.txt',hashlib.sha256(b'original').hexdigest()+'  REPORT.md\n')
        with self.assertRaisesRegex(ValueError,'SHA mismatch'):verify_archive(archive)
    def test_member_manifest_cannot_omit_member(self):
        archive=self.r/'extra.zip'
        with zipfile.ZipFile(archive,'w') as z:
            z.writestr('extra','bad');z.writestr('FILE_HASHES.txt','')
        with self.assertRaisesRegex(ValueError,'member set'):verify_archive(archive)
    def test_missing_actual_hold_adoption_refuses_final_package(self):
        self.fixture();(self.r/'operations/USER_971_BRANCH_HOLD_ADOPTION.json').unlink()
        with self.assertRaisesRegex(ValueError,'adoption receipt missing'):build(self.r,self.t,self.t/'delivery/files')
        self.assertFalse((self.t/'delivery'/NAME).exists())
    def test_missing_graph_receipt_refuses_final_package(self):
        self.fixture();(self.t/'inputs/LATEST_GRAPH_IMPACT.json').unlink()
        with self.assertRaisesRegex(ValueError,'graph-impact receipt missing'):build(self.r,self.t,self.t/'delivery/files')
        self.assertFalse((self.t/'delivery'/NAME).exists())
    def test_hold_never_changes_unknown_into_protocol(self):
        self.fixture();p=self.r/'operations/USER_971_BRANCH_HOLD_ADOPTION.json'
        obj=json.loads(p.read_text(encoding='utf-8'));obj['identity']='UNSUPPORTED_PROTOCOL';save(p,obj)
        with self.assertRaisesRegex(ValueError,'contradicts user request'):build(self.r,self.t,self.t/'delivery/files')
    def test_graph_receipt_must_bind_latest_hold_adoption(self):
        self.fixture();p=self.t/'inputs/LATEST_GRAPH_IMPACT.json'
        obj=json.loads(p.read_text(encoding='utf-8'));obj['user_971_hold_adoption_sha256']='0'*64;save(p,obj)
        with self.assertRaisesRegex(ValueError,'actual 971 user-hold adoption'):build(self.r,self.t,self.t/'delivery/files')
    def test_actual_graph_can_finish_while_semantics_explicitly_pending(self):
        _,scopes=self.fixture();p=self.t/'inputs/LATEST_GRAPH_IMPACT.json';obj=json.loads(p.read_text(encoding='utf-8'))
        obj['status']='ACTUAL_REPLAY_COMPLETE';obj['user_requested_971_hold']['synchronization_status']='APPLIED_IN_CURRENT_GRAPHS'
        for q in scopes['queries']:
            graph=save(self.c/(q['name']+'.json'),{'synthetic':True})
            obj['query_results'].append({'query_name':q['name'],'query_id':q['query_id'],'scope_hash':q['scope_hash'],
                                         'after_collection':reference(graph,self.r),'addresses':{}})
        save(p,obj);out=self.t/'delivery/files';build(self.r,self.t,out)
        data=json.loads((out/'TWO_ADDRESS_IDENTITY_RESULTS.json').read_text(encoding='utf-8'))
        self.assertEqual(data['graph_replay_status'],'ACTUAL_REPLAY_COMPLETE')
        self.assertEqual(data['semantic_replay_status'],'SEMANTIC_REPLAY_PENDING')
        self.assertEqual(data['user_requested_971_hold']['graph_synchronization_status'],'APPLIED_IN_CURRENT_GRAPHS')
    def test_explicit_pending_delivery_never_fabricates_971_adoption(self):
        self.fixture();(self.r/'operations/USER_971_BRANCH_HOLD_ADOPTION.json').unlink()
        out=self.t/'delivery/files';build(self.r,self.t,out,allow_pending_hold=True)
        data=json.loads((out/'TWO_ADDRESS_IDENTITY_RESULTS.json').read_text(encoding='utf-8'))
        self.assertFalse(data['addresses'][0]['adopted_boundary'])
        self.assertTrue(data['addresses'][0]['still_unknown'])
        self.assertEqual(data['user_requested_971_hold']['adoption_status'],'REQUESTED_PENDING_SAFE_POINT')
        self.assertIsNone(data['user_requested_971_hold']['adoption_sha256'])
        self.assertFalse(data['main_stage_completion_claimed'])
        self.assertNotIn('main_checkpoint_unchanged',data)

if __name__=='__main__':unittest.main()
