import json
import sqlite3
import tempfile
import unittest
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch
from contextlib import closing
from test_stage1d_unknown_cost_registry import Fixture, A, BH, PROVIDER
from stage1d_code_cache_admission import admit_operation
from stage1d_current_code_preparation import SCHEMA, digest, validate
from stage1d_transfers_acquisition import cached_member
from stage1d_runtime import Runtime
from stage1d_unknown_cost_boundary import AUTH, state_key
from stage1d_unknown_cost_registry import CURRENT_PATH
from read_retry_r4 import logical_key
from context_access_r3 import sha


def rpc(f, plan, value, name):
    req={'jsonrpc':'2.0','id':name,**plan};resp={'jsonrpc':'2.0','id':name,'result':value}
    body=f.write('raw/'+name+'/response_body.bin',[resp])
    ref=f.write('raw/'+name+'/envelope.json',dict(provider_alias=PROVIDER,evidence_kind='REAL_CHAIN',
        status='SUCCESS_VALIDATED',raw_body_sha256=body['sha256'],http_status=200,response_complete=True,
        request=req,response=resp))
    identity=Runtime().rpc_identity(PROVIDER,plan)
    with closing(sqlite3.connect(f.work/'private/read_retry_r4.sqlite')) as db:
        db.execute('INSERT OR REPLACE INTO read_requests VALUES(?,?,?,?,?)',(logical_key(identity),json.dumps(identity),
            'SUCCESS',json.dumps(value),json.dumps(dict(artifact_path=ref['path'],artifact_sha256=ref['sha256']))))
        db.commit()


class CurrentAdmission(unittest.TestCase):
    def case(self, root, *, header=True):
        f=Fixture(root,n=0)
        state=replace(f.state,arrival=replace(f.state.arrival,event_id='future',block=2,timestamp=1))
        resolver,labels=f.resolver();decision=resolver.resolve(state,f.scope,labels.resolve_state(state))
        self.assertEqual(decision['reason'],'TYPE_UNRESOLVED')
        co=f.write('derived/stage1d/queries/q1/collection.json',dict(query_id='q1',
            states=[dict(state=asdict(state),identity=labels.resolve_state(state),cost_boundary=decision)],
            cost_boundary_policy={'policy_sha256':f.policy_ref['sha256']}))
        co={**co,'path':'code/'+co['path']}
        plan={'method':'eth_getCode','params':[A,'0x2']}
        key=logical_key(Runtime().rpc_identity(PROVIDER,plan))
        gkey=digest(['eip155:1',A,2])
        member=dict(query_name='q1',query_id='q1',scope_hash=f.scope.scope_hash,current_row_index=0,
            state_key=state_key(state,f.scope),saved_decision_sha256=decision['decision_sha256'],
            collection_ref=co,current_identity=labels.resolve_state(state))
        need=dict(request=plan,logical_key=key,first_group_key=gkey,query_names=['q1'])
        sources={p.relative_to(f.work.parent).as_posix():sha(p) for p in
            [f.work/CURRENT_PATH,f.work/co['path'].removeprefix('code/'),*list((f.work/'derived/stage1d/labels').glob('*.json'))]}
        prep=dict(schema_version=SCHEMA,authorization_id=AUTH,source_and_current_input_sha256=sources,
            current_collection_refs={'q1':co},code_groups=[dict(group_key=gkey,chain_id='eip155:1',address=A,
                arrival_block=2,request=plan,states=[member],block_binding_status='EXACT_CURRENT_HEADER_CACHE',
                label_status={'online_label_status':'SUCCESS'})],next_batch={'requests':[need]})
        p=f.write('private/preparation.json',prep);pref={**p,'path':'code/'+p['path']}
        rpc(f,plan,'0x','future_code')
        if header:rpc(f,{'method':'eth_getBlockByNumber','params':['0x2',False]},
            {'number':'0x2','timestamp':'0x1','hash':BH,'transactions':[]},'future_header')
        op=f.write('private/operation.json',{'results':[{'needs':[need],'result':{'members':[cached_member(f.work,plan)]}}]})
        return f,state,pref,op

    def test_future_state_reaches_actual_registry_without_replacing_initial_or_ledgers(self):
        with tempfile.TemporaryDirectory() as tmp:
            f,state,pref,op=self.case(tmp)
            initial=sha(f.work/f.snapshot_ref['path']);cache=sha(f.work/'private/read_retry_r4.sqlite')
            with patch('stage1d_current_code_preparation.active_batch',return_value={'queries':[f.query]}):
                result=admit_operation(f.work,op,apply=True,current_preparation_ref=pref)
            self.assertEqual(result['gaps'],[]);self.assertEqual(len(result['affected_states']),1)
            resolver,labels=f.resolver();decision=resolver.resolve(state,f.scope,labels.resolve_state(state))
            self.assertEqual(decision['action'],'CONTINUE');self.assertEqual(decision['base_identity']['kind'],'UNKNOWN')
            self.assertEqual(initial,sha(f.work/f.snapshot_ref['path']))
            self.assertEqual(cache,sha(f.work/'private/read_retry_r4.sqlite'))

    def test_missing_header_does_not_adopt_or_assert_no_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            f,state,pref,op=self.case(tmp,header=False);before=sha(f.work/CURRENT_PATH)
            with patch('stage1d_current_code_preparation.active_batch',return_value={'queries':[f.query]}):
                result=admit_operation(f.work,op,apply=True,current_preparation_ref=pref)
            self.assertEqual(result['gaps'][0]['reason'],'EXACT_HEADER_BINDING_MISSING')
            self.assertEqual(result['admitted'],[]);self.assertEqual(before,sha(f.work/CURRENT_PATH))

    def test_changed_graph_and_new_label_are_rejected(self):
        for kind in ('graph','label'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as tmp:
                f,state,pref,op=self.case(tmp);before=sha(f.work/CURRENT_PATH)
                if kind=='graph':f.write('derived/stage1d/queries/q1/collection.json',{})
                else:f.write('derived/stage1d/labels/new.json',{'rows':[]})
                with patch('stage1d_current_code_preparation.active_batch',return_value={'queries':[f.query]}),self.assertRaises(ValueError):
                    admit_operation(f.work,op,apply=True,current_preparation_ref=pref)
                self.assertEqual(before,sha(f.work/CURRENT_PATH))

    def test_wrong_state_key_and_clock_owner_are_rejected(self):
        for defect in ('state','owner','selector'):
            with self.subTest(defect=defect),tempfile.TemporaryDirectory() as tmp:
                f,state,pref,op=self.case(tmp);doc=json.loads((f.work.parent/pref['path']).read_bytes())
                if defect=='state':doc['code_groups'][0]['states'][0]['state_key']='0'*64
                elif defect=='owner':doc['next_batch']['requests'][0]['query_names']=['q2']
                else:doc['next_batch']['requests'][0]['request']['params'][1]='0x3'
                ref=f.write('private/preparation.json',doc);pref={**ref,'path':'code/'+ref['path']}
                with patch('stage1d_current_code_preparation.active_batch',return_value={'queries':[f.query]}),self.assertRaises(ValueError):
                    validate(f.work,pref)

    def test_conflicting_header_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            f,state,pref,op=self.case(tmp);before=sha(f.work/CURRENT_PATH)
            rpc(f,{'method':'eth_getBlockByNumber','params':['0x2',False]},
                {'number':'0x2','timestamp':'0x1','hash':'0x'+'e'*64,'transactions':[]},'future_header')
            with patch('stage1d_current_code_preparation.active_batch',return_value={'queries':[f.query]}),self.assertRaises(ValueError):
                admit_operation(f.work,op,apply=True,current_preparation_ref=pref)
            self.assertEqual(before,sha(f.work/CURRENT_PATH))

    def test_existing_legacy_header_uses_raw_manifest_admission_and_cache(self):
        from test_legacy_binding_compat import LegacyBindingTests
        from stage1d_unknown_cost_registry import Registry
        LegacyBindingTests.setUpClass()
        for defect in (None,'raw','cache','http'):
            with self.subTest(defect=defect),tempfile.TemporaryDirectory() as tmp:
                f=Fixture(tmp,n=0)
                for name,raw in LegacyBindingTests.inputs.items():
                    dest=f.work/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(raw)
                row=LegacyBindingTests.records[0];plan=row['plan'];member=dict(row['receipt'])
                env=json.loads((f.work/member['artifact_path']).read_bytes())
                identity=Runtime().rpc_identity(PROVIDER,plan)
                if defect=='http':
                    env['http_status']=200;ref=f.write(member['artifact_path'],env);member['artifact_sha256']=ref['sha256']
                with closing(sqlite3.connect(f.work/'private/read_retry_r4.sqlite')) as db:
                    db.execute('INSERT INTO read_requests VALUES(?,?,?,?,?)',(logical_key(identity),json.dumps(identity),
                        'SUCCESS',json.dumps({} if defect=='cache' else env['response']['result']),json.dumps(member)))
                    db.commit()
                if defect=='raw':(f.work/env['raw_path']).write_bytes(b'{}')
                registry=Registry(f.work);ref={k:member['artifact_'+k] for k in ('path','sha256')}
                if defect is None:
                    request,response=registry._rpc(ref)
                    self.assertEqual(request['params'],plan['params']);self.assertEqual(response,env['response'])
                    self.assertEqual(len(registry._rpc_refs[(ref['path'],ref['sha256'])]),4)
                else:
                    with self.assertRaises(ValueError):registry._rpc(ref)


if __name__=='__main__':unittest.main()
