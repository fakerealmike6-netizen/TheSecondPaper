from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

STAGE = Path(__file__).resolve().parents[1]
CODE = STAGE
sys.path[:0] = [str(STAGE), str(CODE / 'src')]
import stage1d_transfers_acquisition as a
from stage1d_alchemy_transfers import TransferPageChain, request_plan, TransfersRuntime
from page_attempts import atomic_json


class Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=STAGE)
        self.w = Path(self.temp.name)
        (self.w/'private').mkdir()
        from collector import Scope
        queries = []
        for name, depth in [('txphish_src001', 13), ('lifi_src001', 0)]:
            query = {'query_id': 'qry:synthetic:' + name, 'name': name,
                     'start_block': 100, 'end_block': 200,
                     'start_time_utc': '2023-11-14T22:13:20Z', 'end_time_utc': '2023-11-14T22:33:20Z',
                     'max_acquisition_depth': depth, 'window_mode': 'REFERENCE_FULL'}
            scope = Scope.from_policy(query)
            query.update(scope_id=scope.scope_id, scope_hash=scope.scope_hash)
            queries.append(query)
        frozen = self.w/'private/BATCH_QUERY_FREEZE.json'
        atomic_json(frozen, {'queries': queries})
        freezer = patch.object(a, 'FREEZE_SHA', a.sha(frozen))
        freezer.start()
        self.addCleanup(freezer.stop)
        self.q, self.scope = a.frozen_query(self.w, 'txphish_src001')
        self.addr, self.to = '0x'+'1'*40, '0x'+'2'*40
        self.txhash, self.blockhash = '0x'+'a'*64, '0x'+'b'*64
        self.need = {'query_id': self.q['query_id'], 'query_name': self.q['name'], 'scope_id': self.scope.scope_id,
            'scope_hash': self.scope.scope_hash, 'address': self.addr, 'asset': a.NATIVE, 'direction': 'OUTGOING',
            'start_block': self.scope.start_block, 'end_block': self.scope.end_block,
            'start_time': self.scope.start_time, 'end_time': self.scope.end_time,
            'fact_type': 'POSITIVE_NATIVE_CANDIDATE_INDEX', 'gap_reason': 'SYNTHETIC_TEST_ONLY'}
        self.counter = 0
        self.gate_refs=[]
        results = [{'canary_kind': kind, 'status': 'PASS'} for kind in
                   ('ordinary_top', 'internal', 'empty', 'pagination_or_boundary_duplicate')]
        gate_files = {'ROUTE_GATE.json': a.route_gate(results)}
        gate_files.update({r['canary_kind'] + '.result.json': r for r in results})
        for name, value in gate_files.items():
            target=self.w/'canaries'/name;target.parent.mkdir(exist_ok=True)
            atomic_json(target, value)
            self.gate_refs.append(a.ref(self.w,target))

    def tearDown(self):
        self.temp.cleanup()

    def member(self, plan, result):
        self.counter += 1
        path = self.w/'raw'/str(self.counter)
        path.mkdir(parents=True)
        request = dict(plan, id='fixture-'+str(self.counter), jsonrpc='2.0')
        response = {'id': request['id'], 'jsonrpc': '2.0', 'result': result}
        body = json.dumps([response]).encode()
        (path/'response_body.bin').write_bytes(body)
        envelope = {'provider_alias': a.PROVIDER, 'evidence_kind': 'REAL_CHAIN', 'http_status': 200,
            'request': request, 'response': response, 'response_complete': True, 'status': 'SUCCESS_VALIDATED',
            'raw_body_sha256': hashlib.sha256(body).hexdigest()}
        atomic_json(path/'envelope.json', envelope)
        return {'artifact_path': a.ref(self.w,path/'envelope.json')['path'], 'artifact_sha256': a.sha(path/'envelope.json'),
                'result': result, 'cache_hit': False, 'status': 'SUCCESS_VALIDATED'}

    def row(self, category='external'):
        return {'blockNum': hex(self.scope.start_block), 'uniqueId': 'fixture-index-1', 'hash': self.txhash,
            'from': self.addr, 'to': self.to, 'category': category, 'asset': 'ETH',
            'rawContract': {'address': None, 'decimal': '0x12', 'value': '0x00000001'},
            'metadata': {'blockTimestamp': datetime.fromtimestamp(self.scope.start_time, timezone.utc).isoformat()},
            'value': 999999.123}  # Deliberately irrelevant rounded display value.

    def state(self, rows, *, pagekey=None):
        chain = TransferPageChain(self.need)
        plan = request_plan(self.need)
        result = {'transfers': rows}
        if pagekey: result['pageKey'] = pagekey
        chain.append(plan, self.member(plan,result), received_at_seconds=1000, requested_at_seconds=999)
        return {'version': a.VERSION, 'need': self.need, 'pages': chain.summary()['pages'], 'bindings': [],
                'route_dependencies': self.gate_refs, 'plan_sources': [], 'status': 'SYNTHETIC_TEST_ONLY', 'failures': []}

    def bind_top(self, state):
        tx = {'hash': self.txhash, 'blockHash': self.blockhash, 'blockNumber': hex(self.scope.start_block),
              'transactionIndex': '0x0', 'from': self.addr, 'to': self.to, 'value': '0x1'}
        receipt = {'transactionHash': self.txhash, 'blockHash': self.blockhash, 'blockNumber': hex(self.scope.start_block),
            'transactionIndex': '0x0', 'from': self.addr, 'to': self.to, 'status': '0x1',
            'gasUsed': '0x5208', 'effectiveGasPrice': '0x2'}
        header = {'hash': self.blockhash, 'number': hex(self.scope.start_block), 'timestamp': hex(self.scope.start_time),
                  'transactions': [self.txhash]}
        values = {'eth_getTransactionByHash': tx, 'eth_getTransactionReceipt': receipt, 'eth_getBlockByNumber': header}
        for plan in a.binding_plans(a.restore_chain(self.w,state['need'],state['pages'])):
            state['bindings'].append({'plan': plan, 'member': self.member(plan,values[plan['method']])})

    def test_raw_body_round_trip(self):
        plan = request_plan(self.need); m = self.member(plan, {'transfers': []})
        self.assertEqual(a.verified_member(self.w,plan,m), {'transfers': []})

    def test_modified_raw_body_refused(self):
        plan = request_plan(self.need); m = self.member(plan, {'transfers': []})
        (self.w/m['artifact_path']).parent.joinpath('response_body.bin').write_bytes(b'[]')
        with self.assertRaisesRegex(ValueError,'CACHE_RAW_PROOF'): a.verified_member(self.w,plan,m)

    def test_success_boolean_without_raw_refused(self):
        plan = request_plan(self.need); m = self.member(plan, {'transfers': []})
        (self.w/m['artifact_path']).parent.joinpath('response_body.bin').unlink()
        with self.assertRaisesRegex(ValueError,'CACHE_RAW_PROOF'): a.verified_member(self.w,plan,m)

    def test_wrong_request_binding_refused(self):
        plan = request_plan(self.need); m = self.member(plan, {'transfers': []})
        other = deepcopy(plan); other['params'][0]['fromAddress'] = self.to
        with self.assertRaises(ValueError): a.verified_member(self.w,other,m)

    def test_empty_natural_exhaustion_closes_index_only(self):
        result = a.normalized_state(self.w,self.state([]))
        self.assertTrue(result['positive_native_index_complete']); self.assertFalse(result['context_complete'])

    def test_external_exact_binding_and_fee(self):
        state = self.state([self.row()]); self.bind_top(state)
        result = a.normalized_state(self.w,state)
        self.assertTrue(result['positive_native_index_complete'])
        self.assertEqual(result['events'][0]['amount_raw'],1); self.assertEqual(result['events'][0]['gas_raw'],42000)

    def test_external_missing_receipt_remains_partial(self):
        state = self.state([self.row()]); self.bind_top(state)
        state['bindings'] = [b for b in state['bindings'] if b['plan']['method']!='eth_getTransactionReceipt']
        result = a.normalized_state(self.w,state)
        self.assertFalse(result['positive_native_index_complete']); self.assertEqual(result['events'],[])

    def test_internal_has_gap_and_no_guessed_trace(self):
        result = a.normalized_state(self.w,self.state([self.row('internal')]))
        self.assertFalse(result['positive_native_index_complete']); self.assertEqual(result['events'],[])
        self.assertEqual(result['gaps'][0]['fallback'],'CLASSIC_BIGQUERY_OR_VALIDATED_DUNE_NATIVE')

    def test_partial_page_chain_cannot_close_empty_index(self):
        result = a.normalized_state(self.w,self.state([],pagekey='cursor'))
        self.assertFalse(result['positive_native_index_complete'])

    def test_expired_cursor_requires_saved_overlap_proposal(self):
        state=self.state([self.row()],pagekey='cursor')
        chain=a.restore_chain(self.w,self.need,state['pages'])
        with self.assertRaisesRegex(ValueError,'PAGE_KEY_EXPIRED'): chain.next_plan(1600)
        proposal=chain.recovery_range()
        self.assertTrue(proposal['requires_persisted_recovery_decision'])
        self.assertEqual(proposal['needed_range']['start_block'],self.scope.start_block)

    def test_binding_requests_deduplicate_tx_and_block(self):
        state=self.state([self.row()])
        chain=a.restore_chain(self.w,self.need,state['pages']);chain.rows.append(deepcopy(chain.rows[0]))
        plans=a.binding_plans(chain)
        self.assertEqual(len(plans),3); self.assertEqual(len({a.digest(p) for p in plans}),3)

    def test_select_depth_arrival_not_amount(self):
        needs=[];frontier=[]
        for idx,depth in ((3,4),(2,2),(1,2)):
            need=dict(self.need,address='0x'+str(idx)*40);needs.append(need)
            frontier.append({'reason':'INTERVAL_INCOMPLETE','state':{'depth':depth,'address':need['address'],
                'asset':a.NATIVE,'protocol_context':'ordinary','arrival':{'block':self.scope.start_block,
                'timestamp':self.scope.start_time,'tx_index':idx,'event_id':str(idx),'amount_raw':10**idx}}})
        selected=a.select_needs(self.q,{'needed_ranges':needs,'frontier':frontier})
        self.assertEqual([n['address'] for n in selected],[needs[2]['address'],needs[1]['address'],needs[0]['address']])

    def test_max20_and_lifi_stopped(self):
        with self.assertRaises(ValueError): a.select_needs(self.q,{},21)
        with self.assertRaisesRegex(ValueError,'LI.FI'): a.frozen_query(self.w,'lifi_src001')

    def test_cache_record_rederives_raw_and_refuses_fake_complete(self):
        result=a.emit_interval(self.w,self.state([self.row('internal')]))
        record=a.read(self.w/result['coverage_record']['path'])
        self.assertFalse(record['complete']); a.verify_interval_record(self.w,record)
        record['complete']=True;record['native_scope_complete']=True
        with self.assertRaises(ValueError): a.verify_interval_record(self.w,record)

    def test_cachedintervals_new_validator_and_provider(self):
        a.emit_interval(self.w,self.state([]))
        from stage1d_acquisition import CachedIntervals
        provider=CachedIntervals(self.w);provider.bind_scope(self.scope)
        result=provider.fetch_interval(self.addr,a.NATIVE,self.scope.start_block,self.scope.end_block,
            start_time=self.scope.start_time,end_time=self.scope.end_time,global_end_time=self.scope.end_time)
        self.assertTrue(result.complete);self.assertEqual(result.coverage[0]['provider'],'Alchemy')

    def test_import_has_no_network(self):
        self.assertTrue(callable(a.acquire))

    def test_existing_cached_ordinary_envelope_contract_retained(self):
        state=self.state([self.row()]);self.bind_top(state)
        binding=state['bindings'][0];member=dict(binding['member'],cache_hit=True)
        (self.w/member['artifact_path']).parent.joinpath('response_body.bin').unlink()
        self.assertEqual(a.verified_member(self.w,binding['plan'],member)['hash'],self.txhash)
        member['cache_hit']=False
        with self.assertRaisesRegex(ValueError,'CACHE_RAW_PROOF'):a.verified_member(self.w,binding['plan'],member)

    def test_cached_transfers_page_still_requires_original_body(self):
        plan=request_plan(self.need);member=self.member(plan,{'transfers':[]});member['cache_hit']=True
        (self.w/member['artifact_path']).parent.joinpath('response_body.bin').unlink()
        with self.assertRaisesRegex(ValueError,'CACHE_RAW_PROOF'):a.verified_member(self.w,plan,member)

    def test_serial_actual_entry_and_resume_with_synthetic_access(self):
        state=self.state([self.row()]);self.bind_top(state)
        page_member={'artifact_path':state['pages'][0]['artifact_path'],'artifact_sha256':state['pages'][0]['artifact_sha256'],
            'result':{'transfers':[self.row()]},'status':'SUCCESS_VALIDATED','cache_hit':False}
        members={a.digest(request_plan(self.need)):page_member}
        members.update({a.digest(x['plan']):x['member'] for x in state['bindings']})
        endpoint_plan={'method':'eth_getBlockByNumber','params':[hex(self.scope.end_block),False]}
        members[a.digest(endpoint_plan)]=self.member(endpoint_plan,{'number':hex(self.scope.end_block),
            'timestamp':hex(self.scope.end_time),'hash':'0x'+'c'*64,'transactions':[]})
        test=self
        class FakeAccess:
            runtime=TransfersRuntime()
            w=test.w
            calls=[]
            def call_batch(self,plans,*args,**kwargs):
                test.assertEqual(len(plans),1);self.calls.append(plans[0])
                return {'status':'COMPLETE','members':[members[a.digest(plans[0])]]}
        arrival={'block':self.scope.start_block,'timestamp':self.scope.start_time,'tx_index':0,'event_id':'fixture-arrival'}
        frontier={'reason':'INTERVAL_INCOMPLETE','state':{'address':self.addr,'asset':a.NATIVE,'depth':1,
            'protocol_context':'ordinary','arrival':arrival}}
        boundary={'queries':[{'query_name':self.q['name'],'needed_ranges':[self.need],'frontier':[frontier]}]}
        atomic_json(self.w/'boundary.json',boundary)
        plan={'version':a.VERSION,'query_name':self.q['name'],'needs':[self.need],'source_needs':[self.need],
            'maximum_needs':20,'boundary_snapshot':a.ref(self.w,'boundary.json'),'route_dependencies':self.gate_refs}
        atomic_json(self.w/'plan.json',plan)
        access=FakeAccess()
        result=a.acquire(self.w,'plan.json',access,clock=lambda:1000)
        self.assertEqual(result['results'][0]['status'],'NATIVE_CANDIDATE_INDEX_COMPLETE')
        self.assertEqual(len(access.calls),5)
        again=a.acquire(self.w,'plan.json',access,clock=lambda:1000)
        self.assertEqual(again['results'][0]['status'],'CURRENT_OR_ADMITTED_INTERVAL_CACHE_COMPLETE')
        self.assertEqual(len(access.calls),5)


if __name__=='__main__':unittest.main()
