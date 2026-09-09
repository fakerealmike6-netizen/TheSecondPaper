"""Synthetic exact-key/current-demand/manifest audit tests; no raw or DB access."""
import copy
import unittest
from types import SimpleNamespace
import root_tx2_legacy_points as runner
from stage1d_legacy_rpc_import import exact_request_sha

class Tx2LegacyDriverTests(unittest.TestCase):
    def fixture(self):
        plan={'method':'eth_getBlockByNumber','params':['0xa',False]}
        plain=runner.digest(plan);legacy=exact_request_sha(plan)
        need={**plan,'query_id':'q2','scope_id':'s2','scope_hash':'h2','expected_block':10,
              'reason':'CURRENT_POINT','evidence_refs':[{'path':'big-material','sha256':'b'*64}]}
        prep_ref={'path':'prep','sha256':'a'*64}
        prep={'active_batch':{'path':'batch','sha256':'c'*64},'queries':{runner.QUERY:{'inputs':{'collection':{'path':'collection','sha256':'d'*64}},
              'point_requests':[{'request_sha256':plain,'request':plan,'current_cache':{'status':'NO_CURRENT_EXACT_REQUEST'},'legacy_need_for_root_only':need}]}}}
        estimate={'rows':[{'status':runner.SINGLE,'request_sha256':plain,'request':plan,'legacy_exact_request_sha256':legacy,
                          'response_sha256_variants':['f'*64],'expected_block':10}]}
        return prep,estimate,prep_ref

    def test_chain_bound_legacy_hash_differs_but_current_request_is_accepted(self):
        prep,est,ref=self.fixture();row=est['rows'][0]
        self.assertNotEqual(row['request_sha256'],row['legacy_exact_request_sha256'])
        selected=runner.selected_needs(prep,est,ref);self.assertEqual(len(selected),1)
        need=selected[0]['need'];self.assertEqual(need['method'],'eth_getBlockByNumber');self.assertEqual(need['params'],['0xa',False])
        self.assertEqual(need['expected_block'],10);self.assertEqual(need['query_id'],'q2')
        self.assertIn(ref,need['evidence_refs']);self.assertEqual(need['original_context_evidence_refs'],[{'path':'big-material','sha256':'b'*64}])

    def test_wrong_parameters_and_multiple_variants_cannot_enter_single_candidate(self):
        prep,est,ref=self.fixture();est['rows'][0]['request']={'method':'eth_getBlockByNumber','params':['0xb',False]}
        with self.assertRaises(ValueError):runner.selected_needs(prep,est,ref)
        prep,est,ref=self.fixture();est['rows'][0]['response_sha256_variants'].append('0'*64)
        with self.assertRaises(ValueError):runner.selected_needs(prep,est,ref)

    def test_non_single_candidates_stay_out_and_current_success_is_not_read(self):
        prep,est,ref=self.fixture();other=copy.deepcopy(est['rows'][0]);other['status']='LEGACY_RESPONSE_VARIANTS_REQUIRE_QUARANTINE';est['rows'].append(other)
        selected=runner.selected_needs(prep,est,ref);self.assertEqual(len(selected),1)
        calls=[]
        class Importer:
            def prepare_one(self,query,need):calls.append(need);return {'status':'CURRENT_SUCCESS_REUSED_LEGACY_NOT_READ','current_receipt':{'new_network_requests':0}}
        rows=runner.audit_points(Importer(),{'query_id':'q2'},selected)
        self.assertEqual(len(calls),1);self.assertEqual(rows[0]['status'],'CURRENT_SUCCESS_REUSED_LEGACY_NOT_READ')
        self.assertNotIn('legacy_source',rows[0])

    def test_exact_apply_manifest_refuses_changed_need_or_raw(self):
        prep,est,ref=self.fixture();selected=runner.selected_needs(prep,est,ref);candidate=selected[0]
        row={**candidate,'status':'ADMISSIBLE_POINT_PENDING_ROOT_APPLY','need_sha256':runner.digest(candidate['need']),
             'legacy_source':{'raw_sha256':'f'*64}}
        audit={'mode':'AUDIT_ONLY','status':'LEGACY_POINT_AUDIT_COMPLETE','rows':[row]}
        self.assertEqual(runner.exact_apply_needs(audit,selected),[candidate['need']])
        bad=copy.deepcopy(audit);bad['rows'][0]['need']['expected_block']=11
        with self.assertRaises(ValueError):runner.exact_apply_needs(bad,selected)
        bad=copy.deepcopy(audit);bad['rows'][0]['legacy_source']['raw_sha256']='0'*64
        with self.assertRaises(ValueError):runner.exact_apply_needs(bad,selected)

if __name__=='__main__':unittest.main()
