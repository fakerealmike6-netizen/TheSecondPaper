import unittest
from reference_delta_r2 import affected_queries,delta_rows

class ReferenceDeltaR2Tests(unittest.TestCase):
    def test_changed_intermediate_affects_incident_queries_not_unrelated_queries(self):
        changed={'service'};events=[{'event_id':'e1','from_address':'seed','to_address':'service'},{'event_id':'e2','from_address':'unrelated','to_address':'other'}]
        members=[{'incident_id':'i1','event_id':'e1'},{'incident_id':'i2','event_id':'e2'}]
        queries=[{'query_id':'q1','incident_id':'i1'},{'query_id':'q2','incident_id':'i1'},{'query_id':'q3','incident_id':'i2'}]
        affected,ids,incidents=affected_queries(changed,events,members,queries,[])
        self.assertEqual(affected,{'q1','q2'});self.assertEqual(ids,{'e1'});self.assertEqual(incidents,{'i1'})
    def test_changed_registered_target_affects_query_even_without_matching_event(self):
        affected,_,_=affected_queries({'target'},[],[],[],[{'query_id':'q','target_address':'target'}])
        self.assertEqual(affected,{'q'})
    def test_first_service_delta_marks_B_and_C_loss_without_rekeying_relation(self):
        old={'old_relation_id':'rel:x','query_id':'q','target_address':'downstream','actor':'Exchange','target_identity_class':'SERVICE','task_reference_status':'VERIFIED_TASK_REFERENCE','change_reason':'STRICT_POSITIVE_SAME_ASSET_FIRST_IDENTIFIED_SERVICE','collection_90d_status':'WITHIN_PER_ARRIVAL_90D'}
        new=old|{'task_reference_status':'UNRESOLVED','change_reason':'KNOWN_FIRST_SERVICE_OR_BOUNDARY_BLOCKS_AVAILABLE_CERTIFICATES','collection_90d_status':'NOT_APPLICABLE_TO_UNRESOLVED_OR_EXCLUDED_B'}
        row=delta_rows([old],[new])[0]
        self.assertTrue(row['old_B'] and row['old_C']);self.assertFalse(row['new_B'] or row['new_C']);self.assertTrue(row['changed'])
        with self.assertRaises(ValueError):delta_rows([old],[new|{'old_relation_id':'rel:rekeyed'}])
if __name__=='__main__':unittest.main()
