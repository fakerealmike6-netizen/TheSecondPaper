"""Post-repair contract. Run with current src/ and tests/ on PYTHONPATH.
Expected to fail on frozen interim baseline; synthetic only, no network.
"""
import copy
import unittest
import stage1d_context_online as online
import stage1d_closure_context as bridge
from test_stage1d_closure_context_family import native_fixture

class MergeExactnessContract(unittest.TestCase):
    def test_malformed_numeric_member_cannot_be_hidden_by_merge_order(self):
        for field,value in [('value_raw',10.75),('gas_used',3.5),('tx_index',0.75)]:
            for reverse in (False,True):
                with self.subTest(field=field,reverse=reverse):
                    q,c,l,m,_=native_fixture()
                    bad=copy.deepcopy(m['events'][0]);bad[field]=value
                    bad['evidence_ids']=['SYNTHETIC_CONTROLLED:bad-source']
                    left,right=([bad],m['events']) if reverse else (m['events'],[bad])
                    try:
                        merged=online._merge_rows(left,right)
                        mm=copy.deepcopy(m);mm['events']=merged
                        result=bridge.assemble(q,c,l,mm)
                    except ValueError:
                        continue  # Explicit exact-input rejection is acceptable.
                    report=result['context_result']
                    self.assertNotEqual(report['completion_status'],'FULL_CONTEXT_WITHIN_DECLARED_MODEL_SCOPE')
                    self.assertTrue(report.get('fact_conflicts') or report.get('evidence_gaps'),
                                    'Silent source selection is not explicit quarantine')

    def test_equal_decimal_hex_and_large_integer_keep_sources(self):
        _,_,_,m,_=native_fixture()
        for value in (0,10,2**80+7):
            a=copy.deepcopy(m['events'][0]);a['value_raw']=str(value)
            b=copy.deepcopy(a);b['value_raw']=hex(value);b['evidence_ids']=['SYNTHETIC_CONTROLLED:other']
            rows=online._merge_rows([a],[b])
            self.assertEqual(len(rows),1)
            self.assertEqual(set(rows[0]['evidence_ids']),set(a['evidence_ids']+b['evidence_ids']))

if __name__=='__main__':unittest.main()
