"""Accounting units and singleton LP aliases are independent of live data."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from pilot_metrics_r2 import amount_objectives, classify_fee_components, query_surfaces, uncovered_seconds


class PilotMetricsContracts(unittest.TestCase):
    def test_singleton_alias_preserves_joint_evidence_without_extra_solve(self):
        joint={'objective_events':['synthetic:event:1'],'lower_raw':'0','upper_raw':'17'}
        rows=amount_objectives({'joint':joint,'entry_intervals':{}})
        self.assertEqual([(r[0],r[1],r[3]) for r in rows],
                         [('ADDRESS_ASSET_JOINT',None,False),('ENTRY_EVENT','synthetic:event:1',True)])
        self.assertIs(rows[1][2],joint)

    def test_missing_multievent_interval_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'lack corresponding'):
            amount_objectives({'joint':{'objective_events':['a','b']},'entry_intervals':{'a':{}}})

    def test_overlapping_http_and_sessions_are_counted_once(self):
        start=datetime(2026,1,1,tzinfo=timezone.utc)
        seconds=lambda value:start+timedelta(seconds=value)
        self.assertEqual(uncovered_seconds(start,seconds(10),[(seconds(2),seconds(6)),(seconds(5),seconds(9))]),Decimal(3))
        self.assertEqual(uncovered_seconds(seconds(3),seconds(5),[(seconds(2),seconds(6))]),Decimal(0))

    def test_top_only_query_does_not_claim_internal_or_erc20(self):
        sql="-- stage1b-dune-index-adapter-1.0\n SELECT 'top' AS event_kind; SELECT * FROM indexed_events"
        self.assertEqual(query_surfaces(sql,'candidate'),{'top':True,'internal':False,'erc20':False})
        self.assertFalse(any(query_surfaces(sql,'context').values()))

    def test_lower_terminal_cost_retains_peak_risk_without_inflating_actual(self):
        result=classify_fee_components('1.8',None,'0.8',None,True,terminal_execution='0.3')
        self.assertEqual(result['known'],Decimal('0.3'))
        self.assertEqual(result['bounded'],Decimal('1'))
        self.assertEqual(result['discrepancy'],Decimal('0.5'))
        self.assertEqual(result['pending'],Decimal('0.5'))
        self.assertEqual(result['risk'],Decimal('1.8'))
        self.assertEqual(result['known']+result['bounded']+result['pending'],result['risk'])


if __name__=='__main__':unittest.main()
