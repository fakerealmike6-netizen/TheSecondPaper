import unittest
from collector import CollectionResult
from stage1d_context import _collection, _needs_multiasset
from stage1d_cost_boundary_context import validate_overlay
from context_ledger_r3 import EvidenceConflict

class LegacyCostCompatibility(unittest.TestCase):
    def test_no_policy_dataclass_is_not_an_empty_active_policy(self):
        value = CollectionResult('q', 'TEST', [], [], [], [], [], [], [], {})
        self.assertIsNone(_collection(value)['cost_boundary_policy'])
        self.assertIsNone(validate_overlay({}, _collection(value)))
        self.assertNotIn('cost_boundary_policy', value.to_dict())

    def test_explicit_empty_policy_still_rejected(self):
        with self.assertRaises(EvidenceConflict):
            validate_overlay({}, {'cost_boundary_policy': {}})

    def test_native_address_only_role_record_remains_legacy(self):
        self.assertFalse(_needs_multiasset({'states': [{'state': {'address': '0x'+'1'*40}}]}))

    def test_explicit_unsupported_state_or_pending_arrival_rejected(self):
        for c in ({'states': [{'state': {'asset': 'UNKNOWN'}}]},
                  {'unresolved_frontier': [{'state': {'asset': 'native:eip155:1', 'arrival': {'asset': 'UNKNOWN'}}}]}):
            with self.subTest(c=c), self.assertRaises(ValueError):
                _needs_multiasset(c)
