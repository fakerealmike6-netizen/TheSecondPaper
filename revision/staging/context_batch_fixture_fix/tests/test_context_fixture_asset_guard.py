"""Synthetic native fixture contract; unknown/missing assets still reject."""
import unittest
from stage1d_multiasset_context import validate_collection_asset_domain
import test_stage1d_context_online as support

class FixtureAssetGuardTests(unittest.TestCase):
    def test_native_parent_fixture_has_explicit_state_assets(self):
        instance=support.ContextOnlineTests('test_unsafe_output_path_rejected_before_network')
        instance.setUp()
        try:
            self.assertTrue(all(r['state']['asset']=='native:eip155:1' for r in instance.c['states']))
            validate_collection_asset_domain(instance.c,instance.q)
            self.assertEqual(instance.calls,[])
        finally:instance.doCleanups()
    def test_missing_state_asset_is_not_assumed_native(self):
        with self.assertRaises(KeyError):
            validate_collection_asset_domain({'states':[{'state':{'address':'0x'+'1'*40}}]})
    def test_unknown_state_asset_still_rejects(self):
        for value in ('WETH','erc20:eip155:1:0x'+'1'*40,'native:eip155:2',None):
            with self.subTest(asset=value),self.assertRaises(ValueError):
                validate_collection_asset_domain({'states':[{'state':{'address':'0x'+'1'*40,'asset':value}}]})
