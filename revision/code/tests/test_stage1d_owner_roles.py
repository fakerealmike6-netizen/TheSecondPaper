import unittest
from stage1d_owner_roles import missing_owners, owner_sql, validate_details, reclassify


class OwnerRoleRecoveryTests(unittest.TestCase):
    def observation(self):
        return {'observation_id': 'obs:old', 'address': '0x' + '1' * 40, 'platform': 'Dune',
                'source_version': 'labels.owner_addresses@old', 'actor': 'Lifi', 'actor_key': 'lifi',
                'role': 'UNKNOWN', 'raw_label': 'lifi', 'semantic_kind': 'CONTEXT',
                'source_metadata': {'table': 'labels.owner_addresses', 'raw_source_record': {
                    'owner_key': 'lifi', 'custody_owner': 'lifi', 'contract_name': 'LiFiDiamond_v2'}}}

    def test_missing_only_existing_observations(self):
        observation = self.observation()
        self.assertEqual(missing_owners([observation], {'owner_details_cache': {}}),
                         {'lifi': [observation['address']]})
        self.assertEqual(missing_owners([observation], {'owner_details_cache': {'lifi': {'name': 'LI.FI'}}}), {})

    def test_name_alone_never_service_or_protocol(self):
        _, added = reclassify([self.observation()], {'owner_details_cache': {}},
                              {'lifi': {'name': 'lifi', 'primary_category': None}}, {'sha256': 'a' * 64})
        self.assertEqual(added[0]['role'], 'UNKNOWN')

    def test_real_category_reclassifies_with_new_identity_and_preserves_original(self):
        observation = self.observation()
        rules, added = reclassify([observation], {'owner_details_cache': {}},
                                 {'lifi': {'name': 'lifi', 'primary_category': 'Bridge'}}, {'sha256': 'a' * 64})
        self.assertEqual(added[0]['role'], 'BRIDGE_BOUNDARY')
        self.assertNotEqual(added[0]['observation_id'], observation['observation_id'])
        self.assertEqual(observation['role'], 'UNKNOWN')
        self.assertEqual(added[0]['source_metadata']['prior_observation_id'], 'obs:old')

    def test_missing_duplicate_unrequested_and_unproved_result_rejected(self):
        for rows in ([], [{'owner_key': 'lifi', 'matched': False}] * 2,
                     [{'owner_key': 'other', 'matched': True}], [{'owner_key': 'lifi', 'matched': 'true'}]):
            with self.assertRaises(ValueError):
                validate_details(rows, ['lifi'])
        self.assertEqual(validate_details([{'owner_key': 'lifi', 'matched': False}], ['lifi']), {})

    def test_exact_finite_sql_schema_and_keys(self):
        columns = ('owner_key', 'name', 'primary_category', 'category_tags', 'website',
                   'project_documentation', 'project_github_url', 'description')
        schema = [{'table_schema': 'labels', 'table_name': 'owner_details', 'column_name': c,
                   'data_type': 'array(varchar)' if c == 'category_tags' else 'varchar'} for c in columns]
        sql = owner_sql(['lifi'], schema)
        self.assertIn('LEFT JOIN labels.owner_details', sql)
        self.assertNotIn('labels.addresses', sql)
        for keys in ([], ["bad'); DELETE"], ['a'] * 0):
            with self.assertRaises(ValueError):
                owner_sql(keys, schema)


if __name__ == '__main__':
    unittest.main()
