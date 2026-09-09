import tempfile
import unittest
from pathlib import Path
from package_review_r3 import freeze_payloads,write_manifest,verify_equivalence,verify_manifest,deterministic_zip,write_sidecar,validate_public_tree


class R3PublicationDAG(unittest.TestCase):
    def test_late_receipt_is_explicitly_outside_payload_manifest(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);m=root/'min';p=root/'public';m.mkdir();p.mkdir()
            for tree in (m,p):
                (tree/'00_REVIEW_INDEX.md').write_text('Fixed Stage1B-R3 review, PENDING_REVIEW')
            freeze_payloads(m,p);write_manifest(m);write_manifest(p)
            (m/'PUBLICATION_RECEIPT.json').write_text('{"status":"LOCAL_ONLY"}')
            self.assertTrue(verify_equivalence(m,p)['passed']);self.assertTrue(verify_manifest(m)['passed'])
            z=root/'Stage1B_R3_Review_Bundle_MIN.zip';result=deterministic_zip(m,z)
            self.assertEqual(result['sha256'],write_sidecar(z)['archive_sha256'])
    def test_changed_report_cannot_reuse_stale_mapping(self):
        with tempfile.TemporaryDirectory() as d:
            m=Path(d)/'min';p=Path(d)/'public';m.mkdir();p.mkdir()
            for tree in (m,p):(tree/'00_REVIEW_INDEX.md').write_text('initial')
            freeze_payloads(m,p);write_manifest(m);write_manifest(p)
            (p/'00_REVIEW_INDEX.md').write_text('changed')
            with self.assertRaises(ValueError):verify_equivalence(m,p)
            with self.assertRaises(ValueError):verify_manifest(p)
    def test_new_report_allowed_private_input_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);(p/'04_CONTEXT_ACQUISITION_AND_RECONCILIATION.md').write_text('aggregate context summary')
            self.assertEqual(len(validate_public_tree(p)),1)
            (p/'private').mkdir();(p/'private/account.json').write_text('{}')
            with self.assertRaises(ValueError):validate_public_tree(p)
