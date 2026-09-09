"""Synthetic R2 packaging DAG and public boundary tests; no real publication."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from package_review_r2 import (EQUIVALENCE, MANIFEST, RECEIPT, GENERATED, freeze_payloads,
                               verify_equivalence, write_manifest, verify_manifest,
                               deterministic_zip, write_sidecar, git_tree_sha1,
                               validate_public_tree, safe_relative)


class PackageReviewR2Tests(unittest.TestCase):
    def trees(self, root):
        private, public = root / 'min_tree', root / 'public_tree'
        for tree in (private, public):
            (tree / 'src').mkdir(parents=True)
            (tree / 'src/example.py').write_text('value = 1\n', encoding='utf-8')
            (tree / 'README.md').write_text('Synthetic test package.\n', encoding='utf-8')
        (private / 'derived').mkdir()
        (private / 'derived/local_evidence.json').write_text('{"synthetic":true}\n', encoding='utf-8')
        return private, public

    def test_dag_excludes_late_generated_objects_and_binds_receipt_in_min_zip(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            private, public = self.trees(root)
            mapping = freeze_payloads(private, public)
            self.assertEqual(mapping['local_only_payload_count'], 1)
            self.assertNotIn('derived/local_evidence.json', json.dumps(mapping))
            self.assertEqual({row['path'] for row in mapping['excluded_generated']}, set(GENERATED))
            self.assertTrue(all(not any('sha256' in key for key in row) for row in mapping['excluded_generated']))
            write_manifest(private)
            write_manifest(public)
            public_receipt = deterministic_zip(public, root / 'public.zip', public=True)
            old_manifest = (private / MANIFEST).read_bytes()
            (private / RECEIPT).write_text(json.dumps({'public': public_receipt, 'remote': 'NOT_PUBLISHED'}), encoding='utf-8')
            self.assertTrue(verify_equivalence(private, public)['passed'])
            self.assertTrue(verify_manifest(private)['passed'])
            self.assertEqual((private / MANIFEST).read_bytes(), old_manifest)
            result = deterministic_zip(private, root / 'min.zip')
            sidecar = write_sidecar(root / 'min.zip')
            self.assertEqual(result['sha256'], sidecar['archive_sha256'])
            with zipfile.ZipFile(root / 'min.zip') as archive:
                self.assertEqual(archive.read(RECEIPT), (private / RECEIPT).read_bytes())

    def test_different_payload_requires_reason_and_rechecks_each_byte(self):
        with tempfile.TemporaryDirectory() as tmp:
            private, public = self.trees(Path(tmp))
            (public / 'README.md').write_text('Sanitized synthetic report.\n', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'documented reason'):
                freeze_payloads(private, public)
            mapping = freeze_payloads(private, public, {'README.md': {'reason': 'Remove synthetic local detail.'}})
            row = next(row for row in mapping['payload_rows'] if row['public_path'] == 'README.md')
            self.assertEqual(row['transformation'], 'DOCUMENTED_SANITIZATION_DIFFERENCE')
            self.assertNotEqual(row['local_sha256'], row['public_sha256'])
            (private / 'README.md').write_text('Changed after freeze.\n', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'stale'):
                verify_equivalence(private, public)

    def test_unexplained_public_only_and_added_payload_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            private, public = self.trees(Path(tmp))
            (public / 'NOTICE.md').write_text('Synthetic public notice.\n', encoding='utf-8')
            with self.assertRaises(ValueError):
                freeze_payloads(private, public)
            freeze_payloads(private, public, {'NOTICE.md': {'local_path': None, 'reason': 'Public reuse notice.'}})
            (public / 'src/added.py').write_text('pass\n', encoding='utf-8')
            with self.assertRaises(ValueError):
                verify_equivalence(private, public)

    def test_private_only_payload_change_breaks_aggregate_commitment(self):
        with tempfile.TemporaryDirectory() as tmp:
            private, public = self.trees(Path(tmp))
            freeze_payloads(private, public)
            (private / 'derived/local_evidence.json').write_text('{"synthetic":false}\n', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'stale'):
                verify_equivalence(private, public)

    def test_frozen_metadata_cannot_be_silently_regenerated(self):
        with tempfile.TemporaryDirectory() as tmp:
            private, public = self.trees(Path(tmp))
            first = freeze_payloads(private, public)
            self.assertEqual(first, freeze_payloads(private, public))
            write_manifest(private)
            (private / 'src/example.py').write_text('value = 2\n', encoding='utf-8')
            (public / 'src/example.py').write_text('value = 2\n', encoding='utf-8')
            with self.assertRaises(FileExistsError):
                freeze_payloads(private, public)
            with self.assertRaises(FileExistsError):
                write_manifest(private)

    def test_manifest_checks_membership_duplicates_and_modified_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            private, public = self.trees(Path(tmp))
            freeze_payloads(private, public)
            write_manifest(private)
            manifest = private / MANIFEST
            original = manifest.read_text(encoding='utf-8')
            line = next(line for line in original.splitlines() if not line.startswith('#'))
            manifest.write_text(original + line + '\n', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'Duplicate'):
                verify_manifest(private)
            manifest.write_text(original, encoding='utf-8')
            (private / 'extra.txt').write_text('unlisted', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'exactly'):
                verify_manifest(private)
            (private / 'extra.txt').unlink()
            (private / 'README.md').write_text('modified', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'hash mismatch'):
                verify_manifest(private)

    def test_public_allowlist_denies_private_data_full_policy_and_receipt(self):
        for name in ('private/ledger.json', 'raw/page.json', 'derived/events.json',
                     'configs/STAGE1B_R2_POLICY.json', RECEIPT):
            with self.subTest(path=name), tempfile.TemporaryDirectory() as tmp:
                _, public = self.trees(Path(tmp))
                path = public / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{}\n', encoding='utf-8')
                with self.assertRaisesRegex(ValueError, 'not_allowlisted'):
                    validate_public_tree(public)

    def test_public_scan_reports_rule_without_echoing_synthetic_secret(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, public = self.trees(Path(tmp))
            fake = 'ghp_' + 'A' * 36
            (public / 'README.md').write_text(fake, encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'KNOWN_TOKEN_SHAPE') as caught:
                validate_public_tree(public)
            self.assertNotIn(fake, str(caught.exception))

    def test_zip_reproducibility_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            private, public = self.trees(root)
            freeze_payloads(private, public)
            write_manifest(public)
            first = deterministic_zip(public, root / 'a.zip', public=True)
            second = deterministic_zip(public, root / 'b.zip', public=True)
            self.assertEqual(first['sha256'], second['sha256'])
            self.assertEqual((root / 'a.zip').read_bytes(), (root / 'b.zip').read_bytes())
            with self.assertRaises(FileExistsError):
                deterministic_zip(public, root / 'a.zip', public=True)
            with self.assertRaises(ValueError):
                deterministic_zip(public, public / 'nested.zip', public=True)

    def test_git_tree_matches_independently_encoded_single_directory_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'src').mkdir()
            data = b'pass\n'
            (root / 'src/example.py').write_bytes(data)
            def git_object(kind, value):
                return hashlib.sha1(kind + b' ' + str(len(value)).encode() + b'\0' + value).digest()
            blob = git_object(b'blob', data)
            subtree = git_object(b'tree', b'100644 example.py\0' + blob)
            expected = git_object(b'tree', b'40000 src\0' + subtree).hex()
            self.assertEqual(git_tree_sha1(root), expected)

    def test_path_traversal_and_windows_aliases_rejected(self):
        for name in ('../x', 'a/../x', '/x', 'a//x', 'a\\x', 'x.', 'NUL.txt', 'a/COM1', 'C' + ':/x'):
            with self.subTest(path=name), self.assertRaises(ValueError):
                safe_relative(name)


if __name__ == '__main__':
    unittest.main()
