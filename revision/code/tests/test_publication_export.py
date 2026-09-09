"""Synthetic tests of public export boundaries; never a real secret fixture."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from publication_export import SOURCE_NAMES, TEST_NAMES, export, scan_text

class PublicExportTests(unittest.TestCase):
    def synthetic_source(self, directory):
        work = directory / "stage"
        for folder, names in (("src", SOURCE_NAMES), ("tests", TEST_NAMES)):
            (work / folder).mkdir(parents=True)
            for name in names:
                (work / folder / name).write_text("# synthetic placeholder\n")
        (work / "configs").mkdir()
        (work / "configs/DUNE_ADAPTER_SCHEMA.md").write_text("Synthetic schema notice")
        keys = ("schema_version", "checkpoint", "daily_metasleuth_schedule_enabled", "label_policy", "budgets", "resource_limits", "sampling_rules", "query_pilots", "weth_component", "prototype")
        (work / "configs/STAGE1B_POLICY.json").write_text(json.dumps(dict.fromkeys(keys, {})))
        (work / "private").mkdir()
        (work / "private/not-public.json").write_text("Synthetic excluded payload")
        return work

    def test_explicit_task_required(self):
        with self.assertRaises(ValueError):
            export("unused", "unused", "Stage1C")

    def test_suspected_secret_never_echoed(self):
        fake = "ghp_" + "A" * 36
        findings = scan_text("synthetic.py", fake.encode())
        self.assertEqual([{"path": "synthetic.py", "rule": "KNOWN_TOKEN_SHAPE"}], findings)
        self.assertNotIn(fake, json.dumps(findings))
        self.assertEqual([], scan_text("client.py", b"value = os.environ['API_KEY']\n"))

    def test_default_deny_private_and_safe_regeneration(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            work = self.synthetic_source(folder)
            dest = folder / "public"
            first = export(work, dest, "Stage1B")
            second = export(work, dest, "Stage1B")
            self.assertEqual(first, second)
            self.assertFalse((dest / "private/not-public.json").exists())
            (dest / "README.md").write_text("Changed independently; preserve this")
            with self.assertRaises(ValueError):
                export(work, dest, "Stage1B")
            self.assertEqual("Changed independently; preserve this", (dest / "README.md").read_text())

    def test_unmanaged_destination_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            work = self.synthetic_source(folder)
            dest = folder / "public"
            dest.mkdir()
            (dest / "README.md").write_text("Unrelated repository")
            with self.assertRaises(ValueError):
                export(work, dest, "Stage1B")
            self.assertEqual("Unrelated repository", (dest / "README.md").read_text())

if __name__ == "__main__":
    unittest.main()
