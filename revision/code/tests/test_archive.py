import tempfile,unittest,zipfile,stat
from pathlib import Path
from archive_safety import inspect_zip

class ArchiveTests(unittest.TestCase):
    def test_reject_unsafe_entries(self):
        for name in ('../x','/absolute','C:/x','a/../../b','NUL.txt','x:stream','x. '):
            with tempfile.TemporaryDirectory() as d:
                p=Path(d)/'a.zip'
                with zipfile.ZipFile(p,'w') as z:z.writestr(name,b'data')
                with self.assertRaises(ValueError):inspect_zip(p)
    def test_duplicate_case_and_symlink(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'a.zip'
            with zipfile.ZipFile(p,'w') as z:z.writestr('One',b'1');z.writestr('one',b'2')
            with self.assertRaises(ValueError):inspect_zip(p)
            info=zipfile.ZipInfo('link');info.create_system=3;info.external_attr=(stat.S_IFLNK|0o777)<<16
            with zipfile.ZipFile(p,'w') as z:z.writestr(info,b'target')
            with self.assertRaises(ValueError):inspect_zip(p)

if __name__=='__main__': unittest.main()
