"""Deterministic public ZIP from the exact checked-out Git tree (ZIP_STORED).

No credentials are read here. Only this already public tree is archived.
The enclosing ZIP hash belongs in its external sidecar, never inside itself.
"""
from pathlib import Path
import argparse,hashlib,subprocess,zipfile

def archive(root,destination,paths):
    root=Path(root).resolve();destination=Path(destination).resolve()
    destination.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(destination,'x',compression=zipfile.ZIP_STORED) as z:
        for name in sorted(paths):
            p=(root/name).resolve()
            if not p.is_relative_to(root) or not p.is_file() or p.is_symlink():raise ValueError('Unsafe publication path')
            if name.startswith(('private/','raw/','derived/','baseline/')) or name.lower().endswith(('.sqlite','.zip')):
                raise ValueError('Private or nested archive path cannot be public')
            info=zipfile.ZipInfo(name,(2026,9,8,0,0,0));info.create_system=3
            info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_STORED
            z.writestr(info,p.read_bytes())
    digest=hashlib.sha256(destination.read_bytes()).hexdigest()
    destination.with_name(destination.name+'.sha256').write_bytes((digest+'  '+destination.name+'\n').encode())
    return digest

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);a=parser.parse_args()
    root=Path(__file__).resolve().parents[1]
    names=subprocess.check_output(['git','-C',str(root),'ls-files','-z']).decode().split('\0')
    print(archive(root,a.output,[p for p in names if p]))
if __name__=='__main__':main()
