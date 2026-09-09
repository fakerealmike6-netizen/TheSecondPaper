"""Final immutable dual archives and safe fresh extraction for offline execution."""
from pathlib import Path
import argparse,hashlib,importlib.util,json,sys,zipfile
R=Path(__file__).resolve().parent;ROOT=R.parents[3]
OUT=ROOT/'04_deliverables/stage1C/revisions'/R.name
sys.path.insert(0,str(R/'code/src'))
from archive_safety import extract_new,verify_manifest
from run_stage1c import write

def git_tree_sha1(root):
    def oid(kind,data):return hashlib.sha1(kind.encode()+b' '+str(len(data)).encode()+b'\0'+data).digest()
    def walk(folder):
        parts=[]
        for p in sorted(folder.iterdir(),key=lambda p:(p.name+('/' if p.is_dir() else '')).encode()):
            mode=b'40000' if p.is_dir() else b'100644'
            value=walk(p) if p.is_dir() else oid('blob',p.read_bytes())
            parts.append(mode+b' '+p.name.encode()+b'\0'+value)
        return oid('tree',b''.join(parts))
    return walk(root).hex()

def main():
    p=argparse.ArgumentParser();p.add_argument('--prepared',default='prepared_final');a=p.parse_args()
    spec=importlib.util.spec_from_file_location('release',R/a.prepared/'public/tools/stage1c_release.py')
    release=importlib.util.module_from_spec(spec);spec.loader.exec_module(release)
    OUT.mkdir(parents=True,exist_ok=True);identities={}
    for kind,name in [('public','Stage1C_R1_Public_Review_Bundle.zip'),('min','Stage1C_R1_Review_Bundle_MIN.zip')]:
        tree=R/a.prepared/kind;dest=OUT/name
        paths=sorted(p.relative_to(tree).as_posix() for p in tree.rglob('*') if p.is_file())
        count=verify_manifest(tree/'12_FILE_HASHES.txt')
        if kind=='public':digest=release.archive(tree,dest,paths)
        else:
            with zipfile.ZipFile(dest,'x',compression=zipfile.ZIP_DEFLATED,compresslevel=9) as z:
                for n in paths:
                    info=zipfile.ZipInfo(n,(2026,9,8,0,0,0));info.create_system=3;info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_DEFLATED
                    z.writestr(info,(tree/n).read_bytes(),compresslevel=9)
            digest=hashlib.sha256(dest.read_bytes()).hexdigest()
            dest.with_name(name+'.sha256').write_bytes((digest+'  '+name+'\n').encode())
        extracted=R/'final_extracted'/kind
        expanded=extract_new(dest,extracted);assert verify_manifest(extracted/'12_FILE_HASHES.txt')==count
        identities[kind]={'filename':name,'bytes':dest.stat().st_size,'sha256':digest,'file_count':len(paths),'manifest_entries':count,
            'expanded_bytes':expanded,'public_git_tree_sha1':git_tree_sha1(tree) if kind=='public' else None,
            'safe_fresh_extraction':True,'crc_paths_links_duplicates_expansion_checked':True}
    write(R/'checks/ARCHIVE_IDENTITIES.json',identities)
    print(json.dumps(identities,indent=2))
if __name__=='__main__':main()
