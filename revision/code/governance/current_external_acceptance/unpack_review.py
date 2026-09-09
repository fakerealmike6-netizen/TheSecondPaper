from pathlib import Path,PurePosixPath
import zipfile,stat,hashlib,json,re
R=Path(__file__).parent
checks=[]
def unpack(path,dest,expected):
 h=hashlib.sha256(path.read_bytes()).hexdigest();assert h==expected,(path,h)
 assert not dest.exists();dest.mkdir(parents=True)
 with zipfile.ZipFile(path) as z:
  total=sum(i.file_size for i in z.infolist());assert total<800_000_000
  names=set()
  for i in z.infolist():
   p=PurePosixPath(i.filename);assert not p.is_absolute() and '..' not in p.parts and '\\' not in i.filename and ':' not in i.filename and '\0' not in i.filename
   assert i.filename.casefold() not in names;names.add(i.filename.casefold())
   mode=i.external_attr>>16;assert stat.S_IFMT(mode) in (0,stat.S_IFREG,stat.S_IFDIR);assert not i.flag_bits&1
  assert z.testzip() is None;z.extractall(dest)
 c={'name':path.name,'bytes':path.stat().st_size,'sha256':h,'members':len(names),'uncompressed_bytes':total,'crc':'PASS','safe_paths':'PASS','extracted':str(dest)};checks.append(c);print(c)
 return dest
h=unpack(Path('/mnt/data/Stage1C_Review_Handoff.zip'),R/'handoff','8d88e61a35f8862a2b6bf7ed2e0b2e1723ec7a7b3dbea89772dd7ea71b1a2fc7')
for name,kind,sha in [('Stage1C_Review_Bundle_MIN.zip','min','ecae2712435e2bf548fdf94c290fce8d834037900e4255adb252f8096269800e'),('Stage1C_Public_Review_Bundle.zip','public','dfd023105f90808a1b7c9f2531652a4318fb8b95c5f9c976b897e3f818aa58b4')]:
 found=list(h.rglob(name));assert len(found)==1
 unpack(found[0],R/'inputs'/kind,sha)
(R/'package_checks.json').write_text(json.dumps(checks,indent=2))
