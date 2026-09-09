from pathlib import Path
import json,hashlib,re,zipfile,ast
R=Path(__file__).parent
records=[]
for kind in ['min','public']:
 d=R/'inputs'/kind;matched=[]
 for line in (d/'12_FILE_HASHES.txt').read_text(encoding='utf-8-sig').splitlines():
  if not line.strip() or line.startswith('#'):continue
  m=re.fullmatch(r'([0-9a-fA-F]{64})\s+\*?(.+)',line);assert m,line
  h,name=m.groups();p=d/name;assert p.resolve().is_relative_to(d.resolve()) and p.is_file();assert hashlib.sha256(p.read_bytes()).hexdigest()==h.lower(),name;matched.append(name)
 records.append({'tree':kind,'manifest_entries':len(matched),'excluded':[p.relative_to(d).as_posix() for p in d.rglob('*') if p.is_file() and p.relative_to(d).as_posix() not in matched],'status':'PASS'})
def obj(kind,b):return hashlib.sha1(f'{kind} {len(b)}\0'.encode()+b).digest()
def tree(folder):
 parts=[]
 for p in folder.iterdir():
  isdir=p.is_dir();k=p.name.encode()+(b'/' if isdir else b'');mode=b'40000' if isdir else b'100644'
  parts.append((k,mode+b' '+p.name.encode()+b'\0'+(tree(p) if isdir else obj('blob',p.read_bytes()))))
 return obj('tree',b''.join(v for _,v in sorted(parts)))
h=tree(R/'inputs/public').hex();assert h=='cef2f09aaa6033e9cb7393d6dc5cad6119a4f450',h
src=[]
for p in sorted((R/'inputs/public/src').glob('*.py')):
 q=R/'inputs/min/src'/p.name;assert q.read_bytes()==p.read_bytes();a=ast.parse(p.read_text());src.append({'path':'src/'+p.name,'lines':len(p.read_text().splitlines()),'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'functions':[n.name for n in a.body if isinstance(n,(ast.FunctionDef,ast.ClassDef))]})
# Compare to fixed accepted original parent archive already supplied in conversation.
parent=Path('/mnt/data/Stage1B_R4_R1_Review_Handoff.zip')
with zipfile.ZipFile(parent) as z:
 name=next(n for n in z.namelist() if n.endswith('Stage1B_R4_R1_Public_Review_Bundle.zip'));import io
 with zipfile.ZipFile(io.BytesIO(z.read(name))) as zz:
  old={n:zz.read(n) for n in zz.namelist() if n.startswith(('src/','tests/')) and n.endswith('.py')}
oldsrc={n for n in old if n.startswith('src/')};newsrc={x['path'] for x in src}
changes={'new_source':sorted(newsrc-oldsrc),'changed_source':sorted(n for n in oldsrc&newsrc if old[n]!=(R/'inputs/public'/n).read_bytes()),'removed_source':sorted(oldsrc-newsrc),'unchanged_source_count':sum(old[n]==(R/'inputs/public'/n).read_bytes() for n in oldsrc&newsrc),'old_test_count':sum(n.startswith('tests/') for n in old),'changed_old_tests':sorted(n for n in old if n.startswith('tests/') and (not (R/'inputs/public'/n).exists() or old[n]!=(R/'inputs/public'/n).read_bytes()))}
result={'manifest_checks':records,'git_tree':h,'matches_authenticated_remote_tree':True,'commit':'d88b839361b8fa93308430641baf7af423d76ae8','parent':'81eff7601acfc15ce5dfdabe5fd328073499de09','source_file_count':len(src),'source_line_count':sum(s['lines'] for s in src),'all_public_sources_match_min':True,'changes':changes,'source_inventory':src}
(R/'identity_checks.json').write_text(json.dumps(result,indent=2));print(json.dumps({k:v for k,v in result.items() if k!='source_inventory'},indent=2))
