from pathlib import Path, PurePosixPath
import hashlib, zipfile, stat, re, json
R=Path(__file__).parent; p=Path('/mnt/data/Stage1D_Paused_Review_Handoff.zip'); d=R/'input'
assert not d.exists()
with zipfile.ZipFile(p) as z:
    seen=set();total=0
    for i in z.infolist():
        q=PurePosixPath(i.filename);mode=i.external_attr>>16
        assert not q.is_absolute() and '..' not in q.parts and '\\' not in i.filename
        assert not re.match(r'^[A-Za-z]:',i.filename) and not stat.S_ISLNK(mode)
        key=i.filename.casefold();assert key not in seen;seen.add(key)
        total+=i.file_size;assert total<1_000_000_000
    assert z.testzip() is None
    d.mkdir();z.extractall(d)
manifests=[f for f in d.iterdir() if f.is_file() and 'HASH' in f.name.upper()];print('root manifests',[f.name for f in manifests]);print('last root',list(x.name for x in d.iterdir())[-15:])
f=d/'FILE_HASHES.txt'; checked=[]
if f.exists():
    for line in f.read_text(encoding='utf-8-sig').splitlines():
        if not line.strip() or line.lstrip().startswith('#'):continue
        m=re.match(r'^([0-9A-Fa-f]{64})\s+\*?(.+)$',line);assert m,line
        h,n=m.groups();q=d/n;assert q.is_file(),n
        assert hashlib.sha256(q.read_bytes()).hexdigest()==h.lower(),n
        checked.append(n)
report={'zip_bytes':p.stat().st_size,'zip_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'entries':len(seen),'expanded_bytes':total,'crc':'PASS','root_manifest_checked':len(checked),'separate_report_matches':(d/'Stage1D_Paused_Status_For_ChatGPT.md').read_bytes()==Path('/mnt/data/Stage1D_Paused_Status_For_ChatGPT.md').read_bytes(),'external_network_requests':0}
(R/'INPUT_CHECKS.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n');print(report)
