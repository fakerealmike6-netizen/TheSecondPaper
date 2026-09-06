"""Safe bounded ZIP inspection and manifest verification; no extractall."""
import hashlib,re,stat,zipfile
from pathlib import Path,PurePosixPath

def inspect_zip(path,max_files=10000,max_bytes=536870912):
    entries=[];seen=set();total=0
    with zipfile.ZipFile(path) as z:
        if len(z.infolist())>max_files: raise ValueError('File count limit')
        for i in z.infolist():
            name=i.filename.replace('\\','/'); p=PurePosixPath(name);mode=i.external_attr>>16
            if not name or name.startswith('/') or ':' in name or any(x in ('.','..') or x.rstrip(' .')!=x for x in p.parts): raise ValueError('Unsafe path')
            reserved={'CON','PRN','AUX','NUL',*(f'COM{n}' for n in range(1,10)),*(f'LPT{n}' for n in range(1,10))}
            if any(x.split('.')[0].upper() in reserved for x in p.parts): raise ValueError('Reserved device')
            if stat.S_IFMT(mode) not in (0,stat.S_IFREG,stat.S_IFDIR) or i.flag_bits&1: raise ValueError('Special/encrypted entry')
            key=str(p).casefold()
            if key in seen: raise ValueError('Duplicate target')
            seen.add(key);total+=i.file_size
            if i.file_size>64*1024*1024 or total>max_bytes or i.file_size/max(1,i.compress_size)>1000: raise ValueError('Expansion limit')
            entries.append(i)
        if z.testzip() is not None: raise ValueError('CRC mismatch')
    return entries,total

def extract_new(path,destination):
    entries,total=inspect_zip(path);dest=Path(destination).resolve()
    dest.mkdir(parents=True,exist_ok=False)
    with zipfile.ZipFile(path) as z:
        for i in entries:
            target=dest.joinpath(*PurePosixPath(i.filename.replace('\\','/')).parts).resolve()
            if not target.is_relative_to(dest): raise ValueError('Resolved path escape')
            if i.is_dir():target.mkdir(parents=True,exist_ok=True)
            else:target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(z.read(i))
    return total

def verify_manifest(manifest):
    manifest=Path(manifest);count=0
    for line in manifest.read_text(encoding='utf-8-sig').splitlines():
        if not line.strip() or line.startswith('#'):continue
        m=re.fullmatch(r'([0-9a-fA-F]{64})\s+\*?(.+)',line)
        if not m:raise ValueError('Invalid hash manifest')
        p=(manifest.parent/m[2]).resolve()
        if not p.is_relative_to(manifest.parent.resolve()):raise ValueError('Manifest escape')
        if hashlib.sha256(p.read_bytes()).hexdigest()!=m[1].lower():raise ValueError('Hash mismatch')
        count+=1
    return count
