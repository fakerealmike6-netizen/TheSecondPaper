"""Explicit R1 public file selection. Never pushes Git or publishes private data."""
import hashlib,json,re
from pathlib import Path
from publication_export import scan_text

EXCLUDED_SOURCE={
 'build_stage_reports.py':'Superseded local one-off Stage1B report assembly',
 'finalize_local_tables.py':'Superseded local table assembly',
 'finalize_public_update.py':'Superseded Stage1B release assembly',
 'finalize_public.py':'Superseded Stage1B release assembly',
 'lp_record_correction.py':'One-off historical preservation utility',
 'package_review.py':'Private old MIN assembly, not a public repro dependency',
 'refresh_live_evidence.py':'Historical one-off local evidence assembly',
 'labels_build.py':'Superseded by frontier_labels_r1 and reusable labels/reference modules',
 'validate_review_bundle.py':'Superseded fixed-105-test validator; R1 validator included',
}

def selected_sources(work):
    work=Path(work).resolve();selected={};mapping=[]
    for folder in ('src','tests','fixtures'):
        for p in sorted((work/folder).rglob('*')):
            if not p.is_file() or '__pycache__' in p.parts or p.suffix not in ('.py','.json','.md'):continue
            if folder=='src' and p.name in EXCLUDED_SOURCE:continue
            if p.is_symlink() or not p.resolve().is_relative_to(work):raise ValueError('Public source path escape')
            rel=p.relative_to(work).as_posix();data=p.read_bytes()
            if len(data)>2*1024*1024:raise ValueError('Unexpected source/fixture size')
            selected[rel]=data;mapping.append({'path':rel,'source_sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data),'transformation':'IDENTICAL'})
    for p in sorted((work/'docs').glob('*.md')):
        if p.is_symlink() or not p.resolve().is_relative_to(work):raise ValueError('Public document path escape')
        selected[p.relative_to(work).as_posix()]=p.read_bytes()
    for name in ('DUNE_ADAPTER_SCHEMA.md','PROVIDER_REPAIR_NOTES.md','VALIDATION_R1.md','requirements_lp.txt'):
        p=work/'configs'/name
        if p.is_file():
            if p.is_symlink() or not p.resolve().is_relative_to(work):raise ValueError('Public configuration path escape')
            selected['configs/'+name]=p.read_bytes()
    mapping=[{'path':p,'source_sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b),'transformation':'IDENTICAL'} for p,b in sorted(selected.items())]
    return selected,mapping

def scan_selected(selected):
    findings=[hit for p,b in selected.items() for hit in scan_text(p,b)]
    # Keep the original path-escape negative control byte-identical. This is a
    # named synthetic invalid path, not a local user directory or secret.
    path='tests/test_validate_review_bundle_r1.py'
    allowed_invalid_paths=['D'+r':\outside','Z'+':/inaccessible_old_run/freeze_manifest.json']
    if path in selected and re.findall(r"(?i)(?<![A-Za-z0-9_])[A-Z]:[\\/][^\s'\"]+",selected[path].decode('utf-8'))==allowed_invalid_paths:
        findings=[f for f in findings if f!={'path':path,'rule':'LOCAL_DRIVE_PATH'}]
    if sum(map(len,selected.values()))>20*1024*1024:findings.append({'path':'*','rule':'PUBLIC_SIZE_LIMIT'})
    return findings

def write_new_tree(destination,selected):
    destination=Path(destination).resolve()
    if destination.exists():raise FileExistsError('Create a fresh public staging tree')
    findings=scan_selected(selected)
    if findings:raise ValueError(json.dumps({'secret_or_path_scan':findings}))
    destination.mkdir(parents=True)
    for name,data in sorted(selected.items()):
        p=destination/name
        if not p.resolve().is_relative_to(destination):raise ValueError('Path escape')
        p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
    return [{'path':p,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for p,b in sorted(selected.items())]
