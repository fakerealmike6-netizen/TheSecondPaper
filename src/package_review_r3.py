"""R3 payload/equivalence/manifest/ZIP DAG for explicitly prepared local trees.

No data selection, copying, Git mutation, credential access or network action.
The caller prepares separate reviewed MIN/public trees. Generated objects never
claim payload equivalence; a late publication receipt is explicitly outside
the file manifest and is bound by the enclosing MIN ZIP's external sidecar.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import stat
import unicodedata
import zipfile

from public_export_r1 import scan_selected

EQUIVALENCE = 'PUBLIC_LOCAL_EQUIVALENCE.json'
MANIFEST = '12_FILE_HASHES.txt'
RECEIPT = 'PUBLICATION_RECEIPT.json'
PUBLIC_ZIP = 'Stage1B_R3_Public_Review_Bundle.zip'
MIN_ZIP = 'Stage1B_R3_Review_Bundle_MIN.zip'
GENERATED = {
    EQUIVALENCE: 'Equivalence binds frozen payload; it cannot bind itself.',
    MANIFEST: 'Generated after equivalence; excludes itself and publication receipt.',
    RECEIPT: 'Late publication result, excluded from manifest; enclosing ZIP binds its bytes.',
    PUBLIC_ZIP: 'Final archive has an external sidecar, never an internal self-reference.',
    MIN_ZIP: 'Final archive has an external sidecar, never an internal self-reference.',
    PUBLIC_ZIP + '.sha256': 'External archive sidecar.',
    MIN_ZIP + '.sha256': 'External archive sidecar.',
    'REVIEW_HANDOFF.md': 'External handoff may bind both final archive hashes.',
}
ROOT_PUBLIC_FILES = {
    'README.md', 'NOTICE.md', 'LICENSE', 'LICENSE.md', 'requirements.txt', '.gitignore', '.gitattributes',
    '00_REVIEW_INDEX.md', '01_CHECKPOINT_1B_R3_REPORT.md', '02_FINAL_STATUS.json',
    '03_REQUIREMENT_COMPLETION_MATRIX.md', '04_CONTEXT_ACQUISITION_AND_RECONCILIATION.md',
    '05_CONSTRAINT_INTEGRATION_AND_PROVENANCE.md', '06_AMOUNT_COMPARISON_AND_EXPLANATION.md',
    '07_WETH_COMPONENT_STATUS.md', '08_USAGE_AND_RESUME_LEDGER.md', '09_TEST_RESULTS.json',
    '10_GITHUB_PUBLICATION_REPORT.md', '11_OPEN_ITEMS_AND_NEXT_STEP.md', 'CONTINUATION_GATE_R3_PUBLIC.json',
}

PUBLIC_CONFIGS = {
    'DUNE_ADAPTER_SCHEMA.md', 'PROVIDER_REPAIR_NOTES.md', 'VALIDATION_R1.md',
    'VALIDATION_R3.md', 'requirements_lp.txt', 'STAGE1B_POLICY.json',
    'STAGE1B_R1_PUBLIC_POLICY.json', 'STAGE1B_R3_PUBLIC_POLICY.json',
    'PUBLIC_VALIDATION_R3.json', 'STAGE1B_R2_PUBLIC_POLICY.json', 'PUBLIC_VALIDATION_R2.json', 'VALIDATION_R2.md',
}
PUBLIC_MANIFESTS = {'PUBLIC_SOURCE_MAPPING.json', 'PUBLIC_PAYLOAD_APPROVAL.json'}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + '\n').encode('utf-8')


def safe_relative(name):
    if not isinstance(name, str) or not name or '\\' in name or ':' in name or name.startswith('/') or any(ord(char) < 32 for char in name):
        raise ValueError('Unsafe relative payload path')
    path = PurePosixPath(name)
    if str(path) != name or any(part in ('.', '..') or part.rstrip(' .') != part for part in path.parts):
        raise ValueError('Unsafe relative payload path')
    devices = {'CON', 'PRN', 'AUX', 'NUL', *(f'COM{i}' for i in range(1, 10)), *(f'LPT{i}' for i in range(1, 10))}
    if any(part.split('.')[0].upper() in devices for part in path.parts):
        raise ValueError('Reserved device in payload path')
    return path


def tree_files(tree):
    tree = Path(tree)
    if tree.is_symlink() or not tree.is_dir() or getattr(tree.lstat(), 'st_file_attributes', 0) & 0x400:
        raise ValueError('Prepared tree must be a regular directory')
    root = tree.resolve()
    found, seen, total = {}, set(), 0
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root).as_posix()
        safe_relative(relative)
        status = path.lstat()
        if stat.S_ISLNK(status.st_mode) or getattr(status, 'st_file_attributes', 0) & 0x400:
            raise ValueError('Links and reparse points are forbidden in review trees')
        if not path.resolve().is_relative_to(root):
            raise ValueError('Prepared tree path escapes root')
        if any(part in ('.git', '__pycache__', '.venv', '.test_tmp') for part in path.parts[len(root.parts):]):
            raise ValueError('Repository/runtime directories must not enter a review tree')
        key = unicodedata.normalize('NFC', relative).casefold()
        if key in seen:
            raise ValueError('Duplicate normalized payload target')
        seen.add(key)
        if stat.S_ISDIR(status.st_mode):
            continue
        if not stat.S_ISREG(status.st_mode):
            raise ValueError('Special file in prepared tree')
        if status.st_size > 64 * 1024 * 1024:
            raise ValueError('Single review file exceeds 64 MiB')
        total += status.st_size
        if total > 512 * 1024 * 1024 or len(found) >= 10000:
            raise ValueError('Prepared tree resource limit')
        found[relative] = path
    return found


def payload_files(tree):
    return {name: path for name, path in tree_files(tree).items() if name not in GENERATED}


def public_path_allowed(name):
    path = safe_relative(name)
    parts = path.parts
    if name in ROOT_PUBLIC_FILES or name in (EQUIVALENCE, MANIFEST):
        return True
    if len(parts) == 2 and parts[0] in ('src', 'tests') and path.suffix == '.py':
        return True
    if len(parts) >= 3 and parts[0] == 'fixtures' and parts[1] in ('controlled', 'collector', 'fault') and path.suffix in ('.py', '.json', '.md'):
        return True
    if len(parts) == 2 and parts[0] == 'docs' and path.suffix == '.md':
        return True
    if len(parts) == 2 and parts[0] == 'configs' and parts[1] in PUBLIC_CONFIGS:
        return True
    if len(parts) == 2 and parts[0] == 'manifests' and parts[1] in PUBLIC_MANIFESTS:
        return True
    return False


def validate_public_tree(tree):
    files = tree_files(tree)
    rejected = sorted(name for name in files if not public_path_allowed(name))
    if rejected:
        raise ValueError(json.dumps({'public_path_not_allowlisted': rejected}))
    # Existing R1 exceptions preserve only exact named synthetic invalid paths.
    findings = scan_selected({name: path.read_bytes() for name, path in files.items()})
    if findings:
        raise ValueError(json.dumps({'public_content_scan_findings': findings}))
    return files


def _write_once(path, data):
    path = Path(path)
    if path.is_symlink():
        raise ValueError('Generated target is a link')
    if path.exists():
        if path.read_bytes() != data:
            raise FileExistsError('Generated artifact already differs; use a fresh reviewed tree')
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as handle:
        handle.write(data)


def _equivalence(min_tree, public_tree, differences):
    private = payload_files(min_tree)
    public = {name: path for name, path in validate_public_tree(public_tree).items() if name not in GENERATED}
    if set(differences) - set(public):
        raise ValueError('Difference declaration names absent public payload')
    rows = []
    for name, public_path in sorted(public.items()):
        spec = differences.get(name, {})
        if set(spec) - {'local_path', 'reason'}:
            raise ValueError('Unsupported difference declaration field')
        local_name = spec.get('local_path', name)
        if local_name is not None:
            safe_relative(local_name)
            if local_name in GENERATED:
                raise ValueError('Generated metadata cannot be declared payload-equivalent')
        public_data = public_path.read_bytes()
        local_data = private[local_name].read_bytes() if local_name in private else None
        if local_data is None:
            if local_name is not None or not spec.get('reason'):
                raise ValueError('Public-only payload requires local_path null and a reason: ' + name)
            transformation = 'PUBLIC_ONLY_DOCUMENTED'
        elif local_data == public_data:
            transformation = 'IDENTICAL'
        else:
            if not spec.get('reason'):
                raise ValueError('Different payload bytes require a documented reason: ' + name)
            transformation = 'DOCUMENTED_SANITIZATION_DIFFERENCE'
        rows.append({'public_path': name, 'local_path': local_name,
                     'transformation': transformation,
                     'public_sha256': digest(public_data), 'public_bytes': len(public_data),
                     'local_sha256': digest(local_data) if local_data is not None else None,
                     'local_bytes': len(local_data) if local_data is not None else None,
                     'reason': spec.get('reason') if transformation != 'IDENTICAL' else None})
    return {'schema': 'stage1b-r3-payload-equivalence-dag-v1',
            'external_acceptance': 'PENDING_REVIEW',
            'payload_rows': rows, 'public_payload_count': len(public),
            'local_payload_count': len(private),
            'local_payload_inventory_sha256': digest(json_bytes([
                {'path': name, 'bytes': path.stat().st_size, 'sha256': digest(path.read_bytes())}
                for name, path in sorted(private.items())])),
            'local_only_payload_count': len(set(private) - {r['local_path'] for r in rows}),
            'excluded_generated': [{'path': name, 'classification': 'EXCLUDED_FROM_PAYLOAD_EQUIVALENCE', 'reason': reason}
                                   for name, reason in sorted(GENERATED.items())],
            'manifest_exclusions': [MANIFEST, RECEIPT],
            'generation_order': ['freeze_min_and_public_payload', 'equivalence', 'manifests',
                                 'public_zip_and_sidecar', 'public_commit_tag_release',
                                 'external_publication_receipt', 'min_zip_and_sidecar', 'external_handoff']}


def freeze_payloads(min_tree, public_tree, differences=None):
    """Write the same non-self-referential mapping into both prepared trees."""
    min_root, public_root = Path(min_tree).resolve(), Path(public_tree).resolve()
    if min_root == public_root or min_root.is_relative_to(public_root) or public_root.is_relative_to(min_root):
        raise ValueError('MIN and public trees must be separate')
    mapping = _equivalence(min_tree, public_tree, differences or {})
    data = json_bytes(mapping)
    # Preflight both destinations before any mutation; never rewrite old maps.
    for root in (min_root, public_root):
        target = root / EQUIVALENCE
        if target.exists() and target.read_bytes() != data:
            raise FileExistsError('Existing equivalence differs; use a fresh reviewed tree')
    for root in (min_root, public_root):
        _write_once(root / EQUIVALENCE, data)
    verify_equivalence(min_tree, public_tree)
    return mapping


def verify_equivalence(min_tree, public_tree):
    left, right = Path(min_tree) / EQUIVALENCE, Path(public_tree) / EQUIVALENCE
    if left.read_bytes() != right.read_bytes():
        raise ValueError('Public and local equivalence copies differ')
    saved = json.loads(left.read_text(encoding='utf-8'))
    differences = {row['public_path']: {'local_path': row['local_path'], 'reason': row['reason']}
                   for row in saved['payload_rows']
                   if row['transformation'] != 'IDENTICAL' or row['local_path'] != row['public_path']}
    if saved != _equivalence(min_tree, public_tree, differences):
        raise ValueError('Frozen payload mapping is stale or incomplete')
    return {'passed': True, 'public_payload_count': saved['public_payload_count'],
            'identical_count': sum(row['transformation'] == 'IDENTICAL' for row in saved['payload_rows']),
            'equivalence_sha256': digest(left.read_bytes())}


def write_manifest(tree):
    files = tree_files(tree)
    if EQUIVALENCE not in files:
        raise ValueError('Freeze equivalence before generating a manifest')
    forbidden = set(files) & (set(GENERATED) - {EQUIVALENCE, MANIFEST, RECEIPT})
    if forbidden:
        raise ValueError('Archive, sidecar and handoff must remain outside prepared trees')
    included = {name: path for name, path in files.items() if name not in (MANIFEST, RECEIPT)}
    header = '# Stage1B-R3 manifest v1\n# EXCLUDED: 12_FILE_HASHES.txt (self)\n# EXCLUDED: PUBLICATION_RECEIPT.json (late receipt, bound by enclosing ZIP)\n'
    data = (header + ''.join(f'{digest(path.read_bytes())}  {name}\n' for name, path in sorted(included.items()))).encode('utf-8')
    _write_once(Path(tree) / MANIFEST, data)
    return verify_manifest(tree)


def verify_manifest(tree):
    files = tree_files(tree)
    raw = files[MANIFEST].read_text(encoding='utf-8')
    if '# EXCLUDED: PUBLICATION_RECEIPT.json (late receipt, bound by enclosing ZIP)' not in raw:
        raise ValueError('Manifest must explicitly document the late receipt exclusion')
    declared = {}
    for line in raw.splitlines():
        if not line or line.startswith('#'):
            continue
        match = re.fullmatch(r'([0-9a-f]{64})  (.+)', line)
        if not match:
            raise ValueError('Invalid manifest record')
        name = match[2]
        safe_relative(name)
        if name in declared:
            raise ValueError('Duplicate manifest record')
        declared[name] = match[1]
    expected = {name for name in files if name not in (MANIFEST, RECEIPT)}
    if set(declared) != expected:
        raise ValueError('Manifest does not cover exactly the frozen tree')
    for name, value in declared.items():
        if digest(files[name].read_bytes()) != value:
            raise ValueError('Manifest file hash mismatch: ' + name)
    return {'passed': True, 'files_verified': len(declared), 'manifest_sha256': digest(files[MANIFEST].read_bytes()),
            'excluded_paths': [MANIFEST, RECEIPT]}


def git_tree_sha1(tree):
    """Compute the exact all-100644 Git tree represented by the public ZIP."""
    nested = {}
    for name, path in validate_public_tree(tree).items():
        node, parts = nested, safe_relative(name).parts
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = path.read_bytes()

    def obj(kind, data):
        return hashlib.sha1(kind.encode() + b' ' + str(len(data)).encode() + b'\0' + data).digest()

    def encode(node):
        body = b''
        for name, value in sorted(node.items(), key=lambda pair: (pair[0] + ('/' if isinstance(pair[1], dict) else '')).encode()):
            directory = isinstance(value, dict)
            body += (b'40000' if directory else b'100644') + b' ' + name.encode() + b'\0'
            body += encode(value) if directory else obj('blob', value)
        return obj('tree', body)

    return encode(nested).hex()


def deterministic_zip(tree, archive, *, public=False):
    files = validate_public_tree(tree) if public else tree_files(tree)
    tree, archive = Path(tree).resolve(), Path(archive)
    if archive.resolve().is_relative_to(tree):
        raise ValueError('Final ZIP must be outside its input tree')
    manifest = verify_manifest(tree)
    if set(files) & (set(GENERATED) - {EQUIVALENCE, MANIFEST, RECEIPT}):
        raise ValueError('Nested generated archives or handoff are forbidden')
    archive.parent.mkdir(parents=True, exist_ok=True)
    with archive.open('xb') as handle:
        with zipfile.ZipFile(handle, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as bundle:
            for name, path in sorted(files.items()):
                entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                entry.create_system = 3
                entry.external_attr = (stat.S_IFREG | 0o644) << 16
                entry.compress_type = zipfile.ZIP_DEFLATED
                bundle.writestr(entry, path.read_bytes(), compresslevel=9)
    with zipfile.ZipFile(archive) as bundle:
        if bundle.testzip() is not None or set(bundle.namelist()) != set(files):
            raise ValueError('Final ZIP CRC or membership mismatch')
        for name, path in files.items():
            if digest(bundle.read(name)) != digest(path.read_bytes()):
                raise ValueError('Final ZIP differs from frozen input: ' + name)
    verify_manifest(tree)
    receipt = {'file': archive.name, 'bytes': archive.stat().st_size,
               'sha256': digest(archive.read_bytes()), 'members': len(files),
               'manifest_verified': manifest, 'public': public}
    if public:
        receipt['public_git_tree_sha1'] = git_tree_sha1(tree)
    return receipt


def write_sidecar(archive):
    archive = Path(archive)
    value = digest(archive.read_bytes())
    sidecar = archive.with_name(archive.name + '.sha256')
    _write_once(sidecar, f'{value}  {archive.name}\n'.encode('ascii'))
    return {'file': sidecar.name, 'archive_sha256': value, 'archive_bytes': archive.stat().st_size}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('freeze', 'verify-equivalence'):
        child = sub.add_parser(name)
        child.add_argument('--min-tree', type=Path, required=True)
        child.add_argument('--public-tree', type=Path, required=True)
        if name == 'freeze':
            child.add_argument('--differences', type=Path)
    for name in ('manifest', 'verify-manifest', 'git-tree'):
        child = sub.add_parser(name)
        child.add_argument('--tree', type=Path, required=True)
    child = sub.add_parser('zip')
    child.add_argument('--tree', type=Path, required=True)
    child.add_argument('--archive', type=Path, required=True)
    child.add_argument('--public', action='store_true')
    child = sub.add_parser('sidecar')
    child.add_argument('--archive', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'freeze':
        differences = json.loads(args.differences.read_text(encoding='utf-8')) if args.differences else {}
        result = freeze_payloads(args.min_tree, args.public_tree, differences)
    elif args.command == 'verify-equivalence':
        result = verify_equivalence(args.min_tree, args.public_tree)
    elif args.command == 'manifest':
        result = write_manifest(args.tree)
    elif args.command == 'verify-manifest':
        result = verify_manifest(args.tree)
    elif args.command == 'git-tree':
        result = {'git_tree_sha1': git_tree_sha1(args.tree)}
    elif args.command == 'zip':
        result = deterministic_zip(args.tree, args.archive, public=args.public)
    else:
        result = write_sidecar(args.archive)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
