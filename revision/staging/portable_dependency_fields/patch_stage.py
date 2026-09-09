from pathlib import Path
p=Path(__file__).parent/'src/stage1d_batch_binding_route.py'
s=p.read_text(encoding='utf8');start=s.index('def collect_portable_dependencies(');end=s.index('\n\ndef superset_admissibility(',start)
new='''def collect_portable_dependencies(work, preparation_path, job_states, tx_hash):
    """Collect the finite protocol dependency fields, then independently verify.

    A scope snapshot's bytes are an input. Its descriptive historical paths
    are not a recursive file manifest. No arbitrary JSON walk or path-specific
    exclusion is used; all actual page/spec/root proof edges remain required.
    """
    from stage1d_bq_portable import verify_transaction_family_documents, ROOT_BINDINGS_PATH
    family = verified_transaction_family(work, preparation_path, job_states, tx_hash)
    documents = {}
    def add(path, expected=None, size=None):
        path = transfers.inside(work, path)
        name = path.relative_to(Path(work).resolve()).as_posix()
        before = path.stat(); data = path.read_bytes(); after = path.stat()
        if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
            raise ValueError('Portable dependency changed during read: ' + name)
        if expected is not None and hashlib.sha256(data).hexdigest() != expected:
            raise ValueError('Portable dependency hash changed: ' + name)
        if size is not None and (type(size) is not int or len(data) != size):
            raise ValueError('Portable dependency byte count changed: ' + name)
        if name in documents and documents[name] != data:
            raise ValueError('One portable path has conflicting original bytes: ' + name)
        documents[name] = data
        return data
    def dependency(ref):
        if not isinstance(ref, dict) or not ref.get('sha256'):
            raise ValueError('Exact portable protocol dependency SHA required')
        return add(ref['path'], ref['sha256'], ref.get('bytes'))
    def document(path, expected=None):
        return json.loads(add(path, expected).decode('utf-8-sig'))
    def source_envelope(path, expected):
        envelope = document(path, expected)
        # Original schema/dry/page HTTP payloads are direct protocol fields.
        # Their bytes remain leaves, even when a provider payload contains a
        # column literally named path, sha256 or any historical provenance.
        for ref in envelope.get('raw_sources', []): dependency(ref)
        return envelope

    manifest = document(preparation_path)
    for ref in manifest['scope_dependencies']: dependency(ref)
    if set(job_states) != {spec['path'] for spec in manifest['plans']}:
        raise ValueError('Every exact prepared job is required')
    for expected_spec in manifest['plans']:
        spec = json.loads(dependency(expected_spec).decode('utf-8-sig'))
        add(spec['sql_path'], spec['sql_sha256'])
        for ref in spec['scope_dependencies']: dependency(ref)
        for ref in spec['schema_evidence']: source_envelope(ref['path'], ref['sha256'])
        state_path = job_states[expected_spec['path']]
        state = document(state_path)
        plan = document(state['plan_path'], state['plan_sha256'])
        # Check both incoming spec bindings; the pure verifier enforces equality.
        add(plan['dry_spec_path'], plan['dry_spec_sha256'])
        source_envelope(plan['dry_receipt_path'], plan['dry_receipt_sha256'])
        add(transfers.inside(work, state_path).parent / 'terminal_job.json', state['terminal_job_sha256'])
        add(state['rows_path'], state['rows_sha256'])
        for page in state['pages']:
            add(page['rows_path'], page['rows_sha256'])
            response = page['response_receipt']
            source_envelope(response['artifact_path'], response['artifact_sha256'])

    for binding in family['root_bindings']:
        for request in binding['requests']:
            member = request['member']
            envelope = document(member['artifact_path'], member['artifact_sha256'])
            if envelope.get('evidence_kind') == 'REAL_CHAIN_LEGACY_RAW_REUSE':
                source = envelope['legacy_source']
                add(member['admission_path'], member['admission_sha256'])
                add(envelope['raw_path'], source['raw_sha256'], source['raw_bytes'])
                add(envelope['manifest_path'], source['manifest_sha256'], source['manifest_bytes'])
                # Old absolute legacy_source locators describe the acquisition;
                # the copied payload/manifest/admission above are the inputs.
            else:
                original = envelope.get('original_artifact')
                if original is not None: dependency(original)
                original_path = original['path'] if original else member['artifact_path']
                raw = transfers.inside(work, original_path).parent / 'response_body.bin'
                if raw.exists(): add(raw, envelope['raw_body_sha256'])
                # The established successful-cache envelope-only contract is
                # checked by the independent verifier when an old body is absent.
    root_bytes = (json.dumps({'schema_version': 'stage1d-portable-root-rpc-bindings-v1',
        'tx_hash': tx_hash, 'root_bindings': family['root_bindings']}, sort_keys=True, separators=(',', ':')) + '\\n').encode()
    if ROOT_BINDINGS_PATH in documents and documents[ROOT_BINDINGS_PATH] != root_bytes:
        raise ValueError('Derived root-binding metadata collides with original dependency')
    documents[ROOT_BINDINGS_PATH] = root_bytes
    verify_transaction_family_documents(preparation_path, job_states, tx_hash, documents)
    return documents
'''
p.write_text(s[:start]+new+s[end:],encoding='utf8',newline='\n')
