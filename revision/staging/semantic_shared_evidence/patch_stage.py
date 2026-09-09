from pathlib import Path
P=Path(__file__).parent/'src'
def edit(name, changes):
    p=P/name;s=p.read_text(encoding='utf8')
    for old,new in changes:
        if s.count(old)!=1:raise RuntimeError((name,old[:90],s.count(old)))
        s=s.replace(old,new)
    p.write_text(s,encoding='utf8',newline='\n')

edit('weth_evidence.py',[
('def load_portable_evidence_context(portable):','def load_portable_evidence_context(portable, *, blob_reader=None, validation_session=None):'),
("    def decode(key):\n        data=base64.b64decode(portable[key+'_bytes_base64'],validate=True)","    from stage1d_shared_evidence import decode_original\n    def decode(key):\n        data=decode_original(portable[key+'_bytes_base64'],blob_reader)"),
("data=base64.b64decode(item['bytes_base64'],validate=True)","data=decode_original(item['bytes_base64'],blob_reader)"),
("ext=portable['dune_extension'];mb=base64.b64decode(ext['manifest_bytes_base64'],validate=True)","ext=portable['dune_extension'];mb=decode_original(ext['manifest_bytes_base64'],blob_reader)"),
("data={k:base64.b64decode(v,validate=True) for k,v in ext['role_bytes_base64'].items()}","data={k:decode_original(v,blob_reader) for k,v in ext['role_bytes_base64'].items()}"),
("ext=portable['bigquery_extension'];family=load_bigquery_family(ext['family'])","ext=portable['bigquery_extension'];family=load_bigquery_family(ext['family'],blob_reader=blob_reader,validation_session=validation_session)")])

edit('stage1d_bq_portable.py',[
('class Documents:', '''class ValidationSession:
    """Fresh in-memory reuse of original-byte checks, never serialized trust."""
    def __init__(self):
        self._raw = {}; self._jobs = {}
        self.counts = {'job_verifications': 0, 'job_cache_hits': 0, 'tx_indexes': 0}

    def sha(self, data):
        entry = self._raw.get(id(data))
        if entry is None or entry[0] is not data:
            entry = (data, hashlib.sha256(data).hexdigest()); self._raw[id(data)] = entry
        return entry[1]


class Documents:'''),
('    def __init__(self,documents):','    def __init__(self,documents,session=None):'),
('        self.used={}','        self.used={}\n        if session is not None and not isinstance(session,ValidationSession):raise ValueError("Fresh validation session required")\n        self.session=session; self.family_identity=None'),
("data=self.documents[path];sha=hashlib.sha256(data).hexdigest()","data=self.documents[path];sha=self.session.sha(data) if self.session is not None else hashlib.sha256(data).hexdigest()"),
('def verify_export_documents(store,state_path,expected_spec):','''def verify_export_documents(store,state_path,expected_spec):
    if not isinstance(store,Documents):store=Documents(store)
    session=store.session
    if session is None:return _verify_export_documents_uncached(store,state_path,expected_spec)
    state_sha=session.sha(store.raw(state_path))
    key=(store.family_identity,state_path,state_sha,digest(expected_spec))
    cached=session._jobs.get(key)
    if cached is not None:
        # Same state SHA is insufficient: the complete original dependency
        # mapping must still exist with exactly the bytes previously verified.
        value,dependencies=cached
        for dep in dependencies:store.dep(dep)
        session.counts['job_cache_hits']+=1
        return value
    local=Documents(store.documents,session);local.family_identity=store.family_identity
    value=_verify_export_documents_uncached(local,state_path,expected_spec)
    index={}
    for row in value['rows']:index.setdefault(row.get('tx_hash'),[]).append(row)
    value['_tx_index']=index
    dependencies=list(local.used.values())
    session._jobs[key]=(value,dependencies)
    session.counts['job_verifications']+=1;session.counts['tx_indexes']+=1
    for dep in dependencies:store.dep(dep)
    return value


def _verify_export_documents_uncached(store,state_path,expected_spec):'''),
('def verify_transaction_family_documents(preparation_path,job_states,tx_hash,documents):', 'def verify_transaction_family_documents(preparation_path,job_states,tx_hash,documents, *, validation_session=None):'),
("store=Documents(documents);manifest=store.read(preparation_path)","store=Documents(documents,validation_session);manifest=store.read(preparation_path)\n    store.family_identity=store.used[preparation_path]['sha256']"),
("rows.extend(row for row in verified['rows'] if row.get('tx_hash')==tx_hash)","rows.extend(verified['_tx_index'].get(tx_hash,[]) if '_tx_index' in verified else\n                    (row for row in verified['rows'] if row.get('tx_hash')==tx_hash))"),
('def load_bigquery_family(portable):','def load_bigquery_family(portable, *, blob_reader=None, validation_session=None):'),
("    documents={}\n    for name,item in portable['documents'].items():\n        safe_name(name);raw=base64.b64decode(item['bytes_base64'],validate=True)","    from stage1d_shared_evidence import decode_original\n    documents={}\n    for name,item in portable['documents'].items():\n        safe_name(name);raw=decode_original(item['bytes_base64'],blob_reader)"),
("if hashlib.sha256(raw).hexdigest()!=item['sha256'] or len(raw)!=item['bytes']:","if (validation_session.sha(raw) if validation_session is not None else hashlib.sha256(raw).hexdigest())!=item['sha256'] or len(raw)!=item['bytes']:"),
("return verify_transaction_family_documents(portable['preparation_path'],portable['job_states'],portable['tx_hash'],documents)","return verify_transaction_family_documents(portable['preparation_path'],portable['job_states'],portable['tx_hash'],documents,validation_session=validation_session)")])

edit('stage1d_semantic_units.py',[
("    'stage1d_weth_log_index.py','stage1d_timestamp_bracket.py','stage1d_closure_context.py')","    'stage1d_weth_log_index.py','stage1d_timestamp_bracket.py','stage1d_closure_context.py',\n    'stage1d_shared_evidence.py')"),
("        if isinstance(evidence_context,dict) and unit.get('evidence_context_id') in evidence_context:\n            evidence_context=evidence_context[unit['evidence_context_id']]", "        from stage1d_shared_evidence import context_view\n        from collections.abc import Mapping\n        evidence_context=context_view(evidence_context)\n        if isinstance(evidence_context,Mapping) and unit.get('evidence_context_id') in evidence_context:\n            evidence_context=evidence_context[unit['evidence_context_id']]"),
("        self.contexts=copy.deepcopy(evidence_contexts);self.units={};self.by_holder={};self.controlled=controlled", "        from stage1d_shared_evidence import context_view,serialize_contexts\n        # Each resolver owns a fresh proof-reading session. Copy descriptors,\n        # never a prior resolver's in-process verified contexts or seals.\n        self.contexts=context_view(serialize_contexts(evidence_contexts));self.units={};self.by_holder={};self.controlled=controlled")])

edit('collector.py',[
("            result.semantic_evidence_context={k:self.semantic_resolver.contexts[k] for k in sorted(used_contexts)}", "            from stage1d_shared_evidence import serialize_contexts\n            result.semantic_evidence_context=serialize_contexts(self.semantic_resolver.contexts,used_contexts)")])

edit('stage1d_multiasset_context.py',[
("    contexts = document.get('semantic_evidence_context', {})", "    from stage1d_shared_evidence import context_view\n    contexts = context_view(document.get('semantic_evidence_context', {}))"),
("    result = copy.deepcopy(document)\n    result.update", "    from stage1d_shared_evidence import serialize_contexts\n    result = copy.deepcopy(document)\n    result.update"),
("semantic_evidence_context=copy.deepcopy(evidence_context or {}), evidence_kind=evidence_kind)", "semantic_evidence_context=serialize_contexts(evidence_context or {}), evidence_kind=evidence_kind)"),
("    contexts = material.get('semantic_evidence_context', collection.get('semantic_evidence_context', {}))", "    from stage1d_shared_evidence import context_view\n    contexts = context_view(material.get('semantic_evidence_context', collection.get('semantic_evidence_context', {})))")])

edit('stage1d_semantic_catalogue.py',[
("    units=[];contexts={};seen=set()", "    from stage1d_shared_evidence import context_view,SCHEMA as SHARED_SCHEMA\n    units=[];contexts={};seen=set()\n    shared=_bound(work,catalog['shared_evidence_context']) if 'shared_evidence_context' in catalog else None\n    if shared is not None and shared.get('schema_version')!=SHARED_SCHEMA:\n        raise ValueError('Explicit shared semantic evidence catalogue required')"),
("        unit=_bound(work,ref['unit']);context=_bound(work,ref['evidence_context'])", "        unit=_bound(work,ref['unit'])\n        context=_bound(work,ref['evidence_context']) if shared is None else None\n        if shared is not None and 'evidence_context' in ref:\n            raise ValueError('A catalogue must choose one explicit context inventory')"),
("    return FiniteSemanticResolver(units,contexts,capability_gate=", "    if shared is not None:\n        if set(contexts)!=set(shared['contexts']):raise ValueError('Shared catalogue context membership differs')\n        contexts=shared\n    return FiniteSemanticResolver(units,contexts,capability_gate=")])
