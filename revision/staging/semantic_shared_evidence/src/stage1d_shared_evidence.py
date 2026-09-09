"""Portable original-byte pooling, with a fresh verified reader for each load.

Only storage is shared. Paths belong to each context, and certificates, query
membership and method results are never pooled or trusted from serialized seals.
"""
import base64
import copy
import hashlib
import re
from collections.abc import Mapping

SCHEMA = 'stage1d-shared-semantic-evidence-contexts-v1'
REF = '$stage1d_original_blob'


def _sha(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise ValueError('Exact shared original-byte SHA required')
    return value


class BlobPool:
    """A bounded byte inventory, never a persistent verification authority."""
    def __init__(self, blobs=None):
        self.blobs = copy.deepcopy(blobs or {})
        self._bytes = {}
        for key, item in self.blobs.items():
            _sha(key)
            if (not isinstance(item, dict) or set(item) != {'bytes', 'bytes_base64'}
                    or type(item['bytes']) is not int or item['bytes'] < 0):
                raise ValueError('Invalid shared blob descriptor')

    def add(self, data):
        if type(data) is not bytes: raise ValueError('Original bytes required')
        sha = hashlib.sha256(data).hexdigest()
        if sha in self.blobs:
            if self.read({REF: sha, 'bytes': len(data)}) != data:
                raise ValueError('Conflicting shared original bytes')
        else:
            self.blobs[sha] = {'bytes': len(data), 'bytes_base64': base64.b64encode(data).decode('ascii')}
            self._bytes[sha] = data
        return {REF: sha, 'bytes': len(data)}

    def read(self, ref):
        if not isinstance(ref, dict) or set(ref) != {REF, 'bytes'}:
            raise ValueError('Exact shared original-byte reference required')
        sha = _sha(ref[REF]); item = self.blobs.get(sha)
        if item is None: raise ValueError('Shared original blob missing: ' + sha)
        if type(ref['bytes']) is not int or ref['bytes'] != item['bytes']:
            raise ValueError('Shared original byte count differs')
        if sha not in self._bytes:
            raw = base64.b64decode(item['bytes_base64'], validate=True)
            if len(raw) != item['bytes'] or hashlib.sha256(raw).hexdigest() != sha:
                raise ValueError('Shared original blob bytes changed')
            self._bytes[sha] = raw
        return self._bytes[sha]


def decode_original(value, reader=None):
    if isinstance(value, dict):
        if reader is None: raise ValueError('Shared original reference requires its bound pool')
        return reader(value)
    if not isinstance(value, str): raise ValueError('Original base64 or bound reference required')
    return base64.b64decode(value, validate=True)


def pool_portable(value, pool):
    """Replace only explicit base64 byte leaves; retain their context-local paths."""
    if isinstance(value, dict):
        out = {}
        for key, child in value.items():
            if key.endswith('bytes_base64'):
                if isinstance(child, dict) and REF not in child:
                    out[key] = {role: pool.add(decode_original(encoded, pool.read))
                                for role, encoded in child.items()}
                else: out[key] = pool.add(decode_original(child, pool.read))
            else: out[key] = pool_portable(child, pool)
        return out
    if isinstance(value, list): return [pool_portable(x, pool) for x in value]
    return copy.deepcopy(value)


def _refs(value):
    if isinstance(value, dict):
        if REF in value:
            if set(value) != {REF, 'bytes'}: raise ValueError('Conflicting shared reference shape')
            yield value
        else:
            for child in value.values(): yield from _refs(child)
    elif isinstance(value, list):
        for child in value: yield from _refs(child)


class SharedEvidenceContexts(Mapping):
    """Lazy context view. Reconstructed solely from portable original bytes."""
    def __init__(self, value):
        if (not isinstance(value, dict) or value.get('schema_version') != SCHEMA
                or set(value) != {'schema_version', 'contexts', 'blobs'}
                or not isinstance(value['contexts'], dict)):
            raise ValueError('Explicit shared semantic context collection required')
        self._descriptors = copy.deepcopy(value['contexts'])
        self._pool = BlobPool(value['blobs']); self._verified = {}
        from stage1d_bq_portable import ValidationSession
        self._bq_session = ValidationSession()
        # Check the finite graph before loading any certificate. Unused blobs
        # are also rejected so that serialization has an exact dependency set.
        refs = list(_refs(self._descriptors))
        if {r[REF] for r in refs} != set(self._pool.blobs):
            raise ValueError('Shared pool has missing or unreferenced blobs')
        for ref in refs:
            item = self._pool.blobs.get(_sha(ref[REF]))
            if item is None or type(ref['bytes']) is not int or ref['bytes'] != item['bytes']:
                raise ValueError('Shared original reference size differs')

    def __iter__(self): return iter(self._descriptors)
    def __len__(self): return len(self._descriptors)
    def __getitem__(self, key):
        if key not in self._verified:
            descriptor = self._descriptors[key]
            from weth_evidence import PORTABLE_SCHEMA, load_portable_evidence_context
            from stage1d_semantic_units import context_identity
            if isinstance(descriptor, dict) and descriptor.get('schema_version') == PORTABLE_SCHEMA:
                context = load_portable_evidence_context(descriptor, blob_reader=self._pool.read,
                                                         validation_session=self._bq_session)
            else: context = copy.deepcopy(descriptor)
            if context_identity(context) != key:
                raise ValueError('Shared evidence context identity differs from original bytes')
            self._verified[key] = context
        return self._verified[key]

    def serialized(self, keys=None):
        keys = sorted(self._descriptors if keys is None else keys)
        contexts = {key: copy.deepcopy(self._descriptors[key]) for key in keys}
        refs = {ref[REF] for ref in _refs(contexts)}
        return {'schema_version': SCHEMA, 'contexts': contexts,
                'blobs': {sha: copy.deepcopy(self._pool.blobs[sha]) for sha in sorted(refs)}}

    @property
    def validation_counts(self): return dict(self._bq_session.counts)


def context_view(value):
    if isinstance(value, SharedEvidenceContexts): return value
    if isinstance(value, dict) and value.get('schema_version') == SCHEMA:
        return SharedEvidenceContexts(value)
    return value


def serialize_contexts(value, keys=None):
    """No runtime seals or materialized original buffers enter model JSON."""
    value = context_view(value)
    if isinstance(value, SharedEvidenceContexts): return value.serialized(keys)
    if not isinstance(value, dict): raise ValueError('Explicit evidence context mapping required')
    # Preserve the accepted unpooled format by default. Producers opt into pool.
    return {k: copy.deepcopy(value[k]) for k in (value if keys is None else sorted(keys))}


def pack_contexts(contexts):
    """Storage adapter for existing finite portable contexts; verify at load."""
    if isinstance(contexts, SharedEvidenceContexts): return contexts.serialized()
    if isinstance(contexts, dict) and contexts.get('schema_version') == SCHEMA:
        return SharedEvidenceContexts(contexts).serialized()
    if not isinstance(contexts, dict): raise ValueError('Finite context mapping required')
    pool = BlobPool()
    descriptors = {k: pool_portable(v, pool) for k, v in contexts.items()}
    return {'schema_version': SCHEMA, 'contexts': descriptors, 'blobs': pool.blobs}


class SharedContextBuilder:
    """Incremental producer: no list of N fully embedded portable families."""
    def __init__(self):
        from stage1d_bq_portable import ValidationSession
        self.pool = BlobPool(); self.validation_session = ValidationSession()
        self._contexts = {}

    def add(self, portable):
        from weth_evidence import load_portable_evidence_context
        from stage1d_semantic_units import context_identity
        descriptor = pool_portable(portable, self.pool)
        verified = load_portable_evidence_context(descriptor, blob_reader=self.pool.read,
                                                 validation_session=self.validation_session)
        key = context_identity(verified)
        old = self._contexts.get(key)
        if old is not None and old != descriptor:
            raise ValueError('Conflicting descriptors for the same evidence context')
        self._contexts[key] = descriptor
        return key

    def serialized(self):
        refs = {r[REF] for r in _refs(self._contexts)}
        return {'schema_version': SCHEMA, 'contexts': copy.deepcopy(self._contexts),
                'blobs': {sha: copy.deepcopy(self.pool.blobs[sha]) for sha in sorted(refs)}}
