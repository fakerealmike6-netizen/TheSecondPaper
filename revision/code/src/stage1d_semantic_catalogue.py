"""Load only the current, source-bound finite semantic evidence catalogue.

Physical certificates are shared. Collector creates query-specific membership;
the catalogue never imports query answers, prior LP results or source balances.
"""
from pathlib import Path
import hashlib,json

SCHEMA='stage1d-finite-semantic-catalogue-v1'
def _read(path):return json.loads(Path(path).read_text(encoding='utf8'))
def _bound(work,ref):
    rel=ref.get('path')
    if not isinstance(rel,str) or '\\' in rel or ':' in rel or rel.startswith('/') or any(p in ('','..','.') for p in rel.split('/')):
        raise ValueError('Semantic catalogue requires safe relative evidence paths')
    path=(work/rel).resolve()
    if not path.is_relative_to(work) or not path.is_file() or path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest()!=ref.get('sha256'):
        raise ValueError('Semantic catalogue evidence changed or missing')
    return _read(path)
def load_current_resolver(work):
    from stage1d_semantic_units import FiniteSemanticResolver
    work=Path(work).resolve();pointer=work/'private/stage1d_semantics/CURRENT.json'
    if not pointer.exists():return None
    catalog=_read(pointer)
    if catalog.get('schema_version')!=SCHEMA or catalog.get('evidence_kind')!='REAL_CHAIN':
        raise ValueError('Only explicit real-chain semantic catalogue can be adopted')
    from stage1d_shared_evidence import context_view,SCHEMA as SHARED_SCHEMA
    units=[];contexts={};seen=set()
    shared=_bound(work,catalog['shared_evidence_context']) if 'shared_evidence_context' in catalog else None
    if shared is not None and shared.get('schema_version')!=SHARED_SCHEMA:
        raise ValueError('Explicit shared semantic evidence catalogue required')
    for ref in catalog.get('instances',[]):
        unit=_bound(work,ref['unit'])
        context=_bound(work,ref['evidence_context']) if shared is None else None
        if shared is not None and 'evidence_context' in ref:
            raise ValueError('A catalogue must choose one explicit context inventory')
        key=ref['context_id']
        if unit['unit_id'] in seen:raise ValueError('Duplicate physical semantic certificate')
        seen.add(unit['unit_id'])
        if key in contexts and contexts[key]!=context:raise ValueError('Conflicting physical evidence context')
        contexts[key]=context;units.append(unit)
    if shared is not None:
        if set(contexts)!=set(shared['contexts']):raise ValueError('Shared catalogue context membership differs')
        contexts=shared
    return FiniteSemanticResolver(units,contexts,capability_gate=_bound(work,catalog['capability_gate']),work_root=work)
