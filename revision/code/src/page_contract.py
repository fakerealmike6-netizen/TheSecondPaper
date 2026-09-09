"""Pure Dune execution-results contract validation; never performs requests.

Execution identity/state and result metadata are mandatory for this endpoint.
Optional next_offset/next_uri may be absent only at the terminal page. Optional
column metadata is checked when supplied; a previously supplied schema cannot
silently change. No next_uri is followed: it is only checked against the request.
"""
from __future__ import annotations
import json
from urllib.parse import urlsplit, parse_qsl

class PageContractError(ValueError):
    pass

def exact_count(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PageContractError(name + ' must be a nonnegative JSON integer')
    return value

def initial_progress(total=None, schema=None):
    return dict(next_offset=0, observed_rows=0, complete=False, total=total,
                schema=schema, successful_pages=0, page_size=None, row_keys=None)

def schema_of(metadata):
    names=metadata.get('column_names');types=metadata.get('column_types')
    if names is not None and (not isinstance(names,list) or not all(isinstance(n,str) for n in names) or len(names)!=len(set(names))):
        raise PageContractError('Invalid column names')
    if types is not None and (not isinstance(types,list) or not all(isinstance(t,str) for t in types)):
        raise PageContractError('Invalid column types')
    if names is not None and types is not None and len(names)!=len(types):
        raise PageContractError('Column names/types length mismatch')
    return {k:metadata[k] for k in ('column_names','column_types') if k in metadata}

def validate_next_uri(uri, execution_id, next_offset, limit, parameters):
    if uri is None:return
    if not isinstance(uri,str) or not uri or next_offset is None:
        raise PageContractError('Unexpected terminal or malformed next_uri')
    try:
        parsed=urlsplit(uri)
        if parsed.scheme!='https' or parsed.hostname!='api.dune.com' or parsed.port not in (None,443) or parsed.username is not None or parsed.password is not None or parsed.fragment:
            raise PageContractError('next_uri is not the exact official HTTPS origin')
        if parsed.path!='/api/v1/execution/'+execution_id+'/results':
            raise PageContractError('next_uri execution/endpoint mismatch')
        items=parse_qsl(parsed.query,keep_blank_values=True,strict_parsing=True)
    except (ValueError,TypeError) as exc:raise PageContractError('Malformed next_uri') from exc
    query=dict(items)
    if len(query)!=len(items):raise PageContractError('Duplicate next_uri parameter')
    allowed={k:str(v) for k,v in parameters.items() if k!='offset'}
    allowed['offset']=str(next_offset)
    # The API can omit limit in its next_uri; we still use our frozen limit.
    if 'limit' not in query:allowed.pop('limit',None)
    if query!=allowed:raise PageContractError('next_uri contains changed or unsafe parameters')

def validate_page(page, *, execution_id, offset, limit, progress=None,
                  status_metadata=None, receipt=None, parameters=None):
    offset=exact_count(offset,'offset');limit=exact_count(limit,'limit')
    if not 1<=limit<=1000:raise PageContractError('Page limit outside supported bounds')
    previous=dict(progress or initial_progress())
    if previous.get('complete') or previous.get('next_offset')!=offset:
        raise PageContractError('Overlapping, noncontiguous or post-terminal page')
    if previous.get('page_size') not in (None,limit):raise PageContractError('Page size changed')
    if receipt is not None:
        if receipt.get('http_status')!=200 or receipt.get('error_class'):
            raise PageContractError('Transport did not certify an HTTP200 result')
        if receipt.get('execution_id') not in (None,execution_id):raise PageContractError('Receipt execution mismatch')
        params=receipt.get('parameters')
        if params is not None and params!=dict(parameters or {'limit':limit,'offset':offset}):raise PageContractError('Receipt parameters mismatch')
    if not isinstance(page,dict) or page.get('execution_id')!=execution_id:
        raise PageContractError('Missing or mismatched execution identity')
    if page.get('state')!='QUERY_STATE_COMPLETED':raise PageContractError('Missing or incomplete execution state')
    result=page.get('result')
    if not isinstance(result,dict):raise PageContractError('Missing result object')
    rows=result.get('rows');metadata=result.get('metadata')
    if not isinstance(rows,list) or not all(isinstance(row,dict) for row in rows):raise PageContractError('Rows must be a list of objects')
    if len(rows)>limit:raise PageContractError('Returned page exceeds requested limit')
    if not isinstance(metadata,dict):raise PageContractError('Missing result metadata')
    total=exact_count(metadata.get('total_row_count'),'total_row_count')
    if exact_count(metadata.get('row_count'),'row_count')!=len(rows):raise PageContractError('Page row_count mismatch')
    if previous.get('total') not in (None,total):raise PageContractError('Total result row count changed')
    status_metadata=status_metadata or {}
    if status_metadata.get('total_row_count') is not None and exact_count(status_metadata['total_row_count'],'status total_row_count')!=total:
        raise PageContractError('Status and page total_row_count differ')
    schema=schema_of(metadata);status_schema=schema_of(status_metadata)
    old_schema=previous.get('schema') or {}
    for source in (old_schema,status_schema):
        for key,val in source.items():
            if key in schema and schema[key]!=val:raise PageContractError('Result schema changed')
    if old_schema and any(k not in schema for k in old_schema):raise PageContractError('Previously supplied page schema disappeared')
    schema=old_schema|schema
    row_keys=previous.get('row_keys')
    for row in rows:
        keys=sorted(row)
        if row_keys is not None and keys!=row_keys:raise PageContractError('Row schema changed')
        row_keys=keys
        names=schema.get('column_names') or status_schema.get('column_names')
        if names is not None and set(keys)!=set(names):raise PageContractError('Row keys differ from declared schema')
    end=offset+len(rows)
    if end>total:raise PageContractError('Page exceeds full result count')
    nxt=page.get('next_offset')
    if nxt is not None:
        nxt=exact_count(nxt,'next_offset')
        if not rows or nxt!=end or nxt<=offset:raise PageContractError('Noncontiguous or empty-loop cursor')
    terminal=end==total
    if terminal and (nxt is not None or page.get('next_uri') is not None):raise PageContractError('Terminal page still advertises continuation')
    if not terminal and (nxt is None or not rows):raise PageContractError('Missing continuation before total rows reached')
    validate_next_uri(page.get('next_uri'),execution_id,nxt,limit,parameters or {'limit':limit,'offset':offset})
    return dict(next_offset=None if terminal else nxt, observed_rows=end,
                complete=terminal,total=total,schema=schema,row_keys=row_keys,
                successful_pages=previous.get('successful_pages',0)+1,page_size=limit)
