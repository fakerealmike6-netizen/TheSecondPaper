"""Offline request/response binding for WETH evidence, with explicit trust roots.

The separate acquisition catalogue is a trusted input from the authorized
collector/reviewer, NOT an assertion manufactured by verify_component callers.
Hash integrity does not itself authenticate a provider. Real eligibility also
requires a catalogue-pinned verified-source response and semantic review.
"""
from dataclasses import dataclass
import base64
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit


_TOKEN = object()
ROLES = ('transaction', 'receipt', 'internal', 'trace', 'historical_code', 'source', 'deployment_runtime', 'header')


def payload_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=False).encode()).hexdigest()


@dataclass(frozen=True)
class VerifiedEvidenceContext:
    kind: str
    records: dict
    source_review: dict
    manifest_sha256: str
    catalogue_sha256: str
    _token: object
    _integrity_sha256: str


def is_verified_context(value):
    return (isinstance(value, VerifiedEvidenceContext) and value._token is _TOKEN
            and value._integrity_sha256 == payload_hash({'kind': value.kind,
                'records': value.records, 'source_review': value.source_review}))


def context_records(value):
    if is_verified_context(value):
        return value.kind, value.records
    if isinstance(value, dict) and value.get('kind') == 'SYNTHETIC':
        return 'SYNTHETIC', value.get('records', {})
    return 'UNVERIFIED', {}


def audit_bindings(policy, transaction, receipt, internal, trace, historical_code,
                   source_attestation, context):
    """Reject supplied contradictions; distinguish absent binding from conflict."""
    expected_tx = policy['tx_hash'].lower()
    expected_block = int(policy['block_number'])
    expected_contract = policy['contract'].lower()
    conflicts, gaps, bound = [], [], {}
    kind, records = context_records(context)

    def integer(value):
        if isinstance(value, (bool, float)):
            raise ValueError('Exact integer identity required')
        return int(value, 16) if isinstance(value, str) and value.startswith('0x') else int(value)

    def unwrapped(value):
        return value.get('result', value) if isinstance(value, dict) else value

    tx, rc = unwrapped(transaction), unwrapped(receipt)
    def block_hashes(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key in ('blockHash', 'block_hash') and child is not None:
                    yield str(child).lower()
                elif isinstance(child, (dict, list)):
                    yield from block_hashes(child)
        elif isinstance(value, list):
            for child in value:
                yield from block_hashes(child)
    hashes = set().union(*(set(block_hashes(obj)) for obj in
                         (policy, transaction, receipt, internal, trace, historical_code, source_attestation)))
    if len(hashes) > 1:
        conflicts.append('transaction_receipt:block_hash')
    expected_hash = next(iter(hashes)) if len(hashes) == 1 else None

    def ids(obj, location, *, hash_is_tx=False, contract=False, log=False):
        if not isinstance(obj, dict):
            return
        checks = [(('chainId', 'chain_id', 'chainid'), 1, integer),
                  (('blockNumber', 'block_number', 'historical_block_number'), expected_block, integer),
                  (('transactionHash', 'transaction_hash', 'txHash', 'tx_hash') + (('hash',) if hash_is_tx else ()), expected_tx, lambda x: str(x).lower())]
        if expected_hash:
            checks.append((('blockHash', 'block_hash'), expected_hash, lambda x: str(x).lower()))
        if contract:
            checks.append((('contract', 'address'), expected_contract, lambda x: str(x).lower()))
        for names, expected, convert in checks:
            for name in names:
                if obj.get(name) is not None:
                    try:
                        same = convert(obj[name]) == expected
                    except (ValueError, TypeError):
                        same = False
                    if not same:
                        conflicts.append(location + ':' + name)
        if log and obj.get('removed') is True:
            conflicts.append(location + ':removed')

    ids(policy, 'policy', contract=True)
    ids(transaction, 'transaction_envelope', hash_is_tx=True)
    ids(tx, 'transaction', hash_is_tx=True)
    ids(receipt, 'receipt_envelope')
    ids(rc, 'receipt')
    for index, log in enumerate(rc.get('logs', [])):
        ids(log, 'receipt.log:' + str(index), log=True)
    internal = internal.get('response', internal) if isinstance(internal, dict) else internal
    ids(internal, 'internal_response')
    for index, row in enumerate(internal.get('result', [])):
        ids(row, 'internal:' + str(index), hash_is_tx=True)
    if trace is not None:
        ids(trace, 'trace_envelope', hash_is_tx=True)
        todo = [('trace', unwrapped(trace))]
        while todo:
            name, frame = todo.pop()
            ids(frame, name, hash_is_tx=True)
            if not isinstance(frame, dict):
                conflicts.append(name + ':not_object')
                continue
            for i, log in enumerate(frame.get('logs', [])):
                ids(log, name + '.log:' + str(i), log=True)
                # A Deposit with matching semantic fields but a different exact
                # receipt locator is contradictory, not evidence of this event.
                for observed in rc.get('logs', []):
                    if (log.get('address', '').lower() == observed.get('address', '').lower()
                            and log.get('topics') == observed.get('topics')
                            and log.get('data', '').lower() == observed.get('data', '').lower()
                            and integer(observed.get('logIndex', -1)) == policy['deposit_log_index']):
                        for locator in ('logIndex', 'log_index'):
                            if log.get(locator) is not None and integer(log[locator]) != policy['deposit_log_index']:
                                conflicts.append(name + '.log:' + str(i) + ':' + locator)
            todo.extend((name + '.calls:' + str(i), child) for i, child in enumerate(frame.get('calls', [])))
    ids(historical_code, 'historical_code', contract=True)
    ids(source_attestation, 'source_attestation', contract=True)

    payloads = {'transaction': tx, 'receipt': rc, 'internal': internal,
                'trace': unwrapped(trace), 'historical_code': unwrapped(historical_code)}
    methods = {'transaction': 'eth_getTransactionByHash', 'receipt': 'eth_getTransactionReceipt',
               'trace': 'debug_traceTransaction', 'historical_code': 'eth_getCode'}
    for role, payload in payloads.items():
        record = records.get(role)
        if not record:
            bound[role] = False
            continue
        ids(record, 'binding.' + role)
        req = record.get('request', {})
        valid = record.get('payload_sha256') == payload_hash(payload) and record.get('chain_id') == 1
        if record.get('payload_sha256') != payload_hash(payload):
            conflicts.append('binding.' + role + ':payload_sha256')
        if role in {'trace','internal'} and record.get('source_type')=='CLASSIC_BIGQUERY_FULL_TRANSACTION_FAMILY_V1':
            valid &= (is_verified_context(context) and req.get('method')=='CLASSIC_BIGQUERY_ARCHIVED_JOB_CHAIN'
                      and req.get('tx_hash')==expected_tx and req.get('block_number')==expected_block
                      and bool(req.get('preparation_sha256')) and bool(req.get('jobs'))
                      and record.get('archive_binding',{}).get('source_type')=='CLASSIC_BIGQUERY_FULL_TRANSACTION_FAMILY_V1'
                      and record.get('archive_binding',{}).get('full_context_claimed') is False)
        elif role in {'trace', 'internal'} and record.get('source_type') == 'DUNE_ARCHIVED_SQL_COMPLETE_TRANSACTION_TRACE':
            # This is an archived Dune SQL request chain, not an invented RPC
            # or explorer request. It can only be sealed by the finite adapter.
            valid &= (is_verified_context(context)
                      and req.get('method') == 'DUNE_ARCHIVED_SQL_EXECUTION'
                      and req.get('tx_hash') == expected_tx
                      and req.get('block_number') == expected_block
                      and bool(req.get('execution_id')) and bool(req.get('sql_sha256'))
                      and record.get('archive_binding', {}).get('page_contract', {}).get('complete') is True)
        elif role in methods:
            params = req.get('params', [])
            valid &= req.get('method') == methods[role] and isinstance(params, list) and len(params) >= 1
            if valid and role != 'historical_code':
                valid &= str(params[0]).lower() == expected_tx
            if valid and role == 'historical_code':
                valid &= str(params[0]).lower() == expected_contract and len(params) >= 2
                if valid:
                    tag = params[1]
                    if isinstance(tag, dict):
                        valid &= expected_hash is not None and str(tag.get('blockHash', '')).lower() == expected_hash
                    else:
                        try:
                            valid &= integer(tag) == expected_block
                        except (ValueError, TypeError):
                            valid = False
        else:
            params = req.get('params', {})
            valid &= isinstance(params, dict) and params.get('action') == 'txlistinternal' and str(params.get('txhash', '')).lower() == expected_tx and str(params.get('chainid')) == '1'
        if not valid:
            conflicts.append('binding.' + role + ':request_identity')
        bound[role] = bool(valid)
    # A request-bound trace need not contain fields the provider never emits.
    trace_identity = unwrapped(trace) if isinstance(unwrapped(trace), dict) else {}
    embedded_trace_tx = any(str(trace_identity.get(k, '')).lower() == expected_tx
                            for k in ('transactionHash', 'txHash', 'tx_hash', 'hash'))
    trace_bound = bound.get('trace', False) or (kind == 'SYNTHETIC' and embedded_trace_tx)
    if trace is not None and not trace_bound:
        gaps.append('TRACE_REQUEST_RESPONSE_BINDING_MISSING')
    source_verified = False
    if is_verified_context(context) and context.source_review.get('source_validated'):
        source = records.get('source', {})
        req = source.get('request', {}).get('params', {})
        review = context.source_review
        if source.get('source_type') == 'SOURCIFY_V2_VERIFIED_SOURCE':
            from weth_source_adapter_r3 import source_request_matches
            source_request_bound = (source_request_matches(source.get('request', {}), expected_contract)
                                    and review.get('source_validation_method') == 'SOURCIFY_V2_VERIFIED_COMPILATION_PLUS_EXACT_LOCAL_BYTECODE_COMPARISON')
        else:
            source_request_bound = (req.get('action') == 'getsourcecode'
                                    and str(req.get('address', '')).lower() == expected_contract
                                    and str(req.get('chainid')) == '1')
        source_verified = (source.get('chain_id') == 1 and source_request_bound
                           and review.get('chain_id') == 1
                           and str(review.get('contract', '')).lower() == expected_contract
                           and isinstance(payloads['historical_code'], str)
                           and payloads['historical_code'].startswith('0x')
                           and review.get('runtime_code_sha256') == hashlib.sha256(bytes.fromhex(payloads['historical_code'][2:])).hexdigest())
        if not source_verified:
            conflicts.append('verified_source:chain_contract_runtime_binding')
    equivalent = None
    if is_verified_context(context) and source_verified and bound.get('trace') and bound.get('receipt'):
        proof = context.source_review.get('equivalent_deposit_binding', {})
        if proof.get('proof_valid') is True:
            expected = {'transaction': payload_hash(tx), 'receipt': payload_hash(rc),
                        'trace': payload_hash(unwrapped(trace)), 'internal': payload_hash(internal)}
            if (proof.get('bound_payloads') == expected and proof.get('tx_hash') == expected_tx
                    and proof.get('block_number') == expected_block
                    and proof.get('contract') == expected_contract
                    and proof.get('actual_call_tree_path') == policy.get('input_trace_address')
                    and proof.get('log_index') == policy.get('deposit_log_index')
                    and proof.get('caller') == policy.get('credited_address', '').lower()
                    and proof.get('amount_raw') == str(policy.get('amount_raw'))):
                equivalent = proof
            else:
                conflicts.append('equivalent_binding:payload_or_fixed_scope')
    real_ready = is_verified_context(context) and source_verified and all(bound.values()) and not conflicts
    if not real_ready:
        gaps.append('REAL_ACQUISITION_AND_VERIFIED_SOURCE_PROVENANCE_UNAVAILABLE')
    result = {'evidence_kind': kind, 'conflicts': sorted(set(conflicts)), 'gaps': sorted(set(gaps)),
            'request_bound': bound, 'trace_bound': trace_bound,
            'source_verified': source_verified, 'real_provenance_ready': real_ready}
    if equivalent is not None:
        result['equivalent_deposit_binding'] = equivalent
    return result


def extend_dune_evidence_context(context, manifest_path, policy):
    """Validate genuine archived Dune sources before extending a trusted root."""
    import copy
    from weth_trace_adapter_r4 import validate_dune_bundle, native_rows, prove_unique_emitter, SOURCE_TYPE
    if not is_verified_context(context) or context.kind != 'REAL_CHAIN':
        raise ValueError('Validated real acquisition root required')
    required = {'transaction', 'receipt', 'source', 'historical_code'}
    if not required <= set(context.records):
        raise ValueError('Existing request-bound transaction/receipt/source/runtime required')
    bundle = validate_dune_bundle(manifest_path, policy)
    return _extend_validated_dune_context(context,bundle,policy)


def _extend_validated_dune_context(context,bundle,policy):
    import copy
    from weth_trace_adapter_r4 import native_rows,prove_unique_emitter,SOURCE_TYPE
    if not is_verified_context(context) or context.kind!='REAL_CHAIN':
        raise ValueError('Validated real acquisition context required before Dune extension')
    records, review = copy.deepcopy(context.records), copy.deepcopy(context.source_review)
    archive = {k: v for k, v in bundle.items() if k not in {'rows', 'tree'}}
    request = {'method': 'DUNE_ARCHIVED_SQL_EXECUTION', 'execution_id': bundle['execution_id'],
        'sql_sha256': bundle['sql_sha256'], 'tx_hash': policy['tx_hash'].lower(),
        'block_number': policy['block_number']}
    for role, payload in [('trace', bundle['tree']), ('internal', native_rows(bundle['rows']))]:
        records[role] = {'role': role, 'source_type': SOURCE_TYPE, 'evidence_kind': 'REAL_CHAIN',
            'status': 'SUCCESS_VALIDATED', 'origin_url': 'https://api.dune.com', 'chain_id': 1,
            'request': request, 'payload_sha256': payload_hash(payload), 'payload': payload,
            'archive_binding': archive}
    # A failed unique-emitter proof leaves the trace/native-input bindings
    # usable and the twelfth scientific predicate explicitly unavailable.
    try:
        proof = prove_unique_emitter(policy, bundle['rows'], records['transaction']['payload'],
                                     records['receipt']['payload'], review)
        proof['bound_payloads'] = {role: payload_hash(records[role]['payload'])
                                  for role in ('transaction', 'receipt', 'trace', 'internal')}
        review['equivalent_deposit_binding'] = proof
        review.pop('equivalent_binding_gap', None)
    except (ValueError, KeyError, TypeError) as exc:
        review.pop('equivalent_deposit_binding', None)
        review['equivalent_binding_gap'] = str(exc)
    return VerifiedEvidenceContext('REAL_CHAIN', records, review,
        payload_hash({'original_manifest': context.manifest_sha256, 'dune_manifest': bundle['manifest_sha256']}),
        context.catalogue_sha256, _TOKEN,
        payload_hash({'kind': 'REAL_CHAIN', 'records': records, 'source_review': review}))


def load_evidence_context(bundle_path, acquisition_catalogue_path):
    """Verify local artifacts against a separately maintained acquisition root.

    Each catalogue record pins file bytes, extracted payload, request identity,
    provider origin, evidence kind and successful acquisition. The bundle only
    selects record IDs; it cannot override trusted metadata or certify sources.
    No source URL/hash/code argument alone can construct this trusted context.
    """
    bundle_path, catalogue_path = Path(bundle_path).resolve(), Path(acquisition_catalogue_path).resolve()
    bundle_bytes, catalogue_bytes = bundle_path.read_bytes(), catalogue_path.read_bytes()
    def artifact_reader(relative):
        relative = Path(relative)
        artifact = (catalogue_path.parent / relative).resolve()
        if relative.is_absolute() or not artifact.is_relative_to(catalogue_path.parent) or not artifact.is_file():
            raise ValueError('Artifact outside acquisition directory')
        return artifact.read_bytes()
    return _load_evidence_documents(json.loads(bundle_bytes), json.loads(catalogue_bytes), artifact_reader,
        hashlib.sha256(bundle_bytes).hexdigest(), hashlib.sha256(catalogue_bytes).hexdigest())


def _load_evidence_documents(bundle, catalogue, artifact_reader, bundle_sha256, catalogue_sha256):
    """The same trust-root checks for files and portable original-byte maps."""
    if catalogue.get('schema_version') != 'weth-acquisition-catalogue-1' or catalogue.get('evidence_kind') != 'REAL_CHAIN':
        raise ValueError('A REAL_CHAIN acquisition catalogue is required')
    if not catalogue.get('acquisition_run_id') or not catalogue.get('reviewer_record_id'):
        raise ValueError('Acquisition/reviewer provenance is absent')
    records = {}
    for role in ROLES:
        record_id = bundle.get('records', {}).get(role)
        if record_id is None:
            continue
        record = dict(catalogue.get('records', {}).get(record_id, {}))
        if record.get('role') != role or record.get('evidence_kind') != 'REAL_CHAIN' or record.get('status') != 'SUCCESS_VALIDATED':
            raise ValueError('Missing, synthetic or unsuccessful acquisition record: ' + role)
        origin = urlsplit(record.get('origin_url', ''))
        if origin.scheme != 'https' or not origin.hostname or origin.username or origin.password or origin.query or origin.fragment:
            raise ValueError('Public credential-free HTTPS origin required')
        if origin.hostname.endswith('.invalid') or 'synthetic' in json.dumps(record).lower():
            raise ValueError('Synthetic provenance cannot be promoted to real evidence')
        if origin.hostname not in catalogue.get('approved_provider_hosts', []):
            raise ValueError('Provider not approved in acquisition catalogue')
        content = artifact_reader(record.get('artifact_path', ''))
        if hashlib.sha256(content).hexdigest() != record.get('artifact_sha256'):
            raise ValueError('Acquisition artifact bytes changed')
        raw = json.loads(content)
        if not isinstance(raw, dict) or raw.get('request') != record.get('request'):
            raise ValueError('Saved request differs from acquisition record')
        response = raw.get('response')
        if raw.get('http_status') != 200 or response is None:
            raise ValueError('Successful request/response envelope required')
        if role in {'transaction', 'receipt', 'trace', 'historical_code', 'deployment_runtime', 'header'}:
            if (response.get('error') or 'result' not in response or response.get('id') != raw['request'].get('id')
                    or type(response.get('id')) is not type(raw['request'].get('id'))):
                raise ValueError('RPC response does not bind to saved request')
            payload = response['result']
        else:
            payload = response
        if payload_hash(payload) != record.get('payload_sha256'):
            raise ValueError('Extracted payload differs from acquisition record')
        record['payload'] = payload
        records[role] = record
    if not records:
        raise ValueError('At least one validated acquisition record is required')
    def seal(review):
        return VerifiedEvidenceContext('REAL_CHAIN', records, review,
            bundle_sha256, catalogue_sha256, _TOKEN,
            payload_hash({'kind': 'REAL_CHAIN', 'records': records, 'source_review': review}))
    if ({'source', 'historical_code'} <= set(records)
            and records['source'].get('source_type') == 'SOURCIFY_V2_VERIFIED_SOURCE'):
        from weth_source_adapter_r3 import validate_sourcify_records
        review = validate_sourcify_records(records, dict(catalogue.get('source_review', {})), bundle['records']['source'])
        return seal(review)
    if not {'source', 'historical_code', 'deployment_runtime'} <= set(records):
        # A valid trace receipt can improve call binding before source/code
        # arrives, while the real certification gate remains closed.
        return seal({'source_validated': False,
                     'missing_records': sorted(set(ROLES)-set(records))})
    review = dict(catalogue.get('source_review', {}))
    source = records['source']
    response = source['payload']
    # Deliberately narrow supported verified-source response, not arbitrary URL
    # attestation: successful Etherscan-compatible getsourcecode plus a pinned
    # reviewer record tying this source response to the deployed runtime.
    source_rows = response.get('result', []) if isinstance(response, dict) else []
    if source.get('source_type') != 'ETHERSCAN_VERIFIED_SOURCE' or response.get('status') != '1' or len(source_rows) != 1:
        raise ValueError('Supported verified-source response required')
    row = source_rows[0]
    source_text = row.get('SourceCode', '')
    if not source_text or not row.get('CompilerVersion') or row.get('Proxy') != '0' or row.get('ABI') in (None, '', 'Contract source code not verified'):
        raise ValueError('Source response is not verified contract source')
    if not review.get('review_id') or review.get('source_record_id') != bundle['records']['source']:
        raise ValueError('Independent source semantic review absent')
    if review.get('source_text_sha256') != hashlib.sha256(source_text.encode()).hexdigest():
        raise ValueError('Reviewed source differs from verified source response')
    if review.get('semantics') != 'CANONICAL_WETH9_DEPOSIT_NO_FEE_NO_REFUND' or review.get('deployment_runtime_match_evidence_id') != bundle['records']['deployment_runtime']:
        raise ValueError('Deployment runtime and semantic review required')
    # A separate successful historical code acquisition is always required.
    code = records['historical_code']['payload']
    if not isinstance(code, str) or not code.startswith('0x') or len(code) <= 2:
        raise ValueError('Historical runtime is unavailable')
    code_sha = hashlib.sha256(bytes.fromhex(code[2:])).hexdigest()
    if review.get('runtime_code_sha256') != code_sha:
        raise ValueError('Reviewed deployment runtime differs from historical bytes')
    deployment = records['deployment_runtime']
    request = deployment['request']
    params = request.get('params', [])
    if (deployment.get('chain_id') != 1 or request.get('method') != 'eth_getCode'
            or len(params) != 2 or str(params[0]).lower() != str(review.get('contract', '')).lower()
            or not isinstance(params[1], str) or not params[1].startswith('0x')
            or int(params[1], 16) != review.get('verification_block_number')
            or deployment['payload'].lower() != code.lower()):
        raise ValueError('Referenced deployment runtime evidence does not match reviewed source/history')
    review['source_validated'] = True
    return seal(review)


PORTABLE_SCHEMA = 'stage1d-weth-portable-acquisition-context-v1'


def _portable_name(value):
    """Portable relative names cannot reference a host path on either OS."""
    if not isinstance(value,str) or not value or '\\' in value or ':' in value or value.startswith('/') or any(x in ('','..','.') for x in value.split('/')):
        raise ValueError('Unsafe portable evidence name')
    return value


def export_evidence_context(bundle_path, acquisition_catalogue_path, *, dune_manifest_path=None, dune_policy=None,
                            bigquery_family=None,bigquery_policy=None,blob_pool=None,validation_session=None):
    """Export selected original acquisition bytes, after the ordinary validation.

    The receiving package's frozen input inventory pins this portable object;
    provenance is still the separately maintained acquisition catalogue.
    """
    load_evidence_context(bundle_path, acquisition_catalogue_path)
    def encode(raw):
        return blob_pool.add(raw) if blob_pool is not None else base64.b64encode(raw).decode('ascii')
    bundle_path, catalogue_path = Path(bundle_path).resolve(), Path(acquisition_catalogue_path).resolve()
    bb,cb=bundle_path.read_bytes(),catalogue_path.read_bytes();bundle=json.loads(bb);catalogue=json.loads(cb)
    blobs={}
    for record_id in bundle.get('records',{}).values():
        record=catalogue['records'][record_id];relative=Path(record['artifact_path'])
        name=_portable_name(relative.as_posix())
        path=(catalogue_path.parent/relative).resolve()
        if not path.is_relative_to(catalogue_path.parent):raise ValueError('Evidence path escaped catalogue')
        content=path.read_bytes()
        blobs[name]={'sha256':hashlib.sha256(content).hexdigest(),'bytes_base64':encode(content)}
    result={'schema_version':PORTABLE_SCHEMA,'evidence_kind':'REAL_CHAIN',
        'bundle_bytes_base64':encode(bb),'bundle_sha256':hashlib.sha256(bb).hexdigest(),
        'catalogue_bytes_base64':encode(cb),'catalogue_sha256':hashlib.sha256(cb).hexdigest(),
        'artifacts':blobs,'trust_root':'Separately maintained acquisition catalogue pinned by frozen package input inventory'}
    if dune_manifest_path is not None and bigquery_family is not None:
        raise ValueError('Choose one exact trace family for this evidence context')
    if dune_manifest_path is not None:
        from weth_trace_adapter_r4 import validate_dune_bundle
        validate_dune_bundle(dune_manifest_path,dune_policy)
        mp=Path(dune_manifest_path).resolve();mb=mp.read_bytes();manifest=json.loads(mb)
        role_bytes={}
        for role,item in manifest['files'].items():
            path=(mp.parent/item['path']).resolve()
            if not path.is_relative_to(mp.parent):raise ValueError('Dune artifact escaped original closure')
            role_bytes[role]=encode(path.read_bytes())
        result['dune_extension']={'manifest_bytes_base64':encode(mb),
            'manifest_sha256':hashlib.sha256(mb).hexdigest(),'policy':dune_policy,'role_bytes_base64':role_bytes}
    if bigquery_family is not None:
        result['bigquery_extension']={'family':bigquery_family,'policy':bigquery_policy}
    # Export is not merely serialization: verify the exact portable contract.
    load_portable_evidence_context(result,blob_reader=blob_pool.read if blob_pool is not None else None,
                                  validation_session=validation_session)
    return result


def load_portable_evidence_context(portable, *, blob_reader=None, validation_session=None):
    """Recheck original bytes/requests/source/runtime; never deserialize a seal."""
    if not isinstance(portable,dict) or portable.get('schema_version')!=PORTABLE_SCHEMA or portable.get('evidence_kind')!='REAL_CHAIN':
        raise ValueError('Explicit real portable acquisition context required')
    from stage1d_shared_evidence import decode_original
    def decode(key):
        data=decode_original(portable[key+'_bytes_base64'],blob_reader)
        if hashlib.sha256(data).hexdigest()!=portable[key+'_sha256']:raise ValueError('Portable trust-root byte hash differs')
        return data
    bb,cb=decode('bundle'),decode('catalogue')
    def artifact_reader(relative):
        name=_portable_name(str(relative).replace('\\','/'))
        item=portable.get('artifacts',{}).get(name)
        if not isinstance(item,dict):raise ValueError('Selected portable acquisition artifact missing')
        data=decode_original(item['bytes_base64'],blob_reader)
        if hashlib.sha256(data).hexdigest()!=item['sha256']:raise ValueError('Portable acquisition bytes changed')
        return data
    context=_load_evidence_documents(json.loads(bb),json.loads(cb),artifact_reader,
        hashlib.sha256(bb).hexdigest(),hashlib.sha256(cb).hexdigest())
    if 'dune_extension' in portable and 'bigquery_extension' in portable:
        raise ValueError('A portable instance must bind one exact trace family')
    if 'dune_extension' in portable:
        from weth_trace_adapter_r4 import validate_dune_documents
        ext=portable['dune_extension'];mb=decode_original(ext['manifest_bytes_base64'],blob_reader)
        if hashlib.sha256(mb).hexdigest()!=ext['manifest_sha256']:raise ValueError('Dune manifest original bytes changed')
        data={k:decode_original(v,blob_reader) for k,v in ext['role_bytes_base64'].items()}
        bundle=validate_dune_documents(json.loads(mb),data,ext['policy'],ext['manifest_sha256'])
        context=_extend_validated_dune_context(context,bundle,ext['policy'])
    if 'bigquery_extension' in portable:
        from stage1d_bq_portable import load_bigquery_family
        ext=portable['bigquery_extension'];family=load_bigquery_family(ext['family'],blob_reader=blob_reader,validation_session=validation_session)
        context=_extend_validated_bigquery_context(context,family,ext['policy'])
    return context


def _extend_validated_bigquery_context(context,family,policy):
    import copy
    from stage1d_bq_portable import SOURCE_TYPE,semantic_materials
    if not is_verified_context(context) or context.kind!='REAL_CHAIN':
        raise ValueError('A separately validated acquisition root is required')
    if not {'transaction','receipt','header','source','historical_code'}<=set(context.records):
        raise ValueError('Original tx/receipt/header/source/historical-code records required')
    materials=semantic_materials(family,policy)
    records,review=copy.deepcopy(context.records),copy.deepcopy(context.source_review)
    archive={k:v for k,v in family.items() if k not in ('rows','normalized')}
    request={'method':'CLASSIC_BIGQUERY_ARCHIVED_JOB_CHAIN','tx_hash':policy['tx_hash'],
        'block_number':policy['block_number'],'preparation_sha256':family['preparation_dependency']['sha256'],
        'jobs':family['jobs']}
    for role,payload in materials.items():
        records[role]={'role':role,'source_type':SOURCE_TYPE,'evidence_kind':'REAL_CHAIN',
            'status':'SUCCESS_VALIDATED','origin_url':'https://bigquery.googleapis.com','chain_id':1,
            'request':request,'payload_sha256':payload_hash(payload),'payload':payload,'archive_binding':archive}
    # The finite instance verifier recomputes unique emitter/call/log matching
    # directly. Never carry a Deposit-only proof into a withdrawal instance.
    review.pop('equivalent_deposit_binding',None);review.pop('equivalent_binding_gap',None)
    return VerifiedEvidenceContext('REAL_CHAIN',records,review,
        payload_hash({'original_manifest':context.manifest_sha256,'bigquery_family':archive}),
        context.catalogue_sha256,_TOKEN,
        payload_hash({'kind':'REAL_CHAIN','records':records,'source_review':review}))
