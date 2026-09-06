"""Offline request/response binding for WETH evidence, with explicit trust roots.

The separate acquisition catalogue is a trusted input from the authorized
collector/reviewer, NOT an assertion manufactured by verify_component callers.
Hash integrity does not itself authenticate a provider. Real eligibility also
requires a catalogue-pinned verified-source response and semantic review.
"""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit


_TOKEN = object()
ROLES = ('transaction', 'receipt', 'internal', 'trace', 'historical_code', 'source', 'deployment_runtime')


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
        if role in methods:
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
        source_verified = (source.get('chain_id') == 1 and req.get('action') == 'getsourcecode'
                           and str(req.get('address', '')).lower() == expected_contract
                           and str(req.get('chainid')) == '1'
                           and review.get('chain_id') == 1
                           and str(review.get('contract', '')).lower() == expected_contract
                           and isinstance(payloads['historical_code'], str)
                           and payloads['historical_code'].startswith('0x')
                           and review.get('runtime_code_sha256') == hashlib.sha256(bytes.fromhex(payloads['historical_code'][2:])).hexdigest())
        if not source_verified:
            conflicts.append('verified_source:chain_contract_runtime_binding')
    real_ready = is_verified_context(context) and source_verified and all(bound.values()) and not conflicts
    if not real_ready:
        gaps.append('REAL_ACQUISITION_AND_VERIFIED_SOURCE_PROVENANCE_UNAVAILABLE')
    return {'evidence_kind': kind, 'conflicts': sorted(set(conflicts)), 'gaps': sorted(set(gaps)),
            'request_bound': bound, 'trace_bound': trace_bound,
            'source_verified': source_verified, 'real_provenance_ready': real_ready}


def load_evidence_context(bundle_path, acquisition_catalogue_path):
    """Verify local artifacts against a separately maintained acquisition root.

    Each catalogue record pins file bytes, extracted payload, request identity,
    provider origin, evidence kind and successful acquisition. The bundle only
    selects record IDs; it cannot override trusted metadata or certify sources.
    No source URL/hash/code argument alone can construct this trusted context.
    """
    bundle_path, catalogue_path = Path(bundle_path).resolve(), Path(acquisition_catalogue_path).resolve()
    bundle = json.loads(bundle_path.read_text(encoding='utf-8'))
    catalogue = json.loads(catalogue_path.read_text(encoding='utf-8'))
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
        relative = Path(record.get('artifact_path', ''))
        artifact = (catalogue_path.parent / relative).resolve()
        if relative.is_absolute() or not artifact.is_relative_to(catalogue_path.parent) or not artifact.is_file():
            raise ValueError('Artifact outside acquisition directory')
        content = artifact.read_bytes()
        if hashlib.sha256(content).hexdigest() != record.get('artifact_sha256'):
            raise ValueError('Acquisition artifact bytes changed')
        raw = json.loads(content)
        if not isinstance(raw, dict) or raw.get('request') != record.get('request'):
            raise ValueError('Saved request differs from acquisition record')
        response = raw.get('response')
        if raw.get('http_status') != 200 or response is None:
            raise ValueError('Successful request/response envelope required')
        if role in {'transaction', 'receipt', 'trace', 'historical_code', 'deployment_runtime'}:
            if response.get('error') or 'result' not in response or response.get('id') != raw['request'].get('id'):
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
            hashlib.sha256(bundle_path.read_bytes()).hexdigest(),
            hashlib.sha256(catalogue_path.read_bytes()).hexdigest(), _TOKEN,
            payload_hash({'kind': 'REAL_CHAIN', 'records': records, 'source_review': review}))
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
