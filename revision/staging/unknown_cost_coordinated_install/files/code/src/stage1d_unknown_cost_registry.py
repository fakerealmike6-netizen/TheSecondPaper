"""Read-only, exact-source registry for the authorized UNKNOWN cost policy.

No directory-wide event search, mutation, request, retry, or budget operation.
Only explicitly registered immutable observations enter activity calculations.
"""
from contextlib import closing
from copy import deepcopy
from dataclasses import asdict, is_dataclass
import gzip
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from collector import Scope
from physical_facts import canonical_event, FACT_FIELDS, PhysicalFactRegistry
from read_retry_r4 import logical_key
from stage1d_runtime import Runtime
from stage1d_unknown_cost_boundary import (
    AUTH, AUTHORITY_SHA256, POLICY_SCHEMA, VERSION, state_key, state_binding,
    validate_policy, make_decision, decide, activity, classify_code, bind_identity,
    verify_source_bytes, digest)

SCHEMA = 'stage1d-unknown-cost-registry-v1'
CODE_SCHEMA = 'stage1d-unknown-cost-code-evidence-v1'
OBSERVATION_SCHEMA = 'stage1d-unknown-cost-observations-v1'
CHECK_SCHEMA = 'stage1d-unknown-cost-identity-check-v1'
REVIEW_SCHEMA = 'stage1d-unknown-cost-identity-review-v1'
PROVIDER = 'ALCHEMY_ETH_MAINNET_EXISTING'
POLICY_PATH = 'private/stage1d_roles/UNKNOWN_COST_POLICY.json'
CURRENT_PATH = 'private/stage1d_roles/UNKNOWN_COST_CURRENT.json'
_SHA = re.compile('[0-9a-f]{64}')


def _state(value):
    return asdict(value) if is_dataclass(value) else deepcopy(value)


class Registry:
    def __init__(self, work, labels=None):
        self.work = Path(work).resolve()
        self.root = self.work.parent
        self.labels = labels
        self._bytes = {}; self._json = {}; self._dune = {}; self._code_cache = {}; self._rpc_refs = {}; self._source_cache = {}
        self._activity_cache = {}; self._decision_cache = {}
        self._observations = {}; self._observation_refs = {}
        self._checks = {}; self._codes = {}; self._initial = {}; self._initial_arrivals = {}
        self._policy = None; self._current = None; self._initial_ref = None
        self._local_refs = []
        self._role_refs = []
        self._identity = {'schema_version': SCHEMA, 'enabled': False,
                          'implementation_version': VERSION}
        pp = self.work / POLICY_PATH
        cp = self.work / CURRENT_PATH
        if not pp.exists() and not cp.exists():
            return
        if not pp.is_file() or not cp.is_file():
            raise ValueError('Cost policy and CURRENT registry must exist together')
        policy_ref, current_ref = self._ref(pp), self._ref(cp)
        policy, current = self.read(policy_ref), self.read(current_ref)
        validate_policy(policy)
        if current.get('schema_version') != SCHEMA or current.get('policy_ref') != policy_ref:
            raise ValueError('CURRENT does not bind the exact current policy bytes')
        self._policy = dict(policy, policy_sha256=policy_ref['sha256'])
        self._current = current
        self._initial_ref = current['initial_snapshot_ref']
        initial = self.read(self._initial_ref)
        if initial.get('authorization_id') != AUTH or initial.get('authority_sha256') != AUTHORITY_SHA256:
            raise ValueError('Initial screening snapshot authority differs')
        scopes = {}
        for item in initial.get('query_snapshots', []):
            q = Scope.from_policy(item['query'])
            validate_policy(policy, scope=q)
            scopes[q.query_id] = q
        for row in initial.get('state_screen_rows', []):
            s = row['state']; q = scopes[s['query_id']]
            key = state_key(s, q)
            if row.get('scope_hash') != q.scope_hash or row.get('state_key', key) != key:
                raise ValueError('Initial screening state identity differs')
            required = row.get('new_cost_screen_required')
            if type(required) is not bool or type(row.get('initial_unfinished_arrival')) is not bool:
                raise ValueError('Initial screening flags must be explicit booleans')
            if not required and row['initial_unfinished_arrival'] and not row.get('existing_boundary_or_depth'):
                raise ValueError('An unfinished ordinary state cannot masquerade as previously complete')
            if key in self._initial and self._initial[key] != required:
                raise ValueError('Contradictory initial state screening rows')
            self._initial[key] = required
            self._initial_arrivals[key] = self._fact(s['arrival'])
        for name, schema in [('identity_checks', CHECK_SCHEMA), ('historical_codes', CODE_SCHEMA),
                             ('activity_observations', OBSERVATION_SCHEMA)]:
            for ref in current.get(name, []):
                doc = self.read(ref)
                if doc.get('schema_version') != schema or doc.get('chain_id') != 'eip155:1':
                    raise ValueError('Unknown registered evidence schema/chain')
                self._validate_declared_refs(doc)
                if name == 'identity_checks':
                    self._checks.setdefault(doc['address'].lower(), []).append((ref, doc))
                elif name == 'historical_codes':
                    self._codes.setdefault(doc['address'].lower(), []).append((ref, doc))
                else:
                    self._load_observations(ref, doc)
        self._propagate_conflicting_versions()
        # Labels has already read these exact files. Bind that local lookup to
        # its source version; hash each immutable file once for this instance.
        local = [self.work/'private/stage1d_inputs/address_registry.csv.gz',
                 self.work/'private/stage1d_inputs/HISTORICAL_LABEL_SUCCESS.json']
        local += sorted((self.work/'derived/stage1d/labels').glob('*.json'))
        for path in local:
            if path.is_file():
                ref = self._ref(path); self.raw(ref); self._local_refs.append(ref)
        role_validation = self._load_role_binding()
        self._identity = dict(schema_version=SCHEMA, implementation_version=VERSION,
                              policy_ref=policy_ref, current_ref=current_ref,
                              initial_snapshot_ref=deepcopy(self._initial_ref),
                              evidence_refs={k: deepcopy(current.get(k, [])) for k in
                                             ('identity_checks', 'historical_codes', 'activity_observations')},
                              local_label_source_refs=deepcopy(self._local_refs),
                              role_source_refs=deepcopy(self._role_refs),
                              role_validation=role_validation,
                              enabled=policy['enabled'])

    def _load_role_binding(self):
        """Bind the same current role inputs and re-use their existing verifier."""
        from stage1d_role_adoption import TechnicalRoles
        refs = {}
        def visit(value):
            if isinstance(value, dict):
                if 'path' in value and 'sha256' in value:
                    key = (value['path'], value['sha256'])
                    if key in refs: return
                    self.raw(value)
                    refs[key] = deepcopy(value)
                    # Public text has its own success journal, reverified by
                    # TechnicalRoles. Never reinterpret its body as a manifest.
                    if value.get('format') != 'public_text':
                        path = self._path(value['path'])
                        if path.suffix.lower() == '.json': visit(self.read(value))
                    for name, child in value.items():
                        if name not in ('path', 'sha256'): visit(child)
                else:
                    for child in value.values(): visit(child)
            elif isinstance(value, list):
                for child in value: visit(child)
        for name in ('CURRENT.json', 'AUTHORITIES.json', 'USER_TASK_BOUNDARIES.json'):
            path = self.work/'private/stage1d_roles'/name
            if path.is_file(): visit(self._ref(path))
        # Do not trust a certificate's serialized verification=true/status. This
        # constructor replays the existing exact technical-role source checks.
        roles = TechnicalRoles(self.work)
        for certificate in roles.records: visit(certificate)
        self._role_refs = [refs[k] for k in sorted(refs)]
        return {'validator': 'TechnicalRoles', 'status': 'EXISTING_VALIDATOR_PASSED',
                'certificate_ids': sorted(r['certificate_id'] for r in roles.records)}

    def _path(self, value):
        p = (self.work / value).resolve()
        if not p.is_relative_to(self.root):
            raise ValueError('Cost evidence escapes the current revision')
        return p

    def _ref(self, path):
        path = path.resolve()
        raw = path.read_bytes()
        import os
        ref = {'path': Path(os.path.relpath(path, self.work)).as_posix(),
               'sha256': hashlib.sha256(raw).hexdigest()}
        self._bytes[(str(path), ref['sha256'])] = raw
        return ref

    def raw(self, ref):
        if not isinstance(ref, dict) or not isinstance(ref.get('sha256'), str) or not _SHA.fullmatch(ref['sha256']):
            raise ValueError('Exact source SHA-256 is required')
        path = self._path(ref['path']); key = (str(path), ref['sha256'])
        if key not in self._bytes:
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != ref['sha256']:
                raise ValueError('Registered original bytes changed: ' + ref['path'])
            self._bytes[key] = raw
        return self._bytes[key]

    def read(self, ref):
        key = (str(self._path(ref['path'])), ref['sha256'])
        if key not in self._json:
            raw = self.raw(ref)
            if ref['path'].endswith('.gz'):
                raw = gzip.decompress(raw)
            self._json[key] = json.loads(raw.decode('utf-8-sig'))
        return self._json[key]

    def _validate_declared_refs(self, value):
        if isinstance(value, dict):
            if 'path' in value and 'sha256' in value:
                self.raw(value)
            else:
                for v in value.values(): self._validate_declared_refs(v)
        elif isinstance(value, list):
            for v in value: self._validate_declared_refs(v)

    def _sources(self, refs):
        unique = {(r['path'], r['sha256']): r for r in refs}
        key = tuple(sorted(unique))
        if key not in self._source_cache:
            self._source_cache[key] = verify_source_bytes((ref, self.raw(ref)) for ref in unique.values())
        return self._source_cache[key]

    @property
    def identity(self):
        return digest(self._identity)

    @property
    def identity_document(self):
        return deepcopy(self._identity)

    def verify_evidence_refs(self, refs):
        """Recheck declared original bytes once per fresh read-only instance.

        Request guards use the refs already bound into validated decisions. This
        does not re-run provider queries, Dune normalization, or account scans.
        """
        for ref in {(r['path'], r['sha256']): r for r in refs}.values():
            self.raw(ref)
        return True

    def policy_for_scope(self, scope):
        if self._policy is None:
            return None
        q = scope if isinstance(scope, Scope) else Scope(**scope)
        if self._policy['query_scope_hashes'].get(q.query_id, self._policy['query_scope_hashes'].get(q.name)) != q.scope_hash:
            return None
        return deepcopy(self._policy)

    @staticmethod
    def _events(doc):
        if isinstance(doc, list): return doc
        if isinstance(doc, dict):
            if 'candidate_events' in doc or 'context_events' in doc:
                rows = list(doc.get('candidate_events', [])) + list(doc.get('context_events', []))
                for conflict in doc.get('fact_conflicts', []):
                    rows.extend(v.get('raw', dict(v.get('facts', {}), event_id=v.get('event_id')))
                                for v in conflict.get('versions', []))
                for v in doc.get('quarantined_facts', []):
                    rows.append(v.get('raw', dict(v.get('facts', {}), event_id=v.get('event_id'))))
                return rows
            if isinstance(doc.get('events'), list): return doc['events']
        raise ValueError('Source is not an explicit normalized physical event export')

    @staticmethod
    def _fact(row):
        value = canonical_event(row)
        fact = {k: value.get(k) for k in FACT_FIELDS + ('event_id',)}
        for key in ('semantic_virtual', 'is_virtual', 'tx_success', 'ancestor_success',
                    'ancestors_success', 'effective_success', 'reverted'):
            if key in row: fact[key] = row[key]
        return fact

    def _propagate_conflicting_versions(self):
        """An address index must not hide a contradictory sender/asset version."""
        registry = PhysicalFactRegistry()
        for rows in self._observations.values():
            for row in rows:
                registry.add(row)
        for conflict in registry.snapshot(include_events=False)['conflicts']:
            versions = [dict(v['facts'], event_id=v['event_id']) for v in conflict['versions']]
            keys = {(v['chain_id'], v['sender'], v['asset']) for v in versions}
            refs = {}
            for key in keys: refs.update(self._observation_refs.get(key, {}))
            for key in keys:
                self._observations.setdefault(key, []).extend(deepcopy(versions))
                self._observation_refs.setdefault(key, {}).update(refs)

    def _load_observations(self, ref, doc):
        originals = []
        source_refs = doc.get('source_refs', [])
        if not source_refs:
            raise ValueError('Activity list requires its original event source')
        for original in source_refs:
            originals.extend(self._events(self.read(original)))
        original_ids = {digest(self._fact(row)) for row in originals}
        selected = doc.get('events', originals)
        for row in selected:
            fact = self._fact(row)
            if digest(fact) not in original_ids:
                raise ValueError('Activity event differs from its registered original')
            # Keep distinct variants, failures, positions, all assets and normal
            # facts. The shared strict activity function owns qualification.
            key = (fact['chain_id'], fact['sender'], fact['asset'])
            self._observations.setdefault(key, []).append(deepcopy(row))
            self._observation_refs.setdefault(key, {})[(ref['path'], ref['sha256'])] = ref
            for r in source_refs:
                self._observation_refs[key][(r['path'], r['sha256'])] = r

    def _rpc(self, ref):
        key = (ref['path'], ref['sha256'])
        if key in self._code_cache:
            return self._code_cache[key]
        env = self.read(ref)
        request, response = env.get('request', {}), env.get('response', {})
        if env.get('evidence_kind') != 'REAL_CHAIN' or env.get('provider_alias') != PROVIDER or env.get('status') != 'SUCCESS_VALIDATED' or env.get('http_status') != 200 or env.get('response_complete') is not True:
            raise ValueError('Historical code requires a real successful RPC envelope')
        runtime = Runtime()
        if runtime.rpc_result_status(request, response) != 'SUCCESS_VALIDATED':
            raise ValueError('Current RPC normalizer rejects registered envelope')
        plan = {k: request[k] for k in ('method', 'params')}
        identity = runtime.rpc_identity(PROVIDER, plan)
        dbpath = self.work/'private/read_retry_r4.sqlite'
        if not dbpath.is_file():
            raise ValueError('Current success cache is missing; registry never creates it')
        with closing(sqlite3.connect(dbpath.as_uri()+'?mode=ro', uri=True)) as db:
            row = db.execute('SELECT identity_json,state,success_payload,success_receipt FROM read_requests WHERE logical_key=?',
                             (logical_key(identity),)).fetchone()
        if row is None or json.loads(row[0]) != identity or row[1] != 'SUCCESS':
            raise ValueError('Registered RPC is not the current exact successful cache member')
        receipt = json.loads(row[3])
        if (self._path(receipt['artifact_path']) != self._path(ref['path']) or receipt['artifact_sha256'] != ref['sha256'] or
                json.loads(row[2]) != response['result']):
            raise ValueError('Current RPC payload/member provenance differs')
        if not isinstance(env.get('raw_body_sha256'), str) or not _SHA.fullmatch(env['raw_body_sha256']):
            raise ValueError('RPC original transport body identity is missing')
        # This first version admits current rpc_r4 batch envelopes only. A legacy
        # importer envelope needs its own existing migration proof contract.
        import os
        body_path = self._path(ref['path']).parent/'response_body.bin'
        body_ref = dict(path=Path(os.path.relpath(body_path, self.work)).as_posix(),
                        sha256=env['raw_body_sha256'])
        body = json.loads(self.raw(body_ref).decode('utf-8'))
        if not isinstance(body, list):
            raise ValueError('Original RPC transport body is not a batch')
        members = [v for v in body if isinstance(v, dict) and type(v.get('id')) is str and v['id'] == request.get('id')]
        if len(members) != 1 or members[0] != response:
            raise ValueError('RPC response ID/member differs from original body')
        self._rpc_refs[key] = [ref, body_ref]
        self._code_cache[key] = (request, response)
        return request, response

    def _code(self, state, scope):
        s = _state(state); arrival = s['arrival']; b = state_binding(state, scope)
        matches = []
        for ref, doc in self._codes.get(b['address'], []):
            request_ref, response_ref = doc['request_ref'], doc['response_ref']
            if request_ref != response_ref:
                raise ValueError('Current RPC request/response must remain the original same envelope')
            request, response = self._rpc(request_ref)
            if request.get('method') != 'eth_getCode':
                raise ValueError('Code descriptor references another RPC method')
            if request.get('params') != [b['address'], hex(arrival['block'])]:
                continue
            block_hash = doc.get('arrival_block_hash')
            refs = [ref] + self._rpc_refs[(request_ref['path'], request_ref['sha256'])]
            if not isinstance(block_hash, str) or re.fullmatch('0x[0-9a-f]{64}', block_hash) is None:
                continue
            original_arrival = self._initial_arrivals.get(state_key(state, scope), {})
            if arrival.get('block_hash') and original_arrival.get('block_hash') == arrival['block_hash']:
                if block_hash != arrival['block_hash']:
                    raise ValueError('Historical code block conflicts with bound arrival')
                refs.append(self._initial_ref)
            else:
                if not doc.get('header_ref'):
                    continue
                hrequest, hresponse = self._rpc(doc['header_ref'])
                header = hresponse['result']
                if hrequest.get('method') != 'eth_getBlockByNumber' or hrequest.get('params') != [hex(arrival['block']), False] or header.get('hash') != block_hash or int(header['timestamp'], 16) != arrival['timestamp']:
                    raise ValueError('Historical code header binding differs')
                href = doc['header_ref']
                refs.extend(self._rpc_refs[(href['path'], href['sha256'])])
            matches.append(classify_code(state, scope, request, response, sources=self._sources(refs),
                                         evidence_refs=refs, conflict=doc.get('same_block_conflict', False)))
        if not matches: return None
        if len({m['response_sha256'] for m in matches}) != 1:
            raise ValueError('Conflicting historical code at the same address/block')
        return matches[0]

    def _online(self, ref, doc):
        from page_contract import initial_progress, validate_page
        from frontier_labels_r1 import parse_result_rows
        key = (doc['job_ref']['path'], doc['job_ref']['sha256'], doc['freeze_ref']['sha256'])
        if key not in self._dune:
            freeze, job = self.read(doc['freeze_ref']), self.read(doc['job_ref'])
            if freeze.get('kind') != 'frontier_labels' or freeze.get('sql_sha256') != job.get('sql_sha256') or job.get('state') != 'QUERY_STATE_COMPLETED':
                raise ValueError('Online label job/SQL completion binding differs')
            status = job.get('status_response') or {}
            if status.get('state') != 'QUERY_STATE_COMPLETED' or status.get('execution_id') != job.get('execution_id'):
                raise ValueError('Online label final execution status differs')
            metadata = status.get('result_metadata') or {}
            progress = initial_progress(metadata.get('total_row_count'))
            rows, refs = [], [ref, doc['job_ref'], doc['freeze_ref']]
            while not progress['complete']:
                offset = progress['next_offset']
                member = job.get('r4_verified_pages', {}).get(str(offset))
                if member is None:
                    raise ValueError('Online label page chain is incomplete')
                page_ref = dict(path=member['page_path'], sha256=member['page_sha256'])
                receipt_ref = dict(path=member['receipt_path'], sha256=member['receipt_sha256'])
                page, receipt = self.read(page_ref), self.read(receipt_ref)
                if receipt.get('evidence_kind') != 'REAL_PROVIDER' or receipt.get('http_status') != 200 or receipt.get('error_class'):
                    raise ValueError('Online label page is not a successful real provider response')
                raw_ref = dict(path=receipt['raw_path'], sha256=receipt['sha256'])
                if self.read(raw_ref) != page:
                    raise ValueError('Online page differs from original response')
                params = receipt.get('parameters', {})
                progress = validate_page(page, execution_id=job['execution_id'], offset=offset,
                                         limit=params.get('limit'), progress=progress,
                                         status_metadata=metadata, receipt=receipt, parameters=params)
                rows.extend(page['result']['rows']); refs.extend([page_ref, receipt_ref, raw_ref])
            parsed = parse_result_rows(rows, freeze['addresses'])
            self._dune[key] = (parsed, refs)
        parsed, refs = self._dune[key]
        if doc['address'].lower() not in parsed:
            raise ValueError('Online label response did not query this address')
        return {'status': 'ONLINE_CHECKED_NO_ADOPTABLE_ROLE', 'evidence_refs': refs + [ref]}

    def _search(self, ref, doc):
        req = self.read(doc['request_ref']); response = self.read(doc['response_ref'])
        if req != response:
            raise ValueError('Search request/result must stay in one original tool envelope')
        query = req.get('request', {}).get('search_query')
        if req.get('provider') != 'web.run' or not isinstance(query, list) or len(query) != 1:
            raise ValueError('Finite exact-address search original is missing')
        phrase = query[0].get('q', '')
        if set(re.findall(r'0x[0-9a-f]{40}', phrase.lower())) != {doc['address'].lower()} or 'ethereum' not in phrase.lower() or 'tool_result' not in req:
            raise ValueError('Search did not bind the full Ethereum address')
        page_refs = doc.get('page_refs', [])
        if len(page_refs) > 1:
            raise ValueError('Finite search exceeds one relevant original page')
        refs = [ref, doc['request_ref'], doc['response_ref']] + page_refs
        page_failed = False
        for p in page_refs:
            page = self.read(p)
            if page.get('provider') != 'web.run' or 'tool_result' not in page or not isinstance(page.get('request', {}).get('open'), list) or len(page['request']['open']) != 1:
                raise ValueError('Relevant page is not one preserved original web.open')
            page_failed |= page.get('completed') is not True
        if req.get('completed') is not True:
            return {'status': 'LOOKUP_FAILED', 'evidence_refs': refs}
        outcome = doc.get('outcome')
        if outcome in ('LOOKUP_FAILED', 'ACCESS_BLOCKED', 'ROLE_CONFLICT'):
            return {'status': outcome, 'evidence_refs': refs}
        if page_failed:
            return {'status': 'ACCESS_BLOCKED', 'evidence_refs': refs}
        if outcome != 'EXACT_SEARCH_CHECKED' or not doc.get('review_ref'):
            raise ValueError('Completed search needs the finite role review original')
        review = self.read(doc['review_ref'])
        if (review.get('schema_version') != REVIEW_SCHEMA or review.get('chain_id') != 'eip155:1' or
                review.get('address') != doc['address'] or review.get('search_ref') != doc['response_ref'] or
                review.get('conclusion') != 'NO_ADOPTABLE_ROLE_AFTER_FINITE_REVIEW' or
                not isinstance(review.get('explanation'), str) or not review['explanation'].strip()):
            raise ValueError('Finite identity review is not bound to this search/address')
        refs.append(doc['review_ref'])
        return {'status': 'EXACT_SEARCH_CHECKED', 'evidence_refs': refs}

    def _identity_evidence(self, state, scope, identity):
        b = state_binding(state, scope)
        # A supplied Labels instance has actually loaded and resolved its local
        # sources. Without it, local checking must remain pending.
        local_refs = self._local_refs + self._role_refs or [self._identity['current_ref']]
        channels = {'local': {'status': 'LOCAL_CHECKED' if self.labels is not None else 'UNQUERIED',
                              'evidence_refs': local_refs if self.labels is not None else []},
                    'online': {'status': 'UNQUERIED', 'evidence_refs': []},
                    'exact_search': {'status': 'UNQUERIED', 'evidence_refs': []}}
        for ref, doc in self._checks.get(b['address'], []):
            if doc.get('channel') == 'online' and doc.get('adapter') == 'DUNE_FOUR_TABLE_R4':
                result = self._online(ref, doc)
            elif doc.get('channel') == 'exact_search' and doc.get('adapter') == 'EXACT_ADDRESS_SEARCH_V1':
                result = self._search(ref, doc)
            else:
                raise ValueError('Unrecognized finite identity evidence adapter')
            channel = doc['channel']
            old = channels[channel]
            if old['status'] != 'UNQUERIED' and old['status'] != result['status']:
                channels[channel] = {'status': 'ROLE_CONFLICT', 'evidence_refs': old['evidence_refs'] + result['evidence_refs']}
            else:
                channels[channel] = result
        if identity.get('technical_role_status') == 'ROLE_CONFLICT_NEEDS_REVIEW' or identity.get('branch_action') == 'ROLE_CONFLICT_NEEDS_REVIEW':
            channels['local']['status'] = 'ROLE_CONFLICT'
        refs = [r for value in channels.values() for r in value['evidence_refs']]
        refs = refs or [self._identity['current_ref']]
        return bind_identity(state, scope, channels, sources=self._sources(refs), evidence_refs=refs)

    def resolve(self, state, scope, identity):
        policy = self.policy_for_scope(scope)
        if policy is None:
            return None
        key = state_key(state, scope)
        if key in self._initial_arrivals:
            current_arrival = self._fact(_state(state)['arrival'])
            if any(v is not None and current_arrival.get(k) != v for k, v in self._initial_arrivals[key].items()):
                raise ValueError('Current arrival contradicts the preserved initial physical fact')
        cache_key = (key, digest(identity))
        if cache_key in self._decision_cache:
            return deepcopy(self._decision_cache[cache_key])
        base = decide(state, scope, identity, policy_sha256=policy['policy_sha256'], enabled=policy['enabled'])
        if base['action'] == 'BYPASS':
            decision = base
        elif self._initial.get(key) is False:
            decision = make_decision(state, scope, policy_sha256=policy['policy_sha256'], action='BYPASS',
                                     reason='PREVIOUSLY_COMPLETED_EXACT_STATE_PRESERVED',
                                     evidence_refs=[self._initial_ref], base_identity=deepcopy(identity))
        else:
            try:
                identity_evidence = self._identity_evidence(state, scope, identity)
            except (ValueError, KeyError, TypeError) as exc:
                return make_decision(state, scope, policy_sha256=policy['policy_sha256'], action='PENDING',
                                     reason='IDENTITY_CHECK_PENDING', evidence_refs=[self._identity['current_ref']],
                                     base_identity=deepcopy(identity), evidence_validation_error=str(exc))
            try:
                code = self._code(state, scope)
            except (ValueError, KeyError, TypeError) as exc:
                return make_decision(state, scope, policy_sha256=policy['policy_sha256'], action='PENDING',
                                     reason='TYPE_UNRESOLVED', evidence_refs=[self._identity['current_ref']],
                                     base_identity=deepcopy(identity), evidence_validation_error=str(exc))
            b = state_binding(state, scope); facts_key = (b['chain_id'], b['address'], b['asset'])
            if key not in self._activity_cache:
                refs = list(self._observation_refs.get(facts_key, {}).values()) or [self._initial_ref]
                self._activity_cache[key] = activity(state, scope, self._observations.get(facts_key, []),
                                                      sources=self._sources(refs), evidence_refs=refs)
            decision = decide(state, scope, identity, policy_sha256=policy['policy_sha256'],
                              identity=identity_evidence, code=code,
                              observed_activity=self._activity_cache[key], enabled=policy['enabled'])
        self._decision_cache[cache_key] = decision
        return deepcopy(decision)
