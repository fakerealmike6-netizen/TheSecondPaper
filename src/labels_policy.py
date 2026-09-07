"""Versioned observation adoption. Raw records remain immutable and auditable."""
import re
from collections import defaultdict
from reference_core import arr, truth, actor_key, canonical_actor

VERSION = 'stage1b-r4-dune-preferred-provenance-2.0'
KNOWN_ROLES = {'SERVICE', 'BRIDGE_BOUNDARY', 'MIXER_BOUNDARY', 'DEX_OR_PROTOCOL'}
FOCUS_CONFLICT = '0xf5e10380213880111522dd0efd3dbb45b9f62bcc'

def full_address(value):
    if not isinstance(value, str) or not re.fullmatch(r'0x[0-9a-fA-F]{40}', value):
        raise ValueError('Expected complete Ethereum address')
    return value.lower()

def explicit_claim(o):
    if o.get('semantic_kind') != 'ATTRIBUTION' or not o.get('actor'):
        return False
    text = ' '.join(str(o.get(k, '')) for k in ('raw_label', 'source_metadata')).lower()
    # A deployer, behavior, risk or persona does not establish custody.
    if re.search(r'\b(cex users?|dex users?|exchange users?|deployer|deployed by)\b|user of', text):
        return False
    return o.get('role') in KNOWN_ROLES

def resolve_address(observations, baseline=None, *, baseline_observations=None):
    """Adopt identity and its supporting observations together.

    A caller with a frozen prior observation group passes it explicitly. Legacy
    callers can only recover prior support through IDs saved in the baseline;
    a baseline ID by itself is never evidence of that observation's contents.
    """
    observations = list(observations)
    baseline = dict(baseline or {})
    if baseline_observations is None:
        old_ids = set(arr(baseline.get('adopted_observation_ids'))) | set(arr(baseline.get('observation_ids')))
        prior = [o for o in observations if o.get('observation_id') in old_ids]
    else:
        prior = list(baseline_observations)
    all_observations = {}
    for o in prior + observations:
        oid = o['observation_id']
        if oid in all_observations:
            old = all_observations[oid]
            if any(old.get(k) != o.get(k) for k in ('address', 'actor', 'role', 'platform', 'source_version', 'semantic_kind')):
                raise ValueError('Conflicting contents for one label observation ID')
        all_observations[oid] = o
    observations = list(all_observations.values())
    claims = [o for o in observations if explicit_claim(o)]
    dune = [o for o in claims if o.get('platform') == 'Dune']
    picked = dune or claims
    # Only the explicitly approved lexical alias is newly normalized here.
    key = lambda o: 'bitpay' if actor_key(o['actor']) == 'bitpaycom' else actor_key(o['actor'])
    actors = {key(o) for o in picked}
    roles = {o['role'] for o in picked}
    raw_internal_conflict = any(o.get('platform') == 'Dune' and 'Chainflip: Vault' in o.get('raw_label', '') for o in observations) and any(o.get('platform') == 'Dune' and actor_key(o.get('actor')) == 'sushiswap' for o in observations)
    conflict = len(actors) > 1 or len(roles) > 1 or any(truth(o.get('record_conflict')) for o in picked) or raw_internal_conflict
    all_raw_actors = sorted({o['actor'] for o in observations if o.get('semantic_kind') == 'ATTRIBUTION' and o.get('actor')})
    if picked and not conflict:
        role = next(iter(roles)); actor = picked[0]['actor']
        if key(picked[0]) == 'bitpay': actor = 'BitPay'
        rule = 'DUNE_EXPLICIT_INTERNALLY_CONSISTENT_PRIORITY' if dune else 'APPROVED_PROFESSIONAL_SOURCE_FALLBACK'
    elif picked or raw_internal_conflict:
        role, actor = 'CONFLICTED_IDENTITY', None
        rule = 'DUNE_INTERNAL_CONFLICT_PRESERVED' if dune or raw_internal_conflict else 'OTHER_SOURCE_CONFLICT_PRESERVED'
    else:
        role, actor = baseline.get('identity_class', 'UNKNOWN'), baseline.get('actor') or None
        rule = 'EXISTING_ROLE_RETAINED_NO_NEW_EXPLICIT_OWNERSHIP'
    retained = not dune and not raw_internal_conflict and baseline and not any(o.get('collection_scope')=='REFERENCE_TARGETED' for o in observations)
    if retained:
        role, actor = baseline.get('identity_class','UNKNOWN'), baseline.get('actor') or None
        rule = 'EXISTING_OTHER_SOURCE_RESOLUTION_RETAINED'
    address = observations[0]['address'] if observations else baseline.get('address')
    if address in ('0x'+'0'*40, '0x'+'0'*36+'dead'):
        role = 'NON_SERVICE_SENTINEL'; actor = baseline.get('actor'); rule = 'NON_SERVICE_SENTINEL'
    adopted = picked if not conflict else []
    provenance_status = 'SUPPORTED' if adopted else 'NO_ADOPTED_ATTRIBUTION'
    provenance_issues = []
    if retained and role in KNOWN_ROLES and actor:
        wanted = key({'actor': actor})
        adopted = [o for o in prior if explicit_claim(o) and key(o) == wanted and o.get('role') == role
                   and str(o.get('address', '')).lower() == str(address).lower()]
        provenance_status = 'SUPPORTED_BY_EXISTING_OBSERVATIONS' if adopted else 'PROVENANCE_UNRESOLVED'
        if not adopted:
            provenance_issues = ['HISTORICAL_EVIDENCE_GAP']
    elif retained or rule == 'NON_SERVICE_SENTINEL':
        adopted = []
        provenance_status = 'NO_ADOPTED_ATTRIBUTION'
    adopted = list({o['observation_id']: o for o in adopted}.values())
    adopted_ids = [o['observation_id'] for o in adopted]
    out = dict(baseline, address=address, chain_id='1', actor=actor, identity_class=role,
        actor_candidates=all_raw_actors, resolution_rule=rule, label_policy_version=VERSION,
        adopted_observation_ids=adopted_ids,
        adopted_sources=sorted({o['platform'] for o in adopted}),
        adopted_source_versions=sorted({o.get('source_version','') for o in adopted}),
        provenance_status=provenance_status, provenance_issues=provenance_issues,
        unadopted_observation_ids=[o['observation_id'] for o in observations if o['observation_id'] not in adopted_ids],
        preserved_conflict=conflict or len({key(o) for o in claims}) > 1 or baseline.get('identity_class')=='CONFLICTED_IDENTITY',
        conflict_status='DUNE_INTERNAL_CONFLICT' if raw_internal_conflict or (dune and conflict) else ('RAW_CROSS_SOURCE_CONFLICT_RETAINED' if len({key(o) for o in claims})>1 else baseline.get('conflict_status','NO_EXPLICIT_CONFLICT')),
        service_status='NAMED_SERVICE_IDENTITY_USABLE' if role=='SERVICE' else 'NOT_CONFIRMED_AS_REQUESTABLE_SERVICE',
        generation_method_undisclosed_present=any(not o.get('generation_mechanism') for o in observations),
        generation_method_undisclosed_observations=sum(not o.get('generation_mechanism') for o in observations),
        observation_ids=[o['observation_id'] for o in observations],
        source_platforms=sorted({o['platform'] for o in observations}),
        role_candidates=sorted({o['role'] for o in claims}),
        observation_count=len(observations),
        acquisition_scope='REFERENCE_TARGETED_OR_INHERITED_VERSIONED_CACHE',
        temporal_control_claim='No incident-date or legal-subject identity inference')
    return out

def historical_successes(rows):
    return {full_address(r['address']) for r in rows if r.get('api_response_status') == 'SUCCESS'}

def lookup_outcome(payload, address):
    full_address(address)
    if payload.get('code') != 200000 or not isinstance(payload.get('data'), list):
        return 'FAILED_OR_UNRESOLVED'
    matches = [r for r in payload['data'] if r.get('address','').lower()==address.lower() and str(r.get('chain_id'))=='1']
    if len(matches)!=1: return 'MISSING_OR_DUPLICATE_ADDRESS_UNRESOLVED'
    r=matches[0]
    if r.get('main_entity'): return 'NAMED_ENTITY_RETURNED'
    return 'OTHER_LABEL_RETURNED' if r.get('name_tag') or r.get('attributes') or r.get('comp_entities') else 'EMPTY_LABEL_RESULT'
