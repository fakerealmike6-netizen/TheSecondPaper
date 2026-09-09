"""Order-independent physical fact reconciliation; conflicts never elect a winner.

Identity is chain/transaction/physical position, with supplied event IDs retained
as aliases so a changed locator cannot hide a contradiction. Provenance is not a
physical fact. Missing values may be enriched; two explicit values quarantine
the entire component. No label/provider preference applies to chain facts.
"""
from dataclasses import asdict, is_dataclass
from copy import deepcopy
from types import MappingProxyType
import hashlib
import json
import re

CONFLICT_STATUS = 'FACT_CONFLICT_MODEL_NOT_SOLVABLE'
FACT_FIELDS = ('chain_id','tx_hash','kind','sender','recipient','asset','amount_raw',
               'block','block_hash','tx_index','timestamp','log_index','trace_address',
               'execution_index','success','gas_raw','gas_used','gas_price')
INT_FIELDS = {'amount_raw','block','tx_index','timestamp','log_index','execution_index','gas_raw','gas_used','gas_price'}

def exact_integer(value):
    if isinstance(value,bool) or not isinstance(value,(int,str)) or (isinstance(value,str) and not re.fullmatch(r'[0-9]+',value)):
        raise ValueError('physical integer must be an exact nonnegative integer')
    value=int(value)
    if value<0:raise ValueError('physical integer cannot be negative')
    return value

def canonical_event(value):
    d=asdict(value) if is_dataclass(value) else dict(value)
    d={k:v for k,v in d.items() if k in FACT_FIELDS or k in ('event_id','provenance')}
    for k in INT_FIELDS:
        if d.get(k) not in (None,''):d[k]=exact_integer(d[k])
        else:d[k]=None
    for k in ('event_id','tx_hash','sender','recipient','asset','kind','block_hash'):
        if d.get(k) not in (None,''):d[k]=str(d[k]).lower()
        elif k=='block_hash':d[k]=None
    chain=d.get('chain_id','eip155:1')
    d['chain_id']='eip155:'+str(exact_integer(str(chain).removeprefix('eip155:')))
    trace=d.get('trace_address')
    if trace is not None:
        if isinstance(trace,str) and trace.startswith('['):
            trace=json.loads(trace)
            if not isinstance(trace,list):raise ValueError('trace locator JSON must be an array')
        if isinstance(trace,(list,tuple)):trace='_'.join(str(exact_integer(x)) for x in trace)
        else:trace='_'.join(str(exact_integer(x)) for x in str(trace).split('_'))
    d['trace_address']=trace
    if d.get('success') is not None and type(d['success']) is not bool:raise ValueError('physical success must be boolean or unknown')
    return d

def physical_key(d):
    kind=d['kind']
    position='top' if kind=='top' else 'log:'+str(d.get('log_index')) if kind=='erc20' else 'trace:'+str(d.get('trace_address')) if kind=='internal' else kind+':'+d['event_id']
    return '|'.join((d['chain_id'],d['tx_hash'],position))


class _FrozenList(tuple):
    """Distinguish a frozen JSON list from an original tuple for exact thawing."""


def _freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return _FrozenList(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    return deepcopy(value)


def _thaw(value):
    if isinstance(value, MappingProxyType):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, _FrozenList):
        return [_thaw(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_thaw(item) for item in value)
    return deepcopy(value)

class PhysicalFactRegistry:
    def __init__(self):
        self.groups = {}; self.aliases = {}; self._next = 0
        self.revision = 0
        self._snapshots = {}
        self._conflicted_groups = set()

    def add(self,event,*,source=None,raw=None):
        d=canonical_event(event)
        aliases=('id:'+d['event_id'],'physical:'+physical_key(d))
        provenance=[]
        p=d.get('provenance')
        if p:
            try:parsed=json.loads(p) if isinstance(p,str) and p.startswith('[') else None
            except ValueError:parsed=None
            provenance.extend(parsed if isinstance(parsed,list) else [p])
        if source:provenance.append(source)
        version={'facts':{k:d.get(k) for k in FACT_FIELDS},'event_id':d['event_id'],
                 'provenance':sorted(set(provenance)),
                 'raw':deepcopy(raw if raw is not None else (asdict(event) if is_dataclass(event) else dict(event)))}
        digest=hashlib.sha256(json.dumps(version,sort_keys=True,default=str).encode()).hexdigest()
        ids={self.aliases[a] for a in aliases if a in self.aliases}
        changed = False
        if ids:
            group=min(ids)
            for other in sorted(ids-{group}):
                self.groups[group]['versions'].update(self.groups[other]['versions'])
                self.groups[group]['aliases'].update(self.groups[other]['aliases'])
                del self.groups[other]
                self._conflicted_groups.discard(other)
                changed = True
        else:
            group=self._next;self._next+=1
            self.groups[group]={'versions':{},'aliases':set(),'revision':0}
            changed = True
        g=self.groups[group]
        if not set(aliases).issubset(g['aliases']): changed = True
        g['aliases'].update(aliases)
        for alias in g['aliases']:self.aliases[alias]=group
        # A version includes provenance and retained raw evidence. Only an exact
        # duplicate may reuse the revision; a new source must remain observable.
        if digest not in g['versions']:
            g['versions'][digest]=version
            changed = True
        if changed:
            g['revision'] += 1
            g.pop('_view', None)
            self.revision += 1
            self._snapshots.clear()
            if self._view(g)[1]: self._conflicted_groups.add(group)
            else: self._conflicted_groups.discard(group)
        return self.get(d['event_id'])

    def _view(self,g):
        if '_view' not in g:
            # This object graph is shared internally and cannot be modified by
            # a caller. Public get/snapshot thaw fresh, independently owned data.
            g['_view'] = _freeze(self._build_view(g))
        return g['_view']

    def _build_view(self,g):
        versions=[g['versions'][k] for k in sorted(g['versions'])]
        facts={};conflicts={}
        for field in FACT_FIELDS:
            values={json.dumps(v['facts'].get(field),sort_keys=True):v['facts'].get(field) for v in versions if v['facts'].get(field) is not None}
            if len(values)>1:conflicts[field]=[values[k] for k in sorted(values)]
            facts[field]=values[sorted(values)[0]] if values else None
        bindings=[]
        for version in versions:
            v=version['facts'];eid=version['event_id']
            m=re.fullmatch(r'(eip155:[0-9]+):tx:(0x[0-9a-f]{64}):(top|log:[0-9]+|trace:[0-9_]+)',eid)
            if m and (m[1]!=v['chain_id'] or m[2]!=v['tx_hash'] or physical_key(v|{'event_id':eid}).split('|')[-1]!=m[3]):bindings.append(eid)
            asset_chain=re.match(r'(?:native|erc20):(eip155:[0-9]+)',v.get('asset') or '')
            if asset_chain and asset_chain[1]!=v['chain_id']:bindings.append(eid)
        if bindings:conflicts['identity_binding']=sorted(set(bindings))
        ids=sorted({v['event_id'] for v in versions})
        provenance=sorted({p for v in versions for p in v['provenance']})
        canonical_id=facts['chain_id']+':tx:'+facts['tx_hash']+':'+physical_key(facts|{'event_id':ids[0]}).split('|')[-1]
        facts.update(event_id=canonical_id if canonical_id in ids else ids[0],provenance=provenance[0] if len(provenance)==1 else json.dumps(provenance,separators=(',',':')) if provenance else '')
        return facts,conflicts,versions,ids

    def get(self,event_id):
        group=self.aliases.get('id:'+event_id.lower())
        if group is None:return None
        facts,conflicts,_,_=self._view(self.groups[group])
        return None if conflicts else _thaw(facts)

    def snapshot(self, *, include_events=True):
        """Return an owned snapshot; callers needing only conflicts skip events.

        Collector checks conflicts at every response boundary. Its conflict-only
        view avoids reconstructing/serializing every known event on every state.
        The default public contract and deterministic ordering remain unchanged.
        """
        if include_events not in self._snapshots:
            events=[];conflicts=[];quarantine=[]
            groups = self.groups.values() if include_events else (
                self.groups[group] for group in sorted(self._conflicted_groups))
            for g in groups:
                facts,fields,versions,ids=self._view(g)
                if fields:
                    record={'reason':'PHYSICAL_FACT_CONFLICT','event_ids':_thaw(ids),'fields':_thaw(fields),
                            'physical_keys':sorted({physical_key(dict(v['facts'])|{'event_id':v['event_id']}) for v in versions}),
                            'versions':_thaw(versions)}
                    record['conflict_id']=hashlib.sha256(json.dumps(record,sort_keys=True).encode()).hexdigest()
                    conflicts.append(record);quarantine.extend(_thaw(versions))
                elif include_events:events.append(_thaw(facts))
            self._snapshots[include_events] = _freeze({
                'events':sorted(events,key=lambda e:e['event_id']),
                'conflicts':sorted(conflicts,key=lambda c:c['conflict_id']),
                'quarantined_versions':sorted(quarantine,key=lambda v:json.dumps(v,sort_keys=True))})
        return _thaw(self._snapshots[include_events])

def reconcile_events(events,event_type,*,source=None):
    registry=PhysicalFactRegistry()
    for event in events:registry.add(event,source=source)
    snapshot=registry.snapshot()
    return [event_type(**d) for d in snapshot['events']],snapshot

def assert_graph_facts_safe(graph):
    if graph.get('scope')==CONFLICT_STATUS or graph.get('fact_conflicts') or graph.get('fact_validation',{}).get('status')=='CONFLICT':
        raise ValueError('Explicit physical fact conflict prohibits fixed-graph LP construction')
    if 'physical_fact_manifest' in graph:
        registry=PhysicalFactRegistry()
        for event in graph['physical_fact_manifest']:registry.add(event)
        snapshot=registry.snapshot()
        if snapshot['conflicts']:raise ValueError('Physical fact manifest contains contradictory versions')
        facts={e['event_id']:e for e in snapshot['events']}
        for event in graph.get('events',[]):
            f=facts.get(event['id'])
            if f is None:raise ValueError('LP event lacks physical fact manifest binding')
            asset='ETH' if f['asset']=='native:eip155:1' else f['asset']
            if event.get('to')!=f['recipient'] or event.get('from')!=f['sender'] or str(event.get('amount_raw'))!=str(f['amount_raw']) or event.get('asset')!=asset:
                raise ValueError('LP event differs from its physical fact manifest')
