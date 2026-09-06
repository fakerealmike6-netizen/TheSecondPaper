# Reused project R1 finite reference-audit utilities, no provider or formal tracer.
"""Offline Stage1A utilities. No provider client, tracker or amount model."""
from pathlib import Path
from collections import defaultdict
import csv, datetime as dt, gzip, hashlib, io, json, re
from decimal import Decimal, InvalidOperation

VERSION='stage1a-r1-offline-1.1.0'
def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''): h.update(b)
    return h.hexdigest()
def stable(value): return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))
def sid(value): return hashlib.sha256(stable(value).encode()).hexdigest()
def truth(x): return x is True or str(x).lower()=='true'
def arr(x):
    if isinstance(x,(list,dict)): return x
    if not x: return []
    try: return json.loads(x)
    except (ValueError,TypeError): return []
def utc(x):
    if not x: return None
    if isinstance(x,dt.datetime): return x.replace(tzinfo=dt.timezone.utc) if x.tzinfo is None else x.astimezone(dt.timezone.utc)
    parsed=dt.datetime.fromisoformat(str(x).replace('Z','+00:00'))
    return parsed.replace(tzinfo=dt.timezone.utc) if parsed.tzinfo is None else parsed.astimezone(dt.timezone.utc)
def dump(path,obj):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(obj,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
def write_csv(path,rows,columns=None):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    if columns is None: columns=list(rows[0]) if rows else ['no_records']
    s=io.StringIO(newline=''); w=csv.DictWriter(s,fieldnames=columns,lineterminator='\n'); w.writeheader()
    for row in rows:
        w.writerow({k:stable(v) if isinstance(v,(dict,list,tuple,set)) else ('' if v is None else v) for k,v in row.items() if k in columns})
    b=s.getvalue().encode('utf-8')
    if path.suffix=='.gz': b=gzip.compress(b,mtime=0)
    path.write_bytes(b)

class Inputs:
    def __init__(self,root): self.root=Path(root).resolve(); self.items={}; self.path_cache={}
    def path(self,name,role='INPUT',expected=None):
        cache_key=str(name)
        if cache_key in self.path_cache:
            p,rel=self.path_cache[cache_key]
            if expected and self.items[rel]['sha256']!=expected: raise ValueError('Input hash mismatch: '+rel)
            return p
        p=Path(name)
        if not p.is_absolute(): p=self.root/p
        p=p.resolve()
        if not p.is_relative_to(self.root): raise ValueError('Input escaped PROJECT_ROOT')
        rel=p.relative_to(self.root).as_posix()
        if rel not in self.items:
            self.items[rel]={'path':rel,'sha256':digest(p),'bytes':p.stat().st_size,'role':role,'version':'sha256:'+digest(p)}
        if expected and self.items[rel]['sha256']!=expected: raise ValueError('Input hash mismatch: '+rel)
        self.path_cache[cache_key]=(p,rel)
        return p
    def csv(self,name,role='CANONICAL_TABLE'):
        p=self.path(name,role); op=gzip.open if p.suffix=='.gz' else open
        with op(p,'rt',encoding='utf-8-sig',newline='') as f: return list(csv.DictReader(f))
    def json(self,name,role='INPUT_JSON'):
        p=self.path(name,role); op=gzip.open if p.suffix=='.gz' else open
        with op(p,'rt',encoding='utf-8-sig') as f: return json.load(f)
    def unchanged(self): return [r['path'] for r in self.items.values() if digest(self.root/r['path'])!=r['sha256']]

def actor_key(actor): return re.sub('[^a-z0-9]','',str(actor or '').lower())
ALIASES={'okex':('OKX','okx'),'okx':('OKX','okx'),'huobi':('HTX','htx'),'huobiglobal':('HTX','htx'),'htx':('HTX','htx'),'coinbaseexchange':('Coinbase','coinbase')}
def canonical_actor(actor):
    actor=str(actor or '').strip()
    return ALIASES.get(actor_key(actor),(actor,actor_key(actor)))

def label_response(payload):
    if payload.get('code')!=200000:
        return {'request_status':'FAILED_OR_UNRESOLVED','records':None}
    if not isinstance(payload.get('data'),list):
        return {'request_status':'MALFORMED_SUCCESS_RESPONSE','records':None}
    return {'request_status':'SUCCESS','records':payload['data']}

def strictly_before(a,b):
    """Partial execution order, never concatenate trace_address and log_index."""
    if a['event_id']==b['event_id']: return False
    if a.get('block_number') and b.get('block_number'):
        if int(a['block_number'])!=int(b['block_number']): return int(a['block_number'])<int(b['block_number'])
        if a['tx_hash']!=b['tx_hash']:
            if a.get('transaction_index','')=='' or b.get('transaction_index','')=='': return None
            return int(a['transaction_index'])<int(b['transaction_index'])
    elif utc(a['block_timestamp'])!=utc(b['block_timestamp']):
        return utc(a['block_timestamp'])<utc(b['block_timestamp'])
    if a['tx_hash']!=b['tx_hash']: return None
    if a.get('log_index','')!='' and b.get('log_index','')!='': return int(a['log_index'])<int(b['log_index'])
    # Successful top-level value transfer happens before the callee executes.
    if a['event_type']=='ETH_TOP_LEVEL' and b['event_type']!='ETH_TOP_LEVEL': return True
    if b['event_type']=='ETH_TOP_LEVEL' and a['event_type']!='ETH_TOP_LEVEL': return False
    return None

def event_sort(e):
    return (int(e.get('block_number') or 0),int(e.get('transaction_index') or 0),0 if e['event_type']=='ETH_TOP_LEVEL' else 1,int(e.get('log_index') or 0),e['event_id'])

STOP_ROLES={'SERVICE','BRIDGE_BOUNDARY','MIXER_BOUNDARY','DEX_OR_PROTOCOL','CONFLICTED_IDENTITY'}
def amount_status(value):
    """Exact raw-unit validation. Never infer transaction-wide value from top value."""
    if value is None or value=='': return 'AMOUNT_MISSING',None
    if isinstance(value,bool) or not isinstance(value,(int,str)): return 'AMOUNT_INVALID',None
    if isinstance(value,str) and not re.fullmatch(r'[+-]?[0-9]+',value): return 'AMOUNT_INVALID',None
    raw=int(value)
    if raw<0: return 'AMOUNT_NEGATIVE',raw
    return ('AMOUNT_POSITIVE' if raw>0 else 'AMOUNT_ZERO'),raw

def reference_certificates(events,seeds,identities,*,policy=False,window_days=None,amount_diagnostics=None):
    """Finite certificate audit of already registered reference events only.

    No address discovery, requests, contamination, ranking or amount attribution.
    Every accepted event has positive raw capacity and an explicit predecessor.
    UNKNOWN intermediate identity is retained as a caveat, never a known EOA;
    it does not independently prevent first-identified-service certification.
    """
    byid={e['event_id']:e for e in events}; cert={}; arrivals=defaultdict(list)
    positive=set()
    for eid,e in byid.items():
        status,raw=amount_status(e.get('amount_raw'))
        if e.get('transaction_status','SUCCESS')!='SUCCESS': status='EVENT_SUCCESS_UNRESOLVED_OR_FAILED'
        if status=='AMOUNT_POSITIVE': positive.add(eid)
        elif amount_diagnostics is not None: amount_diagnostics.append({'event_id':eid,'amount_status':status,'amount_raw':e.get('amount_raw'),'raw_event_preserved':True})
    for seed in seeds:
        if seed not in positive: continue
        e=byid[seed]; cert[seed]={'path':[seed],'unknown_intermediates':[],'seed_event_id':seed,'depth':0,'max_wait_days':0.0}
        arrivals[(e['to_address'],e['asset_key'])].append(seed)
    for e in sorted(events,key=event_sort):
        eid=e['event_id']
        if eid not in positive: continue
        if eid in cert: continue
        candidates=[]
        for prev in arrivals.get((e['from_address'],e['asset_key']),[]):
            p=byid[prev]
            if strictly_before(p,e) is not True: continue
            role=identities.get(e['from_address'],{}).get('identity_class','UNKNOWN')
            if policy and role in STOP_ROLES: continue
            wait=(utc(e['block_timestamp'])-utc(p['block_timestamp'])).total_seconds()/86400
            if window_days is not None and wait>window_days: continue
            pc=cert[prev]
            unknown=pc['unknown_intermediates']+([e['from_address']] if policy and role=='UNKNOWN' else [])
            candidates.append({'path':pc['path']+[eid],'unknown_intermediates':sorted(set(unknown)),'seed_event_id':pc['seed_event_id'],'depth':pc['depth']+1,'max_wait_days':max(pc['max_wait_days'],wait)})
        if candidates:
            # Deterministic evidence representative only; no financial path ranking.
            cert[eid]=min(candidates,key=lambda c:(len(c['unknown_intermediates']),c['path']))
            # Ordinary self-transfers do not renew the arrival clock.
            if e['from_address']!=e['to_address']: arrivals[(e['to_address'],e['asset_key'])].append(eid)
    return cert

