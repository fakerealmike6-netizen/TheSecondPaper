"""Independent aggregation, selection, timing, and immutable-budget checks.
No project modules used. Only local inputs and archived baseline are read.
"""
from pathlib import Path
from collections import Counter,defaultdict
from fractions import Fraction as F
from decimal import Decimal
from statistics import median
import hashlib,json,csv,zipfile,io,sqlite3,ast
R=Path(__file__).parent;ROOT=R/'inputs/min'
def read(p):return json.loads(p.read_text())
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
index=read(ROOT/'RESULTS_INDEX.json');stats=read(ROOT/'STATISTICS.json');bykind=defaultdict(list);count=Counter();cgroups=Counter();halfn=Counter();recalls=defaultdict(list);timing_n=0
for row in index['method_results_index']:
 p=ROOT/row['path'];m=read(p/'METHOD_RESULTS.json');ev=read(p/'EVALUATION.json');ab=read(p/'ABLATIONS.json');eff=read(p/'EFFICIENCY.json')
 bykind[row['kind']].append((row,m,ev,ab,eff))
 for method,value in m.items():
  profile=eff['profiles'][method];rr=profile['repetitions'];assert len(rr)==6 and [r['repetition'] for r in rr]==list(range(6)) and sum(x['warmup'] for x in rr)==1
  tt=[x['elapsed_seconds'] for x in rr[1:]];assert median(tt)==profile['median_seconds'] and min(tt)==profile['min_seconds'] and max(tt)==profile['max_seconds'];assert len({x['result_semantic_sha256'] for x in rr})==1;timing_n+=1
 if row['kind']=='controlled':
  count[(m['HAIRCUT']['status'])]+=1;cgroups[row['family']]+=1
  for x in ab:
   count['coupling_upper_changed']+=F(x['independent_upper_excess_raw'])>0
   count['protocol_changed']+=x['protocol_result_changed'];count['balance_nested']+=x['balance_nested_endpoints']
  for v in ev['comparisons']:count['zero_endpoint_error']+=F(v['endpoint_error_raw'])==0;count['covered_objectives']+=v['hidden_covered']
  for method,x in ev['address_metrics'].items():
   if x.get('recall') is not None:recalls[method].append(F(x['recall']))
  assert ev['passed']
assert cgroups=={family:10 for family in cgroups} and sum(cgroups.values())==60
assert count['COMPLETED']==54 and count['NOT_APPLICABLE']==6
assert count['coupling_upper_changed']==35 and count['protocol_changed']==10 and count['balance_nested']==60
assert count['covered_objectives']==274 and count['zero_endpoint_error']==274
for method,vals in recalls.items():assert F(stats['controlled']['address_metrics'][method]['query_macro_recall'])==sum(vals)/len(vals)
# independent two per family + component reports explicitly supplied.
tiny=[read(ROOT/row['path']/'EVALUATION.json')['tiny_crosscheck'] for row,_,_,_,_ in bykind['controlled'] if read(ROOT/row['path']/'EVALUATION.json')['tiny_crosscheck'].get('status')!='NOT_REQUIRED']
assert len(tiny)==12 and all(x['status']=='PASS' for x in tiny)
semantic=read(ROOT/'controlled_v1/SEMANTIC_COMPONENT_EQUIVALENCE.json');product=read(ROOT/'controlled_v1/EXPLICIT_PRODUCT_CHECKS.json')
# Selection: reproduce all eligibility checks and deterministic ranking from supplied inputs.
cat=read(ROOT/'private/catalog/CATALOG_INPUTS.json');queries=cat['queries'];assert len({x['query_id'] for x in queries})==len(queries)==49
with (ROOT/'private/catalog/reference_query_members.csv').open(encoding='utf-8-sig',newline='') as f:members=list(csv.DictReader(f))
assert len(members)==67 and {x['query_id'] for x in members}=={x['query_id'] for x in queries}
strata=Counter(q['stratum'] for q in queries);assert strata=={'S1':5,'S2':41,'S3':3}
dev={'INC_ATOMIC_WALLET_2023','INC_HARMONY_BRIDGE_2022'};devids={x['query_id'] for x in read(ROOT/'private/REAL_EXPERIMENT_INPUTS.json')}
def rank(x):return (x['incident_id'] in dev,x['cost_window_seconds'] is None,x['cost_window_seconds'] or 0,x['cost_reference_entry_events'],-x['seed_cached_event_count'],x['query_id'])
e=[x for x in queries if x['next_batch_eligible'] and x['query_id'] not in devids];assert all(x['exact_single_seed'] and x['seed_member_count']==1 and x['stratum']!='S3' and x['seed_assets']==['native:eip155:1'] for x in e)
e.sort(key=rank);primary=[];covered=set(dev)
for x in e:
 if x['incident_id'] not in covered:primary.append(x);covered.add(x['incident_id'])
 if len(primary)==4:break
chosen={x['query_id'] for x in primary}
for x in e:
 if len(primary)==4:break
 if x['query_id'] not in chosen:primary.append(x);chosen.add(x['query_id'])
reserve=[x for x in e if x['query_id'] not in chosen][:2];published=read(ROOT/'NEXT_BATCH_PROPOSAL.json')
assert [x['query_id'] for x in primary]==[x['query_id'] for x in published['primary']]
assert [x['query_id'] for x in reserve]==[x['query_id'] for x in published['reserve']]
# budget: never construct project DB objects; read-only SQLite and baseline byte comparison.
budget=ROOT/'private/ledger/shared_budget_r4.sqlite';before=sha(budget)
with sqlite3.connect(budget.resolve().as_uri()+'?mode=ro',uri=True) as db:assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
with zipfile.ZipFile('/mnt/data/Stage1B_R4_R1_Review_Handoff.zip') as z:
 name=next(n for n in z.namelist() if n.endswith('Stage1B_R4_R1_Review_Bundle_MIN.zip'))
 with zipfile.ZipFile(io.BytesIO(z.read(name))) as zz:
  candidates=[n for n in zz.namelist() if n.endswith('shared_budget_r4.sqlite')]
  prior_db_present=bool(candidates)
  if prior_db_present:assert len(candidates)==1 and hashlib.sha256(zz.read(candidates[0])).hexdigest()==before
  for n in ['private/FINAL_BUDGET_SNAPSHOT_R4.json','private/CUMULATIVE_REQUEST_COST_ROWS_R4.json']:
   assert read(ROOT/n)==json.loads(zz.read(n))
assert sha(budget)==before
recon=read(ROOT/'private/STAGE1C_BUDGET_RECONCILIATION.json');assert before==recon['original_sha256']==recon['copied_sha256'];d=recon['snapshot']['dune_credits'];assert Decimal(d['known_actual_lower_bound'])+Decimal(d['export_upper_not_actual'])+Decimal(d['unknown_or_pending_risk'])==Decimal(d['cumulative_risk'])==Decimal('47.167873093');assert Decimal(d['cap'])-Decimal(d['cumulative_risk'])==Decimal('52.832126907')
# Source contracts and late hidden evaluation.
imports={}
for file in ['stage1c_baselines.py','stage1c_controlled.py','stage1c_oracle.py']:
 parsed=ast.parse((ROOT/'src'/file).read_text());imports[file]=sorted({n.module for n in ast.walk(parsed) if isinstance(n,ast.ImportFrom)}|{nn.name for n in ast.walk(parsed) if isinstance(n,ast.Import) for nn in n.names})
 assert not any('lp_model' in x or 'scipy' in x or 'context_lp' in x for x in imports[file])
result={'status':'PASS','controlled_families':dict(cgroups),'controlled_counts':dict(count),'tiny_enumeration_queries':len(tiny),'timing_profiles_verified':timing_n,'timed_repetitions_total':5*timing_n,'warmup_runs_total':timing_n,'catalog_queries':49,'catalog_members':67,'catalog_strata':dict(strata),'selection_reproduced_from_supplied_metadata':True,'upstream_reference_slice_rebuilt_in_full':False,'next_candidates':[{'role':'primary' if x in primary else 'reserve','query_id':x['query_id'],'incident':x['incident_id'],'window_days':str(F(x['cost_window_seconds'],86400)),'depth':x['proposed_reference_outer_depth'],'zero_hop_reference':x['zero_hop_reference'],'reference_positive_addresses':x['latest_registered_reference_positive_addresses']} for x in primary+reserve],'immutable_budget_sha256':before,'same_exported_budget_snapshot_and_cost_rows_as_parent':True,'database_bytes_match_declared_source_hash':True,'prior_original_database_available_for_independent_byte_comparison':prior_db_present,'dune_cumulative_risk':d['cumulative_risk'],'remaining_risk_budget':d['remaining_for_new_jobs'],'source_import_contracts':imports,'no_research_provider_calls_by_review':True,'component_report_types':{'semantic':type(semantic).__name__,'product':type(product).__name__}}
(R/'statistics_metadata_checks.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));print(json.dumps(result,ensure_ascii=False,indent=2))
