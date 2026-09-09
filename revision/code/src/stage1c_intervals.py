"""Stage1C thin adapters over the accepted source LP and declared relaxations.

No observed fact is edited. Target copies are independent Model objects; the
explicit block-diagonal construction verifies their separable implementation.
"""
from __future__ import annotations
import copy
from fractions import Fraction as F
from time import perf_counter
from lp_model import Model, build_model, solve_interval, fmt
from context_lp_r3 import build_context_model, solve_context_interval, structural_nesting
from stage1c_zero_derivation import model_identity, copy_identity, certify_zero_parent, derive_zero

METHODS = ('FULL_INTERVAL', 'NO_CROSS_TARGET_COUPLING', 'NO_PROTOCOL_CONTINUATION', 'BALANCE_INFORMATION_REMOVED')

def is_context(doc):
    from stage1d_multiasset_context import SCHEMA
    supported = doc.get('schema_version') in {'stage1b-r3-context-model-v1', SCHEMA}
    if 'transactions' in doc and not supported:
        raise ValueError('Unregistered context schema')
    return supported

def groups(doc):
    result = copy.deepcopy(doc.get('objective_groups', {}))
    flat = [e for es in result.values() for e in es]
    if len(flat) != len(set(flat)):
        raise ValueError('First-service entry must belong to exactly one address-asset group')
    if any('|' not in g or not es for g, es in result.items()):
        raise ValueError('Stage1C groups must be nonempty address|asset groups')
    return dict(sorted(result.items()))

def build_variant(doc, method):
    context = is_context(doc)
    model = build_context_model(doc) if context else build_model(doc)
    modifications = {'raw_graph_unchanged': True}
    if method == 'BALANCE_INFORMATION_REMOVED':
        if context:
            relaxed = build_context_model(doc, remove_balance_information=True)
        else:
            relaxed = copy.deepcopy(model)
            relaxed._solver_cache = None
            for v in relaxed.variables:
                if v.kind in ('balance', 'pre_refund_residue'):
                    v.upper = F(relaxed.metadata['source_potential_raw'].get(v.asset, '0')) / v.scale
            relaxed.metadata['balance_status'] = 'INFORMATION_RELAXED'
        modifications['nesting'] = structural_nesting(model, relaxed)
        if not modifications['nesting']['nested']:
            raise ValueError('Information relaxation does not contain full feasible set')
        modifications['removed_bound_variables'] = [a.name for a,b in zip(model.variables, relaxed.variables) if a.upper != b.upper]
        model = relaxed
    if method == 'NO_PROTOCOL_CONTINUATION':
        conversions = ([e for tx in doc['transactions'] for e in tx.get('conversions', [])]
                       if context else [e for e in doc['events'] if e['kind'] == 'conversion'])
        removed = []
        for e in conversions:
            if e.get('certified_semantics') not in ('SYNTHETIC_CONTROLLED', 'CERTIFIED_LOCAL_COMPONENT'):
                raise ValueError('Cannot infer continuation of uncertified protocol')
            name = 'protocol:' + e['id'] + ':fixed_ratio'
            i = model.eq_names.index(name)
            for array in (model.eq, model.rhs, model.eq_names):
                del array[i]
            # Gross input, refund and net source still obey their original
            # conservation equation. Net source terminates at input boundary.
            out = model.variables[model.event_variables[e['id'] + ':output']]
            out.lower = out.upper = F(0)
            removed.append(name)
        model._solver_cache = None
        if context:
            model.metadata['no_protocol_continuation'] = True
        modifications.update({'connected_protocol_count': len(conversions),
            'feature_status': 'APPLIED' if conversions else 'FEATURE_ABSENT_SAME_INPUT',
            'unlinked_source_ratio_equalities': removed,
            'boundary_source_variables': [e['id'] + ':net' for e in conversions],
            'physical_outputs_retained_as_source_zero': [e['id'] + ':output' for e in conversions],
            'gross_and_refund_conservation_preserved': True})
    return model, modifications

def _zero(asset):
    return {'status':'OPTIMAL_EXACT_CERTIFIED','asset':asset,'objective_events':[],
            'lower_raw':'0','upper_raw':'0','proof':'EMPTY_FIXED_TARGET_UNION_AFTER_FEASIBILITY_CHECK'}

def run_interval(doc, method='FULL_INTERVAL', *, stage1d_empty_target_recovery=False,
                 joint_zero_optimization=True):
    if method not in METHODS:
        raise ValueError('Unknown interval method')
    if not isinstance(joint_zero_optimization, bool):
        raise ValueError('joint_zero_optimization must be boolean')
    start = perf_counter()
    model, modifications = build_variant(doc, method)
    targets = groups(doc)
    build_seconds = perf_counter() - start
    solves = interval_calls = matrices_built = memo_hits = derived_intervals = 0
    def solver(m, names, *, recover_empty_target=False):
        nonlocal solves, interval_calls, matrices_built
        interval_calls += 1
        matrix_was_empty = m._solver_cache is None
        if is_context(doc) and recover_empty_target:
            result = solve_context_interval(m, doc, names, stage1d_empty_target_recovery=True)
        else:
            result = solve_context_interval(m, doc, names) if is_context(doc) else solve_interval(m, names)
        # Native/context routines make one call per saved endpoint; bounded
        # context certificate retries are explicit extra attempts, not memo hits.
        solves += result.get('solver_call_count', len(result.get('endpoints', {})) +
            sum(max(0, len(endpoint.get('certificate_recovery', {}).get('attempts', [])) - 1)
                for endpoint in result.get('endpoints', {}).values()))
        matrices_built += int(matrix_was_empty and m._solver_cache is not None)
        return result
    def solve_all(m, selected, target_copy=None):
        # This memo and each exact parent proof exist only in this model/copy
        # within one run_interval invocation, including each 1+5 timed run.
        cache = {}
        zero_parents = []
        identity = model_identity(m, doc, method, target_copy)
        def solve(names, reference):
            nonlocal memo_hits, derived_intervals
            key = tuple(sorted(set(names)))
            if key in cache:
                memo_hits += 1
                return copy.deepcopy(cache[key])
            if joint_zero_optimization:
                for evidence, parent_ref in zero_parents:
                    if set(key) <= set(evidence['events']):
                        cache[key] = derive_zero(evidence, parent_ref, list(key))
                        derived_intervals += 1
                        return copy.deepcopy(cache[key])
            value = solver(m, list(key))
            value['proof_model_identity'] = copy.deepcopy(identity)
            cache[key] = value
            if joint_zero_optimization:
                try:
                    evidence = certify_zero_parent(m, value, identity)
                except (ValueError, KeyError, TypeError):
                    pass  # Unknown/nonzero/uncertified parents never skip a solve.
                else:
                    zero_parents.append((evidence, reference))
            return copy.deepcopy(cache[key])
        assets = sorted({g.rsplit('|',1)[1] for g in selected})
        def joint_rows():
            return {a: solve([e for g,es in selected.items() if g.rsplit('|',1)[1] == a for e in es],
                {'category':'addresses','key':target_copy['address_asset']} if target_copy is not None
                else {'category':'joint_by_asset','key':a}) for a in assets}
        # Disabled mode intentionally retains the accepted old schedule for
        # fixed-input equivalence and measured call-count comparisons.
        joints = joint_rows() if joint_zero_optimization else {}
        addresses = {g: solve(es, {'category':'addresses','key':g}) for g,es in selected.items()}
        entries = {e: solve([e], {'category':'events','key':e}) for es in selected.values() for e in es}
        if not joint_zero_optimization:
            joints = joint_rows()
        return addresses, entries, joints
    if method == 'NO_CROSS_TARGET_COUPLING':
        addresses, entries, joints, copy_records = {}, {}, {}, []
        for number, (g,es) in enumerate(targets.items()):
            # Copy all constraints and physical capacities. No state/solution
            # object is shared among the independently optimized scenarios.
            one = copy.deepcopy(model); one._solver_cache = None
            aa, ee, _ = solve_all(one, {g:es}, copy_identity(targets, g))
            addresses.update(aa); entries.update(ee)
            copy_records.append({'copy_id':f'F^{number}', 'address_asset':g,
                'variables':len(one.variables),'equalities':len(one.eq),
                'physical_identity_namespace':f'target_copy/{number}/',
                'cross_copy_identity_equalities_present':False})
        for asset in sorted({g.rsplit('|',1)[1] for g in targets}):
            vals = [v for g,v in addresses.items() if g.rsplit('|',1)[1] == asset]
            good = all(v['status'] == 'OPTIMAL_EXACT_CERTIFIED' for v in vals)
            joints[asset] = {'status':'OPTIMAL_EXACT_CERTIFIED' if good else 'UNRESOLVED',
                'asset':asset, 'lower_raw':fmt(sum((F(v['lower_raw']) for v in vals),F(0))) if good else None,
                'upper_raw':fmt(sum((F(v['upper_raw']) for v in vals),F(0))) if good else None,
                'interpretation':'PRODUCT_OF_TARGET_SPECIFIC_FEASIBLE_SETS_NOT_JOINTLY_REALIZABLE_FULL_AMOUNT',
                'derivation':'Exact sum of extrema in disjoint variables and constraints',
                'copy_count':len(vals)}
        modifications.update({'product_copies':copy_records,
            'unbound_identical_physical_variables':list(model.event_variables),
            'unbound_all_state_variables_count':len(model.variables),
            'definition':'F_ind = product_a F^(a); omit x_e^(a)=x_e^(b) and corresponding state identity for a != b; each copy retains all targets as absorbing terminals.',
            'global_common_source_budget':False,
            'within_each_copy_common_source_budget':True})
    else:
        addresses, entries, joints = solve_all(model, targets)
    if not targets:
        seed = next(name for name,i in model.event_variables.items() if model.variables[i].lower > 0)
        feasibility = solver(model,[seed], recover_empty_target=stage1d_empty_target_recovery)
        modifications['empty_target_feasibility'] = feasibility
        assets = sorted(model.metadata['source_potential_raw'])
        joints = {a:_zero(a) for a in assets}
        if feasibility['status'] != 'OPTIMAL_EXACT_CERTIFIED':
            for v in joints.values(): v.update(status='UNRESOLVED',lower_raw=None,upper_raw=None)
    values = [*addresses.values(), *entries.values(), *joints.values()]
    good = all(v['status'] == 'OPTIMAL_EXACT_CERTIFIED' for v in values)
    return {'status':'COMPLETED' if good else 'ERROR', 'applicability':'SUPPORTED',
        'output_kind':'product_of_target_specific_copies' if method=='NO_CROSS_TARGET_COUPLING' else 'feasible_interval',
        'addresses':addresses,'events':entries,'joint_by_asset':joints,
        'positive_addresses':[g for g,v in addresses.items() if v['upper_raw'] is not None and F(v['upper_raw'])>0],
        'modifications':modifications,
        'task_counts':{'address_intervals':len(addresses),'entry_intervals':len(entries),'joint_intervals':len(joints),
            'endpoint_optimizations':solves,'base_variables':len(model.variables),'base_equalities':len(model.eq),
            'interval_solver_calls':interval_calls,'solver_matrices_built':matrices_built,
            'base_model_builder_calls':2 if method=='BALANCE_INFORMATION_REMOVED' else 1,
            'target_copy_models':len(targets) if method=='NO_CROSS_TARGET_COUPLING' else 0,
            'objective_memo_hits':memo_hits,'derived_zero_unique_objectives':derived_intervals,
            'derived_endpoint_optimizations_avoided':2*derived_intervals,
            'joint_zero_optimization_enabled':joint_zero_optimization,
            'memo_lifetime':'ONE_METHOD_RUN_AND_ONE_MODEL_OR_TARGET_COPY'},
        'timing_parts':{'model_build_seconds':build_seconds,'solve_and_certification_seconds':perf_counter()-start-build_seconds},
        'model_scope':model.metadata.get('balance_status')}

def explicit_product_model(base, target_groups):
    """Actually instantiate the block-diagonal product; no cross-copy rows."""
    product = Model(metadata={'type':'EXPLICIT_TARGET_PRODUCT','copy_count':len(target_groups)})
    selected = []
    for number,(group,events) in enumerate(sorted(target_groups.items())):
        offset = len(product.variables)
        for v in base.variables:
            product.var(f'copy{number}::{v.name}',v.asset,v.scale,v.lower*v.scale,v.upper*v.scale,v.kind)
        for name,row,rhs in zip(base.eq_names,base.eq,base.rhs):
            product.equality(f'copy{number}::{name}',{i+offset:c for i,c in row.items()},rhs)
        for name,i in base.event_variables.items():
            eid=f'copy{number}::{name}';product.event_variables[eid]=i+offset
            if name in events: selected.append(eid)
    return product, selected

def verify_product(doc):
    if is_context(doc): raise ValueError('Explicit product verification reserved for small synthetic graphs')
    target_groups=groups(doc)
    if not target_groups: return {'status':'NOT_APPLICABLE_NO_TARGETS'}
    if len({g.rsplit('|',1)[1] for g in target_groups})!=1:
        raise ValueError('Explicit comparison must stay within one asset')
    base=build_model(doc)
    product,names=explicit_product_model(base,target_groups)
    exact=solve_interval(product,names)
    separated=run_interval(doc,'NO_CROSS_TARGET_COUPLING')
    asset=next(iter(separated['joint_by_asset']))
    expected=separated['joint_by_asset'][asset]
    match=exact['status']=='OPTIMAL_EXACT_CERTIFIED' and all(F(exact[k])==F(expected[k]) for k in ('lower_raw','upper_raw'))
    full=run_interval(doc)['joint_by_asset'][asset]
    return {'status':'PASS' if match else 'FAIL','explicit_variables':len(product.variables),
        'explicit_equalities':len(product.eq),'base_variables':len(base.variables),'copy_count':len(target_groups),
        'explicit_interval':exact,'decomposed_interval':expected,'full_joint_interval':full,
        'independent_upper_sum_exceeds_full_joint':F(expected['upper_raw'])>F(full['upper_raw']),
        'independent_upper_combination_feasible_in_full':F(expected['upper_raw'])==F(full['upper_raw']),
        'common_realizability_proof':'If sum(U_a)>U_union, the combination contradicts the certified FULL union upper. If equal, its FULL maximizer attains every U_a.'}

def audit_allocation(doc, values):
    model,_=build_variant(doc,'FULL_INTERVAL')
    return model.audit_vector(model.lift_hidden_allocation_for_audit(values))
