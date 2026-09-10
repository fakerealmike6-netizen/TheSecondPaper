"""One bounded solver-coordinate retry, certified against the original exact LP.

x_scaled = FACTOR * x_original; equalities' RHS and all bounds scale together.
The cost and matrix stay unchanged. Dual multipliers thus remain valid in the
original LP after dividing the returned primal vector and objective by FACTOR.
No approximate endpoint is ever reported as a certified amount.
"""
from fractions import Fraction as F
import math

FACTOR = 1000000
MAX_BOUND_RESELECTIONS = 32
POLICY = 'STAGE1D_UNIFORM_COORDINATE_CERTIFICATE_RECOVERY_V1'


def recover(model, cost, matrix, remaining_seconds, *, certify, recover_vertex, propose_vertex=None):
    import numpy as np
    from scipy.optimize import linprog
    evidence = {'policy': POLICY, 'attempt': 2, 'method': 'highs-ds', 'presolve': False,
        'uniform_coordinate_factor': FACTOR, 'mapping': 'x_original=x_scaled/factor',
        'rhs_and_all_bounds_scaled_together': True, 'matrix_and_cost_unchanged': True,
        'original_model_changed': False, 'original_objective_changed': False,
        'primal_feasibility_tolerance': '1e-9', 'dual_feasibility_tolerance': '1e-9',
        'original_exact_audits_required': True}
    if remaining_seconds <= 0:
        evidence['skip_reason'] = 'ORIGINAL_ENDPOINT_TIME_BUDGET_EXHAUSTED'
        return None, evidence
    from time import perf_counter
    started = perf_counter()
    bounds = [(float(v.lower * FACTOR), float(v.upper * FACTOR)) for v in model.variables]
    rhs = np.array([float(b * FACTOR) for b in model.rhs])
    if not all(math.isfinite(v) and abs(v) < 1e19 for pair in bounds for v in pair) or not np.isfinite(rhs).all():
        evidence['skip_reason'] = 'SCALED_NUMERICAL_REPRESENTATION_UNSAFE'
        return None, evidence
    try:
        result = linprog(np.array([float(c) for c in cost]), A_eq=matrix, b_eq=rhs,
            bounds=bounds, method='highs-ds', options={'presolve': False,
            'time_limit': remaining_seconds, 'primal_feasibility_tolerance': 1e-9,
            'dual_feasibility_tolerance': 1e-9})
    except Exception as exc:
        evidence.update(solver_status=None, error=type(exc).__name__ + ': ' + str(exc))
        return None, evidence
    evidence.update(solver_status=int(result.status), message=result.message)
    if result.status != 0:
        return None, evidence
    result.x = result.x / FACTOR
    result.fun = result.fun / FACTOR
    certificate, vector, value = certify(model, result, cost)
    if not certificate['certified'] and certificate['exact_dual_feasible']:
        recovered = recover_vertex(model, result)
        if recovered is None and propose_vertex is not None:
            rejected = propose_vertex(model, result)
            nominated = {}
            selection = {'max_reselections': MAX_BOUND_RESELECTIONS, 'nominated_variable_count': 0,
                'original_bound_values_only': True, 'rounds': []}
            evidence['exact_bound_reselection'] = selection
            for number in range(MAX_BOUND_RESELECTIONS):
                if perf_counter() - started >= remaining_seconds:
                    selection['stop_reason'] = 'ORIGINAL_ENDPOINT_TIME_BUDGET_EXHAUSTED'
                    break
                if rejected is None:
                    selection['stop_reason'] = 'INCONSISTENT_VERTEX_PROPOSAL'
                    break
                violated = [(i, v.lower if x < v.lower else v.upper)
                    for i, (v, x) in enumerate(zip(model.variables, rejected))
                    if not v.lower <= x <= v.upper]
                if not violated:
                    recovered = rejected
                    break
                before = len(nominated)
                nominated.update(violated)
                if len(nominated) == before:
                    selection['stop_reason'] = 'NO_NEW_ORIGINAL_BOUND'
                    break
                selection['nominated_variable_count'] = len(nominated)
                selection['rounds'].append({'round': number + 1, 'violated_bounds': len(violated)})
                rejected = propose_vertex(model, result, sorted(nominated.items()))
                if rejected is not None and model.audit_vector(rejected)['exact_feasible']:
                    recovered = rejected
                    break
        if recovered is not None and model.audit_vector(recovered)['exact_feasible']:
            primal = sum((c * x for c, x in zip(cost, recovered)), F(0))
            if primal == F(certificate['exact_dual_objective']):
                vector, value = recovered, primal
                certificate.update(certified=True, exact_primal_feasible=True,
                    exact_primal_objective=str(primal), failure_detail=None,
                    primal_recovery={'method': POLICY, 'status': 'EXACT_CANDIDATE_RECOVERED',
                        'original_model_changed': False})
    evidence['certified_against_original_lp'] = certificate['certified']
    if not certificate['certified']:
        return None, evidence
    return (result, certificate, vector, value), evidence
