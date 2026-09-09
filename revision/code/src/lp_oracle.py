"""Independent low-dimensional exact rational vertex oracle.

Does not import the LP builder, SciPy, NumPy or any optimizer. The independently
authored oracle_spec.json states hand-derived source constraints in 0-3 free
variables. Vertex enumeration is exact for these bounded continuous polytopes;
no integer grid or hidden allocation is substituted for the interval truth.
"""
from fractions import Fraction as F
from itertools import combinations


def gaussian(rows, rhs):
    n = len(rhs)
    a = [[F(x) for x in row] + [F(b)] for row, b in zip(rows, rhs)]
    for col in range(n):
        pivot = next((r for r in range(col, n) if a[r][col]), None)
        if pivot is None:
            return None
        a[col], a[pivot] = a[pivot], a[col]
        divisor = a[col][col]
        a[col] = [v / divisor for v in a[col]]
        for r in range(n):
            if r != col:
                factor = a[r][col]
                a[r] = [v - factor * p for v, p in zip(a[r], a[col])]
    return tuple(a[r][-1] for r in range(n))


def enumerate_oracle(spec):
    n = len(spec["variables"])
    inequalities = [([F(a) for a in row["a"]], F(row["b"])) for row in spec["inequalities"]]
    if n == 0:
        vertices = {()}
    else:
        vertices = set()
        for active in combinations(inequalities, n):
            candidate = gaussian([a for a, b in active], [b for a, b in active])
            if candidate is not None and all(sum(a[i] * candidate[i] for i in range(n)) <= b for a, b in inequalities):
                vertices.add(candidate)
    if not vertices:
        raise ValueError("No vertices: oracle spec must be a feasible bounded polytope")
    values = {}
    for name, obj in spec["objectives"].items():
        endpoints = [F(obj.get("constant", "0")) + sum(F(a) * x for a, x in zip(obj["a"], v)) for v in vertices]
        values[name] = {"lower_raw": str(min(endpoints)), "upper_raw": str(max(endpoints))}
    return {"method": "INDEPENDENT_EXACT_RATIONAL_VERTEX_ENUMERATION", "vertex_count": len(vertices),
            "vertices": [[str(x) for x in v] for v in sorted(vertices)], "intervals": values,
            "derivation": spec["derivation"]}
