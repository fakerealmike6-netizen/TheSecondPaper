"""Canonical context coverage declarations without changing interval meaning.

Only identical rectangles with identical proof metadata may share a row.
Evidence identifiers are a set; every source identifier is retained. Adjacent
or overlapping intervals, failed proofs and empty evidence remain distinct.
"""
import copy
import json


def merge_coverage(*collections):
    """Return a deterministic, detached and idempotent declaration set."""
    found = {}
    for rows in collections:
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError('Context coverage declaration must be an object')
            evidence = row.get('evidence_ids')
            if evidence is not None and (not isinstance(evidence, list) or
                                        any(not isinstance(item, str) for item in evidence)):
                raise ValueError('Context coverage evidence_ids must be a string list or null')
            # Do not promote absent/null/empty evidence into a proved record.
            kind = ('absent' if 'evidence_ids' not in row else
                    'null' if evidence is None else 'present' if evidence else 'empty')
            base = {key: value for key, value in row.items() if key != 'evidence_ids'}
            key = json.dumps([base, kind], sort_keys=True, separators=(',', ':'), allow_nan=False)
            if key not in found:
                found[key] = copy.deepcopy(row)
                if evidence is not None:
                    found[key]['evidence_ids'] = sorted(set(evidence))
            elif evidence:
                found[key]['evidence_ids'] = sorted(set(found[key]['evidence_ids']) | set(evidence))
    return [found[key] for key in sorted(found)]
