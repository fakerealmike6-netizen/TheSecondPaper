"""Exact protocol-only projection of the existing complete native context SQL.

Ordinary transaction and internal-family evidence stays on the BQ route. This
projection retains all three existing protocol components and proves no other
coverage kind. It does not discover neighbors or interpret arbitrary protocols.
"""
from context_queries_r3 import _build_sql


def build_protocol_sql(query, start, end):
    original = _build_sql(query, start, end)
    parts = original.split('\nUNION ALL\n')
    if len(parts) != 5 or '), top_keys AS (' not in parts[0]:
        raise ValueError('Original complete context SQL shape changed')
    expected = ("'withdrawal'", "'fee_recipient'", "'protocol_credit'")
    for fragment, kind in zip(parts[2:], expected):
        if f"CAST({kind} AS varchar) AS record_type" not in fragment or 'tx_keys' in fragment:
            raise ValueError('Exact existing protocol projection changed')
    scope = parts[0].split('), top_keys AS (', 1)[0] + ')\n'
    return scope + '\nUNION ALL\n'.join(parts[2:])
