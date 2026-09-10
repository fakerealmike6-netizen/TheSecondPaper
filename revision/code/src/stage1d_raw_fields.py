"""Exact validation of every saved fact before physical identity or merging."""
from exact_fields_r4 import exact_uint, field_uint, ExactFieldError
from context_ledger_r3 import trace_path


def exact_trace_path(value):
    if value is None:return None
    if isinstance(value,(tuple,list)):
        return tuple(exact_uint(v,'trace_address') for v in value)
    if type(value) is int:return (exact_uint(value,'trace_address'),)
    if not isinstance(value,str):raise ExactFieldError('INVALID','trace_address')
    # Existing Collector paths use underscores; native ledgers also use JSON
    # lists and comma/brace forms. Every position still uses the exact parser.
    return trace_path(value.replace('_',','))

UINT_GROUPS = (
    ('block_number', 'block', 'blockNumber'),
    ('tx_index', 'transaction_index', 'transactionIndex', 'index'),
    ('value_raw', 'amount_raw', 'value'),
    ('gas_used', 'gasUsed', 'receipt_gas_used'),
    ('gas_limit', 'gas'),
    ('gas_price', 'gasPrice'),
    ('effective_gas_price', 'effectiveGasPrice', 'receipt_effective_gas_price'),
    ('gas_raw', 'fee_raw'),
    ('subtraces',), ('withdrawal_index', 'withdrawalIndex'),
    ('log_index', 'logIndex'), ('nonce',),
    ('blob_gas_used', 'blobGasUsed', 'receipt_blob_gas_used'),
    ('blob_gas_price', 'blobGasPrice', 'receipt_blob_gas_price'),
    ('maxFeePerBlobGas',), ('maxFeePerGas',), ('maxPriorityFeePerGas',),
)
UINT_FIELDS = frozenset(k for group in UINT_GROUPS for k in group)


def transaction_type(row):
    """Optional EVM transaction type, never infer it from a trace call type."""
    return field_uint(row, ('type', 'transaction_type', 'transactionType'), required=False)


def validate_raw_member(row, *, physical=False):
    if not isinstance(row, dict):
        raise ValueError('Saved physical fact must be an object')
    is_fact = physical or any(k in row for k in ('record_type', 'tx_hash', 'transactionHash',
        'block_number', 'blockNumber', 'gasUsed', 'value_raw', 'amount_raw'))
    for group in UINT_GROUPS:
        keys = group if is_fact else tuple(k for k in group if k not in ('value', 'block', 'index', 'gas'))
        if keys:
            field_uint(row, keys, required=False)
    paths = [exact_trace_path(row[k]) for k in ('trace_address', 'traceAddress') if row.get(k) is not None]
    if paths and any(p != paths[0] for p in paths):
        raise ExactFieldError('CONFLICT', 'trace_address|traceAddress')
    kind = str(row.get('record_type', row.get('kind', 'transaction'))).lower()
    if ('transaction_type' in row or 'transactionType' in row or
            is_fact and kind in ('transaction', 'top', 'tx', 'receipt') and 'type' in row):
        transaction_type(row)
    # Validate nested physical RPC objects before a richer header or envelope
    # can replace a poorer one. Provider metadata remains ordinary text.
    for key, value in row.items():
        if key in ('provenance', 'provider_alias', 'root_position_binding', 'root_position_binding_proofs'):
            continue
        if isinstance(value, dict):
            validate_raw_member(value)
        elif key in ('transactions', 'logs', 'withdrawals') and isinstance(value, list):
            for member in value:
                if isinstance(member, dict):
                    validate_raw_member(member, physical=True)
    return row
