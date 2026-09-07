"""Reserved validation results, separate from component business outcomes.

Legacy receipts are interpreted only through an explicit schema branch. A
business status in an old check is ambiguous evidence, never implicit PASS.
"""
from copy import deepcopy

CHECK_SCHEMA = 'stage1b-validation-check-v2'
LEGACY_SCHEMAS = frozenset({
    'stage1b-r1-portable-validation-1',
    'stage1b-r2-portable-validation-v1',
    'stage1b-r3-portable-validation-v1',
})
RESERVED = frozenset({'schema_version', 'name', 'passed', 'check_status', 'status', 'required', 'detail'})


def check_row(name, passed, /, **detail):
    facts = deepcopy(detail)
    # Only an actual boolean true certifies a check. Unknown/truthy objects do not.
    result = passed is True
    row = {k: v for k, v in facts.items() if k not in RESERVED}
    if 'status' in facts and 'component_status' not in row:
        row['component_status'] = facts['status']
    row.update(schema_version=CHECK_SCHEMA, name=name, passed=result,
               check_status='PASS' if result else 'FAIL', status='PASS' if result else 'FAIL',
               required=True, detail=facts)
    return row


def skip_row(name, reason, required=False):
    if type(required) is not bool:
        raise ValueError('A skipped check needs an explicit boolean required flag')
    return {'schema_version': CHECK_SCHEMA, 'name': name, 'passed': None,
            'check_status': 'SKIP', 'status': 'SKIP', 'required': required,
            'reason': reason, 'detail': {}}


def row_failed(row, *, legacy_schema=None):
    if not isinstance(row, dict):
        return True
    if row.get('schema_version') == CHECK_SCHEMA:
        if not isinstance(row.get('name'), str) or not row['name']:
            return True
        state = row.get('check_status')
        if row.get('status') != state:
            return True
        if state == 'SKIP':
            return row.get('passed') is not None or row.get('required') is not False
        return state != 'PASS' or row.get('passed') is not True
    if legacy_schema not in LEGACY_SCHEMAS:
        return True
    # Old schema has no protected boolean. Unknown business statuses cannot be
    # inferred successful even if the enclosing historical receipt says PASS.
    if row.get('status') == 'PASS':
        return row.get('passed', True) is not True or row.get('exit_code', 0) not in (0,)
    if row.get('status') == 'SKIP':
        return row.get('required', False) is not False
    return True


def receipt_failed(receipt):
    if not isinstance(receipt, dict) or not isinstance(receipt.get('commands'), list) or not receipt['commands']:
        return True
    schema = receipt.get('schema_version')
    if schema not in LEGACY_SCHEMAS and schema != 'stage1b-r4-portable-validation-v1':
        return True
    return receipt.get('status') != 'PASS' or any(row_failed(row, legacy_schema=schema) for row in receipt['commands'])


def failures(rows):
    """Current writers accept only protected v2 rows; no legacy guessing."""
    return [row for row in rows if row_failed(row)]
