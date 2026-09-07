"""Small shared parsers for exact chain facts, without provider policy or I/O."""
import re


class ExactFieldError(ValueError):
    def __init__(self, reason_code, field, detail=""):
        self.reason_code = reason_code
        self.field = field
        super().__init__(f"{reason_code}:{field}" + (":" + detail if detail else ""))


def exact_uint(value, field="value"):
    if value is None:
        raise ExactFieldError("NULL", field)
    if type(value) is int:
        parsed = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        parsed = int(value, 10)
    elif isinstance(value, str) and re.fullmatch(r"0[xX][0-9a-fA-F]+", value):
        parsed = int(value, 16)
    else:
        raise ExactFieldError("INVALID", field, "exact nonnegative integer required")
    if parsed < 0:
        raise ExactFieldError("INVALID", field, "negative integer")
    return parsed


def status_bool(value, field="status", *, allow_bool=True):
    if type(value) is bool and allow_bool:
        return value
    if type(value) is int and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        if value in ("0", "0x0", "0X0", "false", "False"):
            return False
        if value in ("1", "0x1", "0X1", "true", "True"):
            return True
    raise ExactFieldError("NULL" if value is None else "INVALID", field, "explicit success boolean required")


def _field(row, keys, parser, *, required=True, strict_null=False):
    keys = tuple(keys)
    present = [(key, row[key]) for key in keys if key in row]
    name = "|".join(keys)
    if not present:
        if required:
            raise ExactFieldError("MISSING", name)
        return None
    populated = [(key, value) for key, value in present if value is not None]
    if not populated:
        if required:
            raise ExactFieldError("NULL", name)
        return None
    if strict_null and len(populated) != len(present):
        raise ExactFieldError("NULL_ALIAS", name)
    values = [(key, parser(value, key)) for key, value in populated]
    if any(value != values[0][1] for _, value in values[1:]):
        raise ExactFieldError("CONFLICT", name, "aliases disagree")
    return values[0][1]


def field_uint(row, keys, *, required=True, strict_null=False):
    return _field(row, keys, exact_uint, required=required, strict_null=strict_null)


def field_status(row, keys, *, required=False, strict_null=False, allow_bool=True):
    return _field(row, keys, lambda value, field: status_bool(value, field, allow_bool=allow_bool), required=required, strict_null=strict_null)


def field_text(row, keys, *, required=False, lower=False):
    def parse(value, field):
        if not isinstance(value, str) or not value:
            raise ExactFieldError("INVALID", field, "nonempty text required")
        return value.lower() if lower else value
    return _field(row, keys, parse, required=required)
