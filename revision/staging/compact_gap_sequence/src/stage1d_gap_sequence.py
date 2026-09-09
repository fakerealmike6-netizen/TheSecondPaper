"""Lossless ordered JSON gap sequences, with shared immutable templates.

This is storage, not an evidence or completeness authority. Order, duplicates,
nulls and exact overlay behavior survive a round trip. No I/O or global memo.
"""
from __future__ import annotations
from collections.abc import Sequence
from dataclasses import dataclass
import hashlib
import json
import math

SCHEMA = 'stage1d-ordered-gap-sequence-v1'
SMALL_FLAT_LIMIT = 128


@dataclass(frozen=True)
class _Map:
    pairs: tuple


def _freeze(value):
    if value is None or type(value) in (str, int, bool): return value
    if type(value) is float:
        if not math.isfinite(value): raise ValueError('Finite JSON gap number required')
        return value
    if type(value) is list: return tuple(_freeze(x) for x in value)
    if type(value) is dict:
        if any(type(k) is not str for k in value): raise ValueError('String JSON gap keys required')
        return _Map(tuple((k, _freeze(v)) for k, v in value.items()))
    raise ValueError('Gap values must be ordinary JSON values')


def _thaw(value):
    if isinstance(value, _Map): return {k: _thaw(v) for k, v in value.pairs}
    if type(value) is tuple: return [_thaw(v) for v in value]
    return value


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def _ops_key(operations):
    # Python True == 1, but wrapper string conversion can distinguish them.
    # Memo identity is exact JSON identity; prefix suppression remains Python ==.
    return tuple(_encoded(_thaw(op)) for op in operations)


def _integer(value, name, minimum=0):
    if type(value) is not int or value < minimum: raise ValueError('Invalid ' + name)
    return value


class _Template:
    __slots__ = ('items', 'sha256', 'reason_strings', 'other_reason', 'all_objects')
    def __init__(self, items):
        self.items = tuple(_freeze(x) for x in items)
        self.sha256 = hashlib.sha256(_encoded([_thaw(x) for x in self.items])).hexdigest()
        reasons, other = set(), False
        for value in self.items:
            pairs = dict(value.pairs) if isinstance(value, _Map) else {}
            reason = pairs.get('reason')
            if type(reason) is str: reasons.add(reason)
            else: other = True
        self.reason_strings, self.other_reason = frozenset(reasons), other
        self.all_objects = all(isinstance(x, _Map) for x in self.items)


@dataclass(frozen=True)
class _Segment:
    template: _Template
    runs: tuple
    operations: tuple = ()
    @property
    def count(self): return sum(end - start for start, end in self.runs)


def _validate_operation(op):
    if type(op) is not dict: raise ValueError('Gap operation must be an object')
    kind = op.get('op')
    if kind == 'overlay':
        if set(op) != {'op', 'value'} or type(op['value']) is not dict:
            raise ValueError('Exact overlay operation required')
    elif kind == 'wrap':
        if set(op) != {'op', 'value', 'field', 'account_from', 'account_suffix'}:
            raise ValueError('Exact wrap operation required')
        if type(op['value']) is not dict or type(op['field']) is not str or not op['field']:
            raise ValueError('Gap wrapper object and field required')
        if op['account_from'] is not None and type(op['account_from']) is not str:
            raise ValueError('Gap account field must be string or null')
        if type(op['account_suffix']) is not str: raise ValueError('Account suffix must be string')
        if op['field'] == 'account_id' and op['account_from'] is not None:
            raise ValueError('Wrapper account and original fields must differ')
    else: raise ValueError('Unknown gap operation')
    return _freeze(op)


def _apply(value, operations):
    value = _thaw(value)
    for frozen in operations:
        op = _thaw(frozen)
        if op['op'] == 'overlay':
            if type(value) is not dict: raise ValueError('Gap overlay requires an object')
            value = {**value, **op['value']}
        else:
            wrapper = op['value']
            if op['account_from'] is not None:
                if type(value) is not dict: raise ValueError('Gap account projection requires an object')
                address = value.get(op['account_from'])
                wrapper['account_id'] = str(address).lower() + op['account_suffix'] if address else None
            value = {**wrapper, op['field']: value}
    return value


def _check_shapes(segment, operations):
    is_object = segment.template.all_objects
    for frozen in operations:
        op = dict(frozen.pairs)
        if op['op'] == 'overlay' or op.get('account_from') is not None:
            if not is_object:
                if any(not isinstance(segment.template.items[i], _Map)
                       for a, b in segment.runs for i in range(a, b)):
                    raise ValueError('Gap operation requires selected objects')
                is_object = True
        if op['op'] == 'wrap': is_object = True


class GapSequence(Sequence):
    """Immutable templates and ordered runs; iteration returns isolated values."""
    __slots__ = ('_segments', '_count')
    def __init__(self, segments=()):
        self._segments = tuple(s for s in segments if s.count)
        self._count = sum(s.count for s in self._segments)
    def __len__(self): return self._count
    def __bool__(self): return bool(self._count)
    def __iter__(self):
        for segment in self._segments:
            for start, end in segment.runs:
                for index in range(start, end):
                    yield _apply(segment.template.items[index], segment.operations)
    def __getitem__(self, key):
        if isinstance(key, slice):
            start, end, step = key.indices(len(self))
            if step != 1: return [self[i] for i in range(start, end, step)]
            return _slice(self, start, end)
        if type(key) is not int: raise TypeError('Gap index must be integer')
        if key < 0: key += len(self)
        if not 0 <= key < len(self): raise IndexError(key)
        for segment in self._segments:
            for start, end in segment.runs:
                count = end - start
                if key < count: return _apply(segment.template.items[start + key], segment.operations)
                key -= count
        raise IndexError(key)
    def __eq__(self, other):
        if not isinstance(other, (GapSequence, GapAccumulator, list, tuple)): return False
        return len(self) == len(other) and all(a == b for a, b in zip(self, other))
    def __deepcopy__(self, memo):
        # dataclasses.asdict remains JSON-compatible without logical expansion.
        result = serialize_gaps(self); memo[id(self)] = result; return result


def _slice(value, first, last):
    position, segments = 0, []
    for segment in value._segments:
        runs = []
        for start, end in segment.runs:
            size = end - start
            left, right = max(first, position), min(last, position + size)
            if left < right: runs.append((start + left - position, start + right - position))
            position += size
        if runs: segments.append(_Segment(segment.template, tuple(runs), segment.operations))
        if position >= last: break
    return GapSequence(segments)


def _wire_sequence(value):
    if set(value) != {'schema_version', 'templates', 'segments', 'expanded_count'} or value.get('schema_version') != SCHEMA:
        raise ValueError('Exact versioned gap sequence required')
    if type(value['templates']) is not dict or type(value['segments']) is not list:
        raise ValueError('Gap templates and ordered segments required')
    total = _integer(value['expanded_count'], 'expanded gap count')
    templates = {}
    for key, raw in value['templates'].items():
        if type(key) is not str or len(key) != 64 or any(c not in '0123456789abcdef' for c in key):
            raise ValueError('Exact gap template SHA required')
        if type(raw) is not dict or set(raw) != {'items'} or type(raw['items']) is not list:
            raise ValueError('Exact gap template items required')
        template = _Template(raw['items'])
        if template.sha256 != key: raise ValueError('Gap template SHA mismatch')
        templates[key] = template
    segments, used = [], set()
    for raw in value['segments']:
        if type(raw) is not dict or set(raw) != {'template', 'runs', 'operations'}:
            raise ValueError('Exact ordered gap segment required')
        key = raw['template']
        if type(key) is not str: raise ValueError('Gap template reference must be string')
        if key not in templates: raise ValueError('Missing gap template')
        if type(raw['runs']) is not list or not raw['runs'] or type(raw['operations']) is not list:
            raise ValueError('Nonempty selected runs and operations array required')
        template = templates[key]; runs = []
        for run in raw['runs']:
            if type(run) is not list or len(run) != 2: raise ValueError('Half-open gap index pair required')
            start, end = (_integer(x, 'gap index') for x in run)
            if not start < end <= len(template.items): raise ValueError('Gap run exceeds template')
            runs.append((start, end))
        operations = tuple(_validate_operation(op) for op in raw['operations'])
        segment = _Segment(template, tuple(runs), operations)
        _check_shapes(segment, operations)
        segments.append(segment); used.add(key)
    if used != set(templates): raise ValueError('Unused gap template is not an exact dependency')
    result = GapSequence(segments)
    if len(result) != total: raise ValueError('Expanded gap count mismatch')
    return result


def as_gap_sequence(value):
    if isinstance(value, GapSequence): return value
    if isinstance(value, GapAccumulator): return value.freeze()
    if type(value) is dict: return _wire_sequence(value)
    if type(value) not in (list, tuple): raise ValueError('Inline gaps or explicit gap sequence required')
    if not value: return GapSequence()
    template = _Template(value)
    return GapSequence((_Segment(template, ((0, len(template.items)),)),))


def iter_gaps(value): return iter(as_gap_sequence(value))
def gap_count(value): return len(as_gap_sequence(value))


def concat_gaps(*values):
    return GapSequence(segment for value in values for segment in as_gap_sequence(value)._segments)


def _operation(value, operation):
    frozen = _validate_operation(operation); value = as_gap_sequence(value)
    # Object shape check is once per distinct template/op, never logical copies.
    checked = set()
    for segment in value._segments:
        key = (id(segment.template), _ops_key(segment.operations), segment.runs)
        if key not in checked:
            _check_shapes(segment, segment.operations + (frozen,))
            checked.add(key)
    return GapSequence(_Segment(s.template, s.runs, s.operations + (frozen,)) for s in value._segments)


def overlay_gaps(value, overlay):
    return _operation(value, {'op': 'overlay', 'value': overlay})


def wrap_gaps(value, wrapper, field='original_gap', *, account_from=None, account_suffix='|ETH'):
    return _operation(value, {'op': 'wrap', 'value': wrapper, 'field': field,
                             'account_from': account_from, 'account_suffix': account_suffix})


def _select(segment, keep):
    runs = []
    for start, end in segment.runs:
        pending = None
        for i in range(start, end):
            if keep(i):
                if pending is None: pending = i
            elif pending is not None:
                runs.append((pending, i)); pending = None
        if pending is not None: runs.append((pending, end))
    return _Segment(segment.template, tuple(runs), segment.operations)


def exclude_gaps(value, prefix):
    """Old pre-append Python membership, including bool/numeric equality."""
    value = as_gap_sequence(value); prefix = list(iter_gaps(prefix))
    if not prefix: return value
    by_reason, other = {}, []
    for gap in prefix:
        reason = gap.get('reason') if type(gap) is dict else None
        if type(reason) is str: by_reason.setdefault(reason, []).append(gap)
        else: other.append(gap)
    selected, memo = [], {}
    for segment in value._segments:
        if not segment.operations and not other and not segment.template.other_reason and not (segment.template.reason_strings & by_reason.keys()):
            selected.append(segment); continue
        key = (id(segment.template), _ops_key(segment.operations), segment.runs)
        if key not in memo:
            def keep(i):
                gap = _apply(segment.template.items[i], segment.operations)
                reason = gap.get('reason') if type(gap) is dict else None
                candidates = by_reason.get(reason, ()) if type(reason) is str else prefix
                return gap not in candidates and (type(reason) is not str or gap not in other)
            memo[key] = _select(segment, keep)
        selected.append(memo[key])
    return GapSequence(selected)


_UNKNOWN = object()


def _constant_account(segment):
    # An overlay binds the same arrival address across a whole global template.
    # Track only explicitly known fields; original template values remain unknown.
    known, default = {}, _UNKNOWN
    for frozen in segment.operations:
        op = _thaw(frozen)
        if op['op'] == 'overlay': known.update(op['value'])
        else:
            address = known.get(op['account_from'], default) if op['account_from'] is not None else _UNKNOWN
            known, default = dict(op['value']), None
            known[op['field']] = _UNKNOWN
            if op['account_from'] is not None:
                known['account_id'] = (_UNKNOWN if address is _UNKNOWN else
                                       str(address).lower() + op['account_suffix'] if address else None)
    return known.get('account_id', default)


def _constant_reason(segment):
    reason = _UNKNOWN
    for frozen in segment.operations:
        op = _thaw(frozen)
        if op['op'] == 'overlay':
            if 'reason' in op['value']: reason = op['value']['reason']
        else:
            reason = _UNKNOWN if op['field'] == 'reason' else op['value'].get('reason', '')
    return reason


def iter_gaps_with_reasons(value, reasons=(), *, substring=None):
    """Reliable negative summaries; positive matches retain original order."""
    value = as_gap_sequence(value); reasons = tuple(reasons)
    def match(reason):
        return (reason in reasons) if substring is None else substring in reason
    for segment in value._segments:
        constant = _constant_reason(segment)
        if constant is not _UNKNOWN:
            if not match(constant): continue
        elif not segment.template.other_reason:
            if not any(match(r) for r in segment.template.reason_strings): continue
        for start, end in segment.runs:
            for index in range(start, end):
                # Use a shallow reason probe until the positive row is needed.
                if not segment.operations:
                    item = segment.template.items[index]
                    if not isinstance(item, _Map): raise ValueError('Reason-filtered gap must be an object')
                    raw_reason = dict(item.pairs).get('reason', '')
                    reason = _thaw(raw_reason)
                    if match(reason): yield _thaw(item)
                else:
                    item = _apply(segment.template.items[index], segment.operations)
                    if match(item.get('reason', '')): yield item


def any_gap_reason(value, reasons=(), *, substring=None):
    return next(iter_gaps_with_reasons(value, reasons, substring=substring), _UNKNOWN) is not _UNKNOWN


def filter_gaps_by_account(value, allowed, include_none=True):
    value = as_gap_sequence(value); allowed = frozenset(allowed); memo = {}; selected = []
    for segment in value._segments:
        constant = _constant_account(segment)
        if constant is not _UNKNOWN:
            if (constant is None and include_none) or constant in allowed: selected.append(segment)
            continue
        key = (id(segment.template), _ops_key(segment.operations), segment.runs)
        if key not in memo:
            def keep(i):
                gap = _apply(segment.template.items[i], segment.operations)
                if type(gap) is not dict: raise ValueError('Account-filtered gap must be an object')
                account = gap.get('account_id')
                return (account is None and include_none) or account in allowed
            memo[key] = _select(segment, keep)
        selected.append(memo[key])
    return GapSequence(selected)


class GapAccumulator:
    """Mutable append-only segment builder; no arrival-expanded dictionary list."""
    __slots__ = ('_segments', '_count')
    def __init__(self, initial=()):
        self._segments, self._count = [], 0
        self.extend(initial)
    def append(self, gap): self.extend([gap])
    def extend(self, value):
        if not isinstance(value, (GapSequence, GapAccumulator, list, tuple, dict)):
            value = list(value)  # Legacy finite generator call sites.
        value = as_gap_sequence(value)
        self._segments.extend(value._segments); self._count += len(value)
    def freeze(self): return GapSequence(self._segments)
    def __len__(self): return self._count
    def __bool__(self): return bool(self._count)
    def __iter__(self): return iter(self.freeze())
    def __getitem__(self, key): return self.freeze()[key]
    def __eq__(self, other): return self.freeze() == other
    def __deepcopy__(self, memo):
        result = serialize_gaps(self); memo[id(self)] = result; return result


def serialize_gaps(value, *, force_compact=False):
    value = as_gap_sequence(value)
    if not force_compact and len(value) <= SMALL_FLAT_LIMIT: return list(value)
    templates, segments = {}, []
    for segment in value._segments:
        key = segment.template.sha256
        if key not in templates:
            templates[key] = {'items': [_thaw(x) for x in segment.template.items]}
        segments.append({'template': key, 'runs': [list(run) for run in segment.runs],
                         'operations': [_thaw(op) for op in segment.operations]})
    return {'schema_version': SCHEMA, 'templates': templates, 'segments': segments,
            'expanded_count': len(value)}


def storage_counts(value):
    value = as_gap_sequence(value)
    templates = {s.template.sha256: s.template for s in value._segments}
    return {'logical_gaps': len(value), 'templates': len(templates),
            'template_items': sum(len(t.items) for t in templates.values()),
            'segments': len(value._segments), 'selected_runs': sum(len(s.runs) for s in value._segments)}
