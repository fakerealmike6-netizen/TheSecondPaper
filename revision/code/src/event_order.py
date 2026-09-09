"""Shared evidence order for cache and live observed-transfer graph builders.

Only block/transaction indices and Collector's supported intra-transaction
positions establish chronology. Hashes give a reproducible presentation of
independent operations; they never resolve a shared balance dependency.
"""
from collections import defaultdict
import heapq

from collector import Event, strictly_after

ORDER_UNRESOLVED = 'ORDER_UNRESOLVED_MODEL_NOT_SOLVABLE'


def _balance_keys(event):
    # These builders model transfers only. A future conversion builder must
    # provide all input/output balance dependencies before using this guard.
    return {(event.sender, event.asset), (event.recipient, event.asset)}


def order_observed_transfers(facts):
    """Return evidence-compatible events plus an explicit solvability audit.

    A deterministic topological presentation preserves every known relation.
    Unknown same-block relations involving a common address/asset prevent LP
    construction; unknown independent pairs need no additional chain request.
    No order alternatives or new optimization assumptions are introduced.
    """
    by_block = defaultdict(list)
    for fact in facts:
        event = Event(**fact)
        if event.kind not in ('top', 'internal', 'erc20'):
            raise ValueError('Shared transfer order guard requires supported transfer facts')
        by_block[event.block].append((event, fact))
    ordered, unresolved, cycles = [], [], []
    independent_unknown = 0
    for block in sorted(by_block):
        items = sorted(by_block[block], key=lambda item: item[0].stable_key())
        edges = [set() for _ in items]
        degree = [0 for _ in items]
        for later in range(len(items)):
            b = items[later][0]
            for earlier in range(later):
                a = items[earlier][0]
                b_after_a, a_after_b = strictly_after(b, a), strictly_after(a, b)
                if b_after_a is True and a_after_b is not True:
                    source, target = earlier, later
                elif a_after_b is True and b_after_a is not True:
                    source, target = later, earlier
                else:
                    shared = _balance_keys(a) & _balance_keys(b)
                    if shared:
                        unresolved.append({'event_ids': [a.event_id, b.event_id],
                                           'block': block,
                                           'shared_address_assets': [list(k) for k in sorted(shared)],
                                           'reason': 'NECESSARY_RELATIVE_ORDER_NOT_ESTABLISHED'})
                    else:
                        independent_unknown += 1
                    continue
                edges[source].add(target)
                degree[target] += 1
        ready = [index for index, value in enumerate(degree) if value == 0]
        heapq.heapify(ready)
        indices = []
        while ready:
            index = heapq.heappop(ready)
            indices.append(index)
            for target in sorted(edges[index]):
                degree[target] -= 1
                if degree[target] == 0:
                    heapq.heappush(ready, target)
        if len(indices) != len(items):
            cycles.append({'block': block, 'event_ids': [item[0].event_id for item in items],
                           'reason': 'INCONSISTENT_KNOWN_ORDER'})
            indices = list(range(len(items)))  # Diagnostic presentation; LP is blocked.
        ordered.extend(items[index][1] for index in indices)
    audit = {'version': 'shared-observed-transfer-order-r2-v1',
             'status': ORDER_UNRESOLVED if unresolved or cycles else 'NECESSARY_ORDER_ESTABLISHED',
             'unresolved_order_pairs': [row['event_ids'] for row in unresolved],
             'unresolved_dependencies': unresolved,
             'inconsistent_order_components': cycles,
             'independent_unknown_pairs': independent_unknown,
             'hash_order_is_chain_evidence': False}
    return ordered, audit
