"""Limited synthetic tests; --real-roles adds the current read-only role check."""
import argparse
import hashlib
import json
from pathlib import Path
import socket
import sys
import time
import unittest

sys.dont_write_bytecode = True
here = Path(__file__).resolve().parent
revision = here.parents[1]
work = revision/'code'
sys.path[:0] = [str(here/'src'), str(here/'tests'), str(work/'src'), str(work/'tests')]
def deny(*args, **kwargs): raise RuntimeError('Role-only check prohibits network')
socket.socket.connect = deny
socket.create_connection = deny


def main():
    parser = argparse.ArgumentParser(); parser.add_argument('--real-roles', action='store_true')
    args = parser.parse_args(); started = time.perf_counter()
    suite = unittest.defaultTestLoader.discover(str(here/'tests'))
    passed = unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    receipt = {'synthetic_tests': 2, 'synthetic_status': 'PASS' if passed else 'FAIL',
               'production_writes': False, 'network_requests': 0, 'activity_loaded': False,
               'production_graph_read': False, 'full_registry_constructed': False}
    if not passed: raise SystemExit(1)
    if args.real_roles:
        from test_stage1d_unknown_cost_role_provenance import role_only
        from stage1d_task_boundaries import TaskBoundaries
        registry, validation = role_only(work)
        boundaries = TaskBoundaries(work)
        receipt.update(real_roles_status='PASS', role_validation=validation,
                       task_boundary_count=len(boundaries.records),
                       task_boundary_identity=boundaries.identity,
                       role_refs=registry._role_refs,
                       graph_alias_in_role_refs=any('/queries/' in ref['path'] and
                           ref['path'].endswith('/collection.json') for ref in registry._role_refs))
        if receipt['graph_alias_in_role_refs']: raise ValueError('Graph alias role-identity cycle remains')
        for ref in registry._role_refs:
            path = (work/ref['path']).resolve()
            if hashlib.sha256(path.read_bytes()).hexdigest() != ref['sha256']:
                raise ValueError('Role input changed during read-only validation')
    receipt['wall_seconds'] = time.perf_counter()-started
    receipt['candidate_sha256'] = hashlib.sha256((here/'src/stage1d_unknown_cost_registry.py').read_bytes()).hexdigest()
    target = here/'CHECK_RECEIPT.json'
    target.write_text(json.dumps(receipt, indent=2)+'\n', encoding='utf-8')
    print(json.dumps({k: v for k, v in receipt.items() if k != 'role_refs'}, ensure_ascii=False))


if __name__ == '__main__': main()
