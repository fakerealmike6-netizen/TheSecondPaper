"""Synthetic-only tests; no current graph, database, credentials or network."""
from pathlib import Path
import socket
import sys
import unittest

sys.dont_write_bytecode = True
here = Path(__file__).resolve().parent
revision = here.parents[1]
sys.path[:0] = [str(here), str(here/'tests'),
               str(revision/'staging/unknown_cost_boundary_core_candidate/src'),
               str(revision/'code/src')]
def denied(*args, **kwargs): raise RuntimeError('Synthetic preparation tests prohibit network')
socket.socket.connect = denied
socket.create_connection = denied
suite = unittest.defaultTestLoader.discover(str(here/'tests'), pattern='test_*.py')
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(not result.wasSuccessful())
