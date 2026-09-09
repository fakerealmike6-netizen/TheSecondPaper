from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from stage1d_recovery_retry import eligible, RecoveryReadRetryStore


class RecoveryRetryTests(unittest.TestCase):
    def test_only_three_proved_old_transient_reads_qualify(self):
        identity = {'method': 'eth_getBalance'}
        attempts = [{'attempt_no': n, 'outcome': 'RETRYABLE_FAILURE', 'finished_at': 1, 'dispatched_at': 0} for n in range(1, 4)]
        self.assertTrue(eligible(identity, attempts))
        self.assertFalse(eligible({'method': 'jobs.insert'}, attempts))
        self.assertFalse(eligible(identity, attempts[:2]))
        for outcome in ('SUCCESS', 'PERMANENT_FAILURE', 'IN_FLIGHT', 'UNRESOLVED'):
            changed = [dict(a) for a in attempts]
            changed[-1]['outcome'] = outcome
            self.assertFalse(eligible(identity, changed))

    def test_restart_never_adds_a_seventh(self):
        identity = {'provider': 'synthetic', 'method': 'eth_getBalance', 'params': ['account', '0x1']}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'retry.sqlite'
            with patch.object(RecoveryReadRetryStore, '_limit', return_value=6):
                for number in range(1, 7):
                    store = RecoveryReadRetryStore(path, clock=lambda: 10, rng=lambda: 0)
                    claim = store.claim(identity)
                    self.assertEqual(claim['attempt_no'], number)
                    store.mark_dispatched(claim['attempt_id'])
                    store.finish(claim['attempt_id'], 'RETRYABLE_FAILURE')
                restarted = RecoveryReadRetryStore(path, clock=lambda: 10, rng=lambda: 0)
                self.assertEqual(restarted.claim(identity)['state'], 'RETRIES_EXHAUSTED')
                self.assertEqual(len(restarted.attempts(identity)), 6)

    def test_recovered_success_is_cached_across_restart(self):
        identity = {'provider': 'synthetic', 'method': 'eth_getBalance', 'params': ['account', '0x2']}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'retry.sqlite'
            with patch.object(RecoveryReadRetryStore, '_limit', return_value=6):
                store = RecoveryReadRetryStore(path, clock=lambda: 10, rng=lambda: 0)
                for number in range(1, 5):
                    claim = store.claim(identity)
                    store.mark_dispatched(claim['attempt_id'])
                    store.finish(claim['attempt_id'], 'SUCCESS' if number == 4 else 'RETRYABLE_FAILURE',
                                 payload='0x1' if number == 4 else None,
                                 receipt={'synthetic': True} if number == 4 else None)
                restarted = RecoveryReadRetryStore(path, clock=lambda: 10, rng=lambda: 0)
                cached = restarted.claim(identity)
                self.assertEqual(cached['state'], 'CACHE_HIT')
                self.assertEqual(cached['payload'], '0x1')
                self.assertEqual(len(restarted.attempts(identity)), 4)

    def test_fresh_ungranted_key_stays_three(self):
        identity = {'provider': 'synthetic', 'method': 'eth_getBalance'}
        with tempfile.TemporaryDirectory() as directory:
            store = RecoveryReadRetryStore(Path(directory) / 'retry.sqlite', clock=lambda: 10, rng=lambda: 0)
            for number in range(3):
                claim = store.claim(identity)
                store.mark_dispatched(claim['attempt_id'])
                store.finish(claim['attempt_id'], 'RETRYABLE_FAILURE')
            self.assertEqual(store.claim(identity)['state'], 'RETRIES_EXHAUSTED')


if __name__ == '__main__':
    unittest.main()
