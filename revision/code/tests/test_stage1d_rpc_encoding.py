import tempfile
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch
from context_access_r4 import RpcAccess, RPC_MAX

class RpcEncodingTests(unittest.TestCase):
    def test_real_transport_requests_identity_and_bounds_decoded_content(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            (work / 'private').mkdir()
            access = RpcAccess.__new__(RpcAccess)
            access.w = work
            access.runtime = object()
            access.transport = None
            access.clock = lambda: 1000
            access.sleep = lambda delay: None
            http = MagicMock()
            session = http.Session.return_value.__enter__.return_value
            response = session.post.return_value.__enter__.return_value
            response.status_code = 200
            response.raw.read.return_value = b'[]'
            response.headers.get.return_value = None
            with patch.dict('sys.modules', {'requests': http}), patch.dict('os.environ', {'ALCHEMY_API_KEY': 'synthetic'}), patch('stage1d_recovery_policy.effective', return_value={'adopted': True}):
                result = access._transport([])
            self.assertEqual(result, (200, b'[]', {'Retry-After': None}))
            self.assertEqual(session.post.call_args.kwargs['headers']['Accept-Encoding'], 'identity')
            response.raw.read.assert_called_once_with(RPC_MAX+1, decode_content=True)
            self.assertEqual(session.post.call_args.kwargs['timeout'], (10,60))

if __name__ == '__main__':
    unittest.main()
