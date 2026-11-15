import asyncio
import unittest

from main import ping


class PingEndpointTests(unittest.TestCase):
    def test_ping_returns_empty_204_response(self):
        response = asyncio.run(ping())
        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.body, b"")


if __name__ == "__main__":
    unittest.main()
