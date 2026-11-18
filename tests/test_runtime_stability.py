"""Guard the production runtime against reintroducing native event-loop extras."""
import importlib.util
import unittest


class RuntimeStabilityTests(unittest.TestCase):
    def test_uvloop_is_not_installed(self):
        # Production explicitly selects asyncio; a transitive uvloop install
        # would make local/default Uvicorn behavior differ from Render.
        self.assertIsNone(importlib.util.find_spec("uvloop"))

    def test_httptools_is_not_installed(self):
        # Production explicitly selects h11 so the HTTP parser is predictable.
        self.assertIsNone(importlib.util.find_spec("httptools"))


if __name__ == "__main__":
    unittest.main()
