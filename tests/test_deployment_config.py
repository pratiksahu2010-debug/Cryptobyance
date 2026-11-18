from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DeploymentDefaultsTests(unittest.TestCase):
    def test_render_blueprint_does_not_override_adx_minimum_to_25(self):
        render = (ROOT / "render.yaml").read_text()
        self.assertIn('      - key: ADX_MIN\n        value: "20"', render)
        self.assertNotIn('      - key: ADX_MIN\n        value: "25"', render)

    def test_render_blueprint_configures_health_check_endpoint(self):
        render = (ROOT / "render.yaml").read_text()
        self.assertIn("    healthCheckPath: /health", render)

    def test_example_environment_uses_adx_minimum_20(self):
        example = (ROOT / ".env.example").read_text()
        self.assertIn("ADX_MIN=20", example)
        self.assertNotIn("ADX_MIN=25", example)


if __name__ == "__main__":
    unittest.main()
