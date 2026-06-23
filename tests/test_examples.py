"""Smoke tests for documented examples."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


def _load_release_ops_example():
    root = Path(__file__).resolve().parents[1]
    path = root / "examples" / "release_ops_resource_closeout.py"
    spec = importlib.util.spec_from_file_location("release_ops_resource_closeout", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ExamplesTests(unittest.TestCase):
    def test_release_ops_resource_closeout_example_runs_both_adapters(self):
        module = _load_release_ops_example()

        status, finalizer_calls = module.run_example()

        self.assertEqual(status.state, "converged")
        self.assertEqual(
            finalizer_calls,
            [
                ("ath-listener-pr-1020", "success"),
                ("relay-automation-run-pr-1020", "success"),
            ],
        )
        self.assertEqual(
            [resource["kind"] for resource in status.resources],
            ["ath.listener", "relay.automation_run"],
        )
        self.assertEqual(
            [result["action"] for result in status.finalizer_results],
            ["ath.listener.retire", "relay.automation_run.retire"],
        )
        self.assertTrue(all(result["status"] == "succeeded" for result in status.finalizer_results))
        dumped = str(status.as_dict())
        self.assertNotIn("hermes_workflows", dumped)
        self.assertNotIn("secret", dumped.lower())


if __name__ == "__main__":
    unittest.main()
