"""Smoke tests for documented examples."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

from hermes_workflows import run_workflow_script


def _load_release_ops_example():
    root = Path(__file__).resolve().parents[1]
    path = root / "examples" / "release_ops_resource_closeout.py"
    spec = importlib.util.spec_from_file_location("release_ops_resource_closeout", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["release_ops_resource_closeout"] = module
    spec.loader.exec_module(module)
    return module


def _extract_python_block_after(path: Path, marker: str) -> str:
    text = path.read_text(encoding="utf-8")
    marker_pos = text.index(marker)
    block_start = text.index("```python", marker_pos) + len("```python")
    block_end = text.index("```", block_start)
    return text[block_start:block_end].strip() + "\n"


def _bughunter_agent_runner(agent_id, input):
    if agent_id == "hermes.bughunter":
        return {"bugs": [], "followups": []}
    return {"echo": dict(input)}


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

    def test_loop_until_dry_docs_examples_run_without_args(self):
        root = Path(__file__).resolve().parents[1]
        cases = [
            (root / "README.md", "Side-by-side translation"),
            (root / "DESIGN.md", "Side-by-side translation"),
        ]

        for path, marker in cases:
            with self.subTest(path=path.name):
                source = _extract_python_block_after(path, marker)
                result = run_workflow_script(source, agent_runner=_bughunter_agent_runner)

                self.assertTrue(result.ok, result.error)
                self.assertEqual(result.value, {"remaining_areas": [], "rounds": 1})


if __name__ == "__main__":
    unittest.main()
