"""Tests for the Hermes plugin registration entrypoint."""

from __future__ import annotations

import importlib.util
import json
import os
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Any


class FakeContext:
    def __init__(self) -> None:
        self.tools: dict[str, dict[str, Any]] = {}

    def register_tool(self, **kwargs: Any) -> None:
        self.tools[kwargs["name"]] = kwargs


def _load_plugin_root() -> ModuleType:
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("dynamic_workflows_plugin", root / "__init__.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _hello_definition() -> dict[str, Any]:
    return {
        "version": "1",
        "name": "hello",
        "inputs": {"name": "string"},
        "policy": {"network": False, "filesystem": False, "max_parallel": 2},
        "steps": [
            {
                "kind": "agent",
                "id": "greet",
                "agent": "hermes.greeter",
                "input": {"subject": "$ref:inputs.name"},
                "output_schema": {"greeting": "string"},
            },
            {
                "kind": "agent",
                "id": "shout",
                "agent": "hermes.uppercaser",
                "input": {"text": "$ref:greet.output.greeting"},
                "output_schema": {"result": "string"},
                "depends_on": ["greet"],
            },
        ],
    }


def test_register_exposes_workflow_tools():
    plugin = _load_plugin_root()
    ctx = FakeContext()
    plugin.register(ctx)

    assert set(ctx.tools) == {"workflow", "workflow_validate", "workflow_run", "workflow_status"}
    for name, registered in ctx.tools.items():
        assert registered["toolset"] == "dynamic_workflows"
        assert registered["schema"]["name"] == name
        assert callable(registered["handler"])


def test_registered_handlers_return_json_success_payloads():
    old_state_dir = os.environ.get("HERMES_WORKFLOWS_STATE_DIR")
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["HERMES_WORKFLOWS_STATE_DIR"] = str(Path(tmp) / "runs")
        try:
            _assert_registered_handlers_return_json_success_payloads()
        finally:
            if old_state_dir is None:
                os.environ.pop("HERMES_WORKFLOWS_STATE_DIR", None)
            else:
                os.environ["HERMES_WORKFLOWS_STATE_DIR"] = old_state_dir


def _assert_registered_handlers_return_json_success_payloads():
    plugin = _load_plugin_root()
    ctx = FakeContext()
    plugin.register(ctx)

    definition = _hello_definition()
    validate_payload = json.loads(ctx.tools["workflow_validate"]["handler"]({"definition": definition}))
    assert validate_payload["success"] is True
    assert validate_payload["data"]["ok"] is True

    run_payload = json.loads(
        ctx.tools["workflow_run"]["handler"]({"definition": definition, "inputs": {"name": "world"}})
    )
    assert run_payload["success"] is True
    run_id = run_payload["data"]["run_id"]

    status_payload = json.loads(ctx.tools["workflow_status"]["handler"]({"run_id": run_id}))
    assert status_payload["success"] is True
    assert status_payload["data"]["status"] == "succeeded"

    unified_payload = json.loads(
        ctx.tools["workflow"]["handler"](
            {"definition": definition, "inputs": {"name": "world"}, "include_journal": True}
        )
    )
    assert unified_payload["success"] is True
    assert unified_payload["data"]["operation"] == "run"
    assert unified_payload["data"]["journal"]
