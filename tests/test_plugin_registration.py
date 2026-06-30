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

    assert set(ctx.tools) == {"workflow", "workflow_control"}
    action_enum = ctx.tools["workflow"]["schema"]["parameters"]["properties"]["action"]["enum"]
    assert "script_save" in action_enum
    assert "run_script" in action_enum
    assert "script_source" in ctx.tools["workflow"]["schema"]["parameters"]["properties"]
    for field in ("script", "scriptPath", "name", "args", "resumeFromRunId"):
        assert field in ctx.tools["workflow"]["schema"]["parameters"]["properties"]
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
    validate_payload = json.loads(
        ctx.tools["workflow"]["handler"]({"definition": definition, "action": "validate"})
    )
    assert validate_payload["success"] is True
    assert validate_payload["data"]["validation"]["ok"] is True

    run_payload = json.loads(
        ctx.tools["workflow"]["handler"]({"definition": definition, "inputs": {"name": "world"}})
    )
    assert run_payload["success"] is True
    run_id = run_payload["data"]["handle"]["run_id"]

    status_payload = json.loads(ctx.tools["workflow"]["handler"]({"run_id": run_id}))
    assert status_payload["success"] is True
    assert status_payload["data"]["status"]["status"] == "succeeded"

    unified_payload = json.loads(
        ctx.tools["workflow"]["handler"](
            {"definition": definition, "inputs": {"name": "world"}, "include_journal": True}
        )
    )
    assert unified_payload["success"] is True
    assert unified_payload["data"]["operation"] == "run"
    assert unified_payload["data"]["journal"]


def test_registered_workflow_handler_runs_saved_script_harness():
    old_state_dir = os.environ.get("HERMES_WORKFLOWS_STATE_DIR")
    old_script_dir = os.environ.get("HERMES_WORKFLOWS_SCRIPT_CATALOG_DIR")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        os.environ["HERMES_WORKFLOWS_STATE_DIR"] = str(root / "runs")
        os.environ["HERMES_WORKFLOWS_SCRIPT_CATALOG_DIR"] = str(root / "scripts")
        try:
            plugin = _load_plugin_root()
            ctx = FakeContext()
            plugin.register(ctx)
            source = (
                'meta = {"name": "saved", "description": "plugin script"}\n'
                'return {"value": args["value"]}\n'
            )

            save_payload = json.loads(
                ctx.tools["workflow"]["handler"](
                    {"action": "script_save", "script_name": "saved", "script_source": source}
                )
            )
            run_payload = json.loads(
                ctx.tools["workflow"]["handler"](
                    {"action": "run_script", "script_name": "saved", "script_args": {"value": 42}}
                )
            )
            inline_payload = json.loads(
                ctx.tools["workflow"]["handler"]({"script": source, "args": {"value": 7}})
            )
            name_payload = json.loads(
                ctx.tools["workflow"]["handler"]({"name": "saved", "args": {"value": 8}})
            )
            path_payload = json.loads(
                ctx.tools["workflow"]["handler"](
                    {"scriptPath": "saved/v000001.workflow.py", "args": {"value": 9}}
                )
            )
        finally:
            if old_state_dir is None:
                os.environ.pop("HERMES_WORKFLOWS_STATE_DIR", None)
            else:
                os.environ["HERMES_WORKFLOWS_STATE_DIR"] = old_state_dir
            if old_script_dir is None:
                os.environ.pop("HERMES_WORKFLOWS_SCRIPT_CATALOG_DIR", None)
            else:
                os.environ["HERMES_WORKFLOWS_SCRIPT_CATALOG_DIR"] = old_script_dir

    assert save_payload["success"] is True
    assert save_payload["data"]["script"]["name"] == "saved"
    assert run_payload["success"] is True
    assert run_payload["data"]["operation"] == "run_script"
    assert run_payload["data"]["result"]["ok"] is True
    assert run_payload["data"]["result"]["value"] == {"value": 42}
    assert inline_payload["success"] is True
    assert inline_payload["data"]["source"] == "inline_script"
    assert inline_payload["data"]["run_id"]
    assert inline_payload["data"]["status"] == "succeeded"
    assert inline_payload["data"]["result"]["value"] == {"value": 7}
    assert name_payload["success"] is True
    assert name_payload["data"]["name"] == "saved"
    assert name_payload["data"]["result"]["value"] == {"value": 8}
    assert path_payload["success"] is True
    assert path_payload["data"]["script_path"] == "saved/v000001.workflow.py"
    assert path_payload["data"]["result"]["value"] == {"value": 9}


def _workflow_control_status_for_saved_script(source: str) -> dict[str, Any]:
    old_state_dir = os.environ.get("HERMES_WORKFLOWS_STATE_DIR")
    old_script_dir = os.environ.get("HERMES_WORKFLOWS_SCRIPT_CATALOG_DIR")
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        os.environ["HERMES_WORKFLOWS_STATE_DIR"] = str(root / "runs")
        os.environ["HERMES_WORKFLOWS_SCRIPT_CATALOG_DIR"] = str(root / "scripts")
        try:
            plugin = _load_plugin_root()
            ctx = FakeContext()
            plugin.register(ctx)
            json.loads(
                ctx.tools["workflow"]["handler"](
                    {"action": "script_save", "script_name": "saved", "script_source": source}
                )
            )
            run_payload = json.loads(
                ctx.tools["workflow"]["handler"](
                    {"action": "run_script", "script_name": "saved", "script_args": {"value": 42}}
                )
            )
            run_id = run_payload["data"]["result"]["run_id"]
            return json.loads(ctx.tools["workflow_control"]["handler"]({"action": "status", "run_id": run_id}))
        finally:
            if old_state_dir is None:
                os.environ.pop("HERMES_WORKFLOWS_STATE_DIR", None)
            else:
                os.environ["HERMES_WORKFLOWS_STATE_DIR"] = old_state_dir
            if old_script_dir is None:
                os.environ.pop("HERMES_WORKFLOWS_SCRIPT_CATALOG_DIR", None)
            else:
                os.environ["HERMES_WORKFLOWS_SCRIPT_CATALOG_DIR"] = old_script_dir


def test_workflow_control_status_surfaces_script_declared_phases():
    source = (
        'meta = {"name": "saved", "description": "plugin script", '
        '"phases": [{"title": "Plan", "detail": "choose work"}, {"title": "Build"}]}\n'
        'return {"value": args["value"]}\n'
    )
    status_payload = _workflow_control_status_for_saved_script(source)

    assert status_payload["success"] is True
    assert status_payload["data"]["lifecycle"] == "succeeded"
    assert status_payload["data"]["phases"] == [
        {"id": "phase_1", "title": "Plan", "detail": "choose work", "status": "queued"},
        {"id": "phase_2", "title": "Build", "detail": "", "status": "queued"},
    ]


def test_workflow_control_status_surfaces_legacy_script_phase_strings():
    source = (
        'meta = {"name": "saved", "description": "plugin script", "phases": ["Plan", "Build"]}\n'
        'return {"value": args["value"]}\n'
    )
    status_payload = _workflow_control_status_for_saved_script(source)

    assert status_payload["success"] is True
    assert status_payload["data"]["lifecycle"] == "succeeded"
    assert status_payload["data"]["phases"] == [
        {"id": "phase_1", "title": "Plan", "detail": "", "status": "queued"},
        {"id": "phase_2", "title": "Build", "detail": "", "status": "queued"},
    ]
