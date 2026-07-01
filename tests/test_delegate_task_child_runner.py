"""Tests for Hermes delegate_task-backed workflow child agents."""

from __future__ import annotations

import json
from typing import Any

from hermes_workflows import (
    ChildAgentRequest,
    DelegateTaskChildAgentRunner,
    build_delegate_task_context,
    parse_delegate_task_json_summary,
    run_workflow_script,
)

META = 'meta = {"name": "delegate-child", "description": "d"}\n'


class FakeDispatcher:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, tool_name: str, args: dict[str, Any]) -> str:
        self.calls.append((tool_name, args))
        return json.dumps(self.payload)


def test_build_delegate_task_context_includes_schema_and_workflow_metadata():
    request = ChildAgentRequest(
        prompt="summarize",
        label="summary",
        phase="analysis",
        schema={"answer": "string"},
        context={"issue": 97},
        effort="medium",
    )

    context = build_delegate_task_context(request)

    assert "label: summary" in context
    assert "phase: analysis" in context
    assert '"issue": 97' in context
    assert '"answer": "string"' in context
    assert "Return ONLY a JSON object" in context


def test_parse_delegate_task_json_summary_accepts_bare_and_fenced_objects():
    assert parse_delegate_task_json_summary('{"answer":"ok"}') == {"answer": "ok"}
    assert parse_delegate_task_json_summary('```json\n{"answer":"ok"}\n```') == {"answer": "ok"}


def test_delegate_task_runner_sync_parses_structured_summary():
    dispatcher = FakeDispatcher(
        {
            "results": [
                {
                    "task_index": 0,
                    "status": "completed",
                    "summary": '{"answer": "ok", "_tokens": 5}',
                    "api_calls": 3,
                    "duration_seconds": 1.2,
                }
            ],
            "total_duration_seconds": 1.2,
        }
    )
    runner = DelegateTaskChildAgentRunner(dispatcher)

    result = runner(ChildAgentRequest("answer as JSON", schema={"answer": "string"}))

    assert result == {"answer": "ok", "_tokens": 5}
    assert dispatcher.calls[0][0] == "delegate_task"
    args = dispatcher.calls[0][1]
    assert args["goal"] == "answer as JSON"
    assert args["background"] is False
    assert args["role"] == "leaf"
    assert "structured_output_schema_json" in args["context"]


def test_delegate_task_runner_without_schema_returns_summary_envelope():
    dispatcher = FakeDispatcher(
        {"results": [{"task_index": 0, "status": "completed", "summary": "done", "api_calls": 2}]}
    )
    runner = DelegateTaskChildAgentRunner(dispatcher)

    result = runner(ChildAgentRequest("summarize"))

    assert result["summary"] == "done"
    assert result["status"] == "completed"
    assert result["api_calls"] == 2


def test_delegate_task_runner_background_returns_dispatch_handle_envelope():
    dispatcher = FakeDispatcher(
        {
            "status": "dispatched",
            "mode": "background",
            "count": 1,
            "delegation_id": "deleg_123",
            "goals": ["do work"],
            "note": "keep working",
        }
    )
    runner = DelegateTaskChildAgentRunner(dispatcher, background=True)

    result = runner(ChildAgentRequest("do work"))

    assert result == {
        "delegation_status": "dispatched",
        "delegation_id": "deleg_123",
        "mode": "background",
        "count": 1,
        "goals": ["do work"],
        "note": "keep working",
    }
    assert dispatcher.calls[0][1]["background"] is True


def test_delegate_task_runner_can_power_workflow_prompt_agent_sync():
    dispatcher = FakeDispatcher(
        {"results": [{"task_index": 0, "status": "completed", "summary": '{"answer": "ok"}'}]}
    )
    runner = DelegateTaskChildAgentRunner(dispatcher)
    script = META + 'return await agent("return json", {"schema": {"answer": "string"}})\n'

    result = run_workflow_script(script, child_agent_runner=runner)

    assert result.ok, result.error
    assert result.value == {"answer": "ok"}
    assert dispatcher.calls


def test_delegate_task_runner_background_can_power_prompt_agent_handle_result():
    dispatcher = FakeDispatcher(
        {"status": "dispatched", "mode": "background", "count": 1, "delegation_id": "deleg_bg"}
    )
    runner = DelegateTaskChildAgentRunner(dispatcher, background=True)
    script = META + 'return await agent("launch", {"schema": {"delegation_status": "string", "delegation_id": "string"}})\n'

    result = run_workflow_script(script, child_agent_runner=runner)

    assert result.ok, result.error
    assert result.value["delegation_status"] == "dispatched"
    assert result.value["delegation_id"] == "deleg_bg"
