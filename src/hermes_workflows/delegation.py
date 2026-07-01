"""Hermes ``delegate_task`` adapter for workflow prompt child agents.

The core workflow package stays pure stdlib and host-neutral.  This module is a
small adapter boundary: callers inject a dispatcher that knows how to call a
host tool (for the Hermes plugin this is ``PluginContext.dispatch_tool``), and
we convert workflow-script ``agent(prompt, opts)`` requests into
``delegate_task`` calls.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Protocol

from .agents import ChildAgentRequest, ChildAgentRunner

__all__ = [
    "ToolDispatcher",
    "DelegateTaskChildAgentRunner",
    "build_delegate_task_context",
    "parse_delegate_task_json_summary",
]


class ToolDispatcher(Protocol):
    """Callable used to invoke a host-owned Hermes tool."""

    def __call__(self, tool_name: str, args: dict[str, Any]) -> str | dict[str, Any]:
        """Dispatch ``tool_name`` with JSON-like ``args`` and return the tool payload."""
        ...


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.IGNORECASE | re.DOTALL)
_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)


def build_delegate_task_context(request: ChildAgentRequest) -> str:
    """Return a compact, explicit context block for a delegated workflow child."""

    rows: list[str] = [
        "You are a child agent spawned by a Dynamic Workflows script.",
        "You do not have the parent conversation unless details are included below.",
    ]
    if request.label:
        rows.append(f"label: {request.label}")
    if request.phase:
        rows.append(f"phase: {request.phase}")
    if request.model:
        rows.append(f"requested_model: {request.model}")
    if request.effort:
        rows.append(f"requested_effort: {request.effort}")
    if request.isolation:
        rows.append(f"requested_isolation: {request.isolation}")
    if request.context:
        rows.append("workflow_context_json:")
        rows.append(json.dumps(request.context, ensure_ascii=False, sort_keys=True))
    if request.schema:
        rows.extend(
            [
                "structured_output_schema_json:",
                json.dumps(request.schema, ensure_ascii=False, sort_keys=True),
                "Return ONLY a JSON object matching the schema. Do not wrap it in prose.",
            ]
        )
    return "\n".join(rows)


def parse_delegate_task_json_summary(summary: str) -> dict[str, Any]:
    """Parse a JSON object from a delegate_task child summary.

    ``delegate_task`` returns child summaries, not a typed object channel.  For
    schema-backed workflow calls we instruct the child to return only JSON, then
    accept either a bare object or a fenced JSON object for robustness.
    """

    text = summary.strip()
    candidates = [text]
    fence = _JSON_FENCE_RE.search(text)
    if fence:
        candidates.insert(0, fence.group(1).strip())
    obj_match = _JSON_OBJECT_RE.search(text)
    if obj_match:
        candidates.append(obj_match.group(0).strip())

    errors: list[str] = []
    for candidate in candidates:
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError as exc:
            errors.append(str(exc))
            continue
        if not isinstance(value, dict):
            raise ValueError("delegate_task summary JSON must be an object")
        return value
    detail = errors[0] if errors else "no JSON object found"
    raise ValueError(f"delegate_task summary did not contain a JSON object: {detail}")


@dataclass
class DelegateTaskChildAgentRunner(ChildAgentRunner):
    """Run workflow prompt child agents through Hermes ``delegate_task``.

    Foreground mode waits for ``delegate_task``'s synchronous result and parses a
    structured JSON object from the first child summary when a schema is present.
    Background mode intentionally returns only a dispatch envelope; it does not
    pretend the workflow has the child result yet.
    """

    dispatch: ToolDispatcher
    background: bool = False
    role: str = "leaf"
    extra_args: dict[str, Any] = field(default_factory=dict)

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        args: dict[str, Any] = {
            "goal": request.prompt,
            "context": build_delegate_task_context(request),
            "role": self.role,
            "background": self.background,
        }
        args.update(self.extra_args)
        payload = self.dispatch("delegate_task", args)
        data = _coerce_tool_payload(payload)
        if self.background:
            return _background_dispatch_envelope(data)
        return _foreground_result(data, request)


def _coerce_tool_payload(payload: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload, dict):
        data = payload
    elif isinstance(payload, str):
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"delegate_task returned non-JSON payload: {exc}") from exc
    else:
        raise RuntimeError(f"delegate_task returned unsupported payload type: {type(payload).__name__}")
    if not isinstance(data, dict):
        raise RuntimeError("delegate_task payload must be a JSON object")
    if "error" in data and not data.get("results"):
        raise RuntimeError(str(data["error"]))
    return data


def _background_dispatch_envelope(data: dict[str, Any]) -> dict[str, Any]:
    if data.get("status") != "dispatched":
        # delegate_task can fall back to synchronous execution when async delivery
        # is unavailable; surface that honestly instead of losing the result.
        if "results" in data:
            return {"delegation_status": "completed_inline", "delegate_task": data}
        raise RuntimeError(f"delegate_task background dispatch failed: {data!r}")
    return {
        "delegation_status": "dispatched",
        "delegation_id": data.get("delegation_id"),
        "mode": data.get("mode", "background"),
        "count": data.get("count", 1),
        "goals": data.get("goals") or [],
        "note": data.get("note"),
    }


def _foreground_result(data: dict[str, Any], request: ChildAgentRequest) -> dict[str, Any]:
    results = data.get("results")
    if not isinstance(results, list) or not results:
        raise RuntimeError("delegate_task foreground result did not include results[]")
    first = results[0]
    if not isinstance(first, dict):
        raise RuntimeError("delegate_task child result must be an object")
    status = str(first.get("status") or "")
    if status not in {"completed", "success"}:
        raise RuntimeError(str(first.get("error") or f"delegate_task child status={status!r}"))
    summary = first.get("summary")
    if not isinstance(summary, str):
        raise RuntimeError("delegate_task child result did not include a text summary")
    if request.schema:
        return parse_delegate_task_json_summary(summary)
    return {
        "summary": summary,
        "status": status,
        "task_index": first.get("task_index"),
        "api_calls": first.get("api_calls"),
        "duration_seconds": first.get("duration_seconds"),
    }
