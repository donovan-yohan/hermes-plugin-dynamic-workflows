"""Hermes ``delegate_task`` adapter for workflow prompt child agents.

The core workflow package stays pure stdlib and host-neutral.  This module is a
small adapter boundary: callers inject a dispatcher that knows how to call
``delegate_task`` (for the Hermes plugin this wraps ``PluginContext.dispatch_tool``),
and we convert workflow-script ``agent(prompt, opts)`` requests into delegate
calls.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Protocol

from .agents import ChildAgentRequest, ChildAgentRunner

__all__ = [
    "DelegateTaskDispatcher",
    "DelegateTaskChildAgentRunner",
    "build_delegate_task_context",
    "parse_delegate_task_json_summary",
]


class DelegateTaskDispatcher(Protocol):
    """Callable used to invoke host-owned Hermes ``delegate_task``."""

    def __call__(self, args: dict[str, Any]) -> str | dict[str, Any]:
        """Dispatch ``delegate_task`` with JSON-like ``args`` and return the payload."""
        ...


_JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*(\{.*\})\s*```\s*$", re.IGNORECASE | re.DOTALL)
_RESERVED_DELEGATE_TASK_ARGS = frozenset({"goal", "context", "role", "background"})
_PROMPT_BEARING_KEYS = frozenset({"context", "goal", "goals", "prompt", "prompts", "tasks"})
_REDACTED = "[redacted]"


def build_delegate_task_context(request: ChildAgentRequest) -> str:
    """Return a compact, explicit context block for a delegated workflow child."""

    rows: list[str] = []
    if request.system_prompt:
        # The broker-resolved agent-type system prompt (issue #104) leads the
        # block so it reads as the child's role, before the harness framing.
        rows.append(request.system_prompt)
    rows.extend(
        [
            "You are a child agent spawned by a Dynamic Workflows script.",
            "You do not have the parent conversation unless details are included below.",
        ]
    )
    if request.agent_type:
        rows.append(f"agent_type: {request.agent_type}")
    if request.label:
        rows.append(f"label: {request.label}")
    if request.phase:
        rows.append(f"phase: {request.phase}")
    if request.tools is not None:
        # The tools allowlist (issue #101) is advisory over this seam until the
        # dispatcher enforces scoping host-side; stating it keeps the child's
        # instructions honest about what it may touch.
        rows.append("allowed_tools: " + (", ".join(request.tools) if request.tools else "(none)"))
    if request.model:
        rows.append(f"requested_model: {request.model}")
    if request.effort:
        rows.append(f"requested_effort: {request.effort}")
    if request.isolation:
        rows.append(f"requested_isolation: {request.isolation}")
    if request.context:
        rows.append("workflow_context_json:")
        rows.append(_strict_json(request.context, what="workflow context"))
    if request.schema:
        rows.extend(
            [
                "structured_output_schema_json:",
                _strict_json(request.schema, what="structured output schema"),
                "Return ONLY a JSON object matching the schema. Do not wrap it in prose.",
            ]
        )
    return "\n".join(rows)


def _strict_json(value: Any, *, what: str) -> str:
    """Serialize strictly (no ``default=``), failing closed with a clean error.

    Script-supplied values always crossed the JSON RPC wire and cannot be
    non-serializable; an in-process host constructing ``ChildAgentRequest``
    directly can pass one, and a ``default=str`` fallback would leak object
    reprs into the delegated child's instructions instead of failing here.
    """
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"delegate_task {what} is not JSON-serializable") from exc


def parse_delegate_task_json_summary(summary: str) -> dict[str, Any]:
    """Parse a JSON object from a delegate_task child summary.

    ``delegate_task`` returns child summaries, not a typed object channel.  For
    schema-backed workflow calls we instruct the child to return only JSON, then
    accept either a bare object or a single fenced JSON object for robustness.
    """

    text = summary.strip()
    fence = _JSON_FENCE_RE.match(text)
    candidates = [fence.group(1).strip()] if fence else [text]

    try:
        value = json.loads(candidates[0])
    except json.JSONDecodeError as exc:
        raise ValueError(f"delegate_task summary did not contain a JSON object: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError("delegate_task summary JSON must be an object")
    return value


@dataclass
class DelegateTaskChildAgentRunner(ChildAgentRunner):
    """Run workflow prompt child agents through Hermes ``delegate_task``.

    Foreground mode waits for ``delegate_task``'s synchronous result and parses a
    structured JSON object from the first child summary when a schema is present.
    Background mode intentionally returns only a dispatch envelope; it does not
    pretend the workflow has the child result yet.
    """

    dispatch: DelegateTaskDispatcher
    background: bool = False
    role: str = "leaf"
    extra_args: dict[str, Any] = field(default_factory=dict)

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        collisions = sorted(set(self.extra_args).intersection(_RESERVED_DELEGATE_TASK_ARGS))
        if collisions:
            raise ValueError(f"delegate_task extra_args cannot override reserved fields: {', '.join(collisions)}")
        args: dict[str, Any] = dict(self.extra_args)
        args.update(
            {
                "goal": request.prompt,
                "context": build_delegate_task_context(request),
                "role": self.role,
                "background": self.background,
            }
        )
        payload = self.dispatch(args)
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
            return {"delegation_status": "completed_inline", "delegate_task": _redact_prompt_payload(data)}
        raise RuntimeError(f"delegate_task background dispatch failed: {data!r}")
    return {
        "delegation_status": "dispatched",
        "delegation_id": data.get("delegation_id"),
        "mode": data.get("mode", "background"),
        "count": data.get("count", 1),
        "note": data.get("note"),
    }


def _redact_prompt_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _REDACTED if str(key).lower() in _PROMPT_BEARING_KEYS else _redact_prompt_payload(child)
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [_redact_prompt_payload(child) for child in value]
    return value


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
