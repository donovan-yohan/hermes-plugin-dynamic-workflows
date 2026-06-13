"""Hermes plugin registration for dynamic workflow primitives.

This root module is the Hermes plugin entrypoint. It wraps the pure-stdlib
``hermes_workflows`` package with JSON-serializable tool handlers so a checkout
can be installed or symlinked under ``~/.hermes/plugins/hermes-dynamic-workflows``.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

# Project-local plugin loading does not necessarily install the src-layout
# package first. Make a checkout usable as a Hermes plugin directory.
_SRC = Path(__file__).resolve().parent / "src"
if _SRC.exists():
    src_text = str(_SRC)
    if src_text not in sys.path:
        sys.path.insert(0, src_text)

from hermes_workflows.errors import WorkflowError  # noqa: E402
from hermes_workflows.primitives import (  # noqa: E402
    workflow as _workflow,
    workflow_run as _workflow_run,
    workflow_status as _workflow_status,
    workflow_validate as _workflow_validate,
)
from hermes_workflows.registry import FileRunStore  # noqa: E402
from hermes_workflows.catalog import FileWorkflowCatalog  # noqa: E402

TOOLSET = "dynamic_workflows"


def _jsonable(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _ok(payload: Any) -> str:
    return json.dumps({"success": True, "data": _jsonable(payload)}, ensure_ascii=False)


def _error(exc: Exception) -> str:
    payload: dict[str, Any] = {
        "success": False,
        "error": {"type": type(exc).__name__, "message": str(exc)},
    }
    result = getattr(exc, "result", None)
    if result is not None:
        payload["validation"] = _jsonable(result)
    return json.dumps(payload, ensure_ascii=False)


def _plugin_store() -> FileRunStore:
    root = os.getenv("HERMES_WORKFLOWS_STATE_DIR")
    if not root:
        hermes_home = Path(os.getenv("HERMES_HOME") or Path.home() / ".hermes").expanduser()
        root = str(hermes_home / "dynamic-workflows" / "runs")
    return FileRunStore(root)


def _plugin_catalog() -> FileWorkflowCatalog:
    return FileWorkflowCatalog()


WORKFLOW_SCHEMA = {
    "name": "workflow",
    "description": (
        "Single dynamic workflow entry point. Validate with dry_run/action=validate, "
        "run with a definition, or query status with run_id. Uses a parent-owned "
        "filesystem run store with compact journal events."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": ["string", "null"],
                "enum": ["validate", "run", "status", "catalog", "run_template", None],
                "description": "Optional explicit operation. Defaults from supplied fields.",
                "default": None,
            },
            "definition": {
                "description": "Workflow definition as a JSON object or JSON string.",
                "oneOf": [{"type": "object"}, {"type": "string"}, {"type": "null"}],
                "default": None,
            },
            "inputs": {
                "type": ["object", "null"],
                "description": "Run inputs referenced by $ref:inputs.<key>.",
                "default": None,
            },
            "run_id": {
                "type": ["string", "null"],
                "description": "Existing run id for status, or caller-supplied id for run.",
                "default": None,
            },
            "template_name": {
                "type": ["string", "null"],
                "description": "Safe template name for action=run_template.",
                "default": None,
            },
            "dry_run": {
                "type": "boolean",
                "description": "Validate only, without creating a run.",
                "default": False,
            },
            "validate": {"type": "boolean", "default": True},
            "max_parallel": {"type": "integer", "minimum": 1, "maximum": 64, "default": 8},
            "include_steps": {"type": "boolean", "default": True},
            "include_journal": {
                "type": "boolean",
                "description": "Include recent compact journal events for file-backed runs.",
                "default": False,
            },
            "journal_limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100},
        },
        "additionalProperties": False,
    },
}

WORKFLOW_VALIDATE_SCHEMA = {
    "name": "workflow_validate",
    "description": (
        "Statically validate a dynamic workflow definition without running agents. "
        "Checks JSON/schema shape, references, cycles, known agent ids, and sandbox policy."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "definition": {
                "description": "Workflow definition as a JSON object or JSON string.",
                "oneOf": [{"type": "object"}, {"type": "string"}],
            },
            "source_path": {
                "type": ["string", "null"],
                "description": "Optional source path used for diagnostic context.",
                "default": None,
            },
            "strict": {
                "type": "boolean",
                "description": "Promote lint warnings to errors.",
                "default": True,
            },
        },
        "required": ["definition"],
        "additionalProperties": False,
    },
}

WORKFLOW_RUN_SCHEMA = {
    "name": "workflow_run",
    "description": (
        "Run a validated dynamic workflow in the deterministic skeleton runtime. "
        "Uses the stub AgentRunner by default; no network or filesystem effects."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "definition": {
                "description": "Workflow definition as a JSON object or JSON string.",
                "oneOf": [{"type": "object"}, {"type": "string"}],
            },
            "inputs": {
                "type": ["object", "null"],
                "description": "Run inputs referenced by $ref:inputs.<key>.",
                "default": None,
            },
            "validate": {
                "type": "boolean",
                "description": "Validate before running.",
                "default": True,
            },
            "max_parallel": {
                "type": "integer",
                "minimum": 1,
                "maximum": 64,
                "description": "Logical fan-out bound.",
                "default": 8,
            },
            "run_id": {
                "type": ["string", "null"],
                "description": "Optional caller-supplied id for idempotency/testing.",
                "default": None,
            },
        },
        "required": ["definition"],
        "additionalProperties": False,
    },
}

WORKFLOW_STATUS_SCHEMA = {
    "name": "workflow_status",
    "description": "Query state/progress for a dynamic workflow run by id.",
    "parameters": {
        "type": "object",
        "properties": {
            "run_id": {"type": "string", "description": "Run id returned by workflow_run."},
            "include_steps": {
                "type": "boolean",
                "description": "Include per-step status records.",
                "default": True,
            },
        },
        "required": ["run_id"],
        "additionalProperties": False,
    },
}


def _handle_workflow(params: dict[str, Any], **_: Any) -> str:
    try:
        store = _plugin_store()
        result = _workflow(
            action=params.get("action"),
            definition=params.get("definition"),
            inputs=params.get("inputs"),
            run_id=params.get("run_id"),
            template_name=params.get("template_name"),
            dry_run=params.get("dry_run", False),
            registry=store,
            catalog=_plugin_catalog(),
            validate=params.get("validate", True),
            max_parallel=params.get("max_parallel", 8),
            include_steps=params.get("include_steps", True),
        )
        if params.get("include_journal") and params.get("run_id"):
            result["journal"] = store.journal(params["run_id"], limit=params.get("journal_limit", 100))
        elif params.get("include_journal") and result.get("handle"):
            rid = result["handle"]["run_id"]
            result["journal"] = store.journal(rid, limit=params.get("journal_limit", 100))
        return _ok(result)
    except WorkflowError as exc:
        return _error(exc)
    except Exception as exc:  # pragma: no cover - defensive boundary
        return _error(exc)


def _handle_validate(params: dict[str, Any], **_: Any) -> str:
    try:
        result = _workflow_validate(
            params["definition"],
            source_path=params.get("source_path"),
            strict=params.get("strict", True),
        )
        return _ok(result)
    except Exception as exc:  # pragma: no cover - defensive boundary
        return _error(exc)


def _handle_run(params: dict[str, Any], **_: Any) -> str:
    try:
        handle = _workflow_run(
            params["definition"],
            inputs=params.get("inputs"),
            registry=_plugin_store(),
            validate=params.get("validate", True),
            max_parallel=params.get("max_parallel", 8),
            run_id=params.get("run_id"),
        )
        return _ok(handle)
    except WorkflowError as exc:
        return _error(exc)
    except Exception as exc:  # pragma: no cover - defensive boundary
        return _error(exc)


def _handle_status(params: dict[str, Any], **_: Any) -> str:
    try:
        status = _workflow_status(
            params["run_id"],
            registry=_plugin_store(),
            include_steps=params.get("include_steps", True),
        )
        return _ok(status)
    except Exception as exc:  # pragma: no cover - defensive boundary
        return _error(exc)


def register(ctx: Any) -> None:
    """Register dynamic workflow tools with Hermes."""
    ctx.register_tool(
        name="workflow",
        toolset=TOOLSET,
        schema=WORKFLOW_SCHEMA,
        handler=_handle_workflow,
        description="Validate, run, or inspect a dynamic workflow via one model-facing entry point.",
    )
