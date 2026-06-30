"""Hermes plugin registration for dynamic workflow primitives.

This root module is the Hermes plugin entrypoint. It wraps the pure-stdlib
``hermes_workflows`` package with JSON-serializable tool handlers so a checkout
can be installed or symlinked under ``~/.hermes/plugins/hermes-dynamic-workflows``.
"""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Optional

# Project-local plugin loading does not necessarily install the src-layout
# package first. Make a checkout usable as a Hermes plugin directory.
_SRC = Path(__file__).resolve().parent / "src"
if _SRC.exists():
    src_text = str(_SRC)
    if src_text not in sys.path:
        sys.path.insert(0, src_text)

from hermes_workflows.errors import ControlError, WorkflowError  # noqa: E402
from hermes_workflows.primitives import (  # noqa: E402
    workflow as _workflow,
    workflow_run as _workflow_run,
    workflow_status as _workflow_status,
    workflow_validate as _workflow_validate,
)
from hermes_workflows.registry import FileRunStore  # noqa: E402
from hermes_workflows.catalog import FileWorkflowCatalog  # noqa: E402
from hermes_workflows.script_catalog import FileWorkflowScriptCatalog  # noqa: E402
from hermes_workflows import controls as _controls  # noqa: E402
from hermes_workflows.controls import FileControlStore  # noqa: E402
from hermes_workflows.script_store import ScriptRunStore  # noqa: E402

TOOLSET = "dynamic_workflows"
_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def _is_safe_segment(value: Any) -> bool:
    return isinstance(value, str) and _SAFE_SEGMENT_RE.fullmatch(value) is not None


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


def _runs_root() -> Path:
    """Directory holding per-run snapshots/journals (the FileRunStore root).

    ``HERMES_WORKFLOWS_STATE_DIR`` keeps its existing meaning — the runs dir —
    so this slice is backward compatible. Controls and script runs live as
    siblings of it (see :func:`_state_root`).
    """
    root = os.getenv("HERMES_WORKFLOWS_STATE_DIR")
    if root:
        return Path(root).expanduser()
    hermes_home = Path(os.getenv("HERMES_HOME") or Path.home() / ".hermes").expanduser()
    return hermes_home / "dynamic-workflows" / "runs"


def _state_root() -> Path:
    """Parent of the runs dir, under which sibling stores (controls, script-runs) live."""
    return _runs_root().parent


def _plugin_store(session_id: Optional[str] = None) -> FileRunStore:
    return FileRunStore(str(_runs_root()), session_id=session_id)


def _plugin_control_store(session_id: Optional[str] = None) -> FileControlStore:
    root = _state_root() / "controls"
    if session_id:
        root = root / session_id
    return FileControlStore(str(root))


def _plugin_script_store() -> Optional[ScriptRunStore]:
    """Return the script-run store iff its directory already exists (best-effort).

    Used only to surface durable Kanban waits in the operator overview; absent a
    script-runs dir there is simply nothing to inspect, never an error.
    """
    path = _state_root() / "script-runs"
    if not path.exists():
        return None
    return ScriptRunStore(str(path))


def _plugin_catalog() -> FileWorkflowCatalog:
    return FileWorkflowCatalog()


def _plugin_script_catalog() -> FileWorkflowScriptCatalog:
    return FileWorkflowScriptCatalog()


def _plugin_script_run_store(session_id: Optional[str] = None) -> ScriptRunStore:
    root = _state_root() / "script-runs"
    if session_id:
        root = root / session_id
    return ScriptRunStore(str(root))


def _session_id_from_kwargs(kwargs: dict[str, Any]) -> Optional[str]:
    """Extract a Hermes session id from dispatched kwargs when available."""
    parent_agent = kwargs.get("parent_agent")
    if parent_agent is None:
        return None
    session_id = getattr(parent_agent, "session_id", None)
    if isinstance(session_id, str) and session_id:
        return session_id
    gateway_key = getattr(parent_agent, "_gateway_session_key", None)
    if isinstance(gateway_key, str) and gateway_key:
        return gateway_key
    return None


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
                "enum": [
                    "validate",
                    "run",
                    "status",
                    "catalog",
                    "run_template",
                    "script_catalog",
                    "script_save",
                    "script_inspect",
                    "run_script",
                    "run_facade_script",
                    None,
                ],
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
            "script_name": {
                "type": ["string", "null"],
                "description": "Safe saved script harness name for script_inspect/run_script/script_save.",
                "default": None,
            },
            "script_source": {
                "type": ["string", "null"],
                "description": "Workflow-script source for action=script_save.",
                "default": None,
            },
            "script_args": {
                "description": "Arguments passed to a saved script harness for action=run_script.",
                "oneOf": [
                    {"type": "object"},
                    {"type": "array"},
                    {"type": "string"},
                    {"type": "number"},
                    {"type": "boolean"},
                    {"type": "null"},
                ],
                "default": None,
            },
            "script": {
                "type": ["string", "null"],
                "description": "Inline workflow-script source for the Claude-style facade.",
                "default": None,
            },
            "scriptPath": {
                "type": ["string", "null"],
                "description": "Catalog-relative .workflow/.workflow.py script path for the Claude-style facade.",
                "default": None,
            },
            "name": {
                "type": ["string", "null"],
                "description": "Saved script harness name for the Claude-style facade.",
                "default": None,
            },
            "args": {
                "description": "Arguments passed to script/name/scriptPath facade runs.",
                "oneOf": [
                    {"type": "object"},
                    {"type": "array"},
                    {"type": "string"},
                    {"type": "number"},
                    {"type": "boolean"},
                    {"type": "null"},
                ],
                "default": None,
            },
            "resumeFromRunId": {
                "type": ["string", "null"],
                "description": "Prior script run id to replay/resume from; identity mismatches fail closed.",
                "default": None,
            },
            "script_version": {
                "type": ["integer", "null"],
                "minimum": 1,
                "description": "Optional saved script harness version.",
                "default": None,
            },
            "include_source": {
                "type": "boolean",
                "description": "Include script source in action=script_inspect output.",
                "default": False,
            },
            "include_versions": {
                "type": "boolean",
                "description": "List all saved script harness versions for action=script_catalog.",
                "default": False,
            },
            "replace": {
                "type": "boolean",
                "description": "Allow action=script_save to replace an existing explicit version.",
                "default": False,
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


WORKFLOW_CONTROL_SCHEMA = {
    "name": "workflow_control",
    "description": (
        "Operator controls and status for dynamic workflow runs. action=overview "
        "lists active/recent runs and blocked waits; action=status returns one "
        "run's compact control state, current phase, waits, child task refs, "
        "links, and the run-level enforcement decisions (may new work start / may "
        "the run continue) an adapter would consult; pause/resume/stop/task_stop/"
        "retry record an append-only control intent (the audit trail is never "
        "deleted). Retry is idempotent per target_ref with explicit replacement "
        "lineage. This surface records and decides intent; it does not itself kill "
        "processes or replay tasks — a backend adapter enforces the decisions."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["overview", "status", "pause", "resume", "stop", "task_stop", "retry"],
                "description": "Operator operation to perform.",
            },
            "run_id": {
                "type": ["string", "null"],
                "description": "Target run id (required for everything except overview).",
                "default": None,
            },
            "target_ref": {
                "type": ["string", "null"],
                "description": "Child call/task id for task_stop and retry.",
                "default": None,
            },
            "replacement_ref": {
                "type": ["string", "null"],
                "description": "Optional caller-minted replacement id for retry lineage.",
                "default": None,
            },
            "force": {
                "type": "boolean",
                "description": "Force a new retry attempt instead of returning the existing one.",
                "default": False,
            },
            "actor": {
                "type": ["string", "null"],
                "description": "Who is issuing the control (recorded for audit).",
                "default": None,
            },
            "reason": {
                "type": ["string", "null"],
                "description": "Why the control is being issued (recorded for audit).",
                "default": None,
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 200,
                "description": "Max runs returned by overview.",
                "default": 20,
            },
            "events_limit": {
                "type": "integer",
                "minimum": 0,
                "maximum": 200,
                "description": "Max recent journal events included in status.",
                "default": 10,
            },
        },
        "required": ["action"],
        "additionalProperties": False,
    },
}


def _control_link_resolver(store: FileRunStore):
    def resolve(record: Any) -> dict[str, Any]:
        run_dir = store.root / record.run_id
        return _controls.run_links(
            run_id=record.run_id,
            snapshot_path=str(run_dir / "snapshot.json"),
            journal_path=str(run_dir / "journal.jsonl"),
        )

    return resolve


def _kanban_waits(script_store: Optional[ScriptRunStore], *, run_id: Optional[str] = None) -> list:
    if script_store is None:
        return []
    try:
        states = script_store.kanban_waits()
    except Exception:  # pragma: no cover - defensive; durable read is best-effort
        return []
    return _controls.waits_from_kanban_states(states, run_id=run_id or "")


def _loop_waits(*, run_id: Optional[str] = None) -> list:
    root = _state_root() / "loop-runs"
    if not root.exists():
        return []
    snapshots: list[dict[str, Any]] = []
    if run_id:
        if not _is_safe_segment(run_id):
            return []
        paths = [root / run_id / "snapshot.json"]
    else:
        paths = sorted(root.glob("*/snapshot.json"))
    for path in paths:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(data, dict):
            snapshots.append(data)
    waits = []
    for snapshot in snapshots:
        waits.extend(_controls.waits_from_loop_status(snapshot))
    return waits


def _known_run(run_id: str, *, session_id: Optional[str]) -> bool:
    if not _is_safe_segment(run_id):
        return False
    try:
        if _plugin_store(session_id=session_id).get(run_id) is not None:
            return True
    except Exception:  # pragma: no cover - defensive; control verbs fail closed below.
        return False
    return bool(_loop_waits(run_id=run_id))


def _handle_control(params: dict[str, Any], **kwargs: Any) -> str:
    try:
        session_id = _session_id_from_kwargs(kwargs)
        action = params.get("action")
        control_store = _plugin_control_store(session_id=session_id)
        run_id = params.get("run_id")

        if action == "overview":
            run_store = _plugin_store(session_id=session_id)
            waits = _kanban_waits(_plugin_script_store()) + _loop_waits()
            overview = _controls.list_runs(
                run_store.list(),
                control_store,
                waits=waits,
                link_resolver=_control_link_resolver(run_store),
                limit=params.get("limit", 20),
            )
            return _ok({"operation": "overview", **overview})

        if not run_id:
            raise ControlError(f"workflow_control action={action!r} requires 'run_id'")
        if not _is_safe_segment(run_id):
            raise ControlError(f"unsafe run_id: {run_id!r}")

        if action == "status":
            run_store = _plugin_store(session_id=session_id)
            record = run_store.get(run_id)
            state = _controls.project_control_state(run_id, control_store.list_for(run_id))
            waits = [
                w
                for w in _kanban_waits(_plugin_script_store(), run_id=run_id) + _loop_waits(run_id=run_id)
                if w.run_id == run_id
            ]
            links = _control_link_resolver(run_store)(record) if record is not None else {"run_id": run_id}
            report = _controls.inspect_run(
                run_id,
                lifecycle=record.status if record is not None else "unknown",
                control_state=state,
                current_phase=_controls.current_phase(record.steps) if record is not None else None,
                progress=record.to_status().progress.as_dict() if record is not None else None,
                waits=waits,
                result=record.result if record is not None else None,
                error=record.error if record is not None else None,
                last_events=run_store.journal(run_id, limit=params.get("events_limit", 10))
                if record is not None
                else [],
                links=links,
                events_limit=params.get("events_limit", 10),
            )
            return _ok({"operation": "status", **report})

        verbs = {
            "pause": lambda: _controls.pause_run(control_store, run_id, actor=params.get("actor"), reason=params.get("reason")),
            "resume": lambda: _controls.resume_run(control_store, run_id, actor=params.get("actor"), reason=params.get("reason")),
            "stop": lambda: _controls.stop_run(control_store, run_id, actor=params.get("actor"), reason=params.get("reason")),
            "task_stop": lambda: _controls.stop_task(
                control_store, run_id, params.get("target_ref") or "", actor=params.get("actor"), reason=params.get("reason")
            ),
            "retry": lambda: _controls.retry(
                control_store,
                run_id,
                params.get("target_ref") or "",
                replacement_ref=params.get("replacement_ref"),
                force=params.get("force", False),
                actor=params.get("actor"),
                reason=params.get("reason"),
            ),
        }
        if action not in verbs:
            raise ControlError(f"unknown workflow_control action: {action!r}")
        if not _known_run(run_id, session_id=session_id):
            raise ControlError(f"unknown workflow run: {run_id!r}")
        control = verbs[action]()
        state = _controls.project_control_state(run_id, control_store.list_for(run_id))
        return _ok({"operation": action, "control": control.to_dict(), "control_state": state.to_dict()})
    except WorkflowError as exc:
        return _error(exc)
    except Exception as exc:  # pragma: no cover - defensive boundary
        return _error(exc)


def _handle_workflow(params: dict[str, Any], **kwargs: Any) -> str:
    try:
        session_id = _session_id_from_kwargs(kwargs)
        store = _plugin_store(session_id=session_id)
        result = _workflow(
            action=params.get("action"),
            definition=params.get("definition"),
            inputs=params.get("inputs"),
            run_id=params.get("run_id"),
            template_name=params.get("template_name"),
            script_name=params.get("script_name"),
            script_source=params.get("script_source"),
            script_args=params.get("script_args"),
            script_version=params.get("script_version"),
            script=params.get("script"),
            script_path=params.get("scriptPath"),
            name=params.get("name"),
            args=params.get("args"),
            resume_from_run_id=params.get("resumeFromRunId"),
            include_source=params.get("include_source", False),
            include_versions=params.get("include_versions", False),
            replace=params.get("replace", False),
            dry_run=params.get("dry_run", False),
            registry=store,
            catalog=_plugin_catalog(),
            script_catalog=_plugin_script_catalog(),
            script_store=_plugin_script_run_store(session_id=session_id),
            validate=params.get("validate", True),
            max_parallel=params.get("max_parallel", 8),
            include_steps=params.get("include_steps", True),
            session_id=session_id,
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


def _handle_run(params: dict[str, Any], **kwargs: Any) -> str:
    try:
        session_id = _session_id_from_kwargs(kwargs)
        handle = _workflow_run(
            params["definition"],
            inputs=params.get("inputs"),
            registry=_plugin_store(session_id=session_id),
            validate=params.get("validate", True),
            max_parallel=params.get("max_parallel", 8),
            run_id=params.get("run_id"),
            session_id=session_id,
        )
        return _ok(handle)
    except WorkflowError as exc:
        return _error(exc)
    except Exception as exc:  # pragma: no cover - defensive boundary
        return _error(exc)


def _handle_status(params: dict[str, Any], **kwargs: Any) -> str:
    try:
        session_id = _session_id_from_kwargs(kwargs)
        status = _workflow_status(
            params["run_id"],
            registry=_plugin_store(session_id=session_id),
            include_steps=params.get("include_steps", True),
            session_id=session_id,
        )
        return _ok(status)
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


def register(ctx: Any) -> None:
    """Register dynamic workflow tools with Hermes."""
    ctx.register_tool(
        name="workflow",
        toolset=TOOLSET,
        schema=WORKFLOW_SCHEMA,
        handler=_handle_workflow,
        description="Validate, run, or inspect a dynamic workflow via one model-facing entry point.",
    )
    ctx.register_tool(
        name="workflow_control",
        toolset=TOOLSET,
        schema=WORKFLOW_CONTROL_SCHEMA,
        handler=_handle_control,
        description="Operator controls, status, and blocked-wait inspection for dynamic workflow runs.",
    )
