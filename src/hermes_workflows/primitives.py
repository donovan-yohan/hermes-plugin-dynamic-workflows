"""Public primitives: ``workflow_validate`` / ``workflow_run`` / ``workflow_status``.

These three functions are the Hermes-facing surface of the plugin. They compose
:mod:`schema`, :mod:`sandbox`, :mod:`runtime`, and :mod:`registry` and contain
no execution logic of their own beyond orchestration. Everything is pure-stdlib
and free of network/filesystem effects.
"""

from __future__ import annotations

from typing import Any, Optional

from . import schema as _schema
from . import sandbox as _sandbox
from . import runtime as _runtime
from .agents import AgentRunner, StubAgentRunner
from .errors import WorkflowValidationError
from .models import Diagnostic, RunHandle, RunStatus, ValidationResult, Progress
from .registry import RunStore, get_default_store

__all__ = ["workflow_validate", "workflow_run", "workflow_status"]


def workflow_validate(
    definition: dict[str, Any] | str,
    *,
    source_path: Optional[str] = None,
    strict: bool = True,
) -> ValidationResult:
    """Statically check a workflow definition without executing anything.

    Performs, in order: JSON parse (stdlib ``json``; YAML unsupported), top-level
    + step structural validation (:mod:`schema`), and sandbox-policy lint
    (:mod:`sandbox`). No run is created and no agent is called.

    Args:
        definition: A parsed ``dict`` or a JSON string.
        source_path: Optional originating path, recorded only for diagnostics'
            context by callers (unused internally; accepted per contract).
        strict: When ``True`` (default), policy-lint *warnings* are promoted to
            errors, so a strictly-valid definition has zero warnings.

    Returns:
        A :class:`~hermes_workflows.models.ValidationResult`. ``ok`` is ``True``
        only when there are no error-severity diagnostics.
    """
    parsed, parse_diag = _schema.parse_definition(definition)
    if parsed is None:
        assert parse_diag is not None
        return ValidationResult(ok=False, errors=[parse_diag], warnings=[], normalized=None, def_hash="")

    diags: list[Diagnostic] = list(_schema.validate_structure(parsed))

    # Only run policy lint when the structure is sound enough to interpret.
    if not any(d.severity == "error" for d in diags):
        diags.extend(_sandbox.policy_lint(parsed))

    if strict:
        diags = [
            Diagnostic(severity="error", code=d.code, message=d.message, pointer=d.pointer)
            if d.severity == "warning"
            else d
            for d in diags
        ]

    errors = [d for d in diags if d.severity == "error"]
    warnings = [d for d in diags if d.severity == "warning"]
    h = _schema.def_hash(parsed)

    return ValidationResult(
        ok=not errors,
        errors=errors,
        warnings=warnings,
        normalized=parsed,
        def_hash=h,
    )


def workflow_run(
    definition: dict[str, Any] | str,
    *,
    inputs: Optional[dict[str, Any]] = None,
    registry: Optional[RunStore] = None,
    agent_runner: Optional[AgentRunner] = None,
    validate: bool = True,
    max_parallel: int = 8,
    run_id: Optional[str] = None,
) -> RunHandle:
    """Execute a workflow definition in the deterministic sandboxed runtime.

    Validates (unless ``validate=False``), creates a run record in ``registry``,
    drives the runtime synchronously, and returns a :class:`RunHandle`. On a
    failing :class:`ValidationResult` a :class:`WorkflowValidationError` is
    raised *before* any run record is created.

    Args:
        definition: A parsed ``dict`` or JSON string.
        inputs: Run inputs resolving ``$ref:inputs.<key>`` references.
        registry: Run store; defaults to the process-global in-memory store.
        agent_runner: Injected agent boundary; defaults to a deterministic
            :class:`~hermes_workflows.agents.StubAgentRunner`.
        validate: Validate before running (default ``True``).
        max_parallel: Logical fan-out bound for ``parallel`` steps.
        run_id: Optional caller-supplied id for idempotency/testing.

    Returns:
        A :class:`RunHandle` describing the (already-completed in the skeleton)
        run. Query progress/result later via :func:`workflow_status`.
    """
    store = registry if registry is not None else get_default_store()
    runner = agent_runner if agent_runner is not None else StubAgentRunner()

    # Validate first (non-strict so benign warnings do not block execution).
    if validate:
        result = workflow_validate(definition, strict=False)
        if not result.ok:
            raise WorkflowValidationError(result)
        normalized = result.normalized
        h = result.def_hash
    else:
        normalized, parse_diag = _schema.parse_definition(definition)
        if normalized is None:
            assert parse_diag is not None
            raise WorkflowValidationError(
                ValidationResult(ok=False, errors=[parse_diag], normalized=None, def_hash="")
            )
        h = _schema.def_hash(normalized)

    assert normalized is not None

    rid = run_id if run_id is not None else store.next_run_id(h)
    record = store.create(rid, h)

    store.set_status(rid, "running")
    ctx = _runtime.RunContext(
        run_id=rid,
        store=store,
        agent_runner=runner,
        inputs=dict(inputs or {}),
        max_parallel=max_parallel,
    )

    try:
        final = _runtime.execute(list(normalized.get("steps", [])), ctx)
    except Exception as exc:
        store.set_status(
            rid, "failed", error={"type": type(exc).__name__, "message": str(exc)}
        )
        return RunHandle(run_id=rid, status="failed", created_at=record.created_at, def_hash=h)

    store.set_status(rid, "succeeded", result=final)
    return RunHandle(run_id=rid, status="succeeded", created_at=record.created_at, def_hash=h)


def workflow_status(
    run_id: str,
    *,
    registry: Optional[RunStore] = None,
    include_steps: bool = True,
) -> RunStatus:
    """Query the state/progress of a run by id.

    An unknown ``run_id`` yields ``status="unknown"`` with empty steps and does
    **not** raise. ``include_steps=False`` omits the per-step list for cheap
    polling (progress counters are still computed).

    Args:
        run_id: The run id to look up.
        registry: Run store; defaults to the process-global in-memory store.
        include_steps: Include the per-step status list (default ``True``).

    Returns:
        A :class:`~hermes_workflows.models.RunStatus` snapshot.
    """
    store = registry if registry is not None else get_default_store()
    record = store.get(run_id)
    if record is None:
        return RunStatus(
            run_id=run_id,
            status="unknown",
            created_at="",
            updated_at="",
            progress=Progress(),
            steps=[],
            result=None,
            error=None,
        )
    return record.to_status(include_steps=include_steps)
