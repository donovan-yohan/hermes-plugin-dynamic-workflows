"""Public primitives: ``workflow_validate`` / ``workflow_run`` / ``workflow_status``.

These three functions are the Hermes-facing surface of the plugin. They compose
:mod:`schema`, :mod:`sandbox`, :mod:`runtime`, and :mod:`registry` and contain
no execution logic of their own beyond orchestration. Everything is pure-stdlib
and free of network/filesystem effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from . import schema as _schema
from . import sandbox as _sandbox
from . import runtime as _runtime
from .agents import AgentRunner, ChildAgentRunner, StubAgentRunner
from .capabilities import CapabilityPolicy, CapabilityRegistry
from .catalog import FileWorkflowCatalog
from .controls import ControlStore
from .errors import ControlDispatchDenied, WorkflowValidationError
from .models import Diagnostic, RunHandle, RunStatus, ValidationResult, Progress
from .registry import RunStore, get_default_store
from .script_catalog import FileWorkflowScriptCatalog
from .script_store import ScriptRunStore
from .script_validator import ScriptValidation, validate_script
from .vm import JournalSink, ScriptRunResult, VMLimits, run_script

if TYPE_CHECKING:  # avoid importing the backend at runtime; annotation-only.
    from .kanban import KanbanBackend

__all__ = [
    "workflow",
    "workflow_validate",
    "workflow_run",
    "workflow_status",
    "workflow_validate_script",
    "run_workflow_script",
    "workflow_script_catalog",
    "workflow_save_script",
    "workflow_inspect_script",
    "workflow_run_script",
]


def workflow(
    *,
    definition: Optional[dict[str, Any] | str] = None,
    inputs: Optional[dict[str, Any]] = None,
    run_id: Optional[str] = None,
    template_name: Optional[str] = None,
    action: Optional[str] = None,
    dry_run: bool = False,
    registry: Optional[RunStore] = None,
    catalog: Optional[FileWorkflowCatalog] = None,
    script_catalog: Optional[FileWorkflowScriptCatalog] = None,
    script_store: Optional[ScriptRunStore] = None,
    agent_runner: Optional[AgentRunner] = None,
    child_agent_runner: Optional[ChildAgentRunner] = None,
    validate: bool = True,
    max_parallel: int = 8,
    include_steps: bool = True,
    session_id: Optional[str] = None,
    script_name: Optional[str] = None,
    script_source: Optional[str] = None,
    script_args: Any = None,
    script_version: Optional[int] = None,
    include_source: bool = False,
    include_versions: bool = False,
    replace: bool = False,
    script: Optional[str] = None,
    script_path: Optional[str] = None,
    name: Optional[str] = None,
    args: Any = None,
    resume_from_run_id: Optional[str] = None,
    capability_registry: Optional[CapabilityRegistry] = None,
    capability_policy: Optional[CapabilityPolicy] = None,
    control_store: Optional[ControlStore] = None,
) -> dict[str, Any]:
    """Model-facing workflow tool facade.

    ``workflow`` is the single product-shaped entry point: validate when asked
    for a dry run, run when supplied a definition, and query status when supplied
    only a ``run_id``. The narrower primitives remain available for tests and
    operator/debug usage.
    """
    store = registry if registry is not None else get_default_store(session_id=session_id)
    facade_script_args = args if args is not None else script_args
    facade_name = name if name is not None else script_name
    has_facade_script = script is not None or script_path is not None or name is not None
    op = action or (
        "validate" if dry_run else
        "run_facade_script" if has_facade_script else
        "run_template" if template_name else
        "status" if definition is None and run_id else
        "run"
    )
    if op == "validate":
        if definition is None:
            raise ValueError("workflow validate requires 'definition'")
        validation = workflow_validate(definition, strict=validate)
        return {"operation": "validate", "validation": validation.as_dict()}
    if op == "status":
        if not run_id:
            raise ValueError("workflow status requires 'run_id'")
        status = workflow_status(run_id, registry=store, include_steps=include_steps)
        return {"operation": "status", "status": status.as_dict()}
    if op == "run":
        if definition is None:
            raise ValueError("workflow run requires 'definition'")
        handle, status = _run_and_status(
            definition,
            inputs=inputs,
            registry=store,
            agent_runner=agent_runner,
            validate=validate,
            max_parallel=max_parallel,
            run_id=run_id,
            include_steps=include_steps,
            control_store=control_store,
        )
        return {"operation": "run", "handle": handle.as_dict(), "status": status.as_dict()}
    if op == "catalog":
        active_catalog = catalog if catalog is not None else FileWorkflowCatalog()
        return {"operation": "catalog", "templates": active_catalog.list_templates()}
    if op == "run_template":
        if not template_name:
            raise ValueError("workflow run_template requires 'template_name'")
        active_catalog = catalog if catalog is not None else FileWorkflowCatalog()
        loaded = active_catalog.load_template(template_name)
        handle, status = _run_and_status(
            loaded,
            inputs=inputs,
            registry=store,
            agent_runner=agent_runner,
            validate=validate,
            max_parallel=max_parallel,
            run_id=run_id,
            include_steps=include_steps,
            control_store=control_store,
        )
        return {
            "operation": "run_template",
            "template_name": template_name,
            "template_hash": handle.def_hash,
            "handle": handle.as_dict(),
            "status": status.as_dict(),
        }
    if op == "script_catalog":
        return workflow_script_catalog(catalog=script_catalog, include_versions=include_versions)
    if op == "run_facade_script":
        source_count = sum(value is not None for value in (script, script_path, name))
        if source_count != 1:
            raise ValueError("workflow script facade requires exactly one of 'script', 'script_path', or 'name'")
        if script is not None:
            result = run_workflow_script(
                script,
                args=facade_script_args,
                store=script_store,
                agent_runner=agent_runner,
                validate=validate,
                run_id=run_id,
                replay_from=resume_from_run_id,
                capability_registry=capability_registry,
                capability_policy=capability_policy,
            )
            return _script_run_payload("inline_script", result)
        if script_path is not None:
            active_catalog = script_catalog if script_catalog is not None else FileWorkflowScriptCatalog()
            source = active_catalog.load_script_path(script_path)
            result = run_workflow_script(
                source,
                args=facade_script_args,
                store=script_store,
                agent_runner=agent_runner,
                validate=validate,
                run_id=run_id,
                replay_from=resume_from_run_id,
                capability_registry=capability_registry,
                capability_policy=capability_policy,
            )
            return _script_run_payload("script_path", result, script_path=script_path)
        if facade_name is None:
            raise ValueError("workflow script facade requires 'name'")
        result = workflow_run_script(
            facade_name,
            args=facade_script_args,
            catalog=script_catalog,
            store=script_store,
            agent_runner=agent_runner,
            version=script_version,
            validate=validate,
            run_id=run_id,
            replay_from=resume_from_run_id,
            capability_registry=capability_registry,
            capability_policy=capability_policy,
        )
        return _script_run_payload("saved_script", result, name=facade_name)
    if op == "script_save":
        if not script_name or script_source is None:
            raise ValueError("workflow script_save requires 'script_name' and 'script_source'")
        return workflow_save_script(
            script_name,
            script_source,
            catalog=script_catalog,
            version=script_version,
            replace=replace,
        )
    if op == "script_inspect":
        if not script_name:
            raise ValueError("workflow script_inspect requires 'script_name'")
        return workflow_inspect_script(
            script_name,
            catalog=script_catalog,
            version=script_version,
            include_source=include_source,
        )
    if op == "run_script":
        selected_name = facade_name
        if not selected_name:
            raise ValueError("workflow run_script requires 'script_name' or 'name'")
        result = workflow_run_script(
            selected_name,
            args=facade_script_args,
            catalog=script_catalog,
            store=script_store,
            agent_runner=agent_runner,
            child_agent_runner=child_agent_runner,
            version=script_version,
            validate=validate,
            run_id=run_id,
            replay_from=resume_from_run_id,
            capability_registry=capability_registry,
            capability_policy=capability_policy,
            control_store=control_store,
        )
        return {"operation": "run_script", "script_name": selected_name, "result": result.as_dict()}
    raise ValueError("workflow action must be one of: validate, run, status, catalog, run_template, script_catalog, script_save, script_inspect, run_script")


def _script_run_payload(source: str, result: ScriptRunResult, **extra: Any) -> dict[str, Any]:
    status = "suspended" if result.suspended else "succeeded" if result.ok else "failed"
    payload: dict[str, Any] = {
        "operation": "run_script",
        "source": source,
        "run_id": result.run_id,
        "status": status,
        "result": result.as_dict(),
    }
    if result.journal_path:
        payload["journal_path"] = result.journal_path
    if result.replayed_calls:
        payload["replayed_calls"] = result.replayed_calls
    payload.update(extra)
    return payload


def _run_and_status(
    definition: dict[str, Any] | str,
    *,
    inputs: Optional[dict[str, Any]],
    registry: Optional[RunStore],
    agent_runner: Optional[AgentRunner],
    validate: bool,
    max_parallel: int,
    run_id: Optional[str],
    include_steps: bool,
    control_store: Optional[ControlStore],
) -> tuple[RunHandle, RunStatus]:
    handle = workflow_run(
        definition,
        inputs=inputs,
        registry=registry,
        agent_runner=agent_runner,
        validate=validate,
        max_parallel=max_parallel,
        run_id=run_id,
        control_store=control_store,
    )
    status = workflow_status(handle.run_id, registry=registry, include_steps=include_steps)
    return handle, status


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


def _effective_max_parallel(definition: dict[str, Any], override: int) -> int:
    """Return the runtime fan-out bound from explicit arg capped by policy."""
    policy = definition.get("policy") if isinstance(definition.get("policy"), dict) else {}
    policy_value = policy.get("max_parallel") if isinstance(policy, dict) else None
    candidates = [override]
    if isinstance(policy_value, int) and not isinstance(policy_value, bool) and policy_value > 0:
        candidates.append(policy_value)
    return max(1, min(candidates))


def _run_lifecycle_for_control_code(code: str) -> str:
    if code == "run_stopped":
        return "stopped"
    if code == "run_paused":
        return "paused"
    return "failed"


def workflow_run(
    definition: dict[str, Any] | str,
    *,
    inputs: Optional[dict[str, Any]] = None,
    registry: Optional[RunStore] = None,
    agent_runner: Optional[AgentRunner] = None,
    validate: bool = True,
    max_parallel: int = 8,
    run_id: Optional[str] = None,
    session_id: Optional[str] = None,
    control_store: Optional[ControlStore] = None,
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
        session_id: Optional Hermes session id for store scoping.

    Returns:
        A :class:`RunHandle` describing the (already-completed in the skeleton)
        run. Query progress/result later via :func:`workflow_status`.
    """
    store = registry if registry is not None else get_default_store(session_id=session_id)
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

    try:
        ctx = _runtime.RunContext(
            run_id=rid,
            store=store,
            agent_runner=runner,
            inputs=dict(inputs or {}),
            max_parallel=_effective_max_parallel(normalized, max_parallel),
            governance=_runtime.GovernancePolicy.from_definition(normalized),
            workflow_id=rid,
            control_store=control_store,
        )
        final = _runtime.execute(list(normalized.get("steps", [])), ctx)
    except ControlDispatchDenied as exc:
        decision = exc.decision.to_dict()
        status = _run_lifecycle_for_control_code(exc.code)
        store.set_status(
            rid,
            status,  # type: ignore[arg-type]
            error={"type": "ControlDispatchDenied", "code": exc.code, "message": str(exc), "decision": decision},
        )
        return RunHandle(run_id=rid, status=status, created_at=record.created_at, def_hash=h)  # type: ignore[arg-type]
    except Exception as exc:
        store.set_status(
            rid, "failed", error={"type": type(exc).__name__, "message": str(exc)}
        )
        return RunHandle(run_id=rid, status="failed", created_at=record.created_at, def_hash=h)

    store.set_status(rid, "succeeded", result=final)
    return RunHandle(run_id=rid, status="succeeded", created_at=record.created_at, def_hash=h)


def workflow_script_catalog(
    *,
    catalog: Optional[FileWorkflowScriptCatalog] = None,
    include_versions: bool = False,
) -> dict[str, Any]:
    """List saved Python workflow-script harnesses from the script catalog."""
    active_catalog = catalog if catalog is not None else FileWorkflowScriptCatalog()
    return {
        "operation": "script_catalog",
        "scripts": active_catalog.list_scripts(include_versions=include_versions),
    }


def workflow_save_script(
    script_name: str,
    source: str,
    *,
    catalog: Optional[FileWorkflowScriptCatalog] = None,
    version: Optional[int] = None,
    replace: bool = False,
) -> dict[str, Any]:
    """Validate and save a versioned Python workflow-script harness."""
    active_catalog = catalog if catalog is not None else FileWorkflowScriptCatalog()
    entry = active_catalog.save_script(script_name, source, version=version, replace=replace)
    return {"operation": "script_save", "script": entry}


def workflow_inspect_script(
    script_name: str,
    *,
    catalog: Optional[FileWorkflowScriptCatalog] = None,
    version: Optional[int] = None,
    include_source: bool = False,
) -> dict[str, Any]:
    """Inspect one saved Python workflow-script harness version."""
    active_catalog = catalog if catalog is not None else FileWorkflowScriptCatalog()
    return {
        "operation": "script_inspect",
        "script": active_catalog.inspect_script(script_name, version=version, include_source=include_source),
    }


def workflow_run_script(
    script_name: str,
    *,
    args: Any = None,
    catalog: Optional[FileWorkflowScriptCatalog] = None,
    store: Optional[ScriptRunStore] = None,
    agent_runner: Optional[AgentRunner] = None,
    child_agent_runner: Optional[ChildAgentRunner] = None,
    limits: Optional[VMLimits] = None,
    journal: Optional[JournalSink] = None,
    validate: bool = True,
    run_id: Optional[str] = None,
    replay_from: Optional[str] = None,
    deterministic_runner: Optional[bool] = None,
    version: Optional[int] = None,
    kanban_backend: Optional["KanbanBackend"] = None,
    capability_registry: Optional[CapabilityRegistry] = None,
    capability_policy: Optional[CapabilityPolicy] = None,
    control_store: Optional[ControlStore] = None,
) -> ScriptRunResult:
    """Load and run a saved Python workflow-script harness by catalog name."""
    active_catalog = catalog if catalog is not None else FileWorkflowScriptCatalog()
    source = active_catalog.load_script(script_name, version=version)
    return run_workflow_script(
        source,
        args=args,
        agent_runner=agent_runner,
        child_agent_runner=child_agent_runner,
        limits=limits,
        journal=journal,
        validate=validate,
        store=store,
        run_id=run_id,
        replay_from=replay_from,
        deterministic_runner=deterministic_runner,
        kanban_backend=kanban_backend,
        capability_registry=capability_registry,
        capability_policy=capability_policy,
        control_store=control_store,
    )


def workflow_validate_script(source: str) -> ScriptValidation:
    """Statically validate a *Python workflow script* against the launch gate.

    This is the side-effect-free, library/operator primitive behind the
    subprocess VM (:mod:`hermes_workflows.vm`). It never executes the script; it
    only reports whether the script is safe to launch and, if so, its parsed
    ``meta``. See :mod:`hermes_workflows.script_validator` for the contract.
    """
    return validate_script(source)


def run_workflow_script(
    source: str,
    *,
    args: Any = None,
    agent_runner: Optional[AgentRunner] = None,
    child_agent_runner: Optional[ChildAgentRunner] = None,
    limits: Optional[VMLimits] = None,
    journal: Optional[JournalSink] = None,
    validate: bool = True,
    store: Optional[ScriptRunStore] = None,
    run_id: Optional[str] = None,
    replay_from: Optional[str] = None,
    deterministic_runner: Optional[bool] = None,
    kanban_backend: Optional["KanbanBackend"] = None,
    capability_registry: Optional[CapabilityRegistry] = None,
    capability_policy: Optional[CapabilityPolicy] = None,
    control_store: Optional[ControlStore] = None,
) -> ScriptRunResult:
    """Run a Python workflow script in the parent-owned subprocess VM.

    The script is statically gated (unless ``validate=False``), then executed in
    a sandboxed subprocess with a scrubbed environment and a narrow RPC surface;
    every capability call is brokered and journaled by the parent. Returns a
    :class:`~hermes_workflows.vm.ScriptRunResult`. This is a library/operator
    primitive: it is intentionally **not** registered as a model-facing tool, so
    the declarative ``workflow`` facade and JSON runtime are unchanged.

    When a :class:`~hermes_workflows.script_store.ScriptRunStore` is supplied the
    run is persisted durably (metadata snapshot + metadata-only journal +
    deterministic replay cache) under a stable ``run_id``. ``replay_from`` names a
    prior run whose deterministic calls are served from the cache instead of
    being re-dispatched. See :func:`hermes_workflows.vm.run_script` for the full
    durable/replay contract.

    ``kanban_backend`` (issue #5) makes ``kanban_agent`` a durable awaitable:
    each call becomes a parent-owned, idempotent Kanban card that the broker
    blocks on until it resolves (completed/blocked/failed), with ``on_block``
    selecting pause/raise/return. The idempotency key is derived from the logical
    run id and the stable call id, so a replay reattaches the same card instead of
    creating a duplicate. Without a backend, ``kanban_agent`` keeps its prior
    synchronous AgentRunner behaviour.
    """
    return run_script(
        source,
        args=args,
        agent_runner=agent_runner,
        child_agent_runner=child_agent_runner,
        limits=limits,
        journal=journal,
        validate=validate,
        store=store,
        run_id=run_id,
        replay_from=replay_from,
        deterministic_runner=deterministic_runner,
        kanban_backend=kanban_backend,
        capability_registry=capability_registry,
        capability_policy=capability_policy,
        control_store=control_store,
    )


def workflow_status(
    run_id: str,
    *,
    registry: Optional[RunStore] = None,
    include_steps: bool = True,
    session_id: Optional[str] = None,
) -> RunStatus:
    """Query the state/progress of a run by id.

    An unknown ``run_id`` yields ``status="unknown"`` with empty steps and does
    **not** raise. ``include_steps=False`` omits the per-step list for cheap
    polling (progress counters are still computed).

    Args:
        run_id: The run id to look up.
        registry: Run store; defaults to the process-global in-memory store.
        include_steps: Include the per-step status list (default ``True``).
        session_id: Optional Hermes session id for store scoping.

    Returns:
        A :class:`~hermes_workflows.models.RunStatus` snapshot.
    """
    store = registry if registry is not None else get_default_store(session_id=session_id)
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
