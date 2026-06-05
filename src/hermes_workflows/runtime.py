"""Deterministic, network-free workflow executor (SKELETON).

The runtime interprets an already-validated workflow definition. It is a tiny
tree-walking interpreter over the four step kinds — it never ``eval``s strings,
never imports user-named modules, and routes every external effect through the
injected :class:`~hermes_workflows.agents.AgentRunner`. See
:mod:`hermes_workflows.sandbox` for the security model this honours.

Determinism
-----------
* ``parallel`` branches are executed sequentially in declaration order (the
  ``max_parallel`` width bounds *logical* fan-out, not OS threads), so output
  ordering and recorded timestamps are reproducible.
* The default :class:`~hermes_workflows.agents.StubAgentRunner` derives output
  purely from inputs, so a given definition + inputs always yields the same run.

Each executed step is recorded into the run store via ``update_step`` as it
starts and finishes, so :func:`hermes_workflows.primitives.workflow_status` can
observe progress.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from .agents import AgentRunner, StubAgentRunner
from .errors import SandboxPolicyError
from .models import StepStatus
from .registry import RunStore, utc_now_iso
from .sandbox import parse_ref

__all__ = ["RunContext", "execute"]


@dataclass
class RunContext:
    """Mutable execution context threaded through a single run.

    Attributes:
        run_id: Id under which steps are recorded.
        store: Backend that step status updates are written to.
        agent_runner: Effect boundary used for ``agent`` steps.
        inputs: Declared run inputs (resolves ``$ref:inputs.<key>``).
        max_parallel: Logical fan-out bound for ``parallel`` steps.
        outputs: Map of ``step_id -> output dict`` for ``$ref:<id>.output``.
        last_output: Output of the most recently completed step (pipeline feed).
    """

    run_id: str
    store: RunStore
    agent_runner: AgentRunner
    inputs: dict[str, Any]
    max_parallel: int = 8
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    last_output: Optional[dict[str, Any]] = None


def execute(steps: list[dict[str, Any]], ctx: RunContext) -> dict[str, Any]:
    """Execute a list of top-level steps with pipeline-by-default semantics.

    Returns the run's final structured result: ``{"outputs": {step_id: out}}``
    augmented with ``"last": <last_output>``. Raises on the first failing step
    so the caller can mark the run ``failed``; the offending step's status is
    recorded as ``failed`` before the exception propagates.
    """
    _exec_sequence(steps, ctx)
    return {"outputs": dict(ctx.outputs), "last": ctx.last_output}


def _exec_sequence(steps: list[dict[str, Any]], ctx: RunContext) -> None:
    """Run ``steps`` in order, streaming each output to the next (no barrier)."""
    for step in steps:
        _exec_step(step, ctx)


def _exec_step(step: dict[str, Any], ctx: RunContext) -> dict[str, Any]:
    """Dispatch a single step by kind; return its output dict."""
    kind = step.get("kind")
    if kind == "agent":
        return _exec_agent(step, ctx)
    if kind == "parallel":
        return _exec_parallel(step, ctx)
    if kind == "pipeline":
        return _exec_pipeline(step, ctx)
    if kind == "phase":
        return _exec_phase(step, ctx)
    raise SandboxPolicyError(f"unsupported step kind at runtime: {kind!r}")


def _exec_agent(step: dict[str, Any], ctx: RunContext) -> dict[str, Any]:
    """Execute an ``agent`` step through the injected runner and record it."""
    step_id = step["id"]
    agent_id = step["agent"]
    status = StepStatus(
        step_id=step_id, kind="agent", status="running", agent=agent_id, started_at=utc_now_iso()
    )
    ctx.store.update_step(ctx.run_id, status)

    try:
        resolved_input = _resolve(step.get("input", {}), ctx)
        if not isinstance(resolved_input, dict):
            resolved_input = {"value": resolved_input}
        output = ctx.agent_runner(agent_id, resolved_input)
        if not isinstance(output, dict):
            raise SandboxPolicyError(
                f"agent {agent_id!r} returned {type(output).__name__}, expected dict"
            )
        _validate_output(output, step.get("output_schema"))
    except Exception as exc:  # record failure, then re-raise for run-level handling.
        status.status = "failed"
        status.ended_at = utc_now_iso()
        status.error = {"type": type(exc).__name__, "message": str(exc)}
        ctx.store.update_step(ctx.run_id, status)
        raise

    status.status = "succeeded"
    status.ended_at = utc_now_iso()
    status.output = output
    ctx.store.update_step(ctx.run_id, status)

    ctx.outputs[step_id] = output
    ctx.last_output = output
    return output


def _exec_parallel(step: dict[str, Any], ctx: RunContext) -> dict[str, Any]:
    """Execute a ``parallel`` step: fan out branches, join all outputs.

    Branches are run sequentially in declaration order for determinism; the
    logical fan-out width is capped at ``ctx.max_parallel`` (a guard, not a
    threading control in the skeleton).
    """
    step_id = step["id"]
    branches = step["branches"]
    if len(branches) > ctx.max_parallel:
        raise SandboxPolicyError(
            f"parallel step {step_id!r} fan-out {len(branches)} exceeds max_parallel={ctx.max_parallel}"
        )

    status = StepStatus(step_id=step_id, kind="parallel", status="running", started_at=utc_now_iso())
    ctx.store.update_step(ctx.run_id, status)

    joined: dict[str, Any] = {}
    try:
        for branch in branches:
            out = _exec_step(branch, ctx)
            bid = branch.get("id", "")
            joined[bid] = out
    except Exception as exc:
        status.status = "failed"
        status.ended_at = utc_now_iso()
        status.error = {"type": type(exc).__name__, "message": str(exc)}
        ctx.store.update_step(ctx.run_id, status)
        raise

    status.status = "succeeded"
    status.ended_at = utc_now_iso()
    status.output = {"branches": joined}
    ctx.store.update_step(ctx.run_id, status)

    result = {"branches": joined}
    ctx.outputs[step_id] = result
    ctx.last_output = result
    return result


def _exec_pipeline(step: dict[str, Any], ctx: RunContext) -> dict[str, Any]:
    """Execute a ``pipeline`` step: chain inner steps, output feeds next."""
    step_id = step["id"]
    status = StepStatus(step_id=step_id, kind="pipeline", status="running", started_at=utc_now_iso())
    ctx.store.update_step(ctx.run_id, status)

    try:
        _exec_sequence(step["steps"], ctx)
    except Exception as exc:
        status.status = "failed"
        status.ended_at = utc_now_iso()
        status.error = {"type": type(exc).__name__, "message": str(exc)}
        ctx.store.update_step(ctx.run_id, status)
        raise

    result = ctx.last_output or {}
    status.status = "succeeded"
    status.ended_at = utc_now_iso()
    status.output = result
    ctx.store.update_step(ctx.run_id, status)

    ctx.outputs[step_id] = result
    return result


def _exec_phase(step: dict[str, Any], ctx: RunContext) -> dict[str, Any]:
    """Execute a ``phase`` step: a barrier; all inner steps complete first.

    In the deterministic sequential skeleton the barrier is implicit (inner
    steps already complete before control returns), but the phase is recorded
    distinctly so callers can see the barrier in the step list.
    """
    step_id = step["id"]
    status = StepStatus(step_id=step_id, kind="phase", status="running", started_at=utc_now_iso())
    ctx.store.update_step(ctx.run_id, status)

    try:
        _exec_sequence(step["steps"], ctx)
    except Exception as exc:
        status.status = "failed"
        status.ended_at = utc_now_iso()
        status.error = {"type": type(exc).__name__, "message": str(exc)}
        ctx.store.update_step(ctx.run_id, status)
        raise

    result = {"completed": [s.get("id") for s in step["steps"] if isinstance(s, dict)]}
    status.status = "succeeded"
    status.ended_at = utc_now_iso()
    status.output = result
    ctx.store.update_step(ctx.run_id, status)

    ctx.outputs[step_id] = result
    ctx.last_output = result
    return result


# ---------------------------------------------------------------------------
# Reference resolution and output validation.
# ---------------------------------------------------------------------------

def _resolve(value: Any, ctx: RunContext) -> Any:
    """Recursively resolve ``$ref:`` references inside an input value.

    ``$ref:inputs.<key>`` resolves against ``ctx.inputs``;
    ``$ref:<step_id>.output[.<field>]`` resolves against recorded step outputs.
    Non-ref values pass through unchanged. Unresolvable refs (which validation
    should have caught) yield ``None`` rather than raising, keeping the
    deterministic executor total.
    """
    if isinstance(value, str) and value.startswith("$ref:"):
        ref = parse_ref(value)
        if not ref or ref.get("kind") == "invalid":
            return None
        if ref["kind"] == "input":
            return ctx.inputs.get(ref["key"])
        # step output reference.
        out = ctx.outputs.get(ref["step_id"])
        if out is None:
            return None
        field_path = ref.get("field")
        if not field_path:
            return out
        cur: Any = out
        for part in field_path.split("."):
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                return None
        return cur
    if isinstance(value, dict):
        return {k: _resolve(v, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve(v, ctx) for v in value]
    return value


# Minimal type-hint-string -> python type table for output_schema checks.
_TYPE_MAP: dict[str, tuple[type, ...]] = {
    "string": (str,),
    "str": (str,),
    "number": (int, float),
    "int": (int,),
    "integer": (int,),
    "float": (float,),
    "bool": (bool,),
    "boolean": (bool,),
    "object": (dict,),
    "dict": (dict,),
    "list": (list,),
    "array": (list,),
    "any": (object,),
}


def _validate_output(output: dict[str, Any], output_schema: Optional[dict[str, Any]]) -> None:
    """Validate ``output`` against a declared ``output_schema`` (best-effort).

    ``output_schema`` maps field -> type-hint string. Missing fields or type
    mismatches raise :class:`SandboxPolicyError` so the step is recorded failed.
    A ``None`` schema (no declaration) accepts any dict output.
    """
    if not output_schema:
        return
    for field_name, hint in output_schema.items():
        if field_name not in output:
            raise SandboxPolicyError(f"output missing declared field {field_name!r}")
        expected = _TYPE_MAP.get(str(hint).lower())
        if expected is None:
            continue  # unknown hint: skip rather than reject.
        value = output[field_name]
        # bool is a subclass of int; guard so "number"/"int" don't accept bools.
        if expected != (bool,) and isinstance(value, bool):
            raise SandboxPolicyError(
                f"output field {field_name!r} expected {hint}, got bool"
            )
        if not isinstance(value, expected):
            raise SandboxPolicyError(
                f"output field {field_name!r} expected {hint}, got {type(value).__name__}"
            )
