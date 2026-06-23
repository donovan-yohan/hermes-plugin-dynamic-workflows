"""Deterministic, network-free workflow executor (SKELETON).

The runtime interprets an already-validated workflow definition. It is a tiny
tree-walking interpreter over workflow step kinds — it never ``eval``s strings,
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

from .agents import AgentRunner, StubAgentRunner, kanban_runner_id
from .errors import SandboxPolicyError
from .models import StepStatus
from .registry import RunStore, utc_now_iso
from .sandbox import parse_ref

__all__ = ["GovernancePolicy", "RunContext", "execute"]


@dataclass(frozen=True)
class GovernancePolicy:
    """Runtime-enforced fanout/backpressure policy for one workflow run."""

    max_agent_calls: Optional[int] = None
    max_kanban_cards: Optional[int] = None
    max_active_awaits: Optional[int] = None
    allowed_profiles: Optional[frozenset[str]] = None

    @classmethod
    def from_definition(cls, definition: dict[str, Any]) -> "GovernancePolicy":
        raw_policy = definition.get("policy")
        policy: dict[str, Any] = raw_policy if isinstance(raw_policy, dict) else {}
        return cls(
            max_agent_calls=_policy_limit(policy, "max_agent_calls"),
            max_kanban_cards=_policy_limit(policy, "max_kanban_cards"),
            max_active_awaits=_policy_limit(policy, "max_active_awaits"),
            allowed_profiles=_policy_profiles(policy),
        )


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
        workflow_id: Optional workflow id surfaced in subagent metadata.
        phase_id: Optional phase id inherited by steps inside a ``phase``.
        phase_title: Optional phase title inherited by steps inside a ``phase``.
    """

    run_id: str
    store: RunStore
    agent_runner: AgentRunner
    inputs: dict[str, Any]
    max_parallel: int = 8
    governance: GovernancePolicy = field(default_factory=GovernancePolicy)
    agent_calls: int = 0
    kanban_cards: int = 0
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    last_output: Optional[dict[str, Any]] = None
    workflow_id: Optional[str] = None
    phase_id: Optional[str] = None
    phase_title: Optional[str] = None


def _optional_nonnegative_int(value: Any) -> Optional[int]:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def _policy_limit(policy: dict[str, Any], key: str) -> Optional[int]:
    if key not in policy:
        return None
    limit = _optional_nonnegative_int(policy.get(key))
    if limit is None:
        raise SandboxPolicyError(f"policy.{key} must be a non-negative integer")
    return limit


def _policy_profiles(policy: dict[str, Any]) -> Optional[frozenset[str]]:
    if "allowed_profiles" not in policy:
        return None
    value = policy.get("allowed_profiles")
    if not isinstance(value, list) or any(not isinstance(item, str) or not _safe_profile(item) for item in value):
        raise SandboxPolicyError("policy.allowed_profiles must be a list of identifier-safe profile strings")
    return frozenset(value)


def _safe_profile(value: str) -> bool:
    return bool(value) and all(c.isalnum() or c in "._-" for c in value)


def _check_effect_budget(ctx: RunContext, *, kind: str, profile: Optional[str] = None) -> None:
    policy = ctx.governance
    if profile is not None and policy.allowed_profiles is not None and profile not in policy.allowed_profiles:
        raise SandboxPolicyError(f"kanban profile {profile!r} is not allowed by workflow policy")
    if policy.max_agent_calls is not None and ctx.agent_calls >= policy.max_agent_calls:
        raise SandboxPolicyError(
            f"agent call budget exceeded: max_agent_calls={policy.max_agent_calls}"
        )
    if kind == "kanban_agent" and policy.max_kanban_cards is not None and ctx.kanban_cards >= policy.max_kanban_cards:
        raise SandboxPolicyError(
            f"kanban card budget exceeded: max_kanban_cards={policy.max_kanban_cards}"
        )
    ctx.agent_calls += 1
    if kind == "kanban_agent":
        ctx.kanban_cards += 1


def _waiting_kanban_count(step: dict[str, Any]) -> int:
    if not isinstance(step, dict):
        return 0
    kind = step.get("kind")
    if kind == "kanban_agent":
        return 1 if step.get("wait", True) is True else 0
    if kind == "parallel":
        branches = step.get("branches")
        if not isinstance(branches, list):
            return 0
        return sum(_waiting_kanban_count(branch) for branch in branches if isinstance(branch, dict))
    if kind in {"pipeline", "phase"}:
        steps = step.get("steps")
        if not isinstance(steps, list):
            return 0
        return max((_waiting_kanban_count(child) for child in steps if isinstance(child, dict)), default=0)
    if kind == "if":
        then_steps = step.get("then")
        else_steps = step.get("else")
        then_count = max(
            (_waiting_kanban_count(child) for child in then_steps if isinstance(child, dict)),
            default=0,
        ) if isinstance(then_steps, list) else 0
        else_count = max(
            (_waiting_kanban_count(child) for child in else_steps if isinstance(child, dict)),
            default=0,
        ) if isinstance(else_steps, list) else 0
        return max(then_count, else_count)
    return 0


def _step_metadata(step: dict[str, Any], ctx: RunContext, *, phase_id: Optional[str] = None, phase_title: Optional[str] = None) -> dict[str, Any]:
    """Build native workflow metadata fields for a step status record."""
    meta: dict[str, Any] = {
        "workflow_id": ctx.workflow_id,
        "workflow_node_id": step.get("id"),
    }
    effective_phase_id = phase_id or step.get("phase_id") or ctx.phase_id
    effective_phase_title = phase_title or step.get("phase_title") or ctx.phase_title
    if effective_phase_id:
        meta["workflow_phase_id"] = effective_phase_id
    if effective_phase_title:
        meta["workflow_phase_title"] = effective_phase_title
    task_title = step.get("title") or step.get("task_title") or step.get("id")
    if task_title:
        meta["workflow_task_title"] = task_title
    return meta


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
    _check_depends_on(step, ctx)
    kind = step.get("kind")
    if kind == "agent":
        return _exec_agent(step, ctx)
    if kind == "kanban_agent":
        return _exec_kanban_agent(step, ctx)
    if kind == "parallel":
        return _exec_parallel(step, ctx)
    if kind == "pipeline":
        return _exec_pipeline(step, ctx)
    if kind == "phase":
        return _exec_phase(step, ctx)
    if kind == "if":
        return _exec_if(step, ctx)
    raise SandboxPolicyError(f"unsupported step kind at runtime: {kind!r}")


def _check_depends_on(step: dict[str, Any], ctx: RunContext) -> None:
    """Fail fast when validate=False skips static dependency-order checks."""
    step_id = step.get("id", "<unknown>")
    for dep in step.get("depends_on", []) or []:
        if isinstance(dep, str) and dep not in ctx.outputs:
            raise SandboxPolicyError(
                f"step {step_id!r} depends on {dep!r}, but that output is not available"
            )


def _exec_agent(step: dict[str, Any], ctx: RunContext) -> dict[str, Any]:
    """Execute an ``agent`` step through the injected runner and record it."""
    agent_id = step["agent"]
    _check_effect_budget(ctx, kind="agent")
    return _exec_effect_step(
        step,
        ctx,
        kind="agent",
        agent_id=agent_id,
        payload=_effect_input(step, ctx),
        error_label=f"agent {agent_id!r}",
    )


def _exec_kanban_agent(step: dict[str, Any], ctx: RunContext) -> dict[str, Any]:
    """Start/await a durable Kanban-backed agent task through the runner boundary.

    The skeleton does not talk to Kanban directly. It normalizes the workflow
    step into a reserved ``kanban.<profile>`` agent call so a real deployment can
    bind that effect boundary to the Kanban backend, while tests remain
    deterministic with :class:`StubAgentRunner`.
    """
    profile = step["profile"]
    _check_effect_budget(ctx, kind="kanban_agent", profile=profile)
    agent_id = kanban_runner_id(profile)
    return _exec_effect_step(
        step,
        ctx,
        kind="kanban_agent",
        agent_id=agent_id,
        payload={
            "profile": profile,
            "task": _resolve(step.get("task", {}), ctx),
            "input": _effect_input(step, ctx),
            "wait": step.get("wait", True),
            "durable": True,
        },
        error_label=f"kanban agent {profile!r}",
    )


def _effect_input(step: dict[str, Any], ctx: RunContext) -> dict[str, Any]:
    """Resolve a leaf-effect ``input`` value into the runner payload shape."""
    resolved = _resolve(step.get("input", {}), ctx)
    return resolved if isinstance(resolved, dict) else {"value": resolved}


def _exec_effect_step(
    step: dict[str, Any],
    ctx: RunContext,
    *,
    kind: str,
    agent_id: str,
    payload: dict[str, Any],
    error_label: str,
) -> dict[str, Any]:
    """Shared lifecycle for leaf steps that cross the AgentRunner boundary."""
    step_id = step["id"]
    status = StepStatus(
        step_id=step_id,
        kind=kind,  # type: ignore[arg-type]
        status="running",
        agent=agent_id,
        started_at=utc_now_iso(),
        **_step_metadata(step, ctx),
    )
    ctx.store.update_step(ctx.run_id, status)

    try:
        output = ctx.agent_runner(agent_id, payload)
        if not isinstance(output, dict):
            raise SandboxPolicyError(
                f"{error_label} returned {type(output).__name__}, expected dict"
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
    active_awaits = sum(_waiting_kanban_count(branch) for branch in branches if isinstance(branch, dict))
    if ctx.governance.max_active_awaits is not None and active_awaits > ctx.governance.max_active_awaits:
        raise SandboxPolicyError(
            f"active await budget exceeded: {active_awaits} waits exceeds max_active_awaits={ctx.governance.max_active_awaits}"
        )
    if len(branches) > ctx.max_parallel:
        raise SandboxPolicyError(
            f"parallel step {step_id!r} fan-out {len(branches)} exceeds max_parallel={ctx.max_parallel}"
        )

    status = StepStatus(
        step_id=step_id,
        kind="parallel",
        status="running",
        started_at=utc_now_iso(),
        workflow_id=ctx.workflow_id,
        workflow_node_id=step_id,
        workflow_phase_id=ctx.phase_id,
        workflow_phase_title=ctx.phase_title,
        workflow_task_title=step_id,
    )
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
    status = StepStatus(
        step_id=step_id,
        kind="pipeline",
        status="running",
        started_at=utc_now_iso(),
        workflow_id=ctx.workflow_id,
        workflow_node_id=step_id,
        workflow_phase_id=ctx.phase_id,
        workflow_phase_title=ctx.phase_title,
        workflow_task_title=step_id,
    )
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
    phase_id = step.get("phase_id") or step_id
    phase_title = step.get("phase_title") or step.get("label")
    status = StepStatus(
        step_id=step_id,
        kind="phase",
        status="running",
        started_at=utc_now_iso(),
        **_step_metadata(step, ctx, phase_id=phase_id, phase_title=phase_title),
    )
    ctx.store.update_step(ctx.run_id, status)

    child_ctx = RunContext(
        run_id=ctx.run_id,
        store=ctx.store,
        agent_runner=ctx.agent_runner,
        inputs=ctx.inputs,
        max_parallel=ctx.max_parallel,
        outputs=ctx.outputs,
        last_output=ctx.last_output,
        workflow_id=ctx.workflow_id,
        phase_id=phase_id,
        phase_title=phase_title,
    )

    try:
        _exec_sequence(step["steps"], child_ctx)
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
    ctx.phase_id = child_ctx.phase_id
    ctx.phase_title = child_ctx.phase_title
    return result
def _exec_if(step: dict[str, Any], ctx: RunContext) -> dict[str, Any]:
    """Execute a deterministic conditional step without leaking branch-local ids."""
    step_id = step["id"]
    status = StepStatus(
        step_id=step_id,
        kind="if",
        status="running",
        started_at=utc_now_iso(),
        workflow_id=ctx.workflow_id,
        workflow_node_id=step_id,
        workflow_phase_id=ctx.phase_id,
        workflow_phase_title=ctx.phase_title,
        workflow_task_title=step_id,
    )
    ctx.store.update_step(ctx.run_id, status)

    try:
        branch_key = "then" if _eval_condition(step["condition"], ctx) else "else"
        branch_steps = step.get(branch_key) or []
        before_keys = set(ctx.outputs)
        before_last = ctx.last_output
        if branch_steps:
            _exec_sequence(branch_steps, ctx)
            branch_output = ctx.last_output or {}
        else:
            ctx.last_output = before_last
            branch_output = {}
        for key in set(ctx.outputs) - before_keys:
            ctx.outputs.pop(key, None)
        result = {"branch": branch_key, "output": branch_output}
    except Exception as exc:
        status.status = "failed"
        status.ended_at = utc_now_iso()
        status.error = {"type": type(exc).__name__, "message": str(exc)}
        ctx.store.update_step(ctx.run_id, status)
        raise

    status.status = "succeeded"
    status.ended_at = utc_now_iso()
    status.output = result
    ctx.store.update_step(ctx.run_id, status)

    ctx.outputs[step_id] = result
    ctx.last_output = result
    return result
def _eval_condition(condition: dict[str, Any], ctx: RunContext) -> bool:
    """Evaluate the tiny declarative condition grammar."""
    op = condition.get("op")
    try:
        value = _resolve(condition.get("ref"), ctx)
        exists = value is not None
    except SandboxPolicyError:
        if op == "exists":
            return False
        raise
    if op == "truthy":
        return bool(value)
    if op == "exists":
        return exists
    if op == "eq":
        return value == condition.get("value")
    if op == "ne":
        return value != condition.get("value")
    raise SandboxPolicyError(f"unsupported if condition op {op!r}")


# ---------------------------------------------------------------------------
# Reference resolution and output validation.
# ---------------------------------------------------------------------------

def _resolve(value: Any, ctx: RunContext) -> Any:
    """Recursively resolve ``$ref:`` references inside an input value.

    ``$ref:inputs.<key>`` resolves against ``ctx.inputs``;
    ``$ref:<step_id>.output[.<field>]`` resolves against recorded step outputs.
    Non-ref values pass through unchanged. Malformed or unavailable step-output
    refs raise :class:`SandboxPolicyError` so ``validate=False`` runs cannot
    silently consume impossible dependency edges as ``None``.
    """
    if isinstance(value, str) and value.startswith("$ref:"):
        ref = parse_ref(value)
        if not ref or ref.get("kind") == "invalid":
            raise SandboxPolicyError(f"malformed reference {value!r}")
        if ref["kind"] == "input":
            return ctx.inputs.get(ref["key"])
        # step output reference.
        out = ctx.outputs.get(ref["step_id"])
        if out is None:
            raise SandboxPolicyError(f"step output reference {value!r} is not available")
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
