"""Sandbox security model and static policy lint (SKELETON).

Security model (read this before extending the runtime)
-------------------------------------------------------
The ``hermes_workflows`` runtime is **not** a JavaScript engine and never will
be in this package. Workflow definitions are declarative JSON, *interpreted* by
:mod:`hermes_workflows.runtime` — never ``eval``'d, never compiled, never used
to import user-named modules. This module enforces a single, explicit boundary:

* **Default-deny capabilities.** A definition's ``policy`` block may *request*
  capabilities (``network``, ``filesystem``). In the skeleton every external
  capability must stay ``false``; requesting ``true`` is a lint error
  (:data:`~hermes_workflows.errors.E_POLICY_NETWORK` /
  :data:`~hermes_workflows.errors.E_POLICY_FILESYSTEM`). The runtime performs no
  network or filesystem I/O whatsoever.
* **Single effect boundary.** The only way a workflow reaches the outside world
  is through the injected :class:`~hermes_workflows.agents.AgentRunner`. There
  is no other escape hatch — no ``import``, no shelling out, no ``open()``.
* **Reference safety.** Data wiring is restricted to the ``$ref:`` mini-grammar
  (``$ref:inputs.<key>`` / ``$ref:<step_id>.output[.<field>]``). Anything else
  is rejected before a run is ever created.
* **Acyclicity.** ``depends_on`` and pipeline edges must form a DAG; cycles are
  rejected statically so the deterministic executor always terminates.

A *real* JS execution sandbox (isolates, resource limits, capability tokens) is
explicitly out of scope here and is called out as future work in ``DESIGN.md``.

This module is pure analysis: it has no side effects and never runs anything.
"""

from __future__ import annotations

import re
from typing import Any

from .models import Diagnostic
from . import errors as err
from . import schema as _schema
from .agents import is_kanban_runner_id, is_known_agent

__all__ = [
    "KNOWN_CAPABILITIES",
    "REF_PATTERN",
    "policy_lint",
    "parse_ref",
]

# Capability keys the policy block is allowed to mention. Anything else trips
# E_DISALLOWED_CAPABILITY. ``max_parallel`` is a tuning knob, not a capability.
KNOWN_CAPABILITIES: frozenset[str] = frozenset({"network", "filesystem", "max_parallel"})

# $ref:inputs.<key>  |  $ref:<step_id>.output[.<field>...]
REF_PATTERN = re.compile(
    r"^\$ref:(?:"
    r"inputs\.(?P<input_key>[A-Za-z_][\w-]*)"
    r"|(?P<step_id>[A-Za-z_][\w.-]*?)\.output(?:\.(?P<field>[\w.-]+))?"
    r")$"
)


def parse_ref(value: str) -> dict[str, Any] | None:
    """Parse a ``$ref:`` string into its components.

    Returns ``{"kind": "input", "key": ...}`` or
    ``{"kind": "step", "step_id": ..., "field": ... | None}`` for a well-formed
    reference, or ``None`` if ``value`` is not a ``$ref:`` string at all.

    A malformed ``$ref:`` (right prefix, wrong grammar) returns
    ``{"kind": "invalid"}`` so callers can distinguish "not a ref" from
    "bad ref".
    """
    if not isinstance(value, str) or not value.startswith("$ref:"):
        return None
    m = REF_PATTERN.match(value)
    if not m:
        return {"kind": "invalid"}
    if m.group("input_key") is not None:
        return {"kind": "input", "key": m.group("input_key")}
    return {"kind": "step", "step_id": m.group("step_id"), "field": m.group("field")}


def policy_lint(definition: dict[str, Any]) -> list[Diagnostic]:
    """Run sandbox-policy lint over a structurally-valid definition.

    Emits diagnostics for: requested network/filesystem capabilities, unknown
    capability keys, a missing policy block (warning, default-deny assumed),
    unknown agent ids, malformed/unresolved ``$ref:`` references, missing
    ``output_schema`` on agent steps (warning), undeclared input refs
    (warning), and cyclic ``depends_on`` graphs.

    Severity follows the contract; callers apply ``strict`` promotion. This
    function never executes the workflow.
    """
    diags: list[Diagnostic] = []
    diags.extend(_lint_policy_block(definition))

    steps = definition.get("steps")
    if not isinstance(steps, list):
        return diags  # structure validation already reported this.

    declared_inputs = set(definition.get("inputs", {}) or {})
    known_step_ids = _collect_all_step_ids(steps)

    diags.extend(_lint_agent_steps(steps, declared_inputs, known_step_ids))
    diags.extend(_lint_if_steps(steps, declared_inputs, known_step_ids))
    diags.extend(_lint_execution_order(steps))
    diags.extend(_lint_cycles(steps))
    return diags


# ---------------------------------------------------------------------------
# Policy block.
# ---------------------------------------------------------------------------

def _lint_policy_block(definition: dict[str, Any]) -> list[Diagnostic]:
    diags: list[Diagnostic] = []
    policy = definition.get("policy")
    if policy is None:
        diags.append(
            Diagnostic(
                severity="warning",
                code=err.W_POLICY_DEFAULT,
                message="no 'policy' block; assuming default-deny (network=false, filesystem=false)",
                pointer="/policy",
            )
        )
        return diags
    if not isinstance(policy, dict):
        diags.append(
            Diagnostic(severity="error", code=err.E_SCHEMA_TOPLEVEL, message="'policy' must be an object", pointer="/policy")
        )
        return diags

    if policy.get("network") is True:
        diags.append(
            Diagnostic(
                severity="error",
                code=err.E_POLICY_NETWORK,
                message="network capability is not permitted in the skeleton (policy.network must be false)",
                pointer="/policy/network",
            )
        )
    if policy.get("filesystem") is True:
        diags.append(
            Diagnostic(
                severity="error",
                code=err.E_POLICY_FILESYSTEM,
                message="filesystem capability is not permitted in the skeleton (policy.filesystem must be false)",
                pointer="/policy/filesystem",
            )
        )
    if "max_parallel" in policy:
        max_parallel = policy.get("max_parallel")
        if not isinstance(max_parallel, int) or isinstance(max_parallel, bool) or max_parallel < 1:
            diags.append(
                Diagnostic(
                    severity="error",
                    code=err.E_DISALLOWED_CAPABILITY,
                    message="policy.max_parallel must be a positive integer",
                    pointer="/policy/max_parallel",
                )
            )
    for key in policy:
        if key not in KNOWN_CAPABILITIES:
            diags.append(
                Diagnostic(
                    severity="error",
                    code=err.E_DISALLOWED_CAPABILITY,
                    message=f"unknown capability {key!r}; allowed: {sorted(KNOWN_CAPABILITIES)}",
                    pointer=f"/policy/{key}",
                )
            )
    return diags


# ---------------------------------------------------------------------------
# Agent steps: agent ids, references, output schema.
# ---------------------------------------------------------------------------

def _lint_agent_steps(
    steps: list[Any],
    declared_inputs: set[str],
    known_step_ids: set[Any],
) -> list[Diagnostic]:
    diags: list[Diagnostic] = []
    for step, ptr in _schema.iter_agent_steps(steps):
        kind = step.get("kind")
        agent = step.get("agent")
        if kind == "agent" and isinstance(agent, str) and is_kanban_runner_id(agent):
            diags.append(
                Diagnostic(
                    severity="error",
                    code=err.E_UNKNOWN_AGENT,
                    message=(
                        f"reserved Kanban runner id {agent!r} cannot be called by an agent step; "
                        "use kind='kanban_agent' with a profile instead"
                    ),
                    pointer=f"{ptr}/agent",
                )
            )
        elif kind == "agent" and isinstance(agent, str) and agent and not is_known_agent(agent):
            diags.append(
                Diagnostic(
                    severity="error",
                    code=err.E_UNKNOWN_AGENT,
                    message=f"unknown agent id {agent!r}",
                    pointer=f"{ptr}/agent",
                )
            )

        if "output_schema" not in step:
            diags.append(
                Diagnostic(
                    severity="warning",
                    code=err.W_NO_OUTPUT_SCHEMA,
                    message="effect step has no 'output_schema'; output will be recorded unvalidated",
                    pointer=ptr,
                )
            )

        diags.extend(_lint_refs(step.get("input"), f"{ptr}/input", declared_inputs, known_step_ids))
        if kind == "kanban_agent":
            diags.extend(_lint_refs(step.get("task"), f"{ptr}/task", declared_inputs, known_step_ids))

        # depends_on references must point at known step ids.
        for j, dep in enumerate(step.get("depends_on", []) or []):
            if dep not in known_step_ids:
                diags.append(
                    Diagnostic(
                        severity="error",
                        code=err.E_UNRESOLVED_REF,
                        message=f"depends_on references unknown step id {dep!r}",
                        pointer=f"{ptr}/depends_on/{j}",
                    )
                )
    return diags


def _lint_if_steps(
    steps: list[Any],
    declared_inputs: set[str],
    known_step_ids: set[Any],
) -> list[Diagnostic]:
    """Lint conditional step refs that are not reached through effect inputs."""
    diags: list[Diagnostic] = []

    def walk(seq: list[Any], base: str) -> None:
        for i, step in enumerate(seq):
            if not isinstance(step, dict):
                continue
            ptr = f"{base}/{i}"
            kind = step.get("kind")
            if kind == "if":
                condition = step.get("condition")
                if isinstance(condition, dict):
                    diags.extend(_lint_refs(condition.get("ref"), f"{ptr}/condition/ref", declared_inputs, known_step_ids))
                then_steps = step.get("then")
                if isinstance(then_steps, list):
                    walk(then_steps, f"{ptr}/then")
                else_steps = step.get("else")
                if isinstance(else_steps, list):
                    walk(else_steps, f"{ptr}/else")
            elif kind in {"parallel", "pipeline", "phase"}:
                child_key = "branches" if kind == "parallel" else "steps"
                children = step.get(child_key)
                if isinstance(children, list):
                    walk(children, f"{ptr}/{child_key}")

    walk(steps, "/steps")
    return diags


def _lint_refs(
    value: Any,
    ptr: str,
    declared_inputs: set[str],
    known_step_ids: set[Any],
) -> list[Diagnostic]:
    """Lint every ``$ref:`` string reachable within an ``input`` value."""
    diags: list[Diagnostic] = []

    def walk(v: Any, p: str) -> None:
        if isinstance(v, str) and v.startswith("$ref:"):
            ref = parse_ref(v)
            if ref is None or ref.get("kind") == "invalid":
                diags.append(
                    Diagnostic(
                        severity="error",
                        code=err.E_BAD_REF,
                        message=f"malformed reference {v!r}",
                        pointer=p,
                    )
                )
            elif ref["kind"] == "input":
                if ref["key"] not in declared_inputs:
                    diags.append(
                        Diagnostic(
                            severity="warning",
                            code=err.W_UNDECLARED_INPUT,
                            message=f"reference to undeclared input {ref['key']!r}",
                            pointer=p,
                        )
                    )
            elif ref["kind"] == "step":
                if ref["step_id"] not in known_step_ids:
                    diags.append(
                        Diagnostic(
                            severity="error",
                            code=err.E_UNRESOLVED_REF,
                            message=f"reference to unknown step id {ref['step_id']!r}",
                            pointer=p,
                        )
                    )
        elif isinstance(v, dict):
            for k, sub in v.items():
                walk(sub, f"{p}/{k}")
        elif isinstance(v, list):
            for i, sub in enumerate(v):
                walk(sub, f"{p}/{i}")

    walk(value, ptr)
    return diags


# ---------------------------------------------------------------------------
# Execution-order validation.
# ---------------------------------------------------------------------------

def _lint_execution_order(steps: list[Any]) -> list[Diagnostic]:
    """Reject refs/depends_on edges the sequential skeleton cannot satisfy.

    The runtime executes steps in declaration order, recursively entering
    containers. A step may only reference or depend on step ids that have already
    completed in that deterministic order.
    """
    diags: list[Diagnostic] = []
    available: set[str] = set()

    def walk_step(step: Any, ptr: str, available_ids: set[str]) -> None:
        if not isinstance(step, dict):
            return
        kind = step.get("kind")
        step_id = step.get("id")
        for j, dep in enumerate(step.get("depends_on", []) or []):
            if isinstance(dep, str) and dep not in available_ids:
                diags.append(
                    Diagnostic(
                        severity="error",
                        code=err.E_UNRESOLVED_REF,
                        message=(
                            f"depends_on references step id {dep!r} before it is available "
                            "in declaration order"
                        ),
                        pointer=f"{ptr}/depends_on/{j}",
                    )
                )
        if kind in {"agent", "kanban_agent"}:
            _lint_step_refs_available(step.get("input"), f"{ptr}/input", available_ids, diags)
            if kind == "kanban_agent":
                _lint_step_refs_available(step.get("task"), f"{ptr}/task", available_ids, diags)
            if isinstance(step_id, str):
                available_ids.add(step_id)
        elif kind == "if":
            condition = step.get("condition")
            if isinstance(condition, dict):
                _lint_step_refs_available(condition.get("ref"), f"{ptr}/condition/ref", available_ids, diags)
            for branch_key in ("then", "else"):
                branch_steps = step.get(branch_key)
                if isinstance(branch_steps, list):
                    branch_available = set(available_ids)
                    for i, child in enumerate(branch_steps):
                        walk_step(child, f"{ptr}/{branch_key}/{i}", branch_available)
            if isinstance(step_id, str):
                available_ids.add(step_id)
        elif kind in {"parallel", "pipeline", "phase"}:
            child_key = "branches" if kind == "parallel" else "steps"
            children = step.get(child_key)
            if isinstance(children, list):
                for i, child in enumerate(children):
                    walk_step(child, f"{ptr}/{child_key}/{i}", available_ids)
            if isinstance(step_id, str):
                available_ids.add(step_id)

    for i, step in enumerate(steps):
        walk_step(step, f"/steps/{i}", available)
    return diags


def _lint_step_refs_available(
    value: Any,
    ptr: str,
    available_step_ids: set[str],
    diags: list[Diagnostic],
) -> None:
    def walk(v: Any, p: str) -> None:
        if isinstance(v, str) and v.startswith("$ref:"):
            ref = parse_ref(v)
            if isinstance(ref, dict) and ref.get("kind") == "step" and ref["step_id"] not in available_step_ids:
                diags.append(
                    Diagnostic(
                        severity="error",
                        code=err.E_UNRESOLVED_REF,
                        message=(
                            f"reference to step id {ref['step_id']!r} before it is available "
                            "in declaration order"
                        ),
                        pointer=p,
                    )
                )
        elif isinstance(v, dict):
            for k, sub in v.items():
                walk(sub, f"{p}/{k}")
        elif isinstance(v, list):
            for i, sub in enumerate(v):
                walk(sub, f"{p}/{i}")

    walk(value, ptr)


# ---------------------------------------------------------------------------
# Cycle detection over depends_on edges.
# ---------------------------------------------------------------------------

def _lint_cycles(steps: list[Any]) -> list[Diagnostic]:
    """Detect cycles in the ``depends_on`` graph of agent steps."""
    edges: dict[str, list[str]] = {}
    for step, _ in _schema.iter_agent_steps(steps):
        sid = step.get("id")
        if isinstance(sid, str):
            edges.setdefault(sid, [])
            for dep in step.get("depends_on", []) or []:
                if isinstance(dep, str):
                    edges[sid].append(dep)

    state: dict[str, int] = {}  # 0=unseen, 1=on-stack, 2=done.
    cyclic: set[str] = set()

    def dfs(node: str) -> None:
        state[node] = 1
        for nxt in edges.get(node, []):
            s = state.get(nxt, 0)
            if s == 0:
                dfs(nxt)
            elif s == 1:
                cyclic.add(nxt)
                cyclic.add(node)
        state[node] = 2

    for node in edges:
        if state.get(node, 0) == 0:
            dfs(node)

    if cyclic:
        return [
            Diagnostic(
                severity="error",
                code=err.E_CYCLE,
                message=f"cyclic depends_on graph involving steps: {sorted(cyclic)}",
                pointer="/steps",
            )
        ]
    return []


def _collect_all_step_ids(steps: list[Any]) -> set[Any]:
    """Collect ids of every step (any kind), recursively."""
    out: set[Any] = set()
    child_keys = ("branches", "steps", "then", "else")
    for step in steps:
        if not isinstance(step, dict):
            continue
        if isinstance(step.get("id"), str):
            out.add(step["id"])
        for ck in child_keys:
            children = step.get(ck)
            if isinstance(children, list):
                out |= _collect_all_step_ids(children)
    return out
