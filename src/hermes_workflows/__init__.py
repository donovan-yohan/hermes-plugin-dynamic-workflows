"""hermes_workflows — sandboxed, deterministic workflow orchestration for Hermes.

A pure-stdlib (Python 3.11) Hermes agent plugin exposing three primitives for
declarative, JS-like orchestration over Hermes agents:

* :func:`workflow_validate` — statically check a definition (parse, schema,
  sandbox-policy lint) without running anything.
* :func:`workflow_run` — execute a validated definition in the deterministic,
  network-free runtime, fanning out to Hermes agents via an injected
  :class:`~hermes_workflows.agents.AgentRunner`.
* :func:`workflow_status` — query a run's state/progress by id from a pluggable
  run registry (in-memory by default).

The runtime is a SKELETON: it interprets declarative JSON, never executes code,
and enforces a default-deny capability policy (no network, no filesystem). See
:mod:`hermes_workflows.sandbox` and ``DESIGN.md`` for the security model.
"""

from __future__ import annotations

from .primitives import workflow_validate, workflow_run, workflow_status
from .models import (
    Diagnostic,
    ValidationResult,
    RunHandle,
    RunStatus,
    StepStatus,
    Progress,
)
from .agents import AgentRunner, StubAgentRunner, KNOWN_AGENTS, is_known_agent
from .registry import (
    RunStore,
    InMemoryRunStore,
    KanbanRunStore,
    RunRecord,
    get_default_store,
)
from .errors import (
    WorkflowError,
    WorkflowValidationError,
    RunNotFound,
    SandboxPolicyError,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # primitives
    "workflow_validate",
    "workflow_run",
    "workflow_status",
    # models
    "Diagnostic",
    "ValidationResult",
    "RunHandle",
    "RunStatus",
    "StepStatus",
    "Progress",
    # agents
    "AgentRunner",
    "StubAgentRunner",
    "KNOWN_AGENTS",
    "is_known_agent",
    # registry
    "RunStore",
    "InMemoryRunStore",
    "KanbanRunStore",
    "RunRecord",
    "get_default_store",
    # errors
    "WorkflowError",
    "WorkflowValidationError",
    "RunNotFound",
    "SandboxPolicyError",
]
