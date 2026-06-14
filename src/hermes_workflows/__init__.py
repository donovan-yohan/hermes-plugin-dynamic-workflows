"""hermes_workflows — sandboxed, deterministic workflow orchestration for Hermes.

A pure-stdlib (Python 3.11) Hermes agent plugin exposing a model-facing
``workflow`` facade plus debug primitives for declarative, script-led
orchestration over Hermes agents and Kanban-backed awaitables:

* :func:`workflow` — validate, run, or inspect a workflow through one entry point.
* :func:`workflow_validate` — statically check a definition (parse, schema,
  sandbox-policy lint) without running anything.
* :func:`workflow_run` — execute a validated definition in the deterministic,
  network-free runtime, fanning out through an injected
  :class:`~hermes_workflows.agents.AgentRunner`.
* :func:`workflow_status` — query a run's state/progress by id from a pluggable
  run registry (in-memory by default, file-backed for plugin persistence).

The runtime is a SKELETON: it interprets declarative JSON, never executes code,
and enforces a default-deny capability policy. Workflow definitions get no direct
network or filesystem authority; parent-owned stores may persist run metadata.
See :mod:`hermes_workflows.sandbox` and ``DESIGN.md`` for the security model.
"""

from __future__ import annotations

from .primitives import (
    workflow,
    workflow_validate,
    workflow_run,
    workflow_status,
    workflow_validate_script,
    run_workflow_script,
)
from .models import (
    Diagnostic,
    ValidationResult,
    RunHandle,
    RunStatus,
    StepStatus,
    Progress,
)
from .script_validator import ScriptValidation, validate_script
from .vm import (
    CapabilityBroker,
    ScriptRunResult,
    VMLimits,
    WorkflowVM,
    run_script,
)
from .agents import (
    AgentRunner,
    StubAgentRunner,
    KNOWN_AGENTS,
    is_known_agent,
    kanban_runner_id,
    is_kanban_runner_id,
)
from .registry import (
    RunStore,
    InMemoryRunStore,
    FileRunStore,
    KanbanRunStore,
    RunRecord,
    get_default_store,
)
from .script_store import (
    ScriptRunStore,
    ScriptRunMeta,
    ReplayCache,
    ReplayEntry,
    CallRecorder,
    SCRIPT_SCHEMA_VERSION,
    script_run_id,
    canonical_hash,
    is_replayable,
)
from .errors import (
    WorkflowError,
    WorkflowValidationError,
    RunNotFound,
    SandboxPolicyError,
    WorkflowScriptError,
    ScriptValidationError,
    WorkflowSubprocessError,
    CapabilityDenied,
    ScriptRunStoreError,
    ScriptRunNotFound,
    CorruptScriptRunError,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # primitives
    "workflow",
    "workflow_validate",
    "workflow_run",
    "workflow_status",
    "workflow_validate_script",
    "run_workflow_script",
    # subprocess workflow VM (issue #2)
    "validate_script",
    "ScriptValidation",
    "WorkflowVM",
    "CapabilityBroker",
    "VMLimits",
    "ScriptRunResult",
    "run_script",
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
    "kanban_runner_id",
    "is_kanban_runner_id",
    # registry
    "RunStore",
    "InMemoryRunStore",
    "FileRunStore",
    "KanbanRunStore",
    "RunRecord",
    "get_default_store",
    # durable script run store + replay cache (issue #3)
    "ScriptRunStore",
    "ScriptRunMeta",
    "ReplayCache",
    "ReplayEntry",
    "CallRecorder",
    "SCRIPT_SCHEMA_VERSION",
    "script_run_id",
    "canonical_hash",
    "is_replayable",
    # errors
    "WorkflowError",
    "WorkflowValidationError",
    "RunNotFound",
    "SandboxPolicyError",
    "WorkflowScriptError",
    "ScriptValidationError",
    "WorkflowSubprocessError",
    "CapabilityDenied",
    "ScriptRunStoreError",
    "ScriptRunNotFound",
    "CorruptScriptRunError",
]
