"""Dataclass models for the ``hermes_workflows`` plugin.

This module defines the structured value objects exchanged across the public
primitive boundary (``workflow_validate`` / ``workflow_run`` /
``workflow_status``). Every model is a plain ``@dataclass`` with type hints and
no behaviour beyond a couple of convenience ``as_dict`` helpers, so the shapes
map cleanly onto the JSON-schema-ish references declared in ``plugin.yaml``.

Nothing here performs I/O, network access, or agent execution.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Optional

__all__ = [
    "Severity",
    "RunState",
    "StepKind",
    "Diagnostic",
    "ValidationResult",
    "RunHandle",
    "Progress",
    "StepStatus",
    "RunStatus",
]

# ---------------------------------------------------------------------------
# Literal aliases shared across the package.
# ---------------------------------------------------------------------------

Severity = Literal["error", "warning"]
RunState = Literal["queued", "running", "paused", "suspended", "stopped", "succeeded", "failed", "cancelled", "unknown"]
StepKind = Literal["agent", "kanban_agent", "parallel", "pipeline", "phase", "if"]


@dataclass(frozen=True)
class Diagnostic:
    """A single static-analysis finding produced by ``workflow_validate``.

    Attributes:
        severity: ``"error"`` or ``"warning"``.
        code: Stable machine-readable code (e.g. ``"E_UNKNOWN_AGENT"``).
            Downstream code and tests assert on this rather than ``message``.
        message: Human-readable explanation.
        pointer: RFC-6901 JSON-Pointer into the definition the finding refers
            to (e.g. ``"/steps/1/agent"``). ``""`` points at the document root.
    """

    severity: Severity
    code: str
    message: str
    pointer: str = ""

    def as_dict(self) -> dict[str, Any]:
        """Return a plain ``dict`` representation."""
        return asdict(self)


@dataclass
class ValidationResult:
    """Outcome of statically checking a workflow definition.

    Attributes:
        ok: ``True`` when there are no error-severity diagnostics.
        errors: Error-severity diagnostics.
        warnings: Warning-severity diagnostics.
        normalized: The parsed + normalized definition, or ``None`` when the
            input could not be parsed.
        def_hash: Full sha256 hex digest of the canonicalized definition, or
            ``""`` when the input could not be parsed/canonicalized.
    """

    ok: bool
    errors: list[Diagnostic] = field(default_factory=list)
    warnings: list[Diagnostic] = field(default_factory=list)
    normalized: Optional[dict[str, Any]] = None
    def_hash: str = ""

    def as_dict(self) -> dict[str, Any]:
        """Return a plain ``dict`` representation (diagnostics expanded)."""
        return {
            "ok": self.ok,
            "errors": [d.as_dict() for d in self.errors],
            "warnings": [d.as_dict() for d in self.warnings],
            "normalized": self.normalized,
            "def_hash": self.def_hash,
        }


@dataclass(frozen=True)
class RunHandle:
    """Lightweight handle returned by ``workflow_run``.

    Attributes:
        run_id: Identifier under which the run is recorded in the registry.
        status: Lifecycle state at the moment the handle was produced.
        created_at: ISO-8601 UTC timestamp of run creation.
        def_hash: Full sha256 hex digest of the canonicalized definition.
    """

    run_id: str
    status: RunState
    created_at: str
    def_hash: str

    def as_dict(self) -> dict[str, Any]:
        """Return a plain ``dict`` representation."""
        return asdict(self)


@dataclass
class Progress:
    """Aggregate progress counters for a run."""

    total: int = 0
    completed: int = 0
    failed: int = 0
    running: int = 0
    queued: int = 0
    cancelled: int = 0
    pct: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        """Return a plain ``dict`` representation."""
        return asdict(self)


@dataclass
class StepStatus:
    """State of a single step within a run.

    Attributes:
        step_id: The step's ``id`` from the definition.
        kind: One of ``agent`` / ``parallel`` / ``pipeline`` / ``phase``.
        status: Free-form lifecycle string (e.g. ``"succeeded"``).
        agent: Hermes agent id for ``agent`` steps, else ``None``.
        started_at: ISO-8601 UTC start timestamp, or ``None`` if not started.
        ended_at: ISO-8601 UTC end timestamp, or ``None`` if not finished.
        output: Structured agent output (schema-validated), or ``None``.
        error: Structured error info, or ``None``.
        workflow_id: Optional run id; maps to native ``workflowId`` in UIs.
        workflow_node_id: Optional step id for native workflow grouping.
        workflow_phase_id: Optional phase id for native workflow grouping.
        workflow_phase_title: Optional human-readable phase label.
        workflow_task_title: Optional human-readable task label.
    """

    step_id: str
    kind: StepKind
    status: str = "queued"
    agent: Optional[str] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    output: Optional[dict[str, Any]] = None
    error: Optional[dict[str, Any]] = None
    workflow_id: Optional[str] = None
    workflow_node_id: Optional[str] = None
    workflow_phase_id: Optional[str] = None
    workflow_phase_title: Optional[str] = None
    workflow_task_title: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        """Return a plain ``dict`` representation."""
        return asdict(self)


@dataclass
class RunStatus:
    """Full status snapshot returned by ``workflow_status``.

    Attributes:
        run_id: The queried run id.
        status: Lifecycle state; ``"unknown"`` for an unrecognised id.
        created_at: ISO-8601 UTC creation timestamp (``""`` if unknown).
        updated_at: ISO-8601 UTC last-update timestamp (``""`` if unknown).
        progress: Aggregate :class:`Progress` counters.
        steps: Per-step :class:`StepStatus` list (may be empty when
            ``include_steps=False`` or for unknown runs).
        result: Final structured result of the run, or ``None``.
        error: Structured error info if the run failed, or ``None``.
    """

    run_id: str
    status: RunState
    created_at: str = ""
    updated_at: str = ""
    progress: Progress = field(default_factory=Progress)
    steps: list[StepStatus] = field(default_factory=list)
    result: Optional[dict[str, Any]] = None
    error: Optional[dict[str, Any]] = None

    def as_dict(self) -> dict[str, Any]:
        """Return a plain ``dict`` representation (nested models expanded)."""
        return {
            "run_id": self.run_id,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "progress": self.progress.as_dict(),
            "steps": [s.as_dict() for s in self.steps],
            "result": self.result,
            "error": self.error,
        }
