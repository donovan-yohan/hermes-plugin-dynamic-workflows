"""In-memory run registry and the pluggable :class:`RunStore` protocol.

The registry holds the authoritative state of every run as a :class:`RunRecord`
(a mutable counterpart to the immutable :class:`~hermes_workflows.models.RunStatus`
snapshot returned to callers). ``primitives`` accept a ``registry=`` injection
so downstream code can swap stores without touching the primitives.

Run-id scheme
-------------
``run_id = "wf_" + <def_hash8> + "_" + <uuid4hex12>`` where:

* ``def_hash8`` is the first 8 hex chars of the sha256 of the canonicalized
  definition (so ids sort by source definition), and
* ``uuid4hex12`` is the first 12 hex chars of a fresh ``uuid4`` (collision-
  resistant per-run suffix).

This makes ids sortable-by-source and collision-resistant, and lets
``workflow_status`` correlate a run back to its definition via the full
``def_hash`` stored on every record. A caller may always override ``run_id``
for idempotency or deterministic tests. ``InMemoryRunStore`` is thread-safe via
a single ``Lock``.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, Protocol, runtime_checkable

from .models import Progress, RunState, RunStatus, StepStatus
from .errors import RunNotFound

__all__ = [
    "utc_now_iso",
    "RunRecord",
    "RunStore",
    "InMemoryRunStore",
    "KanbanRunStore",
    "get_default_store",
]


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string with ``Z`` suffix.

    Centralised so tests can monkeypatch a single function for determinism.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class RunRecord:
    """Authoritative, mutable state of a single run held by a store."""

    run_id: str
    def_hash: str
    status: RunState = "queued"
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)
    steps: list[StepStatus] = field(default_factory=list)
    result: Optional[dict[str, Any]] = None
    error: Optional[dict[str, Any]] = None

    def to_status(self, include_steps: bool = True) -> RunStatus:
        """Project this record into an immutable :class:`RunStatus` snapshot."""
        return RunStatus(
            run_id=self.run_id,
            status=self.status,
            created_at=self.created_at,
            updated_at=self.updated_at,
            progress=self._progress(),
            steps=list(self.steps) if include_steps else [],
            result=self.result,
            error=self.error,
        )

    def _progress(self) -> Progress:
        """Compute aggregate :class:`Progress` from the leaf step statuses."""
        total = len(self.steps)
        completed = sum(1 for s in self.steps if s.status == "succeeded")
        failed = sum(1 for s in self.steps if s.status == "failed")
        running = sum(1 for s in self.steps if s.status == "running")
        pct = round(100.0 * (completed + failed) / total, 2) if total else 0.0
        return Progress(total=total, completed=completed, failed=failed, running=running, pct=pct)


@runtime_checkable
class RunStore(Protocol):
    """Pluggable persistence boundary for run records.

    Implementations must be safe to call from the primitives. The default
    :class:`InMemoryRunStore` is process-global and thread-safe.
    """

    def next_run_id(self, def_hash: str) -> str:
        """Mint a fresh, store-unique run id for ``def_hash``."""
        ...

    def create(self, run_id: str, def_hash: str) -> RunRecord:
        """Create and persist a new run record; return it."""
        ...

    def get(self, run_id: str) -> Optional[RunRecord]:
        """Return the record for ``run_id`` or ``None`` if absent."""
        ...

    def set_status(self, run_id: str, status: RunState, *, result: dict | None = None, error: dict | None = None) -> None:
        """Update lifecycle ``status`` (and optional terminal result/error)."""
        ...

    def update_step(self, run_id: str, step: StepStatus) -> None:
        """Insert or replace a :class:`StepStatus` by ``step_id``."""
        ...

    def list(self) -> list[RunRecord]:
        """Return all records (order unspecified)."""
        ...


class InMemoryRunStore:
    """Thread-safe, process-local :class:`RunStore` (the default backend).

    All mutating operations take a single ``Lock``. Run ids follow the
    ``wf_<def_hash8>_<uuid4hex12>`` scheme documented in the module docstring.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._records: dict[str, RunRecord] = {}

    def next_run_id(self, def_hash: str) -> str:
        """Mint ``wf_<hash8>_<uuid4hex12>`` for ``def_hash``.

        The ``def_hash8`` prefix sorts ids by source definition; the 12-hex
        ``uuid4`` suffix makes them collision-resistant.
        """
        return f"wf_{def_hash[:8]}_{uuid.uuid4().hex[:12]}"

    def create(self, run_id: str, def_hash: str) -> RunRecord:
        """Create a record; raise ``ValueError`` on duplicate ``run_id``."""
        with self._lock:
            if run_id in self._records:
                raise ValueError(f"run_id already exists: {run_id!r}")
            now = utc_now_iso()
            record = RunRecord(run_id=run_id, def_hash=def_hash, created_at=now, updated_at=now)
            self._records[run_id] = record
            return record

    def get(self, run_id: str) -> Optional[RunRecord]:
        """Return the record for ``run_id`` or ``None``."""
        with self._lock:
            return self._records.get(run_id)

    def set_status(self, run_id: str, status: RunState, *, result: dict | None = None, error: dict | None = None) -> None:
        """Update status/result/error; raise :class:`RunNotFound` if absent."""
        with self._lock:
            rec = self._records.get(run_id)
            if rec is None:
                raise RunNotFound(run_id)
            rec.status = status
            if result is not None:
                rec.result = result
            if error is not None:
                rec.error = error
            rec.updated_at = utc_now_iso()

    def update_step(self, run_id: str, step: StepStatus) -> None:
        """Upsert a step by ``step_id``; raise :class:`RunNotFound` if absent."""
        with self._lock:
            rec = self._records.get(run_id)
            if rec is None:
                raise RunNotFound(run_id)
            for i, existing in enumerate(rec.steps):
                if existing.step_id == step.step_id:
                    rec.steps[i] = step
                    break
            else:
                rec.steps.append(step)
            rec.updated_at = utc_now_iso()

    def list(self) -> list[RunRecord]:
        """Return a snapshot list of all records."""
        with self._lock:
            return list(self._records.values())


class KanbanRunStore:
    """Pluggable Kanban-backed :class:`RunStore` alternative (NOT implemented).

    Documented design (see ``DESIGN.md``): a board models one workflow, columns
    model the run lifecycle (``queued`` -> ``running`` -> ``succeeded`` /
    ``failed`` / ``cancelled``), and cards model runs (with checklist items per
    step). This is a deliberate stub — the skeleton has no external Kanban
    dependency, so every method raises :class:`NotImplementedError`.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(
            "KanbanRunStore is a documented, pluggable alternative and is not "
            "implemented in the skeleton; use InMemoryRunStore."
        )

    def next_run_id(self, def_hash: str) -> str:  # pragma: no cover - stub
        raise NotImplementedError

    def create(self, run_id: str, def_hash: str) -> RunRecord:  # pragma: no cover - stub
        raise NotImplementedError

    def get(self, run_id: str) -> Optional[RunRecord]:  # pragma: no cover - stub
        raise NotImplementedError

    def set_status(self, run_id: str, status: RunState, *, result: dict | None = None, error: dict | None = None) -> None:  # pragma: no cover - stub
        raise NotImplementedError

    def update_step(self, run_id: str, step: StepStatus) -> None:  # pragma: no cover - stub
        raise NotImplementedError

    def list(self) -> list[RunRecord]:  # pragma: no cover - stub
        raise NotImplementedError


# Process-global default store used when a primitive is called without an
# explicit ``registry=``. Tests that need isolation should pass their own.
_DEFAULT_STORE = InMemoryRunStore()


def get_default_store() -> InMemoryRunStore:
    """Return the process-global default :class:`InMemoryRunStore`."""
    return _DEFAULT_STORE
