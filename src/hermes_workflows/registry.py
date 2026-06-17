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

from collections import deque
import json
import os
import re
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Protocol, runtime_checkable

from .models import Progress, RunState, RunStatus, StepStatus
from .errors import RunNotFound

_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

__all__ = [
    "utc_now_iso",
    "RunRecord",
    "RunStore",
    "InMemoryRunStore",
    "FileRunStore",
    "KanbanRunStore",
    "get_default_store",
]


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string with ``Z`` suffix.

    Centralised so tests can monkeypatch a single function for determinism.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _new_run_id(def_hash: str) -> str:
    """Mint ``wf_<hash8>_<uuid4hex12>`` for ``def_hash``."""
    return f"wf_{def_hash[:8]}_{uuid.uuid4().hex[:12]}"


def _require_safe_run_id(run_id: str) -> None:
    """Reject run ids that cannot safely be used as one path segment."""
    if not isinstance(run_id, str) or not _RUN_ID_RE.fullmatch(run_id):
        raise ValueError(f"unsafe run_id: {run_id!r}")


def _require_safe_session_id(session_id: Optional[str]) -> Optional[str]:
    """Return a normalized session id or None; reject unsafe values."""
    if session_id is None:
        return None
    if not isinstance(session_id, str) or not _RUN_ID_RE.fullmatch(session_id):
        raise ValueError(f"unsafe session_id: {session_id!r}")
    return session_id


def _apply_status(
    rec: "RunRecord",
    status: RunState,
    *,
    result: dict | None = None,
    error: dict | None = None,
) -> None:
    rec.status = status
    if result is not None:
        rec.result = result
    if error is not None:
        rec.error = error
    rec.updated_at = utc_now_iso()


def _upsert_step(rec: "RunRecord", step: StepStatus) -> None:
    for i, existing in enumerate(rec.steps):
        if existing.step_id == step.step_id:
            rec.steps[i] = step
            break
    else:
        rec.steps.append(step)
    rec.updated_at = utc_now_iso()


def _fsync_dir(path: Path) -> None:
    """Best-effort directory fsync after atomic metadata updates."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


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
        queued = sum(1 for s in self.steps if s.status == "queued")
        cancelled = sum(1 for s in self.steps if s.status == "cancelled")
        pct = round(100.0 * (completed + failed + cancelled) / total, 2) if total else 0.0
        return Progress(
            total=total,
            completed=completed,
            failed=failed,
            running=running,
            queued=queued,
            cancelled=cancelled,
            pct=pct,
        )


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

    When ``session_id`` is supplied the store is scoped to that session: runs
    created in one session are invisible to other sessions. This mirrors the
    native ``dynamic_workflow`` session-scoping behavior.
    """

    def __init__(self, session_id: Optional[str] = None) -> None:
        self._session_id = _require_safe_session_id(session_id)
        self._lock = threading.Lock()
        self._records: dict[str, RunRecord] = {}

    def next_run_id(self, def_hash: str) -> str:
        """Mint ``wf_<hash8>_<uuid4hex12>`` for ``def_hash``.

        The ``def_hash8`` prefix sorts ids by source definition; the 12-hex
        ``uuid4`` suffix makes them collision-resistant.
        """
        return _new_run_id(def_hash)

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
            _apply_status(rec, status, result=result, error=error)

    def update_step(self, run_id: str, step: StepStatus) -> None:
        """Upsert a step by ``step_id``; raise :class:`RunNotFound` if absent."""
        with self._lock:
            rec = self._records.get(run_id)
            if rec is None:
                raise RunNotFound(run_id)
            _upsert_step(rec, step)

    def list(self) -> list[RunRecord]:
        """Return a snapshot list of all records."""
        with self._lock:
            return list(self._records.values())


class FileRunStore:
    """Filesystem-backed :class:`RunStore` with compact journal events.

    The workflow runtime still has no direct filesystem authority; this store is
    a parent-owned persistence backend. Each run gets a directory containing a
    bounded ``snapshot.json`` plus append-only ``journal.jsonl`` events so a
    future workflow process can inspect or resume without main-chat transcript
    spam.
    """

    def __init__(self, root: str | Path, session_id: Optional[str] = None) -> None:
        self._session_id = _require_safe_session_id(session_id)
        base = Path(root).expanduser().resolve()
        if self._session_id:
            base = base / self._session_id
        self.root = base
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._records: dict[str, RunRecord] = {}

    def next_run_id(self, def_hash: str) -> str:
        return _new_run_id(def_hash)

    def create(self, run_id: str, def_hash: str) -> RunRecord:
        _require_safe_run_id(run_id)
        with self._lock:
            if run_id in self._records:
                raise ValueError(f"run_id already exists: {run_id!r}")
            now = utc_now_iso()
            record = RunRecord(run_id=run_id, def_hash=def_hash, created_at=now, updated_at=now)
            try:
                self._run_dir(run_id).mkdir(parents=True, exist_ok=False)
            except FileExistsError as exc:
                raise ValueError(f"run_id already exists: {run_id!r}") from exc
            self._write_snapshot(record)
            self._append_event(run_id, "created", {"def_hash": def_hash, "status": record.status})
            self._records[run_id] = record
            return record

    def get(self, run_id: str) -> Optional[RunRecord]:
        _require_safe_run_id(run_id)
        with self._lock:
            record = self._records.get(run_id)
            if record is not None:
                return record
            snapshot = self._snapshot_path(run_id)
            if not snapshot.exists():
                return None
            record = _record_from_dict(json.loads(snapshot.read_text(encoding="utf-8")))
            self._records[run_id] = record
            return record

    def set_status(
        self,
        run_id: str,
        status: RunState,
        *,
        result: dict | None = None,
        error: dict | None = None,
    ) -> None:
        _require_safe_run_id(run_id)
        with self._lock:
            rec = self._require_record(run_id)
            _apply_status(rec, status, result=result, error=error)
            self._write_snapshot(rec)
            self._append_event(run_id, "status", {"status": status, "has_result": result is not None, "error": error})

    def update_step(self, run_id: str, step: StepStatus) -> None:
        _require_safe_run_id(run_id)
        with self._lock:
            rec = self._require_record(run_id)
            _upsert_step(rec, step)
            self._write_snapshot(rec)
            self._append_event(
                run_id,
                "step",
                {
                    "step_id": step.step_id,
                    "kind": step.kind,
                    "status": step.status,
                    "agent": step.agent,
                    "has_output": step.output is not None,
                    "error": step.error,
                },
            )

    def list(self) -> list[RunRecord]:
        with self._lock:
            for snapshot in self.root.glob("*/snapshot.json"):
                run_id = snapshot.parent.name
                if run_id not in self._records:
                    self._records[run_id] = _record_from_dict(json.loads(snapshot.read_text(encoding="utf-8")))
            return list(self._records.values())

    def journal(self, run_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        """Return recent compact journal events for ``run_id``."""
        _require_safe_run_id(run_id)
        path = self._journal_path(run_id)
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8") as f:
            lines = deque(f, maxlen=max(1, limit))
        events: list[dict[str, Any]] = []
        for line in lines:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append(event)
        return events

    def _require_record(self, run_id: str) -> RunRecord:
        _require_safe_run_id(run_id)
        rec = self._records.get(run_id)
        if rec is not None:
            return rec
        snapshot = self._snapshot_path(run_id)
        if not snapshot.exists():
            raise RunNotFound(run_id)
        rec = _record_from_dict(json.loads(snapshot.read_text(encoding="utf-8")))
        self._records[run_id] = rec
        return rec

    def _run_dir(self, run_id: str) -> Path:
        _require_safe_run_id(run_id)
        return self.root / run_id

    def _snapshot_path(self, run_id: str) -> Path:
        return self._run_dir(run_id) / "snapshot.json"

    def _journal_path(self, run_id: str) -> Path:
        return self._run_dir(run_id) / "journal.jsonl"

    def _write_snapshot(self, record: RunRecord) -> None:
        run_dir = self._run_dir(record.run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(_record_to_dict(record), ensure_ascii=False, indent=2) + "\n"
        tmp = run_dir / "snapshot.json.tmp"
        with tmp.open("w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._snapshot_path(record.run_id))
        _fsync_dir(run_dir)

    def _append_event(self, run_id: str, event_type: str, data: dict[str, Any]) -> None:
        event = {"ts": utc_now_iso(), "type": event_type, "run_id": run_id, **data}
        with self._journal_path(run_id).open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())


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
_DEFAULT_SESSION_STORES: dict[str, InMemoryRunStore] = {}


def get_default_store(session_id: Optional[str] = None) -> InMemoryRunStore:
    """Return the process-global default :class:`InMemoryRunStore`.

    When ``session_id`` is provided, returns a cached session-scoped store so
    runs from one Hermes session do not leak into another and multiple calls in
    the same session see the same records.
    """
    if not session_id:
        return _DEFAULT_STORE
    if session_id not in _DEFAULT_SESSION_STORES:
        _DEFAULT_SESSION_STORES[session_id] = InMemoryRunStore(session_id=session_id)
    return _DEFAULT_SESSION_STORES[session_id]


def _record_to_dict(record: RunRecord) -> dict[str, Any]:
    """Serialize a run record snapshot without transcripts or raw child logs."""
    return asdict(record)


def _record_from_dict(data: dict[str, Any]) -> RunRecord:
    """Restore a :class:`RunRecord` from ``snapshot.json``."""
    steps = []
    for step in data.get("steps", []):
        # Back-compat: older snapshots did not write workflow metadata fields.
        steps.append(
            StepStatus(
                step_id=step["step_id"],
                kind=step.get("kind", "agent"),
                status=step.get("status", "queued"),
                agent=step.get("agent"),
                started_at=step.get("started_at"),
                ended_at=step.get("ended_at"),
                output=step.get("output"),
                error=step.get("error"),
                workflow_id=step.get("workflow_id"),
                workflow_node_id=step.get("workflow_node_id"),
                workflow_phase_id=step.get("workflow_phase_id"),
                workflow_phase_title=step.get("workflow_phase_title"),
                workflow_task_title=step.get("workflow_task_title"),
            )
        )
    return RunRecord(
        run_id=data["run_id"],
        def_hash=data["def_hash"],
        status=data.get("status", "queued"),
        created_at=data.get("created_at", ""),
        updated_at=data.get("updated_at", ""),
        steps=steps,
        result=data.get("result"),
        error=data.get("error"),
    )
