"""Backend-neutral operator controls, status, and wait inspection (issue #9).

A long-running workflow — a JSON-runtime run, a subprocess script run, or a
feedback-controller loop — eventually needs an *operator* surface that is not the
authoring surface: pause it, resume it, stop it without shredding the audit
trail, retry a failed call/task with explicit lineage, and answer "what is this
run blocked on right now?" without hand-decoding raw journal/snapshot JSON.

This module is that surface, and it is deliberately **generic**. It knows nothing
about Relay, ATH, or Kanban. It models three boring-but-load-bearing things:

* **Control records.** Every pause/resume/stop/task_stop/retry is an append-only
  :class:`WorkflowControl` audit record persisted by a :class:`ControlStore`.
  Recording an intent never mutates or deletes a run's existing history — a stop
  *adds* a stop record, it does not erase anything. The records survive a process
  restart because the file store re-reads them from disk.
* **A control-state projection.** :func:`project_control_state` folds a run's
  control records into a compact :class:`RunControlState` (``running`` /
  ``paused`` / ``stopped``, plus the list of stopped child tasks and retry
  lineage). Stop is terminal: once a run is stopped a later resume does not
  un-stop it (it is still recorded for audit). This is *desired* state — actually
  preventing new child work or reattaching to pending work is the backend
  adapter's job; core only records and projects the intent.
* **Enforcement decisions.** :func:`evaluate_control_state` (and the ``may_*``
  wrappers) fold a :class:`RunControlState` plus an *operation* into a
  :class:`ControlDecision` — a yes/no with a stable ``code`` — that an adapter or
  runtime consults *before* it starts child work, continues a task, or retries.
  Core decides; the adapter enforces (it owns the actual decline/cancel/replay).
  A stopped run blocks every operation; a paused run blocks only *new* work and
  never claims to kill in-flight waits; a ``task_stop`` blocks only its matching
  ``target_ref``; a retry decision surfaces any recorded ``replacement_ref`` /
  ``attempt`` so the adapter reuses the replacement instead of silently
  duplicating it. The decision is pure — it reads the projection, never a store.
* **Wait inspection.** :func:`waits_from_loop_status` and
  :func:`waits_from_kanban_states` turn data the other slices already persist
  (a loop's ``waiting_for_*`` state + suspension event, or a script store's
  non-terminal Kanban card states) into uniform :class:`WaitSummary` rows, so an
  operator sees blocked waits without spelunking.

Retry lineage is **idempotent by construction**. ``retry(store, run_id, ref)``
returns the existing retry record for that target instead of forking a duplicate;
``force=True`` mints the next attempt. Each retry carries ``attempt`` and a
``replacement_ref`` (the new call/task id), so the lineage from an original
failed call to its replacement is explicit even though the *replacement
execution* stays adapter-owned.

Pure Python 3.11 stdlib. No network, filesystem-auth, shell, or backend calls.
"""

from __future__ import annotations

import json
import os
import re
import threading
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Literal, Optional, Protocol, runtime_checkable

from .errors import ControlError
from .registry import utc_now_iso

__all__ = [
    "CONTROL_ACTIONS",
    "CONTROL_OPERATIONS",
    "CONTROL_DECISION_CODES",
    "ControlAction",
    "ControlOperation",
    "DesiredRunState",
    "WorkflowControl",
    "RunControlState",
    "ControlDecision",
    "WaitSummary",
    "RunSummary",
    "ControlStore",
    "InMemoryControlStore",
    "FileControlStore",
    "pause_run",
    "resume_run",
    "stop_run",
    "stop_task",
    "retry",
    "record_control",
    "project_control_state",
    "evaluate_control_state",
    "may_start_work",
    "may_continue_task",
    "may_retry",
    "may_check_run",
    "waits_from_loop_status",
    "waits_from_kanban_states",
    "summarize_run",
    "inspect_run",
    "list_runs",
    "run_links",
    "current_phase",
]

# The closed set of operator intents. ``stop`` halts the whole run; ``task_stop``
# halts one named child task; ``retry`` replaces a failed call/task. An unknown
# action fails closed rather than being silently recorded.
ControlAction = Literal["pause", "resume", "stop", "task_stop", "retry"]
CONTROL_ACTIONS: tuple[str, ...] = ("pause", "resume", "stop", "task_stop", "retry")

# Projected desired control state of a run. This is intent, not enforcement.
DesiredRunState = Literal["running", "paused", "stopped"]

# The closed set of enforcement operations an adapter can ask about, and the
# closed set of stable codes :func:`evaluate_control_state` returns. ``code`` is
# the machine-branchable contract; a decision's ``reason`` is only its gloss.
ControlOperation = Literal["start_child", "continue_task", "retry", "check_run"]
CONTROL_OPERATIONS: tuple[str, ...] = ("start_child", "continue_task", "retry", "check_run")
CONTROL_DECISION_CODES: tuple[str, ...] = (
    "allowed",
    "run_stopped",
    "run_paused",
    "task_stopped",
    "retry_exists",
)

# A run id / control id must be usable as exactly one filesystem path segment,
# mirroring the guard the run/script stores already use.
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

# Kanban statuses that are *not* an in-flight wait (mirrors script_store).
_KANBAN_NON_WAIT = frozenset({"completed", "failed", "alias"})

# Loop controller states that mean the run is parked on an external signal.
_LOOP_WAIT_STATES = frozenset({"waiting_for_event", "waiting_for_approval"})


# ---------------------------------------------------------------------------
# Value objects.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkflowControl:
    """One append-only operator-control audit record.

    ``control_id`` is both the audit id and the idempotency key: appending a
    control whose id already exists for the run is a no-op that returns the
    stored record. ``target_ref`` names the child call/task for ``task_stop`` and
    ``retry``; ``replacement_ref`` + ``attempt`` carry the retry lineage. The
    record is immutable — corrections are *new* records, never edits.
    """

    control_id: str
    run_id: str
    action: ControlAction
    created_at: str = field(default_factory=utc_now_iso)
    actor: Optional[str] = None
    reason: Optional[str] = None
    target_ref: Optional[str] = None
    replacement_ref: Optional[str] = None
    attempt: Optional[int] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WorkflowControl":
        """Rebuild a record from a persisted dict; raise on a malformed shape."""
        if not isinstance(data, dict):
            raise ControlError("control record must be an object")
        action = data.get("action")
        if action not in CONTROL_ACTIONS:
            raise ControlError(f"unknown control action: {action!r}")
        control_id = data.get("control_id")
        run_id = data.get("run_id")
        if not isinstance(control_id, str) or not control_id:
            raise ControlError("control record requires a control_id")
        if not isinstance(run_id, str) or not run_id:
            raise ControlError("control record requires a run_id")
        attempt = data.get("attempt")
        if attempt is not None and (not isinstance(attempt, int) or isinstance(attempt, bool)):
            raise ControlError("control attempt must be an integer or null")
        metadata = data.get("metadata")
        return cls(
            control_id=control_id,
            run_id=run_id,
            action=action,  # type: ignore[arg-type]
            created_at=str(data.get("created_at") or utc_now_iso()),
            actor=_opt_str(data.get("actor")),
            reason=_opt_str(data.get("reason")),
            target_ref=_opt_str(data.get("target_ref")),
            replacement_ref=_opt_str(data.get("replacement_ref")),
            attempt=attempt,
            metadata=metadata if isinstance(metadata, dict) else {},
        )


@dataclass(frozen=True)
class RunControlState:
    """Compact projection of a run's control records into desired state.

    ``desired_state`` is the single headline an operator UI needs. ``stopped`` is
    terminal — a resume after a stop is recorded for audit but does not flip the
    run back to ``running``. ``stopped_tasks`` and ``retries`` expose the child
    level: which tasks were individually stopped and the retry lineage.
    """

    run_id: str
    desired_state: DesiredRunState = "running"
    paused: bool = False
    stopped: bool = False
    stop_reason: Optional[str] = None
    stopped_at: Optional[str] = None
    stopped_by: Optional[str] = None
    stopped_control_id: Optional[str] = None
    stopped_tasks: list[dict[str, Any]] = field(default_factory=list)
    retries: list[dict[str, Any]] = field(default_factory=list)
    last_control_id: Optional[str] = None
    last_action: Optional[ControlAction] = None
    controls_total: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ControlDecision:
    """A backend-neutral yes/no on whether one operation may proceed right now.

    :func:`evaluate_control_state` folds a :class:`RunControlState` plus an
    operation into one of these. ``allowed`` is the headline; ``code`` is the
    stable, machine-branchable verdict (one of :data:`CONTROL_DECISION_CODES`)
    and ``reason`` is its human gloss. ``desired_state`` echoes the run's
    projected state so a caller logging a denial has it to hand. ``control_id``
    points at the specific recorded control responsible for a per-target/​retry
    block when there is one. For a ``retry`` operation
    ``replacement_ref`` / ``attempt`` carry the lineage of any retry already
    recorded for ``target_ref``, so an adapter reuses the existing replacement
    instead of silently minting a duplicate.

    This is a *decision*, not enforcement: actually declining to dispatch,
    cancelling, or replaying work stays the adapter's job.
    """

    allowed: bool
    code: str
    reason: str
    run_id: str
    operation: str
    target_ref: Optional[str] = None
    desired_state: DesiredRunState = "running"
    control_id: Optional[str] = None
    replacement_ref: Optional[str] = None
    attempt: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WaitSummary:
    """One uniform row describing what a run is currently blocked on."""

    run_id: str
    wait_id: str
    kind: str  # "event" | "approval" | "kanban" | caller-supplied
    state: str  # e.g. "waiting_for_event" | "blocked" | "waiting"
    summary: str = ""
    source: str = ""  # "loop" | "kanban" | caller-supplied
    ref: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunSummary:
    """Compact, list-friendly view of one run for the ``/workflows`` overview."""

    run_id: str
    kind: str
    status: str
    created_at: str = ""
    updated_at: str = ""
    progress: dict[str, Any] = field(default_factory=dict)
    current_phase: Optional[str] = None
    desired_state: DesiredRunState = "running"
    paused: bool = False
    stopped: bool = False
    wait_count: int = 0
    links: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Persistence boundary.
# ---------------------------------------------------------------------------


@runtime_checkable
class ControlStore(Protocol):
    """Append-only persistence boundary for operator-control records.

    Implementations must be idempotent on ``control_id`` and must preserve
    insertion order in :meth:`list_for`. They never delete or mutate a recorded
    control — the audit trail is append-only.
    """

    def append(self, control: WorkflowControl) -> WorkflowControl:
        ...

    def list_for(self, run_id: str) -> list[WorkflowControl]:
        ...

    def runs(self) -> list[str]:
        ...


class InMemoryControlStore:
    """Process-local, thread-safe :class:`ControlStore` for tests/embedders."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # run_id -> ordered {control_id: control}
        self._by_run: dict[str, dict[str, WorkflowControl]] = {}

    def append(self, control: WorkflowControl) -> WorkflowControl:
        with self._lock:
            bucket = self._by_run.setdefault(control.run_id, {})
            existing = bucket.get(control.control_id)
            if existing is not None:
                return existing  # idempotent: a re-issued control id is a no-op.
            bucket[control.control_id] = control
            return control

    def list_for(self, run_id: str) -> list[WorkflowControl]:
        with self._lock:
            return list(self._by_run.get(run_id, {}).values())

    def runs(self) -> list[str]:
        with self._lock:
            return list(self._by_run.keys())


class FileControlStore:
    """Filesystem :class:`ControlStore`: ``<root>/<run_id>/controls.jsonl``.

    Each run's controls are an append-only JSONL log. Idempotency and
    cross-restart durability come from the same place: every append re-reads the
    run's existing control ids from disk before writing, so a re-issued (e.g.
    deterministic retry) control id is deduped even by a fresh process. A corrupt
    line is skipped rather than failing the whole read, so one torn write never
    hides the rest of a run's audit trail.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def append(self, control: WorkflowControl) -> WorkflowControl:
        _require_safe_segment(control.run_id, "run_id")
        _require_safe_segment(control.control_id, "control_id")
        with self._lock:
            existing = self._load_unlocked(control.run_id)
            if control.control_id in existing:
                return existing[control.control_id]
            run_dir = self.root / control.run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            line = json.dumps(control.to_dict(), ensure_ascii=False, separators=(",", ":"))
            with (run_dir / "controls.jsonl").open("a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()
                os.fsync(f.fileno())
            return control

    def list_for(self, run_id: str) -> list[WorkflowControl]:
        _require_safe_segment(run_id, "run_id")
        with self._lock:
            return list(self._load_unlocked(run_id).values())

    def runs(self) -> list[str]:
        if not self.root.exists():
            return []
        out: list[str] = []
        for child in sorted(self.root.iterdir()):
            if (
                child.is_dir()
                and _SEGMENT_RE.fullmatch(child.name)
                and (child / "controls.jsonl").exists()
            ):
                out.append(child.name)
        return out

    def _load_unlocked(self, run_id: str) -> dict[str, WorkflowControl]:
        path = self.root / run_id / "controls.jsonl"
        out: dict[str, WorkflowControl] = {}
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return out
        except OSError:
            return out
        for raw in text.splitlines():
            raw = raw.strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
                control = WorkflowControl.from_dict(data)
            except (json.JSONDecodeError, ControlError):
                continue  # a torn/malformed line is skipped; the rest still loads.
            if control.run_id != run_id:
                continue  # a well-formed line forged into the wrong run directory is ignored.
            if control.control_id not in out:
                out[control.control_id] = control  # first write wins for deterministic idempotency.
        return out


# ---------------------------------------------------------------------------
# Verbs — mint a control, persist it, return it.
# ---------------------------------------------------------------------------


def record_control(
    store: ControlStore,
    run_id: str,
    action: ControlAction,
    *,
    actor: Optional[str] = None,
    reason: Optional[str] = None,
    target_ref: Optional[str] = None,
    replacement_ref: Optional[str] = None,
    attempt: Optional[int] = None,
    control_id: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> WorkflowControl:
    """Mint and persist one control record (the generic primitive behind the verbs).

    A caller-supplied ``control_id`` makes the append idempotent (re-recording the
    same id returns the stored record). Without one a fresh ``ctl_<uuid>`` id is
    minted, so each call is a distinct audit event.
    """
    if action not in CONTROL_ACTIONS:
        raise ControlError(f"unknown control action: {action!r}")
    _require_nonempty(run_id, "run_id")
    if action in ("task_stop", "retry") and not _opt_str(target_ref):
        raise ControlError(f"{action} requires a target_ref (the child call/task id)")
    control = WorkflowControl(
        control_id=control_id or f"ctl_{uuid.uuid4().hex[:16]}",
        run_id=run_id,
        action=action,
        actor=_opt_str(actor),
        reason=_opt_str(reason),
        target_ref=_opt_str(target_ref),
        replacement_ref=_opt_str(replacement_ref),
        attempt=attempt,
        metadata=dict(metadata or {}),
    )
    return store.append(control)


def pause_run(
    store: ControlStore,
    run_id: str,
    *,
    actor: Optional[str] = None,
    reason: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> WorkflowControl:
    """Record intent to stop launching *new* child work while preserving waits."""
    return record_control(store, run_id, "pause", actor=actor, reason=reason, metadata=metadata)


def resume_run(
    store: ControlStore,
    run_id: str,
    *,
    actor: Optional[str] = None,
    reason: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> WorkflowControl:
    """Record intent to resume a paused run (reattach to existing pending work)."""
    return record_control(store, run_id, "resume", actor=actor, reason=reason, metadata=metadata)


def stop_run(
    store: ControlStore,
    run_id: str,
    *,
    actor: Optional[str] = None,
    reason: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> WorkflowControl:
    """Record a terminal stop. The audit trail is preserved; nothing is deleted."""
    return record_control(store, run_id, "stop", actor=actor, reason=reason, metadata=metadata)


def stop_task(
    store: ControlStore,
    run_id: str,
    target_ref: str,
    *,
    actor: Optional[str] = None,
    reason: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> WorkflowControl:
    """Record intent to stop one named child task without stopping the whole run."""
    return record_control(
        store, run_id, "task_stop", actor=actor, reason=reason, target_ref=target_ref, metadata=metadata
    )


def retry(
    store: ControlStore,
    run_id: str,
    target_ref: str,
    *,
    replacement_ref: Optional[str] = None,
    force: bool = False,
    actor: Optional[str] = None,
    reason: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> WorkflowControl:
    """Record an idempotent retry of a failed call/task with explicit lineage.

    Default behaviour is **idempotent per ``(run_id, target_ref)``**: if a retry
    of ``target_ref`` already exists, the stored record is returned unchanged
    rather than forking a second attempt. ``force=True`` mints the next attempt
    (``attempt`` increments) so a deliberate re-retry is possible after a failed
    replacement.

    Each retry carries ``attempt`` (1-based) and a ``replacement_ref`` — the id
    of the replacement call/task. Pass it explicitly when the backend has already
    minted the replacement; otherwise a deterministic ``<target_ref>#retry<N>``
    placeholder is recorded so lineage is still explicit. The control id is
    derived from ``(run_id, target_ref, attempt)``, so even a retried record is
    deduped across a restart.
    """
    _require_nonempty(run_id, "run_id")
    target = _opt_str(target_ref)
    if not target:
        raise ControlError("retry requires a target_ref (the failed call/task id)")

    prior = [c for c in store.list_for(run_id) if c.action == "retry" and c.target_ref == target]
    if prior and not force:
        return prior[-1]  # idempotent: re-issuing a retry returns the recorded one.

    attempt = (max((c.attempt or 0) for c in prior) + 1) if prior else 1
    control_id = f"ctl_retry_{_short_hash(run_id, target, str(attempt))}"
    replacement = _opt_str(replacement_ref) or f"{target}#retry{attempt}"
    return record_control(
        store,
        run_id,
        "retry",
        actor=actor,
        reason=reason,
        target_ref=target,
        replacement_ref=replacement,
        attempt=attempt,
        control_id=control_id,
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# Projection.
# ---------------------------------------------------------------------------


def project_control_state(run_id: str, controls: Iterable[WorkflowControl]) -> RunControlState:
    """Fold a run's control records into a compact :class:`RunControlState`.

    Records are applied in recorded order. ``stop`` is terminal; a later
    ``resume`` is kept in the audit trail but never un-stops the run.
    ``task_stop`` collects per-task stops; ``retry`` collects lineage. The
    projection is pure and order-stable, so the same records always yield the
    same state.
    """
    ordered = list(controls)
    paused = False
    stopped = False
    stop_reason: Optional[str] = None
    stopped_at: Optional[str] = None
    stopped_by: Optional[str] = None
    stopped_control_id: Optional[str] = None
    stopped_tasks: list[dict[str, Any]] = []
    retries: list[dict[str, Any]] = []
    last_control_id: Optional[str] = None
    last_action: Optional[ControlAction] = None

    for c in ordered:
        last_control_id = c.control_id
        last_action = c.action
        if c.action == "stop":
            if not stopped:
                stopped = True
                stop_reason = c.reason
                stopped_at = c.created_at
                stopped_by = c.actor
                stopped_control_id = c.control_id
            paused = False
        elif c.action == "pause":
            if not stopped:
                paused = True
        elif c.action == "resume":
            if not stopped:
                paused = False
        elif c.action == "task_stop":
            stopped_tasks.append(
                {
                    "target_ref": c.target_ref,
                    "control_id": c.control_id,
                    "reason": c.reason,
                    "at": c.created_at,
                }
            )
        elif c.action == "retry":
            retries.append(
                {
                    "target_ref": c.target_ref,
                    "replacement_ref": c.replacement_ref,
                    "attempt": c.attempt,
                    "control_id": c.control_id,
                    "reason": c.reason,
                    "at": c.created_at,
                }
            )

    desired: DesiredRunState = "stopped" if stopped else ("paused" if paused else "running")
    return RunControlState(
        run_id=run_id,
        desired_state=desired,
        paused=paused,
        stopped=stopped,
        stop_reason=stop_reason,
        stopped_at=stopped_at,
        stopped_by=stopped_by,
        stopped_control_id=stopped_control_id,
        stopped_tasks=stopped_tasks,
        retries=retries,
        last_control_id=last_control_id,
        last_action=last_action,
        controls_total=len(ordered),
    )


# ---------------------------------------------------------------------------
# Enforcement decisions — the reusable seam adapters consult before acting.
# ---------------------------------------------------------------------------


def evaluate_control_state(
    control_state: RunControlState,
    operation: ControlOperation,
    target_ref: Optional[str] = None,
) -> ControlDecision:
    """Decide whether ``operation`` may proceed given a run's control state.

    ``operation`` is one of :data:`CONTROL_OPERATIONS`:

    * ``start_child`` — launch *new* child work. Blocked by a stop (terminal) or
      a pause (new work is held).
    * ``continue_task`` — keep an existing ``target_ref`` running. Blocked by a
      stop or by a matching ``task_stop``; a pause does **not** block it (pausing
      never claims to kill in-flight work). Requires ``target_ref``.
    * ``retry`` — replace a failed ``target_ref``. Blocked by a stop, by a
      matching ``task_stop``, or — to avoid silently duplicating replacement
      work — by an already-recorded retry, in which case the decision carries
      that retry's ``replacement_ref`` / ``attempt`` / ``control_id`` so the
      adapter reuses it (force a fresh attempt with :func:`retry` to override).
      Absent those, a pause also blocks it, since a retry launches new work.
      Requires ``target_ref``.
    * ``check_run`` — may the run make progress at all. Blocked only by a stop;
      a paused run is still alive.

    Pure and side-effect-free: it reads the projection, never a store. The
    returned :class:`ControlDecision` records the verdict; *enforcing* it stays
    the adapter's responsibility.
    """
    if operation not in CONTROL_OPERATIONS:
        raise ControlError(f"unknown control operation: {operation!r}")
    target = _opt_str(target_ref)
    if operation in ("continue_task", "retry") and not target:
        raise ControlError(f"{operation} requires a target_ref (the child call/task id)")

    run_id = control_state.run_id
    desired = control_state.desired_state

    def decide(
        allowed: bool,
        code: str,
        reason: str,
        *,
        control_id: Optional[str] = None,
        replacement_ref: Optional[str] = None,
        attempt: Optional[int] = None,
    ) -> ControlDecision:
        return ControlDecision(
            allowed=allowed,
            code=code,
            reason=reason,
            run_id=run_id,
            operation=operation,
            target_ref=target,
            desired_state=desired,
            control_id=control_id,
            replacement_ref=replacement_ref,
            attempt=attempt,
        )

    # Stop is terminal: it blocks new, continuing, and retry work alike.
    if control_state.stopped:
        gloss = "run is stopped" + (f": {control_state.stop_reason}" if control_state.stop_reason else "")
        return decide(False, "run_stopped", gloss, control_id=control_state.stopped_control_id)

    # An explicit per-task stop blocks only operations on that exact target.
    if target and operation in ("continue_task", "retry"):
        ts = _last_match(control_state.stopped_tasks, target)
        if ts is not None:
            gloss = f"task {target!r} was stopped" + (f": {ts.get('reason')}" if ts.get("reason") else "")
            return decide(False, "task_stopped", gloss, control_id=ts.get("control_id"))

    # A retry already on record: surface its lineage so the adapter reuses the
    # replacement rather than minting a duplicate.
    if operation == "retry" and target:
        pr = _last_match(control_state.retries, target)
        if pr is not None:
            attempt = pr.get("attempt")
            gloss = (
                f"retry of {target!r} already recorded"
                + (f" (attempt {attempt})" if attempt is not None else "")
                + "; reuse the recorded replacement or force a new attempt"
            )
            return decide(
                False,
                "retry_exists",
                gloss,
                control_id=pr.get("control_id"),
                replacement_ref=pr.get("replacement_ref"),
                attempt=attempt,
            )

    # A pause holds only new work (start_child / retry); existing tasks continue.
    if control_state.paused and operation in ("start_child", "retry"):
        return decide(False, "run_paused", "run is paused; new work is held")

    return decide(True, "allowed", "no recorded control blocks this operation")


def may_start_work(control_state: RunControlState) -> ControlDecision:
    """Convenience wrapper: may *new* child work start on this run?"""
    return evaluate_control_state(control_state, "start_child")


def may_continue_task(control_state: RunControlState, target_ref: str) -> ControlDecision:
    """Convenience wrapper: may the existing ``target_ref`` task keep running?"""
    return evaluate_control_state(control_state, "continue_task", target_ref)


def may_retry(control_state: RunControlState, target_ref: str) -> ControlDecision:
    """Convenience wrapper: may ``target_ref`` be retried (or is one already on record)?"""
    return evaluate_control_state(control_state, "retry", target_ref)


def may_check_run(control_state: RunControlState) -> ControlDecision:
    """Convenience wrapper: may the run make progress at all (only a stop blocks)?"""
    return evaluate_control_state(control_state, "check_run")


# ---------------------------------------------------------------------------
# Wait inspection — read existing loop/script-store data, not new backends.
# ---------------------------------------------------------------------------


def waits_from_loop_status(status: Any) -> list[WaitSummary]:
    """Extract the active wait from a loop run's status (or its ``as_dict``).

    A loop parks in ``waiting_for_event`` / ``waiting_for_approval`` and records
    the suspension request as an event of the same kind carrying a ``request``
    payload. This returns a single-row (or empty) list describing that wait, with
    ``wait_id`` taken from the request's ``id`` / ``token`` / ``kind``.
    """
    data = status.as_dict() if hasattr(status, "as_dict") else status
    if not isinstance(data, dict):
        return []
    state = data.get("state")
    if state not in _LOOP_WAIT_STATES:
        return []
    run_id = str(data.get("run_id") or "")
    request = _latest_event_request(data.get("events"), state)
    wait_id = _request_identity(request) or state
    summary = ""
    if isinstance(request, dict):
        summary = str(request.get("summary") or "")
    if not summary:
        report = data.get("report")
        if isinstance(report, dict):
            latest = report.get("latest_sensor")
            if isinstance(latest, dict):
                summary = str(latest.get("summary") or "")
    kind = "approval" if state == "waiting_for_approval" else "event"
    return [
        WaitSummary(
            run_id=run_id,
            wait_id=wait_id,
            kind=kind,
            state=str(state),
            summary=summary,
            source="loop",
            ref=request if isinstance(request, dict) else {},
        )
    ]


def waits_from_kanban_states(states: Iterable[dict[str, Any]], *, run_id: str = "") -> list[WaitSummary]:
    """Turn non-terminal Kanban card states into :class:`WaitSummary` rows.

    ``states`` is the shape ``ScriptRunStore.kanban_waits()`` returns (or any list
    of ``{card_id, status, profile, reason, ...}`` dicts). Terminal cards
    (``completed`` / ``failed`` / ``alias``) are skipped defensively even though
    that store already excludes them.
    """
    out: list[WaitSummary] = []
    for state in states:
        if not isinstance(state, dict):
            continue
        status = state.get("status")
        if status in _KANBAN_NON_WAIT:
            continue
        card_id = str(state.get("card_id") or "")
        if not card_id:
            continue
        out.append(
            WaitSummary(
                run_id=str(state.get("run_id") or run_id or ""),
                wait_id=card_id,
                kind="kanban",
                state=str(status or "waiting"),
                summary=str(state.get("reason") or state.get("profile") or ""),
                source="kanban",
                ref={k: state[k] for k in ("card_id", "profile", "status") if k in state},
            )
        )
    return out


# ---------------------------------------------------------------------------
# Run-level and list-level projections (the operator status / overview surface).
# ---------------------------------------------------------------------------


def summarize_run(
    record: Any,
    *,
    kind: str = "workflow",
    control_state: Optional[RunControlState] = None,
    wait_count: int = 0,
    links: Optional[dict[str, Any]] = None,
) -> RunSummary:
    """Project a run record (anything with the registry ``RunRecord`` shape) into
    a compact :class:`RunSummary`, merging in control state and a wait count."""
    progress = _record_progress(record)
    state = control_state or RunControlState(run_id=getattr(record, "run_id", ""))
    return RunSummary(
        run_id=getattr(record, "run_id", ""),
        kind=kind,
        status=str(getattr(record, "status", "unknown")),
        created_at=str(getattr(record, "created_at", "") or ""),
        updated_at=str(getattr(record, "updated_at", "") or ""),
        progress=progress,
        current_phase=current_phase(getattr(record, "steps", []) or []),
        desired_state=state.desired_state,
        paused=state.paused,
        stopped=state.stopped,
        wait_count=wait_count,
        links=dict(links or {}),
    )


def inspect_run(
    run_id: str,
    *,
    lifecycle: str = "unknown",
    control_state: Optional[RunControlState] = None,
    current_phase: Optional[str] = None,
    progress: Optional[dict[str, Any]] = None,
    waits: Optional[Iterable[WaitSummary]] = None,
    child_task_refs: Optional[Iterable[str]] = None,
    result: Any = None,
    error: Optional[dict[str, Any]] = None,
    phases: Optional[Iterable[dict[str, Any]]] = None,
    last_events: Optional[Iterable[dict[str, Any]]] = None,
    links: Optional[dict[str, Any]] = None,
    events_limit: int = 10,
) -> dict[str, Any]:
    """Build the compact single-run operator status payload (issue #9).

    Everything is plain data so this stays backend-neutral: a caller assembles
    the pieces from whichever stores it has (registry status, control store, loop
    status, script-store waits) and this composes them into one stable shape with
    current phase, waits, child task refs, retry lineage, last events, and
    result/error — no raw JSON spelunking required downstream.

    ``decisions`` exposes the run-level enforcement verdicts (``start_child`` and
    ``check_run``) the adapter would get from :func:`evaluate_control_state`, so a
    status reader sees "may new work start / may the run continue" honestly as a
    *decision* — not a claim that core has cancelled anything.
    """
    state = control_state or RunControlState(run_id=run_id)
    wait_rows = [w.to_dict() for w in (waits or [])]
    child_refs = _child_task_refs(child_task_refs, wait_rows, state)
    events = list(last_events or [])
    if events_limit >= 0:
        events = events[-events_limit:]
    return {
        "run_id": run_id,
        "lifecycle": lifecycle,
        "control_state": state.to_dict(),
        "decisions": {
            "start_child": evaluate_control_state(state, "start_child").to_dict(),
            "check_run": evaluate_control_state(state, "check_run").to_dict(),
        },
        "current_phase": current_phase,
        "phases": _phase_progress_rows(phases or [], current_phase=current_phase, lifecycle=lifecycle, events=events),
        "progress": progress,
        "waits": wait_rows,
        "child_task_refs": child_refs,
        "retries": list(state.retries),
        "stopped_tasks": list(state.stopped_tasks),
        "result": result,
        "error": error,
        "last_events": events,
        "links": dict(links or {}),
        "controls_total": state.controls_total,
    }


def list_runs(
    records: Iterable[Any],
    control_store: Optional[ControlStore] = None,
    *,
    kind: str = "workflow",
    waits: Optional[Iterable[WaitSummary]] = None,
    link_resolver: Optional[Any] = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Build the ``/workflows`` overview: recent runs plus blocked waits.

    ``records`` is any iterable of registry-shaped run records (e.g.
    ``RunStore.list()``). Each is summarised and merged with its control state
    from ``control_store``. ``waits`` (from the loop/Kanban inspectors) are folded
    in both as per-run wait counts and as a flat ``blocked_waits`` list. Runs are
    returned newest-first and capped at ``limit``; the counts cover the full set.
    """
    wait_list = list(waits or [])
    waits_by_run: dict[str, int] = {}
    for w in wait_list:
        waits_by_run[w.run_id] = waits_by_run.get(w.run_id, 0) + 1

    summaries: list[RunSummary] = []
    counts = {"total": 0, "running": 0, "paused": 0, "stopped": 0, "succeeded": 0, "failed": 0}
    for record in records:
        run_id = getattr(record, "run_id", "")
        controls = control_store.list_for(run_id) if control_store is not None else []
        state = project_control_state(run_id, controls)
        links = link_resolver(record) if callable(link_resolver) else {}
        summary = summarize_run(
            record,
            kind=str(getattr(record, "kind", kind) or kind),
            control_state=state,
            wait_count=waits_by_run.get(run_id, 0),
            links=links if isinstance(links, dict) else {},
        )
        summaries.append(summary)
        counts["total"] += 1
        if state.stopped:
            counts["stopped"] += 1
        elif state.paused:
            counts["paused"] += 1
        if summary.status in ("running", "succeeded", "failed"):
            counts[summary.status] += 1

    summaries.sort(key=lambda s: (s.updated_at or "", s.run_id), reverse=True)
    active = [
        s.run_id
        for s in summaries
        if not s.stopped and (s.paused or s.status in ("running", "queued"))
    ]
    counts["waits"] = len(wait_list)
    return {
        "runs": [s.to_dict() for s in summaries[: max(0, limit)]],
        "active": active,
        "blocked_waits": [w.to_dict() for w in wait_list],
        "counts": counts,
    }


def run_links(
    *,
    run_id: str,
    script_path: Optional[str] = None,
    journal_path: Optional[str] = None,
    snapshot_path: Optional[str] = None,
    transcript_path: Optional[str] = None,
    result_path: Optional[str] = None,
    tasks: Optional[Iterable[str]] = None,
    extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Assemble a dashboard link bundle for a run (paths/refs the caller knows).

    Backend-neutral: core does not guess where a run's artefacts live, so the
    caller passes the paths it has (the file run/script stores expose them). Only
    non-empty values are included.
    """
    links: dict[str, Any] = {"run_id": run_id}
    for key, value in (
        ("script", script_path),
        ("journal", journal_path),
        ("snapshot", snapshot_path),
        ("transcript", transcript_path),
        ("result", result_path),
    ):
        if value:
            links[key] = str(value)
    task_list = [str(t) for t in (tasks or []) if t]
    if task_list:
        links["tasks"] = task_list
    if extra:
        links.update(extra)
    return links


# ---------------------------------------------------------------------------
# Internal helpers.
# ---------------------------------------------------------------------------


def _opt_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    return value or None


def _require_nonempty(value: Any, label: str) -> None:
    if not isinstance(value, str) or not value:
        raise ControlError(f"{label} must be a non-empty string")


def _last_match(rows: Iterable[dict[str, Any]], target: str) -> Optional[dict[str, Any]]:
    """Return the last projected row whose ``target_ref`` equals ``target``."""
    match: Optional[dict[str, Any]] = None
    for row in rows:
        if isinstance(row, dict) and row.get("target_ref") == target:
            match = row
    return match


def _require_safe_segment(value: str, label: str) -> None:
    if not isinstance(value, str) or not _SEGMENT_RE.fullmatch(value):
        raise ControlError(f"unsafe {label}: {value!r}")


def _short_hash(*parts: str) -> str:
    import hashlib

    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return digest[:16]


def _latest_event_request(events: Any, state: str) -> Optional[dict[str, Any]]:
    if not isinstance(events, list):
        return None
    for event in reversed(events):
        if isinstance(event, dict) and event.get("kind") == state:
            request = event.get("request")
            if isinstance(request, dict):
                return request
            return {}
    return None


def _request_identity(request: Any) -> Optional[str]:
    if not isinstance(request, dict):
        return None
    for key in ("id", "token", "kind"):
        value = request.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _record_progress(record: Any) -> dict[str, Any]:
    progress = getattr(record, "_progress", None)
    if callable(progress):
        try:
            return progress().as_dict()
        except Exception:  # pragma: no cover - defensive; record shape is trusted
            return {}
    return {}


def current_phase(steps: Iterable[Any]) -> Optional[str]:
    """Return the human-readable phase of a run's most relevant step.

    Prefers the last step still ``running`` (the live phase); otherwise the last
    recorded step. Falls back from ``workflow_phase_title`` to
    ``workflow_phase_id`` and finally ``None`` when no phase metadata exists.
    """
    step_list = list(steps)
    if not step_list:
        return None
    running = [s for s in step_list if getattr(s, "status", None) == "running"]
    pick = running[-1] if running else step_list[-1]
    return getattr(pick, "workflow_phase_title", None) or getattr(pick, "workflow_phase_id", None)


def _phase_progress_rows(
    phases: Iterable[dict[str, Any]],
    *,
    current_phase: Optional[str],
    lifecycle: str,
    events: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    declared: list[dict[str, Any]] = []
    for index, phase in enumerate(phases):
        if not isinstance(phase, dict) or not phase.get("title"):
            continue
        declared.append(
            {
                "id": str(phase.get("id") or f"phase_{index + 1}"),
                "title": str(phase.get("title")),
                "detail": str(phase.get("detail") or ""),
            }
        )
    if not declared:
        return []

    seen: list[str] = []
    for event in events:
        if not isinstance(event, dict) or event.get("method") != "phase" or event.get("ok") is not True:
            continue
        title = event.get("phase_title") or event.get("label")
        if isinstance(title, str) and title:
            seen.append(title)

    active = current_phase or (seen[-1] if seen else None)
    active_index = _phase_index(declared, active)
    seen_indexes = {_phase_index(declared, title) for title in seen}
    seen_indexes.discard(None)
    terminal = lifecycle in {"succeeded", "failed", "cancelled"}

    out: list[dict[str, Any]] = []
    for index, row in enumerate(declared):
        status = "queued"
        if active_index is not None:
            if index < active_index:
                status = "succeeded"
            elif index == active_index:
                status = "succeeded" if terminal else "running"
        elif index in seen_indexes:
            status = "succeeded" if terminal else "running"
        out.append({**row, "status": status})
    return out


def _phase_index(rows: list[dict[str, Any]], value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    for index, row in enumerate(rows):
        if value in {row.get("id"), row.get("title")}:
            return index
    return None


def _child_task_refs(
    explicit: Optional[Iterable[str]],
    wait_rows: list[dict[str, Any]],
    state: RunControlState,
) -> list[str]:
    refs: list[str] = []
    seen: set[str] = set()

    def add(value: Any) -> None:
        if isinstance(value, str) and value and value not in seen:
            seen.add(value)
            refs.append(value)

    for ref in explicit or []:
        add(ref)
    for w in wait_rows:
        add(w.get("wait_id"))
    for r in state.retries:
        add(r.get("replacement_ref"))
        add(r.get("target_ref"))
    for t in state.stopped_tasks:
        add(t.get("target_ref"))
    return refs
