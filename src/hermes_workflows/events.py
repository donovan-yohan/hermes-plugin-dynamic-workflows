"""Backend-neutral workflow event broker for external wakeups (issue #7).

Kanban awaits already have a durable task-event log. This module adds the
parallel seam for non-card events (GitHub/webhook/check/review/deploy style
signals): producers append stable events, consumers wait on predicates, and the
store/notifier pair makes delivery idempotent and restart-safe.

No polling, no GitHub API calls, no dispatcher lives here. Hosts own webhook
receivers and call :func:`publish_workflow_event` or
:func:`publish_github_webhook_event` when a trusted event arrives.
"""

from __future__ import annotations

import copy
from contextlib import contextmanager
import json
import os
import re
import select
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional, Protocol, runtime_checkable

from .grants import redact_credentials
from .registry import utc_now_iso

try:  # pragma: no cover - exercised on POSIX, absent on Windows.
    import fcntl as _fcntl
except ImportError:  # pragma: no cover
    _fcntl = None  # type: ignore[assignment]

__all__ = [
    "WorkflowEvent",
    "WorkflowEventPredicate",
    "WorkflowEventStore",
    "InMemoryWorkflowEventStore",
    "FileWorkflowEventStore",
    "WorkflowEventNotifier",
    "ThreadWorkflowEventNotifier",
    "FifoWorkflowEventNotifier",
    "WorkflowEventBroker",
    "publish_workflow_event",
    "publish_github_webhook_event",
    "workflow_event_from_github_webhook",
    "match_workflow_event",
]

_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,191}$")
_IDLE_WAIT_S = 1.0


@dataclass(frozen=True)
class WorkflowEvent:
    """One durable external workflow wakeup event.

    ``version`` is assigned by the store on first append and is monotonic per
    store. Duplicate ``event_id`` delivery returns the original stored event with
    the original version, so producers can safely retry webhook delivery.
    """

    event_id: str
    source: str
    event_type: str
    subject: str
    payload: dict[str, Any] = field(default_factory=dict)
    version: int = 0
    occurred_at: str = field(default_factory=utc_now_iso)

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _require_safe_text(self.event_id, "event_id"))
        object.__setattr__(self, "source", _require_safe_text(self.source, "source"))
        object.__setattr__(self, "event_type", _require_safe_text(self.event_type, "event_type"))
        object.__setattr__(self, "subject", _require_subject(self.subject))
        if not isinstance(self.version, int) or isinstance(self.version, bool) or self.version < 0:
            raise ValueError("workflow event version must be a non-negative integer")
        if not isinstance(self.payload, dict):
            raise ValueError("workflow event payload must be an object")
        object.__setattr__(self, "payload", redact_credentials(copy.deepcopy(self.payload)))
        object.__setattr__(self, "occurred_at", str(self.occurred_at or utc_now_iso()))

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["payload"] = redact_credentials(data["payload"])
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WorkflowEvent":
        if not isinstance(data, dict):
            raise ValueError("workflow event must be an object")
        payload = data.get("payload")
        return cls(
            event_id=_require_safe_text(data.get("event_id"), "event_id"),
            source=_require_safe_text(data.get("source"), "source"),
            event_type=_require_safe_text(data.get("event_type"), "event_type"),
            subject=_require_subject(data.get("subject")),
            payload=payload if isinstance(payload, dict) else {},
            version=data.get("version", 0),
            occurred_at=str(data.get("occurred_at") or utc_now_iso()),
        )


@dataclass(frozen=True)
class WorkflowEventPredicate:
    """A durable wait predicate for non-Kanban workflow events."""

    source: Optional[str] = None
    event_type: Optional[str] = None
    subject: Optional[str] = None
    payload_match: dict[str, Any] = field(default_factory=dict)
    after_version: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _opt_safe_text(self.source, "source"))
        object.__setattr__(self, "event_type", _opt_safe_text(self.event_type, "event_type"))
        object.__setattr__(self, "subject", _opt_subject(self.subject))
        if not isinstance(self.after_version, int) or isinstance(self.after_version, bool) or self.after_version < 0:
            raise ValueError("after_version must be a non-negative integer")
        if not isinstance(self.payload_match, dict):
            raise ValueError("payload_match must be an object")
        object.__setattr__(self, "payload_match", copy.deepcopy(self.payload_match))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WorkflowEventPredicate":
        if not isinstance(data, dict):
            raise ValueError("workflow event predicate must be an object")
        payload_match = data.get("payload_match")
        return cls(
            source=data.get("source"),
            event_type=data.get("event_type"),
            subject=data.get("subject"),
            payload_match=payload_match if isinstance(payload_match, dict) else {},
            after_version=data.get("after_version", 0),
        )


@runtime_checkable
class WorkflowEventStore(Protocol):
    def append_event(self, event: WorkflowEvent) -> WorkflowEvent:
        ...

    def find_events(self, predicate: WorkflowEventPredicate, *, limit: int = 50) -> list[WorkflowEvent]:
        ...

    def current_version(self) -> int:
        ...


class InMemoryWorkflowEventStore:
    """In-process event store for tests and simple embedders."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: list[WorkflowEvent] = []
        self._by_id: dict[str, WorkflowEvent] = {}
        self._version = 0

    def append_event(self, event: WorkflowEvent) -> WorkflowEvent:
        with self._lock:
            existing = self._by_id.get(event.event_id)
            if existing is not None:
                return copy.deepcopy(existing)
            self._version += 1
            stored = _with_version(event, self._version)
            self._events.append(stored)
            self._by_id[stored.event_id] = stored
            return copy.deepcopy(stored)

    def find_events(self, predicate: WorkflowEventPredicate, *, limit: int = 50) -> list[WorkflowEvent]:
        with self._lock:
            matches = [copy.deepcopy(e) for e in self._events if match_workflow_event(e, predicate)]
        return matches[: max(0, int(limit))]

    def current_version(self) -> int:
        with self._lock:
            return self._version


class FileWorkflowEventStore:
    """Append-only JSONL event store, restart-safe and idempotent by event id.

    Appends and reads are guarded by a per-store lock file on POSIX platforms so
    multiple producer/consumer processes using the same root share one monotonic
    version stream. On platforms without ``fcntl`` this degrades to the in-process
    lock only; production cross-process use should provide a real shared store.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.path = self.root / "workflow_events.jsonl"
        self.lock_path = self.root / "workflow_events.lock"
        self._lock = threading.Lock()

    def append_event(self, event: WorkflowEvent) -> WorkflowEvent:
        with self._lock:
            with _event_file_lock(self.lock_path, exclusive=True):
                events = self._load_all_unlocked()
                for existing in events:
                    if existing.event_id == event.event_id:
                        return existing
                version = (events[-1].version if events else 0) + 1
                stored = _with_version(event, version)
                self.root.mkdir(parents=True, exist_ok=True)
                with self.path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(stored.to_dict(), sort_keys=True, separators=(",", ":")) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                return stored

    def find_events(self, predicate: WorkflowEventPredicate, *, limit: int = 50) -> list[WorkflowEvent]:
        with _event_file_lock(self.lock_path, exclusive=False):
            events = self._load_all_unlocked()
        return [event for event in events if match_workflow_event(event, predicate)][: max(0, int(limit))]

    def current_version(self) -> int:
        with _event_file_lock(self.lock_path, exclusive=False):
            events = self._load_all_unlocked()
        return events[-1].version if events else 0

    def _load_all_unlocked(self) -> list[WorkflowEvent]:
        if not self.path.exists():
            return []
        out: list[WorkflowEvent] = []
        seen: set[str] = set()
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                event = WorkflowEvent.from_dict(json.loads(line))
            except Exception:
                continue  # append-only fail-safe: corrupt lines do not poison the store.
            if event.event_id in seen:
                continue
            seen.add(event.event_id)
            out.append(event)
        out.sort(key=lambda item: item.version)
        return out


@runtime_checkable
class WorkflowEventNotifier(Protocol):
    def notify(self) -> None:
        ...

    def wait(self, timeout: Optional[float]) -> bool:
        ...


class ThreadWorkflowEventNotifier:
    """Same-process wakeup hint for non-Kanban workflow events."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._counter = 0

    def notify(self) -> None:
        with self._cond:
            self._counter += 1
            self._cond.notify_all()

    def wait(self, timeout: Optional[float]) -> bool:
        with self._cond:
            before = self._counter
            woke = self._cond.wait(timeout)
            return woke or self._counter != before


class FifoWorkflowEventNotifier:
    """Cross-process wakeup hint over one POSIX FIFO.

    This mirrors the Kanban FIFO notifier but uses one queue-wide wakeup channel.
    A producer write wakes an attached consumer promptly; other waiters that miss
    the shared FIFO byte still re-read the durable store on bounded idle wake.
    The durable store remains the source of truth; the FIFO is only a latency
    optimization.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._rfd: Optional[int] = None
        self._hold_wfd: Optional[int] = None

    def notify(self) -> None:
        self._ensure_fifo()
        try:
            fd = os.open(self.path, os.O_WRONLY | os.O_NONBLOCK)
        except OSError:
            return  # no consumer attached; consumers still re-read on bounded idle wake.
        try:
            os.write(fd, b"\x01")
        except OSError:
            pass
        finally:
            os.close(fd)

    def wait(self, timeout: Optional[float]) -> bool:
        self._ensure_reader()
        if self._rfd is None:
            return False
        try:
            readable, _, _ = select.select([self._rfd], [], [], timeout)
        except (OSError, ValueError):
            return False
        if not readable:
            return False
        while True:
            try:
                if not os.read(self._rfd, 4096):
                    break
            except BlockingIOError:
                break
            except OSError:
                break
        return True

    def close(self) -> None:
        for attr in ("_rfd", "_hold_wfd"):
            fd = getattr(self, attr)
            if fd is not None:
                setattr(self, attr, None)
                try:
                    os.close(fd)
                except OSError:
                    pass

    def __del__(self) -> None:
        self.close()

    def _ensure_fifo(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        mkfifo = getattr(os, "mkfifo", None)
        if mkfifo is None:
            raise RuntimeError("FifoWorkflowEventNotifier requires a POSIX platform (os.mkfifo)")
        try:
            mkfifo(self.path)
        except FileExistsError:
            pass

    def _ensure_reader(self) -> None:
        if self._rfd is not None:
            return
        self._ensure_fifo()
        self._rfd = os.open(self.path, os.O_RDONLY | os.O_NONBLOCK)
        try:
            self._hold_wfd = os.open(self.path, os.O_WRONLY | os.O_NONBLOCK)
        except OSError:
            os.close(self._rfd)
            self._rfd = None
            raise


class WorkflowEventBroker:
    """Read durable events and wait event-driven on a notifier between reads."""

    def __init__(self, store: WorkflowEventStore, notifier: Optional[WorkflowEventNotifier] = None) -> None:
        self.store = store
        self.notifier = notifier or ThreadWorkflowEventNotifier()

    def publish(self, event: WorkflowEvent) -> WorkflowEvent:
        stored = self.store.append_event(event)
        self.notifier.notify()
        return stored

    def wait_for(self, predicate: WorkflowEventPredicate | dict[str, Any], *, timeout: Optional[float] = None) -> WorkflowEvent:
        pred = predicate if isinstance(predicate, WorkflowEventPredicate) else WorkflowEventPredicate.from_dict(predicate)
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        checked_once = False
        while True:
            if checked_once and deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("workflow event wait timed out")
            found = self.store.find_events(pred, limit=1)
            if found:
                return found[0]
            checked_once = True
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("workflow event wait timed out")
                wait_s = min(_IDLE_WAIT_S, remaining)
            else:
                wait_s = _IDLE_WAIT_S
            self.notifier.wait(wait_s)


def publish_workflow_event(store: WorkflowEventStore, notifier: WorkflowEventNotifier, event: WorkflowEvent) -> WorkflowEvent:
    """Producer helper: append durably, then notify waiters."""

    stored = store.append_event(event)
    notifier.notify()
    return stored


def workflow_event_from_github_webhook(
    payload: dict[str, Any],
    *,
    headers: Optional[dict[str, Any]] = None,
    delivery_id: Optional[str] = None,
) -> WorkflowEvent:
    """Normalize a GitHub webhook payload into a generic workflow event.

    The event id prefers ``X-GitHub-Delivery`` / ``delivery_id`` for idempotent
    webhook retries. Subject is the most specific durable object in the payload:
    PR/check/deployment/issue when present, otherwise the repository.
    """

    if not isinstance(payload, dict):
        raise ValueError("GitHub webhook payload must be an object")
    safe_headers = {str(k).lower(): v for k, v in (headers or {}).items()}
    gh_event = _github_component(safe_headers.get("x-github-event") or payload.get("event") or "github_event", "x-github-event")
    action = payload.get("action")
    action_part = _github_component(action, "action") if isinstance(action, str) and action else None
    event_type = f"github.{gh_event}"
    if action_part is not None:
        event_type += f".{action_part}"
    event_id = str(delivery_id or safe_headers.get("x-github-delivery") or payload.get("delivery_id") or uuid.uuid4().hex)
    repo = _repo_name(payload)
    subject = _github_subject(repo, payload)
    return WorkflowEvent(
        event_id=_require_safe_text(event_id, "event_id"),
        source="github",
        event_type=event_type,
        subject=subject,
        payload=redact_credentials(
            {
                "repository": repo,
                "action": action_part,
                "sender": _nested(payload, "sender.login"),
                "pull_request": _compact_pr(payload.get("pull_request")),
                "issue": _compact_issue(payload.get("issue")),
                "check_run": _compact_check(payload.get("check_run")),
                "check_suite": _compact_check(payload.get("check_suite")),
                "deployment": _compact_id_state(payload.get("deployment")),
                "deployment_status": _compact_id_state(payload.get("deployment_status")),
            }
        ),
    )


def publish_github_webhook_event(
    store: WorkflowEventStore,
    notifier: WorkflowEventNotifier,
    payload: dict[str, Any],
    *,
    headers: Optional[dict[str, Any]] = None,
    delivery_id: Optional[str] = None,
) -> WorkflowEvent:
    return publish_workflow_event(
        store, notifier, workflow_event_from_github_webhook(payload, headers=headers, delivery_id=delivery_id)
    )


def match_workflow_event(event: WorkflowEvent, predicate: WorkflowEventPredicate) -> bool:
    if event.version <= predicate.after_version:
        return False
    if predicate.source is not None and event.source != predicate.source:
        return False
    if predicate.event_type is not None and event.event_type != predicate.event_type:
        return False
    if predicate.subject is not None and event.subject != predicate.subject:
        return False
    for path, expected in predicate.payload_match.items():
        if _nested(event.payload, str(path)) != expected:
            return False
    return True


def _with_version(event: WorkflowEvent, version: int) -> WorkflowEvent:
    return WorkflowEvent(
        event_id=event.event_id,
        source=event.source,
        event_type=event.event_type,
        subject=event.subject,
        payload=redact_credentials(copy.deepcopy(event.payload)),
        version=version,
        occurred_at=event.occurred_at,
    )


@contextmanager
def _event_file_lock(path: Path, *, exclusive: bool):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        if _fcntl is not None:
            mode = _fcntl.LOCK_EX if exclusive else _fcntl.LOCK_SH
            _fcntl.flock(handle.fileno(), mode)
        try:
            yield
        finally:
            if _fcntl is not None:
                _fcntl.flock(handle.fileno(), _fcntl.LOCK_UN)


def _require_safe_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    text = value.strip()
    if _SAFE_ID_RE.fullmatch(text) is None:
        raise ValueError(f"{field_name} contains unsafe characters: {value!r}")
    return text


def _opt_safe_text(value: Any, field_name: str) -> Optional[str]:
    if value is None:
        return None
    return _require_safe_text(value, field_name)


def _github_component(value: Any, field_name: str) -> str:
    text = _require_safe_text(str(value), field_name).lower()
    # Re-check after lowercasing mostly for clarity; `_require_safe_text` already
    # excluded whitespace, slashes, shell-ish punctuation, and path separators.
    return _require_safe_text(text, field_name)


def _require_subject(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("subject must be a non-empty string")
    subject = value.strip()
    if len(subject) > 512 or "\x00" in subject:
        raise ValueError("subject is too large or contains NUL")
    return subject


def _opt_subject(value: Any) -> Optional[str]:
    if value is None:
        return None
    return _require_subject(value)


def _nested(payload: Any, dotted: str) -> Any:
    current = payload
    for part in dotted.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _repo_name(payload: dict[str, Any]) -> str:
    repo = payload.get("repository")
    if isinstance(repo, dict):
        full = repo.get("full_name")
        if isinstance(full, str) and full:
            return full
    return "unknown/repository"


def _github_subject(repo: str, payload: dict[str, Any]) -> str:
    for key, prefix in (
        ("pull_request", "pull"),
        ("issue", "issue"),
        ("check_run", "check_run"),
        ("check_suite", "check_suite"),
        ("deployment_status", "deployment_status"),
        ("deployment", "deployment"),
    ):
        value = payload.get(key)
        if isinstance(value, dict):
            identifier = value.get("number") or value.get("id")
            if identifier is not None:
                return f"github:{repo}:{prefix}:{identifier}"
    return f"github:{repo}"


def _compact_pr(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    return {
        "number": value.get("number"),
        "state": value.get("state"),
        "merged": value.get("merged"),
        "head_sha": _nested(value, "head.sha"),
        "base_ref": _nested(value, "base.ref"),
        "html_url": value.get("html_url"),
    }


def _compact_issue(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    return {"number": value.get("number"), "state": value.get("state"), "html_url": value.get("html_url")}


def _compact_check(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    return {
        "id": value.get("id"),
        "name": value.get("name"),
        "status": value.get("status"),
        "conclusion": value.get("conclusion"),
        "head_sha": value.get("head_sha") or _nested(value, "head_commit.id"),
        "pull_requests": value.get("pull_requests") if isinstance(value.get("pull_requests"), list) else None,
    }


def _compact_id_state(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    return {"id": value.get("id"), "state": value.get("state"), "environment": value.get("environment")}
