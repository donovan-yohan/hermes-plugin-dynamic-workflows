"""Cross-process event notification for durable Kanban awaits (issue #5).

The durable event log (:mod:`hermes_workflows.script_store`) lets a parent *replay*
events that already exist, but a parent **blocking** on a not-yet-produced event
from another process needs a live wakeup — the log is not itself a cross-process
notification. This module supplies that wakeup as a small, swappable seam plus a
backend that resolves a card purely from the durable log, woken event-driven:

* :class:`KanbanEventNotifier` — ``notify(card_id)`` (a producer signals a card
  update) and ``subscribe(card_id)`` (a consumer gets an :class:`EventSubscription`
  whose ``wait(timeout)`` blocks until signalled or the timeout). Production wires
  this to Postgres ``LISTEN/NOTIFY`` or a broker topic.
* :class:`ThreadEventNotifier` — the in-process default (a per-card
  :class:`threading.Condition`); same-process producer/consumer, portable.
* :class:`FifoEventNotifier` — a **cross-process** implementation over per-card
  POSIX FIFOs (``os.mkfifo`` + ``select``). Unix-only.
* :class:`EventLogKanbanBackend` — a :class:`~hermes_workflows.kanban.KanbanBackend`
  that resolves a card from the durable event log (no in-memory event source),
  blocking on the notifier between log reads and bounded by the run deadline. This
  is the production-shaped path (durable Kanban DB + ``LISTEN/NOTIFY`` analog).
* :func:`publish_kanban_event` — the producer helper: append the durable event,
  then notify.

**The notifier is a wakeup hint; the durable log is the source of truth.** A
missed or raced signal degrades to the consumer's bounded re-read of the log, so
an event is never lost — at worst it is observed a little later. The remaining
production residual is a real notification transport (this ships an in-process and
a single-host FIFO transport); cross-host needs ``LISTEN/NOTIFY`` / a broker.

This module is pure Python 3.11 stdlib.
"""

from __future__ import annotations

import os
import re
import select
import threading
import time
from pathlib import Path
from typing import Any, Optional, Protocol, runtime_checkable

from .kanban import (
    KanbanCard,
    KanbanCardSpec,
    KanbanTimeout,
    KanbanUnknownProfile,
    _resolution_to_state,
    _state_to_resolution,
    is_accepted_resolution,
    kanban_card_id,
)
from .kanban import KanbanResolution
from .registry import utc_now_iso

__all__ = [
    "KanbanEventNotifier",
    "EventSubscription",
    "ThreadEventNotifier",
    "FifoEventNotifier",
    "EventLogKanbanBackend",
    "publish_kanban_event",
]

# A card id is used as a filesystem segment for the FIFO; mirror the store guard.
_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

# Upper bound on a single blocking wait when no deadline is given, so a missed
# signal still leads to a log re-read rather than an unbounded block.
_IDLE_WAIT_S = 1.0


def _require_safe_segment(card_id: str) -> None:
    if not isinstance(card_id, str) or not _SAFE_SEGMENT_RE.fullmatch(card_id):
        raise ValueError(f"unsafe card_id: {card_id!r}")


@runtime_checkable
class EventSubscription(Protocol):
    """A consumer's handle for one card's wakeups; close when done."""

    def wait(self, timeout: Optional[float]) -> bool:
        """Block until notified or ``timeout`` elapses; return whether woken."""
        ...

    def close(self) -> None:
        ...


@runtime_checkable
class KanbanEventNotifier(Protocol):
    """Wakeup seam: a producer ``notify``s a card; a consumer ``subscribe``s."""

    def notify(self, card_id: str) -> None:
        ...

    def subscribe(self, card_id: str) -> EventSubscription:
        ...


# --------------------------------------------------------------------------- #
# In-process notifier (default, portable)
# --------------------------------------------------------------------------- #

class _ThreadSubscription:
    def __init__(self, cond: threading.Condition, counter: list[int]) -> None:
        self._cond = cond
        self._counter = counter
        with cond:
            self._seen = counter[0]

    def wait(self, timeout: Optional[float]) -> bool:
        with self._cond:
            if self._counter[0] != self._seen:
                self._seen = self._counter[0]
                return True  # a notify raced in before we waited.
            woke = self._cond.wait(timeout)
            self._seen = self._counter[0]
            return woke

    def close(self) -> None:  # nothing to release.
        pass


class ThreadEventNotifier:
    """In-process :class:`KanbanEventNotifier` over per-card conditions.

    Same-process producer/consumer only (a thread publishing while another awaits).
    Cross-process wakeup needs :class:`FifoEventNotifier` or a production transport.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cards: dict[str, tuple[threading.Condition, list[int]]] = {}

    def _entry(self, card_id: str) -> tuple[threading.Condition, list[int]]:
        with self._lock:
            entry = self._cards.get(card_id)
            if entry is None:
                entry = (threading.Condition(), [0])
                self._cards[card_id] = entry
            return entry

    def notify(self, card_id: str) -> None:
        cond, counter = self._entry(card_id)
        with cond:
            counter[0] += 1
            cond.notify_all()

    def subscribe(self, card_id: str) -> EventSubscription:
        cond, counter = self._entry(card_id)
        return _ThreadSubscription(cond, counter)


# --------------------------------------------------------------------------- #
# Cross-process notifier (POSIX FIFO)
# --------------------------------------------------------------------------- #

class _FifoSubscription:
    def __init__(self, rfd: int, wfd: Optional[int]) -> None:
        # The write end is held only so the read end never sees EOF (which select
        # reports as readable) when no producer happens to be attached — otherwise
        # the consumer would busy-spin. We never write to it ourselves.
        self._rfd: Optional[int] = rfd
        self._wfd = wfd

    def wait(self, timeout: Optional[float]) -> bool:
        if self._rfd is None:
            return False
        try:
            readable, _, _ = select.select([self._rfd], [], [], timeout)
        except (OSError, ValueError):
            return False
        if not readable:
            return False
        while True:  # drain the wakeup bytes so the next wait blocks again.
            try:
                if not os.read(self._rfd, 4096):
                    break
            except BlockingIOError:
                break
            except OSError:
                break
        return True

    def close(self) -> None:
        if self._rfd is not None:
            fd = self._rfd
            self._rfd = None
            try:
                os.close(fd)
            except OSError:
                pass
        if self._wfd is not None:
            fd = self._wfd
            self._wfd = None
            try:
                os.close(fd)
            except OSError:
                pass

    def __del__(self) -> None:
        self.close()


class FifoEventNotifier:
    """Cross-process :class:`KanbanEventNotifier` over per-card POSIX FIFOs.

    A producer writes a byte to ``<dir>/<card_id>.notify``; a consumer holds the
    FIFO read end (plus a write end, so the read end never EOFs) and ``select``s on
    it. Unix-only (``os.mkfifo``). The FIFO is a wakeup hint only — pair it with the
    durable event log, which is the source of truth — so a raced signal (no reader
    attached when the producer writes) is harmless: the consumer reads the log on
    its next wake/timeout.
    """

    def __init__(self, directory: str | Path) -> None:
        self._dir = Path(directory)

    def _path(self, card_id: str) -> Path:
        _require_safe_segment(card_id)
        return self._dir / f"{card_id}.notify"

    def _ensure(self, path: Path) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        mkfifo = getattr(os, "mkfifo", None)
        if mkfifo is None:  # non-POSIX platform.
            raise RuntimeError("FifoEventNotifier requires a POSIX platform (os.mkfifo)")
        try:
            mkfifo(path)
        except FileExistsError:
            pass

    def notify(self, card_id: str) -> None:
        path = self._path(card_id)
        self._ensure(path)
        try:
            fd = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
        except OSError:
            return  # ENXIO: no consumer attached; it will read the durable log.
        try:
            os.write(fd, b"\x01")
        except OSError:
            pass
        finally:
            os.close(fd)

    def subscribe(self, card_id: str) -> EventSubscription:
        path = self._path(card_id)
        self._ensure(path)
        rfd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        try:
            # Hold a write end so the read end never EOFs (a reader-only FIFO with
            # no writer is reported readable by select, busy-spinning wait()). This
            # is load-bearing, so fail the subscribe rather than degrade to a
            # spin-prone subscription if it cannot be opened (only under fd
            # exhaustion, since rfd is already open on the same FIFO).
            wfd = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
        except OSError:
            os.close(rfd)
            raise
        return _FifoSubscription(rfd, wfd)


# --------------------------------------------------------------------------- #
# Backend: resolve from the durable log, woken by the notifier
# --------------------------------------------------------------------------- #

_RESOLUTION_STATUSES = frozenset({"completed", "blocked", "failed"})


def publish_kanban_event(
    store: Any,
    notifier: KanbanEventNotifier,
    card_id: str,
    *,
    status: str,
    result: Optional[dict[str, Any]] = None,
    reason: Optional[str] = None,
    profile: str = "",
) -> dict[str, Any]:
    """Producer seam: durably append a card event, then signal a wakeup.

    The append is the source of truth (it survives a restart); the notify is a
    best-effort wakeup for any consumer currently blocked on the card.
    """
    record = store.append_kanban_event(
        card_id, status=status, result=result, reason=reason, profile=profile
    )
    notifier.notify(card_id)
    return record


def has_kanban_history(store: Any, card_id: str) -> bool:
    """Best-effort check for any durable state/event history for ``card_id``.

    Custom stores may implement only the latest-state index or only the event log.
    Prefer the latest-state lookup because it is cheap for ``ScriptRunStore``;
    fall back to reading events only when no state marker is present.
    """
    load_state = getattr(store, "load_kanban_card_state", None)
    if callable(load_state):
        try:
            if load_state(card_id) is not None:
                return True
        except OSError:
            pass

    read_events = getattr(store, "read_kanban_events", None)
    if not callable(read_events):
        return False
    try:
        return bool(read_events(card_id))
    except OSError:
        return False


class EventLogKanbanBackend:
    """A :class:`~hermes_workflows.kanban.KanbanBackend` resolving a card from the
    durable event log, woken event-driven by a :class:`KanbanEventNotifier`.

    Unlike :class:`~hermes_workflows.kanban.InMemoryKanbanBackend`, there is **no
    in-memory event source**: a card resolves only from events a producer durably
    appended (via :func:`publish_kanban_event`), so the producer may be a different
    process — the production-shaped path. The await reads the log, and between reads
    blocks on the notifier (never a polling sleep-loop), bounded by ``timeout`` (the
    run's wall-clock deadline). ``after_version`` is the log line position already
    consumed, so the broker's pause-retry waits for a strictly newer event in the
    log's own (single) version space.
    """

    def __init__(
        self,
        store: Any,
        notifier: KanbanEventNotifier,
        *,
        known_profiles: Optional[set[str]] = None,
        unknown_profiles: frozenset[str] = frozenset(),
    ) -> None:
        self._store = store
        self._notifier = notifier
        self._known = set(known_profiles) if known_profiles is not None else None
        self._unknown = unknown_profiles

    def _check_profile(self, profile: str) -> None:
        if not isinstance(profile, str) or not profile:
            raise KanbanUnknownProfile(str(profile))
        if profile in self._unknown:
            raise KanbanUnknownProfile(profile)
        if self._known is not None and profile not in self._known:
            raise KanbanUnknownProfile(profile)

    def create_or_reattach(self, idempotency_key: str, spec: KanbanCardSpec) -> KanbanCard:
        self._check_profile(spec.profile)
        card_id = kanban_card_id(idempotency_key)
        reattached = self._has_history(card_id)
        if not reattached:
            try:
                self._store.record_kanban_card_state(
                    card_id,
                    {"card_id": card_id, "status": "waiting", "profile": spec.profile, "version": 0},
                )
            except OSError:  # the waiting marker is a best-effort operator view.
                pass
        return KanbanCard(
            card_id=card_id, profile=spec.profile, reattached=reattached, created_at=utc_now_iso()
        )

    def _has_history(self, card_id: str) -> bool:
        return has_kanban_history(self._store, card_id)

    def await_resolution(
        self,
        card_id: str,
        *,
        accept_blocked: bool,
        timeout: Optional[float],
        after_version: int = 0,
    ) -> KanbanResolution:
        deadline = None if timeout is None else time.monotonic() + timeout
        # Subscribe before the first read so a notify that races in between the read
        # and the wait is not lost (it is buffered and the next wait returns at once).
        sub = self._notifier.subscribe(card_id)
        try:
            while True:
                resolution = self._latest_accepted(card_id, after_version, accept_blocked)
                if resolution is not None:
                    # Mirror the outcome to the latest-state index so kanban_waits()
                    # stops reporting this card as in-flight (matches the durable
                    # path; best-effort — the log remains the source of truth).
                    try:
                        self._store.record_kanban_card_state(card_id, _resolution_to_state(resolution))
                    except OSError:
                        pass
                    return resolution
                if deadline is None:
                    # No deadline given: block in bounded backstops (re-reading the
                    # durable log each wake) rather than forever, in case a signal
                    # was somehow missed. The broker always passes a timeout.
                    remaining: Optional[float] = _IDLE_WAIT_S
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise KanbanTimeout(card_id, timeout)
                # Wait the full remaining window: the wakeup is event-driven (the
                # notifier), and subscribing before the first read means a signal
                # that races in is buffered and observed on the next wait — so this
                # is not a poll loop.
                sub.wait(remaining)
        finally:
            sub.close()

    def _latest_accepted(
        self, card_id: str, after_version: int, accept_blocked: bool
    ) -> Optional[KanbanResolution]:
        """The latest accepted resolution in the log past ``after_version`` (line pos)."""
        latest: Optional[KanbanResolution] = None
        try:
            events = self._store.read_kanban_events(card_id, after_seq=after_version)
        except OSError:
            return None
        for event in events:
            if event.get("status") not in _RESOLUTION_STATUSES:
                continue
            resolution = _state_to_resolution({**event, "version": event.get("seq", 0)})
            if resolution is not None and is_accepted_resolution(resolution, accept_blocked):
                latest = resolution
        return latest
