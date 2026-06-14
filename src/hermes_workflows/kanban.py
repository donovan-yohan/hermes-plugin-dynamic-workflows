"""Parent-owned Kanban backend for the ``kanban_agent`` durable awaitable (issue #5).

A workflow script reaches a named Hermes profile through Kanban by awaiting
``kanban_agent(...)``. The parent broker (:mod:`hermes_workflows.vm`) — never the
sandboxed subprocess — turns that single RPC call into a durable Kanban card and
**blocks until the card reaches a terminal state**, then hands the structured
result back to the script. The subprocess has no Kanban authority and never
writes to a board directly; this is the same trust boundary the VM applies to
every other capability.

Two properties make this a *durable awaitable* rather than a fire-and-forget call:

* **Idempotency.** The broker derives a stable idempotency key per logical call
  (the workflow's logical run id + the stable RPC call id, both reproducible
  across a replay — see :mod:`hermes_workflows.script_store`). A card is created
  on first sight of a key and **re-attached** on every later sight, so re-running
  or replaying a workflow never opens a duplicate card for the same step.
* **Event-driven resolution.** The await is woken by a Kanban *event* (a card
  transitioning to completed / blocked / failed), not by polling the board on a
  timer. :class:`InMemoryKanbanBackend` models this with a
  :class:`threading.Condition`; a production backend wires the same interface to
  a durable subscription (Postgres ``LISTEN/NOTIFY``, a message-broker topic, …).

``on_block`` policy (governs *blocked* cards only — completed always returns and
failed is surfaced as a structured ``status="failed"`` result):

* ``"return"`` (default) — resolve the await with the blocked resolution as a
  structured ``status="blocked"`` result so the script can branch on it.
* ``"raise"`` — raise into the script (a catchable ``CapabilityError``).
* ``"pause"`` — do **not** resolve on a blocked event; keep awaiting until the
  card reaches a terminal completed/failed state (e.g. a human unblocks it). This
  is an *in-process* await bounded by the run's wall-clock limit; cross-process
  durable suspend/resume (persisting the suspended run and resuming it in a later
  process from a replayed event) is **not** implemented in this slice — see
  "Production integration" below.

------------------------------------------------------------------------------
Production integration (what :class:`InMemoryKanbanBackend` deliberately fakes)
------------------------------------------------------------------------------
:class:`InMemoryKanbanBackend` is an **honest in-memory fake** for tests and
local dev. It is NOT production. A real backend implementing :class:`KanbanBackend`
must replace, specifically:

* ``create_or_reattach`` — create/reattach a card through the real Kanban DB/API
  using the idempotency key as the unique key (so concurrent parents and replays
  converge on one card), honouring board/tenant/parents/labels/workspace and the
  assignee *profile* registry (rejecting unknown profiles with a diagnostic).
* ``await_resolution`` — subscribe to durable card events and resolve from them;
  must survive a parent restart (re-attach + replay missed events), which the
  in-memory fake cannot. The dispatcher boundary ("do not run a duplicate
  dispatcher") is the production backend's responsibility: the workflow only
  creates/waits; gateway dispatch claims and executes the work.
* ``on_block="pause"`` — for a true durable pause, persist the suspended run and
  resume it from a later event in a fresh process rather than holding a thread.

This module is pure Python 3.11 stdlib.
"""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Protocol, runtime_checkable

from .registry import utc_now_iso

__all__ = [
    "ON_BLOCK_POLICIES",
    "DEFAULT_ON_BLOCK",
    "CARD_COMPLETED",
    "CARD_BLOCKED",
    "CARD_FAILED",
    "TERMINAL_STATES",
    "KanbanCardSpec",
    "KanbanCard",
    "KanbanResolution",
    "KanbanBackend",
    "InMemoryKanbanBackend",
    "KanbanError",
    "KanbanBlocked",
    "KanbanUnknownProfile",
    "KanbanTimeout",
    "kanban_card_id",
    "normalize_on_block",
]

# Card lifecycle states the awaitable resolves on.
CARD_COMPLETED = "completed"
CARD_BLOCKED = "blocked"
CARD_FAILED = "failed"
TERMINAL_STATES = frozenset({CARD_COMPLETED, CARD_FAILED})

# on_block policies for a blocked card (see module docstring).
ON_BLOCK_POLICIES = frozenset({"pause", "raise", "return"})
DEFAULT_ON_BLOCK = "return"


class KanbanError(RuntimeError):
    """Base class for Kanban backend failures the broker translates to denials."""


class KanbanUnknownProfile(KanbanError):
    """The requested assignee ``profile`` is not a known Hermes profile."""

    def __init__(self, profile: str) -> None:
        self.profile = profile
        super().__init__(f"unknown Kanban profile {profile!r}")


class KanbanTimeout(KanbanError):
    """The card did not reach an accepted resolution within the await window."""

    def __init__(self, card_id: str, timeout: Optional[float]) -> None:
        self.card_id = card_id
        self.timeout = timeout
        super().__init__(f"kanban card {card_id!r} did not resolve within {timeout}s")


class KanbanBlocked(KanbanError):
    """A card resolved to ``blocked`` under ``on_block="raise"``."""

    def __init__(self, resolution: "KanbanResolution") -> None:
        self.resolution = resolution
        reason = resolution.reason or "card blocked"
        super().__init__(f"kanban card {resolution.card_id!r} blocked: {reason}")


def normalize_on_block(value: Any) -> str:
    """Return a valid ``on_block`` policy or raise ``ValueError``.

    ``None`` defaults to :data:`DEFAULT_ON_BLOCK`. The broker turns the
    ``ValueError`` into a structured ``bad_request`` denial for the script.
    """
    if value is None:
        return DEFAULT_ON_BLOCK
    if value in ON_BLOCK_POLICIES:
        return value
    raise ValueError(
        f"on_block={value!r} is not one of {sorted(ON_BLOCK_POLICIES)}"
    )


def kanban_card_id(idempotency_key: str) -> str:
    """Deterministic, content-addressed card id for an idempotency key.

    Deriving the id from the key (rather than a counter or uuid) means a replay
    that reattaches computes the *same* id without consulting any shared counter,
    keeping card ids stable and reproducible across runs.
    """
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:16]
    return f"kbc_{digest}"


@dataclass(frozen=True)
class KanbanCardSpec:
    """The parent-visible specification for one ``kanban_agent`` call.

    Carries everything the backend needs to create a card; all raw fields stay
    parent-side and the subprocess never writes them to Kanban itself.
    """

    profile: str
    title: Optional[str] = None
    prompt: Optional[str] = None
    context: dict[str, Any] = field(default_factory=dict)
    task: dict[str, Any] = field(default_factory=dict)
    input: dict[str, Any] = field(default_factory=dict)
    board: Optional[str] = None
    tenant: Optional[str] = None
    parents: tuple[str, ...] = ()
    labels: tuple[str, ...] = ()
    workspace: Optional[dict[str, Any]] = None
    schema: Optional[dict[str, Any]] = None


@dataclass(frozen=True)
class KanbanCard:
    """A created-or-reattached card handle returned by the backend."""

    card_id: str
    profile: str
    reattached: bool = False
    created_at: str = ""


@dataclass(frozen=True)
class KanbanResolution:
    """A terminal (or, under ``return``/``raise``, blocked) card outcome."""

    card_id: str
    profile: str
    status: str  # completed | blocked | failed
    result: dict[str, Any] = field(default_factory=dict)
    reason: Optional[str] = None


@runtime_checkable
class KanbanBackend(Protocol):
    """Parent-owned Kanban access the broker drives for ``kanban_agent``.

    A backend creates/reattaches a card by idempotency key and resolves a card by
    awaiting its durable events. Implementations must be safe to call from the
    broker thread and must never let a replay create a duplicate card for a key
    already seen.
    """

    def create_or_reattach(self, idempotency_key: str, spec: KanbanCardSpec) -> KanbanCard:
        """Create a card for ``idempotency_key`` or reattach to an existing one.

        Raises :class:`KanbanUnknownProfile` for an unrecognised assignee.
        """
        ...

    def await_resolution(
        self, card_id: str, *, accept_blocked: bool, timeout: Optional[float]
    ) -> KanbanResolution:
        """Block until ``card_id`` reaches an accepted resolution.

        Resolves on a terminal completed/failed event always, and on a blocked
        event only when ``accept_blocked`` (i.e. ``on_block`` is not ``"pause"``).
        Raises :class:`KanbanTimeout` if no accepted resolution arrives in time.
        """
        ...


class InMemoryKanbanBackend:
    """Honest in-memory, event-driven :class:`KanbanBackend` for tests/local dev.

    NOT production (see the module docstring's "Production integration"). It
    models the three properties the broker relies on:

    * **idempotent create/reattach** — an ``idempotency_key -> card_id`` map, so a
      second call with the same key reattaches instead of opening a new card;
    * **event-driven await** — :meth:`await_resolution` blocks on a
      :class:`threading.Condition` and is woken by :meth:`resolve`, never polling;
    * **programmable resolution** — ``auto`` resolves new cards immediately (handy
      for end-to-end runs), or leave ``auto=None`` and drive cards with
      :meth:`resolve` to exercise blocked/pause/wakeup paths.

    ``known_profiles`` (if given) is the allow-list; any profile outside it raises
    :class:`KanbanUnknownProfile`. ``unknown_profiles`` is an explicit deny-list
    overlaid on top, for testing rejection without enumerating a whole roster.
    """

    def __init__(
        self,
        *,
        auto: Any = None,
        known_profiles: Optional[set[str]] = None,
        unknown_profiles: frozenset[str] = frozenset(),
    ) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._by_idem: dict[str, str] = {}
        self._cards: dict[str, KanbanCard] = {}
        self._specs: dict[str, KanbanCardSpec] = {}
        self._resolutions: dict[str, KanbanResolution] = {}
        self._auto = auto
        self._known = set(known_profiles) if known_profiles is not None else None
        self._unknown = unknown_profiles
        # Audit surfaces for tests.
        self.created_cards: list[str] = []
        self.reattachments = 0

    # -- profile policy ---------------------------------------------------
    def _check_profile(self, profile: str) -> None:
        if not isinstance(profile, str) or not profile:
            raise KanbanUnknownProfile(str(profile))
        if profile in self._unknown:
            raise KanbanUnknownProfile(profile)
        if self._known is not None and profile not in self._known:
            raise KanbanUnknownProfile(profile)

    # -- create / reattach ------------------------------------------------
    def create_or_reattach(self, idempotency_key: str, spec: KanbanCardSpec) -> KanbanCard:
        self._check_profile(spec.profile)
        with self._cond:
            existing = self._by_idem.get(idempotency_key)
            if existing is not None:
                self.reattachments += 1
                card = self._cards[existing]
                # Same card, marked as a reattachment for the caller/journal.
                return KanbanCard(
                    card_id=card.card_id,
                    profile=card.profile,
                    reattached=True,
                    created_at=card.created_at,
                )
            card_id = kanban_card_id(idempotency_key)
            card = KanbanCard(
                card_id=card_id, profile=spec.profile, reattached=False, created_at=utc_now_iso()
            )
            self._by_idem[idempotency_key] = card_id
            self._cards[card_id] = card
            self._specs[card_id] = spec
            self.created_cards.append(card_id)
            auto = self._auto_resolution(card_id, spec)
            if auto is not None:
                self._resolutions[card_id] = auto
                self._cond.notify_all()
            return card

    def _auto_resolution(self, card_id: str, spec: KanbanCardSpec) -> Optional[KanbanResolution]:
        """Resolve a freshly-created card immediately, if ``auto`` is configured."""
        if self._auto is None:
            return None
        if callable(self._auto):
            return self._coerce_resolution(card_id, spec, self._auto(spec))
        # A bare status string: completed/blocked/failed with a derived result.
        return self._coerce_resolution(card_id, spec, self._auto)

    def _coerce_resolution(self, card_id: str, spec: KanbanCardSpec, value: Any) -> KanbanResolution:
        if isinstance(value, KanbanResolution):
            return value
        if isinstance(value, str):
            result: dict[str, Any] = {}
            if value == CARD_COMPLETED:
                # A deterministic echo so end-to-end runs can assert on output.
                result = {"echo": dict(spec.context or spec.task or spec.input or {})}
            return KanbanResolution(
                card_id=card_id, profile=spec.profile, status=value, result=result
            )
        if isinstance(value, dict):
            return KanbanResolution(
                card_id=card_id,
                profile=spec.profile,
                status=str(value.get("status", CARD_COMPLETED)),
                result=value.get("result") if isinstance(value.get("result"), dict) else {},
                reason=value.get("reason"),
            )
        raise TypeError(f"cannot coerce {type(value).__name__} to a KanbanResolution")

    # -- await ------------------------------------------------------------
    def await_resolution(
        self, card_id: str, *, accept_blocked: bool, timeout: Optional[float]
    ) -> KanbanResolution:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while True:
                resolution = self._resolutions.get(card_id)
                if resolution is not None and self._is_accepted(resolution, accept_blocked):
                    return resolution
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise KanbanTimeout(card_id, timeout)
                # Event-driven wakeup: block until resolve() notifies (or the
                # bounded wait elapses), never a polling sleep-loop.
                self._cond.wait(remaining)

    @staticmethod
    def _is_accepted(resolution: KanbanResolution, accept_blocked: bool) -> bool:
        if resolution.status in TERMINAL_STATES:
            return True
        if resolution.status == CARD_BLOCKED:
            return accept_blocked
        return False

    # -- inspection / test driver surface --------------------------------
    def spec_for(self, card_id: str) -> Optional[KanbanCardSpec]:
        """Return the spec a card was created with (inspection/test helper)."""
        with self._lock:
            return self._specs.get(card_id)

    def resolve(
        self,
        card_id: str,
        status: str,
        *,
        result: Optional[dict[str, Any]] = None,
        reason: Optional[str] = None,
    ) -> None:
        """Publish a card event (the durable-event analogue) and wake awaiters."""
        with self._cond:
            card = self._cards.get(card_id)
            profile = card.profile if card is not None else ""
            self._resolutions[card_id] = KanbanResolution(
                card_id=card_id,
                profile=profile,
                status=status,
                result=result or {},
                reason=reason,
            )
            self._cond.notify_all()
