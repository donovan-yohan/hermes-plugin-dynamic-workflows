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
from dataclasses import dataclass, field, replace
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
    "DurableKanbanBackend",
    "KanbanWaitStore",
    "is_accepted_resolution",
    "KanbanError",
    "KanbanBlocked",
    "KanbanUnknownProfile",
    "KanbanTimeout",
    "kanban_card_id",
    "normalize_on_block",
    "WORKFLOW_RESULT_KEY",
    "validate_workflow_result",
    "result_contract_instruction",
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


# The worker completes a card by setting this key in the card's completion
# metadata (``metadata.workflow_result``). The runtime validates that payload —
# not the card's free-text body — before resolving the awaitable, so a workflow
# branches on a typed object and never on prose. (Issue #6.)
WORKFLOW_RESULT_KEY = "workflow_result"

# Result-schema type table for the workflow result contract. Mirrors the broker's
# brokered-output table so ``schema=`` means the same thing for an ``agent`` output
# and a ``kanban_agent`` workflow_result.
_RESULT_TYPE_MAP: dict[str, tuple[type, ...]] = {
    "string": (str,), "str": (str,), "number": (int, float), "int": (int,),
    "integer": (int,), "float": (float,), "bool": (bool,), "boolean": (bool,),
    "object": (dict,), "dict": (dict,), "list": (list,), "array": (list,), "any": (object,),
}


def validate_workflow_result(payload: Any, schema: Optional[dict[str, Any]]) -> list[str]:
    """Validate a worker's structured ``workflow_result`` against the call schema.

    Returns a list of human-readable diagnostics; an empty list means valid.

    The contract is deliberately a *template-guided payload schema over a stable
    envelope*, not one global workflow schema:

    * **No schema** → always valid. Unknown payloads are preserved as-is, so a
      repo/agent template may hand back any shape it likes.
    * **With schema** → every declared ``field -> type`` must be present with the
      declared type. **Extra** fields are preserved, not rejected, so a template
      can define a stricter-than-declared shape without tripping the contract.

    A non-dict payload (e.g. the worker completed with prose / nothing) is a
    contract violation, not a silent success — the broker turns a non-empty
    diagnostics list into a deterministic block/fail.
    """
    if not schema:
        return []
    if not isinstance(payload, dict):
        kind = type(payload).__name__
        return [f"workflow_result is missing or not an object (got {kind})"]
    diagnostics: list[str] = []
    for field_name, hint in schema.items():
        if field_name not in payload:
            diagnostics.append(f"missing required field {field_name!r}")
            continue
        expected = (hint,) if isinstance(hint, type) else _RESULT_TYPE_MAP.get(str(hint).lower())
        if expected is None:
            continue  # unknown type hint: leniently accept (matches brokered-output policy).
        value = payload[field_name]
        # bool is an int subclass, so reject it for a numeric/text field — but
        # allow it where bool is explicitly expected or the field is permissive
        # (``any``/``object``, i.e. ``object in expected``), so a valid bool result
        # is not wrongly rejected and turned into a contract violation.
        if isinstance(value, bool) and bool not in expected and object not in expected:
            diagnostics.append(f"field {field_name!r} expected {hint}, got bool")
        elif not isinstance(value, expected):
            diagnostics.append(f"field {field_name!r} expected {hint}, got {type(value).__name__}")
    return diagnostics


def result_contract_instruction(schema: Optional[dict[str, Any]]) -> str:
    """Render the worker-facing instruction a card body carries when ``schema`` is set.

    Empty string when there is no schema (the worker may complete with any
    payload). Otherwise it tells the worker to complete with a
    ``metadata.workflow_result`` matching the schema and to *block* — not complete
    with prose — if it cannot, which is exactly what the runtime enforces.
    """
    if not schema:
        return ""
    fields = ", ".join(
        f"{name}: {hint.__name__ if isinstance(hint, type) else hint}"
        for name, hint in schema.items()
    )
    return (
        f"Complete this card by setting metadata.{WORKFLOW_RESULT_KEY} to a JSON "
        f"object matching this schema: {{{fields}}}. Do not complete with prose. "
        "If you cannot produce the structured result, block the card with a reason."
    )


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
    """A terminal (or, under ``return``/``raise``, blocked) card outcome.

    ``result`` is the worker's structured ``metadata.workflow_result`` payload —
    what the runtime validates against the call's result schema. ``version`` is a
    per-card monotonic event sequence (assigned by the backend) so an await can
    block for a *newer* event than one it already rejected (the retry/unblock
    path after a failed result-contract check).
    """

    card_id: str
    profile: str
    status: str  # completed | blocked | failed
    result: dict[str, Any] = field(default_factory=dict)
    reason: Optional[str] = None
    version: int = 0


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
        self,
        card_id: str,
        *,
        accept_blocked: bool,
        timeout: Optional[float],
        after_version: int = 0,
    ) -> KanbanResolution:
        """Block until ``card_id`` reaches an accepted resolution.

        Resolves on a terminal completed/failed event always, and on a blocked
        event only when ``accept_blocked`` (i.e. ``on_block`` is not ``"pause"``).
        Only events with ``version > after_version`` count, so the broker can wait
        for a *newer* event after rejecting one whose ``workflow_result`` failed
        the result contract (retry/unblock). Raises :class:`KanbanTimeout` if no
        accepted resolution arrives in time.

        Optional hook: a backend MAY also implement
        ``record_event(card_id, kind, detail)`` to attach a comment/event to the
        card (the broker uses it to surface result-validation diagnostics on the
        board); it is called only if present.
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
        self._instructions: dict[str, str] = {}
        self._resolutions: dict[str, KanbanResolution] = {}
        self._seq = 0  # monotonic event sequence for resolution versioning.
        self._auto = auto
        self._known = set(known_profiles) if known_profiles is not None else None
        self._unknown = unknown_profiles
        # Audit surfaces for tests.
        self.created_cards: list[str] = []
        self.reattachments = 0
        self.events: list[dict[str, Any]] = []  # card comments/events (e.g. result_invalid).

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
            # When a schema is set, the card body instructs the worker to complete
            # with a matching metadata.workflow_result (production renders this into
            # the real card; the fake records it for inspection).
            self._instructions[card_id] = result_contract_instruction(spec.schema)
            self.created_cards.append(card_id)
            auto = self._auto_resolution(card_id, spec)
            if auto is not None:
                self._publish(auto)
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

    def _publish(self, resolution: KanbanResolution) -> None:
        """Version, store, and announce a card event (caller holds the lock)."""
        self._seq += 1
        self._resolutions[resolution.card_id] = replace(resolution, version=self._seq)
        self._cond.notify_all()

    # -- await ------------------------------------------------------------
    def await_resolution(
        self,
        card_id: str,
        *,
        accept_blocked: bool,
        timeout: Optional[float],
        after_version: int = 0,
    ) -> KanbanResolution:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while True:
                resolution = self._resolutions.get(card_id)
                if (
                    resolution is not None
                    and resolution.version > after_version
                    and self._is_accepted(resolution, accept_blocked)
                ):
                    return resolution
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise KanbanTimeout(card_id, timeout)
                # Event-driven wakeup: block until resolve() notifies (or the
                # bounded wait elapses), never a polling sleep-loop.
                self._cond.wait(remaining)

    @staticmethod
    def _is_accepted(resolution: KanbanResolution, accept_blocked: bool) -> bool:
        return is_accepted_resolution(resolution, accept_blocked)

    # -- inspection / test driver surface --------------------------------
    def spec_for(self, card_id: str) -> Optional[KanbanCardSpec]:
        """Return the spec a card was created with (inspection/test helper)."""
        with self._lock:
            return self._specs.get(card_id)

    def instruction_for(self, card_id: str) -> str:
        """Return the worker-facing result-contract instruction on a card body."""
        with self._lock:
            return self._instructions.get(card_id, "")

    def record_event(self, card_id: str, kind: str, detail: dict[str, Any]) -> None:
        """Attach a comment/event to a card (the durable-comment analogue).

        The broker calls this to surface result-validation diagnostics on the
        board; production posts a real Kanban comment/event.
        """
        with self._lock:
            self.events.append({"card_id": card_id, "kind": kind, "detail": detail})

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
            self._publish(
                KanbanResolution(
                    card_id=card_id,
                    profile=profile,
                    status=status,
                    result=result or {},
                    reason=reason,
                )
            )


def is_accepted_resolution(resolution: KanbanResolution, accept_blocked: bool) -> bool:
    """Whether an await may resolve on ``resolution`` given the ``accept_blocked`` policy.

    Terminal (completed/failed) always; a ``blocked`` card only when blocked is
    accepted (i.e. ``on_block`` is not ``"pause"``).
    """
    if resolution.status in TERMINAL_STATES:
        return True
    if resolution.status == CARD_BLOCKED:
        return accept_blocked
    return False


# Card states that are an *outcome* (resolution), as opposed to a "waiting" marker.
_RESOLUTION_STATES = frozenset({CARD_COMPLETED, CARD_BLOCKED, CARD_FAILED})


@runtime_checkable
class KanbanWaitStore(Protocol):
    """Durable persistence seam for Kanban card state (issue #5).

    A small interface — satisfied structurally by
    :class:`hermes_workflows.script_store.ScriptRunStore` — that records the latest
    state of a card (keyed by the content-addressed card id) so a restarted or
    replaying parent can resume from a recorded outcome. ``state`` carries at least
    ``status`` and a monotonic ``version``; an older version never overwrites a
    newer one.
    """

    def record_kanban_card_state(self, card_id: str, state: dict[str, Any]) -> None:
        ...

    def load_kanban_card_state(self, card_id: str) -> Optional[dict[str, Any]]:
        ...


def _resolution_to_state(resolution: KanbanResolution) -> dict[str, Any]:
    """Serialise a resolution to a durable card-state record."""
    return {
        "card_id": resolution.card_id,
        "profile": resolution.profile,
        "status": resolution.status,
        # The worker's structured payload, persisted so a resume need not re-await
        # (or lose) it — the non-deterministic analogue of the #3 replay cache.
        WORKFLOW_RESULT_KEY: resolution.result,
        "reason": resolution.reason,
        "version": resolution.version,
    }


def _state_to_resolution(state: Optional[dict[str, Any]]) -> Optional[KanbanResolution]:
    """Rebuild a resolution from durable state, or ``None`` if not a resolution.

    A ``waiting`` marker (or any non-resolution / malformed state) yields ``None``
    so the caller falls through to a live await.
    """
    if not isinstance(state, dict):
        return None
    status = state.get("status")
    if status not in _RESOLUTION_STATES:
        return None
    result = state.get(WORKFLOW_RESULT_KEY)
    version = state.get("version", 0)
    return KanbanResolution(
        card_id=str(state.get("card_id", "")),
        profile=str(state.get("profile", "")),
        status=status,
        result=result if isinstance(result, dict) else {},
        reason=state.get("reason"),
        version=version if isinstance(version, int) and not isinstance(version, bool) else 0,
    )


class DurableKanbanBackend:
    """A :class:`KanbanBackend` wrapper that persists card state to a
    :class:`KanbanWaitStore`, so a restarted/replaying parent **resumes from a
    recorded outcome** instead of re-awaiting (or losing) the worker's result.

    This is the concrete durability the in-memory fake cannot provide on its own
    (DESIGN §5.7): a resolution recorded by one process is served to a later
    process awaiting the same content-addressed card id, even if the inner backend
    has no memory of it. It composes with any inner backend (the in-memory fake
    today, a real Kanban backend in production):

    * ``create_or_reattach`` records a durable ``waiting`` marker for a new card,
      and reports ``reattached=True`` when a durable record already exists.
    * ``await_resolution`` first consults the durable store: a recorded outcome
      newer than ``after_version`` and accepted by the ``on_block`` policy is
      returned **without touching the inner backend** (resume); otherwise it awaits
      the inner backend live and persists whatever outcome it produces.

    Honest boundary: a card that was only ever ``waiting`` when the parent stopped
    is re-awaited on resume (it has no recorded outcome), which still needs the
    inner backend to deliver the event — for the in-memory fake that means the
    event must still be present; a production backend re-subscribes/replays. The
    *recorded outcomes* are what survive a restart here.
    """

    def __init__(self, inner: KanbanBackend, store: KanbanWaitStore) -> None:
        self._inner = inner
        self._store = store
        self._lock = threading.Lock()
        # Per-card crossing-the-seam state. The wrapper joins two incomparable
        # version spaces — a recorded outcome's foreign version and the inner
        # backend's own counter — so it presents the broker a *third*, coherent
        # monotonic space (``_next``): every returned resolution is re-stamped from
        # it, so the broker's strictly-increasing after_version contract holds. We
        # serve a card from the durable record at most once (``_served``) and feed
        # the inner from the highest inner version we have consumed
        # (``_inner_after``), never the broker's after_version.
        self._served: set[str] = set()
        self._inner_after: dict[str, int] = {}
        self._next: dict[str, int] = {}

    def _stamp(self, card_id: str, resolution: KanbanResolution) -> KanbanResolution:
        """Re-stamp a resolution into the wrapper's own monotonic version space."""
        with self._lock:
            self._next[card_id] = self._next.get(card_id, 0) + 1
            return replace(resolution, version=self._next[card_id])

    def create_or_reattach(self, idempotency_key: str, spec: KanbanCardSpec) -> KanbanCard:
        card = self._inner.create_or_reattach(idempotency_key, spec)
        try:
            existing = self._store.load_kanban_card_state(card.card_id)
        except OSError:  # durable read is best-effort; fall back to live on an IO error.
            existing = None
        if existing is not None:
            # A durable record already exists: this is a resume/reattach even if a
            # fresh inner backend believed it was opening a new card.
            return replace(card, reattached=True)
        self._record_card_state(
            card.card_id,
            {"card_id": card.card_id, "status": "waiting", "profile": card.profile, "version": 0},
        )
        return card

    def await_resolution(
        self,
        card_id: str,
        *,
        accept_blocked: bool,
        timeout: Optional[float],
        after_version: int = 0,
    ) -> KanbanResolution:
        # Serve the durable record only on the *first* await (after_version == 0)
        # and at most once per card. A retry (after_version > 0 — the broker already
        # rejected an outcome this run) always goes live to the inner.
        if after_version == 0:
            with self._lock:
                already_served = card_id in self._served
            if not already_served:
                try:
                    recorded = _state_to_resolution(self._store.load_kanban_card_state(card_id))
                except OSError:  # durable read is best-effort.
                    recorded = None
                if recorded is not None and is_accepted_resolution(recorded, accept_blocked):
                    with self._lock:
                        self._served.add(card_id)
                    return self._stamp(card_id, recorded)  # resume from the durable record.
        # Go live. Feed the inner from *its own* version space (the highest inner
        # version we have consumed), never the broker's after_version, which may
        # carry a recorded outcome's foreign version after a first-await resume.
        with self._lock:
            inner_after = self._inner_after.get(card_id, 0)
        resolution = self._inner.await_resolution(
            card_id,
            accept_blocked=accept_blocked,
            timeout=timeout,
            after_version=inner_after,
        )
        if inner_after and resolution.version <= inner_after:
            # The inner ignored after_version (it must return a strictly newer
            # event); fail closed here rather than let a retry hot-spin.
            raise KanbanError("inner backend returned a stale event (after_version ignored)")
        with self._lock:
            self._inner_after[card_id] = resolution.version
        self._record_card_state(card_id, _resolution_to_state(resolution))
        return self._stamp(card_id, resolution)

    def _record_card_state(self, card_id: str, state: dict[str, Any]) -> None:
        """Persist card state best-effort: the outcome already happened, so an IO
        failure must not fail an otherwise-successful await.

        Only ``OSError`` is swallowed (a genuine disk/IO failure) — a programming
        error such as an unsafe card id or a bad payload still surfaces. The cost
        of a swallowed write is a *degraded* future resume (the card has no
        recorded outcome, so a resume re-awaits it live), not a wrong result for
        this run.
        """
        try:
            self._store.record_kanban_card_state(card_id, state)
        except OSError:  # durable persistence is best-effort; a resume re-awaits.
            pass

    def record_event(self, card_id: str, kind: str, detail: dict[str, Any]) -> None:
        """Pass a card comment/event through to the inner backend if it supports it."""
        inner_record = getattr(self._inner, "record_event", None)
        if callable(inner_record):
            inner_record(card_id, kind, detail)
