"""Real Hermes Kanban backend adapter for ``kanban_agent`` (issue #5).

The shipped pieces of issue #5 — the idempotent durable awaitable
(:mod:`hermes_workflows.kanban`), the durable card-state/event log
(:mod:`hermes_workflows.script_store`), and the cross-process notifier +
event-log backend (:mod:`hermes_workflows.kanban_notify`) — are all backed by an
*in-memory honest fake* (``InMemoryKanbanBackend``) or by a generic event-log
reader. None of them talk to a real Hermes Kanban board. This module is the
production-shaped adapter that closes that residual:

* :class:`HermesKanbanBackend` — a :class:`~hermes_workflows.kanban.KanbanBackend`
  whose ``create_or_reattach`` opens/reattaches a **real** Hermes Kanban card
  through the ``hermes kanban create`` CLI seam, and whose ``await_resolution``
  resolves from **real** Kanban terminal events bridged into the durable event
  log. It composes the shipped durability rather than re-implementing it.
* :class:`HermesKanbanClient` / :class:`SubprocessHermesKanbanClient` — the
  card-creation seam. The default implementation shells out to ``hermes kanban
  create`` once per new card; tests inject a recording fake. This runs in the
  **parent/operator** process (which legitimately holds Hermes credentials), not
  the sandboxed workflow subprocess — the same trust boundary every other
  capability uses.
* :func:`build_create_argv` / :func:`build_card_body` — the pure argv/body
  builders, so the exact ``hermes kanban create`` invocation is unit-testable
  without spawning anything.
* :func:`map_hermes_terminal_status` / :func:`publish_hermes_kanban_event` — the
  Kanban task-event bridge (the narrow #7 seam this slice is allowed to touch):
  normalise a real Hermes terminal task status (``completed`` / ``blocked`` /
  ``failed`` / ``timed_out`` / ``crashed`` / ``gave_up`` …) onto the three
  resolution statuses the awaitable understands, then durably publish it.

Deliberate boundaries (do not widen in this slice):

* **No dispatcher.** The adapter only *creates* and *awaits*. It never runs
  ``hermes kanban dispatch`` / a daemon / a worker / a poll loop — gateway
  dispatch owns claiming and executing the work. :func:`assert_no_dispatch`
  enforces this against every argv the adapter builds.
* **Event-driven, not polling.** ``await_resolution`` blocks on the notifier and
  re-reads the durable log on wakeup (via :class:`EventLogKanbanBackend`); it
  never wakes on a timer just to poll status.
* **Library/operator only.** Nothing here is registered as a model-facing tool;
  it is an injectable backend an operator wires into ``run_workflow_script``.

This module is pure Python 3.11 stdlib.
"""

from __future__ import annotations

import json
import re
import subprocess
from typing import Any, Iterable, Optional, Protocol, Sequence, runtime_checkable

from .kanban import (
    CARD_BLOCKED,
    CARD_COMPLETED,
    CARD_FAILED,
    KanbanCard,
    KanbanCardSpec,
    KanbanResolution,
    KanbanUnknownProfile,
    kanban_card_id,
    result_contract_instruction,
)
from .kanban_notify import (
    EventLogKanbanBackend,
    KanbanEventNotifier,
    has_kanban_history,
    publish_kanban_event,
)
from .registry import utc_now_iso

__all__ = [
    "HERMES_TERMINAL_STATUS_MAP",
    "HermesKanbanError",
    "HermesKanbanCommandError",
    "HermesKanbanClient",
    "SubprocessHermesKanbanClient",
    "HermesKanbanBackend",
    "build_create_argv",
    "build_card_body",
    "assert_no_dispatch",
    "map_hermes_terminal_status",
    "resolve_hermes_kanban_event_card_id",
    "publish_hermes_kanban_event",
]


def _redact_command_argv(argv: list[str]) -> list[str]:
    """Retain command shape for diagnostics without prompt/body payloads."""
    redacted = [str(part) for part in argv]
    if len(redacted) > 3 and redacted[1:3] == ["kanban", "create"]:
        redacted[3] = "<redacted-title>"
    for flag in ("--body",):
        try:
            idx = redacted.index(flag)
        except ValueError:
            continue
        if idx + 1 < len(redacted):
            redacted[idx + 1] = "<redacted>"
    return redacted


class HermesKanbanError(RuntimeError):
    """Base class for Hermes Kanban adapter failures."""


class HermesKanbanCommandError(HermesKanbanError):
    """A ``hermes kanban`` CLI invocation exited non-zero or returned bad output."""

    def __init__(self, argv: list[str], returncode: int, stderr: str) -> None:
        self.argv = _redact_command_argv(argv)
        self.returncode = returncode
        self.stderr = stderr
        # The original argv may carry prompt/context/input in the body, so only a
        # redacted shape is retained and only the subcommand/code are surfaced.
        sub = argv[2] if len(argv) > 2 else "?"
        super().__init__(f"hermes kanban {sub} exited {returncode}")


# --------------------------------------------------------------------------- #
# Kanban task-event bridge (#7 seam): real terminal status -> resolution status
# --------------------------------------------------------------------------- #

# A real Hermes Kanban task can finish in more terminal states than the three the
# awaitable resolves on. The bridge folds the failure family
# (``failed``/``timed_out``/``crashed``/``gave_up``/cancellation) onto a single
# structured ``failed`` resolution, passes ``completed``/``blocked`` through, and
# rejects anything non-terminal so a transient running/queued update can never be
# mistaken for an outcome.
HERMES_TERMINAL_STATUS_MAP: dict[str, str] = {
    "completed": CARD_COMPLETED,
    "complete": CARD_COMPLETED,
    "done": CARD_COMPLETED,
    "succeeded": CARD_COMPLETED,
    "blocked": CARD_BLOCKED,
    "failed": CARD_FAILED,
    "error": CARD_FAILED,
    "errored": CARD_FAILED,
    "timed_out": CARD_FAILED,
    "timeout": CARD_FAILED,
    "crashed": CARD_FAILED,
    "gave_up": CARD_FAILED,
    "cancelled": CARD_FAILED,
    "canceled": CARD_FAILED,
    "abandoned": CARD_FAILED,
}

_REAL_TASK_ID_RE = re.compile(r"^t_[A-Za-z0-9][A-Za-z0-9_.-]{0,125}$")
_REAL_TASK_ALIAS_STATUS = "alias"


def map_hermes_terminal_status(status: Any) -> Optional[str]:
    """Map a real Hermes Kanban terminal task status onto a resolution status.

    Returns ``completed`` / ``blocked`` / ``failed`` for a recognised terminal
    status (case-insensitively), or ``None`` for a non-terminal/unknown status so
    the caller can ignore it rather than resolve on it.
    """
    if not isinstance(status, str):
        return None
    return HERMES_TERMINAL_STATUS_MAP.get(status.strip().lower())


def _extract_real_task_id(create_result: Any, logical_card_id: str) -> Optional[str]:
    """Return the real Hermes ``t_*`` task id from a create result, if present.

    Older/unit fake clients return ``{"card_id": <logical kbc_* id>}``; that must
    not be treated as a real board id. The live CLI/API result is expected to
    carry the real task id as ``task_id`` (or an adjacent id/card_id field), so be
    liberal about the shape while only accepting actual Hermes ``t_*`` task ids.
    """
    if not isinstance(create_result, dict):
        return None

    def _candidate(value: Any) -> Optional[str]:
        if (
            isinstance(value, str)
            and value != logical_card_id
            and _REAL_TASK_ID_RE.fullmatch(value) is not None
        ):
            return value
        return None

    for key in ("task_id", "taskId", "id", "card_id"):
        found = _candidate(create_result.get(key))
        if found is not None:
            return found
    for key in ("task", "card", "result"):
        nested = create_result.get(key)
        if isinstance(nested, dict):
            found = _extract_real_task_id(nested, logical_card_id)
            if found is not None:
                return found
    return None


def resolve_hermes_kanban_event_card_id(store: Any, card_id: str) -> str:
    """Map a real Hermes ``t_*`` event id back to the logical workflow card id.

    ``await kanban_agent`` waits on the deterministic ``kbc_*`` id. Live gateway
    terminal events, however, arrive keyed by the real Kanban task id returned by
    ``hermes kanban create --json``. ``HermesKanbanBackend.create_or_reattach``
    records a durable alias under that real id; this helper follows it before the
    event is appended/notified so a waiting parent wakes on the logical id.
    """
    load_state = getattr(store, "load_kanban_card_state", None)
    if not callable(load_state):
        return card_id
    try:
        state = load_state(card_id)
    except (OSError, ValueError):
        return card_id
    if not isinstance(state, dict):
        if _REAL_TASK_ID_RE.fullmatch(card_id) is not None:
            raise HermesKanbanError(f"no logical workflow card mapping recorded for real task {card_id!r}")
        return card_id
    logical = state.get("logical_card_id")
    if isinstance(logical, str) and logical:
        return logical
    if _REAL_TASK_ID_RE.fullmatch(card_id) is not None:
        raise HermesKanbanError(f"no logical workflow card mapping recorded for real task {card_id!r}")
    return card_id


def publish_hermes_kanban_event(
    store: Any,
    notifier: KanbanEventNotifier,
    card_id: str,
    *,
    status: str,
    result: Optional[dict[str, Any]] = None,
    reason: Optional[str] = None,
    profile: str = "",
) -> dict[str, Any]:
    """Producer-side bridge: normalise a real Hermes terminal event and publish it.

    A worker/gateway (possibly a different process) calls this when a real Kanban
    card reaches a terminal state. The raw Hermes status is mapped onto the
    canonical resolution status and durably appended to the event log (then a
    best-effort wakeup is signalled), so a parent awaiting the card resolves from
    it — including a parent that was down when the event was produced.

    A failure-family status whose name is *narrower* than ``failed`` (e.g.
    ``timed_out``) is preserved in ``reason`` (when no explicit reason is given),
    so the script still sees *which* failure occurred even though the resolution
    status is the single structured ``failed``.

    Raises :class:`HermesKanbanError` for a non-terminal/unknown status — the
    bridge only carries outcomes.
    """
    canonical = map_hermes_terminal_status(status)
    if canonical is None:
        raise HermesKanbanError(f"not a terminal Hermes Kanban status: {status!r}")
    if reason is None and canonical != str(status).strip().lower():
        reason = str(status).strip().lower()  # preserve the specific failure name.
    logical_card_id = resolve_hermes_kanban_event_card_id(store, card_id)
    return publish_kanban_event(
        store, notifier, logical_card_id, status=canonical, result=result, reason=reason, profile=profile
    )


# --------------------------------------------------------------------------- #
# Card-creation CLI seam
# --------------------------------------------------------------------------- #

# The only ``hermes kanban`` subcommands the adapter is ever allowed to invoke.
# Anything else — most importantly ``dispatch`` / ``daemon`` / ``worker`` /
# ``spawn`` / ``serve`` — is refused, so the workflow only ever creates and
# comments; gateway dispatch owns claiming/executing the work.
_ALLOWED_SUBCOMMANDS = frozenset({"create", "comment"})


def _workspace_arg(workspace: dict[str, Any]) -> str:
    """Render the current ``hermes kanban create --workspace`` value.

    The CLI accepts ``scratch``, ``dir:<path>``, ``worktree`` or
    ``worktree:<path>`` — not the JSON object carried inside a
    :class:`KanbanCardSpec`.
    """
    kind = str(workspace.get("type") or workspace.get("kind") or "scratch")
    path = workspace.get("path")
    if kind in {"dir", "worktree"} and isinstance(path, str) and path:
        return f"{kind}:{path}"
    return kind


def assert_no_dispatch(argv: list[str]) -> None:
    """Guard: refuse any ``hermes kanban`` argv that is not a create/comment.

    A defence-in-depth check so the adapter can never shell out to a dispatcher,
    daemon, worker, or poll loop even if a builder is changed carelessly. Raises
    :class:`HermesKanbanError` on a non-create/comment subcommand or a malformed
    argv. The only accepted Kanban global option is ``--board <slug>``, and only
    in the real CLI's global-option slot before the subcommand.
    """
    if not isinstance(argv, (list, tuple)) or len(argv) < 3:
        raise HermesKanbanError(f"not a 'hermes kanban' command: {argv!r}")
    if argv[1] != "kanban":
        raise HermesKanbanError(f"not a 'hermes kanban' command: {argv!r}")

    sub_index = 2
    if argv[sub_index] == "--board":
        if len(argv) < 5 or not argv[sub_index + 1] or str(argv[sub_index + 1]).startswith("-"):
            raise HermesKanbanError(f"malformed kanban --board option: {argv!r}")
        sub_index += 2
    elif str(argv[sub_index]).startswith("--board="):
        raise HermesKanbanError(
            "use '--board <slug>' before the kanban subcommand; inline --board=... is refused"
        )

    sub = argv[sub_index]
    if sub not in _ALLOWED_SUBCOMMANDS:
        raise HermesKanbanError(
            f"refusing non-create kanban subcommand {sub!r}; the adapter never dispatches"
        )
    for token in _kanban_option_tokens(argv[sub_index], argv[sub_index + 1 :]):
        if token == "--board" or str(token).startswith("--board="):
            raise HermesKanbanError(
                "kanban --board is only allowed as a global option before create/comment"
            )


def _kanban_option_tokens(subcommand: str, args: Sequence[Any]) -> Iterable[Any]:
    """Yield option-position tokens for the Kanban subcommand guard.

    ``--board`` inside a title/body/comment is user content, not routing. The
    guard only rejects board-looking tokens where they would be parsed as
    command options after the subcommand.
    """
    if subcommand == "create" and args:
        args = args[1:]  # positional title
    elif subcommand == "comment" and args:
        args = args[1:]  # positional card id; remaining tokens are comment text
        return

    value_flags = {
        "--assignee",
        "--idempotency-key",
        "--tenant",
        "--parent",
        "--workspace",
        "--body",
    }
    skip_value = False
    for token in args:
        if skip_value:
            skip_value = False
            continue
        if token in value_flags:
            skip_value = True
            yield token
            continue
        yield token


def build_card_body(spec: KanbanCardSpec, *, logical_card_id: Optional[str] = None) -> str:
    """Render the card body: the worker prompt, context, and result contract.

    The result-contract instruction (issue #6) is embedded here so a worker
    reading the real card knows to complete it by setting
    ``metadata.workflow_result`` matching the call's schema (and to block rather
    than complete with prose if it cannot).
    """
    parts: list[str] = []
    if spec.prompt:
        parts.append(str(spec.prompt))
    if spec.context:
        parts.append("Context:\n" + json.dumps(spec.context, sort_keys=True, indent=2))
    if spec.task:
        parts.append("Task:\n" + json.dumps(spec.task, sort_keys=True, indent=2))
    if spec.input:
        parts.append("Input:\n" + json.dumps(spec.input, sort_keys=True, indent=2))
    metadata: dict[str, Any] = {}
    if logical_card_id:
        metadata["logical_card_id"] = logical_card_id
    if spec.board:
        metadata["board"] = spec.board
    if spec.labels:
        metadata["labels"] = list(spec.labels)
    if metadata:
        parts.append("Kanban metadata:\n" + json.dumps(metadata, sort_keys=True, indent=2))
    instruction = result_contract_instruction(spec.schema)
    if instruction:
        parts.append(instruction)
    return "\n\n".join(parts)


def build_create_argv(
    card_id: str,
    idempotency_key: str,
    spec: KanbanCardSpec,
    *,
    hermes_bin: str = "hermes",
) -> list[str]:
    """Build the exact ``hermes kanban create`` argv for one new card.

    Carries everything the current CLI accepts and nothing the adapter is not
    allowed to do: the global board option (when present), positional title,
    idempotency key (so concurrent parents/replays converge on one card),
    assignee profile, tenant, parents, workspace, the rendered body with the
    result-contract instruction, and ``--json`` so the real ``t_*`` task id
    can be durably mapped back to the logical workflow card id. Unsupported
    fields (labels/logical card id) are not passed as fake flags.
    """
    title = spec.title or f"Workflow Kanban card {card_id}"
    argv: list[str] = [hermes_bin, "kanban"]
    if spec.board:
        argv += ["--board", spec.board]
    argv += [
        "create",
        title,
        "--assignee",
        spec.profile,
        "--idempotency-key",
        idempotency_key,
    ]
    if spec.tenant:
        argv += ["--tenant", spec.tenant]
    for parent in spec.parents:
        argv += ["--parent", parent]
    if spec.workspace is not None:
        argv += ["--workspace", _workspace_arg(spec.workspace)]
    argv += ["--body", build_card_body(spec, logical_card_id=card_id), "--json"]
    return argv


@runtime_checkable
class HermesKanbanClient(Protocol):
    """The card-creation seam the adapter drives (parent-side, with credentials).

    Implementations create/reattach a card through the real Hermes Kanban DB/API.
    ``create`` must be idempotent on ``idempotency_key`` (a re-create with the
    same key must converge on the same card, never open a duplicate) — the adapter
    only calls it when it has *no* durable record of the card, but a real backend
    must still be safe against a concurrent parent racing the same key.
    """

    def create(self, card_id: str, idempotency_key: str, spec: KanbanCardSpec) -> dict[str, Any]:
        """Create (or idempotently reattach) the real card; return the CLI/API result."""
        ...


class SubprocessHermesKanbanClient:
    """Default :class:`HermesKanbanClient`: shell out to ``hermes kanban create``.

    Runs in the **parent/operator** process — it inherits the real environment
    (Hermes credentials) on purpose, unlike the workflow VM's scrubbed subprocess.
    Every argv is guarded by :func:`assert_no_dispatch` before launch, so this can
    only ever create (or comment), never dispatch.
    """

    def __init__(
        self,
        *,
        hermes_bin: str = "hermes",
        timeout: float = 30.0,
        env: Optional[dict[str, str]] = None,
    ) -> None:
        self._hermes_bin = hermes_bin
        self._timeout = timeout
        self._env = env  # None -> inherit (the operator process holds Hermes creds).

    def create(self, card_id: str, idempotency_key: str, spec: KanbanCardSpec) -> dict[str, Any]:
        argv = build_create_argv(card_id, idempotency_key, spec, hermes_bin=self._hermes_bin)
        assert_no_dispatch(argv)
        try:
            proc = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=self._timeout,
                env=self._env,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            detail = f"timed out after {exc.timeout}s"
            if exc.stderr:
                stderr = exc.stderr if isinstance(exc.stderr, str) else exc.stderr.decode("utf-8", "replace")
                detail = f"{detail}; stderr: {stderr[-2000:]}"
            raise HermesKanbanCommandError(argv, -1, detail) from exc
        except OSError as exc:
            raise HermesKanbanCommandError(argv, -1, str(exc)) from exc
        if proc.returncode != 0:
            raise HermesKanbanCommandError(argv, proc.returncode, (proc.stderr or "")[-2000:])
        out = (proc.stdout or "").strip()
        if not out:
            raise HermesKanbanCommandError(argv, 0, "empty JSON output from hermes kanban create --json")
        try:
            parsed = json.loads(out)
        except json.JSONDecodeError as exc:
            detail = f"invalid JSON output from hermes kanban create --json: {exc}; stdout: {out[-2000:]}"
            raise HermesKanbanCommandError(argv, 0, detail) from exc
        if not isinstance(parsed, dict):
            raise HermesKanbanCommandError(
                argv, 0, f"expected JSON object from hermes kanban create --json, got {type(parsed).__name__}"
            )
        if _extract_real_task_id(parsed, card_id) is None:
            raise HermesKanbanCommandError(
                argv, 0, "JSON output from hermes kanban create --json did not include a real t_* task id"
            )
        return parsed


# --------------------------------------------------------------------------- #
# The backend
# --------------------------------------------------------------------------- #


class HermesKanbanBackend:
    """A real-Hermes :class:`~hermes_workflows.kanban.KanbanBackend`.

    ``create_or_reattach`` opens a real card through the :class:`HermesKanbanClient`
    seam (exactly one ``hermes kanban create`` per new card) and records a durable
    ``waiting`` marker; a replay/restart that already has a durable record for the
    content-addressed card id **reattaches** without creating a duplicate.
    ``await_resolution`` is delegated to a composed :class:`EventLogKanbanBackend`,
    so the await is event-driven from the durable log (the production-shaped path
    where a worker/gateway bridges real terminal events in via
    :func:`publish_hermes_kanban_event`), bounded by the run deadline, and honours
    ``after_version`` and the ``on_block`` policy exactly as the shipped backend
    does.

    Unknown assignee profiles are rejected **before** any card is created.
    """

    def __init__(
        self,
        store: Any,
        notifier: KanbanEventNotifier,
        *,
        client: HermesKanbanClient,
        known_profiles: Optional[set[str]] = None,
        unknown_profiles: frozenset[str] = frozenset(),
    ) -> None:
        self._store = store
        self._notifier = notifier
        self._client = client
        self._known = set(known_profiles) if known_profiles is not None else None
        self._unknown = unknown_profiles
        # The await half is the shipped, event-driven event-log backend. The real
        # terminal events are bridged into the durable log already normalised (via
        # publish_hermes_kanban_event), so the generic reader resolves them.
        self._awaiter = EventLogKanbanBackend(
            store, notifier, known_profiles=known_profiles, unknown_profiles=unknown_profiles
        )

    # -- profile policy ---------------------------------------------------
    def _check_profile(self, profile: str) -> None:
        if not isinstance(profile, str) or not profile:
            raise KanbanUnknownProfile(str(profile))
        if profile in self._unknown:
            raise KanbanUnknownProfile(profile)
        if self._known is not None and profile not in self._known:
            raise KanbanUnknownProfile(profile)

    def _has_history(self, card_id: str) -> bool:
        """Whether the durable store already knows this content-addressed card.

        A prior run's ``waiting`` marker or any logged event means the card was
        already created; a replay/restart must reattach, not re-create. Reads are
        best-effort — an IO error degrades to "no history" (re-create), which the
        real client's idempotency key still de-duplicates.
        """
        return has_kanban_history(self._store, card_id)

    def _load_card_state(self, card_id: str) -> Optional[dict[str, Any]]:
        load_state = getattr(self._store, "load_kanban_card_state", None)
        if not callable(load_state):
            return None
        try:
            state = load_state(card_id)
        except (OSError, ValueError):
            return None
        return state if isinstance(state, dict) else None

    def _record_real_task_alias(self, real_task_id: str, logical_card_id: str, profile: str) -> None:
        self._store.record_kanban_card_state(
            real_task_id,
            {
                "card_id": real_task_id,
                "logical_card_id": logical_card_id,
                "real_task_id": real_task_id,
                "status": _REAL_TASK_ALIAS_STATUS,
                "profile": profile,
                "version": 0,
            },
        )

    def create_or_reattach(self, idempotency_key: str, spec: KanbanCardSpec) -> KanbanCard:
        self._check_profile(spec.profile)  # reject unknown assignee BEFORE any create.
        card_id = kanban_card_id(idempotency_key)
        if self._has_history(card_id):
            # Durable record exists (prior run / restart / replay): reattach, no
            # second create — preserves idempotency and the no-duplicate guarantee.
            state = self._load_card_state(card_id)
            real_task_id = state.get("real_task_id") if isinstance(state, dict) else None
            if isinstance(real_task_id, str) and _REAL_TASK_ID_RE.fullmatch(real_task_id) is not None:
                try:
                    self._record_real_task_alias(real_task_id, card_id, spec.profile)
                except (OSError, ValueError) as exc:
                    raise HermesKanbanError(
                        f"failed to repair real task mapping for {real_task_id!r} -> {card_id!r}"
                    ) from exc
            return KanbanCard(
                card_id=card_id, profile=spec.profile, reattached=True, created_at=utc_now_iso()
            )
        # First sight of this logical call: open the real card (one CLI invocation).
        create_result = self._client.create(card_id, idempotency_key, spec)
        real_task_id = _extract_real_task_id(create_result, card_id)
        state = {"card_id": card_id, "status": "waiting", "profile": spec.profile, "version": 0}
        if real_task_id is not None:
            state["real_task_id"] = real_task_id
        try:
            self._store.record_kanban_card_state(card_id, state)
        except OSError as exc:
            if real_task_id is not None:
                raise HermesKanbanError(
                    f"failed to persist real task mapping for {real_task_id!r} -> {card_id!r}"
                ) from exc
        if real_task_id is not None:
            try:
                self._record_real_task_alias(real_task_id, card_id, spec.profile)
            except (OSError, ValueError) as exc:
                raise HermesKanbanError(
                    f"failed to persist real task mapping for {real_task_id!r} -> {card_id!r}"
                ) from exc
        return KanbanCard(
            card_id=card_id, profile=spec.profile, reattached=False, created_at=utc_now_iso()
        )

    def await_resolution(
        self,
        card_id: str,
        *,
        accept_blocked: bool,
        timeout: Optional[float],
        after_version: int = 0,
    ) -> KanbanResolution:
        return self._awaiter.await_resolution(
            card_id, accept_blocked=accept_blocked, timeout=timeout, after_version=after_version
        )

    def record_event(self, card_id: str, kind: str, detail: dict[str, Any]) -> None:
        """Forward a card comment/event (e.g. result-validation diagnostics) to the
        client if it supports it; a no-op otherwise.

        Production posts a real ``hermes kanban comment``; the default subprocess
        client does not yet implement it, so this degrades to nothing rather than
        failing an otherwise-successful await.
        """
        recorder = getattr(self._client, "record_event", None)
        if callable(recorder):
            try:
                recorder(card_id, kind, detail)
            except Exception:  # noqa: BLE001 — card comments are best-effort.
                pass
