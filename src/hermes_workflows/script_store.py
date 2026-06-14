"""Durable run store and deterministic replay cache for workflow scripts (issue #3).

The subprocess VM (:mod:`hermes_workflows.vm`) runs a model-authored Python
orchestration script out-of-process and produces a metadata-only ``calls``
journal with **stable, ascending RPC call ids** (1, 2, 3, ...). Because the
script is deterministic — given the same ``args`` and the same sequence of RPC
return values it makes the exact same sequence of calls in the exact same order
— those call ids are a stable address space across runs of the same script.

This module turns that into durability:

* :class:`ScriptRunStore` persists each run under ``<root>/<run_id>/`` as a
  bounded ``run.json`` metadata snapshot, a metadata-only ``journal.jsonl``
  (``boot`` / ``call`` / ``done`` events — no raw inputs/outputs/prompts), and a
  separate ``cache.jsonl`` replay cache.
* The **replay cache** records the *result* of every deterministic capability
  call keyed by its stable call id, plus a ``method`` + canonical ``args_hash``
  integrity tag. On a later replay the parent broker serves those calls from the
  cache instead of re-dispatching to the :class:`AgentRunner`, so deterministic
  work is not duplicated.

What is *replayable* is deliberately conservative (see :func:`is_replayable`):
``log`` / ``phase`` always (their result is a constant ``None``), and
``agent`` / ``kanban_agent`` **only** when the caller declares the injected
runner deterministic (the default :class:`StubAgentRunner` is — a pure function
of its inputs). A live, non-deterministic Hermes runner caches no agent output,
so on replay those calls re-run rather than returning a stale value. We do not
fake safety: caching is opt-in to determinism.

Failure is fail-closed and typed. A missing run, a corrupt ``run.json`` /
``cache.jsonl`` line, or an incompatible ``schema_version`` raises a
:class:`~hermes_workflows.errors.ScriptRunStoreError` subclass at load time —
never a bare exception — so the parent can decline to replay without corrupting
state. A mid-run *drift* (a replayed call whose method/args no longer match the
recorded run) is surfaced inside the run as a ``replay_mismatch`` denial that
aborts the subprocess (see :mod:`hermes_workflows.vm`).

This module is pure Python 3.11 stdlib.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from .errors import CorruptScriptRunError, ScriptRunNotFound, ScriptRunStoreError
from .registry import utc_now_iso

__all__ = [
    "SCRIPT_SCHEMA_VERSION",
    "ScriptRunMeta",
    "ReplayEntry",
    "ReplayCache",
    "CallRecorder",
    "ScriptRunStore",
    "canonical_hash",
    "script_sha256",
    "script_run_id",
    "replay_args_hash",
    "is_replayable",
]

# Bump when the on-disk layout changes incompatibly. A run.json written by a
# different version is refused at load time as a typed CorruptScriptRunError so
# we never silently misread a stale schema.
SCRIPT_SCHEMA_VERSION = 1

# A run id must be usable as exactly one filesystem path segment (mirrors the
# JSON-runtime FileRunStore guard). Minted ids are ``wfs_<hash8>_<uuid12>``.
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

# Durable kanban card *terminal* states (issue #5). Once a card reaches one of
# these it is final: a non-terminal write (``waiting`` or ``blocked``) must never
# regress it (see ``record_kanban_card_state``).
_KANBAN_TERMINAL_STATUSES = frozenset({"completed", "failed"})

# Methods whose result is a deterministic constant regardless of any runner:
# both ``log`` and ``phase`` are pure side-effect metadata and return ``None``.
_ALWAYS_REPLAYABLE = frozenset({"log", "phase"})
# Methods that cross the AgentRunner boundary: replayable only when the runner
# is declared deterministic for the run.
_RUNNER_METHODS = frozenset({"agent", "kanban_agent"})


def canonical_hash(obj: Any) -> str:
    """Return the sha256 hex of a value's canonical JSON form.

    ``sort_keys`` + compact separators make the encoding order-independent;
    ``default=str`` is a safety net for any non-JSON leaf (real RPC params are
    already JSON, so it rarely fires). Used for both the per-run ``args_hash``
    and the per-call replay integrity tag.
    """
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def script_sha256(script: str) -> str:
    """Return the sha256 hex of the script source (full digest, not truncated)."""
    return hashlib.sha256(script.encode("utf-8", "surrogatepass")).hexdigest()


def script_run_id(script: str, args: Any = None) -> str:
    """Mint ``wfs_<digest8>_<uuid12>`` for a script + args pair.

    The 8-hex prefix is content-addressed (sha256 of the canonicalized
    script+args) so ids sort by source; the 12-hex ``uuid4`` suffix keeps them
    collision-resistant per run. Callers may always pass an explicit ``run_id``
    instead, for idempotency or deterministic tests.
    """
    digest = canonical_hash({"script": script, "args": args})
    return f"wfs_{digest[:8]}_{uuid.uuid4().hex[:12]}"


def replay_args_hash(method: str, params: dict[str, Any]) -> str:
    """Canonical integrity hash of a capability call's *semantic* arguments.

    The cosmetic ``label`` (display-only, does not affect a call's result) is
    excluded so a relabelled-but-equivalent call still replays. Everything else
    in ``params`` (``agent_id``/``profile``, ``input``, ``task``, ``schema``)
    participates, so any change that could change the result is detected as a
    replay mismatch.
    """
    keyed = {k: v for k, v in params.items() if k != "label"}
    return canonical_hash({"method": method, "params": keyed})


def is_replayable(method: str, *, deterministic_runner: bool) -> bool:
    """Whether a call of ``method`` may be cached/served from the replay cache.

    ``log`` / ``phase`` always (constant ``None`` result). ``agent`` /
    ``kanban_agent`` only when ``deterministic_runner`` is true. ``workflow`` and
    anything else are never replayable.
    """
    if method in _ALWAYS_REPLAYABLE:
        return True
    if method in _RUNNER_METHODS:
        return deterministic_runner
    return False


def _require_safe_run_id(run_id: str) -> None:
    if not isinstance(run_id, str) or not _RUN_ID_RE.fullmatch(run_id):
        raise ValueError(f"unsafe run_id: {run_id!r}")


def _require_safe_card_id(card_id: str) -> None:
    # A card id is also used as exactly one filesystem path segment (it keys the
    # durable kanban state file), so it gets the same single-segment guard.
    if not isinstance(card_id, str) or not _RUN_ID_RE.fullmatch(card_id):
        raise ValueError(f"unsafe card_id: {card_id!r}")


def _redact_error(error: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Keep only metadata-safe error fields for the durable journal.

    Drops the free-text ``message`` (which a workflow script controls and could
    fill with input/output-derived data) and keeps the structural ``type`` /
    ``code`` / ``line``, honoring the journal's metadata-only contract.
    """
    if not isinstance(error, dict):
        return error
    return {k: error[k] for k in ("type", "code", "line") if k in error}


def _fsync_dir(path: Path) -> None:
    """Best-effort directory fsync after an atomic snapshot replace."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        # Some filesystems reject fsync on a directory fd (e.g. certain network
        # mounts); the os.replace above already landed the snapshot, so the
        # missing dir-entry flush is a durability best-effort, not a failure.
        pass
    finally:
        os.close(fd)


@dataclass
class ScriptRunMeta:
    """Durable metadata snapshot for one script run.

    Holds no raw script source, inputs, or outputs — only content hashes,
    lifecycle status, the run's ``meta`` literal (already public, name +
    description), and a small ``limits`` view. ``value``/``error`` carry the
    final result of the run (the script's chosen return, which the model already
    intends to surface).
    """

    run_id: str
    script_sha256: str
    args_hash: str
    status: str = "running"  # running | succeeded | failed
    meta: Optional[dict[str, Any]] = None
    limits: Optional[dict[str, Any]] = None
    value: Any = None
    error: Optional[dict[str, Any]] = None
    deterministic_runner: bool = False
    replay_of: Optional[str] = None
    schema_version: int = SCRIPT_SCHEMA_VERSION
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "script_sha256": self.script_sha256,
            "args_hash": self.args_hash,
            "status": self.status,
            "meta": self.meta,
            "limits": self.limits,
            "value": self.value,
            "error": self.error,
            "deterministic_runner": self.deterministic_runner,
            "replay_of": self.replay_of,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class ReplayEntry:
    """One cached deterministic call result, addressed by its stable call id."""

    call_id: int
    method: str
    args_hash: str
    value: Any


class ReplayCache:
    """Immutable, in-memory view of a run's ``cache.jsonl`` for replay.

    Maps a stable call id to its :class:`ReplayEntry`. The parent broker consults
    it before dispatching: a hit (matching method + args hash) returns the cached
    value without touching the runner; a method/args drift is a fail-closed
    mismatch; an absent id is a miss (the original call was non-replayable, so it
    re-runs live).
    """

    def __init__(self, entries: dict[int, ReplayEntry], *, source_run_id: str) -> None:
        self._entries = entries
        self.source_run_id = source_run_id

    def get(self, call_id: Any) -> Optional[ReplayEntry]:
        # ``isinstance(True, int)`` is True, so reject bools explicitly: a call id
        # of ``True``/``False`` must not alias to cached call ids 1/0.
        if not isinstance(call_id, int) or isinstance(call_id, bool):
            return None
        return self._entries.get(call_id)

    def __len__(self) -> int:
        return len(self._entries)


class CallRecorder:
    """Append-only writer for one run's deterministic replay cache.

    Each :meth:`record` writes one ``cache.jsonl`` line ``{call_id, method,
    args_hash, value}`` and fsyncs, so a parent crash mid-run still leaves a
    consistent prefix of cached calls. Only deterministic calls are recorded
    (the broker decides via :func:`is_replayable`); the metadata journal records
    *all* calls separately.
    """

    def __init__(self, path: Path, lock: threading.Lock) -> None:
        self._path = path
        self._lock = lock

    def record(self, call_id: Any, method: str, args_hash: str, value: Any) -> None:
        line = json.dumps(
            {"call_id": call_id, "method": method, "args_hash": args_hash, "value": value},
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        with self._lock:
            with self._path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()
                os.fsync(f.fileno())


class ScriptRunStore:
    """Filesystem-backed durable store for subprocess workflow-script runs.

    Layout, one directory per run::

        <root>/<run_id>/run.json       # bounded metadata snapshot (atomic write)
        <root>/<run_id>/journal.jsonl  # metadata-only boot/call/done events
        <root>/<run_id>/cache.jsonl    # deterministic replay cache (opt-in)

    The store is the parent-owned persistence boundary; the workflow script (in
    its subprocess) still has no filesystem authority. Concurrent writers within
    one process are serialised by a single lock. ``root`` defaults are chosen by
    the caller (e.g. ``$HERMES_HOME/dynamic-workflows/script-runs``).
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    # -- id minting -------------------------------------------------------
    def next_run_id(self, script: str, args: Any = None) -> str:
        """Mint a fresh content-addressed run id (see :func:`script_run_id`)."""
        return script_run_id(script, args)

    # -- lifecycle: begin -> note_call/record -> finish -------------------
    def begin(
        self,
        run_id: str,
        *,
        script: str,
        args: Any,
        limits: Optional[dict[str, Any]],
        deterministic_runner: bool,
        replay_of: Optional[str] = None,
    ) -> ScriptRunMeta:
        """Create the run directory, write ``run.json`` (status=running), and a
        ``boot`` journal event. Raises ``ValueError`` on a duplicate run id."""
        _require_safe_run_id(run_id)
        meta = ScriptRunMeta(
            run_id=run_id,
            script_sha256=script_sha256(script),
            args_hash=canonical_hash(args),
            limits=limits,
            deterministic_runner=deterministic_runner,
            replay_of=replay_of,
        )
        with self._lock:
            run_dir = self._run_dir(run_id)
            try:
                run_dir.mkdir(parents=True, exist_ok=False)
            except FileExistsError as exc:
                raise ValueError(f"run_id already exists: {run_id!r}") from exc
            try:
                self._write_meta(meta)
                self._append_journal(
                    run_id,
                    "boot",
                    {
                        "script_sha256": meta.script_sha256,
                        "args_hash": meta.args_hash,
                        "deterministic_runner": deterministic_runner,
                        "replay_of": replay_of,
                        "limits": limits,
                    },
                )
            except BaseException:
                # Leave no orphan 'running' dir on a partial begin, so the same
                # explicit run_id can be cleanly retried.
                shutil.rmtree(run_dir, ignore_errors=True)
                raise
        return meta

    def note_call(self, run_id: str, event: dict[str, Any]) -> None:
        """Write a metadata-only ``call`` journal event from a broker event.

        The broker emits one event per completed RPC call (call id, method,
        agent id/profile, ok, error, ``replayed``). In this synchronous broker
        the return outcome is folded into the same event, so the journal
        vocabulary is ``boot`` / ``call`` / ``done`` (a separate ``return`` line
        would carry no extra information). Raw ``params`` are never written.
        """
        data = {
            "call_id": event.get("call_id"),
            "method": event.get("method"),
            "ok": event.get("ok"),
        }
        for key in ("agent_id", "profile", "label", "error", "replayed"):
            if event.get(key) is not None:
                data[key] = event.get(key)
        with self._lock:
            self._append_journal(run_id, "call", data)

    def recorder(self, run_id: str) -> CallRecorder:
        """Return the append-only replay-cache writer for ``run_id``."""
        _require_safe_run_id(run_id)
        return CallRecorder(self._cache_path(run_id), self._lock)

    def finish(
        self,
        run_id: str,
        *,
        status: str,
        meta: Optional[dict[str, Any]],
        value: Any,
        error: Optional[dict[str, Any]],
    ) -> None:
        """Write the terminal ``run.json`` (status + result) and a ``done`` event.

        Tolerant of a half-written ``run.json``: it reloads best-effort and, if
        the metadata is unreadable, rewrites a minimal terminal record so the run
        is never left stuck in ``running``.
        """
        _require_safe_run_id(run_id)
        with self._lock:
            # Truly best-effort reload: a *corrupt* or schema-drifted run.json
            # must not make finish() raise (which would leave the run stuck in
            # 'running' and the exception escape the caller). Fall back to a
            # minimal terminal record on any load failure, not just a missing one.
            try:
                record = self._load_meta_unlocked(run_id, missing_ok=True)
            except ScriptRunStoreError:
                record = None
            if record is None:
                record = ScriptRunMeta(run_id=run_id, script_sha256="", args_hash="")
            record.status = status
            record.meta = meta
            record.value = value
            record.error = error
            record.updated_at = utc_now_iso()
            self._write_meta(record)
            # The 'done' journal event is metadata-only: a script-authored
            # exception message can carry arbitrary (possibly sensitive) text, so
            # only its type/code/line reach the journal. The full error is kept
            # on the operator-facing run.json (which already records value/error).
            self._append_journal(
                run_id,
                "done",
                {"status": status, "has_value": value is not None, "error": _redact_error(error)},
            )

    # -- reads ------------------------------------------------------------
    def load_run(self, run_id: str) -> ScriptRunMeta:
        """Load a run's metadata. Raises :class:`ScriptRunNotFound` if absent and
        :class:`CorruptScriptRunError` on a malformed or stale-schema record."""
        _require_safe_run_id(run_id)
        with self._lock:
            record = self._load_meta_unlocked(run_id, missing_ok=False)
        assert record is not None
        return record

    def load_cache(self, run_id: str) -> ReplayCache:
        """Load a run's deterministic replay cache.

        Raises :class:`ScriptRunNotFound` if the run dir is absent and
        :class:`CorruptScriptRunError` (reason ``"corrupt_cache"``) on any
        malformed line or duplicate call id. An absent ``cache.jsonl`` for an
        existing run is an *empty* cache (the run recorded nothing replayable),
        not an error.
        """
        _require_safe_run_id(run_id)
        if not self._run_dir(run_id).exists():
            raise ScriptRunNotFound(run_id)
        path = self._cache_path(run_id)
        entries: dict[int, ReplayEntry] = {}
        if not path.exists():
            return ReplayCache(entries, source_run_id=run_id)
        with path.open("r", encoding="utf-8") as f:
            for lineno, raw in enumerate(f, start=1):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise CorruptScriptRunError(
                        run_id, "corrupt_cache", f"cache.jsonl line {lineno}: {exc.msg}"
                    ) from exc
                call_id_raw = obj.get("call_id") if isinstance(obj, dict) else None
                # ``isinstance(True, int)`` is True, so exclude bools explicitly:
                # a forged ``call_id: true`` must not be accepted as call id 1.
                if not isinstance(obj, dict) or not isinstance(call_id_raw, int) or isinstance(call_id_raw, bool):
                    raise CorruptScriptRunError(
                        run_id, "corrupt_cache", f"cache.jsonl line {lineno}: bad entry shape"
                    )
                call_id = obj["call_id"]
                if call_id in entries:
                    raise CorruptScriptRunError(
                        run_id, "corrupt_cache", f"duplicate cached call id {call_id}"
                    )
                method = obj.get("method")
                args_hash = obj.get("args_hash")
                # Require real strings rather than coercing with str(): a missing
                # method would become the literal "None" and silently match a
                # forged replay entry against the per-call integrity guard.
                if not isinstance(method, str) or not isinstance(args_hash, str):
                    raise CorruptScriptRunError(
                        run_id,
                        "corrupt_cache",
                        f"cache.jsonl line {lineno}: method/args_hash must be strings",
                    )
                entries[call_id] = ReplayEntry(
                    call_id=call_id,
                    method=method,
                    args_hash=args_hash,
                    value=obj.get("value"),
                )
        return ReplayCache(entries, source_run_id=run_id)

    def journal(self, run_id: str, *, limit: int = 200) -> list[dict[str, Any]]:
        """Return the most recent metadata-only journal events for ``run_id``."""
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

    def journal_path(self, run_id: str) -> Path:
        _require_safe_run_id(run_id)
        return self._journal_path(run_id)

    # -- durable kanban card state (issue #5: resume across restart) -------
    # A Kanban await is non-deterministic, so it is excluded from the #3 replay
    # cache; instead the latest known state of each card is persisted under
    # ``<root>/_kanban/<card_id>.json`` (keyed by the content-addressed card id, so
    # it is stable across replays). This lets a restarted/replaying parent resume
    # from a recorded terminal resolution instead of re-awaiting — or losing — the
    # worker's result, and gives operators a durable view of in-flight waits.
    def record_kanban_card_state(self, card_id: str, state: dict[str, Any]) -> None:
        """Persist the latest state of a Kanban card (atomic; status-precedence).

        Last-write-wins among writes, with one precedence guard: once a card has
        reached a **terminal** outcome (completed/failed) a *non-terminal* write
        (``waiting`` or ``blocked``) never overwrites it. So re-opening/reattaching
        a card, or a slow stale writer landing an old ``blocked``/``waiting``, can
        never regress a recorded terminal result (a legitimate ``blocked`` ->
        ``completed`` unblock still lands, since the incoming status is terminal).
        A numeric version is deliberately *not* used to gate writes — a card's
        events can originate in different, incomparable version spaces (a prior
        process's backend vs. a fresh one on resume), so comparing them would
        wrongly drop a live superseding outcome.
        """
        _require_safe_card_id(card_id)
        if not isinstance(state, dict):
            raise ValueError("kanban card state must be a dict")
        record = {**state, "card_id": card_id}
        with self._lock:
            existing = self._load_kanban_card_state_unlocked(card_id)
            if (
                existing is not None
                and existing.get("status") in _KANBAN_TERMINAL_STATUSES
                and record.get("status") not in _KANBAN_TERMINAL_STATUSES
            ):
                return  # a terminal outcome is final; a non-terminal write never regresses it.
            self._kanban_dir().mkdir(parents=True, exist_ok=True)
            path = self._kanban_path(card_id)
            payload = json.dumps(record, ensure_ascii=False, indent=2) + "\n"
            # A unique temp name (not a fixed '<card_id>.json.tmp') so two processes
            # writing the same card never share a temp file and race os.replace.
            fd, tmp_name = tempfile.mkstemp(dir=str(self._kanban_dir()), prefix=f"{card_id}.", suffix=".tmp")
            tmp = Path(tmp_name)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(payload)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, path)
            except BaseException:
                tmp.unlink(missing_ok=True)
                raise
            _fsync_dir(self._kanban_dir())

    def load_kanban_card_state(self, card_id: str) -> Optional[dict[str, Any]]:
        """Return the latest persisted state of ``card_id``, or ``None`` if absent."""
        _require_safe_card_id(card_id)
        with self._lock:
            return self._load_kanban_card_state_unlocked(card_id)

    def kanban_waits(self) -> list[dict[str, Any]]:
        """Return persisted card states that are not yet terminal (in-flight waits).

        Operator-facing durable view of what runs are blocked on, recovered from
        disk so it survives a parent restart. Terminal (``completed``/``failed``)
        cards are excluded; a ``blocked`` card is still an in-flight wait.
        """
        kdir = self._kanban_dir()
        if not kdir.exists():
            return []
        waits: list[dict[str, Any]] = []
        for path in sorted(kdir.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(data, dict) and data.get("status") not in ("completed", "failed"):
                waits.append(data)
        return waits

    def _load_kanban_card_state_unlocked(self, card_id: str) -> Optional[dict[str, Any]]:
        path = self._kanban_path(card_id)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, json.JSONDecodeError):
            return None  # a corrupt card-state file is treated as absent (fail-safe re-await).
        return data if isinstance(data, dict) else None

    # -- durable kanban event log (issue #5: external-producer event seam) --
    # The latest-state file above is written by the *parent's own* await. The
    # event log below is the producer-facing seam: a worker/gateway (possibly a
    # different process) appends card events to an append-only
    # ``<root>/_kanban/<card_id>.events.jsonl``. A parent that was down when the
    # event was produced replays it from the log on its next await — the durable
    # cross-restart event delivery the in-memory backend cannot provide. The log is
    # also a durable audit trail of every card event.
    def append_kanban_event(
        self,
        card_id: str,
        *,
        status: str,
        result: Optional[dict[str, Any]] = None,
        reason: Optional[str] = None,
        profile: str = "",
    ) -> dict[str, Any]:
        """Append one durable card event and return the persisted record.

        ``seq`` is the event's **line position** in the log — assigned at read
        time (not written into the line), so it is inherently unique and monotonic
        even across concurrent producers in different processes (each ``O_APPEND``
        write lands one whole line; the line order is the append order). The
        returned ``seq`` is exact in a single process. The worker's structured
        payload is stored under ``workflow_result`` — the same key the resolution
        serialiser uses — so a logged event reads back as a resolution without
        translation.
        """
        _require_safe_card_id(card_id)
        record = {
            "card_id": card_id,
            "ts": utc_now_iso(),
            "status": status,
            "workflow_result": result if isinstance(result, dict) else {},
            "reason": reason,
            "profile": profile,
        }
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            self._kanban_dir().mkdir(parents=True, exist_ok=True)
            path = self._kanban_events_path(card_id)
            line_count, prefix = self._event_log_shape(path)
            with path.open("a", encoding="utf-8") as f:
                f.write(prefix + line + "\n")
                f.flush()
                os.fsync(f.fileno())
            seq = line_count + 1  # the new line's 1-based position within this process.
        return {"seq": seq, **record}

    def read_kanban_events(self, card_id: str, *, after_seq: int = 0) -> list[dict[str, Any]]:
        """Return durable card events with line position ``> after_seq``.

        ``seq`` is the event's physical line number (1-based), so it is a stable,
        unique cursor regardless of concurrent producers; a corrupt line is skipped
        but still consumes its position so the cursor never shifts.
        """
        _require_safe_card_id(card_id)
        path = self._kanban_events_path(card_id)
        events: list[dict[str, Any]] = []
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return []
        except OSError:
            return []
        for lineno, raw in enumerate(text.splitlines(), start=1):
            if lineno <= after_seq:
                continue
            raw = raw.strip()
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append({**event, "seq": lineno})
        return events

    def latest_kanban_resolution(self, card_id: str) -> Optional[dict[str, Any]]:
        """Return the most recent *outcome* event from the log, or ``None``.

        Shaped like a card-state record (``status`` / ``workflow_result`` /
        ``reason`` / ``version``), so a backend can resume from it directly. A
        non-resolution event (or no events) yields ``None``.
        """
        latest: Optional[dict[str, Any]] = None
        for event in self.read_kanban_events(card_id):
            if event.get("status") in ("completed", "blocked", "failed"):
                latest = event
        if latest is None:
            return None
        return {
            "card_id": card_id,
            "status": latest.get("status"),
            "workflow_result": latest.get("workflow_result") if isinstance(latest.get("workflow_result"), dict) else {},
            "reason": latest.get("reason"),
            "profile": latest.get("profile", ""),
            "version": latest.get("seq", 0),
        }

    @staticmethod
    def _event_log_shape(path: Path) -> tuple[int, str]:
        """Return (physical line count, prefix needed before appending).

        A crash mid-append can leave a torn final line; prefixing the next event
        with a newline isolates the damage to that one (corrupt, skipped) line
        instead of letting the next event concatenate onto it. Count bytes rather
        than decoding text so even a torn UTF-8 sequence cannot break appends.
        """
        try:
            newline_count = 0
            last_byte = b""
            with path.open("rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    newline_count += chunk.count(b"\n")
                    last_byte = chunk[-1:]
        except FileNotFoundError:
            return 0, ""
        except OSError:
            return 0, ""
        if not last_byte:
            return 0, ""
        line_count = newline_count if last_byte == b"\n" else newline_count + 1
        prefix = "" if last_byte == b"\n" else "\n"
        return line_count, prefix

    def _kanban_dir(self) -> Path:
        return self.root / "_kanban"

    def _kanban_path(self, card_id: str) -> Path:
        return self._kanban_dir() / f"{card_id}.json"

    def _kanban_events_path(self, card_id: str) -> Path:
        return self._kanban_dir() / f"{card_id}.events.jsonl"

    # -- internals --------------------------------------------------------
    def _load_meta_unlocked(self, run_id: str, *, missing_ok: bool) -> Optional[ScriptRunMeta]:
        path = self._meta_path(run_id)
        # Read directly instead of gating on path.exists(): a check-then-read
        # races a concurrent finish()/rmtree, and a file that vanishes between the
        # two would escape as a bare OSError. Map FileNotFoundError to the typed
        # missing/not-found outcome and any other OSError to corrupt_run.
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            if missing_ok:
                return None
            raise ScriptRunNotFound(run_id) from None
        except OSError as exc:
            raise CorruptScriptRunError(run_id, "corrupt_run", f"run.json: {exc}") from exc
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CorruptScriptRunError(run_id, "corrupt_run", f"run.json: {exc.msg}") from exc
        if not isinstance(data, dict):
            raise CorruptScriptRunError(run_id, "corrupt_run", "run.json is not an object")
        version = data.get("schema_version")
        if version != SCRIPT_SCHEMA_VERSION:
            raise CorruptScriptRunError(
                run_id, "schema_version",
                f"run.json schema_version {version!r} != {SCRIPT_SCHEMA_VERSION}",
            )
        return ScriptRunMeta(
            run_id=data.get("run_id", run_id),
            script_sha256=data.get("script_sha256", ""),
            args_hash=data.get("args_hash", ""),
            status=data.get("status", "running"),
            meta=data.get("meta"),
            limits=data.get("limits"),
            value=data.get("value"),
            error=data.get("error"),
            deterministic_runner=bool(data.get("deterministic_runner", False)),
            replay_of=data.get("replay_of"),
            schema_version=version,
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
        )

    def _write_meta(self, record: ScriptRunMeta) -> None:
        run_dir = self._run_dir(record.run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(record.to_dict(), ensure_ascii=False, indent=2) + "\n"
        tmp = run_dir / "run.json.tmp"
        with tmp.open("w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._meta_path(record.run_id))
        _fsync_dir(run_dir)

    def _append_journal(self, run_id: str, event_type: str, data: dict[str, Any]) -> None:
        event = {"ts": utc_now_iso(), "type": event_type, "run_id": run_id, **data}
        with self._journal_path(run_id).open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _run_dir(self, run_id: str) -> Path:
        _require_safe_run_id(run_id)
        return self.root / run_id

    def _meta_path(self, run_id: str) -> Path:
        return self._run_dir(run_id) / "run.json"

    def _journal_path(self, run_id: str) -> Path:
        return self._run_dir(run_id) / "journal.jsonl"

    def _cache_path(self, run_id: str) -> Path:
        return self._run_dir(run_id) / "cache.jsonl"


# A journal sink the VM accepts is just ``Callable[[dict], None]``; the store's
# ``note_call`` is adapted into one by :mod:`hermes_workflows.vm`.
JournalSink = Callable[[dict[str, Any]], None]
