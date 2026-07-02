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
from typing import Any, Callable, Literal, Optional, Protocol, runtime_checkable

from .errors import CorruptScriptRunError, ScriptRunNotFound, ScriptRunStoreError
from .registry import utc_now_iso
from .script_validator import normalize_meta_phases

__all__ = [
    "SCRIPT_SCHEMA_VERSION",
    "JournalDurability",
    "JOURNAL_DURABILITY_MODES",
    "ScriptRunMeta",
    "ReplayEntry",
    "PromptReplayEntry",
    "ReplayCache",
    "CallRecorderProtocol",
    "TranscriptRecorderProtocol",
    "ScriptRunStoreProtocol",
    "CallRecorder",
    "TranscriptRecorder",
    "ScriptRunStore",
    "FileScriptRunStore",
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

# Latest-state writes use this to prevent stale wait/block markers from landing after
# a completed/failed resolution and making an already-finished card appear to
# regress it (see ``record_kanban_card_state``).
_KANBAN_TERMINAL_STATUSES = frozenset({"completed", "failed"})
_KANBAN_ALIAS_STATUS = "alias"
_KANBAN_NON_WAIT_STATUSES = _KANBAN_TERMINAL_STATUSES | {_KANBAN_ALIAS_STATUS}

# Methods whose result is a deterministic constant regardless of any runner:
# both ``log`` and ``phase`` are pure side-effect metadata and return ``None``.
_ALWAYS_REPLAYABLE = frozenset({"log", "phase"})
# Methods that cross the AgentRunner boundary: replayable only when the runner
# is declared deterministic for the run.
_RUNNER_METHODS = frozenset({"agent", "kanban_agent"})

# Run-journal (``journal.jsonl``) durability policy (issue #108). ``sync`` fsyncs
# every event (today's behavior, and the default — zero change unless an
# embedder opts in). ``async`` buffers events in memory and fsyncs them in one
# batch every ``async_flush_every`` events (a deterministic *count* trigger —
# never a wall-clock interval, per the module's no-wall-clock contract).
# ``exit`` buffers every event and only reaches disk on the run's terminal
# force-flush. Suspend/finish/abort always force-flush the buffer regardless of
# mode (see :meth:`ScriptRunStore.finish`); only an actual process crash before
# that point can lose buffered ``async``/``exit`` events. Never applies to
# ``run.json`` (always fsynced) or the per-card kanban event log.
JournalDurability = Literal["exit", "async", "sync"]
JOURNAL_DURABILITY_MODES: frozenset[str] = frozenset({"exit", "async", "sync"})
_DEFAULT_JOURNAL_DURABILITY: JournalDurability = "sync"
_DEFAULT_ASYNC_FLUSH_EVERY = 8


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

    The cosmetic ``label`` (display-only for built-in agent calls) is excluded so
    a relabelled-but-equivalent call still replays. Generic host capabilities
    receive ``label`` in handler context, so their full params participate.
    """
    if method == "capability":
        keyed = dict(params)
    else:
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
    ``code`` / ``line`` / ``retryable`` (issue #103), honoring the journal's
    metadata-only contract.
    """
    if not isinstance(error, dict):
        return error
    return {k: error[k] for k in ("type", "code", "line", "retryable") if k in error}


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
    status: str = "running"  # running | succeeded | failed | suspended
    meta: Optional[dict[str, Any]] = None
    limits: Optional[dict[str, Any]] = None
    value: Any = None
    error: Optional[dict[str, Any]] = None
    deterministic_runner: bool = False
    replay_of: Optional[str] = None
    transcripts: Optional[dict[str, Any]] = None
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
            "phases": self.phases,
            "limits": self.limits,
            "value": self.value,
            "error": self.error,
            "deterministic_runner": self.deterministic_runner,
            "replay_of": self.replay_of,
            "transcripts": self.transcripts,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @property
    def phases(self) -> list[dict[str, str]]:
        """Validated/normalized phase declarations exposed as progress metadata."""
        return normalize_meta_phases(self.meta)


@dataclass(frozen=True)
class ReplayEntry:
    """One cached deterministic call outcome, addressed by its stable call id.

    ``ok=True`` (the default, and the only shape written before issue #103) is a
    successful result cached for replay. ``ok=False`` (issue #103) is a
    *retryable* dispatch failure (``code="runner_error"``) that a script caught
    and handled on the source run: recorded metadata-only (``code``/
    ``retryable``, never a payload — ``value`` is unused) so replay reproduces
    the identical :class:`~hermes_workflows.errors.CapabilityDenied` instead of
    silently re-dispatching live against a runner that may now behave
    differently, which would let the replayed run's outcome diverge from the
    source run's for a failure the script had already observed and handled.
    """

    call_id: int
    method: str
    args_hash: str
    value: Any = None
    ok: bool = True
    code: Optional[str] = None
    retryable: bool = False


@dataclass(frozen=True)
class PromptReplayEntry:
    """One completed prompt-agent result, addressed by its semantic fingerprint."""

    fingerprint: str
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

    def __init__(
        self,
        entries: dict[int, ReplayEntry],
        *,
        source_run_id: str,
        prompt_entries: Optional[dict[str, PromptReplayEntry]] = None,
    ) -> None:
        self._entries = entries
        self._prompt_entries = prompt_entries or {}
        self.source_run_id = source_run_id

    def get(self, call_id: Any) -> Optional[ReplayEntry]:
        # ``isinstance(True, int)`` is True, so reject bools explicitly: a call id
        # of ``True``/``False`` must not alias to cached call ids 1/0.
        if not isinstance(call_id, int) or isinstance(call_id, bool):
            return None
        return self._entries.get(call_id)

    def get_prompt(self, fingerprint: Any) -> Optional[PromptReplayEntry]:
        """Return a prompt-agent cache entry by ``v2:<hash>`` fingerprint."""
        if not isinstance(fingerprint, str):
            return None
        return self._prompt_entries.get(fingerprint)

    def __len__(self) -> int:
        return len(self._entries) + len(self._prompt_entries)


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

    def record_failure(self, call_id: Any, method: str, args_hash: str, code: str, retryable: bool) -> None:
        """Append one cached *retryable* dispatch-failure outcome (issue #103).

        Metadata-only — ``call_id``/``method``/``args_hash``/``code``/
        ``retryable``, never a payload — so a caught-and-handled transient
        runner failure replays to the identical classification instead of
        silently re-dispatching live. Mirrors :meth:`record`'s durability
        (append + fsync).
        """
        line = json.dumps(
            {
                "call_id": call_id, "method": method, "args_hash": args_hash,
                "ok": False, "code": code, "retryable": retryable,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        with self._lock:
            with self._path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()
                os.fsync(f.fileno())

    def record_prompt(self, fingerprint: str, method: str, args_hash: str, value: Any) -> None:
        """Append one prompt-agent fingerprint cache entry."""
        line = json.dumps(
            {"fingerprint": fingerprint, "method": method, "args_hash": args_hash, "value": value},
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        with self._lock:
            with self._path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()
                os.fsync(f.fileno())


def _agent_ref(call_id: Any) -> str:
    """Stable, single-segment artifact stem for one brokered subagent call."""
    if isinstance(call_id, int) and not isinstance(call_id, bool) and call_id >= 0:
        return f"agent-{call_id:06d}"
    digest = hashlib.sha256(str(call_id).encode("utf-8", "surrogatepass")).hexdigest()[:12]
    return f"agent-{digest}"


def _safe_text(value: Any, *, max_len: int = 128) -> Optional[str]:
    """Small metadata-safe string coercion; never used for prompts or inputs."""
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = value.replace("\x00", "").strip()
    if not value:
        return None
    return value[:max_len]


def _safe_non_negative_int(value: Any) -> Optional[int]:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def _spawn_depth(params: dict[str, Any]) -> int:
    context = params.get("context") if isinstance(params.get("context"), dict) else {}
    value = context.get("spawn_depth") if isinstance(context, dict) else None
    depth = _safe_non_negative_int(value)
    return depth if depth is not None else 1


def _agent_type(method: str, params: dict[str, Any]) -> str:
    if method == "agent" and "prompt" in params:
        return "prompt_agent"
    if method == "kanban_agent":
        return "kanban_agent"
    return "agent"


def _result_keys(value: Any) -> list[str]:
    if not isinstance(value, dict):
        return []
    return sorted(str(k) for k in value.keys())


def _usage_counts(value: Any) -> tuple[Optional[int], Optional[int]]:
    if not isinstance(value, dict):
        return None, None
    token_count = _safe_non_negative_int(value.get("_tokens"))
    tool_count = _safe_non_negative_int(value.get("_tool_calls"))
    if tool_count is None:
        tool_count = _safe_non_negative_int(value.get("_tool_count"))
    return token_count, tool_count


class TranscriptRecorder:
    """Append-only writer for per-subagent transcript artifacts (issue #76)."""

    def __init__(self, run_dir: Path, lock: threading.Lock) -> None:
        self._dir = run_dir / "transcripts"
        self._lock = lock

    def refs(self) -> dict[str, Any]:
        agents: list[dict[str, Any]] = []
        if self._dir.exists():
            for path in sorted(self._dir.glob("agent-*.meta.json")):
                try:
                    meta = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if not isinstance(meta, dict):
                    continue
                agent_ref = str(meta.get("id") or path.name[:-10])
                ref: dict[str, Any] = {
                    "id": agent_ref,
                    "transcript_path": str(self._dir / f"{agent_ref}.jsonl"),
                    "meta_path": str(path),
                    "state": str(meta.get("state") or "unknown"),
                }
                for key in ("label", "phase"):
                    if meta.get(key) is not None:
                        ref[key] = meta[key]
                agents.append(ref)
        return {"dir": str(self._dir), "journal_path": str(self._dir / "journal.jsonl"), "agents": agents}

    def started(self, call_id: Any, method: str, params: dict[str, Any], *, started_at: str) -> str:
        agent_ref = _agent_ref(call_id)
        base = self._base_meta(agent_ref, call_id, method, params)
        meta = {**base, "state": "running", "started_at": started_at, "completed_at": None, "duration_ms": None}
        event = {**base, "event": "started", "state": "running", "started_at": started_at}
        with self._lock:
            self._dir.mkdir(parents=True, exist_ok=True)
            self._append_locked(self._journal_path(), event)
            self._append_locked(self._agent_path(agent_ref), event)
            self._write_meta_locked(agent_ref, meta)
        return agent_ref

    def result(
        self,
        call_id: Any,
        method: str,
        params: dict[str, Any],
        *,
        started_at: str,
        completed_at: str,
        duration_ms: int,
        value: Any,
        state: str = "succeeded",
        event_name: str = "result",
    ) -> str:
        agent_ref = _agent_ref(call_id)
        token_count, tool_count = _usage_counts(value)
        base = self._base_meta(agent_ref, call_id, method, params)
        update = {
            "state": state,
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_ms": max(0, int(duration_ms)),
            "token_count": token_count,
            "tool_count": tool_count,
        }
        meta = {**base, **update}
        event = {**base, "event": event_name, **update, "result_keys": _result_keys(value)}
        with self._lock:
            self._dir.mkdir(parents=True, exist_ok=True)
            self._append_locked(self._journal_path(), event)
            self._append_locked(self._agent_path(agent_ref), event)
            self._write_meta_locked(agent_ref, meta)
        return agent_ref

    def error(
        self,
        call_id: Any,
        method: str,
        params: dict[str, Any],
        *,
        started_at: str,
        completed_at: str,
        duration_ms: int,
        error_code: Optional[str],
    ) -> str:
        agent_ref = _agent_ref(call_id)
        base = self._base_meta(agent_ref, call_id, method, params)
        update = {
            "state": "failed",
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_ms": max(0, int(duration_ms)),
            "error_code": _safe_text(error_code),
        }
        meta = {**base, **update, "token_count": None, "tool_count": None}
        event = {**base, "event": "error", **update}
        with self._lock:
            self._dir.mkdir(parents=True, exist_ok=True)
            self._append_locked(self._journal_path(), event)
            self._append_locked(self._agent_path(agent_ref), event)
            self._write_meta_locked(agent_ref, meta)
        return agent_ref

    def cache_hit(self, call_id: Any, method: str, params: dict[str, Any], *, at: str, value: Any) -> str:
        return self.result(
            call_id,
            method,
            params,
            started_at=at,
            completed_at=at,
            duration_ms=0,
            value=value,
            state="cache_hit",
            event_name="cache-hit",
        )

    def _base_meta(self, agent_ref: str, call_id: Any, method: str, params: dict[str, Any]) -> dict[str, Any]:
        base: dict[str, Any] = {
            "id": agent_ref,
            "agent_ref": agent_ref,
            "call_id": call_id if isinstance(call_id, int) and not isinstance(call_id, bool) else None,
            "method": method,
            "agent_type": _agent_type(method, params),
            "spawn_depth": _spawn_depth(params),
            "transcript_path": str(self._agent_path(agent_ref)),
            "meta_path": str(self._meta_path(agent_ref)),
        }
        for key in ("label", "phase", "model"):
            text = _safe_text(params.get(key))
            if text is not None:
                base[key] = text
        if method == "agent" and "prompt" not in params:
            agent_id = _safe_text(params.get("agent_id"))
            if agent_id is not None:
                base["agent_id"] = agent_id
        if method == "kanban_agent":
            profile = _safe_text(params.get("profile"))
            if profile is not None:
                base["profile"] = profile
        return base

    def _append_locked(self, path: Path, event: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, separators=(",", ":"), default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _write_meta_locked(self, agent_ref: str, meta: dict[str, Any]) -> None:
        payload = json.dumps(meta, ensure_ascii=False, indent=2, default=str) + "\n"
        tmp = self._dir / f"{agent_ref}.meta.json.tmp"
        with tmp.open("w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._meta_path(agent_ref))
        _fsync_dir(self._dir)

    def _journal_path(self) -> Path:
        return self._dir / "journal.jsonl"

    def _agent_path(self, agent_ref: str) -> Path:
        return self._dir / f"{agent_ref}.jsonl"

    def _meta_path(self, agent_ref: str) -> Path:
        return self._dir / f"{agent_ref}.meta.json"


@runtime_checkable
class CallRecorderProtocol(Protocol):
    """Contract for a run's append-only deterministic replay-cache writer.

    :class:`CallRecorder` (file backend) and the SQLite adapter's recorder both
    satisfy this structurally. Every method is durable the instant it returns
    (cache writes are always immediately committed, independent of a store's
    journal ``durability`` mode — see :class:`ScriptRunStoreProtocol`).
    """

    def record(self, call_id: Any, method: str, args_hash: str, value: Any) -> None:
        """Persist one successful, replayable call outcome."""
        ...

    def record_failure(self, call_id: Any, method: str, args_hash: str, code: str, retryable: bool) -> None:
        """Persist one cached *retryable* dispatch-failure outcome (issue #103)."""
        ...

    def record_prompt(self, fingerprint: str, method: str, args_hash: str, value: Any) -> None:
        """Persist one completed prompt-agent result keyed by fingerprint."""
        ...


@runtime_checkable
class TranscriptRecorderProtocol(Protocol):
    """Contract for a run's per-subagent transcript artifact writer (issue #76)."""

    def refs(self) -> dict[str, Any]:
        """Return artifact refs/paths for every recorded subagent, without content."""
        ...

    def started(self, call_id: Any, method: str, params: dict[str, Any], *, started_at: str) -> str:
        ...

    def result(
        self,
        call_id: Any,
        method: str,
        params: dict[str, Any],
        *,
        started_at: str,
        completed_at: str,
        duration_ms: int,
        value: Any,
        state: str = "succeeded",
        event_name: str = "result",
    ) -> str:
        ...

    def error(
        self,
        call_id: Any,
        method: str,
        params: dict[str, Any],
        *,
        started_at: str,
        completed_at: str,
        duration_ms: int,
        error_code: Optional[str],
    ) -> str:
        ...

    def cache_hit(self, call_id: Any, method: str, params: dict[str, Any], *, at: str, value: Any) -> str:
        ...


@runtime_checkable
class ScriptRunStoreProtocol(Protocol):
    """The durable persistence contract every ``ScriptRunStore`` backend implements.

    Extracted (issue #110) from the file-backed implementation that used to be
    the *only* backend, so a pluggable backend (e.g. the SQLite adapter in
    :mod:`hermes_workflows.script_store_sqlite`) can be verified structurally
    against the same surface the rest of the package (``vm.py``, ``kanban.py``,
    ``background.py``, the resume/replay path) actually calls. Grouped by the
    concern each method covers:

    * **Run lifecycle** — ``next_run_id`` / ``begin`` / ``finish`` / ``load_run``.
    * **Metadata-only journal** — ``note_call`` (append) / ``journal`` (iterate,
      most-recent-``limit``) / ``journal_path`` (operator-facing pointer).
    * **Deterministic replay cache** — ``recorder`` (returns a
      :class:`CallRecorderProtocol`) / ``load_cache``.
    * **Transcript artifacts** (issue #76) — ``transcript_recorder`` (returns a
      :class:`TranscriptRecorderProtocol`) / ``transcript_refs``.
    * **Suspended-run index** (issue #5) — ``suspended_runs``.
    * **Durable Kanban card state + event log** (issue #5) —
      ``record_kanban_card_state`` / ``load_kanban_card_state`` / ``kanban_waits``
      / ``append_kanban_event`` / ``read_kanban_events`` / ``latest_kanban_resolution``.

    A backend also exposes a ``root: Path`` attribute (the store's directory;
    some backends, like SQLite, keep all relational state in one file under it)
    and a constructor accepting ``root`` plus the issue #108 ``durability`` /
    ``async_flush_every`` knobs. The constructor is not part of the structural
    Protocol (``__init__`` isn't checked by ``isinstance``), but every backend
    must accept the same keyword arguments so callers can swap backends by
    changing only which class they construct.

    Every load failure across every backend is a typed
    :class:`~hermes_workflows.errors.ScriptRunStoreError` subclass
    (:class:`~hermes_workflows.errors.ScriptRunNotFound` /
    :class:`~hermes_workflows.errors.CorruptScriptRunError`) — never a bare
    backend-specific exception (``OSError``, ``sqlite3.Error``, ...).
    """

    root: Path

    def next_run_id(self, script: str, args: Any = None) -> str: ...

    def begin(
        self,
        run_id: str,
        *,
        script: str,
        args: Any,
        limits: Optional[dict[str, Any]],
        deterministic_runner: bool,
        meta: Optional[dict[str, Any]] = None,
        replay_of: Optional[str] = None,
    ) -> "ScriptRunMeta": ...

    def note_call(self, run_id: str, event: dict[str, Any]) -> None: ...

    def recorder(self, run_id: str) -> CallRecorderProtocol: ...

    def transcript_recorder(self, run_id: str) -> TranscriptRecorderProtocol: ...

    def finish(
        self,
        run_id: str,
        *,
        status: str,
        meta: Optional[dict[str, Any]],
        value: Any,
        error: Optional[dict[str, Any]],
    ) -> None: ...

    def load_run(self, run_id: str) -> "ScriptRunMeta": ...

    def load_cache(self, run_id: str) -> "ReplayCache": ...

    def journal(self, run_id: str, *, limit: int = 200) -> list[dict[str, Any]]: ...

    def journal_path(self, run_id: str) -> Path: ...

    def transcript_refs(self, run_id: str) -> dict[str, Any]: ...

    def suspended_runs(self) -> list["ScriptRunMeta"]: ...

    def record_kanban_card_state(self, card_id: str, state: dict[str, Any]) -> None: ...

    def load_kanban_card_state(self, card_id: str) -> Optional[dict[str, Any]]: ...

    def kanban_waits(self) -> list[dict[str, Any]]: ...

    def append_kanban_event(
        self,
        card_id: str,
        *,
        status: str,
        result: Optional[dict[str, Any]] = None,
        reason: Optional[str] = None,
        profile: str = "",
    ) -> dict[str, Any]: ...

    def read_kanban_events(self, card_id: str, *, after_seq: int = 0) -> list[dict[str, Any]]: ...

    def latest_kanban_resolution(self, card_id: str) -> Optional[dict[str, Any]]: ...


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

    ``durability`` (issue #108) governs only the ``journal.jsonl`` write policy
    (never ``run.json``, which is always fsynced, and never the per-card kanban
    event log): ``"sync"`` (default) fsyncs every event — today's behavior,
    unchanged unless an embedder opts in. ``"async"`` buffers events in memory
    and fsyncs them together every ``async_flush_every`` events (a deterministic
    *count* trigger, never a wall-clock interval). ``"exit"`` buffers every
    event and only reaches disk on the run's terminal force-flush. Regardless of
    mode, :meth:`finish` (suspend, succeed, fail, stop, pause — every terminal
    status) always force-flushes any buffered events before returning.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        durability: JournalDurability = _DEFAULT_JOURNAL_DURABILITY,
        async_flush_every: int = _DEFAULT_ASYNC_FLUSH_EVERY,
    ) -> None:
        if durability not in JOURNAL_DURABILITY_MODES:
            raise ValueError(
                f"unsupported journal durability: {durability!r} (expected one of "
                f"{sorted(JOURNAL_DURABILITY_MODES)})"
            )
        if not isinstance(async_flush_every, int) or isinstance(async_flush_every, bool) or async_flush_every < 1:
            raise ValueError(f"async_flush_every must be a positive int, got {async_flush_every!r}")
        self._durability: JournalDurability = durability
        self._async_flush_every = async_flush_every
        # Buffered, not-yet-fsynced journal lines per run id (``async``/``exit``
        # modes only; always empty in ``sync`` mode). Only touched under
        # ``self._lock``.
        self._journal_buffer: dict[str, list[str]] = {}
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
        meta: Optional[dict[str, Any]] = None,
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
            meta=meta,
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
        event_type = event.get("type")
        data = {
            "call_id": event.get("call_id"),
            "method": event.get("method"),
            "ok": event.get("ok"),
        }
        for key in (
            "agent_id", "profile", "capability", "label", "phase", "phase_title", "parallel_index",
            "pipeline_item_index", "pipeline_stage_index",
            "fingerprint", "error", "retryable", "replayed", "cache", "has_value", "attempt", "max_retries",
            "dropped_context_keys",
        ):
            if event.get(key) is not None:
                data[key] = event.get(key)
        with self._lock:
            if event_type in ("agent_started", "agent_result", "agent_cache_hit"):
                self._append_journal(run_id, event_type, data)
            else:
                self._append_journal(run_id, "call", data)

    def recorder(self, run_id: str) -> CallRecorder:
        """Return the append-only replay-cache writer for ``run_id``."""
        _require_safe_run_id(run_id)
        return CallRecorder(self._cache_path(run_id), self._lock)

    def transcript_recorder(self, run_id: str) -> TranscriptRecorder:
        """Return the per-subagent transcript artifact writer for ``run_id``."""
        _require_safe_run_id(run_id)
        return TranscriptRecorder(self._run_dir(run_id), self._lock)

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
            refs = self.transcript_refs(run_id)
            record.transcripts = refs if refs.get("agents") else None
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
            # Every terminal status (succeeded/failed/suspended/stopped/paused)
            # force-flushes the journal regardless of durability mode (issue
            # #108): a run that is about to go quiet must not leave events
            # sitting unflushed in memory.
            self._flush_journal_locked(run_id)

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
        prompt_entries: dict[str, PromptReplayEntry] = {}
        if not path.exists():
            return ReplayCache(entries, source_run_id=run_id, prompt_entries=prompt_entries)
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
                if not isinstance(obj, dict):
                    raise CorruptScriptRunError(
                        run_id, "corrupt_cache", f"cache.jsonl line {lineno}: bad entry shape"
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
                if "call_id" in obj:
                    call_id_raw = obj.get("call_id")
                    # ``isinstance(True, int)`` is True, so exclude bools explicitly:
                    # a forged ``call_id: true`` must not be accepted as call id 1.
                    if not isinstance(call_id_raw, int) or isinstance(call_id_raw, bool):
                        raise CorruptScriptRunError(
                            run_id, "corrupt_cache", f"cache.jsonl line {lineno}: bad entry shape"
                        )
                    call_id = obj["call_id"]
                    if call_id in entries:
                        raise CorruptScriptRunError(
                            run_id, "corrupt_cache", f"duplicate cached call id {call_id}"
                        )
                    # "ok" is absent on every line written before issue #103 (all of
                    # which are successful results) and on every success line since,
                    # so absence means True — a fail-closed reader would reject the
                    # entire pre-existing on-disk format.
                    ok = obj.get("ok", True)
                    if not isinstance(ok, bool):
                        raise CorruptScriptRunError(
                            run_id, "corrupt_cache", f"cache.jsonl line {lineno}: ok must be a bool"
                        )
                    code: Optional[str] = None
                    retryable = False
                    if not ok:
                        code = obj.get("code")
                        retryable = obj.get("retryable", False)
                        if not isinstance(code, str) or not code:
                            raise CorruptScriptRunError(
                                run_id, "corrupt_cache",
                                f"cache.jsonl line {lineno}: failed entry requires a non-empty 'code'",
                            )
                        if not isinstance(retryable, bool):
                            raise CorruptScriptRunError(
                                run_id, "corrupt_cache", f"cache.jsonl line {lineno}: retryable must be a bool"
                            )
                    entries[call_id] = ReplayEntry(
                        call_id=call_id,
                        method=method,
                        args_hash=args_hash,
                        value=obj.get("value"),
                        ok=ok,
                        code=code,
                        retryable=retryable,
                    )
                    continue
                fingerprint = obj.get("fingerprint")
                if not isinstance(fingerprint, str) or not fingerprint.startswith("v2:"):
                    raise CorruptScriptRunError(
                        run_id, "corrupt_cache", f"cache.jsonl line {lineno}: bad entry shape"
                    )
                if fingerprint in prompt_entries:
                    raise CorruptScriptRunError(
                        run_id, "corrupt_cache", f"duplicate cached prompt fingerprint {fingerprint}"
                    )
                prompt_entries[fingerprint] = PromptReplayEntry(
                    fingerprint=fingerprint,
                    method=method,
                    args_hash=args_hash,
                    value=obj.get("value"),
                )
        return ReplayCache(entries, source_run_id=run_id, prompt_entries=prompt_entries)

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

    def transcript_refs(self, run_id: str) -> dict[str, Any]:
        """Return transcript artifact refs/paths without loading transcript content."""
        _require_safe_run_id(run_id)
        return self.transcript_recorder(run_id).refs()

    def suspended_runs(self) -> list[ScriptRunMeta]:
        """Return runs durably suspended on an unresolved paused Kanban await (issue #5).

        Operator/resumer-facing discovery: a fresh process scans for suspended
        runs, checks each awaited card's durable event log (the ``error`` field
        carries the metadata-safe ``card_id``/``profile``), and resumes the ready
        ones via ``run_workflow_script(..., replay_from=<run_id>)``. A corrupt or
        unreadable run record is skipped (a resume can still be driven by run id
        directly); non-run entries (e.g. the ``_kanban`` dir, which fails the
        run-id guard) are ignored.
        """
        if not self.root.exists():
            return []
        out: list[ScriptRunMeta] = []
        for child in sorted(self.root.iterdir()):
            if not child.is_dir() or not _RUN_ID_RE.fullmatch(child.name):
                continue
            try:
                meta = self.load_run(child.name)
            except (ScriptRunStoreError, ValueError):
                continue
            if meta.status == "suspended":
                out.append(meta)
        return out

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
            if "run_id" not in record and isinstance(existing, dict) and existing.get("run_id"):
                record["run_id"] = existing["run_id"]
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
            if isinstance(data, dict) and data.get("status") not in _KANBAN_NON_WAIT_STATUSES:
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
        except (OSError, UnicodeDecodeError) as exc:
            # UnicodeDecodeError is a ValueError, not an OSError, so a run.json with
            # invalid UTF-8 bytes would otherwise escape load_run as a bare
            # exception (and crash a bulk scan like suspended_runs); map it to the
            # same typed corrupt_run as any other unreadable record.
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
            transcripts=data.get("transcripts") if isinstance(data.get("transcripts"), dict) else None,
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
        """Append one journal event, applying the store's durability policy.

        Called with ``self._lock`` already held by every caller (:meth:`begin`,
        :meth:`note_call`, :meth:`finish`). ``sync`` (the default) is written and
        fsynced immediately — byte-for-byte the store's original behavior.
        ``async``/``exit`` instead buffer the serialized line in memory;
        ``async`` additionally force-flushes every ``async_flush_every`` events
        (a deterministic count trigger, never a wall-clock interval).
        """
        event = {"ts": utc_now_iso(), "type": event_type, "run_id": run_id, **data}
        line = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
        if self._durability == "sync":
            self._write_journal_lines(run_id, [line])
            return
        buffer = self._journal_buffer.setdefault(run_id, [])
        buffer.append(line)
        if self._durability == "async" and len(buffer) >= self._async_flush_every:
            self._flush_journal_locked(run_id)

    def _write_journal_lines(self, run_id: str, lines: list[str]) -> None:
        """Append+fsync ``lines`` to ``run_id``'s journal in a single write."""
        if not lines:
            return
        with self._journal_path(run_id).open("a", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _flush_journal_locked(self, run_id: str) -> None:
        """Force any buffered ``async``/``exit`` journal events to disk.

        A no-op in ``sync`` mode (nothing is ever buffered) and a no-op if
        nothing is pending. Requires ``self._lock`` already held. The buffer is
        popped only after the write succeeds, so a transient disk failure during
        a force-flush leaves the pending lines intact for a retried ``finish()``.
        """
        pending = self._journal_buffer.get(run_id)
        if pending:
            self._write_journal_lines(run_id, pending)
            self._journal_buffer.pop(run_id, None)

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

# ``ScriptRunStore`` is the file backend and remains the default/public name for
# backward compatibility (every existing embedder constructs it directly). This
# alias names it explicitly now that :mod:`hermes_workflows.script_store_sqlite`
# adds a second backend satisfying the same :class:`ScriptRunStoreProtocol`
# (issue #110) — pick the constructor for the backend you want; nothing selects
# a backend implicitly, and the file backend's behavior is unchanged.
FileScriptRunStore = ScriptRunStore
