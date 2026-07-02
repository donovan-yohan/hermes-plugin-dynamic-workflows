"""SQLite adapter for the durable script-run store contract (issue #110).

:mod:`hermes_workflows.script_store` grew a filesystem-only backend
(``ScriptRunStore``) — the local-host durability substrate for subprocess
workflow-script runs (issue #3), the deterministic replay cache, and the
durable Kanban await mechanism (issue #5). That module's public surface was
extracted into an explicit structural contract,
:class:`~hermes_workflows.script_store.ScriptRunStoreProtocol`. This module is
the first additional backend against that contract: everything durable lives
in **one SQLite database file per store root** (``<root>/store.sqlite3``)
instead of one directory-of-files per run.

**Why SQLite, and what stays on the filesystem.** The stdlib ``sqlite3``
module keeps this a zero-dependency addition (the project's hard constraint).
Per-subagent transcript *artifacts* (issue #76) are the one piece deliberately
left on the filesystem even for this backend: they are large-ish,
append-mostly, non-relational blobs the existing
:class:`~hermes_workflows.script_store.TranscriptRecorder` already writes
generically to any directory, so this adapter reuses it unmodified, pointed at
``<root>/<run_id>/transcripts/``. Everything else this module's docstring
enumerates as the store's real contract — run metadata, the metadata-only
journal, the deterministic replay cache, the suspended-run index, and the
durable Kanban card state/event log — lives in SQL tables.

**Schema versioning.** The database's ``PRAGMA user_version`` records the
schema generation (starts at 1). A fresh (empty) database is initialized and
stamped with the current version. Opening a database stamped with a
*different, non-zero* version raises a typed
:class:`~hermes_workflows.errors.CorruptScriptRunError` (``reason=
"schema_version"``) at construction — the same fail-closed contract
``load_run`` already applies to a stale ``run.json``, just checked once at
open time instead of per record, since SQLite's schema is store-wide rather
than per-run. See DESIGN.md's "SQLite store backend" section for the
migration story once a second schema generation ships.

**Durability modes onto transaction boundaries (issue #108).** ``run.json``'s
role (always-immediate) is played here by the ``runs`` table: every
``begin()``/``finish()`` write commits immediately, in every durability mode
— mirroring the file backend's ``run.json`` being always-fsynced regardless
of the journal knob. The replay cache (``cache_calls`` / ``cache_prompts``)
and the durable Kanban card state/event log are likewise always
immediately committed (their file-backend counterparts always fsync on
every write, independent of ``durability``). Only the ``journal`` table
follows the knob, exactly like ``journal.jsonl``:

* ``"sync"`` (default) — every journal row is inserted **and committed**
  before the call returns.
* ``"async"`` — journal rows are buffered in memory and committed together
  (one ``BEGIN``/``INSERT ... executemany``/``COMMIT``) once
  ``async_flush_every`` events have accumulated — a deterministic *count*
  trigger, never a wall-clock timer.
* ``"exit"`` — journal rows are buffered and only committed at the run's
  terminal ``finish()``.

Regardless of mode, :meth:`SqliteScriptRunStore.finish` always force-commits
any buffered journal rows before returning, so suspend/succeed/fail/stop/pause
all get the same durability guarantee as the file backend's ``finish()``.

This module is pure Python 3.11 stdlib (``sqlite3``).
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Optional

from .errors import CorruptScriptRunError, ScriptRunNotFound, ScriptRunStoreError
from .registry import utc_now_iso
from .script_store import (
    JOURNAL_DURABILITY_MODES,
    SCRIPT_SCHEMA_VERSION,
    JournalDurability,
    PromptReplayEntry,
    ReplayCache,
    ReplayEntry,
    ScriptRunMeta,
    TranscriptRecorder,
    _KANBAN_NON_WAIT_STATUSES,
    _KANBAN_TERMINAL_STATUSES,
    _require_safe_card_id,
    _require_safe_run_id,
    canonical_hash,
    script_run_id,
    script_sha256,
)

__all__ = [
    "SQLITE_STORE_SCHEMA_VERSION",
    "SqliteScriptRunStore",
    "SqliteCallRecorder",
]

# Bumped when the SQLite table layout changes incompatibly. Independent of
# ``SCRIPT_SCHEMA_VERSION`` (the per-record ``run.json``/``runs`` row schema,
# checked separately on every ``load_run``): this one gates the *database's*
# shape as a whole, checked once at open time via ``PRAGMA user_version``.
SQLITE_STORE_SCHEMA_VERSION = 1

_DEFAULT_JOURNAL_DURABILITY: JournalDurability = "sync"
_DEFAULT_ASYNC_FLUSH_EVERY = 8

_SCHEMA_DDL = """
CREATE TABLE runs (
    run_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    script_sha256 TEXT NOT NULL,
    args_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    meta_json TEXT,
    limits_json TEXT,
    value_json TEXT,
    error_json TEXT,
    deterministic_runner INTEGER NOT NULL,
    replay_of TEXT,
    transcripts_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE journal (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    data_json TEXT NOT NULL,
    PRIMARY KEY (run_id, seq)
);

CREATE TABLE cache_calls (
    run_id TEXT NOT NULL,
    call_id INTEGER NOT NULL,
    method TEXT NOT NULL,
    args_hash TEXT NOT NULL,
    value_json TEXT,
    ok INTEGER NOT NULL,
    code TEXT,
    retryable INTEGER NOT NULL,
    PRIMARY KEY (run_id, call_id)
);

CREATE TABLE cache_prompts (
    run_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    method TEXT NOT NULL,
    args_hash TEXT NOT NULL,
    value_json TEXT,
    PRIMARY KEY (run_id, fingerprint)
);

CREATE TABLE kanban_card_state (
    card_id TEXT PRIMARY KEY,
    state_json TEXT NOT NULL
);

CREATE TABLE kanban_events (
    card_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    ts TEXT NOT NULL,
    status TEXT NOT NULL,
    workflow_result_json TEXT NOT NULL,
    reason TEXT,
    profile TEXT NOT NULL,
    PRIMARY KEY (card_id, seq)
);
"""


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _loads_or_corrupt(run_id: str, raw: Optional[str], *, where: str, reason: str = "corrupt_run") -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CorruptScriptRunError(run_id, reason, f"{where}: {exc.msg}") from exc


def _redact_error(error: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Keep only metadata-safe error fields for the durable journal.

    Mirrors :func:`hermes_workflows.script_store._redact_error` exactly
    (duplicated rather than imported since that helper is private module
    plumbing, not part of the shared contract).
    """
    if not isinstance(error, dict):
        return error
    return {k: error[k] for k in ("type", "code", "line", "retryable") if k in error}


class SqliteCallRecorder:
    """Append-only writer for one run's deterministic replay cache (SQLite).

    Satisfies :class:`~hermes_workflows.script_store.CallRecorderProtocol`.
    Every write commits immediately — the replay cache is always durable the
    instant a call succeeds, independent of the store's journal ``durability``
    mode, mirroring :class:`~hermes_workflows.script_store.CallRecorder`'s
    unconditional per-write fsync.
    """

    def __init__(self, conn: sqlite3.Connection, lock: threading.Lock, run_id: str) -> None:
        self._conn = conn
        self._lock = lock
        self._run_id = run_id

    def record(self, call_id: Any, method: str, args_hash: str, value: Any) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO cache_calls"
                "(run_id, call_id, method, args_hash, value_json, ok, code, retryable) "
                "VALUES (?, ?, ?, ?, ?, 1, NULL, 0)",
                (self._run_id, call_id, method, args_hash, _dumps(value)),
            )

    def record_failure(self, call_id: Any, method: str, args_hash: str, code: str, retryable: bool) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO cache_calls"
                "(run_id, call_id, method, args_hash, value_json, ok, code, retryable) "
                "VALUES (?, ?, ?, ?, NULL, 0, ?, ?)",
                (self._run_id, call_id, method, args_hash, code, int(bool(retryable))),
            )

    def record_prompt(self, fingerprint: str, method: str, args_hash: str, value: Any) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO cache_prompts(run_id, fingerprint, method, args_hash, value_json) "
                "VALUES (?, ?, ?, ?, ?)",
                (self._run_id, fingerprint, method, args_hash, _dumps(value)),
            )


class SqliteScriptRunStore:
    """SQLite-backed durable store for subprocess workflow-script runs.

    Satisfies :class:`~hermes_workflows.script_store.ScriptRunStoreProtocol`
    (see that class for the full contract). One database file per store root
    (``<root>/store.sqlite3``, WAL journal mode) replaces the file backend's
    one-directory-per-run layout; every run's metadata, journal, and replay
    cache live in that single file's tables. Transcript artifacts remain
    filesystem blobs under ``<root>/<run_id>/transcripts/`` (see the module
    docstring). Concurrent writers within one process are serialised by a
    single lock, exactly like the file backend.

    ``durability`` maps onto SQLite transaction boundaries — see the module
    docstring's "Durability modes onto transaction boundaries" section.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        durability: JournalDurability = _DEFAULT_JOURNAL_DURABILITY,
        async_flush_every: int = _DEFAULT_ASYNC_FLUSH_EVERY,
        db_filename: str = "store.sqlite3",
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
        # Buffered, not-yet-committed journal rows per run id (``async``/
        # ``exit`` modes only; always empty in ``sync`` mode). ``(seq,
        # data_json)`` pairs, mirroring the file backend's buffered serialized
        # lines. Only touched under ``self._lock``.
        self._journal_buffer: dict[str, list[tuple[int, str]]] = {}
        # Next journal seq to assign per run id, lazily seeded from the table's
        # current max on first use so a store reopened against an existing
        # database continues the same sequence.
        self._journal_seq: dict[str, int] = {}
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._db_path = self.root / db_filename
        self._lock = threading.Lock()
        # ``isolation_level=None`` puts the connection in autocommit mode: a
        # bare ``execute()`` commits immediately (the "always fsynced" writes:
        # runs/cache/kanban), and an explicit ``BEGIN``/``COMMIT`` pair is how
        # this module implements the buffered journal durability modes.
        # ``check_same_thread=False`` is safe because every access is already
        # serialised by ``self._lock``.
        self._conn = sqlite3.connect(str(self._db_path), isolation_level=None, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._open_schema()

    def _open_schema(self) -> None:
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        if version == 0:
            self._conn.executescript(_SCHEMA_DDL)
            self._conn.execute(f"PRAGMA user_version = {SQLITE_STORE_SCHEMA_VERSION}")
            return
        if version != SQLITE_STORE_SCHEMA_VERSION:
            self._conn.close()
            raise CorruptScriptRunError(
                str(self._db_path),
                "schema_version",
                f"sqlite store {self._db_path}: schema_version {version} != {SQLITE_STORE_SCHEMA_VERSION}",
            )

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
        """Create the run row, write it (status=running), and a ``boot`` journal event.

        Raises ``ValueError`` on a duplicate run id.
        """
        _require_safe_run_id(run_id)
        record = ScriptRunMeta(
            run_id=run_id,
            script_sha256=script_sha256(script),
            args_hash=canonical_hash(args),
            limits=limits,
            deterministic_runner=deterministic_runner,
            meta=meta,
            replay_of=replay_of,
        )
        with self._lock:
            try:
                self._insert_meta_locked(record)
            except sqlite3.IntegrityError as exc:
                raise ValueError(f"run_id already exists: {run_id!r}") from exc
            try:
                self._append_journal_locked(
                    run_id,
                    "boot",
                    {
                        "script_sha256": record.script_sha256,
                        "args_hash": record.args_hash,
                        "deterministic_runner": deterministic_runner,
                        "replay_of": replay_of,
                        "limits": limits,
                    },
                )
            except BaseException:
                # Leave no orphan 'running' row on a partial begin, so the same
                # explicit run_id can be cleanly retried (mirrors the file
                # backend's rmtree-on-partial-begin behavior).
                self._conn.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
                self._conn.execute("DELETE FROM journal WHERE run_id=?", (run_id,))
                self._journal_buffer.pop(run_id, None)
                self._journal_seq.pop(run_id, None)
                raise
        return record

    def note_call(self, run_id: str, event: dict[str, Any]) -> None:
        """Write a metadata-only ``call`` journal event from a broker event.

        Same vocabulary/shape as the file backend's ``note_call`` (see
        :meth:`hermes_workflows.script_store.ScriptRunStore.note_call`).
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
        ):
            if event.get(key) is not None:
                data[key] = event.get(key)
        with self._lock:
            if event_type in ("agent_started", "agent_result", "agent_cache_hit"):
                self._append_journal_locked(run_id, event_type, data)
            else:
                self._append_journal_locked(run_id, "call", data)

    def recorder(self, run_id: str) -> SqliteCallRecorder:
        """Return the append-only replay-cache writer for ``run_id``."""
        _require_safe_run_id(run_id)
        return SqliteCallRecorder(self._conn, self._lock, run_id)

    def transcript_recorder(self, run_id: str) -> TranscriptRecorder:
        """Return the per-subagent transcript artifact writer for ``run_id``.

        Transcripts remain filesystem artifacts even on this backend — see the
        module docstring — reusing the file backend's writer unmodified,
        pointed at ``<root>/<run_id>/transcripts/``.
        """
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
        """Write the terminal run row (status + result) and a ``done`` event.

        Tolerant of a missing/unreadable run row: reloads best-effort and, if
        unreadable, writes a minimal terminal record so the run is never left
        stuck in ``running`` — mirrors the file backend's ``finish()``.
        """
        _require_safe_run_id(run_id)
        with self._lock:
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
            self._upsert_meta_locked(record)
            self._append_journal_locked(
                run_id,
                "done",
                {"status": status, "has_value": value is not None, "error": _redact_error(error)},
            )
            # Every terminal status force-flushes the journal regardless of
            # durability mode (issue #108) — see the module docstring.
            self._flush_journal_locked(run_id)

    # -- reads --------------------------------------------------------------
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

        Raises :class:`ScriptRunNotFound` if the run is absent and
        :class:`CorruptScriptRunError` (reason ``"corrupt_cache"``) on any
        malformed stored value. An empty cache is not an error.
        """
        _require_safe_run_id(run_id)
        with self._lock:
            exists = self._conn.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if exists is None:
                raise ScriptRunNotFound(run_id)
            call_rows = self._conn.execute(
                "SELECT call_id, method, args_hash, value_json, ok, code, retryable "
                "FROM cache_calls WHERE run_id=? ORDER BY call_id",
                (run_id,),
            ).fetchall()
            prompt_rows = self._conn.execute(
                "SELECT fingerprint, method, args_hash, value_json FROM cache_prompts WHERE run_id=?",
                (run_id,),
            ).fetchall()
        entries: dict[int, ReplayEntry] = {}
        for row in call_rows:
            value = _loads_or_corrupt(
                run_id, row["value_json"], where=f"cache_calls call_id={row['call_id']}", reason="corrupt_cache"
            )
            entries[row["call_id"]] = ReplayEntry(
                call_id=row["call_id"],
                method=row["method"],
                args_hash=row["args_hash"],
                value=value,
                ok=bool(row["ok"]),
                code=row["code"],
                retryable=bool(row["retryable"]),
            )
        prompt_entries: dict[str, PromptReplayEntry] = {}
        for row in prompt_rows:
            value = _loads_or_corrupt(
                run_id, row["value_json"], where=f"cache_prompts fingerprint={row['fingerprint']}",
                reason="corrupt_cache",
            )
            prompt_entries[row["fingerprint"]] = PromptReplayEntry(
                fingerprint=row["fingerprint"], method=row["method"], args_hash=row["args_hash"], value=value
            )
        return ReplayCache(entries, source_run_id=run_id, prompt_entries=prompt_entries)

    def journal(self, run_id: str, *, limit: int = 200) -> list[dict[str, Any]]:
        """Return the most recent metadata-only journal events for ``run_id``.

        Only committed rows are visible — exactly like the file backend
        reading ``journal.jsonl`` directly: an ``async``/``exit`` run's
        not-yet-flushed events are not observable here until they are
        committed (by the count trigger or the terminal ``finish()``).
        """
        _require_safe_run_id(run_id)
        with self._lock:
            rows = self._conn.execute(
                "SELECT data_json FROM journal WHERE run_id=? ORDER BY seq DESC LIMIT ?",
                (run_id, max(1, limit)),
            ).fetchall()
        events: list[dict[str, Any]] = []
        for row in reversed(rows):
            try:
                event = json.loads(row["data_json"])
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append(event)
        return events

    def journal_path(self, run_id: str) -> Path:
        """Return the shared store database file (not a per-run journal file).

        This backend has no per-run journal file — every run's journal lives
        in this one database's ``journal`` table — so this is an
        operator-facing pointer to *where the data lives*, not a file that by
        itself contains only ``run_id``'s events (see :meth:`journal` for that).
        """
        _require_safe_run_id(run_id)
        return self._db_path

    def transcript_refs(self, run_id: str) -> dict[str, Any]:
        """Return transcript artifact refs/paths without loading transcript content."""
        _require_safe_run_id(run_id)
        return self.transcript_recorder(run_id).refs()

    def suspended_runs(self) -> list[ScriptRunMeta]:
        """Return runs durably suspended on an unresolved paused Kanban await (issue #5)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT run_id FROM runs WHERE status='suspended' ORDER BY run_id"
            ).fetchall()
        out: list[ScriptRunMeta] = []
        for row in rows:
            try:
                out.append(self.load_run(row["run_id"]))
            except (ScriptRunStoreError, ValueError):
                continue
        return out

    # -- durable kanban card state (issue #5) --------------------------------
    def record_kanban_card_state(self, card_id: str, state: dict[str, Any]) -> None:
        """Persist the latest state of a Kanban card (status-precedence).

        Same last-write-wins-with-a-terminal-precedence-guard rule as the file
        backend's ``record_kanban_card_state`` — see that method's docstring.
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
            self._conn.execute(
                "INSERT INTO kanban_card_state(card_id, state_json) VALUES (?, ?) "
                "ON CONFLICT(card_id) DO UPDATE SET state_json=excluded.state_json",
                (card_id, _dumps(record)),
            )

    def load_kanban_card_state(self, card_id: str) -> Optional[dict[str, Any]]:
        """Return the latest persisted state of ``card_id``, or ``None`` if absent."""
        _require_safe_card_id(card_id)
        with self._lock:
            return self._load_kanban_card_state_unlocked(card_id)

    def kanban_waits(self) -> list[dict[str, Any]]:
        """Return persisted card states that are not yet terminal (in-flight waits)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT card_id, state_json FROM kanban_card_state ORDER BY card_id"
            ).fetchall()
        waits: list[dict[str, Any]] = []
        for row in rows:
            try:
                data = json.loads(row["state_json"])
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict) and data.get("status") not in _KANBAN_NON_WAIT_STATUSES:
                waits.append(data)
        return waits

    def _load_kanban_card_state_unlocked(self, card_id: str) -> Optional[dict[str, Any]]:
        row = self._conn.execute("SELECT state_json FROM kanban_card_state WHERE card_id=?", (card_id,)).fetchone()
        if row is None:
            return None
        try:
            data = json.loads(row["state_json"])
        except json.JSONDecodeError:
            return None  # a corrupt card-state row is treated as absent (fail-safe re-await).
        return data if isinstance(data, dict) else None

    # -- durable kanban event log (issue #5) ---------------------------------
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

        ``seq`` is the row's 1-based position within ``card_id``'s event log
        (computed from the table, so it survives a store reopened against the
        same database) — the same semantics as the file backend's physical
        line position.
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
        with self._lock:
            seq = self._conn.execute(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM kanban_events WHERE card_id=?", (card_id,)
            ).fetchone()[0]
            self._conn.execute(
                "INSERT INTO kanban_events(card_id, seq, ts, status, workflow_result_json, reason, profile) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (card_id, seq, record["ts"], status, _dumps(record["workflow_result"]), reason, profile),
            )
        return {"seq": seq, **record}

    def read_kanban_events(self, card_id: str, *, after_seq: int = 0) -> list[dict[str, Any]]:
        """Return durable card events with position ``> after_seq``."""
        _require_safe_card_id(card_id)
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq, ts, status, workflow_result_json, reason, profile "
                "FROM kanban_events WHERE card_id=? AND seq>? ORDER BY seq",
                (card_id, after_seq),
            ).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            try:
                workflow_result = json.loads(row["workflow_result_json"])
            except json.JSONDecodeError:
                continue  # a corrupt row is skipped but still consumes its seq position.
            events.append(
                {
                    "card_id": card_id,
                    "ts": row["ts"],
                    "status": row["status"],
                    "workflow_result": workflow_result,
                    "reason": row["reason"],
                    "profile": row["profile"],
                    "seq": row["seq"],
                }
            )
        return events

    def latest_kanban_resolution(self, card_id: str) -> Optional[dict[str, Any]]:
        """Return the most recent *outcome* event from the log, or ``None``."""
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

    # -- internals ------------------------------------------------------------
    def _load_meta_unlocked(self, run_id: str, *, missing_ok: bool) -> Optional[ScriptRunMeta]:
        row = self._conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            if missing_ok:
                return None
            raise ScriptRunNotFound(run_id)
        version = row["schema_version"]
        if version != SCRIPT_SCHEMA_VERSION:
            raise CorruptScriptRunError(
                run_id, "schema_version", f"runs row schema_version {version!r} != {SCRIPT_SCHEMA_VERSION}"
            )
        meta = _loads_or_corrupt(run_id, row["meta_json"], where="runs.meta_json")
        limits = _loads_or_corrupt(run_id, row["limits_json"], where="runs.limits_json")
        value = _loads_or_corrupt(run_id, row["value_json"], where="runs.value_json")
        error = _loads_or_corrupt(run_id, row["error_json"], where="runs.error_json")
        transcripts = _loads_or_corrupt(run_id, row["transcripts_json"], where="runs.transcripts_json")
        return ScriptRunMeta(
            run_id=row["run_id"],
            script_sha256=row["script_sha256"],
            args_hash=row["args_hash"],
            status=row["status"],
            meta=meta,
            limits=limits,
            value=value,
            error=error,
            deterministic_runner=bool(row["deterministic_runner"]),
            replay_of=row["replay_of"],
            transcripts=transcripts if isinstance(transcripts, dict) else None,
            schema_version=version,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _insert_meta_locked(self, record: ScriptRunMeta) -> None:
        self._conn.execute(
            "INSERT INTO runs(run_id, schema_version, script_sha256, args_hash, status, meta_json, "
            "limits_json, value_json, error_json, deterministic_runner, replay_of, transcripts_json, "
            "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            self._row_values(record),
        )

    def _upsert_meta_locked(self, record: ScriptRunMeta) -> None:
        self._conn.execute(
            "INSERT INTO runs(run_id, schema_version, script_sha256, args_hash, status, meta_json, "
            "limits_json, value_json, error_json, deterministic_runner, replay_of, transcripts_json, "
            "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(run_id) DO UPDATE SET "
            "schema_version=excluded.schema_version, script_sha256=excluded.script_sha256, "
            "args_hash=excluded.args_hash, status=excluded.status, meta_json=excluded.meta_json, "
            "limits_json=excluded.limits_json, value_json=excluded.value_json, "
            "error_json=excluded.error_json, deterministic_runner=excluded.deterministic_runner, "
            "replay_of=excluded.replay_of, transcripts_json=excluded.transcripts_json, "
            "updated_at=excluded.updated_at",
            self._row_values(record),
        )

    @staticmethod
    def _row_values(record: ScriptRunMeta) -> tuple:
        return (
            record.run_id,
            record.schema_version,
            record.script_sha256,
            record.args_hash,
            record.status,
            _dumps(record.meta) if record.meta is not None else None,
            _dumps(record.limits) if record.limits is not None else None,
            _dumps(record.value) if record.value is not None else None,
            _dumps(record.error) if record.error is not None else None,
            int(bool(record.deterministic_runner)),
            record.replay_of,
            _dumps(record.transcripts) if record.transcripts is not None else None,
            record.created_at,
            record.updated_at,
        )

    def _append_journal_locked(self, run_id: str, event_type: str, data: dict[str, Any]) -> None:
        """Append one journal event, applying the store's durability policy.

        Called with ``self._lock`` already held (mirrors the file backend's
        ``_append_journal``, whose docstring explains the durability-mode
        behavior this reproduces via SQL transaction boundaries instead of
        buffered file lines).
        """
        event = {"ts": utc_now_iso(), "type": event_type, "run_id": run_id, **data}
        line = _dumps(event)
        seq = self._next_journal_seq_locked(run_id)
        if self._durability == "sync":
            self._write_journal_rows_locked(run_id, [(seq, line)])
            return
        buffer = self._journal_buffer.setdefault(run_id, [])
        buffer.append((seq, line))
        if self._durability == "async" and len(buffer) >= self._async_flush_every:
            self._flush_journal_locked(run_id)

    def _next_journal_seq_locked(self, run_id: str) -> int:
        if run_id not in self._journal_seq:
            row = self._conn.execute("SELECT COALESCE(MAX(seq), 0) FROM journal WHERE run_id=?", (run_id,))
            self._journal_seq[run_id] = row.fetchone()[0]
        self._journal_seq[run_id] += 1
        return self._journal_seq[run_id]

    def _write_journal_rows_locked(self, run_id: str, rows: list[tuple[int, str]]) -> None:
        """Insert+commit ``rows`` to ``run_id``'s journal in a single transaction."""
        if not rows:
            return
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.executemany(
                "INSERT INTO journal(run_id, seq, data_json) VALUES (?, ?, ?)",
                [(run_id, seq, data) for seq, data in rows],
            )
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        else:
            self._conn.execute("COMMIT")

    def _flush_journal_locked(self, run_id: str) -> None:
        """Force any buffered ``async``/``exit`` journal events to disk.

        A no-op in ``sync`` mode (nothing is ever buffered) and a no-op if
        nothing is pending. The buffer is popped only after the write
        succeeds, so a transient failure during a force-flush leaves the
        pending rows intact for a retried ``finish()``.
        """
        pending = self._journal_buffer.get(run_id)
        if pending:
            self._write_journal_rows_locked(run_id, pending)
            self._journal_buffer.pop(run_id, None)

    def _run_dir(self, run_id: str) -> Path:
        # Only used for the filesystem-based transcript artifacts (see
        # ``transcript_recorder``); everything else lives in the database.
        return self.root / run_id
