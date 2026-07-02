"""Tests specific to the SQLite store backend (issue #110).

The behavioral contract shared with the file backend (begin/finish/load_run,
replay hit/miss, duplicate/unsafe run ids, ...) is pinned once, parametrized
over both backends, in ``tests/test_script_store.py`` and
``tests/test_pending_writes_resume_contract.py``. This module covers what is
backend-specific:

* **Schema/versioning** — one database per store root, ``PRAGMA journal_mode``
  is WAL, ``PRAGMA user_version`` is stamped and checked at open time, and an
  incompatible stamped version fails closed at construction.
* **Durability modes onto SQL transaction boundaries** (issue #108) — the
  SQLite analogue of ``tests/test_journal_durability.py``: ``sync`` commits
  every journal row immediately, ``async``/``exit`` buffer in memory and only
  become visible to a fresh query once flushed (by the count trigger or the
  terminal ``finish()``), and every terminal status force-flushes.
* **SQLite-native corruption** — the SQLite-side equivalents of
  ``tests/test_script_store.py``'s file-tampering corruption tests: tamper the
  store's own database directly (raw SQL) instead of a JSONL/JSON file, and
  confirm the same typed-failure contract.
* **Byte-identical journal event payloads vs the file backend** — the explicit
  issue #110 acceptance criterion: running the same script through both
  backends produces the same journal event dicts (modulo the wall-clock
  ``ts``).
* **End-to-end resume/replay/suspend parity** — the file-backend fixtures in
  ``tests/test_kanban_suspend_resume.py`` / ``tests/test_pending_writes_
  resume_contract.py`` re-run here against the SQLite backend directly, per
  the issue's "resume/replay/suspend fixtures green on SQLite" criterion.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory

from hermes_workflows import VMLimits, run_workflow_script
from hermes_workflows.errors import CorruptScriptRunError, ScriptRunNotFound
from hermes_workflows.kanban import kanban_card_id
from hermes_workflows.kanban_notify import EventLogKanbanBackend, ThreadEventNotifier, publish_kanban_event
from hermes_workflows.script_store import SCRIPT_SCHEMA_VERSION, ScriptRunStore
from hermes_workflows.script_store_sqlite import SQLITE_STORE_SCHEMA_VERSION, SqliteScriptRunStore

META = 'meta = {"name": "demo", "description": "d"}\n'
FULL_SCRIPT = META + (
    'log("start")\n'
    'g = await agent("hermes.greeter", {"subject": args["who"]}, schema={"greeting": "string"})\n'
    'phase("mid")\n'
    'k = await kanban_agent("relayplanner", {"goal": "plan"}, {"repo": "x"})\n'
    'return {"greeting": g["greeting"], "profile": k["profile"]}\n'
)


# --------------------------------------------------------------------------- #
# Schema / versioning / one-DB-per-root
# --------------------------------------------------------------------------- #

def test_one_sqlite_database_file_per_store_root():
    with TemporaryDirectory() as tmp:
        root = Path(tmp) / "runs"
        store = SqliteScriptRunStore(root)
        run_workflow_script(META + 'return {"ok": 1}\n', store=store, run_id="a")
        run_workflow_script(META + 'return {"ok": 2}\n', store=store, run_id="b")

        db_files = list(root.glob("*.sqlite3"))
        assert db_files == [root / "store.sqlite3"]
        # No per-run directories/files for the relational state (unlike the
        # file backend) — only the transcript-artifact directory exists, and
        # only because this script made no agent calls it stays absent too.
        assert not (root / "a").exists()
        assert not (root / "b").exists()


def test_wal_journal_mode_and_schema_version_are_stamped():
    with TemporaryDirectory() as tmp:
        root = Path(tmp) / "runs"
        SqliteScriptRunStore(root)
        conn = sqlite3.connect(str(root / "store.sqlite3"))
        try:
            assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SQLITE_STORE_SCHEMA_VERSION
        finally:
            conn.close()


def test_reopening_the_same_root_reuses_the_database():
    with TemporaryDirectory() as tmp:
        root = Path(tmp) / "runs"
        store_a = SqliteScriptRunStore(root)
        store_a.begin("r1", script="x", args=None, limits=None, deterministic_runner=False)
        store_a.finish("r1", status="succeeded", meta=None, value={"v": 1}, error=None)

        # A fresh store instance pointed at the same root sees the same data —
        # this is what a restarted parent process relies on.
        store_b = SqliteScriptRunStore(root)
        loaded = store_b.load_run("r1")
        assert loaded.status == "succeeded"
        assert loaded.value == {"v": 1}


def test_incompatible_stamped_schema_version_fails_closed_at_construction():
    with TemporaryDirectory() as tmp:
        root = Path(tmp) / "runs"
        SqliteScriptRunStore(root)  # creates + stamps the database.
        conn = sqlite3.connect(str(root / "store.sqlite3"))
        conn.execute(f"PRAGMA user_version = {SQLITE_STORE_SCHEMA_VERSION + 99}")
        conn.close()

        try:
            SqliteScriptRunStore(root)
        except CorruptScriptRunError as exc:
            assert exc.reason == "schema_version"
        else:  # pragma: no cover
            raise AssertionError("expected CorruptScriptRunError for an incompatible database schema_version")


# --------------------------------------------------------------------------- #
# SQLite-native corruption (counterparts to the file backend's file-tamper tests)
# --------------------------------------------------------------------------- #

def test_corrupt_run_row_raises_typed():
    with TemporaryDirectory() as tmp:
        store = SqliteScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(META + 'return {}\n', store=store, run_id="src")

        with store._lock:
            store._conn.execute("UPDATE runs SET meta_json='{ broken' WHERE run_id='src'")

        try:
            store.load_run("src")
        except CorruptScriptRunError as exc:
            assert exc.reason == "corrupt_run"
        else:  # pragma: no cover
            raise AssertionError("expected CorruptScriptRunError for a corrupt meta_json column")


def test_stale_row_schema_version_raises_typed():
    with TemporaryDirectory() as tmp:
        store = SqliteScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(META + 'return {}\n', store=store, run_id="src")

        with store._lock:
            store._conn.execute(
                "UPDATE runs SET schema_version=? WHERE run_id='src'", (SCRIPT_SCHEMA_VERSION + 99,)
            )

        try:
            store.load_run("src")
        except CorruptScriptRunError as exc:
            assert exc.reason == "schema_version"
        else:  # pragma: no cover
            raise AssertionError("expected CorruptScriptRunError for a stale run row schema_version")


def test_corrupt_cache_row_raises_typed_and_parent_recovers():
    with TemporaryDirectory() as tmp:
        store = SqliteScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(FULL_SCRIPT, args={"who": "world"}, store=store, run_id="src")

        with store._lock:
            store._conn.execute(
                "UPDATE cache_calls SET value_json='{ not json' WHERE run_id='src' AND method='agent'"
            )

        try:
            store.load_cache("src")
        except CorruptScriptRunError as exc:
            assert exc.reason == "corrupt_cache"
        else:  # pragma: no cover
            raise AssertionError("expected CorruptScriptRunError for a corrupt cache_calls value_json")

        # Parent intact: an unrelated fresh run still succeeds.
        ok = run_workflow_script(META + 'return {"ok": 1}\n', store=store, run_id="fresh")
        assert ok.ok and ok.value == {"ok": 1}


def test_missing_run_dir_analogue_raises_typed_not_found():
    with TemporaryDirectory() as tmp:
        store = SqliteScriptRunStore(Path(tmp) / "runs")
        try:
            store.load_cache("nope")
        except ScriptRunNotFound:
            pass
        else:  # pragma: no cover
            raise AssertionError("expected ScriptRunNotFound")


def test_pending_writes_drift_abort_via_sqlite_native_tamper():
    """SQLite-native counterpart of the file backend's cache.jsonl-tamper drift test.

    Mirrors ``tests/test_pending_writes_resume_contract.py::
    test_replay_drift_on_a_completed_sibling_aborts_the_resume_fail_closed`` but
    corrupts the cached call's ``args_hash`` via a raw SQL ``UPDATE`` instead of
    rewriting a JSONL line, since this backend has no such file to tamper with.
    """
    script = META + (
        "outs = await parallel([\n"
        "    lambda: agent('hermes.echo', {'i': 0}),\n"
        "    lambda: agent('hermes.echo', {'i': 1}),\n"
        "])\n"
        "return {'outs': outs}\n"
    )

    class _Runner:
        def __call__(self, agent_id, input):  # noqa: A002
            return {"i": input["i"], "via": "live"}

    with TemporaryDirectory() as tmp:
        store = SqliteScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(
            script, store=store, run_id="A", agent_runner=_Runner(),
            limits=VMLimits(max_parallel=2), deterministic_runner=True,
        )
        with store._lock:
            store._conn.execute(
                "UPDATE cache_calls SET args_hash='forged-drift' WHERE run_id='A' AND call_id=1"
            )

        b = run_workflow_script(
            script, store=store, run_id="B", replay_from="A", agent_runner=_Runner(),
            limits=VMLimits(max_parallel=2),
        )
        assert b.ok is False
        assert "replay drift" in b.error["message"]


# --------------------------------------------------------------------------- #
# Durability modes onto SQL transaction boundaries (issue #108)
# --------------------------------------------------------------------------- #

def _committed_journal_types(store: SqliteScriptRunStore, run_id: str) -> list[str]:
    """Read the run's journal directly from the database (bypassing any buffer).

    Mirrors ``tests/test_journal_durability.py``'s ``_read_journal_lines``: what
    is *committed and visible to a fresh query* is what is durable, regardless
    of what this store instance still has buffered in memory.
    """
    with store._lock:
        rows = store._conn.execute(
            "SELECT data_json FROM journal WHERE run_id=? ORDER BY seq", (run_id,)
        ).fetchall()
    return [json.loads(row["data_json"])["type"] for row in rows]


def test_sync_mode_commits_every_event_immediately():
    with TemporaryDirectory() as tmp:
        store = SqliteScriptRunStore(Path(tmp) / "runs")  # default durability="sync".
        store.begin("r", script="x", args=None, limits=None, deterministic_runner=False)
        assert _committed_journal_types(store, "r") == ["boot"]

        store.note_call("r", {"type": "call", "call_id": 1, "method": "log", "ok": True})
        assert _committed_journal_types(store, "r") == ["boot", "call"]

        store.finish("r", status="succeeded", meta=None, value=None, error=None)
        assert _committed_journal_types(store, "r") == ["boot", "call", "done"]


def test_exit_mode_buffers_until_terminal_finish():
    with TemporaryDirectory() as tmp:
        store = SqliteScriptRunStore(Path(tmp) / "runs", durability="exit")
        store.begin("r", script="x", args=None, limits=None, deterministic_runner=False)
        assert _committed_journal_types(store, "r") == []  # boot buffered, not committed.

        store.note_call("r", {"type": "call", "call_id": 1, "method": "log", "ok": True})
        store.note_call("r", {"type": "call", "call_id": 2, "method": "log", "ok": True})
        assert _committed_journal_types(store, "r") == []  # still nothing committed mid-run.

        store.finish("r", status="succeeded", meta=None, value=None, error=None)
        assert _committed_journal_types(store, "r") == ["boot", "call", "call", "done"]


def test_async_mode_auto_commits_once_the_count_threshold_is_reached():
    with TemporaryDirectory() as tmp:
        store = SqliteScriptRunStore(Path(tmp) / "runs", durability="async", async_flush_every=2)
        store.begin("r", script="x", args=None, limits=None, deterministic_runner=False)
        assert _committed_journal_types(store, "r") == []  # 1 buffered, below threshold of 2.

        # The 2nd buffered event (boot + this call) hits the threshold and
        # auto-commits before finish() is ever called.
        store.note_call("r", {"type": "call", "call_id": 1, "method": "log", "ok": True})
        assert _committed_journal_types(store, "r") == ["boot", "call"]

        store.finish("r", status="succeeded", meta=None, value=None, error=None)
        assert _committed_journal_types(store, "r") == ["boot", "call", "done"]


def test_finish_force_commits_every_terminal_status_in_exit_and_async_modes():
    for durability, kwargs in (("exit", {}), ("async", {"async_flush_every": 100})):
        for status in ("succeeded", "failed", "suspended", "stopped", "paused"):
            with TemporaryDirectory() as tmp:
                store = SqliteScriptRunStore(Path(tmp) / "runs", durability=durability, **kwargs)
                store.begin("r", script="x", args=None, limits=None, deterministic_runner=False)
                store.note_call("r", {"type": "call", "call_id": 1, "method": "log", "ok": True})
                assert _committed_journal_types(store, "r") == [], (durability, status)

                store.finish("r", status=status, meta=None, value=None, error=None)
                events = _committed_journal_types(store, "r")
                assert events == ["boot", "call", "done"], (durability, status)


# --------------------------------------------------------------------------- #
# Byte-identical journal event payloads vs the file backend
# --------------------------------------------------------------------------- #

def test_journal_event_payloads_match_the_file_backend_byte_for_byte():
    """Explicit issue #110 acceptance criterion.

    Runs the identical script through both backends and asserts the resulting
    journal event dicts are identical, modulo the wall-clock ``ts`` field —
    same keys, same values, same order of events. This is the parity the
    module docstrings promise: the *content* a caller reads back via
    ``journal()`` does not depend on which backend produced it.
    """
    script = FULL_SCRIPT
    baseline = None
    for store_cls in (ScriptRunStore, SqliteScriptRunStore):
        with TemporaryDirectory() as tmp:
            store = store_cls(Path(tmp) / "runs")
            res = run_workflow_script(script, args={"who": "world"}, store=store, run_id="r")
            assert res.ok, res.error
            events = store.journal("r")
            shaped = [{k: v for k, v in e.items() if k != "ts"} for e in events]
            if baseline is None:
                baseline = shaped
            else:
                assert shaped == baseline


# --------------------------------------------------------------------------- #
# End-to-end resume/replay/suspend parity on SQLite
# --------------------------------------------------------------------------- #

def test_replay_serves_deterministic_calls_without_invoking_runner_on_sqlite():
    with TemporaryDirectory() as tmp:
        store = SqliteScriptRunStore(Path(tmp) / "runs")
        rec = run_workflow_script(FULL_SCRIPT, args={"who": "world"}, store=store, run_id="src")
        assert rec.ok, rec.error
        assert rec.replayed_calls == 0
        assert len(store.load_cache("src")) == 4

        class _Spy:
            def __init__(self):
                self.calls = 0

            def __call__(self, agent_id, input):  # noqa: A002
                self.calls += 1
                return {"_marker": "live"}

        spy = _Spy()
        rep = run_workflow_script(
            FULL_SCRIPT, args={"who": "world"}, store=store, run_id="replay",
            replay_from="src", agent_runner=spy,
        )
        assert rep.ok, rep.error
        assert rep.value == rec.value
        assert spy.calls == 0
        assert rep.replayed_calls == 4


def test_pending_writes_resume_after_a_mid_parallel_crash_on_sqlite():
    script = META + (
        "outs = await parallel([\n"
        "    lambda: agent('hermes.echo', {'i': 0}),\n"
        "    lambda: agent('hermes.echo', {'i': 1}),\n"
        "])\n"
        "return {'outs': outs}\n"
    )

    class _CrashingRunner:
        def __call__(self, agent_id, input):  # noqa: A002
            i = input["i"]
            if i == 0:
                return {"i": 0, "via": "orig-live"}
            raise RuntimeError("simulated crash: branch 1 never completed")

    class _ResumeRunner:
        def __init__(self):
            self.calls: list[int] = []

        def __call__(self, agent_id, input):  # noqa: A002
            self.calls.append(input["i"])
            return {"i": input["i"], "via": "resumed-live"}

    with TemporaryDirectory() as tmp:
        store = SqliteScriptRunStore(Path(tmp) / "runs")
        a = run_workflow_script(
            script, store=store, run_id="A", agent_runner=_CrashingRunner(),
            limits=VMLimits(max_parallel=2), deterministic_runner=True,
        )
        assert a.ok is False

        resume_runner = _ResumeRunner()
        b = run_workflow_script(
            script, store=store, run_id="B", replay_from="A", agent_runner=resume_runner,
            limits=VMLimits(max_parallel=2),
        )
        assert b.ok, b.error
        assert resume_runner.calls == [1]  # only the crashed branch re-dispatches.
        assert b.value == {"outs": [{"i": 0, "via": "orig-live"}, {"i": 1, "via": "resumed-live"}]}


def test_e2e_suspend_then_resume_from_an_externally_produced_event_on_sqlite():
    with TemporaryDirectory() as tmp:
        store = SqliteScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        backend = EventLogKanbanBackend(store, notifier, known_profiles={"planner"})
        script = META + (
            'r = await kanban_agent("planner", prompt="plan", on_block="pause", schema={"plan": "string"})\n'
            'return r["workflow_result"]\n'
        )

        a = run_workflow_script(
            script, args={"i": 1}, store=store, run_id="A",
            limits=VMLimits(max_runtime_s=5.0, kanban_suspend_after_s=0.2),
            kanban_backend=backend,
        )
        assert a.ok is False and a.suspended is True
        card_id = kanban_card_id("A:1")
        assert store.load_run("A").status == "suspended"
        assert [m.run_id for m in store.suspended_runs()] == ["A"]
        assert [w["card_id"] for w in store.kanban_waits()] == [card_id]

        publish_kanban_event(store, notifier, card_id, status="completed", result={"plan": "resumed"}, profile="planner")

        b = run_workflow_script(
            script, args={"i": 1}, store=store, run_id="B", replay_from="A",
            kanban_backend=EventLogKanbanBackend(store, notifier, known_profiles={"planner"}),
        )
        assert b.ok is True, b.error
        assert b.value == {"plan": "resumed"}
        assert store.load_run("B").status == "succeeded"
        assert store.kanban_waits() == []
