"""Tests for the run-journal durability-mode knob on ``ScriptRunStore`` (issue #108).

Covered:

* **Default unchanged** — a store built without ``durability=`` behaves exactly
  like the pre-#108 store: every journal event is on disk (fsynced) the moment
  it is written, with no ``finish()`` required to observe it.
* **Construction guards** — an unsupported ``durability`` value or a
  non-positive ``async_flush_every`` raises ``ValueError`` at construction.
* **``exit`` mode** — journal events are buffered in memory and the on-disk
  ``journal.jsonl`` stays absent/empty until the run's terminal ``finish()``.
* **``async`` mode** — events are buffered and auto-flushed once
  ``async_flush_every`` events have accumulated (a deterministic *count*
  trigger); below that threshold nothing reaches disk until ``finish()``.
* **Force-flush on every terminal status** — ``finish()`` always flushes any
  buffered events regardless of mode, covering ``succeeded`` / ``failed`` /
  ``suspended`` / ``stopped`` / ``paused`` alike (suspend/abort parity).
* **End-to-end parity** — a full script run through ``run_workflow_script``
  under ``exit``/``async`` durability produces the *same* final journal content
  as the ``sync`` default; only the write timing differs.
"""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from hermes_workflows import run_workflow_script
from hermes_workflows.script_store import JOURNAL_DURABILITY_MODES, ScriptRunStore

META = 'meta = {"name": "demo", "description": "d"}\n'
SCRIPT = META + 'log("a")\nlog("b")\nreturn {"ok": True}\n'


def _read_journal_lines(run_dir: Path) -> list[dict]:
    path = run_dir / "journal.jsonl"
    if not path.exists():
        return []
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    return [json.loads(ln) for ln in lines]


# --------------------------------------------------------------------------- #
# Construction
# --------------------------------------------------------------------------- #

def test_modes_constant_matches_the_three_documented_policies():
    assert JOURNAL_DURABILITY_MODES == {"exit", "async", "sync"}


def test_unsupported_durability_raises_value_error():
    with TemporaryDirectory() as tmp:
        try:
            ScriptRunStore(Path(tmp) / "runs", durability="eventually")
        except ValueError as exc:
            assert "eventually" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("expected ValueError for an unsupported durability mode")


def test_non_positive_async_flush_every_raises_value_error():
    with TemporaryDirectory() as tmp:
        for bad in (0, -1, True, "3"):
            try:
                ScriptRunStore(Path(tmp) / "runs", durability="async", async_flush_every=bad)
            except ValueError:
                continue
            raise AssertionError(f"expected ValueError for async_flush_every={bad!r}")


# --------------------------------------------------------------------------- #
# Default (sync) behavior is byte-for-byte unchanged
# --------------------------------------------------------------------------- #

def test_default_durability_writes_every_event_immediately():
    with TemporaryDirectory() as tmp:
        run_dir = Path(tmp) / "runs" / "run_sync"
        store = ScriptRunStore(Path(tmp) / "runs")  # no durability= kwarg: must default to sync.
        store.begin("run_sync", script="x", args=None, limits=None, deterministic_runner=False)
        # No finish() yet: a sync store must already have the boot event on disk.
        events = _read_journal_lines(run_dir)
        assert [e["type"] for e in events] == ["boot"]

        store.note_call("run_sync", {"type": "call", "call_id": 1, "method": "log", "ok": True})
        events = _read_journal_lines(run_dir)
        assert [e["type"] for e in events] == ["boot", "call"]

        store.finish("run_sync", status="succeeded", meta=None, value=None, error=None)
        events = _read_journal_lines(run_dir)
        assert [e["type"] for e in events] == ["boot", "call", "done"]


# --------------------------------------------------------------------------- #
# exit mode: nothing reaches disk until the terminal finish()
# --------------------------------------------------------------------------- #

def test_exit_mode_buffers_until_terminal_finish():
    with TemporaryDirectory() as tmp:
        run_dir = Path(tmp) / "runs" / "run_exit"
        store = ScriptRunStore(Path(tmp) / "runs", durability="exit")
        store.begin("run_exit", script="x", args=None, limits=None, deterministic_runner=False)
        assert _read_journal_lines(run_dir) == []  # boot buffered, not yet on disk.

        store.note_call("run_exit", {"type": "call", "call_id": 1, "method": "log", "ok": True})
        store.note_call("run_exit", {"type": "call", "call_id": 2, "method": "log", "ok": True})
        assert _read_journal_lines(run_dir) == []  # still nothing on disk mid-run.

        store.finish("run_exit", status="succeeded", meta=None, value=None, error=None)
        events = _read_journal_lines(run_dir)
        assert [e["type"] for e in events] == ["boot", "call", "call", "done"]
        assert [e["call_id"] for e in events if e["type"] == "call"] == [1, 2]


# --------------------------------------------------------------------------- #
# async mode: a deterministic count trigger, not a wall-clock timer
# --------------------------------------------------------------------------- #

def test_async_mode_defers_below_the_flush_threshold_until_finish():
    with TemporaryDirectory() as tmp:
        run_dir = Path(tmp) / "runs" / "run_async"
        store = ScriptRunStore(Path(tmp) / "runs", durability="async", async_flush_every=5)
        store.begin("run_async", script="x", args=None, limits=None, deterministic_runner=False)
        store.note_call("run_async", {"type": "call", "call_id": 1, "method": "log", "ok": True})
        # Only 2 events buffered (boot + call), below the threshold of 5: nothing on disk yet.
        assert _read_journal_lines(run_dir) == []

        store.finish("run_async", status="succeeded", meta=None, value=None, error=None)
        events = _read_journal_lines(run_dir)
        assert [e["type"] for e in events] == ["boot", "call", "done"]


def test_async_mode_auto_flushes_once_the_count_threshold_is_reached():
    with TemporaryDirectory() as tmp:
        run_dir = Path(tmp) / "runs" / "run_async2"
        store = ScriptRunStore(Path(tmp) / "runs", durability="async", async_flush_every=2)
        store.begin("run_async2", script="x", args=None, limits=None, deterministic_runner=False)
        assert _read_journal_lines(run_dir) == []  # 1 buffered event, below threshold of 2.

        # The 2nd buffered event (boot + this call = 2) hits the threshold and
        # auto-flushes *before* finish() is ever called.
        store.note_call("run_async2", {"type": "call", "call_id": 1, "method": "log", "ok": True})
        events = _read_journal_lines(run_dir)
        assert [e["type"] for e in events] == ["boot", "call"]

        store.finish("run_async2", status="succeeded", meta=None, value=None, error=None)
        events = _read_journal_lines(run_dir)
        assert [e["type"] for e in events] == ["boot", "call", "done"]


# --------------------------------------------------------------------------- #
# finish() force-flushes on every terminal status, in every mode
# --------------------------------------------------------------------------- #

def test_finish_force_flushes_every_terminal_status_in_exit_and_async_modes():
    for durability, kwargs in (("exit", {}), ("async", {"async_flush_every": 100})):
        for status in ("succeeded", "failed", "suspended", "stopped", "paused"):
            with TemporaryDirectory() as tmp:
                run_id = f"run_{status}"
                run_dir = Path(tmp) / "runs" / run_id
                store = ScriptRunStore(Path(tmp) / "runs", durability=durability, **kwargs)
                store.begin(run_id, script="x", args=None, limits=None, deterministic_runner=False)
                store.note_call(run_id, {"type": "call", "call_id": 1, "method": "log", "ok": True})
                assert _read_journal_lines(run_dir) == [], (durability, status)

                store.finish(run_id, status=status, meta=None, value=None, error=None)
                events = _read_journal_lines(run_dir)
                assert [e["type"] for e in events] == ["boot", "call", "done"], (durability, status)
                assert events[-1]["status"] == status


# --------------------------------------------------------------------------- #
# End-to-end parity: exit/async produce the same final content as sync
# --------------------------------------------------------------------------- #

def test_end_to_end_run_produces_identical_final_journal_across_modes():
    baseline = None
    for durability in ("sync", "async", "exit"):
        with TemporaryDirectory() as tmp:
            store = ScriptRunStore(Path(tmp) / "runs", durability=durability)
            res = run_workflow_script(SCRIPT, store=store, run_id="r")
            assert res.ok, res.error
            events = store.journal("r")
            # Strip the wall-clock timestamp: only the durability-independent
            # content should match across modes.
            shaped = [{k: v for k, v in e.items() if k != "ts"} for e in events]
            if baseline is None:
                baseline = shaped
            else:
                assert shaped == baseline, durability
