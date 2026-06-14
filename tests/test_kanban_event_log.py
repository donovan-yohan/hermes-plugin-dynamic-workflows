"""Tests for the durable Kanban event log + external-producer seam (issue #5).

PR #22 made a card's outcome survive a parent restart *when the parent recorded it
on its own await*. This slice adds the producer-facing half: a worker/gateway —
possibly a different process — appends card events to a durable append-only log
(``<root>/_kanban/<card_id>.events.jsonl``). A parent that was down when the event
was produced **replays it from the log on its next await**, even though no live
in-memory backend ever saw it. The log is also a durable audit trail.

Covers: the store's append/read/latest surface, and an end-to-end resume that
serves an externally-produced event across a backend that has no memory of (and
raises if asked to await) the card.
"""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from hermes_workflows import run_workflow_script
from hermes_workflows.kanban import (
    CARD_BLOCKED,
    CARD_COMPLETED,
    DurableKanbanBackend,
    InMemoryKanbanBackend,
    kanban_card_id,
)
from hermes_workflows.script_store import ScriptRunStore

META = 'meta = {"name": "k5e", "description": "d"}\n'

_SCRIPT = META + (
    'r = await kanban_agent("planner", prompt="plan", schema={"plan": "string"})\n'
    'return r["workflow_result"]\n'
)


class _BoomBackend(InMemoryKanbanBackend):
    """No live event source: raises if awaited, proving resume comes from the log."""

    def await_resolution(self, *args, **kwargs):
        raise AssertionError("inner await must not run on a durable-log resume")


# --------------------------------------------------------------------------- #
# ScriptRunStore: durable event log
# --------------------------------------------------------------------------- #

def test_event_log_append_read_and_latest():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        assert store.read_kanban_events("kbc_a") == []
        assert store.latest_kanban_resolution("kbc_a") is None

        e1 = store.append_kanban_event("kbc_a", status="blocked", reason="need input", profile="planner")
        e2 = store.append_kanban_event("kbc_a", status="completed", result={"plan": "x"}, profile="planner")
        assert e1["seq"] == 1 and e2["seq"] == 2

        events = store.read_kanban_events("kbc_a")
        assert [e["seq"] for e in events] == [1, 2]
        assert store.read_kanban_events("kbc_a", after_seq=1) == [events[1]]

        latest = store.latest_kanban_resolution("kbc_a")
        assert latest["status"] == CARD_COMPLETED
        assert latest["workflow_result"] == {"plan": "x"}
        assert latest["version"] == 2


def test_event_log_latest_is_none_without_a_resolution_event():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        store.append_kanban_event("kbc_w", status="waiting")  # not an outcome.
        assert store.latest_kanban_resolution("kbc_w") is None


def test_event_log_skips_corrupt_lines():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        store.append_kanban_event("kbc_c", status="completed", result={"plan": "ok"})
        path = Path(tmp) / "runs" / "_kanban" / "kbc_c.events.jsonl"
        with path.open("a", encoding="utf-8") as f:
            f.write("{ broken json\n")
        # The corrupt trailing line is skipped; the valid event still reads.
        assert store.latest_kanban_resolution("kbc_c")["workflow_result"] == {"plan": "ok"}


def test_event_log_seq_is_a_stable_positional_cursor_across_instances():
    # Regression (review): seq is the physical line position assigned at read time,
    # so it is unique/monotonic even across two store instances (the cross-process
    # producer seam) — a write-time counter under a per-instance lock would have
    # handed out duplicate seqs and an after_seq consumer would silently drop events.
    with TemporaryDirectory() as tmp:
        a = ScriptRunStore(Path(tmp) / "runs")
        b = ScriptRunStore(Path(tmp) / "runs")
        a.append_kanban_event("kbc_x", status="blocked", reason="r1")
        b.append_kanban_event("kbc_x", status="completed", result={"plan": "p2"})
        a.append_kanban_event("kbc_x", status="completed", result={"plan": "p3"})
        events = a.read_kanban_events("kbc_x")
        assert [e["seq"] for e in events] == [1, 2, 3]
        assert a.read_kanban_events("kbc_x", after_seq=2) == [events[2]]
        assert a.latest_kanban_resolution("kbc_x")["workflow_result"] == {"plan": "p3"}


def test_event_log_recovers_from_a_torn_trailing_line():
    # Regression (review): a crash can leave a newline-less partial final line; the
    # next append must not concatenate onto it and be lost.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        store.append_kanban_event("kbc_y", status="blocked", reason="r1")
        path = Path(tmp) / "runs" / "_kanban" / "kbc_y.events.jsonl"
        with path.open("a", encoding="utf-8") as f:
            f.write('{"status":"completed","workflow_result":{"partial":1}')  # torn: no newline.
        store.append_kanban_event("kbc_y", status="completed", result={"plan": "saved"})
        latest = store.latest_kanban_resolution("kbc_y")
        assert latest is not None and latest["workflow_result"] == {"plan": "saved"}


def test_event_log_rejects_unsafe_card_id():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        try:
            store.append_kanban_event("../escape", status="completed")
        except ValueError as exc:
            assert "unsafe card_id" in str(exc)
            return
        raise AssertionError("expected ValueError for unsafe card_id")


# --------------------------------------------------------------------------- #
# External-producer resume through the subprocess VM
# --------------------------------------------------------------------------- #

def test_e2e_external_producer_event_resumes_parent_from_the_log():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        card_id = kanban_card_id("A:1")  # run_id "A", stable call id 1.
        # A worker/gateway (a different actor than this parent) durably records the
        # card's completion in the event log — no live backend involved.
        store.append_kanban_event(
            card_id, status="completed", result={"plan": "from external"}, profile="planner"
        )
        # The parent runs with a backend that has no memory of the card and raises
        # if awaited; it must resume from the durable event log.
        res = run_workflow_script(
            _SCRIPT, args={"i": 1}, store=store, run_id="A",
            kanban_backend=DurableKanbanBackend(_BoomBackend(known_profiles={"planner"}), store),
        )
        assert res.ok, res.error
        assert res.value == {"plan": "from external"}
        # The log-sourced outcome is mirrored to the latest-state index.
        assert store.kanban_waits() == []


def test_e2e_log_preferred_over_state_when_both_present():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        card_id = kanban_card_id("A:1")
        # A stale latest-state record and a newer event-log outcome: the log wins.
        store.record_kanban_card_state(
            card_id, {"card_id": card_id, "status": "completed", "workflow_result": {"plan": "STATE"}}
        )
        store.append_kanban_event(card_id, status="completed", result={"plan": "LOG"}, profile="planner")
        res = run_workflow_script(
            _SCRIPT, args={"i": 1}, store=store, run_id="A",
            kanban_backend=DurableKanbanBackend(_BoomBackend(known_profiles={"planner"}), store),
        )
        assert res.ok, res.error
        assert res.value == {"plan": "LOG"}


def test_e2e_state_file_fallback_when_no_log_event():
    # With no event-log entry, resume still works from the PR #22 latest-state file.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        card_id = kanban_card_id("A:1")
        store.record_kanban_card_state(
            card_id, {"card_id": card_id, "status": "completed", "workflow_result": {"plan": "STATE"}}
        )
        res = run_workflow_script(
            _SCRIPT, args={"i": 1}, store=store, run_id="A",
            kanban_backend=DurableKanbanBackend(_BoomBackend(known_profiles={"planner"}), store),
        )
        assert res.ok, res.error
        assert res.value == {"plan": "STATE"}


def test_e2e_external_blocked_event_under_return_surfaces_blocked():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        card_id = kanban_card_id("A:1")
        store.append_kanban_event(card_id, status="blocked", reason="awaiting review", profile="planner")
        script = META + (
            'r = await kanban_agent("planner", prompt="plan", on_block="return")\n'
            'return {"status": r["status"], "reason": r.get("reason")}\n'
        )
        res = run_workflow_script(
            script, args={"i": 1}, store=store, run_id="A",
            kanban_backend=DurableKanbanBackend(_BoomBackend(known_profiles={"planner"}), store),
        )
        assert res.ok, res.error
        assert res.value["status"] == CARD_BLOCKED
        assert res.value["reason"] == "awaiting review"
