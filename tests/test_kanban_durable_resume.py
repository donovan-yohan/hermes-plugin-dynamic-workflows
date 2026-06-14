"""Tests for durable Kanban card state + resume-across-restart (issue #5).

A Kanban await is non-deterministic, so it is excluded from the #3 replay cache.
This slice persists the latest state of each card under the run store
(``<root>/_kanban/<card_id>.json``, keyed by the content-addressed card id) so a
restarted or replaying parent **resumes from a recorded outcome** instead of
re-awaiting — or losing — the worker's result. :class:`DurableKanbanBackend`
wraps any inner backend with that persistence.

Covers: the store's record/load/monotonic/waits surface, the resolution
serialisers, the wrapper's record-on-await and resume-on-first-await behaviour,
and an end-to-end resume in which the inner backend has no memory of the card.
"""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from hermes_workflows import run_workflow_script
from hermes_workflows.kanban import (
    CARD_COMPLETED,
    DurableKanbanBackend,
    InMemoryKanbanBackend,
    KanbanCardSpec,
    KanbanResolution,
    kanban_card_id,
)
from hermes_workflows.kanban import _resolution_to_state, _state_to_resolution
from hermes_workflows.script_store import ScriptRunStore

META = 'meta = {"name": "k5d", "description": "d"}\n'

_SCRIPT = META + (
    'r = await kanban_agent("planner", prompt="plan", schema={"plan": "string"})\n'
    'return r["workflow_result"]\n'
)


class _BoomBackend(InMemoryKanbanBackend):
    """An inner backend that fails if awaited — proves resume never touches it."""

    def await_resolution(self, *args, **kwargs):  # noqa: D401, ANN002, ANN003
        raise AssertionError("inner await_resolution must not be called on a durable resume")


def _completed(card_id, result, *, version=1, profile="planner"):
    return KanbanResolution(
        card_id=card_id, profile=profile, status=CARD_COMPLETED, result=result, version=version
    )


# --------------------------------------------------------------------------- #
# ScriptRunStore: durable card state
# --------------------------------------------------------------------------- #

def test_store_card_state_round_trips_and_is_version_monotonic():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        assert store.load_kanban_card_state("kbc_abc") is None
        store.record_kanban_card_state("kbc_abc", {"status": "waiting", "version": 0, "profile": "p"})
        assert store.load_kanban_card_state("kbc_abc")["status"] == "waiting"
        store.record_kanban_card_state(
            "kbc_abc", {"status": "completed", "version": 2, "workflow_result": {"plan": "x"}}
        )
        assert store.load_kanban_card_state("kbc_abc")["status"] == "completed"
        # An older version never overwrites a newer one.
        store.record_kanban_card_state("kbc_abc", {"status": "waiting", "version": 1})
        assert store.load_kanban_card_state("kbc_abc")["status"] == "completed"


def test_store_rejects_unsafe_card_id_without_writing():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        try:
            store.record_kanban_card_state("../escape", {"status": "waiting"})
        except ValueError as exc:
            assert "unsafe card_id" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("expected ValueError for unsafe card_id")
        assert not (Path(tmp) / "escape.json").exists()


def test_store_corrupt_card_state_is_treated_as_absent():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        store.record_kanban_card_state("kbc_x", {"status": "waiting", "version": 0})
        (Path(tmp) / "runs" / "_kanban" / "kbc_x.json").write_text("{ broken", encoding="utf-8")
        assert store.load_kanban_card_state("kbc_x") is None  # fail-safe: re-await.


def test_store_kanban_waits_excludes_terminal():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        store.record_kanban_card_state("kbc_wait", {"status": "waiting", "version": 0})
        store.record_kanban_card_state("kbc_block", {"status": "blocked", "version": 1})
        store.record_kanban_card_state("kbc_done", {"status": "completed", "version": 1})
        waits = {w["card_id"] for w in store.kanban_waits()}
        assert waits == {"kbc_wait", "kbc_block"}  # blocked is still an in-flight wait.


# --------------------------------------------------------------------------- #
# Resolution serialisers
# --------------------------------------------------------------------------- #

def test_resolution_state_round_trip():
    res = _completed("kbc_1", {"plan": "x"}, version=3)
    state = _resolution_to_state(res)
    assert state["workflow_result"] == {"plan": "x"} and state["version"] == 3
    back = _state_to_resolution(state)
    assert back == res


def test_waiting_marker_is_not_a_resolution():
    assert _state_to_resolution({"status": "waiting", "version": 0}) is None
    assert _state_to_resolution(None) is None
    assert _state_to_resolution({"status": "bogus"}) is None


# --------------------------------------------------------------------------- #
# DurableKanbanBackend wrapper
# --------------------------------------------------------------------------- #

def test_wrapper_records_waiting_then_resolution():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        inner = InMemoryKanbanBackend(known_profiles={"planner"})
        dur = DurableKanbanBackend(inner, store)
        card = dur.create_or_reattach("k:1", KanbanCardSpec(profile="planner"))
        assert card.reattached is False
        assert store.load_kanban_card_state(card.card_id)["status"] == "waiting"

        inner.resolve(card.card_id, CARD_COMPLETED, result={"plan": "done"})
        res = dur.await_resolution(card.card_id, accept_blocked=True, timeout=1.0)
        assert res.status == CARD_COMPLETED
        # The outcome is now durably recorded.
        assert store.load_kanban_card_state(card.card_id)["status"] == "completed"
        assert store.load_kanban_card_state(card.card_id)["workflow_result"] == {"plan": "done"}


def test_wrapper_reattaches_when_durable_record_exists():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        card_id = kanban_card_id("k:1")
        store.record_kanban_card_state(card_id, _resolution_to_state(_completed(card_id, {"plan": "x"})))
        dur = DurableKanbanBackend(InMemoryKanbanBackend(known_profiles={"planner"}), store)
        card = dur.create_or_reattach("k:1", KanbanCardSpec(profile="planner"))
        assert card.reattached is True


def test_wrapper_resumes_from_record_without_touching_inner():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        card_id = kanban_card_id("k:1")
        store.record_kanban_card_state(card_id, _resolution_to_state(_completed(card_id, {"plan": "x"})))
        # A Boom inner would raise if awaited; the record must be served instead.
        dur = DurableKanbanBackend(_BoomBackend(known_profiles={"planner"}), store)
        res = dur.await_resolution(card_id, accept_blocked=True, timeout=0.2, after_version=0)
        assert res.status == CARD_COMPLETED and res.result == {"plan": "x"}


def test_wrapper_retry_after_version_goes_live_not_to_record():
    # A retry (after_version > 0) means the broker already rejected an outcome this
    # run, so it must go to the inner backend, not re-serve the stale record — even
    # when the record's version is higher than after_version.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        card_id = kanban_card_id("k:1")
        store.record_kanban_card_state(
            card_id, _resolution_to_state(_completed(card_id, {"plan": "stale"}, version=5))
        )
        inner = InMemoryKanbanBackend(known_profiles={"planner"})
        inner.resolve(card_id, CARD_COMPLETED, result={"plan": "old"})   # inner version 1
        inner.resolve(card_id, CARD_COMPLETED, result={"plan": "live"})  # inner version 2
        dur = DurableKanbanBackend(inner, store)
        res = dur.await_resolution(card_id, accept_blocked=True, timeout=1.0, after_version=1)
        assert res.result == {"plan": "live"}  # from inner (version 2 > 1), not the record.


def test_wrapper_waiting_only_record_re_awaits_inner():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        card_id = kanban_card_id("k:1")
        store.record_kanban_card_state(card_id, {"card_id": card_id, "status": "waiting", "version": 0})
        inner = InMemoryKanbanBackend(known_profiles={"planner"})
        inner.resolve(card_id, CARD_COMPLETED, result={"plan": "live"})
        dur = DurableKanbanBackend(inner, store)
        # A waiting-only record is not a resolution -> re-await the (live) inner.
        res = dur.await_resolution(card_id, accept_blocked=True, timeout=1.0)
        assert res.result == {"plan": "live"}


# --------------------------------------------------------------------------- #
# End-to-end resume through the subprocess VM + durable store
# --------------------------------------------------------------------------- #

def test_e2e_resume_serves_recorded_result_across_a_memoryless_backend():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        live = InMemoryKanbanBackend(
            auto=lambda spec: {"status": "completed", "result": {"plan": "the plan"}},
            known_profiles={"planner"},
        )
        a = run_workflow_script(
            _SCRIPT, args={"i": 1}, store=store, run_id="A",
            kanban_backend=DurableKanbanBackend(live, store),
        )
        assert a.ok, a.error
        assert a.value == {"plan": "the plan"}
        assert store.load_kanban_card_state(kanban_card_id("A:1"))["status"] == "completed"

        # Resume: replay run B with an inner backend that has NO memory of the card
        # (and raises if awaited). The result is served from the durable record.
        b = run_workflow_script(
            _SCRIPT, args={"i": 1}, store=store, run_id="B", replay_from="A",
            kanban_backend=DurableKanbanBackend(_BoomBackend(known_profiles={"planner"}), store),
        )
        assert b.ok, b.error
        assert b.value == {"plan": "the plan"}  # resumed across a memoryless backend.


def test_e2e_completed_card_is_not_listed_as_a_wait():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        live = InMemoryKanbanBackend(
            auto=lambda spec: {"status": "completed", "result": {"plan": "x"}},
            known_profiles={"planner"},
        )
        run_workflow_script(
            _SCRIPT, args={"i": 1}, store=store, run_id="A",
            kanban_backend=DurableKanbanBackend(live, store),
        )
        assert store.kanban_waits() == []  # the card reached a terminal outcome.
