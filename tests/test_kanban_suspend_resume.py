"""Tests for durable suspend/resume of an unresolved paused Kanban await (issue #5).

§5.7 made ``on_block="pause"`` keep awaiting a card until a terminal event, bounded
by the run's wall-clock limit — an in-process hold that *failed* the run with
``kanban_timeout`` if the card never resolved in time. This slice (DESIGN §5.9)
lets a paused, unresolved card **suspend** the run instead (opt-in via
``VMLimits.kanban_suspend_after_s``): the parent stops holding the thread, the run
is recorded with status ``suspended``, and a fresh process **resumes** it from a
replayed event via ``replay_from`` once a worker durably produces the event.

Covers: the broker's suspend decision (and that it preserves the prior
block-to-timeout behaviour when the window is unset or ``>= max_runtime_s``), the
limits-view round-trip that pins the window on a replay, the store's
``suspended_runs`` discovery view, and end-to-end suspend → external event →
resume across a backend with no live memory of the card.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from hermes_workflows import run_workflow_script
from hermes_workflows.kanban import kanban_card_id
from hermes_workflows.kanban_notify import (
    EventLogKanbanBackend,
    ThreadEventNotifier,
    publish_kanban_event,
)
from hermes_workflows.script_store import ScriptRunStore
from hermes_workflows.vm import (
    CapabilityBroker,
    VMLimits,
    _CorruptLimitsView,
    _limits_from_view,
    _limits_view,
)

META = 'meta = {"name": "k5s", "description": "d"}\n'

# The suspend path is pause-only, so the e2e scripts pin on_block="pause".
_PAUSE_SCRIPT = META + (
    'r = await kanban_agent("planner", prompt="plan", on_block="pause", schema={"plan": "string"})\n'
    'return r["workflow_result"]\n'
)

# A deterministic, always-cacheable ``log`` call before the pause, to assert that a
# chained resume still serves pre-pause calls from the (root) replay cache.
_LOG_PAUSE_SCRIPT = META + (
    'log("before pause")\n'
    'r = await kanban_agent("planner", prompt="plan", on_block="pause", schema={"plan": "string"})\n'
    'return r["workflow_result"]\n'
)


def _broker(backend, *, root="root", limits=None):
    return CapabilityBroker(
        agent_runner=lambda agent_id, input: {},
        limits=limits or VMLimits(max_runtime_s=2.0),
        kanban_backend=backend,
        idempotency_root=root,
    )


def _kanban_frame(call_id, **params):
    params.setdefault("profile", "planner")
    return {"t": "call", "id": call_id, "method": "kanban_agent", "params": params}


def _event_log_backend(store, notifier=None):
    return EventLogKanbanBackend(
        store, notifier or ThreadEventNotifier(), known_profiles={"planner"}
    )


# --------------------------------------------------------------------------- #
# Broker-level suspend decision
# --------------------------------------------------------------------------- #

def test_pause_without_suspend_window_still_times_out():
    # Default (kanban_suspend_after_s=None): the prior behaviour is preserved — a
    # never-resolving paused card fails with kanban_timeout, not a suspend.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        broker = _broker(_event_log_backend(store), limits=VMLimits(max_runtime_s=0.3))
        ret = broker.handle(_kanban_frame(1, on_block="pause"))
        assert ret["ok"] is False
        assert ret["error"]["code"] == "kanban_timeout"
        assert broker.should_suspend is False


def test_pause_suspend_window_suspends_an_unresolved_card():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        broker = _broker(
            _event_log_backend(store),
            root="root",
            limits=VMLimits(max_runtime_s=5.0, kanban_suspend_after_s=0.2),
        )
        start = time.monotonic()
        ret = broker.handle(_kanban_frame(1, on_block="pause"))
        elapsed = time.monotonic() - start
        assert ret["ok"] is False
        assert ret["error"]["code"] == "kanban_suspended"
        assert broker.should_suspend is True
        # Suspended on the suspend window, well before the run deadline.
        assert elapsed < 2.0
        info = broker.suspend_info
        assert info["card_id"] == kanban_card_id("root:1")
        assert info["profile"] == "planner"
        assert info["call_id"] == 1
        assert info["on_block"] == "pause"


def test_suspend_window_capped_at_run_deadline_times_out_not_suspends():
    # A suspend window >= max_runtime_s never preempts the genuine kanban_timeout:
    # the run deadline wins, so the run fails rather than suspending.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        broker = _broker(
            _event_log_backend(store),
            limits=VMLimits(max_runtime_s=0.3, kanban_suspend_after_s=5.0),
        )
        ret = broker.handle(_kanban_frame(1, on_block="pause"))
        assert ret["error"]["code"] == "kanban_timeout"
        assert broker.should_suspend is False


def test_non_pause_policy_never_suspends_even_with_a_window():
    # on_block="return" accepts a blocked event and resolves; with no event it
    # times out. Either way the suspend window (a pause-only concept) is inert.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        broker = _broker(
            _event_log_backend(store),
            limits=VMLimits(max_runtime_s=0.3, kanban_suspend_after_s=0.05),
        )
        ret = broker.handle(_kanban_frame(1, on_block="return"))
        assert ret["error"]["code"] == "kanban_timeout"
        assert broker.should_suspend is False


# --------------------------------------------------------------------------- #
# Limits-view round-trip (replay pins the suspend window)
# --------------------------------------------------------------------------- #

def test_limits_view_round_trips_suspend_window():
    view = _limits_view(VMLimits(max_runtime_s=2.0, kanban_suspend_after_s=0.5))
    assert view["kanban_suspend_after_s"] == 0.5
    assert _limits_from_view(view).kanban_suspend_after_s == 0.5

    # None (no window) is distinct from an absent key and round-trips to None.
    none_view = _limits_view(VMLimits())
    assert none_view["kanban_suspend_after_s"] is None
    assert _limits_from_view(none_view).kanban_suspend_after_s is None


def test_limits_view_rejects_a_corrupt_suspend_window():
    for bad in ("0.5", True, float("inf")):
        try:
            _limits_from_view({"kanban_suspend_after_s": bad})
        except _CorruptLimitsView:
            continue
        raise AssertionError(f"expected _CorruptLimitsView for {bad!r}")


# --------------------------------------------------------------------------- #
# ScriptRunStore.suspended_runs discovery
# --------------------------------------------------------------------------- #

def test_suspended_runs_lists_only_suspended():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        for run_id, status in (("ok", "succeeded"), ("susp", "suspended"), ("bad", "failed")):
            store.begin(run_id, script="x", args=None, limits=None, deterministic_runner=False)
            store.finish(run_id, status=status, meta=None, value=None, error=None)
        # An unrelated _kanban dir (fails the run-id guard) must not break the scan.
        store.record_kanban_card_state("kbc_x", {"status": "waiting"})
        ids = [m.run_id for m in store.suspended_runs()]
        assert ids == ["susp"]


# --------------------------------------------------------------------------- #
# End-to-end: suspend -> external event -> resume
# --------------------------------------------------------------------------- #

def test_e2e_suspend_then_resume_from_an_externally_produced_event():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()

        # Run A: no event is ever produced, so the paused await suspends.
        a = run_workflow_script(
            _PAUSE_SCRIPT, args={"i": 1}, store=store, run_id="A",
            limits=VMLimits(max_runtime_s=5.0, kanban_suspend_after_s=0.2),
            kanban_backend=_event_log_backend(store, notifier),
        )
        assert a.ok is False and a.suspended is True
        assert a.error["type"] == "KanbanSuspended"
        card_id = kanban_card_id("A:1")
        assert a.error["card_id"] == card_id
        assert store.load_run("A").status == "suspended"
        # The suspended run is discoverable and the card is still an in-flight wait.
        assert [m.run_id for m in store.suspended_runs()] == ["A"]
        assert [w["card_id"] for w in store.kanban_waits()] == [card_id]

        # A worker/gateway (possibly a different process) durably produces the
        # terminal event in the event log.
        publish_kanban_event(
            store, notifier, card_id, status="completed",
            result={"plan": "resumed"}, profile="planner",
        )

        # Resume: a fresh run replays A; the paused kanban_agent reattaches the same
        # card and resolves from the durable log. No suspend window needed now.
        b = run_workflow_script(
            _PAUSE_SCRIPT, args={"i": 1}, store=store, run_id="B", replay_from="A",
            kanban_backend=_event_log_backend(store, notifier),
        )
        assert b.ok is True, b.error
        assert b.value == {"plan": "resumed"}
        assert store.load_run("B").status == "succeeded"
        assert store.kanban_waits() == []  # outcome mirrored; no longer in-flight.


def test_e2e_resume_suspends_again_while_card_unresolved_then_resolves():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        suspend_limits = VMLimits(max_runtime_s=5.0, kanban_suspend_after_s=0.2)

        a = run_workflow_script(
            _PAUSE_SCRIPT, args={"i": 1}, store=store, run_id="A",
            limits=suspend_limits, kanban_backend=_event_log_backend(store, notifier),
        )
        assert a.suspended is True

        # Resume while STILL unresolved (no event yet): the replay pins A's suspend
        # window, so the run suspends again rather than blocking to the deadline.
        b = run_workflow_script(
            _PAUSE_SCRIPT, args={"i": 1}, store=store, run_id="B", replay_from="A",
            kanban_backend=_event_log_backend(store, notifier),
        )
        assert b.suspended is True
        assert b.error["card_id"] == kanban_card_id("A:1")  # same logical card.

        # Now the event arrives; resuming from A again resolves.
        publish_kanban_event(
            store, notifier, kanban_card_id("A:1"), status="completed",
            result={"plan": "finally"}, profile="planner",
        )
        c = run_workflow_script(
            _PAUSE_SCRIPT, args={"i": 1}, store=store, run_id="C", replay_from="A",
            kanban_backend=_event_log_backend(store, notifier),
        )
        assert c.ok is True, c.error
        assert c.value == {"plan": "finally"}


def test_e2e_concurrent_producer_resolves_within_the_suspend_window():
    # A producer that completes the card *inside* the suspend window resolves the
    # paused await in-process (no suspend) — fast unblocks still work as before.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        card_id = kanban_card_id("A:1")

        def _produce():
            time.sleep(0.1)
            publish_kanban_event(
                store, notifier, card_id, status="completed",
                result={"plan": "in time"}, profile="planner",
            )

        worker = threading.Thread(target=_produce)
        worker.start()
        try:
            a = run_workflow_script(
                _PAUSE_SCRIPT, args={"i": 1}, store=store, run_id="A",
                limits=VMLimits(max_runtime_s=5.0, kanban_suspend_after_s=2.0),
                kanban_backend=_event_log_backend(store, notifier),
            )
        finally:
            worker.join()
        assert a.ok is True, a.error
        assert a.suspended is False
        assert a.value == {"plan": "in time"}


def test_e2e_chained_resume_serves_pre_pause_calls_from_root_cache():
    # Regression (review): a resumed run writes no cache.jsonl of its own, so a
    # chained resume (resuming the *suspended replay* B, which is what
    # suspended_runs() surfaces) must still serve pre-pause deterministic calls from
    # the logical ROOT's cache — not B's empty cache.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        suspend_limits = VMLimits(max_runtime_s=5.0, kanban_suspend_after_s=0.2)
        # The pause is the 2nd RPC call (log is the 1st), so the card keys on ":2".
        card_id = kanban_card_id("A:2")

        a = run_workflow_script(
            _LOG_PAUSE_SCRIPT, args={"i": 1}, store=store, run_id="A",
            limits=suspend_limits, kanban_backend=_event_log_backend(store, notifier),
        )
        assert a.suspended is True
        assert store.load_cache("A").__len__() == 1  # the log call was recorded.

        # B resumes A directly: the log replays from A's cache; B itself records none.
        b = run_workflow_script(
            _LOG_PAUSE_SCRIPT, args={"i": 1}, store=store, run_id="B", replay_from="A",
            kanban_backend=_event_log_backend(store, notifier),
        )
        assert b.suspended is True
        assert b.replayed_calls == 1

        # The card resolves, then C resumes the *chained* suspended run B. Without
        # the root-cache fix, load_cache("B") is empty and the log re-dispatches
        # (replayed_calls == 0); with it, the log is served from A's cache.
        publish_kanban_event(
            store, notifier, card_id, status="completed",
            result={"plan": "done"}, profile="planner",
        )
        c = run_workflow_script(
            _LOG_PAUSE_SCRIPT, args={"i": 1}, store=store, run_id="C", replay_from="B",
            kanban_backend=_event_log_backend(store, notifier),
        )
        assert c.ok is True, c.error
        assert c.value == {"plan": "done"}
        assert c.replayed_calls == 1  # pre-pause log served from the ROOT (A) cache.
