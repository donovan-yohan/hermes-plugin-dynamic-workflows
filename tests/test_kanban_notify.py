"""Tests for cross-process event notification for durable Kanban awaits (issue #5).

The durable event log replays *already-produced* events; this adds the live
**wakeup** a parent blocking on a not-yet-produced event needs. A
`KanbanEventNotifier` decouples the wakeup from the event source: `ThreadEventNotifier`
in-process, `FifoEventNotifier` cross-process (POSIX FIFOs). `EventLogKanbanBackend`
resolves a card purely from the durable log, blocking on the notifier between log
reads (event-driven, not polling) and bounded by the run deadline; the durable log
is the source of truth, so a missed/raced signal is never a lost event.
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from hermes_workflows import run_workflow_script
from hermes_workflows.kanban import KanbanCardSpec, KanbanTimeout, KanbanUnknownProfile, kanban_card_id
from hermes_workflows.kanban_notify import (
    EventLogKanbanBackend,
    FifoEventNotifier,
    ThreadEventNotifier,
    publish_kanban_event,
)
from hermes_workflows.script_store import ScriptRunStore

META = 'meta = {"name": "k5n", "description": "d"}\n'

_SCRIPT = META + (
    'r = await kanban_agent("planner", prompt="plan", schema={"plan": "string"})\n'
    'return r["workflow_result"]\n'
)


# --------------------------------------------------------------------------- #
# Notifiers
# --------------------------------------------------------------------------- #

def test_thread_notifier_wakes_and_times_out():
    n = ThreadEventNotifier()
    sub = n.subscribe("kbc_a")
    try:
        assert sub.wait(0.1) is False  # no notify -> timeout.

        def _notify():
            time.sleep(0.05)
            n.notify("kbc_a")

        worker = threading.Thread(target=_notify)
        worker.start()
        try:
            assert sub.wait(2.0) is True  # woken by the notify.
        finally:
            worker.join()
    finally:
        sub.close()


def test_thread_notifier_does_not_lose_a_notify_that_races_before_wait():
    n = ThreadEventNotifier()
    sub = n.subscribe("kbc_a")
    try:
        n.notify("kbc_a")  # signalled before the consumer waits.
        assert sub.wait(2.0) is True  # the buffered signal is still observed.
    finally:
        sub.close()


def test_fifo_notifier_wakes_across_independent_instances():
    with TemporaryDirectory() as tmp:
        producer = FifoEventNotifier(Path(tmp) / "_kanban")
        consumer = FifoEventNotifier(Path(tmp) / "_kanban")
        sub = consumer.subscribe("kbc_x")
        try:
            assert sub.wait(0.1) is False
            worker = threading.Thread(target=lambda: (time.sleep(0.05), producer.notify("kbc_x")))
            worker.start()
            try:
                assert sub.wait(2.0) is True  # a separate notifier instance woke us via the FIFO.
            finally:
                worker.join()
        finally:
            sub.close()


def test_fifo_subscription_does_not_spin_after_a_notify_and_leave():
    # Regression (review): the held write end keeps the read end from EOFing, which
    # select reports as readable forever. After a producer notify-and-leave, the
    # next wait must BLOCK (time out), not return True instantly in a busy-spin.
    with TemporaryDirectory() as tmp:
        producer = FifoEventNotifier(Path(tmp) / "_kanban")
        consumer = FifoEventNotifier(Path(tmp) / "_kanban")
        sub = consumer.subscribe("kbc_x")
        try:
            producer.notify("kbc_x")  # producer writes a byte and leaves (no lingering writer).
            assert sub.wait(1.0) is True  # observe the notify.
            start = time.monotonic()
            assert sub.wait(0.2) is False  # no new event: blocks to timeout, not instant-True.
            assert time.monotonic() - start >= 0.15
        finally:
            sub.close()


def test_fifo_notify_with_no_subscriber_is_harmless():
    with TemporaryDirectory() as tmp:
        n = FifoEventNotifier(Path(tmp) / "_kanban")
        n.notify("kbc_x")  # no reader attached: must not raise (consumer reads the log).


def test_fifo_notifier_rejects_unsafe_card_id():
    with TemporaryDirectory() as tmp:
        n = FifoEventNotifier(Path(tmp) / "_kanban")
        try:
            n.notify("../escape")
        except ValueError as exc:
            assert "unsafe card_id" in str(exc)
            return
        raise AssertionError("expected ValueError for unsafe card_id")


def test_fifo_notifier_wakes_across_a_real_process():
    if not hasattr(os, "fork"):  # POSIX only.
        return
    with TemporaryDirectory() as tmp:
        consumer = FifoEventNotifier(Path(tmp) / "_kanban")
        sub = consumer.subscribe("kbc_p")
        pid = os.fork()
        if pid == 0:  # child process: notify via its OWN notifier instance, then exit hard.
            try:
                time.sleep(0.1)
                FifoEventNotifier(Path(tmp) / "_kanban").notify("kbc_p")
            finally:
                os._exit(0)
        try:
            assert sub.wait(5.0) is True  # woken by a genuinely separate process.
        finally:
            os.waitpid(pid, 0)
            sub.close()


# --------------------------------------------------------------------------- #
# EventLogKanbanBackend
# --------------------------------------------------------------------------- #

def test_backend_rejects_unknown_profile():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = EventLogKanbanBackend(store, ThreadEventNotifier(), known_profiles={"planner"})
        try:
            backend.create_or_reattach("k:1", KanbanCardSpec(profile="ghost"))
        except KanbanUnknownProfile:
            return
        raise AssertionError("expected KanbanUnknownProfile")


def test_backend_resolves_a_preexisting_log_event_without_waiting():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        backend = EventLogKanbanBackend(store, notifier, known_profiles={"planner"})
        card = backend.create_or_reattach("k:1", KanbanCardSpec(profile="planner"))
        publish_kanban_event(store, notifier, card.card_id, status="completed", result={"plan": "done"})
        res = backend.await_resolution(card.card_id, accept_blocked=True, timeout=0.5)
        assert res.status == "completed" and res.result == {"plan": "done"}


def test_backend_resolution_clears_the_in_flight_wait_view():
    # Regression (review): a resolved card must be mirrored to the latest-state
    # index so kanban_waits() stops reporting it as in-flight (the create_or_reattach
    # 'waiting' marker would otherwise leak forever).
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        backend = EventLogKanbanBackend(store, notifier, known_profiles={"planner"})
        card = backend.create_or_reattach("k:1", KanbanCardSpec(profile="planner"))
        assert [w["card_id"] for w in store.kanban_waits()] == [card.card_id]  # in-flight.
        publish_kanban_event(store, notifier, card.card_id, status="completed", result={"plan": "x"})
        backend.await_resolution(card.card_id, accept_blocked=True, timeout=0.5)
        assert store.kanban_waits() == []  # no longer reported as waiting.


def test_backend_is_woken_by_a_concurrent_producer():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        backend = EventLogKanbanBackend(store, notifier, known_profiles={"planner"})
        card = backend.create_or_reattach("k:1", KanbanCardSpec(profile="planner"))

        def _produce():
            time.sleep(0.1)
            publish_kanban_event(store, notifier, card.card_id, status="completed", result={"plan": "live"})

        worker = threading.Thread(target=_produce)
        worker.start()
        try:
            res = backend.await_resolution(card.card_id, accept_blocked=True, timeout=3.0)
        finally:
            worker.join()
        assert res.result == {"plan": "live"}


def test_backend_times_out_when_no_event_is_produced():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = EventLogKanbanBackend(store, ThreadEventNotifier(), known_profiles={"planner"})
        card = backend.create_or_reattach("k:1", KanbanCardSpec(profile="planner"))
        start = time.monotonic()
        try:
            backend.await_resolution(card.card_id, accept_blocked=True, timeout=0.2)
        except KanbanTimeout:
            assert time.monotonic() - start < 1.0  # bounded by the deadline.
            return
        raise AssertionError("expected KanbanTimeout")


def test_backend_pause_skips_blocked_then_resolves_on_terminal():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        backend = EventLogKanbanBackend(store, notifier, known_profiles={"planner"})
        card = backend.create_or_reattach("k:1", KanbanCardSpec(profile="planner"))
        publish_kanban_event(store, notifier, card.card_id, status="blocked", reason="needs input")

        def _complete():
            time.sleep(0.1)
            publish_kanban_event(store, notifier, card.card_id, status="completed", result={"plan": "ok"})

        worker = threading.Thread(target=_complete)
        worker.start()
        try:
            # accept_blocked=False (pause): the blocked event is skipped, terminal resolves.
            res = backend.await_resolution(card.card_id, accept_blocked=False, timeout=3.0)
        finally:
            worker.join()
        assert res.status == "completed"


def test_backend_after_version_cursor_waits_for_a_newer_log_event():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        backend = EventLogKanbanBackend(store, notifier, known_profiles={"planner"})
        card = backend.create_or_reattach("k:1", KanbanCardSpec(profile="planner"))
        publish_kanban_event(store, notifier, card.card_id, status="completed", result={"plan": "first"})
        publish_kanban_event(store, notifier, card.card_id, status="completed", result={"plan": "second"})
        # after_version=1 -> skip line 1, resolve from the line-2 event.
        res = backend.await_resolution(card.card_id, accept_blocked=True, timeout=0.5, after_version=1)
        assert res.result == {"plan": "second"} and res.version == 2


# --------------------------------------------------------------------------- #
# End-to-end through the subprocess VM
# --------------------------------------------------------------------------- #

def test_e2e_event_driven_resolution_through_the_vm():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        card_id = kanban_card_id("A:1")

        def _produce():
            time.sleep(0.2)
            publish_kanban_event(
                store, notifier, card_id, status="completed", result={"plan": "published"}, profile="planner"
            )

        worker = threading.Thread(target=_produce)
        worker.start()
        try:
            res = run_workflow_script(
                _SCRIPT, args={"i": 1}, store=store, run_id="A",
                kanban_backend=EventLogKanbanBackend(store, notifier, known_profiles={"planner"}),
            )
        finally:
            worker.join()
        assert res.ok, res.error
        assert res.value == {"plan": "published"}
