"""Tests for ``kanban_agent`` as a durable, idempotent awaitable (issue #5).

Covers the acceptance criteria:

* **Card creation** — ``await kanban_agent`` opens exactly one card and resumes
  with a structured ``completed`` result.
* **Parent linking / spec fidelity** — board/tenant/parents/labels/context reach
  the parent-owned card spec (and never the subprocess).
* **Unknown assignee** — an unrecognised profile is rejected with a structured
  diagnostic, not a silent open.
* **Idempotency / no duplicate on replay** — replaying a run reattaches the same
  card rather than opening a second one, even though a live Kanban call is never
  served from the #3 deterministic replay cache.
* **Event wakeup & on_block** — the await is woken by a card event (no polling),
  bounded by the run's wall-clock limit, and honours pause/raise/return.

The backend under test is the in-memory honest fake; broker-level tests drive the
:class:`CapabilityBroker` directly, end-to-end tests go through the real
subprocess VM.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from hermes_workflows import run_workflow_script
from hermes_workflows.kanban import (
    CARD_BLOCKED,
    CARD_COMPLETED,
    InMemoryKanbanBackend,
    KanbanCardSpec,
    KanbanTimeout,
    kanban_card_id,
    normalize_on_block,
)
from hermes_workflows.script_store import ScriptRunStore
from hermes_workflows.controls import InMemoryControlStore, stop_run, stop_task
from hermes_workflows.vm import CapabilityBroker, VMLimits

META = 'meta = {"name": "k5", "description": "d"}\n'


def _broker(backend, *, root="root", limits=None, control_store=None):
    return CapabilityBroker(
        # A stub runner is present but unused on the durable Kanban path.
        agent_runner=lambda agent_id, input: {},
        limits=limits or VMLimits(),
        kanban_backend=backend,
        idempotency_root=root,
        control_store=control_store,
    )


def _kanban_frame(call_id, **params):
    params.setdefault("profile", "planner")
    return {"t": "call", "id": call_id, "method": "kanban_agent", "params": params}


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #

def test_normalize_on_block_policy():
    assert normalize_on_block(None) == "return"
    for policy in ("pause", "raise", "return"):
        assert normalize_on_block(policy) == policy
    try:
        normalize_on_block("explode")
    except ValueError:
        return
    raise AssertionError("expected ValueError for an unknown on_block policy")


def test_card_id_is_deterministic_for_a_key():
    assert kanban_card_id("run:3") == kanban_card_id("run:3")
    assert kanban_card_id("run:3") != kanban_card_id("run:4")
    assert kanban_card_id("run:3").startswith("kbc_")


# --------------------------------------------------------------------------- #
# Backend: idempotency + event-driven await
# --------------------------------------------------------------------------- #

def test_backend_reattaches_same_card_for_repeated_key():
    backend = InMemoryKanbanBackend(known_profiles={"planner"})
    spec = KanbanCardSpec(profile="planner", context={"i": 1})
    first = backend.create_or_reattach("r:1", spec)
    second = backend.create_or_reattach("r:1", spec)
    assert first.card_id == second.card_id
    assert first.reattached is False and second.reattached is True
    assert backend.created_cards == [first.card_id]  # exactly one real card.
    assert backend.reattachments == 1


def test_backend_await_blocks_until_event_then_wakes():
    backend = InMemoryKanbanBackend(known_profiles={"planner"})
    card = backend.create_or_reattach("r:1", KanbanCardSpec(profile="planner"))

    # No event yet: a bounded await must time out rather than resolve spuriously.
    try:
        backend.await_resolution(card.card_id, accept_blocked=True, timeout=0.15)
    except KanbanTimeout:
        pass
    else:  # pragma: no cover
        raise AssertionError("await resolved with no event published")

    # A resolve() from another thread wakes the await (event-driven, no polling).
    def _resolve_soon():
        time.sleep(0.05)
        backend.resolve(card.card_id, CARD_COMPLETED, result={"plan": "ok"})

    worker = threading.Thread(target=_resolve_soon)
    worker.start()
    try:
        resolution = backend.await_resolution(card.card_id, accept_blocked=True, timeout=2.0)
    finally:
        worker.join()
    assert resolution.status == CARD_COMPLETED
    assert resolution.result == {"plan": "ok"}


def test_backend_pause_ignores_blocked_until_terminal():
    backend = InMemoryKanbanBackend(known_profiles={"planner"})
    card = backend.create_or_reattach("r:1", KanbanCardSpec(profile="planner"))
    # A blocked event with accept_blocked=False (pause) must NOT resolve the await.
    backend.resolve(card.card_id, CARD_BLOCKED, reason="needs input")
    try:
        backend.await_resolution(card.card_id, accept_blocked=False, timeout=0.15)
    except KanbanTimeout:
        pass
    else:  # pragma: no cover
        raise AssertionError("pause await resolved on a blocked event")
    # A later terminal event resumes it.
    backend.resolve(card.card_id, CARD_COMPLETED, result={"done": True})
    resolution = backend.await_resolution(card.card_id, accept_blocked=False, timeout=1.0)
    assert resolution.status == CARD_COMPLETED


# --------------------------------------------------------------------------- #
# Broker: on_block policy + diagnostics
# --------------------------------------------------------------------------- #

def test_broker_completed_returns_structured_result():
    backend = InMemoryKanbanBackend(auto="completed", known_profiles={"planner"})
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(1, context={"issue": "#9"}, on_block="return"))
    assert ret["ok"] is True, ret
    value = ret["value"]
    assert value["status"] == CARD_COMPLETED
    assert value["card_id"].startswith("kbc_")
    assert value["reattached"] is False
    assert value["profile"] == "planner"


def test_broker_ignores_negative_token_usage_from_kanban_resolution():
    # Match replay-cache accounting: a backend/result must not be able to skew the
    # budget downward with a negative token value.
    backend = InMemoryKanbanBackend(
        auto=lambda spec: {"status": CARD_COMPLETED, "result": {"_tokens": -50}},
        known_profiles={"planner"},
    )
    broker = _broker(backend, limits=VMLimits(token_budget=10))
    ret = broker.handle(_kanban_frame(1, on_block="return"))
    assert ret["ok"] is True, ret
    assert ret["budget"] == {"total": 10, "spent": 0, "remaining": 10}


def test_broker_blocked_return_surfaces_blocked_status():
    backend = InMemoryKanbanBackend(auto="blocked", known_profiles={"planner"})
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(1, on_block="return"))
    assert ret["ok"] is True
    assert ret["value"]["status"] == CARD_BLOCKED


def test_broker_blocked_raise_denies_into_script():
    backend = InMemoryKanbanBackend(auto="blocked", known_profiles={"planner"})
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(1, on_block="raise"))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "kanban_blocked"


def test_broker_pause_then_unblock_resolves_completed():
    backend = InMemoryKanbanBackend(known_profiles={"planner"})
    broker = _broker(backend, root="paused", limits=VMLimits(max_runtime_s=2.0))
    card_id = kanban_card_id("paused:1")

    # The card is blocked, then a "human" completes it shortly after; the pause
    # await must skip the blocked event and resolve on the terminal one.
    def _drive():
        backend.resolve(card_id, CARD_BLOCKED, reason="awaiting review")
        time.sleep(0.05)
        backend.resolve(card_id, CARD_COMPLETED, result={"unblocked": True})

    worker = threading.Thread(target=_drive)
    worker.start()
    try:
        ret = broker.handle(_kanban_frame(1, on_block="pause"))
    finally:
        worker.join()
    assert ret["ok"] is True
    assert ret["value"]["status"] == CARD_COMPLETED
    assert ret["value"]["workflow_result"] == {"unblocked": True}


def test_broker_unknown_profile_is_rejected_with_diagnostic():
    backend = InMemoryKanbanBackend(auto="completed", known_profiles={"planner"})
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(1, profile="ghost", on_block="return"))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "unknown_profile"
    assert "ghost" in ret["error"]["message"]
    assert backend.created_cards == []  # nothing opened for an unknown assignee.


def test_control_task_stop_blocks_kanban_child_before_card_create():
    controls = InMemoryControlStore()
    stop_task(controls, "root", "qa-label", reason="cancel just this child")
    backend = InMemoryKanbanBackend(auto="completed", known_profiles={"planner"})
    broker = _broker(backend, root="root", control_store=controls)

    ret = broker.handle(_kanban_frame(1, labels=["qa-label"], on_block="return"))

    assert ret["ok"] is False
    assert ret["error"]["code"] == "task_stopped"
    assert backend.created_cards == []


def test_broker_await_timeout_is_bounded_by_runtime_limit():
    # A card that never resolves must not hang the broker forever: the await is
    # bounded by the run's wall-clock limit and surfaces a structured denial.
    backend = InMemoryKanbanBackend(known_profiles={"planner"})  # auto=None: never resolves.
    broker = _broker(backend, limits=VMLimits(max_runtime_s=0.2))
    ret = broker.handle(_kanban_frame(1, on_block="return"))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "kanban_timeout"


def test_broker_kanban_await_bounded_by_shared_run_deadline():
    # Regression (review): the await is bounded by the broker's ABSOLUTE run
    # deadline (armed at construction, shared with the _drive watchdog), not a
    # fresh max_runtime_s window per call. Once the deadline has elapsed, a
    # never-resolving card times out near-immediately instead of blocking for
    # another full window — so a late call can't stretch wall-clock toward 2x.
    backend = InMemoryKanbanBackend(known_profiles={"planner"})  # never resolves.
    broker = _broker(backend, limits=VMLimits(max_runtime_s=0.2))
    time.sleep(0.25)  # let the shared deadline elapse before the call arrives.
    start = time.monotonic()
    ret = broker.handle(_kanban_frame(1, on_block="return"))
    elapsed = time.monotonic() - start
    assert ret["ok"] is False
    assert ret["error"]["code"] == "kanban_timeout"
    assert elapsed < 0.1  # a fresh window would block ~0.2s; shared deadline -> ~0.


def test_broker_invalid_on_block_is_bad_request():
    backend = InMemoryKanbanBackend(auto="completed", known_profiles={"planner"})
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(1, on_block="nonsense"))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "bad_request"


def test_broker_forwards_full_spec_to_backend():
    # Parent linking + spec fidelity: board/tenant/parents/labels/context all reach
    # the parent-owned card spec.
    backend = InMemoryKanbanBackend(auto="completed", known_profiles={"planner"})
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(
        1, profile="planner", title="plan #9", prompt="produce a plan",
        context={"issue_url": "u"}, board="b1", tenant="t1",
        parents=["card_parent"], labels=["triage"], on_block="return",
    ))
    assert ret["ok"] is True
    spec = backend.spec_for(ret["value"]["card_id"])
    assert spec is not None
    assert spec.title == "plan #9" and spec.prompt == "produce a plan"
    assert spec.board == "b1" and spec.tenant == "t1"
    assert spec.parents == ("card_parent",) and spec.labels == ("triage",)
    assert spec.context == {"issue_url": "u"}


# --------------------------------------------------------------------------- #
# End-to-end through the subprocess VM + durable store
# --------------------------------------------------------------------------- #

# Issue #6 moved schema= to validate the worker's workflow_result payload (not the
# envelope), so these idempotency-focused runs intentionally pass no schema; the
# result contract has dedicated coverage in test_kanban_result_contract.py.
_E2E_SCRIPT = META + (
    'r = await kanban_agent("planner", title="plan", prompt="go", '
    'context={"issue": args["i"]}, parents=["root_card"], on_block="return")\n'
    'return {"status": r["status"], "card_id": r["card_id"], "reattached": r["reattached"]}\n'
)


def test_e2e_creates_one_card_and_completes():
    backend = InMemoryKanbanBackend(auto="completed", known_profiles={"planner"})
    res = run_workflow_script(_E2E_SCRIPT, args={"i": "#42"}, kanban_backend=backend)
    assert res.ok, res.error
    assert res.value["status"] == CARD_COMPLETED
    assert res.value["reattached"] is False
    assert len(backend.created_cards) == 1
    # The card carries the parent link from the script call.
    spec = backend.spec_for(backend.created_cards[0])
    assert spec.parents == ("root_card",)
    assert spec.context == {"issue": "#42"}


def test_e2e_replay_reattaches_and_creates_no_duplicate_card():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = InMemoryKanbanBackend(auto="completed", known_profiles={"planner"})

        rec = run_workflow_script(
            _E2E_SCRIPT, args={"i": "#42"}, store=store, run_id="src",
            kanban_backend=backend,
        )
        assert rec.ok, rec.error
        assert len(backend.created_cards) == 1
        original_card = backend.created_cards[0]

        # A live Kanban call is never written to the #3 replay cache, so on replay
        # it re-runs live — and the idempotency key (rooted at the source run id)
        # makes it reattach the same card instead of opening a duplicate.
        rep = run_workflow_script(
            _E2E_SCRIPT, args={"i": "#42"}, store=store, run_id="replay",
            replay_from="src", kanban_backend=backend,
        )
        assert rep.ok, rep.error
        assert rep.value["card_id"] == original_card
        assert rep.value["reattached"] is True
        assert len(backend.created_cards) == 1  # no duplicate card.
        assert backend.reattachments == 1


def test_control_stop_active_replay_blocks_kanban_dispatch_before_reattach():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = InMemoryKanbanBackend(auto="completed", known_profiles={"planner"})
        rec = run_workflow_script(
            _E2E_SCRIPT,
            args={"i": "#42"},
            store=store,
            run_id="src",
            kanban_backend=backend,
        )
        assert rec.ok, rec.error
        controls = InMemoryControlStore()
        stop_run(controls, "replay", reason="active replay stop")
        rep = run_workflow_script(
            _E2E_SCRIPT,
            args={"i": "#42"},
            store=store,
            run_id="replay",
            replay_from="src",
            kanban_backend=backend,
            control_store=controls,
        )
        persisted = store.load_run("replay")

    assert rep.stopped is True
    assert rep.error["code"] == "run_stopped"
    assert persisted.status == "stopped"
    assert len(backend.created_cards) == 1
    assert backend.reattachments == 0


def test_control_task_stop_active_replay_blocks_kanban_dispatch_before_reattach():
    script = META + (
        'r = await kanban_agent("planner", title="plan", prompt="go", labels=["qa-label"], on_block="return")\n'
        'return {"status": r["status"], "card_id": r["card_id"], "reattached": r["reattached"]}\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = InMemoryKanbanBackend(auto="completed", known_profiles={"planner"})
        rec = run_workflow_script(
            script,
            store=store,
            run_id="src",
            kanban_backend=backend,
        )
        assert rec.ok, rec.error
        controls = InMemoryControlStore()
        stop_task(controls, "replay", "qa-label", reason="skip replay child")
        rep = run_workflow_script(
            script,
            store=store,
            run_id="replay",
            replay_from="src",
            kanban_backend=backend,
            control_store=controls,
        )
        persisted = store.load_run("replay")

    assert rep.ok is False
    assert rep.error["code"] == "task_stopped"
    assert persisted.status == "failed"
    assert len(backend.created_cards) == 1
    assert backend.reattachments == 0


def test_e2e_chained_replay_reattaches_the_original_card():
    # Regression (review): replaying a replay (A <- B <- C) must converge on the
    # ONE original card via the transitive idempotency root, not open a fresh card
    # at each generation.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = InMemoryKanbanBackend(auto="completed", known_profiles={"planner"})

        a = run_workflow_script(_E2E_SCRIPT, args={"i": "#42"}, store=store, run_id="A",
                                kanban_backend=backend)
        b = run_workflow_script(_E2E_SCRIPT, args={"i": "#42"}, store=store, run_id="B",
                                replay_from="A", kanban_backend=backend)
        c = run_workflow_script(_E2E_SCRIPT, args={"i": "#42"}, store=store, run_id="C",
                                replay_from="B", kanban_backend=backend)
        assert a.ok and b.ok and c.ok, (a.error, b.error, c.error)

        card = backend.created_cards[0]
        assert a.value["card_id"] == card
        assert b.value["card_id"] == card
        assert c.value["card_id"] == card
        assert len(backend.created_cards) == 1  # exactly one card across the chain.
        assert backend.reattachments == 2       # B and C both reattached A's card.
        assert b.value["reattached"] is True and c.value["reattached"] is True


def test_e2e_unknown_profile_fails_the_run_without_opening_a_card():
    backend = InMemoryKanbanBackend(auto="completed", known_profiles={"planner"})
    script = META + (
        'r = await kanban_agent("ghost", title="x", on_block="return")\n'
        'return {"status": r["status"]}\n'
    )
    res = run_workflow_script(script, kanban_backend=backend)
    assert res.ok is False
    assert res.error["code"] == "unknown_profile"
    assert backend.created_cards == []
