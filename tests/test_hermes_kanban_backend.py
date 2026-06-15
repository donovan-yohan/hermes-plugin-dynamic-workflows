"""Tests for the real Hermes Kanban backend adapter (issue #5).

The shipped #5 pieces (idempotent awaitable, durable card state/event log,
notifier + event-log backend) are all backed by an in-memory fake. This adapter
is the production-shaped backend: it opens/reattaches **real** cards through the
``hermes kanban create`` CLI seam and resolves from **real** Kanban terminal
events bridged into the durable event log.

Acceptance criteria covered:

1. one ``hermes kanban create`` with idempotency key, board, assignee, workspace,
   tenant, parents, and the schema/result-contract instruction in the body;
2. replay/reattach creates no duplicate card;
3. unknown profile is rejected before any card is created;
4. the adapter never invokes dispatch/daemon/spawn commands;
5. a completed real task event resolves the await (status completed + result);
6. blocked maps according to the existing ``on_block`` behaviour;
7. failed/timed_out/crashed/gave_up map to a structured ``failed`` resolution;
8. ``after_version`` ignores stale terminal events and waits for newer ones;
9. an invalid ``workflow_result`` still fails schema/result-contract validation.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from hermes_workflows import run_workflow_script
from hermes_workflows.hermes_kanban import (
    HERMES_TERMINAL_STATUS_MAP,
    HermesKanbanBackend,
    HermesKanbanError,
    assert_no_dispatch,
    build_card_body,
    build_create_argv,
    map_hermes_terminal_status,
    publish_hermes_kanban_event,
)
from hermes_workflows.kanban import (
    CARD_BLOCKED,
    CARD_COMPLETED,
    CARD_FAILED,
    KanbanCardSpec,
    KanbanTimeout,
    KanbanUnknownProfile,
    kanban_card_id,
    result_contract_instruction,
)
from hermes_workflows.kanban_notify import ThreadEventNotifier
from hermes_workflows.script_store import ScriptRunStore
from hermes_workflows.vm import CapabilityBroker, VMLimits

META = 'meta = {"name": "k5h", "description": "d"}\n'


class _RecordingClient:
    """A fake :class:`HermesKanbanClient` that records the argv it would run.

    Stands in for the real ``hermes kanban create`` subprocess so tests assert the
    exact invocation without spawning anything.
    """

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def create(self, card_id, idempotency_key, spec):
        argv = build_create_argv(card_id, idempotency_key, spec)
        assert_no_dispatch(argv)  # the real client guards too; mirror it here.
        self.calls.append(
            {"card_id": card_id, "idempotency_key": idempotency_key, "spec": spec, "argv": argv}
        )
        return {"card_id": card_id}


def _backend(store, *, notifier=None, client=None, known=("planner",), unknown=frozenset()):
    return HermesKanbanBackend(
        store,
        notifier or ThreadEventNotifier(),
        client=client if client is not None else _RecordingClient(),
        known_profiles=set(known) if known is not None else None,
        unknown_profiles=frozenset(unknown),
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


# --------------------------------------------------------------------------- #
# Pure builders / bridge
# --------------------------------------------------------------------------- #

def test_build_create_argv_carries_every_field_and_the_result_contract():  # criterion 1
    spec = KanbanCardSpec(
        profile="planner",
        title="plan #9",
        prompt="produce a plan",
        context={"issue": "#9"},
        board="board-1",
        tenant="acme",
        parents=("root_card", "epic_card"),
        labels=("triage", "p1"),
        workspace={"type": "dir", "path": "/repo"},
        schema={"plan": "string", "steps": "list"},
    )
    argv = build_create_argv("kbc_abc", "root:1", spec)

    assert argv[:3] == ["hermes", "kanban", "create"]

    def _val(flag):
        return argv[argv.index(flag) + 1]

    assert _val("--idempotency-key") == "root:1"
    assert _val("--card-id") == "kbc_abc"
    assert _val("--assignee") == "planner"
    assert _val("--board") == "board-1"
    assert _val("--tenant") == "acme"
    assert _val("--title") == "plan #9"
    # repeated parents / labels both present.
    assert argv.count("--parent") == 2
    assert "root_card" in argv and "epic_card" in argv
    assert argv.count("--label") == 2
    # workspace serialised as JSON.
    assert '"path"' in _val("--workspace") and '"/repo"' in _val("--workspace")
    # the body carries the worker prompt AND the issue #6 result-contract instruction.
    body = _val("--body")
    assert "produce a plan" in body
    assert result_contract_instruction(spec.schema) in body


def test_card_body_without_schema_has_no_contract_instruction():
    spec = KanbanCardSpec(profile="planner", prompt="just do it")
    body = build_card_body(spec)
    assert "just do it" in body
    assert "metadata.workflow_result" not in body  # no schema -> no contract line.


def test_terminal_status_bridge_maps_the_failure_family():  # criterion 7 (mapping)
    assert map_hermes_terminal_status("completed") == CARD_COMPLETED
    assert map_hermes_terminal_status("DONE") == CARD_COMPLETED
    assert map_hermes_terminal_status("blocked") == CARD_BLOCKED
    for raw in ("failed", "timed_out", "crashed", "gave_up", "cancelled"):
        assert map_hermes_terminal_status(raw) == CARD_FAILED, raw
    # non-terminal / unknown -> None (never resolved on).
    assert map_hermes_terminal_status("running") is None
    assert map_hermes_terminal_status("queued") is None
    assert map_hermes_terminal_status(None) is None
    # the published map is the single source of truth for the failure family.
    assert HERMES_TERMINAL_STATUS_MAP["gave_up"] == CARD_FAILED


def test_publish_hermes_event_rejects_a_non_terminal_status():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        try:
            publish_hermes_kanban_event(store, ThreadEventNotifier(), "kbc_x", status="running")
        except HermesKanbanError:
            return
        raise AssertionError("expected HermesKanbanError for a non-terminal status")


# --------------------------------------------------------------------------- #
# Criterion 4: never dispatch
# --------------------------------------------------------------------------- #

def test_assert_no_dispatch_refuses_dispatcher_subcommands():  # criterion 4
    for sub in ("dispatch", "daemon", "worker", "spawn", "serve", "run"):
        try:
            assert_no_dispatch(["hermes", "kanban", sub, "--board", "b"])
        except HermesKanbanError:
            continue
        raise AssertionError(f"expected refusal of 'hermes kanban {sub}'")
    # create / comment are allowed.
    assert_no_dispatch(["hermes", "kanban", "create", "--card-id", "x"])
    assert_no_dispatch(["hermes", "kanban", "comment", "--card-id", "x"])


def test_every_adapter_argv_is_a_create():  # criterion 4
    client = _RecordingClient()
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = _backend(store, client=client)
        backend.create_or_reattach("root:1", KanbanCardSpec(profile="planner"))
    assert len(client.calls) == 1
    for call in client.calls:
        assert call["argv"][:3] == ["hermes", "kanban", "create"]
        for forbidden in ("dispatch", "daemon", "worker", "spawn", "serve"):
            assert forbidden not in call["argv"]


# --------------------------------------------------------------------------- #
# Criterion 3: unknown profile rejected before create
# --------------------------------------------------------------------------- #

def test_unknown_profile_rejected_before_any_create():  # criterion 3
    client = _RecordingClient()
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = _backend(store, client=client, known=("planner",))
        try:
            backend.create_or_reattach("root:1", KanbanCardSpec(profile="ghost"))
        except KanbanUnknownProfile:
            assert client.calls == []  # nothing opened for an unknown assignee.
            assert store.kanban_waits() == []
            return
    raise AssertionError("expected KanbanUnknownProfile")


def test_deny_list_overrides_known_profiles():  # criterion 3
    client = _RecordingClient()
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = _backend(store, client=client, known=("planner", "ops"), unknown=frozenset({"ops"}))
        try:
            backend.create_or_reattach("root:1", KanbanCardSpec(profile="ops"))
        except KanbanUnknownProfile:
            assert client.calls == []
            return
    raise AssertionError("expected KanbanUnknownProfile for a deny-listed profile")


# --------------------------------------------------------------------------- #
# Criterion 1/2: create once, reattach, no duplicate
# --------------------------------------------------------------------------- #

def test_create_opens_one_card_and_records_a_waiting_marker():  # criterion 1
    client = _RecordingClient()
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = _backend(store, client=client)
        card = backend.create_or_reattach("root:1", KanbanCardSpec(profile="planner", board="b"))
        assert card.card_id == kanban_card_id("root:1")
        assert card.reattached is False
        assert len(client.calls) == 1
        # the card is now a durable in-flight wait.
        assert [w["card_id"] for w in store.kanban_waits()] == [card.card_id]


def test_reattach_when_durable_state_exists_creates_no_duplicate():  # criterion 2
    client = _RecordingClient()
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = _backend(store, client=client)
        first = backend.create_or_reattach("root:1", KanbanCardSpec(profile="planner"))
        # A SECOND, fresh backend instance over the same store (a restart): the
        # durable waiting marker makes it reattach without a second create.
        backend2 = _backend(store, client=client)
        second = backend2.create_or_reattach("root:1", KanbanCardSpec(profile="planner"))
        assert second.card_id == first.card_id
        assert second.reattached is True
        assert len(client.calls) == 1  # still exactly one real create.


# --------------------------------------------------------------------------- #
# Criterion 5/6/7/8: resolution from real terminal events
# --------------------------------------------------------------------------- #

def test_completed_event_resolves_the_await():  # criterion 5
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        backend = _backend(store, notifier=notifier)
        card = backend.create_or_reattach("root:1", KanbanCardSpec(profile="planner"))
        publish_hermes_kanban_event(
            store, notifier, card.card_id, status="completed",
            result={"plan": "done"}, profile="planner",
        )
        res = backend.await_resolution(card.card_id, accept_blocked=True, timeout=0.5)
        assert res.status == CARD_COMPLETED
        assert res.result == {"plan": "done"}


def test_blocked_event_honours_on_block_via_accept_blocked():  # criterion 6
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        backend = _backend(store, notifier=notifier)
        card = backend.create_or_reattach("root:1", KanbanCardSpec(profile="planner"))
        publish_hermes_kanban_event(store, notifier, card.card_id, status="blocked", reason="needs input")

        # accept_blocked=False (on_block="pause"): the blocked event is skipped.
        try:
            backend.await_resolution(card.card_id, accept_blocked=False, timeout=0.2)
        except KanbanTimeout:
            pass
        else:  # pragma: no cover
            raise AssertionError("pause await resolved on a blocked event")

        # accept_blocked=True (return/raise): it resolves as blocked.
        res = backend.await_resolution(card.card_id, accept_blocked=True, timeout=0.5)
        assert res.status == CARD_BLOCKED
        assert res.reason == "needs input"


def test_failure_family_resolves_as_structured_failed():  # criterion 7
    for raw in ("failed", "timed_out", "crashed", "gave_up"):
        with TemporaryDirectory() as tmp:
            store = ScriptRunStore(Path(tmp) / "runs")
            notifier = ThreadEventNotifier()
            backend = _backend(store, notifier=notifier)
            card = backend.create_or_reattach("root:1", KanbanCardSpec(profile="planner"))
            publish_hermes_kanban_event(store, notifier, card.card_id, status=raw, profile="planner")
            res = backend.await_resolution(card.card_id, accept_blocked=True, timeout=0.5)
            assert res.status == CARD_FAILED, raw
            # the specific failure name is preserved in the reason for the script.
            if raw != "failed":
                assert res.reason == raw, raw


def test_after_version_skips_stale_and_waits_for_a_newer_event():  # criterion 8
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        backend = _backend(store, notifier=notifier)
        card = backend.create_or_reattach("root:1", KanbanCardSpec(profile="planner"))
        publish_hermes_kanban_event(store, notifier, card.card_id, status="completed", result={"n": 1})
        # after_version=1: the line-1 event is stale and skipped; a newer one resolves.
        try:
            backend.await_resolution(card.card_id, accept_blocked=True, timeout=0.2, after_version=1)
        except KanbanTimeout:
            pass
        else:  # pragma: no cover
            raise AssertionError("resolved on a stale (already-consumed) event")
        publish_hermes_kanban_event(store, notifier, card.card_id, status="completed", result={"n": 2})
        res = backend.await_resolution(card.card_id, accept_blocked=True, timeout=0.5, after_version=1)
        assert res.result == {"n": 2} and res.version == 2


def test_await_is_woken_by_a_concurrent_producer():  # criterion 5 (event-driven)
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        backend = _backend(store, notifier=notifier)
        card = backend.create_or_reattach("root:1", KanbanCardSpec(profile="planner"))

        def _produce():
            time.sleep(0.1)
            publish_hermes_kanban_event(
                store, notifier, card.card_id, status="completed", result={"plan": "live"}
            )

        worker = threading.Thread(target=_produce)
        worker.start()
        try:
            res = backend.await_resolution(card.card_id, accept_blocked=True, timeout=3.0)
        finally:
            worker.join()
        assert res.result == {"plan": "live"}


# --------------------------------------------------------------------------- #
# Broker-level: on_block + unknown profile through the real broker path
# --------------------------------------------------------------------------- #

def test_broker_completed_returns_structured_result():  # criterion 5
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        backend = _backend(store, notifier=notifier)
        broker = _broker(backend, root="root")
        card_id = kanban_card_id("root:1")
        publish_hermes_kanban_event(
            store, notifier, card_id, status="completed", result={"ok": True}, profile="planner"
        )
        ret = broker.handle(_kanban_frame(1, on_block="return"))
        assert ret["ok"] is True, ret
        assert ret["value"]["status"] == CARD_COMPLETED
        assert ret["value"]["card_id"] == card_id
        assert ret["value"]["workflow_result"] == {"ok": True}


def test_broker_blocked_raise_denies_into_script():  # criterion 6
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        backend = _backend(store, notifier=notifier)
        broker = _broker(backend, root="root")
        publish_hermes_kanban_event(
            store, notifier, kanban_card_id("root:1"), status="blocked", reason="x", profile="planner"
        )
        ret = broker.handle(_kanban_frame(1, on_block="raise"))
        assert ret["ok"] is False
        assert ret["error"]["code"] == "kanban_blocked"


def test_broker_unknown_profile_rejected_no_card():  # criterion 3
    client = _RecordingClient()
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = _backend(store, client=client)
        broker = _broker(backend, root="root")
        ret = broker.handle(_kanban_frame(1, profile="ghost", on_block="return"))
        assert ret["ok"] is False
        assert ret["error"]["code"] == "unknown_profile"
        assert client.calls == []


# --------------------------------------------------------------------------- #
# End-to-end through the subprocess VM
# --------------------------------------------------------------------------- #

_E2E_SCRIPT = META + (
    'r = await kanban_agent("planner", title="plan", prompt="go", '
    'context={"issue": args["i"]}, board="b1", parents=["root_card"], on_block="return")\n'
    'return {"status": r["status"], "card_id": r["card_id"], "reattached": r["reattached"]}\n'
)

_SCHEMA_SCRIPT = META + (
    'r = await kanban_agent("planner", prompt="plan", on_block="return", schema={"plan": "string"})\n'
    'return {"status": r["status"], "result": r.get("workflow_result"), '
    '"diagnostics": r.get("diagnostics")}\n'
)


def test_e2e_creates_one_real_card_and_completes():  # criterion 1/5
    client = _RecordingClient()
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        card_id = kanban_card_id("A:1")

        def _produce():
            time.sleep(0.15)
            publish_hermes_kanban_event(
                store, notifier, card_id, status="completed", result={"plan": "x"}, profile="planner"
            )

        worker = threading.Thread(target=_produce)
        worker.start()
        try:
            res = run_workflow_script(
                _E2E_SCRIPT, args={"i": "#42"}, store=store, run_id="A",
                kanban_backend=_backend(store, notifier=notifier, client=client),
            )
        finally:
            worker.join()
        assert res.ok, res.error
        assert res.value["status"] == CARD_COMPLETED
        assert res.value["card_id"] == card_id
        assert res.value["reattached"] is False
        assert len(client.calls) == 1  # exactly one hermes kanban create.
        # board + parent reached the real create argv.
        argv = client.calls[0]["argv"]
        assert "b1" in argv and "root_card" in argv


def test_e2e_replay_reattaches_without_a_duplicate_create():  # criterion 2
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        card_id = kanban_card_id("A:1")
        client_a = _RecordingClient()
        client_b = _RecordingClient()

        # Produce the terminal event concurrently DURING A's await, so A genuinely
        # creates the card first (history is empty at create time) and then resolves.
        def _produce():
            time.sleep(0.15)
            publish_hermes_kanban_event(
                store, notifier, card_id, status="completed", result={"plan": "x"}, profile="planner"
            )

        worker = threading.Thread(target=_produce)
        worker.start()
        try:
            a = run_workflow_script(
                _E2E_SCRIPT, args={"i": "#42"}, store=store, run_id="A",
                kanban_backend=_backend(store, notifier=notifier, client=client_a),
            )
        finally:
            worker.join()
        assert a.ok, a.error
        assert a.value["reattached"] is False
        assert len(client_a.calls) == 1

        # Replay: a fresh backend + client. The durable record reattaches the same
        # card; the live Kanban call is excluded from the #3 replay cache so it
        # re-runs, but reattach means NO second create.
        b = run_workflow_script(
            _E2E_SCRIPT, args={"i": "#42"}, store=store, run_id="B", replay_from="A",
            kanban_backend=_backend(store, notifier=notifier, client=client_b),
        )
        assert b.ok, b.error
        assert b.value["card_id"] == card_id
        assert b.value["reattached"] is True
        assert client_b.calls == []  # no duplicate create on replay.


def test_e2e_failed_event_resolves_failed_through_the_vm():  # criterion 7
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        card_id = kanban_card_id("A:1")

        def _produce():
            time.sleep(0.1)
            publish_hermes_kanban_event(
                store, notifier, card_id, status="timed_out", profile="planner"
            )

        worker = threading.Thread(target=_produce)
        worker.start()
        try:
            res = run_workflow_script(
                _E2E_SCRIPT, args={"i": "#1"}, store=store, run_id="A",
                kanban_backend=_backend(store, notifier=notifier),
            )
        finally:
            worker.join()
        assert res.ok, res.error
        assert res.value["status"] == CARD_FAILED


def test_e2e_invalid_workflow_result_fails_the_contract():  # criterion 9
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        notifier = ThreadEventNotifier()
        card_id = kanban_card_id("A:1")

        def _produce():
            time.sleep(0.1)
            # Completed but with a workflow_result that violates schema={"plan":"string"}:
            # 'plan' is missing entirely.
            publish_hermes_kanban_event(
                store, notifier, card_id, status="completed", result={"wrong": 1}, profile="planner"
            )

        worker = threading.Thread(target=_produce)
        worker.start()
        try:
            res = run_workflow_script(
                _SCHEMA_SCRIPT, args={"i": 1}, store=store, run_id="A",
                kanban_backend=_backend(store, notifier=notifier),
            )
        finally:
            worker.join()
        assert res.ok, res.error
        # The contract violation is turned into a deterministic block (on_block="return"),
        # never a success, with field-level diagnostics.
        assert res.value["status"] == CARD_BLOCKED
        assert any("plan" in d for d in res.value["diagnostics"])
