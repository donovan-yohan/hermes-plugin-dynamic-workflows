"""Tests for the async child-agent lifecycle globals (issue #112).

``agent_start`` / ``agent_check`` / ``agent_cancel`` / ``agent_list`` generalize
``kanban_agent``'s durable-await machinery into a *non-blocking* lifecycle: start
a background child-agent run once, keep orchestrating other work, and poll it
later instead of holding an await open. Covered here:

* the end-to-end round trip through a real subprocess, deterministic across runs;
* deterministic handle derivation and deterministic ``agent_list`` ordering under
  concurrent ``parallel()`` starts;
* the parent-owned broker enforcement layer (forged frames), matching the style
  of ``test_vm_subprocess.py``'s broker unit tests;
* unconditional replay-from-cache for a completed handle, even against a runner
  that would raise if actually dispatched -- the documented cut leaves durable
  suspend of an *unresolved* handle at script end to a follow-up (DESIGN.md §5.14).

Stdlib only; all effects route through the deterministic ``StubAsyncAgentRunner``
except where a fake runner is needed to force a specific broker path.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from hermes_workflows import (
    AsyncChildAgentRequest,
    ScriptRunStore,
    StubAsyncAgentRunner,
    VMLimits,
    run_workflow_script,
)
from hermes_workflows.script_validator import CAPABILITY_GLOBALS
from hermes_workflows.script_store import is_replayable
from hermes_workflows.vm import CapabilityBroker
from hermes_workflows import rpc

META = 'meta = {"name": "async-agents", "description": "d"}\n'

_CHECK_LOOP_SCRIPT = META + (
    'h = await agent_start("hermes.echo", {"x": args["x"]})\n'
    'handle = h["handle"]\n'
    'state = await agent_check(handle)\n'
    'n = 0\n'
    'while state["state"] == "pending" and n < 20:\n'
    '    state = await agent_check(handle)\n'
    '    n += 1\n'
    'return {"handle": handle, "state": state, "polls": n}\n'
)


# --------------------------------------------------------------------------- #
# End-to-end through a real subprocess
# --------------------------------------------------------------------------- #

def test_agent_start_returns_a_handle_immediately_without_waiting_for_completion():
    res = run_workflow_script(
        META + 'h = await agent_start("hermes.echo", {"x": 1})\nreturn h\n',
        async_child_runner=StubAsyncAgentRunner(),
    )
    assert res.ok, res.error
    assert res.value["state"] == "pending"
    assert isinstance(res.value["handle"], str) and res.value["handle"].startswith("ah_")


def test_agent_check_loop_reaches_done_with_a_dict_result():
    res = run_workflow_script(_CHECK_LOOP_SCRIPT, args={"x": 1}, async_child_runner=StubAsyncAgentRunner())
    assert res.ok, res.error
    assert res.value["state"]["state"] == "done"
    result = res.value["state"]["result"]
    assert result["echo"] == {"x": 1}
    assert result["target"] == "hermes.echo"
    assert isinstance(result["digest"], str) and result["digest"]


def test_run_and_handle_are_deterministic_across_two_runs():
    a = run_workflow_script(_CHECK_LOOP_SCRIPT, args={"x": 7}, async_child_runner=StubAsyncAgentRunner())
    b = run_workflow_script(_CHECK_LOOP_SCRIPT, args={"x": 7}, async_child_runner=StubAsyncAgentRunner())
    assert a.ok and b.ok
    assert a.value == b.value


def test_different_args_mint_different_handles():
    a = run_workflow_script(_CHECK_LOOP_SCRIPT, args={"x": 1}, async_child_runner=StubAsyncAgentRunner())
    b = run_workflow_script(_CHECK_LOOP_SCRIPT, args={"x": 2}, async_child_runner=StubAsyncAgentRunner())
    assert a.ok and b.ok
    assert a.value["handle"] != b.value["handle"]


def test_agent_cancel_is_idempotent_and_journaled_as_acknowledged_state():
    script = META + (
        'h = await agent_start("hermes.echo", {})\n'
        'first = await agent_cancel(h["handle"])\n'
        'second = await agent_cancel(h["handle"])\n'
        'checked = await agent_check(h["handle"])\n'
        'return {"first": first, "second": second, "checked": checked}\n'
    )
    res = run_workflow_script(script, async_child_runner=StubAsyncAgentRunner())
    assert res.ok, res.error
    assert res.value["first"] == {"state": "cancelled"}
    assert res.value["second"] == {"state": "cancelled"}
    assert res.value["checked"] == {"state": "cancelled"}


def test_agent_check_unknown_handle_is_a_structured_bad_request_denial():
    script = META + (
        'try:\n'
        '    await agent_check("ah_0000000000000000")\n'
        '    return {"denied": False}\n'
        'except CapabilityError as e:\n'
        '    return {"denied": True, "code": e.code}\n'
    )
    res = run_workflow_script(script, async_child_runner=StubAsyncAgentRunner())
    assert res.ok, res.error
    assert res.value == {"denied": True, "code": "unknown_handle"}


def test_agent_start_denied_without_a_configured_async_runner():
    res = run_workflow_script(META + 'return await agent_start("hermes.echo", {})\n')
    assert res.ok is False
    assert res.error["code"] == "async_child_agent_unavailable"


def test_agent_list_orders_by_start_call_id_under_concurrent_parallel_starts():
    script = META + (
        'async def one(i):\n'
        '    h = await agent_start("hermes.echo", {"i": i})\n'
        '    return h["handle"]\n'
        'handles = await parallel([lambda i=i: one(i) for i in range(8)])\n'
        'listing = await agent_list()\n'
        'order = [item["handle"] for item in listing["handles"]]\n'
        'targets = [item["target"] for item in listing["handles"]]\n'
        'return {"handles": handles, "order": order, "targets": targets}\n'
    )
    for _ in range(5):  # a handful of runs to shake out thread-scheduling flakiness.
        res = run_workflow_script(
            script, async_child_runner=StubAsyncAgentRunner(), limits=VMLimits(max_parallel=8)
        )
        assert res.ok, res.error
        assert res.value["order"] == res.value["handles"]
        assert res.value["targets"] == ["hermes.echo"] * 8


def test_agent_list_reports_current_state_per_handle():
    script = META + (
        'a = await agent_start("hermes.echo", {})\n'
        'b = await agent_start("hermes.uppercaser", {})\n'
        'await agent_cancel(a["handle"])\n'
        'listing = await agent_list()\n'
        'return listing\n'
    )
    res = run_workflow_script(script, async_child_runner=StubAsyncAgentRunner())
    assert res.ok, res.error
    states = {item["handle"]: item["state"] for item in res.value["handles"]}
    assert len(states) == 2
    values = list(states.values())
    assert "cancelled" in values


# --------------------------------------------------------------------------- #
# Parent-owned broker enforcement (forged frames)
# --------------------------------------------------------------------------- #

def _broker(async_child_runner=None, **limit_kwargs) -> CapabilityBroker:
    from hermes_workflows.agents import StubAgentRunner

    return CapabilityBroker(StubAgentRunner(), VMLimits(**limit_kwargs), async_child_runner=async_child_runner)


def _call(method, params, call_id=1):
    return {"t": rpc.T_CALL, "id": call_id, "method": method, "params": params}


def test_broker_agent_start_requires_non_empty_target():
    ret = _broker(StubAsyncAgentRunner()).handle(_call("agent_start", {"target": "", "input": {}}))
    assert ret["ok"] is False and ret["error"]["code"] == "bad_request"


def test_broker_agent_start_requires_object_shaped_input_and_opts():
    broker = _broker(StubAsyncAgentRunner())
    bad_input = broker.handle(_call("agent_start", {"target": "x", "input": "nope"}, call_id=1))
    assert bad_input["ok"] is False and bad_input["error"]["code"] == "bad_request"
    bad_opts = broker.handle(_call("agent_start", {"target": "x", "opts": "nope"}, call_id=2))
    assert bad_opts["ok"] is False and bad_opts["error"]["code"] == "bad_request"


def test_broker_agent_start_handle_is_deterministic_given_same_idempotency_root_and_call_id():
    from hermes_workflows.agents import StubAgentRunner

    broker1 = CapabilityBroker(
        StubAgentRunner(), VMLimits(), async_child_runner=StubAsyncAgentRunner(), idempotency_root="root-a"
    )
    broker2 = CapabilityBroker(
        StubAgentRunner(), VMLimits(), async_child_runner=StubAsyncAgentRunner(), idempotency_root="root-a"
    )
    assert broker1._async_agent_handle(5) == broker2._async_agent_handle(5)
    broker3 = CapabilityBroker(
        StubAgentRunner(), VMLimits(), async_child_runner=StubAsyncAgentRunner(), idempotency_root="root-b"
    )
    assert broker1._async_agent_handle(5) != broker3._async_agent_handle(5)
    assert broker1._async_agent_handle(5) != broker1._async_agent_handle(6)


def test_broker_agent_check_and_cancel_deny_a_pending_handle_if_the_runner_disappears():
    """Defensive branch: a broker whose runner is absent but whose in-memory
    ``_async_agents`` already holds a pending record (only reachable by forging
    that state directly, since a live agent_start would already have denied
    without a runner) still fails closed rather than crashing.
    """
    broker = _broker(None)
    broker._async_agents["ah_forged"] = {
        "token": "tok", "target": "x", "state": "pending", "result": None, "error": None, "call_id": 1,
    }
    checked = broker.handle(_call("agent_check", {"handle": "ah_forged"}, call_id=2))
    assert checked["ok"] is False and checked["error"]["code"] == "async_child_agent_unavailable"
    cancelled = broker.handle(_call("agent_cancel", {"handle": "ah_forged"}, call_id=3))
    assert cancelled["ok"] is False and cancelled["error"]["code"] == "async_child_agent_unavailable"


def test_terminal_cancel_state_is_sticky_against_a_racing_in_flight_check():
    """Broker-level probe: a poll() left in flight when cancel() commits must
    not resurrect the handle once it finally returns (issue #112 review: the
    cancel-racing-completion hazard -- ``_apply_async_status`` must not
    overwrite an already-terminal record)."""
    poll_may_return = threading.Event()
    poll_called = threading.Event()

    class _BlockingPollRunner:
        def start(self, request: AsyncChildAgentRequest) -> Any:
            return "tok"

        def poll(self, token: Any) -> dict[str, Any]:
            poll_called.set()
            poll_may_return.wait(timeout=5)
            return {"state": "done", "result": {"late": True}}

        def cancel(self, token: Any) -> dict[str, Any]:
            return {"state": "cancelled"}

    broker = _broker(_BlockingPollRunner())
    started = broker.handle(_call("agent_start", {"target": "hermes.echo", "input": {}}, call_id=1))
    handle = started["value"]["handle"]

    check_result: dict[str, Any] = {}

    def _run_check() -> None:
        check_result["ret"] = broker.handle(_call("agent_check", {"handle": handle}, call_id=2))

    checker = threading.Thread(target=_run_check)
    checker.start()
    assert poll_called.wait(timeout=5), "agent_check never reached the blocking poll()"

    cancel_ret = broker.handle(_call("agent_cancel", {"handle": handle}, call_id=3))
    assert cancel_ret["ok"] is True and cancel_ret["value"] == {"state": "cancelled"}

    # Only now let the racing poll() return "done" -- cancel has already committed.
    poll_may_return.set()
    checker.join(timeout=5)
    assert check_result["ret"]["ok"] is True

    final = broker.handle(_call("agent_check", {"handle": handle}, call_id=4))
    assert final["ok"] is True and final["value"] == {"state": "cancelled"}


def test_terminal_done_state_is_sticky_against_a_racing_cancel():
    """Mirror of the above with the interleaving reversed: a cancel() left in
    flight when a poll() commits "done" must not wipe the completed result."""
    cancel_may_return = threading.Event()
    cancel_called = threading.Event()

    class _BlockingCancelRunner:
        def start(self, request: AsyncChildAgentRequest) -> Any:
            return "tok"

        def poll(self, token: Any) -> dict[str, Any]:
            return {"state": "done", "result": {"ok": True}}

        def cancel(self, token: Any) -> dict[str, Any]:
            cancel_called.set()
            cancel_may_return.wait(timeout=5)
            return {"state": "cancelled"}

    broker = _broker(_BlockingCancelRunner())
    started = broker.handle(_call("agent_start", {"target": "hermes.echo", "input": {}}, call_id=1))
    handle = started["value"]["handle"]

    cancel_result: dict[str, Any] = {}

    def _run_cancel() -> None:
        cancel_result["ret"] = broker.handle(_call("agent_cancel", {"handle": handle}, call_id=2))

    canceller = threading.Thread(target=_run_cancel)
    canceller.start()
    assert cancel_called.wait(timeout=5), "agent_cancel never reached the blocking cancel()"

    check_ret = broker.handle(_call("agent_check", {"handle": handle}, call_id=3))
    assert check_ret["ok"] is True and check_ret["value"] == {"state": "done", "result": {"ok": True}}

    # Only now let the racing cancel() return -- the "done" result has already committed.
    cancel_may_return.set()
    canceller.join(timeout=5)
    assert cancel_result["ret"]["ok"] is True

    final = broker.handle(_call("agent_check", {"handle": handle}, call_id=4))
    assert final["ok"] is True and final["value"] == {"state": "done", "result": {"ok": True}}


def test_broker_agent_check_result_over_limit_fails_closed_with_structured_error():
    class HugeResultRunner:
        def start(self, request: AsyncChildAgentRequest) -> Any:
            return "tok"

        def poll(self, token: Any) -> dict[str, Any]:
            return {"state": "done", "result": {"blob": "z" * 2000}}

        def cancel(self, token: Any) -> dict[str, Any]:
            return {"state": "cancelled"}

    res = run_workflow_script(
        META + (
            'h = await agent_start("hermes.echo", {})\n'
            'try:\n'
            '    await agent_check(h["handle"])\n'
            '    return {"code": None}\n'
            'except CapabilityError as e:\n'
            '    return {"code": e.code}\n'
        ),
        async_child_runner=HugeResultRunner(),
        limits=VMLimits(max_result_bytes=256),
    )
    assert res.ok, res.error
    assert res.value == {"code": "result_too_large"}


def test_broker_rejects_a_malformed_poll_status_from_the_runner():
    class MisbehavingRunner:
        def start(self, request: AsyncChildAgentRequest) -> Any:
            return "tok"

        def poll(self, token: Any) -> dict[str, Any]:
            return {"state": "not_a_real_state"}

        def cancel(self, token: Any) -> dict[str, Any]:
            return {"state": "cancelled"}

    res = run_workflow_script(
        META + (
            'h = await agent_start("hermes.echo", {})\n'
            'try:\n'
            '    await agent_check(h["handle"])\n'
            '    return {"code": None}\n'
            'except CapabilityError as e:\n'
            '    return {"code": e.code}\n'
        ),
        async_child_runner=MisbehavingRunner(),
    )
    assert res.ok, res.error
    assert res.value == {"code": "async_runner_invalid"}


def test_broker_rejects_a_done_status_missing_its_result():
    class NoResultRunner:
        def start(self, request: AsyncChildAgentRequest) -> Any:
            return "tok"

        def poll(self, token: Any) -> dict[str, Any]:
            return {"state": "done"}

        def cancel(self, token: Any) -> dict[str, Any]:
            return {"state": "cancelled"}

    res = run_workflow_script(
        META + (
            'h = await agent_start("hermes.echo", {})\n'
            'try:\n'
            '    await agent_check(h["handle"])\n'
            '    return {"code": None}\n'
            'except CapabilityError as e:\n'
            '    return {"code": e.code}\n'
        ),
        async_child_runner=NoResultRunner(),
    )
    assert res.ok, res.error
    assert res.value == {"code": "async_runner_invalid"}


def test_broker_surfaces_a_failed_status_with_metadata_only_error():
    class FailingRunner:
        def start(self, request: AsyncChildAgentRequest) -> Any:
            return "tok"

        def poll(self, token: Any) -> dict[str, Any]:
            return {"state": "failed", "error": {"type": "Boom", "message": "kaboom", "code": "boom_code"}}

        def cancel(self, token: Any) -> dict[str, Any]:
            return {"state": "cancelled"}

    res = run_workflow_script(
        META + 'h = await agent_start("hermes.echo", {})\nreturn await agent_check(h["handle"])\n',
        async_child_runner=FailingRunner(),
    )
    assert res.ok, res.error
    assert res.value == {"state": "failed", "error": {"type": "Boom", "message": "kaboom", "code": "boom_code"}}


def test_agent_list_orders_by_call_id_deterministically_even_out_of_insertion_order():
    """Unlike the concurrent-``parallel()`` end-to-end test above (which relies
    on thread-scheduling luck to ever actually put entries out of order), this
    forges ``_async_agents`` directly with insertion order reversed relative to
    call id, so the ``items.sort(...)`` line in ``_handle_agent_list`` is
    load-bearing for *this* assertion no matter how the threads happen to
    interleave."""
    broker = _broker(StubAsyncAgentRunner())
    broker._async_agents["ah_c"] = {
        "token": "t3", "target": "third", "state": "pending", "result": None, "error": None, "call_id": 3,
    }
    broker._async_agents["ah_a"] = {
        "token": "t1", "target": "first", "state": "pending", "result": None, "error": None, "call_id": 1,
    }
    broker._async_agents["ah_b"] = {
        "token": "t2", "target": "second", "state": "pending", "result": None, "error": None, "call_id": 2,
    }
    listed = broker.handle(_call("agent_list", {}, call_id=4))
    assert listed["ok"] is True
    assert [item["handle"] for item in listed["value"]["handles"]] == ["ah_a", "ah_b", "ah_c"]
    assert [item["target"] for item in listed["value"]["handles"]] == ["first", "second", "third"]


# --------------------------------------------------------------------------- #
# Governance: agent_start counts against max_agent_calls, and a "done" async
# result feeds the token budget like any other brokered result
# --------------------------------------------------------------------------- #

def test_agent_start_counts_against_max_agent_calls():
    script = META + (
        'started = []\n'
        'codes = []\n'
        'for i in range(3):\n'
        '    try:\n'
        '        h = await agent_start("hermes.echo", {"i": i})\n'
        '        started.append(h["handle"])\n'
        '        codes.append(None)\n'
        '    except CapabilityError as e:\n'
        '        codes.append(e.code)\n'
        'return {"started": started, "codes": codes}\n'
    )
    res = run_workflow_script(script, async_child_runner=StubAsyncAgentRunner(), limits=VMLimits(max_agent_calls=1))
    assert res.ok, res.error
    assert len(res.value["started"]) == 1
    assert res.value["codes"] == [None, "limit_agent", "limit_agent"]


def test_agent_start_denial_does_not_start_a_background_run():
    class _CountingRunner:
        def __init__(self) -> None:
            self.starts = 0

        def start(self, request: AsyncChildAgentRequest) -> Any:
            self.starts += 1
            return f"tok-{self.starts}"

        def poll(self, token: Any) -> dict[str, Any]:
            return {"state": "pending"}

        def cancel(self, token: Any) -> dict[str, Any]:
            return {"state": "cancelled"}

    runner = _CountingRunner()
    broker = _broker(runner, max_agent_calls=1)
    first = broker.handle(_call("agent_start", {"target": "hermes.echo", "input": {}}, call_id=1))
    assert first["ok"] is True
    second = broker.handle(_call("agent_start", {"target": "hermes.echo", "input": {}}, call_id=2))
    assert second["ok"] is False and second["error"]["code"] == "limit_agent"
    assert runner.starts == 1  # the denied call never reached the runner.


def test_cache_hit_replay_of_agent_start_trips_max_agent_calls_at_the_same_point():
    """A partially-replayed run's cached ``agent_start`` hits must advance the
    same counter a live dispatch would, so a call live-dispatched past the
    cached prefix trips ``max_agent_calls`` at the identical point the source
    run would have (issue #112 review)."""
    script = META + (
        'a = await agent_start("hermes.echo", {"i": 0})\n'
        'b = await agent_start("hermes.echo", {"i": 1})\n'
        'try:\n'
        '    c = await agent_start("hermes.echo", {"i": 2})\n'
        '    return {"code": None}\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code}\n'
    )
    limits = VMLimits(max_agent_calls=2)
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        source = run_workflow_script(
            script, store=store, run_id="src", async_child_runner=StubAsyncAgentRunner(), limits=limits
        )
        assert source.ok, source.error
        assert source.value == {"code": "limit_agent"}

        replayed = run_workflow_script(
            script, store=store, replay_from="src", async_child_runner=_ExplodingAsyncRunner(), limits=limits
        )
        assert replayed.ok, replayed.error
        assert replayed.value == {"code": "limit_agent"}


def test_a_done_async_result_feeds_the_token_budget():
    """Broker-level probe: a "done" ``agent_check`` result's ``_tokens`` must
    land in ``self._tokens`` like any other brokered result, so the async path
    cannot be used to bypass ``token_budget`` (issue #112 review)."""

    class _TokenRunner:
        def start(self, request: AsyncChildAgentRequest) -> Any:
            return "tok"

        def poll(self, token: Any) -> dict[str, Any]:
            return {"state": "done", "result": {"value": 1, "_tokens": 1000}}

        def cancel(self, token: Any) -> dict[str, Any]:
            return {"state": "cancelled"}

    broker = _broker(_TokenRunner())
    started = broker.handle(_call("agent_start", {"target": "hermes.echo", "input": {}}, call_id=1))
    handle = started["value"]["handle"]
    assert broker._tokens == 0
    checked = broker.handle(_call("agent_check", {"handle": handle}, call_id=2))
    assert checked["ok"] is True and checked["value"]["state"] == "done"
    assert broker._tokens == 1000


# --------------------------------------------------------------------------- #
# Replay: a completed handle replays from cache without touching the runner
# --------------------------------------------------------------------------- #

class _ExplodingAsyncRunner:
    """An async runner that must never be called -- proves a full replay never
    re-dispatches, mirroring how the ordinary agent/kanban_agent replay tests
    assert a fresh call never crosses the runner boundary again."""

    def start(self, request: AsyncChildAgentRequest) -> Any:
        raise AssertionError("agent_start must not dispatch on a fully-cached replay")

    def poll(self, token: Any) -> dict[str, Any]:
        raise AssertionError("agent_check must not poll on a fully-cached replay")

    def cancel(self, token: Any) -> dict[str, Any]:
        raise AssertionError("agent_cancel must not cancel on a fully-cached replay")


def test_replay_of_a_completed_handle_serves_every_call_from_cache():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        source = run_workflow_script(
            _CHECK_LOOP_SCRIPT, args={"x": 9}, store=store, run_id="src",
            async_child_runner=StubAsyncAgentRunner(),
        )
        assert source.ok, source.error

        replayed = run_workflow_script(
            _CHECK_LOOP_SCRIPT, args={"x": 9}, store=store, replay_from="src",
            async_child_runner=_ExplodingAsyncRunner(),
        )
        assert replayed.ok, replayed.error
        assert replayed.value == source.value
        assert replayed.replayed_calls == source.value["polls"] + 2  # agent_start + each agent_check.


def test_replay_of_a_cancelled_handle_serves_the_acknowledged_state_from_cache():
    script = META + 'h = await agent_start("hermes.echo", {})\nreturn await agent_cancel(h["handle"])\n'
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        source = run_workflow_script(script, store=store, run_id="src", async_child_runner=StubAsyncAgentRunner())
        assert source.ok and source.value == {"state": "cancelled"}

        replayed = run_workflow_script(
            script, store=store, replay_from="src", async_child_runner=_ExplodingAsyncRunner()
        )
        assert replayed.ok, replayed.error
        assert replayed.value == {"state": "cancelled"}


def test_replayed_agent_list_matches_the_source_run_without_touching_the_runner():
    script = META + (
        'a = await agent_start("hermes.echo", {})\n'
        'b = await agent_start("hermes.uppercaser", {})\n'
        'return await agent_list()\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        source = run_workflow_script(script, store=store, run_id="src", async_child_runner=StubAsyncAgentRunner())
        assert source.ok, source.error

        replayed = run_workflow_script(
            script, store=store, replay_from="src", async_child_runner=_ExplodingAsyncRunner()
        )
        assert replayed.ok, replayed.error
        assert replayed.value == source.value


# --------------------------------------------------------------------------- #
# Validator / replay-cache wiring
# --------------------------------------------------------------------------- #

def test_capability_globals_include_the_async_lifecycle_names():
    assert {"agent_start", "agent_check", "agent_cancel", "agent_list"} <= CAPABILITY_GLOBALS


def test_async_lifecycle_methods_are_replayable_regardless_of_deterministic_runner():
    for method in ("agent_start", "agent_check", "agent_cancel", "agent_list"):
        assert is_replayable(method, deterministic_runner=False) is True
        assert is_replayable(method, deterministic_runner=True) is True


# --------------------------------------------------------------------------- #
# Runner-seam idempotency identity
# --------------------------------------------------------------------------- #

def test_agent_start_passes_the_deterministic_handle_as_the_request_idempotency_key():
    """The broker-derived handle must cross the runner seam so a host can
    dedupe/reattach a duplicate dispatch (issue #112 review) -- e.g. a
    re-dispatch on resume of an agent_start recorded inside a crashed
    parallel() branch under the #109 pending-writes contract."""

    class _RecordingRunner:
        def __init__(self) -> None:
            self.requests: list[AsyncChildAgentRequest] = []

        def start(self, request: AsyncChildAgentRequest) -> Any:
            self.requests.append(request)
            return "tok"

        def poll(self, token: Any) -> dict[str, Any]:
            return {"state": "pending"}

        def cancel(self, token: Any) -> dict[str, Any]:
            return {"state": "cancelled"}

    runner = _RecordingRunner()
    res = run_workflow_script(META + 'return await agent_start("hermes.echo", {})\n', async_child_runner=runner)
    assert res.ok, res.error
    assert len(runner.requests) == 1
    assert runner.requests[0].idempotency_key == res.value["handle"]
    assert runner.requests[0].as_dict()["idempotency_key"] == res.value["handle"]


def test_stub_async_agent_runner_gives_independent_lifecycles_to_identical_requests():
    """Two independent ``start()`` calls with byte-identical requests must not
    collide on one token (issue #112 review) -- cancelling one must not affect
    the other, and each keeps its own poll count."""
    runner = StubAsyncAgentRunner()
    request = AsyncChildAgentRequest(target="hermes.echo", input={"x": 1}, opts={})
    token_a = runner.start(request)
    token_b = runner.start(request)
    assert token_a != token_b

    runner.cancel(token_a)
    assert runner.poll(token_a) == {"state": "cancelled"}
    # b is untouched by a's cancellation and keeps polling toward its own
    # (identically-derived, since the request is identical) required count.
    status_b = runner.poll(token_b)
    assert status_b["state"] in ("pending", "done")
    assert status_b != {"state": "cancelled"}


# --------------------------------------------------------------------------- #
# Documented durable-suspend boundary (DESIGN.md; follow-up tracked separately)
# --------------------------------------------------------------------------- #

def test_resume_with_a_live_agent_check_past_a_cached_agent_start_denies_unknown_handle():
    """Pins the exact boundary DESIGN.md documents: a source run's agent_start
    completes and is durably cached, but its agent_check never completes (the
    async runner raises mid-poll and the script does not catch it, so the run
    dies with the call unrecorded -- mirroring how #109's pending-writes
    contract already proves an uncaught runner failure is never flushed to the
    cache, see test_pending_writes_resume_contract.py). Resuming with
    replay_from replays the cached agent_start without touching
    self._async_agents, then live-dispatches the never-recorded agent_check --
    which fails closed with the typed unknown_handle denial rather than
    crashing or guessing."""
    script = META + (
        'h = await agent_start("hermes.echo", {"x": 1})\n'
        'checked = await agent_check(h["handle"])\n'
        'return {"checked": checked}\n'
    )

    class _CrashesOnPollRunner:
        def start(self, request: AsyncChildAgentRequest) -> Any:
            return "tok"

        def poll(self, token: Any) -> dict[str, Any]:
            raise RuntimeError("simulated process death mid agent_check")

        def cancel(self, token: Any) -> dict[str, Any]:
            return {"state": "cancelled"}

    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        source = run_workflow_script(script, store=store, run_id="src", async_child_runner=_CrashesOnPollRunner())
        # The run dies uncaught: agent_start succeeded (and is durably cached
        # the instant it did), but agent_check never completed and is not
        # flushed to the cache (a failed call that crashes the run is never
        # recorded -- see _persist_failure/flush_pending_failures).
        assert source.ok is False
        assert source.error["code"] == "runner_error"

        resumed = run_workflow_script(
            script, store=store, replay_from="src", async_child_runner=StubAsyncAgentRunner()
        )
        assert resumed.ok is False
        assert resumed.error["code"] == "unknown_handle"
