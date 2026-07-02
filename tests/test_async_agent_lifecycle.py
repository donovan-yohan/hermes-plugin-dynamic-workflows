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
