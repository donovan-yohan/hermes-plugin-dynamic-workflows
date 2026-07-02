"""Retryable classification for recoverable dispatch failures (issue #103).

Every dispatch failure the broker surfaces to a script is a catchable
``CapabilityError``, but not every failure means the same thing to a retry
policy: an unknown agent id or an exhausted budget/limit is a contract
violation — retrying the identical call fails identically — while a runner
raising mid-dispatch is a property of one attempt and may succeed on a fresh
try. ``CapabilityError.retryable`` (mirrored from the parent's
``CapabilityDenied.retryable``) lets a script branch on that distinction:

* ``unknown_agent`` -> ``retryable=False`` (still catchable, as today).
* ``runner_error`` (an injected/live ``AgentRunner`` raising) -> ``retryable=True``.
* Contract violations (schema exhaustion, budget/limit caps, ``result_too_large``,
  ``result_invalid``) stay ``retryable=False`` and run-terminating exactly as
  before this issue — this module only *adds* a field, it never loosens an
  existing denial into something a script could paper over by retrying.

Error frames stay metadata-only: ``retryable`` is a boolean, never a payload
echo, and is journaled next to the existing ``error`` code so replay/audit
consumers see the same classification a script observed. A caught
``runner_error`` (the only ``retryable=True`` code) is *also* recorded in the
deterministic replay cache (call id, method, args hash, code, retryable — no
payload), so replaying a run that caught and handled a transient runner
failure reproduces the identical denial instead of silently re-dispatching the
call live against a runner that may now behave differently.
"""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pytest

from hermes_workflows import run_workflow_script, vm_guest
from hermes_workflows import rpc
from hermes_workflows.capabilities import CapabilityPolicy, CapabilityRegistry
from hermes_workflows.errors import CapabilityDenied
from hermes_workflows.script_store import CallRecorder, ReplayCache, ReplayEntry, ScriptRunStore
from hermes_workflows.vm import CapabilityBroker, VMLimits

META = 'meta = {"name": "retryable-dispatch", "description": "d"}\n'

_CATCH_SCRIPT = META + (
    'try:\n'
    '    return await agent("hermes.agent-does-not-exist", {})\n'
    'except CapabilityError as e:\n'
    '    return {"code": e.code, "retryable": e.retryable}\n'
)


def _call(method: str, params: dict[str, Any], call_id: int = 1) -> dict[str, Any]:
    return {"t": "call", "id": call_id, "method": method, "params": params}


class _RaisingRunner:
    """Deterministic pure-function runner: raises for a marked call, else echoes."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(self, agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append({"agent_id": agent_id, "payload": dict(payload)})
        if payload.get("flaky"):
            raise RuntimeError("injected transient runner failure")
        return {"echo": dict(payload)}


class _AlwaysSucceedsRunner:
    """Deterministic pure-function runner that never raises.

    Used to prove that a *replayed* ``runner_error`` failure is served from the
    cache without ever touching the runner — even a runner that would happily
    succeed for the identical call must not change the replayed outcome.
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(self, agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append({"agent_id": agent_id, "payload": dict(payload)})
        return {"echo": dict(payload)}


# --------------------------------------------------------------------------- #
# CapabilityDenied / CapabilityError default classification
# --------------------------------------------------------------------------- #

def test_capability_denied_defaults_to_not_retryable():
    denied = CapabilityDenied("boom", code="unknown_agent")
    assert denied.retryable is False


def test_capability_denied_accepts_explicit_retryable():
    denied = CapabilityDenied("boom", code="runner_error", retryable=True)
    assert denied.retryable is True


# --------------------------------------------------------------------------- #
# Broker-level classification
# --------------------------------------------------------------------------- #

def test_unknown_agent_id_is_catchable_and_not_retryable():
    broker = CapabilityBroker(_RaisingRunner(), VMLimits())
    ret = broker.handle(_call("agent", {"agent_id": "hermes.does-not-exist", "input": {}}))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "unknown_agent"
    assert ret["error"]["retryable"] is False


def test_runner_exception_is_catchable_and_retryable():
    broker = CapabilityBroker(_RaisingRunner(), VMLimits())
    ret = broker.handle(_call("agent", {"agent_id": "hermes.echo", "input": {"flaky": True}}))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "runner_error"
    assert ret["error"]["retryable"] is True


def test_contract_violation_stays_non_retryable_and_run_terminating():
    # A schema/limit/budget-class denial (max_agent_calls here) is unaffected by
    # this issue: it stays retryable=False, exactly like unknown_agent.
    broker = CapabilityBroker(_RaisingRunner(), VMLimits(max_agent_calls=0))
    ret = broker.handle(_call("agent", {"agent_id": "hermes.echo", "input": {}}))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "limit_agent"
    assert ret["error"]["retryable"] is False


# --------------------------------------------------------------------------- #
# End-to-end: script catches CapabilityError and branches on e.retryable
# --------------------------------------------------------------------------- #

def test_script_catches_unknown_agent_id_and_sees_retryable_false():
    res = run_workflow_script(_CATCH_SCRIPT, agent_runner=_RaisingRunner())
    assert res.ok, res.error
    assert res.value == {"code": "unknown_agent", "retryable": False}


def test_script_catches_runner_failure_and_sees_retryable_true():
    script = META + (
        'try:\n'
        '    return await agent("hermes.echo", {"flaky": True})\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code, "retryable": e.retryable}\n'
    )
    res = run_workflow_script(script, agent_runner=_RaisingRunner())
    assert res.ok, res.error
    assert res.value == {"code": "runner_error", "retryable": True}


def test_script_can_degrade_gracefully_on_a_retryable_failure():
    # Acceptance criterion: a script can try/except and branch on e.retryable,
    # e.g. falling back to a default instead of failing the whole run.
    script = META + (
        'try:\n'
        '    result = await agent("hermes.echo", {"flaky": True})\n'
        'except CapabilityError as e:\n'
        '    if e.retryable:\n'
        '        result = {"echo": {"degraded": True}}\n'
        '    else:\n'
        '        raise\n'
        'return result\n'
    )
    res = run_workflow_script(script, agent_runner=_RaisingRunner())
    assert res.ok, res.error
    assert res.value == {"echo": {"degraded": True}}


# --------------------------------------------------------------------------- #
# Journaled classification
# --------------------------------------------------------------------------- #

def test_retryable_classification_is_journaled_next_to_the_error_code():
    journal: list[dict[str, Any]] = []
    run_workflow_script(_CATCH_SCRIPT, agent_runner=_RaisingRunner(), journal=journal.append)
    calls = [e for e in journal if e.get("type") == "rpc_call" and e.get("method") == "agent"]
    assert len(calls) == 1
    assert calls[0]["error"] == "unknown_agent"
    assert calls[0]["retryable"] is False


# --------------------------------------------------------------------------- #
# Replay determinism (issue #103 acceptance criterion): a run that caught a
# retryable failure and continued replays to the identical outcome. The
# ``runner_error`` failure itself is recorded in the replay cache (metadata
# only — call id/method/args hash/code/retryable, never a payload), so replay
# serves the identical denial without ever touching the runner. The
# unknown-agent-id failure is *not* cached (it is not retryable — a pure
# registry lookup reproduces it identically without help), so it is still
# re-derived live on replay.
# --------------------------------------------------------------------------- #

def test_replay_of_a_run_with_a_caught_retryable_failure_is_deterministic():
    script = META + (
        'results = {}\n'
        'try:\n'
        '    await agent("hermes.agent-does-not-exist", {})\n'
        'except CapabilityError as e:\n'
        '    results["unknown"] = {"code": e.code, "retryable": e.retryable}\n'
        'try:\n'
        '    await agent("hermes.echo", {"flaky": True})\n'
        'except CapabilityError as e:\n'
        '    results["runner"] = {"code": e.code, "retryable": e.retryable}\n'
        'results["ok"] = await agent("hermes.echo", {"flaky": False})\n'
        'return results\n'
    )
    expected = {
        "unknown": {"code": "unknown_agent", "retryable": False},
        "runner": {"code": "runner_error", "retryable": True},
        "ok": {"echo": {"flaky": False}},
    }

    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        live_runner = _RaisingRunner()
        rec = run_workflow_script(
            script,
            store=store,
            run_id="src",
            agent_runner=live_runner,
            deterministic_runner=True,
            limits=VMLimits(max_parallel=1),
        )
        assert rec.ok, rec.error
        assert rec.value == expected
        # The unknown-agent-id call never reaches the runner (rejected against the
        # static registry); the flaky call and the final success both do.
        assert len(live_runner.calls) == 2

        # The durable journal carries the same retryable classification the
        # script observed, keyed to the failing call's error code.
        journal_calls = [
            row for row in store.journal("src")
            if row.get("type") == "call" and row.get("method") == "agent" and row.get("ok") is False
        ]
        assert [(row["error"], row["retryable"]) for row in journal_calls] == [
            ("unknown_agent", False),
            ("runner_error", True),
        ]

        # The runner_error failure was persisted to the replay cache alongside
        # the successful call — the load-bearing check for replay determinism.
        cache = store.load_cache("src")
        failure_entries = [e for e in cache._entries.values() if not e.ok]  # noqa: SLF001
        assert [(e.method, e.code, e.retryable) for e in failure_entries] == [("agent", "runner_error", True)]

        # A runner that would happily *succeed* for the identical flaky call:
        # if the cached failure classification were not honored, replay would
        # silently diverge from the source run's outcome (the bug this test
        # guards against).
        replay_runner = _AlwaysSucceedsRunner()
        rep = run_workflow_script(
            script,
            store=store,
            run_id="replay",
            replay_from="src",
            agent_runner=replay_runner,
            deterministic_runner=True,
            limits=VMLimits(max_parallel=1),
        )

    assert rep.ok, rep.error
    assert rep.value == expected
    assert rep.value == rec.value
    # Every agent call on replay is served from the cache — including the
    # runner_error failure — so the runner is never touched at all.
    assert rep.replayed_calls == 2
    assert replay_runner.calls == []


def test_replayed_runner_error_is_served_by_the_broker_without_dispatching():
    """Broker-level proof of the journal->replay linkage (not just the
    full-script fixture above): a bare :class:`CapabilityBroker` fed a
    pre-built :class:`ReplayCache` containing a recorded ``runner_error``
    failure must serve that denial verbatim without ever calling the runner,
    even when the live runner would succeed for the identical call.
    """
    entry = ReplayEntry(
        call_id=1,
        method="agent",
        args_hash=_args_hash("agent", {"agent_id": "hermes.echo", "input": {"flaky": True}}),
        ok=False,
        code="runner_error",
        retryable=True,
    )
    replay = ReplayCache({1: entry}, source_run_id="src")
    runner = _AlwaysSucceedsRunner()
    broker = CapabilityBroker(runner, VMLimits(), replay=replay, deterministic_runner=True)

    ret = broker.handle(_call("agent", {"agent_id": "hermes.echo", "input": {"flaky": True}}))

    assert ret["ok"] is False
    assert ret["error"]["code"] == "runner_error"
    assert ret["error"]["retryable"] is True
    assert runner.calls == []
    assert broker.replayed_calls == 1


def _args_hash(method: str, params: dict[str, Any]) -> str:
    from hermes_workflows.script_store import replay_args_hash

    return replay_args_hash(method, params)


# --------------------------------------------------------------------------- #
# Capability-boundary classification (a host handler fault stays
# retryable=False even when it raises a BaseException, e.g. sys.exit()).
# --------------------------------------------------------------------------- #

def test_capability_handler_system_exit_is_capability_handler_error_not_runner_error():
    def _handler(context: dict[str, Any]) -> dict[str, Any]:
        raise SystemExit(1)

    registry = CapabilityRegistry()
    registry.register("host.exits", _handler, side_effect_class="read_only", replayable=False)
    broker = CapabilityBroker(
        _RaisingRunner(), VMLimits(), capability_registry=registry, capability_policy=CapabilityPolicy()
    )
    ret = broker.handle(_call("capability", {"name": "host.exits", "input": {}}))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "capability_handler_error"
    assert ret["error"]["retryable"] is False


# --------------------------------------------------------------------------- #
# Sync guest-side connection: the retryable field is parsed the same way on
# the synchronous ``_Connection.call`` path (used by log()/phase()) as on the
# async ``acall``/``_read_returns`` path (used by agent/kanban_agent/
# capability/workflow).
# --------------------------------------------------------------------------- #

class _FakeChannel:
    def __init__(self, ret_frames: list[dict[str, Any]]) -> None:
        self._ret_frames = list(ret_frames)
        self.sent: list[dict[str, Any]] = []

    def send(self, frame: dict[str, Any]) -> None:
        self.sent.append(frame)

    def recv(self) -> dict[str, Any]:
        return self._ret_frames.pop(0)


def test_sync_connection_call_parses_retryable_field_on_a_denied_ret_frame():
    channel = _FakeChannel([
        {
            "t": rpc.T_RET, "id": 1, "ok": False,
            "error": {"code": "runner_error", "message": "boom", "retryable": True},
            "budget": {},
        },
    ])
    conn = vm_guest._Connection(channel, vm_guest._Budget(None))  # noqa: SLF001 - guest-internal test.

    with pytest.raises(vm_guest.CapabilityError) as exc_info:
        conn.call("log", {"message": "x"})

    assert exc_info.value.code == "runner_error"
    assert exc_info.value.retryable is True


def test_sync_connection_call_defaults_retryable_false_when_absent():
    channel = _FakeChannel([
        {
            "t": rpc.T_RET, "id": 1, "ok": False,
            "error": {"code": "unknown_agent", "message": "boom"},
            "budget": {},
        },
    ])
    conn = vm_guest._Connection(channel, vm_guest._Budget(None))  # noqa: SLF001 - guest-internal test.

    with pytest.raises(vm_guest.CapabilityError) as exc_info:
        conn.call("log", {"message": "x"})

    assert exc_info.value.retryable is False


# --------------------------------------------------------------------------- #
# Uncaught propagation: retryable survives to the run's terminal error and the
# redacted durable journal even when the script never catches the failure.
# --------------------------------------------------------------------------- #

def test_uncaught_runner_error_carries_retryable_to_the_terminal_error():
    script = META + 'return await agent("hermes.echo", {"flaky": True})\n'
    res = run_workflow_script(script, agent_runner=_RaisingRunner())
    assert res.ok is False
    assert res.error["type"] == "CapabilityError"
    assert res.error["code"] == "runner_error"
    assert res.error["retryable"] is True


def test_uncaught_runner_error_in_a_pipeline_stage_mirrors_retryable():
    script = META + (
        'async def stage(item):\n'
        '    return await agent("hermes.echo", item)\n'
        'return await pipeline([{"flaky": True}], stage)\n'
    )
    res = run_workflow_script(script, agent_runner=_RaisingRunner())
    assert res.ok is False
    assert res.error["type"] == "PipelineStageError"
    assert res.error["cause_type"] == "CapabilityError"
    assert res.error["code"] == "runner_error"
    assert res.error["retryable"] is True


def test_terminal_error_journal_redacts_message_but_keeps_retryable():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        script = META + 'return await agent("hermes.echo", {"flaky": True})\n'
        res = run_workflow_script(script, agent_runner=_RaisingRunner(), store=store, run_id="uncaught")
        assert res.ok is False

        done_events = [e for e in store.journal("uncaught") if e.get("type") == "done"]
        assert len(done_events) == 1
        error = done_events[0]["error"]
        assert error["code"] == "runner_error"
        assert error["retryable"] is True
        assert "message" not in error  # redacted: metadata-only, no script-authored text.
