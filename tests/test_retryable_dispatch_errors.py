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
consumers see the same classification a script observed.
"""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from hermes_workflows import run_workflow_script
from hermes_workflows.errors import CapabilityDenied
from hermes_workflows.script_store import ScriptRunStore
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
# failure itself is never cached (only successful, replayable calls are), so a
# replay re-dispatches it live; with a deterministic (pure-function) runner
# that reproduces the identical outcome deterministically both times.
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

        replay_runner = _RaisingRunner()
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
    # The one successful, replayable call is served from the cache. The
    # unknown-agent-id failure is deterministic without touching the runner at
    # all (a pure registry lookup); only the flaky runner failure re-dispatches
    # live, and reproduces the identical classification deterministically.
    assert rep.replayed_calls == 1
    assert len(replay_runner.calls) == 1
    assert replay_runner.calls[0]["agent_id"] == "hermes.echo"
    assert replay_runner.calls[0]["payload"] == {"flaky": True}
