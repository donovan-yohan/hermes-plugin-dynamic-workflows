"""Size-bound ``agent``/``kanban_agent`` results in the broker success path (#106).

``capability()`` results are already size-clipped (``CapabilityPolicy.max_result_bytes``,
issue #29); ``agent``/``kanban_agent`` results were not, so a huge child result could
land inline in script memory *and* in the replay cache unbounded. ``VMLimits.max_result_bytes``
mirrors the capability bound: the broker checks the JSON-serialized size of every
``agent``/``kanban_agent`` value (prompt-agent calls are ``agent`` calls with a ``prompt``
param, so they are covered by the same check) immediately after dispatch and before
``_persist_success`` — an over-limit result fails deterministically with ``result_too_large``
(metadata-only: observed size, limit, call id) rather than being silently truncated, and
never reaches the replay cache or the in-memory prompt-agent cache.
"""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from hermes_workflows import ChildAgentRequest, run_workflow_script
from hermes_workflows.script_store import ScriptRunStore
from hermes_workflows.vm import (
    VMLimits,
    _CorruptLimitsView,
    _limits_from_view,
    _limits_view,
)

META = 'meta = {"name": "result-size-bound", "description": "d"}\n'

_CATCH_SCRIPT_AGENT = META + (
    'try:\n'
    '    return await agent("hermes.greeter", {})\n'
    'except CapabilityError as e:\n'
    '    return {"code": e.code}\n'
)

_CATCH_SCRIPT_KANBAN = META + (
    'try:\n'
    '    return await kanban_agent("planner")\n'
    'except CapabilityError as e:\n'
    '    return {"code": e.code}\n'
)

_CATCH_SCRIPT_PROMPT = META + (
    'try:\n'
    '    return await agent("summarize", {"label": "s"})\n'
    'except CapabilityError as e:\n'
    '    return {"code": e.code}\n'
)


class FakeChildRunner:
    """Minimal stub :class:`ChildAgentRunner` returning a fixed output."""

    def __init__(self, output: dict[str, Any]) -> None:
        self.output = output
        self.requests: list[ChildAgentRequest] = []

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        self.requests.append(request)
        return dict(self.output)


# --------------------------------------------------------------------------- #
# VMLimits default / round-trip
# --------------------------------------------------------------------------- #

def test_max_result_bytes_default_is_512kib():
    assert VMLimits().max_result_bytes == 512 * 1024


def test_max_result_bytes_view_round_trips():
    view = _limits_view(VMLimits(max_result_bytes=4096))
    assert view["max_result_bytes"] == 4096
    assert _limits_from_view(view).max_result_bytes == 4096


def test_max_result_bytes_view_defaults_when_absent():
    assert _limits_from_view({}).max_result_bytes == VMLimits().max_result_bytes


def test_max_result_bytes_view_fails_closed_on_corrupt_value():
    try:
        _limits_from_view({"max_result_bytes": "huge"})
    except _CorruptLimitsView:
        return
    raise AssertionError("expected _CorruptLimitsView for a non-numeric max_result_bytes")


# --------------------------------------------------------------------------- #
# Broker success path: fail closed on an over-limit result
# --------------------------------------------------------------------------- #

def test_agent_result_over_limit_fails_closed_with_structured_error():
    def runner(agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"blob": "z" * 2000}

    res = run_workflow_script(
        _CATCH_SCRIPT_AGENT, agent_runner=runner, limits=VMLimits(max_result_bytes=256)
    )
    assert res.ok, res.error
    assert res.value == {"code": "result_too_large"}


def test_agent_result_within_limit_is_unaffected():
    def runner(agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"greeting": "hi"}

    res = run_workflow_script(
        _CATCH_SCRIPT_AGENT, agent_runner=runner, limits=VMLimits(max_result_bytes=256)
    )
    assert res.ok, res.error
    assert res.value == {"greeting": "hi"}


def test_kanban_agent_result_over_limit_fails_closed_with_structured_error():
    def runner(agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"blob": "z" * 2000}

    res = run_workflow_script(
        _CATCH_SCRIPT_KANBAN, agent_runner=runner, limits=VMLimits(max_result_bytes=256)
    )
    assert res.ok, res.error
    assert res.value == {"code": "result_too_large"}


def test_prompt_agent_result_over_limit_fails_closed_with_structured_error():
    runner = FakeChildRunner({"blob": "z" * 2000})
    res = run_workflow_script(
        _CATCH_SCRIPT_PROMPT, child_agent_runner=runner, limits=VMLimits(max_result_bytes=256)
    )
    assert res.ok, res.error
    assert res.value == {"code": "result_too_large"}


def test_result_too_large_error_message_carries_size_limit_and_call_id_only():
    def runner(agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"blob": "SECRET-PAYLOAD-MARKER" * 100}

    script = META + 'return await agent("hermes.greeter", {})\n'
    res = run_workflow_script(script, agent_runner=runner, limits=VMLimits(max_result_bytes=256))
    assert res.ok is False
    assert res.error is not None
    assert res.error["code"] == "result_too_large"
    message = res.error["message"]
    # Metadata-only: the observed/limit sizes and the call id are present...
    assert "256" in message
    assert "1" in message  # call_id == 1 for a single top-level call.
    # ...but the payload content itself is never echoed back.
    assert "SECRET-PAYLOAD-MARKER" not in message


# --------------------------------------------------------------------------- #
# Replay cache / in-memory prompt cache never stores an over-limit payload
# --------------------------------------------------------------------------- #

def test_agent_result_over_limit_is_never_persisted_to_replay_cache():
    calls: list[str] = []

    def runner(agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        calls.append(agent_id)
        return {"blob": "z" * 2000}

    limits = VMLimits(max_result_bytes=256)
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        rec = run_workflow_script(
            _CATCH_SCRIPT_AGENT, store=store, run_id="src", agent_runner=runner, limits=limits
        )
        assert rec.ok, rec.error
        assert rec.value == {"code": "result_too_large"}

        rep = run_workflow_script(
            _CATCH_SCRIPT_AGENT,
            store=store,
            run_id="replay",
            replay_from="src",
            agent_runner=runner,
            limits=limits,
        )

    # A replay-cache hit would serve the oversized value without touching the
    # runner again; instead the call must miss the cache and re-dispatch live,
    # deterministically failing closed a second time.
    assert len(calls) == 2
    assert rep.ok, rep.error
    assert rep.value == {"code": "result_too_large"}
    assert rep.replayed_calls == 0


def test_prompt_agent_result_over_limit_is_never_cached_for_a_repeat_call():
    runner = FakeChildRunner({"blob": "z" * 2000})
    script = META + (
        'results = []\n'
        'for _ in range(2):\n'
        '    try:\n'
        '        await agent("summarize", {"label": "s"})\n'
        '    except CapabilityError as e:\n'
        '        results.append(e.code)\n'
        'return results\n'
    )
    res = run_workflow_script(script, child_agent_runner=runner, limits=VMLimits(max_result_bytes=256))
    assert res.ok, res.error
    assert res.value == ["result_too_large", "result_too_large"]
    # A hit on the in-memory prompt-results cache would skip the second dispatch
    # entirely; two requests proves the oversized value was never cached.
    assert len(runner.requests) == 2
