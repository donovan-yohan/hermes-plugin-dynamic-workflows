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
    assert "call 1" in message  # call_id == 1 for a single top-level call; an
    # unambiguous token so this doesn't spuriously match a digit inside the
    # observed-size number instead (e.g. a 2113-byte observed size).
    # ...but the payload content itself is never echoed back.
    assert "SECRET-PAYLOAD-MARKER" not in message


def test_agent_result_with_non_string_dict_key_over_limit_fails_closed():
    # ``sort_keys=True`` raises TypeError comparing a str key against an int
    # key, which used to make the size check silently pass the whole result
    # through uncapped; the ret-frame/cache encoders (no sort_keys) serialize
    # this shape fine, so the bound must too.
    def runner(agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {1: "z" * 100_000, "ok": True}

    res = run_workflow_script(
        _CATCH_SCRIPT_AGENT, agent_runner=runner, limits=VMLimits(max_result_bytes=256)
    )
    assert res.ok, res.error
    assert res.value == {"code": "result_too_large"}


def test_agent_result_with_non_json_native_value_over_limit_fails_closed():
    # A non-JSON-native value (e.g. bytes) is still accepted by the replay
    # cache's ``default=str`` encoder, so the size bound must measure it the
    # same way rather than bailing out on the first TypeError.
    def runner(agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"blob": b"z" * 100_000, "ok": True}

    res = run_workflow_script(
        _CATCH_SCRIPT_AGENT, agent_runner=runner, limits=VMLimits(max_result_bytes=256)
    )
    assert res.ok, res.error
    assert res.value == {"code": "result_too_large"}


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


def test_agent_result_with_non_json_native_value_over_limit_never_written_to_cache_file():
    # With a deterministic runner the ``agent`` call is cacheable; the recorder
    # (``script_store.CallRecorder.record``) uses ``default=str`` so it never
    # raises on a non-JSON-native payload — confirm the size bound still fires
    # *before* that write, so ``cache.jsonl`` never receives the oversized value.
    def runner(agent_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"blob": b"z" * 100_000, "ok": True}

    limits = VMLimits(max_result_bytes=256)
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        rec = run_workflow_script(
            _CATCH_SCRIPT_AGENT,
            store=store,
            run_id="src",
            agent_runner=runner,
            limits=limits,
            deterministic_runner=True,
        )
        assert rec.ok, rec.error
        assert rec.value == {"code": "result_too_large"}

        cache_path = store.root / "src" / "cache.jsonl"
        cache_text = cache_path.read_text() if cache_path.exists() else ""

    assert "zzzz" not in cache_text
    assert len(cache_text.encode("utf-8")) < limits.max_result_bytes


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
