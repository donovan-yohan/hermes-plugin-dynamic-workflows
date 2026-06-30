"""Tests for script ``agent(prompt, opts)`` prompt subagent calls (issue #70)."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from hermes_workflows import REDACTED, ChildAgentRequest, ScriptRunStore, run_workflow_script

META = 'meta = {"name": "prompt-agent", "description": "d"}\n'


class FakeChildRunner:
    def __init__(self, output: dict[str, Any] | None = None) -> None:
        self.output = output or {"answer": "ok", "_tokens": 3}
        self.requests: list[ChildAgentRequest] = []

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        self.requests.append(request)
        return dict(self.output)


def test_prompt_agent_routes_to_injected_child_runner_and_persists_redacted_outputs():
    secret = "ghp_should_not_persist_secret"
    runner = FakeChildRunner({"answer": "ok", "token": secret, "nested": {"detail": secret}, "_tokens": 3})
    script = META + (
        'result = await agent("summarize the latest PR", {\n'
        '    "label": "summary",\n'
        '    "phase": "analysis",\n'
        '    "schema": {"answer": "string"},\n'
        '    "context": {"pr": 70},\n'
        '    "model": "sonnet",\n'
        '    "effort": "medium",\n'
        '    "isolation": "fresh",\n'
        '})\n'
        'return result\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            script,
            store=store,
            run_id="prompt_run",
            child_agent_runner=runner,
            deterministic_runner=True,
        )
        assert res.ok, res.error
        expected = {"answer": "ok", "token": REDACTED, "nested": {"detail": REDACTED}, "_tokens": 3}
        assert res.value == expected
        assert len(runner.requests) == 1
        request = runner.requests[0]
        assert request.prompt == "summarize the latest PR"
        assert request.label == "summary"
        assert request.phase == "analysis"
        assert request.schema == {"answer": "string"}
        assert request.context == {"pr": 70}
        assert request.model == "sonnet"
        assert request.effort == "medium"
        assert request.isolation == "fresh"

        journal = store.journal("prompt_run")
        started = next(event for event in journal if event["type"] == "agent_started")
        result_event = next(event for event in journal if event["type"] == "agent_result")
        assert started["fingerprint"].startswith("v2:")
        assert result_event["fingerprint"] == started["fingerprint"]
        call = next(event for event in journal if event["type"] == "call" and event["method"] == "agent")
        assert call["label"] == "summary"
        assert call["phase"] == "analysis"
        assert call["fingerprint"] == started["fingerprint"]
        assert "prompt" not in call
        assert "params" not in call
        assert store.load_run("prompt_run").value == expected
        assert store.load_cache("prompt_run").get(1).value == expected
        assert store.load_cache("prompt_run").get_prompt(started["fingerprint"]).value == expected
        run_dir = Path(tmp) / "runs" / "prompt_run"
        assert secret not in (run_dir / "run.json").read_text(encoding="utf-8")
        assert secret not in (run_dir / "cache.jsonl").read_text(encoding="utf-8")
        assert secret not in (run_dir / "journal.jsonl").read_text(encoding="utf-8")


def test_prompt_agent_without_child_runner_fails_closed_instead_of_using_stub_runner():
    res = run_workflow_script(META + 'return await agent("write a plan")\n')
    assert res.ok is False
    assert res.error["code"] == "child_agent_unavailable"


def test_prompt_agent_rejects_unknown_options_explicitly():
    runner = FakeChildRunner()
    script = META + (
        'try:\n'
        '    await agent("write a plan", {"temperature": 0.2})\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code}\n'
        'return {"code": "missing"}\n'
    )
    res = run_workflow_script(script, child_agent_runner=runner)
    assert res.ok, res.error
    assert res.value == {"code": "bad_request"}
    assert runner.requests == []


def test_prompt_agent_child_output_is_json_safe_before_returning_to_script():
    class Weird:
        pass

    runner = FakeChildRunner({"answer": "ok", "bad": Weird(), "items": [Weird()]})
    res = run_workflow_script(
        META + 'return await agent("make json", {"schema": {"answer": "string"}})\n',
        child_agent_runner=runner,
    )
    assert res.ok, res.error
    assert res.value == {
        "answer": "ok",
        "bad": {"_unserializable_type": "Weird"},
        "items": [{"_unserializable_type": "Weird"}],
    }


def test_legacy_agent_id_input_compatibility_is_preserved():
    res = run_workflow_script(
        META + 'return await agent("hermes.greeter", {"subject": "compat"})\n'
    )
    assert res.ok, res.error
    assert res.value == {"greeting": "hello, compat"}


def test_prompt_agent_with_dotted_prompt_is_not_misrouted_as_legacy_agent_id():
    runner = FakeChildRunner({"answer": "dotted prompt"})
    res = run_workflow_script(META + 'return await agent("summarize.")\n', child_agent_runner=runner)
    assert res.ok, res.error
    assert res.value == {"answer": "dotted prompt"}
    assert runner.requests[0].prompt == "summarize."


def test_prompt_agent_rejects_non_dict_positional_options_without_unknown_agent_misroute():
    runner = FakeChildRunner()
    script = META + (
        'try:\n'
        '    await agent("summarize", "not-an-options-object")\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code, "message": str(e)}\n'
        'return {"code": "missing"}\n'
    )
    res = run_workflow_script(script, child_agent_runner=runner)
    assert res.ok, res.error
    assert res.value["code"] == "bad_request"
    assert "unknown agent" not in res.value["message"]
    assert runner.requests == []



def test_prompt_agent_fingerprint_cache_replays_without_respawning_child():
    runner = FakeChildRunner({"answer": "cached", "_tokens": 5})
    script = META + (
        'result = await agent("summarize the latest PR", {\n'
        '    "label": "summary", "phase": "analysis",\n'
        '    "schema": {"answer": "string"}, "context": {"pr": 75},\n'
        '    "model": "sonnet", "effort": "medium", "isolation": "fresh",\n'
        '})\n'
        'return result\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        rec = run_workflow_script(script, store=store, run_id="src", child_agent_runner=runner)
        assert rec.ok, rec.error
        assert len(runner.requests) == 1
        fingerprint = next(e for e in store.journal("src") if e["type"] == "agent_result")["fingerprint"]
        assert fingerprint.startswith("v2:")
        assert store.load_cache("src").get(1) is None  # live child output is not call-id replayable.
        assert store.load_cache("src").get_prompt(fingerprint).value == {"answer": "cached", "_tokens": 5}

        # No child runner is configured on replay. A miss would fail closed with
        # child_agent_unavailable, so success proves the fingerprint cache hit.
        rep = run_workflow_script(script, store=store, run_id="replay", replay_from="src")
        assert rep.ok, rep.error
        assert rep.value == {"answer": "cached", "_tokens": 5}
        assert rep.replayed_calls == 1
        replay_events = store.journal("replay")
        hit = next(e for e in replay_events if e["type"] == "agent_cache_hit")
        assert hit["fingerprint"] == fingerprint
        assert hit["cache"] == "replay"


def test_prompt_agent_duplicate_prompt_hits_current_run_cache():
    runner = FakeChildRunner({"answer": "same", "_tokens": 2})
    script = META + (
        'a = await agent("repeat work", {"label": "same", "schema": {"answer": "string"}})\n'
        'b = await agent("repeat work", {"label": "same", "schema": {"answer": "string"}})\n'
        'return {"a": a["answer"], "b": b["answer"]}\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(script, store=store, run_id="dupe", child_agent_runner=runner)
        assert res.ok, res.error
        assert res.value == {"a": "same", "b": "same"}
        assert len(runner.requests) == 1
        events = store.journal("dupe")
        assert len([e for e in events if e["type"] == "agent_started"]) == 1
        hit = next(e for e in events if e["type"] == "agent_cache_hit")
        assert hit["cache"] == "run"


def test_prompt_agent_semantic_options_change_fingerprint():
    runner = FakeChildRunner({"answer": "ok"})
    script = META + (
        'await agent("same prompt", {"label": "one", "schema": {"answer": "string"}})\n'
        'await agent("same prompt", {"label": "two", "schema": {"answer": "string"}})\n'
        'return {}\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(script, store=store, run_id="labels", child_agent_runner=runner)
        assert res.ok, res.error
        assert len(runner.requests) == 2
        fingerprints = [e["fingerprint"] for e in store.journal("labels") if e["type"] == "agent_started"]
        assert len(fingerprints) == 2
        assert fingerprints[0] != fingerprints[1]


def test_prompt_agent_cache_drift_fails_closed():
    runner = FakeChildRunner({"answer": "cached"})
    script = META + 'return await agent("cache me", {"schema": {"answer": "string"}})\n'
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        rec = run_workflow_script(script, store=store, run_id="src_drift", child_agent_runner=runner)
        assert rec.ok, rec.error
        cache_path = Path(tmp) / "runs" / "src_drift" / "cache.jsonl"
        lines = []
        for raw in cache_path.read_text(encoding="utf-8").splitlines():
            entry = json.loads(raw)
            if "fingerprint" in entry:
                entry["args_hash"] = "tampered"
            lines.append(json.dumps(entry))
        cache_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        rep = run_workflow_script(script, store=store, run_id="replay_drift", replay_from="src_drift")
        assert rep.ok is False
        assert "prompt replay drift" in rep.error["message"]
