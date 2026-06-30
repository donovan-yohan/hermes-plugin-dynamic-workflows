"""Tests for script ``agent(prompt, opts)`` prompt subagent calls (issue #70)."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from hermes_workflows import (
    REDACTED,
    CapabilityBroker,
    ChildAgentRequest,
    ScriptRunStore,
    VMLimits,
    run_workflow_script,
)

META = 'meta = {"name": "prompt-agent", "description": "d"}\n'


class FakeChildRunner:
    def __init__(self, output: dict[str, Any] | None = None) -> None:
        self.output = output or {"answer": "ok", "_tokens": 3}
        self.requests: list[ChildAgentRequest] = []

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        self.requests.append(request)
        return dict(self.output)


class PromptOutputRunner:
    def __init__(self, outputs: dict[str, dict[str, Any]]) -> None:
        self.outputs = outputs
        self.requests: list[ChildAgentRequest] = []

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        self.requests.append(request)
        return dict(self.outputs[request.prompt])


class SequenceChildRunner:
    def __init__(self, outputs: list[Any]) -> None:
        self.outputs = list(outputs)
        self.requests: list[ChildAgentRequest] = []

    def __call__(self, request: ChildAgentRequest) -> Any:
        self.requests.append(request)
        if self.outputs:
            return self.outputs.pop(0)
        return {}


def test_child_agent_request_as_dict_does_not_share_mutable_schema_or_context():
    request = ChildAgentRequest(
        prompt="summarize",
        schema={"nested": {"answer": "string"}},
        context={"items": [{"pr": 75}]},
    )

    exported = request.as_dict()
    exported["schema"]["nested"]["answer"] = "number"
    exported["context"]["items"][0]["pr"] = 87

    assert request.schema == {"nested": {"answer": "string"}}
    assert request.context == {"items": [{"pr": 75}]}


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


def test_prompt_agent_period_sentence_routes_to_child_runner_not_legacy_agent_id():
    runner = FakeChildRunner()
    res = run_workflow_script(
        META + 'return await agent("Summarize.")\n',
        child_agent_runner=runner,
    )

    assert res.ok, res.error
    assert res.value == {"answer": "ok", "_tokens": 3}
    assert len(runner.requests) == 1
    assert runner.requests[0].prompt == "Summarize."


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


def test_prompt_agent_schema_invalid_output_retries_with_validation_context_and_journal():
    runner = SequenceChildRunner([
        {"answer": 7},
        {"answer": "ok", "_tokens": 5},
    ])
    script = META + (
        'result = await agent("summarize", {\n'
        '    "label": "summary",\n'
        '    "phase": "analysis",\n'
        '    "schema": {"answer": "string"},\n'
        '    "context": {"topic": "pr"},\n'
        '})\n'
        'return result\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            script,
            store=store,
            run_id="schema_retry_run",
            child_agent_runner=runner,
            deterministic_runner=True,
        )

        assert res.ok, res.error
        assert res.value == {"answer": "ok", "_tokens": 5}
        assert len(runner.requests) == 2
        assert runner.requests[0].context == {"topic": "pr"}
        retry_context = runner.requests[1].context
        assert retry_context["topic"] == "pr"
        assert retry_context["schema_validation_error"]["code"] == "schema"
        assert "expected string" in retry_context["schema_validation_error"]["message"]

        retry_events = [event for event in res.calls if event.get("error") == "schema_retry"]
        assert len(retry_events) == 1
        retry_event = retry_events[0]
        assert retry_event["type"] == "rpc_call"
        assert retry_event["call_id"] == 1
        assert retry_event["method"] == "agent"
        assert retry_event["agent_id"] == "prompt"
        assert retry_event["ok"] is False
        assert retry_event["label"] == "summary"
        assert retry_event["phase"] == "analysis"
        assert retry_event["attempt"] == 1
        assert retry_event["max_retries"] == 2
        journal_retry = [event for event in store.journal("schema_retry_run") if event.get("error") == "schema_retry"]
        assert journal_retry[0]["attempt"] == 1
        assert journal_retry[0]["max_retries"] == 2
        assert store.load_cache("schema_retry_run").get(1).value == {"answer": "ok", "_tokens": 5}


def test_prompt_agent_schema_retry_journal_redacts_label_and_phase_metadata():
    runner = SequenceChildRunner([
        {"answer": 7},
        {"answer": "ok"},
    ])
    script = META + (
        'result = await agent("summarize", {\n'
        '    "label": "ghp_SECRET_TOKEN",\n'
        '    "phase": "token=phase-secret",\n'
        '    "schema": {"answer": "string"},\n'
        '})\n'
        'return result\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            script,
            store=store,
            run_id="schema_retry_redaction_run",
            child_agent_runner=runner,
            deterministic_runner=True,
        )

        assert res.ok, res.error
        retry_event = next(event for event in res.calls if event.get("error") == "schema_retry")
        assert retry_event["label"] == REDACTED
        assert retry_event["phase"] == REDACTED

        journal_retry = next(
            event for event in store.journal("schema_retry_redaction_run") if event.get("error") == "schema_retry"
        )
        assert journal_retry["label"] == REDACTED
        assert journal_retry["phase"] == REDACTED
        assert "ghp_SECRET_TOKEN" not in repr(journal_retry)
        assert "token=phase-secret" not in repr(journal_retry)


def test_prompt_agent_schema_retry_exhaustion_returns_typed_schema_failure():
    runner = SequenceChildRunner([{"answer": 7}, {}])
    res = run_workflow_script(
        META + 'return await agent("summarize", {"schema": {"answer": "string"}})\n',
        child_agent_runner=runner,
        limits=VMLimits(max_schema_retries=1),
    )

    assert res.ok is False
    assert res.error["code"] == "schema"
    assert "schema validation failed after 2 attempt(s)" in res.error["message"]
    assert len(runner.requests) == 2
    assert [event.get("error") for event in res.calls].count("schema_retry") == 1


def test_punctuated_schema_prompt_routes_to_child_runner_not_legacy_agent_id():
    runner = FakeChildRunner({"answer": "brief"})
    res = run_workflow_script(
        META + 'return await agent("Summarize.", {"schema": {"answer": "string"}})\n',
        child_agent_runner=runner,
    )

    assert res.ok, res.error
    assert res.value == {"answer": "brief"}
    assert len(runner.requests) == 1
    assert runner.requests[0].prompt == "Summarize."
    assert runner.requests[0].schema == {"answer": "string"}


def test_legacy_agent_id_input_compatibility_is_preserved():
    res = run_workflow_script(
        META + 'return await agent("hermes.greeter", {"subject": "compat"})\n'
    )
    assert res.ok, res.error
    assert res.value == {"greeting": "hello, compat"}


def test_legacy_agent_id_option_key_payloads_stay_legacy_input_data():
    for payload in (
        {"label": "as data"},
        {"phase": "as data"},
        {"schema": {"x": "y"}},
        {"context": {"x": 1}},
    ):
        res = run_workflow_script(META + f"return await agent(\"hermes.echo\", {payload!r})\n")
        assert res.ok, res.error
        assert res.value["echo"] == payload


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


def test_prompt_agent_duplicate_prompt_cache_hit_obeys_token_budget():
    runner = FakeChildRunner({"answer": "same", "_tokens": 1})
    script = META + (
        'await agent("same", {"label": "x"})\n'
        'await agent("same", {"label": "x"})\n'
        'return {"code": "bypassed"}\n'
    )

    res = run_workflow_script(script, child_agent_runner=runner, limits=VMLimits(token_budget=1))

    assert res.ok is False
    assert "hard-limit" in res.error["message"]
    assert [request.prompt for request in runner.requests] == ["same"]


def test_prompt_agent_duplicate_prompt_cache_hit_obeys_max_agent_calls():
    runner = FakeChildRunner({"answer": "same", "_tokens": 0})
    script = META + (
        'await agent("same", {"label": "x"})\n'
        'await agent("same", {"label": "x"})\n'
        'return {"code": "bypassed"}\n'
    )

    res = run_workflow_script(script, child_agent_runner=runner, limits=VMLimits(max_agent_calls=1))

    assert res.ok is False
    assert res.error["code"] == "limit_agent"
    assert [request.prompt for request in runner.requests] == ["same"]


def test_prompt_agent_negative_and_bool_tokens_do_not_lower_or_spend_budget():
    runner = PromptOutputRunner(
        {
            "negative usage": {"answer": "ignored", "_tokens": -100},
            "bool usage": {"answer": "ignored", "_tokens": True},
            "spend budget": {"answer": "spent", "_tokens": 1},
            "after budget": {"answer": "should not run", "_tokens": 1},
        }
    )
    script = META + (
        'await agent("negative usage", {"label": "negative"})\n'
        'await agent("negative usage", {"label": "negative"})\n'
        'await agent("bool usage", {"label": "bool"})\n'
        'await agent("bool usage", {"label": "bool"})\n'
        'await agent("spend budget", {"label": "spend"})\n'
        'await agent("after budget", {"label": "after"})\n'
        'return {"code": "bypassed"}\n'
    )

    res = run_workflow_script(script, child_agent_runner=runner, limits=VMLimits(token_budget=1))

    assert res.ok is False
    assert "hard-limit" in res.error["message"]
    assert [request.prompt for request in runner.requests] == [
        "negative usage",
        "bool usage",
        "spend budget",
    ]


def test_prompt_agent_replay_negative_tokens_do_not_lower_budget():
    class TokenRunner:
        def __call__(self, agent_id, input):  # noqa: A002 — match AgentRunner signature.
            return {"ok": agent_id, "_tokens": 1}

    runner = PromptOutputRunner({"negative usage": {"answer": "ignored", "_tokens": -100}})
    source_script = META + 'return await agent("negative usage", {"label": "negative"})\n'
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        rec = run_workflow_script(
            source_script, store=store, run_id="src_tokens", child_agent_runner=runner
        )
        assert rec.ok, rec.error
        replay = store.load_cache("src_tokens")

    broker = CapabilityBroker(TokenRunner(), VMLimits(token_budget=1), replay=replay)
    cached = broker.handle(
        {
            "t": "call",
            "id": 1,
            "method": "agent",
            "params": {"prompt": "negative usage", "label": "negative"},
        }
    )
    spend = broker.handle(
        {"t": "call", "id": 2, "method": "agent", "params": {"agent_id": "hermes.echo", "input": {}}}
    )
    denied = broker.handle(
        {"t": "call", "id": 3, "method": "agent", "params": {"agent_id": "hermes.echo", "input": {}}}
    )

    assert cached["ok"] is True
    assert cached["budget"]["spent"] == 0
    assert spend["ok"] is True
    assert spend["budget"]["spent"] == 1
    assert denied["ok"] is False
    assert denied["error"]["code"] == "limit_token"


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
