"""Tests for script ``agent(prompt, opts)`` prompt subagent calls (issue #70)."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from hermes_workflows import (
    REDACTED,
    AgentTypeRegistry,
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


def test_child_agent_request_as_dict_serializes_tools_and_defaults_to_none():
    without_tools = ChildAgentRequest(prompt="summarize")
    assert without_tools.as_dict()["tools"] is None

    with_tools = ChildAgentRequest(prompt="summarize", tools=("read_file", "grep"))
    assert with_tools.as_dict()["tools"] == ["read_file", "grep"]


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


def test_prompt_agent_schema_retry_preserves_tools_allowlist_on_reconstructed_request():
    # Regression pin (issue #101 review): the retry-context reconstruction in
    # _request_with_schema_retry_context must carry the caller's `tools`
    # allowlist forward on every attempt -- dropping it would silently widen
    # (unscope) the child dispatched on retry.
    runner = SequenceChildRunner([
        {"answer": 7},
        {"answer": "ok"},
    ])
    script = META + (
        'result = await agent("summarize", {\n'
        '    "schema": {"answer": "string"},\n'
        '    "tools": ["read_file"],\n'
        '})\n'
        'return result\n'
    )
    res = run_workflow_script(script, child_agent_runner=runner)

    assert res.ok, res.error
    assert len(runner.requests) == 2
    assert runner.requests[0].tools == ("read_file",)
    assert runner.requests[1].tools == runner.requests[0].tools == ("read_file",)


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


def test_prompt_agent_malformed_dynamic_schema_never_invokes_the_child_runner():
    # A schema built from a variable escapes script_validator's static literal
    # check (issue #107 review): the broker must reject a malformed schema
    # once, up front -- not burn max_schema_retries+1 real child agent calls
    # on a retry loop that can never succeed (a bad schema is never fixed by
    # the *payload* changing).
    runner = SequenceChildRunner([{"answer": "ok"}])
    script = META + (
        'bad_schema = {"type": "object", "bogus": 1}\n'
        'return await agent("summarize", {"schema": bad_schema})\n'
    )
    res = run_workflow_script(
        script,
        child_agent_runner=runner,
        limits=VMLimits(max_schema_retries=3),
    )

    assert res.ok is False
    assert res.error["code"] == "bad_request"
    assert len(runner.requests) == 0
    assert [event.get("error") for event in res.calls].count("schema_retry") == 0


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
        {"tools": ["read_file"]},
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


def test_concurrent_duplicate_prompt_agents_record_one_cache_fingerprint():
    count = 16

    class SlowChildRunner:
        def __init__(self) -> None:
            self.requests: list[ChildAgentRequest] = []
            self._lock = threading.Lock()
            self._barrier = threading.Barrier(count)

        def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
            with self._lock:
                self.requests.append(request)
            self._barrier.wait(timeout=5)
            time.sleep(0.02)
            return {"answer": "same", "_tokens": 1}

    branches = ",\n".join(
        "    lambda: agent('same concurrent prompt', {'label': 'same', 'schema': {'answer': 'string'}})"
        for _ in range(count)
    )
    script = META + f"outs = await parallel([\n{branches}\n])\nreturn {{'count': len(outs)}}\n"
    runner = SlowChildRunner()

    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            script,
            store=store,
            run_id="concurrent_dupe",
            child_agent_runner=runner,
            limits=VMLimits(max_parallel=count),
        )
        assert res.ok, res.error
        assert res.value == {"count": count}

        cache = store.load_cache("concurrent_dupe")
        events = store.journal("concurrent_dupe")
        fingerprint = next(e["fingerprint"] for e in events if e["type"] == "agent_result")
        assert cache.get_prompt(fingerprint).value == {"answer": "same", "_tokens": 1}
        assert len(cache) == 1


def test_pipeline_prompt_agent_stage_allows_internal_dispatch_index_params():
    # pipeline() annotates each child frame with _pipeline_item_index /
    # _pipeline_stage_index; a prompt-agent stage must treat those as internal
    # scheduling metadata, not reject them as unsupported options.
    runner = FakeChildRunner({"answer": "ok", "_tokens": 1})
    script = META + (
        "outs = await pipeline([1, 2],\n"
        "    lambda prev, item, i: agent('summarize ' + str(item), "
        "{'label': 'x', 'schema': {'answer': 'string'}}),\n"
        ")\n"
        "return {'n': len(outs)}\n"
    )
    res = run_workflow_script(script, child_agent_runner=runner, limits=VMLimits(max_parallel=2))
    assert res.ok, res.error
    assert res.value == {"n": 2}


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


def test_prompt_agent_tools_round_trips_deduped_and_ordered_to_child_runner():
    runner = FakeChildRunner({"answer": "ok"})
    script = META + (
        'return await agent("summarize", {\n'
        '    "schema": {"answer": "string"},\n'
        '    "tools": ["read_file", "grep", "read_file"],\n'
        '})\n'
    )
    res = run_workflow_script(script, child_agent_runner=runner)
    assert res.ok, res.error
    assert len(runner.requests) == 1
    assert runner.requests[0].tools == ("read_file", "grep")


def test_prompt_agent_without_tools_leaves_request_tools_none():
    runner = FakeChildRunner()
    res = run_workflow_script(META + 'return await agent("write a plan")\n', child_agent_runner=runner)
    assert res.ok, res.error
    assert runner.requests[0].tools is None


def test_prompt_agent_empty_tools_list_is_most_restrictive_allowlist():
    # Pin (issue #101 review): an explicit empty `tools` list is a distinct,
    # deliberate "no tools at all" allowlist -- not equivalent to omitting
    # `tools`. It normalizes to `()`, reaches the child runner as such, and
    # mints a fingerprint distinct from the tools-less call (see DESIGN.md
    # §5.7.3). See also test_prompt_agent_malformed_tools_rejected_* for the
    # `[""]` case, which is rejected (an empty-*string* item), unlike `[]`.
    runner = FakeChildRunner({"answer": "ok"})
    script = META + (
        'return await agent("summarize", {\n'
        '    "schema": {"answer": "string"},\n'
        '    "tools": [],\n'
        '})\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(script, store=store, run_id="empty_tools_fp", child_agent_runner=runner)
        assert res.ok, res.error
        assert runner.requests[0].tools == ()

        empty_tools_fingerprint = next(
            e["fingerprint"] for e in store.journal("empty_tools_fp") if e["type"] == "agent_started"
        )

    runner_no_tools = FakeChildRunner({"answer": "ok"})
    script_no_tools = META + 'return await agent("summarize", {"schema": {"answer": "string"}})\n'
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            script_no_tools, store=store, run_id="no_tools_fp", child_agent_runner=runner_no_tools
        )
        assert res.ok, res.error
        assert runner_no_tools.requests[0].tools is None

        no_tools_fingerprint = next(
            e["fingerprint"] for e in store.journal("no_tools_fp") if e["type"] == "agent_started"
        )

    assert empty_tools_fingerprint != no_tools_fingerprint


def test_prompt_agent_tools_change_fingerprint():
    runner = FakeChildRunner({"answer": "ok"})
    script = META + (
        'await agent("same prompt", {"schema": {"answer": "string"}, "tools": ["a"]})\n'
        'await agent("same prompt", {"schema": {"answer": "string"}, "tools": ["b"]})\n'
        'await agent("same prompt", {"schema": {"answer": "string"}})\n'
        'return {}\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(script, store=store, run_id="tools_fp", child_agent_runner=runner)
        assert res.ok, res.error
        assert len(runner.requests) == 3
        fingerprints = [e["fingerprint"] for e in store.journal("tools_fp") if e["type"] == "agent_started"]
        assert len(fingerprints) == 3
        assert len(set(fingerprints)) == 3


def test_prompt_agent_fingerprint_without_tools_matches_pre_tools_baseline():
    # Regression pin (issue #101): omitting `tools` must fingerprint identically
    # to how it did before the option existed, so durable resume/replay caches
    # recorded before #101 still hit. Reconstructs the payload the v2
    # fingerprint used pre-#101 (no "tools" key at all, not even a null one) and
    # asserts today's fingerprint for a tools-less call still matches it.
    from hermes_workflows.script_store import canonical_hash
    from hermes_workflows.vm import _prompt_agent_cache_identity

    request = ChildAgentRequest(
        prompt="summarize", label="x", schema={"answer": "string"}, model="sonnet"
    )
    fingerprint, args_hash = _prompt_agent_cache_identity(request)

    pre_tools_payload = {
        "prompt": "summarize",
        "label": "x",
        "phase": None,
        "schema": {"answer": "string"},
        "model": "sonnet",
        "effort": None,
        "isolation": None,
        "context": {},
    }
    expected = "v2:" + canonical_hash({"kind": "agent(prompt,opts)", "version": 2, "request": pre_tools_payload})
    assert fingerprint == expected

    # _maybe_prompt_cache_hit hard-aborts (replay_mismatch) on any args_hash
    # divergence, so pin the pre-#101 args_hash derivation too -- not just the
    # fingerprint -- otherwise a regression there would brick durable resume
    # for every pre-#101 recorded prompt-agent call without a failing test.
    expected_args_hash = canonical_hash(
        {"method": "agent", "fingerprint": expected, "request": pre_tools_payload}
    )
    assert args_hash == expected_args_hash


def test_prompt_agent_malformed_tools_rejected_deterministically_not_retryable():
    runner = FakeChildRunner()
    for bad_tools in ('"not-a-list"', "{'nested': 1}", "[1, 2]", '[""]', "[True]"):
        script = META + (
            'try:\n'
            f'    await agent("summarize", {{"tools": {bad_tools}}})\n'
            'except CapabilityError as e:\n'
            '    return {"code": e.code, "retryable": e.retryable}\n'
            'return {"code": "missing"}\n'
        )
        res = run_workflow_script(script, child_agent_runner=runner)
        assert res.ok, res.error
        assert res.value == {"code": "bad_request", "retryable": False}
    assert runner.requests == []


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


# -- agentType option (issue #92) + file-based agent-type registry (issue #104) --

REVIEWER_DEFINITION = (
    "---\n"
    "name: reviewer\n"
    "description: Reviews code changes for correctness.\n"
    "model: opus\n"
    "effort: high\n"
    "---\n"
    "You are a meticulous code reviewer.\n"
)


def test_prompt_agent_bare_call_resolves_general_purpose_default_system_prompt():
    runner = FakeChildRunner({"answer": "ok"})
    res = run_workflow_script(META + 'return await agent("write a plan")\n', child_agent_runner=runner)
    assert res.ok, res.error
    assert len(runner.requests) == 1
    request = runner.requests[0]
    assert request.agent_type is None
    assert request.system_prompt
    assert request.model is None
    assert request.effort is None


def test_prompt_agent_agent_type_reaches_child_runner_with_type_set_and_composes_with_schema():
    # Regression test named in issue #92's acceptance criteria: agentType composes
    # with schema and is no longer rejected as an unsupported option.
    runner = FakeChildRunner({"answer": "ok"})
    with TemporaryDirectory() as tmp:
        root = Path(tmp) / "agents"
        root.mkdir()
        (root / "reviewer.md").write_text(REVIEWER_DEFINITION, encoding="utf-8")
        registry = AgentTypeRegistry(roots=[root])
        script = META + (
            'return await agent("review this diff", {\n'
            '    "agentType": "reviewer",\n'
            '    "schema": {"answer": "string"},\n'
            '})\n'
        )
        res = run_workflow_script(script, child_agent_runner=runner, agent_type_registry=registry)
        assert res.ok, res.error
        assert len(runner.requests) == 1
        request = runner.requests[0]
        assert request.agent_type == "reviewer"
        assert request.system_prompt == "You are a meticulous code reviewer."
        assert request.model == "opus"
        assert request.effort == "high"


def test_prompt_agent_explicit_opts_win_over_agent_type_registry_defaults():
    runner = FakeChildRunner({"answer": "ok"})
    with TemporaryDirectory() as tmp:
        root = Path(tmp) / "agents"
        root.mkdir()
        (root / "reviewer.md").write_text(REVIEWER_DEFINITION, encoding="utf-8")
        registry = AgentTypeRegistry(roots=[root])
        script = META + (
            'return await agent("review this diff", {\n'
            '    "agentType": "reviewer",\n'
            '    "model": "haiku",\n'
            '    "effort": "low",\n'
            '})\n'
        )
        res = run_workflow_script(script, child_agent_runner=runner, agent_type_registry=registry)
        assert res.ok, res.error
        request = runner.requests[0]
        assert request.model == "haiku"
        assert request.effort == "low"
        # system_prompt still resolves from the registry -- there is no opt to override it.
        assert request.system_prompt == "You are a meticulous code reviewer."


def test_prompt_agent_project_scope_shadows_user_scope_agent_type():
    runner = FakeChildRunner({"answer": "ok"})
    with TemporaryDirectory() as tmp:
        project_root = Path(tmp) / "project-agents"
        user_root = Path(tmp) / "user-agents"
        project_root.mkdir()
        user_root.mkdir()
        (project_root / "reviewer.md").write_text(
            "---\nname: reviewer\nmodel: opus\n---\nProject reviewer prompt.\n", encoding="utf-8"
        )
        (user_root / "reviewer.md").write_text(
            "---\nname: reviewer\nmodel: sonnet\n---\nUser reviewer prompt.\n", encoding="utf-8"
        )
        registry = AgentTypeRegistry(roots=[project_root, user_root])
        script = META + 'return await agent("review", {"agentType": "reviewer"})\n'
        res = run_workflow_script(script, child_agent_runner=runner, agent_type_registry=registry)
        assert res.ok, res.error
        assert runner.requests[0].model == "opus"
        assert runner.requests[0].system_prompt == "Project reviewer prompt."


def test_prompt_agent_unknown_agent_type_rejected_deterministically_not_retryable():
    runner = FakeChildRunner()
    script = META + (
        'try:\n'
        '    await agent("write a plan", {"agentType": "nonexistent-type"})\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code, "retryable": e.retryable}\n'
        'return {"code": "missing"}\n'
    )
    res = run_workflow_script(script, child_agent_runner=runner)
    assert res.ok, res.error
    assert res.value == {"code": "unknown_agent_type", "retryable": False}
    assert runner.requests == []


def test_prompt_agent_registry_denial_replays_deterministically_despite_registry_drift():
    # Regression for a review finding: unknown_agent_type/agent_type_invalid
    # depend on mutable on-disk AgentTypeRegistry state at resolve time,
    # unlike every other CapabilityDenied code (a pure function of the call's
    # own arguments and the run's own state). So they must be frozen into the
    # replay cache exactly like a caught runner_error -- otherwise a replay
    # whose registry root has since gained a definition file re-resolves live
    # and diverges from what the source run actually observed and handled.
    with TemporaryDirectory() as tmp:
        root = Path(tmp) / "agents"
        root.mkdir()
        script = META + (
            'try:\n'
            '    await agent("write a plan", {"agentType": "reviewer"})\n'
            '    branch = "resolved"\n'
            'except CapabilityError as e:\n'
            '    branch = "denied"\n'
            'return {"branch": branch}\n'
        )
        with TemporaryDirectory() as tmp2:
            store = ScriptRunStore(Path(tmp2) / "runs")
            src_runner = FakeChildRunner({"answer": "ok"})
            rec = run_workflow_script(
                script,
                store=store,
                run_id="src",
                child_agent_runner=src_runner,
                agent_type_registry=AgentTypeRegistry(roots=[root]),
            )
            assert rec.ok, rec.error
            assert rec.value == {"branch": "denied"}
            assert src_runner.requests == []

            # An unrelated later deploy adds a "reviewer" definition to the
            # same root -- this must not retroactively change a *replay* of
            # the earlier run.
            (root / "reviewer.md").write_text(REVIEWER_DEFINITION, encoding="utf-8")

            replay_runner = FakeChildRunner({"answer": "ok"})
            rep = run_workflow_script(
                script,
                store=store,
                run_id="replay",
                replay_from="src",
                child_agent_runner=replay_runner,
                agent_type_registry=AgentTypeRegistry(roots=[root]),
            )
    assert rep.ok, rep.error
    assert rep.value == {"branch": "denied"}
    # Serving the recorded denial must never dispatch a live child agent.
    assert replay_runner.requests == []


def test_prompt_agent_agent_type_path_traversal_rejected_deterministically():
    runner = FakeChildRunner()
    script = META + (
        'try:\n'
        '    await agent("write a plan", {"agentType": "../../etc/passwd"})\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code, "retryable": e.retryable}\n'
        'return {"code": "missing"}\n'
    )
    res = run_workflow_script(script, child_agent_runner=runner)
    assert res.ok, res.error
    assert res.value == {"code": "agent_type_invalid", "retryable": False}
    assert runner.requests == []


def test_prompt_agent_malformed_agent_type_rejected_deterministically_not_retryable():
    runner = FakeChildRunner()
    for bad_agent_type in ('""', "123", "True"):
        script = META + (
            'try:\n'
            f'    await agent("write a plan", {{"agentType": {bad_agent_type}}})\n'
            'except CapabilityError as e:\n'
            '    return {"code": e.code, "retryable": e.retryable}\n'
            'return {"code": "missing"}\n'
        )
        res = run_workflow_script(script, child_agent_runner=runner)
        assert res.ok, res.error
        assert res.value == {"code": "bad_request", "retryable": False}
    assert runner.requests == []

    # agentType: None is the same as omitting it -- resolves general-purpose,
    # not a malformed-option denial.
    ok_runner = FakeChildRunner({"answer": "ok"})
    res = run_workflow_script(
        META + 'return await agent("write a plan", {"agentType": None})\n', child_agent_runner=ok_runner
    )
    assert res.ok, res.error
    assert ok_runner.requests[0].agent_type is None


def test_prompt_agent_agent_type_changes_fingerprint():
    runner = FakeChildRunner({"answer": "ok"})
    with TemporaryDirectory() as tmp:
        root = Path(tmp) / "agents"
        root.mkdir()
        (root / "reviewer.md").write_text(REVIEWER_DEFINITION, encoding="utf-8")
        (root / "explainer.md").write_text(
            "---\nname: explainer\n---\nExplain things simply.\n", encoding="utf-8"
        )
        registry = AgentTypeRegistry(roots=[root])
        script = META + (
            'await agent("same prompt", {"schema": {"answer": "string"}, "agentType": "reviewer"})\n'
            'await agent("same prompt", {"schema": {"answer": "string"}, "agentType": "explainer"})\n'
            'await agent("same prompt", {"schema": {"answer": "string"}})\n'
            'return {}\n'
        )
        with TemporaryDirectory() as tmp2:
            store = ScriptRunStore(Path(tmp2) / "runs")
            res = run_workflow_script(
                script, store=store, run_id="agent_type_fp", child_agent_runner=runner, agent_type_registry=registry
            )
            assert res.ok, res.error
            assert len(runner.requests) == 3
            fingerprints = [
                e["fingerprint"] for e in store.journal("agent_type_fp") if e["type"] == "agent_started"
            ]
            assert len(fingerprints) == 3
            assert len(set(fingerprints)) == 3


def test_prompt_agent_fingerprint_without_agent_type_matches_pre_agent_type_baseline():
    # Regression pin (issue #92/#104): omitting `agentType` must fingerprint
    # identically to how it did before the option existed, mirroring the #101
    # `tools` baseline pin -- a bare call resolving the built-in general-purpose
    # default must not perturb the fingerprint or invalidate pre-existing
    # durable resume/replay caches.
    from hermes_workflows.script_store import canonical_hash
    from hermes_workflows.vm import _prompt_agent_cache_identity

    request = ChildAgentRequest(
        prompt="summarize", label="x", schema={"answer": "string"}, model="sonnet"
    )
    fingerprint, args_hash = _prompt_agent_cache_identity(request)

    pre_agent_type_payload = {
        "prompt": "summarize",
        "label": "x",
        "phase": None,
        "schema": {"answer": "string"},
        "model": "sonnet",
        "effort": None,
        "isolation": None,
        "context": {},
    }
    expected = "v2:" + canonical_hash(
        {"kind": "agent(prompt,opts)", "version": 2, "request": pre_agent_type_payload}
    )
    assert fingerprint == expected

    expected_args_hash = canonical_hash(
        {"method": "agent", "fingerprint": expected, "request": pre_agent_type_payload}
    )
    assert args_hash == expected_args_hash


def test_prompt_agent_agent_type_without_registry_configured_still_resolves_general_purpose():
    # No agent_type_registry supplied at all -- the broker defaults to a
    # registry with no roots; the built-in general-purpose default must still
    # resolve for a bare call, and an explicit unknown type must still deny.
    runner = FakeChildRunner({"answer": "ok"})
    res = run_workflow_script(META + 'return await agent("write a plan")\n', child_agent_runner=runner)
    assert res.ok, res.error
    assert runner.requests[0].system_prompt
