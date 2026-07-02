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
    CapabilityBroker,
    ChildAgentRequest,
    ScriptRunStore,
    VMLimits,
    run_workflow_script,
)

META = 'meta = {"name": "prompt-agent", "description": "d"}\n'


class FakeChildRunner:
    def __init__(
        self,
        output: dict[str, Any] | None = None,
        *,
        child_visible_context_keys: frozenset[str] = frozenset(),
    ) -> None:
        self.output = output or {"answer": "ok", "_tokens": 3}
        self.requests: list[ChildAgentRequest] = []
        # Issue #102: declared allowlist for the child-visible-context quarantine.
        # Defaults empty (fail-closed), matching an undeclared runner; individual
        # tests opt a runner into specific keys to keep pre-existing context-echo
        # assertions meaningful.
        self.child_visible_context_keys = child_visible_context_keys

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
    def __init__(
        self,
        outputs: list[Any],
        *,
        child_visible_context_keys: frozenset[str] = frozenset(),
    ) -> None:
        self.outputs = list(outputs)
        self.requests: list[ChildAgentRequest] = []
        self.child_visible_context_keys = child_visible_context_keys

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
    runner = FakeChildRunner(
        {"answer": "ok", "token": secret, "nested": {"detail": secret}, "_tokens": 3},
        child_visible_context_keys=frozenset({"pr"}),
    )
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
    runner = SequenceChildRunner(
        [
            {"answer": 7},
            {"answer": "ok", "_tokens": 5},
        ],
        child_visible_context_keys=frozenset({"topic"}),
    )
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


# --------------------------------------------------------------------------- #
# Host-declared child-visible-context quarantine (issue #102).
# --------------------------------------------------------------------------- #
#
# The runner seam receives a free-form ``context`` dict; the inverse of
# deepagents' ``private_state_keys`` denylist is an explicit, host-declared
# *allowlist* -- ``child_visible_context_keys: frozenset[str]`` -- that the
# parent broker (vm.py) enforces before a ``ChildAgentRequest`` crosses the
# runner boundary. Undeclared defaults to the empty allowlist (fail-closed).


class SpyChildRunner:
    """A :class:`ChildAgentRunner` test double that records exactly what it received."""

    def __init__(self, child_visible_context_keys: frozenset[str] | None = None) -> None:
        self.requests: list[ChildAgentRequest] = []
        # Deliberately omit the attribute entirely when None, to prove the
        # broker's fail-closed default for an *undeclared* spec (not merely an
        # empty declared one -- both must behave identically).
        if child_visible_context_keys is not None:
            self.child_visible_context_keys = child_visible_context_keys

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        self.requests.append(request)
        return {"answer": "ok"}


def test_prompt_agent_context_filtered_to_runner_declared_allowlist():
    runner = SpyChildRunner(frozenset({"pr"}))
    script = META + (
        'return await agent("summarize", {\n'
        '    "schema": {"answer": "string"},\n'
        '    "context": {"pr": 70, "secret_token": "leak-me", "chat_history": ["hi"]},\n'
        '})\n'
    )
    res = run_workflow_script(script, child_agent_runner=runner)

    assert res.ok, res.error
    assert len(runner.requests) == 1
    assert runner.requests[0].context == {"pr": 70}


def test_prompt_agent_undeclared_runner_gets_no_context_at_all():
    # No child_visible_context_keys attribute at all -- the fail-closed default.
    runner = SpyChildRunner(None)
    assert not hasattr(runner, "child_visible_context_keys")
    script = META + (
        'return await agent("summarize", {"context": {"pr": 70}})\n'
    )
    res = run_workflow_script(script, child_agent_runner=runner)

    assert res.ok, res.error
    assert runner.requests[0].context == {}


def test_prompt_agent_explicit_empty_allowlist_behaves_like_undeclared():
    runner = SpyChildRunner(frozenset())
    script = META + 'return await agent("summarize", {"context": {"pr": 70}})\n'
    res = run_workflow_script(script, child_agent_runner=runner)

    assert res.ok, res.error
    assert runner.requests[0].context == {}


def test_prompt_agent_str_declaration_behaves_like_undeclared_not_char_set():
    # A bare string is the most plausible typo for a single-element allowlist
    # (e.g. ``"pr"`` meant to be ``{"pr"}``). Python iterates a str character
    # by character, so an unguarded ``frozenset(declared)`` would silently
    # yield frozenset({"p", "r"}) -- dropping the intended key while letting
    # any single-character context key pass through. Must fail closed instead.
    runner = SpyChildRunner("pr")
    script = META + (
        'return await agent("summarize", {"context": {"pr": 70, "p": "leak"}})\n'
    )
    res = run_workflow_script(script, child_agent_runner=runner)

    assert res.ok, res.error
    assert runner.requests[0].context == {}


def test_prompt_agent_non_iterable_declaration_fails_closed_instead_of_crashing():
    # A malformed declaration (e.g. an int) must not propagate a TypeError up
    # through handle() as a runner_error -- it fails closed to an empty
    # allowlist, per DESIGN.md Sec5.7.5.
    runner = SpyChildRunner(42)
    script = META + 'return await agent("summarize", {"context": {"pr": 70}})\n'
    res = run_workflow_script(script, child_agent_runner=runner)

    assert res.ok, res.error
    assert runner.requests[0].context == {}


def test_filter_child_visible_context_tolerates_non_string_context_keys():
    # ``_prompt_agent_request`` only validates that ``context`` is a dict, so a
    # mixed-key dict can reach the filter. Non-string keys can never match the
    # str allowlist -- they must be dropped (journaled via repr) without the
    # mixed-type ``sorted()`` TypeError crashing the run.
    from hermes_workflows.agents import filter_child_visible_context

    runner = SpyChildRunner(frozenset({"pr"}))
    filtered, dropped = filter_child_visible_context(
        runner, {1: "x", "pr": 70, "secret": "y"}
    )

    assert filtered == {"pr": 70}
    assert dropped == ("1", "secret")


def test_prompt_agent_context_quarantine_journals_dropped_key_names_not_values():
    runner = SpyChildRunner(frozenset({"pr"}))
    script = META + (
        'return await agent("summarize", {\n'
        '    "context": {"pr": 70, "secret_token": "leak-me-please"},\n'
        '})\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            script, store=store, run_id="quarantine_run", child_agent_runner=runner
        )
        assert res.ok, res.error

        journal = store.journal("quarantine_run")
        started = next(event for event in journal if event["type"] == "agent_started")
        assert started["dropped_context_keys"] == ["secret_token"]

        journal_path = Path(tmp) / "runs" / "quarantine_run" / "journal.jsonl"
        journal_text = journal_path.read_text(encoding="utf-8")
        assert "leak-me-please" not in journal_text
        run_text = (Path(tmp) / "runs" / "quarantine_run" / "run.json").read_text(encoding="utf-8")
        assert "leak-me-please" not in run_text


def test_prompt_agent_context_quarantine_redacts_credential_marker_in_dropped_key_name():
    # Context *key names* are script-chosen strings, same class of input as
    # label/phase; a dynamically built context whose key itself carries a
    # credential marker must be redacted before it reaches the journal, not
    # journaled verbatim.
    runner = SpyChildRunner(frozenset({"pr"}))
    script = META + (
        'return await agent("summarize", {\n'
        '    "context": {"pr": 70, "token=ghp_leaked_in_key_name": "x"},\n'
        '})\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            script, store=store, run_id="quarantine_key_redact", child_agent_runner=runner
        )
        assert res.ok, res.error

        journal = store.journal("quarantine_key_redact")
        started = next(event for event in journal if event["type"] == "agent_started")
        assert started["dropped_context_keys"] == [REDACTED]

        journal_path = Path(tmp) / "runs" / "quarantine_key_redact" / "journal.jsonl"
        journal_text = journal_path.read_text(encoding="utf-8")
        assert "token=ghp_leaked_in_key_name" not in journal_text


def test_prompt_agent_context_quarantine_no_journal_note_when_nothing_dropped():
    runner = SpyChildRunner(frozenset({"pr"}))
    script = META + 'return await agent("summarize", {"context": {"pr": 70}})\n'
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            script, store=store, run_id="no_drop_run", child_agent_runner=runner
        )
        assert res.ok, res.error
        started = next(
            event for event in store.journal("no_drop_run") if event["type"] == "agent_started"
        )
        assert "dropped_context_keys" not in started


def test_prompt_agent_context_quarantine_fingerprint_is_pre_filter_and_runner_independent():
    # Design decision (issue #102, DESIGN.md §5.7.5): the v2: fingerprint is
    # minted over the pre-filter, script-supplied context so cache/replay
    # identity never depends on which runner happens to be configured, or on
    # what allowlist it declares. Two runners with wildly different declared
    # allowlists dispatching the *same* prompt/opts call must mint the same
    # fingerprint.
    script = META + (
        'return await agent("summarize", {\n'
        '    "schema": {"answer": "string"},\n'
        '    "context": {"pr": 70, "secret_token": "leak-me"},\n'
        '})\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        wide_runner = SpyChildRunner(frozenset({"pr", "secret_token"}))
        res_wide = run_workflow_script(
            script, store=store, run_id="wide", child_agent_runner=wide_runner
        )
        assert res_wide.ok, res_wide.error
        assert wide_runner.requests[0].context == {"pr": 70, "secret_token": "leak-me"}

        narrow_runner = SpyChildRunner(frozenset())
        res_narrow = run_workflow_script(
            script, store=store, run_id="narrow", child_agent_runner=narrow_runner
        )
        assert res_narrow.ok, res_narrow.error
        assert narrow_runner.requests[0].context == {}

        wide_fp = next(
            e["fingerprint"] for e in store.journal("wide") if e["type"] == "agent_started"
        )
        narrow_fp = next(
            e["fingerprint"] for e in store.journal("narrow") if e["type"] == "agent_started"
        )
        assert wide_fp == narrow_fp


def test_prompt_agent_context_quarantine_full_pass_through_fingerprint_matches_pre_102_baseline():
    # A runner whose allowlist happens to cover every key the script provided
    # sees the context unchanged, and mints the exact same fingerprint a
    # pre-#102 build would have for the identical call -- the filter never
    # rewrites the fingerprint payload, only what crosses the runner boundary.
    from hermes_workflows.script_store import canonical_hash
    from hermes_workflows.vm import _prompt_agent_cache_identity

    request = ChildAgentRequest(prompt="summarize", schema={"answer": "string"}, context={"pr": 70})
    pre_filter_fingerprint, _ = _prompt_agent_cache_identity(request)

    runner = SpyChildRunner(frozenset({"pr"}))
    script = META + (
        'return await agent("summarize", {"schema": {"answer": "string"}, "context": {"pr": 70}})\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(script, store=store, run_id="full_pass", child_agent_runner=runner)
        assert res.ok, res.error
        assert runner.requests[0].context == {"pr": 70}
        started_fp = next(
            e["fingerprint"] for e in store.journal("full_pass") if e["type"] == "agent_started"
        )
        assert started_fp == pre_filter_fingerprint
        assert canonical_hash({"kind": "agent(prompt,opts)", "version": 2, "request": {
            "prompt": "summarize",
            "label": None,
            "phase": None,
            "schema": {"answer": "string"},
            "model": None,
            "effort": None,
            "isolation": None,
            "context": {"pr": 70},
        }}) == pre_filter_fingerprint.removeprefix("v2:")
