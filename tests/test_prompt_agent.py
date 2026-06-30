"""Tests for script ``agent(prompt, opts)`` prompt subagent calls (issue #70)."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from hermes_workflows import REDACTED, ChildAgentRequest, ScriptRunStore, VMLimits, run_workflow_script

META = 'meta = {"name": "prompt-agent", "description": "d"}\n'


class FakeChildRunner:
    def __init__(self, output: dict[str, Any] | None = None) -> None:
        self.output = output or {"answer": "ok", "_tokens": 3}
        self.requests: list[ChildAgentRequest] = []

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        self.requests.append(request)
        return dict(self.output)


class SequenceChildRunner:
    def __init__(self, outputs: list[Any]) -> None:
        self.outputs = list(outputs)
        self.requests: list[ChildAgentRequest] = []

    def __call__(self, request: ChildAgentRequest) -> Any:
        self.requests.append(request)
        if self.outputs:
            return self.outputs.pop(0)
        return {}


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
        call = next(event for event in journal if event["type"] == "call" and event["method"] == "agent")
        assert call["label"] == "summary"
        assert call["phase"] == "analysis"
        assert "prompt" not in call
        assert "params" not in call
        assert store.load_run("prompt_run").value == expected
        assert store.load_cache("prompt_run").get(1).value == expected
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
