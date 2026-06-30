"""Generic workflow-script capability registry tests (issue #29)."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from hermes_workflows import (
    CapabilityPolicy,
    CapabilityRegistry,
    VMLimits,
    run_workflow_script,
)
from hermes_workflows.vm import CapabilityBroker
from hermes_workflows import rpc
from hermes_workflows.agents import StubAgentRunner
from hermes_workflows.controls import InMemoryControlStore, pause_run, stop_task
from hermes_workflows.script_store import ScriptRunStore

META = 'meta = {"name": "cap-demo", "description": "d"}\n'


def _registry() -> CapabilityRegistry:
    registry = CapabilityRegistry()
    registry.register(
        "tools.echo",
        lambda ctx: {"ok": True, "output": {"seen": ctx["input"], "run": ctx["run"]}},
        description="test read-only echo capability",
    )
    registry.register(
        "tools.write",
        lambda ctx: {"ok": True, "summary": "wrote"},
        side_effect_class="external_write",
        description="test write capability",
    )
    registry.register(
        "tools.noisy",
        lambda ctx: {"ok": True, "stdout": "x" * 20, "stderr": "y" * 20},
        description="test bounded stream capture",
    )
    registry.register(
        "tools.bad-json",
        lambda ctx: {"ok": True, "output": object()},
        description="test JSON-safe result enforcement",
    )
    return registry


def test_script_can_call_registered_read_only_capability():
    script = META + (
        'result = await capability("tools.echo", {"subject": args["subject"]}, label="inventory")\n'
        'return {"seen": result["output"]["seen"], "run": result["output"]["run"]}\n'
    )
    res = run_workflow_script(script, args={"subject": "issue-29"}, capability_registry=_registry())
    assert res.ok, res.error
    assert res.value["seen"] == {"subject": "issue-29"}
    assert res.value["run"]["idempotency_root"].startswith("mem_")
    assert res.value["run"]["call_id"] == 1
    assert res.value["run"]["idempotency_key"].endswith(":1")
    event = next(call for call in res.calls if call["method"] == "capability")
    assert event["capability"] == "tools.echo"
    assert event["label"] == "inventory"
    assert "params" not in event


def test_control_pause_blocks_script_capability_dispatch():
    calls = []
    registry = CapabilityRegistry()
    registry.register("tools.echo", lambda ctx: calls.append(ctx) or {"ok": True})
    controls = InMemoryControlStore()
    pause_run(controls, "wfs_pause_cap", reason="hold")
    script = META + 'return await capability("tools.echo", {}, label="inventory")\n'

    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            script,
            store=store,
            run_id="wfs_pause_cap",
            capability_registry=registry,
            control_store=controls,
        )
        persisted = store.load_run("wfs_pause_cap")

    assert res.paused is True
    assert res.error["code"] == "run_paused"
    assert persisted.status == "paused"
    assert calls == []


def test_control_task_stop_blocks_matching_script_child_label():
    calls = []
    registry = CapabilityRegistry()
    registry.register("tools.echo", lambda ctx: calls.append(ctx) or {"ok": True})
    controls = InMemoryControlStore()
    stop_task(controls, "wfs_task_cap", "inventory", reason="skip this call")
    script = META + 'return await capability("tools.echo", {}, label="inventory")\n'

    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            script,
            store=store,
            run_id="wfs_task_cap",
            capability_registry=registry,
            control_store=controls,
        )
        persisted = store.load_run("wfs_task_cap")

    assert res.ok is False
    assert res.error["code"] == "task_stopped"
    assert persisted.status == "failed"
    assert calls == []


def test_unregistered_capability_fails_closed():
    script = META + (
        'try:\n'
        '    await capability("tools.missing", {})\n'
        '    return {"bad": True}\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code}\n'
    )
    res = run_workflow_script(script, capability_registry=_registry())
    assert res.ok, res.error
    assert res.value == {"code": "unknown_capability"}


def test_missing_registry_fails_closed():
    script = META + (
        'try:\n'
        '    await capability("tools.echo", {})\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code}\n'
    )
    res = run_workflow_script(script)
    assert res.ok, res.error
    assert res.value == {"code": "capability_unavailable"}


def test_side_effect_policy_and_approval_are_enforced():
    script = META + (
        'try:\n'
        '    await capability("tools.write", {}, approval_id=args.get("approval"))\n'
        '    return {"ok": True}\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code}\n'
    )
    registry = _registry()

    default_denied = run_workflow_script(script, capability_registry=registry, args={})
    assert default_denied.ok and default_denied.value == {"code": "capability_side_effect_denied"}

    needs_approval = run_workflow_script(
        script,
        capability_registry=registry,
        capability_policy=CapabilityPolicy(allowed_side_effect_classes=("read_only", "external_write")),
        args={},
    )
    assert needs_approval.ok and needs_approval.value == {"code": "capability_approval_required"}

    approved = run_workflow_script(
        script,
        capability_registry=registry,
        capability_policy=CapabilityPolicy(
            allowed_side_effect_classes=("read_only", "external_write"),
            approved_approval_ids=("approve-1",),
        ),
        args={"approval": "approve-1"},
    )
    assert approved.ok and approved.value == {"ok": True}


def test_capability_result_streams_are_bounded():
    script = META + 'return await capability("tools.noisy", {})\n'
    res = run_workflow_script(
        script,
        capability_registry=_registry(),
        capability_policy=CapabilityPolicy(max_stream_bytes=5),
    )
    assert res.ok, res.error
    assert res.value["stdout"] == "xxxxx"
    assert res.value["stdout_truncated"] is True
    assert res.value["stderr"] == "yyyyy"
    assert res.value["stderr_truncated"] is True


def test_capability_output_schema_is_enforced():
    script = META + (
        'try:\n'
        '    await capability("tools.echo", {}, schema={"missing": "string"})\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code}\n'
    )
    res = run_workflow_script(script, capability_registry=_registry())
    assert res.ok, res.error
    assert res.value == {"code": "schema"}


def test_capability_input_rejects_credential_shaped_payload():
    script = META + (
        'try:\n'
        '    await capability("tools.echo", {"token": "do-not-pass"})\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code}\n'
    )
    res = run_workflow_script(script, capability_registry=_registry())
    assert res.ok, res.error
    assert res.value == {"code": "capability_credential"}


def test_capability_metadata_rejects_and_redacts_credential_shaped_label():
    script = META + (
        'try:\n'
        '    await capability("tools.echo", {}, label="password=hunter2")\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code}\n'
    )
    res = run_workflow_script(script, capability_registry=_registry())
    assert res.ok, res.error
    assert res.value == {"code": "capability_credential"}
    serialized = json.dumps(res.as_dict())
    assert "hunter2" not in serialized
    event = next(call for call in res.calls if call["method"] == "capability")
    assert event["label"] == "[REDACTED]"


def test_capability_handler_exceptions_are_sanitized():
    registry = _registry()

    def boom(ctx):
        raise RuntimeError("token=SECRET123 password=hunter2")

    registry.register("tools.raise", boom)
    script = META + (
        'try:\n'
        '    await capability("tools.raise", {})\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code, "message": str(e)}\n'
    )
    res = run_workflow_script(script, capability_registry=registry)
    assert res.ok, res.error
    assert res.value["code"] == "capability_handler_error"
    assert "RuntimeError" in res.value["message"]
    assert "SECRET123" not in res.value["message"]
    assert "hunter2" not in res.value["message"]


def test_malformed_capability_name_is_bad_request():
    script = META + (
        'try:\n'
        '    await capability("bad name", {})\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code}\n'
    )
    res = run_workflow_script(script, capability_registry=_registry())
    assert res.ok, res.error
    assert res.value == {"code": "bad_request"}


def test_capability_result_must_be_json_safe_before_rpc_return():
    script = META + (
        'try:\n'
        '    await capability("tools.bad-json", {})\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code}\n'
    )
    res = run_workflow_script(script, capability_registry=_registry())
    assert res.ok, res.error
    assert res.value == {"code": "capability_result_invalid"}


def test_allowed_names_policy_fails_closed():
    script = META + (
        'try:\n'
        '    await capability("tools.echo", {})\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code}\n'
    )
    res = run_workflow_script(
        script,
        capability_registry=_registry(),
        capability_policy=CapabilityPolicy(allowed_names=("tools.noisy",)),
    )
    assert res.ok, res.error
    assert res.value == {"code": "capability_denied"}


def test_capability_result_total_size_fails_closed():
    registry = _registry()
    registry.register("tools.big", lambda ctx: {"ok": True, "output": "z" * 1000})
    script = META + (
        'try:\n'
        '    await capability("tools.big", {})\n'
        'except CapabilityError as e:\n'
        '    return {"code": e.code}\n'
    )
    res = run_workflow_script(
        script,
        capability_registry=registry,
        capability_policy=CapabilityPolicy(max_result_bytes=256),
    )
    assert res.ok, res.error
    assert res.value == {"code": "capability_result_too_large"}


def test_non_replayable_mutating_capability_fails_closed_on_replay_miss():
    calls = []
    registry = CapabilityRegistry()
    registry.register(
        "tools.write-once",
        lambda ctx: calls.append(ctx["run"]) or {"ok": True},
        side_effect_class="external_write",
    )
    policy = CapabilityPolicy(
        allowed_side_effect_classes=("read_only", "external_write"),
        approved_approval_ids=("ok",),
    )
    script = META + 'await capability("tools.write-once", {}, approval_id="ok")\nreturn {"ok": True}\n'

    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        rec = run_workflow_script(script, store=store, run_id="src", capability_registry=registry, capability_policy=policy)
        assert rec.ok, rec.error
        rep = run_workflow_script(
            script,
            store=store,
            run_id="replay",
            replay_from="src",
            capability_registry=registry,
            capability_policy=policy,
        )

    assert len(calls) == 1
    assert rep.ok is False
    assert rep.error is not None
    assert "non-replayable capability" in rep.error["message"]
    assert rep.calls[-1]["error"] == "capability_replay_unsafe"


def test_replayable_capability_is_cached_and_not_redispatched():
    calls = []
    registry = CapabilityRegistry()
    registry.register(
        "tools.cached",
        lambda ctx: calls.append(ctx["run"]) or {"ok": True, "run": ctx["run"]},
        replayable=True,
    )
    script = META + 'return await capability("tools.cached", {"x": 1})\n'

    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        rec = run_workflow_script(script, store=store, run_id="src", capability_registry=registry)
        assert rec.ok, rec.error
        rep = run_workflow_script(
            script,
            store=store,
            run_id="replay",
            replay_from="src",
            capability_registry=registry,
        )

    assert len(calls) == 1
    assert rep.ok, rep.error
    assert rep.replayed_calls == 1
    assert rep.value["run"]["idempotency_key"] == "src:1"


def test_broker_enforces_capability_limit_softly():
    registry = _registry()
    broker = CapabilityBroker(
        StubAgentRunner(),
        VMLimits(max_capability_calls=1),
        capability_registry=registry,
    )
    frame = {"t": rpc.T_CALL, "method": "capability", "params": {"name": "tools.echo", "input": {}}}
    first = broker.handle({**frame, "id": 1})
    second = broker.handle({**frame, "id": 2})
    assert first["ok"] is True
    assert second["ok"] is False and second["error"]["code"] == "limit_capability"
    assert broker.should_abort is False
