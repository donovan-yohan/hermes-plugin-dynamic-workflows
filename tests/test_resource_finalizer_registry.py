"""Tests for backend-neutral resource finalizer adapter dispatch."""

import json

from hermes_workflows import ResourceFinalizerRegistry, UnknownResourceFinalizerAction
from hermes_workflows.resources import WorkflowResource, run_resource_finalizers


def _resource(action="test.resource.cleanup", *, policy="required", resource_id="owned-test-resource"):
    return WorkflowResource.from_dict(
        {
            "id": resource_id,
            "kind": "test.resource",
            "handle": {"resource_id": "safe-resource-id"},
            "finalizers": [
                {
                    "id": "cleanup",
                    "action": action,
                    "policy": policy,
                    "when": ["success"],
                }
            ],
        }
    )


def test_resource_finalizer_registry_dispatches_by_action():
    calls = []

    def cleanup(context):
        calls.append((context["resource"]["id"], context["finalizer"]["action"], context["trigger"]))
        return {"ok": True, "summary": "resource cleaned", "evidence": [{"kind": "test", "status": "clean"}]}

    registry = ResourceFinalizerRegistry({"test.resource.cleanup": cleanup})
    results = run_resource_finalizers(
        [_resource()],
        trigger="success",
        runner=registry,
        run_id="loop.registry.dispatch",
        loop_name="ticket_loop",
    )

    assert registry.actions() == ("test.resource.cleanup",)
    assert calls == [("owned-test-resource", "test.resource.cleanup", "success")]
    assert len(results) == 1
    assert results[0].status == "succeeded"
    assert results[0].summary == "resource cleaned"


def test_resource_finalizer_registry_rejects_duplicate_and_bad_actions():
    registry = ResourceFinalizerRegistry()
    registry.register("test.resource.cleanup", lambda context: {"ok": True})

    try:
        registry.register("test.resource.cleanup", lambda context: {"ok": True})
    except ValueError as exc:
        assert "already registered" in str(exc)
    else:  # pragma: no cover - assertion clarity
        raise AssertionError("expected duplicate action to be rejected")

    registry.register("test.resource.cleanup", lambda context: {"ok": True, "summary": "replaced"}, replace=True)
    assert registry.handler_for("test.resource.cleanup") is not None

    try:
        registry.register("bad action with spaces", lambda context: {"ok": True})
    except ValueError as exc:
        assert "identifier-safe" in str(exc)
    else:  # pragma: no cover - assertion clarity
        raise AssertionError("expected malformed action to be rejected")

    try:
        registry.register("test.not_callable", object())  # type: ignore[arg-type]
    except TypeError as exc:
        assert "callable" in str(exc)
    else:  # pragma: no cover - assertion clarity
        raise AssertionError("expected non-callable handler to be rejected")


def test_resource_finalizer_registry_unknown_action_fails_closed_and_redacts():
    registry = ResourceFinalizerRegistry()
    results = run_resource_finalizers(
        [_resource("ath.listener.retire")],
        trigger="success",
        runner=registry,
        run_id="loop.registry.unknown",
        loop_name="ticket_loop",
    )
    dumped = json.dumps([result.to_dict() for result in results])

    assert len(results) == 1
    assert results[0].status == "failed"
    assert results[0].policy == "required"
    assert results[0].error is not None
    assert "no resource finalizer adapter registered" in results[0].error
    assert "ath.listener.retire" in dumped


def test_resource_finalizer_registry_can_be_called_directly_for_missing_action():
    registry = ResourceFinalizerRegistry()

    try:
        registry({"finalizer": {"id": "cleanup"}})
    except UnknownResourceFinalizerAction as exc:
        assert "missing action" in str(exc)
    else:  # pragma: no cover - assertion clarity
        raise AssertionError("expected missing action to fail closed")


def test_resource_finalizer_registry_keeps_backend_specific_code_out_of_core():
    def relay_close(context):
        assert context["finalizer"]["action"] == "relay.session.close"
        return {"ok": True, "summary": "relay adapter called"}

    def ath_retire(context):
        assert context["finalizer"]["action"] == "ath.listener.retire"
        return {"ok": True, "summary": "ath adapter called"}

    registry = ResourceFinalizerRegistry()
    registry.register("relay.session.close", relay_close).register("ath.listener.retire", ath_retire)

    relay_resource = _resource("relay.session.close", resource_id="relay-session-1")
    ath_resource = _resource("ath.listener.retire", resource_id="ath-listener-1")
    results = run_resource_finalizers(
        [relay_resource, ath_resource],
        trigger="success",
        runner=registry,
        run_id="loop.registry.multi",
        loop_name="ticket_loop",
    )

    assert [result.summary for result in results] == ["relay adapter called", "ath adapter called"]
