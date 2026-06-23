"""Tests for backend-neutral resource finalizer adapter dispatch."""

import json
import unittest

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


class ResourceFinalizerRegistryTests(unittest.TestCase):
    def test_dispatches_by_action(self):
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

        self.assertEqual(registry.actions(), ("test.resource.cleanup",))
        self.assertEqual(calls, [("owned-test-resource", "test.resource.cleanup", "success")])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, "succeeded")
        self.assertEqual(results[0].summary, "resource cleaned")

    def test_rejects_duplicate_and_bad_actions(self):
        registry = ResourceFinalizerRegistry()
        registry.register("test.resource.cleanup", lambda context: {"ok": True})

        with self.assertRaisesRegex(ValueError, "already registered"):
            registry.register("test.resource.cleanup", lambda context: {"ok": True})

        registry.register("test.resource.cleanup", lambda context: {"ok": True, "summary": "replaced"}, replace=True)
        self.assertIsNotNone(registry.handler_for("test.resource.cleanup"))

        with self.assertRaisesRegex(ValueError, "identifier-safe"):
            registry.register("bad action with spaces", lambda context: {"ok": True})

        with self.assertRaisesRegex(TypeError, "callable"):
            registry.register("test.not_callable", object())  # type: ignore[arg-type]

    def test_unknown_action_fails_closed_and_redacts(self):
        registry = ResourceFinalizerRegistry()
        results = run_resource_finalizers(
            [_resource("ath.listener.retire")],
            trigger="success",
            runner=registry,
            run_id="loop.registry.unknown",
            loop_name="ticket_loop",
        )
        dumped = json.dumps([result.to_dict() for result in results])

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, "failed")
        self.assertEqual(results[0].policy, "required")
        self.assertIsNotNone(results[0].error)
        self.assertIn("no resource finalizer adapter registered", results[0].error or "")
        self.assertIn("ath.listener.retire", dumped)

    def test_can_be_called_directly_for_missing_action(self):
        registry = ResourceFinalizerRegistry()

        with self.assertRaisesRegex(UnknownResourceFinalizerAction, "missing action"):
            registry({"finalizer": {"id": "cleanup"}})

    def test_none_context_fails_closed_as_missing_action(self):
        registry = ResourceFinalizerRegistry()

        with self.assertRaisesRegex(UnknownResourceFinalizerAction, "missing action"):
            registry(None)  # type: ignore[arg-type]

    def test_keeps_backend_specific_code_out_of_core(self):
        def relay_close(context):
            self.assertEqual(context["finalizer"]["action"], "relay.session.close")
            return {"ok": True, "summary": "relay adapter called"}

        def ath_retire(context):
            self.assertEqual(context["finalizer"]["action"], "ath.listener.retire")
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

        self.assertEqual([result.summary for result in results], ["relay adapter called", "ath adapter called"])


if __name__ == "__main__":
    unittest.main()
