"""Release-ops resource closeout example for Dynamic Workflows.

This example intentionally does not import ATH or Relay. Dynamic Workflows owns the
backend-neutral lifecycle model and dispatch registry; ATH and Relay own their
concrete adapter packages:

- ATH registers `ath.listener.retire` from `async_threads.finalizers`.
- Relay registers `relay.automation_run.retire` from its own adapter surface.

The local handlers below are stand-ins so the example runs in this repository's
zero-dependency test environment while preserving the production action names and
resource shapes.
"""

from __future__ import annotations

from typing import Any

from hermes_workflows import ResourceFinalizerRegistry, loop_run


LOOP_SPEC = {
    "version": "1",
    "name": "release_ops_resource_closeout",
    "setpoint": {
        "target": "PR slice shipped with cleanup evidence",
        "stop_condition": "release sensor reports merged=true after validation",
    },
    "sensors": [{"id": "release_verifier", "primary": True, "kind": "callable"}],
    "actuators": [{"id": "release_lane", "kind": "adapter"}],
    "brakes": {"max_steps": 2, "max_repeated_signal": 2, "max_sensor_retries": 1},
}


def release_lane_actuator(context: dict[str, Any]) -> dict[str, Any]:
    """Declare resources a release lane would provision for one PR slice."""
    _ = context
    return {
        "summary": "registered release lane resources for PR closeout",
        "artifacts": ["github-pr:donovan-yohan/example#1020"],
        "resources": [
            {
                "id": "ath-listener-pr-1020",
                "kind": "ath.listener",
                "handle": {"threadKey": "ath_example_thread"},
                "owner": {"issue": 52, "pr": 1020, "lane": "release-ops"},
                "metadata": {"purpose": "wake release lane on PR/CI/review events"},
                "finalizers": [
                    {
                        "id": "retire-ath-listener",
                        "action": "ath.listener.retire",
                        "when": ["success", "failure", "timeout"],
                        "policy": "required",
                        "verification": {"event": "listener_disabled"},
                    }
                ],
            },
            {
                "id": "relay-automation-run-pr-1020",
                "kind": "relay.automation_run",
                "handle": {"automationRunId": "automation-run:example-pr-1020"},
                "owner": {"issue": 52, "pr": 1020, "lane": "release-ops"},
                "metadata": {
                    "purpose": "track Relay watchdog/session closeout for the slice",
                    "note": "child-session termination remains a Relay-owned primitive",
                },
                "finalizers": [
                    {
                        "id": "retire-relay-automation-run",
                        "action": "relay.automation_run.retire",
                        "when": ["success", "failure", "timeout"],
                        "policy": "required",
                        "verification": {"event": "automation_run_retired"},
                    }
                ],
            },
        ],
    }


def make_release_sensor() -> Any:
    """Return a tiny sensor that requires one actuator step, then converges."""
    calls = {"count": 0}

    def sensor(context: dict[str, Any]) -> dict[str, Any]:
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "converged": False,
                "signal_key": "release-slice:needs-lane",
                "summary": "release lane not yet registered",
                "next_hint": "register ATH listener and Relay automation-run resources",
            }
        safe_context = context or {}
        return {
            "converged": True,
            "signal_key": "release-slice:merged-and-verified",
            "summary": "PR merged and release evidence captured",
            "evidence": [
                {"kind": "github_pr", "status": "merged"},
                {"kind": "resource_count", "count": len(safe_context.get("resources", []))},
            ],
        }

    return sensor


def build_demo_finalizers(calls: list[tuple[str, str]]) -> ResourceFinalizerRegistry:
    """Build stand-in finalizers using the production action names."""
    finalizers = ResourceFinalizerRegistry()

    def retire_ath_listener(context: dict[str, Any]) -> dict[str, Any]:
        safe_context = context or {}
        resource = safe_context.get("resource") or {}
        handle = resource.get("handle") or {}
        trigger = safe_context.get("trigger", "unknown")
        calls.append((resource.get("id", "unknown"), trigger))
        return {
            "ok": True,
            "summary": "ATH listener retired",
            "evidence": [
                {
                    "kind": "ath.listener.retire",
                    "threadKey": handle.get("threadKey", "unknown"),
                    "enabledAfter": False,
                }
            ],
        }

    def retire_relay_automation_run(context: dict[str, Any]) -> dict[str, Any]:
        safe_context = context or {}
        resource = safe_context.get("resource") or {}
        handle = resource.get("handle") or {}
        trigger = safe_context.get("trigger", "unknown")
        calls.append((resource.get("id", "unknown"), trigger))
        return {
            "ok": True,
            "summary": "Relay automation run retired",
            "evidence": [
                {
                    "kind": "relay.automation_run.retire",
                    "automationRunId": handle.get("automationRunId", "unknown"),
                    "statusAfter": "retired",
                }
            ],
        }

    finalizers.register("ath.listener.retire", retire_ath_listener)
    finalizers.register("relay.automation_run.retire", retire_relay_automation_run)
    return finalizers


def run_example() -> tuple[Any, list[tuple[str, str]]]:
    """Run the example and return the loop status plus finalizer call log."""
    finalizer_calls: list[tuple[str, str]] = []
    status = loop_run(
        LOOP_SPEC,
        sensor=make_release_sensor(),
        actuator=release_lane_actuator,
        finalizer=build_demo_finalizers(finalizer_calls),
        run_id="release.ops.example.1",
        inputs={"repo": "donovan-yohan/example", "pr": 1020},
    )
    return status, finalizer_calls


if __name__ == "__main__":
    loop_status, cleanup_calls = run_example()
    print("state:", loop_status.state)
    print("finalizers:", cleanup_calls)
    for result in loop_status.finalizer_results:
        print(result["action"], result["status"], "-", result["summary"])
