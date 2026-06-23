"""Tests for the feedback-controller loop runtime (issue #31 slice)."""

import json
import tempfile
import time

from hermes_workflows.errors import WorkflowValidationError
from hermes_workflows.loops import FileLoopRunStore, InMemoryLoopRunStore, LoopSensorResult, loop_run, loop_validate
from hermes_workflows.resources import WorkflowResource, run_resource_finalizers


def loop_spec(**brake_overrides):
    brakes = {"max_steps": 4, "max_repeated_signal": 3, "max_sensor_retries": 1}
    brakes.update(brake_overrides)
    return {
        "version": "1",
        "name": "ticket_loop",
        "setpoint": {
            "target": "issue acceptance criteria are satisfied with evidence",
            "stop_condition": "primary sensor reports converged=true",
        },
        "sensors": [
            {"id": "verification", "primary": True, "kind": "callable"},
        ],
        "actuators": [
            {"id": "implementation", "kind": "relay_or_agent_step"},
        ],
        "brakes": brakes,
    }


def test_loop_validate_accepts_minimal_controller_spec():
    result = loop_validate(loop_spec())

    assert result.ok is True
    assert result.errors == []
    assert result.normalized is not None
    assert result.normalized["name"] == "ticket_loop"
    assert result.def_hash


def test_loop_validate_rejects_missing_primary_sensor_and_brake():
    spec = loop_spec()
    spec["sensors"] = [{"id": "verification", "kind": "callable"}]
    del spec["brakes"]["max_steps"]

    result = loop_validate(spec)

    assert result.ok is False
    pointers = {diag.pointer for diag in result.errors}
    assert "/sensors" in pointers
    assert "/brakes/max_steps" in pointers


def test_loop_validate_rejects_non_numeric_wall_time_and_cost():
    spec = loop_spec(max_wall_seconds="soon", max_cost=True)

    result = loop_validate(spec)

    assert result.ok is False
    pointers = {diag.pointer for diag in result.errors}
    assert "/brakes/max_wall_seconds" in pointers
    assert "/brakes/max_cost" in pointers


def test_loop_run_acts_on_failed_sensor_then_converges_on_next_signal():
    calls = {"sensor": 0, "actuator": 0}

    def sensor(context):
        calls["sensor"] += 1
        if calls["sensor"] == 1:
            return {
                "converged": False,
                "signal_key": "tests failing:unit",
                "summary": "unit test fails",
                "evidence": [{"kind": "test", "name": "unit", "status": "failed"}],
                "next_hint": "fix the failing unit test only",
            }
        return LoopSensorResult(
            converged=True,
            signal_key="tests passing:unit",
            summary="unit test passes",
            evidence=[{"kind": "test", "name": "unit", "status": "passed"}],
        )

    def actuator(context):
        calls["actuator"] += 1
        assert context["latest_sensor"]["signal_key"] == "tests failing:unit"
        assert context["handoff"]["prompt"] == "fix the failing unit test only"
        return {"summary": "patched code", "artifacts": ["src/example.py"], "cost": 0.5}

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator, inputs={"issue": 31})

    assert status.state == "converged"
    assert status.iterations == 2
    assert calls == {"sensor": 2, "actuator": 1}
    assert status.report["convergence_risk"] == "converged_by_sensor"
    assert status.sensor_results[-1].evidence[0]["status"] == "passed"


def test_loop_run_emits_live_events_with_run_identity():
    events = []

    def sensor(context):
        if context["iteration"] == 1:
            return {"converged": False, "signal_key": "needs-work", "summary": "needs work"}
        return {"converged": True, "signal_key": "done", "summary": "done"}

    def actuator(context):
        return {"summary": "patched code"}

    def on_event(event, status):
        assert status.run_id == "loop.events.1"
        events.append(event)

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator, run_id="loop.events.1", on_event=on_event)

    assert status.state == "converged"
    assert [event["kind"] for event in events] == [
        "planned",
        "sensing",
        "sensor_result",
        "acting",
        "actuator_result",
        "sensing",
        "sensor_result",
        "converged",
    ]
    assert [event["event_index"] for event in events] == list(range(len(events)))
    assert {event["run_id"] for event in events} == {"loop.events.1"}
    assert {event["loop_name"] for event in events} == {"ticket_loop"}
    assert all(event["def_hash"] == status.def_hash for event in events)


def test_loop_run_persists_inspectable_status_in_memory_store():
    store = InMemoryLoopRunStore()
    calls = {"sensor": 0}

    def sensor(context):
        calls["sensor"] += 1
        if calls["sensor"] == 1:
            return {"converged": False, "signal_key": "needs-work", "summary": "needs work"}
        return {"converged": True, "signal_key": "done", "summary": "done"}

    def actuator(context):
        stored = store.get_status("loop.store.1")
        assert stored is not None
        assert stored["state"] == "acting"
        assert stored["events"][-1]["kind"] == "acting"
        return {"summary": "patched", "artifacts": ["src/example.py"]}

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator, run_id="loop.store.1", store=store)
    stored = store.get_status("loop.store.1")

    assert stored is not None
    assert stored["state"] == "converged"
    assert stored["report"]["convergence_risk"] == "converged_by_sensor"
    assert stored["sensor_results"][-1]["signal_key"] == "done"
    assert stored["actuator_results"][0]["artifacts"] == ["src/example.py"]
    stored["state"] = "mutated"
    fresh = store.get_status("loop.store.1")
    assert fresh is not None
    assert fresh["state"] == "converged"
    assert status.as_dict() == fresh


def test_file_loop_run_store_writes_snapshot_and_event_journal():
    with tempfile.TemporaryDirectory() as tmp:
        store = FileLoopRunStore(tmp)

        def sensor(context):
            return {"converged": True, "signal_key": "green", "summary": "green"}

        status = loop_run(loop_spec(), sensor=sensor, run_id="loop.file.1", store=store)
        stored = store.get_status("loop.file.1")

        assert stored == status.as_dict()
        with open(f"{tmp}/loop.file.1/snapshot.json", encoding="utf-8") as handle:
            assert json.load(handle)["state"] == "converged"
        with open(f"{tmp}/loop.file.1/events.jsonl", encoding="utf-8") as handle:
            events = [json.loads(line) for line in handle]
        assert [event["kind"] for event in events] == ["planned", "sensing", "sensor_result", "converged"]
        assert all(event["run_id"] == "loop.file.1" for event in events)


def test_loop_run_retries_retryable_noise_once_before_acting():
    calls = {"sensor": 0, "actuator": 0}

    def sensor(context):
        calls["sensor"] += 1
        if calls["sensor"] == 1:
            return {
                "converged": False,
                "signal_key": "ci-timeout",
                "summary": "ci timed out",
                "retryable_noise": True,
            }
        return {"converged": True, "signal_key": "ci-green", "summary": "ci green"}

    def actuator(context):
        calls["actuator"] += 1
        return {"summary": "should not run"}

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator)

    assert status.state == "converged"
    assert calls == {"sensor": 2, "actuator": 0}
    assert any(event["kind"] == "sensor_noise_retry" for event in status.events)


def test_loop_run_defaults_null_sensor_summary_to_signal_key():
    calls = {"sensor": 0}

    def sensor(context):
        calls["sensor"] += 1
        return {"converged": True, "signal_key": "green", "summary": None}

    status = loop_run(loop_spec(), sensor=sensor)

    assert status.state == "converged"
    assert status.sensor_results[-1].summary == "green"
    assert calls["sensor"] == 1


def test_loop_run_halts_on_non_boolean_sensor_flags():
    def sensor(context):
        return {"converged": "false", "signal_key": "bad-bool", "summary": "bad bool"}

    status = loop_run(loop_spec(), sensor=sensor)

    assert status.state == "halted_sensor_error"
    assert status.halted_reason is not None
    assert "converged must be a boolean" in status.halted_reason


def test_loop_run_halts_on_non_boolean_retryable_noise():
    def sensor(context):
        return {
            "converged": False,
            "signal_key": "bad-noise",
            "summary": "bad noise flag",
            "retryable_noise": "false",
        }

    status = loop_run(loop_spec(), sensor=sensor)

    assert status.state == "halted_sensor_error"
    assert status.halted_reason is not None
    assert "retryable_noise must be a boolean" in status.halted_reason


def test_loop_run_halts_if_sensor_remains_noisy_after_retry_cap():
    calls = {"sensor": 0, "actuator": 0}

    def sensor(context):
        calls["sensor"] += 1
        return {
            "converged": False,
            "signal_key": "ci-timeout",
            "summary": "ci timed out",
            "retryable_noise": True,
        }

    def actuator(context):
        calls["actuator"] += 1
        return {"summary": "should not run on noise"}

    status = loop_run(loop_spec(max_sensor_retries=1), sensor=sensor, actuator=actuator)

    assert status.state == "halted_sensor_error"
    assert calls == {"sensor": 2, "actuator": 0}
    assert status.halted_reason is not None
    assert "retryable_noise" in status.halted_reason


def test_loop_run_halts_when_signal_repeats_without_progress():
    def sensor(context):
        return {
            "converged": False,
            "signal_key": "same-blocker",
            "summary": "same blocker remains",
        }

    def actuator(context):
        return {"summary": "attempted fix"}

    status = loop_run(
        loop_spec(max_steps=5, max_repeated_signal=2),
        sensor=sensor,
        actuator=actuator,
    )

    assert status.state == "halted_stalled"
    assert status.iterations == 2
    assert status.halted_reason is not None
    assert "same-blocker" in status.halted_reason
    assert status.report["convergence_risk"] == "not_converged"


def test_loop_run_halts_at_step_cap_after_final_verification():
    def sensor(context):
        return {"converged": False, "signal_key": f"not-done-{context['iteration']}", "summary": "not done"}

    def actuator(context):
        return {"summary": "agent says done"}

    status = loop_run(
        loop_spec(max_steps=1, max_repeated_signal=99),
        sensor=sensor,
        actuator=actuator,
    )

    assert status.state == "halted_step_cap"
    assert status.iterations == 2
    assert len(status.actuator_results) == 1
    assert len(status.sensor_results) == 2
    assert status.report["latest_sensor"]["converged"] is False
    assert status.report["latest_sensor"]["signal_key"] == "not-done-2"


def test_loop_run_allows_final_verification_to_converge_after_last_action():
    calls = {"sensor": 0, "actuator": 0}

    def sensor(context):
        calls["sensor"] += 1
        if calls["sensor"] == 1:
            return {"converged": False, "signal_key": "needs-final-action", "summary": "needs work"}
        return {"converged": True, "signal_key": "verified", "summary": "verified after action"}

    def actuator(context):
        calls["actuator"] += 1
        assert context["limits"]["remaining_actions"] == 1
        assert context["limits"]["action_count"] == 0
        return {"summary": "final action"}

    status = loop_run(loop_spec(max_steps=1, max_repeated_signal=99), sensor=sensor, actuator=actuator)

    assert status.state == "converged"
    assert calls == {"sensor": 2, "actuator": 1}


def test_loop_run_suspends_when_actuator_requests_event_wait():
    store = InMemoryLoopRunStore()
    events = []

    def sensor(context):
        return {"converged": False, "signal_key": "waiting-ci", "summary": "waiting on ci"}

    def actuator(context):
        return {
            "summary": "started external check",
            "handles": [{"kind": "check", "id": "ci-123"}],
            "wait": {"kind": "github_check", "token": "ci-123", "summary": "waiting for GitHub check"},
        }

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator, store=store, on_event=lambda event, status: events.append(event))
    stored = store.get_status(status.run_id)

    assert status.state == "waiting_for_event"
    assert status.halted_reason is None
    assert status.report["convergence_risk"] == "waiting_for_event"
    assert status.events[-1]["kind"] == "waiting_for_event"
    assert status.events[-1]["request"]["token"] == "ci-123"
    assert stored is not None
    assert stored["state"] == "waiting_for_event"
    assert events[-1]["kind"] == "waiting_for_event"


def test_loop_run_suspends_when_actuator_requests_approval():
    def sensor(context):
        return {"converged": False, "signal_key": "needs-human", "summary": "needs approval"}

    def actuator(context):
        return {
            "summary": "approval required",
            "approval_request": {
                "id": "merge-gate",
                "summary": "approve merge",
                "choices": ["approve", "deny"],
            },
        }

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator)

    assert status.state == "waiting_for_approval"
    assert status.halted_reason is None
    assert status.report["convergence_risk"] == "waiting_for_approval"
    assert status.events[-1]["kind"] == "waiting_for_approval"
    assert status.events[-1]["request"]["id"] == "merge-gate"


def test_loop_run_halts_on_malformed_suspension_request():
    def sensor(context):
        return {"converged": False, "signal_key": "bad-wait", "summary": "bad wait"}

    def actuator(context):
        return {"summary": "bad wait", "wait": {}}

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator)

    assert status.state == "halted_actuator_error"
    assert status.halted_reason is not None
    assert "wait requires non-empty id, token, or kind" in status.halted_reason


def test_loop_run_enforces_budget_cap_from_actuator_cost():
    def sensor(context):
        return {"converged": False, "signal_key": "needs-work", "summary": "needs work"}

    def actuator(context):
        return {"summary": "expensive step", "cost": 2.0}

    status = loop_run(loop_spec(max_cost=1.0), sensor=sensor, actuator=actuator)

    assert status.state == "halted_budget_cap"
    assert "exceeded max_cost" in status.halted_reason


def test_loop_run_halts_on_malformed_actuator_cost():
    def sensor(context):
        return {"converged": False, "signal_key": "needs-work", "summary": "needs work"}

    def actuator(context):
        return {"summary": "bad cost", "cost": "999"}

    status = loop_run(loop_spec(max_cost=1.0), sensor=sensor, actuator=actuator)

    assert status.state == "halted_actuator_error"
    assert status.halted_reason is not None
    assert "cost must be a non-negative number" in status.halted_reason


def test_loop_run_halts_if_sensor_exceeds_wall_time_before_claiming_convergence():
    def sensor(context):
        time.sleep(0.03)
        return {"converged": True, "signal_key": "green", "summary": "late green"}

    status = loop_run(loop_spec(max_wall_seconds=0.01), sensor=sensor)

    assert status.state == "halted_time_cap"
    assert status.halted_reason == "max_wall_seconds exceeded during sensing"
    assert status.sensor_results[-1].converged is True


def test_loop_run_halts_if_actuator_exceeds_wall_time():
    calls = {"sensor": 0, "actuator": 0}

    def sensor(context):
        calls["sensor"] += 1
        return {"converged": False, "signal_key": f"needs-work-{calls['sensor']}", "summary": "needs work"}

    def actuator(context):
        calls["actuator"] += 1
        assert context["limits"]["remaining_wall_seconds"] is not None
        time.sleep(0.03)
        return {"summary": "slow action"}

    status = loop_run(loop_spec(max_wall_seconds=0.01), sensor=sensor, actuator=actuator)

    assert status.state == "halted_time_cap"
    assert status.halted_reason == "max_wall_seconds exceeded during acting"
    assert calls == {"sensor": 1, "actuator": 1}


def test_loop_run_raises_validation_error_before_calling_sensor():
    called = False

    def sensor(context):
        nonlocal called
        called = True
        return {"converged": True, "signal_key": "unused", "summary": "unused"}

    try:
        loop_run({"version": "1", "name": "bad"}, sensor=sensor)
    except WorkflowValidationError as exc:
        assert exc.result.ok is False
    else:  # pragma: no cover - assertion clarity
        raise AssertionError("expected WorkflowValidationError")

    assert called is False


def _resource(resource_id="ath-listener-1", *, policy="required", when=None):
    return {
        "id": resource_id,
        "kind": "ath.listener",
        "handle": {"thread_key": "ath_safe_123"},
        "metadata": {"purpose": "release slice"},
        "finalizers": [
            {
                "id": "retire-listener",
                "action": "ath.listener.retire",
                "policy": policy,
                "when": when or ["success", "failure", "timeout"],
                "verification": {"event": "listener_disabled"},
            }
        ],
    }


def test_loop_run_runs_success_resource_finalizer_and_persists_result():
    store = InMemoryLoopRunStore()
    calls = {"sensor": 0, "cleanup": []}

    def sensor(context):
        calls["sensor"] += 1
        if calls["sensor"] == 1:
            return {"converged": False, "signal_key": "needs-cleanup-owned-resource", "summary": "needs work"}
        assert context["resources"][0]["id"] == "ath-listener-1"
        return {"converged": True, "signal_key": "done", "summary": "done"}

    def actuator(context):
        return {"summary": "provisioned listener", "resources": [_resource()]}

    def finalizer(context):
        calls["cleanup"].append(context)
        assert context["trigger"] == "success"
        assert context["resource"]["kind"] == "ath.listener"
        assert context["finalizer"]["action"] == "ath.listener.retire"
        return {"ok": True, "summary": "listener retired", "evidence": [{"kind": "ath", "status": "disabled"}]}

    status = loop_run(
        loop_spec(),
        sensor=sensor,
        actuator=actuator,
        finalizer=finalizer,
        run_id="loop.cleanup.success",
        store=store,
    )
    stored = store.get_status("loop.cleanup.success")

    assert status.state == "converged"
    assert len(calls["cleanup"]) == 1
    assert status.resources[0]["owner"]["run_id"] == "loop.cleanup.success"
    assert status.finalizer_results[0]["status"] == "succeeded"
    assert status.finalizer_results[0]["trigger"] == "success"
    assert status.report["resource_count"] == 1
    assert status.report["finalizer_count"] == 1
    assert stored is not None
    assert stored["finalizer_results"] == status.finalizer_results
    assert any(event["kind"] == "resources_registered" for event in status.events)
    assert any(event["kind"] == "finalizer_result" for event in status.events)


def test_loop_run_required_finalizer_failure_blocks_success():
    calls = {"sensor": 0}

    def sensor(context):
        calls["sensor"] += 1
        if calls["sensor"] == 1:
            return {"converged": False, "signal_key": "needs-work", "summary": "needs work"}
        return {"converged": True, "signal_key": "done", "summary": "done"}

    def actuator(context):
        return {"summary": "provisioned relay session", "resources": [_resource("relay-session-1")]}

    def finalizer(context):
        return {"ok": False, "summary": "relay session still active", "error": "still_running"}

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator, finalizer=finalizer)

    assert status.state == "halted_finalizer_error"
    assert status.halted_reason == "required resource finalizer failed"
    assert status.finalizer_results[0]["status"] == "failed"
    assert status.finalizer_results[0]["policy"] == "required"
    assert status.report["final_state"] == "halted_finalizer_error"
    assert status.report["required_finalizer_failed"] is True
    assert status.report["convergence_risk"] == "required_finalizer_failed"
    assert status.events[-1]["kind"] == "halted"
    assert status.events[-1]["prior_state"] == "converged"


def test_loop_run_runs_timeout_resource_finalizer():
    calls = {"cleanup": []}

    def sensor(context):
        return {"converged": False, "signal_key": "needs-work", "summary": "needs work"}

    def actuator(context):
        time.sleep(0.03)
        return {"summary": "slow provisioned process", "resources": [_resource("process-1", when=["timeout"])]}

    def finalizer(context):
        calls["cleanup"].append(context["trigger"])
        return {"ok": True, "summary": "process stopped"}

    status = loop_run(loop_spec(max_wall_seconds=0.01), sensor=sensor, actuator=actuator, finalizer=finalizer)

    assert status.state == "halted_time_cap"
    assert calls["cleanup"] == ["timeout"]
    assert status.finalizer_results[0]["trigger"] == "timeout"
    assert status.finalizer_results[0]["status"] == "succeeded"


def test_loop_run_dedupes_repeated_resource_finalizer_registration():
    calls = {"sensor": 0, "cleanup": 0}

    def sensor(context):
        calls["sensor"] += 1
        if calls["sensor"] < 3:
            return {"converged": False, "signal_key": f"needs-work-{calls['sensor']}", "summary": "needs work"}
        return {"converged": True, "signal_key": "done", "summary": "done"}

    def actuator(context):
        return {"summary": "same listener still owned", "resources": [_resource("ath-listener-repeat")]}

    def finalizer(context):
        calls["cleanup"] += 1
        return {"ok": True, "summary": "cleanup once"}

    status = loop_run(loop_spec(max_repeated_signal=99), sensor=sensor, actuator=actuator, finalizer=finalizer)

    assert status.state == "converged"
    assert len(status.resources) == 1
    assert len(status.finalizer_results) == 1
    assert calls["cleanup"] == 1


def test_loop_run_rejects_resource_envelope_with_raw_credential_before_journaling():
    def sensor(context):
        return {"converged": False, "signal_key": "needs-work", "summary": "needs work"}

    def actuator(context):
        resource = _resource()
        resource["handle"]["token"] = "ghp_should_not_be_journaled"
        return {"summary": "bad resource", "resources": [resource]}

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator)

    assert status.state == "halted_actuator_error"
    assert status.resources == []
    assert status.actuator_results == []
    assert status.halted_reason is not None
    assert "raw credential" in status.halted_reason
    dumped = json.dumps(status.as_dict())
    assert "ghp_should_not_be_journaled" not in dumped


def test_resource_finalizer_helper_runs_host_owned_cancelled_trigger_once():
    resource = WorkflowResource.from_dict(_resource("relay-session-cancel", when=["cancelled"]))
    calls = []

    def finalizer(context):
        calls.append((context["trigger"], context["resource"]["id"]))
        return {"ok": True, "summary": "cancel cleanup done"}

    first = run_resource_finalizers(
        [resource],
        trigger="cancelled",
        runner=finalizer,
        run_id="loop.cancel.host",
        loop_name="ticket_loop",
    )
    second = run_resource_finalizers(
        [resource],
        trigger="cancelled",
        runner=finalizer,
        run_id="loop.cancel.host",
        loop_name="ticket_loop",
        existing_results=[item.to_dict() for item in first],
    )

    assert [item.status for item in first] == ["succeeded"]
    assert second == []
    assert calls == [("cancelled", "relay-session-cancel")]


def test_duplicate_finalizer_ids_do_not_run_twice():
    resource_payload = _resource("dup-finalizer")
    resource_payload["finalizers"].append(
        {
            "id": "retire-listener",
            "action": "ath.listener.retire",
            "policy": "required",
            "when": ["success"],
        }
    )
    resource = WorkflowResource.from_dict(resource_payload)
    calls = []

    def finalizer(context):
        calls.append(context["finalizer"]["id"])
        return {"ok": True, "summary": "cleanup once"}

    results = run_resource_finalizers(
        [resource],
        trigger="success",
        runner=finalizer,
        run_id="loop.dup.finalizer",
        loop_name="ticket_loop",
    )

    assert [item.finalizer_id for item in resource.finalizers] == ["retire-listener"]
    assert len(results) == 1
    assert calls == ["retire-listener"]


def test_resource_owner_cannot_override_controller_provenance():
    resource = WorkflowResource.from_dict(
        {
            "id": "owned-resource",
            "kind": "ath.listener",
            "owner": {"run_id": "forged", "loop_name": "other", "iteration": 999, "issue": 52},
        },
        default_owner={"run_id": "real", "loop_name": "ticket_loop", "iteration": 1},
    )

    assert resource.owner["run_id"] == "real"
    assert resource.owner["loop_name"] == "ticket_loop"
    assert resource.owner["iteration"] == 1
    assert resource.owner["issue"] == 52


def test_loop_run_finalizer_exception_text_is_redacted_before_journaling():
    calls = {"sensor": 0}

    def sensor(context):
        calls["sensor"] += 1
        if calls["sensor"] == 1:
            return {"converged": False, "signal_key": "needs-work", "summary": "needs work"}
        return {"converged": True, "signal_key": "done", "summary": "done"}

    def actuator(context):
        return {"summary": "provisioned listener", "resources": [_resource("secret-exception-resource")]}

    def finalizer(context):
        raise RuntimeError("cleanup failed with ghp_should_not_be_logged")

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator, finalizer=finalizer)
    dumped = json.dumps(status.as_dict())

    assert status.state == "halted_finalizer_error"
    assert "ghp_should_not_be_logged" not in dumped
    assert "[REDACTED]" in dumped


def test_loop_run_merges_repeated_resource_registration_updates_and_finalizers():
    calls = {"sensor": 0, "cleanup": []}

    def sensor(context):
        calls["sensor"] += 1
        if calls["sensor"] < 3:
            return {"converged": False, "signal_key": f"needs-work-{calls['sensor']}", "summary": "needs work"}
        return {"converged": True, "signal_key": "done", "summary": "done"}

    def actuator(context):
        resource = _resource("relay-session-merge")
        resource["handle"] = {"session_id": f"relay-{context['iteration']}"}
        resource["metadata"] = {"generation": context["iteration"]}
        if context["iteration"] == 2:
            resource["finalizers"].append(
                {
                    "id": "release-work-context",
                    "action": "relay.work_context.release",
                    "policy": "required",
                    "when": ("success",),
                }
            )
        return {"summary": "resource registration", "resources": [resource]}

    def finalizer(context):
        calls["cleanup"].append((context["resource"]["handle"], context["finalizer"]["id"]))
        return {"ok": True, "summary": "cleanup done"}

    status = loop_run(loop_spec(max_repeated_signal=99), sensor=sensor, actuator=actuator, finalizer=finalizer)

    assert status.state == "converged"
    assert len(status.resources) == 1
    resource = status.resources[0]
    assert resource["handle"] == {"session_id": "relay-2"}
    assert resource["metadata"] == {"generation": 2}
    assert [item["id"] for item in resource["finalizers"]] == ["retire-listener", "release-work-context"]
    assert [result["finalizer_id"] for result in status.finalizer_results] == [
        "retire-listener",
        "release-work-context",
    ]
    assert calls["cleanup"] == [
        ({"session_id": "relay-2"}, "retire-listener"),
        ({"session_id": "relay-2"}, "release-work-context"),
    ]


def test_loop_run_finalizer_return_summary_error_and_evidence_redact_embedded_tokens():
    calls = {"sensor": 0}

    def sensor(context):
        calls["sensor"] += 1
        if calls["sensor"] == 1:
            return {"converged": False, "signal_key": "needs-work", "summary": "needs work"}
        return {"converged": True, "signal_key": "done", "summary": "done"}

    def actuator(context):
        return {"summary": "provisioned listener", "resources": [_resource("secret-return-resource")]}

    def finalizer(context):
        return {
            "ok": False,
            "summary": "cleanup failed with ghp_should_not_be_logged",
            "error": "api returned bearer sk-shouldnotbelogged",
            "evidence": [{"detail": "nested github_pat_should_not_be_logged"}],
        }

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator, finalizer=finalizer)
    dumped = json.dumps(status.as_dict())

    assert status.state == "halted_finalizer_error"
    assert "ghp_should_not_be_logged" not in dumped
    assert "sk-shouldnotbelogged" not in dumped
    assert "github_pat_should_not_be_logged" not in dumped
    assert dumped.count("[REDACTED]") >= 3
