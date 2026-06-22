"""Tests for the feedback-controller loop runtime (issue #31 slice)."""

import time

from hermes_workflows.errors import WorkflowValidationError
from hermes_workflows.loops import LoopSensorResult, loop_run, loop_validate


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
