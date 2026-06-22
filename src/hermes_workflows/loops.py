"""Feedback-controller loop runtime for agent workflows.

This module is the first implementation slice for issue #31: it treats a long
running agent workflow as a bounded feedback controller instead of "ask an agent
until it says done".  It intentionally stays pure-stdlib and backend-neutral:
callers inject a sensor/verifier and an optional actuator/backend callable.

The controller owns the boring-but-load-bearing parts:

* a validated loop spec with setpoint, sensors, actuators, and brakes;
* explicit controller states;
* structured sensor signals that drive the next action;
* retry-once filtering for noisy sensors;
* hard brakes for step count, wall time, budget, and repeated no-progress
  signals;
* a durable-ish report shape that can be persisted by a caller or emitted to ATH.

No network, filesystem, shell, Relay, ATH, or Kanban calls happen here. Those are
adapter responsibilities behind the injected actuator/sensor callables.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Optional, Protocol, runtime_checkable

from . import schema as _schema
from .errors import WorkflowValidationError
from .models import Diagnostic, ValidationResult

__all__ = [
    "LoopState",
    "LoopSensorResult",
    "LoopRunStatus",
    "SensorCallable",
    "ActuatorCallable",
    "loop_validate",
    "loop_run",
]

LoopState = Literal[
    "planned",
    "sensing",
    "acting",
    "verifying",
    "waiting_for_event",
    "waiting_for_approval",
    "converged",
    "halted_step_cap",
    "halted_budget_cap",
    "halted_time_cap",
    "halted_stalled",
    "halted_sensor_error",
    "halted_actuator_error",
]


@dataclass
class LoopSensorResult:
    """Structured verifier/sensor signal that controls the next loop step."""

    converged: bool
    signal_key: str
    summary: str
    evidence: list[dict[str, Any]] = field(default_factory=list)
    retryable_noise: bool = False
    next_hint: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class LoopRunStatus:
    """Terminal or in-progress state of one loop-controller run."""

    run_id: str
    state: LoopState
    iterations: int
    created_at: str
    updated_at: str
    def_hash: str
    sensor_results: list[LoopSensorResult] = field(default_factory=list)
    actuator_results: list[dict[str, Any]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    halted_reason: Optional[str] = None
    report: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "state": self.state,
            "iterations": self.iterations,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "def_hash": self.def_hash,
            "sensor_results": [r.as_dict() for r in self.sensor_results],
            "actuator_results": self.actuator_results,
            "events": self.events,
            "halted_reason": self.halted_reason,
            "report": self.report,
        }


@runtime_checkable
class SensorCallable(Protocol):
    """Callable verifier boundary used by :func:`loop_run`."""

    def __call__(self, context: dict[str, Any]) -> LoopSensorResult | dict[str, Any]:
        ...


@runtime_checkable
class ActuatorCallable(Protocol):
    """Callable backend/action boundary used by :func:`loop_run`."""

    def __call__(self, context: dict[str, Any]) -> dict[str, Any]:
        ...


def loop_validate(spec: dict[str, Any] | str) -> ValidationResult:
    """Validate a loop-controller spec without running sensors or actuators.

    The spec is intentionally generic. Repo/tool specifics belong in ``inputs``
    at run time or in adapter config, not as new primitive kinds.
    """

    parsed, parse_diag = _schema.parse_definition(spec)
    if parsed is None:
        assert parse_diag is not None
        return ValidationResult(ok=False, errors=[parse_diag], warnings=[], normalized=None, def_hash="")

    diags = _validate_loop_structure(parsed)
    return ValidationResult(
        ok=not diags,
        errors=diags,
        warnings=[],
        normalized=parsed,
        def_hash=_schema.def_hash(parsed),
    )


def loop_run(
    spec: dict[str, Any] | str,
    *,
    sensor: SensorCallable,
    actuator: Optional[ActuatorCallable] = None,
    inputs: Optional[dict[str, Any]] = None,
    run_id: Optional[str] = None,
) -> LoopRunStatus:
    """Run a bounded feedback-controller loop synchronously.

    ``sensor`` is the verifier. It must return a :class:`LoopSensorResult` or a
    compatible dict. ``actuator`` receives the latest sensor signal plus bounded
    history and may call a backend such as Relay, Kanban, ``delegate_task``, or a
    local process. The controller never trusts the actuator's self-report as
    success; only a later sensor result can converge the run.
    """

    validation = loop_validate(spec)
    if not validation.ok or validation.normalized is None:
        raise WorkflowValidationError(validation)

    normalized = validation.normalized
    brakes = _brakes(normalized)
    now = _utc_now_iso()
    rid = run_id or f"loop_{validation.def_hash[:8]}_{uuid.uuid4().hex[:12]}"
    status = LoopRunStatus(
        run_id=rid,
        state="planned",
        iterations=0,
        created_at=now,
        updated_at=now,
        def_hash=validation.def_hash,
        events=[_event("planned", "planned", 0, "loop planned")],
    )

    started = time.monotonic()
    last_signal: Optional[str] = None
    repeated_signal_count = 0
    total_cost = 0.0

    for iteration in range(1, int(brakes["max_steps"]) + 1):
        status.iterations = iteration
        if _time_exceeded(started, brakes):
            _halt(status, "halted_time_cap", "max_wall_seconds exceeded before sensing")
            break

        status.state = "sensing"
        status.events.append(_event("sensing", "sensing", iteration, "sensor started"))
        sensor_result = _read_sensor_with_noise_retry(
            sensor,
            context=_context(
                normalized,
                inputs,
                status,
                latest=status.sensor_results[-1] if status.sensor_results else None,
            ),
            max_retries=int(brakes["max_sensor_retries"]),
            status=status,
            iteration=iteration,
        )
        if isinstance(sensor_result, Exception):
            _halt(status, "halted_sensor_error", str(sensor_result))
            break

        status.sensor_results.append(sensor_result)
        status.events.append(
            _event(
                "sensor_result",
                "verifying",
                iteration,
                sensor_result.summary,
                signal_key=sensor_result.signal_key,
                converged=sensor_result.converged,
            )
        )

        if sensor_result.converged:
            status.state = "converged"
            status.halted_reason = None
            status.report = _report(status, "converged_by_sensor")
            status.events.append(_event("converged", "converged", iteration, sensor_result.summary))
            break

        if sensor_result.signal_key == last_signal:
            repeated_signal_count += 1
        else:
            last_signal = sensor_result.signal_key
            repeated_signal_count = 1

        if iteration > 1 and repeated_signal_count >= int(brakes["max_repeated_signal"]):
            _halt(
                status,
                "halted_stalled",
                f"sensor signal {sensor_result.signal_key!r} repeated {repeated_signal_count} times",
            )
            break

        if actuator is None:
            _halt(status, "halted_actuator_error", "non-converged loop has no actuator")
            break

        if _time_exceeded(started, brakes):
            _halt(status, "halted_time_cap", "max_wall_seconds exceeded before acting")
            break

        status.state = "acting"
        status.events.append(_event("acting", "acting", iteration, "actuator started"))
        try:
            action = actuator(_context(normalized, inputs, status, latest=sensor_result))
            if not isinstance(action, dict):
                raise TypeError(f"actuator returned {type(action).__name__}, expected dict")
        except Exception as exc:  # pragma: no cover - exact exception type is caller-owned
            _halt(status, "halted_actuator_error", f"{type(exc).__name__}: {exc}")
            break

        status.actuator_results.append(action)
        status.events.append(_event("actuator_result", "verifying", iteration, "actuator completed", result=action))
        total_cost += _numeric(action.get("cost"))
        if brakes.get("max_cost") is not None and total_cost > float(brakes["max_cost"]):
            _halt(status, "halted_budget_cap", f"cost {total_cost} exceeded max_cost {brakes['max_cost']}")
            break
    else:
        _halt(status, "halted_step_cap", f"max_steps {brakes['max_steps']} exhausted without convergence")

    status.updated_at = _utc_now_iso()
    if not status.report:
        status.report = _report(status, "not_converged")
    return status


def _validate_loop_structure(spec: dict[str, Any]) -> list[Diagnostic]:
    diags: list[Diagnostic] = []
    if spec.get("version") != "1":
        diags.append(_e("E_LOOP_SCHEMA", "loop spec requires version '1'", "/version"))
    name = spec.get("name")
    if not isinstance(name, str) or not name or not _identifier_safe(name):
        diags.append(_e("E_LOOP_SCHEMA", "loop spec requires an identifier-safe name", "/name"))

    setpoint = spec.get("setpoint")
    if not isinstance(setpoint, dict):
        diags.append(_e("E_LOOP_SCHEMA", "loop spec requires object 'setpoint'", "/setpoint"))
    elif not any(isinstance(setpoint.get(k), str) and setpoint.get(k) for k in ("target", "description", "stop_condition")):
        diags.append(_e("E_LOOP_SCHEMA", "setpoint needs target, description, or stop_condition", "/setpoint"))

    sensors = spec.get("sensors")
    if not isinstance(sensors, list) or not sensors:
        diags.append(_e("E_LOOP_SCHEMA", "loop spec requires non-empty sensors list", "/sensors"))
    elif not any(isinstance(s, dict) and s.get("primary") is True for s in sensors):
        diags.append(_e("E_LOOP_SCHEMA", "one sensor must be marked primary=true", "/sensors"))
    if isinstance(sensors, list):
        for i, sensor in enumerate(sensors):
            _validate_named_object(sensor, f"/sensors/{i}", "sensor", diags)

    actuators = spec.get("actuators", [])
    if not isinstance(actuators, list):
        diags.append(_e("E_LOOP_SCHEMA", "actuators must be a list when present", "/actuators"))
    else:
        for i, actuator in enumerate(actuators):
            _validate_named_object(actuator, f"/actuators/{i}", "actuator", diags)

    brakes = spec.get("brakes")
    if not isinstance(brakes, dict):
        diags.append(_e("E_LOOP_SCHEMA", "loop spec requires object 'brakes'", "/brakes"))
    else:
        _positive_int(brakes, "max_steps", "/brakes/max_steps", diags, required=True)
        _positive_int(brakes, "max_repeated_signal", "/brakes/max_repeated_signal", diags)
        _nonnegative_int(brakes, "max_sensor_retries", "/brakes/max_sensor_retries", diags)
        _nonnegative_number(brakes, "max_wall_seconds", "/brakes/max_wall_seconds", diags)
        _nonnegative_number(brakes, "max_cost", "/brakes/max_cost", diags)

    return diags


def _validate_named_object(value: Any, ptr: str, label: str, diags: list[Diagnostic]) -> None:
    if not isinstance(value, dict):
        diags.append(_e("E_LOOP_SCHEMA", f"{label} must be an object", ptr))
        return
    ident = value.get("id")
    if not isinstance(ident, str) or not ident or not _identifier_safe(ident):
        diags.append(_e("E_LOOP_SCHEMA", f"{label} requires identifier-safe id", f"{ptr}/id"))


def _positive_int(obj: dict[str, Any], key: str, ptr: str, diags: list[Diagnostic], *, required: bool = False) -> None:
    if key not in obj:
        if required:
            diags.append(_e("E_LOOP_SCHEMA", f"brakes.{key} is required", ptr))
        return
    value = obj[key]
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        diags.append(_e("E_LOOP_SCHEMA", f"brakes.{key} must be a positive integer", ptr))


def _nonnegative_int(obj: dict[str, Any], key: str, ptr: str, diags: list[Diagnostic]) -> None:
    if key not in obj:
        return
    value = obj[key]
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        diags.append(_e("E_LOOP_SCHEMA", f"brakes.{key} must be a non-negative integer", ptr))


def _nonnegative_number(obj: dict[str, Any], key: str, ptr: str, diags: list[Diagnostic]) -> None:
    if key not in obj:
        return
    value = obj[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        diags.append(_e("E_LOOP_SCHEMA", f"brakes.{key} must be a non-negative number", ptr))


def _brakes(spec: dict[str, Any]) -> dict[str, Any]:
    raw = dict(spec.get("brakes") or {})
    return {
        "max_steps": raw["max_steps"],
        "max_repeated_signal": raw.get("max_repeated_signal", 3),
        "max_sensor_retries": raw.get("max_sensor_retries", 1),
        "max_wall_seconds": raw.get("max_wall_seconds"),
        "max_cost": raw.get("max_cost"),
    }


def _read_sensor_with_noise_retry(
    sensor: SensorCallable,
    *,
    context: dict[str, Any],
    max_retries: int,
    status: LoopRunStatus,
    iteration: int,
) -> LoopSensorResult | Exception:
    attempts = 0
    while True:
        try:
            result = _normalize_sensor_result(sensor(context))
        except Exception as exc:  # pragma: no cover - caller-owned exception type
            return exc
        if not result.retryable_noise:
            return result
        if attempts >= max_retries:
            return RuntimeError(
                f"sensor still marked retryable_noise after {attempts} retries: {result.summary}"
            )
        attempts += 1
        status.events.append(
            _event(
                "sensor_noise_retry",
                "sensing",
                iteration,
                result.summary,
                signal_key=result.signal_key,
                attempt=attempts,
            )
        )


def _normalize_sensor_result(value: LoopSensorResult | dict[str, Any]) -> LoopSensorResult:
    if isinstance(value, LoopSensorResult):
        return value
    if not isinstance(value, dict):
        raise TypeError(f"sensor returned {type(value).__name__}, expected dict")
    signal_key = value.get("signal_key")
    if not isinstance(signal_key, str) or not signal_key:
        raise ValueError("sensor result requires non-empty signal_key")
    evidence = value.get("evidence", [])
    if not isinstance(evidence, list):
        raise ValueError("sensor result evidence must be a list")
    return LoopSensorResult(
        converged=bool(value.get("converged", False)),
        signal_key=signal_key,
        summary=str(value["summary"]) if value.get("summary") is not None else signal_key,
        evidence=[e for e in evidence if isinstance(e, dict)],
        retryable_noise=bool(value.get("retryable_noise", False)),
        next_hint=str(value["next_hint"]) if value.get("next_hint") is not None else None,
    )


def _context(
    spec: dict[str, Any],
    inputs: Optional[dict[str, Any]],
    status: LoopRunStatus,
    *,
    latest: Optional[LoopSensorResult],
) -> dict[str, Any]:
    return {
        "run_id": status.run_id,
        "loop_name": spec.get("name"),
        "setpoint": spec.get("setpoint", {}),
        "inputs": dict(inputs or {}),
        "iteration": status.iterations,
        "latest_sensor": latest.as_dict() if latest else None,
        "history": {
            "sensor_results": [r.as_dict() for r in status.sensor_results],
            "actuator_results": list(status.actuator_results),
            "events": list(status.events),
        },
        "handoff": {
            "prompt": latest.next_hint if latest else None,
            "expected_return": {
                "artifacts": "list of changed files, PR/check/session handles, or other evidence",
                "cost": "optional numeric cost for budget brakes",
                "summary": "bounded action summary; success still requires a later sensor result",
            },
        },
    }


def _time_exceeded(started: float, brakes: dict[str, Any]) -> bool:
    max_wall = brakes.get("max_wall_seconds")
    return max_wall is not None and (time.monotonic() - started) > float(max_wall)


def _halt(status: LoopRunStatus, state: LoopState, reason: str) -> None:
    status.state = state
    status.halted_reason = reason
    status.events.append(_event("halted", state, status.iterations, reason))


def _report(status: LoopRunStatus, convergence_risk: str) -> dict[str, Any]:
    latest = status.sensor_results[-1].as_dict() if status.sensor_results else None
    return {
        "final_state": status.state,
        "iterations": status.iterations,
        "latest_sensor": latest,
        "actuator_count": len(status.actuator_results),
        "halted_reason": status.halted_reason,
        "convergence_risk": convergence_risk,
    }


def _event(kind: str, state: LoopState, iteration: int, summary: str, **extra: Any) -> dict[str, Any]:
    payload = {
        "kind": kind,
        "state": state,
        "iteration": iteration,
        "summary": summary,
        "at": _utc_now_iso(),
    }
    payload.update(extra)
    return payload


def _numeric(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def _identifier_safe(value: str) -> bool:
    return all(c.isalnum() or c in "._-" for c in value)


def _utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _e(code: str, message: str, pointer: str) -> Diagnostic:
    return Diagnostic(severity="error", code=code, message=message, pointer=pointer)
