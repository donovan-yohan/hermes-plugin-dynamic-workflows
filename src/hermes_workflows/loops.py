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

import copy
import json
import math
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, Optional, Protocol, runtime_checkable

from . import schema as _schema
from .errors import GrantError, WorkflowValidationError
from .grants import (
    GrantBroker,
    GrantStore,
    REDACTED,
    find_raw_credential,
    redact_credentials,
    request_grant,
    resolve_grant,
    validate_grant,
)
from .models import Diagnostic, ValidationResult

__all__ = [
    "LoopState",
    "LoopEvent",
    "LoopSensorResult",
    "LoopRunStatus",
    "SensorCallable",
    "ActuatorCallable",
    "LoopEventSink",
    "LoopRunStore",
    "InMemoryLoopRunStore",
    "FileLoopRunStore",
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
    "halted_grant_denied",
]

LoopEvent = dict[str, Any]


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
    grants: list[dict[str, Any]] = field(default_factory=list)
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
            "grants": self.grants,
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


@runtime_checkable
class LoopEventSink(Protocol):
    """Live loop lifecycle event sink, e.g. ATH/gateway notifier adapter."""

    def __call__(self, event: LoopEvent, status: LoopRunStatus) -> None:
        ...


@runtime_checkable
class LoopRunStore(Protocol):
    """Persistence boundary for inspectable loop-controller run state."""

    def save_status(self, status: LoopRunStatus) -> None:
        ...

    def get_status(self, run_id: str) -> Optional[dict[str, Any]]:
        ...


class InMemoryLoopRunStore:
    """Small in-process loop status store for tests and embedders."""

    def __init__(self) -> None:
        self._runs: dict[str, dict[str, Any]] = {}

    def save_status(self, status: LoopRunStatus) -> None:
        self._runs[status.run_id] = copy.deepcopy(status.as_dict())

    def get_status(self, run_id: str) -> Optional[dict[str, Any]]:
        snapshot = self._runs.get(run_id)
        return None if snapshot is None else copy.deepcopy(snapshot)


class FileLoopRunStore:
    """Filesystem loop status store: ``<root>/<run_id>/snapshot.json`` + events journal."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def save_status(self, status: LoopRunStatus) -> None:
        run_dir = self.root / _safe_run_id(status.run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        snapshot = status.as_dict()
        tmp = run_dir / "snapshot.json.tmp"
        tmp.write_text(json.dumps(snapshot, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        tmp.replace(run_dir / "snapshot.json")
        events = "".join(json.dumps(event, sort_keys=True) + "\n" for event in snapshot["events"])
        (run_dir / "events.jsonl").write_text(events, encoding="utf-8")

    def get_status(self, run_id: str) -> Optional[dict[str, Any]]:
        path = self.root / _safe_run_id(run_id) / "snapshot.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))


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
    store: Optional[LoopRunStore] = None,
    on_event: Optional[LoopEventSink] = None,
    grant_broker: Optional[GrantBroker] = None,
    grant_store: Optional[GrantStore] = None,
) -> LoopRunStatus:
    """Run a bounded feedback-controller loop synchronously.

    ``sensor`` is the verifier. It must return a :class:`LoopSensorResult` or a
    compatible dict. ``actuator`` receives the latest sensor signal plus bounded
    history and may call a backend such as Relay, Kanban, ``delegate_task``, or a
    local process. The controller never trusts the actuator's self-report as
    success; only a later sensor result can converge the run.

    ``brakes.max_steps`` is an action cap. A run may perform one extra sensor
    read after the final action so the controller can verify the action before it
    converges or halts at the step cap with fresh evidence.

    An actuator that needs authority to launch or control a managed session asks
    for a scoped grant instead of holding a raw token: it returns
    ``grant_request: {scope, side_effect_class, subject, reason, ttl_seconds}``.
    The controller resolves it through ``grant_broker`` (see
    :mod:`hermes_workflows.grants`), records the issued, credential-free grant in
    ``status.grants`` (and ``grant_store`` when given), and exposes it back to
    later steps via ``context["grants"]``. An actuator may instead return a
    previously issued ``grant`` to re-validate before reuse. A denied, expired,
    malformed, or credential-bearing grant — or a missing broker — halts the run
    in ``halted_grant_denied`` with a structured ``grant_denied`` event.
    """

    validation = loop_validate(spec)
    if not validation.ok or validation.normalized is None:
        raise WorkflowValidationError(validation)

    normalized = validation.normalized
    brakes = _brakes(normalized)
    max_actions = int(brakes["max_steps"])
    now = _utc_now_iso()
    rid = run_id or f"loop_{validation.def_hash[:8]}_{uuid.uuid4().hex[:12]}"
    status = LoopRunStatus(
        run_id=rid,
        state="planned",
        iterations=0,
        created_at=now,
        updated_at=now,
        def_hash=validation.def_hash,
        events=[],
    )
    _record_event(status, normalized, store, on_event, "planned", "planned", 0, "loop planned")

    def halt(state: LoopState, reason: str) -> None:
        _halt(status, normalized, store, on_event, state, reason)

    started = time.monotonic()
    last_signal: Optional[str] = None
    repeated_signal_count = 0
    total_cost = 0.0
    action_count = 0

    while True:
        if _time_exceeded(started, brakes):
            halt("halted_time_cap", "max_wall_seconds exceeded before sensing")
            break

        status.iterations += 1
        iteration = status.iterations
        status.state = "sensing"
        _record_event(status, normalized, store, on_event, "sensing", "sensing", iteration, "sensor started")
        sensor_result = _read_sensor_with_noise_retry(
            sensor,
            context=_context(
                normalized,
                inputs,
                status,
                latest=status.sensor_results[-1] if status.sensor_results else None,
                started=started,
                brakes=brakes,
                action_count=action_count,
                total_cost=total_cost,
            ),
            max_retries=int(brakes["max_sensor_retries"]),
            record_event=lambda kind, state, item_iteration, summary, **extra: _record_event(
                status, normalized, store, on_event, kind, state, item_iteration, summary, **extra
            ),
        )
        if isinstance(sensor_result, Exception):
            halt("halted_sensor_error", str(sensor_result))
            break

        status.sensor_results.append(sensor_result)
        _record_event(
            status,
            normalized,
            store,
            on_event,
            "sensor_result",
            "verifying",
            iteration,
            sensor_result.summary,
            signal_key=sensor_result.signal_key,
            converged=sensor_result.converged,
        )

        if _time_exceeded(started, brakes):
            halt("halted_time_cap", "max_wall_seconds exceeded during sensing")
            break

        if sensor_result.converged:
            status.state = "converged"
            status.halted_reason = None
            status.report = _report(status, "converged_by_sensor")
            _record_event(status, normalized, store, on_event, "converged", "converged", iteration, sensor_result.summary)
            break

        if sensor_result.signal_key == last_signal:
            repeated_signal_count += 1
        else:
            last_signal = sensor_result.signal_key
            repeated_signal_count = 1

        if iteration > 1 and repeated_signal_count >= int(brakes["max_repeated_signal"]):
            halt(
                "halted_stalled",
                f"sensor signal {sensor_result.signal_key!r} repeated {repeated_signal_count} times",
            )
            break

        if actuator is None:
            halt("halted_actuator_error", "non-converged loop has no actuator")
            break

        if action_count >= max_actions:
            halt("halted_step_cap", f"max_steps {max_actions} exhausted without convergence")
            break

        if _time_exceeded(started, brakes):
            halt("halted_time_cap", "max_wall_seconds exceeded before acting")
            break

        status.state = "acting"
        _record_event(status, normalized, store, on_event, "acting", "acting", iteration, "actuator started")
        try:
            action = actuator(
                _context(
                    normalized,
                    inputs,
                    status,
                    latest=sensor_result,
                    started=started,
                    brakes=brakes,
                    action_count=action_count,
                    total_cost=total_cost,
                )
            )
            if not isinstance(action, dict):
                raise TypeError(f"actuator returned {type(action).__name__}, expected dict")
            action_cost = _action_cost(action)
            suspension = _actuator_suspension(action)
        except Exception as exc:  # pragma: no cover - exact exception type is caller-owned
            halt("halted_actuator_error", f"{type(exc).__name__}: {exc}")
            break

        action_count += 1
        recorded_action = _redact_grant_envelopes(action)
        status.actuator_results.append(recorded_action)
        _record_event(status, normalized, store, on_event, "actuator_result", "verifying", iteration, "actuator completed", result=recorded_action)

        if _time_exceeded(started, brakes):
            halt("halted_time_cap", "max_wall_seconds exceeded during acting")
            break

        total_cost += action_cost
        if brakes.get("max_cost") is not None and total_cost > float(brakes["max_cost"]):
            halt("halted_budget_cap", f"cost {total_cost} exceeded max_cost {brakes['max_cost']}")
            break

        grant_denial = _handle_actuator_grant(
            action,
            status,
            normalized,
            store,
            on_event,
            iteration,
            grant_broker=grant_broker,
            grant_store=grant_store,
            suspension=suspension,
        )
        if grant_denial is not None:
            halt("halted_grant_denied", grant_denial)
            break

        if suspension is not None:
            state, convergence_risk, request = suspension
            status.state = state
            status.halted_reason = None
            status.report = _report(status, convergence_risk)
            _record_event(
                status,
                normalized,
                store,
                on_event,
                state,
                state,
                iteration,
                str(request.get("summary") or action.get("summary") or state),
                request=request,
            )
            break

    status.updated_at = _utc_now_iso()
    if not status.report:
        status.report = _report(status, "not_converged")
    _persist_status(store, status)
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
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) or value < 0:
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
    record_event: Callable[..., LoopEvent],
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
        record_event(
            "sensor_noise_retry",
            "sensing",
            context.get("iteration", 0),
            result.summary,
            signal_key=result.signal_key,
            attempt=attempts,
        )


def _normalize_sensor_result(value: LoopSensorResult | dict[str, Any]) -> LoopSensorResult:
    if isinstance(value, LoopSensorResult):
        _validate_sensor_result(value)
        return value
    if not isinstance(value, dict):
        raise TypeError(f"sensor returned {type(value).__name__}, expected dict")
    signal_key = value.get("signal_key")
    if not isinstance(signal_key, str) or not signal_key:
        raise ValueError("sensor result requires non-empty signal_key")
    evidence = value.get("evidence", [])
    if not isinstance(evidence, list):
        raise ValueError("sensor result evidence must be a list")
    converged = value.get("converged", False)
    if not isinstance(converged, bool):
        raise ValueError("sensor result converged must be a boolean")
    retryable_noise = value.get("retryable_noise", False)
    if not isinstance(retryable_noise, bool):
        raise ValueError("sensor result retryable_noise must be a boolean")
    return LoopSensorResult(
        converged=converged,
        signal_key=signal_key,
        summary=str(value["summary"]) if value.get("summary") is not None else signal_key,
        evidence=[e for e in evidence if isinstance(e, dict)],
        retryable_noise=retryable_noise,
        next_hint=str(value["next_hint"]) if value.get("next_hint") is not None else None,
    )


def _validate_sensor_result(value: LoopSensorResult) -> None:
    if not isinstance(value.converged, bool):
        raise ValueError("sensor result converged must be a boolean")
    if not isinstance(value.retryable_noise, bool):
        raise ValueError("sensor result retryable_noise must be a boolean")
    if not isinstance(value.signal_key, str) or not value.signal_key:
        raise ValueError("sensor result requires non-empty signal_key")
    if not isinstance(value.evidence, list):
        raise ValueError("sensor result evidence must be a list")
    if not all(isinstance(item, dict) for item in value.evidence):
        raise ValueError("sensor result evidence entries must be objects")


def _context(
    spec: dict[str, Any],
    inputs: Optional[dict[str, Any]],
    status: LoopRunStatus,
    *,
    latest: Optional[LoopSensorResult],
    started: float,
    brakes: dict[str, Any],
    action_count: int,
    total_cost: float,
) -> dict[str, Any]:
    max_actions = int(brakes["max_steps"])
    max_cost = brakes.get("max_cost")
    remaining_cost = None if max_cost is None else max(0.0, float(max_cost) - total_cost)
    return {
        "run_id": status.run_id,
        "loop_name": spec.get("name"),
        "setpoint": spec.get("setpoint", {}),
        "inputs": dict(inputs or {}),
        "iteration": status.iterations,
        "latest_sensor": latest.as_dict() if latest else None,
        "limits": {
            "max_actions": max_actions,
            "action_count": action_count,
            "remaining_actions": max(0, max_actions - action_count),
            "max_wall_seconds": brakes.get("max_wall_seconds"),
            "remaining_wall_seconds": _remaining_wall_seconds(started, brakes),
            "deadline_monotonic": _deadline_monotonic(started, brakes),
            "max_cost": max_cost,
            "total_cost": total_cost,
            "remaining_cost": remaining_cost,
        },
        "history": {
            "sensor_results": [r.as_dict() for r in status.sensor_results],
            "actuator_results": list(status.actuator_results),
            "events": list(status.events),
        },
        "grants": [copy.deepcopy(g) for g in status.grants],
        "handoff": {
            "prompt": latest.next_hint if latest else None,
            "expected_return": {
                "artifacts": "list of changed files, PR/check/session handles, or other evidence",
                "cost": "optional non-negative numeric cost for budget brakes",
                "wait": "optional {id|token|kind, summary?, ...} to suspend in waiting_for_event",
                "approval_request": "optional {id|token|kind, summary?, choices?, ...} to suspend in waiting_for_approval",
                "grant_request": "optional {scope[], side_effect_class, subject, reason, ttl_seconds, requested_by?} to request a scoped session grant (no secrets)",
                "grant": "optional previously-issued grant dict to re-validate before reuse; include grant_action to assert scope",
                "summary": "bounded action summary; success still requires a later sensor result",
            },
        },
    }


def _time_exceeded(started: float, brakes: dict[str, Any]) -> bool:
    remaining = _remaining_wall_seconds(started, brakes)
    return remaining is not None and remaining <= 0


def _deadline_monotonic(started: float, brakes: dict[str, Any]) -> Optional[float]:
    max_wall = brakes.get("max_wall_seconds")
    return None if max_wall is None else started + float(max_wall)


def _remaining_wall_seconds(started: float, brakes: dict[str, Any]) -> Optional[float]:
    deadline = _deadline_monotonic(started, brakes)
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())


def _action_cost(action: dict[str, Any]) -> float:
    if "cost" not in action or action["cost"] is None:
        return 0.0
    value = action["cost"]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("actuator cost must be a non-negative number")
    cost = float(value)
    if not math.isfinite(cost) or cost < 0:
        raise ValueError("actuator cost must be a non-negative number")
    return cost


def _actuator_suspension(action: dict[str, Any]) -> Optional[tuple[LoopState, str, dict[str, Any]]]:
    wait = action.get("wait")
    approval = action.get("approval_request")
    if wait is not None and approval is not None:
        raise ValueError("actuator result cannot request both wait and approval")
    if wait is not None:
        if not isinstance(wait, dict):
            raise ValueError("actuator wait must be an object")
        _validate_request_identity(wait, "wait")
        return "waiting_for_event", "waiting_for_event", copy.deepcopy(wait)
    if approval is not None:
        if not isinstance(approval, dict):
            raise ValueError("actuator approval_request must be an object")
        _validate_request_identity(approval, "approval_request")
        return "waiting_for_approval", "waiting_for_approval", copy.deepcopy(approval)
    return None


def _validate_request_identity(request: dict[str, Any], label: str) -> None:
    ident = request.get("id") or request.get("token") or request.get("kind")
    if not isinstance(ident, str) or not ident:
        raise ValueError(f"actuator {label} requires non-empty id, token, or kind")


def _redact_grant_envelopes(action: dict[str, Any]) -> dict[str, Any]:
    """Mask credential-shaped values before journaling grant-bearing actions.

    For grant-bearing actuator results, redact the whole action so top-level
    smuggling attempts cannot leak before the grant handler rejects them. Plain
    wait/approval actions without a grant envelope are left intact because their
    identity tokens are product data rather than grant authority.
    """

    if "grant_request" not in action and "grant" not in action:
        return action
    return redact_credentials(action)


def _handle_actuator_grant(
    action: dict[str, Any],
    status: LoopRunStatus,
    spec: dict[str, Any],
    store: Optional[LoopRunStore],
    on_event: Optional[LoopEventSink],
    iteration: int,
    *,
    grant_broker: Optional[GrantBroker],
    grant_store: Optional[GrantStore],
    suspension: Optional[tuple[LoopState, str, dict[str, Any]]],
) -> Optional[str]:
    """Resolve/validate a scoped grant envelope; fail closed by returning a reason.

    Returns ``None`` when there is no grant work or the grant is granted/valid
    (in which case the grant is recorded in ``status.grants`` and an event is
    emitted). Returns a halt reason string for any denied, expired, malformed, or
    credential-bearing grant so the controller can halt in ``halted_grant_denied``.

    Denied events never echo the offending payload — only a credential-free code
    and reason — so a smuggled secret is rejected without being journaled.
    """

    has_request = action.get("grant_request") is not None
    has_handle = action.get("grant") is not None
    if not has_request and not has_handle:
        return None

    def denied(code: str, reason: str) -> str:
        _record_event(
            status,
            spec,
            store,
            on_event,
            "grant_denied",
            "halted_grant_denied",
            iteration,
            reason,
            grant_code=code,
        )
        return reason

    if has_request and has_handle:
        return denied("malformed", "actuator result cannot combine grant_request and grant")
    if suspension is not None:
        return denied("malformed", "grant envelope cannot combine with wait/approval suspension")
    credential_scan = {key: value for key, value in action.items() if key not in ("wait", "approval_request")}
    offender = find_raw_credential(credential_scan)
    if offender is not None:
        return denied("malformed", f"actuator grant envelope must not carry a raw credential ({offender!r})")

    if has_request:
        envelope = action["grant_request"]
        if not isinstance(envelope, dict):
            return denied("malformed", "actuator grant_request must be an object")
        offender = find_raw_credential(envelope)
        if offender is not None:
            return denied("malformed", f"grant_request must not carry a raw credential ({offender!r})")
        try:
            req = request_grant(
                scope=envelope.get("scope"),
                side_effect_class=envelope.get("side_effect_class"),
                subject=envelope.get("subject"),
                reason=envelope.get("reason"),
                requested_by=envelope.get("requested_by") or spec.get("name") or "loop_actuator",
                ttl_seconds=envelope.get("ttl_seconds"),
                audit=_grant_audit(envelope.get("audit"), status, spec, iteration),
                request_id=envelope.get("request_id"),
            )
        except GrantError as exc:
            return denied("malformed", f"malformed grant_request: {exc}")
        decision = resolve_grant(grant_broker, req, store=grant_store)
        if not decision.granted or decision.grant is None:
            return denied(decision.code, f"grant denied ({decision.code}): {decision.reason}")
        grant_dict = decision.grant.to_dict()
        status.grants.append(grant_dict)
        _record_event(
            status,
            spec,
            store,
            on_event,
            "grant_issued",
            "acting",
            iteration,
            f"grant {decision.grant.grant_id} issued for {decision.grant.subject}",
            grant=grant_dict,
            grant_code=decision.code,
        )
        return None

    handle = action["grant"]
    grant_action = action.get("grant_action")
    validation = validate_grant(handle, action=grant_action if isinstance(grant_action, str) else None)
    if not validation.ok or validation.grant is None:
        return denied(validation.code, f"grant invalid ({validation.code}): {validation.reason}")
    grant_dict = validation.grant.to_dict()
    status.grants.append(grant_dict)
    _record_event(
        status,
        spec,
        store,
        on_event,
        "grant_validated",
        "acting",
        iteration,
        f"grant {validation.grant.grant_id} re-validated",
        grant=grant_dict,
        grant_code=validation.code,
    )
    return None


def _grant_audit(
    envelope_audit: Any,
    status: LoopRunStatus,
    spec: dict[str, Any],
    iteration: int,
) -> dict[str, Any]:
    reserved = {
        "run_id": status.run_id,
        "def_hash": status.def_hash,
        "loop_name": spec.get("name"),
        "iteration": iteration,
    }
    audit: dict[str, Any] = {}
    if isinstance(envelope_audit, dict):
        audit.update({key: value for key, value in envelope_audit.items() if key not in reserved})
    audit.update(reserved)
    return audit


def _halt(
    status: LoopRunStatus,
    spec: dict[str, Any],
    store: Optional[LoopRunStore],
    on_event: Optional[LoopEventSink],
    state: LoopState,
    reason: str,
) -> None:
    status.state = state
    status.halted_reason = reason
    _record_event(status, spec, store, on_event, "halted", state, status.iterations, reason)


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


def _record_event(
    status: LoopRunStatus,
    spec: dict[str, Any],
    store: Optional[LoopRunStore],
    on_event: Optional[LoopEventSink],
    kind: str,
    state: LoopState,
    iteration: int,
    summary: str,
    **extra: Any,
) -> LoopEvent:
    event = _event(kind, state, iteration, summary, **extra)
    event["run_id"] = status.run_id
    event["def_hash"] = status.def_hash
    event["loop_name"] = spec.get("name")
    event["event_index"] = len(status.events)
    status.events.append(event)
    _persist_status(store, status)
    if on_event is not None:
        on_event(copy.deepcopy(event), status)
    return event


def _persist_status(store: Optional[LoopRunStore], status: LoopRunStatus) -> None:
    if store is not None:
        store.save_status(status)


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


def _safe_run_id(run_id: str) -> str:
    if not run_id or not isinstance(run_id, str) or not _identifier_safe(run_id):
        raise ValueError("loop run_id must be identifier-safe")
    return run_id


def _identifier_safe(value: str) -> bool:
    return all(c.isalnum() or c in "._-" for c in value)


def _utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _e(code: str, message: str, pointer: str) -> Diagnostic:
    return Diagnostic(severity="error", code=code, message=message, pointer=pointer)
