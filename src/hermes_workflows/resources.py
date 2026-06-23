"""Backend-neutral workflow resource/finalizer models.

This module is the first issue #52 slice. Dynamic Workflows owns the
lifecycle contract: a run can declare credential-free resources it owns and
finalizers that should clean them up on terminal paths. Backend adapters still
own the actual ATH/Relay/process/container cleanup work behind an injected
runner.
"""

from __future__ import annotations

import copy
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Optional, Protocol, runtime_checkable

from .grants import REDACTED, find_raw_credential, redact_credentials

__all__ = [
    "FINALIZER_POLICIES",
    "FINALIZER_TRIGGERS",
    "FINALIZER_RESULT_STATUSES",
    "WorkflowResource",
    "ResourceFinalizer",
    "FinalizerResult",
    "ResourceFinalizerCallable",
    "ResourceFinalizerHandler",
    "ResourceFinalizerRegistry",
    "UnknownResourceFinalizerAction",
    "normalize_resource_envelopes",
    "run_resource_finalizers",
    "has_required_finalizer_failure",
]

FINALIZER_TRIGGERS: tuple[str, ...] = (
    "success",
    "failure",
    "timeout",
    "cancelled",
    "superseded",
    "manual",
)

FINALIZER_POLICIES: tuple[str, ...] = (
    "required",
    "best_effort",
    "preserve_only",
    "manual_approval_required",
)

FINALIZER_RESULT_STATUSES: tuple[str, ...] = (
    "succeeded",
    "failed",
    "skipped",
    "preserved",
    "approval_required",
)

_EMBEDDED_CREDENTIAL_RE = re.compile(
    r"(?i)(bearer\s+[^\s]+|basic\s+[^\s]+|gh[pousr]_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+|glpat-[A-Za-z0-9_-]+|sk-[A-Za-z0-9_-]+|xox[abp]-[A-Za-z0-9_-]+)"
)


@dataclass(frozen=True)
class ResourceFinalizer:
    """Credential-free cleanup action declaration for one owned resource."""

    finalizer_id: str
    action: str
    when: tuple[str, ...] = ("success", "failure", "timeout", "cancelled", "superseded")
    policy: Literal["required", "best_effort", "preserve_only", "manual_approval_required"] = "best_effort"
    args: dict[str, Any] = field(default_factory=dict)
    verification: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["id"] = data.pop("finalizer_id")
        data["when"] = list(self.when)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ResourceFinalizer":
        if not isinstance(data, dict):
            raise ValueError("resource finalizer must be an object")
        offender = find_raw_credential(data)
        if offender is not None:
            raise ValueError(f"resource finalizer must not carry a raw credential ({offender!r})")
        ident = data.get("id") or data.get("finalizer_id")
        if not isinstance(ident, str) or not _identifier_safe(ident):
            raise ValueError("resource finalizer requires identifier-safe id")
        action = data.get("action")
        if not isinstance(action, str) or not _dotted_identifier_safe(action):
            raise ValueError("resource finalizer requires identifier-safe action")
        when = _normalize_when(data.get("when"))
        policy = data.get("policy", "best_effort")
        if policy not in FINALIZER_POLICIES:
            raise ValueError(f"unknown resource finalizer policy {policy!r}")
        args = data.get("args", {})
        if not isinstance(args, dict):
            raise ValueError("resource finalizer args must be an object")
        verification = data.get("verification", {})
        if not isinstance(verification, dict):
            raise ValueError("resource finalizer verification must be an object")
        return cls(
            finalizer_id=ident,
            action=action,
            when=when,
            policy=policy,  # type: ignore[arg-type]
            args=copy.deepcopy(args),
            verification=copy.deepcopy(verification),
        )


@dataclass(frozen=True)
class WorkflowResource:
    """Credential-free handle for a runtime resource owned by a workflow run."""

    resource_id: str
    kind: str
    owner: dict[str, Any]
    handle: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    finalizers: tuple[ResourceFinalizer, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.resource_id,
            "kind": self.kind,
            "owner": copy.deepcopy(self.owner),
            "handle": copy.deepcopy(self.handle),
            "metadata": copy.deepcopy(self.metadata),
            "finalizers": [f.to_dict() for f in self.finalizers],
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        default_owner: Optional[dict[str, Any]] = None,
    ) -> "WorkflowResource":
        if not isinstance(data, dict):
            raise ValueError("workflow resource must be an object")
        offender = find_raw_credential(data)
        if offender is not None:
            raise ValueError(f"workflow resource must not carry a raw credential ({offender!r})")
        ident = data.get("id") or data.get("resource_id")
        if not isinstance(ident, str) or not _identifier_safe(ident):
            raise ValueError("workflow resource requires identifier-safe id")
        kind = data.get("kind")
        if not isinstance(kind, str) or not _dotted_identifier_safe(kind):
            raise ValueError("workflow resource requires identifier-safe kind")
        owner = data.get("owner", {})
        if not isinstance(owner, dict):
            raise ValueError("workflow resource owner must be an object")
        # Actuator-supplied owner metadata may add useful product context
        # (issue, PR, repo), but controller provenance is reserved. The run id,
        # loop name, and iteration come from ``default_owner`` and must not be
        # forgeable by the resource envelope that gets persisted/audited.
        merged_owner = copy.deepcopy(owner)
        merged_owner.update(copy.deepcopy(default_owner or {}))
        handle = data.get("handle", {})
        if not isinstance(handle, dict):
            raise ValueError("workflow resource handle must be an object")
        metadata = data.get("metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError("workflow resource metadata must be an object")
        finalizers_raw = data.get("finalizers", [])
        if not isinstance(finalizers_raw, list):
            raise ValueError("workflow resource finalizers must be a list")
        finalizers = _dedupe_finalizers(ResourceFinalizer.from_dict(item) for item in finalizers_raw)
        return cls(
            resource_id=ident,
            kind=kind,
            owner=merged_owner,
            handle=copy.deepcopy(handle),
            metadata=copy.deepcopy(metadata),
            finalizers=finalizers,
        )


@dataclass(frozen=True)
class FinalizerResult:
    """Auditable outcome of one finalizer attempt."""

    resource_id: str
    finalizer_id: str
    action: str
    trigger: str
    policy: str
    status: Literal["succeeded", "failed", "skipped", "preserved", "approval_required"]
    summary: str
    evidence: list[dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@runtime_checkable
class ResourceFinalizerCallable(Protocol):
    """Backend adapter that performs one declared resource finalizer."""

    def __call__(self, context: dict[str, Any]) -> dict[str, Any]:
        ...


@runtime_checkable
class ResourceFinalizerHandler(Protocol):
    """Handler for one concrete finalizer action, e.g. an ATH/Relay adapter."""

    def __call__(self, context: dict[str, Any]) -> dict[str, Any]:
        ...


class UnknownResourceFinalizerAction(LookupError):
    """Raised when no adapter is registered for a declared finalizer action."""


class ResourceFinalizerRegistry:
    """Action-string dispatch for backend-specific resource finalizer adapters.

    The workflow core stays backend-neutral: resources declare dotted action
    strings such as ``ath.listener.retire`` or ``relay.session.close`` and a
    host/application registers handlers for the actions it supports. The
    registry is itself a ``ResourceFinalizerCallable`` and can be passed as
    ``loop_run(..., finalizer=registry)``.
    """

    def __init__(self, handlers: Optional[dict[str, ResourceFinalizerHandler]] = None) -> None:
        self._handlers: dict[str, ResourceFinalizerHandler] = {}
        for action, handler in (handlers or {}).items():
            self.register(action, handler)

    def register(
        self,
        action: str,
        handler: ResourceFinalizerHandler,
        *,
        replace: bool = False,
    ) -> "ResourceFinalizerRegistry":
        if not isinstance(action, str) or not _dotted_identifier_safe(action):
            raise ValueError("resource finalizer action must be identifier-safe")
        if not callable(handler):
            raise TypeError("resource finalizer handler must be callable")
        if action in self._handlers and not replace:
            raise ValueError(f"resource finalizer action already registered: {action}")
        self._handlers[action] = handler
        return self

    def unregister(self, action: str) -> None:
        self._handlers.pop(action, None)

    def actions(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))

    def handler_for(self, action: str) -> Optional[ResourceFinalizerHandler]:
        return self._handlers.get(action)

    def __call__(self, context: dict[str, Any]) -> dict[str, Any]:
        safe_context = context if isinstance(context, dict) else {}
        finalizer = safe_context.get("finalizer")
        action = finalizer.get("action") if isinstance(finalizer, dict) else None
        if not isinstance(action, str):
            raise UnknownResourceFinalizerAction("resource finalizer context missing action")
        handler = self._handlers.get(action)
        if handler is None:
            raise UnknownResourceFinalizerAction(f"no resource finalizer adapter registered for action {action!r}")
        return handler(safe_context)


def normalize_resource_envelopes(
    action: dict[str, Any],
    *,
    run_id: str,
    loop_name: Optional[str],
    iteration: int,
) -> list[WorkflowResource]:
    """Extract credential-free resource declarations from an actuator result."""

    raw_items: list[Any] = []
    if action.get("resource") is not None:
        raw_items.append(action["resource"])
    if action.get("resources") is not None:
        resources = action["resources"]
        if not isinstance(resources, list):
            raise ValueError("actuator resources must be a list")
        raw_items.extend(resources)
    default_owner = {"run_id": run_id, "loop_name": loop_name, "iteration": iteration}
    resources: list[WorkflowResource] = []
    seen: set[str] = set()
    for raw in raw_items:
        resource = WorkflowResource.from_dict(raw, default_owner=default_owner)
        if resource.resource_id in seen:
            continue
        seen.add(resource.resource_id)
        resources.append(resource)
    return resources


def run_resource_finalizers(
    resources: list[WorkflowResource] | tuple[WorkflowResource, ...],
    *,
    trigger: str,
    runner: Optional[ResourceFinalizerCallable],
    run_id: str,
    loop_name: Optional[str],
    existing_results: Optional[list[dict[str, Any]]] = None,
) -> list[FinalizerResult]:
    """Run eligible finalizers once, returning credential-redacted outcomes."""

    if trigger not in FINALIZER_TRIGGERS:
        raise ValueError(f"unknown finalizer trigger {trigger!r}")
    completed = {
        (r.get("resource_id"), r.get("finalizer_id"), r.get("trigger"))
        for r in (existing_results or [])
        if isinstance(r, dict)
    }
    results: list[FinalizerResult] = []
    for resource in resources:
        for finalizer in resource.finalizers:
            key = (resource.resource_id, finalizer.finalizer_id, trigger)
            if key in completed or trigger not in finalizer.when:
                continue
            results.append(
                _run_one_finalizer(
                    resource,
                    finalizer,
                    trigger=trigger,
                    runner=runner,
                    run_id=run_id,
                    loop_name=loop_name,
                )
            )
            completed.add(key)
    return results


def has_required_finalizer_failure(results: list[dict[str, Any] | FinalizerResult]) -> bool:
    for item in results:
        data = item.to_dict() if isinstance(item, FinalizerResult) else item
        if data.get("policy") == "required" and data.get("status") == "failed":
            return True
    return False


def _run_one_finalizer(
    resource: WorkflowResource,
    finalizer: ResourceFinalizer,
    *,
    trigger: str,
    runner: Optional[ResourceFinalizerCallable],
    run_id: str,
    loop_name: Optional[str],
) -> FinalizerResult:
    if finalizer.policy == "preserve_only":
        return FinalizerResult(
            resource_id=resource.resource_id,
            finalizer_id=finalizer.finalizer_id,
            action=finalizer.action,
            trigger=trigger,
            policy=finalizer.policy,
            status="preserved",
            summary=f"resource {resource.resource_id} preserved by policy",
        )
    if finalizer.policy == "manual_approval_required":
        return FinalizerResult(
            resource_id=resource.resource_id,
            finalizer_id=finalizer.finalizer_id,
            action=finalizer.action,
            trigger=trigger,
            policy=finalizer.policy,
            status="approval_required",
            summary=f"resource {resource.resource_id} requires manual cleanup approval",
        )
    if runner is None:
        status: Literal["failed", "skipped"] = "failed" if finalizer.policy == "required" else "skipped"
        return FinalizerResult(
            resource_id=resource.resource_id,
            finalizer_id=finalizer.finalizer_id,
            action=finalizer.action,
            trigger=trigger,
            policy=finalizer.policy,
            status=status,
            summary="no resource finalizer runner configured",
            error="no_runner",
        )
    context = {
        "run_id": run_id,
        "loop_name": loop_name,
        "trigger": trigger,
        "resource": resource.to_dict(),
        "finalizer": finalizer.to_dict(),
    }
    try:
        raw = runner(context)
    except Exception as exc:  # pragma: no cover - backend-owned exception types
        redacted_error = _redact_text(f"{type(exc).__name__}: {exc}")
        return FinalizerResult(
            resource_id=resource.resource_id,
            finalizer_id=finalizer.finalizer_id,
            action=finalizer.action,
            trigger=trigger,
            policy=finalizer.policy,
            status="failed",
            summary=redacted_error,
            error=redacted_error,
        )
    if not isinstance(raw, dict):
        return FinalizerResult(
            resource_id=resource.resource_id,
            finalizer_id=finalizer.finalizer_id,
            action=finalizer.action,
            trigger=trigger,
            policy=finalizer.policy,
            status="failed",
            summary=f"finalizer returned {type(raw).__name__}, expected dict",
            error="malformed_result",
        )
    redacted = _redact_finalizer_payload(raw)
    ok = redacted.get("ok", True)
    if not isinstance(ok, bool):
        return FinalizerResult(
            resource_id=resource.resource_id,
            finalizer_id=finalizer.finalizer_id,
            action=finalizer.action,
            trigger=trigger,
            policy=finalizer.policy,
            status="failed",
            summary="finalizer result ok must be a boolean",
            error="malformed_result",
        )
    evidence = redacted.get("evidence", [])
    if not isinstance(evidence, list):
        evidence = []
    return FinalizerResult(
        resource_id=resource.resource_id,
        finalizer_id=finalizer.finalizer_id,
        action=finalizer.action,
        trigger=trigger,
        policy=finalizer.policy,
        status="succeeded" if ok else "failed",
        summary=str(redacted.get("summary") or ("cleanup succeeded" if ok else "cleanup failed")),
        evidence=[item for item in evidence if isinstance(item, dict)],
        error=str(redacted.get("error")) if redacted.get("error") is not None else None,
    )


def _normalize_when(value: Any) -> tuple[str, ...]:
    if value is None:
        return ("success", "failure", "timeout", "cancelled", "superseded")
    if isinstance(value, str):
        values = (value,)
    elif isinstance(value, (list, tuple)):
        values = tuple(value)
    else:
        raise ValueError("resource finalizer when must be a string, list, or tuple")
    if not values:
        raise ValueError("resource finalizer when must not be empty")
    normalized: list[str] = []
    for item in values:
        if item not in FINALIZER_TRIGGERS:
            raise ValueError(f"unknown resource finalizer trigger {item!r}")
        if item not in normalized:
            normalized.append(item)
    return tuple(normalized)


def _redact_finalizer_payload(payload: Any) -> Any:
    redacted = redact_credentials(payload)
    if isinstance(redacted, dict):
        return {key: _redact_finalizer_payload(value) for key, value in redacted.items()}
    if isinstance(redacted, list):
        return [_redact_finalizer_payload(item) for item in redacted]
    if isinstance(redacted, tuple):
        return tuple(_redact_finalizer_payload(item) for item in redacted)
    if isinstance(redacted, str):
        return _redact_text(redacted)
    return redacted


def _redact_text(text: str) -> str:
    redacted = str(redact_credentials(text))
    return _EMBEDDED_CREDENTIAL_RE.sub(REDACTED, redacted)


def _dedupe_finalizers(finalizers: Any) -> tuple[ResourceFinalizer, ...]:
    deduped: list[ResourceFinalizer] = []
    seen: set[str] = set()
    for finalizer in finalizers:
        if finalizer.finalizer_id in seen:
            continue
        seen.add(finalizer.finalizer_id)
        deduped.append(finalizer)
    return tuple(deduped)


def _identifier_safe(value: str) -> bool:
    return bool(value) and all(c.isalnum() or c in "._-" for c in value)


_dotted_identifier_safe = _identifier_safe
