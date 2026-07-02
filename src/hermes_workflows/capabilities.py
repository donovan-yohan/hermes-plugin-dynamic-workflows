"""Generic host-owned capability registry for workflow scripts.

Dynamic Workflows core must not grow bespoke ``github_*`` / ``relay_*`` /
``ath_*`` primitives. This module provides the generic seam instead: a workflow
script may request a named capability, and the parent/host decides whether that
name, side-effect class, approval, and bounded result are allowed for this run.

The registry does not execute shell commands itself. Hosts may register CLI/tool
handlers behind explicit names, but the controller only sees a credential-free,
JSON-safe, bounded result.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .errors import CapabilityDenied
from .grants import REDACTED, SIDE_EFFECT_CLASSES, find_raw_credential, redact_credentials

__all__ = [
    "CapabilityHandler",
    "WorkflowCapability",
    "CapabilityPolicy",
    "CapabilityRegistry",
    "UnknownWorkflowCapability",
    "find_capability_request_credential",
    "normalize_capability_name",
    "safe_capability_metadata_value",
]

CapabilityHandler = Callable[[dict[str, Any]], dict[str, Any] | Any]


class UnknownWorkflowCapability(CapabilityDenied):
    """Raised when a workflow asks for a capability the host did not register."""

    def __init__(self, name: str) -> None:
        super().__init__(f"unknown workflow capability {name!r}", code="unknown_capability")


@dataclass(frozen=True)
class WorkflowCapability:
    """Registered host-owned capability callable.

    ``side_effect_class`` is the maximum side effect this handler may perform.
    Per-run :class:`CapabilityPolicy` may further restrict it. A handler receives
    a context object with ``name``, ``input``, ``label``, ``run`` metadata, and the
    registered ``side_effect_class``. It must return JSON-safe, credential-free
    data; the registry redacts/limits defensively before returning to the script.
    """

    name: str
    handler: CapabilityHandler
    side_effect_class: str = "read_only"
    description: str = ""
    replayable: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", normalize_capability_name(self.name))
        if self.side_effect_class not in SIDE_EFFECT_CLASSES:
            raise ValueError(
                f"unknown side_effect_class {self.side_effect_class!r}; expected one of {', '.join(SIDE_EFFECT_CLASSES)}"
            )


@dataclass(frozen=True)
class CapabilityPolicy:
    """Per-run policy for generic workflow-script capabilities.

    Default policy allows only registered ``read_only`` capabilities. Mutating or
    session-control capabilities require an approval id unless the host opts out
    by changing ``approval_required_classes``. Bounds are applied to returned
    stdout/stderr/summary/error/evidence strings before they can hit run state.
    """

    allowed_names: Optional[tuple[str, ...]] = None
    allowed_side_effect_classes: tuple[str, ...] = ("read_only",)
    approval_required_classes: tuple[str, ...] = ("session_launch", "session_control", "external_write")
    approved_approval_ids: tuple[str, ...] = ()
    max_result_bytes: int = 16_384
    max_stream_bytes: int = 4_096

    def __post_init__(self) -> None:
        if self.allowed_names is not None:
            object.__setattr__(self, "allowed_names", tuple(normalize_capability_name(n) for n in self.allowed_names))
        for cls in self.allowed_side_effect_classes + self.approval_required_classes:
            if cls not in SIDE_EFFECT_CLASSES:
                raise ValueError(f"unknown side_effect_class {cls!r}; expected one of {', '.join(SIDE_EFFECT_CLASSES)}")
        if self.max_result_bytes < 256:
            raise ValueError("max_result_bytes must be at least 256")
        if self.max_stream_bytes < 0:
            raise ValueError("max_stream_bytes must be non-negative")


@dataclass
class CapabilityRegistry:
    """In-memory registry of host-owned external capabilities."""

    _capabilities: dict[str, WorkflowCapability] = field(default_factory=dict)

    def register(
        self,
        name: str,
        handler: CapabilityHandler,
        *,
        side_effect_class: str = "read_only",
        description: str = "",
        replayable: bool = False,
        replace: bool = False,
    ) -> WorkflowCapability:
        capability = WorkflowCapability(
            name=name,
            handler=handler,
            side_effect_class=side_effect_class,
            description=description,
            replayable=replayable,
        )
        if capability.name in self._capabilities and not replace:
            raise ValueError(f"workflow capability {capability.name!r} is already registered")
        self._capabilities[capability.name] = capability
        return capability

    def get(self, name: str) -> WorkflowCapability:
        normalized = normalize_capability_name(name)
        try:
            return self._capabilities[normalized]
        except KeyError as exc:
            raise UnknownWorkflowCapability(normalized) from exc

    def list(self) -> list[dict[str, Any]]:
        """Return metadata only; handlers are intentionally not exposed."""

        return [
            {
                "name": cap.name,
                "side_effect_class": cap.side_effect_class,
                "description": cap.description,
                "replayable": cap.replayable,
            }
            for cap in sorted(self._capabilities.values(), key=lambda item: item.name)
        ]

    def run(
        self,
        name: str,
        request: dict[str, Any],
        *,
        policy: Optional[CapabilityPolicy] = None,
        run_context: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        policy = policy or CapabilityPolicy()
        capability = self.get(name)
        payload = request.get("input") if isinstance(request.get("input"), dict) else {}
        offender = find_capability_request_credential({**request, "input": payload})
        if offender is not None:
            raise CapabilityDenied(
                f"capability request contains credential-shaped field {offender!r}", code="capability_credential"
            )
        _enforce_policy(capability, request, policy)

        context = {
            "name": capability.name,
            "input": payload,
            "label": request.get("label"),
            "approval_id": request.get("approval_id"),
            "side_effect_class": capability.side_effect_class,
            "run": dict(run_context or {}),
        }
        try:
            raw = capability.handler(context)
        except CapabilityDenied:
            raise
        except KeyboardInterrupt:
            raise  # let a genuine operator interrupt propagate.
        except BaseException as exc:  # noqa: BLE001 - host handlers are contained at the capability
            # boundary, including a deliberate SystemExit/GeneratorExit: a handler fault is a
            # contract violation of *this* handler's own behavior, not a transient dispatch
            # attempt against a live runner, so it stays retryable=False (issue #103) — unlike
            # the broker's runner-exception containment in vm.py, which classifies
            # retryable=True. Catching BaseException (not just Exception) keeps that
            # distinction: without it, a handler-raised SystemExit would escape this boundary
            # and fall into the broker's containment instead, misclassified as retryable=True.
            raise CapabilityDenied(
                f"capability handler raised {type(exc).__name__}", code="capability_handler_error"
            ) from exc
        normalized = _normalize_result(raw)
        normalized.setdefault("side_effect_class", capability.side_effect_class)
        normalized = redact_credentials(normalized)
        return _bound_result(normalized, policy)


def normalize_capability_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("capability name must be a non-empty string")
    normalized = name.strip()
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-/")
    if any(ch not in allowed for ch in normalized):
        raise ValueError("capability name may contain only letters, numbers, '.', '_', ':', '-', '/'")
    return normalized


def find_capability_request_credential(request: dict[str, Any]) -> Optional[str]:
    """Return a credential marker anywhere in capability request metadata.

    ``label`` is journaled and ``approval_id`` is forwarded to the handler, so
    both are part of the credential-smuggling boundary; checking only ``input``
    would let a script leak secrets into metadata-only run state.
    """

    offender = find_raw_credential(
        {
            "input": request.get("input"),
            "label": request.get("label"),
            "approval_id": request.get("approval_id"),
            "schema": request.get("schema"),
        }
    )
    if offender is not None:
        return offender
    for key in ("label", "approval_id"):
        value = request.get(key)
        if isinstance(value, str) and _string_has_credential_marker(value):
            return key
    return None


def safe_capability_metadata_value(value: Any) -> Any:
    """Redact capability metadata before it enters journals/events."""

    redacted = redact_credentials(value)
    if isinstance(redacted, str) and _string_has_credential_marker(redacted):
        return REDACTED
    return redacted


def _enforce_policy(capability: WorkflowCapability, request: dict[str, Any], policy: CapabilityPolicy) -> None:
    if policy.allowed_names is not None and capability.name not in policy.allowed_names:
        raise CapabilityDenied(f"capability {capability.name!r} is not allowed by this run policy", code="capability_denied")
    if capability.side_effect_class not in policy.allowed_side_effect_classes:
        raise CapabilityDenied(
            f"capability {capability.name!r} side_effect_class {capability.side_effect_class!r} is not allowed",
            code="capability_side_effect_denied",
        )
    if capability.side_effect_class in policy.approval_required_classes:
        approval_id = request.get("approval_id")
        if not isinstance(approval_id, str) or approval_id not in policy.approved_approval_ids:
            raise CapabilityDenied(
                f"capability {capability.name!r} requires an approved approval_id",
                code="capability_approval_required",
            )


def _normalize_result(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        out = dict(raw)
    else:
        out = {"output": raw}
    if "ok" not in out:
        out["ok"] = True
    return out


def _bound_result(result: dict[str, Any], policy: CapabilityPolicy) -> dict[str, Any]:
    bounded = _clip_stream_fields(result, policy.max_stream_bytes)
    encoded = _json_bytes(bounded)
    if len(encoded) <= policy.max_result_bytes:
        return bounded
    # If evidence/output is still too large after clipping well-known stream
    # fields, fail closed rather than journaling or returning a surprise blob.
    raise CapabilityDenied(
        f"capability result exceeds max_result_bytes ({policy.max_result_bytes})",
        code="capability_result_too_large",
    )


def _clip_stream_fields(value: Any, max_bytes: int) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if isinstance(key, str) and key in {"stdout", "stderr", "summary", "error"} and isinstance(item, str):
                clipped, truncated = _clip_string(item, max_bytes)
                out[key] = clipped
                if truncated:
                    out[f"{key}_truncated"] = True
            else:
                out[key] = _clip_stream_fields(item, max_bytes)
        return out
    if isinstance(value, list):
        return [_clip_stream_fields(item, max_bytes) for item in value]
    return value


def _clip_string(text: str, max_bytes: int) -> tuple[str, bool]:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text, False
    clipped = encoded[:max_bytes].decode("utf-8", errors="ignore")
    return clipped, True


def _json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    except TypeError as exc:
        raise CapabilityDenied(f"capability result is not JSON-safe: {exc}", code="capability_result_invalid") from exc


def _string_has_credential_marker(value: str) -> bool:
    lowered = value.strip().lower()
    markers = (
        "authorization:",
        "bearer ",
        "basic ",
        "cookie=",
        "session=",
        "set-cookie:",
        "password=",
        "passwd=",
        "secret=",
        "token=",
        "api_key=",
        "apikey=",
        "access_key=",
        "private_key=",
        "ghp_",
        "gho_",
        "ghu_",
        "ghs_",
        "ghr_",
        "github_pat_",
        "glpat-",
        "sk-",
        "xoxb-",
        "xoxp-",
        "xoxa-",
    )
    return any(marker in lowered for marker in markers)
