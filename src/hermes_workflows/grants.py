"""Backend-neutral scoped actuator grants for workflow-managed session control.

This module is the issue #33 slice. A long-running loop controller (see
:mod:`hermes_workflows.loops`) often needs an actuator/backend adapter to *launch*
or *control* a managed agent session — a side effect with real authority. Handing
that adapter a raw shell token or a reused browser cookie is the wrong primitive:
those credentials are ambient, unscoped, non-expiring, and unauditable.

Instead, an adapter requests a **scoped grant**. A grant is an explicit, expiring,
single-purpose authorization with:

* an explicit ``scope`` (the exact actions it permits, e.g. ``session.launch``),
* an explicit ``side_effect_class`` (how dangerous the authorized effect is),
* an explicit expiry (issued/expires timestamps; grants are short-lived),
* audit metadata (who asked, why, which run), and
* an opaque, persistable :class:`GrantHandle` that names the session / work
  context without carrying any reusable secret.

The controller resolves a request through an injected :class:`GrantBroker`. Core
ships :class:`StaticPolicyGrantBroker`, a backend-neutral default that issues
grants from a static allow-policy with **no real authentication** — real auth is a
future backend adapter's job (Relay being one such future backend). Denied,
expired, or malformed grants always fail **closed**: the resolve/validate helpers
return a structured negative decision rather than raising, and the loop runtime
halts the run with a dedicated controller signal.

No network, filesystem auth, shell, Relay, ATH, or Kanban behaviour lives here.
This is pure stdlib and intentionally credential-free: a small guard rejects any
grant payload that smuggles a raw cookie/token/password, so this primitive can
never degrade into browser-cookie reuse.
"""

from __future__ import annotations

import copy
import json
import math
import time
import uuid
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Literal, Optional, Protocol, runtime_checkable

from .errors import GrantError

__all__ = [
    "SIDE_EFFECT_CLASSES",
    "GrantHandle",
    "GrantRequest",
    "SessionGrant",
    "GrantDecision",
    "GrantValidation",
    "GrantBroker",
    "GrantStore",
    "StaticPolicyGrantBroker",
    "InMemoryGrantStore",
    "FileGrantStore",
    "request_grant",
    "resolve_grant",
    "validate_grant",
    "find_raw_credential",
    "redact_credentials",
    "REDACTED",
]

# Placeholder substituted for any credential-shaped value before it is journaled.
REDACTED = "[REDACTED]"

# Generic, backend-neutral severity classes for the authorized side effect. A
# closed set keeps requests honest: an unknown class fails closed rather than
# silently authorizing something the policy never reasoned about.
SIDE_EFFECT_CLASSES: tuple[str, ...] = (
    "read_only",
    "session_launch",
    "session_control",
    "external_write",
)

# Credential-shaped keys that must never appear in a grant request, handle, or
# audit blob. Credential-looking string values are also rejected/redacted for
# common bearer/cookie/token shapes. This is the line that separates a scoped
# grant from raw token / browser-cookie reuse: the authority lives in the
# scope+expiry, never in a smuggled secret.
_FORBIDDEN_CRED_KEYS: frozenset[str] = frozenset(
    {
        "authorization",
        "auth",
        "bearer",
        "api_key",
        "apikey",
        "x-api-key",
        "access_token",
        "refresh_token",
        "auth_token",
        "session_token",
        "id_token",
        "token",
        "set-cookie",
        "cookies",
    }
)

# Substrings that mark a key as credential-shaped regardless of exact spelling.
_FORBIDDEN_CRED_SUBSTRINGS: tuple[str, ...] = (
    "password",
    "passwd",
    "secret",
    "cookie",
    "credential",
    "private_key",
)

# Controller/request-owned audit metadata must not be supplied by broker output.
# Request-owned loop/run metadata is copied back by _sanitize_issued_grant;
# canonical requested_by/reason are restored from the GrantRequest, while
# policy_max_ttl_seconds is treated as broker policy metadata and not persisted.
_RESERVED_AUDIT_KEYS: frozenset[str] = frozenset(
    {
        "requested_by",
        "reason",
        "policy_max_ttl_seconds",
        "run_id",
        "def_hash",
        "loop_name",
        "iteration",
    }
)


# ---------------------------------------------------------------------------
# Value objects.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GrantHandle:
    """Opaque, persistable reference to a managed session and its work context.

    A handle carries **no credentials**. ``handle_ref`` is a backend-issued,
    revocable, scope-bound reference (not a reusable cookie or bearer token).
    ``extra`` holds opaque, credential-free backend metadata.
    """

    backend: str
    session_id: Optional[str] = None
    work_context_id: Optional[str] = None
    handle_ref: Optional[str] = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "session_id": self.session_id,
            "work_context_id": self.work_context_id,
            "handle_ref": self.handle_ref,
            "extra": dict(self.extra),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GrantHandle":
        if not isinstance(data, dict):
            raise GrantError("grant handle must be an object")
        backend = data.get("backend")
        if not isinstance(backend, str) or not backend:
            raise GrantError("grant handle requires non-empty 'backend'")
        extra = data.get("extra", {})
        if not isinstance(extra, dict):
            raise GrantError("grant handle 'extra' must be an object")
        return cls(
            backend=backend,
            session_id=_opt_str(data.get("session_id"), "session_id"),
            work_context_id=_opt_str(data.get("work_context_id"), "work_context_id"),
            handle_ref=_opt_str(data.get("handle_ref"), "handle_ref"),
            extra=dict(extra),
        )


@dataclass(frozen=True)
class GrantRequest:
    """A credential-free ask for a scoped, expiring session-control grant."""

    request_id: str
    scope: tuple[str, ...]
    side_effect_class: str
    subject: str
    reason: str
    requested_by: str
    ttl_seconds: float
    audit: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "scope": list(self.scope),
            "side_effect_class": self.side_effect_class,
            "subject": self.subject,
            "reason": self.reason,
            "requested_by": self.requested_by,
            "ttl_seconds": self.ttl_seconds,
            "audit": dict(self.audit),
        }


@dataclass(frozen=True)
class SessionGrant:
    """An issued scoped grant. Always represents a real authorization.

    Expiry is wall-clock (epoch seconds plus ISO strings) so a persisted grant
    can be re-validated after a process restart without re-deriving the clock.
    """

    grant_id: str
    request_id: str
    scope: tuple[str, ...]
    side_effect_class: str
    subject: str
    issued_at: str
    expires_at: str
    issued_at_epoch: float
    expires_at_epoch: float
    backend: str
    handle: Optional[GrantHandle] = None
    audit: dict[str, Any] = field(default_factory=dict)
    status: Literal["granted", "revoked"] = "granted"

    def to_dict(self) -> dict[str, Any]:
        return {
            "grant_id": self.grant_id,
            "request_id": self.request_id,
            "scope": list(self.scope),
            "side_effect_class": self.side_effect_class,
            "subject": self.subject,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "issued_at_epoch": self.issued_at_epoch,
            "expires_at_epoch": self.expires_at_epoch,
            "backend": self.backend,
            "handle": self.handle.to_dict() if self.handle is not None else None,
            "audit": dict(self.audit or {}),
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SessionGrant":
        if not isinstance(data, dict):
            raise GrantError("grant must be an object")
        for key in ("grant_id", "request_id", "side_effect_class", "subject", "backend"):
            if not isinstance(data.get(key), str) or not data.get(key):
                raise GrantError(f"grant requires non-empty string {key!r}")
        grant_id = _safe_grant_id(data["grant_id"])
        request_id = _normalize_request_id(data["request_id"])
        side_effect_class = data["side_effect_class"]
        if side_effect_class not in SIDE_EFFECT_CLASSES:
            raise GrantError(
                f"unknown side_effect_class {side_effect_class!r}; expected one of {', '.join(SIDE_EFFECT_CLASSES)}"
            )
        scope = _normalize_scope(data.get("scope"))
        issued_epoch = _require_number(data.get("issued_at_epoch"), "issued_at_epoch")
        expires_epoch = _require_number(data.get("expires_at_epoch"), "expires_at_epoch")
        handle_data = data.get("handle")
        handle = GrantHandle.from_dict(handle_data) if handle_data is not None else None
        audit = data.get("audit", {})
        if not isinstance(audit, dict):
            raise GrantError("grant 'audit' must be an object")
        status = data.get("status", "granted")
        if status not in ("granted", "revoked"):
            raise GrantError("grant 'status' must be 'granted' or 'revoked'")
        return cls(
            grant_id=grant_id,
            request_id=request_id,
            scope=scope,
            side_effect_class=side_effect_class,
            subject=data["subject"],
            issued_at=str(data.get("issued_at", "")),
            expires_at=str(data.get("expires_at", "")),
            issued_at_epoch=issued_epoch,
            expires_at_epoch=expires_epoch,
            backend=data["backend"],
            handle=handle,
            audit=dict(audit),
            status=status,
        )


@dataclass(frozen=True)
class GrantDecision:
    """Outcome of asking a broker to issue a grant. ``granted`` is the source of
    truth; ``code`` is a stable machine token; ``reason`` is human/audit text."""

    granted: bool
    code: str
    reason: str
    grant: Optional[SessionGrant] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "granted": self.granted,
            "code": self.code,
            "reason": self.reason,
            "grant": self.grant.to_dict() if self.grant is not None else None,
        }


@dataclass(frozen=True)
class GrantValidation:
    """Fail-closed check of an issued/persisted grant for use right now."""

    ok: bool
    code: str
    reason: str
    grant: Optional[SessionGrant] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "code": self.code,
            "reason": self.reason,
            "grant": self.grant.to_dict() if self.grant is not None else None,
        }


# ---------------------------------------------------------------------------
# Boundaries.
# ---------------------------------------------------------------------------


@runtime_checkable
class GrantBroker(Protocol):
    """Backend boundary that decides and issues grants.

    A real backend (e.g. a future Relay adapter) authenticates and mints a
    revocable session reference here. Core only ships a static-policy default.
    """

    def __call__(self, request: GrantRequest) -> GrantDecision:
        ...


@runtime_checkable
class GrantStore(Protocol):
    """Persistence boundary for issued grants so handles survive a restart."""

    def save_grant(self, grant: SessionGrant) -> None:
        ...

    def get_grant(self, grant_id: str) -> Optional[dict[str, Any]]:
        ...


class InMemoryGrantStore:
    """Small in-process grant store for tests and embedders."""

    def __init__(self) -> None:
        self._grants: dict[str, dict[str, Any]] = {}

    def save_grant(self, grant: SessionGrant) -> None:
        self._grants[grant.grant_id] = copy.deepcopy(grant.to_dict())

    def get_grant(self, grant_id: str) -> Optional[dict[str, Any]]:
        snapshot = self._grants.get(grant_id)
        return None if snapshot is None else copy.deepcopy(snapshot)


class FileGrantStore:
    """Filesystem grant store: ``<root>/<grant_id>.json`` per grant.

    A workflow persists the issued grant here and re-reads it after a restart to
    resume status checks against the same session/work-context handle.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def save_grant(self, grant: SessionGrant) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / f"{_safe_grant_id(grant.grant_id)}.json"
        tmp = self.root / f"{_safe_grant_id(grant.grant_id)}.json.tmp"
        tmp.write_text(json.dumps(grant.to_dict(), sort_keys=True, indent=2) + "\n", encoding="utf-8")
        tmp.replace(path)

    def get_grant(self, grant_id: str) -> Optional[dict[str, Any]]:
        path = self.root / f"{_safe_grant_id(grant_id)}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))


class StaticPolicyGrantBroker:
    """Backend-neutral default broker: issue grants from a static allow-policy.

    There is **no real authentication** here — it exists so the controller,
    docs, and tests have a working, credential-free broker. It clamps the
    requested TTL, rejects out-of-policy scope/side-effect classes, and mints an
    opaque :class:`GrantHandle` (no secrets) referencing the requested subject.
    """

    def __init__(
        self,
        *,
        allowed_scope: Any,
        max_ttl_seconds: float,
        allowed_side_effect_classes: Any = SIDE_EFFECT_CLASSES,
        backend: str = "inproc",
        clock: Optional[Callable[[], float]] = None,
        id_factory: Optional[Callable[[GrantRequest], str]] = None,
    ) -> None:
        if isinstance(allowed_scope, str):
            raise GrantError("allowed_scope must be an iterable of strings, not a single string")
        self.allowed_scope = frozenset(allowed_scope)
        if isinstance(max_ttl_seconds, bool) or not isinstance(max_ttl_seconds, (int, float)):
            raise GrantError("StaticPolicyGrantBroker requires finite numeric max_ttl_seconds")
        if float(max_ttl_seconds) <= 0 or not math.isfinite(float(max_ttl_seconds)):
            raise GrantError("StaticPolicyGrantBroker requires positive finite max_ttl_seconds")
        self.max_ttl_seconds = float(max_ttl_seconds)
        if isinstance(allowed_side_effect_classes, str):
            raise GrantError("allowed_side_effect_classes must be an iterable of strings, not a single string")
        self.allowed_side_effect_classes = frozenset(allowed_side_effect_classes)
        self.backend = backend
        self._clock = clock or time.time
        self._id_factory = id_factory
        self._counter = 0

    def __call__(self, request: GrantRequest) -> GrantDecision:
        if request.side_effect_class not in self.allowed_side_effect_classes:
            return GrantDecision(
                False,
                "denied_class",
                f"side_effect_class {request.side_effect_class!r} is not permitted by policy",
            )
        out_of_scope = sorted(set(request.scope) - self.allowed_scope)
        if out_of_scope:
            return GrantDecision(
                False,
                "denied_scope",
                f"requested scope outside policy: {', '.join(out_of_scope)}",
            )
        ttl = min(float(request.ttl_seconds), self.max_ttl_seconds)
        if ttl <= 0:
            return GrantDecision(False, "denied_ttl", "effective ttl is not positive")

        now = float(self._clock())
        self._counter += 1
        grant_id = self._mint_id(request)
        handle = GrantHandle(
            backend=self.backend,
            session_id=f"sess-{grant_id}",
            work_context_id=request.subject,
            handle_ref=f"{self.backend}:{grant_id}",
        )
        grant = SessionGrant(
            grant_id=grant_id,
            request_id=request.request_id,
            scope=tuple(request.scope),
            side_effect_class=request.side_effect_class,
            subject=request.subject,
            issued_at=_epoch_to_iso(now),
            expires_at=_epoch_to_iso(now + ttl),
            issued_at_epoch=now,
            expires_at_epoch=now + ttl,
            backend=self.backend,
            handle=handle,
            audit={
                **dict(request.audit),
                "requested_by": request.requested_by,
                "reason": request.reason,
                "policy_max_ttl_seconds": self.max_ttl_seconds,
            },
        )
        return GrantDecision(True, "granted", "issued by static policy", grant)

    def _mint_id(self, request: GrantRequest) -> str:
        if self._id_factory is not None:
            return self._id_factory(request)
        return f"grant-{self._counter}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Public functions.
# ---------------------------------------------------------------------------


def request_grant(
    *,
    scope: Any,
    side_effect_class: str,
    subject: str,
    reason: str,
    requested_by: str,
    ttl_seconds: float,
    audit: Optional[dict[str, Any]] = None,
    request_id: Optional[str] = None,
) -> GrantRequest:
    """Build and validate a credential-free :class:`GrantRequest`.

    Raises :class:`~hermes_workflows.errors.GrantError` for a malformed ask: a
    bad scope, an unknown side-effect class, a non-positive TTL, or a payload
    that smuggles a raw credential.
    """

    normalized_scope = _normalize_scope(scope)
    if side_effect_class not in SIDE_EFFECT_CLASSES:
        raise GrantError(
            f"unknown side_effect_class {side_effect_class!r}; expected one of {', '.join(SIDE_EFFECT_CLASSES)}"
        )
    if not isinstance(subject, str) or not subject:
        raise GrantError("grant request requires non-empty 'subject'")
    if not isinstance(reason, str) or not reason:
        raise GrantError("grant request requires non-empty 'reason'")
    if not isinstance(requested_by, str) or not requested_by:
        raise GrantError("grant request requires non-empty 'requested_by'")
    if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, (int, float)):
        raise GrantError("grant request 'ttl_seconds' must be a positive number")
    ttl = float(ttl_seconds)
    if ttl <= 0 or not math.isfinite(ttl):
        raise GrantError("grant request 'ttl_seconds' must be a positive, finite number")
    audit_blob = dict(audit or {})
    normalized_request_id = _normalize_request_id(request_id) if request_id is not None else None
    offender = find_raw_credential(audit_blob)
    if offender is not None:
        raise GrantError(f"grant request audit must not carry a raw credential ({offender!r})")
    return GrantRequest(
        request_id=normalized_request_id or f"greq-{uuid.uuid4().hex[:12]}",
        scope=normalized_scope,
        side_effect_class=side_effect_class,
        subject=subject,
        reason=reason,
        requested_by=requested_by,
        ttl_seconds=ttl,
        audit=audit_blob,
    )


def resolve_grant(
    broker: Optional[GrantBroker],
    request: GrantRequest,
    *,
    store: Optional[GrantStore] = None,
) -> GrantDecision:
    """Resolve a request through ``broker``, failing closed on every error path.

    Returns a negative :class:`GrantDecision` (never raises) when there is no
    broker, the broker errors, or the broker returns a credential-bearing /
    out-of-policy grant. On a clean grant the grant is persisted to ``store``
    when one is provided.
    """

    if broker is None:
        return GrantDecision(False, "no_broker", "no grant broker configured; failing closed")
    try:
        decision = broker(request)
    except Exception as exc:  # pragma: no cover - broker exception type is adapter-owned
        return GrantDecision(False, "broker_error", f"{type(exc).__name__}: {exc}")
    if not isinstance(decision, GrantDecision):
        return GrantDecision(
            False,
            "broker_error",
            f"broker returned {type(decision).__name__}, expected GrantDecision",
        )
    if not decision.granted:
        return decision
    try:
        normalized_grant = _normalize_session_grant(decision.grant)
    except GrantError as exc:
        return GrantDecision(False, "malformed", str(exc))
    decision = replace(decision, grant=normalized_grant)

    problem = _audit_issued_grant(decision.grant, request)
    if problem is not None:
        return problem
    grant = _sanitize_issued_grant(decision.grant, request)
    decision = replace(decision, grant=grant)
    if store is not None:
        try:
            store.save_grant(decision.grant)  # type: ignore[arg-type]
        except Exception as exc:  # pragma: no cover - store exception type is adapter-owned
            return GrantDecision(False, "store_error", f"{type(exc).__name__}: {exc}")
    return decision


def validate_grant(
    grant: Any,
    *,
    action: Optional[str] = None,
    now: Any = None,
) -> GrantValidation:
    """Fail-closed check that a grant authorizes ``action`` right now.

    ``grant`` may be a :class:`SessionGrant` or its ``to_dict`` form (e.g. read
    back from a store after a restart). Returns a negative result — never raises
    — for a malformed, revoked, expired, credential-bearing, or out-of-scope
    grant so callers and the loop runtime can halt deterministically.
    """

    try:
        parsed = _normalize_session_grant(grant)
    except GrantError as exc:
        return GrantValidation(False, "malformed", str(exc))

    interval_problem = _grant_time_problem(parsed)
    if interval_problem is not None:
        return GrantValidation(False, "malformed", interval_problem, parsed)

    offender = find_raw_credential(parsed.to_dict())
    if offender is not None:
        return GrantValidation(False, "malformed", f"grant carries a raw credential ({offender!r})", parsed)
    if parsed.status != "granted":
        return GrantValidation(False, "revoked", f"grant status is {parsed.status!r}", parsed)

    try:
        clock = _require_number(now, "validation clock") if now is not None else time.time()
    except GrantError as exc:
        return GrantValidation(False, "malformed", str(exc), parsed)
    if clock >= parsed.expires_at_epoch:
        return GrantValidation(False, "expired", f"grant expired at {parsed.expires_at}", parsed)
    if action is not None and action not in parsed.scope:
        return GrantValidation(
            False,
            "out_of_scope",
            f"action {action!r} not in grant scope {list(parsed.scope)!r}",
            parsed,
        )
    return GrantValidation(True, "valid", "grant is active and in scope", parsed)


def find_raw_credential(payload: Any) -> Optional[str]:
    """Return the first credential-shaped key/value marker in ``payload``.

    Recurses through nested dicts/lists. This is the guard that keeps scoped
    grants from degrading into raw token or browser-cookie reuse.
    """

    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(key, str) and _is_credential_key(key):
                return key
            found = find_raw_credential(value)
            if found is not None:
                return found
        return None
    if isinstance(payload, (list, tuple)):
        for item in payload:
            found = find_raw_credential(item)
            if found is not None:
                return found
    if isinstance(payload, str) and _looks_like_credential_value(payload):
        return "credential_value"
    return None


def redact_credentials(payload: Any) -> Any:
    """Return a deep copy of ``payload`` with credential-shaped values masked.

    Used to journal a grant envelope without persisting a smuggled secret: the
    grant is still rejected and named by key, but its *value* is never recorded.
    """

    if isinstance(payload, dict):
        out: dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(key, str) and _is_credential_key(key):
                out[key] = REDACTED
            else:
                out[key] = redact_credentials(value)
        return out
    if isinstance(payload, (list, tuple)):
        return [redact_credentials(item) for item in payload]
    if isinstance(payload, str) and _looks_like_credential_value(payload):
        return REDACTED
    return payload


# ---------------------------------------------------------------------------
# Internals.
# ---------------------------------------------------------------------------


def _normalize_session_grant(grant: Any) -> SessionGrant:
    if grant is None:
        raise GrantError("broker granted without a grant object")
    try:
        payload = grant.to_dict() if isinstance(grant, SessionGrant) else grant
        return SessionGrant.from_dict(payload)
    except GrantError:
        raise
    except Exception as exc:
        raise GrantError(f"malformed grant object: {type(exc).__name__}: {exc}") from exc


def _audit_issued_grant(grant: Optional[SessionGrant], request: GrantRequest) -> Optional[GrantDecision]:
    """Defense in depth: a granted decision must carry a clean, in-policy grant."""

    if grant is None:
        return GrantDecision(False, "malformed", "broker granted without a grant object")
    offender = find_raw_credential(grant.to_dict())
    if offender is not None:
        return GrantDecision(False, "malformed", f"issued grant carries a raw credential ({offender!r})")
    if grant.subject != request.subject:
        return GrantDecision(False, "denied_subject", "issued grant subject does not match request")
    if grant.request_id != request.request_id:
        return GrantDecision(False, "denied_request_id", "issued grant request_id does not match request")
    extra = sorted(set(grant.scope) - set(request.scope))
    if extra:
        return GrantDecision(
            False,
            "denied_scope",
            f"issued grant widened scope beyond request: {', '.join(extra)}",
        )
    if grant.side_effect_class != request.side_effect_class:
        return GrantDecision(
            False,
            "denied_class",
            "issued grant side_effect_class does not match request",
        )
    ttl = grant.expires_at_epoch - grant.issued_at_epoch
    interval_problem = _grant_time_problem(grant)
    if interval_problem is not None:
        return GrantDecision(False, "malformed", f"issued grant {interval_problem}")
    if ttl > request.ttl_seconds:
        return GrantDecision(False, "denied_ttl", "issued grant widened ttl beyond request")
    if grant.status != "granted":
        return GrantDecision(False, "revoked", f"issued grant status is {grant.status!r}")
    return None


def _grant_time_problem(grant: SessionGrant) -> Optional[str]:
    try:
        issued_epoch = _require_number(grant.issued_at_epoch, "issued_at_epoch")
        expires_epoch = _require_number(grant.expires_at_epoch, "expires_at_epoch")
    except GrantError as exc:
        return str(exc)
    ttl = expires_epoch - issued_epoch
    if not math.isfinite(issued_epoch) or not math.isfinite(expires_epoch) or not math.isfinite(ttl):
        return "timestamps must be finite"
    if ttl <= 0:
        return "validity interval must be positive"
    return None


def _sanitize_issued_grant(grant: Optional[SessionGrant], request: GrantRequest) -> SessionGrant:
    if grant is None:  # guarded by _audit_issued_grant; defensive for type checkers
        raise GrantError("broker granted without a grant object")
    audit = {key: value for key, value in dict(grant.audit or {}).items() if key not in _RESERVED_AUDIT_KEYS}
    for key, value in request.audit.items():
        if key not in {"requested_by", "reason", "policy_max_ttl_seconds"}:
            audit[key] = value
    audit["requested_by"] = request.requested_by
    audit["reason"] = request.reason
    return replace(grant, audit=audit)


def _is_credential_key(key: str) -> bool:
    lowered = key.lower()
    if lowered in _FORBIDDEN_CRED_KEYS:
        return True
    return any(part in lowered for part in _FORBIDDEN_CRED_SUBSTRINGS)


def _looks_like_credential_value(value: str) -> bool:
    stripped = value.strip()
    lowered = stripped.lower()
    if lowered.startswith(("bearer ", "basic ")):
        return True
    if "session=" in lowered or "cookie=" in lowered or "set-cookie:" in lowered:
        return True
    token_prefixes = (
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
    return any(lowered.startswith(prefix) for prefix in token_prefixes)


def _normalize_request_id(request_id: Any) -> str:
    if not isinstance(request_id, str):
        raise GrantError("grant request 'request_id' must be a non-empty identifier-safe string when provided")
    if request_id.strip() != request_id or not request_id:
        raise GrantError("grant request 'request_id' must be a non-empty identifier-safe string when provided")
    if not _safe_identifier_segment(request_id) or _looks_like_credential_value(request_id):
        raise GrantError("grant request 'request_id' must be a non-empty identifier-safe string when provided")
    return request_id


def _normalize_scope(scope: Any) -> tuple[str, ...]:
    if isinstance(scope, str):
        raise GrantError("grant scope must be a list of action strings, not a single string")
    if not isinstance(scope, (list, tuple)) or not scope:
        raise GrantError("grant scope must be a non-empty list of action strings")
    actions: list[str] = []
    for item in scope:
        if not isinstance(item, str) or not item or not _action_safe(item):
            raise GrantError(f"grant scope action {item!r} must be a dotted identifier-safe string")
        actions.append(item)
    # De-duplicate while preserving order for stable hashing/auditing.
    seen: set[str] = set()
    unique: list[str] = []
    for action in actions:
        if action not in seen:
            seen.add(action)
            unique.append(action)
    return tuple(unique)


def _action_safe(value: str) -> bool:
    return all(c.isalnum() or c in "._-:" for c in value)


def _opt_str(value: Any, label: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise GrantError(f"grant handle {label!r} must be a string when present")
    return value


def _require_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GrantError(f"grant {label!r} must be a number")
    number = float(value)
    if not math.isfinite(number):
        raise GrantError(f"grant {label!r} must be finite")
    return number


def _safe_identifier_segment(value: str) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 128
        and value[0].isalnum()
        and all(c.isalnum() or c in "._-" for c in value)
    )


def _safe_grant_id(grant_id: str) -> str:
    if not _safe_identifier_segment(grant_id):
        raise GrantError("grant_id must be identifier-safe")
    return grant_id


def _epoch_to_iso(epoch: float) -> str:
    from datetime import datetime, timezone

    return (
        datetime.fromtimestamp(epoch, timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
