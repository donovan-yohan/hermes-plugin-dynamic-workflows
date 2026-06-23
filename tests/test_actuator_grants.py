"""Tests for backend-neutral scoped actuator grants (issue #33 slice)."""

import tempfile

import pytest

from hermes_workflows.errors import GrantError
from hermes_workflows.grants import (
    FileGrantStore,
    GrantDecision,
    GrantHandle,
    InMemoryGrantStore,
    SessionGrant,
    StaticPolicyGrantBroker,
    find_raw_credential,
    request_grant,
    resolve_grant,
    validate_grant,
)
from hermes_workflows.loops import InMemoryLoopRunStore, loop_run


# ---------------------------------------------------------------------------
# Fixtures / helpers.
# ---------------------------------------------------------------------------

FIXED_NOW = 1_700_000_000.0


def fixed_clock(value=FIXED_NOW):
    return lambda: value


def policy_broker(**overrides):
    kwargs = dict(
        allowed_scope={"session.launch", "session.status", "session.cancel"},
        max_ttl_seconds=3600,
        clock=fixed_clock(),
    )
    kwargs.update(overrides)
    return StaticPolicyGrantBroker(**kwargs)


def good_request(**overrides):
    kwargs = dict(
        scope=("session.launch", "session.status"),
        side_effect_class="session_launch",
        subject="work-context-abc",
        reason="launch a managed session to drive the issue",
        requested_by="issue_controller",
        ttl_seconds=1800,
    )
    kwargs.update(overrides)
    return request_grant(**kwargs)


def loop_spec(**brake_overrides):
    brakes = {"max_steps": 4, "max_repeated_signal": 3, "max_sensor_retries": 1}
    brakes.update(brake_overrides)
    return {
        "version": "1",
        "name": "session_loop",
        "setpoint": {"target": "managed session reaches a verified state"},
        "sensors": [{"id": "verification", "primary": True, "kind": "callable"}],
        "actuators": [{"id": "launcher", "kind": "adapter"}],
        "brakes": brakes,
    }


# ---------------------------------------------------------------------------
# request_grant / model validation.
# ---------------------------------------------------------------------------


def test_request_grant_builds_credential_free_scoped_request():
    req = good_request()

    assert req.scope == ("session.launch", "session.status")
    assert req.side_effect_class == "session_launch"
    assert req.subject == "work-context-abc"
    assert req.ttl_seconds == 1800
    assert req.request_id


def test_request_grant_rejects_unknown_side_effect_class():
    with pytest.raises(GrantError):
        good_request(side_effect_class="root_shell")


def test_request_grant_rejects_scope_passed_as_bare_string():
    with pytest.raises(GrantError):
        good_request(scope="session.launch")


def test_request_grant_rejects_non_positive_ttl():
    with pytest.raises(GrantError):
        good_request(ttl_seconds=0)


def test_request_grant_rejects_malformed_request_id():
    for bad_request_id in (
        123,
        "",
        "   ",
        ".",
        "..",
        "...",
        "../x",
        "with/slash",
        "a:b",
        "_leading",
        "***",
        "a" * 129,
    ):
        with pytest.raises(GrantError):
            good_request(request_id=bad_request_id)


def test_request_grant_rejects_raw_credential_in_audit():
    with pytest.raises(GrantError):
        good_request(audit={"cookie": "session=abc123"})


def test_find_raw_credential_walks_nested_payloads():
    assert find_raw_credential({"handle": {"authorization": "Bearer x"}}) == "authorization"
    assert find_raw_credential({"items": [{"password": "p"}]}) == "password"
    assert find_raw_credential({"session_id": "s", "work_context_id": "w"}) is None
    assert find_raw_credential({"note": "Bearer VALUE_SMUGGLE_SECRET_33"}) == "credential_value"
    assert find_raw_credential({"note": "GHP_VALUE_SMUGGLE_SECRET_33"}) == "credential_value"


# ---------------------------------------------------------------------------
# StaticPolicyGrantBroker + resolve_grant.
# ---------------------------------------------------------------------------


def test_static_policy_broker_grants_within_policy_and_clamps_ttl():
    broker = policy_broker(max_ttl_seconds=600)
    decision = resolve_grant(broker, good_request(ttl_seconds=1800))

    assert decision.granted is True
    assert decision.code == "granted"
    grant = decision.grant
    assert grant is not None
    # TTL clamped to policy max (600), not the requested 1800.
    assert grant.expires_at_epoch == FIXED_NOW + 600
    assert grant.handle.work_context_id == "work-context-abc"
    assert grant.handle.handle_ref and "inproc:" in grant.handle.handle_ref
    # The issued grant must never carry a raw credential.
    assert find_raw_credential(grant.to_dict()) is None


def test_static_policy_broker_denies_out_of_scope_request():
    broker = policy_broker(allowed_scope={"session.status"})
    decision = resolve_grant(broker, good_request(scope=("session.launch",)))

    assert decision.granted is False
    assert decision.code == "denied_scope"
    assert "session.launch" in decision.reason


def test_static_policy_broker_denies_disallowed_side_effect_class():
    broker = policy_broker(allowed_side_effect_classes={"read_only"})
    decision = resolve_grant(broker, good_request())

    assert decision.granted is False
    assert decision.code == "denied_class"


def test_static_policy_broker_rejects_string_policy_iterables():
    with pytest.raises(GrantError):
        StaticPolicyGrantBroker(allowed_scope="session.launch", max_ttl_seconds=60)
    with pytest.raises(GrantError):
        StaticPolicyGrantBroker(
            allowed_scope={"session.launch"},
            allowed_side_effect_classes="session_launch",
            max_ttl_seconds=60,
        )


def test_static_policy_broker_audit_fields_cannot_be_spoofed():
    req = good_request(
        requested_by="real-requester",
        reason="real reason",
        audit={"requested_by": "evil", "reason": "lie", "policy_max_ttl_seconds": 999, "operator_note": "kept"},
    )

    decision = resolve_grant(policy_broker(max_ttl_seconds=60), req)

    assert decision.grant is not None
    audit = decision.grant.audit
    assert audit["requested_by"] == "real-requester"
    assert audit["reason"] == "real reason"
    assert "policy_max_ttl_seconds" not in audit
    assert audit["operator_note"] == "kept"


def test_resolve_grant_fails_closed_without_broker():
    decision = resolve_grant(None, good_request())

    assert decision.granted is False
    assert decision.code == "no_broker"


def test_resolve_grant_rejects_grant_that_widens_scope():
    def rogue_broker(request):
        grant = SessionGrant(
            grant_id="g1",
            request_id=request.request_id,
            scope=("session.launch", "session.delete_everything"),
            side_effect_class=request.side_effect_class,
            subject=request.subject,
            issued_at="2023-11-14T22:13:20Z",
            expires_at="2023-11-14T23:13:20Z",
            issued_at_epoch=FIXED_NOW,
            expires_at_epoch=FIXED_NOW + 3600,
            backend="rogue",
            handle=GrantHandle(backend="rogue"),
        )
        return GrantDecision(True, "granted", "rogue", grant)

    decision = resolve_grant(rogue_broker, good_request(scope=("session.launch",)))

    assert decision.granted is False
    assert decision.code == "denied_scope"


def test_resolve_grant_rejects_grant_that_retargets_subject_or_request_id():
    def rogue_subject_broker(request):
        grant = SessionGrant(
            grant_id="g1",
            request_id=request.request_id,
            scope=request.scope,
            side_effect_class=request.side_effect_class,
            subject="different-subject",
            issued_at="2023-11-14T22:13:20Z",
            expires_at="2023-11-14T22:14:20Z",
            issued_at_epoch=FIXED_NOW,
            expires_at_epoch=FIXED_NOW + 60,
            backend="rogue",
            handle=GrantHandle(backend="rogue"),
        )
        return GrantDecision(True, "granted", "rogue", grant)

    def rogue_request_broker(request):
        grant = SessionGrant(
            grant_id="g2",
            request_id="different-request",
            scope=request.scope,
            side_effect_class=request.side_effect_class,
            subject=request.subject,
            issued_at="2023-11-14T22:13:20Z",
            expires_at="2023-11-14T22:14:20Z",
            issued_at_epoch=FIXED_NOW,
            expires_at_epoch=FIXED_NOW + 60,
            backend="rogue",
            handle=GrantHandle(backend="rogue"),
        )
        return GrantDecision(True, "granted", "rogue", grant)

    subject_decision = resolve_grant(rogue_subject_broker, good_request(ttl_seconds=60))
    request_decision = resolve_grant(rogue_request_broker, good_request(ttl_seconds=60))

    assert subject_decision.granted is False
    assert subject_decision.code == "denied_subject"
    assert request_decision.granted is False
    assert request_decision.code == "denied_request_id"


def test_resolve_grant_rejects_non_finite_broker_timestamps():
    def rogue_broker(request):
        grant = SessionGrant(
            grant_id="g1",
            request_id=request.request_id,
            scope=request.scope,
            side_effect_class=request.side_effect_class,
            subject=request.subject,
            issued_at="2023-11-14T22:13:20Z",
            expires_at="not-a-real-time",
            issued_at_epoch=FIXED_NOW,
            expires_at_epoch=float("nan"),
            backend="rogue",
            handle=GrantHandle(backend="rogue"),
        )
        return GrantDecision(True, "granted", "rogue", grant)

    decision = resolve_grant(rogue_broker, good_request(ttl_seconds=60))

    assert decision.granted is False
    assert decision.code == "malformed"


def test_resolve_grant_rejects_non_numeric_or_bool_broker_timestamps():
    def string_timestamp_broker(request):
        return GrantDecision(
            True,
            "granted",
            "rogue",
            SessionGrant(
                grant_id="g1",
                request_id=request.request_id,
                scope=request.scope,
                side_effect_class=request.side_effect_class,
                subject=request.subject,
                issued_at="2023-11-14T22:13:20Z",
                expires_at="2023-11-14T22:14:20Z",
                issued_at_epoch="not-a-number",  # type: ignore[arg-type]
                expires_at_epoch=FIXED_NOW + 60,
                backend="rogue",
            ),
        )

    def bool_timestamp_broker(request):
        return GrantDecision(
            True,
            "granted",
            "rogue",
            SessionGrant(
                grant_id="g2",
                request_id=request.request_id,
                scope=request.scope,
                side_effect_class=request.side_effect_class,
                subject=request.subject,
                issued_at="2023-11-14T22:13:20Z",
                expires_at="2023-11-14T22:14:20Z",
                issued_at_epoch=False,  # type: ignore[arg-type]
                expires_at_epoch=True,  # type: ignore[arg-type]
                backend="rogue",
            ),
        )

    string_decision = resolve_grant(string_timestamp_broker, good_request(ttl_seconds=60))
    bool_decision = resolve_grant(bool_timestamp_broker, good_request(ttl_seconds=60))

    assert string_decision.granted is False
    assert string_decision.code == "malformed"
    assert bool_decision.granted is False
    assert bool_decision.code == "malformed"


def test_static_policy_broker_rejects_malformed_clock_values():
    for bad_clock in (True, False, "not-a-number", float("nan"), float("inf")):
        broker = policy_broker(clock=lambda value=bad_clock: value)

        decision = broker(good_request(ttl_seconds=60))

        assert decision.granted is False
        assert decision.code == "malformed"


def test_static_policy_broker_rejects_malformed_id_factory_values():
    request = good_request(ttl_seconds=60)
    for bad_grant_id in (
        "../x",
        ".hidden",
        "grant:colon",
        "sk-test",
        "ghp_SECRET",
        "github_pat_SECRET",
        {"not": "a-string"},
    ):
        broker = policy_broker(id_factory=lambda _request, value=bad_grant_id: value)  # type: ignore[arg-type]

        direct_decision = broker(request)
        resolved_decision = resolve_grant(broker, request)

        assert direct_decision.granted is False
        assert direct_decision.code == "malformed"
        assert resolved_decision.granted is False
        assert resolved_decision.code == "malformed"



def test_grant_value_object_constructors_reject_malformed_backends():
    for bad_backend in (123, None, "", "   ", "\t", "../x", "a:b", ".hidden", "sk-backend"):
        with pytest.raises(GrantError):
            GrantHandle(backend=bad_backend)  # type: ignore[arg-type]

        with pytest.raises(GrantError):
            SessionGrant(
                grant_id="grant-constructor-backend",
                request_id="greq-constructor-backend",
                scope=("session.launch",),
                side_effect_class="session_launch",
                subject="work-context-abc",
                issued_at="2023-11-14T22:13:20Z",
                expires_at="2023-11-14T22:14:20Z",
                issued_at_epoch=FIXED_NOW,
                expires_at_epoch=FIXED_NOW + 60,
                backend=bad_backend,  # type: ignore[arg-type]
            )


def test_grant_value_object_constructors_accept_valid_backends():
    handle = GrantHandle(backend="relay.v1-prod", handle_ref="relay.v1-prod:grant-ok")
    grant = SessionGrant(
        grant_id="grant-constructor-valid",
        request_id="greq-constructor-valid",
        scope=("session.launch",),
        side_effect_class="session_launch",
        subject="work-context-abc",
        issued_at="2023-11-14T22:13:20Z",
        expires_at="2023-11-14T22:14:20Z",
        issued_at_epoch=FIXED_NOW,
        expires_at_epoch=FIXED_NOW + 60,
        backend="relay.v1-prod",
        handle=handle,
    )

    assert handle.backend == "relay.v1-prod"
    assert grant.backend == "relay.v1-prod"

def test_static_policy_broker_rejects_malformed_backend_before_minting_grants():
    for bad_backend in (123, "", "   ", "\t", "../x", "a:b", ".hidden", None, "sk-backend"):
        with pytest.raises(GrantError):
            policy_broker(backend=bad_backend)  # type: ignore[arg-type]



def test_validate_grant_rejects_malformed_backend_fields():
    request = good_request(request_id="greq-backend-fields", ttl_seconds=60)
    valid = policy_broker(id_factory=lambda _request: "grant-backend-fields")(request).grant
    assert valid is not None

    for bad_backend in ("   ", "\t", "../x", "a:b", ".hidden", "sk-backend"):
        poisoned = valid.to_dict()
        poisoned["backend"] = bad_backend
        poisoned["handle"]["backend"] = bad_backend

        validation = validate_grant(poisoned, now=FIXED_NOW + 1)

        assert validation.ok is False
        assert validation.code == "malformed"

    for bad_handle_backend in ("a:b", "sk-backend"):
        poisoned_handle = valid.to_dict()
        poisoned_handle["handle"]["backend"] = bad_handle_backend
        validation = validate_grant(poisoned_handle, now=FIXED_NOW + 1)
        assert validation.ok is False
        assert validation.code == "malformed"


def test_resolve_grant_rejects_rogue_broker_whitespace_backend():
    request = good_request(request_id="greq-rogue-backend", ttl_seconds=60)

    def rogue_broker(req):
        grant = SessionGrant(
            grant_id="grant-rogue-backend",
            request_id=req.request_id,
            scope=req.scope,
            side_effect_class=req.side_effect_class,
            subject=req.subject,
            issued_at="2023-11-14T22:13:20Z",
            expires_at="2023-11-14T22:14:20Z",
            issued_at_epoch=FIXED_NOW,
            expires_at_epoch=FIXED_NOW + 60,
            backend="   ",
            handle=GrantHandle(backend="   ", handle_ref="   :grant-rogue-backend"),
            audit=req.audit,
        )
        return GrantDecision(True, "granted", "rogue", grant=grant)

    decision = resolve_grant(rogue_broker, request)

    assert decision.granted is False
    assert decision.code == "malformed"

def test_resolve_grant_sanitizes_rogue_broker_audit_spoofing():
    req = good_request(requested_by="real-requester", reason="real reason", audit={"run_id": "real-run", "operator_note": "kept"})

    def rogue_broker(request):
        grant = SessionGrant(
            grant_id="g1",
            request_id=request.request_id,
            scope=request.scope,
            side_effect_class=request.side_effect_class,
            subject=request.subject,
            issued_at="2023-11-14T22:13:20Z",
            expires_at="2023-11-14T22:14:20Z",
            issued_at_epoch=FIXED_NOW,
            expires_at_epoch=FIXED_NOW + 60,
            backend="rogue",
            handle=GrantHandle(backend="rogue"),
            audit={
                "run_id": "fake",
                "requested_by": "evil",
                "reason": "lie",
                "policy_max_ttl_seconds": 999,
                "operator_note": "overwritten",
                "custom": "kept",
            },
        )
        return GrantDecision(True, "granted", "rogue", grant)

    store = InMemoryGrantStore()
    decision = resolve_grant(rogue_broker, req, store=store)

    assert decision.granted is True
    assert decision.grant is not None
    audit = decision.grant.audit
    assert audit["run_id"] == "real-run"
    assert audit["requested_by"] == "real-requester"
    assert audit["reason"] == "real reason"
    assert "policy_max_ttl_seconds" not in audit
    assert audit["operator_note"] == "kept"
    assert audit["custom"] == "kept"
    persisted = store.get_grant(decision.grant.grant_id)
    assert persisted is not None
    assert persisted["audit"] == audit


def test_resolve_grant_drops_broker_only_reserved_audit_fields():
    req = good_request(requested_by="real-requester", reason="real reason")

    def rogue_broker(request):
        grant = SessionGrant(
            grant_id="g1",
            request_id=request.request_id,
            scope=request.scope,
            side_effect_class=request.side_effect_class,
            subject=request.subject,
            issued_at="2023-11-14T22:13:20Z",
            expires_at="2023-11-14T22:14:20Z",
            issued_at_epoch=FIXED_NOW,
            expires_at_epoch=FIXED_NOW + 60,
            backend="rogue",
            audit={"run_id": "fake-run", "def_hash": "fake-def", "loop_name": "fake-loop", "iteration": 99, "broker_trace": "kept"},
        )
        return GrantDecision(True, "granted", "rogue", grant)

    decision = resolve_grant(rogue_broker, req)

    assert decision.granted is True
    assert decision.grant is not None
    audit = decision.grant.audit
    assert audit == {"broker_trace": "kept", "requested_by": "real-requester", "reason": "real reason"}


def test_resolve_grant_sanitizes_missing_broker_audit_blob():
    req = good_request(requested_by="real-requester", reason="real reason", audit={"run_id": "real-run"})

    def rogue_broker(request):
        grant = SessionGrant(
            grant_id="g1",
            request_id=request.request_id,
            scope=request.scope,
            side_effect_class=request.side_effect_class,
            subject=request.subject,
            issued_at="2023-11-14T22:13:20Z",
            expires_at="2023-11-14T22:14:20Z",
            issued_at_epoch=FIXED_NOW,
            expires_at_epoch=FIXED_NOW + 60,
            backend="rogue",
        )
        object.__setattr__(grant, "audit", None)
        return GrantDecision(True, "granted", "rogue", grant)

    decision = resolve_grant(rogue_broker, req)

    assert decision.granted is True
    assert decision.grant is not None
    assert decision.grant.audit == {"run_id": "real-run", "requested_by": "real-requester", "reason": "real reason"}


def test_resolve_grant_rejects_grant_that_widens_ttl():
    def rogue_broker(request):
        grant = SessionGrant(
            grant_id="g1",
            request_id=request.request_id,
            scope=request.scope,
            side_effect_class=request.side_effect_class,
            subject=request.subject,
            issued_at="2023-11-14T22:13:20Z",
            expires_at="2023-11-14T23:13:20Z",
            issued_at_epoch=FIXED_NOW,
            expires_at_epoch=FIXED_NOW + request.ttl_seconds + 1,
            backend="rogue",
            handle=GrantHandle(backend="rogue"),
        )
        return GrantDecision(True, "granted", "rogue", grant)

    decision = resolve_grant(rogue_broker, good_request(ttl_seconds=60))

    assert decision.granted is False
    assert decision.code == "denied_ttl"


def test_resolve_grant_rejects_revoked_grant_from_broker():
    def rogue_broker(request):
        grant = SessionGrant(
            grant_id="g1",
            request_id=request.request_id,
            scope=request.scope,
            side_effect_class=request.side_effect_class,
            subject=request.subject,
            issued_at="2023-11-14T22:13:20Z",
            expires_at="2023-11-14T23:13:20Z",
            issued_at_epoch=FIXED_NOW,
            expires_at_epoch=FIXED_NOW + request.ttl_seconds,
            backend="rogue",
            handle=GrantHandle(backend="rogue"),
            status="revoked",
        )
        return GrantDecision(True, "granted", "rogue", grant)

    decision = resolve_grant(rogue_broker, good_request(ttl_seconds=60))

    assert decision.granted is False
    assert decision.code == "revoked"


def test_resolve_grant_fails_closed_when_store_rejects_grant():
    class BrokenStore:
        def save_grant(self, grant):
            raise RuntimeError("disk full")

        def get_grant(self, grant_id):  # pragma: no cover - protocol completeness only
            return None

    decision = resolve_grant(policy_broker(), good_request(), store=BrokenStore())

    assert decision.granted is False
    assert decision.code == "store_error"
    assert "disk full" in decision.reason


# ---------------------------------------------------------------------------
# validate_grant fail-closed semantics.
# ---------------------------------------------------------------------------


def test_validate_grant_accepts_active_in_scope_grant():
    grant = resolve_grant(policy_broker(), good_request()).grant
    result = validate_grant(grant, action="session.status", now=FIXED_NOW + 10)

    assert result.ok is True
    assert result.code == "valid"


def test_validate_grant_rejects_expired_grant():
    grant = resolve_grant(policy_broker(), good_request(ttl_seconds=100)).grant
    result = validate_grant(grant, action="session.status", now=FIXED_NOW + 101)

    assert result.ok is False
    assert result.code == "expired"


def test_validate_grant_rejects_out_of_scope_action():
    grant = resolve_grant(policy_broker(), good_request(scope=("session.status",))).grant
    result = validate_grant(grant, action="session.launch", now=FIXED_NOW + 10)

    assert result.ok is False
    assert result.code == "out_of_scope"


def test_validate_grant_rejects_revoked_grant():
    grant = resolve_grant(policy_broker(), good_request()).grant
    revoked = SessionGrant.from_dict({**grant.to_dict(), "status": "revoked"})
    result = validate_grant(revoked, now=FIXED_NOW + 10)

    assert result.ok is False
    assert result.code == "revoked"


def test_validate_grant_rejects_malformed_grant():
    result = validate_grant({"grant_id": "x"})

    assert result.ok is False
    assert result.code == "malformed"


def test_validate_grant_rejects_non_finite_timestamps():
    grant_obj = resolve_grant(policy_broker(), good_request()).grant
    assert grant_obj is not None
    grant = grant_obj.to_dict()
    grant["expires_at_epoch"] = float("nan")

    result = validate_grant(grant, action="session.status", now=FIXED_NOW + 10)

    assert result.ok is False
    assert result.code == "malformed"


def test_validate_grant_rejects_malformed_validation_clock_without_raising():
    grant = resolve_grant(policy_broker(), good_request()).grant
    assert grant is not None

    for bad_now in ("not-a-number", True, False, float("nan"), float("inf")):
        result = validate_grant(grant, action="session.status", now=bad_now)
        assert result.ok is False
        assert result.code == "malformed"


def test_validate_grant_rejects_non_finite_timestamps_on_session_grant_objects():
    grant_obj = resolve_grant(policy_broker(), good_request()).grant
    assert grant_obj is not None
    poisoned = SessionGrant.from_dict(grant_obj.to_dict())
    object.__setattr__(poisoned, "expires_at_epoch", float("nan"))

    result = validate_grant(poisoned, action="session.status", now=FIXED_NOW + 10)

    assert result.ok is False
    assert result.code == "malformed"


def test_validate_grant_rejects_non_positive_validity_interval_for_dict_and_object():
    grant_obj = resolve_grant(policy_broker(), good_request()).grant
    assert grant_obj is not None
    payload = grant_obj.to_dict()
    payload["issued_at_epoch"] = FIXED_NOW + 100
    payload["expires_at_epoch"] = FIXED_NOW + 50
    object_grant = SessionGrant.from_dict(grant_obj.to_dict())
    object.__setattr__(object_grant, "issued_at_epoch", FIXED_NOW + 100)
    object.__setattr__(object_grant, "expires_at_epoch", FIXED_NOW + 50)

    dict_result = validate_grant(payload, action="session.status", now=FIXED_NOW + 10)
    object_result = validate_grant(object_grant, action="session.status", now=FIXED_NOW + 10)

    assert dict_result.ok is False
    assert dict_result.code == "malformed"
    assert object_result.ok is False
    assert object_result.code == "malformed"


def test_validate_grant_fails_closed_for_poisoned_session_grant_object_fields():
    grant_obj = resolve_grant(policy_broker(), good_request()).grant
    assert grant_obj is not None
    bad_handle = SessionGrant.from_dict(grant_obj.to_dict())
    object.__setattr__(bad_handle, "handle", "not-a-handle")
    bad_audit = SessionGrant.from_dict(grant_obj.to_dict())
    object.__setattr__(bad_audit, "audit", 123)
    bad_request_id = SessionGrant.from_dict(grant_obj.to_dict())
    object.__setattr__(bad_request_id, "request_id", "../x")
    bad_grant_id = SessionGrant.from_dict(grant_obj.to_dict())
    object.__setattr__(bad_grant_id, "grant_id", "../x")
    bad_side_effect_class = SessionGrant.from_dict(grant_obj.to_dict())
    object.__setattr__(bad_side_effect_class, "side_effect_class", "root_shell")

    for poisoned in (bad_handle, bad_audit, bad_request_id, bad_grant_id, bad_side_effect_class):
        result = validate_grant(poisoned, action="session.status", now=FIXED_NOW + 10)
        assert result.ok is False
        assert result.code == "malformed"


def test_resolve_grant_rejects_malformed_grant_id_from_broker():
    request = good_request()
    clean = resolve_grant(policy_broker(), request).grant
    assert clean is not None
    poisoned = SessionGrant.from_dict(clean.to_dict())
    object.__setattr__(poisoned, "grant_id", "../x")

    def rogue_broker(request):
        return GrantDecision(True, "granted", "issued malformed grant id", poisoned)

    decision = resolve_grant(rogue_broker, request)

    assert decision.granted is False
    assert decision.code == "malformed"


# ---------------------------------------------------------------------------
# Persistence + resume after restart (acceptance #3).
# ---------------------------------------------------------------------------


def test_grant_handle_survives_restart_and_revalidates_for_status_checks():
    with tempfile.TemporaryDirectory() as tmp:
        broker = policy_broker()
        store = FileGrantStore(tmp)
        decision = resolve_grant(broker, good_request(ttl_seconds=1800), store=store)
        grant_id = decision.grant.grant_id

        # Simulate a process restart: a fresh store instance, no in-memory state.
        reopened = FileGrantStore(tmp)
        persisted = reopened.get_grant(grant_id)

        assert persisted is not None
        assert persisted["handle"]["work_context_id"] == "work-context-abc"
        # No secret was ever persisted alongside the handle.
        assert find_raw_credential(persisted) is None

        live = validate_grant(persisted, action="session.status", now=FIXED_NOW + 60)
        assert live.ok is True

        expired = validate_grant(persisted, action="session.status", now=FIXED_NOW + 1801)
        assert expired.ok is False
        assert expired.code == "expired"


def test_in_memory_grant_store_returns_isolated_copies():
    store = InMemoryGrantStore()
    grant = resolve_grant(policy_broker(), good_request(), store=store).grant
    snapshot = store.get_grant(grant.grant_id)
    snapshot["status"] = "mutated"

    assert store.get_grant(grant.grant_id)["status"] == "granted"


# ---------------------------------------------------------------------------
# loop_run wiring.
# ---------------------------------------------------------------------------


def test_loop_run_issues_scoped_grant_then_uses_handle_to_converge():
    broker = policy_broker()
    seen_grants = {}

    def sensor(context):
        if context["iteration"] == 1:
            return {"converged": False, "signal_key": "no-session", "summary": "no session yet"}
        # Second pass: the issued grant is visible to the controller context.
        seen_grants["grants"] = context["grants"]
        return {"converged": True, "signal_key": "session-up", "summary": "session verified"}

    def actuator(context):
        return {
            "summary": "requested session launch authority",
            "grant_request": {
                "scope": ["session.launch", "session.status"],
                "side_effect_class": "session_launch",
                "subject": "wc-42",
                "reason": "drive the issue in a managed session",
                "ttl_seconds": 900,
                "audit": {"run_id": "fake", "iteration": 999, "operator_note": "kept"},
            },
        }

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator, grant_broker=broker)

    assert status.state == "converged"
    assert len(status.grants) == 1
    assert status.grants[0]["subject"] == "wc-42"
    assert status.grants[0]["side_effect_class"] == "session_launch"
    assert seen_grants["grants"][0]["handle"]["work_context_id"] == "wc-42"
    issued = [e for e in status.events if e["kind"] == "grant_issued"]
    assert len(issued) == 1
    assert issued[0]["grant_code"] == "granted"
    audit = issued[0]["grant"]["audit"]
    assert audit["run_id"] == status.run_id
    assert audit["iteration"] == 1
    assert audit["operator_note"] == "kept"


def test_loop_run_persists_grant_to_grant_store():
    broker = policy_broker()
    grant_store = InMemoryGrantStore()

    def sensor(context):
        return {"converged": False, "signal_key": "no-session", "summary": "no session"}

    def actuator(context):
        return {
            "summary": "request",
            "grant_request": {
                "scope": ["session.launch"],
                "side_effect_class": "session_launch",
                "subject": "wc-7",
                "reason": "launch",
                "ttl_seconds": 600,
            },
        }

    status = loop_run(
        loop_spec(max_steps=1, max_repeated_signal=99),
        sensor=sensor,
        actuator=actuator,
        grant_broker=broker,
        grant_store=grant_store,
    )

    grant_id = status.grants[0]["grant_id"]
    assert grant_store.get_grant(grant_id) is not None


def test_loop_run_halts_grant_denied_on_out_of_scope_request():
    broker = policy_broker(allowed_scope={"session.status"})
    events = []

    def sensor(context):
        return {"converged": False, "signal_key": "needs-launch", "summary": "needs launch"}

    def actuator(context):
        return {
            "summary": "request launch",
            "grant_request": {
                "scope": ["session.launch"],
                "side_effect_class": "session_launch",
                "subject": "wc-1",
                "reason": "launch",
                "ttl_seconds": 600,
            },
        }

    status = loop_run(
        loop_spec(),
        sensor=sensor,
        actuator=actuator,
        grant_broker=broker,
        on_event=lambda event, status: events.append(event),
    )

    assert status.state == "halted_grant_denied"
    assert status.halted_reason is not None
    assert "denied_scope" in status.halted_reason
    assert status.report["convergence_risk"] == "not_converged"
    denied = [e for e in events if e["kind"] == "grant_denied"]
    assert len(denied) == 1
    assert denied[0]["grant_code"] == "denied_scope"
    assert status.grants == []


def test_loop_run_fails_closed_when_no_broker_configured():
    def sensor(context):
        return {"converged": False, "signal_key": "needs-grant", "summary": "needs grant"}

    def actuator(context):
        return {
            "summary": "request",
            "grant_request": {
                "scope": ["session.launch"],
                "side_effect_class": "session_launch",
                "subject": "wc-1",
                "reason": "launch",
                "ttl_seconds": 600,
            },
        }

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator)

    assert status.state == "halted_grant_denied"
    assert "no_broker" in status.halted_reason


def test_loop_run_grant_denied_does_not_leak_smuggled_credential():
    broker = policy_broker()
    events = []

    def sensor(context):
        return {"converged": False, "signal_key": "needs-grant", "summary": "needs grant"}

    def actuator(context):
        return {
            "summary": "request with smuggled cookie",
            "grant_request": {
                "scope": ["session.launch"],
                "side_effect_class": "session_launch",
                "subject": "wc-1",
                "reason": "launch",
                "ttl_seconds": 600,
                "cookie": "session=supersecretvalue",
            },
        }

    status = loop_run(
        loop_spec(),
        sensor=sensor,
        actuator=actuator,
        grant_broker=broker,
        on_event=lambda event, status: events.append(event),
    )

    assert status.state == "halted_grant_denied"
    assert "cookie" in status.halted_reason  # names the offending key
    # The secret VALUE must never be journaled anywhere in the run state.
    blob = repr(status.as_dict()) + repr(events)
    assert "supersecretvalue" not in blob
    assert status.grants == []


def test_loop_run_non_dict_grant_envelope_does_not_leak_secret_value():
    def sensor(context):
        return {"converged": False, "signal_key": "needs-grant", "summary": "needs grant"}

    def actuator(context):
        return {"summary": "bad grant", "grant_request": "cookie=supersecretvalue"}

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator, grant_broker=policy_broker())

    assert status.state == "halted_grant_denied"
    assert status.actuator_results[0]["grant_request"] == "[REDACTED]"
    assert "supersecretvalue" not in repr(status.as_dict())


def test_loop_run_top_level_and_value_only_grant_secrets_are_fail_closed_and_redacted():
    store = InMemoryLoopRunStore()
    events = []

    def sensor(context):
        return {"converged": False, "signal_key": "needs-grant", "summary": "needs grant"}

    def actuator(context):
        return {
            "summary": "request with smuggled top-level bearer",
            "authorization": "Bearer TOP_LEVEL_SECRET_33",
            "grant_request": {
                "scope": ["session.launch"],
                "side_effect_class": "session_launch",
                "subject": "wc-1",
                "reason": "launch",
                "ttl_seconds": 600,
                "audit": {"note": "Bearer VALUE_SMUGGLE_SECRET_33"},
            },
        }

    status = loop_run(
        loop_spec(),
        sensor=sensor,
        actuator=actuator,
        grant_broker=policy_broker(),
        store=store,
        on_event=lambda event, status: events.append(event),
    )

    blob = repr(status.as_dict()) + repr(store.get_status(status.run_id)) + repr(events)
    assert status.state == "halted_grant_denied"
    assert status.halted_reason is not None
    assert "authorization" in status.halted_reason
    assert "TOP_LEVEL_SECRET_33" not in blob
    assert "VALUE_SMUGGLE_SECRET_33" not in blob
    assert status.actuator_results[0]["authorization"] == "[REDACTED]"
    assert status.actuator_results[0]["grant_request"]["audit"]["note"] == "[REDACTED]"


def test_loop_run_value_only_grant_secret_is_fail_closed_and_redacted():
    def sensor(context):
        return {"converged": False, "signal_key": "needs-grant", "summary": "needs grant"}

    def actuator(context):
        return {
            "summary": "request with value-only bearer",
            "grant_request": {
                "scope": ["session.launch"],
                "side_effect_class": "session_launch",
                "subject": "wc-1",
                "reason": "launch",
                "ttl_seconds": 600,
                "audit": {"note": "Bearer VALUE_SMUGGLE_SECRET_33"},
            },
        }

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator, grant_broker=policy_broker())

    assert status.state == "halted_grant_denied"
    assert status.halted_reason is not None
    assert "credential_value" in status.halted_reason
    assert "VALUE_SMUGGLE_SECRET_33" not in repr(status.as_dict())
    assert status.actuator_results[0]["grant_request"]["audit"]["note"] == "[REDACTED]"


def test_loop_run_rejects_grant_request_combined_with_wait():
    broker = policy_broker()

    def sensor(context):
        return {"converged": False, "signal_key": "needs-grant", "summary": "needs grant"}

    def actuator(context):
        return {
            "summary": "ambiguous envelope",
            "grant_request": {
                "scope": ["session.launch"],
                "side_effect_class": "session_launch",
                "subject": "wc-1",
                "reason": "launch",
                "ttl_seconds": 600,
            },
            "wait": {"token": "ci-1"},
        }

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator, grant_broker=broker)

    assert status.state == "halted_grant_denied"
    assert "cannot combine with wait" in status.halted_reason


def test_loop_run_revalidates_supplied_grant_handle_and_halts_when_expired():
    broker = policy_broker()
    expired_grant = resolve_grant(broker, good_request(ttl_seconds=100)).grant.to_dict()

    def sensor(context):
        return {"converged": False, "signal_key": "reuse", "summary": "reuse handle"}

    def actuator(context):
        # Reuse a grant minted long ago; wall-clock now is far past its expiry.
        return {"summary": "reuse expired grant", "grant": expired_grant, "grant_action": "session.status"}

    status = loop_run(loop_spec(), sensor=sensor, actuator=actuator, grant_broker=broker)

    assert status.state == "halted_grant_denied"
    assert "expired" in status.halted_reason
