"""Interactive interrupt decisions on approval-gated capability calls (issue #111).

deepagents' HITL is richer than the pre-provisioned ``approval_id`` fast path:
an operator can inspect a pending tool call and approve / edit / reject /
respond to it. This closes that gap for Dynamic Workflows' generic
``capability()`` seam:

* a run suspended on an approval-required call (no pre-provisioned
  ``approval_id``, ``CapabilityPolicy.interactive_approval=True``, a
  ``control_store`` wired) exposes the pending call (method, redacted params
  summary, call id) via ``workflow_control status``;
* an operator records one of four decisions via
  ``hermes_workflows.controls.decide_call`` (or the plugin's
  ``workflow_control`` ``decide_call`` action): ``approve`` (run as-is),
  ``edit`` (run with operator-modified arguments — journaled/replayed as
  authoritative), ``reject`` (deterministic, catchable, non-retryable
  denial), ``respond`` (an operator-supplied result, no dispatch at all);
* resuming (``replay_from``) re-reaches the exact pending call and applies the
  decision deterministically;
* the pre-provisioned ``approval_id`` fast path (``interactive_approval``
  defaults ``False``) is entirely unchanged.

Covers both the broker-level mechanics (:class:`CapabilityBroker.handle`
directly, mirroring the existing Kanban-suspend test style) and full
suspend -> decide -> resume end-to-end runs through
:func:`hermes_workflows.run_workflow_script`, plus the ``controls.py``
primitives and the plugin ``workflow_control`` wiring.
"""

from __future__ import annotations

import importlib.util
import json
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from hermes_workflows import CapabilityPolicy, CapabilityRegistry, VMLimits, run_workflow_script
from hermes_workflows.capabilities import finalize_capability_result
from hermes_workflows.controls import (
    ControlError,
    FileControlStore,
    InMemoryControlStore,
    decide_call,
    latest_call_decision,
    project_control_state,
    waits_from_suspended_run,
)
from hermes_workflows.script_store import ScriptRunStore
from hermes_workflows.vm import CapabilityBroker

META = 'meta = {"name": "interactive-approval", "description": "d"}\n'

SCRIPT = META + (
    'try:\n'
    '    r = await capability("tools.write", {"path": "a.txt"})\n'
    '    return {"outcome": "ok", "result": r}\n'
    'except CapabilityError as e:\n'
    '    return {"outcome": "denied", "code": e.code, "message": str(e), "retryable": e.retryable}\n'
)


def _registry(calls: list[dict[str, Any]]) -> CapabilityRegistry:
    registry = CapabilityRegistry()

    def _write(ctx: dict[str, Any]) -> dict[str, Any]:
        calls.append(dict(ctx["input"]))
        return {"ok": True, "wrote": dict(ctx["input"])}

    registry.register(
        "tools.write",
        _write,
        side_effect_class="external_write",
        # Interactive approval only ever suspends/resumes on a run created with
        # replay_from once decided (issue #5's replay-safety guard, unchanged by
        # this slice): a resumed run must never live-redispatch a non-replayable
        # side-effecting capability, so a host wanting interactive approvals on
        # a mutating capability must mark it replayable (and honor the
        # idempotency key it is handed, same as any other resumable capability).
        replayable=True,
        description="test write capability",
    )
    return registry


def _interactive_policy(**overrides: Any) -> CapabilityPolicy:
    kwargs: dict[str, Any] = {
        "allowed_side_effect_classes": ("read_only", "external_write"),
        "interactive_approval": True,
    }
    kwargs.update(overrides)
    return CapabilityPolicy(**kwargs)


def _cap_frame(call_id: int = 1, **params: Any) -> dict[str, Any]:
    params.setdefault("name", "tools.write")
    params.setdefault("input", {"path": "a.txt"})
    return {"t": "call", "id": call_id, "method": "capability", "params": params}


def _broker(
    registry,
    *,
    control_store=None,
    policy=None,
    idempotency_root="run-1",
    active_run_id=None,
    decision_run_ids=(),
    recorder=None,
    replay=None,
):
    return CapabilityBroker(
        agent_runner=lambda agent_id, input: {},
        limits=VMLimits(),
        capability_registry=registry,
        capability_policy=policy if policy is not None else _interactive_policy(),
        control_store=control_store,
        idempotency_root=idempotency_root,
        active_run_id=active_run_id if active_run_id is not None else idempotency_root,
        decision_run_ids=decision_run_ids,
        recorder=recorder,
        replay=replay,
    )


# --------------------------------------------------------------------------- #
# Broker-level mechanics.
# --------------------------------------------------------------------------- #


def test_fast_path_unaffected_when_interactive_approval_is_off():
    # Default CapabilityPolicy.interactive_approval=False: identical to the
    # pre-#111 behaviour even with a control_store wired.
    calls: list[dict[str, Any]] = []
    broker = _broker(
        _registry(calls),
        control_store=InMemoryControlStore(),
        policy=CapabilityPolicy(allowed_side_effect_classes=("read_only", "external_write")),
    )
    ret = broker.handle(_cap_frame(1))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "capability_approval_required"
    assert broker.should_suspend is False
    assert calls == []


def test_fast_path_unaffected_without_a_control_store():
    # interactive_approval=True but no control_store wired: interactive approval
    # cannot function (nowhere to record/read a decision), so it falls back to
    # the same fast-path denial rather than suspending forever.
    calls: list[dict[str, Any]] = []
    broker = _broker(_registry(calls), control_store=None)
    ret = broker.handle(_cap_frame(1))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "capability_approval_required"
    assert broker.should_suspend is False


def test_preapproved_approval_id_still_bypasses_interactive_gate():
    calls: list[dict[str, Any]] = []
    policy = _interactive_policy(approved_approval_ids=("preapproved-1",))
    broker = _broker(_registry(calls), control_store=InMemoryControlStore(), policy=policy)
    ret = broker.handle(_cap_frame(1, approval_id="preapproved-1"))
    assert ret["ok"] is True
    assert ret["value"]["wrote"] == {"path": "a.txt"}
    assert calls == [{"path": "a.txt"}]


def test_undecided_call_suspends_with_pending_call_detail():
    calls: list[dict[str, Any]] = []
    store = InMemoryControlStore()
    broker = _broker(_registry(calls), control_store=store)
    ret = broker.handle(_cap_frame(1))

    assert ret["ok"] is False
    assert ret["error"]["code"] == "capability_approval_pending"
    assert ret["error"]["retryable"] is False
    assert broker.should_suspend is True
    assert broker.suspend_info == {
        "type": "ApprovalPending",
        "call_id": 1,
        "method": "capability",
        "name": "tools.write",
        "side_effect_class": "external_write",
        "params_summary": {"path": "a.txt"},
    }
    assert calls == []  # never dispatched.


def test_approve_decision_dispatches_with_original_params():
    calls: list[dict[str, Any]] = []
    store = InMemoryControlStore()
    decide_call(store, "run-1", "1", "approve")
    broker = _broker(_registry(calls), control_store=store)
    ret = broker.handle(_cap_frame(1))
    assert ret["ok"] is True, ret
    assert ret["value"]["wrote"] == {"path": "a.txt"}
    assert calls == [{"path": "a.txt"}]
    assert broker.should_suspend is False


def test_edit_decision_dispatches_with_operator_modified_input():
    calls: list[dict[str, Any]] = []
    store = InMemoryControlStore()
    decide_call(store, "run-1", "1", "edit", input={"path": "b.txt"})
    broker = _broker(_registry(calls), control_store=store)
    ret = broker.handle(_cap_frame(1))
    assert ret["ok"] is True, ret
    assert ret["value"]["wrote"] == {"path": "b.txt"}
    assert calls == [{"path": "b.txt"}]  # the operator's arguments, not the script's.


def test_reject_decision_denies_deterministically_without_dispatch():
    calls: list[dict[str, Any]] = []
    store = InMemoryControlStore()
    decide_call(store, "run-1", "1", "reject", reason="not authorized")
    broker = _broker(_registry(calls), control_store=store)
    ret = broker.handle(_cap_frame(1))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "capability_rejected"
    assert ret["error"]["retryable"] is False
    assert "not authorized" in ret["error"]["message"]
    assert calls == []
    assert broker.should_suspend is False


def test_respond_decision_returns_operator_value_without_dispatch():
    calls: list[dict[str, Any]] = []
    store = InMemoryControlStore()
    decide_call(store, "run-1", "1", "respond", value={"ok": True, "wrote": {"path": "operator-supplied"}})
    broker = _broker(_registry(calls), control_store=store)
    ret = broker.handle(_cap_frame(1))
    assert ret["ok"] is True, ret
    assert ret["value"]["wrote"] == {"path": "operator-supplied"}
    assert calls == []  # the handler never ran.


def test_respond_decision_result_is_bounded_like_any_capability_result():
    calls: list[dict[str, Any]] = []
    store = InMemoryControlStore()
    decide_call(store, "run-1", "1", "respond", value={"blob": "x" * 1000})
    policy = _interactive_policy(max_result_bytes=256)
    broker = _broker(_registry(calls), control_store=store, policy=policy)
    ret = broker.handle(_cap_frame(1))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "capability_result_too_large"


def test_latest_recorded_decision_governs_a_correction():
    calls: list[dict[str, Any]] = []
    store = InMemoryControlStore()
    decide_call(store, "run-1", "1", "reject", reason="too soon")
    decide_call(store, "run-1", "1", "approve")  # operator reconsiders before it is consumed.
    broker = _broker(_registry(calls), control_store=store)
    ret = broker.handle(_cap_frame(1))
    assert ret["ok"] is True
    assert calls == [{"path": "a.txt"}]


def test_decision_for_a_different_call_id_does_not_apply():
    calls: list[dict[str, Any]] = []
    store = InMemoryControlStore()
    decide_call(store, "run-1", "999", "approve")
    broker = _broker(_registry(calls), control_store=store)
    ret = broker.handle(_cap_frame(1))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "capability_approval_pending"


def test_pending_call_decision_consults_every_replay_chain_generation():
    # A chained resume: A suspends, resume B suspends again (still undecided),
    # and the operator decides against B -- the run they actually watched
    # suspend, not the logical root A. The next resume's broker is handed the
    # whole chain (root-first) via ``decision_run_ids``.
    calls: list[dict[str, Any]] = []
    store = InMemoryControlStore()
    decide_call(store, "B", "1", "approve")
    broker = _broker(
        _registry(calls),
        control_store=store,
        idempotency_root="A",
        active_run_id="C",
        decision_run_ids=("A", "B"),
    )
    ret = broker.handle(_cap_frame(1))
    assert ret["ok"] is True, ret
    assert calls == [{"path": "a.txt"}]


def test_name_disallowed_capability_denies_instead_of_suspending():
    # A capability the run policy's ``allowed_names`` forbids outright must
    # never suspend awaiting an approval it could never be granted.
    calls: list[dict[str, Any]] = []
    store = InMemoryControlStore()
    policy = _interactive_policy(allowed_names=("tools.other",))
    broker = _broker(_registry(calls), control_store=store, policy=policy)
    ret = broker.handle(_cap_frame(1))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "capability_denied"
    assert broker.should_suspend is False
    assert calls == []


def test_name_disallowed_capability_ignores_a_recorded_respond_decision():
    # Even a pre-recorded ``respond`` decision must not smuggle a value back
    # for a capability the host's allowlist forbids entirely -- the name
    # check must run before any decision is consulted.
    calls: list[dict[str, Any]] = []
    store = InMemoryControlStore()
    decide_call(store, "run-1", "1", "respond", value={"ok": True, "wrote": {"path": "smuggled"}})
    policy = _interactive_policy(allowed_names=("tools.other",))
    broker = _broker(_registry(calls), control_store=store, policy=policy)
    ret = broker.handle(_cap_frame(1))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "capability_denied"
    assert calls == []


def test_non_replayable_mutating_capability_denies_instead_of_suspending():
    # A mutating capability that has not opted into replay safety would suspend
    # into a dead end: every resume aborts at the pre-existing replay-unsafe
    # guard before any decision is ever consulted (issue #5). Suspending
    # forever would be worse than denying (DESIGN 5.14), so this must deny up
    # front instead, exactly like the missing-control-store fast path.
    calls: list[dict[str, Any]] = []
    registry = CapabilityRegistry()
    registry.register(
        "tools.mutate",
        lambda ctx: calls.append(dict(ctx["input"])) or {"ok": True},
        side_effect_class="external_write",
        replayable=False,
        description="non-replayable mutating capability",
    )
    store = InMemoryControlStore()
    broker = _broker(registry, control_store=store)
    ret = broker.handle(_cap_frame(1, name="tools.mutate"))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "capability_approval_required"
    assert broker.should_suspend is False
    assert calls == []


# --------------------------------------------------------------------------- #
# ``capabilities.finalize_capability_result``.
# --------------------------------------------------------------------------- #


def test_finalize_capability_result_matches_registry_run_shape():
    registry = CapabilityRegistry()
    registry.register("tools.echo", lambda ctx: {"ok": True, "output": ctx["input"]}, description="d")
    policy = CapabilityPolicy()
    via_registry = registry.run("tools.echo", {"input": {"a": 1}}, policy=policy, run_context={})
    via_finalize = finalize_capability_result({"ok": True, "output": {"a": 1}}, side_effect_class="read_only", policy=policy)
    assert via_registry == via_finalize


def test_finalize_capability_result_redacts_and_bounds():
    policy = CapabilityPolicy(max_result_bytes=256)
    redacted = finalize_capability_result({"token": "secret", "ok": True}, side_effect_class="read_only", policy=policy)
    assert redacted["token"] == "[REDACTED]"
    with pytest.raises(Exception):
        finalize_capability_result({"blob": "x" * 1000}, side_effect_class="read_only", policy=policy)


# --------------------------------------------------------------------------- #
# ``controls.decide_call`` / ``latest_call_decision`` / ``waits_from_suspended_run``.
# --------------------------------------------------------------------------- #


def test_decide_call_requires_a_call_id():
    store = InMemoryControlStore()
    with pytest.raises(ControlError):
        decide_call(store, "run-1", "", "approve")


def test_decide_call_rejects_unknown_decision():
    store = InMemoryControlStore()
    with pytest.raises(ControlError):
        decide_call(store, "run-1", "1", "maybe")  # type: ignore[arg-type]


def test_decide_call_edit_requires_input_object():
    store = InMemoryControlStore()
    with pytest.raises(ControlError):
        decide_call(store, "run-1", "1", "edit")
    with pytest.raises(ControlError):
        decide_call(store, "run-1", "1", "edit", input="not-a-dict")  # type: ignore[arg-type]


def test_decide_call_respond_requires_value():
    store = InMemoryControlStore()
    with pytest.raises(ControlError):
        decide_call(store, "run-1", "1", "respond")


def test_decide_call_respond_accepts_a_literal_none_value():
    store = InMemoryControlStore()
    control = decide_call(store, "run-1", "1", "respond", value=None)
    state = project_control_state("run-1", store.list_for("run-1"))
    row = latest_call_decision(state, "1")
    assert row is not None
    assert row["decision"] == "respond"
    assert row["value"] is None
    assert control.decision == "respond"


def test_decide_call_rejects_input_or_value_on_the_wrong_decision():
    store = InMemoryControlStore()
    with pytest.raises(ControlError):
        decide_call(store, "run-1", "1", "approve", input={"a": 1})
    with pytest.raises(ControlError):
        decide_call(store, "run-1", "1", "approve", value=1)


def test_latest_call_decision_returns_the_last_recorded_row():
    store = InMemoryControlStore()
    decide_call(store, "run-1", "1", "reject", reason="first")
    decide_call(store, "run-1", "1", "reject", reason="second")
    state = project_control_state("run-1", store.list_for("run-1"))
    row = latest_call_decision(state, "1")
    assert row is not None and row["reason"] == "second"
    assert latest_call_decision(state, "does-not-exist") is None


def test_waits_from_suspended_run_extracts_pending_call():
    meta = {
        "run_id": "r1",
        "status": "suspended",
        "error": {
            "type": "ApprovalPending",
            "call_id": 1,
            "method": "capability",
            "name": "tools.write",
            "side_effect_class": "external_write",
            "params_summary": {"path": "a.txt"},
        },
    }
    waits = waits_from_suspended_run(meta)
    assert len(waits) == 1
    wait = waits[0]
    assert wait.run_id == "r1"
    assert wait.wait_id == "1"
    assert wait.kind == "approval"
    assert wait.source == "capability"
    assert wait.ref["name"] == "tools.write"
    assert wait.ref["params_summary"] == {"path": "a.txt"}


@pytest.mark.parametrize(
    "meta",
    [
        {"run_id": "r1", "status": "failed", "error": {"type": "ApprovalPending", "call_id": 1}},
        {"run_id": "r1", "status": "suspended", "error": {"type": "KanbanSuspended", "call_id": 1}},
        {"run_id": "r1", "status": "suspended", "error": None},
        {"run_id": "r1", "status": "suspended"},
        "not-a-dict",
        None,
    ],
)
def test_waits_from_suspended_run_empty_for_non_approval_shapes(meta):
    assert waits_from_suspended_run(meta) == []


# --------------------------------------------------------------------------- #
# End-to-end: suspend -> decide -> resume through run_workflow_script.
# --------------------------------------------------------------------------- #


def _run(store, control_store, calls, *, run_id, replay_from=None):
    return run_workflow_script(
        SCRIPT,
        store=store,
        run_id=run_id,
        replay_from=replay_from,
        capability_registry=_registry(calls),
        capability_policy=_interactive_policy(),
        control_store=control_store,
    )


def test_e2e_suspend_then_approve_then_resume():
    with tempfile.TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        control_store = FileControlStore(Path(tmp) / "controls")
        calls: list[dict[str, Any]] = []

        a = _run(store, control_store, calls, run_id="A")
        assert a.ok is False and a.suspended is True
        assert a.error["type"] == "ApprovalPending"
        assert a.error["call_id"] == 1
        assert a.error["name"] == "tools.write"
        assert store.load_run("A").status == "suspended"
        assert calls == []

        # workflow_control status surfaces the pending call.
        waits = waits_from_suspended_run(store.load_run("A"))
        assert waits[0].wait_id == "1"
        assert waits[0].ref["side_effect_class"] == "external_write"

        decide_call(control_store, "A", "1", "approve", actor="operator")

        b = _run(store, control_store, calls, run_id="B", replay_from="A")
        assert b.ok is True, b.error
        assert b.value == {
            "outcome": "ok",
            "result": {"ok": True, "wrote": {"path": "a.txt"}, "side_effect_class": "external_write"},
        }
        assert calls == [{"path": "a.txt"}]
        assert store.load_run("B").status == "succeeded"


def test_e2e_suspend_then_edit_then_resume_feeds_replay_fingerprint():
    with tempfile.TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        control_store = FileControlStore(Path(tmp) / "controls")
        calls: list[dict[str, Any]] = []

        a = _run(store, control_store, calls, run_id="A")
        assert a.suspended is True

        decide_call(control_store, "A", "1", "edit", input={"path": "operator-edited.txt"})
        b = _run(store, control_store, calls, run_id="B", replay_from="A")

        assert b.ok is True, b.error
        assert b.value["result"]["wrote"] == {"path": "operator-edited.txt"}
        assert calls == [{"path": "operator-edited.txt"}]
        # A resumed (replay_from) run writes no cache of its own — same as every
        # other live-dispatched call during a resume (e.g. kanban_agent); see
        # test_edited_params_feed_the_replay_fingerprint_and_journal below for the
        # cache/fingerprint assertion on a run where a recorder is present.


def test_edited_params_feed_the_replay_fingerprint_and_journal():
    # Acceptance criteria: edited params, not original, feed the replay
    # fingerprint/journal. Exercised directly against a broker with a real
    # CallRecorder (a decision pre-recorded before the *first* live dispatch,
    # rather than via a suspend -> resume cycle, isolates the persistence
    # behaviour from the "a replay writes no cache of its own" fact above).
    from hermes_workflows.script_store import replay_args_hash

    with tempfile.TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        store.begin("A", script="x", args=None, limits=None, deterministic_runner=False)
        control_store = FileControlStore(Path(tmp) / "controls")
        decide_call(control_store, "A", "1", "edit", input={"path": "operator-edited.txt"})
        calls: list[dict[str, Any]] = []
        broker = _broker(
            _registry(calls),
            control_store=control_store,
            idempotency_root="A",
            recorder=store.recorder("A"),
        )

        ret = broker.handle(_cap_frame(1))
        assert ret["ok"] is True, ret
        assert calls == [{"path": "operator-edited.txt"}]

        entry = store.load_cache("A").get(1)
        assert entry is not None and entry.ok is True
        edited_params = {"name": "tools.write", "input": {"path": "operator-edited.txt"}}
        original_params = {"name": "tools.write", "input": {"path": "a.txt"}}
        assert entry.args_hash == replay_args_hash("capability", edited_params)
        assert entry.args_hash != replay_args_hash("capability", original_params)


def test_e2e_suspend_then_reject_then_resume_is_catchable_and_not_retryable():
    with tempfile.TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        control_store = FileControlStore(Path(tmp) / "controls")
        calls: list[dict[str, Any]] = []

        a = _run(store, control_store, calls, run_id="A")
        assert a.suspended is True

        decide_call(control_store, "A", "1", "reject", reason="not authorized")
        b = _run(store, control_store, calls, run_id="B", replay_from="A")

        assert b.ok is True, b.error  # the script caught CapabilityError and returned normally.
        assert b.value == {
            "outcome": "denied",
            "code": "capability_rejected",
            "message": "not authorized",
            "retryable": False,
        }
        assert calls == []
        assert store.load_run("B").status == "succeeded"


def test_e2e_suspend_then_respond_then_resume_never_dispatches():
    with tempfile.TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        control_store = FileControlStore(Path(tmp) / "controls")
        calls: list[dict[str, Any]] = []

        a = _run(store, control_store, calls, run_id="A")
        assert a.suspended is True

        decide_call(control_store, "A", "1", "respond", value={"ok": True, "wrote": {"path": "operator-value"}})
        b = _run(store, control_store, calls, run_id="B", replay_from="A")

        assert b.ok is True, b.error
        assert b.value["outcome"] == "ok"
        assert b.value["result"]["wrote"] == {"path": "operator-value"}
        assert calls == []  # the capability handler never ran.


def test_e2e_resume_suspends_again_while_still_undecided():
    with tempfile.TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        control_store = FileControlStore(Path(tmp) / "controls")
        calls: list[dict[str, Any]] = []

        a = _run(store, control_store, calls, run_id="A")
        assert a.suspended is True

        b = _run(store, control_store, calls, run_id="B", replay_from="A")
        assert b.suspended is True
        assert b.error["call_id"] == 1

        decide_call(control_store, "A", "1", "approve")
        c = _run(store, control_store, calls, run_id="C", replay_from="A")
        assert c.ok is True, c.error
        assert calls == [{"path": "a.txt"}]


def test_e2e_decision_against_the_resumed_generation_is_honored():
    # workflow_control status reports each suspended generation's *own* run id
    # (waits_from_suspended_run), so an operator naturally decides against the
    # run they just watched suspend -- here B, a mid-chain generation, not the
    # logical root A. That decision must still be found and applied on the
    # next resume, not silently ignored forever.
    with tempfile.TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        control_store = FileControlStore(Path(tmp) / "controls")
        calls: list[dict[str, Any]] = []

        a = _run(store, control_store, calls, run_id="A")
        assert a.suspended is True

        b = _run(store, control_store, calls, run_id="B", replay_from="A")
        assert b.suspended is True
        waits = waits_from_suspended_run(store.load_run("B"))
        assert waits[0].run_id == "B"

        decide_call(control_store, "B", "1", "approve")
        c = _run(store, control_store, calls, run_id="C", replay_from="B")
        assert c.ok is True, c.error
        assert calls == [{"path": "a.txt"}]


# --------------------------------------------------------------------------- #
# Plugin ``workflow_control`` wiring: status surfaces the wait, decide_call
# dispatches the operator decision.
# --------------------------------------------------------------------------- #


class _FakeContext:
    def __init__(self) -> None:
        self.tools: dict[str, dict[str, Any]] = {}

    def register_tool(self, **kwargs: Any) -> None:
        self.tools[kwargs["name"]] = kwargs


def _load_plugin_root() -> ModuleType:
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("dynamic_workflows_plugin_approvals", root / "__init__.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_plugin_workflow_control_surfaces_and_decides_a_pending_approval(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp)
        monkeypatch.setenv("HERMES_WORKFLOWS_STATE_DIR", str(state_dir / "runs"))
        plugin = _load_plugin_root()
        ctx = _FakeContext()
        plugin.register(ctx)
        ctl = ctx.tools["workflow_control"]["handler"]

        # The plugin's own ``workflow`` facade does not wire a capability
        # registry (that stays a host/deployment concern, never model-facing
        # JSON) — run directly through the library API against the *same*
        # store paths the plugin reads (§_state_root), so workflow_control
        # observes it exactly as it would a real suspended script run.
        # Controls and script-runs are siblings of HERMES_WORKFLOWS_STATE_DIR
        # (see __init__.py's _state_root), not nested under it.
        store = ScriptRunStore(state_dir / "script-runs")
        control_store = FileControlStore(state_dir / "controls")
        calls: list[dict[str, Any]] = []
        result = run_workflow_script(
            SCRIPT,
            store=store,
            run_id="A",
            capability_registry=_registry(calls),
            capability_policy=_interactive_policy(),
            control_store=control_store,
        )
        assert result.suspended is True

        status = json.loads(ctl({"action": "status", "run_id": "A"}))
        assert status["success"] is True
        assert status["data"]["lifecycle"] == "suspended"
        assert status["data"]["waits"] == [
            {
                "run_id": "A",
                "wait_id": "1",
                "kind": "approval",
                "state": "suspended",
                "summary": "tools.write",
                "source": "capability",
                "ref": {
                    "call_id": 1,
                    "method": "capability",
                    "name": "tools.write",
                    "side_effect_class": "external_write",
                    "params_summary": {"path": "a.txt"},
                },
            }
        ]

        overview = json.loads(ctl({"action": "overview"}))
        assert overview["success"] is True
        assert any(w["wait_id"] == "1" for w in overview["data"]["blocked_waits"])

        decided = json.loads(
            ctl(
                {
                    "action": "decide_call",
                    "run_id": "A",
                    "target_ref": "1",
                    "decision": "edit",
                    "input": {"path": "operator.txt"},
                    "actor": "operator",
                }
            )
        )
        assert decided["success"] is True
        assert decided["data"]["control"]["decision"] == "edit"

        resumed = run_workflow_script(
            SCRIPT,
            store=store,
            run_id="B",
            replay_from="A",
            capability_registry=_registry(calls),
            capability_policy=_interactive_policy(),
            control_store=control_store,
        )
        assert resumed.ok is True, resumed.error
        assert resumed.value["result"]["wrote"] == {"path": "operator.txt"}
        assert calls == [{"path": "operator.txt"}]

        final_status = json.loads(ctl({"action": "status", "run_id": "B"}))
        assert final_status["data"]["lifecycle"] == "succeeded"
        assert final_status["data"]["waits"] == []


def test_plugin_workflow_control_schema_advertises_decide_call(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setenv("HERMES_WORKFLOWS_STATE_DIR", str(Path(tmp) / "runs"))
        plugin = _load_plugin_root()
        ctx = _FakeContext()
        plugin.register(ctx)
        schema = ctx.tools["workflow_control"]["schema"]
        properties = schema["parameters"]["properties"]
        assert "decide_call" in properties["action"]["enum"]
        assert set(properties["decision"]["enum"]) == {"approve", "edit", "reject", "respond", None}
        assert "input" in properties
        assert "value" in properties


def test_plugin_workflow_control_decide_call_respond_and_reject(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp)
        monkeypatch.setenv("HERMES_WORKFLOWS_STATE_DIR", str(state_dir / "runs"))
        plugin = _load_plugin_root()
        ctx = _FakeContext()
        plugin.register(ctx)
        ctl = ctx.tools["workflow_control"]["handler"]
        store = ScriptRunStore(state_dir / "script-runs")
        control_store = FileControlStore(state_dir / "controls")

        calls: list[dict[str, Any]] = []
        run_workflow_script(
            SCRIPT, store=store, run_id="A",
            capability_registry=_registry(calls), capability_policy=_interactive_policy(),
            control_store=control_store,
        )

        respond = json.loads(
            ctl(
                {
                    "action": "decide_call",
                    "run_id": "A",
                    "target_ref": "1",
                    "decision": "respond",
                    "value": {"ok": True, "wrote": {"path": "plugin-respond"}},
                }
            )
        )
        assert respond["success"] is True
        assert respond["data"]["control"]["decision"] == "respond"

        resumed = run_workflow_script(
            SCRIPT, store=store, run_id="B", replay_from="A",
            capability_registry=_registry(calls), capability_policy=_interactive_policy(),
            control_store=control_store,
        )
        assert resumed.ok is True, resumed.error
        assert resumed.value["result"]["wrote"] == {"path": "plugin-respond"}
        assert calls == []

        reject = json.loads(
            ctl(
                {
                    "action": "decide_call",
                    "run_id": "A",
                    "target_ref": "1",
                    "decision": "reject",
                    "reason": "changed my mind",
                }
            )
        )
        assert reject["success"] is True
        assert reject["data"]["control"]["decision"] == "reject"
        # A correction after the run already consumed "respond" has no effect on
        # the already-succeeded run B; it only matters for a not-yet-resumed one.
        assert json.loads(ctl({"action": "status", "run_id": "B"}))["data"]["lifecycle"] == "succeeded"


def test_plugin_workflow_control_decide_call_requires_known_run(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setenv("HERMES_WORKFLOWS_STATE_DIR", str(Path(tmp) / "runs"))
        plugin = _load_plugin_root()
        ctx = _FakeContext()
        plugin.register(ctx)
        ctl = ctx.tools["workflow_control"]["handler"]
        result = json.loads(
            ctl({"action": "decide_call", "run_id": "does-not-exist", "target_ref": "1", "decision": "approve"})
        )
        assert result["success"] is False
