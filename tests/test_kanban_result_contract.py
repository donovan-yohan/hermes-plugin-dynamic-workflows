"""Tests for structured result contracts on Kanban workflow tasks (issue #6).

`await kanban_agent(..., schema=...)` declares a *payload* schema over the worker's
``metadata.workflow_result`` (not the envelope, and not one global workflow
schema). The runtime validates that payload before resolving the awaitable, so a
workflow branches on a typed object — never on prose. A missing/invalid result is
a deterministic block/fail with diagnostics, not a success.

Covers: the validator, the worker-facing card instruction, valid result, missing
result, schema mismatch, no-schema passthrough, retry/unblock under ``pause``,
journal + Kanban-comment diagnostics, and the end-to-end subprocess path.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from hermes_workflows import run_workflow_script
from hermes_workflows.kanban import (
    CARD_BLOCKED,
    CARD_COMPLETED,
    InMemoryKanbanBackend,
    KanbanCard,
    KanbanResolution,
    kanban_card_id,
    result_contract_instruction,
    validate_workflow_result,
)
from hermes_workflows.script_store import ScriptRunStore
from hermes_workflows.vm import CapabilityBroker, VMLimits

META = 'meta = {"name": "k6", "description": "d"}\n'


def _broker(backend, *, root="root", limits=None):
    return CapabilityBroker(
        agent_runner=lambda agent_id, input: {},
        limits=limits or VMLimits(),
        kanban_backend=backend,
        idempotency_root=root,
    )


def _kanban_frame(call_id, **params):
    params.setdefault("profile", "planner")
    return {"t": "call", "id": call_id, "method": "kanban_agent", "params": params}


def _auto(result, status=CARD_COMPLETED):
    return lambda spec: {"status": status, "result": result}


# --------------------------------------------------------------------------- #
# validate_workflow_result — the payload contract
# --------------------------------------------------------------------------- #

def test_validate_no_schema_accepts_any_payload():
    # Unknown payloads are preserved: no schema means no validation.
    assert validate_workflow_result({"anything": [1, 2]}, None) == []
    assert validate_workflow_result("prose", None) == []
    assert validate_workflow_result(None, {}) == []


def test_validate_requires_declared_fields_and_types():
    schema = {"plan": "string", "steps": "list", "confidence": "number"}
    assert validate_workflow_result(
        {"plan": "x", "steps": [], "confidence": 0.9}, schema
    ) == []
    # Missing + wrong type are each reported.
    diags = validate_workflow_result({"plan": 1, "steps": []}, schema)
    assert any("confidence" in d and "missing" in d for d in diags)
    assert any("plan" in d and "expected string" in d for d in diags)


def test_validate_rejects_non_dict_and_bool_for_number():
    assert validate_workflow_result("prose", {"plan": "string"}) == [
        "workflow_result is missing or not an object (got str)"
    ]
    # bool must not satisfy a number/int field (mirrors brokered-output policy).
    assert validate_workflow_result({"n": True}, {"n": "int"})


def test_validate_preserves_extra_fields():
    # Extra fields beyond the schema are allowed (templates may add shape).
    assert validate_workflow_result({"plan": "x", "extra": 1}, {"plan": "string"}) == []


def test_validate_permissive_hints_accept_bool():
    # Regression (review): a permissive field (the `any` hint -> object, or the
    # Python `object` type) must accept a bool — the int-subclass bool guard only
    # applies to numeric/text fields.
    assert validate_workflow_result({"flag": True}, {"flag": "any"}) == []
    assert validate_workflow_result({"flag": True}, {"flag": object}) == []
    # bool is still explicitly accepted where bool/boolean is declared.
    assert validate_workflow_result({"flag": True}, {"flag": "bool"}) == []
    # ...and still rejected for a numeric field (bool is an int subclass).
    assert validate_workflow_result({"n": True}, {"n": "int"})
    # `"object"` means a dict (not Python object), so it correctly rejects a bool.
    assert validate_workflow_result({"d": True}, {"d": "object"})


def test_result_contract_instruction():
    assert result_contract_instruction(None) == ""
    assert result_contract_instruction({}) == ""
    text = result_contract_instruction({"plan": "string"})
    assert "workflow_result" in text
    assert "plan: string" in text
    assert "block" in text.lower()  # instructs to block rather than emit prose.


# --------------------------------------------------------------------------- #
# Card carries the worker instruction when a schema is set
# --------------------------------------------------------------------------- #

def test_card_body_instructs_worker_when_schema_present():
    backend = InMemoryKanbanBackend(auto="blocked", known_profiles={"planner"})
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(1, schema={"plan": "string"}, on_block="return"))
    card_id = ret["value"]["card_id"]
    instruction = backend.instruction_for(card_id)
    assert "workflow_result" in instruction and "plan: string" in instruction


def test_card_has_no_instruction_without_schema():
    backend = InMemoryKanbanBackend(auto="blocked", known_profiles={"planner"})
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(1, on_block="return"))
    assert backend.instruction_for(ret["value"]["card_id"]) == ""


# --------------------------------------------------------------------------- #
# Broker: valid / invalid / missing result
# --------------------------------------------------------------------------- #

def test_valid_result_resolves_with_typed_payload():
    backend = InMemoryKanbanBackend(
        auto=_auto({"plan": "do x", "confidence": 0.9}), known_profiles={"planner"}
    )
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(1, schema={"plan": "string", "confidence": "number"}))
    assert ret["ok"] is True
    assert ret["value"]["status"] == CARD_COMPLETED
    assert ret["value"]["workflow_result"] == {"plan": "do x", "confidence": 0.9}
    assert "diagnostics" not in ret["value"]


def test_invalid_result_blocks_with_diagnostics_and_card_comment():
    backend = InMemoryKanbanBackend(auto=_auto({"plan": 123}), known_profiles={"planner"})
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(1, schema={"plan": "string"}, on_block="return"))
    assert ret["ok"] is True
    assert ret["value"]["status"] == CARD_BLOCKED  # NOT completed: prose is not success.
    assert ret["value"]["diagnostics"]
    assert "schema" in ret["value"]["reason"]
    # The diagnostics are surfaced as a card comment/event.
    assert any(e["kind"] == "result_invalid" for e in backend.events)


def test_missing_result_blocks():
    # auto="completed" yields {"echo": ...}, which lacks the declared field.
    backend = InMemoryKanbanBackend(auto="completed", known_profiles={"planner"})
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(1, schema={"plan": "string"}, on_block="return"))
    assert ret["value"]["status"] == CARD_BLOCKED


def test_invalid_result_under_raise_denies_into_script():
    backend = InMemoryKanbanBackend(auto=_auto({}), known_profiles={"planner"})
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(1, schema={"plan": "string"}, on_block="raise"))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "kanban_blocked"


def test_no_schema_passes_unknown_payload_through():
    backend = InMemoryKanbanBackend(
        auto=_auto({"freeform": [1, 2], "note": "anything"}), known_profiles={"planner"}
    )
    broker = _broker(backend)
    ret = broker.handle(_kanban_frame(1, on_block="return"))  # no schema.
    assert ret["value"]["status"] == CARD_COMPLETED
    assert ret["value"]["workflow_result"] == {"freeform": [1, 2], "note": "anything"}


# --------------------------------------------------------------------------- #
# Retry / unblock under pause
# --------------------------------------------------------------------------- #

def test_pause_retries_until_a_valid_result():
    # An invalid completion under pause must not resolve; the worker retries with a
    # valid result and the await then resolves completed.
    backend = InMemoryKanbanBackend(known_profiles={"planner"})
    broker = _broker(backend, root="retry", limits=VMLimits(max_runtime_s=2.0))
    card_id = kanban_card_id("retry:1")

    def _worker():
        backend.resolve(card_id, CARD_COMPLETED, result={"plan": 123})  # wrong type.
        # Wait until the broker has recorded the rejection, then re-complete valid.
        for _ in range(3000):
            if any(e["kind"] == "result_invalid" for e in list(backend.events)):
                break
            time.sleep(0.001)
        backend.resolve(card_id, CARD_COMPLETED, result={"plan": "ok now"})

    worker = threading.Thread(target=_worker)
    worker.start()
    try:
        ret = broker.handle(_kanban_frame(1, schema={"plan": "string"}, on_block="pause"))
    finally:
        worker.join()
    assert ret["ok"] is True
    assert ret["value"]["status"] == CARD_COMPLETED
    assert ret["value"]["workflow_result"] == {"plan": "ok now"}
    assert sum(e["kind"] == "result_invalid" for e in backend.events) == 1


class _StaleBackend:
    """A broken backend that ignores after_version (always returns the same event)."""

    def create_or_reattach(self, idempotency_key, spec):
        return KanbanCard(card_id="kbc_stale", profile=spec.profile)

    def await_resolution(self, card_id, *, accept_blocked, timeout, after_version=0):
        # Always a completed-but-invalid result at a fixed version, regardless of
        # after_version — exactly the contract violation the broker must not spin on.
        return KanbanResolution(
            card_id=card_id, profile="planner", status=CARD_COMPLETED, result={}, version=1
        )


def test_pause_fails_closed_if_backend_ignores_after_version():
    # Regression (review): the pause retry's only guard against re-consuming the
    # same rejected completion is after_version. A backend that ignores it would
    # hot-spin to the deadline; the broker fails closed with kanban_error instead.
    broker = _broker(_StaleBackend(), limits=VMLimits(max_runtime_s=2.0))
    ret = broker.handle(_kanban_frame(1, schema={"plan": "string"}, on_block="pause"))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "kanban_error"


# --------------------------------------------------------------------------- #
# End-to-end through the subprocess VM
# --------------------------------------------------------------------------- #

def test_e2e_await_returns_typed_object_not_prose():
    backend = InMemoryKanbanBackend(
        auto=_auto({"plan": "ship it", "risk": "low"}), known_profiles={"planner"}
    )
    script = META + (
        'r = await kanban_agent("planner", prompt="plan the work", '
        'schema={"plan": "string", "risk": "string"})\n'
        'return r["workflow_result"]\n'
    )
    res = run_workflow_script(script, kanban_backend=backend)
    assert res.ok, res.error
    assert res.value == {"plan": "ship it", "risk": "low"}  # a typed object, not prose.


def test_e2e_invalid_result_blocks_deterministically_and_journals():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        backend = InMemoryKanbanBackend(auto=_auto({"plan": 7}), known_profiles={"planner"})
        script = META + (
            'r = await kanban_agent("planner", prompt="plan", '
            'schema={"plan": "string"}, on_block="return")\n'
            'return {"status": r["status"], "diagnostics": r.get("diagnostics")}\n'
        )
        res = run_workflow_script(script, store=store, run_id="r6", kanban_backend=backend)
        assert res.ok, res.error
        assert res.value["status"] == CARD_BLOCKED
        assert res.value["diagnostics"]

        # The validation failure is recorded in the durable journal.
        events = store.journal("r6")
        assert any(
            e.get("method") == "kanban_agent"
            and str(e.get("error", "")).startswith("result_invalid")
            for e in events
        ), events
