"""Tests for backend-neutral operator controls / status / waits (issue #9).

Covers: control record shape + round-trip, the pause/resume/stop/task_stop
projection (stop is terminal, audit trail preserved), idempotent retry lineage,
durable FileControlStore survival across a simulated restart, wait inspection
from a real loop suspension and from durable Kanban card states, the compact
single-run (inspect_run) and overview (list_runs) projections, link bundling,
and the plugin ``workflow_control`` tool end to end.

Stdlib only.
"""

from __future__ import annotations

import importlib.util
import json
import os
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from hermes_workflows.controls import (
    FileControlStore,
    InMemoryControlStore,
    RunControlState,
    WaitSummary,
    WorkflowControl,
    current_phase,
    inspect_run,
    list_runs,
    pause_run,
    project_control_state,
    record_control,
    resume_run,
    retry,
    run_links,
    stop_run,
    stop_task,
    summarize_run,
    waits_from_kanban_states,
    waits_from_loop_status,
)
from hermes_workflows.errors import ControlError
from hermes_workflows.loops import FileLoopRunStore, LoopSensorResult, loop_run
from hermes_workflows.primitives import workflow_run
from hermes_workflows.registry import InMemoryRunStore
from hermes_workflows.script_store import ScriptRunStore


# --------------------------------------------------------------------------- #
# Control records
# --------------------------------------------------------------------------- #

def test_control_record_round_trips_through_dict():
    control = WorkflowControl(
        control_id="ctl_1", run_id="wf_a", action="retry", target_ref="call-3",
        replacement_ref="call-3#retry1", attempt=1, actor="op", reason="flaky",
    )
    restored = WorkflowControl.from_dict(control.to_dict())
    assert restored == control


def test_from_dict_rejects_unknown_action_and_missing_ids():
    with pytest.raises(ControlError):
        WorkflowControl.from_dict({"control_id": "c", "run_id": "r", "action": "nope"})
    with pytest.raises(ControlError):
        WorkflowControl.from_dict({"run_id": "r", "action": "stop"})
    with pytest.raises(ControlError):
        WorkflowControl.from_dict({"control_id": "c", "action": "stop"})


def test_record_control_requires_target_ref_for_task_stop_and_retry():
    store = InMemoryControlStore()
    with pytest.raises(ControlError):
        record_control(store, "wf_a", "task_stop")
    with pytest.raises(ControlError):
        record_control(store, "wf_a", "retry")


# --------------------------------------------------------------------------- #
# Projection
# --------------------------------------------------------------------------- #

def test_pause_resume_projection():
    store = InMemoryControlStore()
    pause_run(store, "wf_a", actor="op", reason="cooling off")
    state = project_control_state("wf_a", store.list_for("wf_a"))
    assert state.desired_state == "paused" and state.paused is True

    resume_run(store, "wf_a")
    state = project_control_state("wf_a", store.list_for("wf_a"))
    assert state.desired_state == "running" and state.paused is False


def test_stop_is_terminal_and_preserves_audit_trail():
    store = InMemoryControlStore()
    pause_run(store, "wf_a", reason="pause")
    stop_run(store, "wf_a", actor="op", reason="abort")
    resume_run(store, "wf_a", reason="try anyway")  # must NOT un-stop

    controls = store.list_for("wf_a")
    assert len(controls) == 3  # nothing deleted; full audit trail retained.
    state = project_control_state("wf_a", controls)
    assert state.stopped is True
    assert state.desired_state == "stopped"
    assert state.stop_reason == "abort"
    assert state.stopped_by == "op"


def test_task_stop_collects_per_task_without_stopping_run():
    store = InMemoryControlStore()
    stop_task(store, "wf_a", "call-7", reason="hung")
    state = project_control_state("wf_a", store.list_for("wf_a"))
    assert state.stopped is False
    assert state.desired_state == "running"
    assert state.stopped_tasks == [
        {"target_ref": "call-7", "control_id": state.stopped_tasks[0]["control_id"],
         "reason": "hung", "at": state.stopped_tasks[0]["at"]}
    ]


# --------------------------------------------------------------------------- #
# Retry lineage / idempotency
# --------------------------------------------------------------------------- #

def test_retry_is_idempotent_per_target_then_force_makes_next_attempt():
    store = InMemoryControlStore()
    first = retry(store, "wf_a", "call-3", reason="transient")
    again = retry(store, "wf_a", "call-3")  # idempotent — same record
    assert again.control_id == first.control_id
    assert again.attempt == 1
    assert first.replacement_ref == "call-3#retry1"
    assert len(store.list_for("wf_a")) == 1

    forced = retry(store, "wf_a", "call-3", force=True)
    assert forced.control_id != first.control_id
    assert forced.attempt == 2
    assert forced.replacement_ref == "call-3#retry2"
    assert len(store.list_for("wf_a")) == 2


def test_retry_records_explicit_replacement_ref():
    store = InMemoryControlStore()
    control = retry(store, "wf_a", "task-1", replacement_ref="kanban-card-99")
    assert control.replacement_ref == "kanban-card-99"
    state = project_control_state("wf_a", store.list_for("wf_a"))
    assert state.retries[0]["replacement_ref"] == "kanban-card-99"
    assert state.retries[0]["attempt"] == 1


# --------------------------------------------------------------------------- #
# Durable FileControlStore — survives restart, idempotent across processes
# --------------------------------------------------------------------------- #

def test_file_control_store_survives_restart():
    with tempfile.TemporaryDirectory() as tmp:
        store = FileControlStore(tmp)
        pause_run(store, "wf_a", reason="pause")
        stop_task(store, "wf_a", "call-2", reason="stuck")
        retry(store, "wf_a", "call-2")

        # Simulate a fresh process: brand-new store object over the same root.
        reopened = FileControlStore(tmp)
        controls = reopened.list_for("wf_a")
        assert len(controls) == 3
        state = project_control_state("wf_a", controls)
        assert state.paused is True
        assert state.stopped_tasks[0]["target_ref"] == "call-2"
        assert state.retries[0]["target_ref"] == "call-2"
        assert reopened.runs() == ["wf_a"]


def test_file_control_store_dedupes_retry_id_across_restart():
    with tempfile.TemporaryDirectory() as tmp:
        first = retry(FileControlStore(tmp), "wf_a", "call-9")
        # A fresh store re-issuing the same default retry returns the recorded one
        # and writes no duplicate line (idempotent by deterministic control id).
        again = retry(FileControlStore(tmp), "wf_a", "call-9")
        assert again.control_id == first.control_id
        assert len(FileControlStore(tmp).list_for("wf_a")) == 1


def test_file_control_store_skips_corrupt_line():
    with tempfile.TemporaryDirectory() as tmp:
        store = FileControlStore(tmp)
        pause_run(store, "wf_a")
        log = Path(tmp) / "wf_a" / "controls.jsonl"
        with log.open("a", encoding="utf-8") as f:
            f.write("{not json}\n")
        stop_run(store, "wf_a")  # appended after the torn line
        controls = FileControlStore(tmp).list_for("wf_a")
        assert len(controls) == 2  # the corrupt line is skipped, not fatal.
        assert project_control_state("wf_a", controls).stopped is True


def test_file_control_store_rejects_unsafe_ids():
    with tempfile.TemporaryDirectory() as tmp:
        store = FileControlStore(tmp)
        with pytest.raises(ControlError):
            store.append(WorkflowControl(control_id="c", run_id="../escape", action="stop"))


# --------------------------------------------------------------------------- #
# Wait inspection
# --------------------------------------------------------------------------- #

def _loop_spec(**brakes: Any) -> dict[str, Any]:
    b = {"max_steps": 4, "max_repeated_signal": 3, "max_sensor_retries": 1}
    b.update(brakes)
    return {
        "version": "1",
        "name": "wait_loop",
        "setpoint": {"target": "done", "stop_condition": "sensor converged"},
        "sensors": [{"id": "verify", "primary": True, "kind": "callable"}],
        "actuators": [{"id": "act", "kind": "step"}],
        "brakes": b,
    }


def test_waits_from_loop_status_extracts_event_wait():
    def sensor(_ctx: dict[str, Any]) -> LoopSensorResult:
        return LoopSensorResult(converged=False, signal_key="todo", summary="not done")

    def actuator(_ctx: dict[str, Any]) -> dict[str, Any]:
        return {"wait": {"id": "evt-42", "summary": "waiting on external PR"}, "summary": "dispatched"}

    status = loop_run(_loop_spec(), sensor=sensor, actuator=actuator)
    assert status.state == "waiting_for_event"

    waits = waits_from_loop_status(status)
    assert len(waits) == 1
    wait = waits[0]
    assert wait.kind == "event"
    assert wait.state == "waiting_for_event"
    assert wait.wait_id == "evt-42"
    assert wait.summary == "waiting on external PR"
    assert wait.run_id == status.run_id
    # Equivalent when fed the plain as_dict() snapshot (e.g. read from disk).
    assert waits_from_loop_status(status.as_dict())[0].wait_id == "evt-42"


def test_waits_from_loop_status_empty_when_not_waiting():
    def sensor(_ctx: dict[str, Any]) -> LoopSensorResult:
        return LoopSensorResult(converged=True, signal_key="ok", summary="done")

    status = loop_run(_loop_spec(), sensor=sensor)
    assert status.state == "converged"
    assert waits_from_loop_status(status) == []


def test_waits_from_kanban_states_filters_terminal():
    states = [
        {"card_id": "card-1", "status": "waiting", "profile": "impl"},
        {"card_id": "card-2", "status": "blocked", "reason": "needs review"},
        {"card_id": "card-3", "status": "completed"},  # terminal -> skipped
    ]
    waits = waits_from_kanban_states(states, run_id="wf_a")
    assert [w.wait_id for w in waits] == ["card-1", "card-2"]
    assert all(w.kind == "kanban" and w.run_id == "wf_a" for w in waits)
    assert waits[1].summary == "needs review"


def test_waits_from_real_script_store_kanban_waits():
    with tempfile.TemporaryDirectory() as tmp:
        store = ScriptRunStore(tmp)
        store.record_kanban_card_state("cardA", {"status": "waiting", "profile": "impl"})
        store.record_kanban_card_state("cardB", {"status": "completed"})
        waits = waits_from_kanban_states(store.kanban_waits())
        assert [w.wait_id for w in waits] == ["cardA"]


# --------------------------------------------------------------------------- #
# Run / overview projections
# --------------------------------------------------------------------------- #

def _hello_definition() -> dict[str, Any]:
    return {
        "version": "1",
        "name": "hello",
        "inputs": {"name": "string"},
        "policy": {"network": False, "filesystem": False, "max_parallel": 2},
        "steps": [
            {"kind": "agent", "id": "greet", "agent": "hermes.greeter",
             "input": {"subject": "$ref:inputs.name"}, "output_schema": {"greeting": "string"}},
        ],
    }


def test_summarize_run_merges_control_state():
    run_store = InMemoryRunStore()
    handle = workflow_run(_hello_definition(), inputs={"name": "x"}, registry=run_store)
    control_store = InMemoryControlStore()
    pause_run(control_store, handle.run_id)
    state = project_control_state(handle.run_id, control_store.list_for(handle.run_id))

    summary = summarize_run(run_store.get(handle.run_id), control_state=state, wait_count=2)
    assert summary.run_id == handle.run_id
    assert summary.status == "succeeded"
    assert summary.desired_state == "paused"
    assert summary.paused is True
    assert summary.wait_count == 2
    assert summary.progress["total"] == 1


def test_inspect_run_compact_shape_and_child_refs():
    control_store = InMemoryControlStore()
    retry(control_store, "wf_a", "call-3", replacement_ref="repl-3")
    state = project_control_state("wf_a", control_store.list_for("wf_a"))
    waits = [WaitSummary(run_id="wf_a", wait_id="card-1", kind="kanban", state="blocked")]

    report = inspect_run(
        "wf_a", lifecycle="running", control_state=state, current_phase="implement",
        waits=waits, result={"ok": True},
        last_events=[{"i": n} for n in range(20)], events_limit=5,
    )
    assert report["run_id"] == "wf_a"
    assert report["lifecycle"] == "running"
    assert report["current_phase"] == "implement"
    assert report["control_state"]["desired_state"] == "running"
    assert report["waits"][0]["wait_id"] == "card-1"
    # child refs collapse waits + retry replacement/target, de-duplicated.
    assert set(report["child_task_refs"]) == {"card-1", "repl-3", "call-3"}
    assert report["retries"][0]["replacement_ref"] == "repl-3"
    assert len(report["last_events"]) == 5  # capped
    assert report["result"] == {"ok": True}


def test_list_runs_overview_orders_counts_and_folds_waits():
    run_store = InMemoryRunStore()
    h1 = workflow_run(_hello_definition(), inputs={"name": "a"}, registry=run_store, run_id="wf_aaaaaaaa_1")
    h2 = workflow_run(_hello_definition(), inputs={"name": "b"}, registry=run_store, run_id="wf_bbbbbbbb_2")
    control_store = InMemoryControlStore()
    stop_run(control_store, h1.run_id)

    waits = [WaitSummary(run_id=h2.run_id, wait_id="card-9", kind="kanban", state="blocked")]
    overview = list_runs(run_store.list(), control_store, waits=waits, limit=10)

    ids = [r["run_id"] for r in overview["runs"]]
    assert set(ids) == {h1.run_id, h2.run_id}
    assert overview["counts"]["total"] == 2
    assert overview["counts"]["stopped"] == 1
    assert overview["counts"]["succeeded"] == 2
    assert overview["counts"]["waits"] == 1
    assert overview["blocked_waits"][0]["wait_id"] == "card-9"
    # h1 is stopped, so it is not "active"; h2 (succeeded, no control) is not active either.
    assert h1.run_id not in overview["active"]
    by_id = {r["run_id"]: r for r in overview["runs"]}
    assert by_id[h1.run_id]["stopped"] is True
    assert by_id[h2.run_id]["wait_count"] == 1


def test_list_runs_respects_limit():
    run_store = InMemoryRunStore()
    for i in range(5):
        workflow_run(_hello_definition(), inputs={"name": str(i)}, registry=run_store)
    overview = list_runs(run_store.list(), InMemoryControlStore(), limit=2)
    assert len(overview["runs"]) == 2
    assert overview["counts"]["total"] == 5  # counts cover the full set, not the page.


def test_run_links_includes_only_present_paths():
    links = run_links(run_id="wf_a", journal_path="/x/journal.jsonl", script_path=None, tasks=["t1", "t2"])
    assert links == {"run_id": "wf_a", "journal": "/x/journal.jsonl", "tasks": ["t1", "t2"]}


def test_current_phase_prefers_running_step():
    run_store = InMemoryRunStore()
    handle = workflow_run(_hello_definition(), inputs={"name": "x"}, registry=run_store)
    # The skeleton run completes, so no running step — falls back to last step (no phase metadata here).
    assert current_phase(run_store.get(handle.run_id).steps) is None


# --------------------------------------------------------------------------- #
# Plugin tool surface
# --------------------------------------------------------------------------- #

class _FakeContext:
    def __init__(self) -> None:
        self.tools: dict[str, dict[str, Any]] = {}

    def register_tool(self, **kwargs: Any) -> None:
        self.tools[kwargs["name"]] = kwargs


def _load_plugin_root() -> ModuleType:
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("dynamic_workflows_plugin_ctl", root / "__init__.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_plugin_workflow_control_end_to_end(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setenv("HERMES_WORKFLOWS_STATE_DIR", str(Path(tmp) / "runs"))
        plugin = _load_plugin_root()
        ctx = _FakeContext()
        plugin.register(ctx)
        assert set(ctx.tools) == {"workflow", "workflow_control"}
        wf = ctx.tools["workflow"]["handler"]
        ctl = ctx.tools["workflow_control"]["handler"]

        run = json.loads(wf({"definition": _hello_definition(), "inputs": {"name": "world"}}))
        run_id = run["data"]["handle"]["run_id"]

        # overview lists the run.
        overview = json.loads(ctl({"action": "overview"}))
        assert overview["success"] is True
        assert run_id in {r["run_id"] for r in overview["data"]["runs"]}

        # pause -> status reflects paused desired state.
        paused = json.loads(ctl({"action": "pause", "run_id": run_id, "reason": "hold"}))
        assert paused["success"] is True
        assert paused["data"]["control_state"]["desired_state"] == "paused"

        status = json.loads(ctl({"action": "status", "run_id": run_id}))
        assert status["data"]["lifecycle"] == "succeeded"
        assert status["data"]["control_state"]["paused"] is True
        assert status["data"]["links"]["journal"].endswith("journal.jsonl")

        # idempotent retry lineage.
        r1 = json.loads(ctl({"action": "retry", "run_id": run_id, "target_ref": "greet"}))
        r2 = json.loads(ctl({"action": "retry", "run_id": run_id, "target_ref": "greet"}))
        assert r1["data"]["control"]["control_id"] == r2["data"]["control"]["control_id"]

        # stop is terminal and survives a fresh plugin load (restart).
        json.loads(ctl({"action": "stop", "run_id": run_id, "reason": "done"}))
        plugin2 = _load_plugin_root()
        ctx2 = _FakeContext()
        plugin2.register(ctx2)
        status2 = json.loads(ctx2.tools["workflow_control"]["handler"]({"action": "status", "run_id": run_id}))
        assert status2["data"]["control_state"]["stopped"] is True
        assert status2["data"]["control_state"]["desired_state"] == "stopped"


def test_plugin_workflow_control_requires_run_id(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setenv("HERMES_WORKFLOWS_STATE_DIR", str(Path(tmp) / "runs"))
        plugin = _load_plugin_root()
        ctx = _FakeContext()
        plugin.register(ctx)
        out = json.loads(ctx.tools["workflow_control"]["handler"]({"action": "pause"}))
        assert out["success"] is False
        assert out["error"]["type"] == "ControlError"
