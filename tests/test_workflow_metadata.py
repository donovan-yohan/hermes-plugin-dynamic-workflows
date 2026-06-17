"""Tests for native workflow metadata fields and session scoping."""

from __future__ import annotations

import pytest

from hermes_workflows.primitives import workflow, workflow_status
from hermes_workflows.registry import FileRunStore, InMemoryRunStore


def test_step_status_includes_workflow_metadata():
    definition = {
        "version": "1",
        "name": "meta_test",
        "inputs": {"name": "string"},
        "policy": {"network": False, "filesystem": False, "max_parallel": 2},
        "steps": [
            {
                "kind": "phase",
                "id": "phase_1",
                "label": "Greeting Phase",
                "steps": [
                    {
                        "kind": "agent",
                        "id": "greet",
                        "agent": "hermes.greeter",
                        "input": {"subject": "$ref:inputs.name"},
                        "output_schema": {"greeting": "string"},
                        "title": "say hello",
                    },
                    {
                        "kind": "agent",
                        "id": "shout",
                        "agent": "hermes.uppercaser",
                        "input": {"text": "$ref:greet.output.greeting"},
                        "output_schema": {"result": "string"},
                        "depends_on": ["greet"],
                    },
                ],
            }
        ],
    }

    result = workflow(definition=definition, inputs={"name": "world"})
    status = result["status"]
    rid = result["handle"]["run_id"]

    assert status["progress"]["total"] == 3
    assert status["progress"]["completed"] == 3
    assert status["progress"]["pct"] == 100.0

    by_id = {s["step_id"]: s for s in status["steps"]}

    phase = by_id["phase_1"]
    assert phase["workflow_id"] == rid
    assert phase["workflow_node_id"] == "phase_1"
    assert phase["workflow_phase_id"] == "phase_1"
    assert phase["workflow_phase_title"] == "Greeting Phase"
    assert phase["workflow_task_title"] == "phase_1"

    greet = by_id["greet"]
    assert greet["workflow_id"] == rid
    assert greet["workflow_node_id"] == "greet"
    assert greet["workflow_phase_id"] == "phase_1"
    assert greet["workflow_phase_title"] == "Greeting Phase"
    assert greet["workflow_task_title"] == "say hello"

    shout = by_id["shout"]
    assert shout["workflow_id"] == rid
    assert shout["workflow_phase_id"] == "phase_1"
    assert shout["workflow_task_title"] == "shout"


def test_container_steps_carry_workflow_identity():
    definition = {
        "version": "1",
        "name": "containers",
        "inputs": {},
        "policy": {"network": False, "filesystem": False, "max_parallel": 2},
        "steps": [
            {
                "kind": "parallel",
                "id": "p",
                "branches": [
                    {"kind": "agent", "id": "a1", "agent": "hermes.noop", "input": {}, "output_schema": {}},
                    {"kind": "agent", "id": "a2", "agent": "hermes.noop", "input": {}, "output_schema": {}},
                ],
            },
            {
                "kind": "pipeline",
                "id": "pl",
                "steps": [
                    {"kind": "agent", "id": "b1", "agent": "hermes.noop", "input": {}, "output_schema": {}},
                ],
            },
            {
                "kind": "if",
                "id": "cond",
                "condition": {"ref": "$ref:b1.output", "op": "exists"},
                "then": [
                    {"kind": "agent", "id": "c1", "agent": "hermes.noop", "input": {}, "output_schema": {}},
                ],
                "else": [],
            },
        ],
    }

    result = workflow(definition=definition)
    rid = result["handle"]["run_id"]
    by_id = {s["step_id"]: s for s in result["status"]["steps"]}

    for step_id in ("p", "pl", "cond"):
        s = by_id[step_id]
        assert s["workflow_id"] == rid, step_id
        assert s["workflow_node_id"] == step_id, step_id
        assert s["workflow_task_title"] == step_id, step_id

    for step_id in ("a1", "a2", "b1", "c1"):
        s = by_id[step_id]
        assert s["workflow_id"] == rid, step_id
        assert s["workflow_node_id"] == step_id, step_id


def test_session_scoping_isolates_runs():
    definition = {
        "version": "1",
        "name": "scoped",
        "inputs": {},
        "policy": {"network": False, "filesystem": False, "max_parallel": 2},
        "steps": [
            {"kind": "agent", "id": "noop", "agent": "hermes.noop", "input": {}, "output_schema": {}}
        ],
    }

    s1 = workflow(definition=definition, session_id="sess-a")
    rid = s1["handle"]["run_id"]

    assert workflow_status(rid, session_id="sess-b").status == "unknown"
    assert workflow_status(rid, session_id="sess-a").status == "succeeded"


def test_in_memory_store_session_scoping():
    store_a = InMemoryRunStore(session_id="a")
    store_b = InMemoryRunStore(session_id="b")

    rec = store_a.create("wf_test_123", "hash")
    store_a.set_status("wf_test_123", "succeeded")

    assert store_a.get("wf_test_123") is not None
    assert store_b.get("wf_test_123") is None


def test_file_store_session_scoping(tmp_path):
    base = tmp_path / "runs"
    store_a = FileRunStore(base, session_id="a")
    store_b = FileRunStore(base, session_id="b")

    rec = store_a.create("wf_test_456", "hash")
    store_a.set_status("wf_test_456", "succeeded")

    assert store_a.get("wf_test_456") is not None
    assert store_b.get("wf_test_456") is None
    assert (base / "a" / "wf_test_456" / "snapshot.json").exists()
    assert not (base / "b" / "wf_test_456" / "snapshot.json").exists()


def test_progress_counters_include_queued_and_cancelled():
    store = InMemoryRunStore()
    rec = store.create("wf_prog_1", "hash")
    from hermes_workflows.models import StepStatus

    store.update_step("wf_prog_1", StepStatus(step_id="a", kind="agent", status="succeeded"))
    store.update_step("wf_prog_1", StepStatus(step_id="b", kind="agent", status="failed"))
    store.update_step("wf_prog_1", StepStatus(step_id="c", kind="agent", status="running"))
    store.update_step("wf_prog_1", StepStatus(step_id="d", kind="agent", status="queued"))
    store.update_step("wf_prog_1", StepStatus(step_id="e", kind="agent", status="cancelled"))

    progress = rec.to_status().progress
    assert progress.total == 5
    assert progress.completed == 1
    assert progress.failed == 1
    assert progress.running == 1
    assert progress.queued == 1
    assert progress.cancelled == 1
    assert progress.pct == 60.0
