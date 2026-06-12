"""Tests for journaled run persistence and Kanban awaitable steps."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from hermes_workflows.primitives import workflow, workflow_run, workflow_status, workflow_validate
from hermes_workflows.registry import FileRunStore


def kanban_definition() -> dict:
    return {
        "version": "1",
        "name": "kanban_issue_flow",
        "inputs": {"issue": "string"},
        "policy": {"network": False, "filesystem": False, "max_parallel": 2},
        "steps": [
            {
                "kind": "kanban_agent",
                "id": "plan",
                "profile": "relayplanner",
                "task": {"issue": "$ref:inputs.issue", "goal": "triage and plan"},
                "input": {"repo": "donovan-yohan/relay-ide"},
                "output_schema": {"task_id": "string", "status": "string", "result": "object"},
            }
        ],
    }


def test_file_run_store_persists_snapshot_and_journal():
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        store = FileRunStore(tmp_path / "runs")
        handle = workflow_run(kanban_definition(), inputs={"issue": "#123"}, registry=store)

        snapshot = tmp_path / "runs" / handle.run_id / "snapshot.json"
        journal = tmp_path / "runs" / handle.run_id / "journal.jsonl"
        assert snapshot.exists()
        assert journal.exists()
        assert "raw" not in journal.read_text(encoding="utf-8").lower()

        reopened = FileRunStore(tmp_path / "runs")
        status = workflow_status(handle.run_id, registry=reopened)
        assert status.status == "succeeded"
        assert status.steps[0].kind == "kanban_agent"
        assert status.steps[0].agent == "kanban.relayplanner"
        output = status.steps[0].output
        assert output is not None
        assert output["task_id"].startswith("kb_")
        assert reopened.journal(handle.run_id)


def test_unified_workflow_facade_dry_run_run_and_status():
    with TemporaryDirectory() as tmp:
        store = FileRunStore(Path(tmp) / "runs")
        dry = workflow(definition=kanban_definition(), dry_run=True, registry=store)
        assert dry["operation"] == "validate"
        assert dry["validation"]["ok"] is True
        assert list(store.list()) == []

        ran = workflow(definition=kanban_definition(), inputs={"issue": "#456"}, registry=store)
        run_id = ran["handle"]["run_id"]
        assert ran["operation"] == "run"
        assert ran["status"]["status"] == "succeeded"

        queried = workflow(run_id=run_id, registry=store)
        assert queried["operation"] == "status"
        assert queried["status"]["run_id"] == run_id


def test_kanban_agent_step_validates_missing_profile():
    definition = kanban_definition()
    del definition["steps"][0]["profile"]

    result = workflow_validate(definition)

    assert result.ok is False
    assert any("profile" in diagnostic.message for diagnostic in result.errors)


def test_kanban_task_refs_are_linted():
    definition = kanban_definition()
    definition["steps"][0]["task"] = {"issue": "$ref:inputs.missing"}

    result = workflow_validate(definition)

    assert result.ok is False
    assert any(d.pointer == "/steps/0/task/issue" for d in result.errors)


def test_plain_agent_cannot_call_reserved_kanban_runner():
    definition = kanban_definition()
    definition["steps"][0] = {
        "kind": "agent",
        "id": "bypass",
        "agent": "kanban.relayplanner",
        "input": {},
        "output_schema": {"echo": "object"},
    }

    result = workflow_validate(definition)

    assert result.ok is False
    assert any("reserved Kanban runner id" in diagnostic.message for diagnostic in result.errors)


def test_file_run_store_rejects_unsafe_run_ids():
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        store = FileRunStore(tmp_path / "runs")

        try:
            workflow_run(kanban_definition(), inputs={"issue": "#789"}, registry=store, run_id="../outside")
        except ValueError as exc:
            assert "unsafe run_id" in str(exc)
        else:  # pragma: no cover - assertion clarity for unittest bridge
            raise AssertionError("expected unsafe run_id to raise ValueError")

        assert not (tmp_path / "outside").exists()
