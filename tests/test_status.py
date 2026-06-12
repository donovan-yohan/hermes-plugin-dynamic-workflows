"""Tests for ``workflow_status``.

Verify lookup by run_id of a recorded run, the documented RunStatus shape,
the ``include_steps=False`` cheap-polling path, and the unknown-id behaviour
(status='unknown', empty steps, never raises).

Stdlib only.
"""

from hermes_workflows.primitives import workflow_run, workflow_status
from hermes_workflows.registry import InMemoryRunStore


TERMINAL = {"succeeded", "failed", "cancelled"}
ALL_STATUSES = {"queued", "running", "succeeded", "failed", "cancelled", "unknown"}


def hello_definition() -> dict:
    return {
        "version": "1",
        "name": "hello",
        "inputs": {"name": "string"},
        "policy": {"network": False, "filesystem": False, "max_parallel": 2},
        "steps": [
            {
                "kind": "agent",
                "id": "greet",
                "agent": "hermes.greeter",
                "input": {"subject": "$ref:inputs.name"},
                "output_schema": {"greeting": "string"},
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


# --------------------------------------------------------------------------- #
# Lookup by run_id
# --------------------------------------------------------------------------- #

def test_status_lookup_by_run_id():
    store = InMemoryRunStore()
    handle = workflow_run(hello_definition(), inputs={"name": "world"}, registry=store)

    status = workflow_status(handle.run_id, registry=store)
    assert status.run_id == handle.run_id
    assert status.status in ALL_STATUSES
    assert status.status != "unknown"
    assert isinstance(status.created_at, str) and status.created_at
    assert isinstance(status.updated_at, str) and status.updated_at


def test_status_shape_full():
    store = InMemoryRunStore()
    handle = workflow_run(hello_definition(), inputs={"name": "world"}, registry=store)
    status = workflow_status(handle.run_id, registry=store, include_steps=True)

    # Progress sub-record.
    p = status.progress
    assert isinstance(p.total, int)
    assert isinstance(p.completed, int)
    assert isinstance(p.failed, int)
    assert isinstance(p.running, int)
    assert isinstance(p.pct, float)

    # Steps list of StepStatus.
    assert isinstance(status.steps, list)
    assert len(status.steps) >= 2
    for s in status.steps:
        assert isinstance(s.step_id, str) and s.step_id
        assert s.kind in ("agent", "kanban_agent", "parallel", "pipeline", "phase")
        assert isinstance(s.status, str)
        # agent may be None for non-agent steps; for these it is a str.
        assert s.agent is None or isinstance(s.agent, str)

    # result / error are dict|None.
    assert status.result is None or isinstance(status.result, dict)
    assert status.error is None or isinstance(status.error, dict)
    if status.status == "succeeded":
        assert status.error is None


def test_include_steps_false_omits_step_list():
    store = InMemoryRunStore()
    handle = workflow_run(hello_definition(), inputs={"name": "world"}, registry=store)

    cheap = workflow_status(handle.run_id, registry=store, include_steps=False)
    assert cheap.run_id == handle.run_id
    # Per contract the per-step list is omitted -> empty.
    assert cheap.steps == []
    # Progress is still populated for cheap polling.
    assert cheap.progress.total >= 2


# --------------------------------------------------------------------------- #
# Unknown id
# --------------------------------------------------------------------------- #

def test_unknown_id_returns_unknown_status_no_raise():
    store = InMemoryRunStore()
    status = workflow_status("wf_deadbeef_000000000000", registry=store)
    assert status.status == "unknown"
    assert status.steps == []
    assert status.run_id == "wf_deadbeef_000000000000"


def test_unknown_id_progress_is_empty():
    store = InMemoryRunStore()
    status = workflow_status("nope", registry=store)
    assert status.status == "unknown"
    assert status.progress.total == 0
    assert status.progress.completed == 0
    assert status.progress.failed == 0
    assert status.progress.running == 0
    assert status.result is None


def test_unknown_id_with_include_steps_false_still_unknown():
    store = InMemoryRunStore()
    status = workflow_status("nope", registry=store, include_steps=False)
    assert status.status == "unknown"
    assert status.steps == []


def test_status_does_not_mutate_run():
    """Querying status repeatedly is read-only and stable."""
    store = InMemoryRunStore()
    handle = workflow_run(hello_definition(), inputs={"name": "world"}, registry=store)

    a = workflow_status(handle.run_id, registry=store)
    b = workflow_status(handle.run_id, registry=store)
    assert a.run_id == b.run_id
    assert a.status == b.status
    assert a.created_at == b.created_at
    assert a.progress.total == b.progress.total
