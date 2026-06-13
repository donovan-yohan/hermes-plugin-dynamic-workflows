"""Tests for declarative ``if`` control flow."""

from hermes_workflows.errors import WorkflowValidationError
from hermes_workflows.primitives import workflow_run, workflow_status, workflow_validate
from hermes_workflows.registry import InMemoryRunStore

def if_definition(flag_ref="$ref:inputs.flag"):
    return {
        "version": "1",
        "name": "if_flow",
        "inputs": {"flag": "bool"},
        "policy": {"network": False, "filesystem": False, "max_parallel": 2},
        "steps": [
            {
                "kind": "if",
                "id": "gate",
                "condition": {"ref": flag_ref, "op": "truthy"},
                "then": [
                    {
                        "kind": "agent",
                        "id": "then_step",
                        "agent": "hermes.echo",
                        "input": {"value": "then"},
                        "output_schema": {"echo": "object", "digest": "string"},
                    }
                ],
                "else": [
                    {
                        "kind": "agent",
                        "id": "else_step",
                        "agent": "hermes.echo",
                        "input": {"value": "else"},
                        "output_schema": {"echo": "object", "digest": "string"},
                    }
                ],
            },
            {
                "kind": "agent",
                "id": "after",
                "agent": "hermes.echo",
                "input": {"chosen": "$ref:gate.output.branch", "payload": "$ref:gate.output.output.echo"},
                "output_schema": {"echo": "object", "digest": "string"},
                "depends_on": ["gate"],
            },
        ],
    }


def test_if_then_branch_executes_and_else_branch_not_called():
    store = InMemoryRunStore()
    handle = workflow_run(if_definition(), inputs={"flag": True}, registry=store)
    status = workflow_status(handle.run_id, registry=store)

    assert status.status == "succeeded"
    steps = {s.step_id: s for s in status.steps}
    assert steps["gate"].output["branch"] == "then"
    assert "then_step" in steps
    assert "else_step" not in steps


def test_if_else_branch_executes_when_condition_false():
    store = InMemoryRunStore()
    handle = workflow_run(if_definition(), inputs={"flag": False}, registry=store)
    status = workflow_status(handle.run_id, registry=store)

    steps = {s.step_id: s for s in status.steps}
    assert steps["gate"].output["branch"] == "else"
    assert "else_step" in steps
    assert "then_step" not in steps


def test_if_output_can_feed_downstream_step():
    store = InMemoryRunStore()
    handle = workflow_run(if_definition(), inputs={"flag": True}, registry=store)
    status = workflow_status(handle.run_id, registry=store)
    steps = {s.step_id: s for s in status.steps}

    assert steps["after"].output["echo"]["chosen"] == "then"
    assert steps["after"].output["echo"]["payload"]["value"] == "then"


def test_if_branch_child_output_not_referenceable_after_if():
    definition = if_definition()
    definition["steps"][1]["input"] = {"bad": "$ref:then_step.output.echo"}
    store = InMemoryRunStore()
    try:
        workflow_run(definition, inputs={"flag": True}, registry=store)
    except WorkflowValidationError as exc:
        assert any(d.code == "E_UNRESOLVED_REF" for d in exc.result.errors)
    else:
        raise AssertionError("expected branch-local ref to be rejected")


def test_if_condition_bad_ref_is_validation_error():
    store = InMemoryRunStore()
    try:
        workflow_run(if_definition("$ref:missing.output.value"), inputs={"flag": True}, registry=store)
    except WorkflowValidationError as exc:
        assert any(d.code == "E_UNRESOLVED_REF" for d in exc.result.errors)
    else:
        raise AssertionError("expected bad condition ref to be rejected")


def test_container_depends_on_missing_step_is_validation_error():
    for kind, child_key, child_value in (
        ("if", "then", [{"kind": "agent", "id": "x", "agent": "hermes.noop", "input": {}, "output_schema": {}}]),
        ("parallel", "branches", [{"kind": "agent", "id": "x", "agent": "hermes.noop", "input": {}, "output_schema": {}}]),
        ("phase", "steps", [{"kind": "agent", "id": "x", "agent": "hermes.noop", "input": {}, "output_schema": {}}]),
    ):
        step = {"kind": kind, "id": f"{kind}_container", "depends_on": ["missing"], child_key: child_value}
        if kind == "if":
            step["condition"] = {"ref": "$ref:inputs.flag", "op": "truthy"}
        definition = {
            "version": "1",
            "name": f"bad_{kind}_depends",
            "inputs": {"flag": "bool"},
            "policy": {"network": False, "filesystem": False},
            "steps": [step],
        }
        result = workflow_validate(definition)
        assert not result.ok
        assert any(d.code == "E_UNRESOLVED_REF" for d in result.errors)


def test_container_depends_on_bad_type_is_validation_error():
    for bad_depends_on in ([1], [None], "missing"):
        definition = {
            "version": "1",
            "name": "bad_container_depends_shape",
            "policy": {"network": False, "filesystem": False},
            "steps": [
                {
                    "kind": "phase",
                    "id": "phase_container",
                    "depends_on": bad_depends_on,
                    "steps": [{"kind": "agent", "id": "x", "agent": "hermes.noop", "input": {}, "output_schema": {}}],
                }
            ],
        }
        result = workflow_validate(definition)
        assert not result.ok
        assert any(d.pointer == "/steps/0/depends_on" for d in result.errors)


def test_policy_max_parallel_caps_default_runtime():
    definition = {
        "version": "1",
        "name": "parallel_bound",
        "policy": {"network": False, "filesystem": False, "max_parallel": 1},
        "steps": [
            {
                "kind": "parallel",
                "id": "p",
                "branches": [
                    {"kind": "agent", "id": "a", "agent": "hermes.noop", "input": {}, "output_schema": {}},
                    {"kind": "agent", "id": "b", "agent": "hermes.noop", "input": {}, "output_schema": {}},
                ],
            }
        ],
    }
    store = InMemoryRunStore()
    handle = workflow_run(definition, registry=store)
    status = workflow_status(handle.run_id, registry=store)

    assert status.status == "failed"
    assert status.error is not None
    assert "fan-out 2 exceeds max_parallel=1" in status.error["message"]


def test_invalid_policy_max_parallel_is_validation_error():
    for value in ("many", 0, -1, 1.5, True):
        definition = if_definition()
        definition["policy"]["max_parallel"] = value
        result = workflow_validate(definition)
        assert not result.ok
        assert any(d.pointer == "/policy/max_parallel" for d in result.errors)
