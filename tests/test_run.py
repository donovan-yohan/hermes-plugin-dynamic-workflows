"""Tests for ``workflow_run``.

Verify that a run executes deterministically with the default StubAgentRunner,
that a fresh registry records the steps and a terminal status, that the run_id
scheme / override behave as specified, and that validation failures raise
``WorkflowValidationError`` before any run record is created.

Stdlib only.
"""

from hermes_workflows.primitives import workflow_run, workflow_status
from hermes_workflows.registry import InMemoryRunStore
from hermes_workflows.errors import WorkflowValidationError


TERMINAL = {"succeeded", "failed", "cancelled"}


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


def invalid_definition() -> dict:
    """Requests network capability -> validation error."""
    d = hello_definition()
    d["policy"]["network"] = True
    return d


# --------------------------------------------------------------------------- #
# RunHandle shape
# --------------------------------------------------------------------------- #

def test_run_returns_handle_with_documented_fields():
    store = InMemoryRunStore()
    handle = workflow_run(hello_definition(), inputs={"name": "world"}, registry=store)

    assert isinstance(handle.run_id, str) and handle.run_id
    assert handle.status in ({"queued", "running"} | TERMINAL)
    assert isinstance(handle.created_at, str) and handle.created_at
    assert isinstance(handle.def_hash, str) and handle.def_hash


def test_run_id_scheme_default():
    """run_id = 'wf_' + <def_hash8> + '_' + <uuid12>."""
    store = InMemoryRunStore()
    handle = workflow_run(hello_definition(), inputs={"name": "world"}, registry=store)

    assert handle.run_id.startswith("wf_")
    parts = handle.run_id.split("_")
    # 'wf', <hash8>, <uuid12>
    assert len(parts) == 3
    _, hash8, uuid12 = parts
    assert len(hash8) == 8
    assert len(uuid12) == 12
    # The embedded hash8 is the prefix of the full def_hash on the handle.
    assert handle.def_hash.startswith(hash8)


def test_run_id_override_is_honored():
    store = InMemoryRunStore()
    handle = workflow_run(
        hello_definition(),
        inputs={"name": "world"},
        registry=store,
        run_id="wf_custom_id_123",
    )
    assert handle.run_id == "wf_custom_id_123"
    # And it is queryable under that id.
    status = workflow_status("wf_custom_id_123", registry=store)
    assert status.status != "unknown"


# --------------------------------------------------------------------------- #
# Deterministic execution + recorded steps
# --------------------------------------------------------------------------- #

def test_run_is_deterministic_terminal_status():
    store = InMemoryRunStore()
    handle = workflow_run(hello_definition(), inputs={"name": "world"}, registry=store)

    status = workflow_status(handle.run_id, registry=store)
    # The skeleton runs synchronously, so by the time we query it is terminal.
    assert status.status in TERMINAL
    # With the default no-op StubAgentRunner the hello workflow succeeds.
    assert status.status == "succeeded"


def test_run_records_both_steps():
    store = InMemoryRunStore()
    handle = workflow_run(hello_definition(), inputs={"name": "world"}, registry=store)

    status = workflow_status(handle.run_id, registry=store, include_steps=True)
    recorded = {s.step_id for s in status.steps}
    assert {"greet", "shout"} <= recorded

    for s in status.steps:
        assert s.kind in ("agent", "parallel", "pipeline", "phase")
        assert isinstance(s.status, str) and s.status
        # The two top-level steps are agent steps.
        if s.step_id in {"greet", "shout"}:
            assert s.kind == "agent"
            assert s.agent in ("hermes.greeter", "hermes.uppercaser")


def test_progress_accounting_consistent():
    store = InMemoryRunStore()
    handle = workflow_run(hello_definition(), inputs={"name": "world"}, registry=store)
    status = workflow_status(handle.run_id, registry=store)

    p = status.progress
    assert p.total >= 2
    assert p.completed + p.failed + p.running <= p.total
    assert 0.0 <= p.pct <= 100.0
    # A fully successful run is 100% complete.
    if status.status == "succeeded":
        assert p.failed == 0
        assert p.completed == p.total
        assert abs(p.pct - 100.0) < 1e-9


def test_deterministic_def_hash_across_runs():
    """Two runs of the same definition share the same def_hash (canonical)."""
    store = InMemoryRunStore()
    h1 = workflow_run(hello_definition(), inputs={"name": "a"}, registry=store)
    h2 = workflow_run(hello_definition(), inputs={"name": "b"}, registry=store)
    assert h1.def_hash == h2.def_hash
    # Different runs, different ids.
    assert h1.run_id != h2.run_id


# --------------------------------------------------------------------------- #
# Validation gating
# --------------------------------------------------------------------------- #

def test_validate_failure_raises_before_run_record():
    store = InMemoryRunStore()
    caught: WorkflowValidationError | None = None
    try:
        workflow_run(invalid_definition(), inputs={"name": "world"}, registry=store)
    except WorkflowValidationError as err:
        caught = err
    else:
        raise AssertionError("expected WorkflowValidationError")

    # The error carries the ValidationResult per the error model.
    assert caught is not None
    vr = getattr(caught, "result", None)
    if vr is not None:
        assert vr.ok is False
        assert any(d.code == "E_POLICY_NETWORK" for d in vr.errors)

    # No run record should have been created in the supplied registry.
    assert list(store.list()) == []


def test_validate_false_skips_gate():
    """With validate=False the run is not statically gated; a definition that
    only trips a *lint* policy may still execute (it creates a record).

    We use the network-policy def but disable validation, then assert a record
    exists. (Whether it ultimately succeeds or fails is left to the runtime;
    we only assert that the validation gate did not fire.)
    """
    store = InMemoryRunStore()
    handle = workflow_run(
        invalid_definition(),
        inputs={"name": "world"},
        registry=store,
        validate=False,
    )
    assert handle.run_id
    assert len(list(store.list())) == 1


def test_default_registry_is_process_global_when_omitted():
    """Omitting registry uses the process-global InMemoryRunStore; the run is
    still queryable by id via the same default."""
    handle = workflow_run(hello_definition(), inputs={"name": "world"})
    status = workflow_status(handle.run_id)
    assert status.run_id == handle.run_id
    assert status.status in TERMINAL
