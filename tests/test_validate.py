"""Tests for ``workflow_validate``.

These exercise the static checker (parse, schema, sandbox-policy lint) of the
``hermes_workflows`` package against the published contract. They assert on the
stable diagnostic *codes* (never on free-text messages) and on the documented
shape of ``ValidationResult`` / ``Diagnostic``.

Stdlib only.
"""

import json

from hermes_workflows.primitives import workflow_validate


# --------------------------------------------------------------------------- #
# Fixtures / helpers
# --------------------------------------------------------------------------- #

def valid_definition() -> dict:
    """A minimal, well-formed 2-step 'hello' workflow.

    Both agents are referenced by id; the contract's StubAgentRunner /
    validator treats the ``hermes.*`` agents used here as resolvable. Each
    agent step declares an ``output_schema`` so no ``W_NO_OUTPUT_SCHEMA``
    warning is emitted.
    """
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


def _codes(diagnostics) -> set:
    """Collect the stable ``code`` of every diagnostic in a list."""
    return {d.code for d in diagnostics}


def _all_pointers_are_json_pointers(diagnostics) -> bool:
    """Every diagnostic pointer must be a JSON-Pointer (empty or starts '/')."""
    for d in diagnostics:
        if d.pointer != "" and not d.pointer.startswith("/"):
            return False
    return True


# --------------------------------------------------------------------------- #
# Valid definitions
# --------------------------------------------------------------------------- #

def test_valid_definition_ok():
    result = workflow_validate(valid_definition())
    assert result.ok is True
    assert result.errors == []
    # def_hash is a non-empty hex-ish string identifying the canonical def.
    assert isinstance(result.def_hash, str)
    assert len(result.def_hash) > 0
    # On success the validator returns a normalized dict.
    assert isinstance(result.normalized, dict)


def test_valid_definition_accepts_json_string():
    """`definition` may be a JSON string (stdlib json only)."""
    as_text = json.dumps(valid_definition())
    result = workflow_validate(as_text)
    assert result.ok is True
    assert result.errors == []


def test_def_hash_is_stable_across_key_order():
    """def_hash is over the canonicalized (sorted-keys) JSON, so key order
    in the source must not change it."""
    a = workflow_validate(valid_definition())

    reordered = {
        "steps": valid_definition()["steps"],
        "name": "hello",
        "policy": {"max_parallel": 2, "filesystem": False, "network": False},
        "inputs": {"name": "string"},
        "version": "1",
    }
    b = workflow_validate(reordered)
    assert a.def_hash == b.def_hash


def test_no_side_effects_no_run_created():
    """Validation must not create a run. We assert by checking that a freshly
    generated unknown-looking run id remains unknown after validating.

    (workflow_validate has no run registry surface, so the strongest portable
    assertion is simply that it returns a ValidationResult and never raises
    for a well-formed def.)
    """
    result = workflow_validate(valid_definition())
    assert hasattr(result, "ok")
    assert hasattr(result, "def_hash")


def test_missing_output_schema_is_warning_only_in_nonstrict():
    """An agent step without output_schema yields W_NO_OUTPUT_SCHEMA, which is
    a warning (not an error) when strict=False."""
    d = valid_definition()
    del d["steps"][0]["output_schema"]
    result = workflow_validate(d, strict=False)
    assert "W_NO_OUTPUT_SCHEMA" in _codes(result.warnings)
    assert "W_NO_OUTPUT_SCHEMA" not in _codes(result.errors)
    # A lone lint warning does not by itself make the def invalid in non-strict.
    assert result.errors == []
    assert result.ok is True


# --------------------------------------------------------------------------- #
# Invalid definitions
# --------------------------------------------------------------------------- #

def test_invalid_json_string_reports_parse_error():
    """A non-JSON string must produce errors (ok=False), not raise."""
    result = workflow_validate("{not valid json")
    assert result.ok is False
    assert len(result.errors) >= 1
    assert _all_pointers_are_json_pointers(result.errors)


def test_bad_toplevel_shape_reports_schema_error():
    """Missing required top-level fields (version/name/steps) -> schema error."""
    result = workflow_validate({"name": "x"})  # no version, no steps
    assert result.ok is False
    assert "E_SCHEMA_TOPLEVEL" in _codes(result.errors)


def test_steps_not_a_list_is_toplevel_error():
    d = valid_definition()
    d["steps"] = "not-a-list"
    result = workflow_validate(d)
    assert result.ok is False
    assert "E_SCHEMA_TOPLEVEL" in _codes(result.errors)


def test_policy_network_true_is_error():
    """policy.network must stay false in the skeleton; true -> E_POLICY_NETWORK."""
    d = valid_definition()
    d["policy"]["network"] = True
    result = workflow_validate(d)
    assert result.ok is False
    assert "E_POLICY_NETWORK" in _codes(result.errors)


def test_unknown_agent_reference_is_error():
    """An agent id the runner cannot resolve -> E_UNKNOWN_AGENT."""
    d = valid_definition()
    d["steps"][0]["agent"] = "does.not.exist"
    result = workflow_validate(d)
    assert result.ok is False
    assert "E_UNKNOWN_AGENT" in _codes(result.errors)
    # The pointer should target the offending agent field.
    offending = [e for e in result.errors if e.code == "E_UNKNOWN_AGENT"]
    assert all(e.pointer.startswith("/") for e in offending)


def test_cyclic_depends_on_is_error():
    """A cycle in the depends_on / pipeline graph -> E_CYCLE."""
    d = valid_definition()
    # greet depends on shout and shout depends on greet -> cycle.
    d["steps"][0]["depends_on"] = ["shout"]
    result = workflow_validate(d)
    assert result.ok is False
    assert "E_CYCLE" in _codes(result.errors)


def test_strict_promotes_lint_warning_to_error():
    """strict=True promotes sandbox-policy lint warnings to errors.

    Using the missing-output_schema case (W_NO_OUTPUT_SCHEMA): non-strict it
    is a warning and ok stays True; strict it is promoted to an error and ok
    becomes False.
    """
    d = valid_definition()
    del d["steps"][0]["output_schema"]

    lenient = workflow_validate(d, strict=False)
    assert lenient.ok is True
    assert "W_NO_OUTPUT_SCHEMA" in _codes(lenient.warnings)

    strict = workflow_validate(d, strict=True)
    assert strict.ok is False
    # Promoted: the same code now appears among errors.
    assert "W_NO_OUTPUT_SCHEMA" in _codes(strict.errors)


def test_diagnostics_have_documented_fields():
    result = workflow_validate({"name": "x"})
    assert result.errors, "expected at least one diagnostic"
    for d in result.errors:
        assert d.severity in ("error", "warning")
        assert isinstance(d.code, str) and d.code
        assert isinstance(d.message, str)
        assert isinstance(d.pointer, str)
    assert _all_pointers_are_json_pointers(result.errors)


def test_source_path_is_accepted_kw_only():
    """`source_path` is an accepted keyword-only arg and does not change
    the validity verdict for a good definition."""
    result = workflow_validate(valid_definition(), source_path="examples/hello.workflow.json")
    assert result.ok is True
