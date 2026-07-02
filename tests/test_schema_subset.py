"""Tests for the shared JSON-Schema-subset output validator (issue #107).

Covers the standalone module (:mod:`hermes_workflows.schema_subset`), the
static, before-any-run-starts rejection paths (``workflow_validate`` for the
JSON engine, ``validate_script`` for literal script schemas), and a shared
test matrix proving the two engines' ``_validate_output`` wiring produces
identical verdicts for identical ``(schema, payload)`` pairs.

Stdlib only.
"""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pytest

from hermes_workflows import ChildAgentRequest, VMLimits, run_workflow_script, workflow_validate
from hermes_workflows import runtime as _runtime
from hermes_workflows import vm as _vm
from hermes_workflows import schema_subset
from hermes_workflows.errors import CapabilityDenied, SandboxPolicyError
from hermes_workflows.script_validator import validate_script

META = 'meta = {"name": "schema-subset", "description": "d"}\n'


# --------------------------------------------------------------------------- #
# schema_subset: direct unit tests.
# --------------------------------------------------------------------------- #


def test_no_schema_is_a_noop():
    assert schema_subset.normalize_schema(None) is None
    assert schema_subset.normalize_schema({}) is None
    schema_subset.validate({"anything": object()}, None)  # must not raise.
    schema_subset.validate({}, {})  # must not raise.


def test_legacy_flat_schema_normalizes_to_object_with_every_field_required():
    node = schema_subset.normalize_schema({"plan": "string", "steps": "list"})
    assert node == {
        "type": "object",
        "properties": {"plan": {"type": "string"}, "steps": {"type": "array"}},
        "required": ["plan", "steps"],
    }


def test_legacy_flat_schema_accepts_matching_payload():
    schema_subset.validate({"plan": "x", "steps": []}, {"plan": "string", "steps": "list"})


def test_legacy_flat_schema_rejects_missing_field():
    with pytest.raises(schema_subset.SchemaError, match="plan"):
        schema_subset.validate({"steps": []}, {"plan": "string", "steps": "list"})


def test_legacy_flat_schema_rejects_type_mismatch():
    with pytest.raises(schema_subset.SchemaError, match="string"):
        schema_subset.validate({"plan": 7}, {"plan": "string"})


def test_legacy_flat_schema_extra_fields_are_allowed():
    schema_subset.validate({"plan": "x", "unexpected": True}, {"plan": "string"})


def test_legacy_unknown_hint_leniently_skips_type_check_but_requires_presence():
    # An unrecognized hint value never rejected on type -- matches the
    # historical "unknown hint: leniently accept" flat-checker behaviour.
    schema_subset.validate({"weird": object()}, {"weird": "not-a-real-type"})
    with pytest.raises(schema_subset.SchemaError):
        schema_subset.validate({}, {"weird": "not-a-real-type"})


def test_legacy_python_type_object_hint_is_supported():
    schema_subset.validate({"count": 3}, {"count": int})
    with pytest.raises(schema_subset.SchemaError):
        schema_subset.validate({"count": "3"}, {"count": int})


def test_legacy_bool_excluded_from_numeric_and_string_hints():
    with pytest.raises(schema_subset.SchemaError, match="bool"):
        schema_subset.validate({"n": True}, {"n": "number"})
    with pytest.raises(schema_subset.SchemaError, match="bool"):
        schema_subset.validate({"s": True}, {"s": "string"})
    schema_subset.validate({"b": True}, {"b": "boolean"})  # bool hint still accepts bool.


def test_nested_object_schema_validates():
    schema = {
        "type": "object",
        "properties": {
            "plan": {"type": "string"},
            "detail": {
                "type": "object",
                "properties": {"owner": {"type": "string"}},
                "required": ["owner"],
            },
        },
        "required": ["plan", "detail"],
    }
    schema_subset.validate({"plan": "ship it", "detail": {"owner": "alice"}}, schema)
    with pytest.raises(schema_subset.SchemaError, match="owner"):
        schema_subset.validate({"plan": "ship it", "detail": {}}, schema)


def test_array_of_objects_schema_validates_each_item():
    schema = {
        "type": "object",
        "properties": {
            "steps": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"title": {"type": "string"}},
                    "required": ["title"],
                },
            }
        },
        "required": ["steps"],
    }
    schema_subset.validate({"steps": [{"title": "one"}, {"title": "two"}]}, schema)
    with pytest.raises(schema_subset.SchemaError):
        schema_subset.validate({"steps": [{"title": "one"}, {"nope": True}]}, schema)


def test_enum_schema_validates():
    schema = {
        "type": "object",
        "properties": {"risk": {"type": "string", "enum": ["low", "medium", "high"]}},
        "required": ["risk"],
    }
    schema_subset.validate({"risk": "medium"}, schema)
    with pytest.raises(schema_subset.SchemaError):
        schema_subset.validate({"risk": "extreme"}, schema)


def test_additional_properties_false_rejects_extra_fields():
    schema = {"type": "object", "properties": {"plan": {"type": "string"}}, "additionalProperties": False}
    schema_subset.validate({"plan": "x"}, schema)
    with pytest.raises(schema_subset.SchemaError, match="additionalProperties"):
        schema_subset.validate({"plan": "x", "surprise": 1}, schema)


def test_additional_properties_defaults_permissive():
    schema = {"type": "object", "properties": {"plan": {"type": "string"}}}
    schema_subset.validate({"plan": "x", "surprise": 1}, schema)


@pytest.mark.parametrize(
    "bad_schema",
    [
        {"type": "object", "bogus": 1},
        {"type": "banana"},
        {"type": "object", "properties": {"x": {"type": "object", "unknown_kw": True}}},
        {"type": "array", "items": {"type": "not-a-type"}},
        {"type": "object", "required": "plan"},
        {"type": "object", "additionalProperties": "yes"},
        {"type": "object", "enum": "not-a-list"},
    ],
)
def test_unknown_or_malformed_schema_keyword_is_fail_closed(bad_schema):
    with pytest.raises(schema_subset.SchemaError):
        schema_subset.normalize_schema(bad_schema)
    assert schema_subset.check_schema(bad_schema) is not None


def test_check_schema_never_raises_and_returns_none_for_well_formed_schema():
    assert schema_subset.check_schema({"plan": "string"}) is None
    assert schema_subset.check_schema({"type": "object", "properties": {}}) is None
    assert schema_subset.check_schema({"type": "object", "bogus": 1}) is not None
    assert schema_subset.check_schema(None) is None
    assert schema_subset.check_schema("not-a-schema") is None


# --------------------------------------------------------------------------- #
# Shared test matrix: both engines' ``_validate_output`` must agree.
# --------------------------------------------------------------------------- #

_MATRIX: list[tuple[dict[str, Any], dict[str, Any], bool]] = [
    ({"plan": "string"}, {"plan": "ship it"}, True),
    ({"plan": "string"}, {"plan": 7}, False),
    ({"plan": "string"}, {}, False),
    ({"plan": "string", "steps": "list"}, {"plan": "x", "steps": [1, 2]}, True),
    (
        {
            "type": "object",
            "properties": {
                "plan": {"type": "string"},
                "risk": {"type": "string", "enum": ["low", "high"]},
            },
            "required": ["plan", "risk"],
        },
        {"plan": "x", "risk": "low"},
        True,
    ),
    (
        {
            "type": "object",
            "properties": {
                "plan": {"type": "string"},
                "risk": {"type": "string", "enum": ["low", "high"]},
            },
            "required": ["plan", "risk"],
        },
        {"plan": "x", "risk": "medium"},
        False,
    ),
    (
        {
            "type": "object",
            "properties": {"steps": {"type": "array", "items": {"type": "string"}}},
            "required": ["steps"],
        },
        {"steps": ["a", "b"]},
        True,
    ),
    (
        {
            "type": "object",
            "properties": {"steps": {"type": "array", "items": {"type": "string"}}},
            "required": ["steps"],
        },
        {"steps": ["a", 2]},
        False,
    ),
    ({}, {"anything": object()}, True),
    (None, {"anything": object()}, True),
]


@pytest.mark.parametrize("schema,payload,expect_ok", _MATRIX)
def test_both_engines_agree_on_the_same_schema_payload_pair(schema, payload, expect_ok):
    vm_ok = True
    try:
        _vm._validate_output(payload, schema)
    except CapabilityDenied:
        vm_ok = False

    runtime_ok = True
    try:
        _runtime._validate_output(payload, schema)
    except SandboxPolicyError:
        runtime_ok = False

    assert vm_ok == expect_ok
    assert runtime_ok == expect_ok
    assert vm_ok == runtime_ok


def test_vm_validate_output_wraps_schema_error_as_capability_denied_with_schema_code():
    with pytest.raises(CapabilityDenied) as excinfo:
        _vm._validate_output({}, {"type": "object", "bogus": 1})
    assert excinfo.value.code == "schema"


def test_runtime_validate_output_wraps_schema_error_as_sandbox_policy_error():
    with pytest.raises(SandboxPolicyError):
        _runtime._validate_output({}, {"type": "object", "bogus": 1})


# --------------------------------------------------------------------------- #
# JSON engine: malformed output_schema is rejected at workflow_validate time.
# --------------------------------------------------------------------------- #


def _definition_with_output_schema(output_schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": "1",
        "name": "schema-def",
        "steps": [
            {
                "kind": "agent",
                "id": "step1",
                "agent": "hermes.greeter",
                "input": {"subject": "world"},
                "output_schema": output_schema,
            }
        ],
    }


def test_workflow_validate_accepts_a_well_formed_nested_subset_output_schema():
    definition = _definition_with_output_schema(
        {
            "type": "object",
            "properties": {"greeting": {"type": "string"}, "risk": {"type": "string", "enum": ["low", "high"]}},
            "required": ["greeting"],
        }
    )
    result = workflow_validate(definition, strict=False)
    assert result.ok, result.errors


def test_workflow_validate_accepts_a_legacy_flat_output_schema():
    definition = _definition_with_output_schema({"greeting": "string"})
    result = workflow_validate(definition, strict=False)
    assert result.ok, result.errors


def test_workflow_validate_rejects_unknown_output_schema_keyword_before_any_run():
    definition = _definition_with_output_schema({"type": "object", "bogus_keyword": True})
    result = workflow_validate(definition, strict=False)
    assert not result.ok
    assert any(d.code == "E_SCHEMA_OUTPUT_SCHEMA" for d in result.errors)


def test_workflow_validate_rejects_bad_output_schema_type_value():
    definition = _definition_with_output_schema({"type": "banana"})
    result = workflow_validate(definition, strict=False)
    assert not result.ok
    assert any(d.code == "E_SCHEMA_OUTPUT_SCHEMA" for d in result.errors)


# --------------------------------------------------------------------------- #
# Script VM: a literal ``schema=`` argument is checked before launch.
# --------------------------------------------------------------------------- #


def test_validate_script_accepts_a_well_formed_literal_schema_argument():
    script = META + (
        'r = await kanban_agent("planner", prompt="plan", '
        'schema={"type": "object", "properties": {"plan": {"type": "string"}}, "required": ["plan"]})\n'
        'return r\n'
    )
    result = validate_script(script)
    assert result.ok, result.diagnostics


def test_validate_script_rejects_unknown_keyword_in_literal_schema_argument():
    script = META + (
        'r = await kanban_agent("planner", prompt="plan", schema={"type": "object", "bogus": 1})\n'
        'return r\n'
    )
    result = validate_script(script)
    assert not result.ok
    assert any(d.code == "E_SCRIPT_BAD_SCHEMA" for d in result.diagnostics)


def test_validate_script_leaves_a_non_literal_schema_to_runtime_enforcement():
    # A schema built from a variable is not statically knowable; the script is
    # still valid at launch time -- the parent broker enforces it at call time.
    script = META + (
        'built_schema = {"type": "object", "bogus": 1}\n'
        'r = await kanban_agent("planner", prompt="plan", schema=built_schema)\n'
        'return r\n'
    )
    result = validate_script(script)
    assert result.ok, result.diagnostics


# --------------------------------------------------------------------------- #
# End-to-end (script VM): nested object/enum schema validates and retries.
# --------------------------------------------------------------------------- #


class _SequenceChildRunner:
    def __init__(self, outputs: list[Any]) -> None:
        self.outputs = list(outputs)
        self.requests: list[ChildAgentRequest] = []

    def __call__(self, request: ChildAgentRequest) -> Any:
        self.requests.append(request)
        return self.outputs.pop(0) if self.outputs else {}


def test_nested_object_enum_schema_retries_on_mismatch_then_succeeds_in_script_vm():
    runner = _SequenceChildRunner(
        [
            {"plan": "ship it", "risk": "extreme"},  # fails enum -> retry.
            {"plan": "ship it", "risk": "low"},  # valid.
        ]
    )
    schema = {
        "type": "object",
        "properties": {
            "plan": {"type": "string"},
            "risk": {"type": "string", "enum": ["low", "medium", "high"]},
        },
        "required": ["plan", "risk"],
    }
    script = META + (
        'result = await agent("summarize", {"schema": ' + json.dumps(schema) + '})\n'
        'return result\n'
    )
    with TemporaryDirectory() as tmp:
        from hermes_workflows import ScriptRunStore

        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            script,
            store=store,
            run_id="nested_schema_retry_run",
            child_agent_runner=runner,
            deterministic_runner=True,
        )
        assert res.ok, res.error
        assert res.value == {"plan": "ship it", "risk": "low"}
        assert len(runner.requests) == 2


def test_nested_object_schema_fails_closed_after_retry_exhaustion_in_script_vm():
    runner = _SequenceChildRunner([{"plan": "x", "risk": "extreme"}, {"plan": "x", "risk": "extreme"}])
    schema = {
        "type": "object",
        "properties": {"plan": {"type": "string"}, "risk": {"type": "string", "enum": ["low", "high"]}},
        "required": ["plan", "risk"],
    }
    script = META + (
        'return await agent("summarize", {"schema": ' + json.dumps(schema) + '})\n'
    )
    res = run_workflow_script(
        script,
        child_agent_runner=runner,
        limits=VMLimits(max_schema_retries=1),
    )
    assert res.ok is False
    assert res.error["code"] == "schema"
