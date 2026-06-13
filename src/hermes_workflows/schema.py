"""Definition parsing, canonicalization, hashing, and structural validation.

This module owns everything about the *shape* of a workflow definition (the
``workflow_def_format`` in the contract) that does not require sandbox-policy
judgement. Policy lint lives in :mod:`hermes_workflows.sandbox`; the two are
composed by :func:`hermes_workflows.primitives.workflow_validate`.

No execution, no network, no filesystem access happens here.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .models import Diagnostic
from . import errors as err

__all__ = [
    "SUPPORTED_VERSION",
    "STEP_KINDS",
    "parse_definition",
    "canonicalize",
    "def_hash",
    "validate_structure",
    "iter_agent_steps",
]

SUPPORTED_VERSION = "1"
STEP_KINDS = ("agent", "kanban_agent", "parallel", "pipeline", "phase", "if")
CONDITION_OPS = ("truthy", "exists", "eq", "ne")

# Keys that hold nested step lists, by kind, used for recursive descent.
_CHILD_KEYS = {"parallel": "branches", "pipeline": "steps", "phase": "steps"}
_EFFECT_STEP_KINDS = {"agent", "kanban_agent"}


def parse_definition(definition: dict[str, Any] | str) -> tuple[dict[str, Any] | None, Diagnostic | None]:
    """Coerce ``definition`` into a ``dict``.

    Accepts an already-parsed ``dict`` or a JSON string (stdlib ``json`` only;
    YAML is intentionally unsupported). Returns ``(parsed, None)`` on success or
    ``(None, diagnostic)`` describing the parse/type failure.
    """
    if isinstance(definition, dict):
        return definition, None

    if isinstance(definition, str):
        try:
            loaded = json.loads(definition)
        except json.JSONDecodeError as exc:
            return None, Diagnostic(
                severity="error",
                code=err.E_PARSE,
                message=f"definition is not valid JSON: {exc.msg} (line {exc.lineno})",
                pointer="",
            )
        if not isinstance(loaded, dict):
            return None, Diagnostic(
                severity="error",
                code=err.E_SCHEMA_TOPLEVEL,
                message=f"top-level definition must be an object, got {type(loaded).__name__}",
                pointer="",
            )
        return loaded, None

    return None, Diagnostic(
        severity="error",
        code=err.E_SCHEMA_TOPLEVEL,
        message=f"definition must be a dict or JSON string, got {type(definition).__name__}",
        pointer="",
    )


def canonicalize(definition: dict[str, Any]) -> str:
    """Return the canonical JSON encoding (sorted keys, compact separators).

    Used as the stable input to :func:`def_hash`. Non-JSON values are coerced
    via ``str`` to keep hashing total even for slightly malformed inputs.
    """
    return json.dumps(
        definition,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def def_hash(definition: dict[str, Any]) -> str:
    """Return the full sha256 hex digest of the canonicalized definition."""
    return hashlib.sha256(canonicalize(definition).encode("utf-8")).hexdigest()


def validate_structure(definition: dict[str, Any]) -> list[Diagnostic]:
    """Validate the top-level shape and every step recursively.

    Checks required top-level keys, ``version``, step ``kind`` discrimination,
    required per-kind keys, duplicate ids, and reference *forms* (well-formed
    ``$ref:`` strings — resolvability is checked in :mod:`sandbox`). Returns a
    flat list of :class:`Diagnostic`; an empty list means the structure is
    well-formed.
    """
    diags: list[Diagnostic] = []

    # --- top level -------------------------------------------------------
    if "version" not in definition:
        diags.append(_e(err.E_SCHEMA_TOPLEVEL, "missing required key 'version'", "/version"))
    elif definition["version"] != SUPPORTED_VERSION:
        diags.append(
            _e(
                err.E_VERSION,
                f"unsupported version {definition['version']!r}; expected {SUPPORTED_VERSION!r}",
                "/version",
            )
        )

    name = definition.get("name")
    if "name" not in definition:
        diags.append(_e(err.E_SCHEMA_TOPLEVEL, "missing required key 'name'", "/name"))
    elif not isinstance(name, str) or not name or not _is_identifier_safe(name):
        diags.append(
            _e(err.E_SCHEMA_TOPLEVEL, "'name' must be a non-empty identifier-safe string", "/name")
        )

    if "inputs" in definition and not isinstance(definition["inputs"], dict):
        diags.append(_e(err.E_SCHEMA_TOPLEVEL, "'inputs' must be an object", "/inputs"))

    steps = definition.get("steps")
    if "steps" not in definition:
        diags.append(_e(err.E_SCHEMA_TOPLEVEL, "missing required key 'steps'", "/steps"))
        return diags
    if not isinstance(steps, list):
        diags.append(_e(err.E_SCHEMA_TOPLEVEL, "'steps' must be a list", "/steps"))
        return diags

    # --- steps (recursive) ----------------------------------------------
    seen_ids: set[str] = set()
    _validate_steps(steps, "/steps", diags, seen_ids)
    return diags


def _validate_steps(
    steps: list[Any],
    base_pointer: str,
    diags: list[Diagnostic],
    seen_ids: set[str],
) -> None:
    """Recursively validate a list of step objects rooted at ``base_pointer``."""
    for i, step in enumerate(steps):
        ptr = f"{base_pointer}/{i}"
        if not isinstance(step, dict):
            diags.append(_e(err.E_SCHEMA_STEP, "step must be an object", ptr))
            continue

        kind = step.get("kind")
        if kind not in STEP_KINDS:
            diags.append(
                _e(err.E_SCHEMA_STEP, f"step 'kind' must be one of {STEP_KINDS}, got {kind!r}", f"{ptr}/kind")
            )
            continue

        step_id = step.get("id")
        if not isinstance(step_id, str) or not step_id:
            diags.append(_e(err.E_SCHEMA_STEP, "step 'id' must be a non-empty string", f"{ptr}/id"))
        else:
            if step_id in seen_ids:
                diags.append(_e(err.E_DUP_STEP_ID, f"duplicate step id {step_id!r}", f"{ptr}/id"))
            seen_ids.add(step_id)

        _validate_depends_on(step, ptr, diags)

        if kind == "agent":
            _validate_agent_step(step, ptr, diags)
        elif kind == "kanban_agent":
            _validate_kanban_agent_step(step, ptr, diags)
        elif kind == "if":
            _validate_if_step(step, ptr, diags, seen_ids)
        else:
            child_key = _CHILD_KEYS[kind]
            children = step.get(child_key)
            if not isinstance(children, list) or not children:
                diags.append(
                    _e(err.E_SCHEMA_STEP, f"{kind} step requires a non-empty '{child_key}' list", f"{ptr}/{child_key}")
                )
                continue
            if kind == "phase" and "label" in step and not isinstance(step["label"], str):
                diags.append(_e(err.E_SCHEMA_STEP, "phase 'label' must be a string", f"{ptr}/label"))
            _validate_steps(children, f"{ptr}/{child_key}", diags, seen_ids)


def _validate_agent_step(step: dict[str, Any], ptr: str, diags: list[Diagnostic]) -> None:
    """Validate the per-kind shape of an ``agent`` step (forms only)."""
    agent = step.get("agent")
    if not isinstance(agent, str) or not agent:
        diags.append(_e(err.E_SCHEMA_STEP, "agent step requires a non-empty 'agent' id", f"{ptr}/agent"))

    _validate_effect_input_contract(step, ptr, diags)


def _validate_kanban_agent_step(step: dict[str, Any], ptr: str, diags: list[Diagnostic]) -> None:
    """Validate the durable Kanban-backed agent awaitable step shape."""
    profile = step.get("profile")
    if not isinstance(profile, str) or not profile:
        diags.append(
            _e(err.E_SCHEMA_STEP, "kanban_agent step requires a non-empty 'profile'", f"{ptr}/profile")
        )
    elif not _is_identifier_safe(profile):
        diags.append(
            _e(err.E_SCHEMA_STEP, "kanban_agent 'profile' must be identifier-safe", f"{ptr}/profile")
        )

    if "task" not in step:
        diags.append(_e(err.E_SCHEMA_STEP, "kanban_agent step requires 'task'", f"{ptr}/task"))
    elif not isinstance(step["task"], (dict, str)):
        diags.append(_e(err.E_SCHEMA_STEP, "kanban_agent 'task' must be an object or string", f"{ptr}/task"))

    if "wait" in step and not isinstance(step["wait"], bool):
        diags.append(_e(err.E_SCHEMA_STEP, "kanban_agent 'wait' must be a boolean", f"{ptr}/wait"))

    _validate_effect_input_contract(step, ptr, diags)


def _validate_if_step(
    step: dict[str, Any],
    ptr: str,
    diags: list[Diagnostic],
    seen_ids: set[str],
) -> None:
    """Validate the deterministic conditional step shape."""
    condition = step.get("condition")
    if not isinstance(condition, dict):
        diags.append(_e(err.E_SCHEMA_STEP, "if step requires a 'condition' object", f"{ptr}/condition"))
    else:
        ref = condition.get("ref")
        if not isinstance(ref, str) or not ref.startswith("$ref:"):
            diags.append(_e(err.E_SCHEMA_STEP, "if condition requires a '$ref:' string in 'ref'", f"{ptr}/condition/ref"))
        op = condition.get("op")
        if op not in CONDITION_OPS:
            diags.append(
                _e(err.E_SCHEMA_STEP, f"if condition 'op' must be one of {CONDITION_OPS}, got {op!r}", f"{ptr}/condition/op")
            )
        if op in {"eq", "ne"} and "value" not in condition:
            diags.append(_e(err.E_SCHEMA_STEP, f"if condition op {op!r} requires 'value'", f"{ptr}/condition/value"))

    then_steps = step.get("then")
    if not isinstance(then_steps, list) or not then_steps:
        diags.append(_e(err.E_SCHEMA_STEP, "if step requires a non-empty 'then' list", f"{ptr}/then"))
    else:
        _validate_steps(then_steps, f"{ptr}/then", diags, seen_ids)

    if "else" in step:
        else_steps = step.get("else")
        if not isinstance(else_steps, list):
            diags.append(_e(err.E_SCHEMA_STEP, "if step 'else' must be a list", f"{ptr}/else"))
        else:
            _validate_steps(else_steps, f"{ptr}/else", diags, seen_ids)


def _validate_depends_on(step: dict[str, Any], ptr: str, diags: list[Diagnostic]) -> None:
    """Validate depends_on shape shared by every step kind."""
    if "depends_on" in step:
        dep = step["depends_on"]
        if not isinstance(dep, list) or not all(isinstance(d, str) for d in dep):
            diags.append(_e(err.E_SCHEMA_STEP, "'depends_on' must be a list of step ids", f"{ptr}/depends_on"))


def _validate_effect_input_contract(step: dict[str, Any], ptr: str, diags: list[Diagnostic]) -> None:
    """Validate fields shared by effectful leaf steps."""
    if "input" in step and not isinstance(step["input"], (dict, str)):
        diags.append(_e(err.E_SCHEMA_STEP, "'input' must be an object or a $ref string", f"{ptr}/input"))

    if "output_schema" in step and not isinstance(step["output_schema"], dict):
        diags.append(_e(err.E_SCHEMA_STEP, "'output_schema' must be an object", f"{ptr}/output_schema"))


def iter_agent_steps(steps: list[Any]):
    """Yield ``(step, json_pointer)`` for every ``agent`` step, recursively.

    Pointers are rooted at ``/steps`` to match :func:`validate_structure`.
    Non-conforming entries are skipped silently (structure validation reports
    those separately).
    """
    yield from _iter_agent_steps(steps, "/steps")


def _iter_agent_steps(steps: list[Any], base_pointer: str):
    for i, step in enumerate(steps):
        if not isinstance(step, dict):
            continue
        ptr = f"{base_pointer}/{i}"
        kind = step.get("kind")
        if kind in _EFFECT_STEP_KINDS:
            yield step, ptr
        elif kind in _CHILD_KEYS:
            child_key = _CHILD_KEYS[kind]
            children = step.get(child_key)
            if isinstance(children, list):
                yield from _iter_agent_steps(children, f"{ptr}/{child_key}")
        elif kind == "if":
            then_steps = step.get("then")
            if isinstance(then_steps, list):
                yield from _iter_agent_steps(then_steps, f"{ptr}/then")
            else_steps = step.get("else")
            if isinstance(else_steps, list):
                yield from _iter_agent_steps(else_steps, f"{ptr}/else")


def _is_identifier_safe(value: str) -> bool:
    """Return ``True`` if ``value`` is a conservative identifier-safe name."""
    return all(c.isalnum() or c in "." "_-" for c in value)


def _e(code: str, message: str, pointer: str) -> Diagnostic:
    """Construct an error-severity :class:`Diagnostic`."""
    return Diagnostic(severity="error", code=code, message=message, pointer=pointer)
