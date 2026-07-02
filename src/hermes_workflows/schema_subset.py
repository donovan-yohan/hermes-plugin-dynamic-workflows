"""Shared JSON-Schema-subset output validator (issue #107).

Both engines — the subprocess script VM (:mod:`hermes_workflows.vm`) and the
declarative JSON runtime (:mod:`hermes_workflows.runtime`) — validate agent
output against a caller-declared ``schema``. Before this module the two
engines each hand-rolled an identical *flat* ``{field: type}`` checker; that
shape cannot express a nested object, an array of objects, or an enum, even
though real workflow schemas need exactly that. This module is the single,
stdlib-only source of truth both engines call into.

Documented subset
------------------
Exactly these keywords are understood; anything else is a **fail-closed**
:class:`SchemaError` (never silently ignored) so a typo in a schema is caught
rather than quietly no-op'd:

* ``type`` — one of ``"object"``, ``"array"``, ``"string"``, ``"number"``,
  ``"integer"``, ``"boolean"``, ``"null"``.
* ``properties`` — ``{name: <subschema>}``, meaningful for ``type: object``.
* ``required`` — list of property names that must be present.
* ``items`` — a ``<subschema>`` every array element must satisfy, meaningful
  for ``type: array``.
* ``enum`` — a list of literal values; the instance must equal one of them.
* ``additionalProperties`` — ``bool``; when ``False``, an object instance with
  a field outside ``properties`` is rejected. Defaults permissive (``True``),
  matching the historical flat checkers, which never rejected extra fields.

A ``<subschema>`` is always an ``object`` (``dict``) using only the keywords
above; there is no shorthand form at nested levels.

Backward compatibility (legacy flat schemas)
---------------------------------------------
Both engines historically accepted a flat ``{field_name: type_hint}`` mapping
— no ``type``/``properties`` wrapper — where ``type_hint`` was either one of a
handful of type-name strings (``"string"``, ``"int"``, ``"list"``, ...) or a
Python ``type`` object, every declared field was implicitly *required*, and an
unrecognized hint silently skipped the type check for that field (but still
required its presence). :func:`normalize_schema` recognizes this shape (a
dict whose top level does **not** declare ``type`` as one of the subset type
names) and normalizes it into
``{"type": "object", "properties": {name: {"type": ...}}, "required": [...]}``
with ``additionalProperties`` left permissive, so old schemas keep validating
identically. A legacy ``"float"`` hint (or the Python ``float`` type) widens
to the subset ``"number"`` (``int`` or ``float``) rather than the historical
float-only check — the only intentional behavioural widening, and it was
previously unused by any shipped schema.

Two schema shapes are therefore ambiguous whenever a legacy field happens to
be named ``"type"``: **any** legacy schema containing a field named
``"type"`` is reinterpreted as a declared subset-schema root (see
:func:`_is_declared_subset`), not a legacy field declaration — this fails
closed with a :class:`SchemaError` unless the hint value also happens to be a
valid subset type name (e.g. ``{"type": "string"}``), in which case the
verdict silently flips to subset semantics instead of failing closed. The
same reinterpretation applies, more narrowly, to a legacy schema whose entire
key set is drawn from :data:`ALLOWED_KEYWORDS` and that also declares one of
``"properties"``/``"items"``/``"enum"``/``"additionalProperties"``, or a
*list-valued* ``"required"`` — added so a type-less, idiomatic subset root
such as ``{"properties": {...}, "required": [...]}`` is not silently misread
as a legacy schema requiring literal fields named ``properties``/``required``.
This is a documented, narrow trade-off; no shipped schema relies on a field
named after a subset keyword.

Two further, intentional divergences from the byte-for-byte pre-#107 flat
checkers (both undisclosed until now; neither changes shipped behaviour
because no shipped schema exercises them):

* A legacy hint of ``"any"`` (or any other unrecognized hint) now accepts a
  ``bool`` payload value. The pre-#107 ``vm.py``/``runtime.py`` flat checkers
  special-cased ``"any"`` to still reject ``bool``; ``kanban.py``'s flat
  checker never did (its ``"any"`` hint maps to ``(object,)``, which already
  accepts everything including ``bool``). This module's "unrecognized hint is
  fully unconstrained" rule (:func:`_legacy_property_schema`) matches the
  ``kanban.py`` behaviour, not the ``vm.py``/``runtime.py`` behaviour.
* ``{"type": "str", "value": "int"}`` — a legacy schema with a field
  literally named ``"type"`` whose hint (``"str"``) happens to be a valid
  subset type name — is *not* reinterpreted field-by-field as a subset root
  (a subset root has no keyword named ``"value"``); it fails closed with an
  ``unsupported schema keyword(s) ['value']`` :class:`SchemaError`, where the
  pre-#107 checkers accepted it as a legacy schema requiring both fields.
"""

from __future__ import annotations

from typing import Any, Optional

__all__ = [
    "SchemaError",
    "TYPE_NAMES",
    "ALLOWED_KEYWORDS",
    "check_schema",
    "normalize_schema",
    "validate",
    "is_declared_subset_schema",
]


class SchemaError(Exception):
    """A schema is malformed, unsupported, or a payload violates it.

    Both failure classes (an author-facing malformed schema, and a genuine
    payload/schema mismatch) are represented by the same exception so callers
    that only ever raised one error kind for "schema stuff went wrong" — the
    established behaviour of both engines — keep doing exactly that.
    """


# JSON-Schema-subset ``type`` keyword values this module understands.
TYPE_NAMES: frozenset[str] = frozenset(
    {"object", "array", "string", "number", "integer", "boolean", "null"}
)

# Every keyword honored anywhere in a subschema. Fail-closed on anything else.
ALLOWED_KEYWORDS: frozenset[str] = frozenset(
    {"type", "properties", "required", "items", "enum", "additionalProperties"}
)

_PY_TYPES: dict[str, tuple[type, ...]] = {
    "object": (dict,),
    "array": (list,),
    "string": (str,),
    "number": (int, float),
    "integer": (int,),
    "boolean": (bool,),
    "null": (type(None),),
}

# Legacy flat-schema type-hint aliases (mirrors the pre-#107 vm.py/runtime.py/
# kanban.py ``_TYPE_MAP`` tables) mapped onto the subset type names above.
# ``"any"`` and any unrecognized hint normalize to an unconstrained subschema
# (``{}``), matching the historical "unknown hint: leniently accept" behaviour
# (the field is still required to be *present*, just never type-checked).
_LEGACY_TYPE_ALIASES: dict[str, str] = {
    "string": "string",
    "str": "string",
    "number": "number",
    "float": "number",  # historical float-only narrows; widened deliberately.
    "int": "integer",
    "integer": "integer",
    "bool": "boolean",
    "boolean": "boolean",
    "object": "object",
    "dict": "object",
    "list": "array",
    "array": "array",
}

# Python ``type`` objects a legacy hint could be (the historical code accepted
# a bare type as well as a name string).
_LEGACY_PY_TYPE_NAMES: dict[type, str] = {
    str: "string",
    int: "integer",
    float: "number",
    bool: "boolean",
    dict: "object",
    list: "array",
}


def check_schema(schema: Any) -> Optional[str]:
    """Return an error message if ``schema`` is malformed, else ``None``.

    Never raises — this is the pure, side-effect-free check static validators
    (``workflow_validate`` / the JSON engine's structural lint) use to reject a
    bad schema *before any run starts*, without needing to catch an exception.
    A falsy/absent schema (no declaration) is not malformed: returns ``None``.
    """
    if schema is None or schema == {}:
        return None
    if not isinstance(schema, dict):
        return None  # not a schema at all; callers treat this as "no schema".
    try:
        normalize_schema(schema)
    except SchemaError as exc:
        return str(exc)
    return None


def normalize_schema(schema: Any) -> Optional[dict[str, Any]]:
    """Return ``schema`` normalized into the subset shape, or ``None``.

    ``None`` means "no schema declared" (a falsy value, or anything that is
    not a non-empty ``dict``) — callers must skip validation entirely, exactly
    as the historical flat checkers did. Raises :class:`SchemaError` for a
    malformed schema: an unsupported keyword, a bad ``type`` value, or a
    structurally invalid ``properties``/``items``/``required``/``enum``.
    """
    if not isinstance(schema, dict) or not schema:
        return None
    if _is_declared_subset(schema):
        return _normalize_subset_node(schema, "")
    return _normalize_legacy_flat(schema)


def validate(payload: Any, schema: Any) -> None:
    """Validate ``payload`` against ``schema``; no-op when nothing is declared.

    Raises :class:`SchemaError` for a malformed schema (see
    :func:`normalize_schema`) or a genuine payload mismatch (missing required
    field, wrong type, or an enum violation).
    """
    node = normalize_schema(schema)
    if node is None:
        return
    _check_value(payload, node, "output")


# ---------------------------------------------------------------------------
# Legacy-vs-subset detection and normalization.
# ---------------------------------------------------------------------------


# Keywords whose mere presence at the root — absent ``"type"`` — is still
# strong enough evidence of a hand-authored subset schema to reinterpret a
# type-less root (e.g. ``{"properties": {...}, "required": [...]}``) as
# subset rather than legacy flat. ``"required"`` is deliberately excluded
# here: a legacy field literally named ``"required"`` is common enough (and
# its hint is essentially never a list) that only a *list-valued* "required"
# counts — checked separately below.
_STRUCTURAL_SUBSET_KEYWORDS: frozenset[str] = frozenset(
    {"properties", "items", "enum", "additionalProperties"}
)


def _is_declared_subset(schema: dict[str, Any]) -> bool:
    """Return ``True`` when ``schema`` reads as a hand-authored subset schema.

    A dict is a declared subset schema whenever it declares a ``type`` keyword
    at all — including an *invalid* value, which must fail closed as a
    malformed subset schema rather than be silently reinterpreted as a legacy
    field named ``"type"``. The only shape a legacy flat schema could produce
    that collides with this is a field literally named ``"type"`` whose hint
    happens to be a valid subset type name (see the module docstring).

    Absent ``"type"``, a root is *also* read as a declared subset schema when
    every one of its keys is drawn from :data:`ALLOWED_KEYWORDS` and it
    declares a structural keyword (``properties``/``items``/``enum``/
    ``additionalProperties``) or a list-valued ``required`` — otherwise a
    type-less, idiomatic subset root like
    ``{"properties": {...}, "required": [...]}`` would be silently misread as
    a legacy schema requiring literal fields named ``properties``/``required``
    (see the module docstring).
    """
    if "type" in schema:
        return True
    if not set(schema).issubset(ALLOWED_KEYWORDS):
        return False
    if any(keyword in schema for keyword in _STRUCTURAL_SUBSET_KEYWORDS):
        return True
    return isinstance(schema.get("required"), list)


def is_declared_subset_schema(schema: Any) -> bool:
    """Public wrapper on :func:`_is_declared_subset` for static analyzers.

    ``False`` for anything that is not a non-empty ``dict``. Exists so a
    caller like :mod:`hermes_workflows.script_validator` can tell, ahead of
    :func:`normalize_schema`, whether a literal schema *reads* as a subset
    root — needed to reject a subset-shaped ``schema=`` literal passed to a
    call site whose runtime enforcement only understands legacy flat schemas
    (e.g. ``kanban_agent``'s ``workflow_result`` contract).
    """
    return isinstance(schema, dict) and bool(schema) and _is_declared_subset(schema)


def _normalize_subset_node(node: Any, path: str) -> dict[str, Any]:
    """Validate and return one already-declared subset subschema, recursively."""
    if not isinstance(node, dict):
        raise SchemaError(f"schema{path}: subschema must be an object, got {type(node).__name__}")

    unknown = sorted(set(node) - ALLOWED_KEYWORDS)
    if unknown:
        raise SchemaError(f"schema{path}: unsupported schema keyword(s) {unknown!r}")

    result: dict[str, Any] = {}

    type_value = node.get("type")
    if "type" in node:
        if not isinstance(type_value, str) or type_value not in TYPE_NAMES:
            raise SchemaError(
                f"schema{path}/type: must be one of {sorted(TYPE_NAMES)!r}, got {type_value!r}"
            )
        result["type"] = type_value

    if "required" in node:
        required = node["required"]
        if not isinstance(required, list) or not all(isinstance(r, str) for r in required):
            raise SchemaError(f"schema{path}/required: must be a list of strings")
        result["required"] = list(required)

    if "properties" in node:
        properties = node["properties"]
        if not isinstance(properties, dict) or not all(isinstance(k, str) for k in properties):
            raise SchemaError(f"schema{path}/properties: must be an object keyed by property name")
        result["properties"] = {
            name: _normalize_subset_node(sub, f"{path}/properties/{name}") for name, sub in properties.items()
        }

    if "items" in node:
        result["items"] = _normalize_subset_node(node["items"], f"{path}/items")

    if "enum" in node:
        enum_values = node["enum"]
        if not isinstance(enum_values, list) or not enum_values:
            raise SchemaError(f"schema{path}/enum: must be a non-empty list")
        result["enum"] = list(enum_values)

    if "additionalProperties" in node:
        additional = node["additionalProperties"]
        if not isinstance(additional, bool):
            raise SchemaError(f"schema{path}/additionalProperties: must be a boolean")
        result["additionalProperties"] = additional

    return result


def _normalize_legacy_flat(schema: dict[str, Any]) -> dict[str, Any]:
    """Normalize a flat ``{field: type_hint}`` mapping into the subset shape."""
    properties: dict[str, Any] = {}
    for field_name, hint in schema.items():
        if not isinstance(field_name, str):
            raise SchemaError(f"schema: legacy field name must be a string, got {field_name!r}")
        properties[field_name] = _legacy_property_schema(hint)
    return {"type": "object", "properties": properties, "required": list(schema.keys())}


def _legacy_property_schema(hint: Any) -> dict[str, Any]:
    """Return the subset subschema for one legacy ``field: type_hint`` entry.

    An unrecognized hint (any Python type not in the historical table, or any
    string that is not a historical alias) normalizes to ``{}`` — unconstrained
    — matching the historical "unknown hint: leniently accept" behaviour.
    """
    if isinstance(hint, type):
        name = _LEGACY_PY_TYPE_NAMES.get(hint)
        return {"type": name} if name else {}
    if isinstance(hint, str):
        name = _LEGACY_TYPE_ALIASES.get(hint.lower())
        return {"type": name} if name else {}
    return {}


# ---------------------------------------------------------------------------
# Payload validation against an already-normalized subschema.
# ---------------------------------------------------------------------------


def _check_value(value: Any, node: dict[str, Any], path: str) -> None:
    type_name = node.get("type")
    if type_name is not None:
        expected = _PY_TYPES[type_name]
        if expected != (bool,) and isinstance(value, bool):
            raise SchemaError(f"{path} expected {type_name}, got bool")
        if not isinstance(value, expected):
            raise SchemaError(f"{path} expected {type_name}, got {type(value).__name__}")

    if "enum" in node and value not in node["enum"]:
        raise SchemaError(f"{path} value is not one of the declared enum values")

    if isinstance(value, dict) and (type_name == "object" or "properties" in node or "required" in node):
        _check_object(value, node, path)
    elif isinstance(value, list) and (type_name == "array" or "items" in node):
        _check_array(value, node, path)


def _check_object(value: dict[str, Any], node: dict[str, Any], path: str) -> None:
    for name in node.get("required", ()):
        if name not in value:
            raise SchemaError(f"{path} missing required field {name!r}")

    properties: dict[str, Any] = node.get("properties", {})
    for name, subnode in properties.items():
        if name in value:
            _check_value(value[name], subnode, f"{path}.{name}")

    if node.get("additionalProperties") is False:
        extra = sorted(set(value) - set(properties))
        if extra:
            raise SchemaError(f"{path} has unexpected field(s) {extra!r} (additionalProperties: false)")


def _check_array(value: list[Any], node: dict[str, Any], path: str) -> None:
    items_schema = node.get("items")
    if items_schema is None:
        return
    for index, item in enumerate(value):
        _check_value(item, items_schema, f"{path}[{index}]")
