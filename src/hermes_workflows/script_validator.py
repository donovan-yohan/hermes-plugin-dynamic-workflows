"""Static validation contract for *Python workflow scripts* (issues #2 / #4).

A workflow script is a small, deterministic orchestration brain. Unlike the
declarative JSON definitions handled by :mod:`hermes_workflows.schema`, a script
is real Python — so it is *never* executed inside the parent Hermes process and
*never* trusted on transport. Before the parent launches the subprocess VM
(:mod:`hermes_workflows.vm`), it runs this validator as a hard launch gate; the
subprocess guest (:mod:`hermes_workflows.vm_guest`) re-runs it defensively
before ``exec``. Both layers share this single contract.

What a script may do
--------------------
* Declare a literal ``meta = {...}`` as its **first statement** (``name`` and
  ``description`` required).
* Use deterministic control flow: ``if`` / ``for`` / ``while`` / ``try`` /
  function defs / comprehensions / ``async``/``await``.
* Call the RPC-backed capability globals (``agent``, ``kanban_agent``,
  ``capability``, ``parallel``, ``pipeline``, ``phase``, ``log``, ``workflow``)
  and read ``args`` / ``budget`` plus the injected-safe ``json`` / ``math``
  helpers.
* Use ``return`` and top-level ``await`` (the body is wrapped into a private
  async entrypoint before execution — see :func:`wrap_source`).

What a script may not do (rejected here, *before* launch)
---------------------------------------------------------
* ``import`` / ``from ... import`` anything (no filesystem/network/process/env/
  clock/randomness modules, no dynamic module loading).
* Reference dangerous builtins (``open``, ``eval``, ``exec``, ``compile``,
  ``__import__``, ``globals``, ``getattr``, ``input``, ...).
* Traverse dunder names/attributes (``__class__``, ``__globals__``,
  ``__subclasses__``, ``__builtins__``, ...) — the classic sandbox-escape path.
* Define classes, or use ``global`` / ``nonlocal``.

This module performs **pure static analysis** — it imports nothing the script
names, opens no files, and runs no script code.
"""

from __future__ import annotations

import ast
import textwrap
from typing import Any, Optional

from . import errors as err
from .models import Diagnostic

__all__ = [
    "ENTRY_NAME",
    "MAX_SOURCE_BYTES",
    "MAX_AST_NODES",
    "CAPABILITY_GLOBALS",
    "SAFE_MODULE_GLOBALS",
    "FORBIDDEN_NAMES",
    "ScriptValidation",
    "validate_script",
    "wrap_source",
]

# Name of the synthetic async entrypoint the script body is wrapped into so that
# top-level ``await`` and ``return`` are legal. Kept dunder-free on purpose so a
# script cannot reference or shadow it through the dunder rule below.
ENTRY_NAME = "_hermes_wf_entry"

# Conservative resource bounds. A workflow script is a coordination brain, not a
# program; these are generous for real orchestration yet reject pathological
# inputs cheaply, before any subprocess is spawned.
MAX_SOURCE_BYTES = 256 * 1024
MAX_AST_NODES = 20_000

# Globals the subprocess guest injects as RPC-backed capabilities or read-only
# context. Referencing them is always allowed; the parent broker decides at call
# time whether a specific request is permitted.
CAPABILITY_GLOBALS: frozenset[str] = frozenset(
    {"agent", "kanban_agent", "capability", "parallel", "pipeline", "phase", "log", "workflow", "args", "budget"}
)

# Deterministic, side-effect-free helpers the guest pre-binds so a script never
# needs ``import``. ``math`` is deterministic; ``json`` is pure (de)serialization.
SAFE_MODULE_GLOBALS: frozenset[str] = frozenset({"json", "math"})

# Builtins that are an escape hatch or a non-determinism/IO source. Even though
# the guest restricts ``__builtins__`` to a safe allow-list (defence in depth),
# referencing any of these is rejected here with an actionable diagnostic.
FORBIDDEN_NAMES: frozenset[str] = frozenset(
    {
        "open",
        "eval",
        "exec",
        "compile",
        "__import__",
        "__builtins__",
        "globals",
        "locals",
        "vars",
        "getattr",
        "setattr",
        "delattr",
        "input",
        "breakpoint",
        "exit",
        "quit",
        "help",
        "dir",
        "memoryview",
        "super",
        "object",
        "type",
        "print",  # would corrupt the RPC stream on stdout; use log() instead.
        "classmethod",
        "staticmethod",
        "property",
        "__loader__",
        "__spec__",
        "__name__",
        "__file__",
        "__package__",
    }
)


class ScriptValidation:
    """Result of statically validating a workflow script.

    Attributes:
        ok: ``True`` only when there are no error-severity diagnostics.
        diagnostics: All findings (errors only, in this contract).
        meta: The parsed ``meta`` literal when extractable, else ``None``.
    """

    __slots__ = ("ok", "diagnostics", "meta")

    def __init__(
        self,
        ok: bool,
        diagnostics: list[Diagnostic],
        meta: Optional[dict[str, Any]] = None,
    ) -> None:
        self.ok = ok
        self.diagnostics = diagnostics
        self.meta = meta

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "diagnostics": [d.as_dict() for d in self.diagnostics],
            "meta": self.meta,
        }


def wrap_source(source: str) -> str:
    """Wrap a script body in a private async entrypoint.

    Returns source defining ``async def <ENTRY_NAME>():`` with the original body
    indented beneath it, so top-level ``await`` and ``return`` parse and execute.
    A trailing ``pass`` guarantees a non-empty body. Every wrapped line is offset
    by exactly one line (the ``async def`` header), so a node at wrapped line *N*
    maps to original line *N - 1*.
    """
    indented = textwrap.indent(source, "    ")
    return f"async def {ENTRY_NAME}():\n{indented}\n    pass\n"


def validate_script(source: str) -> ScriptValidation:
    """Validate a workflow script against the launch contract.

    Returns a :class:`ScriptValidation`; ``ok`` is ``True`` only when no
    error-severity diagnostic was produced. Never raises for script content —
    syntax errors become diagnostics — so callers get a total function.
    """
    diags: list[Diagnostic] = []

    if not isinstance(source, str):
        diags.append(_e(err.E_SCRIPT_SYNTAX, f"script must be a string, got {type(source).__name__}", 0))
        return ScriptValidation(False, diags)

    if len(source.encode("utf-8", "surrogatepass")) > MAX_SOURCE_BYTES:
        diags.append(_e(err.E_SCRIPT_TOO_LARGE, f"script exceeds {MAX_SOURCE_BYTES} bytes", 0))
        return ScriptValidation(False, diags)

    if not source.strip():
        diags.append(_e(err.E_SCRIPT_EMPTY, "script is empty", 0))
        return ScriptValidation(False, diags)

    wrapped = wrap_source(source)
    try:
        tree = ast.parse(wrapped, filename="<workflow-script>", mode="exec")
    except SyntaxError as exc:
        line = (exc.lineno or 1) - 1  # undo the wrapper header offset.
        diags.append(_e(err.E_SCRIPT_SYNTAX, f"syntax error: {exc.msg}", max(line, 0)))
        return ScriptValidation(False, diags)

    # The wrapper guarantees a single async function def at module level.
    entry = tree.body[0]
    assert isinstance(entry, ast.AsyncFunctionDef)
    body = [s for s in entry.body if not _is_trailing_pass(s)]

    node_count = sum(1 for _ in ast.walk(tree))
    if node_count > MAX_AST_NODES:
        diags.append(_e(err.E_SCRIPT_TOO_LARGE, f"script AST has {node_count} nodes (max {MAX_AST_NODES})", 0))
        return ScriptValidation(False, diags)

    meta = _validate_meta(body, diags)
    _walk_forbidden(entry, diags)

    errors = [d for d in diags if d.severity == "error"]
    return ScriptValidation(ok=not errors, diagnostics=diags, meta=meta if not errors else meta)


# ---------------------------------------------------------------------------
# meta literal: must be the first statement and a pure dict literal.
# ---------------------------------------------------------------------------

def _validate_meta(body: list[ast.stmt], diags: list[Diagnostic]) -> Optional[dict[str, Any]]:
    if not body:
        diags.append(_e(err.E_SCRIPT_EMPTY, "script has no executable statements", 0))
        return None

    first = body[0]
    if not (isinstance(first, ast.Assign) and len(first.targets) == 1):
        diags.append(_e(err.E_SCRIPT_META_POSITION, "the first statement must be 'meta = {...}'", _line(first)))
        return None
    target = first.targets[0]
    if not (isinstance(target, ast.Name) and target.id == "meta"):
        diags.append(_e(err.E_SCRIPT_META_POSITION, "the first statement must assign to 'meta'", _line(first)))
        return None

    try:
        meta_value = ast.literal_eval(first.value)
    except (ValueError, SyntaxError, TypeError):
        diags.append(_e(err.E_SCRIPT_META_SHAPE, "'meta' must be a pure literal (no calls/names/operators)", _line(first)))
        return None

    if not isinstance(meta_value, dict):
        diags.append(_e(err.E_SCRIPT_META_SHAPE, "'meta' must be a dict literal", _line(first)))
        return None

    missing = [k for k in ("name", "description") if not _nonempty_str(meta_value.get(k))]
    if missing:
        diags.append(
            _e(err.E_SCRIPT_META_FIELDS, f"'meta' must define non-empty {', '.join(missing)}", _line(first))
        )
        return meta_value
    return meta_value


# ---------------------------------------------------------------------------
# Forbidden-construct walk over the (wrapped) script body.
# ---------------------------------------------------------------------------

# Non-dunder attribute prefixes that expose interpreter internals — frames,
# code objects, generator/coroutine/async-generator state, tracebacks. Reaching
# any of these lets a script walk ``cr_frame.f_globals -> sys.modules -> os`` and
# escape the restricted builtins entirely, so they are blocked even though they
# are not dunders. Each prefix ends in ``_`` so ordinary names/methods
# (``find``, ``format``, ``count``, ``copy``, ``age``) never match.
_INTERNAL_ATTR_PREFIXES: tuple[str, ...] = ("gi_", "cr_", "ag_", "f_", "tb_", "co_", "func_")

# Specific non-dunder attribute names that are escape hatches on their own.
_INTERNAL_ATTR_NAMES: frozenset[str] = frozenset({"mro", "gi_frame", "cr_frame"})

# Method names whose *runtime* template mini-language traverses attributes the
# AST never sees: ``"{0.__class__.__base__}".format(x)`` reaches dunders despite
# the static dunder rule (and leaks heap-address reprs into the result).
# f-strings are validated normally (their ``{expr}`` is real AST), so scripts
# keep a safe formatting path.
_FORBIDDEN_METHOD_ATTRS: frozenset[str] = frozenset({"format", "format_map"})


def _is_internal_attr(name: str) -> bool:
    return name in _INTERNAL_ATTR_NAMES or name.startswith(_INTERNAL_ATTR_PREFIXES)


# AST node types that are forbidden outright, mapped to (code, label).
_FORBIDDEN_NODES: dict[type, tuple[str, str]] = {
    ast.Import: (err.E_SCRIPT_IMPORT, "import statements are not allowed"),
    ast.ImportFrom: (err.E_SCRIPT_IMPORT, "from-import statements are not allowed"),
    ast.ClassDef: (err.E_SCRIPT_CLASSDEF, "class definitions are not allowed"),
    ast.Global: (err.E_SCRIPT_SCOPE, "'global' is not allowed"),
    ast.Nonlocal: (err.E_SCRIPT_SCOPE, "'nonlocal' is not allowed"),
}


def _walk_forbidden(entry: ast.AST, diags: list[Diagnostic]) -> None:
    """Walk every node beneath the entry function and flag forbidden constructs.

    Walks the entry node's children (not the synthetic wrapper itself) so the
    ``async def`` header is never mis-reported.
    """
    for node in ast.walk(entry):
        node_type = type(node)
        forbidden = _FORBIDDEN_NODES.get(node_type)
        if forbidden is not None:
            code, label = forbidden
            diags.append(_e(code, label, _line(node)))
            continue

        if isinstance(node, ast.Name):
            _check_name(node.id, node, diags)
        elif isinstance(node, ast.Attribute):
            if _is_dunder(node.attr):
                diags.append(
                    _e(err.E_SCRIPT_DUNDER, f"dunder attribute access '.{node.attr}' is not allowed", _line(node))
                )
            elif _is_internal_attr(node.attr):
                diags.append(
                    _e(
                        err.E_SCRIPT_INTERNAL_ATTR,
                        f"internal attribute access '.{node.attr}' is not allowed (frame/code/generator escape)",
                        _line(node),
                    )
                )
            elif node.attr in _FORBIDDEN_METHOD_ATTRS:
                diags.append(
                    _e(
                        err.E_SCRIPT_FORBIDDEN_NAME,
                        f"'.{node.attr}' is not allowed (its template traverses attributes; use an f-string)",
                        _line(node),
                    )
                )
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if _is_dunder(node.name):
                diags.append(
                    _e(err.E_SCRIPT_DUNDER, f"dunder function name '{node.name}' is not allowed", _line(node))
                )
        elif isinstance(node, ast.keyword):
            # ``f(**kwargs)`` is fine; nothing to check beyond the value (walked).
            pass


def _check_name(name: str, node: ast.AST, diags: list[Diagnostic]) -> None:
    if _is_dunder(name):
        diags.append(_e(err.E_SCRIPT_DUNDER, f"dunder name '{name}' is not allowed", _line(node)))
        return
    if name in FORBIDDEN_NAMES:
        diags.append(_e(err.E_SCRIPT_FORBIDDEN_NAME, f"name '{name}' is not allowed in a workflow script", _line(node)))


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------

def _is_dunder(name: str) -> bool:
    """Return ``True`` for any ``__dunder__`` identifier (escape-hatch guard)."""
    return len(name) >= 4 and name.startswith("__") and name.endswith("__")


def _is_trailing_pass(stmt: ast.stmt) -> bool:
    return isinstance(stmt, ast.Pass)


def _nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _line(node: ast.AST) -> int:
    """Map a wrapped-tree node back to its 1-based line in the original source."""
    lineno = getattr(node, "lineno", None)
    if lineno is None:
        return 0
    return max(lineno - 1, 0)


def _e(code: str, message: str, line: int) -> Diagnostic:
    """Build an error diagnostic with a line-anchored pointer."""
    pointer = f"/script/line/{line}" if line else "/script"
    return Diagnostic(severity="error", code=code, message=message, pointer=pointer)
