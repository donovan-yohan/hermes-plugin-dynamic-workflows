"""Exception hierarchy for the ``hermes_workflows`` plugin.

All exceptions derive from :class:`WorkflowError`. Diagnostics carried by
:class:`WorkflowValidationError` use the stable string codes defined alongside
them so downstream generators and tests can assert on codes, not messages.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:  # avoid an import cycle at runtime; only needed for typing.
    from .models import ValidationResult

__all__ = [
    "WorkflowError",
    "WorkflowValidationError",
    "RunNotFound",
    "SandboxPolicyError",
    "ScriptValidationError",
    "WorkflowSubprocessError",
    "CapabilityDenied",
    "ScriptRunStoreError",
    "ScriptRunNotFound",
    "CorruptScriptRunError",
    "GrantError",
    "GrantDenied",
    # Diagnostic codes.
    "E_PARSE",
    "E_SCHEMA_TOPLEVEL",
    "E_SCHEMA_STEP",
    "E_VERSION",
    "E_DUP_STEP_ID",
    "E_UNKNOWN_AGENT",
    "E_BAD_REF",
    "E_UNRESOLVED_REF",
    "E_CYCLE",
    "E_POLICY_NETWORK",
    "E_POLICY_FILESYSTEM",
    "E_DISALLOWED_CAPABILITY",
    "W_NO_OUTPUT_SCHEMA",
    "W_UNDECLARED_INPUT",
    "W_POLICY_DEFAULT",
    # Script-VM diagnostic codes (issue #2 / #4 validation contract).
    "E_SCRIPT_SYNTAX",
    "E_SCRIPT_EMPTY",
    "E_SCRIPT_TOO_LARGE",
    "E_SCRIPT_META_POSITION",
    "E_SCRIPT_META_SHAPE",
    "E_SCRIPT_META_FIELDS",
    "E_SCRIPT_IMPORT",
    "E_SCRIPT_CLASSDEF",
    "E_SCRIPT_SCOPE",
    "E_SCRIPT_FORBIDDEN_NAME",
    "E_SCRIPT_DUNDER",
    "E_SCRIPT_INTERNAL_ATTR",
    "E_SCRIPT_FORBIDDEN_NODE",
]

# ---------------------------------------------------------------------------
# Stable diagnostic codes. Treat these as a public, append-only enumeration.
# ---------------------------------------------------------------------------

# Errors -------------------------------------------------------------------
E_PARSE = "E_PARSE"  # definition string is not valid JSON.
E_SCHEMA_TOPLEVEL = "E_SCHEMA_TOPLEVEL"  # bad top-level shape/required keys.
E_SCHEMA_STEP = "E_SCHEMA_STEP"  # malformed step object.
E_VERSION = "E_VERSION"  # unsupported "version".
E_DUP_STEP_ID = "E_DUP_STEP_ID"  # duplicate step id within a scope.
E_UNKNOWN_AGENT = "E_UNKNOWN_AGENT"  # agent id not in the known registry.
E_BAD_REF = "E_BAD_REF"  # malformed "$ref:" reference form.
E_UNRESOLVED_REF = "E_UNRESOLVED_REF"  # reference to a missing step/input.
E_CYCLE = "E_CYCLE"  # cyclic depends_on / pipeline edge.
E_POLICY_NETWORK = "E_POLICY_NETWORK"  # policy.network requested true.
E_POLICY_FILESYSTEM = "E_POLICY_FILESYSTEM"  # policy.filesystem requested true.
E_DISALLOWED_CAPABILITY = "E_DISALLOWED_CAPABILITY"  # unknown capability key.

# Warnings (promoted to errors under strict=True) --------------------------
W_NO_OUTPUT_SCHEMA = "W_NO_OUTPUT_SCHEMA"  # agent step lacks output_schema.
W_UNDECLARED_INPUT = "W_UNDECLARED_INPUT"  # $ref:inputs.<key> not declared.
W_POLICY_DEFAULT = "W_POLICY_DEFAULT"  # no explicit policy block (default-deny).

# Script-VM validation codes (issue #2 launch gate / issue #4 contract) ------
# These gate a *Python workflow script* before it is ever launched in the
# subprocess VM. The script is a deterministic orchestration brain only: it may
# call RPC-backed capabilities but must not reach the filesystem, network,
# process table, environment, clock, or randomness, nor traverse dunder
# attributes to break out of the restricted builtins.
E_SCRIPT_SYNTAX = "E_SCRIPT_SYNTAX"  # script is not parseable Python.
E_SCRIPT_EMPTY = "E_SCRIPT_EMPTY"  # script has no executable statements.
E_SCRIPT_TOO_LARGE = "E_SCRIPT_TOO_LARGE"  # source/AST exceeds the size bound.
E_SCRIPT_META_POSITION = "E_SCRIPT_META_POSITION"  # 'meta' is not the first statement.
E_SCRIPT_META_SHAPE = "E_SCRIPT_META_SHAPE"  # 'meta' is not a pure literal dict.
E_SCRIPT_META_FIELDS = "E_SCRIPT_META_FIELDS"  # 'meta' lacks name/description.
E_SCRIPT_IMPORT = "E_SCRIPT_IMPORT"  # import / from-import is forbidden.
E_SCRIPT_CLASSDEF = "E_SCRIPT_CLASSDEF"  # class definitions are forbidden.
E_SCRIPT_SCOPE = "E_SCRIPT_SCOPE"  # global / nonlocal is forbidden.
E_SCRIPT_FORBIDDEN_NAME = "E_SCRIPT_FORBIDDEN_NAME"  # dangerous builtin reference.
E_SCRIPT_DUNDER = "E_SCRIPT_DUNDER"  # dunder name/attribute traversal.
E_SCRIPT_INTERNAL_ATTR = "E_SCRIPT_INTERNAL_ATTR"  # frame/code/generator/coroutine internals.
E_SCRIPT_FORBIDDEN_NODE = "E_SCRIPT_FORBIDDEN_NODE"  # disallowed syntax construct.


class WorkflowError(Exception):
    """Base class for every error raised by this package."""


class WorkflowValidationError(WorkflowError):
    """Raised when ``workflow_run`` is asked to run an invalid definition.

    Carries the full :class:`~hermes_workflows.models.ValidationResult` so the
    caller can inspect every diagnostic without re-running validation.
    """

    def __init__(self, result: "ValidationResult", message: Optional[str] = None) -> None:
        self.result = result
        if message is None:
            codes = ", ".join(d.code for d in result.errors) or "unknown"
            message = f"workflow definition failed validation ({codes})"
        super().__init__(message)


class RunNotFound(WorkflowError):
    """Raised by registry accessors for an unknown ``run_id``.

    Note: ``workflow_status`` intentionally does **not** raise this; it returns
    a :class:`~hermes_workflows.models.RunStatus` with ``status="unknown"``.
    """

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        super().__init__(f"run not found: {run_id!r}")


class SandboxPolicyError(WorkflowError):
    """Raised when the runtime is asked to perform a disallowed capability.

    In the skeleton this guards the default-deny boundary (network/filesystem):
    the runtime never opens sockets or files, so any attempt is a programming
    error surfaced here rather than a silent escape.
    """


class WorkflowScriptError(WorkflowError):
    """Base class for failures specific to the subprocess workflow VM."""


class ScriptValidationError(WorkflowScriptError):
    """Raised when a workflow *script* fails the pre-launch AST validation.

    Carries the list of :class:`~hermes_workflows.models.Diagnostic` findings so
    a caller (or the parent broker) can refuse to launch the subprocess and
    surface actionable, line-numbered diagnostics.
    """

    def __init__(self, diagnostics, message: Optional[str] = None) -> None:
        self.diagnostics = list(diagnostics)
        if message is None:
            codes = ", ".join(d.code for d in self.diagnostics) or "unknown"
            message = f"workflow script failed validation ({codes})"
        super().__init__(message)


class WorkflowSubprocessError(WorkflowScriptError):
    """Raised when the workflow subprocess crashes, times out, or misbehaves.

    The parent process owns the run; this error means the subprocess VM exited
    abnormally (non-zero exit, killed on timeout, or sent an unparseable/
    out-of-protocol frame). Parent state is never corrupted: the run is simply
    marked failed/stopped and the error is reported.
    """

    def __init__(self, message: str, *, exit_code: Optional[int] = None, stderr: str = "") -> None:
        self.exit_code = exit_code
        self.stderr = stderr
        super().__init__(message)


class CapabilityDenied(WorkflowScriptError):
    """Raised parent-side when an RPC request violates the capability policy.

    This is the parent-owned enforcement boundary: an unknown method, a
    malformed request, an unresolved agent id, or an exceeded budget/limit. The
    denial is serialized back to the subprocess as a structured RPC error so the
    script observes an exception rather than a silent success.
    """

    def __init__(self, message: str, *, code: str = "capability_denied") -> None:
        self.code = code
        super().__init__(message)


class ScriptRunStoreError(WorkflowScriptError):
    """Base class for durable script-run store failures (issue #3).

    Loading a persisted run or its replay cache can fail in well-defined ways —
    the run does not exist, its files are corrupt, or it was written by an
    incompatible store schema. Every such failure is one of these typed errors
    so a caller can catch them without crashing parent state; the parent simply
    declines to replay and may fall back to a fresh run.
    """


class ScriptRunNotFound(ScriptRunStoreError):
    """Raised when a durable script run id has no persisted record."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        super().__init__(f"script run not found: {run_id!r}")


class CorruptScriptRunError(ScriptRunStoreError):
    """Raised when a persisted run/journal/cache is unreadable or stale.

    Covers a malformed ``run.json``/``cache.jsonl`` line and an incompatible
    ``schema_version``. ``reason`` is a short stable token (``"corrupt_run"``,
    ``"corrupt_cache"``, ``"schema_version"``) so callers/tests can branch on the
    failure class without parsing the message.
    """

    def __init__(self, run_id: str, reason: str, message: Optional[str] = None) -> None:
        self.run_id = run_id
        self.reason = reason
        super().__init__(message or f"corrupt script run {run_id!r}: {reason}")


class GrantError(WorkflowError):
    """Raised for a malformed scoped-grant request, handle, or store id (issue #33).

    This covers *programming/shape* errors (bad scope, unknown side-effect class,
    non-positive TTL, a payload smuggling a raw credential). A grant *denial* is
    not an error: it is a structured :class:`~hermes_workflows.grants.GrantDecision`
    returned by ``resolve_grant`` so the controller fails closed without raising.
    """


class GrantDenied(GrantError):
    """Raised only when a caller opts into exception-style grant enforcement.

    The loop runtime never raises this — it halts the run with a structured
    ``halted_grant_denied`` signal instead. ``code`` mirrors the stable
    :class:`~hermes_workflows.grants.GrantDecision` code (e.g. ``"denied_scope"``,
    ``"expired"``, ``"no_broker"``) so callers can branch without parsing text.
    """

    def __init__(self, reason: str, *, code: str = "denied") -> None:
        self.code = code
        super().__init__(reason)
