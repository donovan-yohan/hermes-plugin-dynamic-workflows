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
