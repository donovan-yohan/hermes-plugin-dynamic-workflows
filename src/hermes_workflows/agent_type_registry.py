"""File-based agent-type registry backing ``agentType`` resolution (issue #104).

Issue #92 adds the ``agentType`` opt to ``agent(prompt, opts)``; this module is
what it resolves against. One definition file per agent type on disk::

    <root>/<agent-type-name>.md

with a ``---``-delimited frontmatter block of flat ``key: value`` lines
(``name`` / ``description`` / ``model`` / optional ``effort``) followed by the
system prompt body. Multiple roots are checked in order and the first match
wins, so passing a project root before a user root gives project scope
precedence over user scope -- the registry itself has no notion of "project"
vs "user", it is simply ordered roots (mirrors
:class:`hermes_workflows.script_catalog.FileWorkflowScriptCatalog`).

Roots are always supplied explicitly at :class:`AgentTypeRegistry`
construction (which the broker owns -- see ``CapabilityBroker`` /
``WorkflowVM`` in :mod:`hermes_workflows.vm`); there is no implicit
cwd/environment-variable discovery the way
:func:`hermes_workflows.script_catalog.default_script_catalog_roots` has for
saved scripts, since agent-type definitions are host/deployment configuration,
not model-authored artifacts.

A built-in ``general-purpose`` default is always resolvable -- even with zero
roots configured -- so ``agent(prompt)`` with no ``agentType`` has defined
semantics (bare prompt == ``general-purpose``). A project/user root may still
shadow it with an on-disk ``general-purpose.md`` file, exactly like any other
agent type name.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from .script_catalog import ensure_within_root, safe_script_name

__all__ = [
    "AgentTypeDefinition",
    "AgentTypeRegistry",
    "AgentTypeRegistryError",
    "GENERAL_PURPOSE_AGENT_TYPE",
    "safe_agent_type_name",
]

GENERAL_PURPOSE_AGENT_TYPE = "general-purpose"

# The built-in fallback body for the auto-registered ``general-purpose`` type.
# Deliberately generic: it is what every pre-#104 bare ``agent(prompt)`` call
# gets by default, so it must not narrow scope or impose a persona.
_GENERAL_PURPOSE_SYSTEM_PROMPT = (
    "You are a general-purpose agent. Complete the requested task directly "
    "and concisely, using only the prompt and context provided."
)


class AgentTypeRegistryError(Exception):
    """Metadata-only diagnostic for agent-type resolution failure.

    ``code`` distinguishes the two dispatch-error shapes the broker surfaces
    as a :class:`~hermes_workflows.errors.CapabilityDenied` (issue #104):
    ``"unknown_agent_type"`` for a name absent from every configured root (and
    not the built-in default), and ``"agent_type_invalid"`` for a name that
    fails path-safety checks (path traversal) or whose on-disk definition is
    malformed (missing/unparsable frontmatter). Never carries file contents or
    a raw filesystem path -- only the agent-type name and a short reason.
    """

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def safe_agent_type_name(name: str) -> str:
    """Return ``name`` when it is a safe agent-type id, else raise ``ValueError``.

    An agent-type id is, like a saved script name, one safe path segment --
    reuses :func:`hermes_workflows.script_catalog.safe_script_name` for that
    vetted hygiene (no ``/``/``\\``/``..``, restricted charset, no leading
    dot) rather than re-implementing it, per issue #104.

    A trailing ``.md`` or ``.workflow.py`` suffix is rejected outright rather
    than stripped: ``agentType`` is a name, not a filename, and silently
    normalizing a suffixed spelling would let ``"reviewer"``,
    ``"reviewer.md"``, and (via :func:`~hermes_workflows.script_catalog
    .safe_script_name`'s own script-catalog suffix handling) ``"reviewer
    .workflow.py"`` all resolve the same on-disk definition while still
    minting distinct ``ChildAgentRequest`` fingerprints (the request stores
    the raw spelling) -- fragmenting the prompt-agent cache for what is
    semantically one call.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("agent_type must be a non-empty string")
    if name.endswith(".md") or name.endswith(".workflow.py"):
        raise ValueError(f"agent_type must not include a file suffix: {name!r}")
    try:
        return safe_script_name(name)
    except ValueError as exc:
        raise ValueError(f"unsafe agent_type: {name!r}") from exc


@dataclass(frozen=True)
class AgentTypeDefinition:
    """One resolved agent-type definition: frontmatter metadata + system prompt."""

    name: str
    description: Optional[str]
    model: Optional[str]
    effort: Optional[str]
    system_prompt: str
    source_path: Optional[str] = None


def _split_frontmatter(text: str) -> tuple[Optional[dict[str, str]], str]:
    """Split ``---``-delimited frontmatter from the trailing body.

    Returns ``(None, text)`` when ``text`` does not open with ``---`` (no
    frontmatter present at all). Raises ``ValueError`` for a frontmatter block
    that is present but structurally broken: an opening ``---`` that is never
    closed, or a line with no ``:`` separator. The ``ValueError`` message carries only the 1-based
    line number, never the offending line's text -- this diagnostic is
    forwarded verbatim into a script-visible ``CapabilityDenied`` (see
    :meth:`AgentTypeRegistry.resolve` / :class:`AgentTypeRegistryError`), so
    it must stay metadata-only even when the file contains secrets.
    """
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return None, text
    end = None
    for i in range(1, len(lines)):
        if lines[i].strip() == "---":
            end = i
            break
    if end is None:
        # An opened-but-never-closed block is malformed frontmatter, not "no
        # frontmatter" -- reporting the latter would mislead the operator.
        raise ValueError("unclosed '---' frontmatter block")
    frontmatter: dict[str, str] = {}
    for line_no, line in enumerate(lines[1:end], start=2):
        if not line.strip():
            continue
        if ":" not in line:
            raise ValueError(f"malformed frontmatter line {line_no} (missing ':')")
        key, _, value = line.partition(":")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if not key:
            raise ValueError(f"malformed frontmatter line {line_no} (empty key)")
        frontmatter[key] = value
    body = "\n".join(lines[end + 1 :]).strip("\n")
    return frontmatter, body


class AgentTypeRegistry:
    """Project-over-user file-based registry of named agent types.

    ``roots`` are checked in the order given; the first root containing a
    matching ``<name>.md`` definition wins, so project scope shadowing user
    scope is simply "pass the project root first". Construct one instance per
    deployment/session with explicit roots -- there is no implicit discovery.
    """

    def __init__(self, roots: Iterable[str | Path] = ()) -> None:
        self.roots = [Path(r).expanduser() for r in roots]

    def resolve(self, agent_type: str) -> AgentTypeDefinition:
        """Resolve ``agent_type`` to its definition.

        Raises :class:`AgentTypeRegistryError` (``code="agent_type_invalid"``)
        for an unsafe name (path traversal) or a malformed on-disk definition,
        and (``code="unknown_agent_type"``) for a name absent from every root
        and not the built-in :data:`GENERAL_PURPOSE_AGENT_TYPE` default.
        """
        try:
            safe_name = safe_agent_type_name(agent_type)
        except ValueError as exc:
            raise AgentTypeRegistryError(
                f"invalid agentType {agent_type!r}: {exc}", code="agent_type_invalid"
            ) from exc

        for root in self.roots:
            candidate = root / f"{safe_name}.md"
            try:
                ensure_within_root(root, candidate)
            except ValueError as exc:
                raise AgentTypeRegistryError(
                    f"invalid agentType {agent_type!r}: path escaped registry root", code="agent_type_invalid"
                ) from exc
            if candidate.exists() and candidate.is_file():
                return self._load_definition(safe_name, candidate)

        if safe_name == GENERAL_PURPOSE_AGENT_TYPE:
            return AgentTypeDefinition(
                name=GENERAL_PURPOSE_AGENT_TYPE,
                description="General-purpose default agent (built-in).",
                model=None,
                effort=None,
                system_prompt=_GENERAL_PURPOSE_SYSTEM_PROMPT,
                source_path=None,
            )

        raise AgentTypeRegistryError(f"unknown agentType: {agent_type!r}", code="unknown_agent_type")

    @staticmethod
    def _load_definition(name: str, path: Path) -> AgentTypeDefinition:
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise AgentTypeRegistryError(
                f"failed to read agent type definition {name!r}: {type(exc).__name__}", code="agent_type_invalid"
            ) from exc
        try:
            frontmatter, body = _split_frontmatter(text)
        except ValueError as exc:
            raise AgentTypeRegistryError(
                f"malformed agent type definition {name!r}: {exc}", code="agent_type_invalid"
            ) from exc
        if frontmatter is None:
            raise AgentTypeRegistryError(
                f"agent type definition {name!r} is missing '---' frontmatter", code="agent_type_invalid"
            )
        fm_name = frontmatter.get("name")
        if not isinstance(fm_name, str) or not fm_name:
            raise AgentTypeRegistryError(
                f"agent type definition {name!r} frontmatter is missing a non-empty 'name'", code="agent_type_invalid"
            )
        if fm_name != name:
            # A definition claiming a different name than the one it resolves
            # as is a misconfiguration (e.g. a copied file) -- fail closed
            # rather than silently serving it under the filename-derived name.
            raise AgentTypeRegistryError(
                f"agent type definition {name!r} frontmatter 'name' does not match its filename",
                code="agent_type_invalid",
            )
        if not body.strip():
            raise AgentTypeRegistryError(
                f"agent type definition {name!r} has an empty system prompt body", code="agent_type_invalid"
            )
        return AgentTypeDefinition(
            name=name,
            description=frontmatter.get("description") or None,
            model=frontmatter.get("model") or None,
            effort=frontmatter.get("effort") or None,
            system_prompt=body,
            source_path=str(path),
        )
