"""Agent boundary for the workflow runtime.

Every external effect a workflow can produce is routed through a single
:class:`AgentRunner` callable. This is the one injection point where a live
Hermes deployment would be wired in; the skeleton ships a deterministic,
network-free :class:`StubAgentRunner` so runs and tests are reproducible.

The runtime treats an :class:`AgentRunner` as an opaque callable of the form::

    runner(agent_id: str, input: dict) -> dict

The returned ``dict`` is the structured agent output, optionally validated by
the runtime against a step's declared ``output_schema``.
"""

from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass, field
from typing import Any, Optional, Protocol, runtime_checkable

__all__ = [
    "AgentRunner",
    "ChildAgentRequest",
    "ChildAgentRunner",
    "CHILD_AGENT_OPTION_KEYS",
    "StubAgentRunner",
    "KNOWN_AGENTS",
    "is_known_agent",
    "register_known_agent",
    "registered_agent_ids",
    "kanban_runner_id",
    "is_kanban_runner_id",
]

CHILD_AGENT_OPTION_KEYS: frozenset[str] = frozenset(
    {"label", "phase", "schema", "model", "effort", "isolation", "context", "tools", "agentType"}
)


# A small, fixed roster of agent ids the static linter recognises. A real
# deployment would resolve these against a Hermes service registry; for the
# skeleton it is a deterministic allow-list used by ``workflow_validate`` to
# emit ``E_UNKNOWN_AGENT`` and by :class:`StubAgentRunner` to shape stub output.
#
# This roster intentionally covers every ``hermes.*`` id used by the bundled
# examples and tests (surveyed for #105). There is no bare ``hermes.*``
# wildcard fallback: an unregistered id -- including a typo of one of these,
# e.g. ``hermes.summarzier`` -- is rejected the same way any other unknown
# agent id is, matching the hard-rejection behaviour the rest of the runtime
# already enforces (unknown workflow capabilities, disallowed builtins, ...).
KNOWN_AGENTS: frozenset[str] = frozenset(
    {
        "hermes.greeter",
        "hermes.uppercaser",
        "hermes.echo",
        "hermes.summarizer",
        "hermes.classifier",
        "hermes.noop",
        "hermes.bughunter",
        "hermes.github.pr_head",
        "hermes.github.release_exact_head",
        "hermes.github.pr_event_context",
        "hermes.github.pr_validation_summary",
        "hermes.github.issue_inventory",
    }
)

# Ids an embedding host has registered at runtime via :func:`register_known_agent`,
# in addition to the fixed :data:`KNOWN_AGENTS` roster above. Kept process-local
# and additive: a real Hermes deployment resolves its own extended agent roster
# once at startup, so there is no corresponding "unregister".
_REGISTERED_AGENTS: set[str] = set()


def register_known_agent(agent_id: str) -> None:
    """Register ``agent_id`` as known, extending the fixed :data:`KNOWN_AGENTS` roster.

    This is the explicit opt-in hook an embedding host uses to teach
    ``workflow_validate`` and the runtime VM about agent ids beyond the bundled
    skeleton roster -- there is no ``hermes.*`` wildcard fallback, so ids that
    are not in :data:`KNOWN_AGENTS` must be registered here before scripts that
    reference them will validate or run. Registration is idempotent and
    additive for the lifetime of the process. ``kanban.<profile>`` ids are
    reserved for :func:`kanban_runner_id` and may not be registered here.
    """
    if not isinstance(agent_id, str) or not agent_id:
        raise ValueError("agent_id must be a non-empty string")
    if is_kanban_runner_id(agent_id):
        raise ValueError(f"{agent_id!r} is a reserved kanban runner id and cannot be registered as an agent")
    _REGISTERED_AGENTS.add(agent_id)


def registered_agent_ids() -> frozenset[str]:
    """Return the effective known-agent roster: :data:`KNOWN_AGENTS` plus host-registered ids."""
    return KNOWN_AGENTS | frozenset(_REGISTERED_AGENTS)


def is_known_agent(agent_id: str) -> bool:
    """Return ``True`` if ``agent_id`` is a recognised Hermes agent id.

    Recognises exactly the fixed :data:`KNOWN_AGENTS` roster plus any id an
    embedding host has explicitly added via :func:`register_known_agent`.
    There is no bare ``hermes.*`` wildcard fallback: an unrecognised id --
    including a typo under the ``hermes.`` namespace -- fails validation with
    ``E_UNKNOWN_AGENT`` (error severity) instead of validating silently.
    ``kanban.<profile>`` ids are intentionally excluded from ordinary agent
    steps; they are only produced by ``kanban_agent`` through
    :func:`kanban_runner_id`.
    """
    return agent_id in KNOWN_AGENTS or agent_id in _REGISTERED_AGENTS


def kanban_runner_id(profile: str) -> str:
    """Return the reserved runner id for a Kanban-backed profile."""
    return f"kanban.{profile}"


def is_kanban_runner_id(agent_id: str) -> bool:
    """Return ``True`` for reserved Kanban runner ids."""
    return agent_id.startswith("kanban.")


@runtime_checkable
class AgentRunner(Protocol):
    """Protocol for the injected fan-out callable to Hermes agents."""

    def __call__(self, agent_id: str, input: dict[str, Any]) -> dict[str, Any]:
        """Invoke ``agent_id`` with ``input`` and return structured output."""
        ...


@dataclass(frozen=True)
class ChildAgentRequest:
    """Prompt-shaped request handed to a host-owned workflow child-agent runner.

    The subprocess guest never receives parent chat history or credentials. It can
    only send the natural-language ``prompt`` plus options explicitly provided in
    the script. The parent normalizes that into this JSON-friendly contract before
    crossing the injected runner boundary.
    """

    prompt: str
    label: Optional[str] = None
    phase: Optional[str] = None
    schema: Optional[dict[str, Any]] = None
    model: Optional[str] = None
    effort: Optional[str] = None
    isolation: Optional[str] = None
    context: dict[str, Any] = field(default_factory=dict)
    tools: Optional[tuple[str, ...]] = None
    # Named subagent type selector (issue #92), resolved against a file-based
    # registry by the broker (issue #104) -- see
    # :mod:`hermes_workflows.agent_type_registry`. ``None`` means the script
    # never set ``agentType`` explicitly; the broker still resolves a system
    # prompt/defaults for the dispatch (the built-in ``general-purpose``
    # type), it just does not stamp this field or the replay fingerprint.
    agent_type: Optional[str] = None
    # Broker-resolved system prompt for the request's effective agent type
    # (explicit ``agentType`` or the built-in ``general-purpose`` default).
    # Never script-settable directly -- there is no matching opts key -- and
    # never part of the replay fingerprint (see ``_prompt_agent_fingerprint_payload``
    # in :mod:`hermes_workflows.vm`): the agent-type *name* already
    # identifies the call; the resolved prompt text is a deterministic
    # function of it for a given registry.
    system_prompt: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        """Return the runner contract as a plain JSON-friendly dict."""
        return {
            "prompt": self.prompt,
            "label": self.label,
            "phase": self.phase,
            "schema": copy.deepcopy(self.schema),
            "model": self.model,
            "effort": self.effort,
            "isolation": self.isolation,
            "context": copy.deepcopy(self.context),
            "tools": list(self.tools) if self.tools is not None else None,
            "agent_type": self.agent_type,
            "system_prompt": self.system_prompt,
        }


@runtime_checkable
class ChildAgentRunner(Protocol):
    """Protocol for host-owned prompt subagents used by script ``agent(prompt, opts)``."""

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        """Run one isolated child agent and return its final structured result."""
        ...


class StubAgentRunner:

    """Deterministic, network-free default :class:`AgentRunner`.

    Produces stub structured output derived only from its inputs, so identical
    ``(agent_id, input)`` calls always yield identical results. This keeps the
    skeleton runnable and every run reproducible without a live Hermes.

    A few well-known agent ids get bespoke shapes (e.g. ``hermes.greeter`` ->
    ``{"greeting": ...}``); everything else echoes the input under ``echo`` plus
    a stable ``digest`` so callers can still assert on something concrete.
    """

    def __call__(self, agent_id: str, input: dict[str, Any]) -> dict[str, Any]:
        """Return deterministic stub output for ``agent_id`` given ``input``."""
        digest = self._digest(agent_id, input)

        if agent_id == "hermes.greeter":
            subject = str(input.get("subject", "world"))
            return {"greeting": f"hello, {subject}"}

        if agent_id == "hermes.uppercaser":
            text = str(input.get("text", ""))
            return {"result": text.upper()}

        if agent_id == "hermes.summarizer":
            text = str(input.get("text", ""))
            return {"summary": text[:64]}

        if agent_id == "hermes.classifier":
            return {"label": "stub", "score": 1.0}

        if agent_id == "hermes.noop":
            return {}

        if agent_id == "hermes.github.pr_head":
            return {
                "head_sha": str(input.get("head_sha") or input.get("expected_head_sha") or "stub-head-sha"),
                "head_ref": str(input.get("head_ref") or "stub-head"),
            }

        if agent_id == "hermes.github.release_exact_head":
            expected = str(input.get("expected_head_sha") or "")
            qa_value = input.get("qa")
            review_value = input.get("review")
            qa = qa_value if isinstance(qa_value, dict) else {}
            review = review_value if isinstance(review_value, dict) else {}
            release = (
                bool(qa.get("approved", True))
                and bool(review.get("approved", True))
                and qa.get("head_sha", expected) == expected
                and review.get("head_sha", expected) == expected
            )
            return {"release": release, "head_sha": expected}

        if agent_id.startswith("kanban."):
            profile = agent_id.split(".", 1)[1]
            task = input.get("task") if isinstance(input.get("task"), dict) else {}
            call_input = input.get("input") if isinstance(input.get("input"), dict) else {}
            contract = task.get("return_contract") if isinstance(task, dict) else None
            if isinstance(contract, dict) and {"approved", "head_sha"}.issubset(contract):
                expected = task.get("expected_head_sha") if isinstance(task, dict) else None
                head_value = call_input.get("head_sha") if isinstance(call_input, dict) else None
                head_sha = str(head_value or expected or "stub-head-sha")
                return {
                    "task_id": f"kb_{digest}",
                    "profile": profile,
                    "status": "succeeded",
                    "approved": True,
                    "head_sha": head_sha,
                    "blockers": [],
                }
            return {
                "task_id": f"kb_{digest}",
                "profile": profile,
                "status": "succeeded",
                "result": {"echo": dict(input), "digest": digest},
            }

        # Generic echo agent (covers "hermes.echo" and any other known id).
        return {"echo": dict(input), "digest": digest}

    @staticmethod
    def _digest(agent_id: str, input: dict[str, Any]) -> str:
        """Stable short hash of an agent call, used in generic stub output."""
        import json

        payload = json.dumps(
            {"agent": agent_id, "input": input},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]
