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

import hashlib
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "AgentRunner",
    "StubAgentRunner",
    "KNOWN_AGENTS",
    "is_known_agent",
    "kanban_runner_id",
    "is_kanban_runner_id",
]


# A small, fixed roster of agent ids the static linter recognises. A real
# deployment would resolve these against a Hermes service registry; for the
# skeleton it is a deterministic allow-list used by ``workflow_validate`` to
# emit ``E_UNKNOWN_AGENT`` and by :class:`StubAgentRunner` to shape stub output.
KNOWN_AGENTS: frozenset[str] = frozenset(
    {
        "hermes.greeter",
        "hermes.uppercaser",
        "hermes.echo",
        "hermes.summarizer",
        "hermes.classifier",
        "hermes.noop",
    }
)


def is_known_agent(agent_id: str) -> bool:
    """Return ``True`` if ``agent_id`` is a recognised Hermes agent id.

    Recognises the fixed :data:`KNOWN_AGENTS` roster plus any id under the
    reserved ``hermes.`` namespace, so example/stub workflows validate without
    hard-coding every possible agent. ``kanban.<profile>`` ids are intentionally
    excluded from ordinary agent steps; they are only produced by
    ``kanban_agent`` through :func:`kanban_runner_id`.
    """
    return agent_id in KNOWN_AGENTS or agent_id.startswith("hermes.")


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

        if agent_id.startswith("kanban."):
            profile = agent_id.split(".", 1)[1]
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
