"""Tests for the ``hermes_workflows.agents`` known-agent registry (#105).

``is_known_agent`` used to accept *any* id under the reserved ``hermes.``
namespace, so a typo like ``hermes.summarzier`` validated silently. These
tests pin down the tightened contract: exactly the fixed ``KNOWN_AGENTS``
roster plus ids an embedding host explicitly registers via
``register_known_agent`` -- no bare ``hermes.*`` wildcard fallback.

Stdlib only.
"""

from __future__ import annotations

from hermes_workflows import agents


def test_known_agents_roster_all_recognised():
    for agent_id in agents.KNOWN_AGENTS:
        assert agents.is_known_agent(agent_id)


def test_typo_d_hermes_namespace_id_is_not_known():
    """A near-miss of a real id under ``hermes.`` is rejected, not wildcarded in."""
    assert "hermes.summarizer" in agents.KNOWN_AGENTS
    assert agents.is_known_agent("hermes.summarzier") is False
    assert agents.is_known_agent("hermes.totally_made_up") is False


def test_kanban_runner_ids_are_excluded_from_known_agents():
    """``kanban.<profile>`` ids stay reserved for kanban_agent, not ordinary agent steps."""
    runner_id = agents.kanban_runner_id("relayqa")
    assert agents.is_kanban_runner_id(runner_id)
    assert agents.is_known_agent(runner_id) is False


def test_register_known_agent_extends_the_roster():
    new_id = "hermes.test_only.regression_agent_105"
    assert agents.is_known_agent(new_id) is False
    try:
        agents.register_known_agent(new_id)
        assert agents.is_known_agent(new_id) is True
        assert new_id in agents.registered_agent_ids()
        assert new_id not in agents.KNOWN_AGENTS
    finally:
        agents._REGISTERED_AGENTS.discard(new_id)
    assert agents.is_known_agent(new_id) is False


def test_register_known_agent_is_idempotent():
    new_id = "hermes.test_only.idempotent_agent_105"
    try:
        agents.register_known_agent(new_id)
        agents.register_known_agent(new_id)
        assert new_id in agents.registered_agent_ids()
    finally:
        agents._REGISTERED_AGENTS.discard(new_id)


def test_register_known_agent_rejects_empty_or_non_string():
    for bad in ("", None, 123):
        try:
            agents.register_known_agent(bad)  # type: ignore[arg-type]
        except ValueError:
            continue
        raise AssertionError(f"expected ValueError for {bad!r}")


def test_register_known_agent_rejects_reserved_kanban_ids():
    try:
        agents.register_known_agent("kanban.relayqa")
    except ValueError:
        return
    raise AssertionError("expected ValueError registering a reserved kanban.* id")


def test_registered_agent_ids_defaults_to_known_agents():
    assert agents.registered_agent_ids() >= agents.KNOWN_AGENTS
