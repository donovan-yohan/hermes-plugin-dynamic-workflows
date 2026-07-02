"""Tests for the file-based agent-type registry (issue #104)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from hermes_workflows import (
    AgentTypeRegistry,
    AgentTypeRegistryError,
    GENERAL_PURPOSE_AGENT_TYPE,
    safe_agent_type_name,
)


def _write(root: Path, name: str, text: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{name}.md").write_text(text, encoding="utf-8")


REVIEWER_DEFINITION = (
    "---\n"
    "name: reviewer\n"
    "description: Reviews code changes for correctness.\n"
    "model: opus\n"
    "effort: high\n"
    "---\n"
    "You are a meticulous code reviewer. Flag correctness bugs only.\n"
)


def test_resolve_general_purpose_builtin_default_with_no_roots():
    registry = AgentTypeRegistry()
    definition = registry.resolve(GENERAL_PURPOSE_AGENT_TYPE)
    assert definition.name == GENERAL_PURPOSE_AGENT_TYPE
    assert definition.model is None
    assert definition.effort is None
    assert definition.system_prompt
    assert definition.source_path is None


def test_resolve_reads_frontmatter_and_body_as_system_prompt():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "project-agents"
        _write(root, "reviewer", REVIEWER_DEFINITION)
        registry = AgentTypeRegistry(roots=[root])

        definition = registry.resolve("reviewer")
        assert definition.name == "reviewer"
        assert definition.description == "Reviews code changes for correctness."
        assert definition.model == "opus"
        assert definition.effort == "high"
        assert definition.system_prompt == "You are a meticulous code reviewer. Flag correctness bugs only."
        assert definition.source_path == str(root / "reviewer.md")


def test_resolve_project_scope_shadows_user_scope():
    with tempfile.TemporaryDirectory() as d:
        project_root = Path(d) / "project-agents"
        user_root = Path(d) / "user-agents"
        _write(
            project_root,
            "reviewer",
            "---\nname: reviewer\ndescription: project\nmodel: opus\n---\nProject reviewer prompt.\n",
        )
        _write(
            user_root,
            "reviewer",
            "---\nname: reviewer\ndescription: user\nmodel: sonnet\n---\nUser reviewer prompt.\n",
        )
        # Project root listed first -- project scope wins.
        registry = AgentTypeRegistry(roots=[project_root, user_root])
        definition = registry.resolve("reviewer")
        assert definition.model == "opus"
        assert definition.system_prompt == "Project reviewer prompt."

        # User-only definitions (no project shadow) still resolve.
        _write(user_root, "explainer", "---\nname: explainer\n---\nExplain things simply.\n")
        assert registry.resolve("explainer").system_prompt == "Explain things simply."


def test_resolve_unknown_agent_type_raises_deterministic_error():
    registry = AgentTypeRegistry()
    with pytest.raises(AgentTypeRegistryError) as exc_info:
        registry.resolve("nonexistent-type")
    assert exc_info.value.code == "unknown_agent_type"


def test_resolve_unknown_agent_type_with_roots_configured():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "agents"
        _write(root, "reviewer", REVIEWER_DEFINITION)
        registry = AgentTypeRegistry(roots=[root])
        with pytest.raises(AgentTypeRegistryError) as exc_info:
            registry.resolve("ghostwriter")
        assert exc_info.value.code == "unknown_agent_type"


@pytest.mark.parametrize("bad_name", ["../evil", "../../etc/passwd", "a/../../b", "sub/dir", "..", ""])
def test_resolve_path_traversal_and_unsafe_names_rejected(bad_name):
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "agents"
        _write(root, "reviewer", REVIEWER_DEFINITION)
        registry = AgentTypeRegistry(roots=[root])
        with pytest.raises(AgentTypeRegistryError) as exc_info:
            registry.resolve(bad_name)
        assert exc_info.value.code == "agent_type_invalid"


def test_safe_agent_type_name_reuses_script_catalog_hygiene():
    assert safe_agent_type_name("reviewer") == "reviewer"
    with pytest.raises(ValueError):
        safe_agent_type_name("../evil")
    with pytest.raises(ValueError):
        safe_agent_type_name("")


def test_safe_agent_type_name_rejects_suffixed_spellings():
    # agentType is a name, not a filename -- "reviewer", "reviewer.md", and
    # "reviewer.workflow.py" must not silently alias the same definition (that
    # aliasing would still mint distinct ChildAgentRequest fingerprints, since
    # the request stores the raw spelling).
    with pytest.raises(ValueError):
        safe_agent_type_name("reviewer.md")
    with pytest.raises(ValueError):
        safe_agent_type_name("reviewer.workflow.py")


def test_resolve_rejects_suffixed_agent_type_spelling():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "agents"
        _write(root, "reviewer", REVIEWER_DEFINITION)
        registry = AgentTypeRegistry(roots=[root])
        assert registry.resolve("reviewer").system_prompt.startswith("You are a meticulous")
        with pytest.raises(AgentTypeRegistryError) as exc_info:
            registry.resolve("reviewer.md")
        assert exc_info.value.code == "agent_type_invalid"


def test_resolve_malformed_frontmatter_missing_delimiters_rejected():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "agents"
        _write(root, "broken", "no frontmatter here, just a body\n")
        registry = AgentTypeRegistry(roots=[root])
        with pytest.raises(AgentTypeRegistryError) as exc_info:
            registry.resolve("broken")
        assert exc_info.value.code == "agent_type_invalid"


def test_resolve_malformed_frontmatter_missing_name_rejected():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "agents"
        _write(root, "broken", "---\ndescription: no name field\n---\nSome prompt.\n")
        registry = AgentTypeRegistry(roots=[root])
        with pytest.raises(AgentTypeRegistryError) as exc_info:
            registry.resolve("broken")
        assert exc_info.value.code == "agent_type_invalid"


def test_resolve_malformed_frontmatter_unparsable_line_rejected():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "agents"
        _write(root, "broken", "---\nname reviewer\n---\nSome prompt.\n")
        registry = AgentTypeRegistry(roots=[root])
        with pytest.raises(AgentTypeRegistryError) as exc_info:
            registry.resolve("broken")
        assert exc_info.value.code == "agent_type_invalid"


def test_resolve_malformed_frontmatter_unclosed_block_rejected():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "agents"
        _write(root, "broken", "---\nname: reviewer\nSome prompt with no closing delimiter.\n")
        registry = AgentTypeRegistry(roots=[root])
        with pytest.raises(AgentTypeRegistryError) as exc_info:
            registry.resolve("broken")
        assert exc_info.value.code == "agent_type_invalid"


def test_resolve_malformed_frontmatter_error_never_leaks_line_content():
    # A malformed line may contain arbitrary file content (secrets, tokens);
    # the script-visible diagnostic must carry only the line number and a
    # structural reason, never the offending text itself.
    secret = "SECRET_TOKEN=abc123-do-not-leak"
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "agents"
        _write(root, "broken", f"---\nname: reviewer\n{secret}\n---\nSome prompt.\n")
        registry = AgentTypeRegistry(roots=[root])
        with pytest.raises(AgentTypeRegistryError) as exc_info:
            registry.resolve("broken")
        message = str(exc_info.value)
        assert exc_info.value.code == "agent_type_invalid"
        assert secret not in message
        assert "abc123" not in message
        assert "line 3" in message


def test_resolve_empty_system_prompt_body_rejected():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "agents"
        _write(root, "broken", "---\nname: x\n---\n")
        registry = AgentTypeRegistry(roots=[root])
        with pytest.raises(AgentTypeRegistryError) as exc_info:
            registry.resolve("broken")
        assert exc_info.value.code == "agent_type_invalid"


def test_general_purpose_can_be_shadowed_by_a_project_definition():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "agents"
        _write(
            root,
            GENERAL_PURPOSE_AGENT_TYPE,
            "---\nname: general-purpose\ndescription: custom default\n---\nCustom general-purpose prompt.\n",
        )
        registry = AgentTypeRegistry(roots=[root])
        definition = registry.resolve(GENERAL_PURPOSE_AGENT_TYPE)
        assert definition.system_prompt == "Custom general-purpose prompt."
