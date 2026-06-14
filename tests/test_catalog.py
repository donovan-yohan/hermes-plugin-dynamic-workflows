"""Tests for the saved workflow catalog."""

import json
import tempfile
from pathlib import Path

from hermes_workflows.catalog import FileWorkflowCatalog, safe_template_name
from hermes_workflows.primitives import workflow
from hermes_workflows.registry import InMemoryRunStore


def template_definition():
    return {
        "version": "1",
        "name": "catalog_hello",
        "inputs": {"name": "string"},
        "policy": {"network": False, "filesystem": False, "max_parallel": 1},
        "steps": [
            {
                "kind": "agent",
                "id": "greet",
                "agent": "hermes.greeter",
                "input": {"subject": "$ref:inputs.name"},
                "output_schema": {"greeting": "string"},
            }
        ],
    }


def write_template(root: Path, name="hello") -> Path:
    path = root / f"{name}.workflow.json"
    path.write_text(json.dumps(template_definition()), encoding="utf-8")
    return path


def test_file_workflow_catalog_lists_templates():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        write_template(root)
        catalog = FileWorkflowCatalog([root])

        templates = catalog.list_templates()

    assert [t["name"] for t in templates] == ["hello"]
    assert templates[0]["ok"] is True
    assert templates[0]["workflow_name"] == "catalog_hello"
    assert templates[0]["required_inputs"] == ["name"]
    assert templates[0]["def_hash"]


def test_file_workflow_catalog_loads_template():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        write_template(root)
        catalog = FileWorkflowCatalog([root])
        loaded = catalog.load_template("hello")

    assert loaded["name"] == "catalog_hello"


def test_workflow_facade_catalog_action_returns_templates():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        write_template(root)
        catalog = FileWorkflowCatalog([root])
        result = workflow(action="catalog", catalog=catalog)

    assert result["operation"] == "catalog"
    assert result["templates"][0]["name"] == "hello"


def test_workflow_facade_run_template_runs_loaded_definition():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        write_template(root)
        catalog = FileWorkflowCatalog([root])
        store = InMemoryRunStore()

        result = workflow(
            action="run_template",
            template_name="hello",
            inputs={"name": "catalog"},
            catalog=catalog,
            registry=store,
        )

    assert result["operation"] == "run_template"
    assert result["template_name"] == "hello"
    assert result["status"]["status"] == "succeeded"
    assert result["status"]["result"]["last"]["greeting"] == "hello, catalog"


def test_workflow_facade_run_template_defaults_from_template_name():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        write_template(root)
        catalog = FileWorkflowCatalog([root])
        result = workflow(
            template_name="hello",
            inputs={"name": "inferred"},
            catalog=catalog,
            registry=InMemoryRunStore(),
        )

    assert result["operation"] == "run_template"
    assert result["status"]["status"] == "succeeded"


def test_catalog_marks_invalid_workflow_not_ok():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        (root / "bad.workflow.json").write_text(json.dumps({"not": "a workflow"}), encoding="utf-8")
        catalog = FileWorkflowCatalog([root])

        templates = catalog.list_templates()

    assert templates[0]["name"] == "bad"
    assert templates[0]["ok"] is False
    assert templates[0]["validation_errors"]


def test_catalog_marks_policy_invalid_workflow_not_ok():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        definition = template_definition()
        definition["policy"]["network"] = True
        (root / "net.workflow.json").write_text(json.dumps(definition), encoding="utf-8")
        catalog = FileWorkflowCatalog([root])

        templates = catalog.list_templates()

    assert templates[0]["name"] == "net"
    assert templates[0]["ok"] is False
    assert any(e["code"] == "E_POLICY_NETWORK" for e in templates[0]["validation_errors"])


def test_catalog_listing_skips_symlink_escape():
    with tempfile.TemporaryDirectory() as root_dir, tempfile.TemporaryDirectory() as outside_dir:
        root = Path(root_dir)
        outside = Path(outside_dir)
        outside_template = outside / "leak.workflow.json"
        outside_template.write_text(json.dumps(template_definition()), encoding="utf-8")
        (root / "leak.workflow.json").symlink_to(outside_template)
        catalog = FileWorkflowCatalog([root])

        templates = catalog.list_templates()

    assert templates == []


def test_template_name_rejects_path_traversal():
    for bad in ("../hello", "sub/hello", "sub\\hello", "bad name", ".workflow.json"):
        try:
            safe_template_name(bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected unsafe template name to fail: {bad!r}")


def test_bundled_github_issue_lifecycle_hygiene_template_validates_and_runs():
    catalog = FileWorkflowCatalog()

    templates = {entry["name"]: entry for entry in catalog.list_templates()}
    assert templates["github_issue_lifecycle_hygiene"]["ok"] is True
    assert "issue_number" in templates["github_issue_lifecycle_hygiene"]["required_inputs"]

    result = workflow(
        template_name="github_issue_lifecycle_hygiene",
        inputs={
            "repo": "donovan-yohan/hermes-plugin-dynamic-workflows",
            "issue_number": 8,
            "base_branch": "main",
            "workspace": "/repo",
            "profile_bindings": {"planner": "relayplanner", "ops": "relayops"},
        },
        catalog=catalog,
        registry=InMemoryRunStore(),
    )

    assert result["operation"] == "run_template"
    assert result["status"]["status"] == "succeeded"
    outputs = result["status"]["result"]["outputs"]
    assert "inventory" in outputs
    assert "closeout_hygiene" in outputs
    closeout = outputs["closeout_hygiene"]
    assert closeout["profile"] == "ops"
    assert "issue hygiene" in str(closeout["result"]["echo"]).lower()
    assert "docs" in str(outputs["docs_gate"]["result"]["echo"]).lower()
