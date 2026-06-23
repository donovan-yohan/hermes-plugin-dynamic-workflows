"""Tests for versioned saved workflow-script harness catalog (#29)."""

import tempfile
from pathlib import Path

import pytest

from hermes_workflows import (
    FileWorkflowScriptCatalog,
    ScriptValidationError,
    workflow,
    workflow_inspect_script,
    workflow_run_script,
    workflow_save_script,
    workflow_script_catalog,
)
from hermes_workflows.script_catalog import safe_script_name
from hermes_workflows.script_store import ScriptRunStore

META = 'meta = {"name": "issue_lifecycle", "description": "generic issue lifecycle harness"}\n'
HARNESS = META + (
    'log("inventory")\n'
    'plan = await agent("hermes.echo", {"repo": args["repo"], "issue": args["issue"], "phase": "plan"})\n'
    'qa = await kanban_agent(args["qa_profile"], {"goal": "qa", "issue": args["issue"]}, {"repo": args["repo"]})\n'
    'return {"planned": plan["echo"]["phase"], "qa_profile": qa["profile"]}\n'
)
UPDATED = META + 'return {"issue": args["issue"], "version": 2}\n'


def test_script_catalog_saves_versions_lists_latest_and_inspects_source():
    with tempfile.TemporaryDirectory() as d:
        catalog = FileWorkflowScriptCatalog([Path(d) / "scripts"])

        first = catalog.save_script("issue_lifecycle", HARNESS)
        second = catalog.save_script("issue_lifecycle", UPDATED)
        listed = catalog.list_scripts()
        all_versions = catalog.list_scripts(include_versions=True)
        inspected = catalog.inspect_script("issue_lifecycle", version=1, include_source=True)

    assert first["version"] == 1
    assert second["version"] == 2
    assert [entry["version"] for entry in listed] == [2]
    assert [entry["version"] for entry in all_versions] == [1, 2]
    assert inspected["source"] == HARNESS
    assert inspected["meta"]["description"] == "generic issue lifecycle harness"
    assert inspected["ok"] is True


def test_script_catalog_rejects_unsafe_names_and_invalid_scripts():
    with tempfile.TemporaryDirectory() as d:
        catalog = FileWorkflowScriptCatalog([Path(d) / "scripts"])
        for bad_name in ("../x", "nested/x", ".hidden", "bad name"):
            with pytest.raises(ValueError):
                safe_script_name(bad_name)
        with pytest.raises(ScriptValidationError):
            catalog.save_script("bad", META + "import os\n")


def test_script_catalog_versions_are_immutable_unless_replace_true():
    with tempfile.TemporaryDirectory() as d:
        catalog = FileWorkflowScriptCatalog([Path(d) / "scripts"])
        catalog.save_script("issue_lifecycle", HARNESS, version=3)
        with pytest.raises(FileExistsError):
            catalog.save_script("issue_lifecycle", UPDATED, version=3)

        replaced = catalog.save_script("issue_lifecycle", UPDATED, version=3, replace=True)

    assert replaced["version"] == 3
    assert replaced["script_sha256"]


def test_workflow_script_primitives_save_list_inspect_and_run_generic_harness():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        catalog = FileWorkflowScriptCatalog([root / "scripts"])
        store = ScriptRunStore(root / "runs")

        saved = workflow_save_script("issue_lifecycle", HARNESS, catalog=catalog)
        listing = workflow_script_catalog(catalog=catalog)
        inspected = workflow_inspect_script("issue_lifecycle", catalog=catalog)
        result = workflow_run_script(
            "issue_lifecycle",
            args={"repo": "owner/project", "issue": 29, "qa_profile": "qa"},
            catalog=catalog,
            store=store,
        )

        assert saved["operation"] == "script_save"
        assert listing["scripts"][0]["name"] == "issue_lifecycle"
        assert inspected["script"]["meta"]["name"] == "issue_lifecycle"
        assert result.ok is True
        assert result.value == {"planned": "plan", "qa_profile": "qa"}
        assert result.run_id and store.load_run(result.run_id).status == "succeeded"


def test_workflow_facade_script_actions_cover_saved_harness_lifecycle():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        catalog = FileWorkflowScriptCatalog([root / "scripts"])
        store = ScriptRunStore(root / "runs")

        saved = workflow(action="script_save", script_name="issue_lifecycle", script_source=HARNESS, script_catalog=catalog)
        listed = workflow(action="script_catalog", script_catalog=catalog, include_versions=True)
        inspected = workflow(action="script_inspect", script_name="issue_lifecycle", script_catalog=catalog, include_source=True)
        ran = workflow(
            action="run_script",
            script_name="issue_lifecycle",
            script_args={"repo": "another/repo", "issue": 29, "qa_profile": "reviewer"},
            script_catalog=catalog,
            script_store=store,
        )

    assert saved["script"]["version"] == 1
    assert listed["scripts"][0]["version"] == 1
    assert inspected["script"]["source"] == HARNESS
    assert ran["operation"] == "run_script"
    assert ran["result"]["ok"] is True
    assert ran["result"]["value"] == {"planned": "plan", "qa_profile": "reviewer"}


def test_bundled_generic_issue_lifecycle_script_harness_lists_and_runs():
    catalog = FileWorkflowScriptCatalog()
    scripts = {entry["name"]: entry for entry in catalog.list_scripts()}

    assert scripts["generic_issue_lifecycle"]["ok"] is True

    result = workflow_run_script(
        "generic_issue_lifecycle",
        args={
            "repo": "owner/project",
            "issue": 29,
            "workspace": "/repo",
            "review_profile": "reviewer",
            "qa_profile": "qa",
        },
        catalog=catalog,
    )

    assert result.ok is True
    assert result.value == {
        "planned_phase": "plan",
        "review_profile": "reviewer",
        "qa_profile": "qa",
        "issue": 29,
    }


def test_script_catalog_listing_skips_symlink_escape_and_save_refuses_escape():
    with tempfile.TemporaryDirectory() as root_dir, tempfile.TemporaryDirectory() as outside_dir:
        root = Path(root_dir)
        outside = Path(outside_dir)
        outside_script_dir = outside / "escape"
        outside_script_dir.mkdir()
        (outside_script_dir / "v000001.workflow.py").write_text(HARNESS, encoding="utf-8")
        try:
            (root / "escape").symlink_to(outside_script_dir, target_is_directory=True)
        except OSError:
            pytest.skip("symlinks are not supported or allowed on this platform")
        catalog = FileWorkflowScriptCatalog([root])

        assert catalog.list_scripts() == []
        assert catalog.list_versions("escape") == []
        with pytest.raises(ValueError):
            catalog.save_script("escape", HARNESS)


def test_script_catalog_uses_sidecar_metadata_but_revalidates_stale_or_corrupt_meta():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d) / "scripts"
        catalog = FileWorkflowScriptCatalog([root])
        catalog.save_script("issue_lifecycle", HARNESS)
        script_path = root / "issue_lifecycle" / "v000001.workflow.py"
        meta_path = root / "issue_lifecycle" / "v000001.meta.json"

        fast = catalog.inspect_script("issue_lifecycle")
        meta_path.write_text("not-json", encoding="utf-8")
        fallback = catalog.inspect_script("issue_lifecycle")
        script_path.write_text(META + "import os\n", encoding="utf-8")
        stale = catalog.inspect_script("issue_lifecycle")

    assert fast["ok"] is True
    assert fallback["ok"] is True
    assert stale["ok"] is False
    assert any(d["code"] == "E_SCRIPT_IMPORT" for d in stale["diagnostics"])


def test_script_catalog_listing_skips_symlinked_version_files_outside_root():
    with tempfile.TemporaryDirectory() as root_dir, tempfile.TemporaryDirectory() as outside_dir:
        root = Path(root_dir)
        script_dir = root / "escape"
        script_dir.mkdir()
        outside = Path(outside_dir) / "v000001.workflow.py"
        outside.write_text(HARNESS, encoding="utf-8")
        try:
            (script_dir / "v000001.workflow.py").symlink_to(outside)
        except OSError:
            pytest.skip("symlinks are not supported or allowed on this platform")
        catalog = FileWorkflowScriptCatalog([root])

        assert catalog.list_scripts() == []
        assert catalog.list_versions("escape") == []
        with pytest.raises(FileNotFoundError):
            catalog.load_script("escape")


def test_script_catalog_does_not_shadow_existing_version_across_roots_without_replace():
    with tempfile.TemporaryDirectory() as d:
        base = Path(d)
        profile = base / "profile"
        bundled = base / "bundled"
        bundled_catalog = FileWorkflowScriptCatalog([bundled])
        bundled_catalog.save_script("demo", HARNESS, version=1)

        catalog = FileWorkflowScriptCatalog([profile, bundled])
        with pytest.raises(FileExistsError):
            catalog.save_script("demo", UPDATED, version=1)

        appended = catalog.save_script("demo", UPDATED)
        bundled_v1 = catalog.inspect_script("demo", version=1)
        latest = catalog.inspect_script("demo")
        replaced = catalog.save_script("demo", UPDATED, version=1, replace=True)
        profile_v1 = catalog.inspect_script("demo", version=1)

    assert appended["version"] == 2
    assert bundled_v1["meta"]["description"] == "generic issue lifecycle harness"
    assert latest["version"] == 2
    assert replaced["version"] == 1
    assert profile_v1["script_sha256"] == replaced["script_sha256"]


def test_script_catalog_ignores_symlinked_metadata_outside_root():
    with tempfile.TemporaryDirectory() as root_dir, tempfile.TemporaryDirectory() as outside_dir:
        root = Path(root_dir)
        catalog = FileWorkflowScriptCatalog([root])
        saved = catalog.save_script("meta_escape", HARNESS)
        script_dir = root / "meta_escape"
        meta_path = script_dir / "v000001.meta.json"
        meta_path.unlink()
        outside_meta = Path(outside_dir) / "v000001.meta.json"
        outside_meta.write_text(
            '{"script_sha256": "%s", "meta": {"description": "outside poisoned meta"}}' % saved["script_sha256"],
            encoding="utf-8",
        )
        try:
            meta_path.symlink_to(outside_meta)
        except OSError:
            pytest.skip("symlinks are not supported or allowed on this platform")

        inspected = catalog.inspect_script("meta_escape")

    assert inspected["ok"] is True
    assert inspected["meta"]["description"] == "generic issue lifecycle harness"


def test_script_catalog_listing_skips_bad_source_entries_without_aborting():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        catalog = FileWorkflowScriptCatalog([root])
        catalog.save_script("good", HARNESS)
        bad_dir = root / "bad"
        bad_dir.mkdir()
        (bad_dir / "v000001.workflow.py").write_bytes(b"\xff\xfe\x00not-utf8")

        listed = catalog.list_scripts(include_versions=True)

    assert [entry["name"] for entry in listed] == ["good"]


def test_packaged_bundled_script_root_is_declared_as_package_data():
    pyproject = Path("pyproject.toml").read_text(encoding="utf-8")
    packaged = Path("src/hermes_workflows/examples/scripts/generic_issue_lifecycle/v000001.workflow")

    assert packaged.exists()
    assert "[tool.setuptools.package-data]" in pyproject
    assert "examples/scripts/*/*.workflow" in pyproject


def test_script_catalog_listing_falls_back_when_higher_priority_entry_is_unreadable():
    with tempfile.TemporaryDirectory() as d:
        base = Path(d)
        high = base / "high"
        low = base / "low"
        low_catalog = FileWorkflowScriptCatalog([low])
        low_catalog.save_script("demo", HARNESS)
        bad_dir = high / "demo"
        bad_dir.mkdir(parents=True)
        (bad_dir / "v000001.workflow.py").write_bytes(b"\xff\xfe\x00not-utf8")

        catalog = FileWorkflowScriptCatalog([high, low])
        listed = catalog.list_scripts()

    assert len(listed) == 1
    assert listed[0]["name"] == "demo"
    assert listed[0]["ok"] is True
