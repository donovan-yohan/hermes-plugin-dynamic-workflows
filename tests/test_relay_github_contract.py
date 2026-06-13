"""Offline Relay/GitHub exact-head contract fixture tests."""

import json
from pathlib import Path

from hermes_workflows.catalog import FileWorkflowCatalog
from hermes_workflows.primitives import workflow, workflow_run, workflow_status, workflow_validate
from hermes_workflows.registry import InMemoryRunStore

FIXTURE = Path(__file__).resolve().parents[1] / "examples" / "relay_github_exact_head.workflow.json"
HEAD = "a8b60422108709fd2a0478cbdc95c7eda3a0c33f"
OLD_HEAD = "8bd09e0c90f2f2e6a90050155ed28da82aac73ae"


class ExactHeadRunner:
    def __init__(self, stale_qa=False):
        self.calls = []
        self.stale_qa = stale_qa

    def __call__(self, agent_id: str, input: dict):
        payload = input
        self.calls.append((agent_id, payload))
        if agent_id == "hermes.github.pr_head":
            return {"head_sha": HEAD, "head_ref": "feature/exact-head"}
        if agent_id == "kanban.relayqa":
            expected = payload["input"]["exact_head_sha"]
            assert payload["task"]["exact_head_sha"] == expected
            return {
                "task_id": "qa-1",
                "status": "passed",
                "result": {"head_sha": OLD_HEAD if self.stale_qa else expected, "approved": True},
            }
        if agent_id == "kanban.relayreviewer":
            expected = payload["input"]["exact_head_sha"]
            assert payload["task"]["exact_head_sha"] == expected
            return {"task_id": "review-1", "status": "approved", "result": {"head_sha": expected, "approved": True}}
        if agent_id == "hermes.github.release_exact_head":
            expected = payload["expected_head_sha"]
            qa = payload["qa"]
            review = payload["review"]
            release = (
                qa.get("approved") is True
                and review.get("approved") is True
                and qa.get("head_sha") == expected
                and review.get("head_sha") == expected
            )
            return {"release": release, "head_sha": expected}
        raise AssertionError(f"unexpected agent: {agent_id}")


def load_fixture():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_relay_github_exact_head_fixture_validates_strict():
    result = workflow_validate(load_fixture(), strict=True)
    assert result.ok, [d.as_dict() for d in result.errors]


def test_relay_github_exact_head_contract_passes_sha_unchanged():
    store = InMemoryRunStore()
    runner = ExactHeadRunner()
    handle = workflow_run(
        load_fixture(),
        inputs={"repo": "donovan-yohan/relay-ide", "pr_number": 946},
        registry=store,
        agent_runner=runner,
    )
    status = workflow_status(handle.run_id, registry=store)

    assert status.status == "succeeded"
    assert status.result["last"] == {"release": True, "head_sha": HEAD}
    release_call = runner.calls[-1]
    assert release_call[0] == "hermes.github.release_exact_head"
    assert release_call[1]["expected_head_sha"] == HEAD
    assert release_call[1]["qa"]["head_sha"] == HEAD
    assert release_call[1]["review"]["head_sha"] == HEAD


def test_relay_github_exact_head_contract_rejects_stale_qa_evidence():
    store = InMemoryRunStore()
    runner = ExactHeadRunner(stale_qa=True)
    handle = workflow_run(
        load_fixture(),
        inputs={"repo": "donovan-yohan/relay-ide", "pr_number": 946},
        registry=store,
        agent_runner=runner,
    )
    status = workflow_status(handle.run_id, registry=store)

    assert status.status == "succeeded"
    assert status.result["last"] == {"release": False, "head_sha": HEAD}


def test_relay_github_exact_head_template_runs_from_catalog():
    store = InMemoryRunStore()
    catalog = FileWorkflowCatalog([FIXTURE.parent])
    result = workflow(
        action="run_template",
        template_name="relay_github_exact_head",
        inputs={"repo": "donovan-yohan/relay-ide", "pr_number": 946},
        catalog=catalog,
        registry=store,
        agent_runner=ExactHeadRunner(),
    )

    assert result["operation"] == "run_template"
    assert result["status"]["result"]["last"] == {"release": True, "head_sha": HEAD}


def test_relay_github_exact_head_template_runs_with_default_stub_runner():
    store = InMemoryRunStore()
    catalog = FileWorkflowCatalog([FIXTURE.parent])
    result = workflow(
        action="run_template",
        template_name="relay_github_exact_head",
        inputs={"repo": "donovan-yohan/relay-ide", "pr_number": 946},
        catalog=catalog,
        registry=store,
    )

    assert result["status"]["status"] == "succeeded"
    assert result["status"]["result"]["last"] == {"release": True, "head_sha": "stub-head-sha"}
