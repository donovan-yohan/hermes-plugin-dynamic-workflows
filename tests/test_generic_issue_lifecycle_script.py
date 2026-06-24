"""Tests for the generic GitHub issue lifecycle workflow-script example (#8)."""

from pathlib import Path

from hermes_workflows import run_workflow_script, validate_script, workflow_run_script

_SCRIPT = Path("examples/scripts/generic_issue_lifecycle/v000001.workflow.py")
_PACKAGED_SCRIPT = Path("src/hermes_workflows/examples/scripts/generic_issue_lifecycle/v000001.workflow")


def _args(**overrides):
    base = {
        "repo": "donovan-yohan/hermes-plugin-dynamic-workflows",
        "issue_number": 8,
        "base_branch": "main",
        "workspace": "/repo",
        "expected_head_sha": "abc123",
        "profile_bindings": {
            "planner": "plan-prof",
            "implementer": "impl-prof",
            "reviewer": "review-prof",
            "qa": "qa-prof",
            "fixer": "fix-prof",
            "ops": "ops-prof",
        },
    }
    base.update(overrides)
    return base


def test_generic_issue_lifecycle_script_validates_and_packaged_copy_matches():
    source = _SCRIPT.read_text(encoding="utf-8")
    validation = validate_script(source)
    assert validation.ok, [diag.as_dict() for diag in validation.diagnostics]
    assert validation.meta is not None
    assert validation.meta["name"] == "generic_issue_lifecycle_harness"
    assert _PACKAGED_SCRIPT.read_text(encoding="utf-8") == source


def test_generic_issue_lifecycle_script_runs_in_stub_mode_with_profile_bindings():
    source = _SCRIPT.read_text(encoding="utf-8")
    result = run_workflow_script(source, args=_args())

    assert result.ok, result.error
    assert result.value["repo"] == "donovan-yohan/hermes-plugin-dynamic-workflows"
    assert result.value["issue"] == 8
    assert result.value["profiles"]["planner"] == "plan-prof"
    assert result.value["profiles"]["qa"] == "qa-prof"
    assert result.value["head_sha"] == "abc123"
    assert result.value["review_ok"] is True
    assert result.value["qa_ok"] is True
    assert result.value["release"] is True
    assert result.value["blocked"] is False
    assert result.value["fix_attempted"] is False
    assert result.value["fix_attempts"] == 0
    assert str(result.value["closeout_task_id"]).startswith("kb_")

    kanban_profiles = [call.get("profile") for call in result.calls if call.get("method") == "kanban_agent"]
    assert kanban_profiles == ["plan-prof", "impl-prof", "review-prof", "qa-prof", "ops-prof"]


def test_generic_issue_lifecycle_runs_through_default_catalog_path():
    result = workflow_run_script("generic_issue_lifecycle", args=_args())

    assert result.ok, result.error
    assert result.value["issue"] == 8
    assert result.value["profiles"]["reviewer"] == "review-prof"
    assert result.value["release"] is True


class _GateRunner:
    def __init__(self, *, pass_after_attempt: int | None):
        self.pass_after_attempt = pass_after_attempt
        self.calls = []

    def __call__(self, agent_id, input):  # noqa: A002 - AgentRunner protocol name.
        self.calls.append((agent_id, input))
        if agent_id == "hermes.echo":
            return {"echo": dict(input), "digest": "inventory"}
        if agent_id == "hermes.github.pr_head":
            return {"head_sha": str(input.get("expected_head_sha") or "head0"), "head_ref": "branch"}
        if agent_id == "hermes.github.release_exact_head":
            review = input.get("review") if isinstance(input.get("review"), dict) else {}
            qa = input.get("qa") if isinstance(input.get("qa"), dict) else {}
            return {
                "release": bool(review.get("approved")) and bool(qa.get("approved")),
                "head_sha": str(input.get("expected_head_sha") or ""),
            }
        if agent_id.startswith("kanban."):
            profile = agent_id.split(".", 1)[1]
            task = input.get("task") if isinstance(input.get("task"), dict) else {}
            call_input = input.get("input") if isinstance(input.get("input"), dict) else {}
            if profile in {"review-prof", "qa-prof"}:
                attempt = call_input.get("attempt", 0)
                approved = self.pass_after_attempt is not None and attempt >= self.pass_after_attempt
                return {
                    "task_id": f"kb_{profile}_{attempt}",
                    "profile": profile,
                    "status": "succeeded",
                    "approved": approved,
                    "head_sha": str(call_input.get("head_sha") or task.get("expected_head_sha") or "head0"),
                    "blockers": [] if approved else [f"{profile} blocker"],
                }
            result = {"echo": dict(input), "digest": profile}
            if profile == "fix-prof":
                result["head_sha"] = f"fixed-{call_input.get('attempt', 0)}"
            return {"task_id": f"kb_{profile}", "profile": profile, "status": "succeeded", "result": result}
        return {"echo": dict(input), "digest": agent_id}


def test_failed_review_or_qa_suppresses_release_and_closeout():
    runner = _GateRunner(pass_after_attempt=None)
    result = run_workflow_script(
        _SCRIPT.read_text(encoding="utf-8"),
        args=_args(max_fix_attempts=0),
        agent_runner=runner,
    )

    assert result.ok, result.error
    assert result.value["review_ok"] is False
    assert result.value["qa_ok"] is False
    assert result.value["release"] is False
    assert result.value["blocked"] is True
    assert result.value["closeout_task_id"] is None
    assert not any(agent_id == "hermes.github.release_exact_head" for agent_id, _ in runner.calls)
    assert not any(agent_id == "kanban.ops-prof" for agent_id, _ in runner.calls)


def test_max_fix_attempts_really_loops_until_gates_pass():
    runner = _GateRunner(pass_after_attempt=2)
    result = run_workflow_script(
        _SCRIPT.read_text(encoding="utf-8"),
        args=_args(max_fix_attempts=2),
        agent_runner=runner,
    )

    assert result.ok, result.error
    assert result.value["review_ok"] is True
    assert result.value["qa_ok"] is True
    assert result.value["release"] is True
    assert result.value["blocked"] is False
    assert result.value["fix_attempted"] is True
    assert result.value["fix_attempts"] == 2
    assert [agent_id for agent_id, _ in runner.calls].count("kanban.fix-prof") == 2
    assert any(agent_id == "hermes.github.release_exact_head" for agent_id, _ in runner.calls)
    assert any(agent_id == "kanban.ops-prof" for agent_id, _ in runner.calls)
