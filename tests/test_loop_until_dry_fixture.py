"""Archive-backed loop-until-dry parity fixture coverage (issue #77)."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from hermes_workflows import ChildAgentRequest, ScriptRunStore, VMLimits, run_workflow_script

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "loop_until_dry"
SCRIPT_PATH = FIXTURE_DIR / "loop_until_dry.workflow"
CART_PATH = FIXTURE_DIR / "cart.js"
RESPONSES_PATH = FIXTURE_DIR / "fake_child_responses.json"


class FixtureChildRunner:
    """Deterministic child-agent runner backed by sanitized fixture rows."""

    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses
        self.requests: list[ChildAgentRequest] = []

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        self.requests.append(request)
        label = request.label or ""
        phase = request.phase or ""
        if phase == "find":
            round_id = str(request.context.get("round"))
            area = str(request.context.get("area"))
            return dict(self.responses["finders"][round_id][area])
        if phase == "verify":
            bug_id = str(request.context.get("bug", {}).get("id"))
            verifier = str(request.context.get("verifier"))
            return dict(self.responses["verifiers"][bug_id][verifier])
        raise AssertionError(f"unexpected child request: {phase} {label}")


def _load_fixture() -> tuple[str, dict[str, Any], dict[str, Any]]:
    return (
        SCRIPT_PATH.read_text(encoding="utf-8"),
        {"cart_source": CART_PATH.read_text(encoding="utf-8")},
        json.loads(RESPONSES_PATH.read_text(encoding="utf-8")),
    )


def _run_fixture(*, run_id: str = "loop_fixture", replay_from: str | None = None, runner: FixtureChildRunner | None = None, args: dict[str, Any] | None = None, store: ScriptRunStore | None = None):
    source, base_args, responses = _load_fixture()
    merged_args = dict(base_args)
    if args:
        merged_args.update(args)
    if runner is None and replay_from is None:
        runner = FixtureChildRunner(responses)
    result = run_workflow_script(
        source,
        args=merged_args,
        store=store,
        run_id=run_id,
        replay_from=replay_from,
        child_agent_runner=runner,
        limits=VMLimits(max_parallel=3),
    )
    return result, runner


def test_loop_until_dry_runs_rounds_three_finders_dedup_verifiers_and_dry_counter():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        result, runner = _run_fixture(store=store)

        assert result.ok, result.error
        assert result.value == {
            "rounds": 2,
            "dry_count": 1,
            "max_round_reached": False,
            "verified_bug_ids": ["cart-state-reset", "cart-total-duplicate"],
            "candidate_bug_ids": ["cart-state-reset", "cart-total-duplicate"],
            "remaining_areas": [],
        }
        assert runner is not None
        finder_requests = [r for r in runner.requests if r.phase == "find"]
        verifier_requests = [r for r in runner.requests if r.phase == "verify"]
        assert [r.context["area"] for r in finder_requests[:3]] == ["totals", "state", "accessibility"]
        assert len(finder_requests[:3]) == 3
        # Round 1 returns one duplicate candidate; only two unique bug ids receive N verifier calls.
        assert sorted({r.context["bug"]["id"] for r in verifier_requests}) == ["cart-state-reset", "cart-total-duplicate"]
        assert len(verifier_requests) == 4
        assert sorted({r.context["verifier"] for r in verifier_requests}) == ["integration", "unit"]


def test_loop_until_dry_progress_projection_has_phases_and_all_child_rows():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        result, _runner = _run_fixture(store=store, run_id="progress")
        assert result.ok, result.error

        run_meta = store.load_run("progress")
        assert run_meta.phases == [
            {"title": "round find", "detail": "run concurrent finder child agents"},
            {"title": "verify", "detail": "verify deduplicated candidate bugs"},
            {"title": "decision", "detail": "update dry counter and continuation state"},
        ]
        rows = store.journal("progress")
        phase_titles = [row["phase_title"] for row in rows if row.get("method") == "phase"]
        assert phase_titles == ["round 1 find", "round 1 verify", "round 1 decision", "round 2 find", "round 2 decision"]
        child_rows = [row for row in rows if row["type"] in {"agent_started", "agent_result", "agent_cache_hit"}]
        labels = [row["label"] for row in child_rows if "label" in row]
        assert "finder:totals" in labels
        assert "finder:state" in labels
        assert "finder:accessibility" in labels
        assert "verifier:unit:cart-total-duplicate" in labels
        assert "verifier:integration:cart-state-reset" in labels
        assert len([row for row in child_rows if row["type"] == "agent_started"]) == 8
        first_round_parallel = [
            row.get("parallel_index")
            for row in rows
            if row.get("label") in {"finder:totals", "finder:state", "finder:accessibility"}
            and row["type"] == "call"
        ]
        assert sorted(set(first_round_parallel)) == [0, 1, 2]


def test_loop_until_dry_max_round_fallback_reports_remaining_work():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        result, _runner = _run_fixture(store=store, run_id="max_round", args={"max_rounds": 1})

        assert result.ok, result.error
        assert result.value["rounds"] == 1
        assert result.value["dry_count"] == 0
        assert result.value["max_round_reached"] is True
        assert result.value["remaining_areas"] == ["discounts"]


def test_loop_until_dry_resume_cache_skips_completed_prompt_option_fingerprints():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        recorded, runner = _run_fixture(store=store, run_id="recorded")
        assert recorded.ok, recorded.error
        assert runner is not None
        assert len(runner.requests) == 8

        replayed, replay_runner = _run_fixture(store=store, run_id="replayed", replay_from="recorded", runner=None)
        assert replayed.ok, replayed.error
        assert replayed.value == recorded.value
        assert replay_runner is None
        assert replayed.replayed_calls >= 8
        events = store.journal("replayed")
        cache_hits = [row for row in events if row["type"] == "agent_cache_hit"]
        assert len(cache_hits) == 8
        assert {row["cache"] for row in cache_hits} == {"replay"}
        assert not [row for row in events if row["type"] == "agent_started"]
