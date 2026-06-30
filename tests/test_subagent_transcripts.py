"""Tests for per-subagent transcript artifacts (issue #76)."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from hermes_workflows import ChildAgentRequest, ScriptRunStore, run_workflow_script

META = 'meta = {"name": "transcripts", "description": "d"}\n'


class FakeChildRunner:
    def __init__(self, output: dict[str, Any] | None = None) -> None:
        self.output = output or {"answer": "ok", "_tokens": 11, "_tool_calls": 2}
        self.requests: list[ChildAgentRequest] = []

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        self.requests.append(request)
        return dict(self.output)


def _jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_prompt_subagent_persists_transcript_journal_file_and_metadata_refs():
    runner = FakeChildRunner()
    script = META + (
        'result = await agent("summarize secret ticket", {\n'
        '    "label": "summary",\n'
        '    "phase": "analysis",\n'
        '    "model": "sonnet",\n'
        '    "context": {"spawn_depth": 2, "token": "do-not-metadata"},\n'
        '    "schema": {"answer": "string"},\n'
        '})\n'
        'return {"answer": result["answer"]}\n'
    )
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            script,
            store=store,
            run_id="tx_run",
            child_agent_runner=runner,
            deterministic_runner=True,
        )

        assert res.ok, res.error
        run_dir = Path(tmp) / "runs" / "tx_run"
        transcript_dir = run_dir / "transcripts"
        journal_path = transcript_dir / "journal.jsonl"
        agent_path = transcript_dir / "agent-000001.jsonl"
        meta_path = transcript_dir / "agent-000001.meta.json"
        assert journal_path.exists()
        assert agent_path.exists()
        assert meta_path.exists()

        refs = res.as_dict()["transcripts"]
        assert refs == {
            "dir": str(transcript_dir),
            "journal_path": str(journal_path),
            "agents": [
                {
                    "id": "agent-000001",
                    "transcript_path": str(agent_path),
                    "meta_path": str(meta_path),
                    "state": "succeeded",
                    "label": "summary",
                    "phase": "analysis",
                }
            ],
        }
        assert store.load_run("tx_run").transcripts == refs

        ledger = _jsonl(journal_path)
        assert [event["event"] for event in ledger] == ["started", "result"]
        assert ledger[0]["agent_ref"] == "agent-000001"
        assert ledger[1]["state"] == "succeeded"

        transcript = _jsonl(agent_path)
        assert [event["event"] for event in transcript] == ["started", "result"]
        assert transcript[0]["agent_type"] == "prompt_agent"
        assert transcript[1]["result_keys"] == ["_tokens", "_tool_calls", "answer"]

        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        assert meta["agent_type"] == "prompt_agent"
        assert meta["label"] == "summary"
        assert meta["phase"] == "analysis"
        assert meta["model"] == "sonnet"
        assert meta["spawn_depth"] == 2
        assert meta["state"] == "succeeded"
        assert meta["token_count"] == 11
        assert meta["tool_count"] == 2
        assert isinstance(meta["started_at"], str) and meta["started_at"]
        assert isinstance(meta["completed_at"], str) and meta["completed_at"]
        assert isinstance(meta["duration_ms"], int) and meta["duration_ms"] >= 0

        metadata_text = meta_path.read_text(encoding="utf-8")
        assert "summarize secret ticket" not in metadata_text
        assert "do-not-metadata" not in metadata_text


def test_replayed_prompt_subagent_records_cache_hit_transcript_ref_without_child_runner():
    script = META + 'return await agent("summarize", {"label": "cached"})\n'
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        rec = run_workflow_script(
            script,
            store=store,
            run_id="source",
            child_agent_runner=FakeChildRunner({"answer": "cached", "_tokens": 5}),
            deterministic_runner=True,
        )
        assert rec.ok, rec.error

        replay = run_workflow_script(
            script,
            store=store,
            run_id="replay",
            replay_from="source",
            deterministic_runner=True,
        )
        assert replay.ok, replay.error
        assert replay.replayed_calls == 1

        transcript_dir = Path(tmp) / "runs" / "replay" / "transcripts"
        refs = replay.as_dict()["transcripts"]
        assert refs["journal_path"] == str(transcript_dir / "journal.jsonl")
        assert refs["agents"][0]["state"] == "cache_hit"

        ledger = _jsonl(transcript_dir / "journal.jsonl")
        assert [event["event"] for event in ledger] == ["cache-hit"]
        meta = json.loads((transcript_dir / "agent-000001.meta.json").read_text(encoding="utf-8"))
        assert meta["state"] == "cache_hit"
        assert meta["token_count"] == 5
        assert meta["label"] == "cached"
