"""Tests for the local background workflow-script run manager (issue #66)."""

from __future__ import annotations

import importlib.util
import json
import tempfile
import time
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

import hermes_workflows.background as background_module
from hermes_workflows.background import BackgroundRunStore, BackgroundWorkflowRunManager
from hermes_workflows.script_store import ScriptRunStore
from hermes_workflows.vm import ScriptRunResult

SCRIPT = (
    'meta = {"name": "background-demo", "description": "d"}\n'
    'r = await agent("hermes.echo", {"value": args["value"]})\n'
    'return {"answer": r["answer"]}\n'
)
PLUGIN_SCRIPT = (
    'meta = {"name": "background-plugin", "description": "d"}\n'
    'r = await agent("hermes.echo", {"value": args["value"]})\n'
    'return {"answer": r["echo"]["value"]}\n'
)


class _SlowRunner:
    def __init__(self, delay: float = 0.2) -> None:
        self.delay = delay
        self.calls = 0

    def __call__(self, agent_id: str, input: dict[str, Any]) -> dict[str, Any]:  # noqa: A002
        self.calls += 1
        time.sleep(self.delay)
        return {"answer": input["value"], "agent_id": agent_id}


def _eventually(fn: Callable[[], Any], predicate: Callable[[Any], bool], *, timeout: float = 3.0) -> Any:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = fn()
        if predicate(last):
            return last
        time.sleep(0.02)
    raise AssertionError(f"condition not reached; last={last!r}")


def test_background_launch_returns_before_slow_agent_completes_and_persists_result():
    with tempfile.TemporaryDirectory() as tmp:
        background_store = BackgroundRunStore(Path(tmp) / "background-runs")
        script_store = ScriptRunStore(Path(tmp) / "script-runs")
        manager = BackgroundWorkflowRunManager(background_store, script_store)
        runner = _SlowRunner(delay=0.25)

        started = time.monotonic()
        record = manager.launch_script(
            SCRIPT,
            args={"value": "ok"},
            run_id="wfs_background_slow",
            script_name="background-demo",
            agent_runner=runner,
        )
        elapsed = time.monotonic() - started

        assert elapsed < 0.15
        assert record.run_id == "wfs_background_slow"
        assert record.status in {"queued", "running"}
        assert background_store.get(record.run_id).status in {"queued", "running"}

        final = _eventually(
            lambda: background_store.get(record.run_id),
            lambda r: r is not None and r.status == "succeeded",
        )
        assert final.result == {"answer": "ok"}
        assert final.journal_path and Path(final.journal_path).exists()
        assert script_store.load_run(record.run_id).status == "succeeded"
        assert runner.calls == 1


def test_background_store_recovers_queued_and_running_snapshots_as_structured_failures():
    with tempfile.TemporaryDirectory() as tmp:
        store = BackgroundRunStore(Path(tmp) / "background-runs")
        queued = store.begin("wfs_queued", script=SCRIPT, args={"value": "q"})
        running = store.begin("wfs_running", script=SCRIPT, args={"value": "r"})
        store.mark_running(running.run_id)

        recovered = store.recover_incomplete(reason="test restart")

        assert {r.run_id for r in recovered} == {queued.run_id, running.run_id}
        for run_id in {queued.run_id, running.run_id}:
            record = store.get(run_id)
            assert record.status == "failed"
            assert record.error == {"type": "BackgroundRunInterrupted", "message": "test restart"}


def test_background_stop_wins_over_late_worker_completion():
    with tempfile.TemporaryDirectory() as tmp:
        background_store = BackgroundRunStore(Path(tmp) / "background-runs")
        script_store = ScriptRunStore(Path(tmp) / "script-runs")
        manager = BackgroundWorkflowRunManager(background_store, script_store)

        record = manager.launch_script(
            SCRIPT,
            args={"value": "late"},
            run_id="wfs_background_stop",
            agent_runner=_SlowRunner(delay=0.2),
        )
        stopped = manager.stop(record.run_id, reason="operator cancelled")
        assert stopped.status == "stopped"

        time.sleep(0.35)
        final = background_store.get(record.run_id)
        assert final.status == "stopped"
        assert final.error["type"] == "BackgroundRunStopped"


def test_background_finish_does_not_resurrect_stopped_run_when_stop_races_after_read(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        store = BackgroundRunStore(Path(tmp) / "background-runs")
        store.begin("wfs_finish_race", script=SCRIPT, args={"value": "late"})
        original_get = store.get
        injected_stop = False

        def racing_get(run_id: str):
            nonlocal injected_stop
            record = original_get(run_id)
            if not injected_stop:
                injected_stop = True
                store.stop(run_id, reason="operator cancelled during finish race")
            return record

        monkeypatch.setattr(store, "get", racing_get)

        returned = store.finish("wfs_finish_race", ScriptRunResult(ok=True, value={"answer": "late"}))

        final = original_get("wfs_finish_race")
        assert returned.status == "stopped"
        assert final.status == "stopped"
        assert final.result is None
        assert final.error == {
            "type": "BackgroundRunStopped",
            "message": "operator cancelled during finish race",
        }


def test_background_launch_thread_start_failure_cleans_thread_registry_and_marks_failed(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        background_store = BackgroundRunStore(Path(tmp) / "background-runs")
        script_store = ScriptRunStore(Path(tmp) / "script-runs")
        manager = BackgroundWorkflowRunManager(background_store, script_store)
        run_id = "wfs_start_failure"

        def fail_start(self):  # noqa: ANN001
            raise RuntimeError("qa injected thread start failure")

        monkeypatch.setattr(background_module.threading.Thread, "start", fail_start)
        with manager._threads_lock:
            manager._threads.pop(run_id, None)

        try:
            manager.launch_script(SCRIPT, args={"value": "never"}, run_id=run_id, agent_runner=_SlowRunner())
        except RuntimeError as exc:
            assert str(exc) == "qa injected thread start failure"
        else:  # pragma: no cover - defensive assertion clarity
            raise AssertionError("thread.start failure did not propagate")

        with manager._threads_lock:
            assert run_id not in manager._threads
        stored = background_store.get(run_id)
        assert stored.status == "failed"
        assert stored.error == {"type": "RuntimeError", "message": "qa injected thread start failure"}


class _FakeContext:
    def __init__(self) -> None:
        self.tools: dict[str, dict[str, Any]] = {}

    def register_tool(self, **kwargs: Any) -> None:
        self.tools[kwargs["name"]] = kwargs


def _load_plugin_root() -> ModuleType:
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("dynamic_workflows_plugin_bg", root / "__init__.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_plugin_background_run_is_visible_in_workflow_control_status_and_overview(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp)
        monkeypatch.setenv("HERMES_WORKFLOWS_STATE_DIR", str(state_dir / "runs"))
        monkeypatch.setenv("HERMES_WORKFLOWS_SCRIPT_CATALOG_DIR", str(state_dir / "scripts"))
        plugin = _load_plugin_root()
        ctx = _FakeContext()
        plugin.register(ctx)
        wf = ctx.tools["workflow"]["handler"]
        ctl = ctx.tools["workflow_control"]["handler"]

        saved = json.loads(wf({"action": "script_save", "script_name": "bgdemo", "script_source": PLUGIN_SCRIPT}))
        assert saved["success"] is True
        launched = json.loads(
            wf({
                "action": "run_script",
                "script_name": "bgdemo",
                "script_args": {"value": "plugin"},
                "execution_mode": "background",
                "run_id": "wfs_plugin_bg",
            })
        )
        assert launched["success"] is True
        assert launched["data"]["run_id"] == "wfs_plugin_bg"
        assert launched["data"]["execution_mode"] == "background"

        status = _eventually(
            lambda: json.loads(ctl({"action": "status", "run_id": "wfs_plugin_bg"})),
            lambda data: data["data"]["lifecycle"] == "succeeded",
        )
        assert status["data"]["result"] == {"answer": "plugin"}
        assert status["data"]["links"]["result"].endswith("run.json")

        overview = json.loads(ctl({"action": "overview"}))
        by_id = {r["run_id"]: r for r in overview["data"]["runs"]}
        assert by_id["wfs_plugin_bg"]["kind"] == "workflow_script"
        assert by_id["wfs_plugin_bg"]["status"] == "succeeded"
