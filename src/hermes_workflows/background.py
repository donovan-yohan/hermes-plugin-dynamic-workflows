"""Local background manager for durable workflow-script runs (issue #66).

The subprocess VM already persists one script run under ``ScriptRunStore`` once it
starts. This module owns the *launch* lifecycle around that VM: queued/running
status before the caller returns, a process-local worker thread that continues
after the tool call, and a compact durable snapshot that ``workflow_control`` can
inspect even if the launching chat turn is long gone.

This is intentionally local-only. It is not a distributed scheduler and it does
not claim to kill an already-spawned subprocess across process boundaries. Stop
requests are persisted fail-closed: queued work is skipped, and a running worker
will not overwrite a stopped snapshot when it later exits.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

from .agents import AgentRunner
from .capabilities import CapabilityPolicy, CapabilityRegistry
from .errors import ScriptValidationError
from .registry import utc_now_iso
from .script_store import ScriptRunStore, canonical_hash, script_run_id, script_sha256
from .script_validator import validate_script
from .vm import ScriptRunResult, VMLimits, run_script

__all__ = [
    "BACKGROUND_RUN_STATES",
    "BACKGROUND_TERMINAL_STATES",
    "BackgroundRunRecord",
    "BackgroundRunStore",
    "BackgroundWorkflowRunManager",
    "redact_failure",
]

BACKGROUND_RUN_STATES = frozenset({"queued", "running", "succeeded", "failed", "stopped", "suspended"})
BACKGROUND_TERMINAL_STATES = frozenset({"succeeded", "failed", "stopped", "suspended"})
_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_SECRET_KEY_RE = re.compile(r"(token|secret|password|credential|api[_-]?key|authorization)", re.I)
_SECRET_VALUE_RE = re.compile(
    r"(bearer\s+)[A-Za-z0-9._~+/=-]{12,}|"
    r"(token|secret|password|api[_-]?key)(\s*[=:]\s*)[^\s,;]{4,}",
    re.I,
)


def _require_safe_run_id(run_id: str) -> None:
    if not isinstance(run_id, str) or not _SAFE_SEGMENT_RE.fullmatch(run_id):
        raise ValueError(f"unsafe run_id: {run_id!r}")


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _redact_text(value: str) -> str:
    def repl(match: re.Match[str]) -> str:
        if match.group(1):
            return f"{match.group(1)}<redacted>"
        return f"{match.group(2)}{match.group(3)}<redacted>"

    redacted = _SECRET_VALUE_RE.sub(repl, value)
    return redacted[:1000]


def _redact_value(key: str, value: Any) -> Any:
    if _SECRET_KEY_RE.search(key):
        return "<redacted>"
    if isinstance(value, str):
        return _redact_text(value)
    if isinstance(value, dict):
        return {str(k): _redact_value(str(k), v) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_value("", item) for item in value[:50]]
    return value


def redact_failure(error: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Return a bounded, metadata-safe failure payload for background snapshots."""
    if error is None:
        return None
    if not isinstance(error, dict):
        return {"type": type(error).__name__, "message": _redact_text(str(error))}
    out: dict[str, Any] = {}
    for key in ("type", "code", "line", "message", "card_id", "profile", "on_block"):
        if key in error:
            out[key] = _redact_value(key, error[key])
    if not out:
        out["type"] = "WorkflowScriptError"
    return out


@dataclass
class BackgroundRunRecord:
    """Durable launch/lifecycle snapshot for one local background script run."""

    run_id: str
    status: str
    script_sha256: str
    args_hash: str
    script_name: Optional[str] = None
    script_version: Optional[int] = None
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)
    result: Any = None
    error: Optional[dict[str, Any]] = None
    journal_path: Optional[str] = None
    pid: Optional[int] = None
    stopped_reason: Optional[str] = None

    @property
    def kind(self) -> str:
        return "workflow_script"

    @property
    def steps(self) -> list[Any]:
        return []

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BackgroundRunRecord":
        if not isinstance(data, dict):
            raise ValueError("background run record must be an object")
        run_id = data.get("run_id")
        status = data.get("status")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("background run record requires run_id")
        if status not in BACKGROUND_RUN_STATES:
            raise ValueError(f"unknown background run status: {status!r}")
        return cls(
            run_id=run_id,
            status=status,
            script_sha256=str(data.get("script_sha256") or ""),
            args_hash=str(data.get("args_hash") or ""),
            script_name=data.get("script_name") if isinstance(data.get("script_name"), str) else None,
            script_version=data.get("script_version") if isinstance(data.get("script_version"), int) else None,
            created_at=str(data.get("created_at") or ""),
            updated_at=str(data.get("updated_at") or ""),
            result=data.get("result"),
            error=data.get("error") if isinstance(data.get("error"), dict) else None,
            journal_path=data.get("journal_path") if isinstance(data.get("journal_path"), str) else None,
            pid=data.get("pid") if isinstance(data.get("pid"), int) else None,
            stopped_reason=data.get("stopped_reason") if isinstance(data.get("stopped_reason"), str) else None,
        )


class BackgroundRunStore:
    """Filesystem-backed lifecycle store for local background script launches."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def begin(
        self,
        run_id: str,
        *,
        script: str,
        args: Any,
        script_name: Optional[str] = None,
        script_version: Optional[int] = None,
    ) -> BackgroundRunRecord:
        _require_safe_run_id(run_id)
        record = BackgroundRunRecord(
            run_id=run_id,
            status="queued",
            script_sha256=script_sha256(script),
            args_hash=canonical_hash(args),
            script_name=script_name,
            script_version=script_version,
            pid=os.getpid(),
        )
        with self._lock:
            run_dir = self._run_dir(run_id)
            try:
                run_dir.mkdir(parents=True, exist_ok=False)
            except FileExistsError as exc:
                raise ValueError(f"run_id already exists: {run_id!r}") from exc
            try:
                self._write_record(record)
                self._append_event(run_id, "queued", {"status": "queued", "script_name": script_name})
            except BaseException:
                shutil.rmtree(run_dir, ignore_errors=True)
                raise
        return record

    def get(self, run_id: str) -> Optional[BackgroundRunRecord]:
        _require_safe_run_id(run_id)
        with self._lock:
            return self._load_unlocked(run_id)

    def list(self) -> list[BackgroundRunRecord]:
        if not self.root.exists():
            return []
        records: list[BackgroundRunRecord] = []
        with self._lock:
            for path in sorted(self.root.glob("*/run.json")):
                try:
                    record = BackgroundRunRecord.from_dict(json.loads(path.read_text(encoding="utf-8")))
                except (OSError, ValueError, json.JSONDecodeError):
                    continue
                records.append(record)
        return records

    def journal(self, run_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        _require_safe_run_id(run_id)
        path = self._journal_path(run_id)
        try:
            lines = path.read_text(encoding="utf-8").splitlines()[-max(1, limit):]
        except FileNotFoundError:
            return []
        except OSError:
            return []
        events: list[dict[str, Any]] = []
        for raw in lines:
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append(event)
        return events

    def mark_running(self, run_id: str) -> BackgroundRunRecord:
        _require_safe_run_id(run_id)
        with self._lock:
            record = self._load_unlocked(run_id)
            if record is None:
                raise ValueError(f"background run not found: {run_id!r}")
            if record.status == "stopped":
                return record
        return self._transition(run_id, "running", event_type="running")

    def stop(self, run_id: str, *, reason: Optional[str] = None) -> BackgroundRunRecord:
        return self._transition(
            run_id,
            "stopped",
            error={"type": "BackgroundRunStopped", "message": reason or "stopped by operator"},
            stopped_reason=reason,
            event_type="stopped",
        )

    def finish(self, run_id: str, result: ScriptRunResult) -> BackgroundRunRecord:
        current = self.get(run_id)
        if current is not None and current.status == "stopped":
            # A stop request wins over a racing worker completion; do not resurrect
            # stopped work as succeeded/failed in the operator view.
            return current
        if result.suspended:
            status = "suspended"
        elif result.ok:
            status = "succeeded"
        else:
            status = "failed"
        return self._transition(
            run_id,
            status,
            result=result.value if result.ok else None,
            error=redact_failure(result.error),
            journal_path=result.journal_path,
            event_type="finished",
        )

    def fail_launch(self, run_id: str, exc: BaseException) -> BackgroundRunRecord:
        return self._transition(
            run_id,
            "failed",
            error={"type": type(exc).__name__, "message": _redact_text(str(exc))},
            event_type="failed",
        )

    def recover_incomplete(self, *, reason: str = "background manager restarted") -> list[BackgroundRunRecord]:
        """Mark queued/running snapshots as failed after a suspected process loss.

        The local manager cannot prove another OS process is still working without
        a supervisor, so this is explicit. Tests and operators can call it during
        startup recovery when they know no prior manager survived.
        """
        recovered: list[BackgroundRunRecord] = []
        for record in self.list():
            if record.status in {"queued", "running"}:
                recovered.append(
                    self._transition(
                        record.run_id,
                        "failed",
                        error={"type": "BackgroundRunInterrupted", "message": reason},
                        event_type="recovered_failed",
                    )
                )
        return recovered

    def _transition(
        self,
        run_id: str,
        status: str,
        *,
        result: Any = None,
        error: Optional[dict[str, Any]] = None,
        journal_path: Optional[str] = None,
        stopped_reason: Optional[str] = None,
        event_type: str,
    ) -> BackgroundRunRecord:
        if status not in BACKGROUND_RUN_STATES:
            raise ValueError(f"unknown background status: {status!r}")
        _require_safe_run_id(run_id)
        with self._lock:
            record = self._load_unlocked(run_id)
            if record is None:
                raise ValueError(f"background run not found: {run_id!r}")
            if status != "stopped" and record.status == "stopped":
                return record
            record.status = status
            record.updated_at = utc_now_iso()
            if result is not None or status in BACKGROUND_TERMINAL_STATES:
                record.result = result
            if error is not None or status in {"failed", "stopped", "suspended"}:
                record.error = redact_failure(error)
            if journal_path:
                record.journal_path = journal_path
            if stopped_reason is not None:
                record.stopped_reason = stopped_reason
            self._write_record(record)
            self._append_event(
                run_id,
                event_type,
                {"status": status, "has_result": record.result is not None, "error": record.error},
            )
            return record

    def _load_unlocked(self, run_id: str) -> Optional[BackgroundRunRecord]:
        try:
            raw = self._record_path(run_id).read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        data = json.loads(raw)
        return BackgroundRunRecord.from_dict(data)

    def _write_record(self, record: BackgroundRunRecord) -> None:
        run_dir = self._run_dir(record.run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(record.to_dict(), ensure_ascii=False, indent=2) + "\n"
        fd, tmp_name = tempfile.mkstemp(dir=str(run_dir), prefix="run.", suffix=".tmp")
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._record_path(record.run_id))
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        _fsync_dir(run_dir)

    def _append_event(self, run_id: str, event_type: str, data: dict[str, Any]) -> None:
        event = {"ts": utc_now_iso(), "type": event_type, "run_id": run_id, **data}
        with self._journal_path(run_id).open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _run_dir(self, run_id: str) -> Path:
        _require_safe_run_id(run_id)
        return self.root / run_id

    def _record_path(self, run_id: str) -> Path:
        return self._run_dir(run_id) / "run.json"

    def _journal_path(self, run_id: str) -> Path:
        return self._run_dir(run_id) / "journal.jsonl"

class BackgroundWorkflowRunManager:
    """Process-local launcher for durable background workflow-script runs."""

    _threads: dict[str, threading.Thread] = {}
    _threads_lock = threading.Lock()

    def __init__(self, store: BackgroundRunStore, script_store: ScriptRunStore) -> None:
        self.store = store
        self.script_store = script_store

    @classmethod
    def from_script_store(cls, script_store: ScriptRunStore) -> "BackgroundWorkflowRunManager":
        return cls(BackgroundRunStore(script_store.root.parent / "background-runs"), script_store)

    def launch_script(
        self,
        script: str,
        *,
        args: Any = None,
        script_name: Optional[str] = None,
        script_version: Optional[int] = None,
        agent_runner: Optional[AgentRunner] = None,
        limits: Optional[VMLimits] = None,
        validate: bool = True,
        run_id: Optional[str] = None,
        replay_from: Optional[str] = None,
        deterministic_runner: Optional[bool] = None,
        kanban_backend: Any = None,
        capability_registry: Optional[CapabilityRegistry] = None,
        capability_policy: Optional[CapabilityPolicy] = None,
    ) -> BackgroundRunRecord:
        if validate:
            validation = validate_script(script)
            if not validation.ok:
                raise ScriptValidationError(validation.diagnostics)
        rid = run_id if run_id is not None else script_run_id(script, args)
        record = self.store.begin(
            rid,
            script=script,
            args=args,
            script_name=script_name,
            script_version=script_version,
        )
        thread = threading.Thread(
            target=self._worker,
            name=f"workflow-bg-{rid[:24]}",
            daemon=True,
            kwargs={
                "run_id": rid,
                "script": script,
                "args": args,
                "agent_runner": agent_runner,
                "limits": limits,
                "replay_from": replay_from,
                "deterministic_runner": deterministic_runner,
                "kanban_backend": kanban_backend,
                "capability_registry": capability_registry,
                "capability_policy": capability_policy,
            },
        )
        with self._threads_lock:
            self._threads[rid] = thread
        try:
            thread.start()
        except BaseException as exc:
            with self._threads_lock:
                self._threads.pop(rid, None)
            try:
                self.store.fail_launch(rid, exc)
            except BaseException:
                pass
            raise
        # Give the worker a tiny scheduling window so callers usually see
        # ``running`` for trivial scripts, but never wait on user work.
        time.sleep(0)
        return self.store.get(rid) or record

    def stop(self, run_id: str, *, reason: Optional[str] = None) -> BackgroundRunRecord:
        return self.store.stop(run_id, reason=reason)

    def _worker(self, **kwargs: Any) -> None:
        run_id = kwargs["run_id"]
        try:
            current = self.store.get(run_id)
            if current is not None and current.status == "stopped":
                return
            running = self.store.mark_running(run_id)
            if running.status == "stopped":
                return
            result = run_script(
                kwargs["script"],
                args=kwargs["args"],
                agent_runner=kwargs["agent_runner"],
                limits=kwargs["limits"],
                validate=False,
                store=self.script_store,
                run_id=run_id,
                replay_from=kwargs["replay_from"],
                deterministic_runner=kwargs["deterministic_runner"],
                kanban_backend=kwargs["kanban_backend"],
                capability_registry=kwargs["capability_registry"],
                capability_policy=kwargs["capability_policy"],
            )
            self.store.finish(run_id, result)
        except BaseException as exc:  # defensive boundary: never let a worker vanish silently.
            try:
                self.store.fail_launch(run_id, exc)
            except BaseException:
                pass
        finally:
            with self._threads_lock:
                self._threads.pop(run_id, None)
