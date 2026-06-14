"""Parent-owned subprocess workflow VM and RPC capability broker (issue #2).

The parent Hermes process never executes a generated workflow script. Instead it:

* statically validates the script as a hard launch gate
  (:func:`hermes_workflows.script_validator.validate_script`);
* spawns :mod:`hermes_workflows.vm_guest` as a subprocess with a **scrubbed
  environment** (no inherited Hermes/GitHub credentials) and a narrow stdio RPC
  channel;
* brokers every capability the script reaches for, validating each request
  against the method allow-list, known-agent registry, and configured
  budget/limits before any effect crosses the :class:`AgentRunner` boundary;
* journals each structured request with a stable call id;
* tolerates subprocess crashes/timeouts by marking the run failed without
  corrupting parent state.

All effects funnel through the injected :class:`~hermes_workflows.agents.AgentRunner`
(default: the deterministic :class:`StubAgentRunner`), so the VM is reproducible
and testable without a live Hermes. This module is pure stdlib.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from . import rpc
from .agents import AgentRunner, StubAgentRunner, is_known_agent, kanban_runner_id, is_kanban_runner_id
from .errors import CapabilityDenied, ScriptValidationError, WorkflowSubprocessError
from .registry import utc_now_iso
from .script_validator import validate_script

__all__ = [
    "VMLimits",
    "ScriptRunResult",
    "CapabilityBroker",
    "WorkflowVM",
    "run_script",
]

JournalSink = Callable[[dict[str, Any]], None]

# Capability methods the broker is willing to dispatch. Anything else is denied
# regardless of what the (untrusted) subprocess sends.
_ALLOWED_METHODS = frozenset({"agent", "kanban_agent", "log", "phase", "workflow"})

# Output-schema type table (mirrors runtime._TYPE_MAP) for brokered agent calls.
_TYPE_MAP: dict[str, tuple[type, ...]] = {
    "string": (str,), "str": (str,), "number": (int, float), "int": (int,),
    "integer": (int,), "float": (float,), "bool": (bool,), "boolean": (bool,),
    "object": (dict,), "dict": (dict,), "list": (list,), "array": (list,), "any": (object,),
}


@dataclass
class VMLimits:
    """Parent-enforced caps on what a single workflow run may do.

    These are the first slice of issue #11's governance surface: hard ceilings
    the parent applies no matter what the script attempts. Tighten per session
    or per launch approval.
    """

    max_rpc_calls: int = 1000
    max_agent_calls: int = 200
    max_kanban_calls: int = 100
    max_runtime_s: float = 30.0
    allow_nested_workflows: bool = False
    token_budget: Optional[int] = None


@dataclass
class ScriptRunResult:
    """Outcome of running a workflow script in the subprocess VM."""

    ok: bool
    value: Any = None
    error: Optional[dict[str, Any]] = None
    meta: Optional[dict[str, Any]] = None
    calls: list[dict[str, Any]] = field(default_factory=list)
    exit_code: Optional[int] = None
    stderr: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "value": self.value,
            "error": self.error,
            "meta": self.meta,
            "calls": self.calls,
            "exit_code": self.exit_code,
            "stderr": self.stderr,
        }


class CapabilityBroker:
    """Parent-owned validator/dispatcher for one run's RPC capability calls.

    The broker is the trust boundary: it assumes the subprocess is adversarial.
    Every :meth:`handle` call is checked against the method allow-list, the
    known-agent registry, and the configured budget/limits before any effect is
    produced. A denial is returned as a structured error ``ret`` frame (never an
    exception across the channel); a hard-cap breach also raises
    :attr:`should_abort` so the VM can terminate the subprocess.
    """

    def __init__(
        self,
        agent_runner: AgentRunner,
        limits: VMLimits,
        *,
        journal: Optional[JournalSink] = None,
        redact: bool = True,
    ) -> None:
        self._runner = agent_runner
        self._limits = limits
        self._journal = journal
        self._redact = redact
        self._rpc_calls = 0
        self._agent_calls = 0
        self._kanban_calls = 0
        self._tokens = 0
        self.should_abort = False

    # -- budget view piggybacked on every ret frame ------------------------
    def _budget_info(self) -> dict[str, Any]:
        total = self._limits.token_budget
        remaining = None if total is None else max(0, total - self._tokens)
        return {"total": total, "spent": self._tokens, "remaining": remaining}

    def _emit(self, event: dict[str, Any]) -> None:
        if self._journal is not None:
            self._journal({"ts": utc_now_iso(), **event})

    def handle(self, frame: dict[str, Any]) -> dict[str, Any]:
        """Validate and dispatch one ``call`` frame; return its ``ret`` frame."""
        call_id = frame.get("id")
        method = frame.get("method")
        params = frame.get("params") if isinstance(frame.get("params"), dict) else {}

        try:
            self._rpc_calls += 1
            if self._rpc_calls > self._limits.max_rpc_calls:
                self.should_abort = True
                raise CapabilityDenied(
                    f"max_rpc_calls ({self._limits.max_rpc_calls}) exceeded", code="limit_rpc"
                )
            if method not in _ALLOWED_METHODS:
                raise CapabilityDenied(f"method {method!r} is not allowed", code="unknown_method")

            value = self._dispatch(method, params)
            self._emit(self._call_event(call_id, method, params, ok=True))
            return {"t": rpc.T_RET, "id": call_id, "ok": True, "value": value, "budget": self._budget_info()}
        except CapabilityDenied as denied:
            self._emit(self._call_event(call_id, method, params, ok=False, error=denied.code))
            return {
                "t": rpc.T_RET, "id": call_id, "ok": False,
                "error": {"code": denied.code, "message": str(denied)}, "budget": self._budget_info(),
            }
        except KeyboardInterrupt:
            raise  # let a genuine operator interrupt propagate.
        except BaseException as exc:  # noqa: BLE001 — an AgentRunner (even one raising
            # SystemExit/CancelledError) must NOT escape and crash the parent run; it is
            # contained here and reported to the script as a structured error.
            self._emit(self._call_event(call_id, method, params, ok=False, error="runner_error"))
            return {
                "t": rpc.T_RET, "id": call_id, "ok": False,
                "error": {"code": "runner_error", "message": f"{type(exc).__name__}: {exc}"},
                "budget": self._budget_info(),
            }

    def _dispatch(self, method: str, params: dict[str, Any]) -> Any:
        if method == "log":
            return None  # journaling is handled by _emit; nothing to return.
        if method == "phase":
            return None
        if method == "agent":
            return self._handle_agent(params)
        if method == "kanban_agent":
            return self._handle_kanban(params)
        if method == "workflow":
            if not self._limits.allow_nested_workflows:
                raise CapabilityDenied("nested workflows are not permitted", code="nested_denied")
            raise CapabilityDenied("nested workflows are not implemented in this slice", code="nested_unsupported")
        raise CapabilityDenied(f"method {method!r} is not allowed", code="unknown_method")

    def _check_token_budget(self) -> None:
        """Hard ceiling: once the token budget is spent, deny further effects."""
        budget = self._limits.token_budget
        if budget is not None and self._tokens >= budget:
            self.should_abort = True
            raise CapabilityDenied(f"token_budget ({budget}) exhausted", code="limit_token")

    def _handle_agent(self, params: dict[str, Any]) -> dict[str, Any]:
        agent_id = params.get("agent_id")
        if not isinstance(agent_id, str) or not agent_id:
            raise CapabilityDenied("agent call requires a non-empty 'agent_id'", code="bad_request")
        self._check_token_budget()
        if is_kanban_runner_id(agent_id):
            raise CapabilityDenied(
                f"reserved kanban runner id {agent_id!r} must be reached via kanban_agent", code="reserved_agent"
            )
        if not is_known_agent(agent_id):
            raise CapabilityDenied(f"unknown agent id {agent_id!r}", code="unknown_agent")
        self._agent_calls += 1
        if self._agent_calls > self._limits.max_agent_calls:
            # Soft denial: the script may catch CapabilityError and adapt. The
            # max_rpc_calls hard cap is the runaway backstop that aborts the VM.
            raise CapabilityDenied(f"max_agent_calls ({self._limits.max_agent_calls}) exceeded", code="limit_agent")
        payload = params.get("input") if isinstance(params.get("input"), dict) else {}
        return self._invoke(agent_id, payload, params.get("schema"))

    def _handle_kanban(self, params: dict[str, Any]) -> dict[str, Any]:
        profile = params.get("profile")
        if not isinstance(profile, str) or not profile:
            raise CapabilityDenied("kanban_agent call requires a non-empty 'profile'", code="bad_request")
        self._check_token_budget()
        self._kanban_calls += 1
        if self._kanban_calls > self._limits.max_kanban_calls:
            # Soft denial (see _handle_agent): catchable; max_rpc_calls aborts.
            raise CapabilityDenied(f"max_kanban_calls ({self._limits.max_kanban_calls}) exceeded", code="limit_kanban")
        agent_id = kanban_runner_id(profile)
        payload = {
            "profile": profile,
            "task": params.get("task") or {},
            "input": params.get("input") if isinstance(params.get("input"), dict) else {},
            "wait": True,
            "durable": True,
        }
        return self._invoke(agent_id, payload, params.get("schema"))

    def _invoke(self, agent_id: str, payload: dict[str, Any], schema: Any) -> dict[str, Any]:
        output = self._runner(agent_id, payload)
        if not isinstance(output, dict):
            raise CapabilityDenied(
                f"agent {agent_id!r} returned {type(output).__name__}, expected dict", code="bad_output"
            )
        _validate_output(output, schema)
        usage = output.get("_tokens")
        if isinstance(usage, int) and not isinstance(usage, bool):
            self._tokens += usage
        return output

    def _call_event(
        self, call_id: Any, method: Any, params: dict[str, Any], *, ok: bool, error: Optional[str] = None
    ) -> dict[str, Any]:
        event: dict[str, Any] = {"type": "rpc_call", "call_id": call_id, "method": method, "ok": ok}
        if method in ("agent",):
            event["agent_id"] = params.get("agent_id")
        if method in ("kanban_agent",):
            event["profile"] = params.get("profile")
        if params.get("label"):
            event["label"] = params.get("label")
        if error:
            event["error"] = error
        if not self._redact:
            event["params"] = params
        return event


def _validate_output(output: dict[str, Any], schema: Any) -> None:
    """Validate brokered agent output against a flat ``field -> type`` schema."""
    if not isinstance(schema, dict) or not schema:
        return
    for field_name, hint in schema.items():
        if field_name not in output:
            raise CapabilityDenied(f"agent output missing declared field {field_name!r}", code="schema")
        expected = (hint,) if isinstance(hint, type) else _TYPE_MAP.get(str(hint).lower())
        if expected is None:
            continue
        value = output[field_name]
        if expected != (bool,) and isinstance(value, bool):
            raise CapabilityDenied(f"output field {field_name!r} expected {hint}, got bool", code="schema")
        if not isinstance(value, expected):
            raise CapabilityDenied(
                f"output field {field_name!r} expected {hint}, got {type(value).__name__}", code="schema"
            )


class WorkflowVM:
    """Launches and drives one workflow subprocess under a capability broker."""

    def __init__(
        self,
        *,
        agent_runner: Optional[AgentRunner] = None,
        limits: Optional[VMLimits] = None,
        journal: Optional[JournalSink] = None,
        python_executable: Optional[str] = None,
    ) -> None:
        self._runner = agent_runner if agent_runner is not None else StubAgentRunner()
        self._limits = limits if limits is not None else VMLimits()
        self._journal = journal
        self._python = python_executable or sys.executable

    def run(self, script: str, *, args: Any = None, validate: bool = True) -> ScriptRunResult:
        """Validate, launch, and drive a workflow script to completion.

        Raises :class:`ScriptValidationError` (before any subprocess is spawned)
        when ``validate`` is true and the script fails the launch gate. Any
        subprocess-level failure (crash, timeout, protocol breach) is returned as
        a failed :class:`ScriptRunResult`, never raised, so parent state stays
        intact.
        """
        if validate:
            result = validate_script(script)
            if not result.ok:
                raise ScriptValidationError(result.diagnostics)

        # Capture every journaled event locally (returned on the result) and
        # forward it to any externally-supplied sink.
        calls: list[dict[str, Any]] = []
        external_sink = self._journal

        def _collect(event: dict[str, Any]) -> None:
            calls.append(event)
            if external_sink is not None:
                external_sink(event)

        broker = CapabilityBroker(self._runner, self._limits, journal=_collect)
        try:
            return self._drive(script, args, broker, calls)
        except Exception as exc:  # noqa: BLE001 - keep unexpected VM bugs contained.
            return ScriptRunResult(
                ok=False,
                calls=calls,
                error={"type": "WorkflowSubprocessError", "message": f"Internal VM error: {exc}"},
            )

    # -- subprocess lifecycle ---------------------------------------------
    def _drive(
        self, script: str, args: Any, broker: CapabilityBroker, calls: list[dict[str, Any]]
    ) -> ScriptRunResult:
        cmd = [self._python, "-B", "-s", "-m", "hermes_workflows.vm_guest"]
        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=_scrubbed_env(),
                text=True,
                encoding="utf-8",
                bufsize=1,
            )
        except OSError as exc:
            return ScriptRunResult(ok=False, error={"type": "WorkflowSubprocessError", "message": str(exc)})

        stderr_chunks: list[str] = []
        stderr_thread = threading.Thread(target=_drain, args=(proc.stderr, stderr_chunks), daemon=True)
        stderr_thread.start()

        timed_out = threading.Event()

        def _on_timeout() -> None:
            timed_out.set()
            _kill(proc)

        timer = threading.Timer(self._limits.max_runtime_s, _on_timeout)
        timer.start()

        meta: Optional[dict[str, Any]] = None
        done: Optional[dict[str, Any]] = None
        protocol_error: Optional[str] = None

        try:
            assert proc.stdin is not None and proc.stdout is not None
            boot = {
                "t": rpc.T_BOOT,
                "script": script,
                "args": args,
                "limits": {
                    "max_rpc_calls": self._limits.max_rpc_calls,
                    "max_agent_calls": self._limits.max_agent_calls,
                    "max_runtime_s": self._limits.max_runtime_s,
                },
                "budget": {"total": self._limits.token_budget, "spent": 0,
                           "remaining": self._limits.token_budget},
            }
            booted = True
            try:
                rpc.write_frame(proc.stdin, boot)
            except (BrokenPipeError, OSError) as exc:
                protocol_error = f"child closed stdin before boot: {exc}"
                booted = False

            while booted:
                try:
                    frame = rpc.read_frame(proc.stdout)
                except rpc.RPCProtocolError as exc:
                    protocol_error = str(exc)
                    break
                if frame is None:
                    break  # EOF: child exited (cleanly after done, or crashed).
                kind = frame.get("t")
                if kind == rpc.T_READY:
                    meta = frame.get("meta")
                elif kind == rpc.T_CALL:
                    ret = broker.handle(frame)
                    try:
                        rpc.write_frame(proc.stdin, ret)
                    except (BrokenPipeError, OSError) as exc:
                        protocol_error = f"child stdin closed: {exc}"
                        break
                    if broker.should_abort:
                        _kill(proc)
                        protocol_error = "aborted: capability hard-limit exceeded"
                        break
                elif kind == rpc.T_DONE:
                    done = frame
                    break
                # Unknown frame types are ignored (forward-compatible).
        finally:
            timer.cancel()
            _close(proc.stdin)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _kill(proc)
            stderr_thread.join(timeout=5)

        stderr_text = "".join(stderr_chunks)[-4000:]
        exit_code = proc.returncode

        if timed_out.is_set():
            return ScriptRunResult(
                ok=False, meta=meta, calls=calls, exit_code=exit_code, stderr=stderr_text,
                error={"type": "WorkflowSubprocessError",
                       "message": f"workflow timed out after {self._limits.max_runtime_s}s"},
            )
        if protocol_error is not None:
            return ScriptRunResult(
                ok=False, meta=meta, calls=calls, exit_code=exit_code, stderr=stderr_text,
                error={"type": "WorkflowSubprocessError", "message": protocol_error},
            )
        if done is None:
            return ScriptRunResult(
                ok=False, meta=meta, calls=calls, exit_code=exit_code, stderr=stderr_text,
                error={"type": "WorkflowSubprocessError",
                       "message": f"subprocess exited without a result (code {exit_code})"},
            )
        if meta is None:
            meta = (done.get("error") or {}).get("meta") if isinstance(done.get("error"), dict) else None

        if done.get("ok"):
            return ScriptRunResult(ok=True, value=done.get("value"), meta=meta, calls=calls,
                                   exit_code=exit_code, stderr=stderr_text)
        return ScriptRunResult(ok=False, error=done.get("error"), meta=meta, calls=calls,
                               exit_code=exit_code, stderr=stderr_text)


# ---------------------------------------------------------------------------
# Subprocess helpers.
# ---------------------------------------------------------------------------

def _scrubbed_env() -> dict[str, str]:
    """Build a minimal environment with no inherited Hermes/GitHub credentials.

    Only what the interpreter needs to import the package and run UTF-8 cleanly
    is included. Nothing from the parent environment (tokens, Hermes config,
    cwd-derived paths) is passed through.
    """
    pkg_parent = str(Path(__file__).resolve().parents[1])  # dir that contains hermes_workflows/
    env = {
        "PYTHONPATH": pkg_parent,
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONUTF8": "1",
        "PYTHONIOENCODING": "utf-8",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        # Fixed hash seed so str/bytes set & dict iteration order is reproducible
        # across runs — the deterministic/replay direction depends on it.
        "PYTHONHASHSEED": "0",
    }
    return env


def _drain(stream: Any, sink: list[str]) -> None:
    if stream is None:
        return
    try:
        for chunk in stream:
            sink.append(chunk)
    except (ValueError, OSError):
        pass


def _kill(proc: subprocess.Popen) -> None:
    try:
        proc.kill()
    except (ProcessLookupError, OSError):
        pass


def _close(stream: Any) -> None:
    try:
        if stream is not None:
            stream.close()
    except (BrokenPipeError, OSError):
        pass


def run_script(
    script: str,
    *,
    args: Any = None,
    agent_runner: Optional[AgentRunner] = None,
    limits: Optional[VMLimits] = None,
    journal: Optional[JournalSink] = None,
    validate: bool = True,
) -> ScriptRunResult:
    """Convenience wrapper: construct a :class:`WorkflowVM` and run one script."""
    vm = WorkflowVM(agent_runner=agent_runner, limits=limits, journal=journal)
    return vm.run(script, args=args, validate=validate)
