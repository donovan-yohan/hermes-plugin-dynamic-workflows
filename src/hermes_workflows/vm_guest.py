"""In-subprocess guest harness for the workflow VM (issue #2).

This module is the entrypoint of the *sandboxed subprocess*. The parent launches
it with ``python -m hermes_workflows.vm_guest`` under a scrubbed environment with
no Hermes credentials. It:

1. reclaims the real stdout fd for the RPC channel and redirects ``sys.stdout``
   to ``sys.stderr`` so stray writes can never corrupt the protocol stream;
2. reads a single ``boot`` frame (script source, args, limits, budget) from
   stdin — the script is delivered over the channel, never read from disk;
3. *re-validates* the script with :func:`script_validator.validate_script`
   (defence in depth: the transport is never trusted);
4. compiles the script into a private async entrypoint and executes it with a
   restricted ``__builtins__`` and only RPC-backed capability globals;
5. routes every ``agent`` / ``kanban_agent`` / ``capability`` / ``log`` /
   ``phase`` / ``workflow`` call back to the parent broker and blocks for the
   structured response; ``agent_start`` / ``agent_check`` / ``agent_cancel`` /
   ``agent_list`` (issue #112) are the non-blocking counterpart -- each still
   round-trips to the parent, but ``agent_start`` returns immediately with a
   handle instead of waiting for the background run to finish;
6. reports the final return value (or the script's exception) in a ``done``
   frame and exits.

The guest module itself is trusted plugin code and may import freely; the
*workflow script* it executes gets none of that — only the safe globals below.
"""

from __future__ import annotations

import asyncio
import builtins as _builtins
import contextvars
import json as _json
import math as _math
import sys
import traceback
from typing import Any, Optional

from . import rpc
from .script_validator import ENTRY_NAME, validate_script, wrap_source

# Builtins the workflow script is allowed to see. Anything not listed simply
# does not exist inside the script (NameError), independent of the static
# validator — defence in depth against an escape the validator might miss.
_SAFE_BUILTIN_NAMES = (
    # constructors / containers
    "bool", "bytearray", "bytes", "complex", "dict", "float", "frozenset",
    "int", "list", "set", "slice", "str", "tuple",
    # functional / iteration
    "abs", "all", "any", "callable", "chr", "divmod", "enumerate", "filter",
    "format", "hash", "hex", "isinstance", "issubclass", "iter", "len", "map",
    "max", "min", "next", "oct", "ord", "pow", "range", "repr", "reversed",
    "round", "sorted", "sum", "zip",
    # constants
    "True", "False", "None", "NotImplemented", "Ellipsis",
    # common exception types (issue #4: scripts may use try/except)
    "BaseException", "Exception", "ArithmeticError", "AssertionError",
    "AttributeError", "IndexError", "KeyError", "LookupError", "NameError",
    "NotImplementedError", "OverflowError", "RecursionError", "RuntimeError",
    "StopAsyncIteration", "StopIteration", "TypeError", "ValueError",
    "ZeroDivisionError",
)


def _safe_builtins() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name in _SAFE_BUILTIN_NAMES:
        if hasattr(_builtins, name):
            out[name] = getattr(_builtins, name)
    return out


class _Proxy:
    """A flat, read-only attribute namespace exposing only whitelisted members.

    Handing the script a live ``json``/``math`` *module* object is an escape
    surface (a module can expose submodules or ``codecs.open``). A proxy exposes
    only the named callables/constants; each is a plain function or float whose
    sole introspection path is a dunder attribute, which the validator blocks.
    """

    def __init__(self, **members: Any) -> None:
        for key, value in members.items():
            object.__setattr__(self, key, value)

    def __setattr__(self, name: str, value: Any) -> None:  # immutable from script side.
        raise AttributeError("workflow helper namespaces are read-only")


def _json_proxy() -> _Proxy:
    return _Proxy(dumps=_json.dumps, loads=_json.loads, JSONDecodeError=_json.JSONDecodeError)


def _math_proxy() -> _Proxy:
    members = {name: getattr(_math, name) for name in dir(_math) if not name.startswith("_")}
    return _Proxy(**members)


class CapabilityError(RuntimeError):
    """Raised inside the script when the parent broker denies a capability call.

    Injected as a script global so a workflow may ``try/except CapabilityError``
    around brokered calls (e.g. to handle a budget/limit denial gracefully).

    ``retryable`` (issue #103) mirrors the parent's classification of *this
    specific call*: ``False`` for a contract violation (unknown agent id,
    schema/budget/limit exhaustion, a replay drift) where re-issuing the same
    call would fail identically, and ``True`` only when the parent's runner
    itself raised while dispatching (``code="runner_error"``) — a transient
    failure of one dispatch attempt a script may reasonably retry. Defaults to
    ``False`` so every existing catch site remains correct without inspecting
    the attribute.
    """

    def __init__(self, message: str, code: Optional[str] = None, retryable: bool = False) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(message)


class PipelineStageError(RuntimeError):
    """Raised when one item fails at a specific ``pipeline()`` stage."""

    def __init__(self, item_index: int, stage_index: int, exc: BaseException) -> None:
        self.item_index = item_index
        self.stage_index = stage_index
        self.cause_type = type(exc).__name__
        self.code = exc.code if isinstance(exc, CapabilityError) else None
        self.retryable = exc.retryable if isinstance(exc, CapabilityError) else False
        super().__init__(
            f"pipeline item {item_index} stage {stage_index} failed: {type(exc).__name__}: {exc}"
        )


class _Budget:
    """Read-only budget view exposed to the script, synced from the parent.

    The parent is authoritative; values arrive piggybacked on each ``ret`` frame
    and at boot. ``remaining()`` is ``math.inf`` when no total budget is set.
    """

    def __init__(self, info: Optional[dict[str, Any]]) -> None:
        info = info or {}
        self._total = info.get("total")
        self._spent = int(info.get("spent", 0) or 0)
        self._remaining = info.get("remaining", self._total)

    @property
    def total(self) -> Any:
        return self._total

    def spent(self) -> int:
        return self._spent

    def remaining(self) -> float:
        if self._total is None:
            return _math.inf
        return float(self._remaining if self._remaining is not None else 0)

    def _sync(self, info: Optional[dict[str, Any]]) -> None:
        if not info:
            return
        if "total" in info:
            self._total = info["total"]
        if "spent" in info:
            self._spent = int(info["spent"] or 0)
        if "remaining" in info:
            self._remaining = info["remaining"]


class _Connection:
    """Request/response RPC client to the parent broker."""

    def __init__(self, channel: rpc.Channel, budget: _Budget) -> None:
        self._channel = channel
        self._budget = budget
        self._next_id = 0
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._send_lock: Optional[asyncio.Lock] = None
        self._reader_task: Optional[asyncio.Task[None]] = None

    def call(self, method: str, params: dict[str, Any]) -> Any:
        """Send one capability call and block for its structured response."""
        if self._pending:
            raise CapabilityError(
                "synchronous workflow helper cannot run while parallel calls are pending",
                code="parallel_sync_call",
            )
        self._next_id += 1
        call_id = self._next_id
        self._channel.send({"t": rpc.T_CALL, "id": call_id, "method": method, "params": params})
        frame = self._channel.recv()
        if frame is None:
            raise CapabilityError("parent closed the channel", code="channel_closed")
        if frame.get("t") != rpc.T_RET or frame.get("id") != call_id:
            raise CapabilityError(
                f"protocol desync: expected ret#{call_id}, got {frame.get('t')}#{frame.get('id')}",
                code="protocol",
            )
        self._budget._sync(frame.get("budget"))
        if frame.get("ok"):
            return frame.get("value")
        error = frame.get("error") or {}
        raise CapabilityError(
            error.get("message", "capability denied"),
            code=error.get("code"),
            retryable=bool(error.get("retryable", False)),
        )

    async def acall(self, method: str, params: dict[str, Any]) -> Any:
        """Send one capability call without blocking sibling async tasks."""
        loop = asyncio.get_running_loop()
        if self._send_lock is None:
            self._send_lock = asyncio.Lock()
        async with self._send_lock:
            self._next_id += 1
            call_id = self._next_id
            fut: asyncio.Future[Any] = loop.create_future()
            self._pending[call_id] = fut
            self._channel.send({"t": rpc.T_CALL, "id": call_id, "method": method, "params": params})
            if self._reader_task is None or self._reader_task.done():
                self._reader_task = asyncio.create_task(self._read_returns())
        try:
            return await fut
        finally:
            if fut.cancelled():
                self._pending.pop(call_id, None)

    async def _read_returns(self) -> None:
        """Resolve outstanding async RPC calls from parent ``ret`` frames."""
        while self._pending:
            frame = await asyncio.to_thread(self._channel.recv)
            if frame is None:
                self._fail_pending(CapabilityError("parent closed the channel", code="channel_closed"))
                return
            if frame.get("t") != rpc.T_RET:
                self._fail_pending(
                    CapabilityError(f"protocol desync: expected ret, got {frame.get('t')}", code="protocol")
                )
                return
            call_id = frame.get("id")
            fut = self._pending.pop(call_id, None)
            if fut is None:
                continue
            self._budget._sync(frame.get("budget"))
            if frame.get("ok"):
                fut.set_result(frame.get("value"))
            else:
                error = frame.get("error") or {}
                fut.set_exception(
                    CapabilityError(
                        error.get("message", "capability denied"),
                        code=error.get("code"),
                        retryable=bool(error.get("retryable", False)),
                    )
                )
        self._reader_task = None

    def _fail_pending(self, exc: BaseException) -> None:
        pending = list(self._pending.values())
        self._pending.clear()
        for fut in pending:
            if not fut.done():
                fut.set_exception(exc)


def _looks_like_legacy_agent_id(value: Any) -> bool:
    return (
        isinstance(value, str)
        and (value.startswith("hermes.") or value.startswith("kanban."))
        and len(value.split(".", 1)[1]) > 0
        and not any(ch.isspace() for ch in value)
    )


def _build_script_globals(
    conn: _Connection,
    args: Any,
    budget: _Budget,
    meta: Any,
    limits: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Construct the restricted global namespace the script executes within."""

    limit_info = limits if isinstance(limits, dict) else {}
    max_parallel_raw = limit_info.get("max_parallel", 8)
    max_parallel = max_parallel_raw if isinstance(max_parallel_raw, int) and not isinstance(max_parallel_raw, bool) else 8
    max_parallel = max(1, max_parallel)
    parallel_index: contextvars.ContextVar[Optional[int]] = contextvars.ContextVar("parallel_index", default=None)
    pipeline_item_index: contextvars.ContextVar[Optional[int]] = contextvars.ContextVar(
        "pipeline_item_index", default=None
    )
    pipeline_stage_index: contextvars.ContextVar[Optional[int]] = contextvars.ContextVar(
        "pipeline_stage_index", default=None
    )

    def _annotate_call(params: dict[str, Any]) -> dict[str, Any]:
        p_index = parallel_index.get()
        item_index = pipeline_item_index.get()
        stage_index = pipeline_stage_index.get()
        if p_index is not None or item_index is not None or stage_index is not None:
            params = dict(params)
        if p_index is not None:
            params["_parallel_index"] = p_index
        if item_index is not None:
            params["_pipeline_item_index"] = item_index
        if stage_index is not None:
            params["_pipeline_stage_index"] = stage_index
        return params

    async def agent(target: str, input: Optional[dict[str, Any]] = None, *, label: Optional[str] = None,
                     phase: Optional[str] = None, schema: Optional[dict[str, Any]] = None,
                     model: Optional[str] = None, effort: Optional[str] = None,
                     isolation: Optional[str] = None, context: Optional[dict[str, Any]] = None,
                     tools: Optional[Any] = None, agentType: Optional[str] = None) -> Any:
        explicit_opts = {
            "label": label, "phase": phase, "schema": schema, "model": model,
            "effort": effort, "isolation": isolation, "context": context, "tools": tools,
            "agentType": agentType,
        }
        legacy_agent_id = _looks_like_legacy_agent_id(target)
        opts_from_pos = input if isinstance(input, dict) and not legacy_agent_id else None
        if opts_from_pos is not None or not legacy_agent_id:
            params: dict[str, Any] = {"prompt": target}
            if opts_from_pos is not None:
                params.update(opts_from_pos)
            elif input is not None:
                params["_invalid_opts_type"] = type(input).__name__
            params.update({key: value for key, value in explicit_opts.items() if value is not None})
            return await conn.acall("agent", _annotate_call(params))
        return await conn.acall(
            "agent",
            _annotate_call({"agent_id": target, "input": input or {}, "label": label, "schema": schema}),
        )

    async def kanban_agent(profile: str, task: Any = None, input: Optional[dict[str, Any]] = None, *,
                           title: Optional[str] = None, prompt: Optional[str] = None,
                           context: Optional[dict[str, Any]] = None, board: Optional[str] = None,
                           tenant: Optional[str] = None, parents: Any = None, labels: Any = None,
                           workspace: Optional[dict[str, Any]] = None, on_block: Optional[str] = None,
                           label: Optional[str] = None, schema: Optional[dict[str, Any]] = None) -> Any:
        # The parent broker (never this subprocess) turns these into a durable,
        # idempotent Kanban card and blocks until it resolves; ``on_block`` selects
        # pause/raise/return semantics for a blocked card. Only non-None extras are
        # forwarded so the call's replay args-hash stays stable for the common case.
        params: dict[str, Any] = {
            "profile": profile, "task": task or {}, "input": input or {},
            "label": label, "schema": schema,
        }
        extras = {
            "title": title, "prompt": prompt, "context": context, "board": board,
            "tenant": tenant, "parents": parents, "labels": labels,
            "workspace": workspace, "on_block": on_block,
        }
        params.update({key: value for key, value in extras.items() if value is not None})
        return await conn.acall("kanban_agent", _annotate_call(params))

    async def workflow(name: str, args: Any = None) -> Any:  # nested workflows (parent decides support)
        return await conn.acall("workflow", _annotate_call({"name": name, "args": args}))

    async def agent_start(target: str, input: Optional[dict[str, Any]] = None,
                           opts: Optional[dict[str, Any]] = None) -> Any:
        # Non-blocking counterpart to ``agent()`` (issue #112): the parent broker
        # starts a background child-agent run through the injected
        # AsyncChildAgentRunner and returns immediately with a deterministic
        # handle -- it never waits for the run to finish. Poll it later with
        # ``agent_check(handle)``.
        params: dict[str, Any] = {"target": target, "input": input or {}, "opts": opts or {}}
        return await conn.acall("agent_start", _annotate_call(params))

    async def agent_check(handle: Any) -> Any:
        # Non-blocking poll of a run started by ``agent_start``: returns
        # {"state": "pending"} immediately, or the terminal shape once resolved.
        # Polling a completed handle again (including after a durable replay)
        # returns the same resolved state rather than re-dispatching.
        return await conn.acall("agent_check", _annotate_call({"handle": handle}))

    async def agent_cancel(handle: Any) -> Any:
        # Request cancellation of a run started by ``agent_start``; returns the
        # resulting (acknowledged) state. Idempotent once the handle is terminal.
        return await conn.acall("agent_cancel", _annotate_call({"handle": handle}))

    async def agent_list() -> Any:
        # Every ``agent_start`` handle known to this run and its current state,
        # in the deterministic order the handles were started.
        return await conn.acall("agent_list", _annotate_call({}))

    async def capability(name: str, input: Optional[dict[str, Any]] = None, *, label: Optional[str] = None,
                         approval_id: Optional[str] = None, schema: Optional[dict[str, Any]] = None) -> Any:
        return await conn.acall(
            "capability",
            _annotate_call(
                {"name": name, "input": input or {}, "label": label, "approval_id": approval_id, "schema": schema}
            ),
        )

    def log(message: Any) -> None:
        conn.call("log", {"message": str(message)})

    def phase(title: Any) -> None:
        conn.call("phase", {"title": str(title)})

    async def parallel(thunks: Any) -> list[Any]:
        """Run thunks concurrently up to the configured bound, preserving order."""
        thunk_list = list(thunks)
        results: list[Any] = [None] * len(thunk_list)
        running: set[asyncio.Task[None]] = set()
        next_index = 0

        async def run_one(index: int, thunk: Any) -> None:
            token = parallel_index.set(index)
            try:
                results[index] = await _maybe_await(thunk())
            finally:
                parallel_index.reset(token)

        def start_ready() -> None:
            nonlocal next_index
            while next_index < len(thunk_list) and len(running) < max_parallel:
                running.add(asyncio.create_task(run_one(next_index, thunk_list[next_index])))
                next_index += 1

        start_ready()
        while running:
            done, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            try:
                for task in done:
                    await task
            except BaseException:
                for task in running:
                    task.cancel()
                await asyncio.gather(*running, return_exceptions=True)
                raise
            start_ready()
        return results

    async def pipeline(items: Any, *stages: Any) -> list[Any]:
        """Run bounded per-item stage chains without a cross-item stage barrier."""
        item_list = list(items)
        results: list[Any] = [None] * len(item_list)
        running: set[asyncio.Task[None]] = set()
        next_index = 0

        async def run_item(index: int, item: Any) -> None:
            current: Any = item
            item_token = pipeline_item_index.set(index)
            try:
                for stage_index, stage in enumerate(stages):
                    stage_token = pipeline_stage_index.set(stage_index)
                    try:
                        current = await _maybe_await(_call_stage(stage, current, item, index))
                    except BaseException as exc:
                        raise PipelineStageError(index, stage_index, exc) from exc
                    finally:
                        pipeline_stage_index.reset(stage_token)
                results[index] = current
            finally:
                pipeline_item_index.reset(item_token)

        def start_ready() -> None:
            nonlocal next_index
            while next_index < len(item_list) and len(running) < max_parallel:
                running.add(asyncio.create_task(run_item(next_index, item_list[next_index])))
                next_index += 1

        start_ready()
        while running:
            done, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            try:
                for task in done:
                    await task
            except BaseException:
                for task in running:
                    task.cancel()
                await asyncio.gather(*running, return_exceptions=True)
                raise
            start_ready()
        return results

    script_globals: dict[str, Any] = {
        "__builtins__": _safe_builtins(),
        "json": _json_proxy(),
        "math": _math_proxy(),
        "args": args,
        "budget": budget,
        "meta": meta,
        "agent": agent,
        "kanban_agent": kanban_agent,
        "capability": capability,
        "workflow": workflow,
        "agent_start": agent_start,
        "agent_check": agent_check,
        "agent_cancel": agent_cancel,
        "agent_list": agent_list,
        "log": log,
        "phase": phase,
        "parallel": parallel,
        "pipeline": pipeline,
        "CapabilityError": CapabilityError,
        "PipelineStageError": PipelineStageError,
    }
    return script_globals


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value):
        return await value
    return value


def _call_stage(stage: Any, prev: Any, item: Any, index: int) -> Any:
    """Call a pipeline stage with as many of (prev, item, index) as it accepts."""
    import inspect

    try:
        params = inspect.signature(stage).parameters
        arity = len([p for p in params.values()
                     if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)])
    except (TypeError, ValueError):
        arity = 1
    return stage(*(prev, item, index)[:max(arity, 1)])


def _error_payload(exc: BaseException) -> dict[str, Any]:
    """Build a metadata-only error payload (no full traceback to the parent)."""
    line = _script_line(exc)
    payload: dict[str, Any] = {"type": type(exc).__name__, "message": str(exc)}
    if isinstance(exc, CapabilityError):
        if exc.code:
            payload["code"] = exc.code
        payload["retryable"] = exc.retryable
    if isinstance(exc, PipelineStageError):
        payload["item_index"] = exc.item_index
        payload["stage_index"] = exc.stage_index
        payload["cause_type"] = exc.cause_type
        if exc.code:
            payload["code"] = exc.code
        payload["retryable"] = exc.retryable
    if line is not None:
        payload["line"] = line
    return payload


def _script_line(exc: BaseException) -> Optional[int]:
    """Best-effort original-source line of the deepest in-script frame."""
    tb = exc.__traceback__
    found: Optional[int] = None
    while tb is not None:
        if tb.tb_frame.f_code.co_filename == "<workflow-script>":
            found = max(tb.tb_lineno - 1, 0)  # undo the async wrapper offset.
        tb = tb.tb_next
    return found


def run(boot: dict[str, Any], channel: rpc.Channel) -> dict[str, Any]:
    """Validate, execute, and produce the ``done`` frame for one script.

    Returns the ``done`` frame dict (the caller sends it). Never raises for
    script-level problems; only a broken channel would propagate.
    """
    script = boot.get("script")
    validation = validate_script(script if isinstance(script, str) else "")
    if not validation.ok:
        return {
            "t": rpc.T_DONE,
            "ok": False,
            "error": {
                "type": "ScriptValidationError",
                "message": "script failed in-guest validation",
                "diagnostics": [d.as_dict() for d in validation.diagnostics],
            },
        }

    budget = _Budget(boot.get("budget"))
    conn = _Connection(channel, budget)
    script_globals = _build_script_globals(conn, boot.get("args"), budget, validation.meta, boot.get("limits"))

    channel.send({"t": rpc.T_READY, "meta": validation.meta})

    try:
        code = compile(wrap_source(script), "<workflow-script>", "exec")
        exec(code, script_globals)  # defines ENTRY_NAME in script_globals.
        entry = script_globals[ENTRY_NAME]
        value = asyncio.run(entry())
    except BaseException as exc:  # noqa: BLE001 — report any script failure structurally.
        return {"t": rpc.T_DONE, "ok": False, "error": _error_payload(exc)}
    return {"t": rpc.T_DONE, "ok": True, "value": _jsonable(value)}


def _jsonable(value: Any) -> Any:
    """Coerce the script's return value to a JSON-safe shape for the done frame.

    The fallback reports only the type *name* — never ``repr(value)``, whose
    output for functions/objects embeds a live heap address and would inject
    per-process non-determinism into the result the parent records.
    """
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    return {"_unserializable_type": type(value).__name__}


def main() -> int:
    # Reclaim stdout for the RPC channel; route everything else to stderr so a
    # stray print/traceback can never corrupt the protocol stream the parent
    # reads.
    rpc_out = sys.stdout
    sys.stdout = sys.stderr

    channel = rpc.Channel(reader=sys.stdin, writer=rpc_out)

    try:
        boot = channel.recv()
    except rpc.RPCProtocolError as exc:
        channel.send({"t": rpc.T_DONE, "ok": False, "error": {"type": "RPCProtocolError", "message": str(exc)}})
        return 1
    if boot is None or boot.get("t") != rpc.T_BOOT:
        channel.send({"t": rpc.T_DONE, "ok": False,
                      "error": {"type": "RPCProtocolError", "message": "expected boot frame"}})
        return 1

    try:
        done = run(boot, channel)
        channel.send(done)
        return 0 if done.get("ok") else 0  # a script failure is still a clean guest exit.
    except rpc.RPCProtocolError as exc:
        # Channel-level failure: try one best-effort report, then exit non-zero.
        try:
            channel.send({"t": rpc.T_DONE, "ok": False,
                          "error": {"type": "RPCProtocolError", "message": str(exc)}})
        except Exception:
            pass
        return 1
    except BaseException:  # noqa: BLE001
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
