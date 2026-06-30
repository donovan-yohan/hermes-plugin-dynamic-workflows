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
   structured response;
6. reports the final return value (or the script's exception) in a ``done``
   frame and exits.

The guest module itself is trusted plugin code and may import freely; the
*workflow script* it executes gets none of that — only the safe globals below.
"""

from __future__ import annotations

import asyncio
import builtins as _builtins
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
    """

    def __init__(self, message: str, code: Optional[str] = None) -> None:
        self.code = code
        super().__init__(message)


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
    """Synchronous request/response RPC client to the parent broker."""

    def __init__(self, channel: rpc.Channel, budget: _Budget) -> None:
        self._channel = channel
        self._budget = budget
        self._next_id = 0

    def call(self, method: str, params: dict[str, Any]) -> Any:
        """Send one capability call and block for its structured response."""
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
        raise CapabilityError(error.get("message", "capability denied"), code=error.get("code"))


def _looks_like_legacy_agent_id(value: Any) -> bool:
    return (
        isinstance(value, str)
        and (value.startswith("hermes.") or value.startswith("kanban."))
        and len(value.split(".", 1)[1]) > 0
        and not any(ch.isspace() for ch in value)
    )


def _build_script_globals(conn: _Connection, args: Any, budget: _Budget, meta: Any) -> dict[str, Any]:
    """Construct the restricted global namespace the script executes within."""

    async def agent(target: str, input: Optional[dict[str, Any]] = None, *, label: Optional[str] = None,
                     phase: Optional[str] = None, schema: Optional[dict[str, Any]] = None,
                     model: Optional[str] = None, effort: Optional[str] = None,
                     isolation: Optional[str] = None, context: Optional[dict[str, Any]] = None) -> Any:
        explicit_opts = {
            "label": label, "phase": phase, "schema": schema, "model": model,
            "effort": effort, "isolation": isolation, "context": context,
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
            return conn.call("agent", params)
        return conn.call("agent", {"agent_id": target, "input": input or {}, "label": label, "schema": schema})

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
        return conn.call("kanban_agent", params)

    async def workflow(name: str, args: Any = None) -> Any:  # nested workflows (parent decides support)
        return conn.call("workflow", {"name": name, "args": args})

    async def capability(name: str, input: Optional[dict[str, Any]] = None, *, label: Optional[str] = None,
                         approval_id: Optional[str] = None, schema: Optional[dict[str, Any]] = None) -> Any:
        return conn.call(
            "capability",
            {"name": name, "input": input or {}, "label": label, "approval_id": approval_id, "schema": schema},
        )

    def log(message: Any) -> None:
        conn.call("log", {"message": str(message)})

    def phase(title: Any) -> None:
        conn.call("phase", {"title": str(title)})

    async def parallel(thunks: Any) -> list[Any]:
        """Run thunks and join all results (barrier). Sequential & deterministic."""
        results = []
        for thunk in thunks:
            results.append(await _maybe_await(thunk()))
        return results

    async def pipeline(items: Any, *stages: Any) -> list[Any]:
        """Run each item through all stages independently; no cross-item barrier."""
        out = []
        for index, item in enumerate(items):
            current: Any = item
            for stage in stages:
                current = await _maybe_await(_call_stage(stage, current, item, index))
            out.append(current)
        return out

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
        "log": log,
        "phase": phase,
        "parallel": parallel,
        "pipeline": pipeline,
        "CapabilityError": CapabilityError,
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
    if isinstance(exc, CapabilityError) and exc.code:
        payload["code"] = exc.code
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
    script_globals = _build_script_globals(conn, boot.get("args"), budget, validation.meta)

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
