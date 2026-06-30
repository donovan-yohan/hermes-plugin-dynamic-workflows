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

import math
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from . import errors as err, rpc
from .agents import (
    CHILD_AGENT_OPTION_KEYS,
    AgentRunner,
    ChildAgentRequest,
    ChildAgentRunner,
    StubAgentRunner,
    is_known_agent,
    is_kanban_runner_id,
    kanban_runner_id,
)
from .capabilities import (
    CapabilityPolicy,
    CapabilityRegistry,
    normalize_capability_name,
    safe_capability_metadata_value,
)
from .errors import (
    CapabilityDenied,
    CorruptScriptRunError,
    ScriptRunStoreError,
    ScriptValidationError,
    WorkflowSubprocessError,
)
from .kanban import (
    CARD_BLOCKED,
    CARD_COMPLETED,
    DEFAULT_ON_BLOCK,
    KanbanBackend,
    KanbanBlocked,
    KanbanCardSpec,
    KanbanError,
    KanbanResolution,
    KanbanTimeout,
    KanbanUnknownProfile,
    normalize_on_block,
    validate_workflow_result,
)
from .grants import redact_credentials
from .registry import utc_now_iso
from .script_store import (
    CallRecorder,
    ReplayCache,
    ScriptRunStore,
    canonical_hash,
    is_replayable,
    replay_args_hash,
    script_sha256,
)
from .script_validator import ScriptValidation, validate_script

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
_ALLOWED_METHODS = frozenset({"agent", "kanban_agent", "capability", "log", "phase", "workflow"})

# Sentinel: a replay consult that found no cache entry for a call id (a miss),
# so the broker must fall through to a live dispatch.
_MISS = object()

# Cap on how many distinct result-contract violations one kanban_agent call records
# (journal marker + card comment). Under on_block="pause" a misbehaving worker can
# re-complete with bad output rapidly; the await is deadline-bounded, but this keeps
# a buggy worker from amplifying into unbounded journal writes / board comments.
_MAX_RESULT_INVALID_RECORDS = 8

_SCRIPT_META_DIAGNOSTIC_CODES = frozenset(
    {
        err.E_SCRIPT_META_POSITION,
        err.E_SCRIPT_META_SHAPE,
        err.E_SCRIPT_META_FIELDS,
        err.E_SCRIPT_META_PHASES,
    }
)

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
    max_capability_calls: int = 100
    max_runtime_s: float = 30.0
    allow_nested_workflows: bool = False
    token_budget: Optional[int] = None
    # Durable suspend window for an unresolved ``on_block="pause"`` Kanban await
    # (issue #5). ``None`` (default) keeps the prior behaviour: a paused await
    # blocks in-process until the run deadline, then fails with ``kanban_timeout``.
    # When set, a paused await that has not resolved within this many seconds
    # **suspends the run durably** (status ``suspended``) instead of holding the
    # thread to the deadline, so a fresh process resumes it from a replayed event
    # via ``replay_from`` rather than the parent blocking. Capped at
    # ``max_runtime_s`` (a value >= it never suspends; the run deadline wins).
    kanban_suspend_after_s: Optional[float] = None


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
    # True when the run did not finish but was durably *suspended* on an
    # unresolved ``on_block="pause"`` Kanban await (issue #5) — distinct from a
    # genuine failure. ``error`` then carries metadata-safe suspend details
    # (``type="KanbanSuspended"``, ``card_id``, ``profile``, ``on_block``); the run
    # is resumable in a fresh process via ``replay_from`` once the card resolves.
    suspended: bool = False
    # Durable-store fields (issue #3); populated only when a ScriptRunStore is
    # supplied to run_script. Left None/0 for in-memory runs so existing callers
    # are unaffected.
    run_id: Optional[str] = None
    journal_path: Optional[str] = None
    replayed_calls: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "value": self.value,
            "error": self.error,
            "meta": self.meta,
            "calls": self.calls,
            "exit_code": self.exit_code,
            "stderr": self.stderr,
            "suspended": self.suspended,
            "run_id": self.run_id,
            "journal_path": self.journal_path,
            "replayed_calls": self.replayed_calls,
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
        child_agent_runner: Optional[ChildAgentRunner] = None,
        journal: Optional[JournalSink] = None,
        redact: bool = True,
        recorder: Optional[CallRecorder] = None,
        replay: Optional[ReplayCache] = None,
        deterministic_runner: bool = False,
        kanban_backend: Optional[KanbanBackend] = None,
        idempotency_root: str = "",
        capability_registry: Optional[CapabilityRegistry] = None,
        capability_policy: Optional[CapabilityPolicy] = None,
    ) -> None:
        self._runner = agent_runner
        self._child_runner = child_agent_runner
        self._limits = limits
        self._journal = journal
        self._redact = redact
        # Durable-store seams (issue #3): a recorder persists deterministic call
        # results for future replay; a replay cache serves them instead of
        # re-dispatching. Both default off, so the broker is unchanged for
        # in-memory runs and the broker unit tests.
        self._recorder = recorder
        self._replay = replay
        self._deterministic_runner = deterministic_runner
        # Durable Kanban awaitable seam (issue #5): when present, kanban_agent
        # calls are turned into durable, idempotent cards instead of synchronous
        # AgentRunner stub calls. ``idempotency_root`` is the workflow's logical
        # run id; combined with the stable call id it keys card create/reattach so
        # a replay reattaches the same card rather than opening a duplicate.
        self._kanban_backend = kanban_backend
        self._idempotency_root = idempotency_root
        self._capability_registry = capability_registry
        self._capability_policy = capability_policy if capability_policy is not None else CapabilityPolicy()
        # Absolute wall-clock deadline for this run, set at construction (a few ms
        # before the subprocess spawns and the _drive watchdog arms). A durable
        # Kanban await is bounded by *this* shared deadline rather than a fresh
        # per-call window, so a late or repeated kanban_agent call cannot stretch
        # total wall-clock past ~max_runtime_s (the watchdog cannot interrupt an
        # in-progress parent-side await, so the two must share one deadline).
        self._deadline = time.monotonic() + self._limits.max_runtime_s
        self._rpc_calls = 0
        self._agent_calls = 0
        self._kanban_calls = 0
        self._capability_calls = 0
        self._tokens = 0
        self._replayed_calls = 0
        self.should_abort = False
        self.abort_reason: Optional[str] = None
        # Durable suspend signal (issue #5): set when an unresolved paused Kanban
        # await exhausts its suspend window. The VM tears down the subprocess (as
        # for ``should_abort``) but reports a *suspended*, resumable run rather than
        # a failure; ``suspend_info`` carries the metadata-safe card details.
        self.should_suspend = False
        self.suspend_info: Optional[dict[str, Any]] = None

    @property
    def replayed_calls(self) -> int:
        """Number of calls served from the replay cache this run."""
        return self._replayed_calls

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
                self.abort_reason = "aborted: capability hard-limit exceeded"
                raise CapabilityDenied(
                    f"max_rpc_calls ({self._limits.max_rpc_calls}) exceeded", code="limit_rpc"
                )
            if method not in _ALLOWED_METHODS:
                raise CapabilityDenied(f"method {method!r} is not allowed", code="unknown_method")

            # Replay: serve a deterministic call from the cache instead of
            # re-dispatching. A hit returns the recorded value without touching
            # the runner; a method/args drift fails closed; a miss falls through
            # to a live dispatch (the call was non-replayable in the source run).
            if self._replay is not None:
                replayed = self._maybe_replay(call_id, method, params)
                if replayed is not _MISS:
                    return replayed

            value = self._dispatch(call_id, method, params)
            # The effect has already happened. Persist (replay cache + journal)
            # on a best-effort basis *after* building the success frame so a
            # disk/IO failure here never masquerades as a runner failure for a
            # call that actually succeeded (and may have produced an external
            # side effect a live runner cannot take back).
            ret = {"t": rpc.T_RET, "id": call_id, "ok": True, "value": value, "budget": self._budget_info()}
            self._persist_success(call_id, method, params, value)
            return ret
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
                "error": {"code": "runner_error", "message": f"{type(exc).__name__} raised while dispatching brokered call"},
                "budget": self._budget_info(),
            }

    def _persist_success(self, call_id: Any, method: str, params: dict[str, Any], value: Any) -> None:
        """Best-effort persist a successful call (replay cache + journal event).

        Both writes are guarded independently: a cache write failure still lets
        the journal event through, and either failure is swallowed rather than
        reported as a runner failure for a call that already succeeded. A lost
        cache line is fail-safe — on replay the missing id is a miss and the call
        simply re-runs.
        """
        if self._recorder is not None and self._is_cacheable(method, params):
            try:
                self._recorder.record(call_id, method, replay_args_hash(method, params), value)
            except Exception:  # noqa: BLE001 — persistence is best-effort.
                pass
        try:
            self._emit(self._call_event(call_id, method, params, ok=True))
        except Exception:  # noqa: BLE001 — journaling is best-effort.
            pass

    def _is_cacheable(self, method: str, params: dict[str, Any]) -> bool:
        """Whether a call's result may be written to the #3 replay cache.

        Mirrors :func:`is_replayable`, with one subtraction: a ``kanban_agent``
        served by a live :class:`KanbanBackend` is a durable external effect, not
        a pure function, so it is never cached. On replay it re-runs and the
        idempotency key reattaches the same card — no duplicate, no stale value.
        Generic host capabilities are cacheable only when the host registered the
        specific capability as replayable.
        """
        if method == "kanban_agent" and self._kanban_backend is not None:
            return False
        if method == "capability":
            if self._capability_registry is None:
                return False
            try:
                return self._capability_registry.get(params.get("name", "")).replayable
            except (CapabilityDenied, ValueError):
                return False
        return is_replayable(method, deterministic_runner=self._deterministic_runner)

    def _maybe_replay(self, call_id: Any, method: Any, params: dict[str, Any]) -> Any:
        """Consult the replay cache for ``call_id``.

        Returns the ``ret`` frame on a hit, raises :class:`CapabilityDenied`
        (``replay_mismatch``, abort) on a method/args drift, or returns
        :data:`_MISS` when there is no cached entry (the caller dispatches live).
        """
        entry = self._replay.get(call_id)  # type: ignore[union-attr]
        if entry is None:
            return _MISS
        args_hash = replay_args_hash(method, params) if isinstance(params, dict) else ""
        if entry.method != method or entry.args_hash != args_hash:
            self.should_abort = True
            self.abort_reason = (
                f"replay drift at call {call_id}: recorded {entry.method!r} does not "
                f"match {method!r} (or arguments changed)"
            )
            raise CapabilityDenied(self.abort_reason, code="replay_mismatch")
        # Hit. Mirror the live accounting so cap-/budget-gated control flow in the
        # script reproduces the recorded run:
        #  * advance the soft per-method counters, so a later *non-cached* call
        #    still trips max_agent_calls / max_kanban_calls at the same point it
        #    did on the recorded run;
        #  * re-apply the recorded non-negative token spend (a negative/absent
        #    value is ignored — the hard cap is not re-enforced on a faithful
        #    replay, so a tampered _tokens must not skew the budget downward).
        if method == "agent":
            self._agent_calls += 1
        elif method == "kanban_agent":
            self._kanban_calls += 1
        elif method == "capability":
            self._capability_calls += 1
        if method in ("agent", "kanban_agent", "capability") and isinstance(entry.value, dict):
            usage = entry.value.get("_tokens")
            if isinstance(usage, int) and not isinstance(usage, bool) and usage >= 0:
                self._tokens += usage
        self._replayed_calls += 1
        self._emit(self._call_event(call_id, method, params, ok=True, replayed=True))
        return {"t": rpc.T_RET, "id": call_id, "ok": True, "value": entry.value, "budget": self._budget_info()}

    def _dispatch(self, call_id: Any, method: str, params: dict[str, Any]) -> Any:
        if method == "log":
            return None  # journaling is handled by _emit; nothing to return.
        if method == "phase":
            return None
        if method == "agent":
            return self._handle_agent(params)
        if method == "kanban_agent":
            return self._handle_kanban(call_id, params)
        if method == "capability":
            return self._handle_capability(call_id, params)
        if method == "workflow":
            if not self._limits.allow_nested_workflows:
                raise CapabilityDenied("nested workflows are not permitted", code="nested_denied")
            raise CapabilityDenied("nested workflows are not implemented in this slice", code="nested_unsupported")
        raise CapabilityDenied(f"method {method!r} is not allowed", code="unknown_method")

    def _handle_capability(self, call_id: Any, params: dict[str, Any]) -> dict[str, Any]:
        """Dispatch one generic host-owned capability through the registry."""

        raw_name = params.get("name")
        if not isinstance(raw_name, str) or not raw_name:
            raise CapabilityDenied("capability call requires a non-empty 'name'", code="bad_request")
        try:
            name = normalize_capability_name(raw_name)
        except ValueError as exc:
            raise CapabilityDenied(str(exc), code="bad_request") from exc
        self._check_token_budget()
        self._capability_calls += 1
        if self._capability_calls > self._limits.max_capability_calls:
            raise CapabilityDenied(
                f"max_capability_calls ({self._limits.max_capability_calls}) exceeded", code="limit_capability"
            )
        if self._capability_registry is None:
            raise CapabilityDenied("no capability registry configured for this run", code="capability_unavailable")
        capability = self._capability_registry.get(name)
        if self._replay is not None and capability.side_effect_class != "read_only" and not capability.replayable:
            self.should_abort = True
            self.abort_reason = (
                f"replay cannot safely re-dispatch non-replayable capability {capability.name!r}; "
                "register it as replayable and honor the provided idempotency_key, or split it outside replay"
            )
            raise CapabilityDenied(self.abort_reason, code="capability_replay_unsafe")
        result = self._capability_registry.run(
            capability.name,
            params,
            policy=self._capability_policy,
            run_context={
                "idempotency_root": self._idempotency_root,
                "call_id": call_id,
                "idempotency_key": f"{self._idempotency_root}:{call_id}",
                "replay": self._replay is not None,
            },
        )
        usage = result.get("_tokens")
        if isinstance(usage, int) and not isinstance(usage, bool) and usage >= 0:
            self._tokens += usage
        _validate_output(result, params.get("schema"))
        return result

    def _check_token_budget(self) -> None:
        """Hard ceiling: once the token budget is spent, deny further effects."""
        budget = self._limits.token_budget
        if budget is not None and self._tokens >= budget:
            self.should_abort = True
            raise CapabilityDenied(f"token_budget ({budget}) exhausted", code="limit_token")

    def _handle_agent(self, params: dict[str, Any]) -> dict[str, Any]:
        if "prompt" in params:
            return self._handle_prompt_agent(params)

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

    def _handle_prompt_agent(self, params: dict[str, Any]) -> dict[str, Any]:
        """Dispatch ``agent(prompt, opts)`` through an injected child-agent runner."""

        prompt = params.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip():
            raise CapabilityDenied("prompt agent call requires a non-empty 'prompt'", code="bad_request")
        unknown = sorted(set(params) - ({"prompt"} | CHILD_AGENT_OPTION_KEYS))
        if unknown:
            raise CapabilityDenied(
                "unsupported prompt agent option(s): " + ", ".join(unknown), code="bad_request"
            )
        self._check_token_budget()
        self._agent_calls += 1
        if self._agent_calls > self._limits.max_agent_calls:
            raise CapabilityDenied(f"max_agent_calls ({self._limits.max_agent_calls}) exceeded", code="limit_agent")
        if self._child_runner is None:
            raise CapabilityDenied(
                "no child agent runner configured for prompt agent calls", code="child_agent_unavailable"
            )

        request = ChildAgentRequest(
            prompt=prompt,
            label=_optional_str(params, "label"),
            phase=_optional_str(params, "phase"),
            schema=_optional_dict(params, "schema"),
            model=_optional_str(params, "model"),
            effort=_optional_str(params, "effort"),
            isolation=_optional_str(params, "isolation"),
            context=_optional_dict(params, "context") or {},
        )
        output = self._child_runner(request)
        if not isinstance(output, dict):
            raise CapabilityDenied(
                f"prompt child agent returned {type(output).__name__}, expected dict", code="bad_output"
            )
        safe_output = redact_credentials(_json_safe(output))
        assert isinstance(safe_output, dict)
        _validate_output(safe_output, request.schema)
        usage = safe_output.get("_tokens")
        if isinstance(usage, int) and not isinstance(usage, bool):
            self._tokens += usage
        return safe_output

    def _handle_kanban(self, call_id: Any, params: dict[str, Any]) -> dict[str, Any]:
        profile = params.get("profile")
        if not isinstance(profile, str) or not profile:
            raise CapabilityDenied("kanban_agent call requires a non-empty 'profile'", code="bad_request")
        try:
            on_block = normalize_on_block(params.get("on_block"))
        except ValueError as exc:
            raise CapabilityDenied(str(exc), code="bad_request") from exc
        self._check_token_budget()
        self._kanban_calls += 1
        if self._kanban_calls > self._limits.max_kanban_calls:
            # Soft denial (see _handle_agent): catchable; max_rpc_calls aborts.
            raise CapabilityDenied(f"max_kanban_calls ({self._limits.max_kanban_calls}) exceeded", code="limit_kanban")

        if self._kanban_backend is not None:
            return self._handle_kanban_durable(call_id, profile, on_block, params)

        # Legacy synchronous path (no durable backend injected): route through the
        # AgentRunner exactly as before, so existing in-memory runs/tests are
        # unchanged. on_block is inert here — there is no real card to block on.
        agent_id = kanban_runner_id(profile)
        payload = {
            "profile": profile,
            "task": params.get("task") or {},
            "input": params.get("input") if isinstance(params.get("input"), dict) else {},
            "wait": True,
            "durable": True,
        }
        return self._invoke(agent_id, payload, params.get("schema"))

    def _kanban_await_timeout(self) -> float:
        """Remaining time until the shared run deadline (never negative).

        Bounds a durable Kanban await by the same absolute deadline as the _drive
        watchdog instead of a fresh ``max_runtime_s`` window per call. ``0.0`` (the
        deadline already passed) makes :meth:`await_resolution` return a cached
        resolution immediately or raise ``KanbanTimeout`` without blocking.
        """
        remaining = self._deadline - time.monotonic()
        return remaining if remaining > 0.0 else 0.0

    def _pause_suspend_deadline(self, on_block: str) -> Optional[float]:
        """Monotonic deadline after which an unresolved paused await suspends.

        Applies only under ``on_block="pause"`` with a configured
        ``kanban_suspend_after_s``; capped at the shared run deadline so a suspend
        window ``>= max_runtime_s`` never preempts the genuine ``kanban_timeout``
        (the run deadline wins). ``None`` disables suspension, preserving the prior
        block-until-the-run-deadline behaviour for every other policy/config.
        """
        after = self._limits.kanban_suspend_after_s
        if on_block != "pause" or after is None:
            return None
        return min(self._deadline, time.monotonic() + max(0.0, after))

    def _begin_suspend(self, call_id: Any, card_id: str, profile: str, on_block: str) -> None:
        """Flag the run for durable suspension on an unresolved paused await.

        The VM observes :attr:`should_suspend` after the (denial) ret frame and
        tears the subprocess down, reporting a resumable *suspended* run instead of
        a failure. The metadata-only journal ``call`` event is emitted by the
        :class:`CapabilityDenied` path in :meth:`handle` (error ``kanban_suspended``).
        """
        self.should_suspend = True
        self.suspend_info = {
            "card_id": card_id,
            "profile": profile,
            "call_id": call_id,
            "on_block": on_block,
        }

    def _kanban_idempotency_key(self, call_id: Any) -> str:
        """Stable key for one logical ``kanban_agent`` call.

        ``<logical_run_id>:<stable_call_id>``. The call id is reproducible across a
        replay of the same script+args, and a replay inherits the source run's id
        as the root, so create/reattach converges on one card per logical step.
        """
        return f"{self._idempotency_root}:{call_id}"

    def _handle_kanban_durable(
        self, call_id: Any, profile: str, on_block: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        spec = KanbanCardSpec(
            profile=profile,
            title=params.get("title"),
            prompt=params.get("prompt"),
            context=params.get("context") if isinstance(params.get("context"), dict) else {},
            task=params.get("task") if isinstance(params.get("task"), dict) else {},
            input=params.get("input") if isinstance(params.get("input"), dict) else {},
            board=params.get("board"),
            tenant=params.get("tenant"),
            parents=tuple(params["parents"]) if isinstance(params.get("parents"), (list, tuple)) else (),
            labels=tuple(params["labels"]) if isinstance(params.get("labels"), (list, tuple)) else (),
            workspace=params.get("workspace") if isinstance(params.get("workspace"), dict) else None,
            schema=params.get("schema") if isinstance(params.get("schema"), dict) else None,
        )
        idempotency_key = self._kanban_idempotency_key(call_id)
        try:
            card = self._kanban_backend.create_or_reattach(idempotency_key, spec)
        except KanbanUnknownProfile as exc:
            raise CapabilityDenied(str(exc), code="unknown_profile") from exc
        except KanbanError as exc:
            raise CapabilityDenied(str(exc), code="kanban_error") from exc

        resolution, diagnostics = self._await_valid_kanban_result(
            call_id, card.card_id, profile, on_block, spec.schema
        )

        if resolution.status == CARD_BLOCKED and on_block == "raise":
            raise CapabilityDenied(str(KanbanBlocked(resolution)), code="kanban_blocked")

        result: dict[str, Any] = {
            "card_id": resolution.card_id,
            "profile": resolution.profile or profile,
            "status": resolution.status,
            # The validated worker payload (issue #6). Unknown payloads are passed
            # through untouched when the call declared no schema.
            "workflow_result": resolution.result or {},
            "reattached": card.reattached,
        }
        if resolution.reason is not None:
            result["reason"] = resolution.reason
        if diagnostics:
            result["diagnostics"] = diagnostics
        usage = (resolution.result or {}).get("_tokens")
        if isinstance(usage, int) and not isinstance(usage, bool) and usage >= 0:
            self._tokens += usage
        return result

    def _await_valid_kanban_result(
        self, call_id: Any, card_id: str, profile: str, on_block: str, schema: Optional[dict[str, Any]]
    ) -> tuple[KanbanResolution, list[str]]:
        """Await a resolution and enforce the workflow-result contract (issue #6).

        A *completed* card whose ``workflow_result`` is missing or fails ``schema``
        is a contract violation: it must not resolve as success. Under ``pause`` we
        wait for a newer (re-)completion (retry/unblock); otherwise we surface it
        as a deterministic ``blocked`` with diagnostics. Validation diagnostics are
        recorded in the run journal and (best-effort) as a Kanban card comment.
        """
        accept_blocked = on_block != "pause"
        suspend_deadline = self._pause_suspend_deadline(on_block)
        after_version = 0
        has_received = False
        last_recorded: Optional[tuple[str, ...]] = None
        records = 0
        while True:
            timeout = self._kanban_await_timeout()
            if suspend_deadline is not None:
                # Bound this await by the *nearer* of the run deadline and the
                # suspend window, so an unresolved paused card hands control back
                # promptly enough to suspend instead of holding the thread.
                timeout = min(timeout, max(0.0, suspend_deadline - time.monotonic()))
            try:
                resolution = self._kanban_backend.await_resolution(
                    card_id,
                    accept_blocked=accept_blocked,
                    timeout=timeout,
                    after_version=after_version,
                )
            except KanbanTimeout as exc:
                # Distinguish the suspend window elapsing from the genuine run
                # deadline. Decide on the two pre-computed deadlines, NOT a fresh
                # clock read: ``suspend_deadline`` is clamped to ``self._deadline``,
                # so it is strictly less only when the suspend window is the binding
                # (nearer) bound — i.e. this timeout was the suspend window. A
                # config of ``kanban_suspend_after_s >= max_runtime_s`` clamps the
                # two equal, so the run deadline wins and it falls through to a
                # genuine ``kanban_timeout``. Re-sampling the clock here would let a
                # GC/GIL pause misclassify a legitimate suspend near the boundary.
                if suspend_deadline is not None and suspend_deadline < self._deadline:
                    self._begin_suspend(call_id, card_id, profile, on_block)
                    raise CapabilityDenied(
                        f"kanban await suspended on card {card_id!r}; resume the run to continue",
                        code="kanban_suspended",
                    ) from exc
                raise CapabilityDenied(str(exc), code="kanban_timeout") from exc
            except KanbanError as exc:  # any other backend failure -> structured denial.
                raise CapabilityDenied(str(exc), code="kanban_error") from exc

            # Defense: await_resolution must return an event strictly newer than
            # after_version. A backend that ignores after_version would hand back
            # the same rejected completion and the pause retry would hot-spin to the
            # deadline; fail closed instead of spinning.
            if has_received and resolution.version <= after_version:
                raise CapabilityDenied(
                    "kanban backend returned a stale event (after_version ignored)",
                    code="kanban_error",
                )
            after_version = resolution.version
            has_received = True
            if resolution.status != CARD_COMPLETED or not schema:
                return resolution, []  # blocked/failed, or no contract to enforce.

            diagnostics = validate_workflow_result(resolution.result, schema)
            if not diagnostics:
                return resolution, []  # valid structured result.

            # Contract violation: a completed card with a bad/missing workflow_result
            # must not be returned as success. Record the rejection, but de-dup
            # consecutive identical diagnostics and cap total records so a worker
            # stuck re-completing with bad output can't amplify into unbounded
            # journal writes / board comments.
            diag_key = tuple(diagnostics)
            if diag_key != last_recorded and records < _MAX_RESULT_INVALID_RECORDS:
                self._record_kanban_result_invalid(call_id, card_id, profile, diagnostics)
                last_recorded = diag_key
                records += 1
            if on_block == "pause":
                continue  # wait for the worker to re-complete with a valid result.
            reason = "workflow_result failed schema: " + "; ".join(diagnostics)
            blocked = KanbanResolution(
                card_id=resolution.card_id,
                profile=resolution.profile or profile,
                status=CARD_BLOCKED,
                result=resolution.result or {},
                reason=reason,
                version=resolution.version,
            )
            return blocked, diagnostics

    def _record_kanban_result_invalid(
        self, call_id: Any, card_id: str, profile: str, diagnostics: list[str]
    ) -> None:
        """Journal the validation failure and post a card comment (both best-effort)."""
        summary = f"result_invalid ({len(diagnostics)} field(s))"
        try:
            # Metadata-only journal marker (the per-field detail goes to the card
            # comment, not the redacted journal): a call-shaped event the durable
            # journal sink records with method/ok/profile/error.
            self._emit(
                {
                    "type": "rpc_call",
                    "call_id": call_id,
                    "method": "kanban_agent",
                    "profile": profile,
                    "ok": False,
                    "error": summary,
                }
            )
        except Exception:  # noqa: BLE001 — journaling is best-effort.
            pass
        recorder = getattr(self._kanban_backend, "record_event", None)
        if callable(recorder):
            try:
                recorder(card_id, "result_invalid", {"diagnostics": list(diagnostics)})
            except Exception:  # noqa: BLE001 — card comments are best-effort.
                pass

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
        self,
        call_id: Any,
        method: Any,
        params: dict[str, Any],
        *,
        ok: bool,
        error: Optional[str] = None,
        replayed: bool = False,
    ) -> dict[str, Any]:
        event: dict[str, Any] = {"type": "rpc_call", "call_id": call_id, "method": method, "ok": ok}
        if method in ("agent",):
            event["agent_id"] = params.get("agent_id")
        if method in ("kanban_agent",):
            event["profile"] = params.get("profile")
        if method in ("capability",):
            event["capability"] = safe_capability_metadata_value(params.get("name"))
        if method in ("phase",):
            event["phase_title"] = safe_capability_metadata_value(params.get("title"))
        if params.get("label"):
            event["label"] = safe_capability_metadata_value(params.get("label"))
        if params.get("phase"):
            event["phase"] = safe_capability_metadata_value(params.get("phase"))
        if error:
            event["error"] = error
        if replayed:
            event["replayed"] = True
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


def _optional_str(params: dict[str, Any], key: str) -> Optional[str]:
    value = params.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise CapabilityDenied(f"prompt agent option {key!r} must be a string", code="bad_request")


def _optional_dict(params: dict[str, Any], key: str) -> Optional[dict[str, Any]]:
    value = params.get(key)
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    raise CapabilityDenied(f"prompt agent option {key!r} must be an object", code="bad_request")


def _json_safe(value: Any) -> Any:
    """Coerce child-agent output into a deterministic JSON-safe shape."""
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else {"_unserializable_type": "float"}
    if isinstance(value, (str, int, bool, type(None))):
        return value
    return {"_unserializable_type": type(value).__name__}


class WorkflowVM:
    """Launches and drives one workflow subprocess under a capability broker."""

    def __init__(
        self,
        *,
        agent_runner: Optional[AgentRunner] = None,
        child_agent_runner: Optional[ChildAgentRunner] = None,
        limits: Optional[VMLimits] = None,
        journal: Optional[JournalSink] = None,
        python_executable: Optional[str] = None,
        recorder: Optional[CallRecorder] = None,
        replay: Optional[ReplayCache] = None,
        deterministic_runner: bool = False,
        kanban_backend: Optional[KanbanBackend] = None,
        idempotency_root: str = "",
        capability_registry: Optional[CapabilityRegistry] = None,
        capability_policy: Optional[CapabilityPolicy] = None,
    ) -> None:
        self._runner = agent_runner if agent_runner is not None else StubAgentRunner()
        self._child_runner = child_agent_runner
        self._limits = limits if limits is not None else VMLimits()
        self._journal = journal
        self._python = python_executable or sys.executable
        # Durable-store wiring (issue #3): forwarded to the per-run broker.
        self._recorder = recorder
        self._replay = replay
        self._deterministic_runner = deterministic_runner
        # Durable Kanban awaitable wiring (issue #5): forwarded to the broker.
        self._kanban_backend = kanban_backend
        self._idempotency_root = idempotency_root
        # Generic host-owned capability API wiring (issue #29): forwarded to the broker.
        self._capability_registry = capability_registry
        self._capability_policy = capability_policy

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

        broker = CapabilityBroker(
            self._runner,
            self._limits,
            child_agent_runner=self._child_runner,
            journal=_collect,
            recorder=self._recorder,
            replay=self._replay,
            deterministic_runner=self._deterministic_runner,
            kanban_backend=self._kanban_backend,
            idempotency_root=self._idempotency_root,
            capability_registry=self._capability_registry,
            capability_policy=self._capability_policy,
        )
        try:
            result = self._drive(script, args, broker, calls)
            result.replayed_calls = broker.replayed_calls
            return result
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
        suspended: Optional[dict[str, Any]] = None

        try:
            assert proc.stdin is not None and proc.stdout is not None
            boot = {
                "t": rpc.T_BOOT,
                "script": script,
                "args": args,
                "limits": {
                    "max_rpc_calls": self._limits.max_rpc_calls,
                    "max_agent_calls": self._limits.max_agent_calls,
                    "max_capability_calls": self._limits.max_capability_calls,
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
                        protocol_error = broker.abort_reason or "aborted: capability hard-limit exceeded"
                        break
                    if broker.should_suspend:
                        # An unresolved paused Kanban await suspended the run: tear
                        # the subprocess down (the script's local state is discarded;
                        # a resume re-runs it from the replay cache) and report a
                        # resumable suspended run rather than a failure.
                        _kill(proc)
                        suspended = broker.suspend_info or {}
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
            # Close the read pipes too: on a kill path (timeout / abort / suspend)
            # the read loop breaks before stdout EOFs, so without this the pipe fds
            # leak until GC (a ResourceWarning). stderr is closed after the drain
            # thread has joined, so the close never races the reader.
            _close(proc.stdout)
            _close(proc.stderr)

        stderr_text = "".join(stderr_chunks)[-4000:]
        exit_code = proc.returncode

        if timed_out.is_set():
            return ScriptRunResult(
                ok=False, meta=meta, calls=calls, exit_code=exit_code, stderr=stderr_text,
                error={"type": "WorkflowSubprocessError",
                       "message": f"workflow timed out after {self._limits.max_runtime_s}s"},
            )
        if suspended is not None:
            # Durable, resumable suspension (issue #5) — not a failure. The error
            # payload is metadata-safe (card id is a content-address, profile is a
            # role name) so it can live on the operator-facing run.json.
            return ScriptRunResult(
                ok=False, suspended=True, meta=meta, calls=calls,
                exit_code=exit_code, stderr=stderr_text,
                error={"type": "KanbanSuspended", **suspended},
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


def _limits_view(limits: VMLimits) -> dict[str, Any]:
    """A small, metadata-only snapshot of the limits for the durable run.json."""
    return {
        "max_rpc_calls": limits.max_rpc_calls,
        "max_agent_calls": limits.max_agent_calls,
        "max_kanban_calls": limits.max_kanban_calls,
        "max_capability_calls": limits.max_capability_calls,
        "max_runtime_s": limits.max_runtime_s,
        "allow_nested_workflows": limits.allow_nested_workflows,
        "token_budget": limits.token_budget,
        "kanban_suspend_after_s": limits.kanban_suspend_after_s,
    }


def _validation_meta(validation: ScriptValidation) -> Optional[dict[str, Any]]:
    """Return extracted script meta only when the meta contract itself passed."""
    if validation.meta is None:
        return None
    if any(d.code in _SCRIPT_META_DIAGNOSTIC_CODES for d in validation.diagnostics):
        return None
    return validation.meta


class _CorruptLimitsView(ValueError):
    """A persisted ``_limits_view`` carries a present-but-invalid value.

    Raised by :func:`_limits_from_view` so the replay caller can fail closed
    *before* launch instead of silently widening the recorded caps by falling
    back to the permissive global default. A genuinely-absent key (forward/back-
    compat) is not corruption and still defaults.
    """


_MISSING = object()


def _req_int(view: dict[str, Any], key: str, default: int) -> int:
    """A cap int from the view; a missing key (or null) defaults, else strict.

    Only a real JSON number is accepted (``bool`` is excluded: ``True``/``False``
    are ints but never a meaningful cap). A present string / wrong type / non-
    finite value is corruption and raises, so the recorded cap can never be
    silently widened to the global default on a replay.
    """
    value = view.get(key, _MISSING)
    if value is _MISSING or value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _CorruptLimitsView(f"{key}={value!r} is not a number")
    if isinstance(value, float) and not math.isfinite(value):
        raise _CorruptLimitsView(f"{key}={value!r} is not finite")
    return int(value)


def _req_float(view: dict[str, Any], key: str, default: float) -> float:
    """A finite cap float from the view; a missing key (or null) defaults.

    Rejects non-finite ``inf``/``nan`` — Python's ``json`` decodes ``Infinity`` /
    ``NaN`` by default, and a forged ``max_runtime_s`` of ``inf`` would otherwise
    disable the wall-clock watchdog (its :class:`threading.Timer` never fires).
    """
    value = view.get(key, _MISSING)
    if value is _MISSING or value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _CorruptLimitsView(f"{key}={value!r} is not a number")
    result = float(value)
    if not math.isfinite(result):
        raise _CorruptLimitsView(f"{key}={value!r} is not finite")
    return result


def _req_bool(view: dict[str, Any], key: str, default: bool) -> bool:
    """A bool from the view; a missing key (or null) defaults, any non-bool raises.

    A persisted ``"false"`` string is truthy under ``bool()``; refusing to coerce
    it avoids silently flipping ``allow_nested_workflows`` on.
    """
    value = view.get(key, _MISSING)
    if value is _MISSING or value is None:
        return default
    if not isinstance(value, bool):
        raise _CorruptLimitsView(f"{key}={value!r} is not a bool")
    return value


def _req_opt_float(view: dict[str, Any], key: str, default: Optional[float]) -> Optional[float]:
    """An optional finite cap float from the view; a missing key defaults, null means None.

    Mirrors :func:`_req_float` but ``None`` is a meaningful value (no suspend
    window), so it is distinguished from an absent key. A present non-number or a
    non-finite value is corruption and raises, so a forged suspend window cannot
    silently disable or distort the resume behaviour on a replay.
    """
    value = view.get(key, _MISSING)
    if value is _MISSING:
        return default
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _CorruptLimitsView(f"{key}={value!r} is not a number")
    result = float(value)
    if not math.isfinite(result):
        raise _CorruptLimitsView(f"{key}={value!r} is not finite")
    return result


def _req_token_budget(view: dict[str, Any], default: Optional[int]) -> Optional[int]:
    """Token budget from the view; a missing key defaults, null means no budget.

    A present non-int (bool / float / string) is corruption and raises, so a
    partially-corrupt budget field cannot silently drop the budget to unlimited.
    """
    value = view.get("token_budget", _MISSING)
    if value is _MISSING:
        return default
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise _CorruptLimitsView(f"token_budget={value!r} is not an integer")
    return value


def _limits_from_view(view: Any) -> VMLimits:
    """Rebuild :class:`VMLimits` from a persisted ``_limits_view`` snapshot.

    Used to pin a replay's caps/budget to the recorded run so budget-/cap-gated
    control flow reproduces faithfully when the caller does not pass explicit
    ``limits=``. A genuinely-absent key (forward/back-compat) falls back to the
    :class:`VMLimits` default, but a non-dict view or any present-but-invalid
    value raises :class:`_CorruptLimitsView` — the replay caller turns that into a
    typed, fail-closed :class:`~hermes_workflows.errors.CorruptScriptRunError`
    *before* any subprocess spawns, rather than silently widening the recorded
    caps to the permissive global default (or admitting a non-finite runtime that
    would disable the watchdog).
    """
    if not isinstance(view, dict):
        raise _CorruptLimitsView("limits view is not an object")
    default = VMLimits()
    return VMLimits(
        max_rpc_calls=_req_int(view, "max_rpc_calls", default.max_rpc_calls),
        max_agent_calls=_req_int(view, "max_agent_calls", default.max_agent_calls),
        max_kanban_calls=_req_int(view, "max_kanban_calls", default.max_kanban_calls),
        max_capability_calls=_req_int(view, "max_capability_calls", default.max_capability_calls),
        max_runtime_s=_req_float(view, "max_runtime_s", default.max_runtime_s),
        allow_nested_workflows=_req_bool(
            view, "allow_nested_workflows", default.allow_nested_workflows
        ),
        token_budget=_req_token_budget(view, default.token_budget),
        kanban_suspend_after_s=_req_opt_float(
            view, "kanban_suspend_after_s", default.kanban_suspend_after_s
        ),
    )


def run_script(
    script: str,
    *,
    args: Any = None,
    agent_runner: Optional[AgentRunner] = None,
    child_agent_runner: Optional[ChildAgentRunner] = None,
    limits: Optional[VMLimits] = None,
    journal: Optional[JournalSink] = None,
    validate: bool = True,
    store: Optional[ScriptRunStore] = None,
    run_id: Optional[str] = None,
    replay_from: Optional[str] = None,
    deterministic_runner: Optional[bool] = None,
    kanban_backend: Optional[KanbanBackend] = None,
    capability_registry: Optional[CapabilityRegistry] = None,
    capability_policy: Optional[CapabilityPolicy] = None,
) -> ScriptRunResult:
    """Construct a :class:`WorkflowVM` and run one script, optionally durable.

    Without ``store`` this is the original in-memory convenience wrapper. With a
    ``store`` the run is persisted under a stable ``run_id`` (minted if omitted):
    a ``run.json`` metadata snapshot, a metadata-only ``journal.jsonl``
    (``boot`` / ``call`` / ``done``), and — for deterministic calls — a
    ``cache.jsonl`` replay cache.

    ``replay_from`` names a prior run whose deterministic calls are served from
    the cache instead of being re-dispatched (``store`` is required, and the
    cache is loaded up front so a corrupt/missing cache raises a typed
    :class:`~hermes_workflows.errors.ScriptRunStoreError` *before* any subprocess
    is spawned). A replay *reproduces the recorded run*: the cache only ever holds
    the source run's deterministic calls (``log``/``phase`` always, ``agent``/
    ``kanban_agent`` only if the source's runner was deterministic), and those are
    served by call id irrespective of the runner passed to this replay invocation
    — the replay's own ``deterministic_runner`` does not re-gate them. Omit
    ``replay_from`` to dispatch fresh against the live runner instead.
    ``deterministic_runner`` overrides the default detection
    (``isinstance(runner, StubAgentRunner)``); set it ``True`` only when the
    injected runner is a pure function of its inputs, or agent/kanban results
    will not be cached.
    """
    runner = agent_runner if agent_runner is not None else StubAgentRunner()
    deterministic = (
        deterministic_runner
        if deterministic_runner is not None
        else isinstance(runner, StubAgentRunner) and child_agent_runner is None
    )

    # Resolve replay up front, before touching the subprocess, so every failure
    # (no store, identity mismatch, corrupt/missing cache) fails closed and
    # typed rather than mid-run.
    replay_cache: Optional[ReplayCache] = None
    source_limits: Optional[dict[str, Any]] = None
    replay_idempotency_root: Optional[str] = None
    if replay_from is not None:
        if store is None:
            raise ValueError("replay_from requires a store")
        # Bind the replay to the exact (script, args) that produced the cache.
        # The per-call method+args_hash guard is local to each call; without this
        # identity check a *different* script/args could be served another run's
        # cached values at every coincidentally-matching call id, undetected.
        source = store.load_run(replay_from)
        if source.script_sha256 != script_sha256(script) or source.args_hash != canonical_hash(args):
            raise ValueError(
                f"replay_from {replay_from!r} does not match this script/args "
                "(script_sha256 or args_hash differs); a replay must reproduce the recorded run"
            )
        source_limits = source.limits
        # Resolve the *logical root* of the replay chain. Both the Kanban
        # idempotency key and the deterministic replay cache must key on the
        # original run, not the immediate source: replaying a replay (A <- B <- C)
        # would otherwise open a fresh card at each generation, and — because a
        # replay writes no cache.jsonl of its own — would load an *empty* cache
        # from the immediate suspended source and needlessly re-dispatch every
        # pre-pause deterministic call. Walk replay_of to the first non-replay
        # ancestor; degrade to the nearest resolvable run if the chain is broken.
        root_meta = source
        visited = {source.run_id}
        while root_meta.replay_of is not None and root_meta.replay_of not in visited:
            visited.add(root_meta.replay_of)
            try:
                root_meta = store.load_run(root_meta.replay_of)
            except ScriptRunStoreError:
                break
        replay_idempotency_root = root_meta.run_id
        # Serve the cache from the root (the only run that actually executed and
        # recorded the deterministic calls). For a single-generation resume the
        # root *is* the source, so this is unchanged for the common case. Loaded up
        # front so a corrupt/missing cache fails closed and typed before any spawn.
        replay_cache = store.load_cache(root_meta.run_id)

    # Default a replay's caps/budget to the recorded run's so budget-/cap-gated
    # control flow reproduces faithfully; an explicit limits= still overrides.
    if limits is not None:
        effective_limits = limits
    elif source_limits is not None:
        try:
            effective_limits = _limits_from_view(source_limits)
        except _CorruptLimitsView as exc:
            # The recorded run's persisted caps are corrupt. Refuse to replay under
            # silently-widened global defaults (which would loosen every cap and
            # drop the token budget to unlimited) — fail closed, typed, and before
            # any subprocess spawns.
            raise CorruptScriptRunError(
                replay_from, "corrupt_run", f"limits view: {exc}"
            ) from exc
    else:
        effective_limits = VMLimits()

    if store is None:
        # No durable store: there is no logical run id to share across replays, so
        # key Kanban idempotency by the program identity (script+args). Two runs of
        # the same program then reattach the same cards — replay-safe by design.
        idempotency_root = f"mem_{script_sha256(script)[:12]}_{canonical_hash(args)[:8]}"
        vm = WorkflowVM(
            agent_runner=runner,
            child_agent_runner=child_agent_runner,
            limits=effective_limits,
            journal=journal,
            replay=replay_cache,
            deterministic_runner=deterministic,
            kanban_backend=kanban_backend,
            idempotency_root=idempotency_root,
            capability_registry=capability_registry,
            capability_policy=capability_policy,
        )
        return vm.run(script, args=args, validate=validate)

    # Durable path: validate up front so a rejected script never leaves an
    # orphan run directory, then begin -> drive -> finish. Even when callers opt
    # out of the launch gate, run the static pass as metadata extraction only so
    # a valid meta.phases declaration can be persisted in the initial snapshot.
    validation = validate_script(script)
    if validate and not validation.ok:
        raise ScriptValidationError(validation.diagnostics)
    validation_meta = _validation_meta(validation)

    persist_run_id = run_id if run_id is not None else store.next_run_id(script, args)
    store.begin(
        persist_run_id,
        script=script,
        args=args,
        limits=_limits_view(effective_limits),
        deterministic_runner=deterministic,
        meta=validation_meta,
        replay_of=replay_from,
    )

    def _store_journal(event: dict[str, Any]) -> None:
        store.note_call(persist_run_id, event)
        if journal is not None:
            journal(event)

    # Record cache entries only on a fresh run; a replay consumes the cache.
    recorder = store.recorder(persist_run_id) if replay_cache is None else None
    # Kanban idempotency root is the *logical* run id: a fresh run uses its own
    # persisted run id; a replay (or replay-of-a-replay) inherits the original
    # run's id so create/reattach converges on the same card instead of opening a
    # duplicate at each generation.
    idempotency_root = replay_idempotency_root if replay_idempotency_root is not None else persist_run_id
    vm = WorkflowVM(
        agent_runner=runner,
        child_agent_runner=child_agent_runner,
        limits=effective_limits,
        journal=_store_journal,
        recorder=recorder,
        replay=replay_cache,
        deterministic_runner=deterministic,
        kanban_backend=kanban_backend,
        idempotency_root=idempotency_root,
        capability_registry=capability_registry,
        capability_policy=capability_policy,
    )
    result = vm.run(script, args=args, validate=False)
    result.run_id = persist_run_id
    result.journal_path = str(store.journal_path(persist_run_id))
    # A durably-suspended run (issue #5) is recorded as its own terminal status so
    # an operator/resumer can discover it (store.suspended_runs) and resume it with
    # replay_from once the awaited card produces an event. It is neither succeeded
    # nor failed.
    if result.suspended:
        status = "suspended"
    elif result.ok:
        status = "succeeded"
    else:
        status = "failed"
    store.finish(
        persist_run_id,
        status=status,
        meta=result.meta if result.meta is not None else validation_meta,
        value=result.value,
        error=result.error,
    )
    return result
