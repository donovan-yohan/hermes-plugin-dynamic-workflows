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

import json
import math
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
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
from .controls import ControlStore, may_check_run, may_continue_task, may_start_work, project_control_state
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
    kanban_card_id,
    normalize_on_block,
    validate_workflow_result,
)
from .grants import redact_credentials
from .registry import utc_now_iso
from .script_store import (
    CallRecorder,
    ReplayCache,
    ScriptRunStore,
    TranscriptRecorder,
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
    max_parallel: int = 8
    max_runtime_s: float = 30.0
    allow_nested_workflows: bool = False
    token_budget: Optional[int] = None
    # Schema-constrained prompt child agents may retry invalid/missing structured
    # output before surfacing a typed ``schema`` denial. Counts retries after the
    # initial attempt (``2`` => up to three child-agent invocations total).
    max_schema_retries: int = 2
    # Durable suspend window for an unresolved ``on_block="pause"`` Kanban await
    # (issue #5). ``None`` (default) keeps the prior behaviour: a paused await
    # blocks in-process until the run deadline, then fails with ``kanban_timeout``.
    # When set, a paused await that has not resolved within this many seconds
    # **suspends the run durably** (status ``suspended``) instead of holding the
    # thread to the deadline, so a fresh process resumes it from a replayed event
    # via ``replay_from`` rather than the parent blocking. Capped at
    # ``max_runtime_s`` (a value >= it never suspends; the run deadline wins).
    kanban_suspend_after_s: Optional[float] = None
    # Hard ceiling on a single ``agent``/``kanban_agent`` (including prompt-agent)
    # result's JSON-serialized size, mirroring the capability-result bound in
    # ``CapabilityPolicy.max_result_bytes``. A huge child result must not land
    # inline in script memory nor unbounded in the replay cache — this is our
    # single worst context exposure (issue #106). Unlike the capability path,
    # there is no field-level clipping here: an over-limit result fails closed
    # with a deterministic ``result_too_large`` error rather than being silently
    # truncated, so a future spill tier (#93) can convert this error into an
    # offload instead of guessing at what was cut.
    max_result_bytes: int = 512 * 1024


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
    paused: bool = False
    stopped: bool = False
    # Durable-store fields (issue #3); populated only when a ScriptRunStore is
    # supplied to run_script. Left None/0 for in-memory runs so existing callers
    # are unaffected.
    run_id: Optional[str] = None
    journal_path: Optional[str] = None
    transcripts: Optional[dict[str, Any]] = None
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
            "paused": self.paused,
            "stopped": self.stopped,
            "run_id": self.run_id,
            "journal_path": self.journal_path,
            "transcripts": self.transcripts,
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
        transcripts: Optional[TranscriptRecorder] = None,
        replay: Optional[ReplayCache] = None,
        deterministic_runner: bool = False,
        kanban_backend: Optional[KanbanBackend] = None,
        idempotency_root: str = "",
        active_run_id: Optional[str] = None,
        capability_registry: Optional[CapabilityRegistry] = None,
        capability_policy: Optional[CapabilityPolicy] = None,
        control_store: Optional[ControlStore] = None,
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
        self._transcripts = transcripts
        self._replay = replay
        self._deterministic_runner = deterministic_runner
        # Durable Kanban awaitable seam (issue #5): when present, kanban_agent
        # calls are turned into durable, idempotent cards instead of synchronous
        # AgentRunner stub calls. ``idempotency_root`` is the workflow's logical
        # run id; combined with the stable call id it keys card create/reattach so
        # a replay reattaches the same card rather than opening a duplicate.
        self._kanban_backend = kanban_backend
        self._idempotency_root = idempotency_root
        # Operator controls are scoped to the run currently being driven. Replays
        # intentionally keep the original run as idempotency_root for card/cache
        # convergence, but pause/stop/task_stop must read the fresh replay run id.
        self._active_run_id = active_run_id or idempotency_root
        self._capability_registry = capability_registry
        self._capability_policy = capability_policy if capability_policy is not None else CapabilityPolicy()
        self._control_store = control_store
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
        self._lock = threading.Lock()
        # Prompt-agent result cache for this process/run. Durable replays load the
        # same shape from ReplayCache; this in-memory map avoids respawning a child
        # twice for identical prompt/options within one live run.
        self._prompt_results: dict[str, tuple[str, dict[str, Any]]] = {}
        self._recorded_prompt_fingerprints: set[str] = set()
        self.should_abort = False
        self.abort_reason: Optional[str] = None
        # Durable suspend signal (issue #5): set when an unresolved paused Kanban
        # await exhausts its suspend window. The VM tears down the subprocess (as
        # for ``should_abort``) but reports a *suspended*, resumable run rather than
        # a failure; ``suspend_info`` carries the metadata-safe card details.
        self.should_suspend = False
        self.suspend_info: Optional[dict[str, Any]] = None
        self.should_pause = False
        self.pause_info: Optional[dict[str, Any]] = None
        self.should_stop = False
        self.stop_info: Optional[dict[str, Any]] = None

    @property
    def replayed_calls(self) -> int:
        """Number of calls served from the replay cache this run."""
        return self._replayed_calls

    # -- budget view piggybacked on every ret frame ------------------------
    def _budget_info(self) -> dict[str, Any]:
        total = self._limits.token_budget
        remaining = None if total is None else max(0, total - self._tokens)
        return {"total": total, "spent": self._tokens, "remaining": remaining}

    def _control_state(self):
        if self._control_store is None:
            return None
        return project_control_state(self._active_run_id, self._control_store.list_for(self._active_run_id))

    def _deny_for_control(self, decision, *, call_id: Any, method: str) -> None:
        info = {
            "code": decision.code,
            "reason": decision.reason,
            "control_id": decision.control_id,
            "call_id": call_id,
            "method": method,
        }
        if decision.code == "run_stopped":
            self.should_stop = True
            self.stop_info = info
        elif decision.code == "run_paused":
            self.should_pause = True
            self.pause_info = info
        raise CapabilityDenied(decision.reason, code=decision.code)

    def _check_run_control(self, call_id: Any, method: str, params: dict[str, Any]) -> None:
        state = self._control_state()
        if state is None:
            return
        decision = may_check_run(state)
        if not decision.allowed:
            self._deny_for_control(decision, call_id=call_id, method=method)

    def _check_start_control(self, call_id: Any, method: str, params: dict[str, Any]) -> None:
        state = self._control_state()
        if state is None:
            return
        decision = may_start_work(state)
        if not decision.allowed:
            self._deny_for_control(decision, call_id=call_id, method=method)
        for target_ref in self._call_control_refs(call_id, method, params):
            target_decision = may_continue_task(state, target_ref)
            if not target_decision.allowed:
                self._deny_for_control(target_decision, call_id=call_id, method=method)

    def _call_control_refs(self, call_id: Any, method: str, params: dict[str, Any]) -> tuple[str, ...]:
        refs: list[str] = []

        def add(value: Any) -> None:
            if isinstance(value, str) and value and value not in refs:
                refs.append(value)

        add(str(call_id) if call_id is not None else "")
        add(f"{method}:{call_id}" if call_id is not None else "")
        for key in ("label", "agent_id", "profile", "name", "title", "prompt"):
            add(params.get(key))
        labels = params.get("labels")
        if isinstance(labels, (list, tuple)):
            for label in labels:
                add(label)
        if method == "kanban_agent" and call_id is not None:
            add(self._kanban_idempotency_key(call_id))
            add(kanban_card_id(self._kanban_idempotency_key(call_id)))
        return tuple(refs)

    def _emit(self, event: dict[str, Any]) -> None:
        if self._journal is not None:
            self._journal({"ts": utc_now_iso(), **event})

    def _should_transcribe(self, method: Any, params: dict[str, Any]) -> bool:
        return self._transcripts is not None and method in {"agent", "kanban_agent"}

    def _transcript_started(self, call_id: Any, method: Any, params: dict[str, Any], started_at: str) -> None:
        if self._transcripts is None or not isinstance(method, str):
            return
        try:
            self._transcripts.started(call_id, method, params, started_at=started_at)
        except Exception:  # noqa: BLE001 — transcript artifacts are best-effort.
            pass

    def _transcript_result(
        self,
        call_id: Any,
        method: Any,
        params: dict[str, Any],
        started_at: str,
        start_time: float,
        value: Any,
    ) -> None:
        if self._transcripts is None or not isinstance(method, str):
            return
        try:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            self._transcripts.result(
                call_id,
                method,
                params,
                started_at=started_at,
                completed_at=utc_now_iso(),
                duration_ms=duration_ms,
                value=value,
            )
        except Exception:  # noqa: BLE001 — transcript artifacts are best-effort.
            pass

    def _transcript_error(
        self,
        call_id: Any,
        method: Any,
        params: dict[str, Any],
        started_at: str,
        start_time: float,
        error_code: str,
    ) -> None:
        if self._transcripts is None or not isinstance(method, str):
            return
        try:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            self._transcripts.error(
                call_id,
                method,
                params,
                started_at=started_at,
                completed_at=utc_now_iso(),
                duration_ms=duration_ms,
                error_code=error_code,
            )
        except Exception:  # noqa: BLE001 — transcript artifacts are best-effort.
            pass

    def _transcript_cache_hit(self, call_id: Any, method: Any, params: dict[str, Any], value: Any) -> None:
        if self._transcripts is None or not isinstance(method, str):
            return
        try:
            self._transcripts.cache_hit(call_id, method, params, at=utc_now_iso(), value=value)
        except Exception:  # noqa: BLE001 — transcript artifacts are best-effort.
            pass

    def handle(self, frame: dict[str, Any]) -> dict[str, Any]:
        """Validate and dispatch one ``call`` frame; return its ``ret`` frame."""
        call_id = frame.get("id")
        method = frame.get("method")
        params = frame.get("params") if isinstance(frame.get("params"), dict) else {}
        transcript_started_at: Optional[str] = None
        transcript_start = 0.0

        try:
            with self._lock:
                self._rpc_calls += 1
                if self._rpc_calls > self._limits.max_rpc_calls:
                    self.should_abort = True
                    self.abort_reason = "aborted: capability hard-limit exceeded"
                    raise CapabilityDenied(
                        f"max_rpc_calls ({self._limits.max_rpc_calls}) exceeded", code="limit_rpc"
                    )
                if method not in _ALLOWED_METHODS:
                    raise CapabilityDenied(f"method {method!r} is not allowed", code="unknown_method")
            self._check_run_control(call_id, method, params)

            # Prompt-agent calls are resumable by a semantic fingerprint over the
            # prompt/options. Prefer that cache over ordinal call-id replay so a
            # matching completed live child call can be reused without respawning.
            if method == "agent" and "prompt" in params:
                prompt_replayed = self._maybe_prompt_cache_hit(call_id, params)
                if prompt_replayed is not _MISS:
                    return prompt_replayed

            # Replay: serve a deterministic call from the cache instead of
            # re-dispatching. A hit returns the recorded value without touching
            # the runner; a method/args drift fails closed; a miss falls through
            # to a live dispatch (the call was non-replayable in the source run).
            if self._replay is not None:
                replayed = self._maybe_replay(call_id, method, params)
                if replayed is not _MISS:
                    return replayed

            if self._should_transcribe(method, params):
                transcript_started_at = utc_now_iso()
                transcript_start = time.monotonic()
                self._transcript_started(call_id, method, params, transcript_started_at)
            value = self._dispatch(call_id, method, params)
            if method in ("agent", "kanban_agent"):
                self._check_result_size(call_id, method, value)
            if transcript_started_at is not None:
                self._transcript_result(call_id, method, params, transcript_started_at, transcript_start, value)
            # The effect has already happened. Persist (replay cache + journal)
            # on a best-effort basis *after* building the success frame so a
            # disk/IO failure here never masquerades as a runner failure for a
            # call that actually succeeded (and may have produced an external
            # side effect a live runner cannot take back).
            ret = {"t": rpc.T_RET, "id": call_id, "ok": True, "value": value, "budget": self._budget_info()}
            self._persist_success(call_id, method, params, value)
            return ret
        except CapabilityDenied as denied:
            if transcript_started_at is not None:
                self._transcript_error(call_id, method, params, transcript_started_at, transcript_start, denied.code)
            self._emit(
                self._call_event(call_id, method, params, ok=False, error=denied.code, retryable=denied.retryable)
            )
            return {
                "t": rpc.T_RET, "id": call_id, "ok": False,
                "error": {"code": denied.code, "message": str(denied), "retryable": denied.retryable},
                "budget": self._budget_info(),
            }
        except KeyboardInterrupt:
            raise  # let a genuine operator interrupt propagate.
        except BaseException as exc:  # noqa: BLE001 — an AgentRunner (even one raising
            # SystemExit/CancelledError) must NOT escape and crash the parent run; it is
            # contained here and reported to the script as a structured error. Unlike a
            # CapabilityDenied contract violation, this is a property of one dispatch
            # attempt against a live runner, not of the call's arguments — retryable=True
            # (issue #103) so a script may catch it and choose to retry or degrade.
            if transcript_started_at is not None:
                self._transcript_error(call_id, method, params, transcript_started_at, transcript_start, "runner_error")
            self._emit(
                self._call_event(call_id, method, params, ok=False, error="runner_error", retryable=True)
            )
            return {
                "t": rpc.T_RET, "id": call_id, "ok": False,
                "error": {
                    "code": "runner_error",
                    "message": f"{type(exc).__name__} raised while dispatching brokered call",
                    "retryable": True,
                },
                "budget": self._budget_info(),
            }

    def _check_result_size(self, call_id: Any, method: str, value: Any) -> None:
        """Fail closed on an over-limit ``agent``/``kanban_agent`` result.

        Mirrors the capability-result bound (:func:`capabilities._bound_result`):
        a huge child result must not land inline in script memory nor unbounded
        in the replay cache/prompt-agent cache. Unlike the capability path there
        is no field-level clipping — this is a hard ceiling, not a truncation, so
        the denial is deterministic and metadata-only (observed size, limit, call
        id; never the payload itself). Called *before* ``_persist_success`` so a
        cache write for this call never happens.

        The measurement uses the exact encoder settings the persistence paths
        use (:class:`script_store.CallRecorder.record` and the ``ret`` frame in
        :mod:`rpc`): ``ensure_ascii=False`` so it does not over-count multi-byte
        UTF-8 as ``\\uXXXX`` escapes, and ``default=str`` so a non-JSON-native
        value is measured the same way it would actually be persisted rather
        than bypassing the bound. ``sort_keys`` is omitted — ordering is
        irrelevant to size, and sorting raises ``TypeError`` on a dict with
        mixed-type (e.g. non-string) keys even though both persistence paths
        serialize such a dict without issue. If serialization still raises
        (a value ``default=str`` cannot stringify), that fails closed too,
        mirroring :func:`capabilities._json_bytes`'s ``capability_result_invalid``.
        """
        limit = self._limits.max_result_bytes
        try:
            encoded = json.dumps(
                value, ensure_ascii=False, separators=(",", ":"), default=str
            ).encode("utf-8")
        except TypeError as exc:
            raise CapabilityDenied(
                f"{method} result for call {call_id!r} is not JSON-safe: {exc}",
                code="result_invalid",
            ) from exc
        observed = len(encoded)
        if observed <= limit:
            return
        raise CapabilityDenied(
            f"{method} result for call {call_id!r} is {observed} bytes, "
            f"exceeding max_result_bytes ({limit})",
            code="result_too_large",
        )

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
        if method == "agent" and "prompt" in params:
            try:
                request = _prompt_agent_request(params)
                fingerprint, args_hash = _prompt_agent_cache_identity(request)
            except Exception:  # noqa: BLE001 — prompt metadata is best-effort after success.
                request = None
                fingerprint = ""
                args_hash = ""
            if request is not None and isinstance(value, dict):
                with self._lock:
                    self._prompt_results[fingerprint] = (args_hash, value)
                    if (
                        self._recorder is not None
                        and fingerprint not in self._recorded_prompt_fingerprints
                    ):
                        try:
                            self._recorder.record_prompt(fingerprint, method, args_hash, value)
                            self._recorded_prompt_fingerprints.add(fingerprint)
                        except Exception:  # noqa: BLE001 — persistence is best-effort.
                            pass
                try:
                    self._emit_prompt_agent_event(
                        "agent_result", call_id, request, fingerprint, ok=True, has_value=True
                    )
                except Exception:  # noqa: BLE001 — journaling is best-effort.
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

    def started_event(self, frame: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Return a metadata-only start marker for parallel child calls.

        Sequential calls keep the historical one-result-event journal shape.
        Workflow-script ``parallel()`` and ``pipeline()`` calls annotate child
        RPC frames with private index parameters; those calls get a separate
        start marker so operators can see real dispatch timing without exposing
        raw inputs.
        """
        params = frame.get("params") if isinstance(frame.get("params"), dict) else {}
        if not any(
            key in params
            for key in ("_parallel_index", "_pipeline_item_index", "_pipeline_stage_index")
        ):
            return None
        event = self._call_event(
            frame.get("id"),
            frame.get("method"),
            params,
            ok=True,
            event_type="rpc_call_start",
        )
        if "_parallel_index" in params:
            event["parallel_index"] = params.get("_parallel_index")
        return event

    def _maybe_prompt_cache_hit(self, call_id: Any, params: dict[str, Any]) -> Any:
        """Serve a completed ``agent(prompt, opts)`` result by semantic fingerprint."""
        request = _prompt_agent_request(params)
        fingerprint, args_hash = _prompt_agent_cache_identity(request)
        value: Optional[dict[str, Any]] = None
        entry_args_hash: Optional[str] = None
        cache_source: Optional[str] = None

        with self._lock:
            cached = self._prompt_results.get(fingerprint)
        if cached is not None:
            entry_args_hash, value = cached
            cache_source = "run"
        elif self._replay is not None:
            entry = self._replay.get_prompt(fingerprint)
            if entry is not None:
                if entry.method != "agent":
                    self.should_abort = True
                    self.abort_reason = f"prompt replay drift at fingerprint {fingerprint}: recorded method {entry.method!r}"
                    raise CapabilityDenied(self.abort_reason, code="replay_mismatch")
                entry_args_hash = entry.args_hash
                if not isinstance(entry.value, dict):
                    self.should_abort = True
                    self.abort_reason = f"prompt replay drift at fingerprint {fingerprint}: cached value is not an object"
                    raise CapabilityDenied(self.abort_reason, code="replay_mismatch")
                value = entry.value
                cache_source = "replay"

        if value is None or entry_args_hash is None:
            return _MISS
        if entry_args_hash != args_hash:
            self.should_abort = True
            self.abort_reason = (
                f"prompt replay drift at fingerprint {fingerprint}: recorded arguments do not match"
            )
            raise CapabilityDenied(self.abort_reason, code="replay_mismatch")
        _validate_output(value, request.schema)
        self._check_start_control(call_id, "agent", params)
        with self._lock:
            self._check_token_budget()
            self._agent_calls += 1
            if self._agent_calls > self._limits.max_agent_calls:
                raise CapabilityDenied(f"max_agent_calls ({self._limits.max_agent_calls}) exceeded", code="limit_agent")
            usage = _non_negative_token_usage(value)
            if usage is not None:
                self._tokens += usage
            self._replayed_calls += 1
        self._emit_prompt_agent_event(
            "agent_cache_hit", call_id, request, fingerprint, ok=True, cache=cache_source
        )
        self._emit(self._call_event(call_id, "agent", params, ok=True, replayed=True))
        if self._should_transcribe("agent", params):
            self._transcript_cache_hit(call_id, "agent", params, value)
        return {"t": rpc.T_RET, "id": call_id, "ok": True, "value": value, "budget": self._budget_info()}

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
            with self._lock:
                self.should_abort = True
                self.abort_reason = (
                    f"replay drift at call {call_id}: recorded {entry.method!r} does not "
                    f"match {method!r} (or arguments changed)"
                )
                abort_reason = self.abort_reason
            raise CapabilityDenied(abort_reason, code="replay_mismatch")
        # Hit. Mirror the live accounting so cap-/budget-gated control flow in the
        # script reproduces the recorded run:
        #  * advance the soft per-method counters, so a later *non-cached* call
        #    still trips max_agent_calls / max_kanban_calls at the same point it
        #    did on the recorded run;
        #  * re-apply the recorded non-negative token spend (a negative/absent
        #    value is ignored — the hard cap is not re-enforced on a faithful
        #    replay, so a tampered _tokens must not skew the budget downward).
        with self._lock:
            if method == "agent":
                self._agent_calls += 1
            elif method == "kanban_agent":
                self._kanban_calls += 1
            elif method == "capability":
                self._capability_calls += 1
            if method in ("agent", "kanban_agent", "capability") and isinstance(entry.value, dict):
                usage = _non_negative_token_usage(entry.value)
                if usage is not None:
                    self._tokens += usage
            self._replayed_calls += 1
            budget = self._budget_info()
        if self._should_transcribe(method, params):
            self._transcript_cache_hit(call_id, method, params, entry.value)
        self._emit(self._call_event(call_id, method, params, ok=True, replayed=True))
        return {"t": rpc.T_RET, "id": call_id, "ok": True, "value": entry.value, "budget": budget}

    def _dispatch(self, call_id: Any, method: str, params: dict[str, Any]) -> Any:
        if method == "log":
            return None  # journaling is handled by _emit; nothing to return.
        if method == "phase":
            return None
        if method == "agent":
            return self._handle_agent(call_id, params)
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

        self._check_start_control(call_id, "capability", params)
        raw_name = params.get("name")
        if not isinstance(raw_name, str) or not raw_name:
            raise CapabilityDenied("capability call requires a non-empty 'name'", code="bad_request")
        try:
            name = normalize_capability_name(raw_name)
        except ValueError as exc:
            raise CapabilityDenied(str(exc), code="bad_request") from exc
        with self._lock:
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
        usage = _non_negative_token_usage(result)
        if usage is not None:
            with self._lock:
                self._tokens += usage
        _validate_output(result, params.get("schema"))
        return result

    def _check_token_budget(self) -> None:
        """Hard ceiling: once the token budget is spent, deny further effects."""
        budget = self._limits.token_budget
        if budget is not None and self._tokens >= budget:
            self.should_abort = True
            raise CapabilityDenied(f"token_budget ({budget}) exhausted", code="limit_token")

    def _handle_agent(self, call_id: Any, params: dict[str, Any]) -> dict[str, Any]:
        if "prompt" in params:
            self._check_start_control(call_id, "agent", params)
            return self._handle_prompt_agent(call_id, params)

        agent_id = params.get("agent_id")
        if not isinstance(agent_id, str) or not agent_id:
            raise CapabilityDenied("agent call requires a non-empty 'agent_id'", code="bad_request")
        if is_kanban_runner_id(agent_id):
            raise CapabilityDenied(
                f"reserved kanban runner id {agent_id!r} must be reached via kanban_agent", code="reserved_agent"
            )
        if not is_known_agent(agent_id):
            raise CapabilityDenied(f"unknown agent id {agent_id!r}", code="unknown_agent")
        self._check_start_control(call_id, "agent", params)
        with self._lock:
            self._check_token_budget()
            self._agent_calls += 1
            if self._agent_calls > self._limits.max_agent_calls:
                # Soft denial: the script may catch CapabilityError and adapt. The
                # max_rpc_calls hard cap is the runaway backstop that aborts the VM.
                raise CapabilityDenied(f"max_agent_calls ({self._limits.max_agent_calls}) exceeded", code="limit_agent")
        payload = params.get("input") if isinstance(params.get("input"), dict) else {}
        return self._invoke(agent_id, payload, params.get("schema"))

    def _handle_prompt_agent(self, call_id: Any, params: dict[str, Any]) -> dict[str, Any]:
        """Dispatch ``agent(prompt, opts)`` through an injected child-agent runner."""

        request = _prompt_agent_request(params)
        with self._lock:
            self._check_token_budget()
            # Count the script-visible prompt agent call once; schema retries below may
            # cross the child-runner boundary again but remain bounded by max_schema_retries.
            self._agent_calls += 1
            if self._agent_calls > self._limits.max_agent_calls:
                raise CapabilityDenied(f"max_agent_calls ({self._limits.max_agent_calls}) exceeded", code="limit_agent")
        if self._child_runner is None:
            raise CapabilityDenied(
                "no child agent runner configured for prompt agent calls", code="child_agent_unavailable"
            )

        fingerprint, _args_hash = _prompt_agent_cache_identity(request)
        self._emit_prompt_agent_event("agent_started", call_id, request, fingerprint, ok=True)
        return self._invoke_prompt_agent_with_schema_retry(call_id, request)

    def _invoke_prompt_agent_with_schema_retry(
        self, call_id: Any, request: ChildAgentRequest
    ) -> dict[str, Any]:
        """Invoke a prompt child agent, retrying schema-invalid structured output."""

        assert self._child_runner is not None
        retry_limit = max(0, int(self._limits.max_schema_retries))
        attempts = retry_limit + 1
        base_context = dict(request.context)
        last_error: Optional[CapabilityDenied] = None

        for attempt in range(1, attempts + 1):
            effective_request = request
            if last_error is not None:
                effective_request = _request_with_schema_retry_context(
                    request,
                    base_context=base_context,
                    attempt=attempt,
                    max_retries=retry_limit,
                    error=last_error,
                )
            try:
                output = self._child_runner(effective_request)
                if not isinstance(output, dict):
                    raise CapabilityDenied(
                        f"prompt child agent returned {type(output).__name__}, expected dict", code="schema"
                    )
                safe_output = redact_credentials(_json_safe(output))
                assert isinstance(safe_output, dict)
                _validate_output(safe_output, effective_request.schema)
            except CapabilityDenied as exc:
                if exc.code != "schema" or attempt >= attempts:
                    if exc.code == "schema" and attempt >= attempts:
                        raise CapabilityDenied(
                            f"schema validation failed after {attempts} attempt(s): {exc}", code="schema"
                        ) from exc
                    raise
                self._record_schema_retry(call_id, request, attempt, retry_limit)
                last_error = exc
                continue

            usage = _non_negative_token_usage(safe_output)
            if usage is not None:
                with self._lock:
                    self._tokens += usage
            return safe_output

        raise CapabilityDenied("schema validation failed after retry exhaustion", code="schema")

    def _record_schema_retry(
        self,
        call_id: Any,
        request: ChildAgentRequest,
        attempt: int,
        max_retries: int,
    ) -> None:
        """Journal a redacted schema-retry attempt for progress/status consumers."""
        event = self._call_event(
            call_id,
            "agent",
            {"agent_id": "prompt", "label": request.label, "phase": request.phase},
            ok=False,
            error="schema_retry",
        )
        event["attempt"] = attempt
        event["max_retries"] = max_retries
        self._emit(event)

    def _handle_kanban(self, call_id: Any, params: dict[str, Any]) -> dict[str, Any]:
        profile = params.get("profile")
        if not isinstance(profile, str) or not profile:
            raise CapabilityDenied("kanban_agent call requires a non-empty 'profile'", code="bad_request")
        try:
            on_block = normalize_on_block(params.get("on_block"))
        except ValueError as exc:
            raise CapabilityDenied(str(exc), code="bad_request") from exc
        self._check_start_control(call_id, "kanban_agent", params)
        with self._lock:
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
        with self._lock:
            self.should_suspend = True
            self.suspend_info = {
                "card_id": card_id,
                "profile": profile,
                "call_id": call_id,
                "on_block": on_block,
            }

    def control_state(self) -> tuple[
        bool, Optional[str], bool, Optional[dict[str, Any]], bool, Optional[dict[str, Any]], bool, Optional[dict[str, Any]]
    ]:
        """Return broker terminal control flags under one lock for driver threads."""
        with self._lock:
            suspend_info = dict(self.suspend_info) if self.suspend_info is not None else None
            pause_info = dict(self.pause_info) if self.pause_info is not None else None
            stop_info = dict(self.stop_info) if self.stop_info is not None else None
            return (
                self.should_abort,
                self.abort_reason,
                self.should_suspend,
                suspend_info,
                self.should_pause,
                pause_info,
                self.should_stop,
                stop_info,
            )

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
        usage = _non_negative_token_usage(resolution.result or {})
        if usage is not None:
            with self._lock:
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
        usage = _non_negative_token_usage(output)
        if usage is not None:
            with self._lock:
                self._tokens += usage
        return output

    def _emit_prompt_agent_event(
        self,
        event_type: str,
        call_id: Any,
        request: ChildAgentRequest,
        fingerprint: str,
        *,
        ok: bool,
        cache: Optional[str] = None,
        has_value: Optional[bool] = None,
    ) -> None:
        event: dict[str, Any] = {
            "type": event_type,
            "call_id": call_id,
            "method": "agent",
            "fingerprint": fingerprint,
            "ok": ok,
        }
        if request.label:
            event["label"] = safe_capability_metadata_value(request.label)
        if request.phase:
            event["phase"] = safe_capability_metadata_value(request.phase)
        if cache is not None:
            event["cache"] = cache
        if has_value is not None:
            event["has_value"] = has_value
        self._emit(event)

    def _call_event(
        self,
        call_id: Any,
        method: Any,
        params: dict[str, Any],
        *,
        ok: bool,
        error: Optional[str] = None,
        retryable: Optional[bool] = None,
        replayed: bool = False,
        event_type: str = "rpc_call",
    ) -> dict[str, Any]:
        event: dict[str, Any] = {"type": event_type, "call_id": call_id, "method": method, "ok": ok}
        if method in ("agent",):
            event["agent_id"] = params.get("agent_id")
            if "prompt" in params:
                try:
                    request = _prompt_agent_request(params)
                    event["fingerprint"] = _prompt_agent_cache_identity(request)[0]
                except CapabilityDenied:
                    pass
        if method in ("kanban_agent",):
            event["profile"] = params.get("profile")
        if method in ("capability",):
            event["capability"] = safe_capability_metadata_value(params.get("name"))
        if method in ("phase",):
            event["phase_title"] = safe_capability_metadata_value(params.get("title"))
        if params.get("label"):
            event["label"] = safe_capability_metadata_value(params.get("label"))
        if "_parallel_index" in params:
            event["parallel_index"] = params.get("_parallel_index")
        if "_pipeline_item_index" in params:
            event["pipeline_item_index"] = params.get("_pipeline_item_index")
        if "_pipeline_stage_index" in params:
            event["pipeline_stage_index"] = params.get("_pipeline_stage_index")
        if params.get("phase"):
            event["phase"] = safe_capability_metadata_value(params.get("phase"))
        if error:
            event["error"] = error
            # retryable is call-classification metadata (issue #103), journaled
            # alongside the error code so replay/audit consumers see the same
            # classification a script observed via ``CapabilityError.retryable``.
            if retryable is not None:
                event["retryable"] = retryable
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


def _non_negative_token_usage(output: dict[str, Any]) -> Optional[int]:
    """Return valid broker token usage, ignoring bools and negative values."""
    usage = output.get("_tokens")
    if isinstance(usage, int) and not isinstance(usage, bool) and usage >= 0:
        return usage
    return None


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


def _prompt_agent_request(params: dict[str, Any]) -> ChildAgentRequest:
    """Validate and normalize the parent-side prompt-agent request."""
    prompt = params.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip():
        raise CapabilityDenied("prompt agent call requires a non-empty 'prompt'", code="bad_request")
    # parallel()/pipeline() annotate child frames with private dispatch-index
    # params; they are internal scheduling metadata, not script-supplied options.
    unknown = sorted(
        set(params)
        - ({"prompt"} | CHILD_AGENT_OPTION_KEYS
           | {"_parallel_index", "_pipeline_item_index", "_pipeline_stage_index"})
    )
    if unknown:
        raise CapabilityDenied(
            "unsupported prompt agent option(s): " + ", ".join(unknown), code="bad_request"
        )
    return ChildAgentRequest(
        prompt=prompt,
        label=_optional_str(params, "label"),
        phase=_optional_str(params, "phase"),
        schema=_optional_dict(params, "schema"),
        model=_optional_str(params, "model"),
        effort=_optional_str(params, "effort"),
        isolation=_optional_str(params, "isolation"),
        context=_optional_dict(params, "context") or {},
    )


def _prompt_agent_fingerprint_payload(request: ChildAgentRequest) -> dict[str, Any]:
    """Semantic prompt-agent identity payload.

    All currently supported options are semantic for child-agent dispatch, so none
    are excluded: label, phase, schema, model, effort, isolation, and context all
    participate alongside the prompt. Omitted and explicit ``None`` normalize to
    the same JSON ``null`` value.
    """
    return {
        "prompt": request.prompt,
        "label": request.label,
        "phase": request.phase,
        "schema": request.schema,
        "model": request.model,
        "effort": request.effort,
        "isolation": request.isolation,
        "context": request.context,
    }


def _prompt_agent_cache_identity(request: ChildAgentRequest) -> tuple[str, str]:
    payload = _prompt_agent_fingerprint_payload(request)
    fingerprint = "v2:" + canonical_hash(
        {"kind": "agent(prompt,opts)", "version": 2, "request": payload}
    )
    args_hash = canonical_hash({"method": "agent", "fingerprint": fingerprint, "request": payload})
    return fingerprint, args_hash


def _request_with_schema_retry_context(
    request: ChildAgentRequest,
    *,
    base_context: dict[str, Any],
    attempt: int,
    max_retries: int,
    error: CapabilityDenied,
) -> ChildAgentRequest:
    """Return ``request`` with validation-error context for a retry attempt."""
    context = dict(base_context)
    context["schema_validation_error"] = {
        "attempt": attempt,
        "max_retries": max_retries,
        "code": error.code,
        "message": str(error),
    }
    return ChildAgentRequest(
        prompt=request.prompt,
        label=request.label,
        phase=request.phase,
        schema=request.schema,
        model=request.model,
        effort=request.effort,
        isolation=request.isolation,
        context=context,
    )


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
        transcripts: Optional[TranscriptRecorder] = None,
        replay: Optional[ReplayCache] = None,
        deterministic_runner: bool = False,
        kanban_backend: Optional[KanbanBackend] = None,
        idempotency_root: str = "",
        active_run_id: Optional[str] = None,
        capability_registry: Optional[CapabilityRegistry] = None,
        capability_policy: Optional[CapabilityPolicy] = None,
        control_store: Optional[ControlStore] = None,
    ) -> None:
        self._runner = agent_runner if agent_runner is not None else StubAgentRunner()
        self._child_runner = child_agent_runner
        self._limits = limits if limits is not None else VMLimits()
        self._journal = journal
        self._python = python_executable or sys.executable
        # Durable-store wiring (issue #3): forwarded to the per-run broker.
        self._recorder = recorder
        self._transcripts = transcripts
        self._replay = replay
        self._deterministic_runner = deterministic_runner
        # Durable Kanban awaitable wiring (issue #5): forwarded to the broker.
        self._kanban_backend = kanban_backend
        self._idempotency_root = idempotency_root
        self._active_run_id = active_run_id or idempotency_root
        # Generic host-owned capability API wiring (issue #29): forwarded to the broker.
        self._capability_registry = capability_registry
        self._capability_policy = capability_policy
        self._control_store = control_store

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
            transcripts=self._transcripts,
            replay=self._replay,
            deterministic_runner=self._deterministic_runner,
            kanban_backend=self._kanban_backend,
            idempotency_root=self._idempotency_root,
            active_run_id=self._active_run_id,
            capability_registry=self._capability_registry,
            capability_policy=self._capability_policy,
            control_store=self._control_store,
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
        paused: Optional[dict[str, Any]] = None
        stopped: Optional[dict[str, Any]] = None
        parent_calls_still_running: list[dict[str, Any]] = []
        parent_calls_cancelled_before_start: list[dict[str, Any]] = []
        state_lock = threading.Lock()
        child_terminal = threading.Event()
        run_deadline = time.monotonic() + self._limits.max_runtime_s

        def _call_info(frame: dict[str, Any]) -> dict[str, Any]:
            params = frame.get("params") if isinstance(frame.get("params"), dict) else {}
            info: dict[str, Any] = {"call_id": frame.get("id"), "method": frame.get("method")}
            if "_parallel_index" in params:
                info["parallel_index"] = params.get("_parallel_index")
            if frame.get("method") == "agent":
                info["agent_id"] = params.get("agent_id") or params.get("prompt")
            elif frame.get("method") == "kanban_agent":
                info["profile"] = params.get("profile")
            elif frame.get("method") == "capability":
                info["capability"] = safe_capability_metadata_value(params.get("name"))
            return info

        def _set_protocol_error(message: str) -> None:
            nonlocal protocol_error
            with state_lock:
                if protocol_error is None and not child_terminal.is_set():
                    protocol_error = message

        def _set_suspended(info: dict[str, Any]) -> None:
            nonlocal suspended
            with state_lock:
                if suspended is None and not child_terminal.is_set():
                    suspended = dict(info)

        def _set_paused(info: dict[str, Any]) -> None:
            nonlocal paused
            with state_lock:
                if paused is None and not child_terminal.is_set():
                    paused = dict(info)

        def _set_stopped(info: dict[str, Any]) -> None:
            nonlocal stopped
            with state_lock:
                if stopped is None and not child_terminal.is_set():
                    stopped = dict(info)

        def _state_snapshot() -> tuple[
            Optional[str], Optional[dict[str, Any]], Optional[dict[str, Any]], Optional[dict[str, Any]]
        ]:
            with state_lock:
                return (
                    protocol_error,
                    dict(suspended) if suspended is not None else None,
                    dict(paused) if paused is not None else None,
                    dict(stopped) if stopped is not None else None,
                )

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
                    "max_parallel": self._limits.max_parallel,
                    "max_runtime_s": self._limits.max_runtime_s,
                    "max_schema_retries": self._limits.max_schema_retries,
                },
                "budget": {"total": self._limits.token_budget, "spent": 0,
                           "remaining": self._limits.token_budget},
            }
            booted = True
            try:
                rpc.write_frame(proc.stdin, boot)
            except (BrokenPipeError, OSError, ValueError) as exc:
                _set_protocol_error(f"child closed stdin before boot: {exc}")
                booted = False

            ret_executor = ThreadPoolExecutor(max_workers=max(1, int(self._limits.max_parallel or 1)))
            ret_write_lock = threading.Lock()
            pending_rets: set[Future[Any]] = set()
            pending_info: dict[Future[Any], dict[str, Any]] = {}

            def _handle_and_reply(call_frame: dict[str, Any]) -> None:
                ret = broker.handle(call_frame)
                try:
                    with ret_write_lock:
                        rpc.write_frame(proc.stdin, ret)
                except (BrokenPipeError, OSError, ValueError) as exc:
                    _set_protocol_error(f"child stdin closed: {exc}")
                    return
                (
                    should_abort,
                    abort_reason,
                    should_suspend,
                    suspend_info,
                    should_pause,
                    pause_info,
                    should_stop,
                    stop_info,
                ) = broker.control_state()
                if should_stop:
                    _kill(proc)
                    _set_stopped(stop_info or {})
                if should_pause:
                    _kill(proc)
                    _set_paused(pause_info or {})
                if should_abort:
                    _kill(proc)
                    _set_protocol_error(abort_reason or "aborted: capability hard-limit exceeded")
                if should_suspend:
                    # An unresolved paused Kanban await suspended the run: tear
                    # the subprocess down (the script's local state is discarded;
                    # a resume re-runs it from the replay cache) and report a
                    # resumable suspended run rather than a failure.
                    _kill(proc)
                    _set_suspended(suspend_info or {})

            def _collect_done_futures(futures: set[Future[Any]]) -> None:
                for fut in futures:
                    exc = fut.exception()
                    if exc is not None:
                        _set_protocol_error(f"broker reply worker failed: {type(exc).__name__}: {exc}")

            try:
                while booted:
                    done_futures = {fut for fut in pending_rets if fut.done()}
                    pending_rets.difference_update(done_futures)
                    _collect_done_futures(done_futures)
                    state_error, state_suspended, state_paused, state_stopped = _state_snapshot()
                    if (
                        state_error is not None
                        or state_suspended is not None
                        or state_paused is not None
                        or state_stopped is not None
                    ):
                        break
                    try:
                        frame = rpc.read_frame(proc.stdout)
                    except rpc.RPCProtocolError as exc:
                        _set_protocol_error(str(exc))
                        break
                    if frame is None:
                        break  # EOF: child exited (cleanly after done, or crashed).
                    kind = frame.get("t")
                    if kind == rpc.T_READY:
                        meta = frame.get("meta")
                    elif kind == rpc.T_CALL:
                        started = broker.started_event(frame)
                        if started is not None:
                            broker._emit(started)
                        fut = ret_executor.submit(_handle_and_reply, frame)
                        pending_rets.add(fut)
                        pending_info[fut] = _call_info(frame)
                    elif kind == rpc.T_DONE:
                        done = frame
                        child_terminal.set()
                        break
                    # Unknown frame types are ignored (forward-compatible).
            finally:
                if pending_rets:
                    remaining = max(0.0, run_deadline - time.monotonic())
                    finished, unfinished = wait(pending_rets, timeout=remaining)
                    _collect_done_futures(finished)
                    for fut in unfinished:
                        info = pending_info.get(fut, {"call_id": None, "method": "unknown"})
                        if fut.cancel():
                            parent_calls_cancelled_before_start.append(info)
                        else:
                            parent_calls_still_running.append(info)
                ret_executor.shutdown(wait=False, cancel_futures=True)
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

        state_protocol_error, state_suspended, state_paused, state_stopped = _state_snapshot()
        if parent_calls_still_running:
            error: dict[str, Any] = {
                "type": "WorkflowSubprocessError",
                "message": (
                    "parent-side RPC work is still running after workflow terminal state; "
                    "running ThreadPoolExecutor calls cannot be cancelled"
                ),
                "parent_calls_still_running": parent_calls_still_running,
            }
            if parent_calls_cancelled_before_start:
                error["parent_calls_cancelled_before_start"] = parent_calls_cancelled_before_start
            if isinstance(done, dict) and done.get("error") is not None:
                error["child_error"] = done.get("error")
            return ScriptRunResult(
                ok=False, meta=meta, calls=calls, exit_code=exit_code, stderr=stderr_text, error=error,
            )
        if timed_out.is_set() and done is None and state_suspended is None:
            return ScriptRunResult(
                ok=False, meta=meta, calls=calls, exit_code=exit_code, stderr=stderr_text,
                error={"type": "WorkflowSubprocessError",
                       "message": f"workflow timed out after {self._limits.max_runtime_s}s"},
            )
        if state_stopped is not None:
            return ScriptRunResult(
                ok=False, stopped=True, meta=meta, calls=calls,
                exit_code=exit_code, stderr=stderr_text,
                error={"type": "WorkflowStopped", **state_stopped},
            )
        if state_paused is not None:
            return ScriptRunResult(
                ok=False, paused=True, meta=meta, calls=calls,
                exit_code=exit_code, stderr=stderr_text,
                error={"type": "WorkflowPaused", **state_paused},
            )
        if state_suspended is not None:
            # Durable, resumable suspension (issue #5) — not a failure. The error
            # payload is metadata-safe (card id is a content-address, profile is a
            # role name) so it can live on the operator-facing run.json.
            return ScriptRunResult(
                ok=False, suspended=True, meta=meta, calls=calls,
                exit_code=exit_code, stderr=stderr_text,
                error={"type": "KanbanSuspended", **state_suspended},
            )
        if state_protocol_error is not None:
            return ScriptRunResult(
                ok=False, meta=meta, calls=calls, exit_code=exit_code, stderr=stderr_text,
                error={"type": "WorkflowSubprocessError", "message": state_protocol_error},
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
        "max_parallel": limits.max_parallel,
        "max_runtime_s": limits.max_runtime_s,
        "allow_nested_workflows": limits.allow_nested_workflows,
        "token_budget": limits.token_budget,
        "max_schema_retries": limits.max_schema_retries,
        "kanban_suspend_after_s": limits.kanban_suspend_after_s,
        "max_result_bytes": limits.max_result_bytes,
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
        max_parallel=_req_int(view, "max_parallel", default.max_parallel),
        max_runtime_s=_req_float(view, "max_runtime_s", default.max_runtime_s),
        allow_nested_workflows=_req_bool(
            view, "allow_nested_workflows", default.allow_nested_workflows
        ),
        token_budget=_req_token_budget(view, default.token_budget),
        max_schema_retries=_req_int(view, "max_schema_retries", default.max_schema_retries),
        kanban_suspend_after_s=_req_opt_float(
            view, "kanban_suspend_after_s", default.kanban_suspend_after_s
        ),
        max_result_bytes=_req_int(view, "max_result_bytes", default.max_result_bytes),
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
    control_store: Optional[ControlStore] = None,
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
            active_run_id=idempotency_root,
            capability_registry=capability_registry,
            capability_policy=capability_policy,
            control_store=control_store,
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
    transcripts = store.transcript_recorder(persist_run_id)
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
        transcripts=transcripts,
        replay=replay_cache,
        deterministic_runner=deterministic,
        kanban_backend=kanban_backend,
        idempotency_root=idempotency_root,
        active_run_id=persist_run_id,
        capability_registry=capability_registry,
        capability_policy=capability_policy,
        control_store=control_store,
    )
    result = vm.run(script, args=args, validate=False)
    result.run_id = persist_run_id
    result.journal_path = str(store.journal_path(persist_run_id))
    # A durably-suspended run (issue #5) is recorded as its own terminal status so
    # an operator/resumer can discover it (store.suspended_runs) and resume it with
    # replay_from once the awaited card produces an event. It is neither succeeded
    # nor failed.
    if result.stopped:
        status = "stopped"
    elif result.paused:
        status = "paused"
    elif result.suspended:
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
    result.transcripts = store.transcript_refs(persist_run_id)
    return result
