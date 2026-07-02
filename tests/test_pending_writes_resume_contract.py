"""Pending-writes resume contract: crash mid-``parallel()``, resume, verify (issue #109).

LangGraph documents "pending writes" recovery: siblings that already completed
within a failed superstep are preserved and are not re-executed on resume. This
project already has the underlying mechanism — each brokered call is durably
persisted (fsynced) the moment it succeeds (see
:class:`hermes_workflows.script_store.CallRecorder`), independent of whether the
*run* as a whole later fails — but the end-to-end guarantee for a crash
**mid-``parallel()``** was neither pinned by a fixture nor stated in DESIGN.md.

This fixture proves it directly: a four-branch ``parallel()`` fan-out mixes a
deterministic ``agent(agent_id, input)`` call and a semantic
``agent(prompt, opts)`` prompt-agent call that both complete, with a
deterministic call and a prompt-agent call that both fail (the injected
"controlled crash" — see ``test_parallel_width_prevents_dispatching_queued_
children_after_failure`` in ``tests/test_vm_subprocess.py`` for the established
in-repo pattern of simulating a mid-run death via a runner that raises). The run
then reports ``ok=False``. A fresh ``replay_from`` run is driven by *new* runner
instances that record every invocation: the two completed siblings must be
served without ever touching the new runners (proving cache/fingerprint
service, not just "the retry happened to return the same value"), and only the
two crashed siblings dispatch live.
"""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from hermes_workflows import ChildAgentRequest, ScriptRunStore, VMLimits, run_workflow_script

META = 'meta = {"name": "pending-writes", "description": "d"}\n'

# Four-branch fan-out: index 0 (deterministic) and index 1 (prompt-agent)
# complete; index 2 (deterministic) and index 3 (prompt-agent) are the ones
# "in flight" when the run dies mid-superstep.
SCRIPT = META + (
    "outs = await parallel([\n"
    "    lambda: agent('hermes.echo', {'i': 0}),\n"
    "    lambda: agent('summarize branch one', {'label': 'b1', 'schema': {'answer': 'string'}}),\n"
    "    lambda: agent('hermes.echo', {'i': 2}),\n"
    "    lambda: agent('summarize branch three', {'label': 'b3', 'schema': {'answer': 'string'}}),\n"
    "])\n"
    "return {'outs': outs}\n"
)


class _CrashingAgentRunner:
    """Deterministic-call runner for the doomed run: index 0 lives, index 2 dies."""

    def __init__(self) -> None:
        self.calls: list[int] = []

    def __call__(self, agent_id: str, input: dict[str, Any]) -> dict[str, Any]:  # noqa: A002
        i = input["i"]
        self.calls.append(i)
        if i == 0:
            return {"i": 0, "via": "orig-live"}
        raise RuntimeError(f"simulated crash: branch {i} never completed before the run died")


class _CrashingChildRunner:
    """Prompt-agent runner for the doomed run: branch b1 lives, branch b3 dies."""

    def __init__(self) -> None:
        self.requests: list[ChildAgentRequest] = []

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        self.requests.append(request)
        if request.label == "b1":
            return {"answer": "orig-b1", "_tokens": 1}
        raise RuntimeError(f"simulated crash: branch {request.label} never completed before the run died")


class _ResumeAgentRunner:
    """Deterministic-call runner for the resumed run: only the crashed branch may reach it."""

    def __init__(self) -> None:
        self.calls: list[int] = []

    def __call__(self, agent_id: str, input: dict[str, Any]) -> dict[str, Any]:  # noqa: A002
        i = input["i"]
        self.calls.append(i)
        return {"i": i, "via": "resumed-live"}


class _ResumeChildRunner:
    """Prompt-agent runner for the resumed run: only the crashed branch may reach it."""

    def __init__(self) -> None:
        self.requests: list[ChildAgentRequest] = []

    def __call__(self, request: ChildAgentRequest) -> dict[str, Any]:
        self.requests.append(request)
        return {"answer": f"resumed-{request.label}", "_tokens": 2}


def test_completed_parallel_siblings_are_cache_served_after_a_mid_run_crash():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")

        # -- Run A: dies mid-parallel. Branches 0 and 1 complete (and are
        #    durably persisted the instant they succeed); branches 2 and 3
        #    never do. --------------------------------------------------
        crashing_agent_runner = _CrashingAgentRunner()
        crashing_child_runner = _CrashingChildRunner()
        a = run_workflow_script(
            SCRIPT,
            store=store,
            run_id="A",
            agent_runner=crashing_agent_runner,
            child_agent_runner=crashing_child_runner,
            limits=VMLimits(max_parallel=4),
            deterministic_runner=True,  # both crashing runners are pure fns of their input.
        )
        assert a.ok is False
        # All four branches were reached by a runner before the run died (the
        # RPC calls were dispatched concurrently; only two of the four returned
        # successfully).
        assert sorted(crashing_agent_runner.calls) == [0, 2]
        assert sorted(r.label for r in crashing_child_runner.requests) == ["b1", "b3"]

        # The store's persisted journal collapses the raw broker event vocabulary
        # (``rpc_call``/``rpc_call_start``) into a single metadata-only ``call``
        # type (see ``ScriptRunStore.note_call``); ``agent_started`` stays
        # distinct so a prompt-agent call's fingerprint is always discoverable
        # even when the call itself never reaches a result/cache-hit.
        journal_a = store.journal("A")
        calls = [e for e in journal_a if e["type"] == "call" and e["method"] == "agent"]
        branch0_call_id = next(e["call_id"] for e in calls if e.get("parallel_index") == 0)
        branch2_call_id = next(e["call_id"] for e in calls if e.get("parallel_index") == 2)
        agent_started = [e for e in journal_a if e["type"] == "agent_started"]
        fingerprint_b1 = next(e["fingerprint"] for e in agent_started if e.get("label") == "b1")
        fingerprint_b3 = next(e["fingerprint"] for e in agent_started if e.get("label") == "b3")

        cache_a = store.load_cache("A")
        # The two completed siblings are durably cached...
        assert cache_a.get(branch0_call_id).value == {"i": 0, "via": "orig-live"}
        assert cache_a.get_prompt(fingerprint_b1).value == {"answer": "orig-b1", "_tokens": 1}
        # ...the two crashed ones are not (a failed call is never recorded).
        assert cache_a.get(branch2_call_id) is None
        assert cache_a.get_prompt(fingerprint_b3) is None

        # -- Resume: replay_from="A" with FRESH runner instances that have no
        #    memory of run A. If a completed branch were re-dispatched, its
        #    runner would record the call and this fixture would catch it. ---
        resume_agent_runner = _ResumeAgentRunner()
        resume_child_runner = _ResumeChildRunner()
        b = run_workflow_script(
            SCRIPT,
            store=store,
            run_id="B",
            replay_from="A",
            agent_runner=resume_agent_runner,
            child_agent_runner=resume_child_runner,
            limits=VMLimits(max_parallel=4),
        )
        assert b.ok, b.error

        # Only the two branches that never completed on run A dispatch live...
        assert resume_agent_runner.calls == [2]
        assert [r.label for r in resume_child_runner.requests] == ["b3"]
        # ...and the two completed siblings are served byte-for-byte from the
        # durable record — the resumed run never invents a fresh "resumed-*"
        # value for them.
        assert b.value == {
            "outs": [
                {"i": 0, "via": "orig-live"},
                {"answer": "orig-b1", "_tokens": 1},
                {"i": 2, "via": "resumed-live"},
                {"answer": "resumed-b3", "_tokens": 2},
            ]
        }
        # Exactly the two completed siblings were served from the cache (one
        # call-id hit, one fingerprint hit) — the replay counter is the
        # runner-invocation-counting half of the assertion made structural.
        assert b.replayed_calls == 2


def test_replay_drift_on_a_completed_sibling_aborts_the_resume_fail_closed():
    # Boundary: ``replay_from`` already refuses a script/args identity mismatch
    # up front (see ``run_script``'s script_sha256/args_hash guard), so the only
    # way a *cached* completed sibling can drift is a corrupted/tampered
    # cache.jsonl line (e.g. a cross-process write race or disk fault). The
    # pending-writes guarantee must fail closed there too — it never silently
    # serves a stale result for a call whose recorded arguments no longer match
    # (see DESIGN.md "Pending-writes resume contract" for the drift-abort rule
    # this pins).
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        agent_runner = _CrashingAgentRunner()
        child_runner = _CrashingChildRunner()
        run_workflow_script(
            SCRIPT,
            store=store,
            run_id="A",
            agent_runner=agent_runner,
            child_agent_runner=child_runner,
            limits=VMLimits(max_parallel=4),
            deterministic_runner=True,
        )
        calls = [e for e in store.journal("A") if e["type"] == "call" and e["method"] == "agent"]
        branch0_call_id = next(e["call_id"] for e in calls if e.get("parallel_index") == 0)

        # Tamper the durable cache line for the completed deterministic sibling
        # (branch 0): forge its recorded args_hash so a fresh dispatch of the
        # *same* call no longer matches what was recorded.
        cache_path = Path(tmp) / "runs" / "A" / "cache.jsonl"
        forged_lines = []
        for line in cache_path.read_text(encoding="utf-8").splitlines():
            entry = json.loads(line)
            if entry.get("call_id") == branch0_call_id:
                entry["args_hash"] = "forged-drift"
            forged_lines.append(json.dumps(entry))
        cache_path.write_text("\n".join(forged_lines) + "\n", encoding="utf-8")

        resume_agent_runner = _ResumeAgentRunner()
        resume_child_runner = _ResumeChildRunner()
        b = run_workflow_script(
            SCRIPT,
            store=store,
            run_id="B",
            replay_from="A",
            agent_runner=resume_agent_runner,
            child_agent_runner=resume_child_runner,
            limits=VMLimits(max_parallel=4),
        )
        assert b.ok is False
        # A replay drift is a hard, run-aborting mismatch (the subprocess is
        # killed rather than letting the script observe a poisoned result) —
        # see CapabilityBroker._maybe_replay's ``replay_mismatch`` guard.
        assert b.error["type"] == "WorkflowSubprocessError"
        assert "replay drift" in b.error["message"]
        # The drift is caught before the *drifted* call (branch 0) is ever
        # handed to a live runner — fail closed, not "run it live and hope".
        # This does not pin whether sibling branch 2 (a legitimate cache miss
        # the contract permits to dispatch live) reaches the runner before the
        # abort kills the run: the guest submits all four parallel call frames
        # up front, and whether branch 2's frame is read before the abort wins
        # the race is scheduling-dependent, not part of the drift-abort
        # contract.
        assert 0 not in resume_agent_runner.calls
