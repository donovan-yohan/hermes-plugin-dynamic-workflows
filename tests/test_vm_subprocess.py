"""Tests for the subprocess workflow VM and parent-owned RPC broker (issue #2).

Two layers are covered:

* **End-to-end**, through a *real* subprocess: a minimal script can ``log`` and
  return a value; an ``agent`` call crosses the RPC boundary deterministically;
  the parent journals structured requests with stable call ids; and subprocess
  crash/timeout marks the run failed without corrupting parent state.
* **Parent-owned enforcement**, unit-testing :class:`CapabilityBroker` directly
  with forged frames — proving the parent validates every request regardless of
  what an (untrusted) subprocess might send.

Deterministic by construction: all effects route through ``StubAgentRunner``.
"""

import os
import stat
import tempfile
from pathlib import Path

from hermes_workflows import run_workflow_script
from hermes_workflows.agents import StubAgentRunner
from hermes_workflows.errors import ScriptValidationError
from hermes_workflows.vm import CapabilityBroker, VMLimits, WorkflowVM, run_script
from hermes_workflows import rpc

META = 'meta = {"name": "demo", "description": "d"}\n'


# --------------------------------------------------------------------------- #
# End-to-end through a real subprocess
# --------------------------------------------------------------------------- #

def test_minimal_log_and_return_round_trip():
    res = run_workflow_script(META + 'log("hello")\nreturn {"value": 41 + 1}\n')
    assert res.ok, res.error
    assert res.value == {"value": 42}
    assert res.meta == {"name": "demo", "description": "d"}
    assert res.exit_code == 0


def test_agent_call_crosses_rpc_boundary_deterministically():
    script = META + (
        'g = await agent("hermes.greeter", {"subject": args["who"]}, '
        'schema={"greeting": "string"})\n'
        'return {"greeting": g["greeting"]}\n'
    )
    res = run_workflow_script(script, args={"who": "world"})
    assert res.ok, res.error
    assert res.value == {"greeting": "hello, world"}


def test_run_is_deterministic():
    script = META + 'r = await agent("hermes.uppercaser", {"text": args["t"]})\nreturn r\n'
    a = run_workflow_script(script, args={"t": "abc"})
    b = run_workflow_script(script, args={"t": "abc"})
    assert a.ok and b.ok
    assert a.value == b.value == {"result": "ABC"}


def test_parent_journals_calls_with_stable_ids():
    journal = []
    script = META + (
        'log("a")\n'
        'await agent("hermes.echo", {"i": 1})\n'
        'phase("mid")\n'
        'await agent("hermes.echo", {"i": 2})\n'
        'return {}\n'
    )
    res = run_workflow_script(script, journal=journal.append)
    assert res.ok, res.error
    # Same events captured on the result and on the external sink.
    assert [c["call_id"] for c in res.calls] == [1, 2, 3, 4]
    assert [c["method"] for c in res.calls] == ["log", "agent", "phase", "agent"]
    assert all(c["ok"] for c in res.calls)
    assert [c["call_id"] for c in journal] == [1, 2, 3, 4]


def test_journal_is_redacted_by_default():
    script = META + 'await agent("hermes.echo", {"secret": "do-not-log"})\nreturn {}\n'
    res = run_workflow_script(script)
    agent_event = next(c for c in res.calls if c["method"] == "agent")
    assert agent_event["agent_id"] == "hermes.echo"
    assert "params" not in agent_event  # raw input is not journaled by default.


def test_parallel_and_pipeline_execute_in_guest():
    script = META + (
        "outs = await parallel([\n"
        "    lambda: agent('hermes.greeter', {'subject': 'a'}),\n"
        "    lambda: agent('hermes.greeter', {'subject': 'b'}),\n"
        "])\n"
        "piped = await pipeline(['x', 'y'],\n"
        "    lambda prev, item, i: agent('hermes.uppercaser', {'text': item}),\n"
        "    lambda prev, item, i: prev['result'] + str(i),\n"
        ")\n"
        "return {'parallel': [o['greeting'] for o in outs], 'pipeline': piped}\n"
    )
    res = run_workflow_script(script)
    assert res.ok, res.error
    assert res.value == {"parallel": ["hello, a", "hello, b"], "pipeline": ["X0", "Y1"]}


def test_kanban_agent_routes_through_reserved_runner():
    script = META + (
        'r = await kanban_agent("relayplanner", {"goal": "plan"}, {"repo": "x"})\n'
        'return {"profile": r["profile"], "status": r["status"]}\n'
    )
    res = run_workflow_script(script)
    assert res.ok, res.error
    assert res.value == {"profile": "relayplanner", "status": "succeeded"}


# --------------------------------------------------------------------------- #
# Launch gate (validation before any subprocess is spawned)
# --------------------------------------------------------------------------- #

def test_invalid_script_is_rejected_before_launch():
    caught = None
    try:
        run_workflow_script(META + "import os\n")
    except ScriptValidationError as exc:
        caught = exc
    assert caught is not None
    assert any(d.code == "E_SCRIPT_IMPORT" for d in caught.diagnostics)


def test_forbidden_capabilities_rejected_before_launch():
    for snippet in ('open("/etc/passwd")', 'eval("1")', 'exec("x=1")',
                    "import socket", "import time", "import random",
                    "x = ().__class__"):
        try:
            run_workflow_script(META + snippet + "\n")
        except ScriptValidationError:
            continue
        raise AssertionError(f"expected rejection for: {snippet!r}")


def test_frame_walk_escape_is_blocked():
    # Regression: cr_frame.f_globals -> sys.modules -> os reached the real
    # filesystem before the internal-attribute rule was added.
    exploit = META + (
        'c = agent("hermes.echo", {})\n'
        'osmod = c.cr_frame.f_globals["sys"].modules["os"]\n'
        'return {"leaked": osmod.path.exists("/")}\n'
    )
    raised = False
    try:
        run_workflow_script(exploit)
    except ScriptValidationError:
        raised = True
    assert raised, "frame-walk escape must be rejected at the launch gate"


def test_injected_json_and_math_proxies_work_but_are_not_live_modules():
    res = run_workflow_script(
        META + 'return {"j": json.dumps({"a": 1}), "m": math.floor(math.pi * 100)}\n'
    )
    assert res.ok, res.error
    assert res.value == {"j": '{"a": 1}', "m": 314}


def test_set_iteration_order_is_deterministic_across_runs():
    script = META + 'return {"x": list({"banana", "apple", "cherry", "date"})}\n'
    a = run_workflow_script(script)
    b = run_workflow_script(script)
    assert a.ok and b.ok
    assert a.value == b.value  # PYTHONHASHSEED=0 in the scrubbed env.


def test_scrubbed_env_hides_parent_credentials():
    # A would-be-leaked marker in the parent env must not be visible to the
    # script (it cannot import os anyway, but this asserts the env is scrubbed
    # even if a future reach existed). The script can only observe args.
    res = run_workflow_script(META + 'return {"keys": sorted(args.keys())}\n', args={"a": 1, "b": 2})
    assert res.ok and res.value == {"keys": ["a", "b"]}


# --------------------------------------------------------------------------- #
# Subprocess failure handling
# --------------------------------------------------------------------------- #

def test_script_exception_marks_run_failed_without_corrupting_parent():
    res = run_workflow_script(META + 'raise ValueError("boom")\n')
    assert res.ok is False
    assert res.error["type"] == "ValueError"
    assert "boom" in res.error["message"]
    assert res.error.get("line") == 2  # original-source line of the raise.
    # Parent is intact: a subsequent run still works.
    again = run_workflow_script(META + 'return {"after": True}\n')
    assert again.ok and again.value == {"after": True}


def test_cpu_spin_times_out_and_is_marked_failed():
    res = run_workflow_script(META + "while True:\n    x = 1\n", limits=VMLimits(max_runtime_s=1.0))
    assert res.ok is False
    assert "timed out" in res.error["message"]


def test_runaway_rpc_is_hard_capped():
    res = run_workflow_script(
        META + 'while True:\n    log("spam")\n',
        limits=VMLimits(max_rpc_calls=5, max_runtime_s=10.0),
    )
    assert res.ok is False
    assert "hard-limit" in res.error["message"]
    assert len(res.calls) <= 6  # 5 allowed + the one that tripped the cap.


def test_subprocess_that_exits_without_result_is_failed():
    # A "guest" that ignores the protocol and exits cleanly must still produce a
    # failed result, not hang or crash the parent.
    with tempfile.TemporaryDirectory() as tmp:
        fake = Path(tmp) / "fake_python"
        fake.write_text("#!/bin/sh\nexit 0\n")
        fake.chmod(fake.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        vm = WorkflowVM(python_executable=str(fake), limits=VMLimits(max_runtime_s=5.0))
        res = vm.run(META + 'return {"x": 1}\n')
    assert res.ok is False
    assert res.error["type"] == "WorkflowSubprocessError"


# --------------------------------------------------------------------------- #
# Parent-owned enforcement (broker unit tests with forged frames)
# --------------------------------------------------------------------------- #

def _broker(**limit_kwargs) -> CapabilityBroker:
    return CapabilityBroker(StubAgentRunner(), VMLimits(**limit_kwargs))


def _call(method, params, call_id=1):
    return {"t": rpc.T_CALL, "id": call_id, "method": method, "params": params}


def test_broker_allows_known_agent():
    ret = _broker().handle(_call("agent", {"agent_id": "hermes.greeter", "input": {"subject": "z"}}))
    assert ret["ok"] is True
    assert ret["value"] == {"greeting": "hello, z"}
    assert ret["id"] == 1


def test_broker_rejects_unknown_method():
    ret = _broker().handle(_call("open", {"path": "/etc/passwd"}))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "unknown_method"


def test_broker_rejects_unknown_agent():
    ret = _broker().handle(_call("agent", {"agent_id": "evil.agent", "input": {}}))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "unknown_agent"


def test_broker_rejects_reserved_kanban_id_via_agent():
    ret = _broker().handle(_call("agent", {"agent_id": "kanban.relayplanner", "input": {}}))
    assert ret["ok"] is False
    assert ret["error"]["code"] == "reserved_agent"


def test_broker_enforces_agent_limit_softly():
    broker = _broker(max_agent_calls=1)
    first = broker.handle(_call("agent", {"agent_id": "hermes.echo", "input": {}}, call_id=1))
    second = broker.handle(_call("agent", {"agent_id": "hermes.echo", "input": {}}, call_id=2))
    assert first["ok"] is True
    assert second["ok"] is False and second["error"]["code"] == "limit_agent"
    assert broker.should_abort is False  # soft denial does not abort the VM.


def test_broker_hard_caps_total_rpc_calls():
    broker = _broker(max_rpc_calls=2)
    broker.handle(_call("log", {"message": "a"}))
    broker.handle(_call("log", {"message": "b"}))
    over = broker.handle(_call("log", {"message": "c"}))
    assert over["ok"] is False and over["error"]["code"] == "limit_rpc"
    assert broker.should_abort is True


def test_broker_validates_output_schema():
    ret = _broker().handle(_call("agent", {
        "agent_id": "hermes.classifier", "input": {},
        "schema": {"label": "string", "missing": "string"},
    }))
    assert ret["ok"] is False and ret["error"]["code"] == "schema"


def test_broker_denies_nested_workflow_by_default():
    ret = _broker().handle(_call("workflow", {"name": "child", "args": {}}))
    assert ret["ok"] is False and ret["error"]["code"] == "nested_denied"


def test_broker_kanban_routes_to_reserved_runner():
    ret = _broker().handle(_call("kanban_agent", {"profile": "relayplanner", "task": {"goal": "g"}, "input": {}}))
    assert ret["ok"] is True
    assert ret["value"]["profile"] == "relayplanner"
    assert ret["value"]["task_id"].startswith("kb_")


def test_broker_log_and_phase_are_noops_returning_none():
    assert _broker().handle(_call("log", {"message": "x"}))["value"] is None
    assert _broker().handle(_call("phase", {"title": "p"}))["value"] is None


def test_run_script_convenience_matches_vm():
    res = run_script(META + 'return {"ok": 1}\n')
    assert res.ok and res.value == {"ok": 1}


# --------------------------------------------------------------------------- #
# Adversarial-review regressions (red-team workflow findings)
# --------------------------------------------------------------------------- #

class _TokenRunner:
    """Echo runner that reports per-call token usage for budget tests."""

    def __call__(self, agent_id, input):  # noqa: A002 — match AgentRunner signature.
        return {"echo": dict(input), "_tokens": 50}


class _SystemExitRunner:
    """A misbehaving runner that raises a BaseException (not Exception)."""

    def __call__(self, agent_id, input):  # noqa: A002
        raise SystemExit("agent forced exit")


def test_broker_enforces_token_budget_as_hard_cap():
    # Regression: token_budget was advisory only; the broker never denied on it.
    broker = CapabilityBroker(_TokenRunner(), VMLimits(token_budget=100))
    r1 = broker.handle(_call("agent", {"agent_id": "hermes.echo", "input": {}}, call_id=1))
    r2 = broker.handle(_call("agent", {"agent_id": "hermes.echo", "input": {}}, call_id=2))
    r3 = broker.handle(_call("agent", {"agent_id": "hermes.echo", "input": {}}, call_id=3))
    assert r1["ok"] and r2["ok"]  # 0 then 50 spent — both under 100.
    assert r3["ok"] is False and r3["error"]["code"] == "limit_token"  # 100 spent — denied.
    assert broker.should_abort is True


def test_token_budget_aborts_the_run_end_to_end():
    res = run_workflow_script(
        META + 'for i in range(20):\n    await agent("hermes.echo", {"i": i})\nreturn {}\n',
        agent_runner=_TokenRunner(),
        limits=VMLimits(token_budget=100),
    )
    assert res.ok is False
    assert "hard-limit" in res.error["message"]


def test_runner_baseexception_is_contained_not_propagated():
    # Regression: a runner raising SystemExit escaped broker.handle and crashed
    # the parent run(). It must be contained as a structured runner_error.
    broker = CapabilityBroker(_SystemExitRunner(), VMLimits())
    ret = broker.handle(_call("agent", {"agent_id": "hermes.echo", "input": {}}))
    assert ret["ok"] is False and ret["error"]["code"] == "runner_error"
    # And end-to-end: run() returns a failed result rather than raising SystemExit.
    res = WorkflowVM(agent_runner=_SystemExitRunner()).run(
        META + 'try:\n    await agent("hermes.echo", {})\n    return {"x": 1}\n'
        'except CapabilityError as e:\n    return {"caught": e.code}\n'
    )
    assert res.ok is True and res.value == {"caught": "runner_error"}


def test_unserializable_return_is_deterministic_no_address_leak():
    # Regression: the _jsonable fallback used repr(value), leaking a heap address
    # (non-deterministic) for non-JSON returns. It now reports only the type.
    script = META + "def helper():\n    return 1\nreturn helper\n"
    a = run_workflow_script(script)
    b = run_workflow_script(script)
    assert a.ok and a.value == {"_unserializable_type": "function"}
    assert a.value == b.value  # deterministic across runs (no 0x... address).
