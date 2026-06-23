# Examples

A tiny, end-to-end example for the `hermes_workflows` plugin.

## `hello.workflow.json`

A 2-step "hello" workflow that matches the contract's `workflow_def_format`:

1. **`greet`** (`agent` step) calls the `hermes.greeter` agent with
   `subject = $ref:inputs.name` and produces `{ "greeting": "<string>" }`.
2. **`shout`** (`agent` step) depends on `greet`, calls `hermes.uppercaser`
   with `text = $ref:greet.output.greeting`, and produces
   `{ "result": "<string>" }`.

The definition is pure JSON (stdlib `json` only — no YAML) and declares a
default-deny sandbox `policy` (`network: false`, `filesystem: false`). It is
never executed as code; the runtime interprets the validated AST and routes
all agent effects through the injected `AgentRunner`.

## Validate it

```bash
python -c '
import json
from hermes_workflows.primitives import workflow_validate
defn = json.load(open("examples/hello.workflow.json"))
res = workflow_validate(defn, source_path="examples/hello.workflow.json")
print("ok:", res.ok, "def_hash:", res.def_hash)
for d in res.errors + res.warnings:
    print(f"  [{d.severity}] {d.code} {d.pointer}: {d.message}")
'
```

Expected: `ok: True` with no errors. Every agent step declares an
`output_schema`, so there is no `W_NO_OUTPUT_SCHEMA` warning.

## Run it (default StubAgentRunner)

No live Hermes is required: `workflow_run` defaults to a deterministic
`StubAgentRunner` and the process-global in-memory run store.

```bash
python -c '
import json
from hermes_workflows.primitives import workflow_run, workflow_status
defn = json.load(open("examples/hello.workflow.json"))
handle = workflow_run(defn, inputs={"name": "world"})
print("run_id:", handle.run_id, "status:", handle.status)

status = workflow_status(handle.run_id)
print("final status:", status.status)
print("progress:", status.progress.completed, "/", status.progress.total)
for s in status.steps:
    print(f"  {s.step_id} [{s.kind}] -> {s.status}")
'
```

Expected: a terminal `succeeded` status with both `greet` and `shout` steps
recorded, and progress `2 / 2`.

The `run_id` follows the scheme `wf_<def_hash8>_<uuid12>`, so it sorts by
source definition and correlates back to the workflow via its `def_hash`.

## `hello_script.workflow.py` — subprocess script VM (issue #2)

A Python *script* counterpart to `hello.workflow.json`. Instead of a declarative
AST, it is deterministic orchestration code that runs in a sandboxed subprocess
behind a parent-owned RPC capability broker (see [DESIGN.md §5](../DESIGN.md)).
It is **not** loaded by the JSON template catalog (which only globs
`*.workflow.json`) and is **not** a model-facing tool.

```bash
python -c '
from hermes_workflows import workflow_validate_script, run_workflow_script
src = open("examples/hello_script.workflow.py").read()
print("validate ok:", workflow_validate_script(src).ok)
res = run_workflow_script(src, args={"name": "world"})
print("run ok:", res.ok, "value:", res.value)
print("calls:", [(c["method"], c["call_id"]) for c in res.calls])
'
```

Expected: `validate ok: True`, `run ok: True value: {"greeting": "hello, world",
"shout": "HELLO, WORLD"}`, and a journaled call list with stable, ascending call
ids. Pass `agent_runner=` to `run_workflow_script` to swap in a real Hermes
fan-out, or `limits=VMLimits(...)` to tighten budgets and caps.

## `scoped_session_grant.json` — scoped actuator grants (issue #33)

A documentation-only shape file for backend-neutral session-launch grants. It is
**not** a workflow definition and is **not** loaded by the catalog. It shows the
three credential-free shapes: a broker policy, the actuator's `grant_request`
envelope, and the persistable handle inside the issued grant.

```bash
python -c '
from hermes_workflows import (
    StaticPolicyGrantBroker, request_grant, resolve_grant, validate_grant,
)
broker = StaticPolicyGrantBroker(
    allowed_scope={"session.launch", "session.status"}, max_ttl_seconds=3600,
)
req = request_grant(
    scope=("session.launch", "session.status"), side_effect_class="session_launch",
    subject="work-context-abc", reason="launch a managed session",
    requested_by="issue_controller", ttl_seconds=1800,
)
decision = resolve_grant(broker, req)
print("granted:", decision.granted, "scope:", decision.grant.scope)
print("handle:", decision.grant.handle.to_dict())
print("reusable now:", validate_grant(decision.grant, action="session.status").ok)
'
```

Expected: `granted: True`, a `handle` carrying only `session_id` /
`work_context_id` / `handle_ref` (no secret), and `reusable now: True`. A denied,
expired, or credential-bearing grant fails closed instead — see
`DESIGN.md §1.5.1` and the README "Scoped actuator grants" section.

## `release_ops_resource_closeout.py` — ATH + Relay resource finalizers

A runnable release-ops closeout example for issue #52 / #31. It uses the real
Dynamic Workflows loop controller and `ResourceFinalizerRegistry`, declares both
ATH and Relay resources, and dispatches production action names:

- `ath.listener.retire`
- `relay.automation_run.retire`

The file intentionally uses local stand-in handlers so this package stays
backend-neutral and dependency-free. In production, the host process registers the
real adapters in their owning repos instead:

```python
from hermes_workflows import ResourceFinalizerRegistry
from async_threads.finalizers import register_ath_finalizers

finalizers = ResourceFinalizerRegistry()
register_ath_finalizers(finalizers, registry=async_thread_registry, secret_root=secret_root)
# Relay registers `relay.automation_run.retire` from its own runtime/adapter
# boundary; Dynamic Workflows core should not import Relay internals.
```

Run the zero-dependency smoke:

```bash
PYTHONPATH=src python examples/release_ops_resource_closeout.py
```

Expected: terminal `converged` state and two succeeded finalizer results. The
example retires the Relay automation/watchdog record only; artifact-preserving
child-session/process termination remains Relay-owned follow-up work.

## Notes

- Swap in a real Hermes fan-out by passing `agent_runner=` to `workflow_run`.
- Swap the run-status backend by passing `registry=` (e.g. an in-memory store
  per call, or the optional pluggable Kanban backend described in `DESIGN.md`).
