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

## Notes

- Swap in a real Hermes fan-out by passing `agent_runner=` to `workflow_run`.
- Swap the run-status backend by passing `registry=` (e.g. an in-memory store
  per call, or the optional pluggable Kanban backend described in `DESIGN.md`).
