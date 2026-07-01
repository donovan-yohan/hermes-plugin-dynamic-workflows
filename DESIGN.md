# DESIGN.md — `hermes_workflows`

A Hermes agent plugin that exposes one model-facing tool facade plus debug
primitives for sandboxed, script-led orchestration over Hermes agents:

- `workflow` — the only model-facing Hermes tool: dry-run validate, run a definition, or inspect an existing run id.
- `workflow_validate` — library/operator primitive that statically checks a workflow definition (parse, schema, sandbox-policy lint) without running anything.
- `workflow_run` — library/operator primitive that executes a validated definition in a deterministic, sandboxed runtime, fanning out to Hermes agents or durable Kanban awaitables.
- `workflow_status` — library/operator primitive that queries the state/progress of a run by id.

This document describes the architecture, the design decisions behind it, what we
borrow from Claude Dynamic Workflows and how we differ, the sandbox security
model, the Kanban/task-event adapter boundaries, and the roadmap.

The package is **pure Python 3.11 stdlib** (`json`, `dataclasses`, `hashlib`,
`uuid`, `datetime`, `typing`, `threading`, `pathlib`). It has **zero runtime dependencies**
and workflow definitions themselves still get **no direct network or filesystem authority**.
The plugin-owned `FileRunStore` is the parent process persistence boundary: it writes
`snapshot.json` and compact `journal.jsonl` events under the configured Hermes state dir.
YAML is intentionally unsupported (stdlib `json` only) so that `PyYAML` is not pulled in.

---

## 1. Architecture & component overview

### 1.1 Public tool surface

The plugin's public surface is `hermes_workflows.primitives`, which exports the
single model-facing `workflow` Hermes tool plus narrower library/operator primitives.
Each is a thin, side-effect-honest entry point over the internal components.

```python
def workflow(
    *,
    definition: dict | str | None = None,
    inputs: dict | None = None,
    run_id: str | None = None,
    template_name: str | None = None,
    action: str | None = None,
    dry_run: bool = False,
    registry: RunStore | None = None,
    catalog: FileWorkflowCatalog | None = None,
    agent_runner: AgentRunner | None = None,
    validate: bool = True,
    max_parallel: int = 8,
    include_steps: bool = True,
) -> dict

def workflow_validate(
    definition: dict | str,
    *,
    source_path: str | None = None,
    strict: bool = True,
) -> ValidationResult

def workflow_run(
    definition: dict | str,
    *,
    inputs: dict | None = None,
    registry: RunStore | None = None,
    agent_runner: AgentRunner | None = None,
    validate: bool = True,
    max_parallel: int = 8,
    run_id: str | None = None,
) -> RunHandle

def workflow_status(
    run_id: str,
    *,
    registry: RunStore | None = None,
    include_steps: bool = True,
) -> RunStatus
```

- **`workflow`** chooses the operation from supplied fields: `dry_run` or
  `action='validate'` validates only, a `definition` runs, `template_name` runs a
  saved catalog template, `action='catalog'` lists saved JSON templates,
  `action='script_catalog'` / `script_save` / `script_inspect` / `run_script`
  operate the saved Python script-harness catalog, and `run_id` without a
  definition reads status. The same facade also accepts the Claude-style
  `script` / `script_path` / `name` + `args` + `resume_from_run_id` shape, mapping
  it onto inline script execution, catalog-relative script loading, saved script
  lookup, and replay/resume respectively. It is the tool shape meant for model use.

- **`workflow_validate`** parses (`definition` may be a parsed `dict` or a JSON
  string), validates the schema, and runs the sandbox-policy lint. It has **no
  side effects**: no run is created, no agent is called. It returns a
  `ValidationResult` (`ok`, `errors`, `warnings`, `normalized`, `def_hash`). With
  `strict=True`, sandbox-policy lint *warnings* are promoted to *errors*.
  `source_path` is informational only (e.g. surfaced in diagnostics).

- **`workflow_run`** optionally validates (raising `WorkflowValidationError`,
  which carries the failing `ValidationResult`, *before* any run record exists),
  creates and persists a run record in `registry` (default: the process-global
  `InMemoryRunStore`), drives the runtime, and returns a `RunHandle`. Execution
  in the skeleton is **synchronous and deterministic**; `max_parallel` bounds
  logical fan-out width. External effects are routed exclusively through the
  injected `agent_runner` (default: a deterministic `StubAgentRunner`, so the
  skeleton runs without a live Hermes).

- **`workflow_status`** reads back a `RunStatus` from the registry. An unknown
  `run_id` yields `status='unknown'` with empty `steps` rather than raising, so
  pollers do not need exception handling. `include_steps=False` omits the
  per-step list for cheap polling.

- **`workflow_control`** (issue #9) is the operator surface: a second registered
  Hermes tool whose `action` selects `overview` (list active/recent runs and
  blocked waits), `status` (one run's compact control state, current phase,
  waits, child task refs, links, and the run-level enforcement decisions), or one
  of the append-only control verbs `pause` / `resume` / `stop` / `task_stop` /
  `retry`. It is backed by the generic `controls` module (§1.5.3–§1.5.4) and a
  durable `FileControlStore`, so an operator can pause, stop, or retry a run
  without touching the authoring `workflow` tool and without deleting any audit
  history.

### 1.2 Component map

```
          ┌─────────────────────────────────────────────────────────┐
          │                 hermes_workflows.primitives               │
          │ workflow facade + validate/run/status debug primitives      │
          └───────┬───────────────────┬───────────────────┬──────────┘
                  │                   │                   │
          ┌───────▼───────┐   ┌───────▼────────┐   ┌──────▼─────────┐
          │   schema.py   │   │   runtime.py   │   │  registry.py   │
          │  parse +      │   │  deterministic │   │  RunStore      │
          │  schema check │   │  AST interp.   │   │  Protocol      │
          └───────┬───────┘   └───────┬────────┘   │  InMemory...   │
                  │                   │            │  (Kanban stub) │
          ┌───────▼───────┐   ┌───────▼────────┐   └──────┬─────────┘
          │  sandbox.py   │   │   agents.py    │          │
          │  capability   │   │  AgentRunner   │          │
          │  policy lint  │   │  Protocol +    │          │
          │  (default-    │   │  StubAgent...  │          │
          │   deny)       │   └────────────────┘          │
          └───────────────┘                               │
                  │                                        │
          ┌───────▼────────────────────────────────────────▼─────────┐
          │      models.py (dataclasses)   +   errors.py (exceptions) │
          └──────────────────────────────────────────────────────────┘
```

| File | Responsibility |
|------|----------------|
| `primitives.py` | Public entry points; `workflow` facade plus explicit validate/run/status primitives. |
| `catalog.py` | File-backed saved template catalog for safe `<name>.workflow.json` listing/loading. |
| `script_catalog.py` | Versioned file-backed saved Python script harness catalog for validate/save/list/inspect/run-by-name flows (#29). |
| `capabilities.py` | Generic host-owned capability registry/policy for workflow scripts: named handlers, side-effect class allowlists, approval ids, credential guards, and bounded output capture (#29). |
| `events.py` | Backend-neutral workflow event broker for non-Kanban events: durable event ids/versions, predicate waits, GitHub webhook normalization, and no-poll wakeups (#7). |
| `schema.py` | Parse JSON, validate top-level shape, step kinds, references; emit `Diagnostic`s with stable codes and JSON-Pointer `pointer`s. |
| `sandbox.py` | Documents and **enforces** the capability policy (default-deny). Static lint only; not a JS engine. |
| `runtime.py` | Deterministic interpreter over the validated AST (`agent`/`kanban_agent`/`if`/`parallel`/`pipeline`/`phase`). Never `eval()`s; never imports user-named modules. |
| `registry.py` | `RunStore` Protocol + thread-safe `InMemoryRunStore`; file-backed run state for embedders; board-backed stores remain adapter work. |
| `agents.py` | `AgentRunner` Protocol (`(agent_id, input_dict) -> output_dict`) + deterministic `StubAgentRunner`, including reserved `kanban.<profile>` outputs. |
| `delegation.py` | Optional Hermes `delegate_task` child-agent adapter: converts script `agent(prompt, opts)` requests into host-dispatched delegate calls, parses foreground JSON summaries, and returns explicit background dispatch envelopes without pretending the child result is available. |
| `loops.py` | Feedback-controller loop spec validation and synchronous loop runner over injected sensor/actuator adapters, with step/time/budget/stall brakes. |
| `grants.py` | Backend-neutral scoped actuator grants: `GrantRequest` / `SessionGrant` / `GrantHandle` models, in-memory/file `GrantStore`, `StaticPolicyGrantBroker`, `request_grant` / `resolve_grant` / `validate_grant`, and a credential-leak guard. Wired into `loop_run` for fail-closed session-launch authorization. |
| `resources.py` | Backend-neutral workflow resource/finalizer models: credential-free resource declarations, cleanup trigger/policy vocabulary, finalizer runner Protocol, action-string adapter registry, idempotent closeout result helpers, and credential-leak rejection. Wired into `loop_run` for terminal resource cleanup. |
| `models.py` | All dataclasses: `ValidationResult`, `Diagnostic`, `RunHandle`, `RunStatus`, `Progress`, `StepStatus`. |
| `errors.py` | `WorkflowError` base, `WorkflowValidationError`, `RunNotFound`, `SandboxPolicyError`. |

### 1.3 The workflow definition (AST)

A definition is a plain JSON object. Top-level shape:

```json
{
  "version": "1",
  "name": "hello",
  "inputs": { "name": "string" },
  "policy": { "network": false, "filesystem": false, "max_parallel": 8 },
  "steps": [ /* ...Step */ ]
}
```

Step kinds are discriminated by `"kind"`:

- **`agent`** — `{ "kind":"agent", "id", "agent", "input", "output_schema"?, "depends_on"? }`
- **`kanban_agent`** — `{ "kind":"kanban_agent", "id", "profile", "task", "input"?, "wait"?, "output_schema"?, "depends_on"? }` (durable Kanban-backed awaitable contract; skeleton routes through `kanban.<profile>` runner id)
- **`if`** — `{ "kind":"if", "id", "condition": {"ref", "op", "value"?}, "then": [Step, ...], "else"?: [Step, ...] }` (deterministic conditional; branch-local step ids do not leak outside the container)
- **`parallel`** — `{ "kind":"parallel", "id", "branches": [Step, ...] }` (fan-out, joins all branches)
- **`pipeline`** — `{ "kind":"pipeline", "id", "steps": [Step, ...] }` (each step's output feeds the next; no-barrier by default)
- **`phase`** — `{ "kind":"phase", "id", "label", "steps": [Step, ...] }` (explicit barrier: all inner steps complete before the next phase)

**References** wire data between steps. An `"input"` value may be a literal dict or
a reference string:

- `"$ref:inputs.<key>"` — read a declared workflow input.
- `"$ref:<step_id>.output"` or `"$ref:<step_id>.output.<field>"` — read a prior step's structured output.

Outputs are **schema-validated dicts** (structured agent output). The optional
`output_schema` is a flat `field -> type-hint-string` map; the runtime validates
each `AgentRunner` result against it and records the typed result in
`StepStatus.output`.

### 1.4 Run identifiers

```
run_id = "wf_" + <def_hash8> + "_" + <uuid4hex12>
```

where `def_hash` is the SHA-256 of the **canonicalized** definition
(`json.dumps(..., sort_keys=True, separators=(",", ":"))`), truncated to 8 hex
chars for the id and stored in full as `def_hash` on every record. This makes ids
sortable-by-source, collision-resistant, and lets `workflow_status` correlate a
run back to its definition. Callers may pass an explicit `run_id` for
idempotency or deterministic tests.

### 1.5 Feedback-controller loops

Issue #31 adds a controller layer beside the workflow interpreter rather than
turning `workflow` steps into a repo-specific ticket bot. `loops.py` validates a
generic loop spec (`setpoint`, primary sensor, actuator adapters, brakes) and runs
an injected sensor/actuator pair through explicit controller states:

```text
planned -> sensing -> acting -> sensing ... -> converged | halted_*
```

The sensor contract is the source of truth:

```json
{
  "converged": false,
  "signal_key": "stable hash/key for stall detection",
  "summary": "short human-readable result",
  "evidence": [{"kind": "test", "name": "...", "status": "failed"}],
  "retryable_noise": false,
  "next_hint": "bounded context for the next actuator step"
}
```

Actuators are backend adapters: inline code, `delegate_task`, managed processes,
Kanban, Relay, or ATH-triggered execution can all fit behind the same callable
shape. The controller does not trust an actuator's success claim; only a later
sensor result can converge the run. `brakes.max_steps` caps actions; after the
last allowed action, the controller performs a final sensor pass so it converges
or halts from fresh evidence. Runtime-enforced brakes cover action count,
wall-time checks around synchronous calls, repeated `signal_key` stall detection,
strict optional actuator-reported cost, and retry-once handling for noisy sensors.
The handoff context includes remaining actions, remaining wall time/deadline, and
remaining budget so adapters can enforce cooperative internal timeouts. Actuator
results may also suspend the controller with a backend-neutral envelope: `wait`
transitions to `waiting_for_event`, and `approval_request` transitions to
`waiting_for_approval`. The request object must identify itself with `id`, `token`,
or `kind`, is recorded in status/events, and is intentionally not executed by the
controller. Actuator results may also register credential-free `resources` with
declared cleanup `finalizers`; terminal success/failure/timeout paths run matching
finalizers through an injected adapter and persist auditable cleanup results. This
keeps Dynamic Workflows' product primitive generic while still making Relay/ATH
handoffs possible through adapter configuration and run inputs.

Persistence and visibility are generic boundaries, not ATH-specific code paths.
`LoopRunStore.save_status(status)` is called at each lifecycle event and at final
report update; `InMemoryLoopRunStore` and `FileLoopRunStore` are the bundled
embeddable stores. The file store writes a full `snapshot.json` and an `events.jsonl`
journal under `<root>/<run_id>/` so external tooling can inspect the latest state
without importing Python objects. `loop_run(..., on_event=...)` is the live event
observer seam for ATH, gateways, CLIs, notebooks, or dashboards. Event payloads
include run id, loop name, definition hash, event index, controller state,
iteration, summary, and event-specific evidence/handles. The controller still owns
state transitions; adapters own delivery, redaction, retries, and auth.

### 1.5.1 Scoped actuator grants (issue #33)

A controller actuator that must *launch or control a managed agent session*
needs real authority. The wrong way to give it that authority is the obvious
one: hand the adapter a raw shell token or a reused browser cookie. Those are
bearer secrets — ambient, unscoped, non-expiring, unauditable, and fully
reusable by anyone who can read the run state. `grants.py` replaces them with a
**scoped grant**: an explicit, expiring, single-purpose authorization resolved
through an injected broker, never a credential held by the actuator.

The flow rides the existing actuator envelope. An actuator returns a
credential-free `grant_request`:

```json
{
  "grant_request": {
    "scope": ["session.launch", "session.status"],
    "side_effect_class": "session_launch",
    "subject": "work-context-abc",
    "reason": "launch a managed session to drive the issue",
    "ttl_seconds": 1800
  }
}
```

`loop_run(..., grant_broker=..., grant_store=...)` resolves it through a
`GrantBroker`. Core ships `StaticPolicyGrantBroker`, a backend-neutral default
with **no real authentication** — it exists so the controller, docs, and tests
have a working broker. It clamps the requested TTL to a policy maximum, rejects
out-of-policy scope and side-effect classes, and mints an opaque, revocable
`GrantHandle` (`session_id` / `work_context_id` / `handle_ref`) that names the
session without carrying a secret. A real backend (Relay being one future
adapter) implements the same `GrantBroker` Protocol to authenticate and issue a
backend-scoped reference. The primitive stays generic: no `relay_*` grant kinds,
no hard-coded session semantics.

Every issued `SessionGrant` carries the four properties the acceptance demands —
explicit `scope`, explicit `side_effect_class` (`read_only` / `session_launch` /
`session_control` / `external_write`), explicit expiry (`issued_at` /
`expires_at` plus epoch fields so a persisted grant re-validates without
re-deriving the clock), and `audit` metadata (`requested_by`, `reason`,
`run_id`, `def_hash`, iteration). The controller records the grant in
`status.grants` and re-exposes it to later steps via `context["grants"]`, so a
launched session handle is available to the next sensor/actuator. Persistence is
the same generic boundary as loops: `GrantStore.save_grant` / `get_grant`, with
`InMemoryGrantStore` and `FileGrantStore` (`<root>/<grant_id>.json`) bundled. A
workflow persists the handle, restarts, re-reads it, and calls `validate_grant`
to resume status checks against the same session/work-context.

Failure is always closed. `resolve_grant` and `validate_grant` return structured
negative decisions (`GrantDecision` / `GrantValidation` with stable codes like
`denied_scope`, `denied_class`, `expired`, `no_broker`, `malformed`) rather than
raising, and the loop halts in `halted_grant_denied` with a `grant_denied`
event. A missing broker, a malformed request, an expired reused handle, a
broker that widens scope beyond the request, and a grant payload that smuggles a
raw credential all converge on the same fail-closed signal. The credential guard
(`find_raw_credential` / `redact_credentials`) is the line that keeps this from
decaying into cookie reuse: any grant payload whose keys look credential-shaped
(`cookie`, `authorization`, `token`, `password`, `secret`, …) is rejected, and
such values are masked before they are journaled — the denied event names the
offending *key*, never its value. Legitimate `wait` / `approval_request`
identity tokens are untouched because redaction is scoped to the
`grant_request` / `grant` sub-objects only.

This is intentionally narrower than full launch-approval/session-policy
governance (#11): it is the authorization *seam* — models, store, broker
Protocol, fail-closed wiring — not a real auth backend. The bundled broker
authenticates nothing; it makes the shape real, testable, and credential-free so
a backend adapter can drop in behind `GrantBroker` later.

### 1.5.2 Resource lifecycle finalizers (issue #52)

A loop actuator that starts or reuses runtime resources needs a closeout contract
just as much as it needs a launch contract. `resources.py` is that generic model:
`WorkflowResource` names a credential-free resource handle, `ResourceFinalizer`
declares a cleanup action and trigger policy, and `FinalizerResult` records the
auditable outcome. The controller stores resource declarations on
`LoopRunStatus.resources` and finalizer outcomes on `LoopRunStatus.finalizer_results`.

The actuator envelope is deliberately backend-neutral:

```json
{
  "resources": [
    {
      "id": "ath-listener-pr51",
      "kind": "ath.listener",
      "handle": {"thread_key": "ath_safe_ref"},
      "owner": {"issue": 52, "pr": 51},
      "finalizers": [
        {
          "id": "retire-listener",
          "action": "ath.listener.retire",
          "when": ["success", "failure", "timeout"],
          "policy": "required",
          "verification": {"event": "listener_disabled"}
        }
      ]
    }
  ]
}
```

Core does not know how to retire an ATH listener, stop a Relay session, kill a
process group, or delete a worktree. It only decides *when* finalizers are due and
calls the injected `ResourceFinalizerCallable` with `{run_id, loop_name, trigger,
resource, finalizer}`. Backend adapters own the actual cleanup and return bounded
`{ok, summary, evidence}` results.

`ResourceFinalizerRegistry` is the optional host-side dispatch helper for that
callable seam. It maps dotted action strings (`ath.listener.retire`,
`relay.automation_run.retire`, `process.group.terminate`, etc.) to registered
handlers and is itself a valid `ResourceFinalizerCallable`. Unknown actions fail
closed through normal finalizer-result handling, and duplicate action registration
requires `replace=True`. This keeps ATH/Relay/process integrations first-class as
adapters without making them imports or branches inside Dynamic Workflows core.

`examples/release_ops_resource_closeout.py` is the concrete release-ops wiring
smoke for this boundary. It declares an ATH listener and a Relay automation-run
resource, runs both finalizers through `ResourceFinalizerRegistry`, and uses local
stand-in handlers so the core package still has zero ATH/Relay dependencies. The
real adapters stay in their owning repos: ATH owns `ath.listener.retire`, Relay
owns `relay.automation_run.retire`, and child session/process termination remains
a Relay primitive rather than Dynamic Workflows core behavior.

Closeout runs on terminal success, failure, and timeout paths. Waiting states do
not close resources because those resources may be needed by the resumed run.
The trigger vocabulary also includes future host-owned `cancelled` and
`superseded` paths so gateway/ATH/Relay adapters can reuse the same model outside
the synchronous `loop_run` happy path. Finalizers are idempotent at the controller
surface: `(resource_id, finalizer_id, trigger)` is executed once even if a resource
is registered repeatedly or a status snapshot is re-processed.

Policies are explicit. `best_effort` failures are visible but do not change the
terminal state; `preserve_only` records that a resource is intentionally kept;
`manual_approval_required` records an approval-needed closeout result; failed
`required` cleanup changes the run to `halted_finalizer_error`, because a run that
claims success while leaking a required resource is lying. Resource and finalizer
envelopes reject credential-shaped keys/values before journaling, so handles must
be opaque ids or scoped backend refs, not cookies, bearer tokens, passwords, or
API keys.

### 1.5.3 Operator controls, status & wait inspection (issue #9)

A run that is *authored* through `workflow` eventually needs an *operator*
surface: pause it, resume it, stop it, retry a failed call/task, and answer "what
is it blocked on?" — without re-decoding raw journal/snapshot JSON. `controls.py`
is that surface, and it is deliberately generic: no Relay, ATH, or Kanban
behaviour, only run ids and three boring-but-load-bearing pieces.

**Append-only control records.** Every pause/resume/stop/`task_stop`/retry is a
`WorkflowControl` — an immutable audit record persisted by a `ControlStore`
(`InMemoryControlStore`, or `FileControlStore` writing
`<root>/<run_id>/controls.jsonl`). Recording an intent never mutates or deletes a
run's history: a stop *adds* a stop record. Idempotency and cross-restart
durability come from the same place — `FileControlStore.append` re-reads the
run's existing control ids from disk before writing, so a re-issued (e.g.
deterministic retry) id is deduped even by a fresh process. A torn/malformed line
is skipped rather than failing the whole read; a well-formed line whose embedded
`run_id` does not match the directory is ignored; and duplicate `control_id` rows
are first-write-wins so a later forged duplicate cannot change the projection on
restart.

**A control-state projection.** `project_control_state(run_id, controls)` folds
the records into a compact `RunControlState`: `desired_state` (`running` /
`paused` / `stopped`), the per-task `stopped_tasks`, and the retry lineage.
Crucially this is *desired* state, not enforcement — actually preventing new
child work or reattaching to pending work is the backend adapter's job; core only
records and projects intent. **Stop is terminal**: a `resume` recorded after a
`stop` stays in the audit trail but never un-stops the run. A `task_stop` halts
one named child without stopping the whole run.

**Idempotent retry lineage.** `retry(store, run_id, target_ref)` is idempotent
per `(run_id, target_ref)` — it returns the existing retry record instead of
forking a duplicate; `force=True` mints the next `attempt`. Each retry carries
`attempt` (1-based) and a `replacement_ref` (the new call/task id — passed
explicitly when the backend has minted it, else a deterministic
`<target_ref>#retry<N>` placeholder). The control id is derived from
`(run_id, target_ref, attempt)`, so even a forced re-retry is deduped across a
restart. The *replacement execution* stays adapter-owned; core makes the lineage
shape explicit and durable.

**Wait inspection from data the other slices already persist.**
`waits_from_loop_status` turns a loop's `waiting_for_event` / `waiting_for_approval`
state and its suspension event into a uniform `WaitSummary` (§1.5);
`waits_from_kanban_states` does the same for a `ScriptRunStore`'s non-terminal
Kanban card states (§5.8). Durable Kanban waiting markers created through the VM
carry the logical run id from the `<logical_run_id>:<call_id>` idempotency key,
and the store preserves that association across later state writes; for
legacy/manual waits with no stored `run_id`, the plugin `status` path attaches
them to the inspected run rather than silently dropping them. No new backend or
store is introduced — the inspectors read plain dicts, so they compose with
whatever a caller already has.

**Compact projections, no JSON spelunking.** `inspect_run(...)` composes a single
run's lifecycle status, control state, current phase, waits, child task refs,
retry lineage, last events, result/error, and dashboard `links` (`run_links`
bundles script/journal/snapshot/transcript/result/tasks paths the caller knows)
into one stable shape. `list_runs(records, control_store, waits=...)` is the
`/workflows` overview: registry-shaped run records summarised newest-first and
capped, merged with control state, with blocked waits folded in both per-run and
as a flat list, plus aggregate counts. Both take plain data so they stay
backend-neutral and trivially testable.

This is the operator *seam*, narrower than a live scheduler: core records and
projects control intent durably and exposes inspectable status; enforcing pause,
killing in-flight child work, and executing retries are backend-adapter
responsibilities that ride these records. The plugin registers `workflow_control`
through the normal Hermes tool registry; this repo does not assert an
operator-only registration mode. A deployment that makes the tool model-callable
must scope it to trusted operator sessions/toolsets or add a host-level approval
policy around destructive verbs.

### 1.5.4 The enforcement-decision seam (issue #9)

Recording and projecting intent (§1.5.3) is only half a control surface — an
adapter still has to *decide*, at each branch point, whether it may act on that
intent. `evaluate_control_state(control_state, operation, target_ref=None)` is
that decision: it folds a `RunControlState` plus one operation into a
`ControlDecision` — `allowed: bool` plus a stable, machine-branchable `code`
(`allowed` / `run_stopped` / `run_paused` / `task_stopped` / `retry_exists`),
its human `reason`, the `desired_state`, and — where one exists — the
`control_id` of the record responsible for a block. It is **pure**: it reads the
projection, never a store, so the same state always yields the same verdict and
it is trivially testable.

The operation vocabulary covers the four branch points an adapter actually hits:

| Operation | Question | Blocked by |
|-----------|----------|------------|
| `start_child` | may I launch *new* child work? | stop (terminal), pause (new work held) |
| `continue_task` | may an existing `target_ref` keep running? | stop, matching `task_stop` — **not** pause |
| `retry` | may I replace a failed `target_ref`? | stop, matching `task_stop`, an already-recorded retry, then pause |
| `check_run` | may the run make progress at all? | stop only (a paused run is still alive) |

The semantics fall straight out of §1.5.2's intent model: **stop is terminal and
blocks everything**; **pause holds only new work** (`start_child` / `retry`) and
deliberately never blocks `continue_task` or `check_run`, because pausing does
not claim to kill in-flight waits; **`task_stop` blocks only its exact
`target_ref`**, leaving sibling tasks and run-level work untouched. The `retry`
verdict is the dedup guard: when a retry of `target_ref` is already on record the
decision returns `retry_exists` carrying that retry's `replacement_ref` /
`attempt` / `control_id`, so an adapter reuses the existing replacement instead
of silently launching a duplicate (it forces a fresh attempt via `retry(...,
force=True)` when it genuinely wants another). Thin wrappers `may_start_work`,
`may_continue_task`, `may_retry`, and `may_check_run` name the common calls.

This is a *decision*, not enforcement. Core answers "should this proceed?"; the
adapter still owns the act of declining to dispatch, cancelling a process, or
replaying a task. `inspect_run(...)` surfaces the two run-level verdicts
(`start_child`, `check_run`) under a `decisions` key so an operator reading
status sees them honestly as decisions — never as a claim that core has cancelled
anything. No Relay/ATH/Kanban behaviour lives here; an adapter is free to consult
more operations (per-task, per-retry) directly.

### 1.6 The sandboxed runtime (skeleton)

The runtime in `runtime.py` is a **deterministic AST interpreter**, not a
language VM. It walks the validated step tree and:

1. Resolves `$ref` inputs from the input bag and prior step outputs.
2. Calls the injected `AgentRunner` for each `agent` step.
3. Validates the returned dict against `output_schema` (when present).
4. Applies the composition semantics for `parallel` / `pipeline` / `phase`.
5. Records per-step `StepStatus` and aggregate `Progress` into the `RunStore`.

It never evaluates user-supplied strings as code, never imports modules named in
the definition, and routes **all** external effects through the single
`AgentRunner` boundary. Logical concurrency (`parallel`, `max_parallel`) is
modeled deterministically; the skeleton executes synchronously so runs are
reproducible and easy to test.

The first #11 governance slice adds runtime-enforced fanout/backpressure policy
for the declarative runtime. `policy.max_agent_calls` caps total effect-boundary
calls, `policy.max_kanban_cards` caps Kanban-backed awaits, `policy.max_active_awaits`
caps logical simultaneous waits in a `parallel` step, and `policy.allowed_profiles`
allowlists `kanban_agent.profile` before any card/runner call. These guards run in
static validation where possible and again at runtime so `validate=false` cannot
bypass them. Status failures are metadata-only (`SandboxPolicyError` type and a
short policy reason); they do not serialize raw prompts, card bodies, or transcripts.
Gateway/CLI launch approval and child-approval UX remain future host integration,
not something this skeleton pretends to own.

### 1.7 Workflow definition format

A workflow definition is a JSON object with `version`, `name`, optional `inputs`,
a `policy` object, and a recursive `steps` list. The governance keys above are
part of the `policy` object alongside the default-deny `network` / `filesystem`
flags and `max_parallel` logical fanout width.

## 2. Claude Dynamic Workflows observations

The design is directly inspired by the **Claude Dynamic Workflows** model of
deterministic orchestration. The points below state what we borrow and, just as
importantly, where Hermes deliberately differs.

### 2.1 What we borrow

- **Deterministic orchestration.** Claude Dynamic Workflows express the *plan* as
  deterministic JS while the *judgment* lives inside the agents it calls. We keep
  the same split: the orchestration layer is fully deterministic and replayable;
  only the `AgentRunner` boundary is nondeterministic. Determinism is what makes
  validation, hashing, and status-by-id meaningful.

- **The four primitives `agent()` / `parallel()` / `pipeline()` / `phase()`.** Our
  four step kinds map one-to-one onto these combinators:
  - `agent` — a single structured agent call.
  - `parallel` — fan-out across independent branches, join on all.
  - `pipeline` — chain steps where each output feeds the next input.
  - `phase` — an explicit barrier that forces all inner steps to complete before
    the workflow proceeds.

- **Schema-validated structured output.** Agent results are typed dicts validated
  against a declared `output_schema`, exactly the structured-output discipline
  that makes downstream wiring reliable. A missing schema is a lint *warning*
  (`W_NO_OUTPUT_SCHEMA`), not a hard error.

- **Pipeline-by-default, no-barrier semantics.** Top-level ordered steps stream
  output → input to the next step **without** an implicit global barrier. A
  `phase` is the only construct that introduces a barrier. This mirrors the
  no-implicit-barrier behavior of the Dynamic Workflows pipeline model and keeps
  fan-out latency low by default.

- **The resume / journal idea.** Dynamic Workflows treat the run as a journaled,
  resumable sequence of completed steps. We borrow the *concept*: every step
  transition is recorded in the `RunStore` as an append-style update
  (`create` → `update_step` → `set_status`), and the `def_hash` correlates a run
  to its source definition. This is the substrate a future resume/replay engine
  needs (see Roadmap), even though the skeleton does not yet resume.

### 2.2 How Hermes differs

- **Declarative JSON, not executed JS.** The Dynamic Workflows authoring surface
  is a JS program. Hermes definitions are **plain JSON documents** (`version`,
  `name`, `inputs`, `policy`, `steps`). There is no scripting surface in the
  skeleton: the runtime interprets a validated AST. This trades expressiveness
  for a vastly smaller, statically-analyzable, and safely-sandboxable surface.
  (A real embedded JS engine is explicitly out of scope — see §3.)

- **Static `workflow_validate` as a first-class primitive.** Because the
  definition is declarative data, we can fully type-check it *before* execution:
  schema, references, capability policy, and graph acyclicity. Validation is a
  standalone, side-effect-free primitive rather than a runtime concern.

- **Default-deny capability policy in the document.** Every definition carries an
  explicit `policy` block. `network` and `filesystem` must be `false` in the
  skeleton (a `true` is a lint error). Capabilities are part of the contract, not
  ambient runtime authority.

- **Injected, pluggable boundaries.** Both the agent fan-out (`AgentRunner`) and
  the run-status store (`RunStore`) are injected Protocols with deterministic
  default implementations (`StubAgentRunner`, `InMemoryRunStore`). The skeleton
  runs end-to-end with no live Hermes and no external store.

- **Synchronous, deterministic execution in the skeleton.** `parallel` and
  `max_parallel` describe *logical* concurrency; the skeleton schedules them
  deterministically rather than truly concurrently, so runs and tests are
  reproducible.

### 2.3 Python-vs-JS workflow script compatibility boundary

The compatibility target is the **workflow product contract**, not source-level
JavaScript execution. The Claude Dynamic Workflows archive uses JavaScript
examples such as `loop-until-dry-bughunt.js` (`export const meta`, async
functions, `agent(prompt, { label, phase, schema })`, `parallel`, `phase`, `log`,
and `budget`). Hermes currently ships a guarded Python subprocess VM (§5). A
workflow is considered compatible when the same orchestration can be expressed
with the same deterministic primitives, status/replay semantics, and sandbox
guarantees, even if the syntax is Python instead of JavaScript.

Parity primitives that are intentional and product-facing:

| Contract | Current Python VM shape |
|---|---|
| Script metadata | First statement is literal `meta = {...}` with required `name` / `description`; optional metadata such as `phases` is preserved in validation and run state. |
| Agent call | `await agent(agent_id, input, label=..., schema=...)`; every call crosses the parent-owned `AgentRunner` / broker boundary. |
| Fan-out and pipelines | `await parallel([...])` and `await pipeline(...)`; this slice keeps scheduling deterministic and sequential under the hood, while the contract preserves fan-out/join and pipeline semantics. |
| Phase markers | `phase("title")` records brokered phase transitions; issue #63 also surfaces declared script phases through status. |
| Logs, inputs, budget | `log(...)`, read-only `args`, and read-only `budget.remaining()` / `budget.spent()` are injected globals, not imports. |
| Resume/cache | Stable call ids, deterministic replay cache for replayable calls, metadata-only journals, durable Kanban card reattach, and suspended-await replay form the current resume contract. |
| Model-facing script facade | The registered `workflow` tool currently exposes saved-script operations with `script_source`, `script_name`, `script_args`, and `script_version`. CamelCase archive aliases such as `scriptPath` or `resumeFromRunId` are compatibility vocabulary for future facade work, not shipped `0.1.0` schema. |

Intentional differences and security boundaries:

- JavaScript syntax support is not present in `0.1.0`; it is future work unless a
  separate JS guest runtime is designed and added. Core must not claim to execute
  archive `.js` files directly.
- Python harnesses cannot import modules, open files, read environment variables,
  spawn processes, open sockets, read the clock, use randomness, or call ambient
  dynamic builtins (`open`, `eval`, `exec`, `compile`, `__import__`, `globals`,
  `locals`, `getattr`, `print`, etc.). The static validator rejects those names
  before launch, and the guest still runs with restricted builtins and a scrubbed
  environment.
- The script receives only the RPC-backed workflow globals (`agent`,
  `kanban_agent`, `capability`, `parallel`, `pipeline`, `phase`, `log`,
  `workflow`), read-only `args`, `budget`, `meta`, and curated `json` / `math`
  proxies. Direct filesystem/network/env/clock/randomness access is absent by
  design; if a workflow needs a real effect, the host must expose a named
  capability and policy.
- All outside effects remain parent-owned. The subprocess writes no journal files
  itself, holds no Hermes/GitHub credentials, and cannot bypass redaction,
  output limits, approval checks, replay/idempotency, or side-effect-class policy.

Side-by-side translation of the archive loop-until-dry shape:

```js
// Claude archive style — illustrates the reference product shape.
export const meta = { name: "loop-until-dry-bughunt" };

let round = 0;
let areas = ["runtime", "docs", "tests"];

while (areas.length && budget.remaining() > 0 && round < 4) {
  phase(`round ${round + 1}`);
  const results = await parallel(areas.map((area) =>
    agent(`Find remaining bugs in ${area}`, {
      label: `bughunt:${area}`,
      phase: "bughunt",
      schema: { bugs: "array", followups: "array" }
    })
  ));

  areas = results.flatMap((result) => result.followups ?? []);
  log(`round ${round + 1}: ${areas.length} follow-up areas`);
  round += 1;
}
```

```python
# Current Hermes shape — validated, then executed by the guarded Python VM.
meta = {
    "name": "loop_until_dry_bughunt",
    "description": "Repeat bughunt passes until no follow-up areas remain",
    "phases": ["bughunt"],
}

round_index = 0
script_args = args or {}
areas = list(script_args.get("areas", ["runtime", "docs", "tests"]))
max_rounds = script_args.get("max_rounds", 4)


async def scan(area):
    return await agent(
        "hermes.bughunter",
        {"prompt": f"Find remaining bugs in {area}", "area": area},
        label=f"bughunt:{area}",
        schema={"bugs": "list", "followups": "list"},
    )


while areas and budget.remaining() > 0 and round_index < max_rounds:
    phase(f"round {round_index + 1}")
    results = await parallel([lambda area=area: scan(area) for area in areas])

    next_areas = []
    for result in results:
        for followup in result.get("followups", []):
            if followup not in next_areas:
                next_areas.append(followup)
    areas = next_areas
    log(f"round {round_index + 1}: {len(areas)} follow-up areas")
    round_index = round_index + 1

return {"remaining_areas": areas, "rounds": round_index}
```

---

## 3. Sandbox security model

The security posture is **default-deny** and is enforced at two layers: a static
lint at validation time and a constrained interpreter at run time.

### 3.1 The runtime is not a JS engine

This is the central security decision. Despite the "JS-like orchestration"
framing, **workflow definitions are declarative JSON and are never executed as
code.** `sandbox.py` documents and enforces a capability policy; it does not host
an interpreter for a programming language. The runtime walks a validated data
structure. Concretely, the runtime:

- never calls `eval()` / `exec()` / `compile()` on any definition content;
- never imports modules named anywhere in the definition;
- never opens sockets or files;
- routes **all** external effects through the single injected `AgentRunner`.

Why a real JS engine is out of scope for the skeleton: a production-grade embedded
JS sandbox (isolate lifecycle, memory/CPU quotas, deterministic time, syscall
mediation, escape-hardening) is a large, security-critical subsystem with its own
threat model and dependency footprint. It is incompatible with the
zero-dependency, no-network, pure-stdlib constraint of the skeleton, and shipping
a half-built sandbox would be worse than shipping none. We therefore ship a
declarative interpreter with a documented capability contract now, and treat real
code execution as a future, separately-designed milestone (see Roadmap).

### 3.2 What `workflow_validate` enforces (sandbox-policy lint)

The lint produces `Diagnostic`s with stable string `code`s and JSON-Pointer
`pointer`s so downstream generators and tests assert on codes, not prose.
Representative checks and codes:

| Code | Severity | Meaning |
|------|----------|---------|
| `E_SCHEMA_TOPLEVEL` | error | Missing/invalid top-level field (`version`, `name`, `steps`, malformed `policy`). |
| `E_POLICY_NETWORK` | error | `policy.network` is `true` — disallowed in the skeleton. |
| `E_POLICY_FILESYSTEM` | error | `policy.filesystem` is `true` — disallowed in the skeleton. |
| `E_UNKNOWN_AGENT` | error | An `agent` step references an agent id the runner cannot resolve. |
| `E_BAD_REF` | error | A `$ref` is malformed or points at a nonexistent step/input/field. |
| `E_CYCLE` | error | A cycle exists in the `depends_on` / pipeline edge graph. |
| `E_DISALLOWED_BUILTIN` | error | A reserved/disallowed builtin or import-like form appears in the definition. |
| `W_NO_OUTPUT_SCHEMA` | warning | An `agent` step declares no `output_schema`. |

With `strict=True`, warnings are promoted to errors, so a strict caller rejects
any definition that is not fully typed and policy-clean.

### 3.3 Allowed vs blocked

**Allowed (skeleton):**

- Declaring inputs and a flat `output_schema` per agent step.
- Composition via `agent` / `parallel` / `pipeline` / `phase`.
- `$ref:inputs.<key>` and `$ref:<step_id>.output[.<field>]` data wiring.
- A `policy` block with `network: false`, `filesystem: false`, and a bounded `max_parallel`.
- Effects mediated by the injected `AgentRunner`.

**Blocked (skeleton):**

- Any `policy.network: true` or `policy.filesystem: true` (capability requests).
- Arbitrary code, `eval`/`import`-like forms, or disallowed builtins.
- Unresolved agent ids and malformed/dangling `$ref`s.
- Cyclic dependency or pipeline graphs.
- Direct network or filesystem access from the runtime.

### 3.4 The single effect boundary

The only path to the outside world is `AgentRunner`, a Protocol:

```python
class AgentRunner(Protocol):
    def __call__(self, agent_id: str, input: dict) -> dict: ...
```

In production this is the Hermes fan-out wiring. In the skeleton it defaults to
`StubAgentRunner`, a deterministic stub. Because every effect funnels through this
one injected callable, the trust boundary is small, auditable, and easy to mock.

---

## 4. Run stores, task adapters, and notification boundaries

The project has three adjacent but different persistence/visibility seams. Keep
them separate: workflow state, board/task execution, and chat notification have
different consistency, authorization, and lifecycle constraints.

### 4.1 `RunStore`: workflow-owned run state

`registry.py` defines a `RunStore` Protocol and injects it into both
`workflow_run` and `workflow_status` via the `registry=` parameter:

```python
class RunStore(Protocol):
    def create(self, handle: RunHandle, definition: dict) -> None: ...
    def update_step(self, run_id: str, step: StepStatus) -> None: ...
    def set_status(self, run_id: str, status: str, *,
                   result: dict | None = None,
                   error: dict | None = None) -> None: ...
    def get(self, run_id: str) -> RunStatus | None: ...
    def list(self) -> list[RunHandle]: ...
```

The default is `InMemoryRunStore`, a thread-safe (`threading.Lock`) process-global
implementation. `FileRunStore` gives embedders local snapshot/journal persistence.
Because the primitives accept `registry=` injection, downstream can swap the store
without touching the public primitives.

A Kanban-shaped run-status board remains a valid **store adapter** idea: board =
workflow, card = run, checklist = steps. But that is not the same as the shipped
`kanban_agent` task adapter. A board-backed `RunStore` should visualize workflow
state; it should not execute worker tasks, dispatch agents, or define generic
workflow semantics.

### 4.2 `kanban_agent`: durable task/work adapter

`kanban_agent` is the durable multi-agent awaitable, not the generic workflow
abstraction. The shipped implementation path is now:

| Piece | Responsibility |
| --- | --- |
| `KanbanBackend` | Create/reattach one idempotent task and await a terminal resolution. |
| `DurableKanbanBackend` | Persist card state/outcomes so replay/restart does not duplicate cards or lose completed work. |
| `EventLogKanbanBackend` + notifier | Resolve from a durable task-event log, using notifications as wakeup hints. |
| `HermesKanbanBackend` | Production-shaped Hermes CLI adapter: create/comment only, never dispatch. |
| `publish_hermes_kanban_event` | Worker/gateway-side bridge from real terminal task states into the workflow event log. |

The design rule is intentionally narrow: Dynamic Workflows creates or awaits
cards; gateway/Kanban dispatch owns claiming and executing those cards. The
adapter must not shell out to `dispatch`, run a daemon, spawn workers, or poll a
board to rediscover the phase. It translates workflow-owned state into board work
and translates terminal board events back into workflow results.

### 4.3 ATH/source bindings: notification and control ingress

ATH is the operator wakeup surface: signed events, listener/source-binding routing,
thread continuity, compact status updates, and approval prompts. It is not a
workflow executor. A typical long-lived run should look like:

```text
workflow/controller state
  -> task/event wait
  -> Kanban source binding or producer emits signed ATH event
  -> gateway wakes the original Discord/Telegram thread
  -> operator inspects workflow_control status or records approval/stop/retry intent
```

Core modules therefore expose backend-neutral seams (`LoopEventSink`, workflow
events, `workflow_control`, resource finalizer action strings such as
`ath.listener.retire`) rather than importing gateway or ATH internals. Host
adapters own delivery, auth, redaction, retries, and listener lifecycle.

### 4.4 AsyncSessionDB-era implication

Upstream Hermes now routes gateway `SessionDB` access through `AsyncSessionDB`, an
async facade that offloads synchronous SQLite calls with `asyncio.to_thread(...)`.
That improves liveness for the event-driven operator path above: ATH/source-binding
wakeups are less likely to be delayed by unrelated gateway `state.db` contention.

It does **not** move any workflow responsibility into the gateway. Dynamic
Workflows still owns run state, waits, approvals, cancellation intent, retries,
resources, artifacts, and finalizer contracts. ATH still owns signed event ingress
and conversation continuity. Kanban still owns durable worker/task graph state.
Cron remains only for calendar starts, heartbeats, or emergency compatibility — not
phase advancement.

Also note the blast radius: AsyncSessionDB protects Hermes gateway `state.db`
access, not plugin-owned JSONL/file stores, FIFO notifiers, Kanban registries, or
future shared databases. High-volume workflow/event ingestion still needs explicit
store/adapter concurrency design.

---

## 5. Subprocess workflow VM and RPC capability broker (issue #2)

The declarative JSON runtime (§1.5) interprets a static AST. A second,
additive execution mode lets the model author a **Python workflow script** — a
deterministic orchestration brain in the Claude Dynamic Workflows shape
(`agent()` / `kanban_agent()` / `capability()` / `parallel()` / `pipeline()` /
`phase()` / `log()` / `workflow()`, plus `args` / `budget`). Because a script is real code,
it is **never executed inside the parent Hermes process**. It runs in a
sandboxed subprocess; the parent owns every capability.

This mode is exposed as the library/operator primitives
`workflow_validate_script` and `run_workflow_script`, and as saved-harness
facade actions (`script_catalog`, `script_save`, `script_inspect`, `run_script`)
that load validated script versions through the same subprocess VM. The JSON
runtime is unchanged.

### 5.1 Three enforcement layers

```
   model-authored script
            │
            ▼
   ┌──────────────────────┐   layer 1: static launch gate (parent process)
   │  script_validator.py │   literal meta-first, bounded AST, no imports,
   │  validate_script()   │   no fs/process/net/env/clock/randomness names,
   └──────────┬───────────┘   no dunder traversal, no eval/exec/class/global
              │ ok
              ▼
   ┌──────────────────────┐   layer 2: scrubbed subprocess (vm.py / vm_guest.py)
   │  WorkflowVM._drive    │   python -B -s -m hermes_workflows.vm_guest with a
   │  scrubbed env + stdio │   from-scratch env (no Hermes/GitHub creds), narrow
   └──────────┬───────────┘   newline-framed JSON RPC over the child's stdio;
              │ boot           guest re-validates, then exec under restricted
              ▼                __builtins__ (allow-list only) and RPC-backed globals
   ┌──────────────────────┐   layer 3: capability broker (parent process)
   │  CapabilityBroker     │   method allow-list, known-agent/kanban gates,
   │  .handle(call)        │   capability registry policy, output schema,
   └──────────────────────┘   budget + max_rpc/agent/kanban/capability limits;
                              every request journaled with a stable call id
```

The layers are defence-in-depth and mutually distrustful. Even if the static
gate missed a construct, the guest restricts `__builtins__` to a safe allow-list
(no `open`/`eval`/`exec`/`__import__`), provides no module to the script except
deterministic `json`/`math`, and the parent broker treats the subprocess as
adversarial: it validates **every** RPC frame regardless of what the child sent.

### 5.2 The narrow RPC surface

Each frame is one line of UTF-8 JSON. Parent → child: `boot` (script, args,
limits, budget) and `ret` (response, with a piggybacked budget view). Child →
parent: `ready` (parsed meta), `call` (capability request with a stable
per-run id), and `done` (the script's return value or its structured error).
The protocol is strictly request/response, so a single stdio pipe pair carries
it without ambiguity. The guest reclaims the real stdout fd for the channel and
redirects `sys.stdout` to stderr, so a stray `print`/traceback can never corrupt
the stream the parent reads.

The only capabilities that cross to the parent are `agent`, `kanban_agent`,
`capability`, `log`, `phase`, and `workflow`; `parallel`/`pipeline` are
bounded deterministic guest-side combinators. Legacy `agent(agent_id, input)`
effects funnel through the injected `AgentRunner` boundary as the JSON runtime
(default `StubAgentRunner`), so script runs are reproducible and testable without
a live Hermes. Prompt-shaped `agent(prompt, opts)` effects cross the separate
`ChildAgentRunner` seam: by default they fail closed unless a host supplies a
runner; the Hermes plugin can now explicitly supply a `delegate_task`-backed
runner with `child_agent_backend="delegate_task"` (foreground structured summary
parse) or `"delegate_task_background"` (redacted dispatch-handle envelope only).
The adapter is intentionally foreground-script-only for now: local background
script runs reject `child_agent_backend` until delegate handles, completions, and
stop/retry visibility are persisted into workflow status/control.

### 5.3 Generic capability registry and policy (#29)

The `capability()` script global is the generic extension point for external tools/CLIs/session managers. Core does **not** ship a generic shell executor. Instead a host registers named handlers in `CapabilityRegistry`, for example `github.issue.view`, `relay.automation.status`, or `local.build.read_log`. Each registration declares a maximum `side_effect_class` from the same closed vocabulary used by scoped grants: `read_only`, `session_launch`, `session_control`, or `external_write`.

Each run may pass a `CapabilityPolicy` to `run_workflow_script` / `workflow_run_script`:

- `allowed_names` optionally narrows which registered names may be called;
- `allowed_side_effect_classes` defaults to `("read_only",)`, so mutating handlers fail closed unless explicitly allowed;
- `approval_required_classes` defaults to `session_launch`, `session_control`, and `external_write`; a script must supply an `approval_id` that appears in `approved_approval_ids` before those handlers run;
- `max_stream_bytes` clips returned `stdout`, `stderr`, `summary`, and `error` strings and annotates them with `*_truncated` flags;
- `max_result_bytes` fails closed if the total result is still too large after stream clipping.

Capability request metadata (`input`, `label`, `approval_id`, `schema`) is rejected before dispatch if it contains credential-shaped keys/values. Handler results are credential-redacted before returning to the script or entering run state, and handler exceptions are collapsed to bounded generic capability errors rather than forwarding raw exception text. Journal entries remain metadata-only by default (`method`, `call_id`, redacted `capability`, redacted `label`, `ok`, `error`) unless the caller deliberately disables redaction for debugging.

Replay/resume is fail-closed for generic side effects. The broker passes each handler `run.call_id` and `run.idempotency_key` (`<logical_run_id>:<stable_call_id>`). Registered capabilities are only written to the replay cache when `WorkflowCapability.replayable=True`; non-`read_only` capabilities that are not replayable are denied during replay rather than re-dispatched and duplicated. A replayable mutating handler must honor the idempotency key if it ever runs after a cache miss.

This gives workflow scripts a reusable tool/CLI/session-manager seam without adding repo-specific primitives or smuggling ambient credentials into the subprocess.

### 5.4 Failure containment and governance seam

A subprocess crash, a CPU-spin timeout (`VMLimits.max_runtime_s`), a protocol
breach, or an exit-without-result is mapped to a failed `ScriptRunResult` —
never an uncaught exception — so parent state is never corrupted. `VMLimits`
(`max_rpc_calls` hard-abort backstop, soft per-`agent`/`kanban` caps,
`token_budget`, `allow_nested_workflows`) is the first slice of the issue #11
governance surface; launch-approval/session routing remain parent-owned future
work. Journal events are metadata-only by default (method, call id, agent
id/profile, ok) — raw inputs/outputs and prompts are redacted.

### 5.5 What is intentionally deferred

The #2 slice proved the subprocess VM, the parent-owned RPC broker, the static
launch gate, and capability enforcement with tests. The durable run store and
deterministic replay cache (#3) are now implemented additively (see §5.7).
Still out of scope here (tracked by #4/#11): the full script-API surface with
loop guards and richer helpers (#4), true concurrent fan-out, general resume from
a *partial* run (this slice replays a *completed* run; a run suspended on a Kanban
await now resumes via §5.10, but resuming an arbitrarily-interrupted run from its
last completed step is still future), no-duplicate-Kanban side-effect dedup, and
launch-approval/session-policy governance (#11).

### 5.6 Saved script harness catalog (issue #29)

`script_catalog.py` turns the VM into a reusable harness library instead of a
one-off script launcher. A saved harness is a Python workflow script with a safe
single-segment name and an explicit integer version:

```text
<root>/<script-name>/v000001.workflow.py
<root>/<script-name>/v000001.meta.json
```

`FileWorkflowScriptCatalog.save_script()` validates source with the same static
launch gate before writing it, writes atomically, and keeps versions immutable
unless `replace=True` is explicit. New implicit saves append after the highest
visible version across all roots, so a profile-local catalog cannot silently
shadow a bundled example version. `load_script()` / `inspect_script()` resolve
only paths that stay under configured roots, so symlink/path-traversal escapes do
not become a filesystem-write primitive. The default roots are profile-local
`$HERMES_HOME/dynamic-workflows/scripts` plus package-bundled examples under
`hermes_workflows/examples/scripts` (with the repository `examples/scripts`
mirror also discovered in source checkouts).

The `workflow` facade exposes the catalog as model-facing operations:

- `script_catalog` — list latest or all saved versions;
- `script_save` — validate and persist generated harness source;
- `script_inspect` — return metadata and optional source for a saved version;
- `run_script` — load a saved version and execute it through `run_workflow_script`.

For model-authored calls that follow the observed Claude-style contract, the same
facade also accepts `script`, `scriptPath`, `name`, `args`, and
`resumeFromRunId`: inline `script` runs directly in the VM, `name` selects a saved
catalog harness, `scriptPath` is resolved only as a safe catalog-relative
`.workflow` / `.workflow.py` path, `args` feeds the script's `args` global, and
`resumeFromRunId` maps to `replay_from` so script/args identity mismatches fail
closed before launch.

This gives loop-engineering agents a generic "save this capability harness and
reuse it by name" substrate without granting scripts direct filesystem/network
access or hardcoding repo-specific primitives.

The bundled `generic_issue_lifecycle` script is the #8 flagship harness rather
than a fake profile demo. It accepts repo/issue/base/workspace/board/tenant and a
`profile_bindings` map at runtime, then performs the lifecycle with brokered
calls only: issue inventory, planner Kanban card, implementer Kanban card,
`hermes.github.pr_head` exact-head snapshot, reviewer + QA Kanban gates, a
bounded fixer loop controlled by `max_fix_attempts`, `hermes.github.release_exact_head`, and an ops closeout
card. Review/QA gates are strict: workers must return `approved: true` and the
current `head_sha`; missing approval/head values block release rather than
silently passing. The default stub runner can execute the graph end-to-end for dry runs; a
live host swaps in real Kanban/profile bindings and GitHub head/release agents
without changing the script source.

### 5.7 Durable script run store and deterministic replay cache (issue #3)

The broker journals each capability request with a **stable, ascending call id**
(1, 2, 3, ...) minted by the guest's RPC client. Because a workflow script is
deterministic — given the same `args` and the same sequence of RPC return
values it makes the same calls in the same order — those ids are a stable
address space across runs of the same script. `script_store.py` turns that into
durability without re-running deterministic work.

**Store layout.** `ScriptRunStore` persists each run under
`<root>/<run_id>/` (the parent owns the path; the script's subprocess still has
no filesystem authority):

| File | Contents |
|------|----------|
| `run.json` | Bounded metadata snapshot, atomically replaced: `schema_version`, `run_id`, `script_sha256`, `args_hash`, `status` (`running`/`succeeded`/`failed`), the public `meta` literal, a small `limits` view, the final `value`/`error`, `deterministic_runner`, `replay_of`, timestamps. **No raw script source, inputs, or prompts.** |
| `journal.jsonl` | Metadata-only events. Vocabulary: **`boot`** (script/args hashes, limits, determinism, replay provenance), **`call`** (call id, method, agent id/profile, label, `ok`, `error`, `replayed`), **`done`** (terminal status). In this synchronous broker the return outcome is folded into the `call` event, so a separate `return` line carries no extra information. Raw `params` are never written. |
| `cache.jsonl` | The **deterministic replay cache**: one line per replayable call, `{call_id, method, args_hash, value}`. |

**Run ids.** `wfs_<digest8>_<uuid12>` where `digest8` is the sha256 of the
canonicalized script+args (content-addressed, sortable-by-source) and the
`uuid4` suffix keeps it collision-resistant. Callers may pass an explicit
`run_id` for idempotency/tests; it must be a single safe path segment.

**What is replayable (conservative, opt-in to determinism).** `log` / `phase`
always (their result is a constant `None`). `agent` / `kanban_agent` **only**
when the caller declares the injected runner deterministic — auto-detected for
the default `StubAgentRunner` (a pure function of its inputs) or set explicitly
via `deterministic_runner=`. A live, non-deterministic Hermes runner caches **no**
agent output, so on replay those calls re-run rather than returning a stale
value. We do not fake safety: caching agent results is opt-in to a determinism
guarantee the operator makes.

**Replay.** `run_script(..., store=store, replay_from=<prior_run_id>)` loads the
prior run's cache **up front** (a corrupt/missing cache fails closed and typed
*before* any subprocess spawns) and re-runs the script. For each call the broker
consults the cache by call id:

- **hit** (cached entry whose `method` + canonical `args_hash` match the live
  call) → return the recorded value **without invoking the runner**; re-apply the
  recorded `_tokens` so the script's budget view stays consistent.
- **miss** (no cached entry for this id — the call was non-replayable in the
  source run) → fall through to a **live dispatch** (rerun).
- **mismatch** (a cached entry exists but `method`/`args_hash` drifted) →
  **fail closed**: a `replay_mismatch` denial aborts the subprocess and marks the
  run failed, rather than serving a value intended for a different logical call.

The `args_hash` (canonical JSON of the call's semantic params, excluding the
cosmetic `label`) is a per-call **integrity tag**: it detects a script/args drift
that would otherwise silently misalign the call stream. Combined with the fixed
`PYTHONHASHSEED=0` in the scrubbed env and the validator's ban on
clock/randomness/imports, this makes the cached call stream reproducible.

**Failure model.** Every load failure is a typed
`ScriptRunStoreError` subclass — `ScriptRunNotFound`, or `CorruptScriptRunError`
with a stable `reason` (`corrupt_run` / `corrupt_cache` / `schema_version`) — so
the parent declines to replay without corrupting state and may fall back to a
fresh run. A subprocess crash/timeout still yields a failed `ScriptRunResult` and
a `done` event with `status="failed"`.

**Trust boundary / accepted limitations.** The cache lives under the
parent-owned state dir; like `snapshot.json`, it trusts its own on-disk contents.
The integrity tag protects against a *drifting script*, not against an attacker
with write access to the state dir tampering with a cached `value` (out of scope
for this slice — same threat model as the JSON-runtime `FileRunStore`).
`cache.jsonl` can hold deterministic call *results* (e.g. stub agent outputs):
`log` / `phase` entries (constant `None`) are always cached, but `agent` /
`kanban_agent` *outputs* are cached **only** when the runner is declared
deterministic — so a non-deterministic run never persists agent/kanban result
data. The metadata `journal.jsonl` stays redacted by default (raw params and
script-authored error messages are never written). Budget enforcement is
best-effort on replay (recorded token spend is re-applied for determinism but the
hard cap is not re-checked on a faithful replay).

### 5.8 `kanban_agent` as a durable awaitable (issue #5)

`kanban.py` upgrades `kanban_agent` from a synchronous stub call into a
**durable, idempotent awaitable**, owned entirely by the parent broker — the
sandboxed subprocess still issues one RPC and blocks; it never touches a board.

**Backend seam.** A `KanbanBackend` (injected via
`run_workflow_script(..., kanban_backend=)`) exposes two operations the broker
drives: `create_or_reattach(idempotency_key, spec)` and
`await_resolution(card_id, *, accept_blocked, timeout)`. When no backend is
injected, `kanban_agent` keeps its prior AgentRunner behaviour, so existing runs
and tests are unchanged.

**Idempotency / no duplicate on replay.** The broker keys each card by
`<logical_run_id>:<stable_call_id>`. The call id is reproducible across a replay
(§5.7), and a replay inherits the source run's id as the root (via
`replay_from`), so create/reattach converges on **one** card per logical step.
Critically, a live Kanban call is a durable external effect, **not** a pure
function, so it is *excluded from the #3 replay cache* — on replay it re-runs and
the idempotency key reattaches the same card rather than serving a stale value or
opening a duplicate.

**Event-driven resolution.** The await is woken by a card *event*
(completed/blocked/failed), never by polling, and is bounded by the run's
`max_runtime_s` so a never-resolving card surfaces a typed `kanban_timeout`
denial instead of hanging the parent. `on_block` governs a *blocked* card:
`return` (default; surface a structured `status="blocked"` result), `raise` (a
catchable denial into the script), or `pause` (keep awaiting until a terminal
completed/failed event). Unknown assignee profiles are rejected with a structured
`unknown_profile` diagnostic before any card opens.

**Durable card state and resume across restart.** A Kanban await is
non-deterministic, so it is excluded from the #3 replay cache. Instead the latest
state of each card is persisted under the run store at
`<root>/_kanban/<card_id>.json` (keyed by the content-addressed card id, so it is
stable across replays) via `ScriptRunStore.record_kanban_card_state` /
`load_kanban_card_state`. Writes are atomic (unique temp file + `os.replace`) and
status-precedence, not numeric: a `waiting` marker never overwrites a card that
already reached an outcome, but among outcomes the latest real write wins —
because a card's events can originate in *incomparable* version spaces (a prior
process's backend vs. a fresh one on resume), so a numeric compare would wrongly
drop a live superseding outcome. `DurableKanbanBackend` wraps **any** inner
backend with this persistence and re-stamps every resolution into its own
monotonic version space (the inner's and the recorded outcome's versions are not
comparable), feeding the inner from the inner's own counter on a retry: `create_or_reattach` records a `waiting` marker (and reports
`reattached=True` when a record already exists), and `await_resolution` serves a
recorded outcome on the first await **without touching the inner backend** — so a
restarted or replaying parent resumes from the recorded worker result even if the
inner backend has no memory of the card. `kanban_waits()` exposes the in-flight
cards as a durable, operator-facing view.

**Durable event log (the producer seam).** The latest-state file above is written
by the parent's *own* await. The producer-facing half is an append-only
`<root>/_kanban/<card_id>.events.jsonl` via `ScriptRunStore.append_kanban_event` /
`read_kanban_events` / `latest_kanban_resolution`: a worker/gateway — possibly a
*different process* — durably records a card event there. `DurableKanbanBackend`
consults the event log **before** the latest-state file on its first await, so a
parent that was down when an event was produced **replays it from the log** on its
next await, even though no live in-memory backend ever saw it. This closes the
"event arrived while no parent was listening" gap for *recorded* events and gives a
durable audit trail (the in-memory event source could not survive a restart).

**Cross-process wakeup (the live notifier).** Replaying an already-recorded event
is not enough when a parent *blocks* on a not-yet-produced event from another
process. `kanban_notify.py` adds the live wakeup as a swappable seam:
`KanbanEventNotifier` is `notify(card_id)` / `subscribe(card_id).wait(timeout)`;
`ThreadEventNotifier` is the in-process default, and `FifoEventNotifier` is a
**cross-process** transport over per-card POSIX FIFOs (`os.mkfifo` + `select`,
Unix-only — the subscriber holds a write end too so the read end never EOF-spins).
`EventLogKanbanBackend` is the production-shaped backend: it resolves a card purely
from the durable event log (no in-memory event source — the producer may be a
different process via `publish_kanban_event`), blocking on the notifier between log
reads and bounded by the run deadline. The notifier is a wakeup *hint*; the durable
log is the source of truth, so a missed/raced signal is never a lost event (at
worst observed a little later) — and subscribing before the first read means a
signal that races in is buffered, so the await is event-driven, not a poll loop.
The remaining residual is a cross-*host* transport (`LISTEN/NOTIFY` / a broker
topic): the shipped notifiers cover one host (in-process and single-machine FIFO).
A card that was only ever `waiting` (no recorded event) is still re-awaited live.

**Honest fake vs production.** `InMemoryKanbanBackend` is a real, event-driven
(`threading.Condition`) fake for tests/local dev — it is **not** production. A
production backend implementing the same interface must: create/reattach through
the real Kanban DB/API using the idempotency key as the unique key (so concurrent
parents and replays converge), subscribe to **durable** card events that survive a
parent restart (composing with `DurableKanbanBackend` for the recorded-outcome
half), and defer dispatch to the gateway (the workflow only creates/waits — no
duplicate dispatcher). The remaining item — a true `pause` of an *unresolved* card
that persists the suspended run and resumes it in a fresh process from a replayed
event rather than holding a thread — now ships as a seam (§5.10); what a real
backend still owns is durably *producing* the wakeup event from the worker side.

### 5.11 Real Hermes Kanban backend adapter (issue #5)

`hermes_kanban.py` is the production-shaped backend that closes the
"documented/stubbed, not implemented" residual. `HermesKanbanBackend` implements
the same `KanbanBackend` interface and composes the shipped durability rather than
re-implementing it:

* **create/reattach via a CLI seam.** `create_or_reattach` rejects an unknown
  assignee profile **before** any card is opened, derives the content-addressed
  `kanban_card_id(idempotency_key)` (stable across replays), and — only when the
  durable store has *no* record of that card — opens a real card through the
  `HermesKanbanClient` seam. The default `SubprocessHermesKanbanClient` shells out
  to **exactly one** current-contract `hermes kanban create` invocation: global
  `--board` (when supplied), positional title plus `--body`, `--assignee`,
  repeated `--parent`, `--workspace`, `--tenant`, and `--idempotency-key`. Fields
  the CLI does not support as flags (labels and the logical card id) are not
  passed as fake options; they remain visible in the rendered body together with
  the worker prompt, context, task/input payloads, and the issue-#6
  result-contract instruction. This runs in
  the **parent/operator** process (it legitimately holds Hermes credentials) —
  never the sandboxed workflow subprocess, preserving the same trust boundary every
  other capability uses. A restart/replay that already has a durable record
  **reattaches** with no second create, preserving idempotency and the no-duplicate
  guarantee.

* **resolve from real terminal events.** `await_resolution` delegates to a
  composed `EventLogKanbanBackend` (§5.8), so the await is event-driven from the
  durable log, bounded by the run deadline, and honours `after_version` and the
  `on_block` policy. The narrow **Kanban task-event bridge** (the only #7 seam
  this slice touches) is `map_hermes_terminal_status` /
  `publish_hermes_kanban_event`: a worker/gateway normalises a real Hermes
  terminal task status — `completed`/`blocked`/`failed`/`timed_out`/`crashed`/
  `gave_up`/cancellation — onto the three resolution statuses (the failure family
  folds onto a single structured `failed`, with the specific name preserved in
  `reason`), then durably publishes it. A non-terminal/unknown status is rejected,
  so a transient running/queued update is never mistaken for an outcome.

* **never a dispatcher.** The adapter only creates and awaits. `assert_no_dispatch`
  refuses any `hermes kanban` argv whose subcommand is not `create`/`comment`, so
  it can never shell out to `dispatch`/`daemon`/`worker`/`spawn`/`serve` even if a
  builder is changed carelessly — gateway dispatch owns claiming and executing the
  work.

This is a **library/operator** backend injected via
`run_workflow_script(kanban_backend=)`; it is not registered as a model-facing
tool. Production residuals (tracked for the gateway integration, not this slice):
durably *producing* terminal events from the worker side, a `hermes kanban
comment` path for board-side result-validation diagnostics, and a cross-host
notifier transport (§5.8).

### 5.9 Structured result contracts for Kanban tasks (issue #6)

A workflow cannot safely branch on a prose summary. When `kanban_agent` is given
a `schema=`, that schema is treated as a **template-guided payload schema over a
stable envelope** — *not* one global workflow schema:

* The worker completes a card by setting `metadata.workflow_result` (the
  `workflow_result` key) to a structured payload. The card body carries a
  rendered instruction telling the worker to do exactly that, and to **block**
  rather than complete with prose if it cannot.
* The parent runtime validates that payload against the schema with
  `validate_workflow_result` (`kanban.py`) **before resolving the awaitable**.
  Every declared `field -> type` must be present with the declared type; **extra
  fields are preserved** (a repo/agent template may define a stricter shape), and
  with **no schema** any payload passes through untouched.
* A completed card whose `workflow_result` is missing or fails the schema is a
  contract violation, never a success. It is turned into a **deterministic
  block** with field-level diagnostics: under `on_block="pause"` the broker waits
  for the worker to **retry** with a valid result (resolutions are versioned so
  the await blocks for a strictly newer event); under `return`/`raise` it surfaces
  as a `blocked` envelope / `kanban_blocked` denial.
* Diagnostics are recorded in two places: a metadata-only `result_invalid` marker
  in the run journal (the per-field detail stays out of the redacted journal) and
  a Kanban card comment/event via the backend's optional `record_event` hook.

The awaitable therefore resolves to a **typed object** (`workflow_result`) the
script can branch on, or to a deterministic block — never to unvalidated prose
dressed as success. The worker-side enforcement (a Kanban tool that rejects a
completion lacking a valid `metadata.workflow_result`) is the production
counterpart to this parent-side validation and remains future work alongside the
real Kanban backend (§5.8).

### 5.10 Durable suspend/resume of an unresolved paused await (issue #5)

§5.8 made `on_block="pause"` keep awaiting a card until a terminal event, bounded
by the run's wall-clock limit — an *in-process* hold that, on a card that never
resolves in time, simply failed the run with `kanban_timeout`. This slice closes
the last residual of the durable-pause story: a paused, **unresolved** card now
*suspends* the run rather than holding a thread to the deadline, and a fresh
process *resumes* it from a replayed event. It reuses the existing pieces (the
durable card-state file, the append-only event log, and the notifier/event-log
backend of §5.8) — no new store layout, no cross-host transport.

**Opt-in suspend window.** `VMLimits.kanban_suspend_after_s` (default `None`,
preserving the prior block-to-deadline behaviour) bounds how long a paused await
waits in-process before suspending. The broker bounds each `await_resolution` by
the *nearer* of the run deadline and the suspend window; a `KanbanTimeout` raised
by the suspend window — distinguished from the run deadline by re-checking that
wall-clock remains — triggers a suspend instead of a failure. The window is capped
at `max_runtime_s`, so a value `>= max_runtime_s` never preempts the genuine
`kanban_timeout` (the run deadline wins). Fast human unblocks within the window
still resolve in-process exactly as before.

**Suspend is a teardown, not a value.** A paused await cannot hand a non-result
back to the script, so suspension mirrors the existing `should_abort` path: the
broker sets `should_suspend` + `suspend_info` (the metadata-safe `card_id` /
`profile` / `call_id` / `on_block`) and the VM kills the subprocess — the script's
in-subprocess local state is discarded, which is safe because a resume re-runs the
script from scratch over the §5.7 replay cache. The run is reported as a distinct
`ScriptRunResult(suspended=True)` (it is neither `succeeded` nor `failed`) and
recorded with `run.json` status `suspended`; `ScriptRunStore.suspended_runs()` is
the operator/resumer-facing discovery view.

**Resume is just replay.** Resuming a suspended run is the ordinary
`run_workflow_script(..., replay_from=<suspended_run_id>)` path (§5.7). The replay
re-runs the script: deterministic calls before the pause are served from the
cache, and the paused `kanban_agent` reattaches the **same** content-addressed
card (its idempotency key keys on the original logical run, stable across the
replay) and reads the durable event log. If a worker/gateway — possibly a
different process — has since durably appended a terminal event (§5.8's
`publish_kanban_event`), the await resolves and the run completes; if the card is
still unresolved, the run suspends again. Because the Kanban await is excluded
from the replay cache, the resume never serves a stale value: it always re-reads
the durable outcome. The recorded run's `kanban_suspend_after_s` is pinned on the
replay (like the other caps) so a still-unresolved card suspends again rather than
blocking, unless the resumer overrides `limits=`.

**Boundary.** This is single-host and resume-driven, not a live scheduler: a
suspended run is resumed when something re-invokes `run_workflow_script` with
`replay_from` (an operator, a cron, a gateway callback). The parent does not itself
hold the suspension open. Durably *producing* the wakeup event from the worker
side, and a cross-host notification transport, remain the production residuals
(§5.8).

### 5.12 Generic workflow event broker for GitHub/webhook wakeups (issue #7)

Kanban awaits use the task event log above. Non-card signals (GitHub PR/check/review/deployment webhooks, gateway-origin events, future external predicates) use `events.py`:

- `WorkflowEvent` is a durable, credential-redacted event envelope with stable `event_id`, `source`, `event_type`, `subject`, monotonic store-assigned `version`, and compact `payload`.
- `WorkflowEventPredicate` matches `source` / `event_type` / `subject` / dotted `payload_match` fields and ignores stale events with `after_version`; stores expose `current_version()` so callers can register "from now" waits without racing old events.
- `InMemoryWorkflowEventStore` and `FileWorkflowEventStore` append idempotently by `event_id`; duplicate webhook delivery returns the original stored version. The file store uses a POSIX lock file for multi-process local writers and degrades to in-process locking on non-POSIX hosts.
- `WorkflowEventBroker.wait_for(...)` checks the durable store first, then blocks on an injected notifier between reads. `ThreadWorkflowEventNotifier` is same-process; `FifoWorkflowEventNotifier` is a single-host cross-process wakeup channel. The notifier is only a wakeup hint; the file/event store is the source of truth, so process restart after event arrival still works and missed wakeups degrade to bounded idle re-read.
- `workflow_event_from_github_webhook` / `publish_github_webhook_event` normalize GitHub webhook payloads into compact PR/issue/check/deployment subjects without performing any GitHub API polling. GitHub event/action components are validated before becoming `event_type`; headers are not persisted.

This intentionally stops at the event substrate. A host webhook receiver owns authenticity checks and calls the producer helper. A workflow/controller owns phase policy after a matched event. Dynamic Workflows core does not run a daemon, timer poller, or dispatcher.

### 5.13 Adversarial review and residual limitations

The validator/guest/broker boundary was red-teamed with an independent
multi-agent review whose findings were each reproduced against the real VM.
Confirmed-and-fixed escapes (now regression-tested):

- **Live-module pivot** — the real `json` module exposes `codecs` (`json.codecs.open`,
  `codecs.builtins.eval`), a full FS + arbitrary-code escape reachable via
  non-dunder attributes. Closed by injecting curated `json`/`math` proxies
  instead of live module objects (§5.1).
- **Frame/coroutine internals** — `coroutine.cr_frame.f_globals` → `sys.modules`
  → `os`. Closed by rejecting `gi_`/`cr_`/`ag_`/`f_`/`tb_`/`co_` attribute
  prefixes in the validator.
- **`str.format` template traversal** — `"{0.__class__.__base__}".format(x)`
  reaches dunders at runtime, invisible to the AST gate. `.format`/`.format_map`
  are rejected; f-strings (real AST) remain the safe formatting path.
- **Unenforced `token_budget`** — now a hard ceiling that aborts the run.
- **Runner `BaseException`** — a misbehaving `AgentRunner` raising `SystemExit`
  could escape the broker and crash the parent; it is now contained as a
  structured `runner_error`.

Known, accepted limitations (not security escapes):

- **Soft per-agent/kanban caps.** `max_agent_calls` / `max_kanban_calls` are
  *catchable* denials so a script can adapt; total work is still bounded by the
  `max_rpc_calls` hard cap and `max_runtime_s`.
- **Object-repr determinism.** A script that deliberately serializes a *live
  object* (e.g. `repr(some_function)`, or an exception message built by CPython
  that embeds one) gets a heap address, which varies per process. Pure-data
  returns are deterministic; the parent's own serialization fallback never emits
  an address. Fully sanitizing every script-chosen string is out of scope.
- **No memory/FD quota yet.** A script can exhaust memory; the OS/`max_runtime_s`
  is the current backstop. A `resource.setrlimit` guard in the guest is future
  hardening.

## 6. GitHub issue lifecycle hygiene template

`examples/github_issue_lifecycle_hygiene.workflow.json` is the first saved template
for the end-to-end "ship this issue" loop (#8). It intentionally starts with a
GitHub inventory step before planning work, because the overnight dogfood showed
that stale local/session context can otherwise make agents re-open or re-plan
work already merged. The template's durable shape is:

1. `inventory` — collect current issue state, linked/merged PRs, docs touched, and
   known blockers before choosing work.
2. `plan_slice` — pick exactly one non-duplicate implementation slice from that
   inventory.
3. `implementation` — produce the PR and include tests/docs evidence.
4. `verification_gates` — run exact-head review and docs gates in parallel.
5. `closeout_hygiene` — comment/update/close GitHub issues only when acceptance is
   satisfied, update parent roadmap state, and file follow-ups for residual docs or
   product gaps.

The template is a catalog fixture and a contract example, not a production GitHub
adapter by itself: the bundled `StubAgentRunner` returns deterministic echo output
for tests, while a live deployment wires those profiles to real Kanban/Hermes
workers and GitHub tools. Until declarative `kanban_agent.profile` supports dynamic
refs, the template passes a `profile_bindings` config object through each task
payload so the live adapter can map the default lanes (`planner`, `implementer`,
`reviewer`, `docs`, `ops`) to local profile names.

The closeout stage is nevertheless part of the contract: shipping is incomplete
until issue hygiene and docs hygiene have explicit evidence.

## 7. Event-driven trigger migration (#10)

Dynamic Workflows replace timer watchdog orchestration by making the workflow run,
not cron, own phase state. A cron job may still *start* a workflow on a calendar,
and a script-only no-agent job may still send a simple visibility heartbeat, but a
goal-directed workflow must not rely on "wake every N minutes, poll GitHub/Kanban,
reconstruct the phase, and ask an agent what to do next." That pattern is
expensive and unsafe: it duplicates work, loses exact-head context, and turns
stale polls into orchestration decisions.

The migration shape is:

1. **Start from an event or calendar edge.** A webhook, queue message, manual
   operator action, or calendar tick creates one workflow run with the trigger
   payload in `inputs`.
2. **Advance through durable awaits.** `kanban_agent` steps, loop waits, and
   adapter-specific event subscriptions park between phases. The terminal task or
   event record resumes the workflow; a timer does not rediscover the phase.
3. **Expose state through operator status.** `workflow_control overview/status`
   shows blocked waits and projected pause/stop/retry intent, so humans and
   dashboards inspect state without owning it.
4. **Notify and stop.** Status/WIP digests are outputs of a workflow or simple
   notifiers. They do not mutate implementation/review/release phase state.

Existing watchdog migrations:

| Old watchdog | New workflow shape |
| --- | --- |
| Issue lifecycle poller | Start `github_issue_lifecycle_hygiene` once; Kanban task events advance inventory → plan → implementation → verification → closeout. |
| PR validation poller | Start `event_driven_pr_validation_lane` from `pull_request.opened` / `synchronize`; QA and review are durable waits, and the summary emits one update. |
| Board unblocker/fixer loop | Run a loop-controller sensor/actuator pair; the actuator emits one fix card/session and waits for its event instead of polling the board. |
| WIP synthesis/status notification | Use a calendar-started workflow or script-only notifier for one digest; keep orchestration decisions in the workflow state machine. |

`examples/event_driven_pr_validation_lane.workflow.json` is the first concrete #10
fixture. It accepts `trigger_event`, normalizes the PR head once, fans out QA and
review through `kanban_agent` awaits, then produces a validation summary. In the
bundled tests this runs under `StubAgentRunner`; in production the same shape is
backed by real Kanban/Hermes profiles and webhook delivery.

## 8. Roadmap

Near-term and future work, roughly in priority order:

1. **Live Hermes `AgentRunner`.** Replace `StubAgentRunner` with the real Hermes
   fan-out adapter; surface concurrency, timeouts, and retry policy through the
   Protocol while preserving deterministic stubbing for tests.

2. **True asynchronous execution.** The script-VM side now ships real concurrent
   fan-out: bounded-concurrency `parallel()` and no-barrier `pipeline()` honoring
   `max_parallel` (operator-configurable through the `workflow(action="run_script")`
   facade), with lifecycle-safe failure — a failed run drains/awaits already-started
   parent-side runner work and reports work still running past the deadline rather
   than returning terminal while child effects are in flight (issues #71/#72). The
   operator-control seam ships the durable intent half: pause/resume/stop/
   `task_stop`/retry records, a control-state projection, and wait inspection
   (§1.5.2, issue #9). Remaining: an executor that *enforces* pause/retry intents
   on the declarative JSON runtime, force-cancellation of running `ThreadPoolExecutor`
   child calls, and the per-definition `policy.max_parallel` for JSON workflows.

3. **Resume / journal engine.** Build on the existing append-style run records and
   `def_hash` correlation to support resuming an interrupted run from the last
   completed step — the resumable-journal idea borrowed from Dynamic Workflows,
   realized as a first-class feature. The script-VM side now ships the first
   piece of this: a durable `ScriptRunStore` plus a deterministic replay cache
   that re-runs a *completed* script without duplicating deterministic RPC work
   (§5.7, issue #3), and a run *suspended* on an unresolved paused Kanban await now
   resumes in a fresh process from a replayed event (§5.10, issue #5). Remaining:
   general resume from a *partial* run (arbitrary mid-step interruption) and dedup
   of durable side effects (e.g. no-duplicate Kanban task creation) on rerun.

4. **Host adapter hardening and shared stores.** The core seams now exist; the next
   production work is adapter glue, not more hidden runtime authority:
   - ATH event sink/source-binding adapters for compact `workflow.started`,
     `phase.finished`, `approval.required`, `resource.cleanup.failed`, and
     `workflow.finished` notifications;
   - a Kanban terminal-event producer that calls `publish_hermes_kanban_event`
     from trusted worker/gateway events instead of a poll loop;
   - a resume trigger that replays suspended script runs when a matching durable
     task/event arrives;
   - concrete finalizer handlers such as `ath.listener.retire`, process/session
     cleanup, and workspace cleanup behind the action registry;
   - a shared store/notifier adapter (`LISTEN/NOTIFY`, broker topic, or similar)
     for multi-host event waits once the local JSONL/FIFO path is too small.

5. **Richer schema & references.** Nested/typed `output_schema`, conditional
   steps, map-over-collection fan-out, and additional `$ref` forms — each gated
   behind new `workflow_validate` checks and stable diagnostic codes.

6. **Real sandboxed code execution.** The subprocess workflow VM (§5) ships the
   first slice of this: model-authored Python scripts run out-of-process under a
   scrubbed environment, a static launch gate, restricted builtins, and a
   parent-owned RPC capability broker — without a JS engine or any runtime
   dependency. Durable journal + deterministic replay for scripts (#3) now ships
   additively (§5.7). Remaining work: the full script API with loop guards (#4),
   resume from a partial run, and resource quotas beyond the wall-clock timeout
   and call-count caps.

7. **Capability grants beyond default-deny.** Allow `policy` to *request* network
   or filesystem capabilities, mediated by an out-of-band grant mechanism, so the
   default-deny posture can be relaxed deliberately rather than implicitly.

8. **Observability.** Structured event emission per step transition, metrics
   (durations, fan-out width, failure rates), and trace correlation by `run_id` /
   `def_hash`. Observability must emit compact typed facts; raw transcripts,
   secrets, and backend-specific task payloads stay behind adapter-owned stores.

9. **Claude-style API parity residuals.** A verified comparison of the script
   contract against the observed `ultracode` workflow API (`agent`/`parallel`/
   `pipeline`/`phase`/`budget`/`workflow`) confirms most core hooks ship; the
   tracked residual gaps are:
   - nested `workflow(name | {scriptPath}, args)` inline execution — the in-VM
     `workflow()` RPC currently always denies (`nested_denied`/`nested_unsupported`)
     rather than running a child workflow one level deep ([#91]);
   - the `agentType` `agent()` option (custom subagent type selector) ([#92]);
   - a truncate-and-spill result tier (capped completion notification +
     on-demand spill) so the result is not delivered inline ([#93]);
   - a `parallel()`/`pipeline()` per-call element cap and `effort` enum
     validation ([#94]);
   - persisting run-level stats / `workflowProgress` into the journal record ([#68]).

   Intentional divergences (not gaps): the authoring surface is declarative
   JSON / sandboxed Python, not executed JS (§2.2, §3); `agent()` failures raise
   a structured error rather than returning a `null` sentinel; resume is
   positional call-id replay with fail-closed drift-abort plus a `v2` prompt/
   options fingerprint cache, not Claude's content-hash-only ledger; concurrency
   is `max_parallel`-bounded rather than `min(16, cpu-2)`.
