# DESIGN.md — `hermes_workflows`

A Hermes agent plugin that exposes one model-facing tool facade plus debug
primitives for sandboxed, script-led orchestration over Hermes agents:

- `workflow` — the only model-facing Hermes tool: dry-run validate, run a definition, or inspect an existing run id.
- `workflow_validate` — library/operator primitive that statically checks a workflow definition (parse, schema, sandbox-policy lint) without running anything.
- `workflow_run` — library/operator primitive that executes a validated definition in a deterministic, sandboxed runtime, fanning out to Hermes agents or durable Kanban awaitables.
- `workflow_status` — library/operator primitive that queries the state/progress of a run by id.

This document describes the architecture, the design decisions behind it, what we
borrow from Claude Dynamic Workflows and how we differ, the sandbox security
model, the optional Kanban run-status backend, and the roadmap.

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
  saved catalog template, `action='catalog'` lists saved templates, and `run_id`
  without a definition reads status. It is the tool shape meant for model use.

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
| `schema.py` | Parse JSON, validate top-level shape, step kinds, references; emit `Diagnostic`s with stable codes and JSON-Pointer `pointer`s. |
| `sandbox.py` | Documents and **enforces** the capability policy (default-deny). Static lint only; not a JS engine. |
| `runtime.py` | Deterministic interpreter over the validated AST (`agent`/`kanban_agent`/`if`/`parallel`/`pipeline`/`phase`). Never `eval()`s; never imports user-named modules. |
| `registry.py` | `RunStore` Protocol + thread-safe `InMemoryRunStore`; `FileRunStore` snapshots/journals; `KanbanRunStore` documented/stubbed. |
| `agents.py` | `AgentRunner` Protocol (`(agent_id, input_dict) -> output_dict`) + deterministic `StubAgentRunner`, including reserved `kanban.<profile>` outputs. |
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

### 1.5 The sandboxed runtime (skeleton)

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

---

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

## 4. Kanban backend option (pluggable run-status store)

### 4.1 The `RunStore` seam

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
implementation. Because the primitives accept `registry=` injection, downstream
can swap the store **without touching the primitives**.

### 4.2 Board / columns / cards mapping

A Kanban board is a natural visual model for run lifecycle, so `KanbanRunStore` is
**documented and stubbed** here as an alternative store. The mapping:

| Kanban concept | Workflow concept |
|----------------|------------------|
| **Board** | A workflow (grouped by `name` / `def_hash`). |
| **Columns** | The run lifecycle states: `queued` → `running` → `succeeded` / `failed` / `cancelled`. |
| **Cards** | Runs (one card per `run_id`), optionally with sub-cards/checklist items per `StepStatus`. |
| **Card moves** | `set_status` transitions move a card between columns. |
| **Card fields** | `created_at`, `updated_at`, `def_hash`, `Progress`, and per-step `output`/`error`. |

The `RunStore` operations map cleanly onto board operations: `create` opens a card
in the `queued` column, `update_step` updates a card's checklist, `set_status`
moves the card to the next column, and `get` reconstructs a `RunStatus` from the
card.

### 4.3 Pluggable vs in-memory — trade-offs

| | `InMemoryRunStore` (default) | `KanbanRunStore` (optional, stubbed) |
|---|---|---|
| **Dependencies** | None (stdlib only). | External Kanban service/API. Out of scope for the skeleton. |
| **Durability** | Process-lifetime only; lost on restart. | Durable, externally backed. |
| **Visibility** | Programmatic (`workflow_status`). | Human-facing board UI for free. |
| **Latency** | In-process, microseconds. | Network round-trips per transition. |
| **Concurrency** | `threading.Lock`-guarded, single process. | Multi-process / multi-host observers. |
| **Failure modes** | None beyond the process. | Must handle network errors, retries, eventual consistency. |

**`KanbanRunStore` is intentionally NOT implemented** in the skeleton (it would
require an external dependency and network access, both prohibited). It is
described and stubbed so the seam is proven and the contract is documented; a
downstream integrator implements the Protocol against their board of choice and
injects it via `registry=`.

---

## 5. Roadmap

Near-term and future work, roughly in priority order:

1. **Live Hermes `AgentRunner`.** Replace `StubAgentRunner` with the real Hermes
   fan-out adapter; surface concurrency, timeouts, and retry policy through the
   Protocol while preserving deterministic stubbing for tests.

2. **True asynchronous execution.** Move from the deterministic synchronous
   scheduler to real concurrent fan-out honoring `max_parallel` and the per-
   definition `policy.max_parallel`, with backpressure and cancellation
   (`status='cancelled'`).

3. **Resume / journal engine.** Build on the existing append-style run records and
   `def_hash` correlation to support resuming an interrupted run from the last
   completed step — the resumable-journal idea borrowed from Dynamic Workflows,
   realized as a first-class feature.

4. **Durable & Kanban stores.** Provide at least one durable `RunStore`
   implementation and a concrete `KanbanRunStore` against a real board, validating
   the board/columns/cards mapping in §4.

5. **Richer schema & references.** Nested/typed `output_schema`, conditional
   steps, map-over-collection fan-out, and additional `$ref` forms — each gated
   behind new `workflow_validate` checks and stable diagnostic codes.

6. **Real sandboxed code execution (separate milestone).** Design and ship an
   embedded, resource-quota'd, escape-hardened execution sandbox if and only if a
   scripting surface is required. This is explicitly **out of scope** for the
   skeleton (see §3.1) and will carry its own threat model and dependency review.

7. **Capability grants beyond default-deny.** Allow `policy` to *request* network
   or filesystem capabilities, mediated by an out-of-band grant mechanism, so the
   default-deny posture can be relaxed deliberately rather than implicitly.

8. **Observability.** Structured event emission per step transition, metrics
   (durations, fan-out width, failure rates), and trace correlation by `run_id` /
   `def_hash`.
