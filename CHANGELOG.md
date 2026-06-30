# Changelog

This project uses [Semantic Versioning](https://semver.org/) starting at `0.1.0`.

## [0.1.0] - unreleased

First public alpha release line for `hermes-plugin-dynamic-workflows`.

### Added

- Single model-facing `workflow` tool facade for validate/run/status/catalog/script operations.
- Declarative JSON workflow runtime with `agent`, `kanban_agent`, `if`, `parallel`, `pipeline`, and `phase` steps.
- Parent-owned run persistence via snapshots and compact journals.
- Subprocess workflow-script VM with restricted builtins, scrubbed environment, bounded output, and parent-owned RPC capability broker.
- Host-owned capability registry/policy with side-effect classes, approval ids, credential rejection/redaction, and replay/idempotency metadata.
- Backend-neutral workflow event broker for durable webhook/task event waits without polling-owned phase control.
- Versioned saved script harness catalog.
- Feedback loop controller with sensors, actuators, brakes, waits, approvals, scoped session grants, and resource finalizers.
- Operator control/status surface for pause/resume/stop/task_stop/retry intent and blocked-wait inspection.
- Public README landing-page rewrite, examples, and Baoyu-style infographic asset for the `0.1.0` release line.

#### Claude-style dynamic-workflow parity

- Prompt-shaped child agents — `agent(prompt, opts)` (label/phase/schema/model/effort/isolation/context) with schema-constrained structured output and bounded retry-on-mismatch before a typed failure.
- Resumable prompt-agent fingerprint cache keyed by a `v2` prompt/options hash, so a duplicate prompt/options call dedups to one child and replays without respawning.
- Bounded concurrent `parallel()` and no-barrier `pipeline()` item flow with operator-configurable `max_parallel` (wired through the `workflow(action="run_script")` facade) and lifecycle-safe failure: a failed run waits for already-started parent-side runner work and reports any work still running past the deadline instead of returning terminal while it is alive.
- Local background workflow-script run manager: scripts launch/run/inspect/stop outside the main turn with fail-closed stop/terminal-state lifecycle and operator-visible status.
- Per-subagent transcript artifacts — per-call journal plus redacted metadata refs (including replay/cache-hit refs), surfaced in background run links.
- Archive-backed loop-until-dry parity fixture exercising parallel fan-out plus prompt/options replay.

### Fixed

- Replay/cache-hit accounting no longer deadlocks when a journal callback re-enters the broker: shared-state mutations are taken under the broker lock in a short scope and journal/transcript writes happen after the lock is released.
- `parallel()`/`pipeline()` dispatch indices (`_parallel_index`, `_pipeline_item_index`, `_pipeline_stage_index`) are treated as internal scheduling metadata, so prompt-agent calls inside concurrent fan-out are no longer rejected as unsupported options.
- Concurrent identical prompt-agent calls record a single cache fingerprint (no duplicate-fingerprint corruption of the replay cache).

### Known limits

- Public alpha: useful for local Hermes/plugin experiments and adapter prototyping, not a hardened production sandbox for arbitrary untrusted users.
- The core runtime is dependency-free and backend-neutral; real gateway, task-board, CI, or session-control integrations must be supplied by host adapters.
- Declarative JSON workflows do not execute generated code. Script harnesses run only through the subprocess VM and parent-owned capability boundary.
- Event durability is local-store oriented; multi-host production deployments should provide a shared store/notifier adapter.
