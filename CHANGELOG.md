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

#### deepagents stack-up wave (2026-07)

Grounded in the adversarially-verified architecture comparison at
`docs/analysis/deepagents-stackup-2026-07.md` (epics #99/#100; every slice
adversarially reviewed pre-merge, Copilot-reviewed on the PR where available).
Verified end-to-end in a live Hermes session (profile plugin checkout at this
wave's head: `workflow` tool validate + run of a stub-backed script succeeded).

- Explicit known-agent roster registration (`register_known_agent`) — the bare
  `hermes.*` wildcard no longer validates typo'd agent ids silently (#105).
- Fail-closed size bound on `agent`/`kanban_agent` results (`max_result_bytes`,
  `result_too_large`) measured with the persistence-path encoder, so oversized
  payloads never reach script memory or the replay cache (#106).
- Retryable/catchable dispatch-error taxonomy: `CapabilityDenied.retryable`,
  catchable `runner_error` with deterministic failure replay (buffered records
  flushed only for handled failures, preserving the pending-writes contract) (#103).
- Shared JSON-Schema-subset output validator (nested objects/arrays/enums)
  across the script VM and JSON engine, fail-closed on unknown keywords, with
  legacy flat schemas normalized compatibly; `kanban_agent` stays legacy-flat,
  statically enforced (#107).
- Journal durability modes on `ScriptRunStore` (`sync`/`async`/`exit`) with
  force-flush on every terminal status (#108).
- Pending-writes resume contract stated in DESIGN.md §5.7.2 and pinned by a
  crash-mid-`parallel()` fixture (#109).
- `tools` allowlist on `ChildAgentRequest`/`agent()` opts, feeding the replay
  fingerprint only when set (pre-existing fingerprints stay byte-identical) (#101).
- Pluggable store contract (`ScriptRunStoreProtocol`) plus a SQLite backend
  (WAL, versioned schema, typed error boundary, cross-process-safe kanban
  event sequencing) (#110).
- Host-declared child-visible-context quarantine on the `ChildAgentRunner`
  seam — allowlist-only, fail-closed, dropped key names journaled redacted (#102).
- Interactive interrupt decisions on suspended approval-gated calls:
  approve/edit/reject/respond via `workflow_control`, journaled and
  replay-deterministic, with edited params becoming replay-authoritative (#111).
- `agentType` opt (#92) resolved against a file-based agent-type registry
  (project-over-user scope, frontmatter + system-prompt body, fail-closed
  hygiene, built-in `general-purpose` default) (#104).
- Async child-agent lifecycle globals — `agent_start`/`agent_check`/
  `agent_cancel`/`agent_list` over a new `AsyncChildAgentRunner` seam with
  deterministic handles, sticky terminal states, governance caps, and token
  accounting on live and replayed paths (#112; durable suspend follow-up #129).
- Deflaked wall-clock-sensitive background launch/stop timing tests (#119, #125).

### Fixed

- Replay/cache-hit accounting no longer deadlocks when a journal callback re-enters the broker: shared-state mutations are taken under the broker lock in a short scope and journal/transcript writes happen after the lock is released.
- `parallel()`/`pipeline()` dispatch indices (`_parallel_index`, `_pipeline_item_index`, `_pipeline_stage_index`) are treated as internal scheduling metadata, so prompt-agent calls inside concurrent fan-out are no longer rejected as unsupported options.
- Concurrent identical prompt-agent calls record a single cache fingerprint (no duplicate-fingerprint corruption of the replay cache).

### Known limits

- Public alpha: useful for local Hermes/plugin experiments and adapter prototyping, not a hardened production sandbox for arbitrary untrusted users.
- The core runtime is dependency-free and backend-neutral; real gateway, task-board, CI, or session-control integrations must be supplied by host adapters.
- Declarative JSON workflows do not execute generated code. Script harnesses run only through the subprocess VM and parent-owned capability boundary.
- Event durability is local-store oriented; multi-host production deployments should provide a shared store/notifier adapter.
