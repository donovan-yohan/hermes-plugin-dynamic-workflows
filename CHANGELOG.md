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

### Known limits

- Public alpha: useful for local Hermes/plugin experiments and adapter prototyping, not a hardened production sandbox for arbitrary untrusted users.
- The core runtime is dependency-free and backend-neutral; real gateway, task-board, CI, or session-control integrations must be supplied by host adapters.
- Declarative JSON workflows do not execute generated code. Script harnesses run only through the subprocess VM and parent-owned capability boundary.
- Event durability is local-store oriented; multi-host production deployments should provide a shared store/notifier adapter.
