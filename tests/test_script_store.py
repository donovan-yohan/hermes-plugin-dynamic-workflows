"""Tests for the durable script run store + deterministic replay cache (issue #3).

Covered:

* **Durable journal** — a run persists ``run.json`` + ``journal.jsonl`` with a
  stable run id, stable ascending call ids, ``boot`` / ``call`` / ``done``
  events, and *no* raw inputs/outputs (metadata-only).
* **Replay hit** — a deterministic run's calls are served from the cache on
  replay without ever invoking the runner again.
* **Replay miss (rerun)** — a call that was non-replayable in the source run
  (non-deterministic runner) re-runs live on replay, per the documented policy.
* **Replay mismatch (fail closed)** — a replay whose call drifts from the
  recorded args aborts the run rather than returning a stale value.
* **Typed load failures** — missing run, corrupt cache, corrupt run.json, and a
  stale ``schema_version`` raise typed errors and never corrupt parent state.

All effects route through deterministic runners, so the suite is reproducible.

**Backend parametrization (issue #110).** Tests that only exercise the store
through its public :class:`~hermes_workflows.script_store.ScriptRunStoreProtocol`
surface (``begin``/``finish``/``load_run``/``load_cache``/``journal``/...) are
parametrized over ``STORE_BACKENDS`` via the ``backend`` fixture, so they run
against both the file backend and :class:`~hermes_workflows.script_store_sqlite.
SqliteScriptRunStore`. Tests that simulate corruption by reaching *past* the
store's API to tamper with its on-disk representation directly (writing bytes
into ``run.json``/``cache.jsonl``) are inherently file-backend-specific — a
SQLite database has no equivalent "edit one JSONL line" shape — and stay
unparametrized; :mod:`tests.test_script_store_sqlite` covers the same failure
classes (missing/corrupt/stale-schema) via SQLite-native tampering (raw SQL
against the store's own database) instead.
"""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from hermes_workflows import run_workflow_script
from hermes_workflows.errors import CorruptScriptRunError, ScriptRunNotFound
from hermes_workflows.script_store import (
    SCRIPT_SCHEMA_VERSION,
    ReplayCache,
    ReplayEntry,
    ScriptRunStore,
    canonical_hash,
    is_replayable,
    replay_args_hash,
    script_run_id,
)
from hermes_workflows.script_store_sqlite import SqliteScriptRunStore

# Backend registry for parametrized tests (issue #110): every test that takes
# the ``backend`` fixture is run once per entry here.
STORE_BACKENDS = {"file": ScriptRunStore, "sqlite": SqliteScriptRunStore}


@pytest.fixture(params=sorted(STORE_BACKENDS))
def backend(request):
    """The store class under test for this parametrized run."""
    return STORE_BACKENDS[request.param]

META = 'meta = {"name": "demo", "description": "d"}\n'
PHASE_META = (
    'meta = {"name": "demo", "description": "d", '
    '"phases": [{"title": "Plan", "detail": "choose work"}, {"title": "Build"}]}\n'
)
LEGACY_PHASE_META = 'meta = {"name": "demo", "description": "d", "phases": ["Plan", "Build"]}\n'

# A script exercising every replayable method (log, agent, phase, kanban_agent).
FULL_SCRIPT = META + (
    'log("start")\n'
    'g = await agent("hermes.greeter", {"subject": args["who"]}, schema={"greeting": "string"})\n'
    'phase("mid")\n'
    'k = await kanban_agent("relayplanner", {"goal": "plan"}, {"repo": "x"})\n'
    'return {"greeting": g["greeting"], "profile": k["profile"]}\n'
)


class _CountingRunner:
    """Deterministic echo runner that counts how many times it is invoked."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, agent_id, input):  # noqa: A002 — match AgentRunner signature.
        self.calls += 1
        if agent_id == "hermes.greeter":
            return {"greeting": f"hello, {input.get('subject')}", "_marker": "live"}
        if agent_id.startswith("kanban."):
            return {"task_id": "kb_live", "profile": agent_id.split(".", 1)[1],
                    "status": "succeeded", "result": {}, "_marker": "live"}
        return {"echo": dict(input), "_marker": "live"}


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #

def test_script_run_id_is_content_addressed_and_safe():
    rid = script_run_id(FULL_SCRIPT, {"who": "world"})
    assert rid.startswith("wfs_")
    # Same prefix for the same source (suffix is a fresh uuid each time).
    again = script_run_id(FULL_SCRIPT, {"who": "world"})
    assert rid.split("_")[1] == again.split("_")[1]
    assert rid != again


def test_replay_args_hash_ignores_label():
    a = replay_args_hash("agent", {"agent_id": "x", "input": {"a": 1}, "label": "one"})
    b = replay_args_hash("agent", {"agent_id": "x", "input": {"a": 1}, "label": "two"})
    c = replay_args_hash("agent", {"agent_id": "x", "input": {"a": 2}, "label": "one"})
    assert a == b  # label is cosmetic.
    assert a != c  # a real argument change is detected.


def test_is_replayable_policy():
    assert is_replayable("log", deterministic_runner=False)
    assert is_replayable("phase", deterministic_runner=False)
    assert is_replayable("agent", deterministic_runner=True)
    assert not is_replayable("agent", deterministic_runner=False)
    assert not is_replayable("workflow", deterministic_runner=True)


# --------------------------------------------------------------------------- #
# Durable journal
# --------------------------------------------------------------------------- #

def test_run_persists_metadata_only_journal_with_stable_ids():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        script = META + 'await agent("hermes.echo", {"secret": "do-not-persist"})\nreturn {}\n'
        res = run_workflow_script(script, store=store, run_id="run_a")

        assert res.ok, res.error
        assert res.run_id == "run_a"
        assert res.journal_path and Path(res.journal_path).exists()

        run_dir = Path(tmp) / "runs" / "run_a"
        assert (run_dir / "run.json").exists()
        assert (run_dir / "journal.jsonl").exists()

        # run.json is a bounded metadata snapshot.
        meta = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
        assert meta["schema_version"] == SCRIPT_SCHEMA_VERSION
        assert meta["status"] == "succeeded"
        assert meta["script_sha256"] and meta["args_hash"]

        # Journal vocabulary is boot / call / done with stable ascending ids.
        events = store.journal("run_a")
        assert events[0]["type"] == "boot"
        assert events[-1]["type"] == "done"
        calls = [e for e in events if e["type"] == "call"]
        assert [c["call_id"] for c in calls] == [1]
        assert calls[0]["method"] == "agent"
        assert calls[0]["agent_id"] == "hermes.echo"

        # Metadata only: the raw agent input never reaches the durable journal.
        assert "do-not-persist" not in (run_dir / "journal.jsonl").read_text(encoding="utf-8")


def test_load_run_returns_terminal_metadata(backend):
    with TemporaryDirectory() as tmp:
        store = backend(Path(tmp) / "runs")
        res = run_workflow_script(FULL_SCRIPT, args={"who": "world"}, store=store, run_id="run_b")
        assert res.ok, res.error

        loaded = store.load_run("run_b")
        assert loaded.status == "succeeded"
        assert loaded.value == {"greeting": "hello, world", "profile": "relayplanner"}
        assert loaded.deterministic_runner is True  # default stub auto-detected.
        assert loaded.meta == {"name": "demo", "description": "d"}


def test_script_run_snapshot_persists_declared_phase_metadata():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        script = PHASE_META + 'phase("Plan")\nreturn {"ok": True}\n'
        res = run_workflow_script(script, store=store, run_id="run_phases")
        assert res.ok, res.error

        loaded = store.load_run("run_phases")
        assert loaded.phases == [
            {"title": "Plan", "detail": "choose work"},
            {"title": "Build"},
        ]
        snapshot = json.loads((Path(tmp) / "runs" / "run_phases" / "run.json").read_text(encoding="utf-8"))
        assert snapshot["phases"] == loaded.phases
        phase_call = [e for e in store.journal("run_phases") if e.get("method") == "phase"][0]
        assert phase_call["phase_title"] == "Plan"


def test_script_run_snapshot_persists_legacy_string_phase_metadata(backend):
    with TemporaryDirectory() as tmp:
        store = backend(Path(tmp) / "runs")
        script = LEGACY_PHASE_META + 'phase("Plan")\nreturn {"ok": True}\n'
        res = run_workflow_script(script, store=store, run_id="run_legacy_phases")
        assert res.ok, res.error

        loaded = store.load_run("run_legacy_phases")
        assert loaded.meta is not None
        assert loaded.meta["phases"] == [{"title": "Plan"}, {"title": "Build"}]
        assert loaded.phases == [{"title": "Plan"}, {"title": "Build"}]


def test_validate_false_preserves_valid_phase_metadata_on_static_error(backend):
    with TemporaryDirectory() as tmp:
        store = backend(Path(tmp) / "runs")
        script = LEGACY_PHASE_META + "import os\n"
        res = run_workflow_script(script, store=store, run_id="run_invalid_with_meta", validate=False)
        assert not res.ok

        loaded = store.load_run("run_invalid_with_meta")
        assert loaded.status == "failed"
        assert loaded.phases == [{"title": "Plan"}, {"title": "Build"}]


def test_minted_run_id_is_used_when_omitted(backend):
    with TemporaryDirectory() as tmp:
        store = backend(Path(tmp) / "runs")
        res = run_workflow_script(META + 'return {"x": 1}\n', store=store)
        assert res.run_id and res.run_id.startswith("wfs_")
        assert store.load_run(res.run_id).status == "succeeded"


# --------------------------------------------------------------------------- #
# Replay: hit (no re-dispatch)
# --------------------------------------------------------------------------- #

def test_replay_serves_deterministic_calls_without_invoking_runner(backend):
    with TemporaryDirectory() as tmp:
        store = backend(Path(tmp) / "runs")
        # Record with the deterministic default stub: every call is cached.
        rec = run_workflow_script(FULL_SCRIPT, args={"who": "world"}, store=store, run_id="src")
        assert rec.ok, rec.error
        assert rec.replayed_calls == 0

        cache = store.load_cache("src")
        assert len(cache) == 4  # log, agent, phase, kanban_agent

        # Replay with a runner that, if ever called, would flip _marker to "live".
        spy = _CountingRunner()
        rep = run_workflow_script(
            FULL_SCRIPT, args={"who": "world"}, store=store, run_id="replay",
            replay_from="src", agent_runner=spy,
        )
        assert rep.ok, rep.error
        assert rep.value == rec.value  # identical result, served from cache.
        assert spy.calls == 0  # the runner was never invoked.
        assert rep.replayed_calls == 4

        # The replay run is itself journaled, with replayed=True call events.
        replay_calls = [e for e in store.journal("replay") if e["type"] == "call"]
        assert all(e.get("replayed") for e in replay_calls)
        assert store.load_run("replay").replay_of == "src"


# --------------------------------------------------------------------------- #
# Replay: miss -> rerun live (non-deterministic source runner)
# --------------------------------------------------------------------------- #

def test_replay_reruns_calls_that_were_not_cached(backend):
    with TemporaryDirectory() as tmp:
        store = backend(Path(tmp) / "runs")
        script = META + 'r = await agent("hermes.echo", {"i": 1})\nlog("done")\nreturn {"r": r}\n'

        # Record with an explicitly non-deterministic runner: agent output is NOT
        # cached (only log is). Honest: we do not pretend a live runner replays.
        record_runner = _CountingRunner()
        rec = run_workflow_script(
            script, store=store, run_id="src2",
            agent_runner=record_runner, deterministic_runner=False,
        )
        assert rec.ok, rec.error
        assert record_runner.calls == 1
        cache = store.load_cache("src2")
        assert len(cache) == 1  # only the log call was replayable.

        # Replay: the agent call misses the cache and re-runs against the live
        # runner; the log call is served from cache.
        replay_runner = _CountingRunner()
        rep = run_workflow_script(
            script, store=store, run_id="replay2",
            replay_from="src2", agent_runner=replay_runner, deterministic_runner=False,
        )
        assert rep.ok, rep.error
        assert replay_runner.calls == 1  # the non-cached agent call reran live.
        assert rep.replayed_calls == 1   # the log call was served from cache.


# --------------------------------------------------------------------------- #
# Replay: mismatch -> fail closed
# --------------------------------------------------------------------------- #

def test_replay_rejects_mismatched_script_or_args_before_launch():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(FULL_SCRIPT, args={"who": "world"}, store=store, run_id="src3")

        # A replay is bound to the exact (script, args) that produced the cache;
        # different args fail closed *before* any subprocess spawns, so a wrong
        # run's cached values can never be served to a different program.
        try:
            run_workflow_script(
                FULL_SCRIPT, args={"who": "mars"}, store=store, run_id="replay3",
                replay_from="src3",
            )
        except ValueError as exc:
            assert "does not match" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("expected ValueError for mismatched replay args")
        assert not (Path(tmp) / "runs" / "replay3").exists()  # no orphan run dir.


def test_replay_drift_in_run_fails_closed():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(FULL_SCRIPT, args={"who": "world"}, store=store, run_id="src3b")

        # Tamper a cached entry's integrity tag (simulating drift the local
        # per-call guard must catch even when the run identity matches).
        cache_path = Path(tmp) / "runs" / "src3b" / "cache.jsonl"
        lines = []
        for raw in cache_path.read_text(encoding="utf-8").splitlines():
            entry = json.loads(raw)
            if entry["method"] == "agent":
                entry["args_hash"] = "tampered"
            lines.append(json.dumps(entry))
        cache_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        rep = run_workflow_script(
            FULL_SCRIPT, args={"who": "world"}, store=store, run_id="replay3b",
            replay_from="src3b",
        )
        assert rep.ok is False
        assert "replay drift" in rep.error["message"]
        assert store.load_run("replay3b").status == "failed"

        # Parent state is intact: a fresh run still works.
        again = run_workflow_script(META + 'return {"after": True}\n', store=store, run_id="after3")
        assert again.ok and again.value == {"after": True}


def test_finish_tolerates_corrupt_run_json_without_escaping():
    # Regression (review finding): a corrupt/stale run.json must not make finish()
    # raise and leave the run stuck in 'running'. We corrupt run.json mid-flight
    # via a runner that rewrites it during the (single) agent call.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        run_path = Path(tmp) / "runs" / "src3c" / "run.json"

        class _CorruptingRunner:
            def __call__(self, agent_id, input):  # noqa: A002
                run_path.write_text("{ broken json", encoding="utf-8")
                return {"echo": dict(input)}

        res = run_workflow_script(
            META + 'await agent("hermes.echo", {"i": 1})\nreturn {"ok": 1}\n',
            store=store, run_id="src3c", agent_runner=_CorruptingRunner(),
            deterministic_runner=False,
        )
        # finish() did not raise; it rewrote a clean terminal record.
        assert res.ok and res.value == {"ok": 1}
        reloaded = store.load_run("src3c")
        assert reloaded.status == "succeeded"


# --------------------------------------------------------------------------- #
# Typed load failures (fail closed, no parent corruption)
# --------------------------------------------------------------------------- #

def test_missing_run_raises_typed_not_found(backend):
    with TemporaryDirectory() as tmp:
        store = backend(Path(tmp) / "runs")
        try:
            store.load_cache("nope")
        except ScriptRunNotFound:
            pass
        else:  # pragma: no cover - assertion clarity for the unittest bridge.
            raise AssertionError("expected ScriptRunNotFound")

        try:
            store.load_run("nope")
        except ScriptRunNotFound:
            return
        raise AssertionError("expected ScriptRunNotFound for load_run")


def test_corrupt_cache_raises_typed_and_parent_recovers():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(FULL_SCRIPT, args={"who": "world"}, store=store, run_id="src4")

        # Corrupt the replay cache with a non-JSON line.
        (Path(tmp) / "runs" / "src4" / "cache.jsonl").write_text("{not json\n", encoding="utf-8")

        caught = None
        try:
            store.load_cache("src4")
        except CorruptScriptRunError as exc:
            caught = exc
        assert caught is not None and caught.reason == "corrupt_cache"

        # Replaying a corrupt cache (with a matching script/args so the identity
        # guard passes) fails closed *before* any subprocess spawns.
        try:
            run_workflow_script(FULL_SCRIPT, args={"who": "world"}, store=store,
                                run_id="x4", replay_from="src4")
        except CorruptScriptRunError:
            pass
        else:  # pragma: no cover
            raise AssertionError("expected CorruptScriptRunError on replay of corrupt cache")

        # Parent intact: an unrelated fresh run still succeeds.
        ok = run_workflow_script(META + 'return {"ok": 1}\n', store=store, run_id="fresh4")
        assert ok.ok and ok.value == {"ok": 1}


def test_corrupt_run_json_raises_typed():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(META + 'return {}\n', store=store, run_id="src5")
        (Path(tmp) / "runs" / "src5" / "run.json").write_text("{ broken", encoding="utf-8")
        try:
            store.load_run("src5")
        except CorruptScriptRunError as exc:
            assert exc.reason == "corrupt_run"
            return
        raise AssertionError("expected CorruptScriptRunError for corrupt run.json")


def test_load_cache_rejects_bool_call_id():
    # Regression (review finding): isinstance(True, int) is True, so a forged
    # {"call_id": true} line must be rejected, not aliased onto call id 1.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(META + 'return {}\n', store=store, run_id="srcbool")
        (Path(tmp) / "runs" / "srcbool" / "cache.jsonl").write_text(
            '{"call_id": true, "method": "log", "args_hash": "x", "value": null}\n',
            encoding="utf-8",
        )
        try:
            store.load_cache("srcbool")
        except CorruptScriptRunError as exc:
            assert exc.reason == "corrupt_cache"
            return
        raise AssertionError("expected CorruptScriptRunError for bool call_id")


def test_done_journal_event_redacts_script_error_message():
    # Regression (review finding): a script-authored exception message can carry
    # sensitive text; it must not reach the metadata-only journal's done event,
    # though it remains on the operator-facing run.json.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        res = run_workflow_script(
            META + 'raise ValueError("super-secret-marker")\nreturn {}\n',
            store=store, run_id="srcerr",
        )
        assert res.ok is False

        journal_text = (Path(tmp) / "runs" / "srcerr" / "journal.jsonl").read_text(encoding="utf-8")
        assert "super-secret-marker" not in journal_text  # redacted from journal.
        done = [e for e in store.journal("srcerr") if e["type"] == "done"][0]
        assert "message" not in (done.get("error") or {})

        # The full error is retained on run.json (operator-facing result surface).
        assert "super-secret-marker" in store.load_run("srcerr").error["message"]


def test_stale_schema_version_raises_typed():
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(META + 'return {}\n', store=store, run_id="src6")
        path = Path(tmp) / "runs" / "src6" / "run.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        data["schema_version"] = SCRIPT_SCHEMA_VERSION + 99
        path.write_text(json.dumps(data), encoding="utf-8")
        try:
            store.load_run("src6")
        except CorruptScriptRunError as exc:
            assert exc.reason == "schema_version"
            return
        raise AssertionError("expected CorruptScriptRunError for stale schema_version")


# --------------------------------------------------------------------------- #
# Misuse / guards
# --------------------------------------------------------------------------- #

def test_replay_from_requires_a_store():
    try:
        run_workflow_script(META + 'return {}\n', replay_from="anything")
    except ValueError as exc:
        assert "store" in str(exc)
        return
    raise AssertionError("expected ValueError when replay_from has no store")


def test_duplicate_run_id_is_rejected(backend):
    with TemporaryDirectory() as tmp:
        store = backend(Path(tmp) / "runs")
        run_workflow_script(META + 'return {}\n', store=store, run_id="dup")
        try:
            run_workflow_script(META + 'return {}\n', store=store, run_id="dup")
        except ValueError as exc:
            assert "already exists" in str(exc)
            return
        raise AssertionError("expected ValueError on duplicate run_id")


def test_unsafe_run_id_is_rejected_without_writing(backend):
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        store = backend(tmp_path / "runs")
        try:
            run_workflow_script(META + 'return {}\n', store=store, run_id="../escape")
        except ValueError as exc:
            assert "unsafe run_id" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("expected ValueError for unsafe run_id")
        assert not (tmp_path / "escape").exists()


def test_canonical_hash_is_order_independent():
    assert canonical_hash({"a": 1, "b": 2}) == canonical_hash({"b": 2, "a": 1})
    assert canonical_hash({"a": 1}) != canonical_hash({"a": 2})


# --------------------------------------------------------------------------- #
# Review hardening (Gemini PR #18 comments)
# --------------------------------------------------------------------------- #

def test_replay_cache_get_rejects_bool_and_non_int_call_ids():
    # Regression (review finding): isinstance(True, int) is True, so ReplayCache.get
    # must reject bools explicitly — get(True) must not alias cached call id 1.
    entry = ReplayEntry(call_id=1, method="log", args_hash="x", value=None)
    cache = ReplayCache({1: entry, 0: ReplayEntry(0, "phase", "y", None)}, source_run_id="r")
    assert cache.get(1) is entry
    assert cache.get(True) is None   # would alias to 1 under a bare isinstance(int).
    assert cache.get(False) is None  # would alias to 0.
    assert cache.get("1") is None
    assert cache.get(1.0) is None
    assert cache.get(None) is None


def test_load_cache_rejects_non_string_method_or_args_hash():
    # Regression (review finding): method/args_hash must be real strings — coercing
    # a missing field with str() would yield the literal "None" and let a forged
    # entry slip past the per-call integrity tag instead of failing closed.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(META + 'return {}\n', store=store, run_id="srcstr")
        (Path(tmp) / "runs" / "srcstr" / "cache.jsonl").write_text(
            '{"call_id": 1, "method": null, "args_hash": "x", "value": null}\n',
            encoding="utf-8",
        )
        try:
            store.load_cache("srcstr")
        except CorruptScriptRunError as exc:
            assert exc.reason == "corrupt_cache"
            return
        raise AssertionError("expected CorruptScriptRunError for non-string method")


def test_load_run_translates_unreadable_run_json_to_typed_corrupt():
    # Regression (review finding): _load_meta_unlocked reads directly rather than
    # gating on path.exists() (a TOCTOU race with finish()/rmtree). A run.json that
    # is unreadable for a reason other than absence (here: it is a directory, so
    # read_text raises IsADirectoryError/OSError) surfaces as a typed corrupt_run,
    # never a bare OSError.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(META + 'return {}\n', store=store, run_id="srcdir")
        run_json = Path(tmp) / "runs" / "srcdir" / "run.json"
        run_json.unlink()
        run_json.mkdir()  # not a regular file -> OSError on read, not FileNotFound.
        try:
            store.load_run("srcdir")
        except CorruptScriptRunError as exc:
            assert exc.reason == "corrupt_run"
            return
        raise AssertionError("expected CorruptScriptRunError for unreadable run.json")


def test_fsync_dir_is_best_effort_when_fsync_unsupported():
    # Regression (review finding): some filesystems reject fsync on a directory fd;
    # _fsync_dir must swallow that OSError so the (already-landed) atomic snapshot
    # write does not fail.
    import hermes_workflows.script_store as ss

    with TemporaryDirectory() as tmp:
        real_fsync = ss.os.fsync

        def _raise(_fd):  # simulate fsync(dir_fd) rejection.
            raise OSError("fsync not supported on this fs")

        ss.os.fsync = _raise
        try:
            ss._fsync_dir(Path(tmp))  # must not raise.
        finally:
            ss.os.fsync = real_fsync


def test_limits_from_view_defaults_missing_and_passes_through_valid():
    # A replay rebuilds VMLimits from the recorded view. A genuinely-absent key
    # (forward/back-compat) or an explicit null defaults; valid values round-trip.
    from hermes_workflows.vm import VMLimits, _limits_from_view

    default = VMLimits()
    assert _limits_from_view({}) == default
    assert _limits_from_view({"max_rpc_calls": None}).max_rpc_calls == default.max_rpc_calls
    # token_budget None means "no budget" and is preserved.
    assert _limits_from_view({"token_budget": None}).token_budget is None

    good = _limits_from_view(
        {"max_rpc_calls": 5, "max_runtime_s": 2.5,
         "allow_nested_workflows": True, "token_budget": 10}
    )
    assert good.max_rpc_calls == 5
    assert good.max_runtime_s == 2.5
    assert good.allow_nested_workflows is True
    assert good.token_budget == 10


def test_req_token_budget_distinguishes_missing_from_null():
    # Gemini review regression: a missing token_budget must preserve the caller's
    # default; explicit null is the only "no budget" signal.
    from hermes_workflows.vm import _req_token_budget

    assert _req_token_budget({}, 123) == 123
    assert _req_token_budget({"token_budget": None}, 123) is None


def test_limits_from_view_fails_closed_on_corrupt_values():
    # Post-fix review: a *present-but-corrupt* limits value (or a non-dict view)
    # must NOT silently widen the recorded caps to the permissive global default;
    # it raises so the replay caller can fail closed before launch. inf/nan are
    # rejected so a forged max_runtime_s cannot disable the wall-clock watchdog.
    from hermes_workflows.vm import _CorruptLimitsView, _limits_from_view

    for bad_view in (None, [], "x", 5):
        try:
            _limits_from_view(bad_view)  # type: ignore[arg-type]
        except _CorruptLimitsView:
            pass
        else:
            raise AssertionError(f"expected _CorruptLimitsView for view {bad_view!r}")

    corrupt_cases = [
        {"max_rpc_calls": "not-a-number"},
        {"max_rpc_calls": True},             # bool is not a cap.
        {"max_agent_calls": "5"},            # string, not a JSON number.
        {"max_runtime_s": "inf"},            # string.
        {"max_runtime_s": float("inf")},     # non-finite would disable the watchdog.
        {"max_runtime_s": float("nan")},
        {"allow_nested_workflows": "false"},  # truthy string must not flip it on.
        {"token_budget": "oops"},
        {"token_budget": True},
        {"token_budget": 1.5},               # float, not an int budget.
    ]
    for view in corrupt_cases:
        try:
            _limits_from_view(view)
        except _CorruptLimitsView:
            continue
        raise AssertionError(f"expected _CorruptLimitsView for {view!r}")


def test_replay_with_corrupt_limits_view_fails_closed_before_launch():
    # Post-fix review: a corrupt persisted limits view must not let a replay run
    # under silently-widened global-default caps. The replay fails closed with a
    # typed CorruptScriptRunError *before* any subprocess spawns, leaving no run.
    with TemporaryDirectory() as tmp:
        store = ScriptRunStore(Path(tmp) / "runs")
        run_workflow_script(FULL_SCRIPT, args={"who": "world"}, store=store, run_id="srclim")

        # Tamper the recorded run's limits with a non-finite runtime cap.
        run_json = Path(tmp) / "runs" / "srclim" / "run.json"
        data = json.loads(run_json.read_text(encoding="utf-8"))
        data["limits"]["max_runtime_s"] = float("inf")
        run_json.write_text(json.dumps(data), encoding="utf-8")

        try:
            run_workflow_script(
                FULL_SCRIPT, args={"who": "world"}, store=store, run_id="replaylim",
                replay_from="srclim",
            )
        except CorruptScriptRunError as exc:
            assert exc.reason == "corrupt_run"
        else:  # pragma: no cover
            raise AssertionError("expected CorruptScriptRunError for corrupt limits view")
        assert not (Path(tmp) / "runs" / "replaylim").exists()  # no orphan run dir.
