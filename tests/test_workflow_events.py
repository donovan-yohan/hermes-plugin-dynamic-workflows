"""Generic workflow event broker tests (issue #7)."""

import multiprocessing
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import threading

from hermes_workflows.events import (
    FifoWorkflowEventNotifier,
    FileWorkflowEventStore,
    InMemoryWorkflowEventStore,
    ThreadWorkflowEventNotifier,
    WorkflowEvent,
    WorkflowEventBroker,
    WorkflowEventPredicate,
    match_workflow_event,
    publish_github_webhook_event,
    publish_workflow_event,
    workflow_event_from_github_webhook,
)


def _append_same_file_event(root: str) -> None:
    FileWorkflowEventStore(root).append_event(
        WorkflowEvent(event_id="evt_shared", source="github", event_type="github.check_run.completed", subject="github:o/r:check_run:1")
    )


def test_event_store_dedupes_delivery_ids_and_versions_are_monotonic():
    store = InMemoryWorkflowEventStore()
    event = WorkflowEvent(event_id="evt_1", source="github", event_type="github.pull_request.closed", subject="github:o/r:pull:7")

    first = store.append_event(event)
    duplicate = store.append_event(event)
    second = store.append_event(
        WorkflowEvent(event_id="evt_2", source="github", event_type="github.check_run.completed", subject="github:o/r:check_run:9")
    )

    assert first.version == 1
    assert duplicate.version == 1
    assert second.version == 2
    assert store.current_version() == 2
    assert len(store.find_events(WorkflowEventPredicate(source="github"))) == 2


def test_file_event_store_dedupes_across_processes_with_shared_lock():
    with TemporaryDirectory() as tmp:
        processes = [multiprocessing.Process(target=_append_same_file_event, args=(tmp,)) for _ in range(8)]
        for proc in processes:
            proc.start()
        for proc in processes:
            proc.join(timeout=5.0)

        assert all(proc.exitcode == 0 for proc in processes)
        store = FileWorkflowEventStore(Path(tmp))
        found = store.find_events(WorkflowEventPredicate(source="github"))

    assert len(found) == 1
    assert found[0].event_id == "evt_shared"
    assert found[0].version == 1


def test_wait_for_event_reads_persisted_event_after_restart():
    with TemporaryDirectory() as tmp:
        store = FileWorkflowEventStore(Path(tmp))
        notifier = ThreadWorkflowEventNotifier()
        stored = publish_workflow_event(
            store,
            notifier,
            WorkflowEvent(event_id="evt_restart", source="github", event_type="github.pull_request.closed", subject="github:o/r:pull:5"),
        )

        restarted = FileWorkflowEventStore(Path(tmp))
        broker = WorkflowEventBroker(restarted, ThreadWorkflowEventNotifier())
        found = broker.wait_for(
            WorkflowEventPredicate(source="github", event_type="github.pull_request.closed", subject="github:o/r:pull:5"),
            timeout=0.01,
        )

    assert found.event_id == "evt_restart"
    assert found.version == stored.version == 1


def test_fifo_notifier_wakes_waiter_across_store_instances():
    if not hasattr(os, "mkfifo"):
        return
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        broker = WorkflowEventBroker(FileWorkflowEventStore(root), FifoWorkflowEventNotifier(root / "events.notify"))
        result = {}

        def waiter():
            result["event"] = broker.wait_for(
                WorkflowEventPredicate(source="github", event_type="github.pull_request.closed", subject="github:o/r:pull:9"),
                timeout=2.0,
            )

        thread = threading.Thread(target=waiter)
        thread.start()
        publish_workflow_event(
            FileWorkflowEventStore(root),
            FifoWorkflowEventNotifier(root / "events.notify"),
            WorkflowEvent(event_id="evt_fifo", source="github", event_type="github.pull_request.closed", subject="github:o/r:pull:9"),
        )
        thread.join(timeout=3.0)

    assert not thread.is_alive()
    assert result["event"].event_id == "evt_fifo"


def test_wait_for_event_ignores_stale_after_version_and_wakes_on_new_event():
    store = InMemoryWorkflowEventStore()
    notifier = ThreadWorkflowEventNotifier()
    stale = publish_workflow_event(
        store,
        notifier,
        WorkflowEvent(event_id="evt_old", source="github", event_type="github.check_run.completed", subject="github:o/r:check_run:1"),
    )
    broker = WorkflowEventBroker(store, notifier)
    result = {}

    def waiter():
        result["event"] = broker.wait_for(
            WorkflowEventPredicate(
                source="github",
                event_type="github.check_run.completed",
                subject="github:o/r:check_run:1",
                after_version=stale.version,
            ),
            timeout=1.0,
        )

    thread = threading.Thread(target=waiter)
    thread.start()
    publish_workflow_event(
        store,
        notifier,
        WorkflowEvent(event_id="evt_new", source="github", event_type="github.check_run.completed", subject="github:o/r:check_run:1"),
    )
    thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert result["event"].event_id == "evt_new"


def test_predicate_payload_match_supports_nested_paths():
    event = WorkflowEvent(
        event_id="evt_payload",
        source="github",
        event_type="github.check_run.completed",
        subject="github:o/r:check_run:44",
        payload={"check_run": {"head_sha": "abc", "conclusion": "success"}},
        version=3,
    )
    assert match_workflow_event(
        event,
        WorkflowEventPredicate(
            source="github",
            payload_match={"check_run.head_sha": "abc", "check_run.conclusion": "success"},
            after_version=2,
        ),
    )
    assert not match_workflow_event(event, WorkflowEventPredicate(payload_match={"check_run.head_sha": "def"}))


def test_direct_event_and_predicate_constructors_validate_public_inputs():
    try:
        WorkflowEvent(event_id="bad id", source="github", event_type="github.check", subject="github:o/r")
    except ValueError:
        pass
    else:
        raise AssertionError("expected invalid event_id to fail")

    try:
        WorkflowEventPredicate(after_version=-1)
    except ValueError:
        pass
    else:
        raise AssertionError("expected invalid after_version to fail")


def test_github_webhook_rejects_malformed_event_headers():
    try:
        workflow_event_from_github_webhook(
            {"action": "closed", "repository": {"full_name": "octo/repo"}},
            headers={"X-GitHub-Event": "pull/request"},
        )
    except ValueError:
        pass
    else:
        raise AssertionError("expected unsafe GitHub event header to fail")


def test_github_pull_request_webhook_is_normalized_and_redacted():
    payload = {
        "action": "closed",
        "repository": {"full_name": "octo/repo"},
        "sender": {"login": "mona"},
        "pull_request": {
            "number": 12,
            "state": "closed",
            "merged": True,
            "html_url": "https://github.com/octo/repo/pull/12",
            "head": {"sha": "abc123"},
            "base": {"ref": "main"},
            "token": "ghp_should_not_persist",
        },
    }

    event = workflow_event_from_github_webhook(
        payload,
        headers={"X-GitHub-Event": "pull_request", "X-GitHub-Delivery": "delivery-1", "X-Hub-Signature-256": "sha256=secret"},
    )

    assert event.event_id == "delivery-1"
    assert event.source == "github"
    assert event.event_type == "github.pull_request.closed"
    assert event.subject == "github:octo/repo:pull:12"
    assert event.payload["pull_request"]["head_sha"] == "abc123"
    assert "ghp_should_not_persist" not in str(event.to_dict())
    assert "sha256=secret" not in str(event.to_dict())


def test_github_check_run_publish_dedupes_and_matches_head_sha():
    store = InMemoryWorkflowEventStore()
    notifier = ThreadWorkflowEventNotifier()
    payload = {
        "action": "completed",
        "repository": {"full_name": "octo/repo"},
        "check_run": {"id": 44, "name": "tests", "status": "completed", "conclusion": "success", "head_sha": "abc"},
    }

    first = publish_github_webhook_event(
        store,
        notifier,
        payload,
        headers={"X-GitHub-Event": "check_run", "X-GitHub-Delivery": "delivery-check"},
    )
    duplicate = publish_github_webhook_event(
        store,
        notifier,
        payload,
        headers={"X-GitHub-Event": "check_run", "X-GitHub-Delivery": "delivery-check"},
    )

    found = store.find_events(
        WorkflowEventPredicate(
            source="github",
            event_type="github.check_run.completed",
            subject="github:octo/repo:check_run:44",
            payload_match={"check_run.head_sha": "abc", "check_run.conclusion": "success"},
        )
    )

    assert duplicate.version == first.version == 1
    assert len(found) == 1
    assert found[0].event_id == "delivery-check"
