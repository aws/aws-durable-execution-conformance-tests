# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Scheduling and fault isolation tests, without an AWS deployment."""

from typing import Any

import pytest


@pytest.mark.parametrize("dispatch_sequence", [9, 11])
def test_cleanup_dispatch_uses_semantic_sequence_at_closure(dispatch_sequence):
    from aws_durable_execution_conformance_tests.lmi.adjudication import _cleanup_behavior_errors

    original: dict[str, Any] = {
        "attempts": [{"requestId": "request"}],
        "events": [{"requestId": "request", "kind": "wrapper_return", "atMs": 40, "sequence": 10}],
    }
    snapshot = {
        "events": [{**original["events"][0], "sequence": 100}],
        "sdkRequests": [
            {
                "requestId": "request",
                "method": "CheckpointDurableExecution",
                "outcome": "DISPATCHED",
                "atMs": 40,
                "sequence": dispatch_sequence + 90,
            }
        ],
    }
    errors = _cleanup_behavior_errors(original, snapshot, standard=False)
    assert any("dispatched SDK work after admission closed" in error for error in errors) == (dispatch_sequence == 11)


@pytest.mark.parametrize("timeout_sequence", [9, 11])
def test_cleanup_deadline_uses_semantic_sequence_for_inflight_work(timeout_sequence):
    from aws_durable_execution_conformance_tests.lmi.adjudication import _cleanup_behavior_errors

    original: dict[str, Any] = {
        "requirementId": "lmi-lifecycle-3",
        "attempts": [{"requestId": "request", "startedAtMs": 10, "endedAtMs": 40}],
        "selectedRequests": {"victim": "request"},
        "profile": {"timeoutMs": 20},
        "events": [{"requestId": "request", "kind": "work_in_flight", "atMs": 30, "sequence": 10}],
    }
    snapshot = {
        "events": [
            {**original["events"][0], "sequence": 100},
            {
                "requestId": "request",
                "kind": "platform_timeout",
                "source": "platform",
                "atMs": 30,
                "sequence": timeout_sequence + 90,
            },
        ]
    }
    errors = _cleanup_behavior_errors(original, snapshot, standard=True)
    assert any("contradicts the frozen in-flight" in error for error in errors) == (timeout_sequence == 9)


def test_cleanup_body_event_cannot_replay_completed_user_code():
    from pathlib import Path
    from runpy import run_path

    from aws_durable_execution_conformance_tests.lmi.adjudication import _cleanup_behavior_errors

    original = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))["replay_evidence"]()
    resumed = next(row for row in original["attempts"] if row["role"] == "resume")
    operation_id = original["replay"]["before"][0]["id"]
    snapshot = {
        "events": [
            {
                "kind": "body",
                "requestId": resumed["requestId"],
                "executionArn": resumed["executionArn"],
                "operationId": operation_id,
                "atMs": resumed["startedAtMs"] + 1,
            }
        ]
    }
    errors = _cleanup_behavior_errors(original, snapshot, standard=True)
    assert f"ASSERTION: cleanup evidence reports repeated completed user code for {operation_id}" in errors


def test_retirement_completions_project_only_to_matching_frozen_starts():
    from aws_durable_execution_conformance_tests.lmi.adjudication import _project_retirement_completions

    retained = {"kind": "io_exit", "requestId": "request", "taskId": "present"}
    completions = [
        retained,
        {"kind": "io_exit", "requestId": "other", "taskId": "present"},
        {"kind": "io_exit", "requestId": "request", "taskId": "discovered-later"},
        {"kind": "task_exit", "requestId": "request", "taskId": "present"},
    ]
    frozen = [{"kind": "io_start", "requestId": "request", "taskId": "present"}]
    assert _project_retirement_completions(frozen, completions) == [retained]
    assert len(completions) == 4


def test_cleanup_reconciles_call_with_later_request():
    from aws_durable_execution_conformance_tests.lmi.adjudication import _cleanup_behavior_errors

    call = {"kind": "checkpoint_call", "requestId": "request", "callId": "call", "atMs": 20}
    request = {**call, "method": "CheckpointDurableExecution", "outcome": "DISPATCHED"}
    assert any(
        "lacks a unique matching SDK request" in error
        for error in _cleanup_behavior_errors({}, {"events": [call]}, standard=False)
    )
    assert not any(
        "lacks a unique matching SDK request" in error
        for error in _cleanup_behavior_errors(
            {}, {"sdkRequests": [request]}, known_snapshots=[{"events": [call]}], standard=False
        )
    )


def test_cleanup_dispatch_tie_without_a_shared_snapshot_cannot_establish_order():
    from aws_durable_execution_conformance_tests.lmi.adjudication import _cleanup_behavior_errors

    original = {
        "attempts": [{"requestId": "request"}],
        "events": [{"requestId": "request", "kind": "wrapper_return", "atMs": 40, "sequence": 10}],
    }
    snapshot = {
        "sdkRequests": [
            {
                "requestId": "request",
                "method": "CheckpointDurableExecution",
                "outcome": "DISPATCHED",
                "atMs": 40,
                "sequence": 11,
            }
        ]
    }
    errors = _cleanup_behavior_errors(original, snapshot, standard=False)
    assert any("same-time observations need sequences in one snapshot" in error for error in errors)
    assert not any("dispatched SDK work after admission closed" in error for error in errors)


def test_later_duplicate_sdk_request_is_rejected_despite_earlier_unique_match():
    from aws_durable_execution_conformance_tests.lmi.adjudication import _cleanup_behavior_errors

    call = {"kind": "checkpoint_call", "requestId": "request", "callId": "call", "atMs": 20, "outcome": "REJECTED"}
    request = {**call, "method": "CheckpointDurableExecution"}
    errors = _cleanup_behavior_errors(
        {"events": [call], "sdkRequests": [request]},
        {"sdkRequests": [request, request]},
        standard=False,
    )
    assert "ASSERTION: cleanup observed SDK call lacks a unique matching SDK request" in errors


def test_cleanup_uses_platform_end_to_reject_timeout_after_completion():
    from aws_durable_execution_conformance_tests.lmi.adjudication import _cleanup_behavior_errors

    before = {
        "requirementId": "lmi-lifecycle-3",
        "selectedRequests": {"victim": "request"},
        "profile": {"timeoutMs": 20},
        "attempts": [{"requestId": "request", "startedAtMs": 10, "endedAtMs": 40, "endedAtMsSource": "fixture"}],
        "events": [{"kind": "platform_timeout", "source": "platform", "requestId": "request", "atMs": 30}],
    }
    final = {"attempts": [{"requestId": "request", "endedAtMs": 29, "endedAtMsSource": "platform"}]}
    assert "ASSERTION: cleanup timeout falls outside the configured native deadline" in _cleanup_behavior_errors(
        before, final, standard=True
    )


def test_source_aware_native_interval_ignores_superseded_fixture_times():
    from aws_durable_execution_conformance_tests.lmi.rules.retention import _native_intervals

    first = {"attempts": [{"requestId": "request", "startedAtMs": 50, "endedAtMs": 90}]}
    later = {
        "attempts": [
            {
                "requestId": "request",
                "startedAtMs": 55,
                "startedAtMsSource": "platform",
                "endedAtMs": 80,
                "endedAtMsSource": "platform",
            }
        ]
    }
    assert _native_intervals([first, later]) == {"request": {"startedAtMs": (55, True), "endedAtMs": (80, True)}}


def test_partial_same_time_dispatch_does_not_override_complete_order():
    from aws_durable_execution_conformance_tests.lmi.adjudication import _cleanup_behavior_errors

    request = {"requestId": "request", "method": "CheckpointDurableExecution", "outcome": "DISPATCHED", "atMs": 40}
    closure = {"kind": "wrapper_return", "requestId": "request", "atMs": 40, "sequence": 10}
    original = {
        "attempts": [{"requestId": "request"}],
        "events": [closure],
        "sdkRequests": [{**request, "sequence": 9}],
    }
    partial = {"events": [closure], "sdkRequests": [request]}
    errors = _cleanup_behavior_errors(original, original, standard=False, known_snapshots=[partial])
    assert not any("dispatched SDK work after admission closed" in error or "same-time" in error for error in errors)
