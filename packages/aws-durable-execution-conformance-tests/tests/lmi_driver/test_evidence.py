# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
import copy

import pytest

from aws_durable_execution_conformance_tests.lmi import observations as evidence


def event(phase, time=1, **fields):
    return {
        "phase": phase,
        "time": time,
        "sequence": int(time * 10),
        "marker": "a",
        "request": "r1",
        "execution": "exec1",
        "environment": "env1",
        "process": "p1",
        "pid": 10,
        "deadline": 10,
        "gate": "gate",
        **fields,
    }


def alive(events):
    return events + [
        {**e, "phase": "ALIVE", "time": max(v["time"] for v in events) + 1} for e in events if e["phase"] == "BLOCKED"
    ]


def test_overlap_requires_distinct_processes_requests_and_executions():
    events = [
        event("BLOCKED"),
        event(
            "BLOCKED",
            2,
            marker="b",
            process="p2",
            pid=11,
            request="r2",
            execution="exec2",
        ),
    ]
    assert evidence.overlap(alive(events), {"a", "b"}, 2, runtime="python") == "env1"
    with pytest.raises(evidence.PlacementError):
        evidence.overlap(events, {"a", "b"}, 2, runtime="python")  # starts alone do not prove overlap
    for key in ("process", "pid", "request", "execution"):
        invalid = copy.deepcopy(events)
        invalid[1][key] = invalid[0][key]
        with pytest.raises(evidence.PlacementError):
            evidence.overlap(alive(invalid), {"a", "b"}, 2, runtime="python")


def test_different_environment_and_sequential_invocations_cannot_pass():
    a = event("BLOCKED")
    b = event("BLOCKED", 3, marker="b", process="p2", pid=11, request="r2", execution="exec2")
    for events in ([a, {**b, "environment": "env2"}], [a, event("IO_EXIT", 2), b]):
        with pytest.raises(evidence.PlacementError):
            evidence.overlap(alive(events), {"a", "b"}, 2, runtime="python")
    with pytest.raises(evidence.PlacementError):
        evidence.overlap(alive([a, b]), {"a", "b"}, 2, runtime="python", environment="original-env")


def sources(raw, **cloud_fields):
    from types import SimpleNamespace
    from typing import Any, cast

    from aws_durable_execution_conformance_tests.lmi.source_evidence import CaseSources

    from .test_shared_partial_evidence import context

    ctx = context()
    # Only the manifest commit and the fields each case needs are read here.
    cloud = SimpleNamespace(manifest={"commit": "commit"}, **cloud_fields)
    return ctx, CaseSources(
        ctx,
        cast("Any", cloud),
        tuple(raw),
        expected_outcome=lambda item, scenario: {"status": "SUCCEEDED"},
        worker_identity=lambda item: item["process"],
    )


def raw_record(phase, sequence, **fields):
    return {
        "run": "run",
        "commit": "commit",
        "caseId": "case",
        "request": "r1",
        "sequence": sequence,
        "phase": phase,
        "execution": "exec",
        "marker": "m",
        "environment": "env",
        "process": "p",
        "time": 1,
        **fields,
    }


def test_raw_control_failures_become_collection_errors():
    """Collection reads the ledger with control validation disabled.

    Only CONFORMANCE records become observations, so a control fault first seen
    during collection used to vanish and leave full watermarks behind.
    """
    _, case_sources = sources([raw_record("CONTROL_ERROR", 1, gate="fault", error="AccessDenied")])
    events, errors = case_sources.observations()
    assert events == []
    assert any("fault" in error and "AccessDenied" in error for error in errors)


def test_replacement_worker_correlation_is_preserved():
    """A replace-worker runtime reports the worker capacity it took over."""
    ctx, case_sources = sources(
        [raw_record("WRAPPER_ENTER", 1)],
        invocations=[{"marker": "m", "payload": {"scenario": "barrier"}}],
        request_roles={"r1": "victim"},
        raw_histories={},
    )
    ctx.record_attempt("r1", role="victim", requestId="r1")
    events = [
        {"kind": "worker_recovered", "requestId": "r1", "atMs": 10, "replacesWorkerId": "retired-worker"},
        {"kind": "wrapper_return", "requestId": "r1", "atMs": 20, "outcome": "SUCCEEDED"},
    ]
    attempts, _, errors = case_sources.attempts(events, pending=[])
    assert errors == []
    assert "outcome" not in attempts[0]
    assert attempts[0]["replacesWorkerId"] == "retired-worker"
    assert attempts[0]["workerId"] == "p"


def test_binding_selected_worker_identity_key_is_validated_like_process():
    """A ``workerIdentity: worker`` binding must not raise when a raw WRAPPER_ENTER
    event omits ``worker``; CaseSources validates it exactly like ``process``.
    """
    from types import SimpleNamespace
    from typing import Any, cast

    from aws_durable_execution_conformance_tests.lmi.runtime import FixtureBinding
    from aws_durable_execution_conformance_tests.lmi.source_evidence import CaseSources

    from .test_shared_partial_evidence import context

    binding = FixtureBinding(fixtures={}, failure_error_type="ValueError", worker_identity="worker")
    ctx = context()
    cloud = SimpleNamespace(
        manifest={"commit": "commit"},
        invocations=[{"marker": "m", "payload": {"scenario": "barrier"}}],
        request_roles={"r1": "victim"},
        raw_histories={},
    )
    case_sources = CaseSources(
        ctx,
        cast("Any", cloud),
        tuple([raw_record("WRAPPER_ENTER", 1)]),
        expected_outcome=lambda item, scenario: {"status": "SUCCEEDED"},
        worker_identity=lambda raw: raw[binding.worker_identity],
        worker_identity_key=binding.worker_identity,
    )
    ctx.record_attempt("r1", role="victim", requestId="r1")
    events = [{"kind": "wrapper_return", "requestId": "r1", "atMs": 20, "outcome": "SUCCEEDED"}]
    attempts, _, errors = case_sources.attempts(events)
    assert attempts == []
    assert any("Invalid WRAPPER_ENTER identity or time" in error for error in errors)


def retry_body(request, attempt, at_ms):
    return {
        "kind": "body",
        "requestId": request,
        "operation": "retry",
        "operationId": "retry-op",
        "attempt": attempt,
        "atMs": at_ms,
        "sequence": 1,
    }


def retry_exit(request, attempt, outcome, at_ms):
    return {
        "kind": "task_exit",
        "requestId": request,
        "operationId": "retry-op",
        "attempt": attempt,
        "outcome": outcome,
        "atMs": at_ms,
        "sequence": 2,
    }


def test_retry_attempts_follow_causal_time_not_collection_order():
    """Raw collection order follows the S3 request keys, not execution order.

    A batch first read together can list the later successful attempt before its
    earlier failure, which previously rejected a conforming SDK.
    """
    from aws_durable_execution_conformance_tests.lmi.source_evidence import replay_evidence

    events = [
        retry_body("aa-later-request", 2, 200),
        retry_exit("aa-later-request", 2, "SUCCEEDED", 210),
        retry_body("zz-earlier-request", 1, 100),
        retry_exit("zz-earlier-request", 1, "FAILED", 110),
    ]
    replay = replay_evidence(events, [], {"victim": "zz-earlier-request", "resume": "aa-later-request"})
    assert [item["outcome"] for item in replay["retry"]["attempts"]] == ["FAILED", "SUCCEEDED"]
    assert [item["requestId"] for item in replay["operationExecutions"]] == [
        "zz-earlier-request",
        "aa-later-request",
    ]


def test_service_recorded_request_without_a_ledger_stream_is_incomplete():
    # The original was rejected before running, so only its retry published a
    # lifecycle stream. The retry must not stand in for the original silently.
    ctx, case_sources = sources(
        [raw_record("WRAPPER_ENTER", 1, request="r2")],
        invocations=[{"marker": "m", "arn": "exec", "payload": {"scenario": "barrier"}}],
        request_roles={"r2": "victim"},
        raw_histories={
            "exec": [
                {
                    "EventType": "InvocationCompleted",
                    "InvocationCompletedDetails": {
                        "RequestId": "r1",
                        "StartTimestamp": 0.0001,
                        "EndTimestamp": 0.0002,
                        "Error": {"Payload": {"ErrorType": "TooManyRequestsException"}},
                    },
                },
                {
                    "EventType": "InvocationCompleted",
                    "InvocationCompletedDetails": {"RequestId": "r2", "StartTimestamp": 0.5, "EndTimestamp": 0.9},
                },
            ]
        },
    )
    ctx.record_attempt("r2", role="victim", requestId="r2")
    events = [{"kind": "wrapper_return", "requestId": "r2", "atMs": 900, "outcome": "SUCCEEDED"}]
    _, _, errors = case_sources.attempts(events)
    missing = [error for error in errors if "no lifecycle ledger stream" in error]
    assert len(missing) == 1 and "r1" in missing[0] and "TooManyRequestsException" in missing[0], errors


def test_crossed_hook_identity_is_retained_as_a_source_error():
    _, case_sources = sources(
        [
            raw_record(
                "CONFORMANCE",
                1,
                observation={
                    "kind": "checkpoint_call",
                    "callId": "checkpoint",
                    "requestId": "r2",
                    "ownerRequestId": "r2",
                    "executionArn": "elsewhere",
                },
            ),
        ]
    )
    events, errors = case_sources.observations()
    assert [event["kind"] for event in events] == ["observation_error"]
    assert events[0]["requestId"] == events[0]["ownerRequestId"] == "r1"
    assert errors == ["Hook observation disagrees with ledger ownership: requestId, ownerRequestId, executionArn"]


@pytest.mark.parametrize("sequence", [0, -1, 10**40])
def test_source_sequence_is_checked_without_allocating_the_missing_range(sequence):
    _, case_sources = sources([raw_record("WRAPPER_ENTER", sequence)])
    _, errors = case_sources.observations()
    assert errors == [
        "Raw evidence has no native request identity or source sequence"
        if sequence < 1
        else "Raw source publication is incomplete for r1"
    ]


@pytest.mark.parametrize("fields", [{}, {"observation": None}, {"observation": []}])
def test_conformance_row_without_observation_is_a_source_error(fields):
    _, case_sources = sources([raw_record("CONFORMANCE", 1, **fields)])
    events, errors = case_sources.observations()
    assert events == []
    assert errors == ["Raw CONFORMANCE record for r1 at sequence 1 lacks an observation object"]


@pytest.mark.parametrize(
    "bad",
    [
        {},
        {"kind": "wrapper_return"},
        {"kind": [], "atMs": 5},
        {"kind": "task_exit", "taskId": "task", "atMs": float("inf")},
    ],
)
def test_bad_observation_does_not_discard_other_evidence(bad):
    _, case_sources = sources(
        [
            raw_record("CONFORMANCE", 1.0, observation=bad),
            raw_record(
                "CONFORMANCE",
                2.0,
                observation={"kind": "task_start", "taskId": "task", "atMs": 2, "captureSequence": 2.0},
            ),
        ]
    )
    events, errors = case_sources.observations()
    assert [event["kind"] for event in events] == ["task_start"]
    assert type(events[0]["sourceSequence"]) is int
    assert type(events[0]["captureSequence"]) is int
    assert errors == ["Raw CONFORMANCE record for r1 at sequence 1 has invalid kind or atMs"]


@pytest.mark.parametrize("raw_effects", [True, False])
def test_replay_counts_same_named_bodies_by_operation_identity(raw_effects):
    from aws_durable_execution_conformance_tests.lmi.source_evidence import replay_evidence

    bodies = [
        {**retry_body("r1", 1, 95), "operation": "success", "operationId": "completed"},
        *(retry_body("r2", attempt, 100 + attempt) for attempt in (1, 2, 3)),
        *(
            {**retry_body("r2", 1, 106 + index), "operation": "item", "operationId": f"item-{index}"}
            for index in (1, 2)
        ),
        {**retry_body("r2", 1, 109), "operation": "left", "operationId": "parallel-left"},
    ]
    events = [
        {"kind": "invocation_start", "requestId": "r1", "executionArn": "exec"},
        {
            "kind": "invocation_start",
            "requestId": "r2",
            "operations": [
                {
                    "operationId": "completed",
                    "operationType": "STEP",
                    "status": "SUCCEEDED",
                    "name": "success",
                    "serializedResult": "null",
                },
                {
                    "operationId": "wait",
                    "operationType": "WAIT",
                    "status": "SUCCEEDED",
                    "name": "pause",
                },
            ],
        },
        *bodies,
    ]
    raw = (
        [
            raw_record(
                "BODY", sequence, request=body["requestId"], operation=body["operation"], attempt=body["attempt"]
            )
            for sequence, body in enumerate(bodies, 1)
        ]
        if raw_effects
        else []
    )
    histories = [
        {
            "executionArn": "exec",
            "operations": [
                {"id": "completed", "name": "success"},
                {"id": "retry-op", "name": "retry"},
                {"id": "item-1", "name": "item"},
                {"id": "item-2", "name": "item"},
                {"id": "parallel-left", "name": "left"},
            ],
        }
    ]
    errors: list[str] = []
    replay = replay_evidence(
        events,
        histories,
        {"victim": "r1", "resume": "r2"},
        raw=raw,
        expected_effects={"success": 1, "retry": 3, "item": 1, "left": 1},
        errors=errors,
    )
    assert errors == []
    assert replay["sideEffects"] == [
        {"operationId": "completed", "count": 1, "expectedCount": 1},
        {"operationId": "retry-op", "count": 3, "expectedCount": 3},
        {"operationId": "item-1", "count": 1, "expectedCount": 1},
        {"operationId": "item-2", "count": 1, "expectedCount": 1},
        {"operationId": "parallel-left", "count": 1, "expectedCount": 1},
        {"operationId": "wait", "count": 0, "expectedCount": 0},
    ]
    partial = replay_evidence(events, [], {"victim": "r1", "resume": "r2"}, raw=raw, partial=True)
    assert "sideEffects" not in partial
    assert "outcome" not in partial["retry"]["attempts"][0]


def test_replay_keeps_missing_expectations_and_conflicting_source_counts_incomplete():
    from aws_durable_execution_conformance_tests.lmi.source_evidence import replay_evidence

    events = [
        {"kind": "invocation_start", "requestId": "r1", "executionArn": "exec"},
        retry_body("r2", 1, 100),
        {**retry_body("r2", 1, 101), "operationId": "second-retry"},
    ]
    errors: list[str] = []
    evidence = replay_evidence(
        events,
        [
            {
                "executionArn": "exec",
                "operations": [{"id": identity, "name": "retry"} for identity in ("retry-op", "second-retry")],
            }
        ],
        {"victim": "r1", "resume": "r2"},
        raw=[raw_record("BODY", 1, request="r2", operation="retry", attempt=1)],
        expected_effects={"success": 1, "retry": 2},
        errors=errors,
    )
    assert errors == [
        "Raw replay side effects do not match fixture body observations by request, operation and attempt",
    ]
    assert evidence["missingExpectedEffects"] == ["retry", "success"]


def test_missing_effect_on_one_same_named_step_is_a_count_mismatch():
    from aws_durable_execution_conformance_tests.lmi.source_evidence import replay_evidence

    events = [
        {"kind": "invocation_start", "requestId": "r1", "executionArn": "exec"},
        {**retry_body("r1", 1.0, 100), "operation": "item", "operationId": "item-1"},
    ]
    histories = [
        {
            "executionArn": "exec",
            "operations": [{"id": "item-1", "name": "item"}, {"id": "item-2", "name": "item"}],
        }
    ]
    errors: list[str] = []
    evidence = replay_evidence(
        events,
        histories,
        {"victim": "r1"},
        raw=[raw_record("BODY", 1e0, operation="item", attempt=1e0)],
        expected_effects={"item": 1},
        errors=errors,
    )
    assert evidence["sideEffects"] == [
        {"operationId": "item-1", "count": 1, "expectedCount": 1},
        {"operationId": "item-2", "count": 0, "expectedCount": 1},
    ]
    assert evidence["operationExecutions"][0]["attemptId"] == "r1:1"
    assert not errors


def test_unsealed_gap_is_pending_during_collection():
    _, case_sources = sources([raw_record("WRAPPER_ENTER", 1), raw_record("BODY", 3)])
    pending: list[str] = []
    _, errors = case_sources.observations(pending=pending)
    assert errors == []
    assert pending == ["Raw source publication is incomplete for r1"]
