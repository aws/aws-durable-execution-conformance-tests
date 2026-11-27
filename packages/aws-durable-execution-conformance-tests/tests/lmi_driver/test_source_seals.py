# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Source coverage is a positive witness, separate from visible work pairing."""

from types import SimpleNamespace
from typing import Any, cast

import pytest

from aws_durable_execution_conformance_tests.lmi.evidence import (
    validate_common_evidence,
    validate_evidence,
    validate_retirement_evidence,
)
from aws_durable_execution_conformance_tests.lmi.observations import (
    UNDISPATCHED_ERROR_TYPES,
    is_undispatched_rejection,
)
from aws_durable_execution_conformance_tests.lmi.source_evidence import (
    _quiescent,
    ledger_seal_status,
    requests_after_cutoff,
    undispatched_requests,
)

from .support import build_evidence
from .test_shared_adapter import context

LAUNCHES = [{"arn": "execution", "marker": "marker", "native_requests": ["native"]}]


def rejection_payload(error_type="TooManyRequestsException", **overrides):
    """The real payload shape from a service capacity rejection, confirmed from
    retained InvocationCompleted records in aws/aws-durable-execution-sdk-python
    LMI runs 43973096696, 43978687067 and 44191933207."""
    payload = {
        "ErrorMessage": (
            "You currently do not have sufficient capacity for the function. If this error "
            "persists, check your function version's scaling config to allocate more execution "
            "environments for it."
        ),
        "ErrorType": error_type,
    }
    payload.update(overrides)
    return payload


def rejected_completion(request, event_id, start, end, *, payload=None, truncated=False):
    return {
        "EventType": "InvocationCompleted",
        "EventId": event_id,
        "InvocationCompletedDetails": {
            "RequestId": request,
            "StartTimestamp": start,
            "EndTimestamp": end,
            "Error": {"Payload": payload if payload is not None else rejection_payload(), "Truncated": truncated},
        },
    }


def raw_stream(*observations):
    scope = {
        "request": "native",
        "execution": "execution",
        "marker": "marker",
        "environment": "environment",
        "process": "process",
        "run": "run",
        "caseId": "case",
        "commit": "commit",
    }
    records = [
        {**scope, "phase": "WRAPPER_ENTER", "sequence": 1, "time": 0.1},
        *(
            {
                **scope,
                "phase": "CONFORMANCE",
                "sequence": capture + 1,
                "time": 2,
                "observation": {**event, "captureSequence": capture},
            }
            for capture, event in enumerate(observations, 1)
        ),
        {
            **scope,
            "phase": "SEALED",
            "sequence": len(observations) + 2,
            "time": 2,
            "sourceProtocol": 2,
            "throughCapture": len(observations),
        },
    ]
    return records


def cloud_source(tmp_path, *extra):
    ctx = context(scenario="normal-return", roles=("victim",))
    ctx.record_attempt("native", role="victim", requestId="native")
    records = raw_stream(
        {"kind": "wrapper_enter", "atMs": 100},
        {"kind": "task_start", "taskId": "root", "atMs": 110},
        {"kind": "task_exit", "taskId": "root", "atMs": 300},
        {"kind": "wrapper_return", "outcome": "SUCCEEDED", "atMs": 400},
        *extra,
    )
    cloud = SimpleNamespace(
        manifest={"bucket": "private", "commit": "commit", "invocationTimeout": 60},
        observation_ended_at=1,
        retired_at=3,
        request_roles={"native": "victim"},
        events={r["sequence"]: r for r in records},
        raw_histories={
            "execution": [
                {"EventId": 1, "EventType": "ExecutionStarted", "EventTimestamp": 0.05},
                {"EventId": 2, "EventType": "ExecutionSucceeded", "EventTimestamp": 0.45},
                {
                    "EventId": 3,
                    "EventType": "InvocationCompleted",
                    "InvocationCompletedDetails": {
                        "RequestId": "native",
                        "StartTimestamp": 0.1,
                        "EndTimestamp": 0.5,
                    },
                },
            ]
        },
        terminal_histories={"execution"},
        execution_results={"execution": {"Status": "SUCCEEDED", "Result": '"marker"', "EndTimestamp": 0.45}},
        invocations=[{"arn": "execution", "marker": "marker", "native_requests": ["native"]}],
        source_uris={"memory://ledger"},
        control_records=[],
        artifacts_dir=tmp_path,
        _save=lambda *args: None,
    )
    return ctx, cast("Any", cloud)


def test_service_rejected_before_dispatch_settles_as_undispatched_and_stays_incomplete(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_observed_retirement

    ctx, cloud = cloud_source(tmp_path)
    cloud.raw_histories["execution"].append(rejected_completion("rejected", 4, 0.6, 0.8))
    history_uri = (tmp_path / "histories/marker.json").resolve().as_uri()
    cloud.source_uris.add(history_uri)

    observation = build_evidence(ctx, cloud)
    assert observation["collectionErrors"] == []
    rejected_pending = [pending for pending in observation["collection"]["pending"] if "rejected" in pending]
    assert any("has no lifecycle ledger stream" in pending for pending in rejected_pending)
    assert any("Missing ledger seals" in pending for pending in rejected_pending)
    assert "lifecycle" not in observation["collection"]["watermarks"]
    assert "sdkRequests" not in observation["collection"]["watermarks"]
    assert validate_evidence(observation, phase="observations")

    final = build_evidence(ctx, cloud, phase="final")
    assert final["collectionErrors"] == []
    assert final["collection"]["watermarks"] == {"lifecycle": 1000, "sdkRequests": 1000, "history": 1000}
    settlement = next(item for item in final["retirement"]["resources"] if item["id"] == "rejected")
    assert settlement["outcome"] == "UNDISPATCHED"
    assert settlement["executionArn"] == "execution"
    assert settlement["serviceErrorType"] == "TooManyRequestsException"
    assert settlement["sourceEventId"] == 4
    assert settlement["startedAtMs"] == 600
    assert settlement["settledAtMs"] == 800
    assert settlement["evidenceUri"] == history_uri
    assert validate_retirement_evidence(final) == []
    assert validate_observed_retirement(final) == []
    errors = validate_evidence(final)
    assert any(
        "service rejected native request rejected before its handler ran" in error
        and "TooManyRequestsException" in error
        for error in errors
    )


@pytest.mark.parametrize("call_kind", ["checkpoint_call", "poll_call"])
def test_rejected_sdk_call_without_an_exit_settles_quiescent(tmp_path, call_kind):
    """A rejected `checkpoint_call`/`poll_call` with no exit may still settle as quiescent.

    A matching REJECTED SDK request corroborates it: the SDK never obtained a
    transport response to exit on, the same corroboration evidence-schema.md
    documents for a rejected start.
    """
    ctx, cloud = cloud_source(
        tmp_path,
        {"kind": "admission_probe", "probe": "operation", "outcome": "REJECTED", "atMs": 410},
        {"kind": call_kind, "callId": "closed", "outcome": "REJECTED", "atMs": 411},
    )
    frozen = build_evidence(ctx, cloud)
    assert frozen["collectionErrors"] == []
    assert not any(
        "has not established a complete observation window" in pending
        for pending in frozen["collection"].get("pending", [])
    )
    assert frozen["collection"]["watermarks"] == {"lifecycle": 1000, "sdkRequests": 1000, "history": 1000}
    rejected_requests = [row for row in frozen["sdkRequests"] if row.get("outcome") == "REJECTED"]
    assert len(rejected_requests) == 1
    assert rejected_requests[0]["callId"] == "closed"

    final = build_evidence(ctx, cloud, phase="final")
    assert final["collectionErrors"] == []
    settlement = next(item for item in final["retirement"]["resources"] if item["id"] == "native")
    assert settlement["outcome"] == "QUIESCENT"
    assert validate_retirement_evidence(final) == []


@pytest.mark.parametrize("call_kind", ["checkpoint_call", "poll_call"])
def test_dispatched_call_without_an_exit_stays_pending_and_unknown(tmp_path, call_kind):
    """Unlike a corroborated rejection, a dispatched call with no exit never settles."""
    ctx, cloud = cloud_source(
        tmp_path,
        {"kind": call_kind, "callId": "open", "outcome": "DISPATCHED", "atMs": 411},
    )
    frozen = build_evidence(ctx, cloud)
    assert any(
        "has not established a complete observation window" in pending
        for pending in frozen["collection"].get("pending", [])
    )

    final = build_evidence(ctx, cloud, phase="final")
    settlement = next(item for item in final["retirement"]["resources"] if item["id"] == "native")
    assert settlement["outcome"] == "UNKNOWN"


def test_invalid_sdk_call_outcome_is_a_collection_error_that_withholds_watermarks(tmp_path):
    ctx, cloud = cloud_source(
        tmp_path,
        {"kind": "poll_call", "callId": "odd", "outcome": "TIMEOUT", "atMs": 411},
        {"kind": "poll_exit", "callId": "odd", "atMs": 412},
    )
    frozen = build_evidence(ctx, cloud)
    assert "SDK request odd has an invalid outcome" in frozen["collectionErrors"]
    assert frozen["collection"]["watermarks"] == {}


def _in_progress_events():
    return [
        {"requestId": "native", "kind": "wrapper_enter", "atMs": 100},
        {"requestId": "native", "kind": "task_start", "taskId": "root", "atMs": 110},
        {"requestId": "native", "kind": "task_exit", "taskId": "root", "atMs": 300},
        {"requestId": "native", "kind": "wrapper_return", "atMs": 400},
    ]


def test_quiescent_rejects_an_unmatched_rejected_call_without_sdk_requests():
    events = [
        *_in_progress_events(),
        {"requestId": "native", "kind": "checkpoint_call", "callId": "closed", "outcome": "REJECTED", "atMs": 411},
    ]
    assert _quiescent("native", events, sdk_requests=[]) is False


def test_quiescent_rejects_two_rejected_starts_sharing_a_call_id():
    events = [
        *_in_progress_events(),
        {"requestId": "native", "kind": "checkpoint_call", "callId": "closed", "outcome": "REJECTED", "atMs": 411},
        {"requestId": "native", "kind": "checkpoint_call", "callId": "closed", "outcome": "REJECTED", "atMs": 412},
    ]
    sdk_requests = [
        {
            "requestId": "native",
            "callId": "closed",
            "method": "CheckpointDurableExecution",
            "outcome": "REJECTED",
            "atMs": 411,
        }
    ]
    assert _quiescent("native", events, sdk_requests=sdk_requests) is False


def _unbound_wrapper_enter(sequence, request, *, marker="marker", execution="execution"):
    return {
        "phase": "WRAPPER_ENTER",
        "sequence": sequence,
        "time": 0.1,
        "request": request,
        "execution": execution,
        "marker": marker,
        "environment": "environment",
        "process": "process",
        "run": "run",
        "caseId": "case",
        "commit": "commit",
    }


def test_unbound_request_is_pending_not_an_error_while_binding_is_deferred(tmp_path):
    # A budget-expiry refresh intentionally skips binding from a partial read
    # (see Cloud.refresh): the gap is incomplete evidence, not proof of a
    # real collection failure, until a later complete refresh binds it.
    ctx, cloud = cloud_source(tmp_path)
    cloud.events[100] = _unbound_wrapper_enter(100, "orphan")
    cloud.binding_deferred = True

    observation = build_evidence(ctx, cloud)
    message = "Native request orphan was not bound to an original launch"
    assert message not in observation["collectionErrors"]
    assert message in observation["collection"]["pending"]


def test_unbound_request_is_a_collection_error_once_binding_is_not_deferred(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    cloud.events[100] = _unbound_wrapper_enter(100, "orphan")
    cloud.binding_deferred = False

    observation = build_evidence(ctx, cloud)
    message = "Native request orphan was not bound to an original launch"
    assert message in observation["collectionErrors"]
    assert message not in observation["collection"].get("pending", [])


def test_unbound_request_is_still_a_final_phase_error_even_while_deferred(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    cloud.events[100] = _unbound_wrapper_enter(100, "orphan")
    cloud.binding_deferred = True

    final = build_evidence(ctx, cloud, phase="final")
    message = "Native request orphan was not bound to an original launch"
    assert message in final["collectionErrors"]


def test_unbound_request_under_an_unowned_marker_is_always_an_error_even_while_deferred(tmp_path):
    # An unknown marker (no launch owns it at all, e.g. one `Cloud.start`
    # removed after a service rejection) is never "not yet bound" - that
    # sub-case requires an owned launch. It must stay an error regardless
    # of `binding_deferred`.
    ctx, cloud = cloud_source(tmp_path)
    cloud.events[100] = _unbound_wrapper_enter(100, "orphan", marker="unowned-marker")
    cloud.binding_deferred = True

    observation = build_evidence(ctx, cloud)
    message = "Native request orphan was not bound to an original launch"
    assert message in observation["collectionErrors"]


def test_unbound_request_under_a_launch_already_bound_elsewhere_is_always_an_error_even_while_deferred(tmp_path):
    # The owned launch's marker exists, but its launch is already bound to a
    # *different* execution than this stream's. That is a real ownership
    # mismatch, not a launch merely awaiting its first binding, so it must
    # stay an error regardless of `binding_deferred`.
    ctx, cloud = cloud_source(tmp_path)
    cloud.events[100] = _unbound_wrapper_enter(100, "orphan", execution="different-execution")
    cloud.binding_deferred = True

    observation = build_evidence(ctx, cloud)
    message = "Native request orphan was not bound to an original launch"
    assert message in observation["collectionErrors"]


def _owned_sealed_stream(request, *, marker="marker", execution="execution"):
    """A fully sealed, self-consistent two-record stream (no captures)."""
    scope = {
        "request": request,
        "execution": execution,
        "marker": marker,
        "environment": "environment",
        "process": "process",
        "run": "run",
        "caseId": "case",
        "commit": "commit",
    }
    return [
        {**scope, "phase": "WRAPPER_ENTER", "sequence": 1, "time": 0.1},
        {**scope, "phase": "SEALED", "sequence": 2, "time": 0.2, "sourceProtocol": 2, "throughCapture": 0},
    ]


def test_sealed_unbound_request_under_an_owned_launch_is_pending_while_binding_is_deferred(tmp_path):
    # A refresh that read a newly discovered request's whole stream (its
    # launch's WRAPPER_ENTER and SEALED) under an owned launch's marker
    # before the budget expired has not had a chance to bind it yet; that
    # gap is incomplete evidence, not a real ownership inconsistency.
    ctx, cloud = cloud_source(tmp_path)
    for sequence, record in enumerate(_owned_sealed_stream("orphan2"), start=200):
        cloud.events[sequence] = record
    cloud.binding_deferred = True

    observation = build_evidence(ctx, cloud)
    message = "Native request orphan2 has inconsistent ledger, launch or service ownership"
    assert message not in observation["collectionErrors"]
    assert any("orphan2" in item for item in observation["collection"]["pending"])


def test_sealed_unbound_request_is_an_ownership_error_once_binding_is_not_deferred(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    for sequence, record in enumerate(_owned_sealed_stream("orphan2"), start=200):
        cloud.events[sequence] = record
    cloud.binding_deferred = False

    observation = build_evidence(ctx, cloud)
    message = "Native request orphan2 has inconsistent ledger, launch or service ownership"
    assert message in observation["collectionErrors"]


def test_sealed_unbound_request_is_still_a_final_phase_error_even_while_deferred(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    for sequence, record in enumerate(_owned_sealed_stream("orphan2"), start=200):
        cloud.events[sequence] = record
    cloud.binding_deferred = True

    final = build_evidence(ctx, cloud, phase="final")
    message = "Native request orphan2 has inconsistent ledger, launch or service ownership"
    assert message in final["collectionErrors"]


def test_sealed_request_under_an_unowned_marker_stays_an_ownership_error_even_while_deferred(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    for sequence, record in enumerate(_owned_sealed_stream("orphan3", marker="unowned-marker"), start=200):
        cloud.events[sequence] = record
    cloud.binding_deferred = True

    observation = build_evidence(ctx, cloud)
    message = "Native request orphan3 has inconsistent ledger, launch or service ownership"
    assert message in observation["collectionErrors"]


def test_sealed_request_under_a_launch_already_bound_elsewhere_stays_an_ownership_error_even_while_deferred(tmp_path):
    # Ledger-side counterpart: the launch is owned (marker matches) but
    # already bound to a different execution, so this is a genuine mismatch,
    # never a "not yet bound" gap, regardless of the deferral.
    ctx, cloud = cloud_source(tmp_path)
    for sequence, record in enumerate(_owned_sealed_stream("orphan2", execution="different-execution"), start=200):
        cloud.events[sequence] = record
    cloud.binding_deferred = True

    observation = build_evidence(ctx, cloud)
    message = "Native request orphan2 has inconsistent ledger, launch or service ownership"
    assert message in observation["collectionErrors"]


def test_sealed_request_under_a_conflicting_launch_stays_an_ownership_error_even_while_deferred(tmp_path):
    # A launch `_bind_requests` has already flagged as conflicting can never
    # become bound, so it is never merely "not yet bound" either.
    ctx, cloud = cloud_source(tmp_path)
    cloud.invocations[0]["launch_conflict"] = True
    for sequence, record in enumerate(_owned_sealed_stream("orphan2"), start=200):
        cloud.events[sequence] = record
    cloud.binding_deferred = True

    observation = build_evidence(ctx, cloud)
    message = "Native request orphan2 has inconsistent ledger, launch or service ownership"
    assert message in observation["collectionErrors"]


def test_allowlist_is_pinned_exactly():
    # Any addition must be reviewed; a wrong string only matches nothing and
    # leaves the target fenced, so widening this set is never a safe fix.
    assert frozenset({"TooManyRequestsException"}) == UNDISPATCHED_ERROR_TYPES


@pytest.mark.parametrize(
    ("mutate", "valid"),
    [
        (lambda error, payload: None, True),
        (lambda error, payload: payload.update(ErrorType="Sandbox.Timedout"), False),
        (lambda error, payload: payload.update(ErrorType="Runtime.ExitError"), False),
        (lambda error, payload: payload.update(ErrorType="ClientError"), False),
        (lambda error, payload: payload.update(ErrorType="toomanyrequestsexception"), False),
        (lambda error, payload: payload.update(ErrorType="Lambda.TooManyRequestsException"), False),
        (lambda error, payload: payload.update(ErrorType=["TooManyRequestsException"]), False),
        (
            lambda error, payload: (payload.pop("ErrorType"), payload.update(ErrorMessage="TooManyRequestsException")),
            False,
        ),
        (lambda error, payload: payload.update(ErrorData="TooManyRequestsException"), False),
        (lambda error, payload: payload.update(StackTrace=["line 1"]), False),
        (lambda error, payload: payload.update(ErrorData={"detail": "capacity"}), False),
        (lambda error, payload: error.update(Payload=[]), False),
        (lambda error, payload: error.update(Payload="TooManyRequestsException"), False),
        (lambda error, payload: error.update(Truncated=True), False),
        (lambda error, payload: error.update(Truncated="false"), False),
        (lambda error, payload: error.update(Truncated=0), False),
        (lambda error, payload: error.update(Truncated=None), False),
        (lambda error, payload: payload.update(ErrorMessage=123), False),
        (lambda error, payload: payload.pop("ErrorMessage"), False),
    ],
)
def test_undispatched_rejection_discriminator_matches_only_the_confirmed_allowlisted_payload_shape(mutate, valid):
    details = rejected_completion("rejected", 4, 0.6, 0.8)["InvocationCompletedDetails"]
    mutate(details["Error"], details["Error"]["Payload"])
    assert is_undispatched_rejection(details) is valid


@pytest.mark.parametrize(
    "details",
    [None, {}, {"Error": None}, {"Error": "rejected"}, {"Error": []}],
)
def test_undispatched_rejection_discriminator_rejects_missing_or_malformed_error(details):
    assert is_undispatched_rejection(details) is False


HISTORIES = {"execution": [rejected_completion("rejected", 4, 0.6, 0.8)]}
LAUNCH = {"arn": "execution", "marker": "marker", "native_requests": []}


def test_undispatched_requests_proves_a_request_with_no_runtime_trace_at_all():
    proven = undispatched_requests((), HISTORIES, [LAUNCH])
    assert proven == {
        "rejected": {
            "executionArn": "execution",
            "serviceErrorType": "TooManyRequestsException",
            "sourceEventId": 4,
            "startedAtMs": 600,
            "settledAtMs": 800,
        }
    }


@pytest.mark.parametrize(
    "corrupt",
    [
        lambda event: event.update(EventType="StepSucceeded"),
        lambda event: event["InvocationCompletedDetails"].update(StartTimestamp=None),
        lambda event: event["InvocationCompletedDetails"].update(EndTimestamp=None),
        lambda event: event["InvocationCompletedDetails"].update(EndTimestamp=0.5, StartTimestamp=0.6),
        lambda event: event.update(EventId=None),
        lambda event: event.update(EventId="4"),
        lambda event: event.update(EventId=0),
        lambda event: event.update(EventId=-1),
    ],
)
def test_undispatched_requests_requires_a_sound_event_and_interval(corrupt):
    event = rejected_completion("rejected", 4, 0.6, 0.8)
    corrupt(event)
    assert undispatched_requests((), {"execution": [event]}, [LAUNCH]) == {}


def test_undispatched_requests_rejects_any_ledger_row_for_the_request():
    for row in (
        {"request": "rejected", "phase": "WRAPPER_ENTER"},
        {"request": "rejected", "phase": "CONFORMANCE", "observation": {"kind": "body"}},
        {"request": "rejected", "phase": "CONTROL_ERROR"},
        {"request": "rejected", "phase": "WRAPPER_RETURN"},
    ):
        assert undispatched_requests((row,), HISTORIES, [LAUNCH]) == {}


def test_undispatched_requests_rejects_a_bound_request():
    bound_launch = {**LAUNCH, "native_requests": ["rejected"]}
    assert undispatched_requests((), HISTORIES, [bound_launch]) == {}


def test_undispatched_requests_rejects_duplicate_or_foreign_completions():
    duplicate_in_one_history = {"execution": [*HISTORIES["execution"], rejected_completion("rejected", 5, 0.6, 0.8)]}
    assert undispatched_requests((), duplicate_in_one_history, [LAUNCH]) == {}

    duplicate_across_histories = {**HISTORIES, "other-execution": [rejected_completion("rejected", 4, 0.6, 0.8)]}
    other_launch = {"arn": "other-execution", "marker": "other-marker", "native_requests": []}
    assert undispatched_requests((), duplicate_across_histories, [LAUNCH, other_launch]) == {}

    assert undispatched_requests((), HISTORIES, []) == {}
    foreign_launch = {"arn": "another-execution", "marker": "marker", "native_requests": []}
    assert undispatched_requests((), HISTORIES, [foreign_launch]) == {}


def test_seal_is_required_even_when_service_reports_native_completion(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    cloud.events.pop(max(cloud.events))
    evidence = build_evidence(ctx, cloud)
    assert evidence["collection"]["watermarks"] == {"history": 1000}
    assert evidence["collectionErrors"] == []
    assert any("Missing ledger seals" in pending for pending in evidence["collection"]["pending"])

    final = build_evidence(ctx, cloud, phase="final")
    assert final["collection"]["watermarks"] == {}
    assert any("Missing ledger seals" in error for error in final["collectionErrors"])
    assert validate_retirement_evidence(final)


@pytest.mark.parametrize(
    "problem",
    [
        "wrong-version",
        "trailing-record",
        "duplicate-seal",
        "capture-gap",
        "publication-gap",
        "wrong-arn",
        "wrong-marker",
    ],
)
def test_malformed_seals_fail_closed(problem):
    records = raw_stream({"kind": "task_exit", "atMs": 500})
    seal = records[-1]
    if problem == "wrong-version":
        seal["sourceProtocol"] = 1
    elif problem == "trailing-record":
        records.append({**seal, "phase": "RAW", "sequence": seal["sequence"] + 1})
    elif problem == "duplicate-seal":
        records.append({**seal, "sequence": seal["sequence"] + 1})
    elif problem == "capture-gap":
        seal["throughCapture"] = 2
    elif problem == "wrong-arn":
        seal["execution"] = "wrong"
    elif problem == "wrong-marker":
        seal["marker"] = "wrong"
    else:
        seal["sequence"] += 1
    _, missing, errors = ledger_seal_status(records, {}, LAUNCHES, set())
    assert not missing and errors


def test_service_discovered_retry_needs_its_own_seal():
    records = raw_stream({"kind": "task_exit", "atMs": 500})
    service = {
        "execution": [
            {"InvocationCompletedDetails": {"RequestId": "native"}},
            {"InvocationCompletedDetails": {"RequestId": "unpublished-retry"}},
        ]
    }
    assert ledger_seal_status(records, service, LAUNCHES, set()) == (
        {"native", "unpublished-retry"},
        {"unpublished-retry"},
        [],
    )


@pytest.mark.parametrize("service_state", ["running", "stale-terminal", "history-gap", "no-terminal-event"])
def test_matching_ledger_seals_do_not_certify_unproven_service_history(tmp_path, service_state):
    ctx, cloud = cloud_source(tmp_path)
    if service_state == "running":
        cloud.execution_results["execution"]["Status"] = "RUNNING"
    elif service_state == "stale-terminal":
        cloud.terminal_histories.clear()
    elif service_state == "history-gap":
        cloud.raw_histories["execution"][-1]["EventId"] = 10
    else:
        cloud.raw_histories["execution"][1]["EventType"] = "StepSucceeded"
    evidence = build_evidence(ctx, cloud)
    assert evidence["collection"]["watermarks"] == {"lifecycle": 1000, "sdkRequests": 1000}
    assert evidence["collectionErrors"] == []
    assert any("Terminal service history" in pending for pending in evidence["collection"]["pending"])


def test_late_publication_and_late_activity_use_occurrence_time(tmp_path):
    ctx, cloud = cloud_source(
        tmp_path,
        {"kind": "checkpoint_call", "callId": "late", "atMs": 1100},
        {"kind": "checkpoint_exit", "callId": "late", "atMs": 1200},
    )
    before = build_evidence(ctx, cloud)
    assert before["collectionErrors"] == []
    assert before["collection"]["watermarks"] == {"lifecycle": 1000, "sdkRequests": 1000, "history": 1000}
    assert before["sdkRequests"] == []
    assert {event["kind"] for event in before["events"]} == {
        "wrapper_enter",
        "wrapper_return",
        "task_start",
        "task_exit",
    }
    assert len(cloud.events) == 8

    cloud.observation_ended_at = 2
    final = build_evidence(ctx, cloud, phase="final")
    assert final["collectionErrors"] == []
    assert [row["callId"] for row in final["sdkRequests"]] == ["late"]


def _reorder_capture(cloud, kinds):
    """Reassign same-timestamp ledger captures to ``kinds`` order.

    Keeps the existing capture-sequence *values* of these events (just a
    different permutation among themselves), so the ledger seal's capture set
    stays exactly ``1..high`` and ``ledger_seal_status`` still certifies it.
    """
    observations = {}
    for record in cloud.events.values():
        observation = record.get("observation")
        if isinstance(observation, dict) and observation.get("kind") in kinds:
            observations[observation["kind"]] = observation
    values = sorted(observations[kind]["captureSequence"] for kind in kinds)
    for kind, value in zip(kinds, values, strict=True):
        observations[kind]["captureSequence"] = value


def test_tied_dispatch_before_wrapper_return_keeps_its_sequence_and_is_not_late(tmp_path):
    ctx, cloud = cloud_source(
        tmp_path,
        {"kind": "checkpoint_call", "callId": "tied", "atMs": 400},
        {"kind": "checkpoint_exit", "callId": "tied", "atMs": 400},
    )
    _reorder_capture(cloud, ["checkpoint_call", "wrapper_return"])
    evidence = build_evidence(ctx, cloud)
    assert evidence["collectionErrors"] == []
    (request,) = evidence["sdkRequests"]
    wrapper_return = next(event for event in evidence["events"] if event["kind"] == "wrapper_return")
    # The fix: the normalizer must keep the per-request `sequence` `_causal_order`
    # assigned on the sdkRequest row, not strip it. Without it, `no_late_dispatch`
    # falls back to an inclusive same-millisecond comparison and misreports this
    # strictly-earlier dispatch as late.
    assert request["atMs"] == wrapper_return["atMs"] == 400
    assert request["sequence"] < wrapper_return["sequence"]
    errors = validate_common_evidence(evidence, phase="observations")
    assert not any("dispatched SDK work after admission closed" in error for error in errors), errors


def test_tied_dispatch_before_admission_closed_keeps_its_sequence_and_is_not_late(tmp_path):
    ctx, cloud = cloud_source(
        tmp_path,
        {"kind": "admission_closed", "atMs": 400},
        {"kind": "checkpoint_call", "callId": "tied", "atMs": 400},
        {"kind": "checkpoint_exit", "callId": "tied", "atMs": 400},
    )
    # Order the dispatch before both same-millisecond closures: the admission
    # probe under test, and the wrapper's own exit.
    _reorder_capture(cloud, ["checkpoint_call", "admission_closed", "wrapper_return"])
    evidence = build_evidence(ctx, cloud)
    assert evidence["collectionErrors"] == []
    (request,) = evidence["sdkRequests"]
    admission_closed = next(event for event in evidence["events"] if event["kind"] == "admission_closed")
    assert request["atMs"] == admission_closed["atMs"] == 400
    assert request["sequence"] < admission_closed["sequence"]
    errors = validate_common_evidence(evidence, phase="observations")
    assert not any("dispatched SDK work after admission closed" in error for error in errors), errors


def test_tied_dispatch_after_wrapper_return_is_still_a_late_dispatch_assertion(tmp_path):
    # Unlike the two tests above, the dispatch here is captured after the
    # wrapper's own exit at the same millisecond: a genuinely late dispatch
    # that the fix must still flag once real sequence numbers are compared.
    ctx, cloud = cloud_source(
        tmp_path,
        {"kind": "checkpoint_call", "callId": "late", "atMs": 400},
        {"kind": "checkpoint_exit", "callId": "late", "atMs": 400},
    )
    evidence = build_evidence(ctx, cloud)
    assert evidence["collectionErrors"] == []
    (request,) = evidence["sdkRequests"]
    wrapper_return = next(event for event in evidence["events"] if event["kind"] == "wrapper_return")
    assert request["atMs"] == wrapper_return["atMs"] == 400
    assert request["sequence"] > wrapper_return["sequence"]
    errors = validate_common_evidence(evidence, phase="observations")
    assert any(
        error.startswith("ASSERTION:") and "dispatched SDK work after admission closed" in error for error in errors
    ), errors


def test_verified_launch_inventory_survives_missing_lifecycle_attempt(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    cloud.events.clear()
    cloud.invocations[0]["native_requests"].clear()
    cloud.raw_histories["execution"].append(
        {"EventType": "CallbackStarted", "Id": "callback-operation", "EventTimestamp": 0.2}
    )
    cloud.control_records = [{"gate": "never-created", "action": "hold", "state": "REJECTED"}]

    evidence = build_evidence(ctx, cloud, phase="partial")

    assert not evidence["attempts"]
    assert evidence["resourcesCreated"] == [
        {"kind": "invocation", "id": "native", "ownerCaseId": "case"},
        {"kind": "execution", "id": "execution", "ownerCaseId": "case"},
        {
            "kind": "callback",
            "id": "execution/callback/callback-operation",
            "ownerCaseId": "case",
            "executionArn": "execution",
            "operationId": "callback-operation",
        },
    ]
    assert any("has no lifecycle ledger stream" in pending for pending in evidence["collection"]["pending"])


def test_integral_json_ordinals_are_accepted_in_a_ledger_seal():
    records = raw_stream({"kind": "task_exit", "atMs": 500})
    for record in records:
        record["sequence"] = float(record["sequence"])
    records[1]["observation"]["captureSequence"] = 1e0
    records[-1].update(throughCapture=1e0, sourceProtocol=2e0)
    assert ledger_seal_status(records, {}, LAUNCHES, set()) == ({"native"}, set(), [])


@pytest.mark.parametrize("details", [None, {}, {"RequestId": []}])
def test_all_raw_and_service_native_requests_are_audited(tmp_path, details):
    ctx, cloud = cloud_source(tmp_path)
    cloud.events["orphan"] = {**cloud.events[1], "phase": "BLOCKED", "request": "unentered-request"}
    cloud.raw_histories["execution"].append({"EventType": "InvocationCompleted", "InvocationCompletedDetails": details})
    snapshot = build_evidence(ctx, cloud, phase="final")
    assert snapshot["collection"]["watermarks"] == {}
    assert any("unentered-request" in message for message in snapshot["collectionErrors"])
    assert any("has no request ID" in message for message in snapshot["collectionErrors"])


@pytest.mark.parametrize("request_fields", [{}, {"request": []}])
def test_malformed_wrapper_and_truncated_payload_do_not_erase_owned_inventory(tmp_path, request_fields):
    ctx, cloud = cloud_source(tmp_path)
    malformed = {key: value for key, value in cloud.events[1].items() if key != "request"}
    cloud.events["malformed"] = {**malformed, **request_fields}
    cloud.raw_histories["execution"].extend(
        [
            {
                "EventType": "StepSucceeded",
                "Id": "truncated-step",
                "EventTimestamp": 0.2,
                "StepSucceededDetails": {"Result": {"Payload": "prefix", "Truncated": True}},
            },
            {"EventType": "CallbackStarted", "Id": "callback-op", "EventTimestamp": 0.2},
        ]
    )
    partial = build_evidence(ctx, cloud, phase="partial")
    assert any("Invalid WRAPPER_ENTER identity" in error for error in partial["collectionErrors"])
    assert any("Truncated service data" in error for error in partial["collectionErrors"])
    operation = partial["histories"][0]["operations"][0]
    assert operation["id"] == "truncated-step" and operation["status"] == "SUCCEEDED"
    assert "result" not in operation
    assert {row["kind"] for row in partial["resourcesCreated"]} == {"invocation", "execution", "callback"}


def test_ledger_only_request_is_retained_only_for_a_verified_launch(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    cloud.events["owned"] = {**cloud.events[1], "phase": "BLOCKED", "request": "seen-without-wrapper"}
    cloud.events["foreign"] = {
        **cloud.events[1],
        "phase": "BLOCKED",
        "request": "another-launch",
        "marker": "other-marker",
    }
    final = build_evidence(ctx, cloud, phase="final")
    inventory = {row["id"]: row for row in final["retirement"]["resources"]}
    assert inventory["seen-without-wrapper"]["outcome"] == "UNKNOWN"
    assert "another-launch" not in inventory
    assert final["collection"]["watermarks"] == {}


def test_observation_scope_does_not_need_a_post_cutoff_retry_seal(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    cloud.events["future"] = {**cloud.events[1], "request": "future-retry", "time": 2}
    cloud.invocations[0]["native_requests"].append("future-retry")
    cloud.raw_histories["execution"].append(
        {
            "EventType": "InvocationCompleted",
            "EventId": 4,
            "InvocationCompletedDetails": {"RequestId": "future-retry", "StartTimestamp": 1.9, "EndTimestamp": 2.5},
        }
    )
    cloud.execution_results["execution"]["Status"] = "RUNNING"
    assert requests_after_cutoff(ctx.attempts, 1000) == set()
    ctx.record_attempt(
        "retry-attempt",
        role="victim",
        requestId="future-retry",
        executionArn="execution",
        source="service-observed",
        nativeStartedAtMs=1900,
    )
    excluded = requests_after_cutoff(ctx.attempts, 1000)
    assert excluded == {"future-retry"}
    assert ledger_seal_status(tuple(cloud.events.values()), cloud.raw_histories, cloud.invocations, excluded) == (
        {"native"},
        set(),
        [],
    )
    observation = build_evidence(ctx, cloud)
    assert observation["collectionErrors"] == []
    assert observation["collection"]["watermarks"] == {"lifecycle": 1000, "sdkRequests": 1000}
    assert "future" in cloud.events

    final = build_evidence(ctx, cloud, phase="final")
    assert any("future-retry" in message for message in final["collectionErrors"])
    assert final["collection"]["watermarks"] == {}


def _malformed_task_id_source(tmp_path):
    ctx, cloud = cloud_source(tmp_path, {"kind": "task_start", "taskId": [], "atMs": 410})
    cloud.raw_histories["execution"].append(
        {"EventType": "CallbackStarted", "Id": "callback-operation", "EventTimestamp": 0.2}
    )
    return ctx, cloud


def test_malformed_task_id_does_not_hide_verified_resources(tmp_path):
    ctx, cloud = _malformed_task_id_source(tmp_path)
    evidence = build_evidence(ctx, cloud, phase="partial")
    assert any("native at sequence 6 has invalid taskId" in message for message in evidence["collectionErrors"])
    assert {row["kind"] for row in evidence["resourcesCreated"]} == {"invocation", "execution", "callback"}


@pytest.mark.parametrize("missing_end", [False, True])
@pytest.mark.parametrize(
    ("status", "request_at", "pause_at", "expected_status"),
    [
        ("RUNNING", None, None, "RUNNING"),
        ("PAUSED", 0.8, 0.9, "PAUSED"),
        ("PAUSED", 1.01, 1.02, "RUNNING"),
        ("PAUSED", 0.99, 1.01, None),
        ("PAUSING", None, None, None),
    ],
)
def test_postcutoff_terminal_proof_does_not_leak_stop_into_frozen_observations(
    tmp_path, missing_end, status, request_at, pause_at, expected_status
):
    ctx, cloud = cloud_source(tmp_path)
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-3",
        ctx.profile.profile_id,
        "deadline-during-step",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud.invocations[0]["role"] = "victim"
    witnessed_at = max(1001, pause_at * 1000 + 1) if status == "PAUSED" and pause_at is not None else 1001
    cloud.cutoff_results = {"execution": {"Status": status, "observedAtMs": witnessed_at}}
    cloud.execution_results["execution"] = {
        "Status": "STOPPED",
        "Error": {"ErrorType": "ControlStop"},
        **({} if missing_end else {"EndTimestamp": 1.5}),
    }
    cloud.raw_histories["execution"][1].update(EventType="ExecutionStopped", EventTimestamp=1.5)
    cloud.raw_histories["execution"].append(
        {"EventType": "StepSucceeded", "EventId": 4, "Id": "later", "EventTimestamp": 1.3}
    )
    history = cloud.raw_histories["execution"]
    if pause_at is not None:
        history.insert(
            1,
            {
                "EventType": "ExecutionPaused",
                "EventTimestamp": pause_at,
                "ExecutionPausedDetails": {"PauseRequestTimestamp": request_at},
            },
        )
    for index, event in enumerate(history, 1):
        event["EventId"] = index
    observation = build_evidence(ctx, cloud)
    if expected_status is None:
        assert "history" not in observation["collection"]["watermarks"]
        assert any("does not establish the pause state" in reason for reason in observation["collection"]["pending"])
    else:
        assert observation["collection"]["watermarks"]["history"] == 1000
    assert observation["histories"][0]["status"] == expected_status
    assert observation["histories"][0]["operations"] == []
    assert "error" not in observation["histories"][0]

    cloud.observation_ended_at = 2
    final = build_evidence(ctx, cloud, phase="final")
    assert final["collection"]["watermarks"]["history"] == 2000
    assert final["histories"][0]["status"] == "STOPPED"


def test_cutoff_running_witness_cannot_certify_a_contradictory_terminal_end(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-3",
        ctx.profile.profile_id,
        "deadline-during-step",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud.invocations[0]["role"] = "victim"
    cloud.cutoff_results = {"execution": {"Status": "RUNNING", "observedAtMs": 1001}}
    observation = build_evidence(ctx, cloud)
    assert "history" not in observation["collection"]["watermarks"]
    assert observation["histories"][0]["status"] == "RUNNING"


def test_deadline_history_proof_does_not_substitute_for_a_missing_native_seal(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-3",
        ctx.profile.profile_id,
        "deadline-during-step",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud.invocations[0]["role"] = "victim"
    cloud.events.pop(max(cloud.events))
    cloud.cutoff_results = {"execution": {"Status": "RUNNING", "observedAtMs": 1001}}
    cloud.execution_results["execution"].update(Status="STOPPED", EndTimestamp=1.5)
    cloud.raw_histories["execution"][1].update(EventType="ExecutionStopped", EventTimestamp=1.5)
    observation = build_evidence(ctx, cloud)
    assert observation["collection"]["watermarks"] == {"history": 1000}
    assert any("Missing ledger seals" in pending for pending in observation["collection"]["pending"])


def test_first_attempt_for_each_role_is_preserved_even_with_late_service_start():
    starts = [
        {
            "requestId": "first-resume",
            "attempt": 1,
            "source": "service-observed",
            "executionArn": "exec",
            "nativeStartedAtMs": 1100,
        },
        {
            "requestId": "wrapper-only",
            "attempt": 2,
            "source": "service-observed",
            "executionArn": "exec",
            "launchedAtMs": 1100,
        },
        {
            "requestId": "script-launch",
            "attempt": 2,
            "source": "driver-invoke",
            "executionArn": "exec",
            "nativeStartedAtMs": 1100,
        },
    ]
    assert requests_after_cutoff(starts, 1000) == set()


def test_partial_observation_at_the_cutoff_does_not_include_driver_stop(tmp_path):
    ctx, cloud = cloud_source(tmp_path, {"kind": "task_exit", "taskId": "late", "atMs": 1100})
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-3",
        ctx.profile.profile_id,
        "deadline-during-step",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud.invocations[0]["role"] = "victim"
    cloud.cutoff_results = {"execution": {"Status": "RUNNING", "observedAtMs": 1001}}
    cloud.execution_results["execution"].update(Status="STOPPED", EndTimestamp=1.5)
    cloud.raw_histories["execution"][1].update(EventType="ExecutionStopped", EventTimestamp=1.5)
    before_release = build_evidence(ctx, cloud, phase="partial")
    assert before_release["observationEndedAtMs"] == 1000
    assert before_release["histories"][0]["status"] == "RUNNING"
    assert not any(event.get("taskId") == "late" for event in before_release["events"])
    assert before_release["collection"]["watermarks"] == {}

    physical = build_evidence(ctx, cloud, phase="retirement-partial")
    assert physical["histories"][0]["status"] == "STOPPED"
    assert any(event.get("taskId") == "late" for event in physical["events"])
    assert physical["collection"]["watermarks"] == {}


@pytest.mark.parametrize(
    "operations",
    [
        None,
        [None],
        [{"operationType": "STEP", "status": "SUCCEEDED"}],
        [{"operationId": "id", "operationType": "STEP", "status": "SUCCEEDED", "name": []}],
    ],
)
def test_malformed_replay_checkpoint_does_not_hide_owned_resources(tmp_path, operations):
    _, cloud = cloud_source(tmp_path, {"kind": "invocation_start", "atMs": 110, "operations": operations})
    # The journal is immutable through ScenarioContext.attempts, so journal the
    # resume role on a suspension-retry context instead of editing a copy.
    ctx = context(scenario="suspension-retry", roles=("victim", "resume"))
    ctx.record_attempt("native", role="resume", requestId="native")
    cloud.request_roles["native"] = "resume"
    cloud.raw_histories["execution"].append(
        {"EventType": "CallbackStarted", "Id": "callback-operation", "EventTimestamp": 0.2}
    )
    snapshot = build_evidence(ctx, cloud, phase="partial")
    assert not snapshot["collectionErrors"]
    assert any("Replay request native has a malformed" in error for error in snapshot["replay"]["projectionErrors"])
    assert {row["kind"] for row in snapshot["resourcesCreated"]} == {"invocation", "execution", "callback"}


def test_unpublished_post_cutoff_service_retry_is_included_only_in_retirement(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    ctx.update_attempt(ctx.attempts[0]["attemptId"], executionArn="execution")
    cloud.invocations[0].update(role="victim", resume_roles=())
    cloud.raw_histories["execution"].append(
        {
            "EventType": "InvocationCompleted",
            "EventId": 4,
            "InvocationCompletedDetails": {"RequestId": "future", "StartTimestamp": 1.1, "EndTimestamp": 1.3},
        }
    )
    cloud.execution_results["execution"]["Status"] = "RUNNING"
    observed = build_evidence(ctx, cloud)
    assert observed["collection"]["watermarks"] == {"lifecycle": 1000, "sdkRequests": 1000}
    assert "future" not in {item["id"] for item in observed["resourcesCreated"]}
    final = build_evidence(ctx, cloud, phase="final")
    assert final["collection"]["watermarks"] == {}
    assert any("future" in error for error in final["collectionErrors"])

    # An as-yet unbound first resume is not an ignorable retry.
    cloud.invocations[0]["resume_roles"] = ("resume",)
    observed = build_evidence(ctx, cloud)
    assert observed["collection"]["watermarks"] == {}


def test_undated_operation_is_not_placed_in_the_frozen_window(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    cloud.raw_histories["execution"].append(
        {
            "EventType": "CallbackStarted",
            "Id": "undated",
            "EventId": 4,
        }
    )
    frozen = build_evidence(ctx, cloud)
    assert "history" not in frozen["collection"]["watermarks"]
    assert not frozen["histories"][0]["operations"]
    assert "execution/callback/undated" not in {item["id"] for item in frozen["resourcesCreated"]}
    retired = build_evidence(ctx, cloud, phase="final")
    assert retired["histories"][0]["operations"][0]["id"] == "undated"
    assert "execution/callback/undated" in {item["id"] for item in retired["resourcesCreated"]}


@pytest.mark.parametrize(
    ("mode", "suspended", "keeps_checkpoint", "repeats_completed"),
    [
        ("wait", False, True, False),
        ("wait", True, True, False),
        ("wait", True, False, False),
        ("wait", True, True, True),
        ("callback", True, True, False),
        ("callback", True, False, False),
        ("callback", False, True, False),
    ],
)
def test_builder_checks_the_public_suspension_checkpoint_and_service_wakeup_tie(
    tmp_path, mode, suspended, keeps_checkpoint, repeats_completed
):
    ctx, cloud = cloud_source(tmp_path)
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-5",
        ctx.profile.profile_id,
        "suspension-retry",
        roles=("victim", "resume"),
        coverage_profile="standard",
    )
    cloud.suspension_mode = mode
    cloud.observation_ended_at = 2
    cloud.invocations[0]["native_requests"].append("resume")
    cloud.request_roles["resume"] = "resume"
    ctx.record_attempt("resume", role="resume", requestId="resume", executionArn="execution")
    victim = raw_stream(
        {"kind": "wrapper_enter", "atMs": 100},
        {"kind": "task_start", "taskId": "root", "atMs": 110},
        {"kind": "invocation_end", "outcome": "PENDING", "atMs": 300},
        {"kind": "task_exit", "taskId": "root", "atMs": 350},
        {"kind": "wrapper_return", "outcome": "PENDING", "atMs": 400},
    )
    victim.insert(-1, {**victim[0], "phase": "WRAPPER_RETURN", "status": "PENDING", "time": 0.4})
    for seq, row in enumerate(victim, 1):
        row["sequence"] = seq
    prefix = "Wait" if mode == "wait" else "Callback"
    checkpoint = {"operationId": "operation", "operationType": prefix.upper(), "status": "SUCCEEDED"}
    if mode == "callback":
        checkpoint.update(name="completion", serializedResult="marker")
    checkpoints = [checkpoint] if suspended and keeps_checkpoint else []
    resume = raw_stream(
        {"kind": "wrapper_enter", "atMs": 600},
        {"kind": "invocation_start", "operations": checkpoints, "atMs": 610},
        {"kind": "task_start", "taskId": "root", "atMs": 620},
        *(
            [{"kind": "body", "operation": "already completed", "operationId": "operation", "attempt": 1, "atMs": 630}]
            if repeats_completed
            else []
        ),
        {"kind": "task_exit", "taskId": "root", "atMs": 700},
        {"kind": "wrapper_return", "outcome": "SUCCEEDED", "atMs": 710},
    )
    for row in resume:
        row["request"] = "resume"
    resume[0]["time"] = 0.6
    cloud.events = dict(enumerate([*victim, *resume]))
    history: list[dict[str, Any]] = [
        {"EventType": "ExecutionStarted", "EventTimestamp": 0.05},
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {
                "RequestId": "native",
                "StartTimestamp": 0.1,
                "EndTimestamp": 0.5,
            },
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {
                "RequestId": "resume",
                "StartTimestamp": 0.6,
                "EndTimestamp": 0.75,
            },
        },
        {"EventType": "ExecutionSucceeded", "EventTimestamp": 0.74},
    ]
    if suspended or mode == "callback":
        start = {"EventType": prefix + "Started", "Id": "operation", "EventTimestamp": 0.2 if suspended else 0.61}
        end: dict[str, Any] = {
            "EventType": prefix + "Succeeded",
            "Id": "operation",
            "EventTimestamp": 0.6 if suspended else 0.62,
        }
        if repeats_completed:
            start["Name"] = end["Name"] = checkpoint["name"] = "already completed"
        if mode == "callback":
            start["Name"] = end["Name"] = "completion"
            end["CallbackSucceededDetails"] = {"Result": {"Payload": "marker"}}
        history[2:2] = [start, end]
    for index, row in enumerate(history, 1):
        row["EventId"] = index
    cloud.raw_histories["execution"] = history
    cloud.execution_results["execution"]["EndTimestamp"] = 0.74
    evidence = build_evidence(ctx, cloud)
    assert evidence["collectionErrors"] == []
    if mode == "wait":
        assert evidence["replay"]["durableWaitObserved"] is suspended
    assert evidence["replay"]["suspensionCheckpointMatches"] is (
        suspended and keeps_checkpoint and not repeats_completed
    )


def test_failed_native_pending_does_not_authorize_callback():
    from aws_durable_execution_conformance_tests.lmi.observations import pending_callback

    ledger: list[dict[str, Any]] = [
        {"phase": "WRAPPER_ENTER", "request": "request"},
        {"phase": "WRAPPER_RETURN", "request": "request", "status": "PENDING", "time": 1},
    ]
    completion = {
        "RequestId": "request",
        "StartTimestamp": 0,
        "EndTimestamp": 1.1,
        "Error": {"Payload": {"ErrorType": "Runtime.ExitError"}},
    }
    history: list[dict[str, Any]] = [
        {
            "EventType": "CallbackStarted",
            "Name": "completion",
            "Id": "callback",
            "EventTimestamp": 0.9,
            "CallbackStartedDetails": {"CallbackId": "token"},
        },
        {"EventType": "InvocationCompleted", "InvocationCompletedDetails": completion},
    ]
    assert pending_callback(ledger, history, "completion") is None
    del completion["Error"]
    assert pending_callback(ledger, history, "completion") == "token"


@pytest.mark.parametrize(
    ("observations", "selected"),
    [
        ([], True),
        ([{"outcome": "PENDING", "atMs": 800}], False),
        ([{"outcome": "PENDING", "atMs": 800}, {"outcome": "PENDING", "atMs": 850}], True),
        ([{"outcome": "SUCCEEDED", "atMs": 800}], True),
        ([{"outcome": "PENDING", "atMs": 1050}], True),
        ([{"outcome": "PENDING", "atMs": -1}], True),
        ([{"outcome": "PENDING", "atMs": True}], True),
        ([{"outcome": "PENDING"}], True),
    ],
)
def test_ambiguous_pending_decision_falls_back_to_the_wrapper_return(observations, selected):
    """Only one in-range PENDING invocation_end moves the decision before the wrapper return.

    The callback starts at 900 ms and the wrapper returns at 1000 ms, and every
    observed atMs other than the out-of-range ones is before 900 ms, so the
    callback belongs to the suspension only when the decision is the wrapper
    return.
    """
    from aws_durable_execution_conformance_tests.lmi.observations import pending_callback

    ledger: list[dict[str, Any]] = [
        {"phase": "WRAPPER_ENTER", "request": "request"},
        *(
            {"phase": "CONFORMANCE", "request": "request", "observation": {"kind": "invocation_end", **row}}
            for row in observations
        ),
        {"phase": "WRAPPER_RETURN", "request": "request", "status": "PENDING", "time": 1},
    ]
    history: list[dict[str, Any]] = [
        {
            "EventType": "CallbackStarted",
            "Name": "completion",
            "Id": "callback",
            "EventTimestamp": 0.9,
            "CallbackStartedDetails": {"CallbackId": "token"},
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "request", "StartTimestamp": 0, "EndTimestamp": 1.1},
        },
    ]
    assert pending_callback(ledger, history, "completion") == ("token" if selected else None)


def test_wrapper_return_just_after_the_service_end_keeps_the_suspension():
    """A wrapper clock slightly ahead of the service must not drop a PENDING suspension.

    Adjudication does not bound the wrapper return by the service end either.
    """
    from aws_durable_execution_conformance_tests.lmi.observations import pending_callback

    ledger: list[dict[str, Any]] = [
        {"phase": "WRAPPER_ENTER", "request": "request"},
        {"phase": "WRAPPER_RETURN", "request": "request", "status": "PENDING", "time": 1.105},
    ]
    history: list[dict[str, Any]] = [
        {
            "EventType": "CallbackStarted",
            "Name": "completion",
            "Id": "callback",
            "EventTimestamp": 0.9,
            "CallbackStartedDetails": {"CallbackId": "token"},
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "request", "StartTimestamp": 0, "EndTimestamp": 1.1},
        },
    ]
    assert pending_callback(ledger, history, "completion") == "token"


def test_service_proven_late_retry_with_a_scoped_nonwrapper_row_is_deferred(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    ctx.update_attempt(ctx.attempts[0]["attemptId"], executionArn="execution")
    cloud.invocations[0].update(role="victim", resume_roles=())
    cloud.raw_histories["execution"].append(
        {
            "EventType": "InvocationCompleted",
            "EventId": 4,
            "InvocationCompletedDetails": {"RequestId": "future", "StartTimestamp": 1.1, "EndTimestamp": 1.3},
        }
    )
    cloud.events["future"] = {**cloud.events[1], "phase": "BLOCKED", "request": "future", "time": 1.2}
    observed = build_evidence(ctx, cloud)
    assert "lifecycle" in observed["collection"]["watermarks"]
    assert any("future" in error for error in build_evidence(ctx, cloud, phase="final")["collectionErrors"])

    cloud.events["future"]["marker"] = "wrong-launch"
    observed = build_evidence(ctx, cloud)
    assert "lifecycle" not in observed["collection"]["watermarks"]


def test_old_pause_cycle_cannot_identify_a_new_pausing_witness(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-3",
        ctx.profile.profile_id,
        "deadline-during-step",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud.invocations[0]["role"] = "victim"
    cloud.cutoff_results = {"execution": {"Status": "PAUSING", "observedAtMs": 1001}}
    cloud.execution_results["execution"].update(Status="STOPPED", EndTimestamp=1.5)
    history = cloud.raw_histories["execution"]
    history[1].update(EventType="ExecutionStopped", EventTimestamp=1.5)
    history[1:1] = [
        {
            "EventType": "ExecutionPaused",
            "EventTimestamp": 0.7,
            "ExecutionPausedDetails": {"PauseRequestTimestamp": 0.6},
        },
        {"EventType": "ExecutionResumed", "EventTimestamp": 0.8},
    ]
    for index, event in enumerate(history, 1):
        event["EventId"] = index
    observed = build_evidence(ctx, cloud)
    assert observed["histories"][0]["status"] is None
    assert "history" not in observed["collection"]["watermarks"]


def test_sealed_stream_without_wrapper_is_an_observation_error(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    wrapper = next(key for key, row in cloud.events.items() if row["phase"] == "WRAPPER_ENTER")
    del cloud.events[wrapper]

    evidence = build_evidence(ctx, cloud)

    assert any("ledger seal but no wrapper entry" in error for error in evidence["collectionErrors"])
    assert not evidence["collection"]["watermarks"]


def _late_retry_source(tmp_path):
    """A normal-return victim whose sealed, journaled service retry starts after the 1000ms cutoff."""
    from aws_durable_execution_conformance_tests.lmi.coordinator import Scenario

    ctx, cloud = cloud_source(
        tmp_path,
        {"kind": "root_completed", "outcome": "SUCCEEDED", "atMs": 290},
        {"kind": "admission_probe", "probe": "operation", "outcome": "REJECTED", "atMs": 410},
        {"kind": "admission_probe", "probe": "checkpoint", "outcome": "REJECTED", "atMs": 411},
    )
    ctx.scenario = Scenario(
        "case",
        "lmi-lifecycle-2",
        ctx.profile.profile_id,
        "normal-return",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud.manifest["conformanceRevision"] = "conformance"
    ctx.update_attempt(ctx.attempts[0]["attemptId"], executionArn="execution")
    ctx.record_attempt(
        "late-attempt",
        role="victim",
        requestId="late-retry",
        executionArn="execution",
        source="service-observed",
        nativeStartedAtMs=1100,
    )
    retry = raw_stream(
        {"kind": "wrapper_enter", "atMs": 1100},
        {"kind": "task_start", "taskId": "root", "atMs": 1110},
        {"kind": "task_exit", "taskId": "root", "atMs": 1290},
        {"kind": "wrapper_return", "outcome": "SUCCEEDED", "atMs": 1295},
    )
    for row in retry:
        row.update(request="late-retry", time=max(row["time"], 1.1))
        cloud.events[f"late-retry/{row['sequence']}"] = row
    cloud.request_roles["late-retry"] = "victim"
    cloud.invocations[0]["native_requests"].append("late-retry")
    cloud.raw_histories["execution"].append(
        {
            "EventType": "InvocationCompleted",
            "EventId": 4,
            "InvocationCompletedDetails": {"RequestId": "late-retry", "StartTimestamp": 1.1, "EndTimestamp": 1.3},
        }
    )
    return ctx, cloud


def test_deferred_service_retry_is_proven_by_independent_retirement(tmp_path):
    from copy import deepcopy

    from aws_durable_execution_conformance_tests.lmi.adjudication import evaluate_evidence
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_common_evidence

    ctx, cloud = _late_retry_source(tmp_path)
    frozen = build_evidence(ctx, cloud)
    assert frozen["collection"]["deferredServiceRequests"] == [{"requestId": "late-retry", "executionArn": "execution"}]
    assert [attempt["requestId"] for attempt in frozen["attempts"]] == ["native"]
    assert any("independent retirement audit" in error for error in validate_evidence(frozen, phase="final"))
    # Retirement reads sources again after the late retry has completed.
    cloud.observation_ended_at = 1.4
    final = build_evidence(ctx, cloud, phase="final")
    assert {attempt["requestId"]: attempt["startedAtMs"] for attempt in final["attempts"]} == {
        "native": 100,
        "late-retry": 1100,
    }

    def common(evidence, *, phase="final"):
        return validate_common_evidence(evidence, phase=phase)

    def adjudicate(frozen_evidence, retirement_evidence, validator=None):
        return evaluate_evidence(
            frozen_evidence,
            retirement_evidence=retirement_evidence,
            cleanup_evidence={},
            attempts=ctx.attempts,
            standard=True,
            validator=validator,
        )

    for validator in (None, common):
        assert adjudicate(frozen, final, validator) == []
    phantom = deepcopy(frozen)
    phantom["collection"]["deferredServiceRequests"].append({"requestId": "phantom", "executionArn": "execution"})
    early = deepcopy(final)
    next(row for row in early["attempts"] if row["requestId"] == "late-retry")["startedAtMs"] = 1000
    for validator in (None, common):
        assert (
            "INCOMPLETE: independent retirement requests must match the frozen and deferred inventories"
            in adjudicate(phantom, final, validator)
        )
        assert "INCOMPLETE: Retirement did not prove deferred service request late-retry started after the cutoff" in (
            adjudicate(frozen, early, validator)
        )
    concealed = deepcopy(frozen)
    del concealed["collection"]["deferredServiceRequests"]
    assert "INCOMPLETE: frozen observations do not cover all client-journaled launches" in adjudicate(concealed, final)


@pytest.mark.parametrize("proof", ["sealed", "unsealed", "no-completion", "journal-interval"])
def test_case_sources_mark_only_a_proven_victim_retry_active_at_the_cutoff(tmp_path, proof):
    from aws_durable_execution_conformance_tests.lmi.source_evidence import CaseSources

    ctx, cloud = cloud_source(tmp_path)
    ctx.update_attempt(ctx.attempts[0]["attemptId"], executionArn="execution")
    ctx.record_attempt(
        "retry-attempt",
        role="victim",
        requestId="in-flight",
        executionArn="execution",
        source="service-observed",
        nativeStartedAtMs=900,
        nativeEndedAtMs=1250 if proof == "journal-interval" else 1300,
    )
    for row in raw_stream(
        {"kind": "wrapper_enter", "atMs": 900}, {"kind": "task_start", "taskId": "root", "atMs": 910}
    ):
        row.update(request="in-flight", time=0.9 if row["phase"] == "WRAPPER_ENTER" else 0.95)
        cloud.events[f"in-flight/{row['sequence']}"] = row
    cloud.request_roles["in-flight"] = "victim"
    cloud.invocations[0]["native_requests"].append("in-flight")
    if proof != "no-completion":
        cloud.raw_histories["execution"].append(
            {
                "EventType": "InvocationCompleted",
                "EventId": 4,
                "InvocationCompletedDetails": {"RequestId": "in-flight", "StartTimestamp": 0.9, "EndTimestamp": 1.3},
            }
        )
    sources = CaseSources(
        ctx,
        cloud,
        tuple(cloud.events.values()),
        expected_outcome=lambda *_: {"status": "SUCCEEDED"},
        worker_identity=lambda raw: raw["process"],
    )
    pending: list[str] = []
    events, errors = sources.observations(pending=pending)
    attempts, selected, attempt_errors = sources.attempts(
        events,
        pending=pending,
        cutoff_ms=1000,
        excluded=set(),
        sealed={"native"} if proof == "unsealed" else {"native", "in-flight"},
    )

    assert errors == attempt_errors == []
    assert selected == {"victim": "native"}
    retry = next(attempt for attempt in attempts if attempt["requestId"] == "in-flight")
    assert retry["startedAtMs"] == 900
    assert not {"endedAtMs", "outcome"} & retry.keys()
    unbounded = "Native request in-flight has no observed completion boundary"
    if proof == "sealed":
        assert retry["activeAtObservationEnd"] is True
        assert unbounded not in pending
    else:
        assert "activeAtObservationEnd" not in retry
        assert unbounded in pending


@pytest.mark.parametrize(("ordinal", "retry"), [(2, True), (2.0, True), (1, False), (1.5, False), (True, False)])
def test_cutoff_rules_accept_only_integral_retry_ordinals(ordinal, retry):
    from aws_durable_execution_conformance_tests.lmi.rules.cutoff import active_retry_interval

    attempt = {
        "requestId": "in-flight",
        "executionArn": "execution",
        "role": "victim",
        "source": "service-observed",
        "attempt": ordinal,
        "nativeStartedAtMs": 900,
        "nativeEndedAtMs": 1300,
    }
    assert active_retry_interval(attempt, 1000, "execution") is retry
    late = {**attempt, "nativeStartedAtMs": 1100}
    assert requests_after_cutoff([late], 1000) == ({"in-flight"} if retry else set())


@pytest.mark.parametrize("kind", [[], {}, None, 7])
def test_sdk_requests_skip_a_malformed_kind(kind):
    from aws_durable_execution_conformance_tests.lmi.source_evidence import _sdk_requests

    call = {"kind": "poll_call", "requestId": "native", "callId": "c", "atMs": 1}
    assert _sdk_requests([{"kind": kind, "requestId": "native"}, call]) == [
        {**call, "method": "GetDurableExecutionState", "outcome": "DISPATCHED"}
    ]
