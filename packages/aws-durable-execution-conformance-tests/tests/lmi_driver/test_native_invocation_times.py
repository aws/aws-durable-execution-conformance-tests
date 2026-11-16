# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Native timing comes from the invocation, independently of fixture startup."""

from datetime import UTC, datetime
from pathlib import Path
from runpy import run_path
from unittest.mock import Mock

import pytest

from aws_durable_execution_conformance_tests.lmi.coordinator import Profile, Scenario, ScenarioContext
from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence
from aws_durable_execution_conformance_tests.lmi.source_evidence import CaseSources


def native_sources(*, start=0.010, observed_start=11, end=0.030, event_time=0.030, error=None):
    value = run_path(str(Path(__file__).parents[1] / "test_lmi_evidence.py"))["deadline_evidence"]()
    victim = value["attempts"][0]
    request = victim["requestId"]
    arn = victim["executionArn"]
    context = ScenarioContext(
        value["runId"],
        Scenario(value["caseId"], value["requirementId"], "c2", "deadline-during-step"),
        Profile("c2", "function", 2, 2, "sdk", "config"),
    )
    context.record_attempt(request, role="victim", requestId=request)
    # The native invocation starts at 10 ms, but fixture initialization does not
    # finish until 11 ms. Its runtime context correctly reports 19 ms remaining.
    raw = {
        "phase": "WRAPPER_ENTER",
        "request": request,
        "execution": arn,
        "marker": "marker",
        "environment": victim["environmentId"],
        "process": victim["processId"],
        "time": observed_start / 1000,
    }
    events = [
        {**event, **({"outcome": "FAILED"} if event["kind"] == "wrapper_return" else {})}
        for event in value["events"]
        if event["requestId"] == request and event["kind"] != "platform_timeout"
    ]
    events.append(
        {
            "kind": "runtime_deadline",
            "requestId": request,
            "ownerRequestId": request,
            "executionArn": arn,
            "atMs": observed_start,
            "remainingTimeMs": 30 - observed_start,
            "deadlineAtMs": 30,
        }
    )
    cloud = Mock(
        invocations=[{"marker": "marker", "payload": {"scenario": "deadline"}}],
        request_roles={request: "victim"},
        raw_histories={
            arn: [
                # The durable execution and a previous native invocation can
                # both start earlier. Neither is this request's native start.
                {"EventType": "ExecutionStarted", "EventTimestamp": 0.001},
                {
                    "EventType": "InvocationCompleted",
                    "EventTimestamp": 0.009,
                    "InvocationCompletedDetails": {
                        "RequestId": "previous-request",
                        "StartTimestamp": 0.002,
                        "EndTimestamp": 0.009,
                    },
                },
                {
                    "EventType": "InvocationCompleted",
                    "EventId": 3,
                    "EventTimestamp": event_time,
                    "InvocationCompletedDetails": {
                        "RequestId": request,
                        "StartTimestamp": start,
                        "EndTimestamp": end,
                        "Error": error if error is not None else {"ErrorType": "Sandbox.Timedout"},
                    },
                },
            ]
        },
    )
    sources = CaseSources(context, cloud, (raw,), lambda *_: {}, lambda raw: raw["process"])
    return sources, events, value


def normalized_deadline(sources, events, value):
    attempts, _, errors = sources.attempts(events)
    value["attempts"][0] = attempts[0]
    value["events"] = [event for event in value["events"] if event["requestId"] != attempts[0]["requestId"]] + events
    value["collectionErrors"] = errors
    return value, errors


@pytest.mark.parametrize("start", [0.010, datetime.fromtimestamp(0.010, UTC), "1970-01-01T00:00:00.010Z"])
def test_delayed_fixture_start_does_not_shorten_the_native_timeout(start):
    sources, events, value = native_sources(start=start)
    value, errors = normalized_deadline(sources, events, value)
    assert errors == []
    assert value["attempts"][0]["startedAtMs"] == 10
    assert value["attempts"][0]["deadlineAtMs"] == 30
    assert validate_evidence(value) == []


def test_genuinely_premature_native_timeout_still_fails():
    sources, events, value = native_sources(start=0.015, observed_start=16)
    value, errors = normalized_deadline(sources, events, value)
    assert errors == []
    assert value["attempts"][0]["startedAtMs"] == 15
    assert any("did not cross the configured invocation deadline" in error for error in validate_evidence(value))


@pytest.mark.parametrize("start", [None, "invalid", True, -0.001, float("nan"), float("inf"), 10**400, 0.012])
def test_malformed_native_start_cannot_fall_back_to_a_passing_observation(start):
    sources, events, value = native_sources(start=start)
    value, errors = normalized_deadline(sources, events, value)
    assert any("StartTimestamp" in error for error in errors)
    assert validate_evidence(value)


def test_completed_invocation_cannot_omit_its_native_start():
    sources, events, value = native_sources()
    sources.cloud.raw_histories[value["attempts"][0]["executionArn"]][-1]["InvocationCompletedDetails"].pop(
        "StartTimestamp"
    )
    _, errors = normalized_deadline(sources, events, value)
    assert any("StartTimestamp" in error for error in errors)


def test_running_invocation_keeps_observed_interval_without_inventing_a_native_start():
    sources, events, value = native_sources()
    sources.cloud.raw_histories[value["attempts"][0]["executionArn"]].pop()
    events = [event for event in events if event["kind"] != "wrapper_return"]
    attempts, _, errors = sources.attempts(events)
    assert attempts[0]["startedAtMs"] == 11
    assert "endedAtMs" not in attempts[0]
    assert errors == ["Native request request-victim has no observed completion boundary"]


def test_native_timeout_is_measured_at_the_invocation_end_not_the_history_event():
    # The service writes the history event after the invocation ends. Using the
    # event time would move the timeout boundary past the admission closure and
    # cleanup it bounds, rejecting a conforming SDK.
    sources, events, value = native_sources(end=0.030, event_time=0.040)
    value, errors = normalized_deadline(sources, events, value)
    assert errors == []
    timeout = next(event for event in events if event["kind"] == "platform_timeout")
    assert timeout["atMs"] == 30
    assert value["attempts"][0]["endedAtMs"] == 30
    assert validate_evidence(value) == []


@pytest.mark.parametrize("end", [None, "invalid", True, float("nan"), float("inf"), 10**400, 0.009])
def test_malformed_native_end_cannot_fall_back_to_the_history_event(end):
    sources, events, value = native_sources(end=end)
    value, errors = normalized_deadline(sources, events, value)
    assert any("EndTimestamp" in error for error in errors)
    assert validate_evidence(value)


# The service wraps the error in a Payload, as in a real LMI run.
REAL_TIMEOUT = {"Payload": {"ErrorType": "Sandbox.Timedout", "ErrorMessage": "Task timed out after 60.00 seconds"}}


def test_platform_timeout_is_read_from_the_service_error_payload():
    sources, events, _ = native_sources(error={**REAL_TIMEOUT, "Truncated": False})
    attempts, _, errors = sources.attempts(events)
    assert errors == []
    assert attempts[0]["outcome"] == "TIMED_OUT"


@pytest.mark.parametrize(
    "error",
    [
        {"Payload": {"ErrorType": "DatabaseError", "ErrorMessage": "database timeout after retries"}},
        {"Payload": {"ErrorType": "ValueError", "ErrorMessage": "bad", "StackTrace": ["timeout_helpers.py:3"]}},
    ],
)
def test_application_errors_mentioning_timeout_are_not_platform_timeouts(error):
    # Matching the serialized error turned these into native timeouts and
    # overwrote the observed FAILED outcome with TIMED_OUT.
    sources, events, _ = native_sources(error=error)
    attempts, _, _ = sources.attempts(events)
    assert not any(event["kind"] == "platform_timeout" for event in events)
    assert attempts[0]["outcome"] == "FAILED"
