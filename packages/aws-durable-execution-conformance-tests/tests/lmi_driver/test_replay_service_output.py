# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Replay snapshots must be compared against independent service output."""

from copy import deepcopy

import pytest

from aws_durable_execution_conformance_tests.lmi.source_evidence import replay_evidence

from .test_evidence import sources


def history_sources(event_type, details, snapshot):
    _, observed = sources(
        [],
        invocations=[{"arn": "exec", "marker": "m"}],
        raw_histories={
            "exec": [
                {
                    "EventType": event_type,
                    "Id": "done",
                    "Name": "completed-operation",
                    "ParentId": None,
                    event_type + "Details": details,
                }
            ]
        },
        execution_results={"exec": {"Status": "SUCCEEDED"}},
    )
    events = [
        {"kind": "invocation_start", "requestId": "victim", "executionArn": "exec"},
        {
            "kind": "invocation_start",
            "requestId": "resume",
            "executionArn": "exec",
            "operations": [
                {
                    "operationId": "done",
                    "operationType": "STEP",
                    "name": "completed-operation",
                    "parentId": None,
                    **snapshot,
                }
            ],
        },
    ]
    return observed, events


@pytest.mark.parametrize(
    "event_type,snapshot,output",
    [
        ("StepSucceeded", {"status": "SUCCEEDED", "serializedResult": '{"value":"corrupt"}'}, "result"),
        ("StepFailed", {"status": "FAILED", "error": {"type": "CorruptError", "message": "corrupt"}}, "error"),
    ],
)
def test_missing_service_output_cannot_be_filled_from_replay_snapshot(event_type, snapshot, output):
    observed, events = history_sources(event_type, {}, snapshot)
    retained = deepcopy(events)
    histories, _, errors = observed.histories(events)
    replay = replay_evidence(events, histories, {"victim": "victim", "resume": "resume"})

    assert errors == [f"Service history is missing the {output} for operation done"]
    assert output in replay["before"][0]
    assert output not in replay["after"][0]
    assert replay["before"] != replay["after"]
    assert events == retained


@pytest.mark.parametrize(
    "event_type,details,snapshot,output,expected",
    [
        (
            "StepSucceeded",
            {"Result": {"Payload": '{"value":"service"}', "Truncated": False}},
            {"status": "SUCCEEDED", "serializedResult": '{"value":"corrupt"}'},
            "result",
            {"value": "service"},
        ),
        (
            "StepFailed",
            {"Error": {"Payload": {"ErrorType": "ServiceError", "ErrorMessage": "service"}, "Truncated": False}},
            {"status": "FAILED", "error": {"type": "CorruptError", "message": "corrupt"}},
            "error",
            {"type": "ServiceError", "message": "service"},
        ),
    ],
)
def test_replay_comparison_retains_conflicting_independent_service_output(
    event_type, details, snapshot, output, expected
):
    observed, events = history_sources(event_type, details, snapshot)
    histories, _, errors = observed.histories(events)
    replay = replay_evidence(events, histories, {"victim": "victim", "resume": "resume"})

    assert errors == []
    assert replay["after"][0][output] == expected
    assert replay["before"][0][output] != expected


@pytest.mark.parametrize("service_result", ["null", {"Payload": "null", "Truncated": False}])
def test_persisted_service_null_is_a_result(service_result):
    observed, events = history_sources(
        "StepSucceeded",
        {"Result": service_result},
        {"status": "SUCCEEDED", "serializedResult": "null"},
    )
    histories, _, errors = observed.histories(events)
    assert errors == []
    assert histories[0]["operations"][0]["result"] is None


def test_wait_success_uses_the_service_models_empty_result():
    observed, events = history_sources("WaitSucceeded", {}, {"status": "SUCCEEDED", "serializedResult": '"corrupt"'})
    histories, _, errors = observed.histories(events)
    assert errors == []
    assert histories[0]["operations"][0]["result"] is None


@pytest.mark.parametrize("error_source", ["operation", "execution"])
def test_truncated_service_error_cannot_establish_replay_equality(error_source):
    error = {"Payload": {"ErrorType": "ServiceError", "ErrorMessage": "retained prefix"}, "Truncated": False}
    observed, events = history_sources(
        "StepFailed",
        {"Error": error},
        {"status": "FAILED", "error": {"type": "ServiceError", "message": "retained prefix"}},
    )
    if error_source == "operation":
        error["Truncated"] = True
    else:
        observed.cloud.execution_results["exec"] = {"Status": "FAILED", "Error": {**error, "Truncated": True}}

    with pytest.raises(ValueError, match="Truncated service data"):
        observed.histories(events)
