# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Replay snapshots must be compared against independent service output."""

from copy import deepcopy
from typing import Any

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
    histories, _, errors = observed.histories()
    replay = replay_evidence(events, histories, {"victim": "victim", "resume": "resume"})

    assert errors == (["Service history is missing the error for operation done"] if output == "error" else [])
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
    histories, _, errors = observed.histories()
    replay = replay_evidence(events, histories, {"victim": "victim", "resume": "resume"})

    assert errors == []
    assert replay["after"][0][output] == expected
    assert replay["before"][0][output] != expected


@pytest.mark.parametrize("service_result", ["null", {"Payload": "null", "Truncated": False}])
def test_persisted_service_null_is_a_result(service_result):
    observed, _ = history_sources(
        "StepSucceeded",
        {"Result": service_result},
        {"status": "SUCCEEDED", "serializedResult": "null"},
    )
    histories, _, errors = observed.histories()
    assert errors == []
    assert histories[0]["operations"][0]["result"] is None


def test_wait_success_uses_the_service_models_empty_result():
    observed, _ = history_sources("WaitSucceeded", {}, {"status": "SUCCEEDED", "serializedResult": '"corrupt"'})
    histories, _, errors = observed.histories()
    assert errors == []
    assert histories[0]["operations"][0]["result"] is None


@pytest.mark.parametrize("error_source", ["operation", "execution"])
def test_truncated_service_error_cannot_establish_replay_equality(error_source):
    error = {"Payload": {"ErrorType": "ServiceError", "ErrorMessage": "retained prefix"}, "Truncated": False}
    observed, _ = history_sources(
        "StepFailed",
        {"Error": error},
        {"status": "FAILED", "error": {"type": "ServiceError", "message": "retained prefix"}},
    )
    if error_source == "operation":
        error["Truncated"] = True
    else:
        observed.cloud.execution_results["exec"] = {"Status": "FAILED", "Error": {**error, "Truncated": True}}

    histories, _, errors = observed.histories()
    assert any("Truncated service data" in error for error in errors)
    if error_source == "operation":
        operation = histories[0]["operations"][0]
        assert operation["id"] == "done" and operation["status"] == "FAILED"
        assert "error" not in operation
    else:
        assert histories[0]["status"] == "FAILED" and "error" not in histories[0]


def test_service_identity_cannot_be_filled_from_replay_snapshot():
    """An unnamed root operation's service events carry no Name or ParentId.

    replay.before reads the SDK snapshot, so filling the service history's
    identity from that same snapshot would let a corrupted name or parent
    compare equal to itself.
    """
    _, observed = sources(
        [],
        invocations=[{"arn": "exec", "marker": "m"}],
        raw_histories={
            "exec": [
                {
                    "EventType": "StepSucceeded",
                    "Id": "done",
                    "StepSucceededDetails": {"Result": '{"value":1}'},
                }
            ]
        },
        execution_results={"exec": {"Status": "SUCCEEDED"}},
    )
    events: list[dict[str, Any]] = [
        {"kind": "invocation_start", "requestId": "victim", "executionArn": "exec"},
        {
            "kind": "invocation_start",
            "requestId": "resume",
            "executionArn": "exec",
            "operations": [
                {
                    "operationId": "done",
                    "operationType": "STEP",
                    "name": "corrupt-name",
                    "parentId": "corrupt-parent",
                    "status": "SUCCEEDED",
                    "serializedResult": '{"value":1}',
                }
            ],
        },
    ]
    histories, _, errors = observed.histories()
    replay = replay_evidence(events, histories, {"victim": "victim", "resume": "resume"})

    assert errors == []
    operation = histories[0]["operations"][0]
    assert operation["name"] is None
    assert operation["parentId"] is None
    assert replay["before"][0]["name"] == "corrupt-name"
    assert replay["before"] != replay["after"]


@pytest.mark.parametrize("fields", [{}, {"serializedResult": None}])
def test_missing_sdk_result_does_not_match_explicit_service_null(fields):
    observed, events = history_sources(
        "StepSucceeded",
        {"Result": "null"},
        {"status": "SUCCEEDED", **fields},
    )
    histories, _, errors = observed.histories()
    assert errors == []
    replay = replay_evidence(events, histories, {"victim": "victim", "resume": "resume"})
    assert "result" not in replay["before"][0]
    assert replay["after"][0]["result"] is None


def test_retry_scheduled_service_failure_stays_nonterminal_until_success():
    observed, _ = history_sources(
        "StepFailed",
        {"Error": {"ErrorType": "transient"}, "RetryDetails": {"NextAttemptDelaySeconds": 0}},
        {"status": "SUCCEEDED"},
    )
    histories, _, errors = observed.histories(pending=[])
    assert errors == []
    operation = histories[0]["operations"][0]
    assert operation["status"] == "STARTED"
    assert "error" not in operation
    observed.cloud.raw_histories["exec"].extend(
        [
            {"EventType": "StepStarted", "Id": "done"},
            {"EventType": "StepSucceeded", "Id": "done", "StepSucceededDetails": {"Result": '"success"'}},
            {"EventType": "WaitCancelled", "Id": "wait"},
        ]
    )
    histories, _, errors = observed.histories()
    assert errors == []
    operation, wait = histories[0]["operations"]
    assert operation["status"] == "SUCCEEDED"
    assert operation["result"] == "success"
    assert "error" not in operation
    assert wait["status"] == "CANCELLED"


@pytest.mark.parametrize("details", [{}, {"Result": {}}, {"Result": {"Truncated": False}}])
def test_resultless_context_does_not_fail_generic_history_collection(details):
    observed, _ = history_sources("ContextSucceeded", details, {"status": "SUCCEEDED"})
    histories, _, errors = observed.histories()
    assert errors == []
    operation = histories[0]["operations"][0]
    assert operation["status"] == "SUCCEEDED"
    assert "result" not in operation


@pytest.mark.parametrize(
    ("service_wire", "sdk_wire", "matches"),
    [
        ('"marker"', "marker", False),
        ('"marker"', ' "marker" ', True),
        ("marker", "marker", True),
    ],
)
def test_replay_distinguishes_json_encoding_from_raw_serialization(service_wire, sdk_wire, matches):
    from dataclasses import replace

    observed, events = history_sources(
        "StepSucceeded", {"Result": {"Payload": service_wire}}, {"status": "SUCCEEDED", "serializedResult": sdk_wire}
    )
    observed.context.scenario = replace(observed.context.scenario, scenario="suspension-retry")
    histories, _, errors = observed.histories()
    assert errors == []
    replay = replay_evidence(events, histories, {"victim": "victim", "resume": "resume"})
    assert (replay["before"] == replay["after"]) is matches


def test_pre_resume_service_projection_keeps_every_completed_checkpoint_and_omits_later_work():
    from dataclasses import replace

    from aws_durable_execution_conformance_tests.lmi.source_evidence import _replay_history_before_resume

    observed, _ = history_sources("StepSucceeded", {"Result": '"first"'}, {"status": "SUCCEEDED"})
    observed.context.scenario = replace(observed.context.scenario, scenario="suspension-retry")
    history = observed.cloud.raw_histories["exec"]
    history[0]["EventTimestamp"] = 0.2
    history.extend(
        [
            {
                "Id": "other",
                "EventType": "StepSucceeded",
                "EventTimestamp": 0.4,
                "StepSucceededDetails": {"Result": {"Payload": '"second"'}},
            },
            {
                "EventType": "InvocationCompleted",
                "InvocationCompletedDetails": {"RequestId": "resume", "StartTimestamp": 0.5, "EndTimestamp": 0.9},
            },
            {
                "Id": "after",
                "EventType": "StepSucceeded",
                "EventTimestamp": 0.6,
                "StepSucceededDetails": {"Result": '"after"'},
            },
        ]
    )
    errors: list[str] = []
    before = _replay_history_before_resume(
        observed,
        [{"requestId": "resume", "executionArn": "exec", "startedAtMs": 500}],
        {"resume": "resume"},
        errors,
    )
    assert errors == [] and before is not None
    assert [(op["id"], op["result"], op["resultEncoding"]) for op in before] == [
        ("done", "first", "json"),
        ("other", "second", "json"),
    ]


@pytest.mark.parametrize("payload", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_json_wire_values_remain_raw(payload):
    from aws_durable_execution_conformance_tests.lmi.source_evidence import _decoded, _serialized_encoding

    assert _decoded(payload) == payload
    assert _serialized_encoding(payload) == "raw"


@pytest.mark.parametrize(
    ("service_wire", "sdk_wire", "matches"),
    [
        ("[1e309]", "[ 10e308 ]", True),
        ("[1e5000]", "[ 10e4999 ]", True),
        ("9007199254740993", "9007199254740993.0", True),
        ("10000000000000000.1", "10000000000000000.2", False),
        ('{"value": 1.00}', '{ "value": 1e0 }', True),
    ],
)
def test_checkpoint_preserves_exact_json_numbers(service_wire, sdk_wire, matches):
    from dataclasses import replace

    from aws_durable_execution_conformance_tests.lmi.evidence import same_checkpoint

    observed, events = history_sources(
        "StepSucceeded", {"Result": {"Payload": service_wire}}, {"status": "SUCCEEDED", "serializedResult": sdk_wire}
    )
    observed.context.scenario = replace(observed.context.scenario, scenario="suspension-retry")
    histories, _, errors = observed.histories()
    assert errors == []
    replay = replay_evidence(events, histories, {"victim": "victim", "resume": "resume"})
    before, after = replay["before"][0], replay["after"][0]
    assert before["resultEncoding"] == after["resultEncoding"] == "json"
    assert (before["resultFingerprint"] == after["resultFingerprint"]) is matches
    assert same_checkpoint(before, after) is matches
    if service_wire == "9007199254740993":
        assert before["result"] == after["result"] == 9007199254740993


def test_conflicting_sdk_start_snapshots_do_not_select_the_first():
    observed, events = history_sources(
        "StepSucceeded", {"Result": "null"}, {"status": "SUCCEEDED", "serializedResult": "null"}
    )
    histories, _, errors = observed.histories()
    events.append({**events[-1], "operations": []})
    replay = replay_evidence(events, histories, {"victim": "victim", "resume": "resume"}, errors=errors)
    assert replay["before"] == []
    assert any("multiple invocation-start snapshots" in error for error in errors)


@pytest.mark.parametrize(
    ("original", "tampered"),
    [
        (True, 1),
        ({"key": [True]}, {"key": [1]}),
        ({"key": "value"}, {"key": "different"}),
        ({"key": 1}, {"key": 2}),
    ],
)
def test_matching_fingerprint_does_not_hide_display_value_changes(original, tampered):
    from aws_durable_execution_conformance_tests.lmi.evidence import same_checkpoint

    base = {"id": "checkpoint", "resultEncoding": "json", "resultFingerprint": "a" * 64}
    assert not same_checkpoint({**base, "result": original}, {**base, "result": tampered})


def test_two_named_retries_keep_distinct_operations_and_effects():
    events: list[dict[str, Any]] = [{"kind": "invocation_start", "requestId": "first", "executionArn": "execution"}]
    for name in ("left", "right"):
        for ordinal, outcome in ((1, "FAILED"), (2, "SUCCEEDED")):
            body = {"operationId": name, "operation": "retry", "requestId": "first", "attempt": ordinal}
            events.extend([{**body, "kind": "body"}, {**body, "kind": "task_exit", "outcome": outcome}])
    history = [
        {
            "executionArn": "execution",
            "operations": [{"id": identity, "name": "retry"} for identity in ("left", "right")],
        }
    ]
    replay = replay_evidence(events, history, {"victim": "first"}, expected_effects={"retry": 2})
    assert replay["retry"] == {
        "operationId": "left",
        "attempts": [
            {"attemptId": "first:1", "requestId": "first", "outcome": "FAILED"},
            {"attemptId": "first:2", "requestId": "first", "outcome": "SUCCEEDED"},
        ],
    }
    assert {item["operationId"]: item["count"] for item in replay["sideEffects"]} == {"left": 2, "right": 2}
    assert {item["operationId"] for item in replay["operationExecutions"]} == {"left", "right"}


def test_replay_retry_excludes_operations_already_completed_before_resume():
    operations = [
        {"id": identity, "name": "retry", "type": "STEP", "status": "SUCCEEDED", "result": "value", "parentId": None}
        for identity in ("before", "after")
    ]
    events: list[dict[str, Any]] = [
        {"kind": "invocation_start", "requestId": "first", "executionArn": "execution"},
        {
            "kind": "invocation_start",
            "requestId": "resume",
            "operations": [
                {
                    "operationId": "before",
                    "operationType": "STEP",
                    "status": "SUCCEEDED",
                    "name": "retry",
                    "parentId": None,
                    "serializedResult": '"value"',
                }
            ],
        },
    ]
    for identity, request in (("before", "first"), ("after", "resume")):
        for ordinal, outcome in ((1, "FAILED"), (2, "SUCCEEDED")):
            body = {"operationId": identity, "operation": "retry", "requestId": request, "attempt": ordinal}
            events.extend([{**body, "kind": "body"}, {**body, "kind": "task_exit", "outcome": outcome}])
    result = replay_evidence(
        events,
        [{"executionArn": "execution", "operations": operations}],
        {"victim": "first", "resume": "resume"},
        expected_effects={"retry": 2},
    )
    assert result["retry"]["operationId"] == "after"
    assert {row["operationId"]: row["count"] for row in result["sideEffects"]} == {"before": 2, "after": 2}


@pytest.mark.parametrize("partial", [False, True])
def test_fixture_policy_only_retains_proven_partial_excess(partial):
    events: list[dict[str, Any]] = [
        {"kind": "invocation_start", "requestId": "first", "executionArn": "execution"},
        *(
            {"kind": "body", "requestId": "first", "operationId": identity, "operation": "item", "attempt": 1}
            for identity in ("owner", "owner", "duplicate", ["malformed"])
        ),
    ]
    errors: list[str] = []

    def policy(bodies):
        assert [row["operationId"] for row in bodies] == ["owner", "owner", "duplicate"]
        bodies[0]["operationId"] = "unobserved"
        return {"owner": 1, "duplicate": 0, "unobserved": 1}, ["item:3"]

    replay = replay_evidence(
        events,
        [{"executionArn": "execution", "operations": [{"id": key, "name": "item"} for key in ("owner", "duplicate")]}],
        {"victim": "first"},
        expected_effects={"item": 1},
        effect_policy=policy,
        partial=partial,
        errors=errors,
    )
    assert {item["operationId"]: (item["count"], item["expectedCount"]) for item in replay["sideEffects"]} == {
        "owner": (2, 1),
        "duplicate": (1, 0),
    }
    assert ("item:3" in replay.get("missingExpectedEffects", ())) is not partial
    assert len(errors) == 1 and "invalid request" in errors[0]


@pytest.mark.parametrize("partial", [True, False])
def test_unsealed_raw_body_is_only_checked_after_publication(partial):
    errors: list[str] = []
    replay_evidence(
        [{"kind": "invocation_start", "requestId": "first", "executionArn": "execution"}],
        [],
        {"victim": "first"},
        raw=[{"execution": "execution", "phase": "BODY", "request": "first", "operation": "item", "attempt": 1}],
        partial=partial,
        effect_policy=lambda _: ({}, []),
        errors=errors,
    )
    assert bool(errors) is not partial


def test_replay_policy_exception_is_recorded():
    errors: list[str] = []

    def fail(_):
        raise RuntimeError("fixture policy unavailable")

    replay_evidence([], [], {}, partial=True, effect_policy=fail, errors=errors)
    assert errors == ["Replay effect policy: fixture policy unavailable"]


def test_complete_effect_policy_requires_service_operation():
    errors: list[str] = []
    replay_evidence(
        [
            {"kind": "invocation_start", "requestId": "first", "executionArn": "execution"},
            {"kind": "body", "requestId": "first", "operationId": "missing", "operation": "item", "attempt": 1},
        ],
        [],
        {"victim": "first"},
        effect_policy=lambda _: ({"missing": 1}, []),
        errors=errors,
    )
    assert errors == ["Replay body missing lacks a matching service operation"]


def test_default_replay_rejects_swapped_service_operation_ids():
    events: list[dict[str, Any]] = [
        {"kind": "invocation_start", "requestId": "first", "executionArn": "execution"},
        {"kind": "body", "requestId": "first", "operationId": "failed", "operation": "success", "attempt": 1},
        {"kind": "body", "requestId": "first", "operationId": "succeeded", "operation": "failure", "attempt": 1},
    ]
    histories = [
        {
            "executionArn": "execution",
            "operations": [{"id": "succeeded", "name": "success"}, {"id": "failed", "name": "failure"}],
        }
    ]
    errors: list[str] = []
    replay_evidence(events, histories, {"victim": "first"}, errors=errors)
    assert errors == [
        "Replay body failed lacks a matching service operation",
        "Replay body succeeded lacks a matching service operation",
    ]


@pytest.mark.parametrize(
    ("wire", "expected", "matches"),
    [
        ("9007199254740993.0", 9007199254740993, True),
        ("9007199254740993.0", 9007199254740992, False),
        ("0.10000000000000001", 0.1, False),
        ("0.10000000000000000", 0.1, True),
        ("1e23", 1e23, True),
    ],
)
def test_execution_result_fingerprint_distinguishes_numeric_rounding(wire, expected, matches):
    from dataclasses import replace

    observed, _ = history_sources("StepSucceeded", {}, {"status": "SUCCEEDED"})
    observed.cloud.execution_results["exec"]["Result"] = {"Payload": wire}
    observed = replace(observed, expected_outcome=lambda *_: {"status": "SUCCEEDED", "result": expected})
    histories, expected_histories, errors = observed.histories()
    assert errors == []
    from aws_durable_execution_conformance_tests.lmi.evidence import _same_execution_result

    assert "resultFingerprint" not in expected_histories["exec"]
    assert _same_execution_result(histories[0], expected_histories["exec"]) is matches
    if wire == "9007199254740993.0":
        assert histories[0]["result"] == 9007199254740993
