# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The executable validator checks the structure of complete, extensible evidence."""

import json
from copy import deepcopy
from pathlib import Path
from runpy import run_path
from typing import Any

import pytest

from aws_durable_execution_conformance_tests.lmi.evidence import (
    validate_common_evidence,
    validate_evidence,
    validate_retirement_evidence,
)


@pytest.fixture(scope="module")
def evidence_factories() -> dict[str, Any]:
    return run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))


@pytest.mark.parametrize(
    ("kind", "outcome", "reason"),
    [
        ("invocation", "SUCCEEDED", "invocation quiescence is not established"),
        ("execution", "RUNNING", "an execution may still resume after retirement"),
    ],
)
def test_validator_requires_settled_invocation_and_execution_outcomes(
    evidence_factories: dict[str, Any],
    kind: str,
    outcome: str,
    reason: str,
) -> None:
    value = evidence_factories["evidence"]()
    assert validate_evidence(value) == []
    resource = next(resource for resource in value["retirement"]["resources"] if resource["kind"] == kind)
    resource["outcome"] = outcome
    assert any(reason in error for error in validate_evidence(value))


@pytest.mark.parametrize(
    ("path", "reason"),
    [
        (("runId",), "runId"),
        (("caseId",), "caseId"),
        (("collection", "evidenceUris", 0), "collection.evidenceUris"),
        (("retirement", "resources", 0, "evidenceUri"), "retirement.resources.evidenceUri"),
        (("artifact", "sdk"), "artifact.sdk"),
        (("attempts", 0, "workerId"), "workerId"),
    ],
)
@pytest.mark.parametrize("blank", ["", " ", "\t\n"])
def test_validator_rejects_blank_identity(
    evidence_factories: dict[str, Any],
    path: tuple[str | int, ...],
    reason: str,
    blank: str,
) -> None:
    value = evidence_factories["evidence"]()
    record = value
    for key in path[:-1]:
        record = record[key]
    record[path[-1]] = blank
    assert any(reason in error and "non-empty string" in error for error in validate_evidence(value))


@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize("field", ["admissionMs", "cleanupMs", "recoveryMs"])
def test_deadline_scenarios_require_their_declared_budgets(evidence_factories, requirement, field):
    value = evidence_factories["deadline_evidence" if requirement == 3 else "recovery_evidence"]()
    del value["budgets"][field]
    errors = validate_evidence(value)
    if requirement == 3 and field == "recoveryMs":
        assert errors == [], errors
    else:
        assert any(f"budgets.{field}" in error for error in errors), errors


@pytest.mark.parametrize(
    ("factory", "arguments"),
    [
        ("evidence", (1,)),
        ("evidence", (2,)),
        ("deadline_evidence", ()),
        ("recovery_evidence", ()),
        ("recovery_evidence", ("python",)),
        ("replay_evidence", ()),
        ("warm_evidence", ()),
        ("failure_evidence", ()),
        ("unnamed_context_replay_evidence", ()),
    ],
)
def test_validator_accepts_complete_lifecycle_fixtures(evidence_factories, factory, arguments):
    value = evidence_factories[factory](*arguments)
    assert validate_evidence(value) == []


@pytest.mark.parametrize("retry_count", [0, 1])
def test_builder_reports_missing_retry_as_behavior_evidence(evidence_factories, retry_count):
    import json

    from aws_durable_execution_conformance_tests.lmi.source_evidence import replay_evidence

    value = evidence_factories["replay_evidence"]()
    original = value["replay"]
    victim = value["selectedRequests"]["victim"]
    resume = value["selectedRequests"]["resume"]
    execution = next(item["executionArn"] for item in value["attempts"] if item["requestId"] == victim)
    events = [
        {"kind": "invocation_start", "requestId": victim, "executionArn": execution},
        {
            "kind": "invocation_start",
            "requestId": resume,
            "operations": [
                {
                    "operationId": operation["id"],
                    "operationType": operation["type"],
                    "status": operation["status"],
                    "name": operation["name"],
                    "parentId": operation["parentId"],
                    **(
                        {"serializedResult": json.dumps(operation["result"])}
                        if "result" in operation
                        else {
                            "error": operation["error"],
                        }
                    ),
                }
                for operation in original["before"]
            ],
        },
        *(
            {
                "kind": "body",
                "requestId": victim,
                "operationId": operation["id"],
                "operation": operation["name"],
                "attempt": 1,
            }
            for operation in original["before"]
        ),
    ]
    history = next(item for item in value["histories"] if item["executionArn"] == execution)
    history["operations"] = original["before"]
    if retry_count:
        history["operations"] = [
            *history["operations"],
            {
                "id": "retry",
                "name": "retry",
                "type": "STEP",
                "status": "SUCCEEDED",
                "result": "value",
                "parentId": None,
            },
        ]
        events.extend(
            [
                {"kind": "body", "requestId": victim, "operationId": "retry", "operation": "retry", "attempt": 1},
                {"kind": "task_exit", "requestId": victim, "operationId": "retry", "attempt": 1, "outcome": "FAILED"},
            ]
        )
    expected = {operation["name"]: 1 for operation in original["before"]}
    value["replay"] = replay_evidence(
        events, value["histories"], value["selectedRequests"], expected_effects={**expected, "retry": 2}
    )
    if retry_count:
        assert len(value["replay"]["retry"]["attempts"]) == 1
    else:
        assert "retry" not in value["replay"]
    assert value["replay"]["missingExpectedEffects"] == ["retry"]
    errors = validate_evidence(value)
    assert any(error.startswith("ASSERTION:") for error in errors)
    assert "ASSERTION: replay omitted the expected side-effect operation retry" in errors


def test_validator_preserves_null_payloads_optional_metadata_and_extensions(evidence_factories):
    value = evidence_factories["unnamed_context_replay_evidence"]()
    for operation in value["replay"]["before"]:
        operation["result" if operation["status"] == "SUCCEEDED" else "error"] = None
        operation["adapterMetadata"] = {"unknown": None}
    value["replay"]["after"][: len(value["replay"]["before"])] = deepcopy(value["replay"]["before"])
    value["histories"][0]["operations"] = deepcopy(value["replay"]["after"])
    value["adapterMetadata"] = None
    value["budgets"] = {"adapterMetadata": None}
    value["replay"]["adapterMetadata"] = {"revision": 1}
    value["replay"]["retry"]["adapterMetadata"] = None
    value["replay"]["sideEffects"][0]["adapterMetadata"] = None
    assert validate_evidence(value) == []


def test_validator_checks_side_effect_counts_against_the_retry_policy(evidence_factories):
    value = evidence_factories["replay_evidence"]()
    value["replay"]["sideEffects"][0]["count"] = 2
    assert any("side-effect count differs" in error for error in validate_evidence(value))


def appended_replay_record(evidence_factories, kind):
    value = evidence_factories["replay_evidence"]()
    if kind == "side-effect":
        record = {"operationId": "additional-operation", "count": 1, "expectedCount": 1}
        value["replay"]["sideEffects"].append(record)
        return value, record, "operationId"
    record = {"attemptId": "retry-middle", "requestId": "request-resume", "outcome": "FAILED"}
    value["replay"]["retry"]["attempts"].insert(1, record)
    value["replay"]["operationExecutions"].append({**record, "operationId": "retry"})
    retry_effect = next(item for item in value["replay"]["sideEffects"] if item["operationId"] == "retry")
    retry_effect.update(count=3, expectedCount=3)
    return value, record, "outcome"


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("kind", ["side-effect", "retry"])
@pytest.mark.parametrize(
    ("present", "invalid"),
    [(False, None), (True, None), (True, False), (True, 1), (True, ""), (True, " "), (True, []), (True, {})],
)
def test_additional_replay_records_require_declared_string_fields(evidence_factories, phase, kind, present, invalid):
    value, record, field = appended_replay_record(evidence_factories, kind)
    if present:
        record[field] = invalid
    else:
        del record[field]

    assert validate_evidence(value, phase=phase)


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize(
    ("kind", "valid"),
    [
        ("side-effect", "additional-operation"),
        ("side-effect", " another-operation "),
        ("retry", "FAILED"),
        ("retry", "TIMED_OUT"),
    ],
)
def test_additional_replay_records_preserve_nonempty_values_without_new_enums(evidence_factories, phase, kind, valid):
    value, record, field = appended_replay_record(evidence_factories, kind)
    record[field] = valid

    assert validate_evidence(value, phase=phase) == []


ABSENT = object()


@pytest.mark.parametrize("field", ["probe", "outcome"])
@pytest.mark.parametrize("invalid", [ABSENT, None, "UNKNOWN"])
def test_admission_probe_requires_recognized_fields(evidence_factories, field, invalid):
    value = evidence_factories["evidence"](2)
    probe = next(event for event in value["events"] if event["kind"] == "admission_probe")
    if invalid is ABSENT:
        del probe[field]
    else:
        probe[field] = invalid

    for phase in ("observations", "final"):
        assert validate_evidence(value, phase=phase)
        assert validate_common_evidence(value, phase=phase)
        assert validate_common_evidence(value, phase=phase, requirement_ids=(value["requirementId"],))

    evidence_factories["complete_retirement_events"](value)
    assert validate_retirement_evidence(value) == []


def test_adapter_events_can_supply_their_own_probe_fields(evidence_factories):
    value = evidence_factories["evidence"](2)
    probe = next(event for event in value["events"] if event["kind"] == "admission_probe")
    value["events"].append({**probe, "kind": "adapter_probe", "probe": "adapter-specific", "outcome": None})
    assert validate_evidence(value) == []


def ordinary_evidence_with_optional_fields(evidence_factories, factory):
    value = evidence_factories[factory]() if factory == "warm_evidence" else evidence_factories["evidence"](2)
    value["scenario"] = "adapter-specific-description"
    value["coverageProfile"] = "standard"
    value["budgets"] = deepcopy(evidence_factories["deadline_evidence"]()["budgets"])
    value["replay"] = deepcopy(evidence_factories["replay_evidence"]()["replay"])
    return value


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("factory", ["evidence", "warm_evidence"])
@pytest.mark.parametrize(
    ("path", "invalid"),
    [
        (("replay",), None),
        (("replay",), []),
        (("budgets",), None),
        (("profile", "recoveryPolicy"), False),
        (("profile", "recoveryPolicy"), "unknown"),
        (("scenario",), False),
        (("scenario",), ""),
        (("coverageProfile",), False),
        (("coverageProfile",), ""),
        (("budgets", "admissionMs"), True),
        (("budgets", "cleanupMs"), -1),
        (("budgets", "recoveryMs"), "1"),
        (("budgets", "observationGraceMs"), None),
        (("replay", "before"), None),
        (("replay", "before"), []),
        (("replay", "after"), None),
        (("replay", "after"), []),
        (("replay", "before", 0), None),
        (("replay", "before", 0, "id"), ABSENT),
        (("replay", "before", 0, "name"), False),
        (("replay", "before", 0, "type"), 1),
        (("replay", "before", 0, "status"), None),
        (("replay", "before", 0, "parentId"), False),
        (("replay", "before", 0, "result"), ABSENT),
        (("replay", "before", 1, "error"), ABSENT),
        (("replay", "before", 0, "startedAtMs"), False),
        (("replay", "after", 0, "id"), ABSENT),
        (("replay", "after", 0, "name"), ABSENT),
        (("replay", "after", 0, "parentId"), []),
        (("replay", "after", 0, "endedAtMs"), -1),
        (("replay", "operationExecutions"), None),
        (("replay", "operationExecutions"), []),
        (("replay", "operationExecutions", 0, "requestId"), ABSENT),
        (("replay", "operationExecutions", 0, "operationId"), False),
        (("replay", "retry"), None),
        (("replay", "retry"), {}),
        (("replay", "retry", "operationId"), None),
        (("replay", "retry", "attempts"), None),
        (("replay", "retry", "attempts"), []),
        (("replay", "retry", "attempts"), [{"attemptId": "one", "requestId": "request-victim", "outcome": "FAILED"}]),
        (("replay", "retry", "attempts", 0, "attemptId"), ABSENT),
        (("replay", "retry", "attempts", 0, "requestId"), 1),
        (("replay", "retry", "attempts", 0, "outcome"), False),
        (("replay", "sideEffects"), None),
        (("replay", "sideEffects"), []),
        (("replay", "sideEffects", 0, "operationId"), ABSENT),
        (("replay", "sideEffects", 0, "count"), True),
        (("replay", "sideEffects", 0, "expectedCount"), -1),
    ],
)
def test_supplied_optional_wire_fields_have_their_declared_structure_in_other_scenarios(
    evidence_factories, phase, factory, path, invalid
):
    value = ordinary_evidence_with_optional_fields(evidence_factories, factory)
    target = value
    for key in path[:-1]:
        target = target[key]
    if invalid is ABSENT:
        del target[path[-1]]
    else:
        target[path[-1]] = deepcopy(invalid)

    assert validate_evidence(value, phase=phase)
    assert validate_common_evidence(value, phase=phase)


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("factory", ["evidence", "warm_evidence"])
@pytest.mark.parametrize("replay_shape", ["absent", "empty", "extension", "complete"])
def test_optional_wire_fields_do_not_activate_other_scenarios_or_restrict_extensions(
    evidence_factories, phase, factory, replay_shape
):
    value = ordinary_evidence_with_optional_fields(evidence_factories, factory)
    if replay_shape == "absent":
        del value["replay"]
        del value["budgets"]
        del value["scenario"]
        del value["coverageProfile"]
        del value["profile"]["recoveryPolicy"]
    elif replay_shape in {"empty", "extension"}:
        value["replay"] = {} if replay_shape == "empty" else {"adapter": {"opaque": None}}
        value["budgets"] = {"adapter": False}
    else:
        # These fields are structurally valid; their cross-record semantics are
        # specific to replay and must not become requirements of this scenario.
        value["replay"]["after"][0]["result"] = "different"
        value["replay"]["before"][0]["name"] = None
        value["replay"]["before"][0]["result"] = None
        value["replay"]["retry"]["attempts"][0]["outcome"] = "OTHER"
        value["replay"]["sideEffects"][0]["count"] = 2
        value["replay"]["operationExecutions"][0]["attemptId"] = {"opaque": None}
        value["budgets"]["recoveryMs"] = 0
        value["profile"]["recoveryPolicy"] = "replace-worker"
    value["adapter"] = [None, False]

    assert validate_evidence(value, phase=phase) == []
    assert validate_common_evidence(value, phase=phase) == []


@pytest.mark.parametrize("phase", ["observations", "final"])
def test_common_validation_preserves_caller_owned_profile_and_requirement_names(evidence_factories, phase):
    value = ordinary_evidence_with_optional_fields(evidence_factories, "evidence")
    value.update(requirementId="sdk-check", coverageProfile="sdk-regression")
    assert validate_common_evidence(value, phase=phase, requirement_ids=("sdk-check",)) == []
    value["requirementId"] = "lmi-lifecycle-2"
    assert validate_common_evidence(value, phase=phase) == []
    assert validate_evidence(value, phase=phase)


@pytest.mark.parametrize("partial", [ABSENT, None, {}, {"resources": []}])
def test_optional_wire_checks_do_not_require_final_retirement_during_observations(evidence_factories, partial):
    value = ordinary_evidence_with_optional_fields(evidence_factories, "evidence")
    if partial is ABSENT:
        del value["retirement"]
    else:
        value["retirement"] = deepcopy(partial)
    assert validate_evidence(value, phase="observations") == []
    assert validate_common_evidence(value, phase="observations") == []
    assert validate_evidence(value)


def test_physical_retirement_preserves_partial_behavior_diagnostics(evidence_factories):
    value = evidence_factories["evidence"](2)
    value["replay"] = {"retry": {"attempts": [{"attemptId": "partial"}]}}
    value["budgets"] = {"observationGraceMs": None}
    evidence_factories["complete_retirement_events"](value)
    assert validate_retirement_evidence(value) == []


@pytest.mark.parametrize("factory", ["deadline_evidence", "recovery_evidence", "replay_evidence"])
def test_common_validation_does_not_require_scenario_specific_optional_sections(evidence_factories, factory):
    value = evidence_factories[factory]()
    del value["replay" if factory == "replay_evidence" else "budgets"]
    assert validate_common_evidence(value) == []
    assert validate_evidence(value)


INTEGER_WIRE_PATHS = (
    ("schemaVersion",),
    ("profile", "nativeConcurrency"),
    ("attempts", 0, "attempt"),
    ("replay", "sideEffects", 0, "count"),
    ("replay", "sideEffects", 0, "expectedCount"),
)


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("encoding", [".0", "e0"])
@pytest.mark.parametrize("path", INTEGER_WIRE_PATHS)
def test_integral_json_number_encodings_are_accepted_for_integer_wire_fields(evidence_factories, phase, encoding, path):
    value = evidence_factories["replay_evidence"]()
    target = value
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = json.loads(f"{target[path[-1]]}{encoding}")
    assert type(target[path[-1]]) is float

    assert validate_evidence(value, phase=phase) == []
    assert validate_common_evidence(value, phase=phase) == []


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("path", INTEGER_WIRE_PATHS)
@pytest.mark.parametrize("invalid", [True, False, 1.5, float("inf"), float("nan")])
def test_integer_wire_fields_still_reject_booleans_fractions_and_nonfinite_values(
    evidence_factories, phase, path, invalid
):
    value = evidence_factories["replay_evidence"]()
    target = value
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = invalid
    assert validate_evidence(value, phase=phase)


def test_large_integer_wire_values_do_not_require_float_conversion(evidence_factories):
    value = evidence_factories["replay_evidence"]()
    large = 10**1000
    value["profile"]["nativeConcurrency"] = large
    value["replay"]["sideEffects"][0].update(count=large, expectedCount=large)
    assert validate_evidence(value) == []


@pytest.mark.parametrize("encoding", [".0", "e0"])
def test_physical_retirement_accepts_integral_json_sequence_numbers(evidence_factories, encoding):
    value = evidence_factories["evidence"]()
    evidence_factories["complete_retirement_events"](value)
    for event in value["events"]:
        event["sequence"] = json.loads(f"{event['sequence']}{encoding}")
    assert validate_retirement_evidence(value) == []


@pytest.mark.parametrize("invalid", [True, 0.5, float("inf"), float("nan")])
def test_physical_retirement_rejects_noninteger_sequence_numbers(evidence_factories, invalid):
    value = evidence_factories["evidence"]()
    evidence_factories["complete_retirement_events"](value)
    value["events"][0]["sequence"] = invalid
    assert any("sequence" in error for error in validate_retirement_evidence(value))


def test_active_attempt_is_only_an_observation_of_a_deadline_victim_retry(evidence_factories):
    from copy import deepcopy

    from aws_durable_execution_conformance_tests.lmi.evidence import validate_retirement_evidence

    value = evidence_factories["deadline_evidence"]()
    active = {
        **value["attempts"][0],
        "requestId": "in-flight",
        "attempt": 2,
        "startedAtMs": 100,
        "activeAtObservationEnd": True,
    }
    active.pop("endedAtMs")
    active.pop("outcome")
    value["attempts"].append(active)
    assert validate_evidence(value, phase="observations") == []
    assert any("independent retirement audit" in error for error in validate_evidence(value, phase="final"))
    assert any("cannot represent an active retry" in error for error in validate_retirement_evidence(value))
    for invalid in ({"attempt": 1}, {"role": "peer"}, {"endedAtMs": 150}, {"outcome": "PENDING"}):
        malformed = deepcopy(value)
        malformed["attempts"][-1].update(invalid)
        assert any(
            "cannot represent an active retry" in error for error in validate_evidence(malformed, phase="observations")
        )

    for invalid in ({"startedAtMs": 201}, {"executionArn": "another-execution"}):
        malformed = deepcopy(value)
        malformed["attempts"][-1].update(invalid)
        assert validate_evidence(malformed, phase="observations")


def test_complete_service_snapshot_detects_a_missing_pre_resume_operation(evidence_factories):
    value = evidence_factories["replay_evidence"]()
    before = value["replay"]["before"]
    extra = dict(before[0], id="missing", name="second-completed-success")
    value["replay"]["expectedBefore"] = [*before, extra]
    assert any("complete pre-resume service history" in error for error in validate_evidence(value))


def test_complete_wait_replay_that_only_suspended_for_backoff_is_a_behavior_failure(evidence_factories):
    value = evidence_factories["replay_evidence"]()
    value["replay"]["durableWaitObserved"] = False
    assert validate_evidence(value) == ["ASSERTION: replay did not verify a durable wait across suspension and resume"]


def test_duplicate_expected_checkpoint_ids_never_pass(evidence_factories):
    value = evidence_factories["replay_evidence"]()
    before = value["replay"]["before"]
    conflicting = dict(before[0], result="conflicting")
    for expected in ([conflicting, *before], [*before, conflicting]):
        value["replay"]["expectedBefore"] = expected
        assert any("duplicate operation IDs" in error for error in validate_evidence(value))


def test_resultless_context_preserves_absence_in_replay(evidence_factories):
    value = evidence_factories["replay_evidence"]()
    before = value["replay"]["before"]
    context = {"id": "ctx", "name": "context", "type": "CONTEXT", "parentId": None, "status": "SUCCEEDED"}
    before.append(context)
    value["replay"]["expectedBefore"] = list(before)
    value["replay"]["after"].append(dict(context))
    victim = value["selectedRequests"]["victim"]
    execution = next(item["executionArn"] for item in value["attempts"] if item["requestId"] == victim)
    history = next(row for row in value["histories"] if row["executionArn"] == execution)
    history["operations"] = value["replay"]["after"]
    value["replay"]["sideEffects"].append({"operationId": "ctx", "count": 0, "expectedCount": 0})
    assert validate_evidence(value) == []
    value["replay"]["before"][-1] = {**context, "result": None}
    assert validate_evidence(value)


def test_result_encoding_without_result_fails_validation(evidence_factories):
    value = evidence_factories["replay_evidence"]()
    context = {
        "id": "ctx",
        "name": "context",
        "type": "CONTEXT",
        "status": "SUCCEEDED",
        "parentId": None,
        "resultEncoding": "json",
    }
    value["replay"]["before"].append(context)
    assert any("resultEncoding requires a stored result" in error for error in validate_evidence(value))


def test_saved_projection_error_fails_behavior_without_reclassifying_retirement(evidence_factories):
    value = evidence_factories["replay_evidence"]()
    value["replay"]["projectionErrors"] = ["Service operation has no valid event timestamp"]
    assert any(error.startswith("INCOMPLETE: replay service projection") for error in validate_evidence(value))


@pytest.mark.parametrize("status", ["PAUSING", "PAUSED"])
def test_pause_status_is_valid_frozen_history_without_terminal_result(status, evidence_factories):
    value = evidence_factories["deadline_evidence"]()
    victim = value["attempts"][0]["executionArn"]
    history = next(item for item in value["histories"] if item["executionArn"] == victim)
    history["status"] = status
    history.pop("result")
    value["expectedHistories"][victim] = {"allowedStatuses": [status]}
    assert validate_evidence(value, phase="observations") == []


def test_deferred_request_shape_is_validated(evidence_factories):
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_retirement_evidence

    value = evidence_factories["deadline_evidence"]()
    arn = value["attempts"][0]["executionArn"]
    value["collection"]["deferredServiceRequests"] = [{"requestId": "future", "executionArn": arn}]
    assert not validate_evidence(value, phase="observations")
    assert any("settle every deferred service request" in error for error in validate_retirement_evidence(value))
    value["collection"]["deferredServiceRequests"] = {}
    assert validate_evidence(value, phase="observations")


def test_unverified_revision_is_not_behavior_evidence(evidence_factories):
    value = evidence_factories["deadline_evidence"]()
    value["artifact"]["conformance"] = "unverified"
    assert any(
        "conformance revision is unverified" in error for error in validate_evidence(value, phase="observations")
    )


@pytest.mark.parametrize("field", ["histories", "expectedHistories"])
@pytest.mark.parametrize("digest", ["invalid", "a" * 64])
def test_execution_history_fingerprint_shape(evidence_factories, field, digest):
    value = evidence_factories["evidence"](2)
    record = value["histories"][0] if field == "histories" else next(iter(value["expectedHistories"].values()))
    record["resultFingerprint"] = digest
    shape_errors = [error for error in validate_evidence(value) if "resultFingerprint requires" in error]
    assert bool(shape_errors) is (len(digest) != 64), shape_errors


@pytest.mark.parametrize("kind", ["checkpoint_call", "poll_call"])
@pytest.mark.parametrize("extra", [{}, {"callId": " "}, {"callId": "call", "outcome": "UNKNOWN"}])
def test_validator_rejects_malformed_sdk_call_event(
    evidence_factories: dict[str, Any], kind: str, extra: dict[str, str]
) -> None:
    value = evidence_factories["evidence"](2)
    value["events"].append(evidence_factories["event"](value["attempts"][0], kind, 26, **extra))
    assert any("observed SDK call" in error for error in validate_evidence(value))


@pytest.mark.parametrize("parent", ["", " "])
def test_validator_rejects_blank_operation_parent(evidence_factories: dict[str, Any], parent: str) -> None:
    value = evidence_factories["replay_evidence"]()
    for operations in (value["replay"]["before"], value["replay"]["after"], value["histories"][0]["operations"]):
        operations[0]["parentId"] = parent
    assert any("parentId must be a non-empty string" in error for error in validate_evidence(value))
