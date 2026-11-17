# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The published wire schema validates complete, extensible evidence structures."""

import json
from copy import deepcopy
from importlib.resources import files
from pathlib import Path
from runpy import run_path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from aws_durable_execution_conformance_tests.lmi.evidence import (
    validate_common_evidence,
    validate_evidence,
    validate_retirement_evidence,
)


@pytest.fixture(scope="module")
def schema_validator() -> Draft202012Validator:
    resource = files("aws_durable_execution_conformance_tests.lmi").joinpath("evidence-schema.json")
    schema = json.loads(resource.read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


@pytest.fixture(scope="module")
def evidence_factories() -> dict[str, Any]:
    return run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))


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
def test_schema_accepts_complete_lifecycle_fixtures(schema_validator, evidence_factories, factory, arguments):
    value = evidence_factories[factory](*arguments)
    assert validate_evidence(value) == []
    schema_validator.validate(value)


@pytest.mark.parametrize("field", ["before", "after", "operationExecutions", "retry", "sideEffects"])
@pytest.mark.parametrize("mutation", ["missing", "empty", "wrong-type"])
def test_replay_requires_complete_nonempty_structures(schema_validator, evidence_factories, field, mutation):
    value = evidence_factories["replay_evidence"]()
    if mutation == "missing":
        del value["replay"][field]
    else:
        value["replay"][field] = ({} if field == "retry" else []) if mutation == "empty" else None
    assert not schema_validator.is_valid(value)


def test_replay_requirement_requires_the_replay_object(schema_validator, evidence_factories):
    value = evidence_factories["replay_evidence"]()
    del value["replay"]
    assert not schema_validator.is_valid(value)


@pytest.mark.parametrize(
    "path",
    [
        ("before", 0, "id"),
        ("before", 0, "name"),
        ("before", 0, "type"),
        ("before", 0, "status"),
        ("before", 0, "parentId"),
        ("before", 0, "result"),
        ("before", 1, "error"),
        ("after", 0, "id"),
        ("after", 0, "parentId"),
        ("retry", "operationId"),
        ("retry", "attempts"),
        ("retry", "attempts", 0, "attemptId"),
        ("retry", "attempts", 0, "requestId"),
        ("retry", "attempts", 0, "outcome"),
        ("sideEffects", 0, "operationId"),
        ("sideEffects", 0, "count"),
        ("sideEffects", 0, "expectedCount"),
    ],
)
def test_replay_records_require_their_structural_fields(schema_validator, evidence_factories, path):
    value = evidence_factories["replay_evidence"]()
    record = value["replay"]
    for part in path[:-1]:
        record = record[part]
    del record[path[-1]]
    assert not schema_validator.is_valid(value)


@pytest.mark.parametrize("count", [0, 1])
def test_retry_requires_multiple_attempt_records(schema_validator, evidence_factories, count):
    value = evidence_factories["replay_evidence"]()
    value["replay"]["retry"]["attempts"] = value["replay"]["retry"]["attempts"][:count]
    assert not schema_validator.is_valid(value)


@pytest.mark.parametrize("field", ["count", "expectedCount"])
@pytest.mark.parametrize("invalid", [None, True, -1, 0.5, "1"])
def test_side_effect_counts_are_nonnegative_integers(schema_validator, evidence_factories, field, invalid):
    value = evidence_factories["replay_evidence"]()
    value["replay"]["sideEffects"][0][field] = invalid
    assert not schema_validator.is_valid(value)


@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize("field", [None, "admissionMs", "cleanupMs", "observationGraceMs"])
def test_deadline_scenarios_require_declared_budgets(schema_validator, evidence_factories, requirement, field):
    factory = "deadline_evidence" if requirement == 3 else "recovery_evidence"
    value = evidence_factories[factory]()
    if field is None:
        del value["budgets"]
    else:
        del value["budgets"][field]
    assert not schema_validator.is_valid(value)


def test_recovery_budget_is_required_only_for_recovery(schema_validator, evidence_factories):
    for factory, valid in (("deadline_evidence", True), ("recovery_evidence", False)):
        value = evidence_factories[factory]()
        del value["budgets"]["recoveryMs"]
        assert schema_validator.is_valid(value) is valid


@pytest.mark.parametrize("field", ["admissionMs", "cleanupMs", "recoveryMs", "observationGraceMs"])
@pytest.mark.parametrize("invalid", [None, True, -1, "1"])
def test_declared_budget_values_are_nonnegative_numbers(schema_validator, evidence_factories, field, invalid):
    value = evidence_factories["recovery_evidence"]()
    value["budgets"][field] = invalid
    assert not schema_validator.is_valid(value)


def test_zero_evidence_budgets_are_structurally_valid(schema_validator, evidence_factories):
    # Positive manifest declarations are checked separately by load_manifest.
    value = evidence_factories["recovery_evidence"]()
    value["budgets"] = dict.fromkeys(value["budgets"], 0)
    schema_validator.validate(value)


def test_schema_preserves_null_payloads_optional_metadata_and_extensions(schema_validator, evidence_factories):
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
    schema_validator.validate(value)


def test_schema_leaves_cross_record_semantics_to_the_executable_validator(schema_validator, evidence_factories):
    value = evidence_factories["replay_evidence"]()
    value["replay"]["sideEffects"][0]["count"] = 2
    schema_validator.validate(value)
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
def test_additional_replay_records_require_declared_string_fields(
    schema_validator, evidence_factories, phase, kind, present, invalid
):
    value, record, field = appended_replay_record(evidence_factories, kind)
    if present:
        record[field] = invalid
    else:
        del record[field]

    assert not schema_validator.is_valid(value)
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
def test_additional_replay_records_preserve_nonempty_values_without_new_enums(
    schema_validator, evidence_factories, phase, kind, valid
):
    value, record, field = appended_replay_record(evidence_factories, kind)
    record[field] = valid

    schema_validator.validate(value)
    assert validate_evidence(value, phase=phase) == []


ABSENT = object()


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
    schema_validator, evidence_factories, phase, factory, path, invalid
):
    value = ordinary_evidence_with_optional_fields(evidence_factories, factory)
    target = value
    for key in path[:-1]:
        target = target[key]
    if invalid is ABSENT:
        del target[path[-1]]
    else:
        target[path[-1]] = deepcopy(invalid)

    assert not schema_validator.is_valid(value)
    assert validate_evidence(value, phase=phase)
    assert validate_common_evidence(value, phase=phase)


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("factory", ["evidence", "warm_evidence"])
@pytest.mark.parametrize("replay_shape", ["absent", "empty", "extension", "complete"])
def test_optional_wire_fields_do_not_activate_other_scenarios_or_restrict_extensions(
    schema_validator, evidence_factories, phase, factory, replay_shape
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

    schema_validator.validate(value)
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
def test_integral_json_number_encodings_match_the_integer_wire_schema(
    schema_validator, evidence_factories, phase, encoding, path
):
    value = evidence_factories["replay_evidence"]()
    target = value
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = json.loads(f"{target[path[-1]]}{encoding}")
    assert type(target[path[-1]]) is float

    schema_validator.validate(value)
    assert validate_evidence(value, phase=phase) == []
    assert validate_common_evidence(value, phase=phase) == []


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("path", INTEGER_WIRE_PATHS)
@pytest.mark.parametrize("invalid", [True, False, 1.5, float("inf"), float("nan")])
def test_integer_wire_fields_still_reject_booleans_fractions_and_nonfinite_values(
    schema_validator, evidence_factories, phase, path, invalid
):
    value = evidence_factories["replay_evidence"]()
    target = value
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = invalid
    assert not schema_validator.is_valid(value)
    assert validate_evidence(value, phase=phase)


def test_large_integer_wire_values_do_not_require_float_conversion(schema_validator, evidence_factories):
    value = evidence_factories["replay_evidence"]()
    large = 10**1000
    value["profile"]["nativeConcurrency"] = large
    value["replay"]["sideEffects"][0].update(count=large, expectedCount=large)
    schema_validator.validate(value)
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
