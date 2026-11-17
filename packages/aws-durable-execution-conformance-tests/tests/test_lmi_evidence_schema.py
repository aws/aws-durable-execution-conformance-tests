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

from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence


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
