# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Packaged OTel requirement resource tests."""

import json
import re
from copy import deepcopy
from pathlib import Path

import pytest

from aws_durable_execution_conformance_tests.validate import (
    discover_test_files,
    load_yaml_file,
)
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"


def _requirements(suite_name: str) -> dict[str, str]:
    suites = {suite.name: suite for suite in OtelExtension().requirement_suites()}
    return discover_test_files(suites[suite_name].root, suite="all")


@pytest.mark.parametrize("language", ["java", "python", "typescript"])
def test_example_templates_do_not_use_top_level_testing_metadata(language: str) -> None:
    template = (EXAMPLES_DIR / language / "template.yaml").read_text(encoding="utf-8")

    assert all(not line.startswith("TestingMetadata:") for line in template.splitlines())


def test_extension_exposes_packaged_otel_view_requirements() -> None:
    suites = {suite.name: suite for suite in OtelExtension().requirement_suites()}

    assert set(suites) == {
        "otel-execution",
        "otel-invocation",
        "otel-long-running",
    }
    assert set(_requirements("otel-invocation")) == {f"otel-invocation-{case_number}" for case_number in range(1, 20)}
    assert set(_requirements("otel-execution")) == {f"otel-execution-{case_number}" for case_number in range(1, 20)}
    assert set(_requirements("otel-long-running")) == {
        f"otel-long-running-{case_number}" for case_number in range(1, 4)
    }


def test_long_running_catalog_uses_configurable_delays() -> None:
    requirements = _requirements("otel-long-running")

    for case_number in range(1, 4):
        requirement = load_yaml_file(requirements[f"otel-long-running-{case_number}"])

        assert requirement["Variables"] == {"LONG_DELAY_SECONDS": 1}
        assert requirement["Input"]["delay_seconds"] == "${LONG_DELAY_SECONDS}"
        assert requirement["AsyncInvoke"] is True
        assert requirement["ExpectedResult"]["ExecutionStatus"] == "SUCCEEDED"
        assert requirement["TelemetryAssertions"]["minimum_invocations"] == 2


@pytest.mark.parametrize("case_number", range(1, 20))
def test_views_share_scenarios_but_define_distinct_telemetry_assertions(case_number: int) -> None:
    invocation = load_yaml_file(_requirements("otel-invocation")[f"otel-invocation-{case_number}"])
    execution = load_yaml_file(_requirements("otel-execution")[f"otel-execution-{case_number}"])

    comparable_invocation = deepcopy(invocation)
    comparable_execution = deepcopy(execution)
    for requirement in (comparable_invocation, comparable_execution):
        requirement.pop("description")
        requirement.pop("TelemetryAssertions")

    assert comparable_invocation == comparable_execution
    assert invocation["TelemetryAssertions"] != execution["TelemetryAssertions"]


def test_invocation_view_catalog_exercises_span_hierarchy_assertions() -> None:
    requirements = _requirements("otel-invocation")

    for case_number in range(1, 20):
        requirement = load_yaml_file(requirements[f"otel-invocation-{case_number}"])
        assertions = requirement["TelemetryAssertions"]

        assert assertions["require_execution_correlation"] is True
        assert assertions["require_all_spans"] is True
        expected_scopes = [
            {"attributes": {"durable.execution.arn": "${EXECUTION_ARN}"}},
        ]
        if case_number in {11, 18}:
            expected_scopes.append(
                {"attributes": {"durable.execution.arn": "${TARGET_EXECUTION_ARN}"}},
            )
            assert assertions["allowed_execution_arns"] == [
                "${EXECUTION_ARN}",
                "${TARGET_EXECUTION_ARN}",
            ]
        actual_scopes = assertions["span_assertion_scope"]
        if isinstance(actual_scopes, dict):
            actual_scopes = [actual_scopes]
        assert actual_scopes == expected_scopes
        assert assertions["exact_attribute_prefixes"] == ["durable."]
        assert assertions["span_assertions"]
        for span_assertion in assertions["span_assertions"]:
            assert "count" not in span_assertion
            selected_name = span_assertion["select"]["name"]
            expected = span_assertion["expect"]
            assert "name" not in expected
            assert expected["status"] in {
                "ERROR",
                "OK",
                "UNSET",
            }
            assert expected["service_name"] == "invocation"
            links = expected["links"]
            if isinstance(links, dict):
                assert set(links) == {"$any_of"}
                assert links["$any_of"][0] == []
                links = links["$any_of"][1]
            assert isinstance(links, list)
            assert len(links) <= 1
            if links:
                linked_span = links[0]
                assert linked_span["name"]
                assert linked_span["attributes"]["durable.operation.id"]
                assert "trace_id" not in linked_span
                assert "span_id" not in linked_span
            assert expected["kind"] == "INTERNAL"
            assert "span.name" not in expected["attributes"]
            assert "span.kind" not in expected["attributes"]
            expected_attributes = expected["attributes"]
            if "durable.attempt.outcome" in expected_attributes:
                assert expected["parent"] == {
                    "name": expected_attributes["durable.operation.name"],
                    "kind": "INTERNAL",
                    "attributes": {
                        "durable.operation.id": expected_attributes["durable.operation.id"],
                        "durable.operation.type": expected_attributes["durable.operation.type"],
                        "durable.operation.subtype": expected_attributes["durable.operation.subtype"],
                    },
                }
            if selected_name == "invocation":
                selector_attributes = span_assertion["select"]["attributes"]
                assert isinstance(expected_attributes["durable.invocation.first"], bool)
                assert expected_attributes["durable.invocation.status"] in {
                    "FAILED",
                    "PENDING",
                    "RETRY",
                    "SUCCEEDED",
                }
                assert (
                    expected["status"]
                    == {
                        "FAILED": "ERROR",
                        "PENDING": "UNSET",
                        "RETRY": "UNSET",
                        "SUCCEEDED": "OK",
                    }[expected_attributes["durable.invocation.status"]]
                )
                assert (
                    selector_attributes["durable.invocation.first"] == expected_attributes["durable.invocation.first"]
                )
                assert (
                    selector_attributes["durable.invocation.status"] == expected_attributes["durable.invocation.status"]
                )
            if parent := expected.get("parent"):
                assert parent["kind"] == "INTERNAL"
                assert "span.name" not in parent["attributes"]
                assert "span.kind" not in parent["attributes"]

        telemetry_json = json.dumps(assertions)
        history_json = json.dumps(requirement["ExpectedExecutionHistory"])
        telemetry_placeholders = set(re.findall(r"\$\{([A-Z0-9_]+)\}", telemetry_json))
        history_placeholders = set(re.findall(r"\$\{([A-Z0-9_]+)\}", history_json))

        assert "${/^(?:OK|UNSET)$/}" not in telemetry_json
        assert '"*"' not in telemetry_json
        assert telemetry_placeholders <= history_placeholders | {"EXECUTION_ARN"}


def test_execution_view_catalog_asserts_workflow_parentage_and_invocation_links() -> None:
    execution_requirements = _requirements("otel-execution")
    invocation_requirements = _requirements("otel-invocation")

    for case_number in range(1, 20):
        requirement = load_yaml_file(execution_requirements[f"otel-execution-{case_number}"])
        invocation_requirement = load_yaml_file(
            invocation_requirements[f"otel-invocation-{case_number}"],
        )
        assertions = requirement["TelemetryAssertions"]
        span_assertions = assertions["span_assertions"]
        assert all("count" not in assertion for assertion in span_assertions)
        workflows = [item for item in span_assertions if item["select"]["name"] == "Workflow"]
        expected_workflow_statuses = {
            "${EXECUTION_ARN}": requirement["ExpectedResult"]["ExecutionStatus"],
        }
        if case_number in {11, 18}:
            expected_workflow_statuses["${TARGET_EXECUTION_ARN}"] = "SUCCEEDED" if case_number == 11 else "FAILED"
            assert assertions["allowed_execution_arns"] == [
                "${EXECUTION_ARN}",
                "${TARGET_EXECUTION_ARN}",
            ]

        assert assertions["require_execution_correlation"] is True
        assert "require_all_spans" not in assertions
        assert "exact_attribute_prefixes" not in assertions
        assert len(workflows) == len(expected_workflow_statuses)
        for workflow in workflows:
            execution_arn = workflow["select"]["attributes"]["durable.execution.arn"]
            execution_status = expected_workflow_statuses[execution_arn]
            expected_span_status = (
                "UNSET"
                if case_number in {15, 19}
                else {
                    "SUCCEEDED": "OK",
                    "FAILED": "ERROR",
                }[execution_status]
            )
            assert workflow["select"] == {
                "name": "Workflow",
                "status": expected_span_status,
                "attributes": {
                    "durable.execution.arn": execution_arn,
                },
            }
            assert workflow["expect"]["parent_span_id"] is None
            assert workflow["expect"]["status"] == expected_span_status
            if case_number in {15, 19}:
                assert workflow["expect"]["attributes"] == {
                    "durable.execution.arn": execution_arn,
                }
            else:
                assert workflow["expect"]["attributes"] == {
                    "durable.execution.arn": execution_arn,
                    "durable.execution.status": execution_status,
                }

        invocations = [item for item in span_assertions if item["select"]["name"] == "Invocation"]
        expected_invocations = [
            item
            for item in invocation_requirement["TelemetryAssertions"]["span_assertions"]
            if item["select"]["name"] == "invocation"
        ]
        assert len(invocations) == len(expected_invocations)
        for invocation, expected_invocation in zip(invocations, expected_invocations, strict=True):
            expected_attributes = expected_invocation["expect"]["attributes"]
            invocation_status = expected_attributes["durable.invocation.status"]
            assert invocation["select"] == {
                "name": "Invocation",
                "attributes": expected_attributes,
            }
            expected = invocation["expect"]
            assert expected["status"] == ("ERROR" if invocation_status == "FAILED" else "UNSET")
            assert expected["links"] == []
            assert expected["kind"] == "INTERNAL"
            assert expected["service_name"] == "invocation"
            assert expected["attributes"] == expected_attributes
            assert invocation["select"]["attributes"] == expected["attributes"]
            for relation in ("before", "after"):
                expected_relation = expected_invocation["expect"].get(relation)
                if expected_relation:
                    expected_relation = deepcopy(expected_relation)
                    expected_relation["name"] = "Invocation"
                    assert expected[relation] == expected_relation
                else:
                    assert relation not in expected

        descendants = [item for item in span_assertions if item not in workflows and item not in invocations]
        assert bool(descendants) is (case_number != 19)
        for descendant in descendants:
            expected = descendant["expect"]
            assert expected["kind"] == "INTERNAL"
            assert expected["parent"]
            assert expected["links"] == [{"name": "invocation"}]
            if "durable.attempt.outcome" in expected["attributes"]:
                assert expected["inside"] == {
                    "$linked": True,
                    "name": "invocation",
                }
            else:
                assert "inside" not in expected

        telemetry_json = json.dumps(assertions)
        history_json = json.dumps(requirement["ExpectedExecutionHistory"])
        telemetry_placeholders = set(re.findall(r"\$\{([A-Z0-9_]+)\}", telemetry_json))
        history_placeholders = set(re.findall(r"\$\{([A-Z0-9_]+)\}", history_json))

        assert "${/^(?:OK|UNSET)$/}" not in telemetry_json
        assert telemetry_placeholders <= history_placeholders | {
            "EXECUTION_ARN",
            "TARGET_EXECUTION_ARN",
        }


@pytest.mark.parametrize(
    ("case_number", "submitter_name", "operation_id"),
    [
        (10, "${/^otel-callback(?: submitter|-submitter)$/}", "${SUBMITTER_STEP}"),
        (17, "${/^otel-failed-callback(?: submitter|-submitter)$/}", "${CALLBACK_SUBMITTER}"),
    ],
)
@pytest.mark.parametrize("suite_name", ["otel-invocation", "otel-execution"])
def test_callback_submitter_assertions_emit_once_without_retry(
    suite_name: str,
    case_number: int,
    submitter_name: str,
    operation_id: str,
) -> None:
    requirement = load_yaml_file(_requirements(suite_name)[f"{suite_name}-{case_number}"])
    submitter_assertions = [
        assertion
        for assertion in requirement["TelemetryAssertions"]["span_assertions"]
        if assertion["select"]["name"] == submitter_name
        and assertion["expect"]["attributes"]["durable.operation.id"] == operation_id
    ]

    assert len(submitter_assertions) == 1
    submitter_assertion = submitter_assertions[0]
    assert submitter_assertion["select"]["status"] == "OK"
    assert submitter_assertion["expect"]["status"] == "OK"

    if suite_name == "otel-invocation":
        assert submitter_assertion["expect"]["links"] == []
        assert submitter_assertion["expect"]["parent"]["status"] == "UNSET"
    else:
        assert submitter_assertion["expect"]["links"] == [{"name": "invocation"}]
        assert "inside" not in submitter_assertion["expect"]


@pytest.mark.parametrize(
    ("suite_name", "ordered_cases"),
    [
        ("otel-execution", {2, 3, 8, 9, 10, 11, 17, 18}),
        ("otel-invocation", {2, 3, 8, 9, 10, 11, 17, 18}),
    ],
)
def test_otel_catalog_asserts_every_deterministic_span_order(
    suite_name: str,
    ordered_cases: set[int],
) -> None:
    requirements = _requirements(suite_name)

    for case_number in range(1, 20):
        requirement = load_yaml_file(requirements[f"{suite_name}-{case_number}"])
        assert all(
            not {"before", "after"} & set(span_assertion["select"])
            for span_assertion in requirement["TelemetryAssertions"]["span_assertions"]
        )
        relationships = {
            key
            for span_assertion in requirement["TelemetryAssertions"]["span_assertions"]
            for key in {"before", "after"} & set(span_assertion["expect"])
        }

        assert bool(relationships) is (case_number in ordered_cases)
        if relationships:
            assert relationships == {"before", "after"}
