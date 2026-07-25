# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Packaged OTel requirement resource tests."""

import json
import re
from copy import deepcopy

import pytest

from aws_durable_execution_conformance_tests.validate import (
    discover_test_files,
    load_yaml_file,
)
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension


def _requirements(suite_name: str) -> dict[str, str]:
    suites = {suite.name: suite for suite in OtelExtension().requirement_suites()}
    return discover_test_files(suites[suite_name].root, suite="all")


def test_extension_exposes_packaged_otel_view_requirements() -> None:
    suites = {suite.name: suite for suite in OtelExtension().requirement_suites()}

    assert set(suites) == {"otel-execution", "otel-invocation"}
    assert set(_requirements("otel-invocation")) == {f"otel-invocation-{case_number}" for case_number in range(1, 20)}
    assert set(_requirements("otel-execution")) == {f"otel-execution-{case_number}" for case_number in range(1, 20)}


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
    requirements = _requirements("otel-execution")

    for case_number in range(1, 20):
        requirement = load_yaml_file(requirements[f"otel-execution-{case_number}"])
        assertions = requirement["TelemetryAssertions"]
        span_assertions = assertions["span_assertions"]
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
            assert workflow["expect"]["parent_span_id"] is None
            assert (
                workflow["expect"]["status"]
                == {
                    "SUCCEEDED": "OK",
                    "FAILED": "ERROR",
                    "TIMED_OUT": "ERROR",
                }[execution_status]
            )
            assert workflow["expect"]["attributes"] == {
                "durable.execution.arn": execution_arn,
                "durable.execution.status": execution_status,
            }

        descendants = [item for item in span_assertions if item not in workflows]
        assert bool(descendants) is (case_number != 19)
        for descendant in descendants:
            expected = descendant["expect"]
            assert expected["kind"] == "INTERNAL"
            assert expected["parent"]
            assert expected["links"] == [{"name": "invocation"}]
            assert expected["inside"] == {
                "$linked": True,
                "name": "invocation",
            }

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
    ("suite_name", "ordered_cases"),
    [
        ("otel-execution", {2, 3, 8, 9}),
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
