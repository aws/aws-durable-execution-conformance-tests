# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Packaged OTel requirement resource tests."""

import json
import re
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest

from aws_durable_execution_conformance_tests.validate import (
    discover_test_files,
    load_yaml_file,
)
from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension
from aws_durable_execution_conformance_tests_otel.model import (
    Span,
    TelemetryQuery,
    Trace,
)
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendFeatureDisparity,
)
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

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
        f"otel-long-running-{case_number}" for case_number in range(1, 5)
    }


def test_long_running_catalog_uses_configurable_delays() -> None:
    requirements = _requirements("otel-long-running")

    for case_number in range(1, 5):
        requirement = load_yaml_file(requirements[f"otel-long-running-{case_number}"])

        assert requirement["Variables"] == {"LONG_DELAY_SECONDS": 1}
        assert requirement["Input"]["delay_seconds"] == "${LONG_DELAY_SECONDS}"
        assert requirement["AsyncInvoke"] is True
        assert requirement["ExpectedResult"]["ExecutionStatus"] == "SUCCEEDED"
        expected_invocations = 4 if case_number == 4 else 2
        assert requirement["TelemetryAssertions"]["minimum_invocations"] == expected_invocations
        assert requirement["ExecutionTelemetryAssertions"]["minimum_invocations"] == expected_invocations
        assert requirement["TelemetryAssertions"] != requirement["ExecutionTelemetryAssertions"]

        execution_span_assertions = requirement["ExecutionTelemetryAssertions"]["span_assertions"]
        execution_workflow_spans = [
            assertion for assertion in execution_span_assertions if assertion["select"]["name"] == "Workflow"
        ]
        execution_invocation_spans = [
            assertion
            for assertion in execution_span_assertions
            if assertion["select"]["name"] == "${/^[Ii]nvocation$/}"
        ]
        invocation_span_assertions = requirement["TelemetryAssertions"]["span_assertions"]
        invocation_workflow_spans = [
            assertion for assertion in invocation_span_assertions if assertion["select"]["name"] == "Workflow"
        ]
        invocation_invocation_spans = [
            assertion for assertion in invocation_span_assertions if assertion["select"]["name"] == "Invocation"
        ]
        assert len(execution_workflow_spans) == 1
        assert len(execution_invocation_spans) == expected_invocations
        for execution_invocation, invocation in zip(
            execution_invocation_spans,
            invocation_invocation_spans,
            strict=True,
        ):
            assert (
                json.loads(json.dumps(execution_invocation).replace("${/^[Ii]nvocation$/}", "Invocation")) == invocation
            )
        for assertion in execution_span_assertions:
            if assertion["select"]["name"] in {"Workflow", "${/^[Ii]nvocation$/}"}:
                assert assertion["expect"]["links"] == []
                continue
            assert assertion["expect"]["links"] == [{"name": "${/^[Ii]nvocation$/}"}]
            if inside := assertion["expect"].get("inside"):
                assert inside == {
                    "$linked": True,
                    "name": "${/^[Ii]nvocation$/}",
                }
        expected_workflow_arns = ["${EXECUTION_ARN}"]
        if case_number == 4:
            expected_workflow_arns.append("${TARGET_EXECUTION_ARN}")
            assert requirement["TelemetryAssertions"]["minimum_spans"] == 10
        assert [
            assertion["select"]["attributes"]["durable.execution.arn"] for assertion in invocation_workflow_spans
        ] == expected_workflow_arns
        assert len(invocation_invocation_spans) == expected_invocations
        for execution_arn, workflow_assertion in zip(
            expected_workflow_arns,
            invocation_workflow_spans,
            strict=True,
        ):
            assert workflow_assertion == {
                "select": {
                    "name": "Workflow",
                    "status": "OK",
                    "attributes": {
                        "durable.execution.arn": execution_arn,
                    },
                },
                "expect": {
                    "parent_span_id": None,
                    "status": "OK",
                    "service_name": "invocation",
                    "links": [],
                    "kind": "INTERNAL",
                    "attributes": {
                        "durable.execution.arn": execution_arn,
                        "durable.execution.status": "SUCCEEDED",
                    },
                },
            }
        assert '"name": "invocation"' not in json.dumps(requirement["TelemetryAssertions"])
        for assertion in invocation_span_assertions:
            if assertion["select"]["name"] in {"Invocation", "Workflow"}:
                assert assertion["expect"]["links"] == []
                continue
            links = assertion["expect"]["links"]
            link_alternatives = links["$any_of"] if isinstance(links, dict) else [links]
            execution_arn = assertion["expect"]["attributes"]["durable.execution.arn"]
            assert all(
                link_set[-1]
                == {
                    "name": "Workflow",
                    "attributes": {
                        "durable.execution.arn": execution_arn,
                    },
                }
                for link_set in link_alternatives
            )


@pytest.mark.parametrize(
    ("assertion_key", "callback_count"),
    [
        ("TelemetryAssertions", 2),
        ("ExecutionTelemetryAssertions", 1),
    ],
)
def test_long_callback_assertions_accept_typescript_generic_span_names(
    assertion_key: str,
    callback_count: int,
) -> None:
    requirement = load_yaml_file(_requirements("otel-long-running")["otel-long-running-3"])
    placeholders = PlaceholderContext()
    bindings = {
        "EXECUTION_ARN": "arn:test",
        "CALLBACK_CONTEXT": "callback-context",
        "CALLBACK1": "callback-1",
        "SUBMITTER_STEP": "submitter-step",
    }
    for name, value in bindings.items():
        placeholders.bind(name, value)
    assertions = placeholders.substitute(requirement[assertion_key])
    focused_assertions = {"span_assertions": assertions["span_assertions"][-3:]}

    now = datetime.now(UTC)
    trace_id = "1" * 32
    context_span = Span(
        trace_id=trace_id,
        span_id="2" * 16,
        name="otel-long-callback",
        start_time=now,
        end_time=now,
        kind="INTERNAL",
        status="OK",
        service_name="invocation",
        attributes={
            "durable.execution.arn": bindings["EXECUTION_ARN"],
            "durable.operation.id": bindings["CALLBACK_CONTEXT"],
            "durable.operation.subtype": "WaitForCallback",
        },
    )
    invocation_span = Span(
        trace_id=trace_id,
        span_id="7" * 16,
        name="invocation",
        start_time=now,
        end_time=now,
        kind="INTERNAL",
        status="OK",
        service_name="invocation",
        attributes={
            "durable.execution.arn": bindings["EXECUTION_ARN"],
        },
    )
    callbacks = tuple(
        Span(
            trace_id=trace_id,
            span_id=str(index + 3) * 16,
            parent_span_id=(invocation_span.span_id if callback_count == 2 and index == 0 else context_span.span_id),
            name="CALLBACK",
            start_time=now,
            end_time=now,
            kind="INTERNAL",
            status="OK",
            service_name="invocation",
            attributes={
                "durable.execution.arn": bindings["EXECUTION_ARN"],
                "durable.operation.id": bindings["CALLBACK1"],
                "durable.operation.type": "CALLBACK",
                "durable.operation.subtype": "Callback",
                "durable.operation.name": "otel-long-callback-callback",
            },
        )
        for index in range(callback_count)
    )
    step_span = Span(
        trace_id=trace_id,
        span_id="5" * 16,
        parent_span_id=context_span.span_id,
        name="STEP",
        start_time=now,
        end_time=now,
        kind="INTERNAL",
        status="OK",
        service_name="invocation",
        attributes={
            "durable.execution.arn": bindings["EXECUTION_ARN"],
            "durable.operation.id": bindings["SUBMITTER_STEP"],
            "durable.operation.type": "STEP",
            "durable.operation.subtype": "Step",
            "durable.operation.status": "SUCCEEDED",
            "durable.operation.name": "otel-long-callback-submitter",
            "durable.attempt.number": 1,
        },
    )
    attempt_span = Span(
        trace_id=trace_id,
        span_id="6" * 16,
        parent_span_id=step_span.span_id,
        name="STEP attempt 1",
        start_time=now,
        end_time=now,
        kind="INTERNAL",
        status="OK",
        service_name="invocation",
        attributes={
            "durable.execution.arn": bindings["EXECUTION_ARN"],
            "durable.operation.id": bindings["SUBMITTER_STEP"],
            "durable.operation.type": "STEP",
            "durable.operation.subtype": "Step",
            "durable.operation.name": "otel-long-callback-submitter",
            "durable.attempt.number": 1,
            "durable.attempt.outcome": "SUCCEEDED",
        },
    )
    trace = Trace(
        trace_id=trace_id,
        spans=(context_span, invocation_span, *callbacks, step_span, attempt_span),
    )
    query = TelemetryQuery(
        execution_arn=bindings["EXECUTION_ARN"],
        service_name="invocation",
        started_at=now,
        ended_at=now,
    )

    assert (
        validate_trace(
            trace,
            focused_assertions,
            query,
            feature_disparities={BackendFeatureDisparity.SPAN_LINKS},
        )
        == []
    )


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
        span_assertions = assertions["span_assertions"]
        assert span_assertions
        workflows = [assertion for assertion in span_assertions if assertion["select"]["name"] == "Workflow"]
        expected_execution_status = requirement["ExpectedResult"]["ExecutionStatus"]
        if case_number == 15:
            assert workflows == []
        else:
            expected_workflow_status = {
                "FAILED": "ERROR",
                "SUCCEEDED": "OK",
            }[expected_execution_status]
            expected_workflow_arns = ["${EXECUTION_ARN}"]
            if case_number in {11, 18}:
                expected_workflow_arns.append("${TARGET_EXECUTION_ARN}")
                assert assertions["minimum_spans"] == 7
            assert len(workflows) == len(expected_workflow_arns)
            for execution_arn, workflow in zip(expected_workflow_arns, workflows, strict=True):
                assert workflow == {
                    "select": {
                        "name": "Workflow",
                        "status": expected_workflow_status,
                        "attributes": {
                            "durable.execution.arn": execution_arn,
                        },
                    },
                    "expect": {
                        "parent_span_id": None,
                        "status": expected_workflow_status,
                        "service_name": "invocation",
                        "links": [],
                        "kind": "INTERNAL",
                        "attributes": {
                            "durable.execution.arn": execution_arn,
                            "durable.execution.status": expected_execution_status,
                        },
                    },
                }
        for span_assertion in span_assertions:
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
            if "links" not in expected:
                assert case_number == 15
                assert selected_name == "otel-interrupted-wait"
                link_alternatives = []
            else:
                links = expected["links"]
                if isinstance(links, dict):
                    assert set(links) == {"$any_of"}
                    link_alternatives = links["$any_of"]
                else:
                    link_alternatives = [links]
                for link_set in link_alternatives:
                    assert isinstance(link_set, list)
                    assert len(link_set) <= 2
                    for linked_span in link_set:
                        assert linked_span["name"]
                        assert "trace_id" not in linked_span
                        assert "span_id" not in linked_span
                        if linked_span["name"] == "Workflow":
                            assert linked_span["attributes"] == {
                                "durable.execution.arn": "${EXECUTION_ARN}",
                            }
                        else:
                            assert linked_span["attributes"]["durable.operation.id"]
            if selected_name not in {"Invocation", "Workflow"} and case_number != 15:
                assert link_alternatives
                assert all(link_set[-1]["name"] == "Workflow" for link_set in link_alternatives)
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
            if selected_name == "Invocation":
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
                        "PENDING": "OK",
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
                if case_number == 19:
                    assert expected_attributes["durable.invocation.status"] == "FAILED"
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


def test_execution_view_catalog_asserts_workflow_parentage_and_ambient_links() -> None:
    execution_requirements = _requirements("otel-execution")
    invocation_requirements = _requirements("otel-invocation")
    ambient_only_cases = {15, 19}

    for case_number in range(1, 20):
        requirement = load_yaml_file(execution_requirements[f"otel-execution-{case_number}"])
        invocation_requirement = load_yaml_file(
            invocation_requirements[f"otel-invocation-{case_number}"],
        )
        assertions = requirement["TelemetryAssertions"]
        span_assertions = assertions["span_assertions"]
        assert all("count" not in assertion for assertion in span_assertions)
        workflows = [item for item in span_assertions if item["select"]["name"] == "Workflow"]
        expected_workflow_statuses = (
            {}
            if case_number in ambient_only_cases
            else {
                "${EXECUTION_ARN}": requirement["ExpectedResult"]["ExecutionStatus"],
            }
        )
        if case_number in {11, 18}:
            expected_workflow_statuses["${TARGET_EXECUTION_ARN}"] = "SUCCEEDED" if case_number == 11 else "FAILED"
            assert assertions["allowed_execution_arns"] == [
                "${EXECUTION_ARN}",
                "${TARGET_EXECUTION_ARN}",
            ]

        assert assertions["require_execution_correlation"] is True
        assert "require_all_spans" not in assertions
        assert "exact_attribute_prefixes" not in assertions
        assert assertions["minimum_spans"] >= len(span_assertions)
        assert len(workflows) == len(expected_workflow_statuses)
        for workflow in workflows:
            execution_arn = workflow["select"]["attributes"]["durable.execution.arn"]
            execution_status = expected_workflow_statuses[execution_arn]
            expected_span_status = {
                "SUCCEEDED": "OK",
                "FAILED": "ERROR",
            }[execution_status]
            assert workflow["select"] == {
                "name": "Workflow",
                "status": expected_span_status,
                "attributes": {
                    "durable.execution.arn": execution_arn,
                },
            }
            assert workflow["expect"]["parent_span_id"] is None
            assert workflow["expect"]["status"] == expected_span_status
            assert workflow["expect"]["attributes"] == {
                "durable.execution.arn": execution_arn,
                "durable.execution.status": execution_status,
            }

        invocations = [item for item in span_assertions if item["select"]["name"] == "Invocation"]
        expected_invocations = [
            item
            for item in invocation_requirement["TelemetryAssertions"]["span_assertions"]
            if item["select"]["name"] == "Invocation"
        ]
        assert invocations == expected_invocations
        assert assertions.get("minimum_invocations", 1) == len(expected_invocations)

        descendants = [item for item in span_assertions if item not in workflows and item not in invocations]
        assert bool(descendants) is (case_number not in ambient_only_cases)
        for descendant in descendants:
            expected = descendant["expect"]
            assert expected["kind"] == "INTERNAL"
            assert expected["links"] == [{"name": "${/^[Ii]nvocation$/}"}]
            if "durable.attempt.outcome" in expected["attributes"]:
                assert expected["inside"] == {
                    "$linked": True,
                    "name": "${/^[Ii]nvocation$/}",
                }
            else:
                assert "inside" not in expected
            parent = expected["parent"]
            if parent["name"] == "Workflow":
                assert "$allow_outside" not in parent
            elif case_number in {3, 9, 10, 17}:
                assert parent["$allow_outside"] is True
            else:
                assert "$allow_outside" not in parent

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
        assert submitter_assertion["expect"]["links"] == [
            {
                "name": "Workflow",
                "attributes": {
                    "durable.execution.arn": "${EXECUTION_ARN}",
                },
            },
        ]
        assert submitter_assertion["expect"]["parent"]["status"] == "UNSET"
    else:
        assert submitter_assertion["expect"]["links"] == [{"name": "${/^[Ii]nvocation$/}"}]
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
