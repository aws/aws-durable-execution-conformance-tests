# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Packaged OTel requirement resource tests."""

import json
import re

from aws_durable_execution_conformance_tests.validate import (
    discover_test_files,
    load_yaml_file,
)
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension


def test_extension_exposes_packaged_otel_requirements() -> None:
    suite = OtelExtension().requirement_suites()[0]
    requirements = discover_test_files(suite.root, suite="all")

    assert suite.name == "otel"
    assert set(requirements) == {f"otel-{case_number}" for case_number in range(1, 20)}


def test_expanded_catalog_exercises_span_hierarchy_assertions() -> None:
    suite = OtelExtension().requirement_suites()[0]
    requirements = discover_test_files(suite.root, suite="all")

    for case_number in range(1, 20):
        requirement = load_yaml_file(requirements[f"otel-{case_number}"])
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
                "${/^(?:ERROR|UNSET)$/}",
                "${/^(?:OK|UNSET)$/}",
            }
            assert expected["service_name"] == "invocation"
            one_link = [
                {
                    "trace_id": "${/^[0-9a-f]{32}$/}",
                    "span_id": "${/^[0-9a-f]{16}$/}",
                }
            ]
            assert expected["links"] in (
                [],
                one_link,
                {"$any_of": [[], one_link]},
            )
            assert expected["attributes"]["span.name"] == selected_name
            assert expected["attributes"]["span.kind"] == ("SERVER" if selected_name == "invocation" else "INTERNAL")
            if parent := expected.get("parent"):
                assert parent["attributes"]["span.name"] == parent["name"]
                assert parent["attributes"]["span.kind"] == ("SERVER" if parent["name"] == "invocation" else "INTERNAL")

        telemetry_json = json.dumps(assertions)
        history_json = json.dumps(requirement["ExpectedExecutionHistory"])
        telemetry_placeholders = set(re.findall(r"\$\{([A-Z0-9_]+)\}", telemetry_json))
        history_placeholders = set(re.findall(r"\$\{([A-Z0-9_]+)\}", history_json))

        assert '"*"' not in telemetry_json
        assert telemetry_placeholders <= history_placeholders | {"EXECUTION_ARN"}
