# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Packaged OTel requirement resource tests."""

from aws_durable_execution_conformance_tests.validate import (
    discover_test_files,
    load_yaml_file,
)
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension


def test_extension_exposes_packaged_otel_requirements() -> None:
    suite = OtelExtension().requirement_suites()[0]
    requirements = discover_test_files(suite.root, suite="all")

    assert suite.name == "otel"
    assert set(requirements) == {f"otel-{case_number}" for case_number in range(1, 10)}


def test_expanded_catalog_exercises_span_hierarchy_assertions() -> None:
    suite = OtelExtension().requirement_suites()[0]
    requirements = discover_test_files(suite.root, suite="all")

    for case_number in range(5, 10):
        requirement = load_yaml_file(requirements[f"otel-{case_number}"])
        assertions = requirement["TelemetryAssertions"]

        assert assertions["require_execution_correlation"] is True
        assert assertions["span_assertions"]
