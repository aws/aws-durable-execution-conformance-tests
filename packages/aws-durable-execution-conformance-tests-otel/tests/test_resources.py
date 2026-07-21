# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Packaged OTel requirement resource tests."""

from aws_durable_execution_conformance_tests.validate import discover_test_files
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension


def test_extension_exposes_packaged_otel_requirements() -> None:
    suite = OtelExtension().requirement_suites()[0]
    requirements = discover_test_files(suite.root, suite="all")

    assert suite.name == "otel"
    assert set(requirements) == {"otel-1", "otel-2", "otel-3", "otel-4"}
