# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for runtime configuration."""

from aws_durable_execution_conformance_tests.config import TESTS_DIR
from aws_durable_execution_conformance_tests.validate import discover_suites


def test_requirement_suites_are_available() -> None:
    """The configured requirement resource contains discoverable suites."""
    suites = discover_suites(TESTS_DIR)

    assert "step" in suites
    assert "wait" in suites
