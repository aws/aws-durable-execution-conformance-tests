# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Requirement-suite resource resolution tests."""

from __future__ import annotations

from aws_durable_execution_conformance_tests_insight.extension import InsightExtension


def test_requirement_suite_resolves_and_is_named_insight() -> None:
    suites = InsightExtension().requirement_suites()
    assert len(suites) == 1
    suite = suites[0]
    assert suite.name == "insight"
    assert suite.provider == "insight"
    assert suite.validation_hook is not None
    # Resolves to a real directory (source tree preferred, packaged fallback otherwise).
    assert suite.root.is_dir()


def test_requirement_files_are_discoverable() -> None:
    suite = InsightExtension().requirement_suites()[0]
    yaml_files = sorted(path.name for path in suite.root.rglob("*.yaml"))
    # The suite directory exists so discovery works even before requirement YAMLs land.
    assert all(name.startswith("insight-") for name in yaml_files)


def test_extension_declares_core_compatibility() -> None:
    extension = InsightExtension()
    assert extension.name == "insight"
    assert extension.requires_core == ">=1.0.0,<2.0.0"
