# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the command-line interface."""

from aws_durable_execution_conformance_tests.app import parse_args
from aws_durable_execution_conformance_tests.config import DEFAULT_REGION


def test_region_defaults_to_configured_region() -> None:
    args = parse_args(["--template", "template.yaml", "--language", "python"])

    assert args.region == DEFAULT_REGION


def test_region_accepts_override() -> None:
    args = parse_args(
        [
            "--template",
            "template.yaml",
            "--language",
            "python",
            "--region",
            "eu-west-1",
        ]
    )

    assert args.region == "eu-west-1"


def test_suite_defaults_to_all() -> None:
    args = parse_args(["--template", "template.yaml", "--language", "python"])

    assert args.suite == ["all"]


def test_suite_accepts_discovered_suite() -> None:
    args = parse_args(
        [
            "--template",
            "template.yaml",
            "--language",
            "python",
            "--suite",
            "step",
        ]
    )

    assert args.suite == ["step"]


def test_cleanup_enabled_by_default() -> None:
    args = parse_args(["--template", "template.yaml", "--language", "python"])

    assert args.cleanup is True


def test_cleanup_can_be_disabled() -> None:
    args = parse_args(
        [
            "--template",
            "template.yaml",
            "--language",
            "python",
            "--no-cleanup",
        ]
    )

    assert args.cleanup is False


def test_cleanup_explicit_enable() -> None:
    args = parse_args(
        [
            "--template",
            "template.yaml",
            "--language",
            "python",
            "--cleanup",
        ]
    )

    assert args.cleanup is True
