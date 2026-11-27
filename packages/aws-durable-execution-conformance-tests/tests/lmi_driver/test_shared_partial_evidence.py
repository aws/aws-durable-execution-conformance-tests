# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Shared scenario-context fixture for tests that only need a deadline-step case."""

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    Scenario,
    ScenarioContext,
)


def context():
    profile = Profile("c1", "function", 1, 1, "sdk", "configuration")
    return ScenarioContext(
        "run",
        Scenario(
            "case",
            "lmi-lifecycle-3",
            "c1",
            "deadline-during-step",
            roles=("victim", "retry"),
            coverage_profile="standard",
        ),
        profile,
    )
