# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Shared launch identity and deployment manifest fixtures for the AWS transport tests."""

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    Scenario,
    ScenarioContext,
)

from .support import RUNTIME

FUNCTION = "arn:aws:lambda:us-west-2:123456789012:function:lmi:$LATEST.PUBLISHED"
EXECUTION = FUNCTION + "/durable-execution/name/id"


def context(
    case_id="case",
    scenario="suspension-retry",
    roles=("victim", "resume"),
    *,
    native=1,
):
    profile = Profile(f"native-{native}", FUNCTION, native, native, "sdk-sha", "configuration-sha")
    return ScenarioContext(
        "run",
        Scenario(
            case_id,
            "lmi-lifecycle-5",
            profile.profile_id,
            scenario,
            roles=roles,
            coverage_profile="standard",
        ),
        profile,
    )


def deployment():
    return {
        **RUNTIME.to_manifest(),
        "functions": {"c1": FUNCTION, "c2": FUNCTION + "-c2"},
        "concurrencies": {"c1": 1, "c2": 2},
        "region": "us-west-2",
        "run": "run",
        "bucket": "private-evidence",
        "commit": "sdk-sha",
        "runtime": "python3.14",
        "role": "role",
        "provider": "provider",
        "codeSha256": "code",
        "invocationTimeout": 60,
        "executionTimeout": 240,
        "driverTimeout": 1,
    }


def enter(item, request, at):
    return {
        "phase": "WRAPPER_ENTER",
        "marker": item["marker"],
        "request": request,
        "execution": item.get("arn") or EXECUTION,
        "time": at,
        "sequence": 0,
        "run": "run",
        "commit": "sdk-sha",
        "runtime": "python3.14",
        "role": "role",
        "provider": "provider",
        "codeSha256": "code",
        "invocationTimeout": 60,
        "executionTimeout": 240,
    }
