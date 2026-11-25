# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Explicit test fixture policies; no language SDK is needed by the shared harness."""

from aws_durable_execution_conformance_tests.lmi.runtime import FixtureBinding, RuntimeDefinition

FIXTURES = {
    "native-overlap": "barrier",
    "normal-return": "success",
    "deadline-during-step": "deadline",
    "peer-recovery": "deadline",
    "suspension-retry": "replay",
    "warm-reuse": "warm-owner",
    "normal-failure": "failure",
    "peer": "barrier",
    "recovery": "barrier",
    "warm": "warm-probe",
}
BINDING = FixtureBinding(
    fixtures=FIXTURES,
    failure_error_type="ValueError",
    worker_identity="process",
)
RUNTIME = RuntimeDefinition("python", "fixture.handler", binding=BINDING)
