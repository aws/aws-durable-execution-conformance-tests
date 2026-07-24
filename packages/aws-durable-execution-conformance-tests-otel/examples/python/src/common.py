# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Shared input validation for the Python OTel conformance examples."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


def otel_plugin() -> DurableInstrumentationPlugin:
    """Select the telemetry view configured for this deployed function."""

    if os.environ.get("OTEL_PLUGIN_MODE") == "execution":
        return ExecutionOtelPlugin(
            OtelPluginConfig(use_default_tracer_provider=True),
        )
    return InvocationOtelPlugin()


def require_scenario(event: Mapping[str, Any], expected: str) -> None:
    """Reject an event that was routed to the wrong conformance handler."""

    actual = event.get("scenario")
    if actual != expected:
        raise ValueError(f"Expected scenario {expected!r}, received {actual!r}")
