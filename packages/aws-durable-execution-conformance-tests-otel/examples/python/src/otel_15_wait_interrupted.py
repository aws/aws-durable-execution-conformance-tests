# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Interrupted wait scenario for OTel requirement otel-15."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python_otel import OtelPlugin
from common import require_scenario


@durable_execution(plugins=[OtelPlugin()])
def handler(event: dict[str, Any], context: DurableContext) -> None:
    require_scenario(event, "wait-interrupted")
    context.wait(
        Duration.from_seconds(30),
        name="otel-interrupted-wait",
    )
