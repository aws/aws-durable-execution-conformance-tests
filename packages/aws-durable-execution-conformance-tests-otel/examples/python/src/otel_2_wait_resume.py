# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Wait and resume scenario for OTel requirement otel-2."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python_otel import OtelPlugin
from common import require_scenario


@durable_step
def complete_after_resume(_step_context: StepContext) -> str:
    return "resumed"


@durable_execution(plugins=[OtelPlugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "wait-resume")
    context.wait(Duration.from_seconds(1), name="otel-wait")
    return context.step(complete_after_resume(), name="otel-after-resume")
