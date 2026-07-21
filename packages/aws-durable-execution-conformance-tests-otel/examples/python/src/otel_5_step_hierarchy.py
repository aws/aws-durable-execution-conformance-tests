# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Named step hierarchy scenario for OTel requirement otel-5."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python_otel import OtelPlugin
from common import require_scenario


@durable_step
def complete_basic_step(_step_context: StepContext) -> str:
    return "step-complete"


@durable_execution(plugins=[OtelPlugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "step-hierarchy")
    return context.step(complete_basic_step(), name="otel-basic-step")
