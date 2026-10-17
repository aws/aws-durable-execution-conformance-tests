# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Workflow Insight sink adapters and the built-in catalog."""

from aws_durable_execution_conformance_tests_insight.sinks.cloudwatch import (
    CloudWatchSink,
    CloudWatchSinkFactory,
)
from aws_durable_execution_conformance_tests_insight.sinks.s3 import (
    S3Sink,
    S3SinkFactory,
)

BUILTIN_SINKS = {
    "s3": S3SinkFactory,
    "cloudwatch": CloudWatchSinkFactory,
}

__all__ = [
    "BUILTIN_SINKS",
    "CloudWatchSink",
    "CloudWatchSinkFactory",
    "S3Sink",
    "S3SinkFactory",
]
