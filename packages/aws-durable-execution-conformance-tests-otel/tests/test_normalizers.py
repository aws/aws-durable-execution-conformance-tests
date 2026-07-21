# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Canonical telemetry normalization tests."""

from __future__ import annotations

import json

from aws_durable_execution_conformance_tests_otel.backends.dash0 import (
    normalize_dash0,
)
from aws_durable_execution_conformance_tests_otel.backends.datadog import (
    normalize_datadog,
)
from aws_durable_execution_conformance_tests_otel.backends.xray import normalize_xray
from aws_durable_execution_conformance_tests_otel.normalizers import (
    normalize_otlp_json,
)


def _otlp_payload() -> dict:
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "conformance"},
                        }
                    ]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "1" * 32,
                                "spanId": "2" * 16,
                                "name": "durable step",
                                "startTimeUnixNano": "1000000000",
                                "endTimeUnixNano": "2000000000",
                                "status": {"code": "STATUS_CODE_OK"},
                                "attributes": [
                                    {
                                        "key": "durable.execution.arn",
                                        "value": {"stringValue": "arn:test"},
                                    }
                                ],
                            }
                        ]
                    }
                ],
            }
        ]
    }


def test_normalizes_otlp_json_resource_and_span_attributes() -> None:
    trace = normalize_otlp_json(_otlp_payload())[0]
    span = trace.spans[0]

    assert trace.trace_id == "1" * 32
    assert span.service_name == "conformance"
    assert span.attributes["durable.execution.arn"] == "arn:test"
    assert span.status == "OK"


def test_normalizes_xray_segments_and_subsegments() -> None:
    document = {
        "trace_id": "1-aaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbb",
        "id": "1" * 16,
        "name": "conformance",
        "start_time": 1,
        "end_time": 2,
        "annotations": {"indexed": "value"},
        "metadata": {
            "default": {
                "durable.execution.arn": "arn:test",
                "faas.invocation_id": "invocation-1",
            },
            "custom": {"tenant": "example"},
        },
        "subsegments": [
            {
                "id": "2" * 16,
                "parent_id": "1" * 16,
                "name": "child",
                "start_time": 1.2,
                "end_time": 1.8,
            }
        ],
    }

    trace = normalize_xray([json.dumps(document)])[0]
    assert trace.trace_id == "a" * 8 + "b" * 24
    assert len(trace.spans) == 2
    assert trace.spans[0].attributes["indexed"] == "value"
    assert trace.spans[0].attributes["durable.execution.arn"] == "arn:test"
    assert trace.spans[0].attributes["faas.invocation_id"] == "invocation-1"
    assert trace.spans[0].attributes["xray.custom.tenant"] == "example"
    assert trace.spans[1].parent_span_id == "1" * 16


def test_normalizes_datadog_decimal_identifiers() -> None:
    payload = {
        "data": [
            {
                "id": "7",
                "attributes": {
                    "trace_id": "10",
                    "span_id": "7",
                    "parent_id": "0",
                    "service": "conformance",
                    "resource_name": "step",
                    "start_timestamp": "2026-01-01T00:00:00Z",
                    "duration": 100,
                    "attributes": {"durable.execution.arn": "arn:test"},
                },
            }
        ]
    }

    span = normalize_datadog(payload)[0].spans[0]
    assert span.trace_id.endswith("a")
    assert span.span_id.endswith("7")
    assert span.service_name == "conformance"


def test_normalizes_dash0_otlp_shape() -> None:
    trace = normalize_dash0(_otlp_payload())[0]
    assert trace.spans[0].name == "durable step"
