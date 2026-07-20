# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""End-to-end tests for the deterministic OTLP collector."""

from __future__ import annotations

import json
import urllib.request
from datetime import UTC, datetime

from aws_durable_execution_conformance_tests_otel.backends import CollectorBackend
from aws_durable_execution_conformance_tests_otel.collector import CollectorServer
from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery
from aws_durable_execution_conformance_tests_otel.polling import PollingPolicy
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)


def _json_payload() -> dict:
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
                                "name": "step",
                                "startTimeUnixNano": "1000000000",
                                "endTimeUnixNano": "2000000000",
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


def _query() -> TelemetryQuery:
    now = datetime.now(UTC)
    return TelemetryQuery("arn:test", "conformance", now, now)


def test_collector_receives_json_and_backend_queries_canonical_trace() -> None:
    with CollectorServer() as collector:
        request = urllib.request.Request(
            f"{collector.endpoint}/v1/traces",
            data=json.dumps(_json_payload()).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request) as response:
            assert response.status == 200

        backend = CollectorBackend(
            collector.endpoint,
            sleep=lambda _seconds: None,
        )
        trace = backend.find_trace(
            _query(),
            PollingPolicy(timeout_seconds=1, interval_seconds=0, max_attempts=1),
        )

    assert trace.trace_id == "1" * 32
    assert trace.spans[0].attributes["durable.execution.arn"] == "arn:test"


def test_collector_receives_otlp_protobuf() -> None:
    payload = ExportTraceServiceRequest()
    resource_spans = payload.resource_spans.add()
    resource_attr = resource_spans.resource.attributes.add()
    resource_attr.key = "service.name"
    resource_attr.value.string_value = "conformance"
    span = resource_spans.scope_spans.add().spans.add()
    span.trace_id = bytes.fromhex("3" * 32)
    span.span_id = bytes.fromhex("4" * 16)
    span.name = "protobuf span"
    span.start_time_unix_nano = 1_000_000_000
    span.end_time_unix_nano = 2_000_000_000
    execution_attr = span.attributes.add()
    execution_attr.key = "durable.execution.arn"
    execution_attr.value.string_value = "arn:protobuf"

    with CollectorServer() as collector:
        request = urllib.request.Request(
            f"{collector.endpoint}/v1/traces",
            data=payload.SerializeToString(),
            method="POST",
            headers={"Content-Type": "application/x-protobuf"},
        )
        with urllib.request.urlopen(request) as response:
            assert response.status == 200
        trace = collector.store.find(
            execution_arn="arn:protobuf",
            service_name="conformance",
            trace_id=None,
        )

    assert trace is not None
    assert trace.trace_id == "3" * 32
