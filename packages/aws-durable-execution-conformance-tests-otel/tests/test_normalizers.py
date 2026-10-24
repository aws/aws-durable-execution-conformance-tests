# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Canonical telemetry normalization tests."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from aws_durable_execution_conformance_tests_otel.backends.dash0 import (
    normalize_dash0,
)
from aws_durable_execution_conformance_tests_otel.backends.datadog import (
    normalize_datadog,
)
from aws_durable_execution_conformance_tests_otel.backends.xray import normalize_xray
from aws_durable_execution_conformance_tests_otel.model import span_to_dict
from aws_durable_execution_conformance_tests_otel.normalizers import (
    normalize_otlp_json,
    normalize_otlp_protobuf,
    normalize_span_kind,
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
                                "kind": "SPAN_KIND_INTERNAL",
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
    assert span.kind == "INTERNAL"
    assert span.attributes["durable.execution.arn"] == "arn:test"
    assert span.status == "OK"
    assert span.exception_event_count == 0


@pytest.mark.parametrize("wire_format", ["json", "protobuf"])
@pytest.mark.parametrize(
    ("event_names", "dropped", "expected"),
    [
        ([], 0, 0),
        (["annotation"], 0, 0),
        (["exception"], 0, 1),
        (["exception", "annotation", "exception"], 0, 2),
        ([], 1, None),
        (["exception"], 1, None),
    ],
)
def test_otlp_exception_events_require_complete_observation(
    wire_format: str, event_names: list[str], dropped: int, expected: int | None
) -> None:
    if wire_format == "json":
        payload = _otlp_payload()
        raw = payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
        raw.update(events=[{"name": name} for name in event_names], droppedEventsCount=dropped)
        trace = normalize_otlp_json(payload)[0]
    else:
        request = ExportTraceServiceRequest()
        raw_span = request.resource_spans.add().scope_spans.add().spans.add()
        raw_span.trace_id = bytes.fromhex("1" * 32)
        raw_span.span_id = bytes.fromhex("2" * 16)
        raw_span.dropped_events_count = dropped
        for name in event_names:
            raw_span.events.add(name=name)
        trace = normalize_otlp_protobuf(request.SerializeToString())[0]
    assert trace.spans[0].exception_event_count == expected
    assert span_to_dict(trace.spans[0])["exception_event_count"] == expected


def test_normalizes_span_kind_names_and_numbers() -> None:
    assert normalize_span_kind("SPAN_KIND_SERVER") == "SERVER"
    assert normalize_span_kind("client") == "CLIENT"
    assert normalize_span_kind(4) == "PRODUCER"
    assert normalize_span_kind(None) == "UNSPECIFIED"
    assert normalize_span_kind("unexpected") == "UNSPECIFIED"


def test_normalizes_xray_segments_and_subsegments() -> None:
    document = {
        "trace_id": "1-aaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbb",
        "id": "1" * 16,
        "parent_id": "f" * 16,
        "name": "conformance",
        "start_time": 1,
        "end_time": 2,
        "annotations": {"indexed": "value"},
        "metadata": {
            "durable.execution.arn": "arn:test",
            "faas.invocation_id": "invocation-1",
            "default": {
                "legacy.attribute": "legacy-value",
                "span.name": "conformance",
                "span.kind": "SERVER",
            },
            "custom": {"tenant": "example"},
        },
        "subsegments": [
            {
                "id": "2" * 16,
                "name": "child",
                "start_time": 1.2,
                "end_time": 1.8,
                "subsegments": [
                    {
                        "id": "3" * 16,
                        "name": "grandchild",
                        "start_time": 1.3,
                        "end_time": 1.7,
                    }
                ],
            }
        ],
    }

    trace = normalize_xray([json.dumps(document)])[0]
    assert trace.trace_id == "a" * 8 + "b" * 24
    assert len(trace.spans) == 3
    assert trace.spans[0].attributes["indexed"] == "value"
    assert trace.spans[0].attributes["durable.execution.arn"] == "arn:test"
    assert trace.spans[0].attributes["faas.invocation_id"] == "invocation-1"
    assert trace.spans[0].attributes["legacy.attribute"] == "legacy-value"
    assert trace.spans[0].attributes["span.name"] == "conformance"
    assert trace.spans[0].attributes["span.kind"] == "SERVER"
    assert trace.spans[0].kind == "SERVER"
    assert trace.spans[0].attributes["xray.custom.tenant"] == "example"
    assert trace.spans[0].parent_span_id == "f" * 16
    assert trace.spans[1].parent_span_id == "1" * 16
    assert trace.spans[2].parent_span_id == "2" * 16
    assert all(span.service_name == "conformance" for span in trace.spans)


def test_normalizes_xray_otel_service_name_and_inherits_it_for_subsegments() -> None:
    document = {
        "trace_id": "1-aaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbb",
        "id": "1" * 16,
        "name": "Workflow",
        "start_time": 1,
        "end_time": 2,
        "metadata": {"default": {"aws.local.service": "invocation"}},
        "subsegments": [
            {
                "id": "2" * 16,
                "name": "inherited child",
                "start_time": 1.2,
                "end_time": 1.8,
            },
            {
                "id": "3" * 16,
                "name": "overriding child",
                "start_time": 1.3,
                "end_time": 1.7,
                "metadata": {"default": {"aws.local.service": "child-service"}},
            },
        ],
    }

    spans = normalize_xray([document])[0].spans

    assert spans[0].service_name == "invocation"
    assert spans[1].service_name == "invocation"
    assert spans[2].service_name == "child-service"


@pytest.mark.parametrize(
    ("raw_first_invocation", "expected_first_invocation"),
    [("true", True), ("false", False)],
)
def test_normalizes_datadog_span_search_payload(
    raw_first_invocation: str,
    expected_first_invocation: bool,
) -> None:
    payload = {
        "data": [
            {
                "id": "event-id",
                "type": "spans",
                "attributes": {
                    "trace_id": "10",
                    "span_id": "7",
                    "parent_id": "0",
                    "service": "conformance",
                    "resource_name": "step",
                    "start_timestamp": "2026-01-01T00:00:00Z",
                    "end_timestamp": "2026-01-01T00:00:01Z",
                    "custom": {
                        "durable": {
                            "execution": {
                                "arn": "arn:test",
                            },
                            "invocation": {
                                "first": raw_first_invocation,
                            },
                        },
                        "unrelated": {"string_boolean": "true"},
                        "otel": {
                            "status_code": "Error",
                            "trace_id": "11111111111111111111111111111111",
                        },
                        "span": {"kind": "client"},
                    },
                },
            }
        ]
    }

    span = normalize_datadog(payload)[0].spans[0]
    assert span.trace_id == "1" * 32
    assert span.span_id.endswith("7")
    assert span.parent_span_id is None
    assert span.service_name == "conformance"
    assert span.kind == "CLIENT"
    assert span.status == "ERROR"
    assert span.attributes["durable.execution.arn"] == "arn:test"
    assert span.attributes["durable.invocation.first"] is expected_first_invocation
    assert span.attributes["unrelated.string_boolean"] == "true"
    assert span.end_time - span.start_time == timedelta(seconds=1)


def test_normalizes_dash0_otlp_shape() -> None:
    payload = _otlp_payload()
    raw_span = payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    raw_span["traceId"] = "EREREREREREREREREREREQ=="
    raw_span["spanId"] = "IiIiIiIiIiI="
    raw_span["links"] = [
        {
            "traceId": "MzMzMzMzMzMzMzMzMzMzMw==",
            "spanId": "REREREREREQ=",
        }
    ]

    trace = normalize_dash0(payload)[0]

    assert trace.trace_id == "1" * 32
    assert trace.spans[0].span_id == "2" * 16
    assert trace.spans[0].links[0].trace_id == "3" * 32
    assert trace.spans[0].links[0].span_id == "4" * 16
    assert trace.spans[0].name == "durable step"
    assert trace.spans[0].kind == "INTERNAL"
