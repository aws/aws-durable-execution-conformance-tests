# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Shared OTLP wire-format normalizers for the canonical telemetry model."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)

from aws_durable_execution_conformance_tests_otel.model import (
    Span,
    SpanLink,
    Trace,
    normalize_id,
    parse_timestamp,
)


def normalize_status(value: Any) -> str:
    """Normalize provider and OTLP status values."""

    text = str(value or "UNSET").upper()
    if text in {"1", "STATUS_CODE_OK", "OK"}:
        return "OK"
    if text in {"2", "STATUS_CODE_ERROR", "ERROR", "FAILURE", "FAILED"}:
        return "ERROR"
    return "UNSET"


def normalize_span_kind(value: Any) -> str:
    """Normalize OTLP span-kind enum names and numbers."""

    kinds = {
        "0": "UNSPECIFIED",
        "1": "INTERNAL",
        "2": "SERVER",
        "3": "CLIENT",
        "4": "PRODUCER",
        "5": "CONSUMER",
    }
    text = str(value if value is not None else 0).strip().upper()
    text = text.removeprefix("SPAN_KIND_")
    return kinds.get(text, text if text in kinds.values() else "UNSPECIFIED")


def _json_any_value(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return value
    keys = {
        "stringValue": str,
        "intValue": int,
        "doubleValue": float,
        "boolValue": bool,
        "bytesValue": str,
    }
    for key, converter in keys.items():
        if key in value:
            return converter(value[key])
    if "arrayValue" in value:
        return [_json_any_value(item) for item in value["arrayValue"].get("values", [])]
    if "kvlistValue" in value:
        return _json_attributes(value["kvlistValue"].get("values", []))
    return dict(value)


def _json_attributes(values: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return {str(item.get("key")): _json_any_value(item.get("value")) for item in values if item.get("key") is not None}


def normalize_otlp_json(payload: Mapping[str, Any]) -> list[Trace]:
    """Normalize an OTLP/HTTP JSON export request."""

    grouped: dict[str, list[Span]] = defaultdict(list)
    for resource_spans in payload.get("resourceSpans", []):
        resource_attributes = _json_attributes(resource_spans.get("resource", {}).get("attributes", []))
        service_name = resource_attributes.get("service.name")
        scopes = resource_spans.get(
            "scopeSpans",
            resource_spans.get("instrumentationLibrarySpans", []),
        )
        for scope_spans in scopes:
            for raw_span in scope_spans.get("spans", []):
                trace_id = normalize_id(raw_span.get("traceId"), 32)
                span_id = normalize_id(raw_span.get("spanId"), 16)
                if trace_id is None or span_id is None:
                    continue
                attributes = {
                    **resource_attributes,
                    **_json_attributes(raw_span.get("attributes", [])),
                }
                links = tuple(
                    SpanLink(
                        trace_id=normalize_id(link.get("traceId"), 32) or "",
                        span_id=normalize_id(link.get("spanId"), 16) or "",
                    )
                    for link in raw_span.get("links", [])
                )
                grouped[trace_id].append(
                    Span(
                        trace_id=trace_id,
                        span_id=span_id,
                        parent_span_id=normalize_id(
                            raw_span.get("parentSpanId") or None,
                            16,
                        ),
                        name=str(raw_span.get("name", "")),
                        kind=normalize_span_kind(raw_span.get("kind")),
                        start_time=parse_timestamp(raw_span.get("startTimeUnixNano", 0)),
                        end_time=parse_timestamp(raw_span.get("endTimeUnixNano", 0)),
                        status=normalize_status(raw_span.get("status", {}).get("code")),
                        attributes=attributes,
                        links=links,
                        service_name=str(service_name) if service_name else None,
                    )
                )
    return [Trace(trace_id=trace_id, spans=tuple(spans), raw_artifact=payload) for trace_id, spans in grouped.items()]


def _proto_value(value: Any) -> Any:
    field = value.WhichOneof("value")
    if field == "array_value":
        return [_proto_value(item) for item in value.array_value.values]
    if field == "kvlist_value":
        return {item.key: _proto_value(item.value) for item in value.kvlist_value.values}
    return getattr(value, field) if field else None


def normalize_otlp_protobuf(
    payload: bytes | ExportTraceServiceRequest,
) -> list[Trace]:
    """Normalize an OTLP protobuf export request."""

    if isinstance(payload, bytes):
        request: ExportTraceServiceRequest = ExportTraceServiceRequest.FromString(payload)
    else:
        request = payload
    grouped: dict[str, list[Span]] = defaultdict(list)
    for resource_spans in request.resource_spans:
        resource_attributes = {item.key: _proto_value(item.value) for item in resource_spans.resource.attributes}
        service_name = resource_attributes.get("service.name")
        for scope_spans in resource_spans.scope_spans:
            for raw_span in scope_spans.spans:
                trace_id = raw_span.trace_id.hex()
                span_id = raw_span.span_id.hex()
                attributes = {
                    **resource_attributes,
                    **{item.key: _proto_value(item.value) for item in raw_span.attributes},
                }
                links = tuple(
                    SpanLink(
                        trace_id=link.trace_id.hex(),
                        span_id=link.span_id.hex(),
                    )
                    for link in raw_span.links
                )
                grouped[trace_id].append(
                    Span(
                        trace_id=trace_id,
                        span_id=span_id,
                        parent_span_id=raw_span.parent_span_id.hex() or None,
                        name=raw_span.name,
                        kind=normalize_span_kind(raw_span.kind),
                        start_time=parse_timestamp(raw_span.start_time_unix_nano),
                        end_time=parse_timestamp(raw_span.end_time_unix_nano),
                        status=normalize_status(raw_span.status.code),
                        attributes=attributes,
                        links=links,
                        service_name=str(service_name) if service_name else None,
                    )
                )
    return [Trace(trace_id=trace_id, spans=tuple(spans)) for trace_id, spans in grouped.items()]
