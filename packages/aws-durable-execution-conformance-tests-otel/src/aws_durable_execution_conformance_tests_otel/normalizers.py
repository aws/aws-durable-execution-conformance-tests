# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Provider response normalizers for the canonical telemetry model."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import timedelta
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


def _status_name(value: Any) -> str:
    text = str(value or "UNSET").upper()
    if text in {"1", "STATUS_CODE_OK", "OK"}:
        return "OK"
    if text in {"2", "STATUS_CODE_ERROR", "ERROR", "FAILURE", "FAILED"}:
        return "ERROR"
    return "UNSET"


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
                        start_time=parse_timestamp(raw_span.get("startTimeUnixNano", 0)),
                        end_time=parse_timestamp(raw_span.get("endTimeUnixNano", 0)),
                        status=_status_name(raw_span.get("status", {}).get("code")),
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
                        start_time=parse_timestamp(raw_span.start_time_unix_nano),
                        end_time=parse_timestamp(raw_span.end_time_unix_nano),
                        status=_status_name(raw_span.status.code),
                        attributes=attributes,
                        links=links,
                        service_name=str(service_name) if service_name else None,
                    )
                )
    return [Trace(trace_id=trace_id, spans=tuple(spans)) for trace_id, spans in grouped.items()]


def normalize_xray(documents: Iterable[str | Mapping[str, Any]]) -> list[Trace]:
    """Normalize AWS X-Ray segment documents."""

    grouped: dict[str, list[Span]] = defaultdict(list)

    def ingest(
        segment: Mapping[str, Any],
        trace_id: str,
        service_name: str | None,
    ) -> None:
        span_id = normalize_id(segment.get("id"), 16)
        if span_id is None:
            return
        start = parse_timestamp(segment.get("start_time", 0))
        end = parse_timestamp(segment.get("end_time", segment.get("start_time", 0)))
        attributes = {
            **dict(segment.get("annotations", {})),
            **{f"xray.{key}": value for key, value in segment.get("metadata", {}).items()},
        }
        status = "ERROR" if any(segment.get(flag) for flag in ("error", "fault", "throttle")) else "OK"
        grouped[trace_id].append(
            Span(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=normalize_id(segment.get("parent_id"), 16),
                name=str(segment.get("name", "")),
                start_time=start,
                end_time=end,
                status=status,
                attributes=attributes,
                service_name=service_name,
            )
        )
        for child in segment.get("subsegments", []):
            ingest(child, trace_id, service_name)

    raw_documents = list(documents)
    for document in raw_documents:
        segment = json.loads(document) if isinstance(document, str) else document
        trace_id = normalize_id(segment.get("trace_id"), 32)
        if trace_id is None:
            continue
        ingest(segment, trace_id, str(segment.get("name") or "") or None)
    return [
        Trace(trace_id=trace_id, spans=tuple(spans), raw_artifact=raw_documents) for trace_id, spans in grouped.items()
    ]


def normalize_datadog(payload: Mapping[str, Any]) -> list[Trace]:
    """Normalize Datadog v2 span-search events."""

    grouped: dict[str, list[Span]] = defaultdict(list)
    for item in payload.get("data", []):
        outer = item.get("attributes", {})
        attributes = dict(outer.get("attributes", outer.get("meta", {})) or {})
        trace_raw = outer.get("trace_id", outer.get("traceId"))
        span_raw = outer.get("span_id", outer.get("spanId", item.get("id")))
        try:
            trace_value: str | int | None = int(trace_raw) if str(trace_raw).isdigit() else trace_raw
            span_value: str | int | None = int(span_raw) if str(span_raw).isdigit() else span_raw
        except (TypeError, ValueError):
            continue
        trace_id = normalize_id(trace_value, 32)
        span_id = normalize_id(span_value, 16)
        if trace_id is None or span_id is None:
            continue
        start = parse_timestamp(outer.get("start_timestamp", outer.get("start", outer.get("timestamp"))))
        duration_ns = int(outer.get("duration", 0) or 0)
        end = start + timedelta(seconds=duration_ns / 1e9)
        parent_raw = outer.get("parent_id", outer.get("parentId"))
        parent_value = int(parent_raw) if parent_raw is not None and str(parent_raw).isdigit() else parent_raw
        grouped[trace_id].append(
            Span(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=normalize_id(parent_value, 16),
                name=str(outer.get("resource_name", outer.get("name", ""))),
                start_time=start,
                end_time=end,
                status="ERROR" if outer.get("status") == "error" else "OK",
                attributes=attributes,
                service_name=str(outer.get("service") or "") or None,
            )
        )
    return [Trace(trace_id=trace_id, spans=tuple(spans), raw_artifact=payload) for trace_id, spans in grouped.items()]


def normalize_dash0(payload: Mapping[str, Any]) -> list[Trace]:
    """Normalize Dash0 OTLP-shaped trace search responses."""

    if "resourceSpans" in payload:
        return normalize_otlp_json(payload)
    if isinstance(payload.get("trace"), Mapping):
        return normalize_otlp_json(payload["trace"])

    grouped: dict[str, list[Span]] = defaultdict(list)
    raw_spans = payload.get("spans", payload.get("data", []))
    for raw_span in raw_spans:
        trace_id = normalize_id(raw_span.get("traceId", raw_span.get("trace_id")), 32)
        span_id = normalize_id(raw_span.get("spanId", raw_span.get("span_id")), 16)
        if trace_id is None or span_id is None:
            continue
        attributes = dict(raw_span.get("attributes", {}))
        grouped[trace_id].append(
            Span(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=normalize_id(
                    raw_span.get("parentSpanId", raw_span.get("parent_span_id")),
                    16,
                ),
                name=str(raw_span.get("name", "")),
                start_time=parse_timestamp(raw_span.get("startTimeUnixNano", raw_span.get("start_time"))),
                end_time=parse_timestamp(raw_span.get("endTimeUnixNano", raw_span.get("end_time"))),
                status=_status_name(raw_span.get("status")),
                attributes=attributes,
                service_name=raw_span.get("serviceName") or attributes.get("service.name"),
            )
        )
    return [Trace(trace_id=trace_id, spans=tuple(spans), raw_artifact=payload) for trace_id, spans in grouped.items()]


def canonical_trace_from_dict(payload: Mapping[str, Any]) -> Trace:
    """Deserialize the test collector's canonical JSON representation."""

    spans = tuple(
        Span(
            trace_id=str(item["trace_id"]),
            span_id=str(item["span_id"]),
            parent_span_id=item.get("parent_span_id"),
            name=str(item.get("name", "")),
            start_time=parse_timestamp(item["start_time"]),
            end_time=parse_timestamp(item["end_time"]),
            status=str(item.get("status", "UNSET")),
            service_name=item.get("service_name"),
            attributes=dict(item.get("attributes", {})),
            links=tuple(
                SpanLink(trace_id=str(link["trace_id"]), span_id=str(link["span_id"])) for link in item.get("links", [])
            ),
        )
        for item in payload.get("spans", [])
    )
    return Trace(
        trace_id=str(payload["trace_id"]),
        spans=spans,
        log_trace_ids=tuple(payload.get("log_trace_ids", [])),
        raw_artifact=payload,
    )
