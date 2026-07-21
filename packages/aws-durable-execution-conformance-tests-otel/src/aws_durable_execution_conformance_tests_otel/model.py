# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Canonical provider-neutral telemetry model."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


def normalize_id(value: str | int | None, width: int) -> str | None:
    """Normalize hexadecimal, decimal, and X-Ray identifiers."""

    if value is None:
        return None
    if isinstance(value, int):
        return f"{value:0{width}x}"[-width:]
    text = str(value).lower().strip()
    if text.startswith("0x"):
        text = text[2:]
    if text.startswith("1-") and width == 32:
        text = text.replace("-", "")[1:]
    if text.isdigit() and len(text) > width:
        text = f"{int(text):x}"
    return text.replace("-", "").zfill(width)[-width:]


def parse_timestamp(value: Any) -> datetime:
    """Parse OTLP nanoseconds, epoch seconds, or an ISO-8601 timestamp."""

    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 1e15:
            numeric /= 1e9
        elif numeric > 1e12:
            numeric /= 1e3
        return datetime.fromtimestamp(numeric, tz=UTC)
    text = str(value or "")
    if text.isdigit():
        return parse_timestamp(int(text))
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)


@dataclass(frozen=True)
class SpanLink:
    trace_id: str
    span_id: str


@dataclass(frozen=True)
class Span:
    trace_id: str
    span_id: str
    name: str
    start_time: datetime
    end_time: datetime
    parent_span_id: str | None = None
    status: str = "UNSET"
    attributes: Mapping[str, Any] = field(default_factory=dict)
    links: tuple[SpanLink, ...] = ()
    service_name: str | None = None


@dataclass(frozen=True)
class Trace:
    trace_id: str
    spans: tuple[Span, ...]
    log_trace_ids: tuple[str, ...] = ()
    raw_artifact: Any = None


@dataclass(frozen=True)
class TelemetryQuery:
    execution_arn: str
    service_name: str
    started_at: datetime
    ended_at: datetime
    trace_id: str | None = None


def trace_to_dict(trace: Trace) -> dict[str, Any]:
    """Serialize a canonical trace for sanitized failure artifacts."""

    return {
        "trace_id": trace.trace_id,
        "log_trace_ids": list(trace.log_trace_ids),
        "spans": [span_to_dict(span) for span in trace.spans],
    }


def span_to_dict(span: Span) -> dict[str, Any]:
    """Serialize every canonical span property for generic assertions."""

    return {
        "trace_id": span.trace_id,
        "span_id": span.span_id,
        "parent_span_id": span.parent_span_id,
        "name": span.name,
        "start_time": span.start_time.isoformat(),
        "end_time": span.end_time.isoformat(),
        "status": span.status,
        "service_name": span.service_name,
        "attributes": dict(span.attributes),
        "links": [{"trace_id": link.trace_id, "span_id": link.span_id} for link in span.links],
    }
