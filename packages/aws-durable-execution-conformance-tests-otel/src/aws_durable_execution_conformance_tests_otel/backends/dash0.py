# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Dash0 telemetry backend."""

from __future__ import annotations

import os
import urllib.parse
from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from aws_durable_execution_conformance_tests_otel.backends._common import (
    HttpClient,
    JsonHttpClient,
    matching_trace,
)
from aws_durable_execution_conformance_tests_otel.model import (
    Span,
    TelemetryQuery,
    Trace,
    normalize_id,
    parse_timestamp,
)
from aws_durable_execution_conformance_tests_otel.normalizers import (
    normalize_otlp_json,
    normalize_span_kind,
    normalize_status,
)
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    BackendFeatureDisparity,
    PollingBackend,
)


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
                kind=normalize_span_kind(
                    raw_span.get("kind") or raw_span.get("spanKind") or attributes.get("span.kind"),
                ),
                start_time=parse_timestamp(raw_span.get("startTimeUnixNano", raw_span.get("start_time"))),
                end_time=parse_timestamp(raw_span.get("endTimeUnixNano", raw_span.get("end_time"))),
                status=normalize_status(raw_span.get("status")),
                attributes=attributes,
                service_name=raw_span.get("serviceName") or attributes.get("service.name"),
            )
        )
    return [Trace(trace_id=trace_id, spans=tuple(spans), raw_artifact=payload) for trace_id, spans in grouped.items()]


class Dash0Backend(PollingBackend):
    name = "dash0"
    feature_disparities = frozenset({BackendFeatureDisparity.SPAN_LINKS})

    def __init__(
        self,
        endpoint: str,
        token: str,
        *,
        http: HttpClient | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._endpoint = endpoint.rstrip("/")
        self._headers = {"Authorization": f"Bearer {token}"}
        self._http = http or JsonHttpClient()

    def _lookup(self, query: TelemetryQuery) -> Trace | None:
        params = urllib.parse.urlencode(
            {
                "service.name": query.service_name,
                "durable.execution.arn": query.execution_arn,
                "from": query.started_at.isoformat(),
                "to": query.ended_at.isoformat(),
            }
        )
        response = self._http.request_json(
            "GET",
            f"{self._endpoint}/api/traces?{params}",
            headers=self._headers,
        )
        return matching_trace(normalize_dash0(response), query)


class Dash0BackendFactory:
    name = "dash0"

    def create(
        self,
        options: Mapping[str, Any],
        *,
        region: str,
    ) -> PollingBackend:
        del region
        token = os.environ.get("DASH0_AUTH_TOKEN")
        endpoint = str(options.get("otel_backend_endpoint") or os.environ.get("DASH0_API_URL", ""))
        if not token:
            raise BackendError("Dash0 requires DASH0_AUTH_TOKEN in the environment")
        if not endpoint:
            raise BackendError("Dash0 requires --otel-backend-endpoint or DASH0_API_URL")
        return Dash0Backend(endpoint, token)
