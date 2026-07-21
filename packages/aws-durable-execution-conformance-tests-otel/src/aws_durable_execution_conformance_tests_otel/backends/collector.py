# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Test OTLP collector telemetry backend."""

from __future__ import annotations

import urllib.parse
from collections.abc import Mapping
from typing import Any

from aws_durable_execution_conformance_tests_otel.backends._common import (
    HttpClient,
    JsonHttpClient,
)
from aws_durable_execution_conformance_tests_otel.model import (
    Span,
    SpanLink,
    TelemetryQuery,
    Trace,
    parse_timestamp,
)
from aws_durable_execution_conformance_tests_otel.polling import PollingBackend


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


class CollectorBackend(PollingBackend):
    name = "collector"

    def __init__(
        self,
        endpoint: str,
        *,
        http: HttpClient | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._endpoint = endpoint.rstrip("/")
        self._http = http or JsonHttpClient()

    def _lookup(self, query: TelemetryQuery) -> Trace | None:
        params = urllib.parse.urlencode(
            {
                "execution_arn": query.execution_arn,
                "service_name": query.service_name,
                **({"trace_id": query.trace_id} if query.trace_id else {}),
            }
        )
        response = self._http.request_json(
            "GET",
            f"{self._endpoint}/api/traces?{params}",
        )
        trace_payload = response.get("trace")
        if not isinstance(trace_payload, Mapping):
            return None
        return canonical_trace_from_dict(trace_payload)


class CollectorBackendFactory:
    name = "collector"

    def create(
        self,
        options: Mapping[str, Any],
        *,
        region: str,
    ) -> PollingBackend:
        del region
        endpoint = str(options.get("otel_backend_endpoint") or options.get("otel_endpoint") or "http://127.0.0.1:4318")
        return CollectorBackend(endpoint)
