# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Datadog telemetry backend."""

from __future__ import annotations

import os
from collections import defaultdict
from collections.abc import Mapping
from datetime import timedelta
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
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    BackendFeatureDisparity,
    PollingBackend,
)


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


class DatadogBackend(PollingBackend):
    name = "datadog"
    feature_disparities: frozenset[BackendFeatureDisparity] = frozenset()

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        application_key: str,
        *,
        http: HttpClient | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._endpoint = endpoint.rstrip("/")
        self._headers = {
            "DD-API-KEY": api_key,
            "DD-APPLICATION-KEY": application_key,
        }
        self._http = http or JsonHttpClient()

    def _lookup(self, query: TelemetryQuery) -> Trace | None:
        search = f'service:{query.service_name} @durable.execution.arn:"{query.execution_arn}"'
        response = self._http.request_json(
            "POST",
            f"{self._endpoint}/api/v2/spans/events/search",
            headers=self._headers,
            body={
                "filter": {
                    "query": search,
                    "from": query.started_at.isoformat(),
                    "to": query.ended_at.isoformat(),
                },
                "page": {"limit": 1000},
                "sort": "timestamp",
            },
        )
        return matching_trace(normalize_datadog(response), query)


class DatadogBackendFactory:
    name = "datadog"

    def create(
        self,
        options: Mapping[str, Any],
        *,
        region: str,
    ) -> PollingBackend:
        del region
        api_key = os.environ.get("DD_API_KEY")
        application_key = os.environ.get("DD_APPLICATION_KEY")
        if not api_key or not application_key:
            raise BackendError("Datadog requires DD_API_KEY and DD_APPLICATION_KEY in the environment")
        endpoint = str(options.get("otel_backend_endpoint") or "")
        if not endpoint:
            site = os.environ.get("DD_SITE", "datadoghq.com")
            endpoint = f"https://api.{site}"
        return DatadogBackend(endpoint, api_key, application_key)
