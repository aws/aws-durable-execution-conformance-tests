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
from aws_durable_execution_conformance_tests_otel.normalizers import (
    normalize_span_kind,
    normalize_status,
)
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    BackendFeatureDisparity,
    PollingBackend,
)


def _flatten_attributes(
    values: Mapping[str, Any],
    *,
    prefix: str = "",
) -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for key, value in values.items():
        name = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            flattened.update(_flatten_attributes(value, prefix=name))
        else:
            flattened[name] = value
    return flattened


def _span_attributes(outer: Mapping[str, Any]) -> dict[str, Any]:
    attributes: dict[str, Any] = {}
    for key in ("custom", "attributes", "meta"):
        values = outer.get(key)
        if isinstance(values, Mapping):
            attributes.update(_flatten_attributes(values))
    return attributes


def _decimal_id(value: Any, width: int) -> str | None:
    normalized: str | int | None = value
    if value is not None and str(value).isdigit():
        normalized = int(value)
    return normalize_id(normalized, width)


def _trace_id(outer: Mapping[str, Any], attributes: Mapping[str, Any]) -> str | None:
    otel_trace_id = attributes.get("otel.trace_id")
    if otel_trace_id is not None:
        return normalize_id(otel_trace_id, 32)
    return _decimal_id(outer.get("trace_id", outer.get("traceId")), 32)


def normalize_datadog(payload: Mapping[str, Any]) -> list[Trace]:
    """Normalize Datadog v2 span-search events."""

    grouped: dict[str, list[Span]] = defaultdict(list)
    for item in payload.get("data", []):
        outer = item.get("attributes", {})
        if not isinstance(outer, Mapping):
            continue
        attributes = _span_attributes(outer)
        trace_id = _trace_id(outer, attributes)
        span_id = _decimal_id(
            outer.get("span_id", outer.get("spanId", item.get("id"))),
            16,
        )
        if trace_id is None or span_id is None:
            continue
        start = parse_timestamp(outer.get("start_timestamp", outer.get("start", outer.get("timestamp"))))
        end_raw = outer.get("end_timestamp", outer.get("end"))
        if end_raw is not None:
            end = parse_timestamp(end_raw)
        else:
            duration_ns = int(outer.get("duration", attributes.get("duration", 0)) or 0)
            end = start + timedelta(seconds=duration_ns / 1e9)
        parent_raw = outer.get("parent_id", outer.get("parentId"))
        parent_span_id = _decimal_id(parent_raw, 16)
        if parent_span_id == "0" * 16:
            parent_span_id = None
        grouped[trace_id].append(
            Span(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=parent_span_id,
                name=str(outer.get("resource_name", outer.get("name", ""))),
                kind=normalize_span_kind(
                    outer.get("kind") or outer.get("span.kind") or attributes.get("span.kind"),
                ),
                start_time=start,
                end_time=end,
                status=normalize_status(
                    outer.get("status") or attributes.get("otel.status_code"),
                ),
                attributes=attributes,
                service_name=str(outer.get("service") or "") or None,
            )
        )
    return [Trace(trace_id=trace_id, spans=tuple(spans), raw_artifact=payload) for trace_id, spans in grouped.items()]


class DatadogBackend(PollingBackend):
    name = "datadog"
    feature_disparities = frozenset({BackendFeatureDisparity.SPAN_LINKS})

    def __init__(
        self,
        endpoint: str,
        access_token: str,
        *,
        http: HttpClient | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._endpoint = endpoint.rstrip("/")
        self._headers = {"Authorization": f"Bearer {access_token}"}
        self._http = http or JsonHttpClient()

    def _lookup(self, query: TelemetryQuery) -> Trace | None:
        if query.trace_id:
            expected_trace_id = normalize_id(query.trace_id, 32)
            if expected_trace_id is None:
                return None
            search = f'@otel.trace_id:"{expected_trace_id}"'
        else:
            arn_search = " OR ".join(
                f'@durable.execution.arn:"{execution_arn}"'
                for execution_arn in (query.execution_arns or (query.execution_arn,))
            )
            search = f"service:{query.service_name} ({arn_search})"
        discovery_payload = self._search(query, search)

        native_trace_ids: list[str] = []
        for trace in normalize_datadog(discovery_payload):
            native_trace_id = self._native_trace_id(discovery_payload, trace.trace_id)
            if native_trace_id and native_trace_id not in native_trace_ids:
                native_trace_ids.append(native_trace_id)
        if not native_trace_ids:
            return None

        traces: list[Trace] = []
        for native_trace_id in native_trace_ids:
            trace_payload = self._search(query, f"trace_id:{native_trace_id}")
            traces.extend(normalize_datadog(trace_payload))
        return matching_trace(traces, query)

    def _search(
        self,
        query: TelemetryQuery,
        search: str,
    ) -> Mapping[str, Any]:
        body: dict[str, Any] = {
            "data": {
                "type": "search_request",
                "attributes": {
                    "filter": {
                        "query": search,
                        "from": query.started_at.isoformat(),
                        "to": query.ended_at.isoformat(),
                    },
                    "page": {"limit": 1000},
                    "sort": "timestamp",
                },
            }
        }
        data: list[Any] = []
        seen_cursors: set[str] = set()
        endpoint = (
            self._endpoint
            if self._endpoint.endswith("/api/v2/spans/events/search")
            else f"{self._endpoint}/api/v2/spans/events/search"
        )
        while True:
            response = self._http.request_json(
                "POST",
                endpoint,
                headers=self._headers,
                body=body,
            )
            page = response.get("data", [])
            if not isinstance(page, list):
                raise BackendError("Datadog span-search API returned a non-list data value")
            data.extend(page)

            meta = response.get("meta")
            page_meta = meta.get("page") if isinstance(meta, Mapping) else None
            cursor = page_meta.get("after") if isinstance(page_meta, Mapping) else None
            if not cursor:
                break
            cursor = str(cursor)
            if cursor in seen_cursors:
                raise BackendError("Datadog span-search API returned a repeated pagination cursor")
            seen_cursors.add(cursor)
            body["data"]["attributes"]["page"]["cursor"] = cursor

        return {"data": data}

    @staticmethod
    def _native_trace_id(
        payload: Mapping[str, Any],
        expected_trace_id: str,
    ) -> str | None:
        for item in payload.get("data", []):
            outer = item.get("attributes", {})
            if not isinstance(outer, Mapping):
                continue
            if _trace_id(outer, _span_attributes(outer)) == expected_trace_id:
                native_trace_id = outer.get("trace_id", outer.get("traceId"))
                if native_trace_id is not None:
                    return str(native_trace_id)
        return None


class DatadogBackendFactory:
    name = "datadog"

    def create(
        self,
        options: Mapping[str, Any],
        *,
        region: str,
    ) -> PollingBackend:
        del region
        access_token = os.environ.get("DATADOG_ACCESS_TOKEN")
        if not access_token:
            raise BackendError("Datadog requires DATADOG_ACCESS_TOKEN in the environment")
        endpoint = str(options.get("otel_backend_endpoint") or "")
        if not endpoint:
            site = os.environ.get("DD_SITE", "datadoghq.com")
            endpoint = f"https://api.{site}"
        return DatadogBackend(endpoint, access_token)
