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
from urllib.parse import quote

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

DATADOG_RETENTION_FILTER_NAME = "durable-execution-conformance-tests"
DATADOG_SERVICE_NAME = "durable-execution-conformance"


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
    first_invocation = attributes.get("durable.invocation.first")
    if first_invocation == "true":
        attributes["durable.invocation.first"] = True
    elif first_invocation == "false":
        attributes["durable.invocation.first"] = False
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


def configure_datadog_retention(
    endpoint: str,
    api_key: str,
    application_key: str,
    *,
    service_name: str = DATADOG_SERVICE_NAME,
    http: HttpClient | None = None,
) -> str:
    """Create or update the conformance service's complete-trace retention filter."""

    client = http or JsonHttpClient()
    headers = {
        "DD-API-KEY": api_key,
        "DD-APPLICATION-KEY": application_key,
    }
    base_endpoint = endpoint.rstrip("/")
    filters_endpoint = (
        base_endpoint
        if base_endpoint.endswith("/api/v2/apm/config/retention-filters")
        else f"{base_endpoint}/api/v2/apm/config/retention-filters"
    )
    response = client.request_json("GET", filters_endpoint, headers=headers)
    filters = response.get("data", [])
    if not isinstance(filters, list):
        raise BackendError("Datadog retention-filter API returned a non-list data value")

    existing_id: str | None = None
    for item in filters:
        if not isinstance(item, Mapping):
            continue
        attributes = item.get("attributes")
        if not isinstance(attributes, Mapping) or attributes.get("name") != DATADOG_RETENTION_FILTER_NAME:
            continue
        filter_id = item.get("id")
        if filter_id is None:
            raise BackendError("Datadog retention filter is missing its id")
        existing_id = str(filter_id)
        break

    body: dict[str, Any] = {
        "data": {
            "type": "apm_retention_filter",
            "attributes": {
                "enabled": True,
                "filter": {"query": f"service:{service_name}"},
                "filter_type": "spans-sampling-processor",
                "name": DATADOG_RETENTION_FILTER_NAME,
                "rate": 1.0,
                "trace_rate": 1.0,
            },
        }
    }
    if existing_id is None:
        client.request_json("POST", filters_endpoint, headers=headers, body=body)
        return "created"

    body["data"]["id"] = existing_id
    client.request_json(
        "PUT",
        f"{filters_endpoint}/{quote(existing_id, safe='')}",
        headers=headers,
        body=body,
    )
    return "updated"


class DatadogBackend(PollingBackend):
    name = "datadog"
    feature_disparities = frozenset(
        {
            BackendFeatureDisparity.SPAN_LINKS,
            BackendFeatureDisparity.UNSET_STATUS,
        }
    )

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
        self._span_cache: dict[tuple[str, str], Span] = {}

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
        return matching_trace(self._merge_search_results(self._search(query, search)), query)

    def _merge_search_results(self, payload: Mapping[str, Any]) -> list[Trace]:
        for trace in normalize_datadog(payload):
            for span in trace.spans:
                self._span_cache[(span.trace_id, span.span_id)] = span

        grouped: dict[str, list[Span]] = defaultdict(list)
        for span in self._span_cache.values():
            grouped[span.trace_id].append(span)
        return [
            Trace(trace_id=trace_id, spans=tuple(spans), raw_artifact=payload) for trace_id, spans in grouped.items()
        ]

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
