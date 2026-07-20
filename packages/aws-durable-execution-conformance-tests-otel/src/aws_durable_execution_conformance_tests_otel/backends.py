# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Hosted and test OpenTelemetry backend adapters."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from typing import Any, Protocol

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from aws_durable_execution_conformance_tests_otel.model import (
    TelemetryQuery,
    Trace,
    normalize_id,
)
from aws_durable_execution_conformance_tests_otel.normalizers import (
    canonical_trace_from_dict,
    normalize_dash0,
    normalize_datadog,
    normalize_xray,
)
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    PollingBackend,
)
from aws_durable_execution_conformance_tests_otel.redaction import redact


class HttpClient(Protocol):
    def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        """Send an HTTP request and parse a JSON object."""


class JsonHttpClient:
    """Small stdlib JSON client that keeps headers out of diagnostics."""

    def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "application/json",
                **({"Content-Type": "application/json"} if body is not None else {}),
                **dict(headers or {}),
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                payload = json.loads(response.read() or b"{}")
        except (json.JSONDecodeError, urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            safe_url = redact(url)
            raise BackendError(f"Telemetry backend request to {safe_url!r} failed: {type(exc).__name__}") from exc
        if not isinstance(payload, Mapping):
            raise BackendError("Telemetry backend returned a non-object JSON response")
        return payload


def _matching_trace(traces: list[Trace], query: TelemetryQuery) -> Trace | None:
    if query.trace_id:
        expected = normalize_id(query.trace_id, 32)
        return next((trace for trace in traces if trace.trace_id == expected), None)
    for trace in traces:
        for span in trace.spans:
            if query.execution_arn in {str(value) for value in span.attributes.values()}:
                return trace
    service_matches = [
        trace for trace in traces if any(span.service_name == query.service_name for span in trace.spans)
    ]
    if len(service_matches) == 1:
        return service_matches[0]
    return traces[0] if len(traces) == 1 else None


class XRayBackend(PollingBackend):
    name = "xray"

    def __init__(self, client: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._client = client

    def _lookup(self, query: TelemetryQuery) -> Trace | None:
        try:
            trace_ids: list[str] = []
            if query.trace_id:
                trace_ids = [query.trace_id]
            else:
                response = self._client.get_trace_summaries(
                    StartTime=query.started_at,
                    EndTime=query.ended_at,
                    FilterExpression=f'service("{query.service_name}")',
                )
                trace_ids = [item["Id"] for item in response.get("TraceSummaries", []) if item.get("Id")]
            if not trace_ids:
                return None
            response = self._client.batch_get_traces(TraceIds=trace_ids[:5])
        except (BotoCoreError, ClientError) as exc:
            raise BackendError(f"X-Ray telemetry query failed: {type(exc).__name__}") from exc
        documents = [
            segment["Document"]
            for trace in response.get("Traces", [])
            for segment in trace.get("Segments", [])
            if segment.get("Document")
        ]
        return _matching_trace(normalize_xray(documents), query)


class DatadogBackend(PollingBackend):
    name = "datadog"

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
        return _matching_trace(normalize_datadog(response), query)


class Dash0Backend(PollingBackend):
    name = "dash0"

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
        return _matching_trace(normalize_dash0(response), query)


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


class XRayBackendFactory:
    name = "xray"

    def create(
        self,
        options: Mapping[str, Any],
        *,
        region: str,
    ) -> PollingBackend:
        return XRayBackend(boto3.client("xray", region_name=region))


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


BUILTIN_BACKENDS = {
    "xray": XRayBackendFactory,
    "datadog": DatadogBackendFactory,
    "dash0": Dash0BackendFactory,
    "collector": CollectorBackendFactory,
}
