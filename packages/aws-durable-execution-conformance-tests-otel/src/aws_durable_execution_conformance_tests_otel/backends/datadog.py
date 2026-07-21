# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Datadog telemetry backend."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from aws_durable_execution_conformance_tests_otel.backends._common import (
    HttpClient,
    JsonHttpClient,
    matching_trace,
)
from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.normalizers import (
    normalize_datadog,
)
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    PollingBackend,
)


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
