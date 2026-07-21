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
from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.normalizers import (
    canonical_trace_from_dict,
)
from aws_durable_execution_conformance_tests_otel.polling import PollingBackend


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
