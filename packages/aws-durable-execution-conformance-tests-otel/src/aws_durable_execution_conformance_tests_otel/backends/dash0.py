# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Dash0 telemetry backend."""

from __future__ import annotations

import os
import urllib.parse
from collections.abc import Mapping
from typing import Any

from aws_durable_execution_conformance_tests_otel.backends._common import (
    HttpClient,
    JsonHttpClient,
    matching_trace,
)
from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.normalizers import normalize_dash0
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    PollingBackend,
)


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
