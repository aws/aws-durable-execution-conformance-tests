# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Dash0 telemetry backend."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from aws_durable_execution_conformance_tests_otel.backends._common import (
    HttpClient,
    JsonHttpClient,
    matching_trace,
)
from aws_durable_execution_conformance_tests_otel.model import (
    TelemetryQuery,
    Trace,
    normalize_id,
)
from aws_durable_execution_conformance_tests_otel.normalizers import normalize_otlp_json
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    PollingBackend,
)


def normalize_dash0(payload: Mapping[str, Any]) -> list[Trace]:
    """Normalize the OTLP/JSON payload returned by Dash0's spans API."""

    return normalize_otlp_json(payload)


class Dash0Backend(PollingBackend):
    name = "dash0"

    def __init__(
        self,
        endpoint: str,
        token: str,
        *,
        dataset: str | None = None,
        http: HttpClient | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._endpoint = endpoint.rstrip("/")
        self._dataset = dataset
        self._headers = {"Authorization": f"Bearer {token}"}
        self._http = http or JsonHttpClient()

    def _lookup(self, query: TelemetryQuery) -> Trace | None:
        time_range = {
            "from": query.started_at.isoformat(),
            "to": query.ended_at.isoformat(),
        }
        trace_ids: list[str] = []
        if query.trace_id:
            trace_id = normalize_id(query.trace_id, 32)
            if trace_id:
                trace_ids.append(trace_id)
        else:
            for execution_arn in query.execution_arns or (query.execution_arn,):
                discovery_payload = self._get_spans(
                    time_range,
                    {
                        "filter": [
                            {
                                "key": "service.name",
                                "operator": "is",
                                "value": query.service_name,
                            },
                            {
                                "key": "durable.execution.arn",
                                "operator": "is",
                                "value": execution_arn,
                            },
                        ]
                    },
                )
                for trace in normalize_dash0(discovery_payload):
                    if trace.trace_id not in trace_ids:
                        trace_ids.append(trace.trace_id)
        if not trace_ids:
            return None

        traces: list[Trace] = []
        for trace_id in trace_ids:
            payload = self._get_spans(
                time_range,
                {
                    "filter": [
                        {
                            "key": "otel.trace.id",
                            "operator": "is",
                            "value": trace_id,
                        }
                    ]
                },
            )
            traces.extend(trace for trace in normalize_dash0(payload) if trace.trace_id == trace_id)
        return matching_trace(traces, query)

    def _get_spans(
        self,
        time_range: Mapping[str, str],
        query_body: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        body: dict[str, Any] = {
            "timeRange": time_range,
            **query_body,
            "pagination": {"limit": 100},
            "sampling": {
                "mode": "disabled",
                "timeRange": time_range,
            },
        }
        if self._dataset:
            body["dataset"] = self._dataset

        resource_spans: list[Any] = []
        seen_cursors: set[str] = set()
        endpoint = self._endpoint if self._endpoint.endswith("/api/spans") else f"{self._endpoint}/api/spans"
        while True:
            response = self._http.request_json(
                "POST",
                endpoint,
                headers=self._headers,
                body=body,
            )
            page = response.get("resourceSpans", [])
            if not isinstance(page, list):
                raise BackendError("Dash0 spans API returned a non-list resourceSpans value")
            resource_spans.extend(page)

            cursors = response.get("cursors")
            cursor = cursors.get("after") if isinstance(cursors, Mapping) else None
            if not cursor:
                break
            cursor = str(cursor)
            if cursor in seen_cursors:
                raise BackendError("Dash0 spans API returned a repeated pagination cursor")
            seen_cursors.add(cursor)
            body["pagination"]["cursor"] = cursor

        return {"resourceSpans": resource_spans}


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
        dataset = os.environ.get("DASH0_DATASET") or None
        if not token:
            raise BackendError("Dash0 requires DASH0_AUTH_TOKEN in the environment")
        if not endpoint:
            raise BackendError("Dash0 requires --otel-backend-endpoint or DASH0_API_URL")
        return Dash0Backend(endpoint, token, dataset=dataset)
