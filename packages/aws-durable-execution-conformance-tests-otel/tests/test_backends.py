# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Backend query and normalization tests using fake clients."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from aws_durable_execution_conformance_tests_otel.backends.dash0 import Dash0Backend
from aws_durable_execution_conformance_tests_otel.backends.datadog import (
    DatadogBackend,
)
from aws_durable_execution_conformance_tests_otel.backends.xray import XRayBackend
from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery
from aws_durable_execution_conformance_tests_otel.polling import PollingPolicy


class _Http:
    def __init__(self, response: Mapping[str, Any]) -> None:
        self.response = response
        self.calls: list[tuple[str, str, Mapping[str, Any] | None]] = []

    def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        del headers
        self.calls.append((method, url, body))
        return self.response


def _query() -> TelemetryQuery:
    now = datetime.now(UTC)
    return TelemetryQuery(
        execution_arn="arn:test",
        service_name="conformance",
        started_at=now - timedelta(minutes=1),
        ended_at=now + timedelta(minutes=1),
    )


def test_datadog_queries_span_search_and_correlates_execution() -> None:
    http = _Http(
        {
            "data": [
                {
                    "id": "7",
                    "attributes": {
                        "trace_id": "10",
                        "span_id": "7",
                        "service": "conformance",
                        "resource_name": "step",
                        "start_timestamp": "2026-01-01T00:00:00Z",
                        "duration": 100,
                        "attributes": {"durable.execution.arn": "arn:test"},
                    },
                }
            ]
        }
    )
    backend = DatadogBackend(
        "https://api.datadoghq.com",
        "api-secret",
        "app-secret",
        http=http,
        sleep=lambda _seconds: None,
    )

    trace = backend.find_trace(
        _query(),
        PollingPolicy(timeout_seconds=1, interval_seconds=0, max_attempts=1),
    )

    assert trace.spans[0].service_name == "conformance"
    assert http.calls[0][0] == "POST"
    assert "/api/v2/spans/events/search" in http.calls[0][1]
    assert "arn:test" in str(http.calls[0][2])


def test_dash0_queries_trace_api() -> None:
    http = _Http(
        {
            "spans": [
                {
                    "traceId": "1" * 32,
                    "spanId": "2" * 16,
                    "name": "step",
                    "start_time": "2026-01-01T00:00:00Z",
                    "end_time": "2026-01-01T00:00:01Z",
                    "serviceName": "conformance",
                    "attributes": {"durable.execution.arn": "arn:test"},
                }
            ]
        }
    )
    backend = Dash0Backend(
        "https://api.dash0.example",
        "secret",
        http=http,
        sleep=lambda _seconds: None,
    )

    trace = backend.find_trace(
        _query(),
        PollingPolicy(timeout_seconds=1, interval_seconds=0, max_attempts=1),
    )

    assert trace.trace_id == "1" * 32
    assert "durable.execution.arn=arn%3Atest" in http.calls[0][1]


def test_xray_queries_summaries_then_batch_get() -> None:
    class _XRay:
        def get_trace_summaries(self, **kwargs: Any) -> dict[str, Any]:
            assert kwargs["FilterExpression"] == 'service("conformance")'
            return {"TraceSummaries": [{"Id": "1-aaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbb"}]}

        def batch_get_traces(self, **kwargs: Any) -> dict[str, Any]:
            assert kwargs["TraceIds"]
            document = {
                "trace_id": "1-aaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbb",
                "id": "1" * 16,
                "name": "conformance",
                "start_time": 1,
                "end_time": 2,
                "metadata": {"default": {"durable.execution.arn": "arn:test"}},
            }
            return {"Traces": [{"Segments": [{"Document": json.dumps(document)}]}]}

    backend = XRayBackend(_XRay(), sleep=lambda _seconds: None)
    trace = backend.find_trace(
        _query(),
        PollingPolicy(timeout_seconds=1, interval_seconds=0, max_attempts=1),
    )

    assert trace.spans[0].attributes["durable.execution.arn"] == "arn:test"
