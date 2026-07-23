# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Backend query and normalization tests using fake clients."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from aws_durable_execution_conformance_tests_otel.backends.collector import (
    CollectorBackend,
)
from aws_durable_execution_conformance_tests_otel.backends.dash0 import Dash0Backend
from aws_durable_execution_conformance_tests_otel.backends.datadog import (
    DatadogBackend,
)
from aws_durable_execution_conformance_tests_otel.backends.xray import XRayBackend
from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendFeatureDisparity,
    PollingBackend,
    PollingPolicy,
)


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


@pytest.mark.parametrize(
    ("backend_type", "expected"),
    [
        (
            XRayBackend,
            frozenset({BackendFeatureDisparity.UNSET_STATUS}),
        ),
        (DatadogBackend, frozenset()),
        (Dash0Backend, frozenset()),
        (CollectorBackend, frozenset()),
    ],
)
def test_backends_declare_feature_disparities(
    backend_type: type[PollingBackend],
    expected: frozenset[BackendFeatureDisparity],
) -> None:
    assert backend_type.feature_disparities == expected


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
        batch_get_calls = 0

        def get_trace_summaries(self, **kwargs: Any) -> dict[str, Any]:
            assert kwargs["FilterExpression"] == 'service("conformance")'
            return {"TraceSummaries": [{"Id": "1-aaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbb"}]}

        def batch_get_traces(self, **kwargs: Any) -> dict[str, Any]:
            self.batch_get_calls += 1
            assert kwargs["TraceIds"]
            document = {
                "trace_id": "1-aaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbb",
                "id": "1" * 16,
                "name": "conformance",
                "start_time": 1,
                "end_time": 2,
                "metadata": {"durable.execution.arn": ("arn:stale" if self.batch_get_calls == 1 else "arn:test")},
            }
            return {"Traces": [{"Segments": [{"Document": json.dumps(document)}]}]}

    client = _XRay()
    backend = XRayBackend(client, sleep=lambda _seconds: None)
    trace = backend.find_trace(
        _query(),
        PollingPolicy(timeout_seconds=1, interval_seconds=0, max_attempts=2),
    )

    assert client.batch_get_calls == 2
    assert trace.spans[0].attributes["durable.execution.arn"] == "arn:test"


def test_xray_paginates_summaries_and_trace_batches() -> None:
    trace_ids = [f"1-aaaaaaaa-{index:024x}" for index in range(1, 7)]

    def document(trace_id: str, span_id: str, execution_arn: str) -> dict[str, Any]:
        return {
            "Traces": [
                {
                    "Segments": [
                        {
                            "Document": json.dumps(
                                {
                                    "trace_id": trace_id,
                                    "id": span_id,
                                    "name": "conformance",
                                    "start_time": 1,
                                    "end_time": 2,
                                    "metadata": {"durable.execution.arn": execution_arn},
                                }
                            )
                        }
                    ]
                }
            ]
        }

    class _XRay:
        def __init__(self) -> None:
            self.summary_calls: list[dict[str, Any]] = []
            self.batch_calls: list[dict[str, Any]] = []

        def get_trace_summaries(self, **kwargs: Any) -> dict[str, Any]:
            self.summary_calls.append(kwargs)
            if "NextToken" not in kwargs:
                return {
                    "TraceSummaries": [{"Id": trace_id} for trace_id in trace_ids[:5]],
                    "NextToken": "summary-page-2",
                }
            assert kwargs["NextToken"] == "summary-page-2"
            return {"TraceSummaries": [{"Id": trace_ids[5]}]}

        def batch_get_traces(self, **kwargs: Any) -> dict[str, Any]:
            self.batch_calls.append(kwargs)
            requested_ids = kwargs["TraceIds"]
            if requested_ids == trace_ids[:5]:
                response: dict[str, Any] = {"Traces": []}
                for index, trace_id in enumerate(requested_ids, start=1):
                    response["Traces"].extend(document(trace_id, f"{index:016x}", "arn:stale")["Traces"])
                return response
            assert requested_ids == trace_ids[5:]
            if "NextToken" not in kwargs:
                return {
                    **document(trace_ids[5], "6" * 16, "arn:stale"),
                    "NextToken": "trace-page-2",
                }
            assert kwargs["NextToken"] == "trace-page-2"
            return document(trace_ids[5], "7" * 16, "arn:test")

    client = _XRay()
    backend = XRayBackend(client, sleep=lambda _seconds: None)

    trace = backend.find_trace(
        _query(),
        PollingPolicy(timeout_seconds=1, interval_seconds=0, max_attempts=1),
    )

    assert trace.trace_id == "aaaaaaaa000000000000000000000006"
    assert len(trace.spans) == 2
    assert len(client.summary_calls) == 2
    assert len(client.batch_calls) == 3
