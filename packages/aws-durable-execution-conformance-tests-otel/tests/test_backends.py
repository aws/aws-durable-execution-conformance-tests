# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Backend query and normalization tests using fake clients."""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from aws_durable_execution_conformance_tests_otel.backends._common import (
    matching_trace,
)
from aws_durable_execution_conformance_tests_otel.backends.collector import (
    CollectorBackend,
)
from aws_durable_execution_conformance_tests_otel.backends.dash0 import (
    Dash0Backend,
    Dash0BackendFactory,
)
from aws_durable_execution_conformance_tests_otel.backends.datadog import (
    DatadogBackend,
)
from aws_durable_execution_conformance_tests_otel.backends.xray import (
    XRayBackend,
    XRayBackendFactory,
)
from aws_durable_execution_conformance_tests_otel.model import Span, TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendFeatureDisparity,
    PollingBackend,
    PollingPolicy,
)


class _Http:
    def __init__(self, *responses: Mapping[str, Any]) -> None:
        self.responses = list(responses)
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
        self.calls.append((method, url, copy.deepcopy(body)))
        return self.responses.pop(0)


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
            frozenset(
                {
                    BackendFeatureDisparity.SPAN_LINKS,
                    BackendFeatureDisparity.UNSET_STATUS,
                }
            ),
        ),
        (DatadogBackend, frozenset({BackendFeatureDisparity.SPAN_LINKS})),
        (Dash0Backend, frozenset()),
        (CollectorBackend, frozenset()),
    ],
)
def test_backends_declare_feature_disparities(
    backend_type: type[PollingBackend],
    expected: frozenset[BackendFeatureDisparity],
) -> None:
    assert backend_type.feature_disparities == expected


@pytest.mark.parametrize(
    ("language", "exporter", "expected"),
    [
        ("java", "adot", True),
        ("python", "adot", False),
        ("javascript", "adot", False),
        ("java", "community", False),
    ],
)
def test_xray_plugin_mode_service_name_is_scoped_to_java_adot(
    language: str,
    exporter: str,
    expected: bool,
) -> None:
    backend = XRayBackendFactory().create_with_clients(
        {
            "language": language,
            "otel_exporter": exporter,
        },
        region="us-west-2",
        aws_clients={"xray": object()},
    )

    assert (BackendFeatureDisparity.PLUGIN_MODE_SERVICE_NAME in backend.feature_disparities) is expected


def test_matching_trace_collects_ambient_invocations_by_execution_arn() -> None:
    now = datetime.now(UTC)

    def span(
        trace_id: str,
        span_id: str,
        name: str,
        execution_arn: str | None,
    ) -> Span:
        return Span(
            trace_id=trace_id,
            span_id=span_id,
            name=name,
            start_time=now,
            end_time=now,
            attributes=({"durable.execution.arn": execution_arn} if execution_arn else {}),
        )

    source_trace_id = "1" * 32
    source_invocation_trace_id = "2" * 32
    target_invocation_trace_id = "3" * 32
    workflow_trace = Trace(
        trace_id=source_trace_id,
        spans=(
            span(source_trace_id, "1" * 16, "Workflow", "arn:test"),
            span(source_trace_id, "2" * 16, "Workflow", "arn:target"),
        ),
    )
    source_invocation_trace = Trace(
        trace_id=source_invocation_trace_id,
        spans=(
            span(source_invocation_trace_id, "3" * 16, "handler", None),
            span(source_invocation_trace_id, "4" * 16, "Invocation", "arn:test"),
        ),
    )
    target_invocation_trace = Trace(
        trace_id=target_invocation_trace_id,
        spans=(span(target_invocation_trace_id, "5" * 16, "Invocation", "arn:target"),),
    )
    unrelated_trace = Trace(
        trace_id="4" * 32,
        spans=(span("4" * 32, "6" * 16, "Invocation", "arn:other"),),
    )

    result = matching_trace(
        [
            source_invocation_trace,
            unrelated_trace,
            workflow_trace,
            target_invocation_trace,
        ],
        replace(
            _query(),
            execution_arns=("arn:test", "arn:target"),
        ),
    )

    assert result is not None
    assert result.trace_id == source_trace_id
    assert [(item.name, item.attributes["durable.execution.arn"]) for item in result.spans] == [
        ("Workflow", "arn:test"),
        ("Workflow", "arn:target"),
        ("Invocation", "arn:test"),
        ("Invocation", "arn:target"),
    ]
    assert {item.trace_id for item in result.spans} == {
        source_trace_id,
        source_invocation_trace_id,
        target_invocation_trace_id,
    }


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
        replace(
            _query(),
            execution_arns=("arn:test", "arn:target"),
        ),
        PollingPolicy(timeout_seconds=1, interval_seconds=0, max_attempts=1),
    )

    assert trace.spans[0].service_name == "conformance"
    assert http.calls[0][0] == "POST"
    assert "/api/v2/spans/events/search" in http.calls[0][1]
    assert "arn:test" in str(http.calls[0][2])
    assert "arn:target" in str(http.calls[0][2])


def test_dash0_queries_trace_api() -> None:
    trace_payload = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "conformance"},
                        }
                    ]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "EREREREREREREREREREREQ==",
                                "spanId": "IiIiIiIiIiI=",
                                "name": "step",
                                "startTimeUnixNano": "1767225600000000000",
                                "endTimeUnixNano": "1767225601000000000",
                                "attributes": [
                                    {
                                        "key": "durable.execution.arn",
                                        "value": {"stringValue": "arn:test"},
                                    }
                                ],
                            }
                        ]
                    }
                ],
            }
        ],
    }
    http = _Http(
        {
            **trace_payload,
            "cursors": {"after": "page-2"},
        },
        {
            "resourceSpans": [],
        },
        trace_payload,
    )
    backend = Dash0Backend(
        "https://api.dash0.example",
        "secret",
        dataset="conformance",
        http=http,
        sleep=lambda _seconds: None,
    )
    query = _query()

    trace = backend.find_trace(
        query,
        PollingPolicy(timeout_seconds=1, interval_seconds=0, max_attempts=1),
    )

    assert trace.trace_id == "1" * 32
    assert [call[0] for call in http.calls] == ["POST", "POST", "POST"]
    assert all(call[1] == "https://api.dash0.example/api/spans" for call in http.calls)
    assert http.calls[0][2] == {
        "dataset": "conformance",
        "filter": [
            {"key": "service.name", "operator": "is", "value": "conformance"},
            {
                "key": "durable.execution.arn",
                "operator": "is",
                "value": "arn:test",
            },
        ],
        "pagination": {"limit": 100},
        "sampling": {
            "mode": "disabled",
            "timeRange": {
                "from": query.started_at.isoformat(),
                "to": query.ended_at.isoformat(),
            },
        },
        "timeRange": {
            "from": query.started_at.isoformat(),
            "to": query.ended_at.isoformat(),
        },
    }
    second_body = http.calls[1][2]
    assert second_body is not None
    assert second_body["pagination"]["cursor"] == "page-2"
    third_body = http.calls[2][2]
    assert third_body is not None
    assert third_body["filter"] == [
        {
            "key": "otel.trace.id",
            "operator": "is",
            "value": "1" * 32,
        }
    ]


def test_dash0_factory_uses_environment_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DASH0_AUTH_TOKEN", "secret")
    monkeypatch.setenv("DASH0_API_URL", "https://api.us-west-2.aws.dash0.com")
    monkeypatch.setenv("DASH0_DATASET", "conformance")

    backend = Dash0BackendFactory().create({}, region="us-west-2")

    assert isinstance(backend, Dash0Backend)
    assert backend._endpoint == "https://api.us-west-2.aws.dash0.com"
    assert backend._dataset == "conformance"


def test_dash0_discovers_and_correlates_all_execution_arns() -> None:
    def trace_payload(
        trace_id: str,
        span_id: str,
        execution_arn: str,
    ) -> Mapping[str, Any]:
        return {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name",
                                "value": {"stringValue": "conformance"},
                            }
                        ]
                    },
                    "scopeSpans": [
                        {
                            "spans": [
                                {
                                    "traceId": trace_id,
                                    "spanId": span_id,
                                    "name": "Invocation",
                                    "startTimeUnixNano": "1767225600000000000",
                                    "endTimeUnixNano": "1767225601000000000",
                                    "attributes": [
                                        {
                                            "key": "durable.execution.arn",
                                            "value": {"stringValue": execution_arn},
                                        }
                                    ],
                                }
                            ]
                        }
                    ],
                }
            ]
        }

    source_payload = trace_payload(
        "EREREREREREREREREREREQ==",
        "IiIiIiIiIiI=",
        "arn:test",
    )
    target_payload = trace_payload(
        "MzMzMzMzMzMzMzMzMzMzMw==",
        "REREREREREQ=",
        "arn:target",
    )
    http = _Http(
        source_payload,
        target_payload,
        source_payload,
        target_payload,
    )
    backend = Dash0Backend(
        "https://api.dash0.example",
        "secret",
        http=http,
        sleep=lambda _seconds: None,
    )

    trace = backend.find_trace(
        replace(
            _query(),
            execution_arns=("arn:test", "arn:target"),
        ),
        PollingPolicy(timeout_seconds=1, interval_seconds=0, max_attempts=1),
    )

    assert trace.trace_id == "1" * 32
    assert [call[2]["filter"][1]["value"] for call in http.calls[:2] if call[2]] == [
        "arn:test",
        "arn:target",
    ]
    assert [call[2]["filter"][0]["value"] for call in http.calls[2:] if call[2]] == [
        "1" * 32,
        "3" * 32,
    ]
    assert {span.attributes["durable.execution.arn"] for span in trace.spans} == {
        "arn:test",
        "arn:target",
    }


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
