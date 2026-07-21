# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Provider-neutral telemetry validator and redaction tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

from aws_durable_execution_conformance_tests_otel.model import (
    Span,
    SpanLink,
    TelemetryQuery,
    Trace,
)
from aws_durable_execution_conformance_tests_otel.redaction import REDACTED, redact
from aws_durable_execution_conformance_tests_otel.validators import validate_trace


def _trace(execution_arn: str = "arn:test") -> Trace:
    now = datetime.now(UTC)
    root = Span(
        trace_id="1" * 32,
        span_id="2" * 16,
        name="root",
        start_time=now,
        end_time=now,
        status="OK",
        service_name="service",
        attributes={
            "durable.execution.arn": execution_arn,
            "faas.invocation_id": "invocation-1",
            "durable.operation.outcome": "retry",
        },
    )
    child = Span(
        trace_id="1" * 32,
        span_id="3" * 16,
        parent_span_id=root.span_id,
        name="child",
        start_time=now,
        end_time=now,
        status="OK",
        service_name="service",
        attributes={
            "durable.execution.arn": execution_arn,
            "faas.invocation_id": "invocation-2",
            "custom.metadata": {
                "attempt": 2,
                "labels": ["durable", "resumed"],
            },
        },
        links=(SpanLink(trace_id="1" * 32, span_id=root.span_id),),
    )
    return Trace(trace_id="1" * 32, spans=(root, child), log_trace_ids=("1" * 32,))


def _query() -> TelemetryQuery:
    now = datetime.now(UTC)
    return TelemetryQuery("arn:test", "service", now, now)


def test_validates_stable_cross_invocation_invariants() -> None:
    errors = validate_trace(
        _trace(),
        {
            "minimum_spans": 2,
            "minimum_invocations": 2,
            "require_execution_correlation": True,
            "require_continuation": True,
            "require_log_trace_correlation": True,
            "required_outcomes": ["retry", "success"],
        },
        _query(),
    )
    assert errors == []


def test_reports_correlation_and_outcome_mismatches() -> None:
    errors = validate_trace(
        _trace("arn:wrong"),
        {
            "require_execution_correlation": True,
            "required_outcomes": ["failure"],
        },
        _query(),
    )
    assert any("durable execution ARN" in error for error in errors)
    assert any("Missing operation outcome" in error for error in errors)


def test_infers_retry_outcome_from_later_attempt() -> None:
    trace = _trace()
    root, child = trace.spans
    trace = replace(
        trace,
        spans=(
            replace(
                root,
                attributes={key: value for key, value in root.attributes.items() if key != "durable.operation.outcome"},
            ),
            replace(
                child,
                attributes={
                    **child.attributes,
                    "durable.attempt.number": 2,
                },
            ),
        ),
    )

    assert (
        validate_trace(
            trace,
            {"required_outcomes": ["retry", "success"]},
            _query(),
        )
        == []
    )


def test_asserts_any_property_and_nested_metadata_on_one_span() -> None:
    errors = validate_trace(
        _trace(),
        {
            "span_assertions": {
                "select": {
                    "name": "child",
                    "attributes": {"faas.invocation_id": "invocation-2"},
                },
                "expect": {
                    "trace_id": "1" * 32,
                    "span_id": "3" * 16,
                    "parent_span_id": "2" * 16,
                    "parent": {
                        "name": "root",
                        "status": "OK",
                        "parent_span_id": None,
                        "attributes": {
                            "faas.invocation_id": "invocation-1",
                            "durable.operation.outcome": "retry",
                        },
                    },
                    "name": "child",
                    "start_time": "*",
                    "end_time": "*",
                    "status": "OK",
                    "service_name": "service",
                    "attributes": {
                        "durable.execution.arn": "arn:test",
                        "faas.invocation_id": "invocation-2",
                        "custom.metadata": {
                            "attempt": 2,
                            "labels": ["durable", "resumed"],
                        },
                    },
                    "links": [
                        {
                            "trace_id": "1" * 32,
                            "span_id": "2" * 16,
                        }
                    ],
                },
            }
        },
        _query(),
    )

    assert errors == []


def test_reports_missing_external_and_mismatched_parent_assertions() -> None:
    trace = _trace()
    root, child = trace.spans
    external_child = Span(
        trace_id=child.trace_id,
        span_id=child.span_id,
        parent_span_id="9" * 16,
        name=child.name,
        start_time=child.start_time,
        end_time=child.end_time,
        status=child.status,
        service_name=child.service_name,
        attributes=child.attributes,
        links=child.links,
    )

    errors = validate_trace(
        trace,
        {
            "span_assertions": [
                {
                    "select": {"name": "root"},
                    "expect": {"parent": {"name": "root"}},
                },
                {
                    "select": {"name": "child"},
                    "expect": {
                        "parent": {
                            "name": "not-root",
                            "attributes": {"missing.key": "value"},
                        }
                    },
                },
                {
                    "select": {"name": "child"},
                    "expect": {"parent": "root"},
                },
            ]
        },
        _query(),
    )
    external_errors = validate_trace(
        Trace(trace_id=trace.trace_id, spans=(root, external_child)),
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {"parent": {"name": "root"}},
            }
        },
        _query(),
    )

    assert "span_assertions[0].expect.parent: selected span has no parent" in errors
    assert "span_assertions[1].expect.parent.name: expected 'not-root'" in errors
    assert "span_assertions[1].expect.parent.attributes.missing.key: property is missing" in errors
    assert "span_assertions[2].expect.parent must be a mapping" in errors
    assert external_errors == ["span_assertions[0].expect.parent: parent span is not present in the trace"]


def test_reports_missing_ambiguous_and_mismatched_span_assertions() -> None:
    errors = validate_trace(
        _trace(),
        {
            "span_assertions": [
                {
                    "select": {"name": "missing"},
                    "expect": {"status": "OK"},
                },
                {
                    "select": {"status": "OK"},
                    "expect": {"service_name": "service"},
                },
                {
                    "select": {"name": "child"},
                    "expect": {
                        "status": "ERROR",
                        "attributes": {"missing.key": "value"},
                        "links": [],
                    },
                },
            ]
        },
        _query(),
    )

    assert "span_assertions[0].select matched no spans" in errors
    assert "span_assertions[1].select matched 2 spans; it must select exactly one" in errors
    assert "span_assertions[2].expect.status: expected 'ERROR'" in errors
    assert "span_assertions[2].expect.attributes.missing.key: property is missing" in errors
    assert "span_assertions[2].expect.links: expected 0 item(s), found 1" in errors


def test_reports_invalid_span_assertion_schema() -> None:
    assert validate_trace(
        _trace(),
        {"span_assertions": "not-a-mapping-or-sequence"},
        _query(),
    ) == ["span_assertions must be a mapping or sequence of mappings"]

    errors = validate_trace(
        _trace(),
        {
            "span_assertions": [
                "not-a-mapping",
                {"select": "root", "expect": {}},
                {"select": {"name": "root"}},
                {"select": {}, "expect": {}, "unknown": True},
            ]
        },
        _query(),
    )

    assert errors == [
        "span_assertions[0] must be a mapping",
        "span_assertions[1].select must be a mapping",
        "span_assertions[2].expect must be a mapping",
        "span_assertions[3] has unknown field(s): unknown",
    ]


def test_redacts_secret_keys_and_values() -> None:
    payload = {
        "headers": "x-api-key=secret-value",
        "message": "request used secret-value",
        "nested": {"token": "secret-value"},
    }
    safe = redact(payload, secrets=["secret-value"])

    assert safe["headers"] == REDACTED
    assert safe["nested"]["token"] == REDACTED
    assert safe["message"] == f"request used {REDACTED}"
