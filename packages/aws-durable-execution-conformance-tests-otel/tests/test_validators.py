# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Provider-neutral telemetry validator and redaction tests."""

from __future__ import annotations

from datetime import UTC, datetime

from aws_durable_execution_conformance_tests_otel.model import (
    Span,
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
        },
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
