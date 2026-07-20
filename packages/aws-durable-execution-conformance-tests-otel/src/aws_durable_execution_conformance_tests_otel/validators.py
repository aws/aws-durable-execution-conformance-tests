# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Stable, provider-neutral OpenTelemetry conformance assertions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from aws_durable_execution_conformance_tests_otel.model import (
    TelemetryQuery,
    Trace,
    normalize_id,
)

_EXECUTION_ATTRIBUTE_KEYS = (
    "aws.lambda.durable_execution.arn",
    "durable.execution.arn",
    "durable_execution_arn",
)
_INVOCATION_ATTRIBUTE_KEYS = (
    "faas.invocation_id",
    "aws.lambda.invocation_id",
)
_OUTCOME_ATTRIBUTE_KEYS = (
    "durable.operation.outcome",
    "durable.execution.outcome",
    "durable.execution.status",
)


def _attribute_values(
    trace: Trace,
    keys: tuple[str, ...],
) -> list[str]:
    return [str(span.attributes[key]).lower() for span in trace.spans for key in keys if key in span.attributes]


def validate_trace(
    trace: Trace,
    assertions: Mapping[str, Any],
    query: TelemetryQuery,
) -> list[str]:
    """Validate stable integration invariants without prescribing span schemas."""

    errors: list[str] = []
    minimum_spans = int(assertions.get("minimum_spans", 1))
    if len(trace.spans) < minimum_spans:
        errors.append(f"Expected at least {minimum_spans} span(s), found {len(trace.spans)}")

    inconsistent = [span.span_id for span in trace.spans if span.trace_id != trace.trace_id]
    if inconsistent:
        errors.append("Canonical trace contains spans with a different trace id: " + ", ".join(inconsistent))

    if assertions.get("require_execution_correlation", True):
        execution_values = _attribute_values(trace, _EXECUTION_ATTRIBUTE_KEYS)
        if query.execution_arn.lower() not in execution_values:
            errors.append(
                "No span carries the durable execution ARN in a supported "
                f"correlation attribute ({', '.join(_EXECUTION_ATTRIBUTE_KEYS)})"
            )
        wrong = {value for value in execution_values if value != query.execution_arn.lower()}
        if wrong:
            errors.append("Spans contain conflicting durable execution correlation values")

    minimum_invocations = int(assertions.get("minimum_invocations", 1))
    if minimum_invocations > 1:
        invocations = set(_attribute_values(trace, _INVOCATION_ATTRIBUTE_KEYS))
        if len(invocations) < minimum_invocations:
            errors.append(
                f"Expected telemetry from at least {minimum_invocations} Lambda invocations, found {len(invocations)}"
            )

    required_outcomes = {str(value).lower() for value in assertions.get("required_outcomes", [])}
    if required_outcomes:
        actual_outcomes = set(_attribute_values(trace, _OUTCOME_ATTRIBUTE_KEYS))
        actual_outcomes.update(
            "success" if span.status == "OK" else "failure" for span in trace.spans if span.status in {"OK", "ERROR"}
        )
        missing = required_outcomes - actual_outcomes
        if missing:
            errors.append("Missing operation outcome(s): " + ", ".join(sorted(missing)))

    if assertions.get("require_continuation", False):
        span_ids = {span.span_id for span in trace.spans}
        linked = any(
            span.parent_span_id in span_ids
            or any(
                normalize_id(link.trace_id, 32) == trace.trace_id and link.span_id in span_ids for link in span.links
            )
            for span in trace.spans
        )
        if not linked:
            errors.append("No parent or span-link relationship connects the durable continuation")

    if assertions.get("require_log_trace_correlation", False):
        if not trace.log_trace_ids:
            errors.append("No log trace identifiers were returned by the backend")
        mismatched = {
            normalize_id(trace_id, 32)
            for trace_id in trace.log_trace_ids
            if normalize_id(trace_id, 32) != trace.trace_id
        }
        if mismatched:
            errors.append("Log trace identifiers do not match the active trace")

    return errors
